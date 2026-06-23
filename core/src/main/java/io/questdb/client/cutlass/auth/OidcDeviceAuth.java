/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.cutlass.auth;

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.cutlass.http.client.Fragment;
import io.questdb.client.cutlass.http.client.HttpClient;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.HttpClientFactory;
import io.questdb.client.cutlass.http.client.Response;
import io.questdb.client.cutlass.json.JsonException;
import io.questdb.client.cutlass.json.JsonLexer;
import io.questdb.client.cutlass.json.JsonParser;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Misc;
import io.questdb.client.std.Mutable;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Os;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.std.str.StringSink;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Obtains an OIDC access or id token via the OAuth 2.0 Device Authorization Grant
 * (RFC 8628), so a browserless process (remote notebook kernel, container, headless job)
 * can sign a human in: the user authorizes on any device while the token request travels
 * outbound only.
 * <p>
 * The token works on any auth path the server validates:
 * <ul>
 *     <li>HTTP {@code Authorization: Bearer <token>} (REST {@code /exec}, or the ingestion
 *     {@link io.questdb.client.Sender} via {@code httpToken});</li>
 *     <li>PG-wire: connect as user {@code _sso} with the token as the password
 *     (requires {@code acl.oidc.pg.token.as.password.enabled=true} on the server).</li>
 * </ul>
 * Typical use, discovering everything from the QuestDB server:
 * <pre>{@code
 * try (OidcDeviceAuth auth = OidcDeviceAuth.fromQuestDB("https://questdb.example.com:9000")) {
 *     String token = auth.getToken(); // signs in on first use, then caches and refreshes
 *     // ... use token as an HTTP Bearer header or a PG-wire _sso password ...
 * }
 * }</pre>
 * Or configuring the identity provider explicitly:
 * <pre>{@code
 * OidcDeviceAuth auth = OidcDeviceAuth.builder()
 *         .clientId("questdb")
 *         .deviceAuthorizationEndpoint("https://idp.example.com/as/device_authz.oauth2")
 *         .tokenEndpoint("https://idp.example.com/as/token.oauth2")
 *         .scope("openid groups")
 *         .groupsInToken(true)
 *         .build();
 * }</pre>
 * {@link #getToken()} serves a cached token while valid, silently refreshes when a refresh token
 * exists, otherwise re-runs the interactive flow. An instance lock serializes calls, so two
 * sign-ins never start at once. A sign-in waiting for the user holds that lock for the device code
 * lifetime (up to an hour), so a concurrent {@link #getToken()} or {@link #clearCache()} blocks
 * behind it - but {@link #getTokenSilently()} never waits: it fails fast with an
 * {@link OidcAuthException} so a request/flush path never stalls. To abort a waiting sign-in, call
 * {@link #close()} from another thread; it signals the flow to stop, which then fails with an
 * {@link OidcAuthException} rather than polling until the device code expires. Cancellation is seen
 * between polls (within ~100ms while waiting out an interval); a poll already in flight is not
 * interrupted, so the abort - and {@link #close()} - can take up to one HTTP request timeout (see
 * {@link Builder#httpTimeoutMillis(int)}), still far short of the device-code lifetime.
 * <p>
 * Instances are interactive and hold a network connection; close them when done. Token state is
 * in-memory only and does not survive a process restart.
 */
public class OidcDeviceAuth implements QuietCloseable {
    public static final String DEFAULT_SCOPE = "openid";
    static final String GRANT_TYPE_DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code";
    static final String GRANT_TYPE_REFRESH_TOKEN = "refresh_token";
    private static final int DEFAULT_CLOCK_SKEW_SECONDS = 30;
    // device code TTL when the device authorization response omits (or zeroes) expires_in; matches Python
    private static final int DEFAULT_DEVICE_CODE_TTL_SECONDS = 600;
    private static final int DEFAULT_HTTP_TIMEOUT_MILLIS = 30_000;
    private static final int DEFAULT_POLL_INTERVAL_SECONDS = 5;
    // token cache TTL when the token response omits expires_in
    private static final int DEFAULT_TOKEN_TTL_SECONDS = 300;
    private static final String ERROR_AUTHORIZATION_PENDING = "authorization_pending";
    private static final String ERROR_SLOW_DOWN = "slow_down";
    private static final HttpClientConfiguration HTTP_CONFIG = DefaultHttpClientConfiguration.INSTANCE;
    // Token responses carry JWTs (an id token with group claims can be several KB), and a single
    // value may arrive split across HTTP fragments. The lexer stashes a split value and rejects it
    // past JSON_LEXER_MAX_VALUE_BYTES, so the limit must comfortably exceed any real token or large
    // tokens fail to parse with "String is too long".
    private static final int JSON_LEXER_CACHE_SIZE = 1024;
    private static final int JSON_LEXER_MAX_VALUE_BYTES = 1 << 20;
    // abort polling after this many consecutive transport failures instead of silently retrying
    // until the device code expires
    private static final int MAX_CONSECUTIVE_POLL_ERRORS = 3;
    // upper bound on the device code lifetime (the device authorization response's expires_in), so a
    // hostile or buggy provider cannot make the client poll for an absurd duration; matches the Python client
    private static final int MAX_DEVICE_CODE_TTL_SECONDS = 1800;
    // upper bound on the token cache lifetime (the token response's expires_in), so an absurd or hostile
    // value cannot overflow the timing arithmetic or make the client trust a token for absurdly long
    private static final int MAX_EXPIRES_IN_SECONDS = 3600;
    private static final int MAX_POLL_INTERVAL_SECONDS = 300;
    // cap bytes drained per response so a hostile/MITM'd server cannot stream an endless body and
    // wedge the thread; far above any real OIDC JSON response
    private static final int MAX_RESPONSE_BODY_BYTES = 4 * 1024 * 1024;
    private static final int POLL_PENDING = 1;
    private static final long POLL_SLEEP_SLICE_MILLIS = 100;
    private static final int POLL_SLOW_DOWN = 2;
    private static final int POLL_SUCCESS = 0;
    private static final int POLL_TRANSIENT_ERROR = 3;
    private static final int SLOW_DOWN_INCREMENT_SECONDS = 5;
    private static final String USER_AGENT = "questdb/java-client-oidc";
    private static final String WELL_KNOWN_OPENID_CONFIGURATION_PATH = "/.well-known/openid-configuration";
    private final String audience;
    private final String clientId;
    private final long clockSkewMillis;
    private final DeviceAuthorizationResponseParser deviceAuthParser = new DeviceAuthorizationResponseParser();
    private final Endpoint deviceAuthorizationEndpoint;
    private final StringSink formSink = new StringSink();
    private final boolean groupsInToken;
    private final int httpTimeoutMillis;
    // serializes getToken()/getTokenSilently()/clearCache()/close(); getToken() holds it for the whole
    // interactive flow, getTokenSilently() uses tryLock so the flush path never stalls behind a sign-in
    private final ReentrantLock lock = new ReentrantLock();
    private final DeviceCodePrompt prompt;
    private final StringSink responseStatus = new StringSink();
    private final String scope;
    private final ClientTlsConfiguration tlsConfig;
    private final Endpoint tokenEndpoint;
    private final TokenResponseParser tokenParser = new TokenResponseParser();
    private String accessToken;
    private volatile boolean closed;
    private long expiresAtMillis;
    private String idToken;
    private JsonLexer jsonLexer;
    private HttpClient plainClient;
    private String refreshToken;
    private HttpClient tlsClient;

    private OidcDeviceAuth(Builder builder, ClientTlsConfiguration tlsConfig) {
        this.clientId = builder.clientId;
        this.deviceAuthorizationEndpoint = Endpoint.parse(builder.deviceAuthorizationEndpoint);
        this.tokenEndpoint = Endpoint.parse(builder.tokenEndpoint);
        this.scope = builder.scope;
        this.audience = builder.audience;
        this.groupsInToken = builder.groupsInToken;
        this.httpTimeoutMillis = builder.httpTimeoutMillis;
        this.clockSkewMillis = builder.clockSkewSeconds * 1000L;
        this.prompt = builder.prompt;
        this.tlsConfig = tlsConfig;
        // allocate the native lexer last: an Endpoint.parse above can throw on a malformed url, and
        // the half-built instance is never returned, so close() could not free an earlier alloc
        this.jsonLexer = new JsonLexer(JSON_LEXER_CACHE_SIZE, JSON_LEXER_MAX_VALUE_BYTES);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Discovers the OIDC configuration from a running QuestDB server and builds an instance.
     * Reads the public {@code /settings} endpoint (no auth) for the client id, scope, token
     * endpoint, device authorization endpoint and groups-in-token mode.
     * <p>
     * <b>Trust model:</b> the endpoints the user signs in against come from the server's
     * unauthenticated {@code /settings} response, so a spoofed, compromised, or MITM'd server can
     * redirect the whole sign-in to an attacker-controlled identity provider and harvest the
     * authorization. Only call {@code fromQuestDB} against a trusted server reached over {@code https}
     * (required by default; {@link Builder#allowInsecureTransport(boolean)} removes that protection).
     * For an untrusted server, configure the identity provider explicitly with {@link #builder()}, or
     * pin it via {@link #fromQuestDB(String, DiscoveryOptions)} and {@link DiscoveryOptions#issuer(String)}.
     *
     * @param questdbUrl the QuestDB HTTP base URL, for example {@code https://questdb.example.com:9000}
     * @return a configured, ready-to-use instance
     * @throws OidcAuthException if the server has OIDC disabled, or does not advertise a device
     *                           authorization endpoint and no issuer was pinned to discover it
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl) {
        return fromQuestDB(questdbUrl, new DiscoveryOptions());
    }

    /**
     * Discovers the OIDC configuration from a running QuestDB server, like {@link #fromQuestDB(String)},
     * but with explicit {@link DiscoveryOptions}: an identity provider pin (issuer or discovery URL), a
     * TLS configuration, an insecure-transport opt-in, and the device code prompt - for example
     * {@link DeviceCodePrompt#openBrowser()} to also open the verification URL in a browser.
     *
     * @param questdbUrl the QuestDB HTTP base URL, for example {@code https://questdb.example.com:9000}
     * @param options    how to pin the identity provider, configure TLS, permit insecure transport, and
     *                   show the device code challenge; see {@link DiscoveryOptions}
     * @return a configured, ready-to-use instance
     * @throws OidcAuthException if the server has OIDC disabled, or does not advertise a device
     *                           authorization endpoint and no issuer or discovery URL was pinned
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl, DiscoveryOptions options) {
        String issuer = options.issuer;
        String discoveryUrl = options.discoveryUrl;
        ClientTlsConfiguration tlsConfig = options.tlsConfig != null ? options.tlsConfig : defaultTlsConfig();
        boolean allowInsecureTransport = options.allowInsecureTransport;
        Endpoint server = Endpoint.parse(questdbUrl);
        if (!allowInsecureTransport) {
            requireSecureTransport(server.isTls, "QuestDB server url", questdbUrl);
        }
        SettingsDiscoveryParser parser = new SettingsDiscoveryParser();
        discoverSettings(server, tlsConfig, parser);
        if (!parser.isOidcEnabled) {
            throw new OidcAuthException().put("OIDC is not enabled on the QuestDB server [url=").put(questdbUrl).put(']');
        }
        if (parser.clientId.length() == 0) {
            throw new OidcAuthException().put("the QuestDB server does not advertise an OIDC client id [url=").put(questdbUrl).put(']');
        }
        String tokenEndpoint = parser.tokenEndpoint.length() > 0 ? parser.tokenEndpoint.toString() : null;
        String deviceAuthorizationEndpoint = parser.deviceAuthorizationEndpoint.length() > 0 ? parser.deviceAuthorizationEndpoint.toString() : null;
        String resolvedIssuer = issuer != null && !issuer.isEmpty() ? issuer : null;
        String pinnedDiscoveryUrl = discoveryUrl != null && !discoveryUrl.isEmpty() ? discoveryUrl : null;

        // Over a plaintext, MITM-able http /settings channel (only reachable with allowInsecureTransport;
        // the default rejects it), advertised endpoints can be tampered in transit to route the device
        // code and long-lived refresh token to an attacker. The missing-endpoint discovery path below
        // already demands an out-of-band pin, but a tampered /settings advertising BOTH endpoints at one
        // attacker origin skips that path - the co-location check passes trivially and there is no issuer
        // to pin against - so require the same pin before trusting /settings endpoints over such a channel.
        boolean settingsSuppliedCredentials = tokenEndpoint != null || deviceAuthorizationEndpoint != null;
        if (settingsSuppliedCredentials && resolvedIssuer == null && pinnedDiscoveryUrl == null && settingsChannelIsPlaintext(server)) {
            throw new OidcAuthException()
                    .put("the QuestDB server was reached over insecure http, so its /settings response - and the OIDC ")
                    .put("endpoints it advertises - can be tampered in transit and used to redirect the device-code and ")
                    .put("refresh-token requests to an attacker; pin the identity provider with an issuer (its origin, for ")
                    .put("example https://your-idp), configure the endpoints explicitly with OidcDeviceAuth.builder(), or ")
                    .put("connect to QuestDB over https [url=").put(questdbUrl).put(']');
        }

        // For /settings-supplied endpoints with an out-of-band issuer, require each under the issuer's PATH,
        // not just its origin (validateEndpointOrigins): a path-based identity provider shares one origin per
        // tenant (Keycloak issuers are https://host/realms/<realm>), so the origin check alone cannot stop a
        // tampered /settings from steering credentials to a different realm. The issuer is supplied out of
        // band and cannot be forged. Endpoints discovered from the identity provider below are not scoped this
        // way - some providers (for example Azure AD) place their endpoints outside the issuer path.
        if (issuer != null && !issuer.isEmpty()) {
            if (tokenEndpoint != null && !isEndpointUnderIssuerPath(tokenEndpoint, issuer)) {
                throw endpointNotUnderIssuer("token endpoint", tokenEndpoint, issuer);
            }
            if (deviceAuthorizationEndpoint != null && !isEndpointUnderIssuerPath(deviceAuthorizationEndpoint, issuer)) {
                throw endpointNotUnderIssuer("device authorization endpoint", deviceAuthorizationEndpoint, issuer);
            }
        }

        // Fall back to identity provider discovery when the server omits the device authorization endpoint
        // (and/or the token endpoint). The provider's origin must be pinned out of band: the discovery
        // target is never derived from a server-supplied value, else a tampered or intercepted /settings
        // could steer discovery - and the credential POSTs - to an attacker while the co-location and
        // issuer checks pass trivially.
        if (deviceAuthorizationEndpoint == null || tokenEndpoint == null) {
            if (resolvedIssuer == null && pinnedDiscoveryUrl == null) {
                throw new OidcAuthException()
                        .put("the QuestDB server did not advertise the OIDC device authorization endpoint (and/or the token ")
                        .put("endpoint), so it must be discovered from the identity provider, but the identity provider is not ")
                        .put("pinned; pass an issuer (its origin, for example https://your-idp) to OidcDeviceAuth.fromQuestDB so ")
                        .put("a tampered or intercepted /settings response cannot redirect the device-code and refresh-token ")
                        .put("requests to an attacker, or configure the endpoints explicitly with OidcDeviceAuth.builder() [url=")
                        .put(questdbUrl).put(']');
            }
            WellKnownDiscoveryParser doc = new WellKnownDiscoveryParser();
            discoverFromIdp(resolvedIssuer, pinnedDiscoveryUrl, tlsConfig, allowInsecureTransport, doc);
            if (deviceAuthorizationEndpoint == null && doc.deviceAuthorizationEndpoint.length() > 0) {
                deviceAuthorizationEndpoint = doc.deviceAuthorizationEndpoint.toString();
            }
            if (tokenEndpoint == null && doc.tokenEndpoint.length() > 0) {
                tokenEndpoint = doc.tokenEndpoint.toString();
            }
            // The endpoint pin's trust anchor is the out-of-band discovery origin (the caller's issuer, else
            // the discoveryUrl origin derived after this block), never an issuer the document declares about
            // itself. When discovery ran off a pinned discoveryUrl (no caller issuer), reject a document
            // whose own "issuer" is on a different origin (RFC 8414 section 3.3): else a tampered or
            // content-injected document at the pinned url could name an attacker issuer, co-locate both
            // endpoints under it, and route the device code and refresh token there while the checks below
            // pass trivially. A provider serving its discovery document on a different origin than its
            // endpoints must use explicit endpoints via OidcDeviceAuth.builder().
            if (resolvedIssuer == null && pinnedDiscoveryUrl != null && doc.issuer.length() > 0) {
                Endpoint docIssuer = Endpoint.parse(doc.issuer.toString());
                Endpoint discoveryEndpoint = Endpoint.parse(pinnedDiscoveryUrl);
                if (!sameOrigin(docIssuer, discoveryEndpoint)) {
                    throw new OidcAuthException()
                            .put("the OIDC discovery document declares an issuer (").put(originOf(docIssuer))
                            .put(") on a different origin than the pinned discovery url (").put(originOf(discoveryEndpoint))
                            .put("); refusing to send credentials to an issuer outside the pinned discovery origin");
                }
            }
        }

        // A caller-supplied discoveryUrl pins the provider just as an issuer does: derive the pin origin
        // from it so validateEndpointOrigins rejects any endpoint not on it - whether read from the
        // discovery document above or advertised by /settings (when it supplied both endpoints and the
        // discovery branch was skipped). Without this, a tampered response advertising both endpoints at one
        // attacker origin would slip past a discoveryUrl pin, the co-location check alone passing trivially.
        if (resolvedIssuer == null && pinnedDiscoveryUrl != null) {
            resolvedIssuer = originOf(Endpoint.parse(pinnedDiscoveryUrl));
        }

        if (tokenEndpoint == null) {
            throw new OidcAuthException()
                    .put("could not resolve the OIDC token endpoint from the QuestDB /settings response or the identity ")
                    .put("provider discovery document; configure it explicitly with OidcDeviceAuth.builder() [url=").put(questdbUrl).put(']');
        }
        if (deviceAuthorizationEndpoint == null) {
            throw new OidcAuthException()
                    .put("could not resolve the device authorization endpoint; the identity provider discovery document did ")
                    .put("not advertise \"device_authorization_endpoint\". Ensure the identity provider supports the device ")
                    .put("grant, or configure the endpoint explicitly with OidcDeviceAuth.builder() [url=").put(questdbUrl).put(']');
        }
        return builder()
                .clientId(parser.clientId.toString())
                .deviceAuthorizationEndpoint(deviceAuthorizationEndpoint)
                .tokenEndpoint(tokenEndpoint)
                .scope(parser.scope.length() > 0 ? parser.scope.toString() : DEFAULT_SCOPE)
                .audience(parser.audience.length() > 0 ? parser.audience.toString() : null)
                .groupsInToken(parser.groupsInToken)
                .issuer(resolvedIssuer)
                .allowInsecureTransport(allowInsecureTransport)
                .tlsConfig(tlsConfig)
                .prompt(options.prompt)
                .build();
    }

    /**
     * Drops any cached token so the next {@link #getToken()} starts a fresh interactive sign-in.
     */
    public void clearCache() {
        lock.lock();
        try {
            throwIfClosed();
            accessToken = null;
            idToken = null;
            refreshToken = null;
            expiresAtMillis = 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Frees the network connections and native buffers this instance holds. If a {@link #getToken()}
     * sign-in is in flight on another thread, signals it to stop so it fails with an
     * {@link OidcAuthException} instead of polling until the device code expires. The signal is observed
     * between polls (within ~100ms while waiting out a poll interval); a poll request already in flight
     * is not interrupted, so {@code close()} acquires the lock - and returns - only once that request
     * finishes or times out, i.e. after at most one HTTP request timeout
     * (see {@link Builder#httpTimeoutMillis(int)}), not the full device-code lifetime. Idempotent. After
     * close, {@link #getToken()} and {@link #clearCache()} throw.
     */
    @Override
    public void close() {
        // flag cancellation before taking the lock: getToken() holds it for the whole flow, so signal the
        // in-flight sign-in to stop via a lock-free volatile write, then acquire the lock - released by the
        // cancelled flow once it observes the flag (between polls, or after an in-flight poll returns) - and
        // free the native resources. close() never frees while a flow holds the lock, so no use-after-free
        closed = true;
        lock.lock();
        try {
            plainClient = Misc.free(plainClient);
            tlsClient = Misc.free(tlsClient);
            jsonLexer = Misc.free(jsonLexer);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return {@code "Bearer " + getToken()}, ready to use as the value of an HTTP
     * {@code Authorization} header.
     */
    public String getAuthorizationHeaderValue() {
        return "Bearer " + getToken();
    }

    /**
     * Returns a valid token to present to QuestDB: the cached token while still valid, otherwise a
     * silent refresh when possible, otherwise the interactive device flow. The token is the id token
     * when the server expects groups encoded in the token, the access token otherwise.
     *
     * @return a non-null, non-empty token
     * @throws OidcAuthException if the interactive flow fails, times out, or the identity provider
     *                           does not return the expected token
     */
    public String getToken() {
        lock.lock();
        try {
            throwIfClosed();
            // only the kind of token getToken() actually serves counts as a cache hit; a grant that
            // returned the other kind (access token when the server wants the id token, or vice versa)
            // leaves the served token null, so re-run the flow rather than report the unusable grant as
            // valid and have selectToken() throw on this and every later call
            final String cachedToken = groupsInToken ? idToken : accessToken;
            if (cachedToken != null) {
                if (System.currentTimeMillis() < expiresAtMillis - clockSkewMillis) {
                    return cachedToken;
                }
                if (refreshToken != null && tryRefresh()) {
                    return selectToken();
                }
            }
            runDeviceFlow();
            return selectToken();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Like {@link #getToken()} but never starts the interactive device flow and never blocks: returns
     * the cached token while valid, silently refreshes when a refresh token is available, otherwise
     * throws. Designed for the request/flush path of a long-lived client, for example
     * {@code Sender.builder(...).httpTokenProvider(auth::getTokenSilently)}, where an interactive prompt
     * is inappropriate and a stalled flush unacceptable. Call {@link #getToken()} once to sign in first.
     * <p>
     * To keep the flush path responsive it returns or throws promptly - it never waits for an interactive
     * {@link #getToken()} on another thread (which would stall the flush for the whole device-code
     * lifetime). While such a sign-in runs there is no token to return anyway, so it throws and the caller
     * should retry once the sign-in completes.
     *
     * @return a non-null, non-empty token
     * @throws OidcAuthException if no token has been obtained yet, if the cached token expired and could
     *                           not be refreshed without an interactive sign-in, or if a sign-in or
     *                           refresh is already in progress on another thread
     */
    public String getTokenSilently() {
        throwIfClosed();
        // never wait on the flush path: getToken()'s sign-in holds the lock for the whole device-code
        // lifetime (up to an hour), so tryLock and fail fast if held. A sign-in in progress means there
        // is no token to serve yet, so the caller gets a prompt exception to retry rather than a stalled
        // flush
        if (!lock.tryLock()) {
            throw new OidcAuthException("a sign-in or token refresh is already in progress on another thread; no token is available without blocking - retry shortly");
        }
        try {
            throwIfClosed();
            final String cachedToken = groupsInToken ? idToken : accessToken;
            if (cachedToken != null) {
                if (System.currentTimeMillis() < expiresAtMillis - clockSkewMillis) {
                    return cachedToken;
                }
                if (refreshToken != null && tryRefresh()) {
                    return selectToken();
                }
                throw new OidcAuthException("the cached token expired and could not be refreshed without an interactive sign-in; call getToken() to sign in again");
            }
            throw new OidcAuthException("no token has been obtained yet; call getToken() to sign in before using getTokenSilently()");
        } finally {
            lock.unlock();
        }
    }

    private static String appendSettingsPath(String basePath) {
        String trimmed = basePath;
        while (trimmed.length() > 1 && trimmed.charAt(trimmed.length() - 1) == '/') {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return "/".equals(trimmed) ? "/settings" : trimmed + "/settings";
    }

    private static int boundedSeconds(int value, int defaultValue, int maxValue) {
        if (value <= 0) {
            return defaultValue;
        }
        return Math.min(value, maxValue);
    }

    private static ClientTlsConfiguration defaultTlsConfig() {
        return new ClientTlsConfiguration(null, null, ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL);
    }

    private static void discardBody(Response body, int timeoutMillis) {
        // best-effort drain after a parse failure to keep the keep-alive connection usable; bounded like
        // parseBody so a hostile server cannot wedge the thread here either
        final long deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
        long totalBytes = 0;
        try {
            while (true) {
                final long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return;
                }
                Fragment fragment = body.recv((int) Math.max(1, Math.min(remainingNanos / 1_000_000L, Integer.MAX_VALUE)));
                if (fragment == null) {
                    return;
                }
                totalBytes += fragment.hi() - fragment.lo();
                if (totalBytes > MAX_RESPONSE_BODY_BYTES) {
                    return;
                }
            }
        } catch (HttpClientException ignore) {
            // the connection is re-established on the next request if it is now unusable
        }
    }

    private static void discoverFromIdp(String issuer, String discoveryUrl, ClientTlsConfiguration tlsConfig, boolean allowInsecureTransport, WellKnownDiscoveryParser parser) {
        // the discovery URL is pinned out of band (a caller-supplied discoveryUrl, else built from the
        // issuer; the caller guarantees one is non-null), so the server cannot choose where discovery -
        // and the credential POSTs it resolves - are aimed
        String url = discoveryUrl != null ? discoveryUrl : wellKnownUrl(issuer);
        Endpoint endpoint = Endpoint.parse(url);
        requireSecureIdpEndpoint(endpoint, "OIDC issuer / discovery url", url, allowInsecureTransport);
        fetchJson(endpoint, endpoint.path, tlsConfig, parser,
                "could not reach the identity provider to discover OIDC settings",
                "could not parse the identity provider discovery document");
    }

    private static void discoverSettings(Endpoint server, ClientTlsConfiguration tlsConfig, SettingsDiscoveryParser parser) {
        fetchJson(server, appendSettingsPath(server.path), tlsConfig, parser,
                "could not reach the QuestDB server to discover OIDC settings",
                "could not parse the QuestDB /settings response");
    }

    private static void fetchJson(Endpoint endpoint, String path, ClientTlsConfiguration tlsConfig, JsonParser parser, String reachError, String parseError) {
        HttpClient client = endpoint.isTls
                ? HttpClientFactory.newTlsInstance(HTTP_CONFIG, tlsConfig)
                : HttpClientFactory.newPlainTextInstance(HTTP_CONFIG);
        JsonLexer lexer = new JsonLexer(JSON_LEXER_CACHE_SIZE, JSON_LEXER_MAX_VALUE_BYTES);
        try {
            HttpClient.Request request = client.newRequest(endpoint.host, endpoint.port)
                    .GET()
                    .url(path)
                    .header("Accept", "application/json")
                    .header("User-Agent", USER_AGENT);
            HttpClient.ResponseHeaders response = request.send(DEFAULT_HTTP_TIMEOUT_MILLIS);
            response.await(DEFAULT_HTTP_TIMEOUT_MILLIS);
            Response body = response.getResponse();
            // parseBody enforces a wall-clock deadline and a byte cap so an untrusted server cannot wedge
            // discovery, and its parseLast rejects a truncated document
            parseBody(body, lexer, parser, DEFAULT_HTTP_TIMEOUT_MILLIS);
        } catch (HttpClientException e) {
            throw new OidcAuthException(e).put(reachError);
        } catch (JsonException e) {
            throw new OidcAuthException(e).put(parseError);
        } finally {
            Misc.free(lexer);
            Misc.free(client);
        }
    }

    private static boolean isDottedIpv4(String host) {
        // validate a dotted IPv4 literal (four 0-255 octets) without DNS, so a hostname merely starting
        // with "127." is not mistaken for the loopback block
        int octets = 1;
        int value = 0;
        int digits = 0;
        for (int i = 0, n = host.length(); i < n; i++) {
            char c = host.charAt(i);
            if (c == '.') {
                if (digits == 0 || value > 255) {
                    return false;
                }
                octets++;
                value = 0;
                digits = 0;
            } else if (c >= '0' && c <= '9') {
                value = value * 10 + (c - '0');
                if (++digits > 3) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return octets == 4 && digits > 0 && value <= 255;
    }

    private static String[] decodePathSegments(String path) {
        // Repeatedly percent-decode (a server or proxy may unescape more than once, so %252e%252e -> .. )
        // and fold backslash to slash (some proxies do), then split into segments. Comparing these decoded
        // segments, not the raw wire string, means an encoding the server later undoes cannot hide a "..".
        String decoded = path;
        for (int i = 0; i < 10; i++) { // bounded; a real path needs 0-1 passes
            String next = percentDecodeOnce(decoded);
            if (next.equals(decoded)) {
                break;
            }
            decoded = next;
        }
        return decoded.replace('\\', '/').split("/", -1);
    }

    private static OidcAuthException endpointNotUnderIssuer(String label, String url, String issuer) {
        return new OidcAuthException()
                .put("the OIDC ").put(label).put(" advertised by the QuestDB /settings response (").put(url)
                .put(") is not under the pinned issuer (").put(issuer).put("); refusing to send credentials to ")
                .put("an endpoint outside the trusted issuer, for example a different realm on the same host; ")
                .put("if the identity provider places its endpoints outside the issuer path, configure them ")
                .put("explicitly with OidcDeviceAuth.builder()");
    }

    private static int hexValue(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a' + 10;
        }
        if (c >= 'A' && c <= 'F') {
            return c - 'A' + 10;
        }
        return -1;
    }

    private static boolean isEndpointUnderIssuerPath(String endpointUrl, String issuer) {
        // The endpoint's path must be the issuer's path or a sub-path of it, compared segment by segment (so
        // /realms/prod does not match /realms/production). A root issuer (no path) constrains the origin only.
        // This stops a tampered /settings from redirecting credentials to a different tenant on a path-based
        // multi-tenant identity provider (Keycloak issuers are https://host/realms/<realm>), which the origin
        // check alone cannot catch. Mirrors the Python client.
        String basePath = pathOnly(issuer);
        int baseEnd = basePath.length();
        while (baseEnd > 0 && basePath.charAt(baseEnd - 1) == '/') {
            baseEnd--; // trailing slashes do not add a path segment
        }
        if (baseEnd == 0) {
            return true; // root issuer: origin-only, every path is under it
        }
        String[] baseSegs = decodePathSegments(basePath.substring(0, baseEnd));
        String[] endpointSegs = decodePathSegments(pathOnly(endpointUrl));
        // a "." or ".." segment is rejected outright: the server normalizes it away, so a naive prefix test
        // would pass /realms/acme/../evil/token yet it resolves to a different realm
        for (int i = 0; i < endpointSegs.length; i++) {
            if (".".equals(endpointSegs[i]) || "..".equals(endpointSegs[i])) {
                return false;
            }
        }
        if (endpointSegs.length < baseSegs.length) {
            return false;
        }
        for (int i = 0; i < baseSegs.length; i++) {
            if (!baseSegs[i].equals(endpointSegs[i])) {
                return false;
            }
        }
        return true;
    }

    private static String pathOnly(String url) {
        // the path component only (drop any ?query / #fragment); a ;matrix parameter stays part of the path,
        // so a traversal hidden in it (.../token;..%2f..) is still scanned
        String path = Endpoint.parse(url).path;
        for (int i = 0, n = path.length(); i < n; i++) {
            char c = path.charAt(i);
            if (c == '?' || c == '#') {
                return path.substring(0, i);
            }
        }
        return path;
    }

    private static String percentDecodeOnce(String s) {
        int pct = s.indexOf('%');
        if (pct < 0) {
            return s; // nothing encoded
        }
        StringSink sink = new StringSink();
        sink.put(s, 0, pct);
        for (int i = pct, n = s.length(); i < n; ) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < n) {
                int hi = hexValue(s.charAt(i + 1));
                int lo = hexValue(s.charAt(i + 2));
                if (hi >= 0 && lo >= 0) {
                    sink.put((char) ((hi << 4) | lo));
                    i += 3;
                    continue;
                }
            }
            sink.put(c);
            i++;
        }
        return sink.toString();
    }

    private static boolean isLoopbackHost(String host) {
        // loopback traffic never leaves the host, so a plaintext /settings fetch to it has no network
        // interception risk; match localhost and the whole IPv4 127.0.0.0/8 block
        return host != null && (host.equalsIgnoreCase("localhost") || (host.startsWith("127.") && isDottedIpv4(host)));
    }

    private static String originOf(Endpoint endpoint) {
        return (endpoint.isTls ? "https://" : "http://") + endpoint.host + ':' + endpoint.port;
    }

    private static void parseBody(Response body, JsonLexer lexer, JsonParser parser, int timeoutMillis) throws JsonException {
        // read and parse the whole body, bounded by a wall-clock deadline and a cumulative byte cap, so a
        // hostile or stalled server cannot wedge the thread by dribbling or endlessly streaming
        final long deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
        long totalBytes = 0;
        while (true) {
            final long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new HttpClientException("timed out reading the identity provider response body");
            }
            Fragment fragment = body.recv((int) Math.max(1, Math.min(remainingNanos / 1_000_000L, Integer.MAX_VALUE)));
            if (fragment == null) {
                break;
            }
            totalBytes += fragment.hi() - fragment.lo();
            if (totalBytes > MAX_RESPONSE_BODY_BYTES) {
                throw new HttpClientException("the identity provider response body exceeded the size limit");
            }
            lexer.parse(fragment.lo(), fragment.hi(), parser);
        }
        lexer.parseLast(); // reject a truncated body (unterminated string/object)
    }

    private static int parseIntOrZero(CharSequence value) {
        try {
            return Numbers.parseInt(value);
        } catch (NumericException e) {
            return 0;
        }
    }

    private static void putNonNull(StringSink sink, CharSequence tag) {
        // clear before storing so a repeated key replaces, not concatenates onto, the previous value; a
        // JSON null arrives from the lexer as the literal "null", so treat it as absent rather than store
        // the 4-char string "null" as a token, error code, endpoint or user code
        sink.clear();
        if (!Chars.equals("null", tag)) {
            sink.put(tag);
        }
    }

    private static void requireSecureIdpEndpoint(Endpoint endpoint, String label, String url, boolean allowInsecureTransport) {
        // https is always fine; plaintext http is allowed only to a loopback host, where the request never
        // leaves the machine. allowInsecureTransport relaxes the QuestDB link but never the identity
        // provider: the device code and refresh token must not cross the network in cleartext (matching
        // the Python client)
        if (endpoint.isTls || isLoopbackHost(endpoint.host)) {
            return;
        }
        OidcAuthException ex = new OidcAuthException()
                .put("the ").put(label).put(" uses insecure http, which would send the device code and ")
                .put("refresh token across the network in cleartext; use an https url");
        if (allowInsecureTransport) {
            ex.put(" (allowInsecureTransport relaxes only the QuestDB connection, not the identity provider endpoints)");
        }
        throw ex.put(" [url=").put(url).put(']');
    }

    private static void requireSecureTransport(boolean isTls, String label, String url) {
        if (!isTls) {
            throw new OidcAuthException()
                    .put("the ").put(label).put(" uses insecure http, which exposes the OIDC sign-in to network ")
                    .put("attackers; use an https url, or call allowInsecureTransport(true) to override [url=").put(url).put(']');
        }
    }

    private static boolean sameOrigin(Endpoint a, Endpoint b) {
        // scheme (via isTls), host and port - the security origin; path is deliberately not compared, the
        // token and device endpoints legitimately differ in path on one authorization server
        return a.isTls == b.isTls && a.port == b.port && a.host.equalsIgnoreCase(b.host);
    }

    private static String sanitizeForDisplay(String value) {
        if (value == null) {
            return null;
        }
        final int n = value.length();
        int firstUnsafe = -1;
        for (int i = 0; i < n; ) {
            final int cp = value.codePointAt(i);
            if (OidcAuthException.isUnsafeForDisplay(cp)) {
                firstUnsafe = i;
                break;
            }
            i += Character.charCount(cp);
        }
        if (firstUnsafe < 0) {
            return value; // common case: nothing to strip
        }
        // an attacker-influenced device-auth field can smuggle in terminal-spoofing characters - ANSI
        // escapes, CR/LF, or bidi/zero-width formatting (including supplementary-plane "tag" chars that
        // arrive as surrogate pairs) - that reorder or hide text, so strip them per code point; else a
        // right-to-left override could make the verification URL a human reads differ from the one their
        // browser opens
        StringSink sink = new StringSink();
        sink.put(value, 0, firstUnsafe);
        for (int i = firstUnsafe; i < n; ) {
            final int cp = value.codePointAt(i);
            final int count = Character.charCount(cp);
            if (!OidcAuthException.isUnsafeForDisplay(cp)) {
                sink.put(value, i, i + count);
            }
            i += count;
        }
        return sink.toString();
    }

    private static boolean settingsChannelIsPlaintext(Endpoint server) {
        // /settings over plaintext http to a non-loopback host is MITM-able (only possible with
        // allowInsecureTransport; the default rejects it), so its advertised endpoints must not be trusted
        // to route credentials without an out-of-band pin
        return !server.isTls && !isLoopbackHost(server.host);
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static void validateEndpointOrigins(Endpoint tokenEndpoint, Endpoint deviceAuthorizationEndpoint, Endpoint issuer) {
        // the device code and long-lived refresh token are POSTed to the device authorization and token
        // endpoints. RFC 8628 co-locates them on one authorization server, so reject a config that splits
        // them across origins (a tampered /settings or discovery document siphoning one off), and - when
        // the issuer is pinned - reject either endpoint not on it. The pin compares origins, so a provider
        // hosting its endpoints on a different origin than its issuer must be configured without an issuer
        // (or with explicit endpoints).
        if (!sameOrigin(tokenEndpoint, deviceAuthorizationEndpoint)) {
            throw new OidcAuthException()
                    .put("the OIDC token and device authorization endpoints are on different origins (")
                    .put(originOf(tokenEndpoint)).put(" vs ").put(originOf(deviceAuthorizationEndpoint))
                    .put("); refusing to send credentials. This indicates a misconfigured or tampered OIDC configuration");
        }
        if (issuer != null) {
            if (!sameOrigin(tokenEndpoint, issuer)) {
                throw new OidcAuthException()
                        .put("the OIDC token endpoint origin (").put(originOf(tokenEndpoint))
                        .put(") does not match the issuer origin (").put(originOf(issuer))
                        .put("); refusing to send credentials to an endpoint outside the trusted issuer");
            }
            if (!sameOrigin(deviceAuthorizationEndpoint, issuer)) {
                throw new OidcAuthException()
                        .put("the OIDC device authorization endpoint origin (").put(originOf(deviceAuthorizationEndpoint))
                        .put(") does not match the issuer origin (").put(originOf(issuer))
                        .put("); refusing to send credentials to an endpoint outside the trusted issuer");
            }
        }
    }

    private static void validateTokenChars(CharSequence token, String tokenName) {
        // The selected token goes verbatim into the "Authorization: Bearer <token>" header sent to the
        // trusted QuestDB server and into the PG-wire _sso password. A CR/LF or other control char would
        // break out of the header into the request line (the lexer now decodes a \r or \n escape in the
        // provider's response into a real control byte), and a non-ASCII char is silently truncated to one
        // byte by the ASCII header writer. A real OAuth token is printable ASCII, so reject anything else
        // rather than route a tampered or corrupt credential onto the wire. Token bytes are never embedded
        // in the message: they are the secret this class protects.
        for (int i = 0, n = token.length(); i < n; i++) {
            char c = token.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                throw new OidcAuthException()
                        .put("the identity provider returned an ").put(tokenName)
                        .put(" containing a disallowed control or non-ASCII character; refusing to use it as a credential");
            }
        }
    }

    private static String wellKnownUrl(String issuer) {
        String trimmed = issuer;
        while (trimmed.length() > 1 && trimmed.charAt(trimmed.length() - 1) == '/') {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed + WELL_KNOWN_OPENID_CONFIGURATION_PATH;
    }

    private void appendParam(StringSink sink, String name, String value) {
        sink.putAscii('&').putAscii(name).putAscii('=').putAscii(urlEncode(value));
    }

    private HttpClient httpClient(boolean isTls) {
        if (isTls) {
            if (tlsClient == null) {
                tlsClient = HttpClientFactory.newTlsInstance(HTTP_CONFIG, tlsConfig);
            }
            return tlsClient;
        }
        if (plainClient == null) {
            plainClient = HttpClientFactory.newPlainTextInstance(HTTP_CONFIG);
        }
        return plainClient;
    }

    private boolean isHttpStatusSuccess() {
        // responseStatus is the numeric HTTP status captured by readResponse; a 2xx starts with '2'
        return responseStatus.length() > 0 && responseStatus.charAt(0) == '2';
    }

    private void pollForToken(String deviceCode, int expiresInSeconds, int intervalSeconds) {
        final long deadlineNanos = System.nanoTime() + expiresInSeconds * 1_000_000_000L;
        long intervalMillis = (long) intervalSeconds * 1000L;
        int consecutiveTransportErrors = 0;
        while (true) {
            throwIfClosed();
            // check the deadline before polling so an expiry that elapsed during the previous sleep aborts
            // here, not after one more wasted poll round-trip
            if (System.nanoTime() >= deadlineNanos) {
                throw new OidcAuthException("timed out waiting for authorization, the device code expired; please retry");
            }
            try {
                int result = pollOnce(deviceCode);
                if (result == POLL_SUCCESS) {
                    return;
                }
                if (result == POLL_TRANSIENT_ERROR) {
                    // a non-2xx with no parseable answer; charge the transport-error budget so a
                    // persistently failing token endpoint aborts instead of polling until the code expires
                    if (++consecutiveTransportErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
                        throw new OidcAuthException().put("the token endpoint returned repeated unexpected responses [httpStatus=").put(responseStatus).put(']');
                    }
                } else {
                    consecutiveTransportErrors = 0;
                    if (result == POLL_SLOW_DOWN) {
                        // grow the interval per RFC 8628, capped at the same bound as the initial value so
                        // repeated slow_down responses cannot inflate the wait without bound
                        intervalMillis = Math.min(intervalMillis + SLOW_DOWN_INCREMENT_SECONDS * 1000L, MAX_POLL_INTERVAL_SECONDS * 1000L);
                    }
                }
            } catch (HttpClientException e) {
                // a brief network blip is fine to retry, but a persistent failure (rejected TLS cert,
                // refused connection, unresolvable host) must surface with its cause rather than
                // masquerade as a device-code timeout
                if (++consecutiveTransportErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
                    throw new OidcAuthException(e).put("the token endpoint became unreachable while waiting for authorization");
                }
            } catch (OidcAuthException e) {
                // a garbled / non-JSON body (a JsonException cause) is a transport-class blip, retried on
                // the same budget; a well-formed OAuth error or unexpected response (no parse cause) is a
                // real answer from the identity provider and aborts immediately
                if (!(e.getCause() instanceof JsonException)) {
                    throw e;
                }
                if (++consecutiveTransportErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
                    throw e;
                }
            }
            // wait for the next poll, never past the device-code deadline, so the timeout check at the top
            // of the loop fires promptly at expiry instead of up to one poll interval late
            sleepBetweenPolls(Math.min(intervalMillis, (deadlineNanos - System.nanoTime()) / 1_000_000L));
        }
    }

    private int pollOnce(String deviceCode) {
        formSink.clear();
        formSink.putAscii("grant_type=").putAscii(urlEncode(GRANT_TYPE_DEVICE_CODE));
        appendParam(formSink, "device_code", deviceCode);
        appendParam(formSink, "client_id", clientId);

        tokenParser.clear();
        // a transport failure here propagates to pollForToken, which retries a brief blip but aborts on a
        // persistent failure rather than swallowing it as a pending authorization
        postForm(tokenEndpoint, tokenParser);

        // RFC 6749 5.2: an error response is an error even if the body also carries a token, so handle the
        // OAuth error first - a token smuggled alongside an error must never count as a grant
        if (tokenParser.error.length() > 0) {
            if (Chars.equals(ERROR_AUTHORIZATION_PENDING, tokenParser.error)) {
                return POLL_PENDING;
            }
            if (Chars.equals(ERROR_SLOW_DOWN, tokenParser.error)) {
                return POLL_SLOW_DOWN;
            }
            throw OidcAuthException.oauthError(tokenParser.error, tokenParser.errorDescription);
        }
        // RFC 6749 5.1: a grant is a 2xx response carrying a token; a token under a non-2xx status is
        // malformed or hostile - charge the transport-error budget rather than trust it
        if (tokenParser.accessToken.length() > 0 || tokenParser.idToken.length() > 0) {
            if (isHttpStatusSuccess()) {
                storeTokens(tokenParser);
                return POLL_SUCCESS;
            }
            return POLL_TRANSIENT_ERROR;
        }
        // no tokens and no OAuth error: a 2xx is a definitive but malformed answer and aborts; a non-2xx
        // (gateway 5xx, empty body) is a transport-class blip - retry rather than abort the sign-in
        if (isHttpStatusSuccess()) {
            throw new OidcAuthException().put("unexpected response from the token endpoint [httpStatus=").put(responseStatus).put(']');
        }
        return POLL_TRANSIENT_ERROR;
    }

    private void postForm(Endpoint endpoint, JsonParser parser) {
        HttpClient client = httpClient(endpoint.isTls);
        HttpClient.Request request = client.newRequest(endpoint.host, endpoint.port)
                .POST()
                .url(endpoint.path)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT);
        request.withContent();
        request.putAscii(formSink);
        HttpClient.ResponseHeaders response = request.send(httpTimeoutMillis);
        response.await(httpTimeoutMillis);
        readResponse(response, parser);
    }

    private void readResponse(HttpClient.ResponseHeaders response, JsonParser parser) {
        // capture only the HTTP status for diagnostics; the body is never retained or surfaced in a
        // message - it carries access, id and refresh tokens that must not reach logs or exceptions
        responseStatus.clear();
        DirectUtf8Sequence statusCode = response.getStatusCode();
        Response body = response.getResponse();
        if (statusCode != null) {
            // a well-formed HTTP status code is bare digits, but the header parser copies the status-line
            // token verbatim apart from SP/CR/LF, so a non-digit byte means a malformed or hostile status
            // line. Reject it rather than echo any byte (which could smuggle ESC or other control sequences
            // into a log or terminal when responseStatus is surfaced in a message below) or trust its
            // leading digit as a success gate. Drain the body first to keep the keep-alive connection usable.
            CharSequence raw = statusCode.asAsciiCharSequence();
            for (int i = 0, n = raw.length(); i < n; i++) {
                char c = raw.charAt(i);
                if (c < '0' || c > '9') {
                    discardBody(body, httpTimeoutMillis);
                    throw new OidcAuthException("the identity provider returned a malformed HTTP status code");
                }
                responseStatus.put(c);
            }
        }
        jsonLexer.clear();
        try {
            parseBody(body, jsonLexer, parser, httpTimeoutMillis);
        } catch (JsonException e) {
            // drain the rest to keep the keep-alive connection usable; never embed the body, it may carry
            // tokens
            discardBody(body, httpTimeoutMillis);
            throw new OidcAuthException(e)
                    .put("could not parse the identity provider response [httpStatus=").put(responseStatus).put(']');
        }
    }

    private void runDeviceFlow() {
        formSink.clear();
        formSink.putAscii("client_id=").putAscii(urlEncode(clientId));
        appendParam(formSink, "scope", scope);
        if (audience != null) {
            appendParam(formSink, "audience", audience);
        }

        deviceAuthParser.clear();
        try {
            postForm(deviceAuthorizationEndpoint, deviceAuthParser);
        } catch (HttpClientException e) {
            throw new OidcAuthException(e).put("could not reach the device authorization endpoint");
        }

        if (deviceAuthParser.error.length() > 0) {
            throw OidcAuthException.oauthError(deviceAuthParser.error, deviceAuthParser.errorDescription);
        }
        // RFC 8628 3.2: a device authorization grant is a 2xx response. A non-2xx body with no OAuth error
        // (handled above) is malformed or hostile; reject it rather than prompt the user and poll on it -
        // the same 2xx gate pollOnce and tryRefresh apply before trusting a token
        if (!isHttpStatusSuccess()) {
            throw new OidcAuthException().put("unexpected response from the device authorization endpoint [httpStatus=").put(responseStatus).put(']');
        }
        if (deviceAuthParser.deviceCode.length() == 0 || deviceAuthParser.userCode.length() == 0
                || deviceAuthParser.verificationUri.length() == 0) {
            throw new OidcAuthException().put("incomplete device authorization response from the identity provider [httpStatus=").put(responseStatus).put(']');
        }

        final String deviceCode = deviceAuthParser.deviceCode.toString();
        final int expiresInSeconds = boundedSeconds(deviceAuthParser.expiresIn, DEFAULT_DEVICE_CODE_TTL_SECONDS, MAX_DEVICE_CODE_TTL_SECONDS);
        final int intervalSeconds = boundedSeconds(deviceAuthParser.interval, DEFAULT_POLL_INTERVAL_SECONDS, MAX_POLL_INTERVAL_SECONDS);
        final DeviceAuthorizationChallenge challenge = new DeviceAuthorizationChallenge(
                sanitizeForDisplay(deviceAuthParser.userCode.toString()),
                sanitizeForDisplay(deviceAuthParser.verificationUri.toString()),
                deviceAuthParser.verificationUriComplete.length() > 0 ? sanitizeForDisplay(deviceAuthParser.verificationUriComplete.toString()) : null,
                expiresInSeconds,
                intervalSeconds
        );

        throwIfClosed();
        prompt.promptUser(challenge);
        pollForToken(deviceCode, expiresInSeconds, intervalSeconds);
    }

    private String selectToken() {
        if (groupsInToken) {
            if (idToken != null) {
                return idToken;
            }
            throw new OidcAuthException()
                    .put("the server expects groups encoded in the token (acl.oidc.groups.encoded.in.token=true) but the ")
                    .put("identity provider returned no id_token; ensure the requested scope includes 'openid'");
        }
        if (accessToken != null) {
            return accessToken;
        }
        throw new OidcAuthException("the identity provider returned no access_token");
    }

    private void sleepBetweenPolls(long millis) {
        // sleep in short slices so close() can abort an in-flight sign-in within ~POLL_SLEEP_SLICE_MILLIS
        // instead of after a full (possibly slow_down-inflated) interval; Os.sleep ignores thread
        // interrupts, so polling the closed flag is the only way to stay responsive to cancellation
        long remaining = millis;
        while (remaining > 0) {
            throwIfClosed();
            long slice = Math.min(POLL_SLEEP_SLICE_MILLIS, remaining);
            Os.sleep(slice);
            remaining -= slice;
        }
    }

    private void storeTokens(TokenResponseParser parser) {
        // reject a token with control or non-ASCII chars before caching: getToken() serves it verbatim as
        // an HTTP Authorization header value and a PG-wire password, where a decoded CR/LF would inject
        // into the request line sent to the trusted QuestDB server
        validateTokenChars(parser.accessToken, "access_token");
        validateTokenChars(parser.idToken, "id_token");
        accessToken = parser.accessToken.length() > 0 ? parser.accessToken.toString() : null;
        idToken = parser.idToken.length() > 0 ? parser.idToken.toString() : null;
        // a refresh response usually omits a new refresh token; keep the current one in that case
        if (parser.refreshToken.length() > 0) {
            refreshToken = parser.refreshToken.toString();
        }
        // clamp like the device-side expires_in: default for a non-positive value, cap an absurd one, so a
        // hostile or buggy token TTL cannot cache the token for decades (the server still enforces the real
        // expiry; this only bounds how long the client trusts its cached copy)
        int ttlSeconds = boundedSeconds(parser.expiresIn, DEFAULT_TOKEN_TTL_SECONDS, MAX_EXPIRES_IN_SECONDS);
        expiresAtMillis = System.currentTimeMillis() + ttlSeconds * 1000L;
    }

    private void throwIfClosed() {
        if (closed) {
            throw new OidcAuthException("the OidcDeviceAuth instance is closed");
        }
    }

    private boolean tryRefresh() {
        formSink.clear();
        formSink.putAscii("grant_type=").putAscii(urlEncode(GRANT_TYPE_REFRESH_TOKEN));
        appendParam(formSink, "refresh_token", refreshToken);
        appendParam(formSink, "client_id", clientId);
        if (scope != null) {
            appendParam(formSink, "scope", scope);
        }
        if (audience != null) {
            appendParam(formSink, "audience", audience);
        }

        tokenParser.clear();
        try {
            postForm(tokenEndpoint, tokenParser);
        } catch (HttpClientException e) {
            // could not reach the token endpoint; fall back to the interactive flow
            return false;
        } catch (OidcAuthException e) {
            // postForm throws OidcAuthException only on a parse failure (a garbled / unparseable refresh
            // response), never an OAuth error: a genuine OAuth error arrives in tokenParser.error, handled
            // by hasRequiredToken below. So treat this as a transient blip and fall back to the interactive
            // flow rather than fail the whole getToken() call
            return false;
        }
        // succeed only on a clean 2xx (no OAuth error) returning the token getToken() actually serves (the
        // id token when groups are encoded in it, the access token otherwise). A refresh that omits the id
        // token - which RFC 6749 permits and many providers do - or carries an error or a non-2xx status
        // must fall back to the interactive flow rather than be cached (and later fail in selectToken())
        boolean hasRequiredToken = (groupsInToken
                ? tokenParser.idToken.length() > 0
                : tokenParser.accessToken.length() > 0)
                && isHttpStatusSuccess()
                && tokenParser.error.length() == 0;
        if (hasRequiredToken) {
            storeTokens(tokenParser);
            return true;
        }
        // the refresh token expired or was revoked, or did not return the token we need; fall back to the
        // interactive flow
        return false;
    }

    /**
     * Fluent builder for an {@link OidcDeviceAuth} configured against a known identity provider.
     * The client id, device authorization endpoint and token endpoint are required.
     */
    public static final class Builder {
        private boolean allowInsecureTransport;
        private String audience;
        private String clientId;
        private int clockSkewSeconds = DEFAULT_CLOCK_SKEW_SECONDS;
        private String deviceAuthorizationEndpoint;
        private boolean groupsInToken;
        private int httpTimeoutMillis = DEFAULT_HTTP_TIMEOUT_MILLIS;
        private String issuer;
        private DeviceCodePrompt prompt = DeviceCodePrompt.openBrowser();
        private String scope = DEFAULT_SCOPE;
        private ClientTlsConfiguration tlsConfig;
        private String tokenEndpoint;

        private Builder() {
        }

        /**
         * Opts into insecure {@code http} for the QuestDB {@code /settings} link (only meaningful via
         * {@link #fromQuestDB}). It does <b>not</b> relax the identity provider endpoints configured here:
         * the device authorization and token endpoints always require {@code https} unless they are
         * loopback, so the device code and refresh token never cross the network in cleartext (matching
         * the Python client). Defaults to {@code false}.
         */
        public Builder allowInsecureTransport(boolean allowInsecureTransport) {
            this.allowInsecureTransport = allowInsecureTransport;
            return this;
        }

        /**
         * Sets the {@code audience} (or {@code resource}) request parameter, sent on the device
         * authorization and refresh requests. Some identity providers require it so the issued token
         * carries the {@code aud} claim QuestDB expects. {@link #fromQuestDB} discovers it from
         * {@code acl.oidc.audience}. Optional.
         */
        public Builder audience(String audience) {
            this.audience = audience;
            return this;
        }

        public OidcDeviceAuth build() {
            if (clientId == null || clientId.isEmpty()) {
                throw new OidcAuthException("clientId is required");
            }
            if (deviceAuthorizationEndpoint == null || deviceAuthorizationEndpoint.isEmpty()) {
                throw new OidcAuthException("deviceAuthorizationEndpoint is required");
            }
            if (tokenEndpoint == null || tokenEndpoint.isEmpty()) {
                throw new OidcAuthException("tokenEndpoint is required");
            }
            if (scope == null || scope.isEmpty()) {
                scope = DEFAULT_SCOPE;
            }
            Endpoint deviceEndpoint = Endpoint.parse(deviceAuthorizationEndpoint);
            Endpoint parsedTokenEndpoint = Endpoint.parse(tokenEndpoint);
            Endpoint issuerEndpoint = issuer != null && !issuer.isEmpty() ? Endpoint.parse(issuer) : null;
            requireSecureIdpEndpoint(deviceEndpoint, "device authorization endpoint", deviceAuthorizationEndpoint, allowInsecureTransport);
            requireSecureIdpEndpoint(parsedTokenEndpoint, "token endpoint", tokenEndpoint, allowInsecureTransport);
            // enforce the credential-endpoint co-location / issuer pin on every construction path, not just
            // discovery, so the documented guarantee holds for the explicit builder too
            validateEndpointOrigins(parsedTokenEndpoint, deviceEndpoint, issuerEndpoint);
            ClientTlsConfiguration tls = tlsConfig != null ? tlsConfig : defaultTlsConfig();
            return new OidcDeviceAuth(this, tls);
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * Seconds before the real expiry at which a cached token is treated as expired, absorbing clock
         * drift and request latency. Defaults to 30.
         */
        public Builder clockSkewSeconds(int clockSkewSeconds) {
            this.clockSkewSeconds = clockSkewSeconds;
            return this;
        }

        public Builder deviceAuthorizationEndpoint(String deviceAuthorizationEndpoint) {
            this.deviceAuthorizationEndpoint = deviceAuthorizationEndpoint;
            return this;
        }

        /**
         * Selects which token {@link #getToken()} returns. Set to {@code true} when the server has
         * {@code acl.oidc.groups.encoded.in.token=true} (the id token is returned), {@code false}
         * otherwise (the access token is returned). Defaults to {@code false}.
         */
        public Builder groupsInToken(boolean groupsInToken) {
            this.groupsInToken = groupsInToken;
            return this;
        }

        public Builder httpTimeoutMillis(int httpTimeoutMillis) {
            this.httpTimeoutMillis = httpTimeoutMillis;
            return this;
        }

        /**
         * Pins the identity provider by its {@code issuer} origin (for example
         * {@code https://idp.example.com}). When set, {@link #build()} rejects a token or device
         * authorization endpoint not on this origin, so a compromised or tampered configuration cannot
         * redirect the device code and refresh token to an attacker. {@link #fromQuestDB(String, DiscoveryOptions)}
         * sets it for you when discovering from a server, and additionally requires each endpoint advertised
         * by {@code /settings} to be under the issuer's path (not just its origin), so a tampered
         * {@code /settings} cannot redirect credentials to a different tenant on a path-based provider (for
         * example a Keycloak realm path like {@code /realms/acme}). A provider hosting its endpoints on a
         * different origin than its issuer is rejected when pinned; configure it without an issuer. Optional.
         */
        public Builder issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        /**
         * Sets how the device code challenge is shown to the user. Defaults to
         * {@link DeviceCodePrompt#openBrowser()} - prints to {@code System.out} and also opens the
         * verification URL in a browser when one is available; pass {@link DeviceCodePrompt#SYSTEM_OUT}
         * to print only.
         */
        public Builder prompt(DeviceCodePrompt prompt) {
            this.prompt = prompt != null ? prompt : DeviceCodePrompt.openBrowser();
            return this;
        }

        public Builder scope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder tlsConfig(ClientTlsConfiguration tlsConfig) {
            this.tlsConfig = tlsConfig;
            return this;
        }

        public Builder tokenEndpoint(String tokenEndpoint) {
            this.tokenEndpoint = tokenEndpoint;
            return this;
        }
    }

    /**
     * Options for {@link #fromQuestDB(String, DiscoveryOptions)}: how to pin the identity provider
     * (issuer or discovery URL), the TLS configuration for discovery and sign-in, whether to permit
     * insecure {@code http}, and how to show the device code challenge. Every option is optional; an
     * instance with nothing set behaves like {@link #fromQuestDB(String)}.
     */
    public static final class DiscoveryOptions {
        private boolean allowInsecureTransport;
        private String discoveryUrl;
        private String issuer;
        private DeviceCodePrompt prompt = DeviceCodePrompt.openBrowser();
        private ClientTlsConfiguration tlsConfig;

        /**
         * Permits insecure {@code http} for the QuestDB server link only (the {@code /settings} discovery
         * request). It does <b>not</b> relax the identity provider endpoints, which always require
         * {@code https} unless they are loopback, so the device code and refresh token are never sent in
         * cleartext. Enable only for local development on a trusted network. Defaults to {@code false}.
         */
        public DiscoveryOptions allowInsecureTransport(boolean allowInsecureTransport) {
            this.allowInsecureTransport = allowInsecureTransport;
            return this;
        }

        /**
         * Pins the identity provider by its discovery document URL directly, an alternative to
         * {@link #issuer(String)} (which otherwise derives {@code {issuer}/.well-known/openid-configuration}).
         * Either pins where discovery - and the credential requests it resolves - are aimed, so a tampered
         * {@code /settings} cannot redirect them. Optional.
         */
        public DiscoveryOptions discoveryUrl(String discoveryUrl) {
            this.discoveryUrl = discoveryUrl;
            return this;
        }

        /**
         * Pins the identity provider by its {@code issuer} origin (for example
         * {@code https://idp.example.com}). It plays two roles: when the server does not advertise the
         * device authorization endpoint, it is discovered from the issuer's
         * {@code .well-known/openid-configuration} (the discovery origin comes only from this out-of-band
         * issuer, never from {@code /settings}); and it pins the token and device authorization endpoints,
         * so any endpoint not on the issuer origin is rejected. When the issuer has a path, an endpoint
         * advertised by {@code /settings} must also be under that path, so a tampered {@code /settings}
         * cannot redirect credentials to a different tenant on a path-based provider (for example a Keycloak
         * realm path like {@code /realms/acme}). A provider hosting its endpoints on a different origin than
         * its issuer must be configured without an issuer. Optional.
         */
        public DiscoveryOptions issuer(String issuer) {
            this.issuer = issuer;
            return this;
        }

        /**
         * Sets how the device code challenge is shown to the user. Defaults to
         * {@link DeviceCodePrompt#openBrowser()} - prints to {@code System.out} and also opens the
         * verification URL in a browser when one is available; pass {@link DeviceCodePrompt#SYSTEM_OUT}
         * to print only.
         */
        public DiscoveryOptions prompt(DeviceCodePrompt prompt) {
            this.prompt = prompt != null ? prompt : DeviceCodePrompt.openBrowser();
            return this;
        }

        /**
         * Sets the TLS configuration used for the {@code /settings} discovery request, any identity
         * provider discovery document, and the later sign-in requests. Defaults to full validation.
         */
        public DiscoveryOptions tlsConfig(ClientTlsConfiguration tlsConfig) {
            this.tlsConfig = tlsConfig;
            return this;
        }
    }

    private static final class DeviceAuthorizationResponseParser implements JsonParser, Mutable {
        private static final int FIELD_DEVICE_CODE = 1;
        private static final int FIELD_ERROR = 7;
        private static final int FIELD_ERROR_DESCRIPTION = 8;
        private static final int FIELD_EXPIRES_IN = 5;
        private static final int FIELD_INTERVAL = 6;
        private static final int FIELD_NONE = 0;
        private static final int FIELD_USER_CODE = 2;
        private static final int FIELD_VERIFICATION_URI = 3;
        private static final int FIELD_VERIFICATION_URI_COMPLETE = 4;
        final StringSink deviceCode = new StringSink();
        final StringSink error = new StringSink();
        final StringSink errorDescription = new StringSink();
        final StringSink userCode = new StringSink();
        final StringSink verificationUri = new StringSink();
        final StringSink verificationUriComplete = new StringSink();
        int expiresIn;
        int interval;
        private int depth;
        private int field = FIELD_NONE;

        @Override
        public void clear() {
            deviceCode.clear();
            error.clear();
            errorDescription.clear();
            userCode.clear();
            verificationUri.clear();
            verificationUriComplete.clear();
            expiresIn = 0;
            interval = 0;
            depth = 0;
            field = FIELD_NONE;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    if (depth == 1) {
                        if (Chars.equals("device_code", tag)) {
                            field = FIELD_DEVICE_CODE;
                        } else if (Chars.equals("user_code", tag)) {
                            field = FIELD_USER_CODE;
                        } else if (Chars.equals("verification_uri", tag) || Chars.equals("verification_url", tag)) {
                            field = FIELD_VERIFICATION_URI;
                        } else if (Chars.equals("verification_uri_complete", tag) || Chars.equals("verification_url_complete", tag)) {
                            field = FIELD_VERIFICATION_URI_COMPLETE;
                        } else if (Chars.equals("expires_in", tag)) {
                            field = FIELD_EXPIRES_IN;
                        } else if (Chars.equals("interval", tag)) {
                            field = FIELD_INTERVAL;
                        } else if (Chars.equals("error", tag)) {
                            field = FIELD_ERROR;
                        } else if (Chars.equals("error_description", tag)) {
                            field = FIELD_ERROR_DESCRIPTION;
                        } else {
                            field = FIELD_NONE;
                        }
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    if (depth == 1) {
                        switch (field) {
                            case FIELD_DEVICE_CODE:
                                putNonNull(deviceCode, tag);
                                break;
                            case FIELD_USER_CODE:
                                putNonNull(userCode, tag);
                                break;
                            case FIELD_VERIFICATION_URI:
                                putNonNull(verificationUri, tag);
                                break;
                            case FIELD_VERIFICATION_URI_COMPLETE:
                                putNonNull(verificationUriComplete, tag);
                                break;
                            case FIELD_EXPIRES_IN:
                                expiresIn = parseIntOrZero(tag);
                                break;
                            case FIELD_INTERVAL:
                                interval = parseIntOrZero(tag);
                                break;
                            case FIELD_ERROR:
                                putNonNull(error, tag);
                                break;
                            case FIELD_ERROR_DESCRIPTION:
                                putNonNull(errorDescription, tag);
                                break;
                            default:
                                break;
                        }
                    }
                    field = FIELD_NONE;
                    break;
                default:
                    break;
            }
        }
    }

    private static final class Endpoint {
        final String host;
        final boolean isTls;
        final String path;
        final int port;

        private Endpoint(String host, int port, String path, boolean isTls) {
            this.host = host;
            this.port = port;
            this.path = path;
            this.isTls = isTls;
        }

        static Endpoint parse(String url) {
            if (url == null) {
                throw new OidcAuthException("url is required");
            }
            // Reject control characters, whitespace and display-unsafe code points anywhere in the url
            // before it is split or used. A smuggled CR/LF (or other control char) in the host corrupts the
            // outbound Host header; in the path or query it injects into the HTTP request line (postForm
            // sends the path verbatim via .url(endpoint.path)) - a request-smuggling / header-injection
            // vector when the url comes from a tampered /settings or discovery document. A bidi, zero-width
            // or other format char (isUnsafeForDisplay, scanned per code point so a supplementary-plane one
            // is not missed) reorders, hides or forges text when the url is echoed into a log line or the
            // parse errors below. Rejecting up front keeps the raw url safe on the wire and on screen.
            for (int i = 0, n = url.length(); i < n; ) {
                final int cp = url.codePointAt(i);
                if (cp <= ' ' || OidcAuthException.isUnsafeForDisplay(cp)) {
                    throw new OidcAuthException().put("invalid url, it contains an illegal character [url=").put(sanitizeForDisplay(url)).put(']');
                }
                i += Character.charCount(cp);
            }
            int schemeEnd = url.indexOf("://");
            if (schemeEnd < 0) {
                throw new OidcAuthException().put("invalid url, expected a scheme [url=").put(url).put(']');
            }
            boolean isTls;
            String scheme = url.substring(0, schemeEnd);
            if ("https".equals(scheme)) {
                isTls = true;
            } else if ("http".equals(scheme)) {
                isTls = false;
            } else {
                throw new OidcAuthException().put("invalid url, expected http or https [url=").put(url).put(']');
            }
            int hostStart = schemeEnd + 3;
            int pathStart = url.indexOf('/', hostStart);
            String hostPort = pathStart < 0 ? url.substring(hostStart) : url.substring(hostStart, pathStart);
            String path = pathStart < 0 ? "/" : url.substring(pathStart);
            if (hostPort.startsWith("[")) {
                // bracketed IPv6 literal: the client's HTTP layer does not bracket the Host header, so
                // reject it clearly rather than mis-parse it on a ':' inside the address
                throw new OidcAuthException().put("invalid url, IPv6 literal hosts are not supported [url=").put(url).put(']');
            }
            int colon = hostPort.indexOf(':');
            String host;
            int port;
            if (colon >= 0) {
                host = hostPort.substring(0, colon);
                try {
                    port = Integer.parseInt(hostPort.substring(colon + 1));
                } catch (NumberFormatException e) {
                    throw new OidcAuthException().put("invalid url, could not parse the port [url=").put(url).put(']');
                }
                if (port < 1 || port > 65535) {
                    throw new OidcAuthException().put("invalid url, the port must be between 1 and 65535 [url=").put(url).put(']');
                }
            } else {
                host = hostPort;
                port = isTls ? 443 : 80;
            }
            if (host.isEmpty()) {
                throw new OidcAuthException().put("invalid url, the host is empty [url=").put(url).put(']');
            }
            return new Endpoint(host, port, path, isTls);
        }
    }

    private static final class SettingsDiscoveryParser implements JsonParser {
        private static final int FIELD_AUDIENCE = 7;
        private static final int FIELD_CLIENT_ID = 2;
        private static final int FIELD_DEVICE_AUTHORIZATION_ENDPOINT = 5;
        private static final int FIELD_ENABLED = 1;
        private static final int FIELD_GROUPS_IN_TOKEN = 6;
        private static final int FIELD_NONE = 0;
        private static final int FIELD_SCOPE = 3;
        private static final int FIELD_TOKEN_ENDPOINT = 4;
        final StringSink audience = new StringSink();
        final StringSink clientId = new StringSink();
        final StringSink deviceAuthorizationEndpoint = new StringSink();
        final StringSink scope = new StringSink();
        final StringSink tokenEndpoint = new StringSink();
        boolean groupsInToken;
        boolean isOidcEnabled;
        private int depth;
        private int field = FIELD_NONE;
        private boolean isConfigNext;
        private boolean isInConfig;

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    if (depth == 2 && isConfigNext) {
                        isInConfig = true;
                    }
                    isConfigNext = false;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    if (depth == 2) {
                        isInConfig = false;
                    }
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    if (depth == 1) {
                        // only the top-level "config" object is trusted; the sibling "preferences" object
                        // holds arbitrary user-written keys and must not feed OIDC discovery
                        isConfigNext = Chars.equals("config", tag);
                        field = FIELD_NONE;
                    } else if (depth == 2 && isInConfig) {
                        if (Chars.equals("acl.oidc.enabled", tag)) {
                            field = FIELD_ENABLED;
                        } else if (Chars.equals("acl.oidc.client.id", tag)) {
                            field = FIELD_CLIENT_ID;
                        } else if (Chars.equals("acl.oidc.scope", tag)) {
                            field = FIELD_SCOPE;
                        } else if (Chars.equals("acl.oidc.token.endpoint", tag)) {
                            field = FIELD_TOKEN_ENDPOINT;
                        } else if (Chars.equals("acl.oidc.device.authorization.endpoint", tag)) {
                            field = FIELD_DEVICE_AUTHORIZATION_ENDPOINT;
                        } else if (Chars.equals("acl.oidc.groups.encoded.in.token", tag)) {
                            field = FIELD_GROUPS_IN_TOKEN;
                        } else if (Chars.equals("acl.oidc.audience", tag)) {
                            field = FIELD_AUDIENCE;
                        } else {
                            field = FIELD_NONE;
                        }
                    } else {
                        field = FIELD_NONE;
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    if (depth == 2 && isInConfig) {
                        switch (field) {
                            case FIELD_ENABLED:
                                isOidcEnabled = Chars.equals("true", tag);
                                break;
                            case FIELD_CLIENT_ID:
                                putNonNull(clientId, tag);
                                break;
                            case FIELD_SCOPE:
                                putNonNull(scope, tag);
                                break;
                            case FIELD_TOKEN_ENDPOINT:
                                putNonNull(tokenEndpoint, tag);
                                break;
                            case FIELD_DEVICE_AUTHORIZATION_ENDPOINT:
                                putNonNull(deviceAuthorizationEndpoint, tag);
                                break;
                            case FIELD_GROUPS_IN_TOKEN:
                                groupsInToken = Chars.equals("true", tag);
                                break;
                            case FIELD_AUDIENCE:
                                putNonNull(audience, tag);
                                break;
                            default:
                                break;
                        }
                    }
                    field = FIELD_NONE;
                    break;
                default:
                    break;
            }
        }
    }

    private static final class TokenResponseParser implements JsonParser, Mutable {
        private static final int FIELD_ACCESS_TOKEN = 1;
        private static final int FIELD_ERROR = 6;
        private static final int FIELD_ERROR_DESCRIPTION = 7;
        private static final int FIELD_EXPIRES_IN = 4;
        private static final int FIELD_ID_TOKEN = 2;
        private static final int FIELD_NONE = 0;
        private static final int FIELD_REFRESH_TOKEN = 3;
        final StringSink accessToken = new StringSink();
        final StringSink error = new StringSink();
        final StringSink errorDescription = new StringSink();
        final StringSink idToken = new StringSink();
        final StringSink refreshToken = new StringSink();
        int expiresIn;
        private int depth;
        private int field = FIELD_NONE;

        @Override
        public void clear() {
            accessToken.clear();
            error.clear();
            errorDescription.clear();
            idToken.clear();
            refreshToken.clear();
            expiresIn = 0;
            depth = 0;
            field = FIELD_NONE;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    if (depth == 1) {
                        if (Chars.equals("access_token", tag)) {
                            field = FIELD_ACCESS_TOKEN;
                        } else if (Chars.equals("id_token", tag)) {
                            field = FIELD_ID_TOKEN;
                        } else if (Chars.equals("refresh_token", tag)) {
                            field = FIELD_REFRESH_TOKEN;
                        } else if (Chars.equals("expires_in", tag)) {
                            field = FIELD_EXPIRES_IN;
                        } else if (Chars.equals("error", tag)) {
                            field = FIELD_ERROR;
                        } else if (Chars.equals("error_description", tag)) {
                            field = FIELD_ERROR_DESCRIPTION;
                        } else {
                            field = FIELD_NONE;
                        }
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    if (depth == 1) {
                        switch (field) {
                            case FIELD_ACCESS_TOKEN:
                                putNonNull(accessToken, tag);
                                break;
                            case FIELD_ID_TOKEN:
                                putNonNull(idToken, tag);
                                break;
                            case FIELD_REFRESH_TOKEN:
                                putNonNull(refreshToken, tag);
                                break;
                            case FIELD_EXPIRES_IN:
                                expiresIn = parseIntOrZero(tag);
                                break;
                            case FIELD_ERROR:
                                putNonNull(error, tag);
                                break;
                            case FIELD_ERROR_DESCRIPTION:
                                putNonNull(errorDescription, tag);
                                break;
                            default:
                                break;
                        }
                    }
                    field = FIELD_NONE;
                    break;
                default:
                    break;
            }
        }
    }

    private static final class WellKnownDiscoveryParser implements JsonParser {
        private static final int FIELD_DEVICE_AUTHORIZATION_ENDPOINT = 1;
        private static final int FIELD_ISSUER = 3;
        private static final int FIELD_NONE = 0;
        private static final int FIELD_TOKEN_ENDPOINT = 2;
        final StringSink deviceAuthorizationEndpoint = new StringSink();
        final StringSink issuer = new StringSink();
        final StringSink tokenEndpoint = new StringSink();
        private int depth;
        private int field = FIELD_NONE;

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    // the OIDC discovery document is a flat top-level object; only read top-level keys so a
                    // nested value cannot be mistaken for an endpoint
                    if (depth == 1) {
                        if (Chars.equals("device_authorization_endpoint", tag)) {
                            field = FIELD_DEVICE_AUTHORIZATION_ENDPOINT;
                        } else if (Chars.equals("token_endpoint", tag)) {
                            field = FIELD_TOKEN_ENDPOINT;
                        } else if (Chars.equals("issuer", tag)) {
                            field = FIELD_ISSUER;
                        } else {
                            field = FIELD_NONE;
                        }
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    if (depth == 1) {
                        switch (field) {
                            case FIELD_DEVICE_AUTHORIZATION_ENDPOINT:
                                putNonNull(deviceAuthorizationEndpoint, tag);
                                break;
                            case FIELD_TOKEN_ENDPOINT:
                                putNonNull(tokenEndpoint, tag);
                                break;
                            case FIELD_ISSUER:
                                putNonNull(issuer, tag);
                                break;
                            default:
                                break;
                        }
                    }
                    field = FIELD_NONE;
                    break;
                default:
                    break;
            }
        }
    }
}
