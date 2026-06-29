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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Locale;
import java.util.Objects;
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
 *     String token = auth.signIn(); // signs in on first use, then caches and refreshes
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
 * {@link #signIn()} serves a cached token while valid, silently refreshes when a refresh token
 * exists, otherwise re-runs the interactive flow. An instance lock serializes calls, so two
 * sign-ins never start at once. A sign-in waiting for the user holds that lock for the device code
 * lifetime (up to 30 minutes), so a concurrent {@link #signIn()} or {@link #clearCache()} blocks
 * behind it - but {@link #getToken()} never waits: it fails fast with an
 * {@link OidcAuthException} so a request/flush path never stalls. To abort a waiting sign-in, call
 * {@link #close()} from another thread; it signals the flow to stop, which then fails with an
 * {@link OidcAuthException} rather than polling until the device code expires. Cancellation is seen
 * between polls (within ~100ms while waiting out an interval); a poll already in flight is not
 * interrupted, so the abort - and {@link #close()} - can take up to one HTTP request timeout (see
 * {@link Builder#httpTimeoutMillis(int)}), still far short of the device-code lifetime (a
 * {@link DeviceCodePrompt} that blocks in {@code promptUser}, such as the default browser launch, can
 * extend that wait by however long it runs).
 * <p>
 * Instances are interactive and hold a network connection; close them when done. Token state is
 * in-memory only by default; pass a {@link TokenStore} (via {@link Builder#tokenStore(TokenStore)} or
 * {@link DiscoveryOptions#tokenStore(TokenStore)}) to persist it across process restarts, so a restarted
 * process resumes from a saved refresh token instead of running the interactive flow again.
 */
public class OidcDeviceAuth implements QuietCloseable {
    public static final String DEFAULT_SCOPE = "openid";
    static final String GRANT_TYPE_DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code";
    static final String GRANT_TYPE_REFRESH_TOKEN = "refresh_token";
    // fixed clock-skew margin (matches the Python client questdb.auth): getToken() treats a cached token as
    // expired this many millis before its real exp, to absorb clock drift and request latency. Not
    // configurable; effectiveSkewMillis() caps it at half the token lifetime so a short-lived token is not
    // reported expired the instant it is issued.
    private static final long CLOCK_SKEW_MILLIS = 30_000L;
    // device code TTL when the device authorization response omits (or zeroes) expires_in; matches Python
    private static final int DEFAULT_DEVICE_CODE_TTL_SECONDS = 600;
    private static final int DEFAULT_HTTP_TIMEOUT_MILLIS = 30_000;
    private static final int DEFAULT_POLL_INTERVAL_SECONDS = 5;
    // token cache TTL when the token response omits expires_in
    private static final int DEFAULT_TOKEN_TTL_SECONDS = 300;
    private static final String ERROR_AUTHORIZATION_PENDING = "authorization_pending";
    private static final String ERROR_SLOW_DOWN = "slow_down";
    // the grant_type values are constants, so url-encode them once at class load rather than on every
    // device-code poll and token refresh
    private static final String GRANT_TYPE_DEVICE_CODE_ENCODED = urlEncode(GRANT_TYPE_DEVICE_CODE);
    private static final String GRANT_TYPE_REFRESH_TOKEN_ENCODED = urlEncode(GRANT_TYPE_REFRESH_TOKEN);
    private static final HttpClientConfiguration HTTP_CONFIG = DefaultHttpClientConfiguration.INSTANCE;
    // a rate-limited identity provider answers 429; the token poll treats it as a transient backoff
    private static final String HTTP_STATUS_TOO_MANY_REQUESTS = "429";
    // Token responses carry JWTs (an id token with group claims can be several KB), and a single
    // value may arrive split across HTTP fragments. The lexer stashes a split value and rejects it
    // past JSON_LEXER_MAX_VALUE_BYTES, so the limit must comfortably exceed any real token or large
    // tokens fail to parse with "String is too long".
    private static final int JSON_LEXER_CACHE_SIZE = 1024;
    private static final int JSON_LEXER_MAX_VALUE_BYTES = 1 << 20;
    // upper bound on the device code lifetime (the device authorization response's expires_in), so a
    // hostile or buggy provider cannot make the client poll for an absurd duration; matches the Python client
    private static final int MAX_DEVICE_CODE_TTL_SECONDS = 1800;
    // upper bound on the token cache lifetime (the token response's expires_in), so an absurd or hostile
    // value cannot overflow the timing arithmetic or make the client trust a token for absurdly long
    private static final int MAX_EXPIRES_IN_SECONDS = 3600;
    // upper bound on the configurable HTTP request timeout. A token-endpoint round-trip never needs longer,
    // and bounding it keeps a refresh held under the FileTokenStore cross-process lock (send + await + parse,
    // plus a body drain on a parse failure - each separately bounded by this, so up to ~4x this) safely
    // shorter than that store's lock-staleness window, so a slow refresh's live lock is not stolen by a peer
    private static final int MAX_HTTP_TIMEOUT_MILLIS = 120_000;
    // upper bound on the poll interval, both the initial value and the growth after a slow_down or 429, so
    // a hostile or buggy provider cannot stall the poll loop; matches the Python client
    private static final int MAX_POLL_INTERVAL_SECONDS = 60;
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
    private final String audienceEncoded;
    private final String clientIdEncoded;
    private final DeviceAuthorizationResponseParser deviceAuthParser = new DeviceAuthorizationResponseParser();
    private final Endpoint deviceAuthorizationEndpoint;
    private final StringSink formSink = new StringSink();
    private final boolean groupsInToken;
    private final int httpTimeoutMillis;
    // serializes signIn()/getToken()/clearCache()/close(); signIn() holds it for the whole
    // interactive flow, getToken() uses tryLock so the flush path never stalls behind a sign-in
    private final ReentrantLock lock = new ReentrantLock();
    private final DeviceCodePrompt prompt;
    private final StringSink responseStatus = new StringSink();
    private final String scopeEncoded;
    private final TokenStoreKey storeKey;
    private final ClientTlsConfiguration tlsConfig;
    private final Endpoint tokenEndpoint;
    private final TokenResponseParser tokenParser = new TokenResponseParser();
    private final TokenStore tokenStore;
    private String accessToken;
    private volatile boolean closed;
    private long expiresAtMillis;
    private String idToken;
    private JsonLexer jsonLexer;
    private String lastPersistedRefreshToken;
    private HttpClient plainClient;
    private String refreshToken;
    private boolean storeLoadAttempted;
    private HttpClient tlsClient;
    // lifetime in millis of the currently cached token (its clamped TTL); effectiveSkewMillis() caps the
    // clock skew at half of this so a short-lived token is not treated as expired the instant it is issued
    private long tokenTtlMillis;

    private OidcDeviceAuth(Builder builder, ClientTlsConfiguration tlsConfig) {
        String clientId = builder.clientId;
        // pre-encode the invariant form params once here, so the poll loop and silent refresh do not
        // re-run URLEncoder on every request (mirrors the pre-encoded GRANT_TYPE_* constants)
        this.clientIdEncoded = urlEncode(clientId);
        this.deviceAuthorizationEndpoint = Endpoint.parse(builder.deviceAuthorizationEndpoint);
        this.tokenEndpoint = Endpoint.parse(builder.tokenEndpoint);
        String scope = builder.scope;
        this.scopeEncoded = urlEncode(scope);
        String audience = builder.audience;
        this.audienceEncoded = audience != null ? urlEncode(audience) : null;
        this.groupsInToken = builder.groupsInToken;
        this.httpTimeoutMillis = builder.httpTimeoutMillis;
        this.prompt = builder.prompt;
        this.tlsConfig = tlsConfig;
        this.tokenStore = builder.tokenStore;
        // key any persisted token by the identity it belongs to, built before the native lexer alloc so a
        // throw here cannot leak it. Canonicalise the endpoints (lower-case scheme/host, explicit port) and
        // normalise an empty audience to null, so the hash matches across processes and language clients.
        this.storeKey = tokenStore == null ? null : new TokenStoreKey(
                clientId,
                canonicalEndpoint(this.tokenEndpoint),
                canonicalEndpoint(this.deviceAuthorizationEndpoint),
                scope,
                audience != null && !audience.isEmpty() ? audience : null,
                this.groupsInToken
        );
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
     * but with explicit {@link DiscoveryOptions}: an identity provider pin (issuer), a TLS configuration, an
     * insecure-transport opt-in, and the device code prompt - for example
     * {@link DeviceCodePrompt#openBrowser()} to also open the verification URL in a browser.
     *
     * @param questdbUrl the QuestDB HTTP base URL, for example {@code https://questdb.example.com:9000}
     * @param options    how to pin the identity provider, configure TLS, permit insecure transport, and
     *                   show the device code challenge; see {@link DiscoveryOptions}
     * @return a configured, ready-to-use instance
     * @throws OidcAuthException if the server has OIDC disabled, or does not advertise a device
     *                           authorization endpoint and no issuer was pinned
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl, DiscoveryOptions options) {
        String issuer = options.issuer;
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
        // capture each endpoint's provenance before discovery may fill a missing one: only an endpoint the
        // untrusted /settings response advertised is origin-pinned to the issuer below. An endpoint discovered
        // from the provider's own .well-known is authoritative for wherever the pinned issuer hosts it.
        final boolean tokenEndpointFromSettings = tokenEndpoint != null;
        final boolean deviceEndpointFromSettings = deviceAuthorizationEndpoint != null;

        // Over a plaintext, MITM-able http /settings channel (only reachable with allowInsecureTransport;
        // the default rejects it), advertised endpoints can be tampered in transit to route the device
        // code and long-lived refresh token to an attacker. The missing-endpoint discovery path below
        // already demands an out-of-band pin, but a tampered /settings advertising BOTH endpoints at one
        // attacker origin skips that path - the co-location check passes trivially and there is no issuer
        // to pin against - so require the same pin before trusting /settings endpoints over such a channel.
        boolean settingsSuppliedCredentials = tokenEndpoint != null || deviceAuthorizationEndpoint != null;
        if (settingsSuppliedCredentials && resolvedIssuer == null && settingsChannelIsPlaintext(server)) {
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
            if (resolvedIssuer == null) {
                throw new OidcAuthException()
                        .put("the QuestDB server did not advertise the OIDC device authorization endpoint (and/or the token ")
                        .put("endpoint), so it must be discovered from the identity provider, but the identity provider is not ")
                        .put("pinned; pass an issuer (its origin, for example https://your-idp) to OidcDeviceAuth.fromQuestDB so ")
                        .put("a tampered or intercepted /settings response cannot redirect the device-code and refresh-token ")
                        .put("requests to an attacker, or configure the endpoints explicitly with OidcDeviceAuth.builder() [url=")
                        .put(questdbUrl).put(']');
            }
            WellKnownDiscoveryParser doc = new WellKnownDiscoveryParser();
            discoverFromIdp(resolvedIssuer, tlsConfig, allowInsecureTransport, doc);
            if (deviceAuthorizationEndpoint == null && doc.deviceAuthorizationEndpoint.length() > 0) {
                deviceAuthorizationEndpoint = doc.deviceAuthorizationEndpoint.toString();
            }
            if (tokenEndpoint == null && doc.tokenEndpoint.length() > 0) {
                tokenEndpoint = doc.tokenEndpoint.toString();
            }
        }

        // Pin the ORIGIN of any endpoint the untrusted /settings response advertised to the pinned issuer
        // origin, so a tampered /settings cannot redirect the device code and refresh token to
        // an attacker. An endpoint discovered from the identity provider's own .well-known is deliberately NOT
        // origin-pinned: that document is fetched from the pinned origin and is authoritative for wherever the
        // issuer hosts its endpoints - some providers (for example Google) serve the token and device endpoints
        // from a different origin than the issuer. The co-location check (token and device share one origin)
        // still applies to every endpoint, enforced by validateEndpointOrigins in build().
        if (resolvedIssuer != null) {
            Endpoint pin = Endpoint.parse(resolvedIssuer);
            if (tokenEndpointFromSettings && !sameOrigin(Endpoint.parse(tokenEndpoint), pin)) {
                throw endpointOriginNotPinned("token endpoint", tokenEndpoint, originOf(pin));
            }
            if (deviceEndpointFromSettings && !sameOrigin(Endpoint.parse(deviceAuthorizationEndpoint), pin)) {
                throw endpointOriginNotPinned("device authorization endpoint", deviceAuthorizationEndpoint, originOf(pin));
            }
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
                .allowInsecureTransport(allowInsecureTransport)
                .tlsConfig(tlsConfig)
                .prompt(options.prompt)
                .tokenStore(options.tokenStore)
                .build();
    }

    /**
     * Drops any cached token so the next {@link #signIn()} starts a fresh interactive sign-in.
     */
    public void clearCache() {
        lock.lock();
        try {
            throwIfClosed();
            accessToken = null;
            idToken = null;
            refreshToken = null;
            expiresAtMillis = 0;
            tokenTtlMillis = 0;
            lastPersistedRefreshToken = null;
            if (tokenStore != null) {
                try {
                    tokenStore.clear(storeKey);
                } catch (RuntimeException e) {
                    warnPersistence("clear", e);
                }
            }
            // do not reload the entry we just removed on the next signIn()/getToken()
            storeLoadAttempted = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Frees the network connections and native buffers this instance holds. If a {@link #signIn()}
     * sign-in is in flight on another thread, signals it to stop so it fails with an
     * {@link OidcAuthException} instead of polling until the device code expires. The signal is observed
     * between polls (within ~100ms while waiting out a poll interval); a poll request already in flight
     * is not interrupted, so {@code close()} acquires the lock - and returns - only once that request
     * finishes or times out, i.e. after at most one HTTP request timeout
     * (see {@link Builder#httpTimeoutMillis(int)}), not the full device-code lifetime. The exception is a
     * {@link DeviceCodePrompt} that blocks in {@code promptUser} - for example the default
     * {@link DeviceCodePrompt#openBrowser()} prompt while it hands the verification URL to the OS browser,
     * which is not bounded by the HTTP timeout: the flow holds the lock across that one-off prompt, so a
     * racing {@code close()} waits it out too. Idempotent. After close, {@link #signIn()},
     * {@link #getToken()} and {@link #clearCache()} throw.
     */
    @Override
    public void close() {
        // flag cancellation before taking the lock: signIn() holds it for the whole flow, so signal the
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
     * @return {@code "Bearer " + signIn()}, ready to use as the value of an HTTP
     * {@code Authorization} header.
     */
    public String getAuthorizationHeaderValue() {
        return "Bearer " + signIn();
    }

    /**
     * Like {@link #signIn()} but never starts the interactive device flow, never prompts, and never waits
     * on interactive input: returns the cached token while valid, silently refreshes when a refresh token is
     * available, otherwise throws. Designed for the request/flush path of a long-lived client, for example
     * {@code Sender.builder(...).httpTokenProvider(auth::getToken)}, where an interactive prompt is
     * inappropriate. Call {@link #signIn()} once to sign in first.
     * <p>
     * It does not wait behind an interactive {@link #signIn()} running on another thread (which would stall
     * the flush for the whole device-code lifetime): if such a sign-in holds the lock it fails fast, and the
     * caller should retry once the sign-in completes. It is not, however, instantaneous - when the cached
     * token has expired it makes one synchronous refresh round-trip to the token endpoint, bounded by
     * {@link Builder#httpTimeoutMillis(int)} (30s by default); when a {@link TokenStore} coordinates the
     * refresh across processes it may first wait briefly to acquire the store's per-identity lock (a few
     * seconds at most for {@link FileTokenStore}, then it proceeds without the lock) before that round-trip.
     * That is the "quick silent refresh" the {@code HttpTokenProvider} contract permits on the flush path,
     * not an unbounded interactive wait.
     *
     * @return a non-null, non-empty token
     * @throws OidcAuthException if no token has been obtained yet, if the cached token expired and could
     *                           not be refreshed without an interactive sign-in, or if a sign-in or
     *                           refresh is already in progress on another thread
     */
    public String getToken() {
        throwIfClosed();
        // never wait on the flush path: signIn()'s sign-in holds the lock for the whole device-code
        // lifetime (up to 30 minutes), so tryLock and fail fast if held. A sign-in in progress means there
        // is no token to serve yet, so the caller gets a prompt exception to retry rather than a stalled
        // flush
        if (!lock.tryLock()) {
            throw new OidcAuthException("a sign-in or token refresh is already in progress on another thread; no token is available without blocking - retry shortly");
        }
        try {
            throwIfClosed();
            maybeLoadFromStore();
            final String cachedToken = groupsInToken ? idToken : accessToken;
            if (cachedToken != null) {
                if (System.currentTimeMillis() < expiresAtMillis - effectiveSkewMillis()) {
                    return cachedToken;
                }
                if (refreshToken != null && tryRefreshCoordinated()) {
                    return selectToken();
                }
                throw new OidcAuthException("the cached token expired and could not be refreshed without an interactive sign-in; call signIn() to sign in again");
            }
            throw new OidcAuthException("no token has been obtained yet; call signIn() to sign in before using getToken()");
        } finally {
            lock.unlock();
        }
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
    public String signIn() {
        lock.lock();
        try {
            throwIfClosed();
            maybeLoadFromStore();
            // only the kind of token signIn() actually serves counts as a cache hit; a grant that
            // returned the other kind (access token when the server wants the id token, or vice versa)
            // leaves the served token null, so re-run the flow rather than report the unusable grant as
            // valid and have selectToken() throw on this and every later call
            final String cachedToken = groupsInToken ? idToken : accessToken;
            if (cachedToken != null) {
                if (System.currentTimeMillis() < expiresAtMillis - effectiveSkewMillis()) {
                    return cachedToken;
                }
                if (refreshToken != null && tryRefreshCoordinated()) {
                    return selectToken();
                }
            }
            runDeviceFlow();
            return selectToken();
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

    private static String canonicalEndpoint(Endpoint endpoint) {
        // scheme and host lower-cased, port explicit, path verbatim: a stable rendering that hashes to the
        // same TokenStoreKey across processes and language clients sharing this identity
        return (endpoint.isTls ? "https://" : "http://")
                + endpoint.host.toLowerCase(Locale.ROOT) + ':' + endpoint.port + endpoint.path;
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

    private static ClientTlsConfiguration defaultTlsConfig() {
        return new ClientTlsConfiguration(null, null, ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL);
    }

    private static boolean discardBody(Response body, int timeoutMillis) {
        // best-effort drain after a parse failure to keep the keep-alive connection usable; bounded like
        // parseBody so a hostile server cannot wedge the thread here either. Returns true only when the body
        // was fully drained (so the connection can be reused); returns false when the drain stopped early -
        // on the deadline, the byte cap, or a transport error - leaving unconsumed bytes, so the caller must
        // drop the connection rather than parse this response's leftovers on the next request.
        final long deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
        long totalBytes = 0;
        try {
            while (true) {
                final long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return false;
                }
                Fragment fragment = body.recv((int) Math.max(1, Math.min(remainingNanos / 1_000_000L, Integer.MAX_VALUE)));
                if (fragment == null) {
                    return true;
                }
                totalBytes += fragment.hi() - fragment.lo();
                if (totalBytes > MAX_RESPONSE_BODY_BYTES) {
                    return false;
                }
            }
        } catch (HttpClientException ignore) {
            return false;
        }
    }

    private static void discoverFromIdp(String issuer, ClientTlsConfiguration tlsConfig, boolean allowInsecureTransport, WellKnownDiscoveryParser parser) {
        // the issuer is pinned out of band (the caller guarantees it is non-null), so the server cannot choose
        // where discovery - and the credential POSTs it resolves - are aimed
        String url = wellKnownUrl(issuer);
        Endpoint endpoint = Endpoint.parse(url);
        requireSecureIdpEndpoint(endpoint, "OIDC issuer", url, allowInsecureTransport);
        fetchJson(endpoint, endpoint.path, tlsConfig, parser,
                "could not reach the identity provider to discover OIDC settings",
                "could not parse the identity provider discovery document");
    }

    private static void discoverSettings(Endpoint server, ClientTlsConfiguration tlsConfig, SettingsDiscoveryParser parser) {
        fetchJson(server, appendSettingsPath(server.path), tlsConfig, parser,
                "could not reach the QuestDB server to discover OIDC settings",
                "could not parse the QuestDB /settings response");
    }

    private static OidcAuthException endpointNotUnderIssuer(String label, String url, String issuer) {
        return new OidcAuthException()
                .put("the OIDC ").put(label).put(" advertised by the QuestDB /settings response (").put(url)
                .put(") is not under the pinned issuer (").put(issuer).put("); refusing to send credentials to ")
                .put("an endpoint outside the trusted issuer, for example a different realm on the same host; ")
                .put("if the identity provider places its endpoints outside the issuer path, configure them ")
                .put("explicitly with OidcDeviceAuth.builder()");
    }

    private static OidcAuthException endpointOriginNotPinned(String label, String url, String pinOrigin) {
        return new OidcAuthException()
                .put("the OIDC ").put(label).put(" advertised by the QuestDB /settings response (").put(url)
                .put(") is not on the pinned identity-provider origin (").put(pinOrigin).put("); refusing to send ")
                .put("credentials to an endpoint outside the trusted issuer. If the identity provider hosts its ")
                .put("endpoints on a different origin than its issuer, configure them explicitly with ")
                .put("OidcDeviceAuth.builder()");
    }

    private static boolean endpointPathHasEncodedSeparator(String rawEndpointPath) {
        // Scan for a literal backslash (decodePathSegments folds it to '/') or a percent-encoded path
        // separator - %2f ('/'), %5c ('\'), or an encoded percent %25 that gates a split or double encoding
        // such as %2%66 or %252f - at every decode level, not just the raw string. A separator that only
        // emerges after the server unescapes more than once would pass a single-pass scan yet split one
        // segment in two, letting .../realms/acme%2%66evil/token slip the issuer-path scope. A real OIDC
        // endpoint path encodes none of these. Bounded like decodePathSegments; a real path needs 0-1 passes.
        String decoded = rawEndpointPath;
        for (int pass = 0; pass < 10; pass++) {
            for (int i = 0, n = decoded.length(); i < n; i++) {
                char c = decoded.charAt(i);
                if (c == '\\') {
                    return true;
                }
                if (c == '%' && i + 2 < n) {
                    char a = decoded.charAt(i + 1);
                    char b = decoded.charAt(i + 2);
                    if ((a == '2' && (b == 'f' || b == 'F' || b == '5')) || (a == '5' && (b == 'c' || b == 'C'))) {
                        return true;
                    }
                }
            }
            String next = percentDecodeOnce(decoded);
            if (next.equals(decoded)) {
                break;
            }
            decoded = next;
        }
        return false;
    }

    private static void fetchJson(Endpoint endpoint, String path, ClientTlsConfiguration tlsConfig, JsonParser parser, String reachError, String parseError) {
        HttpClient client = endpoint.isTls
                ? HttpClientFactory.newTlsInstance(HTTP_CONFIG, tlsConfig)
                : HttpClientFactory.newPlainTextInstance(HTTP_CONFIG);
        // allocate the native lexer inside the try: new JsonLexer mallocs and can throw (native OOM), and
        // the client is already allocated, so a throw before the try is entered would skip the finally and
        // leak the client's native buffers
        JsonLexer lexer = null;
        try {
            lexer = new JsonLexer(JSON_LEXER_CACHE_SIZE, JSON_LEXER_MAX_VALUE_BYTES);
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

    private static boolean hasOnlyTokenChars(CharSequence token) {
        for (int i = 0, n = token.length(); i < n; i++) {
            char c = token.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                return false;
            }
        }
        return true;
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
        String rawEndpointPath = pathOnly(endpointUrl);
        // A real OIDC endpoint path never encodes a path separator or uses a backslash; reject either before
        // the segment comparison, since decodePathSegments resolves them and would split one path segment in
        // two, letting .../realms/acme%2fevil/token (or its split/backslash forms) slip the issuer-path scope.
        if (endpointPathHasEncodedSeparator(rawEndpointPath)) {
            return false;
        }
        String[] endpointSegs = decodePathSegments(rawEndpointPath);
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
        try {
            // the Charset overload is Java 10; the client targets Java 8, so use the String-charset form
            return URLEncoder.encode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // UTF-8 is guaranteed present on every JVM, so this is unreachable; rethrow defensively
            throw new OidcAuthException(e).put("UTF-8 encoding is not supported");
        }
    }

    private static void validateEndpointOrigins(Endpoint tokenEndpoint, Endpoint deviceAuthorizationEndpoint, Endpoint issuer) {
        // the device code and long-lived refresh token are POSTed to the device authorization and token
        // endpoints. RFC 8628 co-locates them on one authorization server, so reject a config that splits
        // them across origins (a tampered /settings or discovery document siphoning one off) on every
        // construction path. The issuer-origin pin here is the explicit builder().issuer() opt-in - a sanity
        // check that user-supplied endpoints sit on the pinned origin; a provider hosting its endpoints off
        // the issuer origin must then be configured without an issuer. fromQuestDB pins differently: it
        // origin-pins only the /settings-advertised endpoints itself (a discovered endpoint is trusted), so
        // it passes no issuer here and relies on this method only for the co-location check.
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
        if (!hasOnlyTokenChars(token)) {
            throw new OidcAuthException()
                    .put("the identity provider returned an ").put(tokenName)
                    .put(" containing a disallowed control or non-ASCII character; refusing to use it as a credential");
        }
    }

    private static String wellKnownUrl(String issuer) {
        String trimmed = issuer;
        while (trimmed.length() > 1 && trimmed.charAt(trimmed.length() - 1) == '/') {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed + WELL_KNOWN_OPENID_CONFIGURATION_PATH;
    }

    private boolean adopt(PersistedToken token) {
        if (token == null) {
            return false;
        }
        // the file is attacker-writable, so treat the served token (the one getToken() puts verbatim into an
        // Authorization header or a PG-wire password) as untrusted: reject a control/non-ASCII char - and the
        // whole entry - rather than route a tampered credential onto the wire. A null served token is unusable.
        String servedToken = groupsInToken ? token.getIdToken() : token.getAccessToken();
        if (servedToken == null || !hasOnlyTokenChars(servedToken)) {
            return false;
        }
        accessToken = token.getAccessToken();
        idToken = token.getIdToken();
        refreshToken = token.getRefreshToken();
        // the file is attacker-writable (and may have been written under a skewed clock), so bound how long
        // the loaded token is trusted exactly as storeTokens() bounds a token from the wire: never past
        // MAX_EXPIRES_IN_SECONDS from now. Capping (not flooring) the expiry preserves an already-expired
        // entry, so a stale access token still falls through to a refresh rather than being served forever.
        long maxTokenLifeMillis = MAX_EXPIRES_IN_SECONDS * 1000L;
        tokenTtlMillis = Math.max(0L, Math.min(token.getTokenTtlMillis(), maxTokenLifeMillis));
        expiresAtMillis = Math.min(token.getExpiresAtMillis(), System.currentTimeMillis() + maxTokenLifeMillis);
        // it is already on disk, so a later non-rotating refresh must not rewrite the file
        lastPersistedRefreshToken = refreshToken;
        return true;
    }

    private void appendEncodedParam(StringSink sink, String name, String encodedValue) {
        sink.putAscii('&').putAscii(name).putAscii('=').putAscii(encodedValue);
    }

    private void appendParam(StringSink sink, String name, String value) {
        sink.putAscii('&').putAscii(name).putAscii('=').putAscii(urlEncode(value));
    }

    private long effectiveSkewMillis() {
        // mirror the Python client's TokenSet.is_valid: cap the fixed 30s skew at half the token lifetime, so
        // a short-lived (< 60s) token is not treated as expired the instant it is issued. With an unknown
        // lifetime (no token cached yet), fall back to the full skew.
        if (tokenTtlMillis <= 0) {
            return CLOCK_SKEW_MILLIS;
        }
        return Math.min(CLOCK_SKEW_MILLIS, tokenTtlMillis / 2);
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
        // responseStatus is the bare-digit HTTP status captured by readResponse. A real status is exactly 3
        // digits, so require that before reading the leading digit: a malformed short status such as "2" must
        // not be mistaken for a 2xx success and accepted as a grant.
        return responseStatus.length() == 3 && responseStatus.charAt(0) == '2';
    }

    private boolean isHttpStatusTerminal4xx() {
        // a 4xx other than 429 is a terminal client-error rejection (429 is a transient rate-limit); require a
        // full 3-digit status so a malformed short "4" is not classified as a terminal 4xx
        return responseStatus.length() == 3 && responseStatus.charAt(0) == '4' && !Chars.equals(HTTP_STATUS_TOO_MANY_REQUESTS, responseStatus);
    }

    private boolean isHttpStatusTransient() {
        // a 5xx server error or a 429 rate-limit is transient - keep polling; any other non-2xx (a 4xx
        // rejection) is terminal. Mirrors the Python client's _http_status_is_transient. Require a full
        // 3-digit status so a malformed short "5" is not classified as a transient 5xx.
        return responseStatus.length() == 3 && (responseStatus.charAt(0) == '5' || Chars.equals(HTTP_STATUS_TOO_MANY_REQUESTS, responseStatus));
    }

    private void maybeLoadFromStore() {
        if (tokenStore == null || storeLoadAttempted) {
            return;
        }
        // attempt the disk read once per instance, even if it yields nothing, so a missing or bad file is
        // not re-read on every call
        storeLoadAttempted = true;
        PersistedToken token;
        try {
            token = tokenStore.load(storeKey);
        } catch (RuntimeException e) {
            // best-effort: a store read failure must not break sign-in
            warnPersistence("load", e);
            return;
        }
        adopt(token);
    }

    private void persistIfRotated() {
        if (tokenStore == null) {
            return;
        }
        // persist on a new or rotated refresh token (the interactive sign-in, or a provider that rotates the
        // refresh token on every refresh); skip when it is unchanged, so the hot getToken() refresh path does
        // not rewrite the file every few minutes. The on-disk access token then goes stale, which costs only
        // one silent refresh on the next restart. With no refresh token there is nothing worth persisting.
        if (Objects.equals(refreshToken, lastPersistedRefreshToken)) {
            return;
        }
        try {
            tokenStore.save(storeKey, snapshot());
            lastPersistedRefreshToken = refreshToken;
        } catch (RuntimeException e) {
            // best-effort: a save failure never fails an otherwise-valid sign-in; the token is valid in memory
            warnPersistence("save", e);
        }
    }

    private void pollForToken(String deviceCode, int expiresInSeconds, int intervalSeconds) {
        // url-encode the opaque device code once here, not on every poll: it is invariant for the whole
        // poll loop (the grant_type and client_id are likewise pre-encoded)
        final String deviceCodeEncoded = urlEncode(deviceCode);
        final long deadlineNanos = System.nanoTime() + expiresInSeconds * 1_000_000_000L;
        long intervalMillis = (long) intervalSeconds * 1000L;
        while (true) {
            throwIfClosed();
            // check the deadline before polling so an expiry that elapsed during the previous sleep aborts
            // here, not after one more wasted poll round-trip
            if (System.nanoTime() >= deadlineNanos) {
                throw new OidcAuthException("timed out waiting for authorization, the device code expired; please retry");
            }
            try {
                int result = pollOnce(deviceCodeEncoded);
                if (result == POLL_SUCCESS) {
                    return;
                }
                if (result == POLL_SLOW_DOWN) {
                    // grow the interval per RFC 8628, capped at the same bound as the initial value so
                    // repeated slow_down / 429 responses cannot inflate the wait without bound
                    intervalMillis = Math.min(intervalMillis + SLOW_DOWN_INCREMENT_SECONDS * 1000L, MAX_POLL_INTERVAL_SECONDS * 1000L);
                }
                // POLL_PENDING and POLL_TRANSIENT_ERROR (a transient 5xx) just poll again
            } catch (HttpClientException e) {
                // a transport failure (dropped connection, DNS blip, timeout) is transient: the user may
                // already have authorized, and RFC 8628 expects polling to continue until the device code
                // expires, so poll again rather than discard the sign-in (the deadline bounds the total
                // wait). Matches the Python client.
            } catch (OidcAuthException e) {
                // a garbled / non-JSON body (a JsonException cause) is transient too, UNLESS its HTTP status
                // is a terminal rejection (a non-JSON 4xx from a WAF or proxy); a well-formed terminal answer
                // - an OAuth error, a terminal 4xx, a malformed status line - always aborts
                if (!(e.getCause() instanceof JsonException) || isHttpStatusTerminal4xx()) {
                    throw e;
                }
            }
            // wait for the next poll, never past the device-code deadline, so the timeout check at the top
            // of the loop fires promptly at expiry instead of up to one poll interval late
            sleepBetweenPolls(Math.min(intervalMillis, (deadlineNanos - System.nanoTime()) / 1_000_000L));
        }
    }

    private int pollOnce(String deviceCodeEncoded) {
        formSink.clear();
        formSink.putAscii("grant_type=").putAscii(GRANT_TYPE_DEVICE_CODE_ENCODED);
        appendEncodedParam(formSink, "device_code", deviceCodeEncoded);
        appendEncodedParam(formSink, "client_id", clientIdEncoded);

        tokenParser.clear();
        // a transport failure here propagates to pollForToken, which keeps polling (a transient blip) until
        // the device-code deadline rather than swallowing it as a pending authorization
        postForm(tokenEndpoint, tokenParser);

        // RFC 6749 5.2: an error response is an error even if the body also carries a token, or the status is
        // 429 - so handle the OAuth error first. A terminal error (e.g. access_denied) must abort even when
        // the identity provider also rate-limits, and a token smuggled alongside an error must never count as
        // a grant.
        if (tokenParser.error.length() > 0) {
            if (Chars.equals(ERROR_AUTHORIZATION_PENDING, tokenParser.error)) {
                return POLL_PENDING;
            }
            if (Chars.equals(ERROR_SLOW_DOWN, tokenParser.error)) {
                return POLL_SLOW_DOWN;
            }
            throw OidcAuthException.oauthError(tokenParser.error, tokenParser.errorDescription);
        }

        // A rate-limited identity provider answers 429 with no OAuth error; RFC 8628 does not define it, but
        // the Python client and common practice treat it as "poll slower". Back off and keep polling (like
        // slow_down) rather than treating it as a terminal error, so transient rate limiting does not fail
        // the sign-in.
        if (Chars.equals(HTTP_STATUS_TOO_MANY_REQUESTS, responseStatus)) {
            return POLL_SLOW_DOWN;
        }
        // RFC 6749 5.1: a grant is a 2xx response carrying a token; a token under a non-2xx is malformed and
        // is not trusted (the non-2xx is classified below instead)
        if (isHttpStatusSuccess()) {
            if (tokenParser.accessToken.length() > 0 || tokenParser.idToken.length() > 0) {
                storeTokens(tokenParser);
                return POLL_SUCCESS;
            }
            // a 2xx with neither a token nor an OAuth error is a definitive but malformed answer
            throw new OidcAuthException().put("unexpected response from the token endpoint [httpStatus=").put(responseStatus).put(']');
        }
        // a non-2xx with no recognized OAuth error: a 5xx (or 429, handled above) is a transient server or
        // gateway condition - keep polling to the deadline; any other status is a terminal rejection (a 4xx
        // from the identity provider, a WAF or a proxy) that aborts immediately rather than polling on to a
        // misleading "device code expired". Matches the Python client.
        if (isHttpStatusTransient()) {
            return POLL_TRANSIENT_ERROR;
        }
        throw new OidcAuthException().put("the token endpoint rejected the request [httpStatus=").put(responseStatus).put("]; refusing to keep polling");
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
        try {
            HttpClient.ResponseHeaders response = request.send(httpTimeoutMillis);
            response.await(httpTimeoutMillis);
            readResponse(client, response, parser);
        } catch (HttpClientException e) {
            // a transport failure, or a bounded-read abort in parseBody (its wall-clock deadline or the
            // MAX_RESPONSE_BODY_BYTES cap), leaves the response half-read with unconsumed bytes in this
            // cached keep-alive connection. Drop it so the next poll or refresh reconnects with a clean
            // socket instead of parsing the previous response's leftovers - which pollForToken would
            // otherwise keep doing, on a corrupted connection, until the device code expires. Mirrors the
            // disconnect-on-failure handling in AbstractLineHttpSender.flush0.
            client.disconnect();
            throw e;
        }
    }

    private void readResponse(HttpClient client, HttpClient.ResponseHeaders response, JsonParser parser) {
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
            // leading digit as a success gate. Drain the body first to keep the keep-alive connection usable;
            // if it could not be fully drained, drop the connection so the next request does not read this
            // body's leftovers.
            CharSequence raw = statusCode.asAsciiCharSequence();
            for (int i = 0, n = raw.length(); i < n; i++) {
                char c = raw.charAt(i);
                if (c < '0' || c > '9') {
                    if (!discardBody(body, httpTimeoutMillis)) {
                        client.disconnect();
                    }
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
            // tokens. A body too large to drain within the cap (e.g. a multi-MB malformed response) leaves
            // unconsumed bytes, so drop the connection rather than mis-frame the next request's response.
            if (!discardBody(body, httpTimeoutMillis)) {
                client.disconnect();
            }
            throw new OidcAuthException(e)
                    .put("could not parse the identity provider response [httpStatus=").put(responseStatus).put(']');
        }
    }

    private boolean refreshUnderLock() {
        // runs inside the store's cross-process lock: re-read first, since another process sharing this
        // identity may have refreshed (and rotated the refresh token) since our last load. Adopt a fresher
        // entry and skip the network when it already yields a valid token; otherwise refresh with the freshest
        // known refresh token (the one just adopted, so a rotated token is not replayed).
        //
        // Only re-read when the in-memory refresh token still matches what we last persisted. If they differ,
        // a previous save failed (persistence is best-effort), so the in-memory token is newer than the
        // on-disk one; re-adopting would regress it to the stale - and, on a rotating identity provider,
        // already-revoked - on-disk token and force a needless re-prompt. In that case keep the in-memory
        // token and refresh with it.
        if (Objects.equals(refreshToken, lastPersistedRefreshToken)) {
            PersistedToken fresh;
            try {
                fresh = tokenStore.load(storeKey);
            } catch (RuntimeException e) {
                warnPersistence("load", e);
                fresh = null;
            }
            if (adopt(fresh)) {
                final String servedToken = groupsInToken ? idToken : accessToken;
                if (servedToken != null && System.currentTimeMillis() < expiresAtMillis - effectiveSkewMillis()) {
                    return true;
                }
            }
        }
        return tryRefresh();
    }

    private void runDeviceFlow() {
        formSink.clear();
        formSink.putAscii("client_id=").putAscii(clientIdEncoded);
        appendEncodedParam(formSink, "scope", scopeEncoded);
        if (audienceEncoded != null) {
            appendEncodedParam(formSink, "audience", audienceEncoded);
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
        // the device code is sent in the poll requests, not shown, so check it on the wire; the user code and
        // verification URL are shown to the user, so sanitize them first and require them non-empty after
        // sanitizing - a value made entirely of control/format chars is non-empty on the wire but would
        // otherwise display as a blank code or URL
        final String deviceCode = deviceAuthParser.deviceCode.toString();
        final String userCode = sanitizeForDisplay(deviceAuthParser.userCode.toString());
        final String verificationUri = sanitizeForDisplay(deviceAuthParser.verificationUri.toString());
        if (deviceCode.isEmpty() || userCode.isEmpty() || verificationUri.isEmpty()) {
            throw new OidcAuthException().put("incomplete device authorization response from the identity provider [httpStatus=").put(responseStatus).put(']');
        }
        // a verification_uri_complete that is non-empty on the wire but sanitizes to empty is treated as
        // absent (null), so the prompt prints no blank "(or open this URL ...)" line and the browser launcher
        // is never handed an empty string
        String verificationUriComplete = deviceAuthParser.verificationUriComplete.length() > 0
                ? sanitizeForDisplay(deviceAuthParser.verificationUriComplete.toString())
                : null;
        if (verificationUriComplete != null && verificationUriComplete.isEmpty()) {
            verificationUriComplete = null;
        }

        final int expiresInSeconds = boundedSeconds(deviceAuthParser.expiresIn, DEFAULT_DEVICE_CODE_TTL_SECONDS, MAX_DEVICE_CODE_TTL_SECONDS);
        final int intervalSeconds = boundedSeconds(deviceAuthParser.interval, DEFAULT_POLL_INTERVAL_SECONDS, MAX_POLL_INTERVAL_SECONDS);
        final DeviceAuthorizationChallenge challenge = new DeviceAuthorizationChallenge(
                userCode,
                verificationUri,
                verificationUriComplete,
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

    private PersistedToken snapshot() {
        return new PersistedToken(accessToken, idToken, refreshToken, expiresAtMillis, tokenTtlMillis);
    }

    private void storeTokens(TokenResponseParser parser) {
        // reject a token with control or non-ASCII chars before caching: getToken() serves it verbatim as an
        // HTTP Authorization header value and a PG-wire password, where a decoded CR/LF would inject into the
        // request line sent to the trusted QuestDB server. Validate only the kind getToken() actually serves
        // (the one that reaches the wire); the other kind is cached but never sent, so a stray char in it must
        // not abort an otherwise-usable grant.
        if (groupsInToken) {
            validateTokenChars(parser.idToken, "id_token");
        } else {
            validateTokenChars(parser.accessToken, "access_token");
        }
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
        tokenTtlMillis = ttlSeconds * 1000L;
        expiresAtMillis = System.currentTimeMillis() + tokenTtlMillis;
        persistIfRotated();
    }

    private void throwIfClosed() {
        if (closed) {
            throw new OidcAuthException("the OidcDeviceAuth instance is closed");
        }
    }

    private boolean tryRefresh() {
        formSink.clear();
        formSink.putAscii("grant_type=").putAscii(GRANT_TYPE_REFRESH_TOKEN_ENCODED);
        appendParam(formSink, "refresh_token", refreshToken);
        appendEncodedParam(formSink, "client_id", clientIdEncoded);
        appendEncodedParam(formSink, "scope", scopeEncoded);
        if (audienceEncoded != null) {
            appendEncodedParam(formSink, "audience", audienceEncoded);
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

    private boolean tryRefreshCoordinated() {
        if (tokenStore == null) {
            return tryRefresh();
        }
        // serialise the read-refresh-write across processes (and adopt a peer's just-rotated refresh token)
        // through the store's per-identity lock; a store that does not coordinate just runs the refresh
        return tokenStore.inLock(storeKey, this::refreshUnderLock);
    }

    private void warnPersistence(String operation, Throwable cause) {
        // best-effort persistence: report to System.err and carry on with the in-memory token. The store
        // never puts token bytes in its messages, so this cannot leak the secret.
        String detail = cause.getMessage();
        System.err.println("questdb client: OIDC token store " + operation
                + " failed; continuing without persistence" + (detail != null ? " [" + detail + ']' : ""));
    }

    /**
     * Fluent builder for an {@link OidcDeviceAuth} configured against a known identity provider.
     * The client id, device authorization endpoint and token endpoint are required.
     */
    public static final class Builder {
        private boolean allowInsecureTransport;
        private String audience;
        private String clientId;
        private String deviceAuthorizationEndpoint;
        private boolean groupsInToken;
        private int httpTimeoutMillis = DEFAULT_HTTP_TIMEOUT_MILLIS;
        private String issuer;
        private DeviceCodePrompt prompt = DeviceCodePrompt.openBrowser();
        private String scope = DEFAULT_SCOPE;
        private ClientTlsConfiguration tlsConfig;
        private String tokenEndpoint;
        private TokenStore tokenStore;

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
            if (httpTimeoutMillis <= 0) {
                throw new OidcAuthException("httpTimeoutMillis must be positive");
            }
            if (httpTimeoutMillis > MAX_HTTP_TIMEOUT_MILLIS) {
                throw new OidcAuthException()
                        .put("httpTimeoutMillis must not exceed ").put(MAX_HTTP_TIMEOUT_MILLIS)
                        .put("; a token-endpoint round-trip never needs longer, and a larger value could let a ")
                        .put("slow refresh outlast the token store's cross-process lock staleness window");
            }
            this.httpTimeoutMillis = httpTimeoutMillis;
            return this;
        }

        /**
         * Pins the identity provider by its {@code issuer} origin (for example
         * {@code https://idp.example.com}). When set, {@link #build()} rejects the explicitly configured token
         * or device authorization endpoint if it is not on this origin - a sanity check that the endpoints you
         * supplied belong to the issuer you intended. A provider hosting its endpoints on a different origin
         * than its issuer (for example Google) is rejected when pinned this way; for such a provider, configure
         * the endpoints without an issuer. Optional.
         * <p>
         * {@link #fromQuestDB(String, DiscoveryOptions)} pins differently: it constrains only the endpoints the
         * untrusted {@code /settings} response advertised (to the issuer's origin, and under its path when the
         * issuer has one), while endpoints discovered from the provider's own {@code .well-known} are trusted
         * wherever the issuer hosts them - so discovery against an off-origin provider like Google works.
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

        /**
         * Persists the obtained token through the given {@link TokenStore}, so a restarted process can resume
         * from the saved refresh token instead of running the device flow again. Defaults to {@code null}
         * (in-memory only). Use {@link FileTokenStore#atDefaultLocation()} for the default file-backed store,
         * or supply your own to back persistence with an OS keychain or a secrets manager. Optional.
         */
        public Builder tokenStore(TokenStore tokenStore) {
            this.tokenStore = tokenStore;
            return this;
        }
    }

    /**
     * Options for {@link #fromQuestDB(String, DiscoveryOptions)}: how to pin the identity provider
     * (issuer), the TLS configuration for discovery and sign-in, whether to permit insecure {@code http},
     * and how to show the device code challenge. Every option is optional; an instance with nothing set
     * behaves like {@link #fromQuestDB(String)}.
     */
    public static final class DiscoveryOptions {
        private boolean allowInsecureTransport;
        private String issuer;
        private DeviceCodePrompt prompt = DeviceCodePrompt.openBrowser();
        private ClientTlsConfiguration tlsConfig;
        private TokenStore tokenStore;

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
         * Pins the identity provider by its {@code issuer} origin (for example
         * {@code https://idp.example.com}). It plays two roles: when the server does not advertise the
         * device authorization endpoint, it is discovered from the issuer's
         * {@code .well-known/openid-configuration} (the discovery origin comes only from this out-of-band
         * issuer, never from {@code /settings}); and it constrains the endpoints the untrusted
         * {@code /settings} response advertised - they must be on the issuer's origin, and under its path when
         * the issuer has one, so a tampered {@code /settings} cannot redirect credentials to a different origin
         * or to a different tenant on a path-based provider (for example a Keycloak realm path like
         * {@code /realms/acme}). Endpoints discovered from the provider's own {@code .well-known} are trusted
         * wherever the issuer hosts them, so an identity provider that serves its endpoints from a different
         * origin than its issuer (for example Google) works through discovery. Optional.
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

        /**
         * Persists the obtained token through the given {@link TokenStore}, so a restarted process can resume
         * from the saved refresh token instead of running the device flow again. Defaults to {@code null}
         * (in-memory only). See {@link FileTokenStore#atDefaultLocation()} for the default file-backed store.
         */
        public DiscoveryOptions tokenStore(TokenStore tokenStore) {
            this.tokenStore = tokenStore;
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
            // the authority ([userinfo@]host[:port]) ends at the first '/', '?' or '#'; splitting only on
            // '/' (as before) folded a query/fragment - or userinfo - into the host on a path-less url
            int authorityEnd = url.length();
            for (int i = hostStart, n = url.length(); i < n; i++) {
                char c = url.charAt(i);
                if (c == '/' || c == '?' || c == '#') {
                    authorityEnd = i;
                    break;
                }
            }
            String hostPort = url.substring(hostStart, authorityEnd);
            // a path-less url uses '/'; a query/fragment with no path is prefixed with '/' so the request
            // line stays well-formed (a '/'-terminated authority already carries its own leading slash)
            String path;
            if (authorityEnd == url.length()) {
                path = "/";
            } else if (url.charAt(authorityEnd) == '/') {
                path = url.substring(authorityEnd);
            } else {
                path = "/" + url.substring(authorityEnd);
            }
            if (hostPort.indexOf('@') >= 0) {
                // userinfo (user[:pass]@host) is unsupported: the HTTP layer would connect to the literal
                // "user@host". Reject it clearly rather than mis-resolve it or surface a misleading port error
                throw new OidcAuthException().put("invalid url, userinfo (user@host) is not supported [url=").put(url).put(']');
            }
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
        private static final int FIELD_NONE = 0;
        private static final int FIELD_TOKEN_ENDPOINT = 2;
        final StringSink deviceAuthorizationEndpoint = new StringSink();
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
