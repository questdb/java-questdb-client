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
import io.questdb.client.cutlass.http.HttpException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
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
 * behind it - but {@link #getToken()} never waits behind an interactive sign-in: it fails fast with an
 * {@link OidcAuthException} rather than stall a request/flush path (a needed silent refresh still runs,
 * each HTTP round-trip phase bounded by {@link Builder#httpTimeoutMillis(int)}, and the TCP connect and TLS
 * handshake bounded by it too - though DNS resolution is still the OS's to bound - plus, with a
 * coordinating {@link TokenStore}, a brief
 * cross-process lock wait; see {@link #getToken()}). To abort a waiting sign-in, call
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
    // The config the DISCOVERY clients take. Discovery runs off DEFAULT_HTTP_TIMEOUT_MILLIS rather than a
    // builder value, because it happens before there is an instance to carry one. See httpConfig().
    private static final HttpClientConfiguration DISCOVERY_HTTP_CONFIG = httpConfig(DEFAULT_HTTP_TIMEOUT_MILLIS);
    private static final String ERROR_AUTHORIZATION_PENDING = "authorization_pending";
    private static final String ERROR_SLOW_DOWN = "slow_down";
    // getToken() polls for the instance lock in slices this small so it observes an interactive sign-in that
    // starts while it waits (and close()) promptly, rather than blocking a whole refresh behind a single
    // acquire; see acquireForGetToken()
    private static final long GET_TOKEN_LOCK_POLL_SLICE_MILLIS = 50;
    // the grant_type values are constants, so url-encode them once at class load rather than on every
    // device-code poll and token refresh
    private static final String GRANT_TYPE_DEVICE_CODE_ENCODED = urlEncode(GRANT_TYPE_DEVICE_CODE);
    private static final String GRANT_TYPE_REFRESH_TOKEN_ENCODED = urlEncode(GRANT_TYPE_REFRESH_TOKEN);
    // a rate-limited identity provider answers 429; the token poll treats it as a transient backoff
    private static final String HTTP_STATUS_TOO_MANY_REQUESTS = "429";
    // Token responses carry JWTs (an id token with group claims can be several KB), and a single
    // value may arrive split across HTTP fragments. The lexer stashes a split value and rejects it
    // past JSON_LEXER_MAX_VALUE_BYTES, so the limit must comfortably exceed any real token or large
    // tokens fail to parse with "String is too long".
    private static final int JSON_LEXER_CACHE_SIZE = 1024;
    private static final int JSON_LEXER_MAX_VALUE_BYTES = 1 << 20;
    // the I/O portion of a coordinated refresh, as a multiple of httpTimeoutMillis: the refresh under the lock
    // runs send + await + parse, plus a body drain on a parse failure, each separately bounded by
    // httpTimeoutMillis. The connection phase that precedes the send is bounded by httpTimeoutMillis too -
    // httpConfig() derives the TCP connect timeout and the TLS handshake budget from it - so the only part of a
    // hold this multiple does not account for is DNS resolution, which the OS bounds. build() requires the
    // FileTokenStore staleness window to exceed this multiple as a floor (see build())
    private static final int LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(OidcDeviceAuth.class);
    // upper bound on the device code lifetime (the device authorization response's expires_in), so a
    // hostile or buggy provider cannot make the client poll for an absurd duration; matches the Python client
    private static final int MAX_DEVICE_CODE_TTL_SECONDS = 1800;
    /**
     * Floor on how often {@link #getToken()} will re-attempt a silent refresh after one failed. Without it a
     * revoked refresh token, or an IdP outage, cost a full token-endpoint round trip on EVERY call - and
     * getToken() is called once per ILP flush and once per WebSocket (re)connect, so a producer retrying its
     * rows drove a sustained request flood at the identity provider (enough to trip its rate limits and
     * lengthen the very outage being retried) while each call blocked the producer for the round trip, up to
     * httpTimeoutMillis against a black-holed endpoint.
     * <p>
     * Deliberately short: this is a stampede guard, not a circuit breaker. A credential that comes back
     * within seconds is picked up on the next call, and any explicit {@link #signIn()} or
     * {@link #clearCache()} clears the latch outright.
     */
    private static final long MIN_REFRESH_RETRY_INTERVAL_MILLIS = 5_000L;
    // First non-zero back-off between token store reads; it doubles per consecutive failure, up to
    // MAX_STORE_LOAD_RETRY_INTERVAL_MILLIS. Same floor as the refresh back-off above, and the same kind of
    // stampede guard. The FIRST failure arms a ZERO-length back-off, so the very next call still re-reads the
    // store: a one-shot fault - notably a carried interrupt flag, which makes the InterruptibleChannel under
    // FileTokenStore throw on a thread that merely carries it - must recover on the next call rather than wait
    // this out. Anything that survives that free retry needs an operator (a chmod, a remount), so waiting is
    // no longer costing a recovery that was about to happen anyway.
    private static final long MIN_STORE_LOAD_RETRY_INTERVAL_MILLIS = 5_000L;
    // upper bound on the token cache lifetime (the token response's expires_in), so an absurd or hostile
    // value cannot overflow the timing arithmetic or make the client trust a token for absurdly long
    private static final int MAX_EXPIRES_IN_SECONDS = 3600;
    // upper bound on the configurable HTTP request timeout. A token-endpoint round-trip never needs longer, and
    // bounding it keeps the I/O portion of a refresh held under the FileTokenStore cross-process lock (send +
    // await + parse, plus a body drain on a parse failure - each separately bounded by this, so up to ~4x this)
    // a known, bounded multiple, so the store's staleness window can be sized to dominate it. The connection
    // phase is bounded by this too - httpConfig() derives the connect timeout and the TLS handshake budget
    // from the same figure - leaving only DNS resolution to the OS (see Builder.build())
    private static final int MAX_HTTP_TIMEOUT_MILLIS = 120_000;
    // upper bound on the poll interval, both the initial value and the growth after a slow_down or 429, so
    // a hostile or buggy provider cannot stall the poll loop; matches the Python client
    private static final int MAX_POLL_INTERVAL_SECONDS = 60;
    // cap bytes drained per response so a hostile/MITM'd server cannot stream an endless body and
    // wedge the thread; far above any real OIDC JSON response
    private static final int MAX_RESPONSE_BODY_BYTES = 4 * 1024 * 1024;
    // Ceiling on the back-off between token store reads. maybeLoadFromStore() runs on the getToken() path,
    // which an ILP producer calls once per flush, so a store that is permanently unreadable - a chmod or uid
    // mismatch in a container, EIO/ESTALE on an NFS home - otherwise cost a blocking file open, two exception
    // fills and a WARN line on EVERY flush, on the producer thread and under this instance's lock. A store
    // that simply has nothing to return is unaffected: load() reports that by returning null rather than by
    // throwing, and that latches storeLoadAttempted outright.
    private static final long MAX_STORE_LOAD_RETRY_INTERVAL_MILLIS = 60_000L;
    private static final int POLL_PENDING = 1;
    private static final long POLL_SLEEP_SLICE_MILLIS = 100;
    private static final int POLL_SLOW_DOWN = 2;
    private static final int POLL_SUCCESS = 0;
    private static final int POLL_TRANSIENT_ERROR = 3;
    private static final int SLOW_DOWN_INCREMENT_SECONDS = 5;
    private static final String USER_AGENT = "questdb/java-client-oidc";
    private static final String WELL_KNOWN_OPENID_CONFIGURATION_PATH = "/.well-known/openid-configuration";
    private final String audienceEncoded;
    // This instance's HTTP transport budgets, derived from httpTimeoutMillis. See httpConfig().
    private final HttpClientConfiguration clientConfig;
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
    // set only while signIn() runs the interactive device flow (holding the lock for up to the device-code
    // lifetime). getToken() reads it lock-free to fail fast behind an interactive sign-in while still waiting
    // briefly behind a peer's quick silent refresh; volatile for that cross-thread read. See acquireForGetToken()
    private volatile boolean interactiveSignInInProgress;
    private JsonLexer jsonLexer;
    private String lastPersistedRefreshToken;
    // earliest wall-clock millis at which maybeLoadFromStore() may re-read the store after a read threw; 0
    // until the first failure. See isStoreLoadBackedOff()
    private long nextStoreLoadAttemptMillis;
    private HttpClient plainClient;
    private long refreshFailedAtMillis;
    private String refreshToken;
    private boolean storeLoadAttempted;
    // back-off applied to the NEXT failed store read, doubling from MIN_ to MAX_STORE_LOAD_RETRY_INTERVAL_MILLIS;
    // 0 while no read has failed yet, which is what makes the first retry immediate
    private long storeLoadRetryIntervalMillis;
    private HttpClient tlsClient;
    // lifetime in millis of the currently cached token (its clamped TTL); effectiveSkewMillis() caps the
    // clock skew at half of this so a short-lived token is not treated as expired the instant it is issued
    private long tokenTtlMillis;

    private OidcDeviceAuth(Builder builder, ClientTlsConfiguration tlsConfig, Endpoint deviceAuthorizationEndpoint, Endpoint tokenEndpoint) {
        String clientId = builder.clientId;
        // pre-encode the invariant form params once here, so the poll loop and silent refresh do not
        // re-run URLEncoder on every request (mirrors the pre-encoded GRANT_TYPE_* constants)
        this.clientIdEncoded = urlEncode(clientId);
        // build() already parsed and validated these endpoints; reuse them rather than re-parse the raw strings
        this.deviceAuthorizationEndpoint = deviceAuthorizationEndpoint;
        this.tokenEndpoint = tokenEndpoint;
        String scope = builder.scope;
        this.scopeEncoded = urlEncode(scope);
        String audience = builder.audience;
        this.audienceEncoded = audience != null ? urlEncode(audience) : null;
        this.groupsInToken = builder.groupsInToken;
        this.httpTimeoutMillis = builder.httpTimeoutMillis;
        // Derive the transport budgets from the SAME figure the rest of the class quotes, so the connection
        // phase is bounded by it too rather than by the 600s HttpClientConfiguration default (TLS) and the
        // OS (TCP connect). See httpConfig().
        this.clientConfig = httpConfig(this.httpTimeoutMillis);
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
        // allocate the native lexer last: urlEncode and the TokenStoreKey construction above can throw, and
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
            if (tokenEndpointFromSettings && !isSameOrigin(Endpoint.parse(tokenEndpoint), pin)) {
                throw endpointOriginNotPinned("token endpoint", tokenEndpoint, originOf(pin));
            }
            if (deviceEndpointFromSettings && !isSameOrigin(Endpoint.parse(deviceAuthorizationEndpoint), pin)) {
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
            // the same sweep close() runs: nulling the served token is not enough on its own, since the raw
            // response and request bytes are still legible in the reusable sinks that carried them
            wipeCredentialState();
            expiresAtMillis = 0;
            tokenTtlMillis = 0;
            refreshFailedAtMillis = 0;
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
     * finishes or times out, not the full device-code lifetime. That bound is the in-flight operation's own
     * worst case, which is NOT a single HTTP request timeout: a silent refresh under the lock runs a send, an
     * await and a body parse (each bounded by {@link Builder#httpTimeoutMillis(int)}), and its connection phase -
     * TCP connect, TLS handshake - is bounded by that timeout as well, leaving only DNS resolution to the
     * OS, so a black-holed token endpoint can hold the lock, and this {@code close()}, for roughly that (on
     * Linux) rather than a single httpTimeoutMillis. The exception is a
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
            // Drop the credential material FIRST. Every token operation is already refused by the closed flag
            // above, so nothing here can be needed again - and nulling a String or overwriting a sink cannot
            // throw, whereas an HttpClient close conceivably can, so doing it first means a failing free
            // cannot leave a refresh token legible in this instance for the rest of the JVM's life.
            wipeCredentialState();
            // free the native lexer first: its close() is a bare Unsafe.free that cannot throw, whereas an
            // HttpClient close conceivably could - freeing the native buffer first means such a throw cannot
            // strand it (Misc.free nulls each field, so a second close() is still a safe no-op)
            jsonLexer = Misc.free(jsonLexer);
            plainClient = Misc.free(plainClient);
            tlsClient = Misc.free(tlsClient);
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
     * caller should retry once the sign-in completes. It does, however, wait briefly behind another thread's
     * quick cached read or silent refresh rather than fail every concurrent caller sharing this instance on
     * each token refresh - the {@code HttpTokenProvider} contract permits that bounded wait, capped here at
     * FOUR times {@link Builder#httpTimeoutMillis(int)} (two minutes at the 30s default), which is the
     * holder's own worst case: a silent refresh under the lock runs a send, an await and a body parse, each
     * separately bounded by that timeout, so a peer waiting only one would fail every concurrent caller
     * behind a refresh that was going to succeed. Size flush backpressure against the four-times figure, not
     * against {@code httpTimeoutMillis} itself. It still fails fast the moment an interactive sign-in or
     * {@link #close()} begins meanwhile. It is not, otherwise, instantaneous - when the cached
     * token has expired it makes one synchronous refresh round-trip to the token endpoint (and, with a
     * coordinating {@link TokenStore}, may first wait to acquire the store's per-identity lock before that
     * round-trip). For {@link FileTokenStore} the CROSS-process file lock is bounded to a few seconds and then
     * proceeds without it; but the IN-process lock that serializes two instances sharing one identity in the
     * same JVM (an ILP {@code Sender} and a {@code QwpQueryClient}, say) is not time-bounded, so such a
     * concurrent caller instead waits out the peer's whole refresh - itself bounded only by the OS connect
     * stall described next, not by a few seconds.
     * The send, response wait and body parse of that round-trip are each bounded by
     * {@link Builder#httpTimeoutMillis(int)} (30s by default); the connection phase that precedes them - DNS
     * the TCP connect and the TLS handshake - is bounded by httpTimeoutMillis as well, since httpConfig()
     * derives the connect timeout and the handshake budget from it. Only DNS resolution is left to the OS, so
     * an unreachable (black-holed) token endpoint stalls this refresh for about the configured timeout rather
     * than the OS TCP-connect timeout. That is the "quick silent refresh" the {@code HttpTokenProvider}
     * contract permits on the flush path, not an unbounded interactive wait.
     *
     * @return a non-null, non-empty token
     * @throws OidcAuthException if no token has been obtained yet, if the cached token expired and could
     *                           not be refreshed without an interactive sign-in, if an interactive sign-in is
     *                           in progress on another thread, or if a concurrent refresh did not complete in time
     */
    public String getToken() {
        throwIfClosed();
        acquireForGetToken();
        try {
            throwIfClosed();
            maybeLoadFromStore();
            final String cachedToken = groupsInToken ? idToken : accessToken;
            if (cachedToken != null && System.currentTimeMillis() < expiresAtMillis - effectiveSkewMillis()) {
                return cachedToken;
            }
            // The served-kind token is absent or expired. Try a silent refresh whenever a refresh token is
            // available - including the case where a prior grant returned only the OTHER kind, leaving the served
            // kind null: a refresh may yield the served kind and avoid forcing an interactive sign-in. selectToken()
            // reports a clear error if the refresh still did not produce the kind the server expects.
            // Back off after a failed refresh instead of re-attempting on every call. getToken() runs once
            // per ILP flush and once per (re)connect, and a producer retrying rows calls it in a tight loop,
            // so a revoked token or an unreachable IdP otherwise meant one full token-endpoint round trip per
            // attempt - a request flood at the provider, and a producer blocked for each round trip. The
            // failure is still reported on every call; only the network attempt is rate-limited.
            // A caller whose thread is already interrupted is cancelled, and a silent refresh is a network
            // round trip - exactly the work a cancellation is trying to stop. Decline it here, once, for
            // three reasons the old shape got wrong:
            //
            //   - the only interrupt guard was inside FileTokenStore.inLock, so this held ONLY when a token
            //     store was configured. Without one, tryRefreshCoordinated() went straight to tryRefresh()
            //     and POSTed to the token endpoint on a cancelled thread.
            //   - when the store's guard did decline, the fall-through reported "the cached token expired
            //     and could not be refreshed without an interactive sign-in; call signIn()". The endpoint was
            //     reachable and the lock free; the caller's own interrupt was the reason. That sends a user
            //     to re-authenticate over a credential that is fine.
            //   - it then latched the refresh back-off below, so one interrupt-carrying caller suppressed
            //     the next five seconds of legitimate refreshes for every thread sharing this instance.
            //
            // isInterrupted(), never interrupted(): the flag is the caller's cancellation signal and must
            // survive this call, exactly as FileTokenStore.load()/save() preserve it.
            if (refreshToken != null && Thread.currentThread().isInterrupted()) {
                throw new OidcAuthException("the calling thread is interrupted, so no silent token refresh was attempted; retry on an uninterrupted thread");
            }
            // Arm the latch ONLY when a refresh was actually attempted and failed. Stamping it on a call
            // that the back-off itself skipped slides the window forward by one call every time, so it
            // never expires for a caller that returns faster than MIN_REFRESH_RETRY_INTERVAL_MILLIS - and
            // getToken() runs once per ILP flush, at a default auto-flush interval of one second. One
            // transient refresh failure then wedges the sender for the life of the process, long after the
            // identity provider recovered, which is a circuit breaker rather than the stampede guard this
            // is documented to be. maybeLoadFromStore() arms its sibling back-off inside the catch for the
            // same reason: only a real attempt may re-arm.
            if (refreshToken != null && !isRefreshBackedOff()) {
                if (tryRefreshCoordinated()) {
                    refreshFailedAtMillis = 0;
                    return selectToken();
                }
                refreshFailedAtMillis = System.currentTimeMillis();
            }
            if (cachedToken != null) {
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
     * @throws OidcAuthException if the interactive flow fails, times out, the identity provider does not
     *                           return the expected token, or the calling thread carries an interrupt and
     *                           no valid cached token is available - a cancelled caller is not sent through
     *                           a silent refresh or a device flow, both of which are network work
     */
    public String signIn() {
        lock.lock();
        try {
            throwIfClosed();
            // signIn() is an explicit user action, and it is about to spend a whole interactive device flow -
            // so it is never the caller the store-read back-off exists to throttle. Clear that back-off, for
            // the same reason the refresh back-off is cleared further down: a store that has become readable
            // again must be re-read here, rather than have a human sent through the device flow over a refresh
            // token that is sitting on disk.
            nextStoreLoadAttemptMillis = 0;
            storeLoadRetryIntervalMillis = 0;
            maybeLoadFromStore();
            // only the kind of token signIn() actually serves counts as a cache hit; a grant that
            // returned the other kind (access token when the server wants the id token, or vice versa)
            // leaves the served token null, so fall through rather than report the unusable grant as valid
            // and have selectToken() throw on this and every later call
            final String cachedToken = groupsInToken ? idToken : accessToken;
            if (cachedToken != null && System.currentTimeMillis() < expiresAtMillis - effectiveSkewMillis()) {
                return cachedToken;
            }
            // A cached token needs no network and was served above whatever the caller's state; everything
            // from here on is a network round trip, which is the work a cancellation is trying to stop. So
            // decline it, for the same reason and with the same test getToken() applies further up.
            //
            // Checked HERE rather than left to the store, because leaving it there made the outcome depend
            // on whether a TokenStore was configured, and the two branches were wrong in OPPOSITE
            // directions. With no store, tryRefreshCoordinated() went straight to tryRefresh() and POSTed to
            // the token endpoint on a cancelled thread. With a FileTokenStore, inLock declined the carried
            // interrupt by returning false - which reads here as "the refresh failed", so signIn() skipped a
            // refresh it could have completed and started the DEVICE FLOW instead: a human prompt and a poll
            // loop that is far more work than the round trip just declined, and that runs to the device-code
            // lifetime (up to MAX_DEVICE_CODE_TTL_SECONDS) because sleepBetweenPolls uses Os.sleep, which
            // ignores interrupts. A caller who cancelled got a browser prompt and a thread parked for half
            // an hour.
            //
            // isInterrupted(), never interrupted(): the flag is the caller's cancellation signal and must
            // survive this call, exactly as getToken() and FileTokenStore.load()/save() preserve it.
            if (Thread.currentThread().isInterrupted()) {
                throw new OidcAuthException("the calling thread is interrupted, so no sign-in was attempted; retry on an uninterrupted thread");
            }
            // Spend a silent refresh before prompting, whenever a refresh token is available - the same rule
            // getToken() applies. That deliberately includes a null served kind: a restored entry whose grant
            // only ever produced the OTHER kind still carries a usable refresh token, and one round-trip
            // beats sending a human back through the device flow. Gating this on cachedToken != null, as it
            // used to, meant such an entry always re-prompted. A refresh that does not yield the served kind
            // returns false and falls straight through to the flow below, so this costs at most one wasted
            // request and cannot loop.
            // No back-off here, and the latch is cleared either way: signIn() is an explicit user action, it
            // falls through to the interactive flow when the refresh fails, and it is exactly the call a user
            // makes to recover from the failure getToken() is backing off from.
            refreshFailedAtMillis = 0;
            if (refreshToken != null && tryRefreshCoordinated()) {
                return selectToken();
            }
            // Re-check the flag, because the guard on entry only covers an interrupt the caller ARRIVED with.
            // tryRefreshCoordinated() above is a network round trip - up to four times httpTimeoutMillis plus
            // an OS connect stall - and a cancellation landing inside it is the common case, not a narrow
            // race: it is precisely when a refresh is failing that a caller gives up. Proceeding would then
            // launch a browser and park for the device-code lifetime on a thread whose owner has already
            // asked it to stop.
            throwIfInterrupted("the calling thread was interrupted before the interactive sign-in started");
            // flag the interactive phase so a concurrent getToken() fails fast (rather than waiting behind this
            // for the whole device-code lifetime); it still waits behind the cheap cache/refresh work above
            interactiveSignInInProgress = true;
            try {
                runDeviceFlow();
            } finally {
                interactiveSignInInProgress = false;
            }
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
                "could not parse the identity provider discovery document",
                "the identity provider did not return an OIDC discovery document");
    }

    private static void discoverSettings(Endpoint server, ClientTlsConfiguration tlsConfig, SettingsDiscoveryParser parser) {
        fetchJson(server, appendSettingsPath(server.path), tlsConfig, parser,
                "could not reach the QuestDB server to discover OIDC settings",
                "could not parse the QuestDB /settings response",
                "the QuestDB server did not return its settings");
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
        // A real OIDC endpoint path is plain ASCII with no percent-encoding and no backslash, so reject either
        // outright rather than try to out-decode the server. Percent-encoding is exactly where a tampered
        // /settings hides a path separator ('/', '\') or a '..' that only surfaces once the server unescapes -
        // and not only the forms this client's byte-oriented percentDecodeOnce resolves (%2f, %5c, %25, %252f,
        // %2%66), but ones it deliberately does NOT: an overlong-UTF-8 %c0%ae or %e0%80%ae, or an IIS-style
        // %u002e, which a permissive server decodes to '/' or '.' yet a single-byte decode leaves as high bytes
        // or literal text - so they would sail past the segment scan in isEndpointUnderIssuerPath and, sitting
        // past the issuer prefix, slip the scope. A literal backslash likewise folds to '/' on some proxies.
        // Failing closed on any '%' or '\' keeps the issuer-path scope airtight against every encoding trick; a
        // provider that genuinely percent-encodes its endpoint path must be configured explicitly with
        // OidcDeviceAuth.builder(), which pins the origin only.
        for (int i = 0, n = rawEndpointPath.length(); i < n; i++) {
            char c = rawEndpointPath.charAt(i);
            if (c == '%' || c == '\\') {
                return true;
            }
        }
        return false;
    }

    private static void fetchJson(Endpoint endpoint, String path, ClientTlsConfiguration tlsConfig, JsonParser parser, String reachError, String parseError, String statusError) {
        HttpClient client = endpoint.isTls
                ? HttpClientFactory.newTlsInstance(DISCOVERY_HTTP_CONFIG, tlsConfig)
                : HttpClientFactory.newPlainTextInstance(DISCOVERY_HTTP_CONFIG);
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
            // A discovery document decides WHERE the user signs in and where the refresh token is POSTed,
            // so it must be read only out of a response that actually claims to carry one. Parsing
            // regardless of status let an error page supply that configuration: a 500 from /settings or a
            // 404 from .well-known whose body happens to hold the right keys - an error envelope, a proxy's
            // branded page, a captive portal, a tenant-not-found stub - constructed a working instance
            // pointed wherever those keys said. The token and device-authorization paths already gate on
            // status; this one did not.
            requireSuccessStatus(client, response, body, statusError);
            // parseBody enforces an elapsed-time deadline and a byte cap so an untrusted server cannot wedge
            // discovery, and its parseLast rejects a truncated document
            parseBody(body, lexer, parser, DEFAULT_HTTP_TIMEOUT_MILLIS);
        } catch (HttpClientException | HttpException e) {
            // HttpException covers a malformed or oversized RESPONSE HEAD rejected by HttpHeaderParser (see
            // postForm). It is a sibling of HttpClientException, not a subclass, so it escaped both catches
            // here and left fromQuestDB throwing a type its own javadoc does not name - past every caller's
            // catch (OidcAuthException) degrade handler. Discovery reaching an unusable response is the same
            // outcome either way, so report it the same way.
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

    /**
     * The HTTP transport budgets for a client this class owns, all derived from one timeout.
     * <p>
     * DefaultHttpClientConfiguration answers 0 for the connect timeout and 600s for the request timeout, and
     * HttpClient reads BOTH of those on the connection path: it leaves the TCP connect to the OS when the
     * connect timeout is 0, and it sizes the TLS handshake as {@code connectTimeout > 0 ? connectTimeout :
     * defaultTimeout}. Taking the defaults therefore gave the handshake alone a 600s budget -- the whole of
     * FileTokenStore's DEFAULT_LOCK_STALE_MILLIS -- derived from nothing the caller set, so neither
     * MAX_HTTP_TIMEOUT_MILLIS nor Builder.build()'s lockStaleMillis floor constrained it.
     * <p>
     * That matters beyond a slow request. A refresh runs inside the store's cross-process lock, whose file
     * is stamped once at creation and never re-stamped, so a hold that outruns the staleness window is
     * judged abandoned and stolen by a peer. Two holders then POST the same rotating refresh token, and an
     * identity provider with reuse detection answers by revoking the whole family. Deriving both budgets
     * from httpTimeoutMillis is what makes the "up to LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE x httpTimeoutMillis"
     * figure -- which the lock-stale floor, acquireForGetToken's wait cap and getToken()'s javadoc all
     * quote -- actually true of the code.
     */
    private static HttpClientConfiguration httpConfig(final int timeoutMillis) {
        return new DefaultHttpClientConfiguration() {
            @Override
            public int getConnectTimeout() {
                return timeoutMillis;
            }

            @Override
            public int getTimeout() {
                return timeoutMillis;
            }
        };
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
            // strip an RFC 3986 ";matrix" parameter suffix before the dot-segment test: a server or proxy that
            // drops matrix params resolves "..;" (or "..;x") to "..", so /realms/acme/..;/evil/token would
            // otherwise slip the issuer-path pin to a sibling realm. decodePathSegments already percent-decoded,
            // so a "%3b"-encoded ";" is a literal ";" here too.
            String seg = endpointSegs[i];
            int semi = seg.indexOf(';');
            if (semi >= 0) {
                seg = seg.substring(0, semi);
            }
            if (".".equals(seg) || "..".equals(seg)) {
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
        // loopback traffic never leaves the host, so a plaintext fetch to it has no network interception
        // risk; match the whole IPv4 127.0.0.0/8 block and the name "localhost"
        if (host == null) {
            return false;
        }
        // an address literal needs no resolution: it IS the address, and nobody can point it elsewhere
        if (host.startsWith("127.") && isDottedIpv4(host)) {
            return true;
        }
        if (!host.equalsIgnoreCase("localhost")) {
            return false;
        }
        // "localhost" is a NAME. RFC 6761 says it must resolve to loopback, and every normal host honours
        // that - but a minimal image with no /etc/hosts entry leaves it to DNS, and this exemption is
        // precisely what allows the device code and the refresh token to travel in cleartext. Confirm what
        // it actually resolves to rather than trusting the spelling, and require EVERY answer to be
        // loopback: one non-loopback address is enough to send the credential off the machine.
        try {
            final InetAddress[] resolved = InetAddress.getAllByName(host);
            if (resolved.length == 0) {
                return false;
            }
            for (int i = 0; i < resolved.length; i++) {
                if (!resolved[i].isLoopbackAddress()) {
                    return false;
                }
            }
            return true;
        } catch (UnknownHostException e) {
            // fail CLOSED: the caller then requires https, which is never the less safe answer
            return false;
        }
    }

    private static boolean isSameOrigin(Endpoint a, Endpoint b) {
        // scheme (via isTls), host and port - the security origin; path is deliberately not compared, the
        // token and device endpoints legitimately differ in path on one authorization server. Endpoint.parse
        // rejects a non-ASCII host, so this equalsIgnoreCase host compare only ever folds ASCII case - no
        // non-ASCII homoglyph can fold onto a pinned issuer host here.
        return a.isTls == b.isTls && a.port == b.port && a.host.equalsIgnoreCase(b.host);
    }

    private static String originOf(Endpoint endpoint) {
        return (endpoint.isTls ? "https://" : "http://") + endpoint.host + ':' + endpoint.port;
    }

    private static void parseBody(Response body, JsonLexer lexer, JsonParser parser, int timeoutMillis) throws JsonException {
        // read and parse the whole body, bounded by an elapsed-time deadline and a cumulative byte cap, so a
        // hostile or stalled server cannot wedge the thread by dribbling or endlessly streaming. nanoTime,
        // not the wall clock: an NTP step or an operator setting the date back must not stretch this bound,
        // which is the only thing standing between a dribbling identity provider and a wedged caller.
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
        // the path component only; Endpoint.parse rejects a url carrying a ?query or #fragment up front, so the
        // returned path never contains one. A ;matrix parameter, by contrast, stays part of the path, so a
        // traversal hidden in it (.../token;..%2f..) is still scanned by the issuer-path check.
        return Endpoint.parse(url).path;
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

    /**
     * Requires a well-formed 2xx status before a discovery body is trusted as configuration.
     * <p>
     * The status is validated to be exactly three bare digits BEFORE any of it is echoed: the header parser
     * copies the status-line token verbatim apart from SP/CR/LF, so a non-digit byte means a malformed or
     * hostile status line that must not splice ESC or other control bytes into a message, a log or a
     * terminal. A short all-digit status ({@code 2}, {@code 5}) is malformed too, and must not be read as a
     * 2xx class by its leading digit. Mirrors the check {@code readResponse} applies on the token path.
     * <p>
     * On rejection the body is drained within the usual bound so the keep-alive connection stays usable; a
     * body too large or too slow to drain leaves unconsumed bytes, so the connection is dropped instead of
     * mis-framing the next request's response.
     */
    private static void requireSuccessStatus(
            HttpClient client,
            HttpClient.ResponseHeaders response,
            Response body,
            String statusError
    ) {
        DirectUtf8Sequence statusCode = response.getStatusCode();
        StringSink status = new StringSink();
        boolean malformed = statusCode == null;
        if (!malformed) {
            CharSequence raw = statusCode.asAsciiCharSequence();
            for (int i = 0, n = raw.length(); i < n; i++) {
                char c = raw.charAt(i);
                if (c < '0' || c > '9') {
                    malformed = true;
                    break;
                }
                status.put(c);
            }
            malformed |= status.length() != 3;
        }
        if (malformed) {
            if (!discardBody(body, DEFAULT_HTTP_TIMEOUT_MILLIS)) {
                client.disconnect();
            }
            throw new OidcAuthException().put(statusError)
                    .put("; the response carried a malformed HTTP status code");
        }
        if (status.charAt(0) != '2') {
            if (!discardBody(body, DEFAULT_HTTP_TIMEOUT_MILLIS)) {
                client.disconnect();
            }
            // the status is proven to be bare digits, so echoing it cannot smuggle control bytes
            throw new OidcAuthException().put(statusError)
                    .put(" [httpStatus=").put(status).put(']');
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

    // package-private, not private: FileTokenStore needs the same treatment for the operator-supplied path
    // its IO errors embed, and a second copy of this walk in the same package would be the thing to avoid.
    static String sanitizeForDisplay(String value) {
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
        if (!isSameOrigin(tokenEndpoint, deviceAuthorizationEndpoint)) {
            throw new OidcAuthException()
                    .put("the OIDC token and device authorization endpoints are on different origins (")
                    .put(originOf(tokenEndpoint)).put(" vs ").put(originOf(deviceAuthorizationEndpoint))
                    .put("); refusing to send credentials. This indicates a misconfigured or tampered OIDC configuration");
        }
        if (issuer != null) {
            if (!isSameOrigin(tokenEndpoint, issuer)) {
                throw new OidcAuthException()
                        .put("the OIDC token endpoint origin (").put(originOf(tokenEndpoint))
                        .put(") does not match the issuer origin (").put(originOf(issuer))
                        .put("); refusing to send credentials to an endpoint outside the trusted issuer");
            }
            if (!isSameOrigin(deviceAuthorizationEndpoint, issuer)) {
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
        // in the message: they are the secret this class protects. (A blank/whitespace-only served token is
        // handled by storeTokens, which caches it as absent so it is never served, rather than rejected here -
        // an EMPTY served kind is the legitimate "the grant returned the other kind" case selectToken handles.)
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

    private void acquireForGetToken() {
        throwIfClosed();
        // Uncontended fast path: a plain CAS. It deliberately bypasses the interruptible timed tryLock in the
        // loop below, which throws InterruptedException the moment the calling thread merely carries a set
        // interrupt flag - even on a FREE lock - and then re-arms that flag, so every later getToken() on the
        // same thread would fail with a valid token sitting in the cache. An ILP producer on a pooled or
        // managed thread, where interrupt is the standard cancellation signal, is the common case. An
        // uncontended acquire cannot be behind an interactive sign-in (which holds the lock), so it is correct.
        if (lock.tryLock()) {
            return;
        }
        // Contended - a peer holds the lock. Never wait behind an interactive signIn(): it holds the lock for
        // the whole device-code lifetime (up to 30 min) with no token to serve until it completes, so fail fast
        // and let the caller retry. A peer holding the lock for a quick cached read or a silent refresh
        // (bounded, usually well under a second) is different - the HttpTokenProvider contract permits a brief
        // wait behind such a refresh - so poll for the lock in short slices rather than fail every concurrent
        // caller sharing this instance on each token refresh (the old unconditional tryLock() did exactly that).
        // Polling, not one blocking acquire, lets us still fail fast the moment an interactive sign-in - or
        // close() - begins while we wait. Bound the total wait so a stuck or pathologically slow holder degrades
        // to a retryable failure instead of stalling the flush path without bound - but size the bound to the
        // holder's OWN worst-case hold, not a single httpTimeoutMillis. A legitimate silent refresh under the
        // lock runs a send, an await and a body parse, each bounded by httpTimeoutMillis
        // (LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE x in total, the same figure the FileTokenStore lock-stale floor is
        // derived from), so a peer that waited only one httpTimeoutMillis would fail every concurrent caller
        // behind a refresh that is going to succeed.
        // nanoTime, not currentTimeMillis: this bound is an ELAPSED budget on the producer thread, and the
        // wall clock is adjustable. An NTP step or an operator setting the date back stretches a millis-based
        // deadline by the size of the jump, so the flush path this exists to protect would stall for however
        // long the clock moved rather than the documented multiple of httpTimeoutMillis. The body reads
        // (discardBody, parseBody) and the device-code poll already bound themselves this way. Compare by
        // DIFFERENCE rather than by ordering, so the arithmetic stays correct across nanoTime's wraparound.
        final long deadlineNanos = System.nanoTime()
                + (long) LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE * httpTimeoutMillis * 1_000_000L;
        while (true) {
            throwIfClosed();
            if (interactiveSignInInProgress) {
                throw new OidcAuthException("an interactive sign-in is in progress on another thread; no token is available without blocking - retry once it completes");
            }
            final long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new OidcAuthException("a token refresh is already in progress on another thread and no token became available in time; retry shortly");
            }
            try {
                if (lock.tryLock(Math.min(remainingNanos, GET_TOKEN_LOCK_POLL_SLICE_MILLIS * 1_000_000L), TimeUnit.NANOSECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new OidcAuthException("interrupted while waiting to acquire the OIDC token");
            }
        }
    }

    private boolean adopt(PersistedToken token) {
        if (token == null) {
            return false;
        }
        // The file is attacker-writable, so the served token - the one getToken() puts verbatim into an
        // Authorization header or a PG-wire password - is untrusted input. Two failure shapes look similar
        // here and must be told apart, because the safe answer to each is the opposite of the other.
        String servedToken = groupsInToken ? token.getIdToken() : token.getAccessToken();
        String fileRefreshToken = token.getRefreshToken();
        if (servedToken == null) {
            // ABSENT, which is a legitimate shape rather than evidence of anything. Under
            // groupsInToken=false a grant that returned only an id_token has storeTokens null the access
            // token, and persistIfRotated writes the entry regardless; FileTokenStore also maps an empty
            // on-disk value to null. Discarding such an entry throws away the refresh token, which is the
            // one thing persistence exists to preserve, and sends a human back through the device flow
            // where a single silent refresh would have done - a hard failure for a headless getToken()
            // consumer. So keep the refresh token and leave the cache empty and expired, which puts
            // getToken()/signIn() on the refresh path they already have for a null served kind. The refresh
            // token needs no character check of its own: tryRefresh url-encodes it into the form body
            // (appendParam -> urlEncode), unlike the served token, which reaches a header verbatim.
            if (fileRefreshToken == null) {
                return false; // nothing usable in this entry at all
            }
            if (token.getAccessToken() == null && token.getIdToken() == null) {
                // NEITHER kind present, only a refresh token. That is not the legitimate shape above - it is
                // positive evidence the entry was not written by this client, so reject the whole thing for
                // the same reason the tampered-served-token branch below does.
                //
                // No grant this client stores can produce it. The device path reaches storeTokens only behind
                // "accessToken.length() > 0 || idToken.length() > 0", the refresh path only behind a non-blank
                // served kind, and persistIfRotated runs solely at the tail of storeTokens - so every entry we
                // write carries at least one token kind. A file with a refresh token and nothing else came
                // from somewhere else.
                //
                // Left adopted, it is the cheapest credential swap there is: an attacker who can WRITE the
                // store directory - never needing to read our 0600 file - drops in a file whose fingerprint
                // fields are all derivable from public config, and the next silent refresh presents THEIR
                // refresh token. The client then ingests and queries as them, with no prompt, no error, and
                // nothing in any log recording that the identity changed. Unlike a directory-permission check
                // this holds on every filesystem, Windows included, where owner-only permissions cannot be
                // enforced at all.
                //
                // The cost when it fires on an honest file is one interactive sign-in. design/oidc-token-
                // persistence.md states the rule for cross-language writers.
                return false;
            }
            accessToken = null;
            idToken = null;
            refreshToken = fileRefreshToken;
            expiresAtMillis = 0;
            tokenTtlMillis = 0;
            lastPersistedRefreshToken = fileRefreshToken;
            return true;
        }
        if (Chars.isBlank(servedToken) || !hasOnlyTokenChars(servedToken) || Chars.equals("null", servedToken)) {
            // PRESENT but unusable: whitespace-only (which passes hasOnlyTokenChars vacuously, space being
            // 0x20, yet would be served as a blank "Bearer " header the server only answers with 401),
            // carrying a control or non-ASCII character, or the four characters "null". Unlike an absent
            // token this is positive evidence that something else wrote this file, so reject the WHOLE entry
            // - refresh token included. Adopting the refresh token of a file we know was tampered with would
            // let an attacker who can write the store swap in their own, and this client would silently sign
            // in as them.
            //
            // On "null" specifically: JsonLexer reports a bare JSON null and a quoted "null" identically, so
            // design/oidc-token-persistence.md forbids a writer from emitting a bare null at all and requires
            // an absent value to be OMITTED. A served token that reads as "null" is therefore either a writer
            // violating that rule - json.dumps({"access_token": None}) is the natural way to get there from
            // Python, and cross-language sharing is the whole point of freezing the format - or a token
            // pathological enough to be indistinguishable from one. Neither may become "Bearer null": the
            // server answers that with 401, and because the persisted expiry is honoured getToken() would go
            // on serving it rather than refreshing, so the producer 401s with nothing naming the cause until
            // the clamped expiry lapses. Refusing costs one interactive sign-in, and only to a caller whose
            // real bearer token is four characters long.
            //
            // Checked HERE rather than in FileTokenStore because adopt() is the choke point every TokenStore
            // goes through, including a caller's own implementation of the SPI. The refresh token needs no
            // equivalent arm: it is url-encoded into a form body rather than spliced into a header, so a
            // "null" there is simply rejected by the token endpoint and degrades to an interactive sign-in.
            return false;
        }
        accessToken = token.getAccessToken();
        idToken = token.getIdToken();
        // keep the current refresh token when the file carries none, mirroring the REFRESH branch of
        // storeTokens() -- a stored entry is the same authorization read back, never a new one, so the
        // grant-specific clearing that a fresh device grant does has no counterpart here. A file with a
        // valid served token but no refresh_token - a cross-language peer that never received one, or a
        // tampered file - must not null a live in-memory refresh token: doing so would make a later
        // tryRefresh() urlEncode(null) and throw an uncaught NPE (aborting the sign-in) instead of refreshing
        // with the token we still hold or degrading to an interactive sign-in.
        if (fileRefreshToken != null) {
            refreshToken = fileRefreshToken;
        }
        // the file is attacker-writable (and may have been written under a skewed clock), so bound how long
        // the loaded token is trusted exactly as storeTokens() bounds a token from the wire: never past
        // MAX_EXPIRES_IN_SECONDS from now. Clamp the expiry to [0, now + maxLife]: the ceiling stops a tampered
        // far-future expiry from being trusted for decades, and the floor of 0 keeps a tampered far-past expiry
        // in the past (1970, well before now) while keeping the validity check (now < expiresAtMillis - skew)
        // underflow-safe - a near-Long.MIN_VALUE expiry would otherwise wrap that subtraction to a huge
        // positive and serve a garbage-expiry token as valid forever. An already-expired entry still reads as
        // expired and falls through to a refresh rather than being served.
        long maxTokenLifeMillis = MAX_EXPIRES_IN_SECONDS * 1000L;
        long now = System.currentTimeMillis();
        expiresAtMillis = Math.max(0L, Math.min(token.getExpiresAtMillis(), now + maxTokenLifeMillis));
        // Trust the file's stored ISSUED lifetime (bounded to [0, maxLife] against a tampered value), NOT the
        // remaining span expiresAtMillis - now. effectiveSkewMillis() caps the clock-skew margin at half the
        // lifetime - a guard meant only for a genuinely short-issued (< 60s) token - so deriving it from the
        // shrinking remaining span would collapse the 30s skew toward zero as a normal token nears expiry and
        // let getToken() serve a near-expired token on the flush path instead of refreshing. A tampered ttl can
        // only shrink the skew (never inflate it past CLOCK_SKEW_MILLIS), exactly as the remaining-span form
        // could, so trusting the stored value is no less safe; a file that carries no ttl (0) yields the full
        // skew via effectiveSkewMillis()'s <= 0 branch. storeTokens() stores this same full issued lifetime, so
        // both paths now give tokenTtlMillis one meaning.
        tokenTtlMillis = Math.max(0L, Math.min(token.getTokenTtlMillis(), maxTokenLifeMillis));
        // track what the file actually carried (which is null when it had no refresh_token but we kept a live
        // one above), so a later non-rotating refresh does not rewrite an unchanged on-disk token, yet a token
        // we kept that the file did not carry is not mistaken for already-persisted and can be re-saved
        lastPersistedRefreshToken = fileRefreshToken;
        return true;
    }

    /**
     * Adopts a rotated {@code refresh_token} from a refresh response that did NOT carry the served token
     * kind, so the rotation is not lost with the rest of the response.
     * <p>
     * A refresh response may legally omit the served kind - RFC 6749 6 makes {@code id_token} optional, and
     * OIDC Core 12.2 says the refresh response is the token response "except that it might not contain an
     * id_token" - which is exactly the shape a {@code groupsInToken} client meets against a provider that
     * only mints an id token at authorization time. That response is still a clean 2xx, and the
     * {@code refresh_token} in it is authoritative: a rotating provider has already invalidated the one we
     * presented. Keeping the old token would replay a spent credential on every later refresh, which a
     * reuse-detecting provider answers by revoking the whole token family - so the caller loses the
     * credential entirely rather than merely failing to refresh it once.
     * <p>
     * Only the refresh token is taken. The served kind did not arrive, so the cached tokens and the expiry
     * stay as they were: the entry reads as expired, {@code tryRefresh()} still reports failure, and the
     * caller falls back to the interactive flow exactly as before - now holding a refresh token that is
     * still live, so the NEXT refresh can succeed on its own.
     */
    private void adoptRotatedRefreshToken() {
        if (tokenParser.refreshToken.length() == 0) {
            return;
        }
        refreshToken = tokenParser.refreshToken.toString();
        // Persist it, for the same reason it is adopted at all. Without this the on-disk entry keeps the
        // token the provider just burned, so the next process start adopts a dead credential and re-prompts
        // a human who did not need to be asked. persistIfRotated() writes the snapshot's stale served token
        // and past expiry alongside it, which adopt() reads back as expired and refreshes - one silent
        // round trip, against a refresh token that works.
        persistIfRotated();
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
                tlsClient = HttpClientFactory.newTlsInstance(clientConfig, tlsConfig);
            }
            return tlsClient;
        }
        if (plainClient == null) {
            plainClient = HttpClientFactory.newPlainTextInstance(clientConfig);
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

    private boolean isRefreshBackedOff() {
        if (refreshFailedAtMillis == 0) {
            return false;
        }
        // elapsed == 0 is the COMMON case, not an edge one: a producer retrying rows calls getToken() many
        // times within the same millisecond, and that is exactly the flood this exists to stop - so zero
        // counts as backed off. Only a NEGATIVE span, which means the clock jumped backwards, releases the
        // latch early rather than pinning it until the clock catches up.
        final long elapsed = System.currentTimeMillis() - refreshFailedAtMillis;
        return elapsed >= 0 && elapsed < MIN_REFRESH_RETRY_INTERVAL_MILLIS;
    }

    private boolean isStoreLoadBackedOff() {
        final long remaining = nextStoreLoadAttemptMillis - System.currentTimeMillis();
        // Unlike isRefreshBackedOff(), a zero remaining span does NOT count as backed off: the first failure
        // arms a zero-length back-off on purpose, so a same-millisecond retry still re-reads the store.
        // A span longer than the cap cannot have been armed here, so it means the clock jumped BACKWARDS -
        // release the latch rather than pin the store unreadable until the clock catches up.
        return remaining > 0 && remaining <= MAX_STORE_LOAD_RETRY_INTERVAL_MILLIS;
    }

    private void maybeLoadFromStore() {
        if (tokenStore == null || storeLoadAttempted || isStoreLoadBackedOff()) {
            return;
        }
        PersistedToken token;
        try {
            token = tokenStore.load(storeKey);
        } catch (RuntimeException e) {
            // Best-effort: a store read failure must not break sign-in. Leave storeLoadAttempted UNSET so a
            // transient failure is retried on a later call. Latching it here instead would make one failed
            // read disable persistence for the whole life of this instance - so a process that owns a
            // perfectly good refresh token on disk would re-run the interactive device flow, which for a
            // headless getToken() consumer is a hard failure rather than a degraded one.
            //
            // Retried, but not on EVERY call: this runs on the getToken() path ahead of the cache check, so
            // without a back-off a store that never becomes readable costs a blocking file open, two stack
            // trace fills and a WARN line per ILP flush, forever, on the producer thread and under the lock.
            // The first failure arms a zero-length back-off (an immediate retry, for the one-shot faults
            // above), then each consecutive failure doubles it up to MAX_STORE_LOAD_RETRY_INTERVAL_MILLIS.
            final long backOffMillis = storeLoadRetryIntervalMillis;
            storeLoadRetryIntervalMillis = backOffMillis == 0
                    ? MIN_STORE_LOAD_RETRY_INTERVAL_MILLIS
                    : Math.min(backOffMillis * 2, MAX_STORE_LOAD_RETRY_INTERVAL_MILLIS);
            nextStoreLoadAttemptMillis = System.currentTimeMillis() + backOffMillis;
            warnPersistence("load", e);
            return;
        }
        // the read COMPLETED, so its answer is definitive: a missing, corrupt or foreign-identity file yields
        // null without throwing, and re-reading it on every later call would buy nothing
        storeLoadAttempted = true;
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
                storeTokens(tokenParser, false);
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
            // a transport failure, or a bounded-read abort in parseBody (its elapsed-time deadline or the
            // MAX_RESPONSE_BODY_BYTES cap), leaves the response half-read with unconsumed bytes in this
            // cached keep-alive connection. Drop it so the next poll or refresh reconnects with a clean
            // socket instead of parsing the previous response's leftovers - which pollForToken would
            // otherwise keep doing, on a corrupted connection, until the device code expires. Mirrors the
            // disconnect-on-failure handling in AbstractLineHttpSender.flush0.
            client.disconnect();
            throw e;
        } catch (HttpException e) {
            // The RESPONSE HEAD was malformed or oversized, so HttpHeaderParser rejected it: a header block
            // past the fixed 4096-byte parse buffer (a WAF or proxy stacking Set-Cookie/CSP), a malformed
            // Content-Length, or a status line that is not HTTP/1.x. HttpException is a SIBLING of
            // HttpClientException, not a subclass, so it missed the catch above - and with it the disconnect,
            // leaving this CACHED keep-alive connection holding a half-read response for the next poll to
            // parse as its own, exactly the corruption that catch exists to prevent. It also missed every
            // classification downstream, aborting an interactive sign-in outright on a condition the same
            // code rides out when it arrives as a transport error, and surfacing a type fromQuestDB/signIn
            // do not document. The identity provider is untrusted here, so this is a response shape it can
            // choose at will.
            //
            // Both are "the response is unusable", so answer identically: drop the connection and re-report
            // as the transport-class failure every caller already handles. The message is a parser constant,
            // never response bytes, so it carries no untrusted text. Copying it out also detaches the
            // thread-local flyweight HttpException.instance() hands back, whose message the next
            // HttpException on this thread would overwrite.
            client.disconnect();
            throw new HttpClientException("malformed response from the identity provider: " + e.getMessage());
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
        // Only re-read when the in-memory refresh token still matches what we last persisted. A mismatch no
        // longer strictly means "in-memory is a newer unsaved token": it covers two cases, and re-adopting
        // would regress in both, so keep the in-memory token and refresh with it. (1) A previous save failed
        // (persistence is best-effort), so the in-memory token is genuinely newer than the on-disk one;
        // re-adopting would regress it to the stale - and, on a rotating identity provider, already-revoked -
        // on-disk token and force a needless re-prompt. (2) adopt() kept a live in-memory token that the loaded
        // file did not carry (a cross-language peer that never received a refresh_token), leaving
        // lastPersistedRefreshToken null; here the trade-off is that if a rotating-IdP peer has since revoked
        // our token and written a fresher one, we skip that fresher on-disk token this round and fall back to an
        // interactive re-prompt. Both are benign (never a stale/wrong served token; the pre-fix alternative in
        // case 2 was an uncaught urlEncode(null) NPE) and cross-process-only.
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
        // Sleep in short slices so close() can abort an in-flight sign-in within ~POLL_SLEEP_SLICE_MILLIS
        // instead of after a full (possibly slow_down-inflated) interval.
        //
        // Thread.sleep, not Os.sleep: Os.sleep catches InterruptedException, recomputes its deadline and
        // keeps sleeping, and Thread.sleep CLEARS the flag when it throws - so the caller's interrupt was
        // not merely ignored here, it was destroyed. A caller who cancelled then returned from signIn() with
        // Thread.interrupted() reading false, its own cancellation bookkeeping none the wiser, having waited
        // out a poll loop that runs to the device-code lifetime. This class states the opposite invariant
        // twice ("the flag is the caller's cancellation signal and must survive this call"), and getToken()
        // and FileTokenStore.load()/save() honour it.
        long remaining = millis;
        while (remaining > 0) {
            throwIfClosed();
            throwIfInterrupted("the calling thread was interrupted while waiting for authorization");
            long slice = Math.min(POLL_SLEEP_SLICE_MILLIS, remaining);
            try {
                Thread.sleep(slice);
            } catch (InterruptedException e) {
                // Thread.sleep cleared the flag; put it back and let the caller see the cancellation both
                // ways - as the exception below and as the flag their own shutdown path is waiting on.
                Thread.currentThread().interrupt();
                throwIfInterrupted("the calling thread was interrupted while waiting for authorization");
            }
            remaining -= slice;
        }
    }

    private PersistedToken snapshot() {
        return new PersistedToken(accessToken, idToken, refreshToken, expiresAtMillis, tokenTtlMillis);
    }

    /**
     * @param isRefreshGrant true for a refresh_token grant, false for a fresh device grant. Decides what an
     *                       OMITTED refresh_token means, which is not the same question for the two grants.
     */
    private void storeTokens(TokenResponseParser parser, boolean isRefreshGrant) {
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
        // treat a blank (empty OR whitespace-only) token as absent (null), not as a usable credential: a
        // whitespace-only served token passes the char check vacuously (space is 0x20) but would be served as
        // a blank "Bearer " header the server only answers with 401, so cache it as null and let selectToken /
        // the wrong-token-kind fallback handle a missing served kind rather than serve it. An empty string was
        // already treated as absent here; this only additionally folds in whitespace-only, matching adopt() and
        // the sender's own HttpTokenProvider.validateToken (Chars.isBlank).
        accessToken = Chars.isBlank(parser.accessToken) ? null : parser.accessToken.toString();
        idToken = Chars.isBlank(parser.idToken) ? null : parser.idToken.toString();
        // What an omitted refresh_token means depends on the grant, so the two must not share a policy.
        // A refresh response usually omits one (RFC 6749 6 makes it optional) and is the SAME authorization
        // continuing, so the current token stays valid and is kept -- dropping it would send a human back
        // through the device flow every time a non-rotating provider answers.
        // A device grant is a NEW authorization and may be a DIFFERENT human. Keeping the previous user's
        // refresh token across it is cross-account confusion: user A's refresh fails, user B completes the
        // device flow without a refresh token, B's access token expires, and the next silent refresh presents
        // A's retained token and resumes as A -- no prompt, no error, nothing in any log to say the identity
        // changed. So an omission here clears it: this authorization has no refresh token, and the honest
        // outcome is that getToken() asks for an interactive sign-in.
        if (parser.refreshToken.length() > 0) {
            refreshToken = parser.refreshToken.toString();
        } else if (!isRefreshGrant) {
            refreshToken = null;
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

    /**
     * Abandons the current step when the calling thread carries an interrupt, LEAVING THE FLAG SET.
     * <p>
     * isInterrupted(), never interrupted(): the flag is the caller's cancellation signal and has to outlive
     * this call, so the shutdown path that raised it - an ExecutorService.shutdownNow(), a Future.cancel,
     * QWP's ConnectCancellation - still sees it. Clearing it here would leave the caller believing it was
     * never cancelled, which is the failure this guard exists to stop rather than one more of its causes.
     *
     * @param message what the caller was doing when the cancellation was noticed
     */
    private void throwIfInterrupted(String message) {
        if (Thread.currentThread().isInterrupted()) {
            throw new OidcAuthException(message);
        }
    }

    private boolean tryRefresh() {
        if (refreshToken == null) {
            // nothing to present: degrade to the interactive flow rather than urlEncode(null) and throw.
            // adopt() keeps a live refresh token, so this only fires if a caller reaches here with none.
            return false;
        }
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
        // id token when groups are encoded in it, the access token otherwise). A refresh that omits the served
        // kind - which RFC 6749 permits and many providers do - or returns it blank/whitespace-only, or carries
        // an error or a non-2xx status, must fall back to the interactive flow rather than be cached. Test the
        // served kind with Chars.isBlank, the SAME contract storeTokens/adopt use to fold a blank token to null:
        // gating on length() > 0 here would pass a whitespace-only token, which storeTokens then nulls, so
        // tryRefresh would report success while selectToken() throws "no token" instead of falling back.
        //
        // Split the "this response is a clean grant" half out: it is what decides whether a rotated
        // refresh_token in the SAME body is authoritative, and that question outlives the served-kind test
        // below. See adoptRotatedRefreshToken().
        final boolean isCleanGrant = isHttpStatusSuccess() && tokenParser.error.length() == 0;
        boolean hasRequiredToken = (groupsInToken
                ? !Chars.isBlank(tokenParser.idToken)
                : !Chars.isBlank(tokenParser.accessToken))
                && isCleanGrant;
        if (hasRequiredToken) {
            try {
                storeTokens(tokenParser, true);
            } catch (OidcAuthException e) {
                // storeTokens -> validateTokenChars rejects a refreshed served token carrying a control or
                // non-ASCII char (reachable now that JsonLexer decodes an escaped \r/\n in the response into a
                // real byte). Fall back to the interactive flow like the transport/parse-failure arms above,
                // rather than let the rejection propagate out of getToken()/signIn() past the runDeviceFlow()
                // fallback the caller expects. validateTokenChars runs before any state mutation, so the cached
                // token and refresh token are left intact for that fallback.
                return false;
            }
            return true;
        }
        if (isCleanGrant) {
            // The provider accepted our refresh token and answered 2xx; it simply did not return the kind
            // getToken() serves. Take the rotated refresh_token before dropping the rest of the response -
            // see adoptRotatedRefreshToken() for why keeping the old one is worse than failing this refresh.
            adoptRotatedRefreshToken();
        }
        // the refresh token expired or was revoked, or did not return the token we need; fall back to the
        // interactive flow
        return false;
    }

    private boolean tryRefreshCoordinated() {
        if (tokenStore == null) {
            return tryRefresh();
        }
        // Serialise the read-refresh-write across processes (and adopt a peer's just-rotated refresh token)
        // through the store's per-identity lock; a store that does not coordinate just runs the refresh.
        //
        // TokenStore is a user-implemented SPI and persistence is documented best-effort, so a store that
        // throws must not take the sign-in down with it - it did, because inLock was called bare. What the
        // right degrade is depends entirely on whether the refresh already ran, which only the action
        // itself can report:
        //   - the store threw BEFORE the action ran: nothing was refreshed, so run ONE uncoordinated
        //     refresh. Exactly one: the point of the lock is that a rotating refresh token must not be
        //     POSTed twice, and a reuse-detecting provider answers a replay by revoking the whole family.
        //   - the store threw AFTER the action completed (releasing a lock, closing a handle): the refresh
        //     HAPPENED and the token is live. Report what the action returned; re-running it would be that
        //     same double-POST, and throwing would tell the caller a completed sign-in failed.
        //   - the action itself threw: that is the refresh's own failure, not the store's. Never swallow
        //     it and never replay it - let it propagate exactly as it did before.
        // Error is deliberately not caught: an OutOfMemoryError is not a store fault to degrade around.
        final boolean[] actionEntered = new boolean[1];
        final boolean[] actionCompleted = new boolean[1];
        final boolean[] actionResult = new boolean[1];
        try {
            return tokenStore.inLock(storeKey, () -> {
                actionEntered[0] = true;
                boolean refreshed = refreshUnderLock();
                actionResult[0] = refreshed;
                actionCompleted[0] = true;
                return refreshed;
            });
        } catch (RuntimeException e) {
            if (actionCompleted[0]) {
                warnPersistence("lock release", e);
                return actionResult[0];
            }
            if (actionEntered[0]) {
                throw e;
            }
            warnPersistence("lock", e);
            return tryRefresh();
        }
    }

    private void wipeCredentialState() {
        // Best effort, and worth being precise about what that means.
        //
        // Nulling the four String fields is all Java offers for a String - the characters live on until the
        // GC reclaims them - but it does stop this instance from handing them back.
        //
        // The sinks are the part a plain null misses. formSink carries the request body, which on the refresh
        // path is literally "refresh_token=<the token>"; the two parsers hold every field of the last
        // response, tokens and device code included. All of them are REUSED, and clear() only rewinds the
        // write position, so a long secret followed by a short write stays legible in the tail. wipe()
        // overwrites the whole backing array instead.
        //
        // What it cannot reach: any String already handed to a caller, and the HTTP client's native receive
        // buffers, where the raw token bytes also passed. Freeing those returns the pages to the allocator
        // without zeroing them. A caller who needs more than this should not be persisting tokens in this
        // process at all.
        accessToken = null;
        idToken = null;
        refreshToken = null;
        lastPersistedRefreshToken = null;
        formSink.wipe();
        responseStatus.wipe();
        deviceAuthParser.wipe();
        tokenParser.wipe();
    }

    private void warnPersistence(String operation, Throwable cause) {
        // best-effort persistence: warn through SLF4J and carry on with the in-memory token. The store never
        // puts token bytes in its messages, but an IO error can carry the operator-supplied store path, which
        // could itself hold terminal-spoofing characters - sanitize the detail before printing, as every other
        // untrusted display string is sanitized (sanitizeForDisplay is null-safe).
        String detail = sanitizeForDisplay(cause.getMessage());
        LOG.warn("OIDC token store {} failed; continuing without persistence{}",
                operation, detail != null ? " [" + detail + ']' : "");
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
            // a FileTokenStore steals a lock older than its staleness window, presuming a crashed holder; that
            // window must exceed the worst-case time a live refresh holds the lock, or a peer could steal a live
            // holder's lock mid-refresh and reopen the rotating-refresh-token race the lock prevents. Enforce the
            // bounded part of that worst case here, where both values are known: the refresh I/O under the lock is
            // up to LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE x httpTimeoutMillis. httpConfig() bounds the connection phase
            // that precedes the send by httpTimeoutMillis too (the TCP connect and the TLS handshake; DNS
            // resolution remains the OS's), so this floor covers the hold rather than only part of it. The default
            // 600s window leaves ample headroom over the floor even at the 120s timeout cap; a caller raising
            // httpTimeoutMillis should raise lockStaleMillis to keep it. A non-coordinating TokenStore is exempt -
            // it takes no lock.
            if (tokenStore instanceof FileTokenStore) {
                long minStaleMillis = (long) LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE * httpTimeoutMillis;
                long staleMillis = ((FileTokenStore) tokenStore).getLockStaleMillis();
                if (staleMillis < minStaleMillis) {
                    throw new OidcAuthException()
                            .put("the FileTokenStore lockStaleMillis (").put(staleMillis)
                            .put(") must be at least ").put(LOCK_HOLD_HTTP_TIMEOUT_MULTIPLE)
                            .put("x httpTimeoutMillis (").put(minStaleMillis)
                            .put("), otherwise a slow refresh's live cross-process lock could be stolen by a peer mid-refresh");
                }
            }
            return new OidcDeviceAuth(this, tls, deviceEndpoint, parsedTokenEndpoint);
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
        // objects nested inside a JSON array are never trusted: an array-wrapped response must not surface its
        // element object's fields at the top-level depth. arrayDepth gates every name/value read on being 0.
        private int arrayDepth;
        private int depth;
        private int field = FIELD_NONE;

        void wipe() {
            // clear() rewinds; this overwrites. The device code is a credential until it expires, and the
            // verification URIs carry the user code, so none of it should outlive the instance that read it.
            deviceCode.wipe();
            error.wipe();
            errorDescription.wipe();
            userCode.wipe();
            verificationUri.wipe();
            verificationUriComplete.wipe();
            clear();
        }

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
            arrayDepth = 0;
            depth = 0;
            field = FIELD_NONE;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_ARRAY_START:
                    arrayDepth++;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    arrayDepth--;
                    break;
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    if (arrayDepth == 0 && depth == 1) {
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
                    if (arrayDepth == 0 && depth == 1) {
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
            // reject a fragment (#...): it has no meaning in an endpoint url (RFC 3986 fragments are client-side
            // only, never sent to a server), and folding it into the path opens a pin-bypass - pathOnly() strips
            // it before the issuer-path check while postForm sends endpoint.path verbatim on the wire, so a
            // lenient server that normalizes a '..' hidden past the '#' (POST /realms/acme#/../other/token) could
            // resolve the request-target to a path the issuer-path pin never validated. Fail closed instead.
            if (url.indexOf('#') >= 0) {
                throw new OidcAuthException().put("invalid url, a fragment (#) is not supported [url=").put(url).put(']');
            }
            // reject a query (?...) for the same pin-bypass reason as the fragment above: pathOnly() strips it
            // before the issuer-path check, yet postForm sends endpoint.path - query included - verbatim on the
            // wire, so a tampered /settings could advertise an endpoint carrying a query the issuer-path pin
            // never validated (and a lenient server could even normalize a '..' hidden past the '?'). An OIDC
            // device/token endpoint carries its parameters in the request body (application/x-www-form-urlencoded),
            // never the url query - RFC 6749 3.2 permits a query component but no real provider uses one here - so
            // fail closed. The user-facing verification url, which legitimately carries the user code as a query,
            // is parsed by BrowserLauncher (java.net.URI), not this method, so it is unaffected.
            if (url.indexOf('?') >= 0) {
                throw new OidcAuthException().put("invalid url, a query (?) is not supported [url=").put(url).put(']');
            }
            int schemeEnd = url.indexOf("://");
            if (schemeEnd < 0) {
                throw new OidcAuthException().put("invalid url, expected a scheme [url=").put(url).put(']');
            }
            boolean isTls;
            // lower-case the scheme before matching: RFC 3986 schemes are case-insensitive, so HTTPS/Http are
            // valid. toLowerCase(Locale.ROOT) folds only ASCII case, so a homoglyph scheme (a long-s for the s,
            // say) does NOT fold onto http/https and still falls through to the reject below.
            String scheme = url.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
            if ("https".equals(scheme)) {
                isTls = true;
            } else if ("http".equals(scheme)) {
                isTls = false;
            } else {
                throw new OidcAuthException().put("invalid url, expected http or https [url=").put(url).put(']');
            }
            int hostStart = schemeEnd + 3;
            // the authority ([userinfo@]host[:port]) ends at the first '/', or at the end of the url for a
            // path-less endpoint. A ?query or #fragment was already rejected above, so neither can fold into the
            // host or the path here (this used to also split on '?'/'#' to guard that, now handled up front).
            int authorityEnd = url.length();
            for (int i = hostStart, n = url.length(); i < n; i++) {
                if (url.charAt(i) == '/') {
                    authorityEnd = i;
                    break;
                }
            }
            String hostPort = url.substring(hostStart, authorityEnd);
            // a path-less url uses '/'; otherwise the authority is '/'-terminated and the path starts at that
            // slash, which already carries its own leading slash
            String path = authorityEnd == url.length() ? "/" : url.substring(authorityEnd);
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
                String portStr = hostPort.substring(colon + 1);
                // reject a leading '+': Integer.parseInt would read ":+443" as 443 and slip the range check,
                // but a real authority port is bare digits. A leading '-' or any non-digit still flows to
                // parseInt below, which rejects it (a negative fails the 1..65535 range check, a non-number
                // throws NumberFormatException) - so only the '+' that parseInt silently accepts is caught here
                if (portStr.isEmpty() || portStr.charAt(0) == '+') {
                    throw new OidcAuthException().put("invalid url, could not parse the port [url=").put(url).put(']');
                }
                try {
                    port = Integer.parseInt(portStr);
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
            // reject a non-ASCII host. The HTTP layer hands the host to the OS resolver as raw UTF-8 with no
            // IDNA, so a non-ASCII name would not resolve anyway; and a non-ASCII code point makes the origin-pin
            // host compare (isSameOrigin -> String.equalsIgnoreCase) unsafe, because equalsIgnoreCase folds
            // several non-ASCII letters (U+0130, U+0131, U+017F, U+212A, ...) onto ASCII - so a homoglyph host
            // advertised by a tampered /settings could otherwise pass the pin against the issuer. LDH ASCII
            // hosts, punycode (xn--...) and dotted IPv4 are all ASCII and unaffected.
            for (int i = 0, n = host.length(); i < n; i++) {
                char hc = host.charAt(i);
                if (hc > 0x7f) {
                    throw new OidcAuthException().put("invalid url, the host contains a non-ASCII character [url=").put(url).put(']');
                }
                // reject a backslash in the host: the WHATWG URL spec folds '\' to '/', so a host like
                // good.com\.evil.com could be re-split by a lenient consumer into a different authority. The OS
                // resolver this client hands the host to never resolves such a name anyway, so fail closed.
                if (hc == '\\') {
                    throw new OidcAuthException().put("invalid url, the host contains a backslash [url=").put(url).put(']');
                }
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
        // objects nested inside a JSON array are never trusted config: track array depth and require it 0 for
        // every name/value/config-arming decision, so a tampered {"config":[{...}]} (or a top-level array
        // wrapper) cannot surface the array element's object at the config depth. Array VALUES are ignored
        // regardless; legitimate array-valued config keys (never read here) are harmlessly skipped.
        private int arrayDepth;
        private int depth;
        private int field = FIELD_NONE;
        private boolean isConfigNext;
        private boolean isInConfig;

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_ARRAY_START:
                    arrayDepth++;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    arrayDepth--;
                    break;
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    if (arrayDepth == 0 && depth == 2 && isConfigNext) {
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
                    if (arrayDepth == 0 && depth == 1) {
                        // only the top-level "config" object is trusted; the sibling "preferences" object
                        // holds arbitrary user-written keys and must not feed OIDC discovery
                        isConfigNext = Chars.equals("config", tag);
                        field = FIELD_NONE;
                    } else if (arrayDepth == 0 && depth == 2 && isInConfig) {
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
                    if (arrayDepth == 0 && depth == 2 && isInConfig) {
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
        // objects nested inside a JSON array are never trusted: an array-wrapped response must not surface its
        // element object's fields at the top-level depth. arrayDepth gates every name/value read on being 0.
        private int arrayDepth;
        private int depth;
        private int field = FIELD_NONE;

        void wipe() {
            // clear() rewinds; this overwrites. These five sinks hold the raw grant: the access token, the id
            // token and the refresh token, exactly as the identity provider sent them.
            accessToken.wipe();
            error.wipe();
            errorDescription.wipe();
            idToken.wipe();
            refreshToken.wipe();
            clear();
        }

        @Override
        public void clear() {
            accessToken.clear();
            error.clear();
            errorDescription.clear();
            idToken.clear();
            refreshToken.clear();
            expiresIn = 0;
            arrayDepth = 0;
            depth = 0;
            field = FIELD_NONE;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_ARRAY_START:
                    arrayDepth++;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    arrayDepth--;
                    break;
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    if (arrayDepth == 0 && depth == 1) {
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
                    if (arrayDepth == 0 && depth == 1) {
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
        // objects nested inside a JSON array are never trusted: an array-wrapped document must not surface its
        // element object's fields at the top-level depth. arrayDepth gates every name/value read on being 0.
        private int arrayDepth;
        private int depth;
        private int field = FIELD_NONE;

        @Override
        public void onEvent(int code, CharSequence tag, int position) {
            switch (code) {
                case JsonLexer.EVT_ARRAY_START:
                    arrayDepth++;
                    break;
                case JsonLexer.EVT_ARRAY_END:
                    arrayDepth--;
                    break;
                case JsonLexer.EVT_OBJ_START:
                    depth++;
                    break;
                case JsonLexer.EVT_OBJ_END:
                    depth--;
                    break;
                case JsonLexer.EVT_NAME:
                    // the OIDC discovery document is a flat top-level object; only read top-level keys so a
                    // nested value cannot be mistaken for an endpoint
                    if (arrayDepth == 0 && depth == 1) {
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
                    if (arrayDepth == 0 && depth == 1) {
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
