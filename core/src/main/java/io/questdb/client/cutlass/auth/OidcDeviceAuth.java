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

/**
 * Obtains an OIDC access or id token using the OAuth 2.0 Device Authorization Grant
 * (RFC 8628), so a process with no local browser (a remote notebook kernel, a container,
 * a headless job) can still sign a human in. The user authorizes on any device, while the
 * token request travels outbound only.
 * <p>
 * The resulting token can be presented to QuestDB Enterprise over any of the auth paths
 * the server already validates:
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
 * {@link #getToken()} returns a cached token while it is still valid, silently refreshes it
 * when a refresh token is available, and otherwise re-runs the interactive flow. The method
 * is synchronized, so concurrent callers never start two sign-ins at once; the trade-off is
 * that a sign-in waiting for the user holds the instance lock for the lifetime of the device
 * code (up to an hour), and any other {@link #getToken()} or {@link #clearCache()} call on the
 * same instance blocks behind it. To abort a sign-in that is waiting, call {@link #close()}
 * from another thread: it cancels the in-flight flow, which then fails promptly with an
 * {@link OidcAuthException} rather than running to the device-code timeout.
 * <p>
 * Instances are interactive by design and hold a network connection; close them when done.
 * Token state lives in memory only and does not survive a restart of the process.
 */
public class OidcDeviceAuth implements QuietCloseable {
    public static final String DEFAULT_SCOPE = "openid";
    static final String GRANT_TYPE_DEVICE_CODE = "urn:ietf:params:oauth:grant-type:device_code";
    static final String GRANT_TYPE_REFRESH_TOKEN = "refresh_token";
    private static final int DEFAULT_CLOCK_SKEW_SECONDS = 30;
    // how long the device code stays valid for the interactive sign-in when the identity provider's
    // device authorization response omits expires_in
    private static final int DEFAULT_DEVICE_CODE_TTL_SECONDS = 300;
    private static final int DEFAULT_HTTP_TIMEOUT_MILLIS = 30_000;
    private static final int DEFAULT_POLL_INTERVAL_SECONDS = 5;
    // how long a token is cached before getToken() refreshes it, when the token response omits expires_in
    private static final int DEFAULT_TOKEN_TTL_SECONDS = 300;
    private static final String ERROR_AUTHORIZATION_PENDING = "authorization_pending";
    private static final String ERROR_SLOW_DOWN = "slow_down";
    private static final HttpClientConfiguration HTTP_CONFIG = DefaultHttpClientConfiguration.INSTANCE;
    // Token responses carry JWTs - an id token with group claims can be several KB - and a single
    // value may arrive split across HTTP response fragments. The JSON lexer stashes a split value
    // and rejects it once it grows past JSON_LEXER_MAX_VALUE_BYTES, so the limit must comfortably
    // exceed any real token, otherwise large tokens fail to parse with "String is too long".
    private static final int JSON_LEXER_CACHE_SIZE = 1024;
    private static final int JSON_LEXER_MAX_VALUE_BYTES = 1 << 20;
    // a persistent transport failure while polling aborts after this many consecutive attempts,
    // instead of silently retrying until the device code expires
    private static final int MAX_CONSECUTIVE_POLL_ERRORS = 3;
    // upper bounds on the expires_in / interval the identity provider reports, so an absurd or
    // hostile value cannot overflow the poll timing arithmetic or make the client wait absurdly long
    private static final int MAX_EXPIRES_IN_SECONDS = 3600;
    private static final int MAX_POLL_INTERVAL_SECONDS = 300;
    // cap the bytes drained from a single response so a hostile or MITM'd server cannot stream an endless
    // body and wedge the thread; set far above any real OIDC JSON response
    private static final int MAX_RESPONSE_BODY_BYTES = 4 * 1024 * 1024;
    private static final int POLL_PENDING = 1;
    private static final long POLL_SLEEP_SLICE_MILLIS = 100;
    private static final int POLL_SLOW_DOWN = 2;
    private static final int POLL_SUCCESS = 0;
    private static final int POLL_TRANSIENT_ERROR = 3;
    private static final int SLOW_DOWN_INCREMENT_SECONDS = 5;
    private static final String USER_AGENT = "questdb/java-client-oidc";
    private final String audience;
    private final String clientId;
    private final long clockSkewMillis;
    private final DeviceAuthorizationResponseParser deviceAuthParser = new DeviceAuthorizationResponseParser();
    private final Endpoint deviceAuthorizationEndpoint;
    private final StringSink formSink = new StringSink();
    private final boolean groupsInToken;
    private final int httpTimeoutMillis;
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
        // allocate the native JSON lexer last: an Endpoint.parse above can throw on a malformed url,
        // and the half-built instance is never returned, so close() could not free an earlier alloc
        this.jsonLexer = new JsonLexer(JSON_LEXER_CACHE_SIZE, JSON_LEXER_MAX_VALUE_BYTES);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Discovers the OIDC configuration from a running QuestDB server and builds an instance
     * around it. Reads the public {@code /settings} endpoint (no auth required) and picks up
     * the client id, scope, token endpoint, device authorization endpoint and the
     * groups-in-token mode the server expects.
     * <p>
     * <b>Trust model:</b> the token and device authorization endpoints the user signs in against are
     * taken from the server's unauthenticated {@code /settings} response. A spoofed, compromised, or
     * man-in-the-middled server can therefore redirect the entire sign-in to an attacker-controlled
     * identity provider and harvest the user's authorization. Only call {@code fromQuestDB} against a
     * server you trust, reached over {@code https} (required by default; relaxing it with
     * {@link Builder#allowInsecureTransport(boolean)} removes the transport protection). When the
     * server is not trusted, configure the identity provider explicitly with {@link #builder()}
     * rather than discovering it.
     *
     * @param questdbUrl the QuestDB HTTP base URL, for example {@code https://questdb.example.com:9000}
     * @return a configured, ready-to-use instance
     * @throws OidcAuthException if the server has OIDC disabled, or does not advertise a device
     *                           authorization endpoint (an older server, or one not configured for it)
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl) {
        return fromQuestDB(questdbUrl, defaultTlsConfig(), false);
    }

    /**
     * Same as {@link #fromQuestDB(String)} but lets the caller permit insecure {@code http} transport
     * for the QuestDB server and the discovered identity provider endpoints (see
     * {@link Builder#allowInsecureTransport(boolean)}). Intended for local development only.
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl, boolean allowInsecureTransport) {
        return fromQuestDB(questdbUrl, defaultTlsConfig(), allowInsecureTransport);
    }

    /**
     * Same as {@link #fromQuestDB(String)} but with an explicit TLS configuration, used both for
     * the discovery request and for the later identity provider requests.
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl, ClientTlsConfiguration tlsConfig) {
        return fromQuestDB(questdbUrl, tlsConfig, false);
    }

    /**
     * Same as {@link #fromQuestDB(String, ClientTlsConfiguration)} but lets the caller permit insecure
     * {@code http} transport for the QuestDB server and the discovered identity provider endpoints
     * (see {@link Builder#allowInsecureTransport(boolean)}). Intended for local development only.
     */
    public static OidcDeviceAuth fromQuestDB(String questdbUrl, ClientTlsConfiguration tlsConfig, boolean allowInsecureTransport) {
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
        if (parser.tokenEndpoint.length() == 0) {
            throw new OidcAuthException().put("the QuestDB server does not advertise an OIDC token endpoint [url=").put(questdbUrl).put(']');
        }
        if (parser.deviceAuthorizationEndpoint.length() == 0) {
            throw new OidcAuthException()
                    .put("the QuestDB server does not advertise a device authorization endpoint; upgrade the server ")
                    .put("or configure the endpoint explicitly with OidcDeviceAuth.builder() [url=").put(questdbUrl).put(']');
        }
        return builder()
                .clientId(parser.clientId.toString())
                .deviceAuthorizationEndpoint(parser.deviceAuthorizationEndpoint.toString())
                .tokenEndpoint(parser.tokenEndpoint.toString())
                .scope(parser.scope.length() > 0 ? parser.scope.toString() : DEFAULT_SCOPE)
                .groupsInToken(parser.groupsInToken)
                .allowInsecureTransport(allowInsecureTransport)
                .tlsConfig(tlsConfig)
                .build();
    }

    /**
     * Drops any cached token so the next {@link #getToken()} starts a fresh interactive sign-in.
     */
    public synchronized void clearCache() {
        throwIfClosed();
        accessToken = null;
        idToken = null;
        refreshToken = null;
        expiresAtMillis = 0;
    }

    /**
     * Frees the network connections and native buffers this instance holds. If a {@link #getToken()}
     * sign-in is in flight on another thread, {@code close()} cancels it, so the blocked sign-in fails
     * promptly with an {@link OidcAuthException} instead of polling to the device-code timeout. Safe to
     * call more than once. After close, {@link #getToken()} and {@link #clearCache()} throw.
     */
    @Override
    public void close() {
        // flag cancellation before taking the lock: getToken() holds the monitor for the whole
        // interactive flow, so close() signals the in-flight sign-in to stop with a lock-free volatile
        // write, then acquires the lock - which the now-cancelled flow releases promptly - and frees the
        // native resources. close() never frees while a flow holds the lock, so there is no use-after-free
        closed = true;
        synchronized (this) {
            plainClient = Misc.free(plainClient);
            tlsClient = Misc.free(tlsClient);
            jsonLexer = Misc.free(jsonLexer);
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
     * Returns a valid token to present to QuestDB. Returns the cached token while it is still
     * valid; otherwise refreshes it silently when possible, or runs the interactive device flow.
     * The returned token is the id token when the server expects groups encoded in the token,
     * and the access token otherwise.
     *
     * @return a non-null, non-empty token
     * @throws OidcAuthException if the interactive flow fails, times out, or the identity provider
     *                           does not return the expected token
     */
    public synchronized String getToken() {
        throwIfClosed();
        // only a cached copy of the token getToken() actually serves counts as a cache hit; a grant
        // that returned the other kind (an access token when the server wants the id token, or vice
        // versa) leaves the served token null, so the flow must re-run rather than report the unusable
        // grant as valid and have selectToken() throw on this and every later call
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
    }

    /**
     * Returns a valid token like {@link #getToken()} but never starts the interactive device flow:
     * it returns the cached token while it is valid and silently refreshes it when a refresh token is
     * available, otherwise it throws. Intended as a per-request token source for a long-lived client,
     * for example {@code Sender.builder(...).httpTokenProvider(auth::getTokenSilently)}, where an
     * interactive prompt on the request path would be inappropriate. Call {@link #getToken()} once to
     * sign in before handing this method to a client.
     *
     * @return a non-null, non-empty token
     * @throws OidcAuthException if no token has been obtained yet, or the cached token expired and
     *                           could not be refreshed without an interactive sign-in
     */
    public synchronized String getTokenSilently() {
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
        // best-effort drain after a parse failure so the keep-alive connection stays usable; bounded the
        // same way as parseBody so a hostile server cannot wedge the thread here either
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

    private static void discoverSettings(Endpoint server, ClientTlsConfiguration tlsConfig, SettingsDiscoveryParser parser) {
        HttpClient client = server.isTls
                ? HttpClientFactory.newTlsInstance(HTTP_CONFIG, tlsConfig)
                : HttpClientFactory.newPlainTextInstance(HTTP_CONFIG);
        JsonLexer lexer = new JsonLexer(JSON_LEXER_CACHE_SIZE, JSON_LEXER_MAX_VALUE_BYTES);
        try {
            HttpClient.Request request = client.newRequest(server.host, server.port)
                    .GET()
                    .url(appendSettingsPath(server.path))
                    .header("Accept", "application/json")
                    .header("User-Agent", USER_AGENT);
            HttpClient.ResponseHeaders response = request.send(DEFAULT_HTTP_TIMEOUT_MILLIS);
            response.await(DEFAULT_HTTP_TIMEOUT_MILLIS);
            Response body = response.getResponse();
            // bounded read: parseBody enforces a wall-clock deadline and a byte cap so an untrusted
            // server cannot wedge discovery, and its parseLast rejects a truncated /settings document
            parseBody(body, lexer, parser, DEFAULT_HTTP_TIMEOUT_MILLIS);
        } catch (HttpClientException e) {
            throw new OidcAuthException(e).put("could not reach the QuestDB server to discover OIDC settings");
        } catch (JsonException e) {
            throw new OidcAuthException(e).put("could not parse the QuestDB /settings response");
        } finally {
            Misc.free(lexer);
            Misc.free(client);
        }
    }

    private static void parseBody(Response body, JsonLexer lexer, JsonParser parser, int timeoutMillis) throws JsonException {
        // read and parse the whole body, bounded by an overall wall-clock deadline and a cumulative byte
        // cap, so a hostile or stalled server cannot wedge the thread by dribbling or endlessly streaming
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

    private static void putValue(StringSink sink, CharSequence tag) {
        // clear before storing so a repeated key in the response replaces, rather than concatenates onto,
        // the previous value (the same clear-before-put guard SettingsDiscoveryParser.putNonNull applies)
        sink.clear();
        sink.put(tag);
    }

    private static void requireSecureTransport(boolean isTls, String label, String url) {
        if (!isTls) {
            throw new OidcAuthException()
                    .put("the ").put(label).put(" uses insecure http, which exposes the OIDC sign-in to network ")
                    .put("attackers; use an https url, or call allowInsecureTransport(true) to override [url=").put(url).put(']');
        }
    }

    private static String sanitizeForDisplay(String value) {
        if (value == null) {
            return null;
        }
        int firstControl = -1;
        int n = value.length();
        for (int i = 0; i < n; i++) {
            if (Character.isISOControl(value.charAt(i))) {
                firstControl = i;
                break;
            }
        }
        if (firstControl < 0) {
            // common case: nothing to strip
            return value;
        }
        // an attacker-influenced device-auth field smuggled in control characters (ANSI escapes,
        // CR/LF); strip them so a prompt cannot be tricked into rewriting or spoofing the terminal
        StringSink sink = new StringSink();
        sink.put(value, 0, firstControl);
        for (int i = firstControl + 1; i < n; i++) {
            char c = value.charAt(i);
            if (!Character.isISOControl(c)) {
                sink.put(c);
            }
        }
        return sink.toString();
    }

    private static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
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
        // responseStatus holds the numeric HTTP status captured by readResponse; a 2xx starts with '2'
        return responseStatus.length() > 0 && responseStatus.charAt(0) == '2';
    }

    private int pollOnce(String deviceCode) {
        formSink.clear();
        formSink.putAscii("grant_type=").putAscii(urlEncode(GRANT_TYPE_DEVICE_CODE));
        appendParam(formSink, "device_code", deviceCode);
        appendParam(formSink, "client_id", clientId);

        tokenParser.clear();
        // a transport failure here propagates to pollForToken, which retries a brief blip but aborts
        // on a persistent failure rather than swallowing it as a pending authorization
        postForm(tokenEndpoint, tokenParser);

        if (tokenParser.accessToken.length() > 0 || tokenParser.idToken.length() > 0) {
            storeTokens(tokenParser);
            return POLL_SUCCESS;
        }
        if (tokenParser.error.length() == 0) {
            // a 2xx with neither tokens nor an OAuth error is a definitive but malformed answer and
            // aborts; a non-2xx with no parseable error (a gateway 5xx, an empty body) is a transport-
            // class blip - retry it rather than abort the whole sign-in on a momentary upstream failure
            if (isHttpStatusSuccess()) {
                throw new OidcAuthException().put("unexpected response from the token endpoint [httpStatus=").put(responseStatus).put(']');
            }
            return POLL_TRANSIENT_ERROR;
        }
        if (Chars.equals(ERROR_AUTHORIZATION_PENDING, tokenParser.error)) {
            return POLL_PENDING;
        }
        if (Chars.equals(ERROR_SLOW_DOWN, tokenParser.error)) {
            return POLL_SLOW_DOWN;
        }
        throw OidcAuthException.oauthError(tokenParser.error, tokenParser.errorDescription);
    }

    private void pollForToken(String deviceCode, int expiresInSeconds, int intervalSeconds) {
        final long deadlineNanos = System.nanoTime() + expiresInSeconds * 1_000_000_000L;
        long intervalMillis = (long) intervalSeconds * 1000L;
        int consecutiveTransportErrors = 0;
        while (true) {
            throwIfClosed();
            try {
                int result = pollOnce(deviceCode);
                if (result == POLL_SUCCESS) {
                    return;
                }
                if (result == POLL_TRANSIENT_ERROR) {
                    // a non-2xx with no parseable answer; charge it to the transport-error budget so a
                    // persistently failing token endpoint aborts instead of polling until the code expires
                    if (++consecutiveTransportErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
                        throw new OidcAuthException().put("the token endpoint returned repeated unexpected responses [httpStatus=").put(responseStatus).put(']');
                    }
                } else {
                    consecutiveTransportErrors = 0;
                    if (result == POLL_SLOW_DOWN) {
                        intervalMillis += SLOW_DOWN_INCREMENT_SECONDS * 1000L;
                    }
                }
            } catch (HttpClientException e) {
                // a brief network blip is fine to retry, but a persistent failure (a rejected TLS
                // certificate, a refused connection, an unresolvable host) must surface with its cause
                // rather than masquerade as a device-code timeout
                if (++consecutiveTransportErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
                    throw new OidcAuthException(e).put("the token endpoint became unreachable while waiting for authorization");
                }
            } catch (OidcAuthException e) {
                // a garbled / non-JSON body (a JsonException cause) is a transport-class blip and is
                // retried on the same budget; a well-formed OAuth error or unexpected response (no
                // parse cause) is a real answer from the identity provider and aborts immediately
                if (!(e.getCause() instanceof JsonException)) {
                    throw e;
                }
                if (++consecutiveTransportErrors >= MAX_CONSECUTIVE_POLL_ERRORS) {
                    throw e;
                }
            }
            if (System.nanoTime() >= deadlineNanos) {
                throw new OidcAuthException("timed out waiting for authorization, the device code expired; please retry");
            }
            sleepBetweenPolls(intervalMillis);
        }
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
        // capture only the HTTP status for diagnostics; the body is never retained or surfaced in
        // a message, it carries access, id and refresh tokens that must not reach logs or exceptions
        responseStatus.clear();
        DirectUtf8Sequence statusCode = response.getStatusCode();
        if (statusCode != null) {
            responseStatus.put(statusCode.asAsciiCharSequence());
        }
        jsonLexer.clear();
        Response body = response.getResponse();
        try {
            parseBody(body, jsonLexer, parser, httpTimeoutMillis);
        } catch (JsonException e) {
            // drain the rest so the keep-alive connection stays usable; never embed the body, it may
            // carry tokens
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
        if (deviceAuthParser.deviceCode.length() == 0 || deviceAuthParser.userCode.length() == 0
                || deviceAuthParser.verificationUri.length() == 0) {
            throw new OidcAuthException().put("incomplete device authorization response from the identity provider [httpStatus=").put(responseStatus).put(']');
        }

        final String deviceCode = deviceAuthParser.deviceCode.toString();
        final int expiresInSeconds = boundedSeconds(deviceAuthParser.expiresIn, DEFAULT_DEVICE_CODE_TTL_SECONDS, MAX_EXPIRES_IN_SECONDS);
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
        // instead of after a full (possibly slow_down-inflated) poll interval; Os.sleep ignores thread
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
        accessToken = parser.accessToken.length() > 0 ? parser.accessToken.toString() : null;
        idToken = parser.idToken.length() > 0 ? parser.idToken.toString() : null;
        // a refresh response usually omits a new refresh token, in that case we keep the current one
        if (parser.refreshToken.length() > 0) {
            refreshToken = parser.refreshToken.toString();
        }
        int ttlSeconds = parser.expiresIn > 0 ? parser.expiresIn : DEFAULT_TOKEN_TTL_SECONDS;
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

        tokenParser.clear();
        try {
            postForm(tokenEndpoint, tokenParser);
        } catch (HttpClientException e) {
            // could not reach the token endpoint, fall back to the interactive flow
            return false;
        } catch (OidcAuthException e) {
            // a garbled / unparseable refresh response is a transient blip, not a definitive answer;
            // fall back to the interactive flow rather than fail the whole getToken() call. A genuine
            // OAuth error arrives in tokenParser.error (handled below), not as a thrown oauthError here
            if (e.getOauthError() != null) {
                throw e;
            }
            return false;
        }
        // only treat the refresh as a success if it returned the token getToken() actually serves
        // (the id token when groups are encoded in it, the access token otherwise); a refresh that
        // omits the id token - which RFC 6749 permits and many providers do - must fall back to the
        // interactive flow rather than fail later in selectToken()
        boolean hasRequiredToken = groupsInToken
                ? tokenParser.idToken.length() > 0
                : tokenParser.accessToken.length() > 0;
        if (hasRequiredToken) {
            storeTokens(tokenParser);
            return true;
        }
        // the refresh token expired or was revoked, or it did not return the token we need;
        // fall back to the interactive flow
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
        private DeviceCodePrompt prompt = DeviceCodePrompt.SYSTEM_OUT;
        private String scope = DEFAULT_SCOPE;
        private ClientTlsConfiguration tlsConfig;
        private String tokenEndpoint;

        private Builder() {
        }

        /**
         * Permits insecure {@code http} (rather than {@code https}) for the device authorization and
         * token endpoints. Tokens then travel in cleartext, so this is rejected by default and should
         * only be enabled for local development on a trusted network. Defaults to {@code false}.
         */
        public Builder allowInsecureTransport(boolean allowInsecureTransport) {
            this.allowInsecureTransport = allowInsecureTransport;
            return this;
        }

        /**
         * Sets the {@code audience} (or {@code resource}) request parameter. Some identity providers
         * require it so the issued token carries the {@code aud} claim QuestDB expects. Optional.
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
            if (!allowInsecureTransport) {
                requireSecureTransport(Endpoint.parse(deviceAuthorizationEndpoint).isTls, "device authorization endpoint", deviceAuthorizationEndpoint);
                requireSecureTransport(Endpoint.parse(tokenEndpoint).isTls, "token endpoint", tokenEndpoint);
            }
            ClientTlsConfiguration tls = tlsConfig != null ? tlsConfig : defaultTlsConfig();
            return new OidcDeviceAuth(this, tls);
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        /**
         * Sets how many seconds before the real expiry a cached token is treated as expired. Defaults
         * to 30 seconds. The margin absorbs clock drift and request latency.
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
         * Sets how the device code challenge is shown to the user. Defaults to
         * {@link DeviceCodePrompt#SYSTEM_OUT}.
         */
        public Builder prompt(DeviceCodePrompt prompt) {
            this.prompt = prompt != null ? prompt : DeviceCodePrompt.SYSTEM_OUT;
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
                                putValue(deviceCode, tag);
                                break;
                            case FIELD_USER_CODE:
                                putValue(userCode, tag);
                                break;
                            case FIELD_VERIFICATION_URI:
                                putValue(verificationUri, tag);
                                break;
                            case FIELD_VERIFICATION_URI_COMPLETE:
                                putValue(verificationUriComplete, tag);
                                break;
                            case FIELD_EXPIRES_IN:
                                expiresIn = parseIntOrZero(tag);
                                break;
                            case FIELD_INTERVAL:
                                interval = parseIntOrZero(tag);
                                break;
                            case FIELD_ERROR:
                                putValue(error, tag);
                                break;
                            case FIELD_ERROR_DESCRIPTION:
                                putValue(errorDescription, tag);
                                break;
                            default:
                                break;
                        }
                    }
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
                // bracketed IPv6 literal: the client's HTTP layer does not bracket the Host header,
                // so reject it clearly rather than mis-parse it on a ':' inside the address
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
        private static final int FIELD_CLIENT_ID = 2;
        private static final int FIELD_DEVICE_AUTHORIZATION_ENDPOINT = 5;
        private static final int FIELD_ENABLED = 1;
        private static final int FIELD_GROUPS_IN_TOKEN = 6;
        private static final int FIELD_NONE = 0;
        private static final int FIELD_SCOPE = 3;
        private static final int FIELD_TOKEN_ENDPOINT = 4;
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
                        // only the top-level "config" object is trusted; the sibling "preferences"
                        // object holds arbitrary user-written keys and must not feed OIDC discovery
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

        private static void putNonNull(StringSink sink, CharSequence tag) {
            // a JSON null is delivered as the literal "null", treat it as absent; clear first so a
            // duplicate key cannot concatenate onto an earlier value
            sink.clear();
            if (!Chars.equals("null", tag)) {
                sink.put(tag);
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
                                putValue(accessToken, tag);
                                break;
                            case FIELD_ID_TOKEN:
                                putValue(idToken, tag);
                                break;
                            case FIELD_REFRESH_TOKEN:
                                putValue(refreshToken, tag);
                                break;
                            case FIELD_EXPIRES_IN:
                                expiresIn = parseIntOrZero(tag);
                                break;
                            case FIELD_ERROR:
                                putValue(error, tag);
                                break;
                            case FIELD_ERROR_DESCRIPTION:
                                putValue(errorDescription, tag);
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
