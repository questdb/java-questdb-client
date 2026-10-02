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

package io.questdb.client.azure;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpResponse;
import com.azure.identity.CredentialUnavailableException;
import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.TokenSource;
import io.questdb.client.cutlass.auth.TokenUnavailableException;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TokenSource} that obtains Microsoft Entra ID access tokens from an Azure Identity
 * {@link TokenCredential} - by default a {@code DefaultAzureCredential} - for the scope
 * {@code <resource>/.default} (design/qwp-token-provider-spec.md, section 7.5).
 * <p>
 * Use it directly to put an Azure credential of your choice behind a
 * {@link io.questdb.client.cutlass.auth.RefreshingTokenProvider}:
 * <pre>{@code
 * TokenCredential credential = new ClientCertificateCredentialBuilder()...build();
 * RefreshingTokenProvider tokens = RefreshingTokenProvider.builder(
 *         new AzureTokenSource(credential, "api://<questdb-app-id>")).build();
 * }</pre>
 * or let a connect string select {@code token_provider=azure}, which uses {@code DefaultAzureCredential}.
 * <p>
 * Expiry and the refresh hint come from the library's {@link AccessToken}. Failures are classified as section
 * 7.5 prescribes:
 * <ul>
 *   <li>permanent - "no credential available in the chain" ({@link CredentialUnavailableException}), and
 *   authentication failures Entra reports: an HTTP 400 or 401 from the token endpoint, or a known AADSTS
 *   configuration error such as an invalid client or an application that does not exist;</li>
 *   <li>retryable - network errors, timeouts, throttling, 5xx responses, and anything else. A
 *   {@code Retry-After} in seconds is passed on.</li>
 * </ul>
 * Each attempt is bounded (30 s by default). Library exceptions never travel as a cause - their response
 * objects reference the raw HTTP request, which can carry a client secret - only their sanitized message does.
 */
public final class AzureTokenSource implements TokenSource {
    /**
     * The bound on one token request.
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final String DEFAULT_SCOPE_SUFFIX = "/.default";
    private static final int MAX_CAUSE_DEPTH = 16;
    // Entra (AADSTS) errors that a retry cannot fix: the configuration is wrong, an operator must act.
    private static final String[] PERMANENT_AADSTS = {
            "AADSTS700016", // application not found in the directory
            "AADSTS7000215", // invalid client secret
            "AADSTS7000222", // client secret expired
            "AADSTS700027", // client assertion signature invalid
            "AADSTS70011", // invalid scope
            "AADSTS500011", // resource principal not found: azure_resource is wrong
            "AADSTS90002", // tenant not found
            "AADSTS700213", // no matching federated identity record
            "AADSTS70021", // no matching federated identity record
            "AADSTS7000112", // application disabled
            "AADSTS50049", // unknown or invalid instance
    };
    private final TokenRequestContext context;
    private final TokenCredential credential;
    private final String scope;
    private final Duration timeout;

    /**
     * @param credential the Azure Identity credential
     * @param resource   the application ID URI ({@code api://<app-id>}) or client ID of the QuestDB app
     *                   registration; a trailing {@code /.default} is accepted
     */
    public AzureTokenSource(TokenCredential credential, String resource) {
        this(credential, resource, DEFAULT_TIMEOUT);
    }

    /**
     * @param credential the Azure Identity credential
     * @param resource   the application ID URI ({@code api://<app-id>}) or client ID of the QuestDB app
     *                   registration; a trailing {@code /.default} is accepted
     * @param timeout    the bound on one token request
     */
    public AzureTokenSource(TokenCredential credential, String resource, Duration timeout) {
        if (credential == null) {
            throw new IllegalArgumentException("credential must not be null");
        }
        if (resource == null || resource.isEmpty()) {
            throw new IllegalArgumentException("resource must not be empty");
        }
        if (timeout == null || timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        this.credential = credential;
        this.scope = resource.endsWith(DEFAULT_SCOPE_SUFFIX) ? resource : resource + DEFAULT_SCOPE_SUFFIX;
        this.context = new TokenRequestContext().addScopes(scope);
        this.timeout = timeout;
    }

    /**
     * Classifies a failure from Azure Identity per section 7.5. Never attaches the library exception.
     */
    static TokenUnavailableException classify(Throwable failure, String scope) {
        final String description = describe(failure);
        Throwable t = failure;
        for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause()) {
            if (t instanceof CredentialUnavailableException) {
                return TokenUnavailableException.permanent(
                        "no credential available in the Azure Identity chain for " + scope + ": " + description);
            }
            if (t instanceof HttpResponseException) {
                final HttpResponse response = ((HttpResponseException) t).getResponse();
                if (response != null) {
                    final int status = response.getStatusCode();
                    if (status == 400 || status == 401) {
                        return TokenUnavailableException.permanent(
                                "Entra rejected the token request for " + scope + " with HTTP " + status + ": " + description);
                    }
                    return TokenUnavailableException.retryable(
                            "the token request for " + scope + " failed with HTTP " + status + ": " + description,
                            retryAfterMillis(response));
                }
            }
            final String message = t.getMessage();
            if (message != null) {
                for (String code : PERMANENT_AADSTS) {
                    if (message.contains(code)) {
                        return TokenUnavailableException.permanent(
                                "Entra rejected the token request for " + scope + " (" + code + "): " + description);
                    }
                }
            }
        }
        return TokenUnavailableException.retryable("the token request for " + scope + " failed: " + description);
    }

    private static String describe(Throwable t) {
        final String message = t.getMessage();
        return message == null ? t.getClass().getName() : t.getClass().getSimpleName() + ": " + message;
    }

    private static long retryAfterMillis(HttpResponse response) {
        final String value;
        try {
            value = response.getHeaderValue("Retry-After");
        } catch (RuntimeException e) {
            return TokenUnavailableException.NO_RETRY_AFTER;
        }
        if (value == null) {
            return TokenUnavailableException.NO_RETRY_AFTER;
        }
        try {
            // delta-seconds only; an HTTP-date is ignored and the provider's own backoff applies
            return TimeUnit.SECONDS.toMillis(Long.parseLong(value.trim()));
        } catch (NumberFormatException e) {
            return TokenUnavailableException.NO_RETRY_AFTER;
        }
    }

    private static long refreshAtMillis(AccessToken token) {
        try {
            final OffsetDateTime refreshAt = token.getRefreshAt();
            return refreshAt == null ? ExpiringToken.NO_REFRESH_AT : refreshAt.toInstant().toEpochMilli();
        } catch (NoSuchMethodError e) {
            // an azure-core older than the refresh hint
            return ExpiringToken.NO_REFRESH_AT;
        }
    }

    @Override
    public ExpiringToken fetchToken() {
        final AccessToken token;
        try {
            token = credential.getToken(context).block(timeout);
        } catch (RuntimeException e) {
            throw classify(e, scope);
        }
        if (token == null) {
            throw TokenUnavailableException.retryable("Azure Identity returned no token for " + scope);
        }
        final OffsetDateTime expiresAt = token.getExpiresAt();
        if (token.getToken() == null || expiresAt == null) {
            // the shape of the problem only, never the response
            throw TokenUnavailableException.retryable("Azure Identity returned a token for " + scope
                    + " without " + (token.getToken() == null ? "a value" : "an expiry"));
        }
        try {
            return new ExpiringToken(token.getToken(), expiresAt.toInstant().toEpochMilli(), refreshAtMillis(token));
        } catch (IllegalArgumentException e) {
            throw TokenUnavailableException.retryable("Azure Identity returned an unusable token for " + scope
                    + ": " + e.getMessage());
        }
    }

    /**
     * @return the requested scope, {@code <resource>/.default}
     */
    public String getScope() {
        return scope;
    }

    @Override
    public String toString() {
        return "AzureTokenSource{scope=" + scope + ", credential=" + credential.getClass().getSimpleName() + '}';
    }
}
