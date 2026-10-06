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
import com.azure.core.util.Configuration;
import com.azure.identity.CredentialUnavailableException;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.EnvironmentCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import io.questdb.client.cutlass.auth.TokenProviderFactory;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.auth.TokenSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * The {@code azure} token provider (design/qwp-token-provider-spec.md, section 7.5): Microsoft Entra ID tokens
 * from Azure Identity. Discovered through {@link java.util.ServiceLoader}; put this artifact on the class path or
 * module path and connect with
 * <pre>
 * wss::addr=qdb1:9000,qdb2:9000;token_provider=azure;azure_resource=api://&lt;questdb-app-id&gt;;azure_credential=managed_identity;
 * </pre>
 * <ul>
 *   <li>{@code azure_resource} (required) - the application ID URI or client ID of the QuestDB app
 *   registration; the requested scope is {@code <azure_resource>/.default}.</li>
 *   <li>{@code azure_credential} (optional) - which credential to use:
 *     <ul>
 *       <li>{@code default}, the default - {@code DefaultAzureCredential}, a discovery chain meant for
 *       development: an environment service principal ({@code AZURE_CLIENT_ID}, {@code AZURE_TENANT_ID},
 *       {@code AZURE_CLIENT_SECRET} or a certificate), a workload identity, a managed identity, then developer
 *       tools. The chain checks for the managed-identity endpoint once, without retrying, so an outage of that
 *       endpoint looks like a host without a credential: a {@code sync} startup fails fast instead of riding it
 *       out. The provider logs a warning saying so when it starts, unless Azure Identity's own selector,
 *       {@code AZURE_TOKEN_CREDENTIALS}, already narrows the chain.</li>
 *       <li>{@code managed_identity}, {@code workload_identity} or {@code environment} - that credential alone,
 *       recommended in production. A managed identity used alone retries an unreachable endpoint with backoff,
 *       and reports it apart from an identity that is not assigned to the host.</li>
 *     </ul>
 *   </li>
 *   <li>{@code azure_client_id} (optional) - the client ID of a user-assigned managed identity or of a workload
 *   identity. With {@code default} it selects both; it cannot be combined with {@code environment}, which reads
 *   its client ID from the platform's configuration.</li>
 * </ul>
 * Secrets come from the platform's standard configuration, never from the connect string.
 * <p>
 * The deterministic credentials are built directly, not by passing {@code AZURE_TOKEN_CREDENTIALS} to
 * {@code DefaultAzureCredentialBuilder.configuration(...)}: with that, Azure Identity 1.18 selects the credential
 * but keeps the managed-identity endpoint check of a chain, because each credential re-reads the selector from the
 * process-wide configuration.
 */
public final class AzureTokenProviderFactory implements TokenProviderFactory {
    // Azure Identity's own credential selector, an environment variable or system property
    private static final String AZURE_TOKEN_CREDENTIALS = "AZURE_TOKEN_CREDENTIALS";
    private static final Logger LOG = LoggerFactory.getLogger(AzureTokenProviderFactory.class);

    // Validates azure_credential and the keys it constrains. Null for default.
    private static String credentialMode(Map<String, String> params) {
        final String credential = params.get(TokenProviderSpec.KEY_AZURE_CREDENTIAL);
        if (credential == null) {
            return null;
        }
        switch (credential) {
            case TokenProviderSpec.AZURE_CREDENTIAL_MANAGED_IDENTITY:
            case TokenProviderSpec.AZURE_CREDENTIAL_WORKLOAD_IDENTITY:
                return credential;
            case TokenProviderSpec.AZURE_CREDENTIAL_ENVIRONMENT:
                if (params.get(TokenProviderSpec.KEY_AZURE_CLIENT_ID) != null) {
                    throw new IllegalArgumentException(TokenProviderSpec.KEY_AZURE_CLIENT_ID + " cannot be combined with "
                            + TokenProviderSpec.KEY_AZURE_CREDENTIAL + '=' + TokenProviderSpec.AZURE_CREDENTIAL_ENVIRONMENT);
                }
                return credential;
            default:
                throw new IllegalArgumentException("invalid " + TokenProviderSpec.KEY_AZURE_CREDENTIAL + ": " + credential
                        + " (expected default, managed_identity, workload_identity, environment)");
        }
    }

    private static TokenCredential deterministicCredential(String mode, String clientId) {
        try {
            switch (mode) {
                case TokenProviderSpec.AZURE_CREDENTIAL_MANAGED_IDENTITY: {
                    final ManagedIdentityCredentialBuilder builder = new ManagedIdentityCredentialBuilder();
                    if (clientId != null) {
                        builder.clientId(clientId);
                    }
                    return builder.build();
                }
                case TokenProviderSpec.AZURE_CREDENTIAL_WORKLOAD_IDENTITY: {
                    final WorkloadIdentityCredentialBuilder builder = new WorkloadIdentityCredentialBuilder();
                    if (clientId != null) {
                        builder.clientId(clientId);
                    }
                    return builder.build();
                }
                default:
                    return new EnvironmentCredentialBuilder().build();
            }
        } catch (RuntimeException e) {
            // The credential lacks configuration it requires and cannot even be built: a workload identity without
            // its tenant ID, client ID or federated token file. That is permanent (section 7.5), and it is reported
            // like any other credential failure - by the provider, when a client connects - rather than from inside
            // a client's build().
            return new UnconfiguredCredential(e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        }
    }

    /**
     * Whether {@code DefaultAzureCredential} runs as a discovery chain that includes the managed-identity
     * endpoint check: Azure Identity's selector, {@code AZURE_TOKEN_CREDENTIALS}, is unset or {@code prod}. With
     * {@code dev} the chain has no managed identity, and a credential name selects that credential alone.
     */
    static boolean isDiscoveryChain(String selector) {
        if (selector == null) {
            return true;
        }
        final String value = selector.trim();
        return value.isEmpty() || "prod".equalsIgnoreCase(value);
    }

    @Override
    public TokenSource createSource(Map<String, String> params) {
        final String resource = params.get(TokenProviderSpec.KEY_AZURE_RESOURCE);
        if (resource == null) {
            throw new IllegalArgumentException("token_provider=azure requires " + TokenProviderSpec.KEY_AZURE_RESOURCE);
        }
        final String mode = credentialMode(params);
        final String clientId = params.get(TokenProviderSpec.KEY_AZURE_CLIENT_ID);
        if (mode != null) {
            return new AzureTokenSource(deterministicCredential(mode, clientId), resource);
        }
        final DefaultAzureCredentialBuilder builder = new DefaultAzureCredentialBuilder();
        if (clientId != null) {
            builder.managedIdentityClientId(clientId);
            builder.workloadIdentityClientId(clientId);
        }
        final AzureTokenSource source = new AzureTokenSource(builder.build(), resource);
        if (isDiscoveryChain(Configuration.getGlobalConfiguration().get(AZURE_TOKEN_CREDENTIALS))) {
            // section 7.5: once, when the provider starts
            LOG.warn("token provider {}: azure_credential is not set, so tokens come from DefaultAzureCredential, a "
                    + "discovery chain meant for development. It checks the managed-identity endpoint once, without "
                    + "retrying, so an outage of that endpoint fails a sync startup as if this host had no credential. "
                    + "In production, select the credential: azure_credential=managed_identity, workload_identity or "
                    + "environment.", describe(params));
        }
        return source;
    }

    @Override
    public String describe(Map<String, String> params) {
        final StringBuilder sb = new StringBuilder(TokenProviderSpec.AZURE)
                .append("[resource=").append(params.get(TokenProviderSpec.KEY_AZURE_RESOURCE));
        final String credential = params.get(TokenProviderSpec.KEY_AZURE_CREDENTIAL);
        if (credential != null) {
            sb.append(", credential=").append(credential);
        }
        final String clientId = params.get(TokenProviderSpec.KEY_AZURE_CLIENT_ID);
        if (clientId != null) {
            sb.append(", client_id=").append(clientId);
        }
        return sb.append(']').toString();
    }

    @Override
    public String name() {
        return TokenProviderSpec.AZURE;
    }

    @Override
    public void validate(Map<String, String> params) {
        if (params.get(TokenProviderSpec.KEY_AZURE_RESOURCE) == null) {
            throw new IllegalArgumentException("token_provider=azure requires " + TokenProviderSpec.KEY_AZURE_RESOURCE);
        }
        credentialMode(params);
    }

    // A credential whose configuration is missing: every request fails with Azure Identity's own "unavailable"
    // exception, which AzureTokenSource classifies as permanent.
    private static final class UnconfiguredCredential implements TokenCredential {
        private final String reason;

        UnconfiguredCredential(String reason) {
            this.reason = reason;
        }

        @Override
        public Mono<AccessToken> getToken(TokenRequestContext request) {
            return Mono.error(new CredentialUnavailableException(reason));
        }
    }
}
