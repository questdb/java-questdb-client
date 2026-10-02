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

import com.azure.identity.DefaultAzureCredentialBuilder;
import io.questdb.client.cutlass.auth.TokenProviderFactory;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.auth.TokenSource;

import java.util.Map;

/**
 * The {@code azure} token provider (design/qwp-token-provider-spec.md, section 7.5): Microsoft Entra ID tokens
 * from Azure Identity's {@code DefaultAzureCredential}. Discovered through {@link java.util.ServiceLoader}; put
 * this artifact on the class path or module path and connect with
 * <pre>
 * wss::addr=qdb1:9000,qdb2:9000;token_provider=azure;azure_resource=api://&lt;questdb-app-id&gt;;
 * </pre>
 * <ul>
 *   <li>{@code azure_resource} (required) - the application ID URI or client ID of the QuestDB app
 *   registration; the requested scope is {@code <azure_resource>/.default}.</li>
 *   <li>{@code azure_client_id} (optional) - the client ID of a user-assigned managed identity or of a workload
 *   identity; it selects both.</li>
 * </ul>
 * Depending on the environment, the credential chain finds an environment service principal
 * ({@code AZURE_CLIENT_ID}, {@code AZURE_TENANT_ID}, {@code AZURE_CLIENT_SECRET} or a certificate), a workload
 * identity, a managed identity, or developer tools. Secrets come from that standard configuration, never from
 * the connect string.
 */
public final class AzureTokenProviderFactory implements TokenProviderFactory {

    @Override
    public TokenSource createSource(Map<String, String> params) {
        final String resource = params.get(TokenProviderSpec.KEY_AZURE_RESOURCE);
        if (resource == null) {
            throw new IllegalArgumentException("token_provider=azure requires " + TokenProviderSpec.KEY_AZURE_RESOURCE);
        }
        final DefaultAzureCredentialBuilder builder = new DefaultAzureCredentialBuilder();
        final String clientId = params.get(TokenProviderSpec.KEY_AZURE_CLIENT_ID);
        if (clientId != null) {
            builder.managedIdentityClientId(clientId);
            builder.workloadIdentityClientId(clientId);
        }
        return new AzureTokenSource(builder.build(), resource);
    }

    @Override
    public String describe(Map<String, String> params) {
        final StringBuilder sb = new StringBuilder(TokenProviderSpec.AZURE)
                .append("[resource=").append(params.get(TokenProviderSpec.KEY_AZURE_RESOURCE));
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
    }
}
