/*
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package smoke;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.TokenProviderFactory;
import io.questdb.client.cutlass.auth.TokenProviderRegistry;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.auth.TokenSource;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        String config = "wss::addr=localhost:1;token_provider=azure;azure_resource=api://qdb;azure_credential=environment;";
        Sender.builder(config);
        QwpQueryClient.validateConfig(new ConfigView(ConfigString.parse(config)), true);
        TokenProviderFactory factory = TokenProviderRegistry.findFactory("azure");
        if (factory == null) {
            throw new AssertionError("Azure factory missing from service binding");
        }
        Map<String, String> params = new HashMap<>();
        params.put(TokenProviderSpec.KEY_AZURE_RESOURCE, "api://qdb");
        params.put(TokenProviderSpec.KEY_AZURE_CREDENTIAL, "environment");
        TokenSource source = factory.createSource(params);
        if (!"io.questdb.client.azure.AzureTokenSource".equals(source.getClass().getName())) {
            throw new AssertionError("unexpected source: " + source.getClass().getName());
        }
        System.out.println("azure-module-path-ok");
    }
}
