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

package io.questdb.client.azure.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.questdb.client.azure.AzureTokenProviderFactory;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.auth.TokenSource;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * {@code azure_credential} with the real factory, in process and without network I/O (design/qwp-token-provider-spec.md,
 * sections 7.1, 7.2 and 7.5, conformance test C24): what a connect string selects, which credential the factory
 * builds, and the warning a discovery chain logs when its provider starts. The library's behaviour behind each
 * credential is covered by {@link AzureManagedIdentityLibraryTest}.
 */
public class AzureCredentialSelectionTest {
    private static final String CLIENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    private static final String WSS = "wss::addr=localhost:9000;token_provider=azure;azure_resource=api://qdb;";

    @Test
    public void testConnectStringSelectsTheCredential() {
        TokenProviderSpec managed = spec("azure_credential=managed_identity;azure_client_id=" + CLIENT_ID.toUpperCase() + ';');
        Assert.assertEquals("managed_identity", managed.params().get(TokenProviderSpec.KEY_AZURE_CREDENTIAL));
        Assert.assertEquals("azure[resource=api://qdb, credential=managed_identity, client_id=" + CLIENT_ID + ']',
                managed.describe());

        // default is the default: no parameter, so it shares a provider with a string that omits the key
        TokenProviderSpec omitted = spec("");
        TokenProviderSpec explicit = spec("azure_credential=default;");
        Assert.assertNull(explicit.params().get(TokenProviderSpec.KEY_AZURE_CREDENTIAL));
        Assert.assertEquals(omitted.registryKey(), explicit.registryKey());
        Assert.assertEquals("azure[resource=api://qdb]", explicit.describe());
        // different credentials never share a provider
        Assert.assertNotEquals(omitted.registryKey(), spec("azure_credential=managed_identity;").registryKey());
        Assert.assertNotEquals(spec("azure_credential=workload_identity;").registryKey(),
                spec("azure_credential=managed_identity;").registryKey());

        assertRejected("azure_credential=bogus;",
                "invalid azure_credential: bogus (expected default, managed_identity, workload_identity, environment)");
        assertRejected("azure_credential=environment;azure_client_id=" + CLIENT_ID + ';',
                "azure_client_id cannot be combined with azure_credential=environment");
    }

    @Test
    public void testDiscoveryChainWarnsOnceWhenTheProviderStarts() {
        // the platform's selector would narrow the chain and silence the warning (AzureManagedIdentityLibraryTest)
        Assume.assumeTrue(System.getenv("AZURE_TOKEN_CREDENTIALS") == null);
        Logger logger = (Logger) LoggerFactory.getLogger(AzureTokenProviderFactory.class);
        ListAppender<ILoggingEvent> events = new ListAppender<>();
        events.start();
        logger.addAppender(events);
        try {
            source(null, CLIENT_ID); // creating the source is starting the provider; it fetches nothing
            Assert.assertEquals(1, warnings(events));
            String warning = firstWarning(events);
            Assert.assertTrue(warning, warning.contains("azure[resource=api://qdb, client_id=" + CLIENT_ID + ']'));
            Assert.assertTrue(warning, warning.contains("DefaultAzureCredential"));
            Assert.assertTrue(warning, warning.contains("azure_credential=managed_identity"));

            // a credential selected deterministically is not a chain
            source("managed_identity", CLIENT_ID);
            source("environment", null);
            Assert.assertEquals(1, warnings(events));
        } finally {
            logger.detachAppender(events);
        }
    }

    @Test
    public void testFactoryBuildsTheSelectedCredentialWithoutNetworkIo() {
        Assert.assertTrue(source(null, null).toString().contains("credential=DefaultAzureCredential"));
        Assert.assertTrue(source("managed_identity", null).toString().contains("credential=ManagedIdentityCredential"));
        Assert.assertTrue(source("managed_identity", CLIENT_ID).toString().contains("credential=ManagedIdentityCredential"));
        Assert.assertTrue(source("environment", null).toString().contains("credential=EnvironmentCredential"));
    }

    @Test
    public void testFactoryRejectsParametersItCannotUse() {
        AzureTokenProviderFactory factory = new AzureTokenProviderFactory();
        Map<String, String> bogus = params("bogus", null);
        assertRejected(() -> factory.validate(bogus), "invalid azure_credential: bogus");
        assertRejected(() -> factory.createSource(bogus), "invalid azure_credential: bogus");
        Map<String, String> environmentWithClientId = params("environment", CLIENT_ID);
        assertRejected(() -> factory.validate(environmentWithClientId),
                "azure_client_id cannot be combined with azure_credential=environment");
        assertRejected(() -> factory.createSource(environmentWithClientId),
                "azure_client_id cannot be combined with azure_credential=environment");
        factory.validate(params("workload_identity", CLIENT_ID));
    }

    @Test
    public void testUnconfiguredWorkloadIdentityFailsPermanentlyWhenFetched() {
        // Azure Identity refuses to build a workload identity without its tenant ID and federated token file. That
        // must not escape from a client's build(): it is a permanent token failure, like any missing configuration.
        Assume.assumeTrue(System.getenv("AZURE_TENANT_ID") == null || System.getenv("AZURE_FEDERATED_TOKEN_FILE") == null);
        TokenSource source = source("workload_identity", CLIENT_ID);
        try {
            source.fetchToken();
            Assert.fail("expected the fetch to fail");
        } catch (TokenUnavailableException e) {
            Assert.assertFalse("missing configuration is permanent: " + e.getMessage(), e.isRetryable());
            Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("no credential available for api://qdb/.default: "));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("WorkloadIdentityCredentialBuilder"));
        }
    }

    private static void assertRejected(String keys, String fragment) {
        assertRejected(() -> spec(keys), fragment);
    }

    private static void assertRejected(Runnable action, String fragment) {
        try {
            action.run();
            Assert.fail("expected rejection with: " + fragment);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("[" + e.getMessage() + "] does not contain [" + fragment + ']', e.getMessage().contains(fragment));
        }
    }

    private static String firstWarning(ListAppender<ILoggingEvent> events) {
        for (ILoggingEvent event : events.list) {
            if (event.getLevel() == Level.WARN) {
                return event.getFormattedMessage();
            }
        }
        return null;
    }

    private static Map<String, String> params(String credential, String clientId) {
        Map<String, String> params = new HashMap<>();
        params.put(TokenProviderSpec.KEY_AZURE_RESOURCE, "api://qdb");
        if (credential != null) {
            params.put(TokenProviderSpec.KEY_AZURE_CREDENTIAL, credential);
        }
        if (clientId != null) {
            params.put(TokenProviderSpec.KEY_AZURE_CLIENT_ID, clientId);
        }
        return params;
    }

    private static TokenSource source(String credential, String clientId) {
        return new AzureTokenProviderFactory().createSource(params(credential, clientId));
    }

    private static TokenProviderSpec spec(String keys) {
        return TokenProviderSpec.parse(new ConfigView(ConfigString.parse(WSS + keys)), true);
    }

    private static long warnings(ListAppender<ILoggingEvent> events) {
        long n = 0;
        for (ILoggingEvent event : events.list) {
            if (event.getLevel() == Level.WARN) {
                n++;
            }
        }
        return n;
    }
}
