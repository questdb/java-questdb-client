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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenProviderRegistry;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Conformance tests C18 (connect-string validation) and C19 (registry lifecycle) of the dynamic-credential
 * specification (design/qwp-token-provider-spec.md, sections 7.2, 7.4 and 10).
 */
public class TokenProviderConfigTest {
    private static final String WSS = "wss::addr=localhost:9000;";
    private TestTokenProviderFactory azure;
    private TestTokenProviderFactory other;

    @Before
    public void setUp() {
        azure = new TestTokenProviderFactory("azure");
        other = new TestTokenProviderFactory("vault");
        TestTokenProviderFactory.install(azure, other);
    }

    @After
    public void tearDown() {
        TestTokenProviderFactory.uninstall();
    }

    @Test
    public void testAzureKeysAreNormalized() {
        Map<String, Object> snap = Sender.builder(WSS + "token_provider=azure;azure_resource=api://qdb-app/.default;"
                + "azure_client_id=AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE;").wsConfigSnapshotForTest();
        Assert.assertEquals("azure", snap.get("token_provider"));
        Assert.assertEquals("a trailing /.default must be stripped", "api://qdb-app", snap.get("azure_resource"));
        Assert.assertEquals("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", snap.get("azure_client_id"));

        // equivalent strings share a registry key; a different identity does not
        Assert.assertEquals(spec("token_provider=azure;azure_resource=api://qdb-app/.default;").registryKey(),
                spec("token_provider=azure;azure_resource=api://qdb-app;").registryKey());
        Assert.assertNotEquals(spec("token_provider=azure;azure_resource=api://qdb-app;").registryKey(),
                spec("token_provider=azure;azure_resource=api://qdb-app;"
                        + "azure_client_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee;").registryKey());
    }

    @Test
    public void testBothClientsAcceptTheKeys() throws Exception {
        assertMemoryLeak(() -> {
            String cfg = WSS + "token_provider=azure;azure_resource=api://qdb-app;";
            Assert.assertEquals("azure", Sender.builder(cfg).wsConfigSnapshotForTest().get("token_provider"));
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg)) {
                Map<String, Object> snap = client.configSnapshotForTest();
                Assert.assertEquals("azure", snap.get("token_provider"));
                Assert.assertEquals("api://qdb-app", snap.get("azure_resource"));
            }
        });
    }

    @Test
    public void testRegistryLifecycle() {
        // C19: lease release, linger, and re-acquiring a lease during the linger period.
        TokenProviderRegistry registry = new TokenProviderRegistry(300);
        TokenProviderSpec spec = spec("token_provider=vault;");
        TokenProviderRegistry.Lease a = registry.acquire(spec);
        TokenProviderRegistry.Lease b = registry.acquire(spec);
        RefreshingTokenProvider provider = a.provider();
        Assert.assertSame("one provider per configuration", provider, b.provider());
        Assert.assertEquals(1, other.created.get());
        Assert.assertEquals(2, registry.leaseCount(spec));
        Assert.assertTrue(provider.awaitReady(10_000));

        a.close();
        a.close(); // releasing twice is harmless
        Assert.assertEquals(1, registry.leaseCount(spec));
        b.close();
        Assert.assertEquals(0, registry.leaseCount(spec));
        Assert.assertTrue("the provider lingers after its last lease", registry.isActive(spec));
        Assert.assertFalse(provider.isClosed());

        // re-acquired during the linger: the same warm provider, and the pending close is cancelled
        TokenProviderRegistry.Lease c = registry.acquire(spec);
        Assert.assertSame(provider, c.provider());
        Assert.assertEquals(1, other.created.get());
        c.close();
        await(provider::isClosed, 10_000, "the provider to close once the linger elapses");
        Assert.assertFalse(registry.isActive(spec));

        // the next lease starts a fresh provider
        TokenProviderRegistry.Lease d = registry.acquire(spec);
        Assert.assertNotSame(provider, d.provider());
        Assert.assertEquals(2, other.created.get());
        d.close();
        await(d.provider()::isClosed, 10_000, "the second provider to close");
    }

    @Test
    public void testRegistryWithoutLingerClosesOnTheLastRelease() {
        TokenProviderRegistry registry = new TokenProviderRegistry(0);
        TokenProviderSpec spec = spec("token_provider=vault;");
        TokenProviderRegistry.Lease lease = registry.acquire(spec);
        RefreshingTokenProvider provider = lease.provider();
        lease.close();
        Assert.assertTrue(provider.isClosed());
        Assert.assertFalse(registry.isActive(spec));
    }

    @Test
    public void testRegistrySharesOnlyEquivalentConfigurations() {
        TokenProviderRegistry registry = new TokenProviderRegistry(0);
        TokenProviderRegistry.Lease a = registry.acquire(spec("token_provider=azure;azure_resource=api://one;"));
        TokenProviderRegistry.Lease b = registry.acquire(spec("token_provider=azure;azure_resource=api://one/.default;"));
        TokenProviderRegistry.Lease c = registry.acquire(spec("token_provider=azure;azure_resource=api://two;"));
        try {
            Assert.assertSame(a.provider(), b.provider());
            Assert.assertNotSame(a.provider(), c.provider());
            Assert.assertEquals(2, azure.created.get());
            Assert.assertTrue(a.provider().getName(), a.provider().getName().startsWith("azure"));
        } finally {
            a.close();
            b.close();
            c.close();
        }
    }

    @Test
    public void testValidationRules() throws Exception {
        // C18: every rule of section 7.2, on both clients, naming the offending key
        assertMemoryLeak(() -> {
            assertRejectedOnBoth(WSS + "token_provider=azure;azure_resource=x;token=abc;",
                    "token_provider cannot be combined with token");
            assertRejectedOnBoth(WSS + "token_provider=azure;azure_resource=x;username=u;password=p;",
                    "token_provider cannot be combined with username");
            assertRejectedOnBoth(WSS + "token_provider=;", "token_provider must not be empty", "supported values: [azure, vault]");
            assertRejectedOnBoth(WSS + "token_provider=kerberos;", "unsupported token_provider: kerberos",
                    "supported values: [azure, vault]");
            assertRejectedOnBoth(WSS + "token_provider=azure_imds;", "azure_imds is reserved",
                    "supported values: [azure, vault]");
            assertRejectedOnBoth(WSS + "azure_resource=api://x;", "azure_resource requires token_provider=azure");
            assertRejectedOnBoth(WSS + "azure_client_id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee;",
                    "azure_client_id requires token_provider=azure");
            assertRejectedOnBoth(WSS + "token_provider=vault;azure_resource=api://x;",
                    "azure_resource is only valid with token_provider=azure");
            assertRejectedOnBoth(WSS + "token_provider=azure;", "token_provider=azure requires azure_resource");
            assertRejectedOnBoth(WSS + "token_provider=azure;azure_resource=/.default;", "azure_resource must not be empty");
            assertRejectedOnBoth(WSS + "token_provider=azure;azure_resource=api://x;azure_client_id=not-a-guid;",
                    "invalid azure_client_id");
            assertRejectedOnBoth(WSS + "token_provider=azure;azure_resource=invalid:x;", "rejected by the factory");
            // decision D4: never over plain ws::
            assertRejectedOnBoth("ws::addr=localhost:9000;token_provider=azure;azure_resource=api://x;",
                    "token_provider requires the wss:: schema");
            // decision D9: never on a non-QWP schema
            assertRejected(() -> Sender.fromConfig("http::addr=localhost:9000;token_provider=azure;"),
                    "token_provider is only supported with the wss:: schema");
            assertRejected(() -> Sender.fromConfig("https::addr=localhost:9000;azure_resource=api://x;"),
                    "azure_resource is only supported with the wss:: schema");

            // an application-supplied provider is exclusive with token_provider
            String cfg = WSS + "token_provider=azure;azure_resource=api://x;";
            assertRejected(() -> Sender.builder(cfg).httpTokenProvider(() -> "t"),
                    "application-supplied token provider cannot be combined with token_provider");
            assertRejected(() -> Sender.builder(cfg).httpToken("t"), "token cannot be combined with token_provider");
            try (QwpQueryClient client = QwpQueryClient.fromConfig(cfg)) {
                assertRejected(() -> client.withBearerTokenProvider(() -> "t"),
                        "withBearerTokenProvider cannot be combined with token_provider");
                assertRejected(() -> client.withBearerToken("t"), "withBearerToken cannot be combined with token_provider");
            }
            assertRejected(() -> QuestDB.builder().fromConfig(cfg).httpTokenProvider(() -> "t").build(),
                    "httpTokenProvider cannot be combined with token_provider");
        });
    }

    @Test
    public void testValidationWithoutConnectingFetchesNoToken() throws Exception {
        // section 7.2: validation that does not connect - a facade checking its configuration at build time, a
        // parsed builder, a client that has not connected - must not fetch a token.
        assertMemoryLeak(() -> {
            String cfg = WSS + "token_provider=azure;azure_resource=api://validate-only;"
                    + "sender_pool_min=0;query_pool_min=0;";
            Sender.builder(cfg);
            QwpQueryClient.validateConfig(new ConfigView(ConfigString.parse(cfg)), true);
            QwpQueryClient.fromConfig(cfg).close();
            try (QuestDB ignored = QuestDB.connect(cfg)) {
                Assert.assertEquals("no provider may be started", 0, azure.created.get());
            }
            Assert.assertEquals(0, azure.created.get());
            Assert.assertEquals(0, azure.fetches.get());
        });
    }

    @Test
    public void testWithoutTheModuleAzureIsRejectedWithAHint() {
        TestTokenProviderFactory.install(); // nothing installed
        assertRejectedOnBoth(WSS + "token_provider=azure;azure_resource=api://x;",
                "token_provider=azure requires the org.questdb:questdb-client-azure module",
                "supported values: none installed");
    }

    private static void assertRejected(Runnable action, String... fragments) {
        try {
            action.run();
            Assert.fail("expected the configuration to be rejected with: " + fragments[0]);
        } catch (IllegalArgumentException | IllegalStateException | LineSenderException e) {
            for (String fragment : fragments) {
                Assert.assertTrue("[" + e.getMessage() + "] does not contain [" + fragment + ']',
                        e.getMessage().contains(fragment));
            }
        }
    }

    private static void assertRejectedOnBoth(String cfg, String... fragments) {
        assertRejected(() -> Sender.builder(cfg), fragments);
        assertRejected(() -> QwpQueryClient.fromConfig(cfg).close(), fragments);
    }

    private static TokenProviderSpec spec(String keys) {
        return TokenProviderSpec.parse(new ConfigView(ConfigString.parse(WSS + keys)), true);
    }
}
