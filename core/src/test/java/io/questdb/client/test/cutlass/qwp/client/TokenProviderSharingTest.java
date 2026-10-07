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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.TokenProviderRegistry;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.test.cutlass.auth.TestTokenProviderFactory;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Conformance test C17 of the dynamic-credential specification (design/qwp-token-provider-spec.md, sections 7.4
 * and 10): senders and query clients built from one {@code token_provider} connect string share one provider,
 * with one fetch per refresh, over real TLS connections.
 */
public class TokenProviderSharingTest {
    private static final long HOUR = 3_600_000L;

    @After
    public void tearDown() {
        TestTokenProviderFactory.uninstall();
    }

    @Test(timeout = 60_000)
    public void testFacadePoolsShareOneProvider() throws Exception {
        assertMemoryLeak(() -> {
            TestTokenProviderFactory azure = new TestTokenProviderFactory("azure");
            TestTokenProviderFactory.install(azure);
            try (TestWebSocketServer server = startTlsServer()) {
                String cfg = config(server, "c17-facade") + "sender_pool_min=2;query_pool_min=2;";
                TokenProviderSpec spec = spec(cfg);
                try (QuestDB ignored = QuestDB.connect(cfg)) {
                    Assert.assertEquals("one provider for the whole facade", 1, azure.created.get());
                    Assert.assertEquals("one fetch for every pooled connection", 1, azure.fetches.get());
                    Assert.assertEquals("a lease per pooled connection", 4, TokenProviderRegistry.global().leaseCount(spec));
                    for (int i = 0; i < 4; i++) {
                        Assert.assertEquals("Bearer TOKEN-azure", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    }
                }
                Assert.assertEquals("every pooled connection released its lease",
                        0, TokenProviderRegistry.global().leaseCount(spec));
                Assert.assertTrue("the provider lingers for the next client", TokenProviderRegistry.global().isActive(spec));
            }
        });
    }

    @Test(timeout = 60_000)
    public void testSendersAndQueryClientsShareOneProviderWithOneFetchPerRefresh() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger issued = new AtomicInteger();
            TestTokenProviderFactory azure = new TestTokenProviderFactory("azure",
                    p -> new ExpiringToken("SHARED-" + issued.incrementAndGet(), System.currentTimeMillis() + HOUR));
            TestTokenProviderFactory.install(azure);
            try (TestWebSocketServer server = startTlsServer()) {
                String cfg = config(server, "c17-clients");
                TokenProviderSpec spec = spec(cfg);
                List<Sender> senders = new ArrayList<>();
                List<QwpQueryClient> queries = new ArrayList<>();
                try {
                    for (int i = 0; i < 3; i++) {
                        senders.add(Sender.fromConfig(cfg + "sender_id=s" + i + ';'));
                    }
                    for (int i = 0; i < 2; i++) {
                        QwpQueryClient client = QwpQueryClient.fromConfig(cfg);
                        queries.add(client);
                        client.connect();
                    }
                    Assert.assertEquals("one provider for every client", 1, azure.created.get());
                    Assert.assertEquals("one fetch for all five connections", 1, azure.fetches.get());
                    Assert.assertEquals(5, TokenProviderRegistry.global().leaseCount(spec));
                    for (int i = 0; i < 5; i++) {
                        Assert.assertEquals("Bearer SHARED-1", server.pollAuthorizationHeader(5, TimeUnit.SECONDS));
                    }

                    // One refresh serves everyone: force one through the shared provider, then drop every
                    // connection. The senders reconnect with the new token without fetching again.
                    try (TokenProviderRegistry.Lease probe = TokenProviderRegistry.global().acquire(spec)) {
                        probe.provider().onTokenRejected("SHARED-1", 401);
                        Assert.assertEquals("SHARED-2", probe.provider().getToken().toString());
                    }
                    Assert.assertEquals(2, azure.fetches.get());
                    server.dropAllConnections();
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals("Bearer SHARED-2", server.pollAuthorizationHeader(10, TimeUnit.SECONDS));
                    }
                    // the reconnects completed: every sender delivers on its new connection
                    for (Sender s : senders) {
                        s.table("t").longColumn("v", 1).atNow();
                        s.flush();
                        Assert.assertTrue("the sender must deliver after reconnecting", s.awaitAckedFsn(0, 10_000));
                    }
                    Assert.assertEquals("reconnects reuse the cached token", 2, azure.fetches.get());
                } finally {
                    for (Sender s : senders) {
                        s.close();
                    }
                    for (QwpQueryClient q : queries) {
                        q.close();
                    }
                }
                Assert.assertEquals(0, TokenProviderRegistry.global().leaseCount(spec));
                Assert.assertTrue(TokenProviderRegistry.global().isActive(spec));
            }
        });
    }

    private static String config(TestWebSocketServer server, String identity) {
        // a resource unique to the test keeps the process-wide registry entry from being shared across tests
        return "wss::addr=localhost:" + server.getPort() + ";tls_verify=unsafe_off;token_provider=azure;"
                + "azure_resource=api://" + identity + '-' + System.nanoTime() + ';';
    }

    private static TokenProviderSpec spec(String cfg) {
        return TokenProviderSpec.parse(new ConfigView(ConfigString.parse(cfg)), true);
    }

    private static TestWebSocketServer startTlsServer() throws Exception {
        TestWebSocketServer server = TestWebSocketServer.tls(new WebSocketDynamicCredentialTest.AckHandler());
        server.setSendServerInfo(true);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }
}
