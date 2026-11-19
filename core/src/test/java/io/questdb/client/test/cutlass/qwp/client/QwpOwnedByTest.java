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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * The {@code owned_by} option: the owner of the tables and columns a sender auto-creates, sent as
 * {@code X-QWP-Owned-By} on every WebSocket upgrade, and confirmed by the server's echo.
 */
public class QwpOwnedByTest {
    private static final String OWNER = "caf\u00e9 team";
    private static final String OWNER_ENCODED = "caf%C3%A9%20team";

    @Test
    public void testEncodeOwnedBy() {
        // RFC 3986 unreserved characters stay as they are
        Assert.assertEquals("Team-A_1.x~", WebSocketClient.encodeOwnedBy("Team-A_1.x~"));
        // everything else is percent-encoded UTF-8 with upper-case hex digits, '%' and '+' included
        Assert.assertEquals("a%20b%2Bc%25d%2F%C3%A9%E8%A1%A8", WebSocketClient.encodeOwnedBy("a b+c%d/\u00e9\u8868"));
    }

    @Test
    public void testNoHeaderWithoutOwnedBy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                try (WebSocketClient client = upgradedClient(server, null)) {
                    Assert.assertTrue(client.isConnected());
                }
                Assert.assertEquals("", server.pollOwnedByRequest(5, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testRejectReasonCapturedFromPlainTextBody() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setRejectWithStatus(400, "Bad Request");
                server.setRejectBody("owned_by refused: access control is disabled\r\n");
                try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
                    client.setQwpOwnedBy(OWNER);
                    client.connect("127.0.0.1", server.getPort());
                    try {
                        client.upgrade("/write/v4", null);
                        Assert.fail("expected the upgrade to fail");
                    } catch (WebSocketUpgradeException e) {
                        Assert.assertEquals(400, e.getStatusCode());
                        Assert.assertEquals("owned_by refused: access control is disabled", client.getUpgradeRejectReason());
                        TestUtils.assertContains(e.getMessage(),
                                "WebSocket upgrade failed: HTTP/1.1 400 Bad Request [reason=owned_by refused: access control is disabled]");
                    }
                }
            }
        });
    }

    @Test
    public void testRejectReasonNullWithoutBody() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setRejectWithStatus(400, "Bad Request");
                try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
                    client.connect("127.0.0.1", server.getPort());
                    try {
                        client.upgrade("/write/v4", null);
                        Assert.fail("expected the upgrade to fail");
                    } catch (WebSocketUpgradeException e) {
                        Assert.assertNull(client.getUpgradeRejectReason());
                        Assert.assertEquals("WebSocket upgrade failed: HTTP/1.1 400 Bad Request", e.getMessage());
                    }
                }
            }
        });
    }

    @Test
    public void testSenderFailsOnRefusedOwner() throws Exception {
        // The server refuses an owner the principal may not name with a 403 and the reason. The sender fails at
        // build, and the reason tells the refusal apart from a rejected credential.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setRejectWithStatus(403, "Forbidden");
                server.setRejectBody("owned_by refused: bob is not part of group team");
                try (Sender ignored = Sender.fromConfig(senderConfig(server))) {
                    Assert.fail("expected build to fail");
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals(403, e.getStatusCode());
                    Assert.assertEquals("owned_by refused: bob is not part of group team", e.getReason());
                    TestUtils.assertContains(e.getMessage(),
                            "auth-rejected: WebSocket upgrade rejected with HTTP 403 for localhost:" + server.getPort()
                                    + " [reason=owned_by refused: bob is not part of group team]");
                }
            }
        });
    }

    @Test
    public void testSenderFailsWhenServerIgnoresOwnedBy() throws Exception {
        // A server that does not know the header completes the upgrade without the echo. The tables the sender
        // auto-created there would not get the owner, so build fails rather than connecting.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setSuppressOwnedByEcho(true);
                try (Sender ignored = Sender.fromConfig(senderConfig(server))) {
                    Assert.fail("expected build to fail");
                } catch (WebSocketUpgradeException e) {
                    TestUtils.assertContains(fullMessage(e), "WebSocket upgrade failed: server did not confirm owned_by, "
                            + "it cannot set the owner of auto-created tables and columns [owned_by=" + OWNER
                            + ", echoed=<no X-QWP-Owned-By header>]");
                }
                Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(5, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testSenderSendsOwnedBy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                try (Sender ignored = Sender.fromConfig(senderConfig(server))) {
                    Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(5, TimeUnit.SECONDS));
                }
                try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + server.getPort())
                        .ownedBy("team_a")
                        .build()) {
                    Assert.assertEquals("team_a", server.pollOwnedByRequest(5, TimeUnit.SECONDS));
                }
            }
        });
    }

    @Test
    public void testUpgradeAcceptsEcho() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                try (WebSocketClient client = upgradedClient(server, OWNER)) {
                    Assert.assertTrue(client.isConnected());
                }
                Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(5, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testUpgradeFailsOnMismatchedEcho() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setOwnedByEchoValue("other");
                assertUpgradeFailsOnEcho(server, "echoed=other]");
            }
        });
    }

    @Test
    public void testUpgradeFailsOnMissingEcho() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setSuppressOwnedByEcho(true);
                assertUpgradeFailsOnEcho(server, "echoed=<no X-QWP-Owned-By header>]");
            }
        });
    }

    private static void assertUpgradeFailsOnEcho(TestWebSocketServer server, String expectedEcho) {
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
            client.setQwpOwnedBy(OWNER);
            client.connect("127.0.0.1", server.getPort());
            try {
                client.upgrade("/write/v4", null);
                Assert.fail("expected the upgrade to fail");
            } catch (WebSocketUpgradeException e) {
                // a non-421 upgrade error, which the connect walk latches as terminal
                Assert.assertFalse(e.isRoleMismatch());
                Assert.assertFalse(client.isConnected());
                TestUtils.assertContains(e.getMessage(), "WebSocket upgrade failed: server did not confirm owned_by, "
                        + "it cannot set the owner of auto-created tables and columns [owned_by=" + OWNER + ", " + expectedEcho);
            }
        }
    }

    private static String fullMessage(Throwable e) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            sb.append(t.getMessage()).append(" <- ");
        }
        return sb.toString();
    }

    private static String senderConfig(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";owned_by=" + OWNER + ";";
    }

    private static TestWebSocketServer startServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
        });
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static WebSocketClient upgradedClient(TestWebSocketServer server, String ownedBy) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        try {
            client.setQwpOwnedBy(ownedBy);
            client.connect("127.0.0.1", server.getPort());
            client.upgrade("/write/v4", null);
            return client;
        } catch (Throwable th) {
            client.close();
            throw th;
        }
    }
}
