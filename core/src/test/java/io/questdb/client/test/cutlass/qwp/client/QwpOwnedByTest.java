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

import io.questdb.client.ConnectionHealth;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpOwnedByMismatchException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;

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
        // the ends of the unreserved ranges stay as they are, the characters just outside them do not: the server
        // re-encodes the owner it echoes, so the two sides must draw these lines at the same characters
        Assert.assertEquals("AZaz09", WebSocketClient.encodeOwnedBy("AZaz09"));
        Assert.assertEquals("%40%5B%60%7B%2F%3A", WebSocketClient.encodeOwnedBy("@[`{/:"));
    }

    @Test
    public void testHealthReportsUnconfirmedOwnedByWithoutStatus() throws Exception {
        // A running sender keeps retrying an endpoint that stops confirming the owner. That endpoint answered 101,
        // so health reports the failure without an HTTP status, as it does a missing durable-ack echo.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                try (Sender sender = Sender.fromConfig(senderConfig(server)
                        + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=20;")) {
                    Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(5, TimeUnit.SECONDS));
                    server.setOwnedByEchoSuppressed(true);
                    server.dropAllConnections();
                    await(() -> {
                        ConnectionHealth.Failure failure = sender.health().getLastFailure();
                        return failure != null && failure.getMessage().contains("did not confirm owned_by");
                    }, 10_000, "a reconnect round that fails on the missing owned_by echo");
                    ConnectionHealth health = sender.health();
                    Assert.assertEquals(health.toString(), ConnectionHealth.State.RECONNECTING, health.getState());
                    Assert.assertEquals(health.toString(), ConnectionHealth.FailureClass.OTHER,
                            health.getLastFailure().getFailureClass());
                    Assert.assertEquals(health.toString(), 0, health.getLastFailure().getStatusCode());
                }
            }
        });
    }

    @Test
    public void testMixedRoundReportsRefusedOwner() throws Exception {
        // A replica's 421 does not hide the primary's refusal of the owner: the round reports the refusal, as it does
        // with the primary alone, not a missing primary.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer replica = startServer(); TestWebSocketServer primary = startServer()) {
                replica.setRejectWithRole("REPLICA");
                primary.setRejectWithStatus(403, "Forbidden");
                primary.setRejectBody("owned_by refused: bob is not part of group team");
                try (Sender ignored = Sender.fromConfig(mixedRoundConfig(replica, primary))) {
                    Assert.fail("expected build to fail");
                } catch (QwpAuthFailedException e) {
                    Assert.assertEquals(403, e.getStatusCode());
                    Assert.assertEquals("owned_by refused: bob is not part of group team", e.getReason());
                }
            }
        });
    }

    @Test
    public void testMixedRoundReportsUnconfirmedOwner() throws Exception {
        // A replica's 421 does not turn a primary that answered 101 without the owned_by echo into a missing primary:
        // the round did find a primary, one that cannot set the owner, and reports that, as it does with the primary
        // alone.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer replica = startServer(); TestWebSocketServer primary = startServer()) {
                replica.setRejectWithRole("REPLICA");
                primary.setOwnedByEchoSuppressed(true);
                try (Sender ignored = Sender.fromConfig(mixedRoundConfig(replica, primary))) {
                    Assert.fail("expected build to fail");
                } catch (QwpOwnedByMismatchException e) {
                    TestUtils.assertContains(e.getMessage(), "WebSocket upgrade failed: server did not confirm owned_by, "
                            + "it cannot set the owner of auto-created tables and columns [owned_by=" + OWNER
                            + ", echoed=<no X-QWP-Owned-By header>]");
                }
                Assert.assertEquals(OWNER_ENCODED, primary.pollOwnedByRequest(5, TimeUnit.SECONDS));
            }
        });
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
    public void testRejectReasonNullUnlessBodyIsUsable() throws Exception {
        // The reason is best effort: a text/plain body that arrived whole with the headers. Anything else yields
        // none rather than a guess, and above all no byte past the ones received.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setRejectWithStatus(400, "Bad Request");
                // body, Content-Type (null for text/plain), Content-Length (null for the length of the body)
                String[][] cases = {
                        {"owned_by refused", null, "1000"},
                        {"owned_by refused", null, "abc"},
                        {"owned_by refused", null, "-1"},
                        {"owned_by refused", null, "0"},
                        {"<p>owned_by refused</p>", "text/html", null},
                        {" \r\n\t ", null, null}
                };
                for (String[] c : cases) {
                    server.setRejectBody(c[0]);
                    server.setRejectBodyHeaders(c[1], c[2]);
                    assertBadRequestReason(server, null);
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
    public void testRejectReasonSanitized() throws Exception {
        // Whatever answers the upgrade, a proxy included, writes the reason, so it reaches messages and logs only
        // as display-safe text: without control and format characters, and at most 256 characters long.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                server.setRejectWithStatus(400, "Bad Request");
                server.setRejectBody("owned_by refused:\r\n\u0007bob\u202E is\u200B not part of group caf\u00e9");
                server.setRejectBodyHeaders("text/plain; charset=utf-8", null);
                assertBadRequestReason(server, "owned_by refused:bob is not part of group caf\u00e9");

                StringBuilder body = new StringBuilder();
                for (int i = 0; i < 300; i++) {
                    body.append('x');
                }
                server.setRejectBody(body.toString());
                server.setRejectBodyHeaders(null, null);
                // cut to 256 characters, the last three of which mark the cut
                assertBadRequestReason(server, body.substring(0, 253) + "...");
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
                server.setOwnedByEchoSuppressed(true);
                try (Sender ignored = Sender.fromConfig(senderConfig(server))) {
                    Assert.fail("expected build to fail");
                } catch (QwpOwnedByMismatchException e) {
                    TestUtils.assertContains(fullMessage(e), "WebSocket upgrade failed: server did not confirm owned_by, "
                            + "it cannot set the owner of auto-created tables and columns [owned_by=" + OWNER
                            + ", echoed=<no X-QWP-Owned-By header>]");
                }
                Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(5, TimeUnit.SECONDS));
            }
        });
    }

    @Test
    public void testSenderResendsOwnedByOnReconnect() throws Exception {
        // The owner travels in every upgrade, not only in the first one, so the tables and columns auto-created
        // over a re-established connection get it too.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startServer()) {
                try (Sender ignored = Sender.fromConfig(senderConfig(server))) {
                    Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(5, TimeUnit.SECONDS));
                    server.dropAllConnections();
                    Assert.assertEquals(OWNER_ENCODED, server.pollOwnedByRequest(10, TimeUnit.SECONDS));
                }
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
                server.setOwnedByEchoSuppressed(true);
                assertUpgradeFailsOnEcho(server, "echoed=<no X-QWP-Owned-By header>]");
            }
        });
    }

    // Upgrades against a server that rejects it with 400 Bad Request, and asserts the reason the client takes
    // from the body of the rejection, null for none.
    private static void assertBadRequestReason(TestWebSocketServer server, String expectedReason) {
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
            client.connect("127.0.0.1", server.getPort());
            try {
                client.upgrade("/write/v4", null);
                Assert.fail("expected the upgrade to fail");
            } catch (WebSocketUpgradeException e) {
                Assert.assertEquals(400, e.getStatusCode());
                Assert.assertEquals(expectedReason, client.getUpgradeRejectReason());
                Assert.assertEquals("WebSocket upgrade failed: HTTP/1.1 400 Bad Request"
                        + (expectedReason != null ? " [reason=" + expectedReason + ']' : ""), e.getMessage());
            }
        }
    }

    private static void assertUpgradeFailsOnEcho(TestWebSocketServer server, String expectedEcho) {
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
            client.setQwpOwnedBy(OWNER);
            client.connect("127.0.0.1", server.getPort());
            try {
                client.upgrade("/write/v4", null);
                Assert.fail("expected the upgrade to fail");
            } catch (QwpOwnedByMismatchException e) {
                // no status, as the server answered 101, and a non-421 upgrade error, which fails the initial
                // connect
                Assert.assertEquals(WebSocketUpgradeException.STATUS_NONE, e.getStatusCode());
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

    // A connect string that walks a replica, then a primary, in one round.
    private static String mixedRoundConfig(TestWebSocketServer replica, TestWebSocketServer primary) {
        return "ws::addr=localhost:" + replica.getPort() + ",localhost:" + primary.getPort() + ";owned_by=" + OWNER + ";";
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
