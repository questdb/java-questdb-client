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
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Behavior of {@code initial_connect_retry=async}: the producer-thread
 * {@code Sender.fromConfig} must return immediately even when no server
 * is reachable; the I/O thread retries connect in the background, and
 * terminal failures (auth/upgrade reject, budget exhaustion) are
 * delivered through the async error inbox rather than thrown at the
 * call site.
 */
public class InitialConnectAsyncTest {

    @Test
    public void testAsyncAuthFailureDeliversToErrorInbox() throws Exception {
        // Server returns HTTP 401 on every upgrade attempt. Auth failures
        // are terminal at the I/O thread; in async mode they are
        // delivered as a SenderError, not thrown from fromConfig.
        int port = TestPorts.findUnusedPort();
        try (Always401Fixture fixture = new Always401Fixture(port)) {
            fixture.start();
            AtomicReference<SenderError> observedError = new AtomicReference<>();
            String cfg = "ws::addr=localhost:" + port
                    + sfDirOpt() + ";initial_connect_retry=async"
                    + ";reconnect_max_duration_millis=10000"
                    + ";close_flush_timeout_millis=0;";
            Sender sender = Sender.builder(cfg)
                    .errorHandler(observedError::set)
                    .build();
            try {
                // Auth-terminal must surface within hundreds of ms even
                // though the cap is 10s.
                long t0 = System.nanoTime();
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && observedError.get() == null) {
                    Thread.sleep(20);
                }
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                SenderError err = observedError.get();
                Assert.assertNotNull(
                        "401 upgrade reject must surface a SenderError",
                        err);
                Assert.assertTrue(
                        "auth-terminal must surface well inside the cap; took "
                                + elapsedMs + "ms (cap was 10000ms)",
                        elapsedMs < 5_000L);
                Assert.assertEquals(
                        "category must be SECURITY_ERROR for ws-upgrade-failed",
                        SenderError.Category.SECURITY_ERROR, err.getCategory());
                Assert.assertEquals(
                        "auth failure is HALT",
                        SenderError.Policy.HALT, err.getAppliedPolicy());
                String msg = err.getServerMessage() == null ? "" : err.getServerMessage();
                Assert.assertTrue(
                        "error message must mention ws-upgrade-failed: " + msg,
                        msg.contains("ws-upgrade-failed")
                                || msg.contains("401"));
            } finally {
                assertCloseRethrowsTerminal(sender, "ws-upgrade-failed");
            }
        }
    }

    @Test
    public void testAsyncBudgetExhaustionDeliversToErrorInbox() throws Exception {
        // No server. With async mode and a tight cap, the I/O thread
        // exhausts its connect budget and surfaces a SenderError to the
        // user-supplied handler. fromConfig itself does not throw; only
        // close() rethrows the latched terminal so a user who never
        // installed a handler still sees the failure on shutdown.
        int port = TestPorts.findUnusedPort();
        AtomicReference<SenderError> observedError = new AtomicReference<>();
        String cfg = "ws::addr=localhost:" + port
                + sfDirOpt() + ";initial_connect_retry=async"
                + ";reconnect_max_duration_millis=400"
                + ";reconnect_initial_backoff_millis=10"
                + ";reconnect_max_backoff_millis=50"
                + ";close_flush_timeout_millis=0;";
        Sender sender = Sender.builder(cfg)
                .errorHandler(observedError::set)
                .build();
        try {
            // Wait up to 5s for the I/O thread to exhaust its budget.
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline
                    && observedError.get() == null) {
                Thread.sleep(20);
            }
            SenderError err = observedError.get();
            Assert.assertNotNull(
                    "async budget exhaustion must surface a SenderError to the inbox",
                    err);
            Assert.assertEquals(
                    "budget exhaustion is a HALT-policy terminal",
                    SenderError.Policy.HALT, err.getAppliedPolicy());
            Assert.assertEquals(
                    "category must be PROTOCOL_VIOLATION for budget exhaustion",
                    SenderError.Category.PROTOCOL_VIOLATION, err.getCategory());
            String msg = err.getServerMessage() == null ? "" : err.getServerMessage();
            Assert.assertTrue(
                    "error message must use never-connected tag (no successful connect): " + msg,
                    msg.contains("never-connected-budget-exhausted"));
            Assert.assertTrue(
                    "error message must hint at config-likely cause: " + msg,
                    msg.contains("never reached the server"));
            Assert.assertFalse(
                    "wasEverConnected() must be false when no connect ever succeeded",
                    ((QwpWebSocketSender) sender).wasEverConnected());
        } finally {
            assertCloseRethrowsTerminal(sender,
                    "never-connected-budget-exhausted");
        }
    }

    @Test
    public void testAsyncDeliversBufferedRowsWhenServerArrivesLate() {
        // Sender opens before the server is listening. Frames are
        // appended to the cursor SF engine on the producer thread. The
        // I/O thread retries connect in the background; once the server
        // comes up, the buffered frame is sent and ACKed.
        int port = TestPorts.findUnusedPort();
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            String cfg = "ws::addr=localhost:" + port
                    + sfDirOpt() + ";initial_connect_retry=async"
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=20"
                    + ";reconnect_max_backoff_millis=200"
                    + ";close_flush_timeout_millis=2000;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                // wasEverConnected starts false in async mode — the I/O
                // thread has not yet completed an upgrade.
                Assert.assertFalse(
                        "wasEverConnected() must be false before the I/O thread connects",
                        ((QwpWebSocketSender) sender).wasEverConnected());

                // Append before the server exists.
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();

                // Server starts AFTER the producer has already published.
                Thread.sleep(150);
                server.start();
                Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));

                // Wait up to 5s for the buffered frame to land + ACK.
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && handler.totalAcked.get() < 1L) {
                    Thread.sleep(20);
                }
                Assert.assertTrue(
                        "buffered frame must be delivered once server is up",
                        handler.totalAcked.get() >= 1L);
                // Once the I/O thread completes its upgrade, the sticky
                // flag flips to true.
                Assert.assertTrue(
                        "wasEverConnected() must flip to true after the I/O thread connects",
                        ((QwpWebSocketSender) sender).wasEverConnected());
            }
        } catch (Exception ignored) {
            // already closed
        }
    }

    @Test
    public void testAsyncReturnsImmediatelyWithNoServer() {
        // No server. With async mode, fromConfig must return fast — the
        // I/O thread will keep retrying in the background until cap, but
        // the producer is unblocked. A 60s cap would normally hang
        // anything that waited on connect; we assert a sub-second
        // construction time.
        int port = TestPorts.findUnusedPort();
        long t0 = System.nanoTime();
        String cfg = "ws::addr=localhost:" + port
                + sfDirOpt() + ";initial_connect_retry=async"
                + ";reconnect_max_duration_millis=60000"
                + ";reconnect_initial_backoff_millis=10"
                + ";reconnect_max_backoff_millis=50"
                + ";close_flush_timeout_millis=0;";
        try (Sender sender = Sender.fromConfig(cfg)) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            Assert.assertTrue(
                    "fromConfig must return immediately in async mode (took " + elapsedMs + "ms)",
                    elapsedMs < 2_000L);
            // Producer-thread API works without a live wire — frames
            // accumulate on the cursor SF engine while the I/O thread
            // is still trying to connect.
            sender.table("foo").longColumn("v", 1L).atNow();
            sender.flush();
        }
    }

    @Test
    public void testConnectionLostBudgetExhaustionTagsDifferently() {
        // Server is up at first (initial connect succeeds + ACKs one
        // batch), then we tear it down. The I/O loop tries to reconnect,
        // every attempt hits TCP refused, and the budget exhausts.
        // Because the loop did connect at least once before the outage,
        // the SenderError must use the connection-lost tag and the sender
        // must report wasEverConnected()==true.
        int port = TestPorts.findUnusedPort();
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));

            AtomicReference<SenderError> observedError = new AtomicReference<>();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=400"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";close_flush_timeout_millis=0;";
            Sender sender = Sender.builder(cfg)
                    .errorHandler(observedError::set)
                    .build();
            try {
                // Confirm we connected and got an ACK.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && handler.totalAcked.get() < 1L) {
                    Thread.sleep(20);
                }
                Assert.assertTrue("expected at least one ACK before tearing down server",
                        handler.totalAcked.get() >= 1L);
                Assert.assertTrue(
                        "wasEverConnected() must be true after a successful connect",
                        ((QwpWebSocketSender) sender).wasEverConnected());

                // Tear the server down; subsequent reconnects will exhaust
                // the budget.
                server.close();

                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && observedError.get() == null) {
                    try {
                        sender.table("foo").longColumn("v", 2L).atNow();
                        sender.flush();
                    } catch (Throwable ignored) {
                        // Producer-side throw is fine; we want the inbox
                        // delivery either way.
                    }
                    Thread.sleep(50);
                }
                SenderError err = observedError.get();
                Assert.assertNotNull("budget exhaustion must surface a SenderError", err);
                String msg = err.getServerMessage() == null ? "" : err.getServerMessage();
                Assert.assertTrue(
                        "error message must use connection-lost tag: " + msg,
                        msg.contains("connection-lost-budget-exhausted"));
                Assert.assertTrue(
                        "error message must hint at transient cause: " + msg,
                        msg.contains("server unreachable since last connect"));
                Assert.assertTrue(
                        "wasEverConnected() must remain true after the outage",
                        ((QwpWebSocketSender) sender).wasEverConnected());
            } finally {
                assertCloseRethrowsTerminal(sender, "connection-lost-budget-exhausted");
            }
        } catch (Exception ignored) {
            // already closed
        }
    }

    @Test
    public void testWasEverConnectedTrueImmediatelyInSyncMode() {
        // Default (OFF) and SYNC modes both connect on the user thread
        // before fromConfig returns. wasEverConnected() must therefore
        // already be true the instant the sender becomes visible to the
        // caller — there is no observable "never connected" window in
        // those modes, so misclassifying a budget exhaustion as
        // never-connected is impossible.
        int port = TestPorts.findUnusedPort();
        try (TestWebSocketServer server = new TestWebSocketServer(port, new AckHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=0;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                Assert.assertTrue(
                        "wasEverConnected() must be true immediately in OFF/SYNC mode",
                        ((QwpWebSocketSender) sender).wasEverConnected());
            }
        } catch (Exception ignored) {
            // already closed
        }
    }

    /**
     * Closes the sender and tolerates either outcome:
     * * close() throws -- the latched terminal must mention the expected
     * substring (safety-net rethrow path);
     * * close() returns cleanly -- the user installed an async error
     * handler in this test, so the dispatcher already delivered the
     * error to the handler (or will, on shutdown). Rethrowing on top
     * of that would mask try-with-resources cleanup in real callers,
     * so close() suppresses the rethrow when a custom handler is
     * installed.
     * Either way, the inbox observation earlier in the test pins the
     * primary contract -- this helper just guards against close() throwing
     * with a wrong message.
     */
    private static void assertCloseRethrowsTerminal(Sender sender, String expectedSubstring) {
        try {
            sender.close();
        } catch (Throwable t) {
            String msg = t.getMessage() == null ? "" : t.getMessage();
            Assert.assertTrue(
                    "close() rethrow must mention " + expectedSubstring + ": " + msg,
                    msg.contains(expectedSubstring));
        }
    }

    /**
     * Returns a unique temp sf_dir snippet for embedding in a config
     * string. initial_connect_retry on/sync/async requires sf_dir per
     * spec §3.5; without it the builder rejects construction.
     */
    private static String sfDirOpt() {
        String dir = java.nio.file.Paths.get(
                System.getProperty("java.io.tmpdir"),
                "qdb-async-" + System.nanoTime()).toString();
        return ";sf_dir=" + dir;
    }

    /**
     * Acks every binary frame so the sender's flush completes.
     */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalAcked = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                long seq = nextSeq.getAndIncrement();
                client.sendBinary(buildAck(seq));
                totalAcked.incrementAndGet();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }

    /**
     * Raw-socket fixture: every accepted connection responds with HTTP
     * 401 Unauthorized and closes. Used to drive the async-init
     * auth-terminal path: the I/O thread's first connect attempt classifies
     * the response as a terminal upgrade failure.
     */
    private static class Always401Fixture implements AutoCloseable {
        private final java.util.List<Socket> openSockets = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final ServerSocket serverSocket;
        private Thread acceptThread;
        private volatile boolean running;

        Always401Fixture(int port) throws IOException {
            this.serverSocket = new ServerSocket(port);
        }

        @Override
        public void close() {
            running = false;
            try {
                serverSocket.close();
            } catch (IOException ignored) {
                // best-effort
            }
            for (Socket s : openSockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                    // best-effort
                }
            }
            if (acceptThread != null) {
                try {
                    acceptThread.join(1_000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void acceptLoop() {
            try {
                while (running) {
                    Socket s;
                    try {
                        s = serverSocket.accept();
                    } catch (IOException e) {
                        if (!running) return;
                        throw e;
                    }
                    openSockets.add(s);
                    Thread t = new Thread(() -> handleClient(s),
                            "always401-fixture-client");
                    t.setDaemon(true);
                    t.start();
                }
            } catch (Throwable ignored) {
                // best-effort fixture
            }
        }

        private void handleClient(Socket s) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        s.getInputStream(), StandardCharsets.US_ASCII));
                OutputStream out = s.getOutputStream();
                // Drain request headers up to blank line.
                in.readLine();
                String line;
                while ((line = in.readLine()) != null && !line.isEmpty()) {
                    // discard
                }
                String resp = "HTTP/1.1 401 Unauthorized\r\n"
                        + "Content-Length: 0\r\n"
                        + "Connection: close\r\n\r\n";
                out.write(resp.getBytes(StandardCharsets.US_ASCII));
                out.flush();
                s.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }

        void start() {
            running = true;
            acceptThread = new Thread(this::acceptLoop, "always401-fixture-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }
    }
}
