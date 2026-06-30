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
import org.jetbrains.annotations.NotNull;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
        try (Always401Fixture fixture = new Always401Fixture()) {
            fixture.start();
            int port = fixture.getPort();
            ErrorInbox inbox = new ErrorInbox();
            String cfg = "ws::addr=localhost:" + port
                    + sfDirOpt() + ";initial_connect_retry=async"
                    + ";reconnect_max_duration_millis=10000"
                    + ";close_flush_timeout_millis=0;";
            Sender sender = Sender.builder(cfg)
                    .errorHandler(inbox)
                    .build();
            try {
                // Auth-terminal must surface within hundreds of ms even
                // though the cap is 10s.
                long t0 = System.nanoTime();
                Assert.assertTrue(
                        "401 upgrade reject must surface a SenderError within 5s",
                        inbox.await(5, TimeUnit.SECONDS));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                SenderError err = inbox.get();
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
        ErrorInbox inbox = new ErrorInbox();
        String cfg = "ws::addr=localhost:" + port
                + sfDirOpt() + ";initial_connect_retry=async"
                + ";reconnect_max_duration_millis=400"
                + ";reconnect_initial_backoff_millis=10"
                + ";reconnect_max_backoff_millis=50"
                + ";close_flush_timeout_millis=0;";
        Sender sender = Sender.builder(cfg)
                .errorHandler(inbox)
                .build();
        try {
            // Wait up to 5s for the I/O thread to exhaust its budget.
            Assert.assertTrue(
                    "async budget exhaustion must surface a SenderError within 5s",
                    inbox.await(5, TimeUnit.SECONDS));
            SenderError err = inbox.get();
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
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + sfDirOpt() + ";initial_connect_retry=async"
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=20"
                    + ";reconnect_max_backoff_millis=200"
                    + ";close_flush_timeout_millis=2000;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                // wasEverConnected starts false in async mode — the I/O
                // thread has not yet completed an upgrade.
                Assert.assertFalse(
                        "wasEverConnected() must be false before the I/O thread connects",
                        ((QwpWebSocketSender) sender).wasEverConnected());

                // Append before the server exists.
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();

                // Server starts AFTER the producer has published AND after
                // the I/O thread has registered at least one failed connect
                // attempt — that's what makes "server arrives late" the
                // scenario under test rather than "server is already up".
                awaitAtLeastOneConnectAttempt(wss);
                server.start();
                Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));

                // Wait up to 5s for the buffered frame to land + ACK.
                Assert.assertTrue(
                        "buffered frame must be delivered once server is up",
                        handler.awaitFirstAck(5, TimeUnit.SECONDS));
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
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));

            ErrorInbox inbox = new ErrorInbox();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=400"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";close_flush_timeout_millis=0;";
            Sender sender = Sender.builder(cfg)
                    .errorHandler(inbox)
                    .build();
            try {
                // Confirm we connected and got an ACK.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                Assert.assertTrue("expected at least one ACK before tearing down server",
                        handler.awaitFirstAck(5, TimeUnit.SECONDS));
                Assert.assertTrue(
                        "wasEverConnected() must be true after a successful connect",
                        ((QwpWebSocketSender) sender).wasEverConnected());

                // Tear the server down. The cursor I/O loop's tryReceiveAcks
                // polls every 50us and discovers the peer disconnect on its
                // own, then enters the reconnect loop and exhausts the
                // 400ms budget — no producer activity required.
                server.close();
                Assert.assertTrue("budget exhaustion must surface a SenderError within 5s",
                        inbox.await(5, TimeUnit.SECONDS));
                SenderError err = inbox.get();
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
        try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
            int port = server.getPort();
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
     * Spins on {@link QwpWebSocketSender#getTotalReconnectAttempts()} until
     * the I/O thread has logged at least one connect attempt. The
     * connectLoop bumps that counter on every iteration of the retry loop —
     * including the async-initial-connect path — so seeing it advance is a
     * deterministic signal that the I/O thread has tried (and so far failed)
     * to reach a server.
     */
    private static void awaitAtLeastOneConnectAttempt(QwpWebSocketSender wss) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (wss.getTotalReconnectAttempts() < 1L) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError(
                        "I/O thread did not log a connect attempt within 5s");
            }
            io.questdb.client.std.Compat.onSpinWait();
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
     * Acks every binary frame so the sender's flush completes. Latches the
     * first ACK so tests can await it deterministically instead of polling
     * {@code totalAcked} with sleeps.
     */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalAcked = new AtomicLong();
        private final CountDownLatch firstAck = new CountDownLatch(1);
        private final AtomicLong nextSeq = new AtomicLong(0);

        boolean awaitFirstAck(long timeout, TimeUnit unit) throws InterruptedException {
            return firstAck.await(timeout, unit);
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                long seq = nextSeq.getAndIncrement();
                client.sendBinary(buildAck(seq));
                totalAcked.incrementAndGet();
                firstAck.countDown();
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
     * Bridges the {@link Sender}'s async {@code errorHandler} callback to a
     * {@link CountDownLatch} so tests can block deterministically until the
     * first error lands, instead of polling an {@link AtomicReference} via
     * {@code Thread.sleep}. The reference is preserved for the assertions
     * that inspect the error's category/policy/message.
     */
    private static class ErrorInbox implements io.questdb.client.SenderErrorHandler {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<SenderError> ref = new AtomicReference<>();

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        SenderError get() {
            return ref.get();
        }

        @Override
        public void onError(@NotNull SenderError err) {
            if (ref.compareAndSet(null, err)) {
                latch.countDown();
            }
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

        Always401Fixture() throws IOException {
            // Bind the listener up front on an OS-assigned loopback port and
            // hold it for the fixture's lifetime; read it back via getPort().
            // Owning the port from allocation to teardown avoids the bind race
            // a pre-selected port would carry.
            this.serverSocket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        }

        int getPort() {
            return serverSocket.getLocalPort();
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
