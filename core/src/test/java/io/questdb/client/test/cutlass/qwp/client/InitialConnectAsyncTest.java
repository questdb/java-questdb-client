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
 * is reachable; the I/O thread retries connect in the background. Plain
 * connect failures are retried indefinitely (Invariant B: no wall-clock
 * budget give-up); only genuine terminals (auth/upgrade reject,
 * durable-ack capability gap) are delivered through the async error
 * inbox rather than thrown at the call site.
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
                        "auth failure is TERMINAL",
                        SenderError.Policy.TERMINAL, err.getAppliedPolicy());
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
    public void testAsyncNoServerRetriesForeverNoTerminal() throws Exception {
        // INVARIANT B: an SF sender in async mode pointed at a dead port must
        // NEVER surface a connection-error terminal -- a down server is transient
        // (it may appear; the data is safe in SF), so the I/O thread retries
        // forever. reconnect_max_duration_millis is IGNORED as a give-up deadline:
        // no SenderError lands, the sender stays usable, and wasEverConnected()
        // stays false. Only a GENUINE terminal (auth/upgrade) or SF exhaustion may
        // surface -- see testAsyncAuthFailureDeliversToErrorInbox.
        int port = TestPorts.findUnusedPort();
        ErrorInbox inbox = new ErrorInbox();
        String cfg = "ws::addr=localhost:" + port
                + sfDirOpt() + ";initial_connect_retry=async"
                + ";reconnect_max_duration_millis=200"
                + ";reconnect_initial_backoff_millis=10"
                + ";reconnect_max_backoff_millis=50"
                + ";close_flush_timeout_millis=0;";
        Sender sender = Sender.builder(cfg)
                .errorHandler(inbox)
                .build();
        try {
            // Observe well past the (ignored) 200ms budget: no terminal lands.
            Assert.assertFalse(
                    "async SF sender must NOT surface a connection-error terminal "
                            + "(Invariant B: retries forever past the budget)",
                    inbox.await(1500, TimeUnit.MILLISECONDS));
            Assert.assertNull("no SenderError may be delivered for a down server", inbox.get());
            // Sender stays usable -- producer keeps appending to SF.
            sender.table("foo").longColumn("v", 1L).atNow();
            sender.flush();
            Assert.assertFalse(
                    "wasEverConnected() stays false while no server is reachable",
                    ((QwpWebSocketSender) sender).wasEverConnected());
            // LIVENESS: no-terminal-in-window alone cannot distinguish
            // "retries forever" from "gave up silently" -- an I/O thread
            // that exits without latching a terminal also delivers nothing,
            // and the flush above lands in SF regardless of wire liveness.
            // The retry loop bumps getTotalReconnectAttempts() on every
            // iteration, so the counter advancing NOW -- long past the
            // (ignored) 200ms budget -- proves the loop is still running.
            awaitReconnectAttemptsAdvance((QwpWebSocketSender) sender);
        } finally {
            sender.close();
        }
    }

    @Test
    public void testAsyncDeliversBufferedRowsWhenServerArrivesLate() throws Exception {
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
            // fromConfig/flush/setup failures must fail the test -- only
            // close() teardown noise is tolerated (see closeQuietly).
            Sender sender = Sender.fromConfig(cfg);
            try {
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
            } finally {
                closeQuietly(sender);
            }
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
    public void testConnectionLostRetriesForeverNoTerminal() throws Exception {
        // INVARIANT B: after a successful connect, if the server drops, the
        // mid-stream reconnect must retry FOREVER -- it must NEVER surface a
        // connection-lost terminal on a wall-clock budget. The rows are safe in
        // SF and the server may return, so reconnect_max_duration_millis is
        // ignored as a give-up deadline. wasEverConnected() stays true.
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));

            ErrorInbox inbox = new ErrorInbox();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=200"
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

                // Tear the server down. The I/O loop discovers the disconnect and
                // enters reconnect -- which must retry forever, NOT surface a
                // terminal on the (ignored) 200ms budget.
                server.close();
                Assert.assertFalse(
                        "mid-stream reconnect must NOT surface a connection-lost terminal "
                                + "(Invariant B: retries forever past the budget)",
                        inbox.await(1500, TimeUnit.MILLISECONDS));
                Assert.assertNull("no terminal may be delivered on a transient outage", inbox.get());
                Assert.assertTrue(
                        "wasEverConnected() must remain true after the outage",
                        ((QwpWebSocketSender) sender).wasEverConnected());
                // LIVENESS: same discriminator as the async-init test above.
                // The mid-stream reconnect loop must still be making connect
                // attempts long past the (ignored) 200ms budget -- not merely
                // failing to report that it stopped.
                awaitReconnectAttemptsAdvance((QwpWebSocketSender) sender);
            } finally {
                // closeQuietly (not a bare close()) so a close-path exception
                // cannot replace a pending AssertionError from the contract
                // assertions above and mask a genuine failure.
                closeQuietly(sender);
            }
        }
    }

    @Test
    public void testWasEverConnectedTrueImmediatelyInSyncMode() throws Exception {
        // Default (OFF) and SYNC modes both connect on the user thread
        // before fromConfig returns. wasEverConnected() must therefore
        // already be true the instant the sender becomes visible to the
        // caller — there is no observable "never connected" window in
        // those modes.
        try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, java.util.concurrent.TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=0;";
            Sender sender = Sender.fromConfig(cfg);
            try {
                Assert.assertTrue(
                        "wasEverConnected() must be true immediately in OFF/SYNC mode",
                        ((QwpWebSocketSender) sender).wasEverConnected());
            } finally {
                closeQuietly(sender);
            }
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
     * Proves the I/O thread's retry loop is still ALIVE rather than merely
     * quiet: snapshots {@link QwpWebSocketSender#getTotalReconnectAttempts()}
     * and spins until it advances past the snapshot. Callers invoke this
     * after the (ignored) reconnect budget has already elapsed, so a stuck
     * counter means the loop gave up silently -- exactly the Invariant B
     * regression a no-terminal-in-window assertion cannot see. A plain
     * {@code attempts >= 1} check would NOT discriminate: attempts made
     * before the budget expired would satisfy it even if the loop then
     * exited.
     */
    private static void awaitReconnectAttemptsAdvance(QwpWebSocketSender wss) {
        long snapshot = wss.getTotalReconnectAttempts();
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (wss.getTotalReconnectAttempts() <= snapshot) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError(
                        "Invariant B violation: reconnect attempts stuck at "
                                + snapshot + " for 5s past the budget -- the I/O "
                                + "thread stopped retrying without surfacing a terminal");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    /**
     * Closes the sender, tolerating close-path teardown noise only. Used
     * instead of a broad {@code catch (Exception ignored)} around a whole
     * test body, which would swallow fromConfig/flush/setup failures and
     * let the contract assertions pass vacuously.
     */
    private static void closeQuietly(Sender sender) {
        try {
            sender.close();
        } catch (Exception ignored) {
            // close() teardown noise only
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
     * string. The builder does NOT require sf_dir for any
     * initial_connect_retry mode — without it the sender builds in
     * memory mode and buffers rows in the in-RAM cursor ring. These
     * tests set an sf_dir so the rows accumulated before the first
     * successful connect are disk-backed (the durable SF path).
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
