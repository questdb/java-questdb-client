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
import io.questdb.client.cutlass.line.LineSenderException;
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
import java.security.MessageDigest;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for the reconnect machinery in {@link io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop}.
 * <p>
 * The cursor I/O loop used to treat any wire failure as terminal — first
 * disconnect = sender broken, every subsequent batch threw. Reconnect
 * machinery now handles transient drops: detect, build a fresh client
 * via the registered factory, reset wire state, and reposition the replay
 * cursor at {@code engine.ackedFsn() + 1}. Cursor frames are self-sufficient
 * (every frame carries full schema + full symbol-dict delta), so post-reconnect
 * replay needs no producer-side schema-reset signal.
 * <p>
 * This commit covers the mechanics with a single-attempt retry; backoff,
 * per-outage time cap, and auth-failure detection follow.
 */
public class ReconnectTest {

    @Test
    public void testReconnectAfterServerInducedDisconnect() throws Exception {
        // Server ACKs the first batch then closes the client connection.
        // Without reconnect, the next batch's flush() would throw. With
        // reconnect, the I/O loop opens a fresh connection (same port,
        // same server) and the second batch goes through.
        DisconnectAfterFirstAckHandler handler = new DisconnectAfterFirstAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                // Batch 1: server receives, ACKs, then closes the socket.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                // Brief pause so the I/O loop has time to see the EOF and
                // run through its reconnect path before we try to flush again.
                Thread.sleep(200);

                // Batch 2 must land on the new connection (server-side
                // counter advances) — proves the reconnect+resume worked
                // end-to-end. Producer's flush() must not throw.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
                waitFor(() -> handler.totalBinaryReceived.get() >= 2, 5_000);

                Assert.assertTrue(
                        "server must observe two distinct client connections "
                                + "(close-after-first-ACK forced reconnect): saw "
                                + handler.connectionsAccepted.get(),
                        handler.connectionsAccepted.get() >= 2);
            }
        }
    }

    @Test
    public void testReconnectNeverGivesUpInvariantB() throws Exception {
        // INVARIANT B: server is up at first (initial connect + ACK), then torn
        // down. The I/O loop enters reconnect and must retry FOREVER -- flush()
        // must keep succeeding (publishing to the in-RAM cursor ring; no sf_dir
        // is configured here), never surface a give-up / budget terminal. The
        // rows are buffered and the server may return, so
        // reconnect_max_duration_millis is ignored as a give-up deadline.
        // Because flush() succeeds on the user thread regardless of I/O-thread
        // liveness, the no-throw loop below is backed by two discriminators:
        // the reconnect-attempt counter must keep advancing past the budget
        // (loop is alive, not silently exited), and a server revived on the
        // SAME port must receive the replayed frames (retrying converts into
        // delivery).
        try (TestWebSocketServer server = new TestWebSocketServer(new AckHandler())) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=300"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";close_flush_timeout_millis=0;";
            Throwable observed = null;
            // fromConfig/first-flush/setup failures must fail the test --
            // only close() teardown noise is tolerated in the finally below.
            Sender sender = Sender.fromConfig(cfg);
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                // Tear down the server: the I/O loop gets EOF and enters
                // reconnect; every attempt hits TCP refused but must keep
                // retrying past the (ignored) 300ms budget.
                server.close();

                long deadline = System.currentTimeMillis() + 2_000;
                long iter = 0;
                while (System.currentTimeMillis() < deadline) {
                    iter++;
                    try {
                        sender.table("foo").longColumn("v", iter).atNow();
                        sender.flush();
                    } catch (Throwable t) {
                        observed = t;
                        break;
                    }
                    Thread.sleep(50);
                }

                if (observed == null) {
                    // LIVENESS: snapshot the attempt counter well past the
                    // (ignored) 300ms budget and require it to advance. A
                    // silently-exited I/O thread leaves it frozen -- the one
                    // regression the flush loop above cannot see.
                    io.questdb.client.cutlass.qwp.client.QwpWebSocketSender wss =
                            (io.questdb.client.cutlass.qwp.client.QwpWebSocketSender) sender;
                    long snapshot = wss.getTotalReconnectAttempts();
                    long spinDeadline = System.currentTimeMillis() + 5_000;
                    while (wss.getTotalReconnectAttempts() <= snapshot
                            && System.currentTimeMillis() < spinDeadline) {
                        Thread.sleep(20);
                    }
                    Assert.assertTrue(
                            "Invariant B violation: reconnect attempts stuck at "
                                    + snapshot + " for 5s past the (ignored) 300ms "
                                    + "budget -- the I/O loop stopped retrying "
                                    + "without surfacing a terminal",
                            wss.getTotalReconnectAttempts() > snapshot);

                    // RECOVERY: bring a server back on the SAME port (the
                    // requestedPort constructor exists for down-then-up
                    // outage realism). The still-retrying loop must reconnect
                    // and replay the frames that accumulated unacked during
                    // the outage -- a binary frame arriving on the revived
                    // endpoint is end-to-end proof that "retry forever"
                    // converts into delivery once the server returns.
                    ReceiveOnlyHandler revived = new ReceiveOnlyHandler();
                    try (TestWebSocketServer server2 =
                                 new TestWebSocketServer(revived, false, null, port)) {
                        server2.start();
                        Assert.assertTrue(server2.awaitStart(5, TimeUnit.SECONDS));
                        waitFor(() -> revived.totalBinaryReceived.get() >= 1, 5_000);
                    }
                }
            } finally {
                try {
                    sender.close();
                } catch (Exception ignored) {
                    // close() teardown noise -- the contract under test is the
                    // flush loop above, captured in `observed`.
                }
            }
            Assert.assertNull(
                    "mid-stream reconnect must retry forever, not surface a terminal "
                            + "(Invariant B); flush() threw: " + observed,
                    observed);
        }
    }

    @Test
    public void testTerminalUpgradeErrorAbortsReconnect() throws Exception {
        // Bespoke raw-socket fixture: first connection completes the
        // WebSocket upgrade and feeds back STATUS_OK ACKs; any subsequent
        // connection gets HTTP 401 Unauthorized — exercising the
        // auth-terminal path. With reconnect_max_duration_millis=10s and
        // a 401 happening on the very first reconnect, the cursor I/O
        // loop should surface the terminal error within hundreds of ms,
        // not after 10s.
        try (Auth401AfterFirstConnectionFixture fixture =
                     new Auth401AfterFirstConnectionFixture()) {
            fixture.start();
            int port = fixture.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=10000"
                    + ";close_flush_timeout_millis=0;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                // Wait for first connection to ACK + close
                waitFor(() -> fixture.acceptedConnections.get() >= 2, 5_000);

                long t0 = System.nanoTime();
                Throwable observed = null;
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        sender.table("foo").longColumn("v", 2L).atNow();
                        sender.flush();
                    } catch (Throwable t) {
                        observed = t;
                        break;
                    }
                    Thread.sleep(50);
                }
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                Assert.assertNotNull("expected terminal error after auth rejection",
                        observed);
                Assert.assertTrue(
                        "terminal upgrade error must surface well inside the cap; took "
                                + elapsedMs + "ms (cap was 10000ms)",
                        elapsedMs < 5_000);
                String msg = observed.getMessage() == null ? "" : observed.getMessage();
                Assert.assertTrue(
                        "error must mention the terminal upgrade failure: " + msg,
                        msg.contains("WebSocket upgrade failed")
                                || msg.contains("I/O thread failed")
                                || msg.contains("401"));
            } catch (LineSenderException ignored) {
            }
            // close() rethrows the latched terminal upgrade error
            // (commit 052f6ee). Already observed and asserted above.
        }
    }

    @Test
    public void testReplayResendsUnackedFramesAcrossReconnect() throws Exception {
        // First batch is received but the server closes the socket BEFORE
        // sending its ACK. The sender's engine has the frame at FSN 0 but
        // ackedFsn is still -1. On reconnect, the cursor must reposition at
        // FSN 0 and replay it — the new connection should observe the
        // *same* batch a second time before any new batch arrives.
        ReceiveThenDisconnectHandler handler = new ReceiveThenDisconnectHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 99L).atNow();
                sender.flush();
                // First connection received the batch and dropped without
                // ACKing → the I/O loop reconnects and replays. Wait for
                // the second connection to receive the (replayed) frame.
                waitFor(() -> handler.totalBinaryReceived.get() >= 2, 5_000);
                Assert.assertTrue(
                        "expected at least 2 binary frames across the two "
                                + "connections (replay): saw "
                                + handler.totalBinaryReceived.get(),
                        handler.totalBinaryReceived.get() >= 2);
                Assert.assertTrue(
                        "expected ≥ 2 distinct connections (reconnect): saw "
                                + handler.connectionsAccepted.get(),
                        handler.connectionsAccepted.get() >= 2);

                io.questdb.client.cutlass.qwp.client.QwpWebSocketSender wss =
                        (io.questdb.client.cutlass.qwp.client.QwpWebSocketSender) sender;
                Assert.assertTrue(
                        "getTotalFramesReplayed must count the replayed frame: saw "
                                + wss.getTotalFramesReplayed(),
                        wss.getTotalFramesReplayed() >= 1L);
            }
        }
    }

    /**
     * Polls a condition with a short sleep until it's true or the timeout
     * elapses. Throws {@link AssertionError} on timeout.
     */
    private static void waitFor(BoolCondition cond, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted");
            }
        }
        Assert.fail("waitFor timed out after " + timeoutMillis + "ms");
    }

    @FunctionalInterface
    private interface BoolCondition {
        boolean test();
    }

    /**
     * Single-server handler shared across all client connections it serves.
     * On every binary frame: ACK; if this is the first connection's first
     * frame, close the connection right after sending the ACK so the
     * sender's I/O loop has to reconnect to deliver the second batch.
     */
    private static class DisconnectAfterFirstAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler firstClient;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // First frame from a new client — record the connection.
            if (firstClient == null || firstClient != client) {
                connectionsAccepted.incrementAndGet();
                if (firstClient == null) {
                    firstClient = client;
                }
            }
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
                if (totalBinaryReceived.get() == 1) {
                    // Tear down this connection — sender must reconnect.
                    // Brief sleep so the ACK we just queued has time to flush
                    // before the socket is closed under it.
                    Thread.sleep(50);
                    client.close();
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Receives the first frame on the first connection without ACKing,
     * then closes — forcing the sender's I/O loop to reconnect and replay
     * that unacked frame on the new connection. The new connection then
     * ACKs normally, so the test can observe the replay landing.
     */
    private static class ReceiveThenDisconnectHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler firstClient;
        private boolean firstFrameDropped;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (firstClient == null || firstClient != client) {
                connectionsAccepted.incrementAndGet();
                if (firstClient == null) {
                    firstClient = client;
                }
            }
            totalBinaryReceived.incrementAndGet();
            // First frame on the first connection: drop without ACKing,
            // then close so the sender has to reconnect + replay.
            if (!firstFrameDropped && client == firstClient) {
                firstFrameDropped = true;
                try {
                    Thread.sleep(20);
                    client.close();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            // Any later frame (including the replayed one): ACK normally.
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Raw-socket WebSocket fixture: the first accepted connection
     * completes the upgrade handshake and feeds back STATUS_OK ACKs for
     * binary frames; every subsequent connection receives an HTTP 401
     * Unauthorized response and is closed. Used to exercise the cursor
     * I/O loop's auth-failure-on-reconnect terminal path.
     */
    private static class Auth401AfterFirstConnectionFixture implements AutoCloseable {
        private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        final AtomicInteger acceptedConnections = new AtomicInteger();
        private final ServerSocket serverSocket;
        private Thread acceptThread;
        private volatile boolean running;
        private final java.util.List<Socket> openSockets = new java.util.concurrent.CopyOnWriteArrayList<>();

        Auth401AfterFirstConnectionFixture() throws IOException {
            // Bind the listener up front on an OS-assigned loopback port and
            // hold it for the fixture's lifetime; read it back via getPort().
            // Owning the port from allocation to teardown avoids the bind race
            // a pre-selected port would carry.
            this.serverSocket = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        void start() {
            running = true;
            acceptThread = new Thread(this::acceptLoop, "auth401-fixture-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
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
                    int n = acceptedConnections.incrementAndGet();
                    final boolean isFirst = n == 1;
                    Thread t = new Thread(() -> handleClient(s, isFirst),
                            "auth401-fixture-client-" + n);
                    t.setDaemon(true);
                    t.start();
                }
            } catch (Throwable ignored) {
                // best-effort fixture
            }
        }

        private void handleClient(Socket s, boolean firstConnection) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        s.getInputStream(), StandardCharsets.US_ASCII));
                OutputStream out = s.getOutputStream();
                in.readLine();
                String secKey = null;
                String line;
                while ((line = in.readLine()) != null && !line.isEmpty()) {
                    if (line.regionMatches(true, 0, "Sec-WebSocket-Key:", 0, 18)) {
                        secKey = line.substring(18).trim();
                    }
                }
                if (!firstConnection) {
                    String resp = "HTTP/1.1 401 Unauthorized\r\n"
                            + "Content-Length: 0\r\n"
                            + "Connection: close\r\n\r\n";
                    out.write(resp.getBytes(StandardCharsets.US_ASCII));
                    out.flush();
                    s.close();
                    return;
                }
                // First connection: accept the upgrade properly.
                String accept = computeAcceptKey(secKey);
                String resp = "HTTP/1.1 101 Switching Protocols\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";
                out.write(resp.getBytes(StandardCharsets.US_ASCII));
                out.flush();
                // Read one binary frame, send STATUS_OK ACK, then close.
                readOneFrame(s);
                writeBinaryFrame(out, buildAck(0));
                Thread.sleep(50);
                s.close();
            } catch (Exception ignored) {
                // best-effort
            }
        }

        private static String computeAcceptKey(String secKey) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-1");
                md.update((secKey + WEBSOCKET_GUID).getBytes(StandardCharsets.US_ASCII));
                return Base64.getEncoder().encodeToString(md.digest());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static void readOneFrame(Socket s) throws IOException {
            java.io.InputStream raw = s.getInputStream();
            int b0 = raw.read();
            int b1 = raw.read();
            if (b0 < 0 || b1 < 0) return;
            int lenField = b1 & 0x7F;
            long payloadLen;
            if (lenField <= 125) {
                payloadLen = lenField;
            } else if (lenField == 126) {
                payloadLen = ((raw.read() & 0xFF) << 8) | (raw.read() & 0xFF);
            } else {
                payloadLen = 0;
                for (int i = 0; i < 8; i++) payloadLen = (payloadLen << 8) | (raw.read() & 0xFF);
            }
            // Mask key (4 bytes if masked — clients always mask)
            boolean masked = (b1 & 0x80) != 0;
            if (masked) {
                for (int i = 0; i < 4; i++) raw.read();
            }
            for (long i = 0; i < payloadLen; i++) raw.read();
        }

        private static void writeBinaryFrame(OutputStream out, byte[] payload) throws IOException {
            out.write(0x82); // FIN | BINARY
            int len = payload.length;
            if (len <= 125) {
                out.write(len);
            } else if (len <= 0xFFFF) {
                out.write(126);
                out.write((len >> 8) & 0xFF);
                out.write(len & 0xFF);
            } else {
                out.write(127);
                for (int i = 7; i >= 0; i--) out.write((int) ((((long) len) >> (i * 8)) & 0xFF));
            }
            out.write(payload);
            out.flush();
        }

        @Override
        public void close() {
            running = false;
            try {
                serverSocket.close();
            } catch (IOException ignored) {
            }
            for (Socket s : openSockets) {
                try {
                    s.close();
                } catch (IOException ignored) {
                }
            }
            if (acceptThread != null) {
                try {
                    acceptThread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Counts binary frames without ACKing. Receipt-only revival probe for
     * {@link #testReconnectNeverGivesUpInvariantB}: observing a replayed
     * frame proves reconnect + replay without coupling the test to the
     * exact FSN sequence a faked ACK would have to carry (a wrong-seq ACK
     * risks tripping the invalid-ACK terminal and polluting the result).
     */
    private static class ReceiveOnlyHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalBinaryReceived = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalBinaryReceived.incrementAndGet();
        }
    }

    /** Acks every binary frame so the sender doesn't hang. */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
    static byte[] buildAck(long seq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(seq);
        bb.putShort((short) 0);
        return buf;
    }
}
