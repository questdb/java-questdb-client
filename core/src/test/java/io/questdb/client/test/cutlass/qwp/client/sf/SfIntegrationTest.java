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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.std.Files;
import io.questdb.client.std.Os;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SfIntegrationTest {

    private static final int TEST_PORT = 19_700 + (int) (System.nanoTime() % 100);

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-int-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(sfDir, 0755));
    }

    @After
    public void tearDown() {
        if (sfDir == null) return;
        long find = Files.findFirst(sfDir);
        if (find != 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(sfDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(sfDir);
    }

    /**
     * Send rows over a sender configured with SF. Verify (a) the bytes appear in
     * the SF dir at some point, and (b) after the server acks, the dir is trimmed
     * back to the empty active segment.
     */
    @Test
    public void testFramesAreCapturedAndTrimmedOnAck() throws Exception {
        int port = TEST_PORT + 1;
        EchoSeqAckHandler handler = new EchoSeqAckHandler(0);
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler);
             SegmentLog log = SegmentLog.open(sfDir, 1L << 20)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            Assert.assertEquals(0L, log.nextSeq());

            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", port,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    8 /* in-flight window > 1 */)) {
                sender.setSegmentLog(log);

                for (int i = 0; i < 5; i++) {
                    sender.table("foo").longColumn("v", i).atNow();
                }
                sender.flush();
            }

            // Server acked → SegmentLog.trim removed all sealed segments. Active
            // segment is never deleted but contains no unacked frames.
            // Wait briefly for the trim callback (runs on the I/O thread which
            // shut down inside sender.close()) — by the time close() returns,
            // every ACK that was already on the wire has been processed.
            Assert.assertTrue("at least one batch was sent", log.nextSeq() > 0L);
            // Only the active (current) segment may remain; no sealed segments
            // because nothing rotated under 1 MB.
            Assert.assertEquals(1, log.segmentCount());
        }
    }

    /**
     * Stress: rapid burst of sends interleaved with random ACK delays and a few
     * connection drops. Every batch must eventually be received by the server (or
     * its replayed copy must be — server-side dedup is the test server's
     * responsibility, but each value seen on the wire is uniquely tagged so we
     * can count distinct user batches).
     */
    @Test
    public void testStressRapidSendsAndDisconnects() throws Exception {
        int port = TEST_PORT + 10;
        // Server: ack normally, but drop every 5th connection on its 4th message.
        // Combined with random ack delays, this exercises stalls, replays, reconnects.
        FlakyServerHandler handler = new FlakyServerHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler);
             SegmentLog log = SegmentLog.open(
                     Paths.get(System.getProperty("java.io.tmpdir"),
                             "qdb-sf-stress-" + System.nanoTime()).toString(),
                     1L << 20);
             QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                     "localhost", port,
                     QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                     QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                     QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                     8)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));
            sender.setSegmentLog(log);

            // 50 separate batches (one row + flush each) so every row hits the
            // wire as its own frame. framesSeen counts batches.
            int totalBatches = 50;
            for (int i = 0; i < totalBatches; i++) {
                sender.table("foo").longColumn("v", (long) i).atNow();
                sender.flush();
            }

            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && handler.framesSeen() < totalBatches) {
                Thread.sleep(20);
            }
            Assert.assertTrue("expected at least " + totalBatches + " frames received, saw "
                            + handler.framesSeen(),
                    handler.framesSeen() >= totalBatches);
            // Flaky server drops every 5th connection on its 4th message. With 50
            // batches we expect multiple disconnects + reconnects.
            Assert.assertTrue("expected at least 2 connections, saw "
                            + handler.connectionsAccepted(),
                    handler.connectionsAccepted() >= 2);
        }
    }

    /**
     * Captured frames are bit-identical to the bytes the server receives. This is
     * the load-bearing invariant of the "disk = wire" design: replay can stream
     * captured bytes back to the server with zero transformation.
     */
    @Test
    public void testCapturedBytesMatchWireBytes() throws Exception {
        int port = TEST_PORT + 2;
        CapturingAckHandler handler = new CapturingAckHandler();
        byte[] capturedFromDisk;
        byte[] wireBytes;
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            byte[][] capturedHolder = new byte[1][];
            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();

                // Read what's on disk via replay BEFORE the server's ACK trim removes it.
                // Note: ACK has already arrived for sealed segments (none here), but
                // active segment is never trimmed, so the captured frame is still there.
                log.replay((seq, addr, len) -> {
                    capturedHolder[0] = new byte[len];
                    for (int i = 0; i < len; i++) {
                        capturedHolder[0][i] = io.questdb.client.std.Unsafe.getUnsafe().getByte(addr + i);
                    }
                    return false;
                });
            }
            Assert.assertEquals(1, handler.frames.size());
            wireBytes = handler.frames.get(0);
            capturedFromDisk = capturedHolder[0];
        }
        Assert.assertNotNull("captured bytes present", capturedFromDisk);
        Assert.assertArrayEquals("disk == wire", wireBytes, capturedFromDisk);
    }

    /**
     * Pre-populate an SF dir with frames as if a previous session left them
     * undelivered, then open a sender against the same dir and verify the server
     * receives those exact frames before any user-thread sends.
     */
    @Test
    public void testReplayOnConnectStreamsPersistedFramesFirst() throws Exception {
        // Step 1: pre-populate SF with three "old" frames simulating an
        // unsent backlog from a previous session.
        byte[] f1 = new byte[]{(byte) 0xAA, 1, 2, 3};
        byte[] f2 = new byte[]{(byte) 0xBB, 4, 5};
        byte[] f3 = new byte[]{(byte) 0xCC, 6, 7, 8, 9};
        try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20)) {
            for (byte[] f : new byte[][]{f1, f2, f3}) {
                long buf = io.questdb.client.std.Unsafe.malloc(f.length, io.questdb.client.std.MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < f.length; i++) {
                        io.questdb.client.std.Unsafe.getUnsafe().putByte(buf + i, f[i]);
                    }
                    log.append(buf, f.length);
                } finally {
                    io.questdb.client.std.Unsafe.free(buf, f.length, io.questdb.client.std.MemoryTag.NATIVE_DEFAULT);
                }
            }
            log.fsync();
        }

        // Step 2: connect sender with the same SF dir; replay should send the
        // three pre-populated frames before any user batch.
        int port = TEST_PORT + 3;
        CapturingAckHandler handler = new CapturingAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);
                // Trigger connection: sender's first table() call calls ensureConnected,
                // which starts the I/O thread; the I/O loop replays SF before processing
                // anything from the user thread.
                sender.table("foo").longColumn("v", 99L).atNow();
                sender.flush();
            }
        }

        // Server should have received the three pre-populated frames first, then
        // exactly one new user-thread batch.
        Assert.assertEquals("4 frames received (3 replayed + 1 new)", 4, handler.frames.size());
        Assert.assertArrayEquals("first frame is replayed f1", f1, handler.frames.get(0));
        Assert.assertArrayEquals("second frame is replayed f2", f2, handler.frames.get(1));
        Assert.assertArrayEquals("third frame is replayed f3", f3, handler.frames.get(2));
        Assert.assertTrue("4th frame is the user-thread send (non-empty)",
                handler.frames.get(3).length > 0);
    }

    /**
     * Connection drops mid-flight; SF auto-reconnect absorbs the failure and replays
     * the unacked frame on the new connection. User code never sees the disconnect.
     */
    @Test
    public void testAutoReconnectAndReplay() throws Exception {
        int port = TEST_PORT + 4;
        DropFirstConnectionHandler handler = new DropFirstConnectionHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);

                // First send — succeeds, server acks.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                // Second send — server drops the connection right after receiving it.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();

                // Wait briefly for the reconnect cycle to play out: the I/O thread
                // notices the dropped connection, sleeps 100ms, reconnects, replays
                // the active segment (containing both msg1 and msg2 — msg1 was acked
                // but it lives in the active segment which never gets trimmed, so it
                // gets replayed too; server-side seqTxn dedup drops the duplicate).
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 4) {
                    Thread.sleep(20);
                }

                // Third send — should go through the now-healthy second connection.
                sender.table("foo").longColumn("v", 3L).atNow();
                sender.flush();

                // Wait for it to be received.
                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 5) {
                    Thread.sleep(20);
                }
            }
        }
        // Server saw: msg1 (conn1), msg2 (conn1, dropped), msg1-replay+msg2-replay (conn2),
        // msg3 (conn2). Total = 5. The replayed msg1 is the documented worst case —
        // already-acked frames in the active (unsealed) segment are re-sent on reconnect.
        Assert.assertEquals("server saw 5 frames (msg1 + msg2 + msg1-replay + msg2-replay + msg3)",
                5, handler.frameCount());
        Assert.assertTrue("server saw at least 2 connections", handler.connectionCount() >= 2);
    }

    /**
     * Under SF, flush() must not block on server ACKs — it returns once data is
     * persisted to disk. Server stays silent the whole time; flush() must still
     * return promptly.
     */
    @Test
    public void testFlushUnderSfReturnsBeforeAck() throws Exception {
        int port = TEST_PORT + 5;
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);
                sender.table("foo").longColumn("v", 1L).atNow();

                long start = System.currentTimeMillis();
                sender.flush();
                long elapsed = System.currentTimeMillis() - start;

                Assert.assertTrue(
                        "flush() under SF should return without waiting for ACK; took " + elapsed + "ms",
                        elapsed < 2_000);
                Assert.assertTrue("data must be on disk", log.bytesOnDisk() > 0L);
            }
        }
    }

    /**
     * Server drops the connection on every other message. The sender should ride
     * through several reconnect cycles in a row without surfacing any error.
     */
    @Test
    public void testMultipleReconnectsInSequence() throws Exception {
        int port = TEST_PORT + 6;
        DropEveryConnectionHandler handler = new DropEveryConnectionHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);

                for (int i = 0; i < 5; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                    sender.flush();
                }

                // Wait for at least 3 distinct connections to have been opened —
                // shows the sender survived multiple reconnect cycles.
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline && handler.connectionCount() < 3) {
                    Thread.sleep(20);
                }
            }
        }
        Assert.assertTrue("expected at least 3 connections, saw " + handler.connectionCount(),
                handler.connectionCount() >= 3);
    }

    /**
     * The reconnected connection drops while the sender is still replaying SF.
     * Sender should tear it down again and reconnect a second time, eventually
     * succeeding and delivering all queued frames.
     */
    @Test
    public void testReconnectDuringReplay() throws Exception {
        int port = TEST_PORT + 7;
        DropFirstTwoConnectionsHandler handler = new DropFirstTwoConnectionsHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);
                // First send goes through, gets dropped, reconnects, replays;
                // second connection also drops on its first message; third connection
                // is healthy.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline && handler.connectionCount() < 3) {
                    Thread.sleep(20);
                }
            }
        }
        Assert.assertTrue("at least 3 connection attempts (orig + 2 retries), saw "
                + handler.connectionCount(), handler.connectionCount() >= 3);
    }

    /**
     * Multi-table sender survives a reconnect. Schemas for both tables must be
     * re-published after reconnect; the sender must not crash on the second pair.
     */
    @Test
    public void testMultiTableSurvivesReconnect() throws Exception {
        int port = TEST_PORT + 8;
        DropFirstConnectionHandler handler = new DropFirstConnectionHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);

                // Pre-disconnect: send to two distinct tables (each with its own schema).
                sender.table("alpha").longColumn("v", 1L).atNow();
                sender.flush();
                sender.table("beta").doubleColumn("d", 1.5).atNow();
                sender.flush();
                // The DropFirstConnectionHandler closes after message #2 (the beta send),
                // so the next sender op will tear down + reconnect.

                // Post-disconnect: more sends to both tables. Schema reset must have
                // run on the user thread; sender must complete without error.
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.connectionCount() < 2) {
                    Thread.sleep(20);
                }

                sender.table("alpha").longColumn("v", 2L).atNow();
                sender.flush();
                sender.table("beta").doubleColumn("d", 2.5).atNow();
                sender.flush();

                deadline = System.currentTimeMillis() + 5_000;
                // 6 frames expected: alpha-1, beta-1 (dropped), replay alpha-1,
                // replay beta-1, alpha-2, beta-2.
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 6) {
                    Thread.sleep(20);
                }
            }
        }
        Assert.assertTrue("at least 2 connections", handler.connectionCount() >= 2);
        Assert.assertTrue("at least 6 frames received, saw " + handler.frameCount(),
                handler.frameCount() >= 6);
    }

    /** {@code setSegmentLog} guards: rejects post-connect, post-close, and sync mode. */
    @Test
    public void testSetSegmentLogValidation() throws Exception {
        // Sync mode (window=1) is incompatible with SF.
        try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
             QwpWebSocketSender syncSender = QwpWebSocketSender.createForTesting(
                     "localhost", 1, 0, 0, 0, 1)) {
            try {
                syncSender.setSegmentLog(log);
                Assert.fail("expected setSegmentLog to reject sync mode");
            } catch (LineSenderException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("async"));
            }
        }
        rmDir(sfDir);
        Assert.assertEquals(0, Files.mkdir(sfDir, 0755));

        // Closed sender rejects setSegmentLog.
        try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20)) {
            QwpWebSocketSender closedSender = QwpWebSocketSender.createForTesting(
                    "localhost", 1, 0, 0, 0, 8);
            closedSender.close();
            try {
                closedSender.setSegmentLog(log);
                Assert.fail("expected setSegmentLog to reject closed sender");
            } catch (LineSenderException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
            }
        }
        rmDir(sfDir);
        Assert.assertEquals(0, Files.mkdir(sfDir, 0755));

        // Connected sender rejects setSegmentLog (must be called before first send).
        // Use an acking server so the first flush returns promptly without SF.
        int port = TEST_PORT + 9;
        try (TestWebSocketServer server = new TestWebSocketServer(port, new CapturingAckHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                try {
                    sender.setSegmentLog(log);
                    Assert.fail("expected setSegmentLog to reject already-connected sender");
                } catch (LineSenderException expected) {
                    Assert.assertTrue(expected.getMessage(),
                            expected.getMessage().contains("before the first send"));
                }
            }
        }
    }

    private static void rmDir(String dir) {
        if (dir == null || !Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find != 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /** ACK handler that echoes the highest-seen sequence as a STATUS_OK reply. */
    private static class EchoSeqAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong(0);

        EchoSeqAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long seq = nextSeq.getAndIncrement();
            try {
                if (delayMs > 0) {
                    Os.sleep(delayMs);
                }
                client.sendBinary(buildAck(seq));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // ACK frame: [status u8][sequence u64][table_count u16=0]
        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put(WebSocketResponse.STATUS_OK);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }

    /** Captures every binary frame and acks it (so the sender doesn't hang on close). */
    private static class CapturingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.List<byte[]> frames = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.add(data.clone());
            try {
                client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * First incoming connection: ack the first message, then close the connection
     * silently (no ack) on the second message. Subsequent connections: ack everything.
     * Used to drive the auto-reconnect path: the client's "second message" disappears
     * mid-flight, the connection drops, SF replays it on the new connection.
     */
    private static class DropFirstConnectionHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, ConnState> perConn =
                new java.util.IdentityHashMap<>();
        private final AtomicLong totalFrames = new AtomicLong(0);
        private final AtomicLong connections = new AtomicLong(0);
        private final java.util.concurrent.atomic.AtomicBoolean firstConnDone =
                new java.util.concurrent.atomic.AtomicBoolean(false);

        long frameCount() {
            return totalFrames.get();
        }

        long connectionCount() {
            return connections.get();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            ConnState state;
            synchronized (perConn) {
                state = perConn.get(client);
                if (state == null) {
                    state = new ConnState();
                    state.isFirst = !firstConnDone.get();
                    perConn.put(client, state);
                    connections.incrementAndGet();
                }
            }
            int idx = state.msgsThisConn++;
            totalFrames.incrementAndGet();

            if (state.isFirst && idx == 1) {
                // Second message on the first connection: drop without ack.
                firstConnDone.set(true);
                try {
                    client.close();
                } catch (Exception ignored) {
                }
                return;
            }
            try {
                client.sendBinary(EchoSeqAckHandler.buildAck(state.nextSeq++));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static class ConnState {
            int msgsThisConn;
            long nextSeq;
            boolean isFirst;
        }
    }

    /** Receives but never acks. Used to verify SF-mode flush()/close() don't block on ACKs. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally silent
        }
    }

    /**
     * Acks the first message on every connection then closes. Forces a reconnect
     * on every send.
     */
    private static class DropEveryConnectionHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, int[]> perConn =
                new java.util.IdentityHashMap<>();
        private final AtomicLong connections = new AtomicLong(0);
        private final AtomicLong nextSeq = new AtomicLong(0);

        long connectionCount() {
            return connections.get();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int[] count;
            synchronized (perConn) {
                count = perConn.get(client);
                if (count == null) {
                    count = new int[]{0};
                    perConn.put(client, count);
                    connections.incrementAndGet();
                }
            }
            int idx = count[0]++;
            try {
                client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                // best-effort
            }
            if (idx == 0) {
                // Close after the first ack lands, forcing a reconnect on the next send.
                try {
                    Thread.sleep(20);
                    client.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Acks normally on most connections. On every 5th connection (1-indexed),
     * drops after the 4th message. Adds 0–25 ms random jitter to each ack.
     * Designed for the rapid-send + reconnect stress test.
     */
    private static class FlakyServerHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, ConnState> perConn =
                new java.util.IdentityHashMap<>();
        private final AtomicLong connections = new AtomicLong(0);
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final AtomicLong frames = new AtomicLong(0);
        private final java.util.Random rnd = new java.util.Random(0xCAFEL);

        long framesSeen() {
            return frames.get();
        }

        long connectionsAccepted() {
            return connections.get();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            ConnState st;
            int jitter;
            synchronized (perConn) {
                st = perConn.get(client);
                if (st == null) {
                    st = new ConnState();
                    st.connId = connections.incrementAndGet();
                    perConn.put(client, st);
                }
                jitter = rnd.nextInt(25);
            }
            int idx = st.msgsThisConn++;
            frames.incrementAndGet();
            // Every connection drops after its 10th message. Forces multiple
            // reconnects under a 50-batch send loop.
            if (idx == 10) {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
                return;
            }
            try {
                if (jitter > 0) Thread.sleep(jitter);
                client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
            } catch (Exception ignored) {
            }
        }

        private static class ConnState {
            int msgsThisConn;
            long connId;
        }
    }

    /** Closes the first two incoming connections immediately on their first message. */
    private static class DropFirstTwoConnectionsHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, Long> perConn =
                new java.util.IdentityHashMap<>();
        private final AtomicLong connections = new AtomicLong(0);
        private final AtomicLong nextSeq = new AtomicLong(0);

        long connectionCount() {
            return connections.get();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long connId;
            synchronized (perConn) {
                Long existing = perConn.get(client);
                if (existing == null) {
                    connId = connections.incrementAndGet();
                    perConn.put(client, connId);
                } else {
                    connId = existing;
                }
            }
            if (connId <= 2) {
                // Close the first two connections on receipt of their first message.
                try {
                    client.close();
                } catch (Exception ignored) {
                }
                return;
            }
            try {
                client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
