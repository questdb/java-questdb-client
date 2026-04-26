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
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
        // Handler captures the wire bytes but does NOT ack. Without an ack
        // the I/O thread never calls trim, so the active segment stays
        // stable while the test thread calls log.replay() (avoiding a
        // race against trim's force-rotate-on-fully-acked). The wire bytes
        // are still observable on the server side.
        CapturingNoAckHandler handler = new CapturingNoAckHandler();
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

                // Wait for the server to receive the frame before reading
                // from disk; flush() under SF returns once the bytes are
                // persisted, but the wire send is async on the I/O thread.
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frames.isEmpty()) {
                    Thread.sleep(10);
                }
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

                // Wait for msg1's ACK to round-trip and trim to fire its
                // force-rotate-on-fully-acked path (drops bytesOnDisk back
                // to HEADER_SIZE). Without this, msg2 may be appended to
                // SF before the ACK lands, leaving both msg1 and msg2 in
                // the active segment with only msg2 acked, defeating the
                // "msg1 trimmed before disconnect" precondition the test
                // is trying to demonstrate.
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && log.bytesOnDisk() > SegmentLog.HEADER_SIZE) {
                    Thread.sleep(20);
                }

                // Second send — server drops the connection right after receiving it.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();

                // Wait briefly for the reconnect cycle to play out: the I/O thread
                // notices the dropped connection, sleeps 100ms, reconnects, replays
                // the active segment. Under per-frame trim (force-rotate-on-fully-
                // acked) msg1 was acked-and-trimmed before the disconnect, so only
                // msg2 (the unacked frame) remains on disk to replay.
                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 3) {
                    Thread.sleep(20);
                }

                // Third send — should go through the now-healthy second connection.
                sender.table("foo").longColumn("v", 3L).atNow();
                sender.flush();

                // Wait for it to be received.
                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 4) {
                    Thread.sleep(20);
                }
            }
        }
        // Server saw: msg1 (conn1), msg2 (conn1, dropped), msg2-replay (conn2),
        // msg3 (conn2). Total = 4. msg1 is NOT replayed because trim's force-
        // rotate-on-fully-acked dropped it from SF as soon as the ACK arrived.
        Assert.assertEquals("server saw 4 frames (msg1 + msg2 + msg2-replay + msg3)",
                4, handler.frameCount());
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
                // 5 frames expected: alpha-1 (acked + trimmed before drop),
                // beta-1 (dropped without ack), beta-1 replay, alpha-2, beta-2.
                // alpha-1 is NOT replayed because force-rotate-on-fully-acked
                // dropped it from SF the moment its ACK landed.
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 5) {
                    Thread.sleep(20);
                }
            }
        }
        Assert.assertTrue("at least 2 connections", handler.connectionCount() >= 2);
        Assert.assertTrue("at least 5 frames received, saw " + handler.frameCount(),
                handler.frameCount() >= 5);
    }

    /**
     * Schema-reset race protection — between-batches case.
     * <p>
     * After a (real or simulated) reconnect, {@code connectionGeneration} is
     * bumped and {@code schemaResetNeeded} flips. The next user-thread
     * {@code flushPendingRows} must observe the bump, reset schema state,
     * and emit a fresh batch — server receives a frame carrying full
     * schema definitions, not stale refs into the previous connection's
     * id space. This covers the simple "reconnect happened, then user
     * flushes" path.
     */
    @Test
    public void testGenerationBumpBetweenBatchesTriggersSchemaReset() throws Exception {
        int port = TEST_PORT + 90;
        CapturingAckHandler handler = new CapturingAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", port,
                    1, // autoFlushRows = 1 → each atNow ships one batch
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    8)) {
                // First batch: server sees a fresh schema definition.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frames.size() < 1) {
                    Thread.sleep(20);
                }
                Assert.assertEquals(1, handler.frames.size());
                int firstBatchSize = handler.frames.get(0).length;

                // Simulate a reconnect: flip schemaResetNeeded and bump
                // connectionGeneration via reflection. Closes the loop
                // without going through the network — we're testing the
                // user-thread side of the contract here.
                forceSchemaResetAndBumpGeneration(sender);

                // Second batch: must carry a full schema definition again,
                // not a ref. Frame should be at least as large as the
                // first (definition is strictly heavier than a ref).
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frames.size() < 2) {
                    Thread.sleep(20);
                }
                Assert.assertEquals(2, handler.frames.size());
                int secondBatchSize = handler.frames.get(1).length;
                Assert.assertTrue(
                        "post-reset batch must carry a fresh schema definition; "
                                + "first=" + firstBatchSize + " bytes, second=" + secondBatchSize
                                + " bytes (a ref-only batch would be strictly smaller)",
                        secondBatchSize >= firstBatchSize);
            }
        }
    }

    /**
     * Schema-reset race protection — concurrent stress.
     * <p>
     * Spawn a thread that bumps {@code connectionGeneration} as fast as
     * it can while the main thread flushes batches in a tight loop. Any
     * landing of a bump during {@code flushPendingRows}' encode window
     * must be caught by the post-encode generation re-read and re-driven
     * through the retry loop. The test passes as long as no exception
     * escapes flush() (other than the bounded MAX_SCHEMA_RACE_RETRIES
     * fail-fast, which we tolerate at the very upper end of bumper rates).
     */
    @Test(timeout = 30_000)
    public void testSchemaResetRaceUnderConcurrentBumps() throws Exception {
        int port = TEST_PORT + 91;
        CapturingAckHandler handler = new CapturingAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", port,
                    1,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    8)) {
                Field genField = QwpWebSocketSender.class.getDeclaredField("connectionGeneration");
                genField.setAccessible(true);
                Field resetField = QwpWebSocketSender.class.getDeclaredField("schemaResetNeeded");
                resetField.setAccessible(true);

                final int batches = 200;
                final java.util.concurrent.atomic.AtomicBoolean stopBumper = new java.util.concurrent.atomic.AtomicBoolean(false);
                final java.util.concurrent.atomic.AtomicLong bumpCount = new java.util.concurrent.atomic.AtomicLong(0);
                Thread bumper = new Thread(() -> {
                    try {
                        while (!stopBumper.get()) {
                            // Throttled: pause so most bumps land between
                            // batches; a few will land mid-encode and
                            // exercise the retry path.
                            Thread.sleep(0, 50_000); // 50 microseconds
                            resetField.setBoolean(sender, true);
                            genField.setLong(sender, genField.getLong(sender) + 1);
                            bumpCount.incrementAndGet();
                        }
                    } catch (Exception ignored) {
                    }
                }, "schema-race-bumper");
                bumper.setDaemon(true);
                bumper.start();

                try {
                    int sent = 0;
                    LineSenderException maxRetryError = null;
                    for (int i = 0; i < batches; i++) {
                        try {
                            sender.table("foo").longColumn("v", (long) i).atNow();
                            sender.flush();
                            sent++;
                        } catch (LineSenderException e) {
                            // The only acceptable exception is the
                            // bounded retry-limit fail-fast — bumper is
                            // running flat-out so it can occasionally
                            // win 10 races back-to-back.
                            if (e.getMessage() != null
                                    && e.getMessage().contains("schema-reset race exceeded retry limit")) {
                                maxRetryError = e;
                                break;
                            }
                            throw e;
                        }
                    }
                    Assert.assertTrue(
                            "bumper must have fired at least once; bumps=" + bumpCount.get(),
                            bumpCount.get() > 0);
                    Assert.assertTrue(
                            "either every batch shipped or the retry-limit fail-fast tripped; "
                                    + "sent=" + sent + ", maxRetryError=" + maxRetryError,
                            sent == batches || maxRetryError != null);
                } finally {
                    stopBumper.set(true);
                    bumper.join(5_000);
                }
            }
        }
    }

    private static void forceSchemaResetAndBumpGeneration(QwpWebSocketSender sender) throws Exception {
        Field genField = QwpWebSocketSender.class.getDeclaredField("connectionGeneration");
        genField.setAccessible(true);
        Field resetField = QwpWebSocketSender.class.getDeclaredField("schemaResetNeeded");
        resetField.setAccessible(true);
        resetField.setBoolean(sender, true);
        genField.setLong(sender, genField.getLong(sender) + 1);
    }

    /**
     * sf_fsync_on_flush=off (default): the user's flush() must NOT call
     * segmentLog.fsync(). Pre-fix the docs claimed an fsync happened on
     * every flush in the default config, which would have penalised the
     * common small-batch + frequent-flush workload — exactly why the
     * user wanted this knob to be opt-in.
     */
    @Test
    public void testFlushDoesNotFsyncByDefault() throws Exception {
        runFlushFsyncObservation(/* fsyncOnFlush */ false, /* expectFsync */ false);
    }

    /**
     * sf_fsync_on_flush=on (opt-in): the user's flush() must route a
     * fsync to the I/O thread before returning. Proves the wiring from
     * Sender.storeAndForwardFsyncOnFlush → QwpWebSocketSender.flush →
     * WebSocketSendQueue.requestSegmentLogFsync → SegmentLog.fsync →
     * ff.fsync is end-to-end functional.
     */
    @Test
    public void testFlushFsyncsWhenOptedIn() throws Exception {
        runFlushFsyncObservation(/* fsyncOnFlush */ true, /* expectFsync */ true);
    }

    private void runFlushFsyncObservation(boolean fsyncOnFlush, boolean expectFsync) throws Exception {
        int port = TEST_PORT + (fsyncOnFlush ? 81 : 80);
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            FsyncCountingFacade ff = new FsyncCountingFacade();
            // Open SegmentLog first with a no-op count so the open-time
            // createActive's header fsync doesn't pollute the per-flush
            // counter we're about to observe.
            SegmentLog log = SegmentLog.open(sfDir, ff, 1L << 20, Long.MAX_VALUE, false);
            int fsyncsAtStartup = ff.fsyncs.get();

            QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, /* tlsConfig */ null,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    8, /* authHeader */ null,
                    QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                    /* requestDurableAck */ false, log, fsyncOnFlush);
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                int fsyncsBeforeFlush = ff.fsyncs.get();
                sender.flush();

                // Wait for any I/O-thread-side fsync to settle. flush()
                // under SF returns once data is on disk; the
                // requestSegmentLogFsync path (when opted in) blocks on
                // the I/O thread fsync round-trip, so by the time
                // flush() returns the counter reflects the request.
                int fsyncsAfterFlush = ff.fsyncs.get();
                int delta = fsyncsAfterFlush - fsyncsBeforeFlush;

                if (expectFsync) {
                    Assert.assertTrue(
                            "opt-in flush must trigger at least one fsync; "
                                    + "fsyncs at startup=" + fsyncsAtStartup
                                    + ", before flush=" + fsyncsBeforeFlush
                                    + ", after flush=" + fsyncsAfterFlush,
                            delta >= 1);
                } else {
                    Assert.assertEquals(
                            "default flush must NOT fsync; "
                                    + "fsyncs at startup=" + fsyncsAtStartup
                                    + ", before flush=" + fsyncsBeforeFlush
                                    + ", after flush=" + fsyncsAfterFlush,
                            0, delta);
                }
            } finally {
                try {
                    sender.close();
                } catch (Throwable ignored) {
                    // best-effort
                }
            }
        }
    }

    /**
     * End-to-end verification of the per-frame trim behaviour. A quiet
     * sender that flushes some batches, lets every ACK land, and then
     * shuts down must leave nothing on disk for the next sender to
     * replay. Before per-frame trim landed, the active segment retained
     * every acked-but-unsealed frame and the next sender re-shipped them
     * (relying on server-side seqTxn dedup to avoid duplicate rows). This
     * test asserts the public Sender API doc — "trimmed when the server
     * acknowledges it" — is now load-bearing.
     */
    @Test(timeout = 30_000)
    public void testRestartAfterAckedBatchesReplaysNothing() throws Exception {
        int port = TEST_PORT + 70;
        CountingAckHandler handler = new CountingAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            // Phase 1: send N batches, wait for every ACK to land + trim to
            // fire, then close. After this block the SF dir must contain
            // only an empty active segment.
            final int batchCount = 5;
            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         /* autoFlushRows */ 1,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);
                for (int i = 0; i < batchCount; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                }
                sender.flush();

                // Wait for every batch to reach the server AND for trim's
                // force-rotate to land bytesOnDisk back at the empty
                // active's header size.
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline
                        && (handler.frameCount() < batchCount
                                || log.bytesOnDisk() > SegmentLog.HEADER_SIZE)) {
                    Thread.sleep(20);
                }
                Assert.assertEquals(batchCount, handler.frameCount());
                Assert.assertEquals(
                        "active segment must be empty after every batch is acked",
                        (long) SegmentLog.HEADER_SIZE, log.bytesOnDisk());
                Assert.assertEquals("oldestSeq -1 = no frames on disk",
                        -1L, log.oldestSeq());
            }
            long framesAfterPhase1 = handler.frameCount();
            long connectionsAfterPhase1 = handler.connectionCount();

            // Phase 2: open a fresh sender against the same SF dir. Send
            // one new batch. The server must see exactly one new frame —
            // no replay of the phase-1 batches.
            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8)) {
                sender.setSegmentLog(log);
                sender.table("foo").longColumn("v", 99L).atNow();
                sender.flush();

                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && handler.frameCount() < framesAfterPhase1 + 1) {
                    Thread.sleep(20);
                }
            }

            Assert.assertEquals(
                    "phase-2 must ship exactly 1 frame; any extra means the trim "
                            + "contract leaked acked-but-unsealed frames into replay. "
                            + "Frames after phase1=" + framesAfterPhase1
                            + ", frames after phase2=" + handler.frameCount(),
                    framesAfterPhase1 + 1, handler.frameCount());
            Assert.assertTrue(
                    "phase-2 must open a fresh connection",
                    handler.connectionCount() > connectionsAfterPhase1);
        }
    }

    /**
     * Red test for the poisoned-frame reconnect loop.
     * <p>
     * SF persists wire frames before send and replays them on reconnect. If a
     * persisted frame causes the server to return a non-success status (parse
     * error, schema mismatch, write error, etc.), the client's
     * {@code ResponseHandler} treats it as a transient connection failure and
     * triggers an SF reconnect. The reconnect re-runs SF replay, which ships
     * the same poisoned bytes, which provoke the same error, which triggers
     * another reconnect — forever. The bytes are immutable on disk and there
     * is no path that drops them after a server-error response.
     * <p>
     * This test plants a single malformed frame in SF, opens a sender against
     * a server that responds with {@code STATUS_PARSE_ERROR} to every binary
     * message, and counts the number of times the server sees the frame within
     * a bounded window. Bug behaviour: tens of replays as the I/O thread loops
     * through reconnect cycles. Fix behaviour: the sender either drops the
     * poisoned frame after a bounded number of error responses (and trims it
     * from SF) or surfaces a terminal {@code LineSenderException} to the user.
     * <p>
     * The schema-reset race documented in the PR description ("self-healing
     * via the next reconnect cycle") is one way to produce a poisoned frame in
     * SF, but the failure mode is the same regardless of how the frame got
     * there. This test is independent of the race timing.
     */
    @Test(timeout = 30_000)
    public void testPoisonedFrameInSfDoesNotLoopForever() throws Exception {
        // Step 1: plant a malformed wire frame directly in SF. Bytes are
        // arbitrary garbage; the server will treat it as an invalid QWP frame.
        byte[] poison = new byte[]{(byte) 0xFF, (byte) 0xFE, 0x01, 0x02, 0x03};
        try (SegmentLog plantLog = SegmentLog.open(sfDir, 1L << 20)) {
            long buf = Unsafe.malloc(poison.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < poison.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, poison[i]);
                }
                plantLog.append(buf, poison.length);
            } finally {
                Unsafe.free(buf, poison.length, MemoryTag.NATIVE_DEFAULT);
            }
            plantLog.fsync();
        }

        // Step 2: server that responds STATUS_PARSE_ERROR to every binary
        // frame. Counts how many times the poisoned frame is replayed.
        int port = TEST_PORT + 50;
        AlwaysParseErrorHandler handler = new AlwaysParseErrorHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
            QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, /* tlsConfig */ null,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    8, /* authHeader */ null,
                    QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                    /* requestDurableAck */ false, log);
            try {
                // I/O thread is up. Replay-on-startup ships the poisoned frame.
                // Server returns STATUS_PARSE_ERROR. failConnection(_, false)
                // triggers SF reconnect. Reconnect re-replays the poisoned frame.
                // Bug: this loop runs unbounded.
                //
                // 3-second observation window. With the 100 ms initial backoff
                // (which resets to 100 ms after every successful reconnect)
                // each cycle is roughly 100 ms + connect + replay. In 3 s a
                // looping bug racks up well over 5 server-side frames.
                Thread.sleep(3_000);

                long frames = handler.frameCount();
                long connections = handler.connectionCount();
                Assert.assertTrue(
                        "Sender entered an unbounded reconnect loop replaying the same poisoned "
                                + "SF frame; connections=" + connections + ", frames=" + frames
                                + ". The fix must drop the poisoned frame from SF after a bounded "
                                + "number of server-error responses (or surface a terminal "
                                + "LineSenderException to the user).",
                        frames <= 5);
            } finally {
                try {
                    sender.close();
                } catch (Throwable ignored) {
                    // Best-effort: under the bug the I/O thread may take time
                    // to wind down through interrupts and shutdown timeouts.
                }
            }
        }
    }

    /**
     * Red test for the {@code retryStalled} mis-classification.
     * <p>
     * Production path at {@code WebSocketSendQueue.retryStalled} (lines 956-989):
     * <pre>
     *   try {
     *       sendBatch(batch);  // can throw SfException, SfDiskFullException, or other
     *       cleared = true;
     *   } catch (SfDiskFullException dfe) { ... still stalled ... }
     *   catch (Throwable t) {
     *       failConnection(_, false);   // ← always fatal=false
     *       if (batch.isSealed()) batch.markSending();
     *       if (batch.isSending()) batch.markRecycled();   // ← recycles as if sent
     *       cleared = true;
     *   }
     * </pre>
     * The main-loop {@code sendBatch} catch ladder (lines 723-738) correctly
     * splits {@code SfException} (fatal=true → terminal) from {@code Throwable}
     * (fatal=false → reconnect). The retry path collapses both into fatal=false.
     * <p>
     * Two consequences:
     * <ol>
     *   <li><b>Wrong reconnect on fatal storage error:</b> instead of going
     *       terminal and surfacing the error, the I/O thread reconnects.
     *       For a transient fsync failure the next retry would succeed, so the
     *       symptom is just an unnecessary reconnect cycle. For a persistent
     *       fsync failure (e.g. an EIO-stuck filesystem), the loop would only
     *       break when the next user-driven {@code sendBatch} hits the same
     *       fault in the main loop and is correctly classified as fatal there
     *       — by which time the user has already lost track of one batch.</li>
     *   <li><b>Silent buffer recycle:</b> {@code markRecycled} runs as if the
     *       batch was successfully sent, even though
     *       {@code segmentLog.append} threw before persisting all bytes.</li>
     * </ol>
     * <p>
     * Setup: a {@link FilesFacade} that (a) returns a short payload write on
     * demand to trigger {@code SfDiskFullException} from
     * {@code SegmentLog.append}, and (b) returns -1 from the next {@code fsync}
     * to trigger {@code SfException} from the retry's {@code fsync}-after-append.
     * Send a warm-up batch, arm both flags, send the second batch — the second
     * batch stalls on the short write, the I/O thread retries, the retry's
     * write succeeds (the short-write flag is one-shot) but the fsync fails.
     * <p>
     * Observation: handler connection count. Under the bug, {@code retryStalled}
     * triggers a reconnect (count grows by 1). Under the fix, the sender goes
     * terminal and connection count stays the same.
     */
    @Test(timeout = 30_000)
    public void testRetryStalledTreatsSfStorageErrorAsTerminal() throws Exception {
        int port = TEST_PORT + 60;
        CountingAckHandler handler = new CountingAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            StallThenFsyncFailFacade ff = new StallThenFsyncFailFacade();
            // Large segment + total caps so DiskFull is driven exclusively by the
            // FF's short-write injection, never by real space pressure.
            // fsyncEachAppend=true so every successful append calls fsync.
            SegmentLog log = SegmentLog.open(sfDir, ff, 4096, Long.MAX_VALUE, /* fsyncEachAppend */ true);
            QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, /* tlsConfig */ null,
                    /* autoFlushRows */ 1,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                    QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    8, /* authHeader */ null,
                    QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                    /* requestDurableAck */ false, log);
            try {
                // Step 1: warm up. Send + flush batch1 normally so we know
                // the connection is live and one fsync has already passed.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline && handler.frameCount() < 1) {
                    Thread.sleep(20);
                }
                Assert.assertEquals("warm-up batch did not reach the server",
                        1, handler.frameCount());

                // Wait for batch1's ACK to round-trip and for trim's force-
                // rotate-on-fully-acked to settle (it triggers an extra
                // rotate fsync on every ack). Without this wait the ACK
                // could land AFTER step 2's flag arming and the rotate
                // fsync would consume failNextFsync, masking the bug.
                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && log.bytesOnDisk() > SegmentLog.HEADER_SIZE) {
                    Thread.sleep(20);
                }
                Assert.assertEquals("post-ACK trim should leave only the new active's header",
                        (long) SegmentLog.HEADER_SIZE, log.bytesOnDisk());

                long connectionsBefore = handler.connectionCount();
                Assert.assertEquals("expected exactly one connection so far",
                        1, connectionsBefore);

                // Step 2: arm the failure pair. The next payload write returns
                // a short count → SfDiskFullException → stall. The next fsync
                // returns -1 → SfException → bug-triggering retry-path catch.
                ff.failNextPayloadWrite = true;
                ff.failNextFsync = true;

                // Step 3: send batch2. atNow with autoFlushRows=1 enqueues the
                // batch without blocking; the I/O thread picks it up and hits
                // the short write, which sets stalledBuffer.
                sender.table("foo").longColumn("v", 2L).atNow();

                // Step 4: confirm the stall registered.
                deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && sender.getTotalSfDiskFullStalls() == 0) {
                    Thread.sleep(20);
                }
                Assert.assertTrue("expected at least one disk-full stall, saw "
                                + sender.getTotalSfDiskFullStalls(),
                        sender.getTotalSfDiskFullStalls() > 0);

                // Step 5: wait for the retry to fire. The retry's append:
                //   - write succeeds (failNextPayloadWrite was consumed on first hit)
                //   - fsync fails (failNextFsync still armed) → SfException
                //   - bug: retryStalled catches Throwable, calls failConnection(_, false)
                //     under SF+reconnector → reconnect → handler sees a new connection
                //   - fix: catches SfException specifically, calls failConnection(_, true)
                //     → terminal, no reconnect, handler sees no new connection
                Thread.sleep(1_000);

                long connectionsAfter = handler.connectionCount();
                Assert.assertEquals(
                        "WebSocketSendQueue.retryStalled (lines 973-980) must classify "
                                + "SfException as fatal, like the main-loop sendBatch catch does. "
                                + "Reconnecting on a fatal SF storage error masks the failure from "
                                + "the user. connectionsBefore=" + connectionsBefore
                                + ", connectionsAfter=" + connectionsAfter,
                        connectionsBefore, connectionsAfter);
            } finally {
                try {
                    sender.close();
                } catch (Throwable ignored) {
                    // best-effort: under the bug the I/O thread may be slow to
                    // wind down through interrupt + shutdown timeout.
                }
            }
        }
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

    /**
     * Red test for the symbol-watermark reconnect bug.
     * <p>
     * After SF reconnect, {@link QwpWebSocketSender#performReconnect()} flips
     * {@code schemaResetNeeded} so the next encode pass calls
     * {@code resetSchemaStateForNewConnection()}. That helper resets the
     * schema-id state ({@code maxSentSchemaId}, {@code nextSchemaId}, per-table
     * schema ids) — but it does <b>not</b> reset
     * {@code maxSentSymbolId}/{@code currentBatchMaxSymbolId}.
     * <p>
     * The encoder uses {@code maxSentSymbolId} as the "confirmed by server"
     * watermark for the symbol-delta dictionary
     * (see {@code QwpWebSocketEncoder.beginMessage}):
     * <pre>
     *   deltaStart = confirmedMaxId + 1;
     *   deltaCount = max(0, batchMaxId - confirmedMaxId);
     * </pre>
     * After a reconnect the new server has zero symbol mappings, but the
     * client still believes the old server's high-water mark applies. The
     * first post-reconnect batch ships a delta dictionary that excludes every
     * symbol id ≤ the stale {@code maxSentSymbolId}; subsequent column
     * payloads then reference dictionary ids the new server has never seen,
     * producing silent mis-decoding (or PARSE_ERROR if the wire ref happens
     * to fall outside the empty range).
     * <p>
     * Required behaviour: the post-reconnect batch's delta dictionary must
     * include every symbol id the batch references, starting from id 0,
     * because the new server starts with an empty dictionary.
     */
    @Test(timeout = 60_000)
    public void testReconnectResetsSymbolWatermark() throws Exception {
        int port = TEST_PORT + 11;
        AckThenCloseAndCaptureHandler handler = new AckThenCloseAndCaptureHandler();
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

                // Batch 1 introduces the symbol "alpha" (gets global id 0).
                // After ack lands and SF trims, maxSentSymbolId becomes 0.
                sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                sender.flush();

                // Wait until batch 1 is on the wire AND its ack has trimmed
                // SF back to the empty-active baseline (proves
                // maxSentSymbolId was advanced to 0). The I/O thread is now
                // IDLE; the server-side close that follows the ack is sitting
                // in the client's TCP buffer, undetected, until the next
                // user-thread send wakes the I/O thread.
                long deadline = System.currentTimeMillis() + 30_000;
                while (System.currentTimeMillis() < deadline
                        && (handler.frames.size() < 1
                                || log.bytesOnDisk() > SegmentLog.HEADER_SIZE)) {
                    Thread.sleep(20);
                }
                Assert.assertTrue("batch 1 received", handler.frames.size() >= 1);
                Assert.assertEquals("SF trimmed after batch 1 acked",
                        SegmentLog.HEADER_SIZE, log.bytesOnDisk());

                // Give the server-side close (handler sleeps 20ms post-ack)
                // time to propagate to the client TCP buffer so the I/O
                // thread's next send fails immediately and triggers reconnect.
                Thread.sleep(200);

                // Batch 2 reuses "alpha" — already in the global dictionary
                // at id 0. With the bug, the encoder treats id 0 as "already
                // confirmed by the server" because maxSentSymbolId is still 0,
                // so the symbol-delta dictionary in batch 2 is empty. With the
                // fix, resetSchemaStateForNewConnection() reset
                // maxSentSymbolId to -1 and the encoder ships id 0 ("alpha")
                // in the delta so the new server can decode the column refs.
                sender.table("foo").symbol("s", "alpha").longColumn("v", 2L).atNow();
                sender.flush();

                // Wait for batch 2 to arrive on conn 2. The I/O thread sends
                // it on conn 1 (which fails — close is in the TCP buffer),
                // detects the failure, reconnects, and replays batch 2 from
                // SF on conn 2. The captured frame is the post-reconnect one.
                while (System.currentTimeMillis() < deadline
                        && (handler.frames.size() < 2 || handler.connections.get() < 2)) {
                    Thread.sleep(20);
                }
                Assert.assertTrue("batch 2 received, frames=" + handler.frames.size(),
                        handler.frames.size() >= 2);
                Assert.assertTrue("reconnect happened, connections=" + handler.connections.get(),
                        handler.connections.get() >= 2);
            }
        }

        // Parse batch 2's delta-dictionary header. Wire layout:
        //   bytes 0..3   "QWP1"
        //   byte  4      version
        //   byte  5      flags (FLAG_DELTA_SYMBOL_DICT bit always set in async mode)
        //   bytes 6..7   tableCount (LE u16)
        //   bytes 8..11  payloadLength (LE u32)
        //   byte  12+    payload starts:
        //                  varint deltaStart
        //                  varint deltaCount
        //                  deltaCount * (varint utf8Len, utf8Len bytes)
        //                  ...column data...
        // Last captured frame is the post-reconnect one. If batch 2's first
        // send happened to land on conn 1 before the reconnect-trigger fired,
        // the SF replay will have re-shipped the same encoded bytes on conn 2,
        // which is what we want to inspect.
        byte[] frame2 = handler.frames.get(handler.frames.size() - 1);
        Assert.assertTrue("frame too short: " + frame2.length, frame2.length > 14);
        long[] startCursor = readUnsignedVarint(frame2, 12);
        long deltaStart = startCursor[0];
        long[] countCursor = readUnsignedVarint(frame2, (int) startCursor[1]);
        long deltaCount = countCursor[0];

        // BUG: deltaStart=1, deltaCount=0 — empty dictionary even though
        // batch references symbol id 0 which the new server has never seen.
        // FIX: deltaStart=0, deltaCount=1 — re-publishes "alpha" with id 0.
        if (deltaCount == 0) {
            Assert.fail("BUG: post-reconnect batch shipped an empty symbol-delta "
                    + "dictionary (deltaStart=" + deltaStart + ", deltaCount=0), "
                    + "but the new server has never seen any symbols. "
                    + "performReconnect()/resetSchemaStateForNewConnection() must "
                    + "reset maxSentSymbolId so the post-reconnect batch's delta "
                    + "dictionary covers every referenced id starting from 0.");
        }
        Assert.assertEquals("delta dictionary must start from id 0 because the "
                        + "new server has an empty dictionary",
                0L, deltaStart);
        Assert.assertEquals("delta dictionary must contain exactly one symbol (\"alpha\")",
                1L, deltaCount);

        // Sanity: the bytes immediately after the deltaCount varint must be
        // the length-prefixed UTF-8 encoding of "alpha".
        int symbolStart = (int) countCursor[1];
        long[] strLenCursor = readUnsignedVarint(frame2, symbolStart);
        Assert.assertEquals("\"alpha\" length", 5L, strLenCursor[0]);
        int utf8Start = (int) strLenCursor[1];
        byte[] expected = "alpha".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals("\"alpha\" byte " + i, expected[i], frame2[utf8Start + i]);
        }
    }

    /**
     * Red test for the future-ACK trim bug.
     * <p>
     * {@code InFlightWindow.acknowledgeUpTo} caps incoming server sequence
     * numbers at {@code highestSent} so a bogus future-ACK cannot mark
     * unsent batches as acknowledged. {@code ResponseHandler.onBinaryMessage}
     * (in the same class) feeds the <b>raw, uncapped</b> server sequence
     * into {@code segmentLog.trim(fsnAtZero + sequence)} — there is no
     * symmetric clamp on the SF trim path. A buggy/misbehaving/replayed
     * server ACK with a sequence beyond what the client has sent drives
     * {@code SegmentLog.trim} past every real {@code lastSeq}, deleting
     * every sealed segment and force-rotating-then-deleting the active —
     * including frames that the server has never seen and never
     * acknowledged.
     * <p>
     * Concrete failure: a previous session left N unsent frames on disk;
     * on reconnect, replay starts. After the server receives only the first
     * frame and emits a malformed/replayed ACK with a huge sequence, the
     * client deletes frames 1..N-1 from disk before they are sent.
     * Permanent silent data loss.
     * <p>
     * Required behaviour: the trim sequence must be clamped to
     * {@code nextBatchSequence - 1} (the highest wire seq actually sent on
     * this connection) before being passed to {@link SegmentLog#trim}.
     */
    @Test(timeout = 60_000)
    public void testFutureAckMustNotTrimUnsentSfData() throws Exception {
        // Pre-populate SF with twenty frames simulating a previous session's
        // unsent backlog. We need substantially more frames than the in-flight
        // window so the bogus ACK arrives mid-replay (i.e., before every frame
        // has been sent on the wire) — that's the only configuration in which
        // capping the trim sequence at highestSent has a different effect from
        // trimming at the raw bogus sequence.
        final int frameCount = 20;
        final byte[][] frames = new byte[frameCount][];
        for (int i = 0; i < frameCount; i++) {
            frames[i] = new byte[]{(byte) (0xA0 | (i & 0x0F)), (byte) i, 0x42, 0x43, 0x44};
        }
        try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20)) {
            for (byte[] f : frames) {
                long buf = Unsafe.malloc(f.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < f.length; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, f[i]);
                    }
                    log.append(buf, f.length);
                } finally {
                    Unsafe.free(buf, f.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
            log.fsync();
        }

        // Sanity: SF holds exactly the five pre-populated frames.
        assertReplayCount(sfDir, frameCount);

        int port = TEST_PORT + 12;
        FutureAckThenSilentHandler handler = new FutureAckThenSilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8 /* in-flight window — smaller than frame count */)) {
                sender.setSegmentLog(log);

                // Opening the I/O thread triggers replay of the SF backlog.
                // With window=8 and 20 frames, the I/O thread sends 8 frames
                // and then blocks on tryReceiveAcks waiting for window space.
                // The server replies to the first frame with a malformed ACK
                // (seq=999_999) — at that moment highestSent==7, so capping
                // the trim sequence at 7 leaves the active segment's
                // lastSeq=19 untouched. Without the cap the active is
                // force-rotated and every persisted frame is unlinked.
                //
                // flush() with no pending rows still calls ensureConnected();
                // we deliberately do NOT enqueue a user batch because the
                // post-bogus-ACK reconnect spin would otherwise block close
                // (sendQueue.flush() waits for pendingBuffer to drain, and
                // the I/O thread is stuck spinning on a closed connection
                // until close() sets running=false).
                sender.flush();

                // Wait for the bogus ACK to have been dispatched. The
                // I/O thread will then consume it and either keep the SF
                // intact (with the fix) or wipe it (with the bug).
                long deadline = System.currentTimeMillis() + 10_000;
                while (System.currentTimeMillis() < deadline
                        && (!handler.bogusAckSent || handler.framesReceived.get() < 8)) {
                    Thread.sleep(10);
                }
                Assert.assertTrue("bogus ACK dispatched", handler.bogusAckSent);

                // Let the I/O thread consume the bogus ACK and run trim.
                Thread.sleep(300);
            }
        }

        // The server confirmed at most the first replayed frame, so the vast
        // majority of pre-populated frames must still be on disk. Use a
        // conservative threshold (3/4 of the original) so the test isn't
        // brittle to small timing variations in how many frames the I/O
        // thread shipped before consuming the bogus ACK.
        int survivors = countReplayableFrames(sfDir);
        int minSurvivors = (frameCount * 3) / 4;
        if (survivors < minSurvivors) {
            Assert.fail("BUG: SegmentLog dropped " + (frameCount - survivors)
                    + " of " + frameCount + " pre-populated frames after the "
                    + "server emitted a malformed future-ACK (seq=999_999) "
                    + "early in the replay. With at most 8 frames in flight at "
                    + "the time of the bogus ACK, the server confirmed nothing "
                    + "beyond frame 0, so at least " + minSurvivors + " frames "
                    + "must still be on disk for the next session to replay. "
                    + "Found " + survivors + " on disk. The trim path in "
                    + "WebSocketSendQueue.ResponseHandler.onBinaryMessage must "
                    + "clamp the server sequence to nextBatchSequence-1 before "
                    + "calling segmentLog.trim, mirroring the cap in "
                    + "InFlightWindow.acknowledgeUpTo.");
        }
    }

    /**
     * Red test for the replay-spin-hang bug.
     * <p>
     * {@code replayPersistedFrames} fills the in-flight window during replay
     * and then enters a spin loop waiting for ACKs to free space:
     * <pre>
     *   while (running &amp;&amp; !inFlightWindow.hasWindowSpace()) {
     *       if (client.isConnected()) tryReceiveAcks();
     *       Thread.onSpinWait();
     *   }
     * </pre>
     * The {@code if (client.isConnected())} guard means: once the connection
     * dies (peer reset, server crash, mid-replay close), {@code tryReceiveAcks}
     * is never called again. The window can't drain. The spin loop never
     * exits. The I/O thread is stuck inside {@code replayPersistedFrames}
     * inside {@code doReconnectCycle} inside {@code ioLoop}, so the outer
     * reconnect state machine never gets to re-run, and {@code flush()} /
     * {@code close()} block indefinitely (until the user signals close,
     * which finally sets {@code running=false}).
     * <p>
     * Worse still, even when the first spin iteration successfully reads a
     * server close frame and {@code failConnection} sets
     * {@code reconnectRequested=true}, the spin loop ignores that flag —
     * it only looks at {@code running} and {@code hasWindowSpace}.
     * <p>
     * Required behaviour: when the connection dies (or
     * {@code reconnectRequested} is set) during the in-replay window-wait,
     * the spin must exit so the outer state machine can drive a reconnect.
     */
    @Test(timeout = 30_000)
    public void testReplayMustNotHangWhenConnectionDropsMidReplay() throws Exception {
        // Pre-populate SF with more frames than the in-flight window so the
        // I/O thread enters the window-wait spin during replay.
        final int frameCount = 20;
        final byte[][] frames = new byte[frameCount][];
        for (int i = 0; i < frameCount; i++) {
            frames[i] = new byte[]{(byte) (0xC0 | (i & 0x0F)), (byte) i, 0x55, 0x66, 0x77};
        }
        try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20)) {
            for (byte[] f : frames) {
                long buf = Unsafe.malloc(f.length, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < f.length; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, f[i]);
                    }
                    log.append(buf, f.length);
                } finally {
                    Unsafe.free(buf, f.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
            log.fsync();
        }

        int port = TEST_PORT + 13;
        CloseAfterFirstFrameThenNormalAckHandler handler =
                new CloseAfterFirstFrameThenNormalAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("server start", server.awaitStart(5, TimeUnit.SECONDS));

            try (SegmentLog log = SegmentLog.open(sfDir, 1L << 20);
                 QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                         "localhost", port,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                         QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                         8 /* in-flight window — smaller than frame count */)) {
                sender.setSegmentLog(log);

                // Triggers ensureConnected → I/O thread starts → replay starts.
                // Replay sends frames 0..7, fills the window, enters spin.
                // Server received frame 0, closes. Subsequent spin iterations
                // see isConnected==false and never call tryReceiveAcks.
                sender.flush();

                // Wait for the server to have received the first frame and
                // to have closed connection 1.
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline
                        && handler.framesReceived.get() < 1) {
                    Thread.sleep(10);
                }
                Assert.assertTrue("server received the first replayed frame",
                        handler.framesReceived.get() >= 1);

                // The I/O thread MUST detect the dropped connection and
                // re-enter the reconnect state machine within a reasonable
                // window. With the bug, the spin loop never breaks out of
                // the window-wait; no second connection ever arrives.
                while (System.currentTimeMillis() < deadline
                        && handler.connectionsAccepted.get() < 2) {
                    Thread.sleep(20);
                }
                if (handler.connectionsAccepted.get() < 2) {
                    Assert.fail("BUG: replay spin loop did not detect the "
                            + "mid-replay connection drop. The I/O thread "
                            + "is stuck in replayPersistedFrames's "
                            + "window-wait spin (running=true, "
                            + "isConnected=false, hasWindowSpace=false), "
                            + "preventing the outer state machine from "
                            + "running another doReconnectCycle. "
                            + "framesReceived=" + handler.framesReceived.get()
                            + ", connectionsAccepted="
                            + handler.connectionsAccepted.get()
                            + ". The spin must also exit on "
                            + "!client.isConnected() or "
                            + "reconnectRequested.");
                }
            }
        }
    }

    /** Asserts that opening {@code dir} as a SegmentLog replays exactly {@code expected} frames. */
    private static void assertReplayCount(String dir, int expected) {
        int[] count = {0};
        try (SegmentLog log = SegmentLog.open(dir, 1L << 20)) {
            log.replay((seq, addr, len) -> {
                count[0]++;
                return true;
            });
        }
        Assert.assertEquals("expected " + expected + " replayable frames in "
                + dir + ", saw " + count[0], expected, count[0]);
    }

    /** Counts the number of frames the next replay would visit. */
    private static int countReplayableFrames(String dir) {
        int[] count = {0};
        try (SegmentLog log = SegmentLog.open(dir, 1L << 20)) {
            log.replay((seq, addr, len) -> {
                count[0]++;
                return true;
            });
        }
        return count[0];
    }

    /** Reads an unsigned LEB128 varint from {@code data} starting at {@code pos}. */
    private static long[] readUnsignedVarint(byte[] data, int pos) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = data[pos++];
            value |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                return new long[]{value, pos};
            }
            shift += 7;
            if (shift > 63) {
                throw new IllegalStateException("varint too long");
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

    /**
     * Captures every binary frame but does NOT ack. Used by tests that need
     * to read the SF active segment from the test thread without racing
     * the I/O thread's trim (which under per-frame trim force-rotates the
     * active when every frame is acked).
     */
    private static class CapturingNoAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.List<byte[]> frames = java.util.Collections.synchronizedList(new java.util.ArrayList<>());

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.add(data.clone());
            // intentionally no ack
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
     * Connection 1: receives the first frame, sleeps briefly, closes the
     * connection without acking anything. Connection 2+: acks every frame
     * normally. Used to drive the mid-replay socket-drop path in the
     * replay-spin-hang test.
     */
    private static class CloseAfterFirstFrameThenNormalAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong framesReceived = new AtomicLong(0);
        final AtomicLong connectionsAccepted = new AtomicLong(0);
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, ConnState> perConn =
                new java.util.IdentityHashMap<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            ConnState state;
            synchronized (perConn) {
                state = perConn.get(client);
                if (state == null) {
                    state = new ConnState();
                    state.connIdx = connectionsAccepted.incrementAndGet();
                    perConn.put(client, state);
                }
            }
            framesReceived.incrementAndGet();
            int idxOnConn = state.frameIdx++;
            if (state.connIdx == 1 && idxOnConn == 0) {
                try {
                    Thread.sleep(20);
                    client.close();
                } catch (Exception ignored) {
                }
                return;
            }
            if (state.connIdx >= 2) {
                try {
                    client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
                } catch (IOException ignored) {
                }
            }
        }

        private static class ConnState {
            long connIdx;
            int frameIdx;
        }
    }

    /**
     * On the first incoming binary message, sends a malformed ACK with a
     * sequence far beyond anything the client could have sent. Stays open
     * (silent) thereafter — does not ack subsequent frames and does not
     * close. The I/O thread will eventually fill its window, spin until
     * the test closes the sender (running=false breaks the spin).
     */
    private static class FutureAckThenSilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong framesReceived = new AtomicLong(0);
        volatile boolean bogusAckSent;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long n = framesReceived.incrementAndGet();
            if (n == 1) {
                try {
                    client.sendBinary(EchoSeqAckHandler.buildAck(999_999L));
                    bogusAckSent = true;
                } catch (IOException ignored) {
                }
            }
            // n > 1: silent receive; do not ack and do not close.
        }
    }

    /**
     * Captures every binary frame across all connections, acks each one, then
     * closes the connection so the client must reconnect for the next batch.
     * Used by the symbol-watermark reconnect test which needs the
     * post-reconnect batch's wire bytes to inspect its symbol-delta
     * dictionary.
     */
    private static class AckThenCloseAndCaptureHandler implements TestWebSocketServer.WebSocketServerHandler {
        final java.util.List<byte[]> frames = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        final AtomicLong connections = new AtomicLong(0);
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, int[]> perConn =
                new java.util.IdentityHashMap<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

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
            frames.add(data.clone());
            int idx = count[0]++;
            try {
                client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (idx == 0) {
                // Brief sleep so the ack reaches the client before close, then
                // tear down the connection to force a reconnect on the next batch.
                try {
                    Thread.sleep(20);
                    client.close();
                } catch (Exception ignored) {
                }
            }
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

    /**
     * Acks every binary frame and counts both incoming frames and the number
     * of distinct WebSocket connections opened against the server.
     */
    private static class CountingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, Boolean> seen =
                new java.util.IdentityHashMap<>();
        private final AtomicLong connections = new AtomicLong(0);
        private final AtomicLong frames = new AtomicLong(0);
        private final AtomicLong nextSeq = new AtomicLong(0);

        long connectionCount() {
            return connections.get();
        }

        long frameCount() {
            return frames.get();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            synchronized (seen) {
                if (seen.put(client, Boolean.TRUE) == null) {
                    connections.incrementAndGet();
                }
            }
            frames.incrementAndGet();
            try {
                client.sendBinary(EchoSeqAckHandler.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException ignored) {
                // best-effort
            }
        }
    }

    /**
     * One-shot fault injector for the C1 retry-classification test.
     * <ul>
     *   <li>{@code failNextPayloadWrite}: the next {@code write} whose length
     *       exceeds the SF frame-header size (8 bytes) returns a short count,
     *       which {@code SegmentLog.append} interprets as ENOSPC and raises
     *       {@code SfDiskFullException}. Auto-resets on fire.</li>
     *   <li>{@code failNextFsync}: the next {@code fsync} returns -1, which
     *       {@code SegmentLog.append} (with {@code fsyncEachAppend=true})
     *       turns into {@code SfException}. Auto-resets on fire.</li>
     * </ul>
     */
    /**
     * Counts every {@code ff.fsync(fd)} call. Used by the sf_fsync_on_flush
     * tests to observe whether {@code flush()} routed an fsync to the I/O
     * thread (opt-in path) or skipped it (default path).
     */
    private static class FsyncCountingFacade implements FilesFacade {
        final java.util.concurrent.atomic.AtomicInteger fsyncs = new java.util.concurrent.atomic.AtomicInteger();

        @Override
        public long allocNativePath(String path) {
            return FilesFacade.INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            return FilesFacade.INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return FilesFacade.INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            FilesFacade.INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return FilesFacade.INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return FilesFacade.INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return FilesFacade.INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return FilesFacade.INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            FilesFacade.INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            fsyncs.incrementAndGet();
            return FilesFacade.INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return FilesFacade.INSTANCE.length(fd);
        }

        @Override
        public int lock(int fd) {
            return FilesFacade.INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return FilesFacade.INSTANCE.mkdir(path, mode);
        }

        @Override
        public int openCleanRW(String path, long size) {
            return FilesFacade.INSTANCE.openCleanRW(path, size);
        }

        @Override
        public int openRW(String path) {
            return FilesFacade.INSTANCE.openRW(path);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return FilesFacade.INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return FilesFacade.INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return FilesFacade.INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return FilesFacade.INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.write(fd, addr, len, offset);
        }
    }

    private static class StallThenFsyncFailFacade implements FilesFacade {
        volatile boolean failNextFsync;
        volatile boolean failNextPayloadWrite;

        @Override
        public long allocNativePath(String path) {
            return FilesFacade.INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            return FilesFacade.INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return FilesFacade.INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            FilesFacade.INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return FilesFacade.INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return FilesFacade.INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return FilesFacade.INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return FilesFacade.INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            FilesFacade.INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            if (failNextFsync) {
                failNextFsync = false;
                return -1;
            }
            return FilesFacade.INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return FilesFacade.INSTANCE.length(fd);
        }

        @Override
        public int lock(int fd) {
            return FilesFacade.INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return FilesFacade.INSTANCE.mkdir(path, mode);
        }

        @Override
        public int openCleanRW(String path, long size) {
            return FilesFacade.INSTANCE.openCleanRW(path, size);
        }

        @Override
        public int openRW(String path) {
            return FilesFacade.INSTANCE.openRW(path);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return FilesFacade.INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return FilesFacade.INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return FilesFacade.INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return FilesFacade.INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            // Frame header writes are exactly 8 bytes; payload writes are
            // larger. Discriminate by length without inspecting content.
            if (failNextPayloadWrite && len > 8) {
                failNextPayloadWrite = false;
                // Actually short-write 1 byte so the on-disk state is
                // consistent with the short return value. SegmentLog.append
                // truncates back via ff.truncate before throwing.
                return FilesFacade.INSTANCE.write(fd, addr, 1, offset);
            }
            return FilesFacade.INSTANCE.write(fd, addr, len, offset);
        }
    }

    /**
     * Replies with {@code STATUS_PARSE_ERROR} to every incoming binary frame.
     * Used to provoke the SF reconnect-on-error path and observe whether the
     * client loops indefinitely replaying the same poisoned bytes.
     */
    private static class AlwaysParseErrorHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong frames = new AtomicLong(0);
        private final AtomicLong connections = new AtomicLong(0);
        private final java.util.IdentityHashMap<TestWebSocketServer.ClientHandler, Boolean> seen =
                new java.util.IdentityHashMap<>();

        long connectionCount() {
            return connections.get();
        }

        long frameCount() {
            return frames.get();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            synchronized (seen) {
                if (seen.put(client, Boolean.TRUE) == null) {
                    connections.incrementAndGet();
                }
            }
            frames.incrementAndGet();
            try {
                // Error frame layout: [status u8][sequence u64][msgLen u16][msg bytes]
                String errMsg = "poisoned frame rejected";
                byte[] errBytes = errMsg.getBytes(StandardCharsets.UTF_8);
                byte[] response = new byte[1 + 8 + 2 + errBytes.length];
                ByteBuffer bb = ByteBuffer.wrap(response).order(ByteOrder.LITTLE_ENDIAN);
                bb.put(WebSocketResponse.STATUS_PARSE_ERROR);
                bb.putLong(0L); // server doesn't track real seq for the test
                bb.putShort((short) errBytes.length);
                bb.put(errBytes);
                client.sendBinary(response);
            } catch (IOException ignored) {
                // best-effort; the client may have already disconnected
            }
        }
    }
}
