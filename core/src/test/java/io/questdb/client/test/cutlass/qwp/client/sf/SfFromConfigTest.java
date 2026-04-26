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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SfFromConfigTest {

    private static final int TEST_PORT = 19_900 + (int) (System.nanoTime() % 100);

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-config-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        rmDir(sfDir);
    }

    @Test
    public void testFromConfigEnablesSfAndOwnsLog() throws Exception {
        int port = TEST_PORT + 1;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";store_and_forward=on;sf_dir=" + sfDir + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 42L).atNow();
                sender.flush();
            }
            // SF dir was created by the sender via SegmentLog.open
            Assert.assertTrue("sfDir created", Files.exists(sfDir));
            // After sender close, the SegmentLog lock file should be released —
            // re-opening it must succeed.
            try (SegmentLog reopened = SegmentLog.open(sfDir, 1L << 20)) {
                Assert.assertTrue("reopen after sender close succeeds", reopened.nextSeq() >= 0);
            }
        }
    }

    @Test
    public void testStoreAndForwardOnWithoutDirRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject store_and_forward=on without sf_dir");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("requires sf_dir"));
        }
    }

    @Test
    public void testSfDirWithoutStoreAndForwardRejected() {
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject sf_dir without store_and_forward=on");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("store_and_forward is not enabled"));
        }
    }

    @Test
    public void testStoreAndForwardOnTcpRejected() {
        String config = "tcp::addr=localhost:9009;store_and_forward=on;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected build() to reject store_and_forward on TCP");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testSfMaxBytesParsing() throws Exception {
        int port = TEST_PORT + 2;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir + ";sf_max_bytes=131072;";
            try (Sender sender = Sender.fromConfig(config)) {
                // Write enough data that segments rotate at ~128 KiB boundary.
                for (int i = 0; i < 50; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                }
                sender.flush();
            }
            // Just confirm SF dir was populated; rotation under load is exercised
            // exhaustively in SegmentLogTest.
            Assert.assertTrue("sfDir was used", Files.exists(sfDir));
        }
    }

    @Test
    public void testStoreAndForwardOffIgnoresSfDir() throws Exception {
        // Without store_and_forward=on, sf_dir isn't a valid combo (sender errors).
        // But store_and_forward=off without sf_dir should be a clean no-op.
        int port = TEST_PORT + 3;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";store_and_forward=off;";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
            Assert.assertFalse("no sf dir created", Files.exists(sfDir));
        }
    }

    @Test
    public void testInvalidStoreAndForwardValueRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=maybe;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection of invalid value");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid store_and_forward"));
        }
    }

    /**
     * SF disk-full back-pressures user thread via flush(): when the configured
     * sf_max_total_bytes is reached, flush() blocks until ACKs trim sealed
     * segments and free space. The user code never sees an error.
     */
    @Test
    public void testDiskFullBackpressureUnblocksAfterAck() throws Exception {
        int port = TEST_PORT + 4;
        // Slow-acking server: each batch acked after 1.5 s. The user thread
        // sends faster than the server can ACK, so SF disk fills before any
        // trim runs — disk-full path triggers reliably.
        DelayedAckHandler handler = new DelayedAckHandler(1_500);
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // Each per-row batch is ~30 B over the wire. With segment cap 128 B
            // and total cap 256 B, the disk fills after ~6 batches. The user
            // thread sends 20 → multiple disk-full stalls before ACKs free space.
            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_max_bytes=128"
                    + ";sf_max_total_bytes=256"
                    + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                Assert.assertTrue(sender instanceof QwpWebSocketSender);
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                // Send a flood of batches faster than ACKs can drain.
                for (int i = 0; i < 20; i++) {
                    sender.table("foo").longColumn("v", (long) i).atNow();
                    sender.flush();
                }
                long stalls = wsSender.getTotalSfDiskFullStalls();
                Assert.assertTrue(
                        "expected at least one disk-full stall, saw " + stalls,
                        stalls > 0);
            }
        }
    }

    /**
     * Red test for bugs C1 + C2 in the SF disk-full retry path.
     * <p>
     * When {@code segmentLog.append} throws {@link
     * io.questdb.client.cutlass.qwp.client.sf.SfDiskFullException} from inside
     * {@code WebSocketSendQueue.sendBatch}, the buffer state has already been
     * advanced from SEALED to SENDING (line 1019) and {@code nextBatchSequence}
     * has been bumped (line 1028). When the I/O loop later calls
     * {@code retryStalled() -> sendBatch(batch)}, {@code markSending()} throws
     * {@code IllegalStateException} because the buffer is in SENDING, not
     * SEALED. The retry catch recycles the buffer without ever calling
     * {@code segmentLog.append} a second time, so the bytes the user wrote
     * are never persisted. Under SF + reconnector mode the wrapped failure is
     * non-fatal, so the user's {@code flush()} returns success and the data
     * is lost silently.
     * <p>
     * Repro shape:
     * <ul>
     *   <li>Slow-acking server (~500 ms per batch) so disk fills before any trim.</li>
     *   <li>Tight SF caps so multiple batches hit {@code SfDiskFullException}.</li>
     *   <li>Each batch uses a uniquely-named table so we can detect missing
     *       batches by scanning captured wire frames for the table name in
     *       plaintext UTF-8 (the QWP schema header carries it verbatim the
     *       first time a schema is sent).</li>
     *   <li>After {@code close()}, a second sender re-opens the same SF dir to
     *       drive replay of any unacked frames left on disk.</li>
     * </ul>
     * The test then asserts that every original batch's table name appears in
     * the server's captured frames. With C1 + C2 in place, at least one is
     * missing because the disk-full retry path never persisted it.
     */
    /**
     * Regression test for bug M3 — connect-string {@code sf_max_bytes} and
     * {@code sf_max_total_bytes} were parsed via {@code parseIntValue} which
     * threw {@code NumericException} for values &gt; {@link Integer#MAX_VALUE}
     * (~2.1 GB), artificially capping the SF size from the connect string
     * even though the builder API and {@code SegmentLog} accept {@code long}.
     * This test exercises a 4 GB total cap from the connect string.
     */
    @Test
    public void testSfMaxTotalBytesAcceptsLargeValue() throws Exception {
        int port = TEST_PORT + 8;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // 4 GiB > Integer.MAX_VALUE; pre-fix this would throw "invalid sf_max_total_bytes".
            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_max_total_bytes=" + (4L * 1024 * 1024 * 1024) + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
        }
    }

    @Test
    public void testDiskFullRetryDoesNotLoseUserData() throws Exception {
        int port = TEST_PORT + 6;
        int totalBatches = 20;
        CapturingDelayedAckHandler handler = new CapturingDelayedAckHandler(500);

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_max_bytes=128"
                    + ";sf_max_total_bytes=256;";
            try (Sender sender = Sender.fromConfig(config)) {
                Assert.assertTrue(sender instanceof QwpWebSocketSender);
                QwpWebSocketSender wsSender = (QwpWebSocketSender) sender;
                for (int i = 0; i < totalBatches; i++) {
                    sender.table(uniqueTableName(i)).longColumn("v", (long) i).atNow();
                    sender.flush();
                }
                Assert.assertTrue(
                        "test must hit at least one disk-full stall to be a real repro of C1/C2; saw 0",
                        wsSender.getTotalSfDiskFullStalls() > 0);
            }

            // close() under SF returns once data is on disk; some frames may
            // still be unacked. Re-open against the same dir to drive replay.
            String drainConfig = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_max_bytes=128;sf_max_total_bytes=" + (1L << 20) + ";";
            try (Sender drain = Sender.fromConfig(drainConfig)) {
                drain.flush();
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline) {
                    int seen = 0;
                    for (int i = 0; i < totalBatches; i++) {
                        if (handler.sawTableName(uniqueTableName(i))) {
                            seen++;
                        }
                    }
                    if (seen == totalBatches) break;
                    Thread.sleep(100);
                }
            }

            StringBuilder missing = new StringBuilder();
            for (int i = 0; i < totalBatches; i++) {
                String name = uniqueTableName(i);
                if (!handler.sawTableName(name)) {
                    if (missing.length() > 0) missing.append(", ");
                    missing.append(name);
                }
            }
            Assert.assertEquals(
                    "every batch the user wrote must reach the server "
                            + "(directly or via SF replay); missing batches: " + missing,
                    "", missing.toString());
        }
    }

    private static String uniqueTableName(int i) {
        // Fixed-width zero-padded so no name is a substring of another, e.g.
        // "tbl_07" vs "tbl_71". The byte-search in the handler relies on this.
        return String.format("tbl_%02d", i);
    }

    /**
     * Red test for bug C3 — pendingBuffer dropped on every reconnect attempt
     * without SF persistence.
     * <p>
     * {@code WebSocketSendQueue.doReconnectCycle} unconditionally polls and
     * recycles {@code pendingBuffer} at lines 783-794, before any reconnect
     * logic runs. {@code segmentLog.append} is never called for it.
     * <p>
     * The reliable repro: make reconnects FAIL repeatedly. While the I/O
     * thread is sleeping inside a failed {@code doReconnectCycle} (between
     * its drop step and the doomed reconnect attempt), the user thread can
     * enqueue a batch. The very next {@code doReconnectCycle} entry drops
     * that batch, then sleeps again, the user enqueues the next batch, the
     * cycle after drops it, and so on — every batch enqueued during the
     * outage is silently lost.
     * <p>
     * Repro shape:
     * <ul>
     *   <li>Server S1 accepts and acks normally.</li>
     *   <li>Sender connects, sends a couple of batches successfully.</li>
     *   <li>Test thread shuts S1 down. Sender's I/O thread starts cycling
     *       through failed reconnect attempts (port refused).</li>
     *   <li>A producer thread keeps enqueueing the remaining batches during
     *       the outage. Most of them land in {@code pendingBuffer} and get
     *       dropped by subsequent {@code doReconnectCycle} entries.</li>
     *   <li>After a 2 s outage, server S2 starts on the same port. Reconnect
     *       succeeds; replay flushes whatever made it to SF. Anything dropped
     *       by C3 is gone for good.</li>
     * </ul>
     * The test fails because at least one batch's table name never appears
     * in the server's captured frames after the dust settles.
     */
    @Test
    public void testReconnectDoesNotLoseEnqueuedBuffer() throws Exception {
        int port = TEST_PORT + 7;
        int totalBatches = 30;
        CapturingDelayedAckHandler handler = new CapturingDelayedAckHandler(0);

        TestWebSocketServer s1 = new TestWebSocketServer(port, handler);
        s1.start();
        Assert.assertTrue(s1.awaitStart(5, TimeUnit.SECONDS));

        String config = "ws::addr=localhost:" + port
                + ";store_and_forward=on;sf_dir=" + sfDir
                + ";sf_max_bytes=" + (1L << 16)
                + ";sf_max_total_bytes=" + (1L << 20)
                + ";";
        Sender sender = Sender.fromConfig(config);
        try {
            // Warm-up: a few batches go through cleanly so we know the
            // baseline path works and the I/O thread is humming.
            for (int i = 0; i < 3; i++) {
                sender.table(uniqueTableName(i)).longColumn("v", (long) i).atNow();
                sender.flush();
            }

            // Kick the server out. The I/O thread will start cycling on
            // doReconnectCycle, each entry dropping any pendingBuffer.
            s1.close();

            // Producer thread: keep pushing batches while reconnect attempts fail.
            Thread producer = new Thread(() -> {
                for (int i = 3; i < totalBatches; i++) {
                    try {
                        sender.table(uniqueTableName(i)).longColumn("v", (long) i).atNow();
                        sender.flush();
                    } catch (Exception ignored) {
                        // SF mode swallows transient errors; flush() should not throw.
                    }
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "c3-producer");
            producer.start();

            // 2-second outage during which doReconnectCycle keeps firing
            // its drop step on each retry.
            Thread.sleep(2_000);

            // Bring the server back up on the same port; the sender will
            // reconnect on its next attempt.
            try (TestWebSocketServer s2 = new TestWebSocketServer(port, handler)) {
                s2.start();
                Assert.assertTrue(s2.awaitStart(5, TimeUnit.SECONDS));

                producer.join(20_000);
                Assert.assertFalse("producer thread did not finish", producer.isAlive());

                // Wait for replay + ACKs to drain.
                long deadline = System.currentTimeMillis() + 15_000;
                while (System.currentTimeMillis() < deadline) {
                    int seen = 0;
                    for (int i = 0; i < totalBatches; i++) {
                        if (handler.sawTableName(uniqueTableName(i))) {
                            seen++;
                        }
                    }
                    if (seen == totalBatches) break;
                    Thread.sleep(100);
                }
            } finally {
                sender.close();
            }
        } catch (Throwable t) {
            try { sender.close(); } catch (Throwable ignored) {}
            try { s1.close(); } catch (Throwable ignored) {}
            throw t;
        }

        StringBuilder missing = new StringBuilder();
        for (int i = 0; i < totalBatches; i++) {
            String name = uniqueTableName(i);
            if (!handler.sawTableName(name)) {
                if (missing.length() > 0) missing.append(", ");
                missing.append(name);
            }
        }
        Assert.assertEquals(
                "every batch the user wrote must reach the server "
                        + "(directly or via SF replay); missing batches: " + missing,
                "", missing.toString());
    }

    @Test
    public void testSfFsyncParsesAndWorks() throws Exception {
        int port = TEST_PORT + 5;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // sf_fsync=on forces fsync on every append. The test mostly proves
            // the connect-string parses, the path is wired, and basic send works.
            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_fsync=on;";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
            }
            Assert.assertTrue(Files.exists(sfDir));
        }
    }

    @Test
    public void testInvalidSfFsyncValueRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;sf_dir=" + sfDir
                + ";sf_fsync=maybe;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid sf_fsync"));
        }
    }

    /**
     * sf_fsync_on_flush is opt-in. Verify the connect-string parses both
     * values and the wiring reaches the sender (basic round-trip — the
     * actual fsync-on-flush behaviour is exercised in WebSocketSendQueueTest
     * with a counting FilesFacade).
     */
    @Test
    public void testSfFsyncOnFlushParses() throws Exception {
        int port = TEST_PORT + 6;
        AckHandler handler = new AckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String config = "ws::addr=localhost:" + port
                    + ";store_and_forward=on;sf_dir=" + sfDir
                    + ";sf_fsync_on_flush=on;";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
            Assert.assertTrue(Files.exists(sfDir));
        }
    }

    @Test
    public void testInvalidSfFsyncOnFlushValueRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;sf_dir=" + sfDir
                + ";sf_fsync_on_flush=maybe;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("invalid sf_fsync_on_flush"));
        }
    }

    @Test
    public void testSfFsyncOnFlushOnTcpRejected() {
        String config = "tcp::addr=localhost:1;sf_fsync_on_flush=on;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("WebSocket"));
        }
    }

    @Test
    public void testStoreAndForwardWithSyncWindowRejected() {
        String config = "ws::addr=localhost:1;store_and_forward=on;sf_dir=" + sfDir
                + ";in_flight_window=1;";
        try (Sender ignored = Sender.fromConfig(config)) {
            Assert.fail("expected rejection of SF with sync mode");
        } catch (LineSenderException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("async"));
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

    /**
     * Like {@link DelayedAckHandler} but also retains every received frame so
     * tests can assert on payload content (e.g. that a given table-name byte
     * pattern reached the server).
     */
    private static class CapturingDelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final List<byte[]> frames = Collections.synchronizedList(new ArrayList<>());

        CapturingDelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.add(data);
            long seq = nextSeq.getAndIncrement();
            new Thread(() -> {
                try {
                    Thread.sleep(delayMs);
                    client.sendBinary(AckHandler.buildAck(seq));
                } catch (Exception ignored) {
                }
            }, "capturing-delayed-acker").start();
        }

        boolean sawTableName(String name) {
            byte[] needle = name.getBytes(StandardCharsets.UTF_8);
            synchronized (frames) {
                for (byte[] frame : frames) {
                    if (containsBytes(frame, needle)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private static boolean containsBytes(byte[] hay, byte[] needle) {
            if (needle.length == 0 || needle.length > hay.length) return false;
            outer:
            for (int i = 0, n = hay.length - needle.length; i <= n; i++) {
                for (int j = 0; j < needle.length; j++) {
                    if (hay[i + j] != needle[j]) continue outer;
                }
                return true;
            }
            return false;
        }
    }

    /** Acks each frame after a configurable delay, on a background thread. */
    private static class DelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong(0);

        DelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long seq = nextSeq.getAndIncrement();
            new Thread(() -> {
                try {
                    Thread.sleep(delayMs);
                    client.sendBinary(AckHandler.buildAck(seq));
                } catch (Exception ignored) {
                }
            }, "delayed-acker").start();
        }
    }
}
