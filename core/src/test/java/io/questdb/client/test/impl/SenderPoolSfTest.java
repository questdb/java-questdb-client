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

package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.client.TestPorts;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Exhaustive tests for {@link SenderPool} interaction with store-and-forward
 * (SF) slots.
 * <p>
 * The pool reuses one immutable config string for every sender it builds, so
 * before the slot-id fix every SF sender inherited the same {@code sender_id},
 * pointed at the same {@code <sf_dir>/<sender_id>} slot, and the second sender
 * to start died with "sf slot already in use by another process". These tests
 * pin down the fix: each pooled SF sender gets a distinct, stable slot id
 * {@code <base>-<index>}; indices are reused deterministically; a slot is only
 * returned to the free set once its delegate releases the {@code flock}; and
 * the cross-writer guard that the slot lock exists for is still enforced
 * between independent pools.
 */
public class SenderPoolSfTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-pool-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        rmDir(sfDir);
    }

    // ----------------------------------------------------------------------
    // Core fix: the original claim repro -- two concurrent SF senders.
    // ----------------------------------------------------------------------

    @Test
    public void testTwoConcurrentSfSendersGetDistinctSlots() throws Exception {
        // The exact scenario from the bug report: a maxSize=2 SF pool must
        // hand out two live senders. Pre-fix, the second borrow() blew up on
        // the slot flock.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertNotSame("two borrows must be distinct wrappers", a, b);
                        Assert.assertTrue("slot default-0 must exist", Files.exists(slot("default-0")));
                        Assert.assertTrue("slot default-1 must exist", Files.exists(slot("default-1")));
                        Assert.assertEquals("exactly two slot dirs", 2, countSlotDirs());
                    } finally {
                        b.close();
                        a.close();
                    }
                }
            }
        });
    }

    @Test
    public void testGrowToMaxAllSfSendersCoexist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 4, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    PooledSender c = pool.borrow();
                    PooledSender d = pool.borrow();
                    try {
                        Assert.assertEquals(4, pool.totalSize());
                        for (int i = 0; i < 4; i++) {
                            Assert.assertTrue("slot default-" + i + " must exist",
                                    Files.exists(slot("default-" + i)));
                        }
                        Assert.assertEquals(4, countSlotDirs());
                        // At max -- the 5th borrow must time out, not collide.
                        try {
                            pool.borrow();
                            Assert.fail("5th borrow must time out at max=4");
                        } catch (LineSenderException e) {
                            Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
                        }
                    } finally {
                        d.close();
                        c.close();
                        b.close();
                        a.close();
                    }
                }
            }
        });
    }

    @Test
    public void testConfiguredSenderIdUsedAsSlotBase() throws Exception {
        // A sender_id in the config string becomes the base prefix; the pool
        // appends -<index> per slot. This is the knob that lets two pools (or
        // two processes) share one sf_dir without colliding.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";sender_id=myapp;";
                try (SenderPool pool = new SenderPool(config, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    Assert.assertTrue(Files.exists(slot("myapp-0")));
                    Assert.assertTrue(Files.exists(slot("myapp-1")));
                    Assert.assertFalse("no default-* slots when sender_id is set",
                            Files.exists(slot("default-0")));
                    Assert.assertEquals(2, countSlotDirs());
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // Slot lifecycle: reuse, reap-and-reuse, deterministic index recycling.
    // ----------------------------------------------------------------------

    @Test
    public void testReturnedSenderReusesSameSlot() throws Exception {
        // Borrow, return, borrow again: the wrapper (and thus its slot) is
        // recycled, NOT grown into a new slot dir.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender first = pool.borrow();
                    first.close();
                    PooledSender second = pool.borrow();
                    try {
                        Assert.assertSame("returned slot must be recycled", first, second);
                        Assert.assertEquals("no new slot dir on recycle", 1, countSlotDirs());
                        Assert.assertTrue(Files.exists(slot("default-0")));
                    } finally {
                        second.close();
                    }
                }
            }
        });
    }

    @Test
    public void testReapIdleFreesSlotAndIndexIsReused() throws Exception {
        // Grow to max, return all, reap the over-min idle slots, then borrow
        // again. The reaped slot indices must be returned to the free set and
        // re-used -- no new index beyond the original high-water mark, and no
        // "no free SF slot index" / "sf slot already in use" failure.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 3, 1_000, 80, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    PooledSender c = pool.borrow();
                    Assert.assertEquals(3, pool.totalSize());
                    a.close();
                    b.close();
                    c.close();

                    Thread.sleep(150);
                    pool.reapIdle();
                    Assert.assertEquals("reap shrinks to min", 1, pool.totalSize());

                    // High-water mark of slot dirs created so far is 3.
                    int dirsAfterReap = countSlotDirs();
                    Assert.assertTrue("slot dirs persist on disk after reap (>=1)", dirsAfterReap >= 1);

                    // Borrow back up to max. Must reuse the freed indices: no
                    // new slot dir beyond default-0..2 is ever created.
                    PooledSender x = pool.borrow();
                    PooledSender y = pool.borrow();
                    PooledSender z = pool.borrow();
                    try {
                        Assert.assertEquals(3, pool.totalSize());
                        Assert.assertEquals("indices recycled -- no 4th slot dir",
                                3, countSlotDirs());
                        Assert.assertFalse("default-3 must never be created",
                                Files.exists(slot("default-3")));
                    } finally {
                        x.close();
                        y.close();
                        z.close();
                    }
                }
            }
        });
    }

    @Test
    public void testRepeatedSaturationNeverExhaustsSlotIndices() throws Exception {
        // Regression guard for the cap/slot-allocator invariant: hammer the
        // pool through many full saturate/return/reap cycles. If freeing and
        // the closingSlots accounting ever drifted, allocateSlotIndex() would
        // throw "no free SF slot index" or a borrow would collide on a flock.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 3, 2_000, 1, Long.MAX_VALUE)) {
                    for (int cycle = 0; cycle < 20; cycle++) {
                        PooledSender a = pool.borrow();
                        PooledSender b = pool.borrow();
                        PooledSender c = pool.borrow();
                        Assert.assertEquals(3, pool.totalSize());
                        a.close();
                        b.close();
                        c.close();
                        pool.reapIdle();
                    }
                    // Never grew past max -- indices stayed within [0,3).
                    Assert.assertEquals(3, countSlotDirs());
                    Assert.assertFalse(Files.exists(slot("default-3")));
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // End-to-end ingest through pooled SF senders.
    // ----------------------------------------------------------------------

    @Test
    public void testEndToEndIngestThroughPooledSenders() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 3, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    try {
                        a.table("t1").longColumn("v", 1L).atNow();
                        a.flush();
                        b.table("t2").longColumn("v", 2L).atNow();
                        b.flush();
                        Assert.assertTrue("server must receive frames from both pooled senders",
                                awaitAtLeast(handler.frames, 2, 5_000));
                    } finally {
                        b.close();
                        a.close();
                    }
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // Cross-writer guard is preserved between independent pools / processes.
    // ----------------------------------------------------------------------

    @Test
    public void testSecondPoolSameSfDirSameBaseFailsFast() throws Exception {
        // The slot flock is the multi-writer footgun guard. Two pools sharing
        // one sf_dir with the same base would both try slot <base>-0; the
        // second must fail fast rather than interleave FSNs on disk. The pool
        // fix must NOT weaken this contract.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool1 = new SenderPool(config, 1, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    Assert.assertTrue(Files.exists(slot("default-0")));
                    try {
                        SenderPool pool2 = new SenderPool(config, 1, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
                        pool2.close();
                        Assert.fail("second pool on same sf_dir+base must fail on the slot lock");
                    } catch (IllegalStateException e) {
                        Assert.assertTrue("message must name the slot-in-use contract, was: " + e.getMessage(),
                                e.getMessage().contains("sf slot already in use"));
                    }
                }
            }
        });
    }

    @Test
    public void testTwoPoolsDistinctBaseShareSfDir() throws Exception {
        // Distinct sender_id base per pool -> distinct slot dirs -> both pools
        // coexist on one sf_dir and both can ingest.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String configA = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";sender_id=appA;";
                String configB = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";sender_id=appB;";
                try (SenderPool poolA = new SenderPool(configA, 1, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE);
                     SenderPool poolB = new SenderPool(configB, 1, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = poolA.borrow();
                    PooledSender b = poolB.borrow();
                    try {
                        Assert.assertTrue(Files.exists(slot("appA-0")));
                        Assert.assertTrue(Files.exists(slot("appB-0")));
                        a.table("t").longColumn("v", 1L).atNow();
                        a.flush();
                        b.table("t").longColumn("v", 2L).atNow();
                        b.flush();
                        Assert.assertTrue(awaitAtLeast(handler.frames, 2, 5_000));
                    } finally {
                        b.close();
                        a.close();
                    }
                }
            }
        });
    }

    @Test
    public void testCloseReleasesAllSlots() throws Exception {
        // After the pool closes, every slot flock must be released so the
        // dirs can be re-acquired -- by a fresh pool or a standalone sender.
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                SenderPool pool = new SenderPool(config, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
                PooledSender a = pool.borrow();
                PooledSender b = pool.borrow();
                // Leave them borrowed: close() must still release their flocks.
                pool.close();

                // A fresh pool over the same dirs must re-acquire slot 0 and 1.
                try (SenderPool reopened = new SenderPool(config, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    Assert.assertEquals(2, reopened.totalSize());
                    Assert.assertTrue(Files.exists(slot("default-0")));
                    Assert.assertTrue(Files.exists(slot("default-1")));
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // Recovery: stable slot ids let a re-created pool re-adopt unacked data.
    // ----------------------------------------------------------------------

    @Test
    public void testRecoveryReplayThroughPooledSlot() throws Exception {
        // Phase 1: write rows to a slot against a silent server (no acks), so
        // the data persists unacked on disk under default-0. Close.
        // Phase 2: a new pool against an ack-ing server re-adopts default-0
        // (stable index) and replays the unacked frames. Stable, deterministic
        // slot ids are exactly what make this recovery possible.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1 -- silent server.
            int silentPort = TestPorts.findUnusedPort();
            try (TestWebSocketServer silent = new TestWebSocketServer(silentPort, new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg1 = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=500;";
                try (SenderPool pool = new SenderPool(cfg1, 1, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender s = pool.borrow();
                    for (int i = 0; i < 3; i++) {
                        s.table("recover").longColumn("v", i).atNow();
                        s.flush();
                    }
                    s.close();
                }
            }
            // Data must be on disk, unacked, under default-0.
            Assert.assertTrue("unacked data must persist on disk", hasSegmentFile(slot("default-0")));

            // Phase 2 -- ack-ing server, brand-new pool, same sf_dir.
            int ackPort = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(ackPort, handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg2 = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(cfg2, 1, 1, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender s = pool.borrow();
                    try {
                        // Drain replays the recovered, unacked frames.
                        s.drain(5_000);
                        Assert.assertTrue("recovered frames must be replayed to the new server",
                                awaitAtLeast(handler.frames, 1, 5_000));
                    } finally {
                        s.close();
                    }
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // Concurrency stress: borrow/return churn must never collide on a slot.
    // ----------------------------------------------------------------------

    @Test
    public void testConcurrentBorrowReturnStress() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                final int threads = 6;
                final int iterations = 25;
                try (SenderPool pool = new SenderPool(config, 1, 4, 10_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    final CountDownLatch start = new CountDownLatch(1);
                    final CountDownLatch done = new CountDownLatch(threads);
                    final AtomicReference<Throwable> failure = new AtomicReference<>();
                    for (int t = 0; t < threads; t++) {
                        final int id = t;
                        Thread worker = new Thread(() -> {
                            try {
                                start.await();
                                for (int i = 0; i < iterations; i++) {
                                    PooledSender s = pool.borrow();
                                    try {
                                        s.table("stress").longColumn("thread", id)
                                                .longColumn("i", i).atNow();
                                        s.flush();
                                    } finally {
                                        s.close();
                                    }
                                }
                            } catch (Throwable e) {
                                failure.compareAndSet(null, e);
                            } finally {
                                done.countDown();
                            }
                        });
                        worker.start();
                    }
                    start.countDown();
                    Assert.assertTrue("workers must finish", done.await(60, TimeUnit.SECONDS));
                    if (failure.get() != null) {
                        throw new AssertionError("concurrent borrow/return failed", failure.get());
                    }
                    // Invariants after the storm.
                    Assert.assertTrue("totalSize within max", pool.totalSize() <= 4);
                    Assert.assertTrue("available <= total",
                            pool.availableSize() <= pool.totalSize());
                    Assert.assertTrue("no slot dir beyond max created", countSlotDirs() <= 4);
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // Helpers.
    // ----------------------------------------------------------------------

    private String slot(String name) {
        return sfDir + "/" + name;
    }

    private int countSlotDirs() {
        if (!Files.exists(sfDir)) {
            return 0;
        }
        int count = 0;
        long find = Files.findFirst(sfDir);
        if (find <= 0) {
            return 0;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
                if (name == null || ".".equals(name) || "..".equals(name)) {
                    continue;
                }
                // Slot dirs are the only children the pool creates under sfDir.
                if (Files.exists(sfDir + "/" + name + "/.lock")) {
                    count++;
                }
            }
        } finally {
            Files.findClose(find);
        }
        return count;
    }

    private static boolean hasSegmentFile(String slotPath) {
        if (!Files.exists(slotPath)) {
            return false;
        }
        long find = Files.findFirst(slotPath);
        if (find <= 0) {
            return false;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
                if (name != null && name.endsWith(".sfa")) {
                    return true;
                }
            }
        } finally {
            Files.findClose(find);
        }
        return false;
    }

    private static boolean awaitAtLeast(AtomicInteger counter, int target, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (counter.get() >= target) {
                return true;
            }
            Thread.sleep(10);
        }
        return counter.get() >= target;
    }

    private static void rmDir(String dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            rmDir(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /**
     * Acks every binary frame with a per-connection running sequence (each
     * pooled sender is its own WebSocket connection, so each needs its own
     * FSN counter), and counts total frames received across all connections.
     */
    private static final class CountingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger frames = new AtomicInteger();
        private final Map<TestWebSocketServer.ClientHandler, AtomicLong> seqByClient =
                new ConcurrentHashMap<>();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.incrementAndGet();
            AtomicLong seq = seqByClient.computeIfAbsent(client, c -> new AtomicLong(0));
            try {
                client.sendBinary(buildAck(seq.getAndIncrement()));
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

    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // No ack -- frames stay unacked on disk for the recovery test.
        }
    }
}
