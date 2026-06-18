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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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

    @Test
    public void testSlotLeakedWhenDelegateCloseDoesNotReleaseFlock() throws Exception {
        // Latent-fragility guard (M2): the pool returns a slot index to the
        // free set ONLY after the delegate's close() has released the SF
        // flock. If close() returns with the flock still held (it bailed out
        // early with the I/O thread still running), the index must stay
        // reserved forever -- otherwise the pool would hand the still-locked
        // dir to the next borrow and resurrect "sf slot already in use".
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 2, 500, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    Assert.assertTrue(Files.exists(slot("default-0")));

                    // Tear the delegate down for real first (so the test leaks
                    // no native resources), then forge the exact symptom:
                    // close() returned WITHOUT clearing slotLockReleased.
                    // close() is idempotent, so the discardBroken re-close
                    // below is a no-op and leaves the forged flag in place.
                    Sender delegate = getDelegate(a);
                    delegate.close();
                    setBooleanField(delegate, "slotLockReleased", false);

                    // Route the wrapper through the pool's broken-eviction path.
                    invokeDiscardBroken(pool, a);

                    // The leaked index must NOT be returned to the free set,
                    // and capacity must be accounted as permanently consumed.
                    Assert.assertEquals("one slot must be retired as leaked",
                            1, getIntField(pool, "leakedSlots"));
                    boolean[] slotInUse = (boolean[]) getField(pool, "slotInUse");
                    Assert.assertTrue("leaked slot index 0 must stay reserved", slotInUse[0]);

                    // The next borrow must take a fresh index -- never reuse the
                    // still-locked default-0 dir.
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertTrue("new borrow must use a fresh slot dir",
                                Files.exists(slot("default-1")));
                        Assert.assertEquals(2, countSlotDirs());

                        // Capacity is permanently reduced by the leaked slot:
                        // max=2, one leaked + one live => the next borrow times
                        // out rather than colliding on the locked dir.
                        try {
                            pool.borrow();
                            Assert.fail("capacity must be reduced by the leaked slot");
                        } catch (LineSenderException e) {
                            Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
                        }
                    } finally {
                        b.close();
                    }
                }
            }
        });
    }

    @Test
    public void testLeakedSlotIsObservable() throws Exception {
        // M1 (observability): when a delegate's close() returns with the SF
        // flock still held, the pool retires the slot forever and silently
        // shrinks capacity. SenderPool has no logger today, so a pool that
        // bleeds capacity this way degrades to "every borrow() times out"
        // with nothing in the logs to explain why. Pin the contract: the
        // leakedSlots++ path MUST emit a WARN (or louder) that names the
        // retired slot. This test is RED until that log line is added.
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Capture everything SenderPool logs (logback is the SLF4J
                // binding on the test classpath).
                Logger poolLogger = (Logger) LoggerFactory.getLogger(SenderPool.class);
                ListAppender<ILoggingEvent> appender = new ListAppender<>();
                appender.start();
                Level savedLevel = poolLogger.getLevel();
                poolLogger.setLevel(Level.ALL);
                poolLogger.addAppender(appender);
                try {
                    String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                    try (SenderPool pool = new SenderPool(config, 1, 2, 500, Long.MAX_VALUE, Long.MAX_VALUE)) {
                        PooledSender a = pool.borrow();

                        // Forge the exact leak symptom: close() returned with
                        // the flock still held (I/O thread refused to stop).
                        // Tear the delegate down for real first so the test
                        // leaks no native resources; close() is idempotent so
                        // discardBroken's re-close leaves the forged flag set.
                        Sender delegate = getDelegate(a);
                        delegate.close();
                        setBooleanField(delegate, "slotLockReleased", false);
                        invokeDiscardBroken(pool, a);

                        // Sanity: the slot really was retired as leaked.
                        Assert.assertEquals("precondition: one slot must leak",
                                1, getIntField(pool, "leakedSlots"));
                        // The leak must be observable via public API (metric).
                        Assert.assertEquals("leaked slot must be observable via leakedSlotCount()",
                                1, pool.leakedSlotCount());

                        // Contract under test: the leak must be observable.
                        boolean warned = appender.list.stream().anyMatch(e ->
                                e.getLevel().isGreaterOrEqual(Level.WARN)
                                        && e.getFormattedMessage().toLowerCase().contains("slot"));
                        Assert.assertTrue(
                                "leakedSlots++ must emit a WARN naming the retired slot, "
                                        + "otherwise capacity loss is invisible; captured events="
                                        + appender.list,
                                warned);
                    }
                } finally {
                    poolLogger.detachAppender(appender);
                    poolLogger.setLevel(savedLevel);
                    appender.stop();
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
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
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
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
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

    @Test
    public void testConcurrentFirstBorrowsWithMinZeroRaceOnSfDir() throws Exception {
        // C2 regression: senderPoolMin(0) means no single-threaded pre-warm,
        // so the shared parent sf_dir is NOT created at construction (the
        // constructor probe only parses the config). The first concurrent
        // borrows then race into build() -> Files.mkdir(sfDir) outside the
        // pool lock. Pre-fix, the mkdir loser got a non-zero rc (EEXIST) and
        // its borrow() threw "could not create sf_dir" on a perfectly healthy
        // pool. Post-fix, a benign creation race is treated as success.
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                // minSize=0 -> no pre-warm -> sf_dir absent until first borrow.
                try (SenderPool pool = new SenderPool(config, 0, 4, 10_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    Assert.assertFalse("sf_dir must not exist before the first borrow",
                            Files.exists(sfDir));

                    final int threads = 4;
                    final CyclicBarrier barrier = new CyclicBarrier(threads);
                    final CountDownLatch done = new CountDownLatch(threads);
                    final AtomicReference<Throwable> failure = new AtomicReference<>();
                    final PooledSender[] borrowed = new PooledSender[threads];
                    for (int t = 0; t < threads; t++) {
                        final int id = t;
                        Thread worker = new Thread(() -> {
                            try {
                                // Align all first borrows so they race into the
                                // shared parent mkdir simultaneously.
                                barrier.await();
                                borrowed[id] = pool.borrow();
                            } catch (Throwable e) {
                                failure.compareAndSet(null, e);
                            } finally {
                                done.countDown();
                            }
                        });
                        worker.start();
                    }
                    Assert.assertTrue("workers must finish", done.await(30, TimeUnit.SECONDS));
                    try {
                        if (failure.get() != null) {
                            throw new AssertionError(
                                    "concurrent first borrows must not race on sf_dir", failure.get());
                        }
                        Assert.assertEquals(threads, pool.totalSize());
                        Assert.assertEquals("one slot dir per borrow", threads, countSlotDirs());
                    } finally {
                        for (PooledSender s : borrowed) {
                            if (s != null) {
                                s.close();
                            }
                        }
                    }
                }
            }
        });
    }

    // ----------------------------------------------------------------------
    // drain_orphans=on + pool: the pool must NOT treat its own sibling slots
    // as drainable orphans, but MUST still drain genuine foreign leftovers.
    // ----------------------------------------------------------------------

    @Test
    public void testDrainOrphansPoolDoesNotCannibalizeSiblingSlots() throws Exception {
        // Regression guard. The pool gives each SF sender a sibling slot
        // <base>-<index>. With drain_orphans=on, every pooled build runs an
        // orphan scan -- and before the namespace-exclusion fix that scan
        // listed the pool's OWN siblings (default-1 holds unacked .sfa) as
        // orphans and dispatched a background drainer at them. That drainer
        // could win a sibling's flock and re-surface the exact
        // "sf slot already in use" collision the per-slot ids were added to
        // prevent. After the fix the pool fences off its whole "<base>-"
        // namespace, so building a drain_orphans pool over pre-existing
        // sibling data is clean and the data is recovered by the pool itself.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: seed unacked data into default-0 AND default-1 via a
            // plain (no drain_orphans) pool against a silent server.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=500;";
                try (SenderPool seed = new SenderPool(seedCfg, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = seed.borrow();
                    PooledSender b = seed.borrow();
                    a.table("recover").longColumn("v", 1L).atNow();
                    a.flush();
                    b.table("recover").longColumn("v", 2L).atNow();
                    b.flush();
                    b.close();
                    a.close();
                }
            }
            Assert.assertTrue("default-0 must hold unacked data", hasSegmentFile(slot("default-0")));
            Assert.assertTrue("default-1 must hold unacked data", hasSegmentFile(slot("default-1")));

            // Phase 2: a drain_orphans=on pool over the same sf_dir. Pre-fix
            // this construction could throw "sf slot already in use"; post-fix
            // it is deterministically clean -- no drainer targets a sibling.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir
                        + ";drain_orphans=on;";
                try (SenderPool pool = new SenderPool(cfg, 2, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    Assert.assertEquals(2, pool.totalSize());
                    Assert.assertEquals("no extra slot dirs spawned by a rogue drainer",
                            2, countSlotDirs());
                    // A drainer must NOT have given up on a sibling slot.
                    Assert.assertFalse("sibling must not be flagged .failed",
                            Files.exists(slot("default-0") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                    Assert.assertFalse("sibling must not be flagged .failed",
                            Files.exists(slot("default-1") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

                    // The pool owns and recovers both slots: borrowing + draining
                    // replays the recovered frames through the legitimate senders.
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    try {
                        a.drain(5_000);
                        b.drain(5_000);
                        Assert.assertTrue("both slots' recovered data must replay",
                                awaitAtLeast(handler.frames, 2, 5_000));
                    } finally {
                        b.close();
                        a.close();
                    }
                }
            }
        });
    }

    @Test
    public void testDrainOrphansPoolStillDrainsForeignOrphan() throws Exception {
        // The fix excludes only the pool's OWN "<base>-" namespace -- a
        // genuine foreign leftover (a different sender_id base) must still be
        // adopted and drained, otherwise we would have silently disabled the
        // drain_orphans feature for pooled deployments.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: a standalone sender with a DIFFERENT base leaves unacked
            // data under <sf_dir>/legacy.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                // close_flush_timeout_millis=0 => close() does not drain, so
                // the flushed-but-unacked frames stay on disk (silent server
                // never acks) and close() never throws a drain timeout.
                String ghostCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";sender_id=legacy;close_flush_timeout_millis=0;";
                try (Sender ghost = Sender.fromConfig(ghostCfg)) {
                    for (int i = 0; i < 3; i++) {
                        ghost.table("foreign").longColumn("v", i).atNow();
                        ghost.flush();
                    }
                } catch (Exception ignored) {
                    // best-effort: we only need the unacked .sfa on disk
                }
            }
            Assert.assertTrue("foreign leftover must hold unacked data",
                    hasSegmentFile(slot("legacy")));

            // Phase 2: a drain_orphans=on pool with the default base. Its
            // pooled senders are default-*, so "legacy" is NOT in the excluded
            // namespace -- the background drainer must adopt it, replay its
            // frames to the ack server, and clear the slot.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir
                        + ";drain_orphans=on;";
                try (SenderPool pool = new SenderPool(cfg, 1, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender s = pool.borrow();
                    try {
                        Assert.assertTrue("foreign orphan frames must be replayed by a drainer",
                                awaitAtLeast(handler.frames, 1, 10_000));
                        Assert.assertTrue("foreign orphan slot must be drained (no unacked .sfa left)",
                                awaitNoSegmentFile(slot("legacy"), 10_000));
                    } finally {
                        s.close();
                    }
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

    private static boolean awaitNoSegmentFile(String slotPath, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (!hasSegmentFile(slotPath)) {
                return true;
            }
            Thread.sleep(10);
        }
        return !hasSegmentFile(slotPath);
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

    private static Sender getDelegate(PooledSender ps) throws Exception {
        Field f = PooledSender.class.getDeclaredField("delegate");
        f.setAccessible(true);
        return (Sender) f.get(ps);
    }

    private static void setBooleanField(Object target, String name, boolean value) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        f.setBoolean(target, value);
    }

    private static int getIntField(Object target, String name) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(target);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field f = target.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static void invokeDiscardBroken(SenderPool pool, PooledSender ps) throws Exception {
        Method m = SenderPool.class.getDeclaredMethod("discardBroken", PooledSender.class);
        m.setAccessible(true);
        m.invoke(pool, ps);
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
