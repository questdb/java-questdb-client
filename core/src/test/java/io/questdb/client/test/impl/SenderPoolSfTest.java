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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLockContentionException;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

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
                        // borrow() allocates a fresh PooledSender wrapper on every
                        // call, so comparing the wrappers is vacuously true.
                        // Distinctness of the two borrowed senders lives in the
                        // underlying slots (mirrors SenderPoolTest.slotOf usage).
                        Assert.assertFalse("two borrows must hold distinct slots",
                                a.hasSameSlotForTesting(b));
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
                        // borrow() now returns a fresh wrapper each time; the
                        // recycled thing is the underlying slot.
                        Assert.assertTrue("returned slot must be recycled",
                                first.hasSameSlotForTesting(second));
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
        // After the pool closes AND every lease has come home, every slot
        // flock must be released so the dirs can be re-acquired -- by a fresh
        // pool or a standalone sender.
        //
        // Note the ordering contract (C1): pool.close() itself never tears
        // down a BORROWED delegate -- a producer thread could be inside it,
        // and freeing its native buffers mid-append would be a
        // use-after-free/SEGV. A lease still outstanding when close() gives
        // up keeps its flock until the lease is closed, at which point the
        // returning thread tears the delegate down (retireLease) and releases
        // the flock.
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Short acquire timeout: it doubles as close()'s bounded
                // lease-wait budget, which this test intentionally exhausts.
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                SenderPool pool = new SenderPool(config, 2, 2, 300, Long.MAX_VALUE, Long.MAX_VALUE);
                PooledSender a = pool.borrow();
                PooledSender b = pool.borrow();
                // Close with both leases outstanding: close() waits out its
                // budget, then returns WITHOUT touching the borrowed
                // delegates (their flocks stay held -- leak over SEGV).
                pool.close();
                // The leases come home after close: each returning thread
                // tears down its own delegate, releasing the slot flock.
                a.close();
                b.close();

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
                    ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(false);

                    // Route the wrapper through the pool's broken-eviction path.
                    pool.discardBrokenForTesting(a);

                    // The leaked index must NOT be returned to the free set,
                    // and capacity must be accounted as permanently consumed.
                    Assert.assertEquals("one slot must be retired as leaked",
                            1, pool.leakedSlotCount());
                    Assert.assertTrue("leaked slot index 0 must stay reserved",
                            pool.isSlotInUseForTesting(0));

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
                        ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(false);
                        pool.discardBrokenForTesting(a);

                        // Sanity: the slot really was retired as leaked.
                        Assert.assertEquals("precondition: one slot must leak",
                                1, pool.leakedSlotCount());
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

    @Test
    public void testSlotLeakedWhenDelegateCloseDoesNotReleaseFlockDuringReap() throws Exception {
        // Coverage twin of testSlotLeakedWhenDelegateCloseDoesNotReleaseFlock,
        // but the close-and-reclaim is driven through reapIdle()'s leaked
        // branch -- reclaimSlot(s, " during idle reaping") -- rather than
        // discardBroken(). reapIdle is the only one of the three
        // close-and-reclaim paths whose flockReleased()==false branch had no
        // test: the existing reap tests use live QWP delegates (flock IS
        // released => free path), and testReapIdleSurvivesDelegateCloseError
        // is HTTP (storeAndForward off => reclaimSlot never runs). Pin it: an
        // idle delegate whose close() leaves the flock held must retire the
        // slot permanently (leakedSlots++, slotInUse stays set), never hand
        // the still-locked dir to a later borrow.
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                // minSize=0 so reapIdle is free to evict; idleTimeout=1ms so a
                // returned slot is immediately reap-eligible.
                try (SenderPool pool = new SenderPool(config, 0, 2, 500, 1, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    Assert.assertTrue(Files.exists(slot("default-0")));

                    // Return it to the idle set with a LIVE delegate, then
                    // forge the exact leak symptom: tear the delegate down for
                    // real (so no native resources leak) and clear
                    // slotLockReleased. close() is idempotent, so reapIdle's
                    // re-close is a no-op that leaves the flock "still held".
                    pool.giveBack(a);
                    Sender delegate = getDelegate(a);
                    delegate.close();
                    ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(false);

                    // Drive the sweep: the idle timeout has elapsed.
                    Thread.sleep(10);
                    pool.reapIdle();

                    // The reap leaked branch must have fired.
                    Assert.assertEquals("reapIdle must retire the still-locked slot as leaked",
                            1, pool.leakedSlotCount());
                    Assert.assertTrue("leaked slot index 0 must stay reserved",
                            pool.isSlotInUseForTesting(0));
                    Assert.assertEquals("leaked slot must be observable via leakedSlotCount()",
                            1, pool.leakedSlotCount());

                    // The next borrow must take a fresh index -- never reuse
                    // the still-locked default-0 dir.
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
    public void testRetiredSlotRecoveredByHousekeeperAfterLateFlockRelease() throws Exception {
        // Recovery twin of testSlotLeakedWhenDelegateCloseDoesNotReleaseFlock:
        // a slot retired because close() returned with the flock still held is
        // NOT lost until process exit. Engine cleanup may complete later on a
        // worker/I/O-thread exit path (isSlotLockReleased() re-probes the
        // retained engine), and the housekeeper's reapIdle() tick must then
        // return the index to the free set: leakedSlots back down, slotInUse
        // cleared, full capacity restored.
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

                    // Forge the retire: real teardown first (no native leaks),
                    // then clear slotLockReleased so discardBroken retires the
                    // slot as leaked.
                    Sender delegate = getDelegate(a);
                    delegate.close();
                    ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(false);
                    pool.discardBrokenForTesting(a);
                    Assert.assertEquals("precondition: one slot must be retired",
                            1, pool.leakedSlotCount());

                    // Forge the late release: the deferred cleanup finished and
                    // the delegate now reports the flock dropped (in production
                    // this flip comes from isSlotLockReleased() re-probing the
                    // retained engine after the manager worker exited).
                    ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(true);

                    // The housekeeper tick is the recovery driver.
                    pool.reapIdle();

                    Assert.assertEquals("recovered slot must leave the leaked count",
                            0, pool.leakedSlotCount());
                    Assert.assertFalse("recovered slot index 0 must return to the free set",
                            pool.isSlotInUseForTesting(0));

                    // Full capacity restored: with maxSize=2, two concurrent
                    // borrows must succeed again (index 0 is reusable — its
                    // flock is genuinely free).
                    PooledSender b = pool.borrow();
                    PooledSender c = pool.borrow();
                    try {
                        Assert.assertEquals(2, countSlotDirs());
                    } finally {
                        c.close();
                        b.close();
                    }
                }
            }
        });
    }

    @Test
    public void testCapacityStarvedBorrowRecoversRetiredSlot() throws Exception {
        // Borrow-path twin of the housekeeper recovery test: a borrow that
        // would otherwise park on the cap check must re-probe retired slots
        // before waiting, so a late flock release converts a guaranteed
        // borrow timeout into an immediate creation on the recovered index —
        // no housekeeper tick required.
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

                    Sender delegate = getDelegate(a);
                    delegate.close();
                    ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(false);
                    pool.discardBrokenForTesting(a);
                    Assert.assertEquals("precondition: one slot must be retired",
                            1, pool.leakedSlotCount());

                    // One live borrow + one retired slot = cap reached.
                    PooledSender b = pool.borrow();
                    Assert.assertTrue(Files.exists(slot("default-1")));
                    try {
                        // Late release lands while the pool is capacity-starved.
                        ((QwpWebSocketSender) delegate).setSlotLockReleasedForTesting(true);

                        // The next borrow hits the cap check, re-probes, frees
                        // index 0, and must create on it instead of timing out.
                        PooledSender c = pool.borrow();
                        try {
                            Assert.assertEquals("borrow must recover the retired slot's capacity",
                                    0, pool.leakedSlotCount());
                            Assert.assertEquals(2, countSlotDirs());
                        } finally {
                            c.close();
                        }
                    } finally {
                        b.close();
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testDeferredFlockReleaseWakesParkedLongTimeoutBorrower() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(
                        config, 1, 1, 60_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender lease = pool.borrow();
                    Sender delegate = getDelegate(lease);
                    CursorSendEngine engine = ((QwpWebSocketSender) delegate).getCursorEngineForTesting();
                    SlotLock slotLock = engine.getSlotLockForTesting();
                    CountDownLatch borrowerAcquired = new CountDownLatch(1);
                    CountDownLatch borrowerParked = new CountDownLatch(1);
                    CountDownLatch releaseBorrower = new CountDownLatch(1);
                    AtomicReference<Throwable> borrowerFailure = new AtomicReference<>();
                    AtomicReference<PooledSender> recovered = new AtomicReference<>();
                    Thread borrower = new Thread(() -> {
                        try {
                            recovered.set(pool.borrow());
                            borrowerAcquired.countDown();
                            if (!releaseBorrower.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("timed out waiting to release borrower");
                            }
                        } catch (Throwable t) {
                            borrowerFailure.compareAndSet(null, t);
                        } finally {
                            PooledSender sender = recovered.get();
                            if (sender != null) {
                                sender.close();
                            }
                        }
                    }, "sender-pool-deferred-release-waiter");

                    SlotLock.ReleaseFailureForTesting releaseFailure =
                            slotLock.injectReleaseFailureForTesting();
                    try {
                        pool.discardBrokenForTesting(lease);
                        Assert.assertEquals("failed release must retire the only slot",
                                1, pool.leakedSlotCount());
                        Assert.assertFalse(engine.isCloseCompleted());

                        pool.setBeforeBorrowWaitHook(borrowerParked::countDown);
                        borrower.start();
                        Assert.assertTrue("borrower must reach the condition wait with a long timeout",
                                borrowerParked.await(5, TimeUnit.SECONDS));
                        // The hook runs under the pool lock immediately before awaitNanos.
                        // Acquiring that lock here proves awaitNanos atomically enqueued the
                        // borrower and released the lock before restoration can start. This
                        // read-only operation neither changes pool state nor signals a waiter.
                        pool.availableSize();

                        // This restored fd is the only source of progress: the retry driver
                        // confirms the release. There is no housekeeper or pool mutation.
                        releaseFailure.close();
                        Assert.assertTrue("deferred flock release must wake the parked borrower",
                                borrowerAcquired.await(5, TimeUnit.SECONDS));
                        Assert.assertNull("borrower must not fail", borrowerFailure.get());
                        Assert.assertEquals("release wakeup must recover retired capacity",
                                0, pool.leakedSlotCount());
                    } finally {
                        pool.setBeforeBorrowWaitHook(null);
                        releaseBorrower.countDown();
                        if (!engine.isCloseCompleted()) {
                            releaseFailure.close();
                        }
                        borrower.join(TimeUnit.SECONDS.toMillis(1));
                        if (borrower.isAlive()) {
                            borrower.interrupt();
                            borrower.join(TimeUnit.SECONDS.toMillis(5));
                        }
                        Assert.assertFalse("borrower thread must finish", borrower.isAlive());
                    }
                    if (borrowerFailure.get() != null) {
                        throw new AssertionError("borrower failed", borrowerFailure.get());
                    }
                }
            }
        });
    }

    @Test
    public void testDirectRetiredSlotCallbacksHaveLinearProbeCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                final int slotCount = 32;
                String config = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(
                        config, 0, slotCount, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] leases = new PooledSender[slotCount];
                    Sender[] delegates = new Sender[slotCount];
                    Runnable[] callbacks = new Runnable[slotCount];
                    for (int i = 0; i < slotCount; i++) {
                        leases[i] = pool.borrow();
                        delegates[i] = getDelegate(leases[i]);
                        callbacks[i] = ((QwpWebSocketSender) delegates[i]).getSlotLockReleaseListenerForTesting();
                        Assert.assertNotNull(callbacks[i]);
                    }
                    for (int i = 0; i < slotCount; i++) {
                        delegates[i].close();
                        ((QwpWebSocketSender) delegates[i]).setSlotLockReleasedForTesting(false);
                        pool.discardBrokenForTesting(leases[i]);
                    }
                    Assert.assertEquals(slotCount, pool.leakedSlotCount());
                    pool.setRetiredSlotProbeCountForTesting(0);

                    int[] geometricCheckpoints = {4, 8, 16, 32};
                    int checkpoint = 0;
                    for (int i = 0; i < slotCount; i++) {
                        ((QwpWebSocketSender) delegates[i]).setSlotLockReleasedForTesting(true);
                        callbacks[i].run();
                        if (i + 1 == geometricCheckpoints[checkpoint]) {
                            Assert.assertEquals("direct release probes must grow linearly",
                                    i + 1, pool.getRetiredSlotProbeCountForTesting());
                            checkpoint++;
                        }
                    }
                    Assert.assertEquals(0, pool.leakedSlotCount());
                    Assert.assertTrue(pool.getRetiredSlotCountForTesting() == 0);
                }
            }
        });
    }

    @Test
    public void testDirectRetiredSlotCallbackFallbackAndIdempotence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 0, 3, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] leases = new PooledSender[3];
                    Sender[] delegates = new Sender[3];
                    Runnable[] callbacks = new Runnable[3];
                    for (int i = 0; i < 3; i++) {
                        leases[i] = pool.borrow();
                        delegates[i] = getDelegate(leases[i]);
                        callbacks[i] = ((QwpWebSocketSender) delegates[i]).getSlotLockReleaseListenerForTesting();
                        delegates[i].close();
                        ((QwpWebSocketSender) delegates[i]).setSlotLockReleasedForTesting(false);
                        pool.discardBrokenForTesting(leases[i]);
                    }
                    Assert.assertEquals(3, pool.leakedSlotCount());
                    pool.setRetiredSlotProbeCountForTesting(0);

                    // Simulate callback registration becoming unavailable. The
                    // periodic housekeeper scan must remain a complete fallback.
                    ((QwpWebSocketSender) delegates[0]).setSlotLockReleaseListener(null);
                    ((QwpWebSocketSender) delegates[0]).setSlotLockReleasedForTesting(true);
                    pool.reapIdle();
                    Assert.assertEquals(2, pool.leakedSlotCount());

                    // A premature callback must not remove an unreleased slot.
                    callbacks[1].run();
                    Assert.assertEquals(2, pool.leakedSlotCount());
                    ((QwpWebSocketSender) delegates[1]).setSlotLockReleasedForTesting(true);
                    callbacks[1].run();
                    Assert.assertEquals(1, pool.leakedSlotCount());

                    // Duplicate and stale callbacks are idempotent and do not
                    // probe or mutate the slot after its direct removal.
                    ((QwpWebSocketSender) delegates[2]).setSlotLockReleasedForTesting(true);
                    callbacks[2].run();
                    long probesAfterRecovery = pool.getRetiredSlotProbeCountForTesting();
                    callbacks[2].run();
                    callbacks[1].run();
                    Assert.assertEquals(probesAfterRecovery,
                            pool.getRetiredSlotProbeCountForTesting());
                    Assert.assertEquals(0, pool.leakedSlotCount());
                    Assert.assertTrue(pool.getRetiredSlotCountForTesting() == 0);
                }
            }
        });
    }

    @Test
    public void testMixedRetiredSlotsRecoverWithoutLosingAccounting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(
                        config, 0, 8, 500, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] leases = new PooledSender[8];
                    Sender[] delegates = new Sender[8];
                    for (int i = 0; i < leases.length; i++) {
                        leases[i] = pool.borrow();
                        delegates[i] = getDelegate(leases[i]);
                    }
                    for (int i = 0; i < leases.length; i++) {
                        // Release the real native resources, then forge the
                        // delayed publication which makes the pool retire the
                        // slot. The idempotent second close in discardBroken()
                        // leaves the forged state unchanged.
                        delegates[i].close();
                        ((QwpWebSocketSender) delegates[i]).setSlotLockReleasedForTesting(false);
                        pool.discardBrokenForTesting(leases[i]);
                    }
                    Assert.assertEquals(8, pool.leakedSlotCount());

                    // Mix completed and incomplete entries throughout the
                    // retired list. A recovery pass must remove exactly these
                    // four without skipping a swapped entry or corrupting the
                    // bitmap/count relationship.
                    int[] released = {0, 2, 5, 7};
                    for (int i = 0; i < released.length; i++) {
                        ((QwpWebSocketSender) delegates[released[i]]).setSlotLockReleasedForTesting(true);
                    }
                    pool.reapIdle();

                    Assert.assertEquals(4, pool.leakedSlotCount());
                    Assert.assertEquals(4, pool.getRetiredSlotCountForTesting());
                    for (int i = 0; i < delegates.length; i++) {
                        boolean mustRemainRetired = i == 1 || i == 3 || i == 4 || i == 6;
                        Assert.assertEquals("slot reservation mismatch at index " + i,
                                mustRemainRetired, pool.isSlotInUseForTesting(i));
                    }

                    // Exactly the four restored reservations are reusable.
                    PooledSender[] recovered = new PooledSender[4];
                    try {
                        for (int i = 0; i < recovered.length; i++) {
                            recovered[i] = pool.borrow();
                        }
                        Assert.assertEquals(4, pool.totalSize());
                        Assert.assertEquals(4, pool.leakedSlotCount());
                    } finally {
                        for (int i = 0; i < recovered.length; i++) {
                            if (recovered[i] != null) {
                                recovered[i].close();
                            }
                        }
                    }
                }
            }
        });
    }

    @Test(timeout = 30_000)
    public void testMultipleWaitingBorrowersWakeForStaggeredRetiredSlotRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(
                        config, 0, 6, 10_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] leases = new PooledSender[6];
                    Sender[] delegates = new Sender[6];
                    for (int i = 0; i < leases.length; i++) {
                        leases[i] = pool.borrow();
                        delegates[i] = getDelegate(leases[i]);
                    }
                    for (int i = 0; i < leases.length; i++) {
                        delegates[i].close();
                        ((QwpWebSocketSender) delegates[i]).setSlotLockReleasedForTesting(false);
                        pool.discardBrokenForTesting(leases[i]);
                    }
                    Assert.assertEquals("all capacity must start retired",
                            6, pool.leakedSlotCount());

                    final int borrowerCount = 3;
                    CountDownLatch allAcquired = new CountDownLatch(borrowerCount);
                    CountDownLatch allDone = new CountDownLatch(borrowerCount);
                    CountDownLatch firstAcquired = new CountDownLatch(1);
                    CountDownLatch initialWaiters = new CountDownLatch(borrowerCount);
                    CountDownLatch releaseBorrowers = new CountDownLatch(1);
                    CountDownLatch reparkedWaiters = new CountDownLatch(borrowerCount - 1);
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    ConcurrentHashMap<Thread, Boolean> initialWaiterThreads = new ConcurrentHashMap<>();
                    ConcurrentHashMap<Thread, Boolean> reparkedWaiterThreads = new ConcurrentHashMap<>();
                    PooledSender[] recovered = new PooledSender[borrowerCount];
                    pool.setBeforeBorrowWaitHook(() -> {
                        if (initialWaiterThreads.putIfAbsent(Thread.currentThread(), Boolean.TRUE) == null) {
                            initialWaiters.countDown();
                        }
                    });
                    for (int i = 0; i < borrowerCount; i++) {
                        final int borrower = i;
                        Thread thread = new Thread(() -> {
                            try {
                                recovered[borrower] = pool.borrow();
                                allAcquired.countDown();
                                firstAcquired.countDown();
                                if (!releaseBorrowers.await(10, TimeUnit.SECONDS)) {
                                    throw new AssertionError("timed out waiting to release recovered borrower");
                                }
                            } catch (Throwable e) {
                                failure.compareAndSet(null, e);
                            } finally {
                                try {
                                    if (recovered[borrower] != null) {
                                        recovered[borrower].close();
                                    }
                                } catch (Throwable e) {
                                    failure.compareAndSet(null, e);
                                } finally {
                                    allDone.countDown();
                                }
                            }
                        }, "sender-pool-retired-waiter-" + i);
                        thread.start();
                    }

                    try {
                        Assert.assertTrue("all borrowers must reach the condition wait",
                                initialWaiters.await(5, TimeUnit.SECONDS));
                        Assert.assertEquals("three distinct borrowers must reach the wait path",
                                borrowerCount, initialWaiterThreads.size());

                        // The hook runs while each borrower holds the pool lock,
                        // immediately before awaitNanos atomically releases that
                        // lock and enqueues it. Once the last latch count lands,
                        // the first reapIdle() cannot acquire the lock until all
                        // three borrowers are definitely condition waiters.
                        pool.setBeforeBorrowWaitHook(() -> {
                            if (reparkedWaiterThreads.putIfAbsent(Thread.currentThread(), Boolean.TRUE) == null) {
                                reparkedWaiters.countDown();
                            }
                        });
                        ((QwpWebSocketSender) delegates[2]).setSlotLockReleasedForTesting(true);
                        pool.reapIdle();

                        // One restored index admits exactly one borrower. The
                        // other two must consume signalAll(), lose the capacity
                        // race, and deterministically re-enter the wait path.
                        Assert.assertTrue("one borrower must take the first restored index",
                                firstAcquired.await(5, TimeUnit.SECONDS));
                        Assert.assertTrue("two distinct borrowers must re-park after the first recovery",
                                reparkedWaiters.await(5, TimeUnit.SECONDS));
                        Assert.assertEquals("exactly two distinct borrowers must re-enter the wait path",
                                borrowerCount - 1, reparkedWaiterThreads.size());
                        Assert.assertEquals("exactly one borrower should hold restored capacity",
                                borrowerCount - 1, allAcquired.getCount());

                        // Recover two non-contiguous entries in one reverse /
                        // swap-remove pass. A single signal() here would wake
                        // only one of the two proven waiters; signalAll() must
                        // wake both and let them claim the two restored indices.
                        pool.setBeforeBorrowWaitHook(null);
                        ((QwpWebSocketSender) delegates[0]).setSlotLockReleasedForTesting(true);
                        ((QwpWebSocketSender) delegates[5]).setSlotLockReleasedForTesting(true);
                        pool.reapIdle();
                        Assert.assertTrue("all waiting borrowers must receive restored capacity",
                                allAcquired.await(5, TimeUnit.SECONDS));
                        Assert.assertEquals(3, pool.totalSize());
                        Assert.assertEquals(3, pool.leakedSlotCount());

                        boolean[] seen = new boolean[6];
                        for (int i = 0; i < recovered.length; i++) {
                            int slotIndex = recovered[i].getSlotIndexForTesting();
                            Assert.assertFalse("borrowers must receive distinct restored indices",
                                    seen[slotIndex]);
                            seen[slotIndex] = true;
                        }
                        Assert.assertTrue("slot 0 must be restored", seen[0]);
                        Assert.assertTrue("slot 2 must be restored", seen[2]);
                        Assert.assertTrue("slot 5 must be restored", seen[5]);
                        if (failure.get() != null) {
                            throw new AssertionError("waiting borrower failed", failure.get());
                        }
                    } finally {
                        pool.setBeforeBorrowWaitHook(null);
                        releaseBorrowers.countDown();
                        Assert.assertTrue("borrower threads must finish",
                                allDone.await(10, TimeUnit.SECONDS));
                    }
                    if (failure.get() != null) {
                        throw new AssertionError("waiting borrower failed", failure.get());
                    }
                }
            }
        });
    }

    @Test
    public void testPoolRetiresAndRecoversSlotThroughFailedFlockReleaseRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 1, 1, 2_000,
                        Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    Sender delegate = getDelegate(a);
                    CursorSendEngine engine = ((QwpWebSocketSender) delegate).getCursorEngineForTesting();
                    SlotLock slotLock = engine.getSlotLockForTesting();
                    SlotLock.ReleaseFailureForTesting releaseFailure =
                            slotLock.injectReleaseFailureForTesting();
                    try {
                        // Inject one persistent explicit-unlock failure.
                        // Delegate close must retire the only pool slot rather
                        // than publish a release while the real flock remains held.
                        pool.discardBrokenForTesting(a);
                        Assert.assertEquals("failed release must retire pool capacity",
                                1, pool.leakedSlotCount());
                        Assert.assertFalse(engine.isCloseCompleted());

                        // Remove the fault without calling close again: the
                        // engine's error-path retry driver runs outside the
                        // pool lock and must eventually publish completion.
                        releaseFailure.close();
                        long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (!engine.isCloseCompleted()) {
                            if (System.nanoTime() > deadlineNs) {
                                throw new AssertionError("flock-release retry never completed");
                            }
                            Thread.sleep(1L);
                        }

                        // A capacity-starved public borrow re-probes the
                        // retained delegate, frees index 0, and proves the
                        // recovered flock is genuinely acquirable.
                        PooledSender recovered = pool.borrow();
                        try {
                            Assert.assertEquals("retry must restore pool capacity",
                                    0, pool.leakedSlotCount());
                            Assert.assertEquals(1, countSlotDirs());
                        } finally {
                            recovered.close();
                        }
                    } finally {
                        if (!engine.isCloseCompleted()) {
                            releaseFailure.close();
                            Assert.assertTrue("restored fd must release cleanly",
                                    slotLock.release());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPoolRetiresAndRecoversSlotThroughRealManagerWorkerWedge() throws Exception {
        // Full-stack twin of the two forged-flag recovery tests above: no
        // reflection-forged slotLockReleased anywhere. The REAL mechanism is
        // driven end to end — the delegate's owned SegmentManager worker is
        // wedged mid service pass (test hook), the delegate's close() takes
        // the real timed-out-join → worker-exit handoff path, the pool
        // retires the slot off the delegate's genuine isSlotLockReleased()
        // report, the worker's deferred cleanup releases the real flock, and
        // the housekeeper re-probe restores capacity — proving the recovered
        // index is genuinely reusable by borrowing on it again (a forged flag
        // would pass the accounting asserts but collide on the flock here).
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

                    Sender delegate = getDelegate(a);
                    CursorSendEngine engine = ((QwpWebSocketSender) delegate).getCursorEngineForTesting();
                    Assert.assertNotNull("SF delegate must own a cursor engine", engine);
                    SegmentManager manager = engine.getManagerForTesting();

                    CountDownLatch workerBlocked = new CountDownLatch(1);
                    CountDownLatch releaseWorker = new CountDownLatch(1);
                    AtomicBoolean fired = new AtomicBoolean();
                    AtomicReference<Throwable> hookErr = new AtomicReference<>();
                    try {
                        // Park the manager worker inside a service pass for
                        // the delegate's ring. The trim-sync point is reached
                        // on every ~1ms tick, so this wedges deterministically.
                        manager.setBeforeTrimSyncHook(() -> {
                            if (!fired.compareAndSet(false, true)) return;
                            workerBlocked.countDown();
                            try {
                                if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                                    hookErr.compareAndSet(null, new AssertionError(
                                            "timed out waiting for test to release worker"));
                                }
                            } catch (Throwable t) {
                                hookErr.compareAndSet(null, t);
                            }
                        });
                        Assert.assertTrue("manager worker never entered a service pass",
                                workerBlocked.await(5, TimeUnit.SECONDS));

                        // Real teardown against the wedged worker: the
                        // delegate's engine close times out its bounded join,
                        // hands cleanup to the worker's exit path, and reports
                        // the retained flock; the pool must retire the slot.
                        // Fault the old callback-allocation/registration path.
                        // C3 makes the owned-engine handoff preallocated, so
                        // this hook must no longer be reached by production
                        // teardown.
                        manager.setBeforeExitCleanupRegistrationHook(() -> {
                            throw new OutOfMemoryError("simulated callback allocation failure");
                        });
                        manager.setWorkerJoinTimeoutMillis(50L);
                        pool.discardBrokenForTesting(a);
                        Assert.assertEquals(
                                "pool must retire the slot while the delegate's manager "
                                        + "worker holds the deferred cleanup",
                                1, pool.leakedSlotCount());
                        Assert.assertFalse("engine cleanup must still be pending",
                                engine.isCloseCompleted());

                        // Un-wedge and deterministically reap the manager.
                        // No sender/engine close retry is allowed: worker exit
                        // itself must own and finish the preallocated handoff.
                        releaseWorker.countDown();
                        manager.close();
                        Assert.assertTrue("manager worker must be reaped", manager.isWorkerReaped());
                        Assert.assertTrue("worker exit must run deferred cleanup despite the "
                                        + "old allocation fault injection",
                                engine.isCloseCompleted());

                        // The housekeeper tick is the recovery driver.
                        pool.reapIdle();
                        Assert.assertEquals("recovered slot must leave the leaked count",
                                0, pool.leakedSlotCount());
                        Assert.assertFalse("recovered slot index 0 must return to the free set",
                                pool.isSlotInUseForTesting(0));

                        // The proof a forged flag cannot fake: both indices —
                        // including the recovered one, whose flock was really
                        // dropped by the worker-exit cleanup — admit live
                        // senders again.
                        PooledSender b = pool.borrow();
                        PooledSender c = pool.borrow();
                        try {
                            Assert.assertEquals(2, countSlotDirs());
                        } finally {
                            c.close();
                            b.close();
                        }
                        if (hookErr.get() != null) {
                            throw new AssertionError("trim hook failed", hookErr.get());
                        }
                    } finally {
                        manager.setBeforeExitCleanupRegistrationHook(null);
                        manager.setBeforeTrimSyncHook(null);
                        releaseWorker.countDown();
                    }
                }
            }
        });
    }

    @Test
    public void testPreallocatedExitHandoffCleansInRangeStartupRecoverer() throws Exception {
        assertPreallocatedExitHandoffCleansStartupRecoverer(0, 1);
    }

    @Test
    public void testPreallocatedExitHandoffCleansOutOfRangeStartupRecoverer() throws Exception {
        assertPreallocatedExitHandoffCleansStartupRecoverer(1, 1);
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

    @Test
    public void testRecoveryDelegateForcesOffInitialConnectMode() throws Exception {
        // M1 regression: a startup-recovery delegate runs on the PoolHousekeeper
        // thread, so its build() must NOT inherit the user's SYNC initial-connect
        // mode (auto-enabled by any reconnect_* knob). SYNC would retry the
        // connect for the whole reconnect budget inside build() -- far past
        // PoolHousekeeper.STOP_TIMEOUT_MILLIS -- so a close() landing during that
        // build would make the housekeeper join time out and leave the recoverer
        // holding the slot flock after close() returned. The recovery factory
        // forces initial_connect_mode=OFF (at most one connect attempt); the
        // normal factory must still honour the user's promoted SYNC mode.
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                // reconnect_max_duration_millis set, initial_connect_mode unset
                // -> the builder promotes to SYNC for ordinary senders.
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir
                        + ";reconnect_max_duration_millis=30000;";
                // min=0 (no prewarm connect), no stranded data (recovery no-op).
                try (SenderPool pool = new SenderPool(cfg, 0, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    // Normal managed-slot delegate: inherits the promoted SYNC.
                    Sender normal = pool.buildSenderForTesting(0);
                    try {
                        Assert.assertEquals(
                                "ordinary pooled sender must honour the user's promoted SYNC mode",
                                Sender.InitialConnectMode.SYNC, ((QwpWebSocketSender) normal).getInitialConnectModeForTesting());
                    } finally {
                        normal.close();
                    }
                    // Recovery delegate on a different slot: forced OFF.
                    Sender recoverer = pool.buildRecoverySenderForTesting(1);
                    try {
                        Assert.assertEquals(
                                "recovery delegate must force OFF so build() makes at most one connect attempt",
                                Sender.InitialConnectMode.OFF, ((QwpWebSocketSender) recoverer).getInitialConnectModeForTesting());
                    } finally {
                        recoverer.close();
                    }
                }
            }
        });
    }

    @Test
    public void testSharedManagerPassCompletionRecoversRetiredPoolSlot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                long segSize = io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment.HEADER_SIZE
                        + io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment.FRAME_HEADER_SIZE + 32L;
                SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
                CountDownLatch cleanupFinished = new CountDownLatch(1);
                CountDownLatch workerBlocked = new CountDownLatch(1);
                CountDownLatch releaseWorker = new CountDownLatch(1);
                AtomicBoolean fired = new AtomicBoolean();
                AtomicReference<Throwable> hookErr = new AtomicReference<>();
                AtomicReference<CursorSendEngine> engineRef = new AtomicReference<>();
                manager.setAfterRingCleanupHook(cleanupFinished::countDown);
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) {
                        return;
                    }
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });
                manager.start();
                Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                IntFunction<Sender> factory = slotIndex -> {
                    CursorSendEngine engine = new CursorSendEngine(
                            slot("default-" + slotIndex), segSize, manager);
                    engineRef.set(engine);
                    QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", port);
                    sender.setCursorEngine(engine, true);
                    return sender;
                };
                SenderPool pool = new SenderPool(
                        config, 0, 1, 500, 0, Long.MAX_VALUE, factory);
                try {
                    PooledSender lease = pool.borrow();
                    Assert.assertTrue("shared manager never entered the sender ring's service pass",
                            workerBlocked.await(5, TimeUnit.SECONDS));
                    lease.close();

                    manager.setWorkerJoinTimeoutMillis(50L);
                    pool.reapIdle();
                    Assert.assertEquals("pool must retire the slot while its shared-manager pass is live",
                            1, pool.leakedSlotCount());
                    CursorSendEngine engine = engineRef.get();
                    Assert.assertFalse("engine cleanup must remain pending during the live pass",
                            engine.isCloseCompleted());

                    releaseWorker.countDown();
                    Assert.assertTrue("deferred cleanup did not finish with the ring pass",
                            cleanupFinished.await(5, TimeUnit.SECONDS));
                    if (hookErr.get() != null) {
                        throw new AssertionError("install hook failed", hookErr.get());
                    }

                    pool.reapIdle();
                    Assert.assertEquals("pass completion must drive deferred engine cleanup and restore capacity",
                            0, pool.leakedSlotCount());
                    Assert.assertTrue("deferred cleanup must publish the released flock",
                            engine.isCloseCompleted());
                    try (PooledSender recovered = pool.borrow()) {
                        Assert.assertTrue("recovered slot must be reusable", Files.exists(slot("default-0")));
                    }
                } finally {
                    releaseWorker.countDown();
                    manager.setAfterRingCleanupHook(null);
                    manager.setBeforeInstallSyncHook(null);
                    pool.close();
                    manager.close();
                }
            }
        });
    }

    @Test
    public void testStartupRecoveryRetiresSlotWhenRecovererCloseLeavesFlockHeld() throws Exception {
        // C1 regression: the startup recovery loop MUST mirror discardBroken /
        // reapIdle. When a recoverer's delegate close() returns with the SF
        // flock still held (the I/O thread refused to stop), the recovered slot
        // index must be retired (leakedSlots++, slotInUse stays
        // set) -- NOT freed. Freeing it would let a later borrow re-pick the
        // still-locked dir and resurrect "sf slot already in use", the exact
        // failure class this PR exists to kill. Pre-fix the recovery finally
        // set slotInUse[i]=false unconditionally; this test is RED until it
        // consults flockReleased() like the other two close-and-reclaim paths.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: leave unacked data on disk under default-0 so startup
            // recovery treats it as a candidate orphan and builds a recoverer.
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
            Assert.assertTrue("unacked data must persist under default-0",
                    hasSegmentFile(slot("default-0")));

            // Phase 2: ack-ing server + a new pool whose injected factory forges
            // the exact leak symptom for the recovery build of slot 0. The
            // factory returns a real, flock-holding QwpWebSocketSender but
            // pre-sets closed=true, so the recovery close() is a complete no-op
            // (checkNotClosed short-circuits drain too): the flock stays held
            // and slotLockReleased never flips -- precisely a refused I/O-thread
            // stop. flockReleased(recoverer) must therefore report false.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg2 = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";

                AtomicReference<Sender> forged = new AtomicReference<>();
                IntFunction<Sender> factory = idx -> {
                    Sender real = Sender.builder(cfg2).senderId("default-" + idx).build();
                    if (idx == 0) {
                        try {
                            ((QwpWebSocketSender) real).setClosedForTesting(true);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        forged.set(real);
                    }
                    return real;
                };

                // minSize=0 so prewarm never adopts slot 0 -- recovery is the
                // only builder of slot 0. maxSize=2 so a later borrow can still
                // get a fresh slot (default-1), proving capacity dropped by one.
                SenderPool pool = newPoolWithFactory(cfg2, 0, 2, 500, factory);
                try {
                    // The forge must actually have reached recovery's build.
                    Assert.assertNotNull("recovery must have built slot 0", forged.get());
                    // The retire branch must have fired during construction.
                    Assert.assertEquals("recovery must retire the still-locked slot as leaked",
                            1, pool.leakedSlotCount());
                    Assert.assertTrue("retired slot 0 must stay reserved",
                            pool.isSlotInUseForTesting(0));
                    Assert.assertFalse("slot 1 must remain free",
                            pool.isSlotInUseForTesting(1));

                    // A later borrow must take the fresh slot 1, never re-pick
                    // the still-locked default-0 (which would throw "sf slot
                    // already in use").
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertTrue("borrow must use a fresh slot dir",
                                Files.exists(slot("default-1")));
                        // Capacity stays reduced while the flock is held:
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
                } finally {
                    pool.close();
                    // Release the forged recoverer's real flock + native
                    // resources: pool.close() never saw it (recovery never
                    // added the recoverer to `all`), so un-forge closed and
                    // close it for real, otherwise assertMemoryLeak trips.
                    Sender leaked = forged.get();
                    if (leaked != null) {
                        ((QwpWebSocketSender) leaked).setClosedForTesting(false);
                        leaked.close();
                    }
                }
            }
        });
    }

    @Test
    public void testStartupRetiredSlotRecoveredAfterLateFlockRelease() throws Exception {
        // Recovery twin of
        // testStartupRecoveryRetiresSlotWhenRecovererCloseLeavesFlockHeld: a
        // slot retired by STARTUP recovery (recoverer close() returned with
        // the flock still held) must not stay lost until process exit.
        // isSlotLockReleased() is no longer a one-shot snapshot -- deferred
        // engine cleanup on a worker exit path can release the flock later --
        // so the pool must keep the recoverer in retiredSlots and re-probe it.
        // Pre-fix, startup recovery only ticked leakedSlots and dropped the
        // recoverer: at maxSize=1 every later borrow timed out forever even
        // after the flock dropped. This test is RED until the recoverer is
        // retained.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: strand unacked data under default-0 so startup recovery
            // treats it as a candidate orphan and builds a recoverer.
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
            Assert.assertTrue("unacked data must persist under default-0",
                    hasSegmentFile(slot("default-0")));

            // Phase 2: maxSize=1 -- the worst case, where the single slot's
            // retirement starves the whole pool. Forge the retention exactly
            // like the retire test (closed=true makes the recovery drain and
            // close a no-op, so the real flock stays held and slotLockReleased
            // stays false), but only for the FIRST build of slot 0: the
            // post-recovery borrow below must get a working delegate on the
            // recovered index.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg2 = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";

                AtomicReference<Sender> forged = new AtomicReference<>();
                IntFunction<Sender> factory = idx -> {
                    Sender real = Sender.builder(cfg2).senderId("default-" + idx).build();
                    if (idx == 0 && forged.compareAndSet(null, real)) {
                        try {
                            ((QwpWebSocketSender) real).setClosedForTesting(true);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return real;
                };

                try (SenderPool pool = newPoolWithFactory(cfg2, 0, 1, 500, factory)) {
                    Assert.assertNotNull("recovery must have built slot 0", forged.get());
                    Assert.assertEquals("precondition: startup recovery must retire the slot",
                            1, pool.leakedSlotCount());

                    // While the flock is genuinely held, borrows time out: the
                    // cap-check re-probe finds the flock still reported held.
                    try {
                        pool.borrow();
                        Assert.fail("borrow must time out while the slot is retired");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(), e.getMessage().contains("timed out"));
                    }

                    // The late release: the "wedged worker" finishes and the
                    // flock genuinely drops (un-forge and close the recoverer
                    // for real; in production this flip comes from
                    // isSlotLockReleased() re-probing the retained engine after
                    // the worker exited).
                    Sender recoverer = forged.get();
                    ((QwpWebSocketSender) recoverer).setClosedForTesting(false);
                    recoverer.close();

                    // The capacity-starved borrow must re-probe the startup-
                    // retired slot, recover its capacity, and create on the
                    // freed index -- no housekeeper tick required.
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertEquals("borrow must recover the startup-retired slot's capacity",
                                0, pool.leakedSlotCount());
                        Assert.assertTrue("recovered index 0 must carry the new borrow",
                                pool.isSlotInUseForTesting(0));
                        Assert.assertEquals(1, countSlotDirs());
                    } finally {
                        b.close();
                    }
                }
            }
        });
    }

    @Test
    public void testZeroTimeoutBorrowProbesRetiredSlotBeforeThrowing() throws Exception {
        // Boundary twin of testStartupRetiredSlotRecoveredAfterLateFlockRelease:
        // acquireTimeoutMillis=0 is a valid try-once borrow (builder rejects only
        // < 0). Pre-fix, borrow() ran the terminal timeout check BEFORE
        // reprobeRetiredSlots(), so a zero-budget borrow threw "timed out"
        // without its one probe -- even when the retired slot's flock had
        // already dropped and a probe would have restored capacity and admitted
        // a creation. Deterministic: no housekeeper runs in this test, so
        // borrow() is the only reprobe driver. Recovery is driven manually via
        // the deferred-pool step helper because the inline path reuses
        // acquireTimeoutMillis as its recovery budget -- 0 would skip recovery
        // outright. This test is RED until the probe is hoisted above the
        // timeout check.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: strand unacked data under default-0 so startup recovery
            // treats it as a candidate orphan and builds a recoverer.
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
            Assert.assertTrue("unacked data must persist under default-0",
                    hasSegmentFile(slot("default-0")));

            // Phase 2: maxSize=1, zero acquire budget, deferred recovery driven
            // step-by-step (the housekeeper's per-tick unit, budgeted by
            // RECOVERY_DRAIN_BUDGET_MILLIS, not the zero acquire budget). Forge
            // the retirement (closed=true makes the recovery drain and close a
            // no-op, so the real flock stays held), then release the flock for
            // real BEFORE borrowing: the recovery is discoverable, but only via
            // a probe.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg2 = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";

                AtomicReference<Sender> forged = new AtomicReference<>();
                IntFunction<Sender> factory = idx -> {
                    Sender real = Sender.builder(cfg2).senderId("default-" + idx).build();
                    if (idx == 0 && forged.compareAndSet(null, real)) {
                        try {
                            ((QwpWebSocketSender) real).setClosedForTesting(true);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return real;
                };

                try (SenderPool pool = newDeferredPoolWithFactory(cfg2, 0, 1, 0, factory)) {
                    //noinspection StatementWithEmptyBody
                    while (pool.runStartupRecoveryStepForTesting()) {
                        // drive the whole backlog, one housekeeper-tick unit at a time
                    }
                    Assert.assertNotNull("recovery must have built slot 0", forged.get());
                    Assert.assertEquals("precondition: startup recovery must retire the slot",
                            1, pool.leakedSlotCount());

                    // The late release: un-forge and close the recoverer for
                    // real. The flock genuinely drops, but nothing signals the
                    // pool -- the release happens in the delegate, volatile
                    // writes only.
                    Sender recoverer = forged.get();
                    ((QwpWebSocketSender) recoverer).setClosedForTesting(false);
                    recoverer.close();

                    // Try-once borrow: its single pass must probe, recover the
                    // capacity, and create on the freed index. Pre-fix this
                    // threw "timed out" without ever probing.
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertEquals("zero-timeout borrow must recover the retired slot's capacity",
                                0, pool.leakedSlotCount());
                    } finally {
                        b.close();
                    }
                }
            }
        });
    }

    @Test
    public void testParkedBorrowerGetsFinalProbeAfterBudgetExpiry() throws Exception {
        // Positive-timeout twin of the zero-timeout test. A borrower parks in
        // awaitNanos while the retired slot's flock is genuinely held and
        // sleeps out its full budget. A test hook releases the flock after the
        // wait reports expiry but before the terminal loop pass. Pre-fix that
        // pass hit the timeout check before reprobeRetiredSlots() and threw --
        // missing capacity that had already come back. Post-fix the terminal
        // pass probes first, recovers the index, and admits the creation.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: strand unacked data under default-0.
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
            Assert.assertTrue("unacked data must persist under default-0",
                    hasSegmentFile(slot("default-0")));

            // Phase 2: maxSize=1 and a positive acquire budget. A test hook
            // releases the flock only after awaitNanos() has returned with that
            // budget exhausted, so the terminal wake-up pass is deterministic.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg2 = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";

                AtomicReference<Sender> forged = new AtomicReference<>();
                IntFunction<Sender> factory = idx -> {
                    Sender real = Sender.builder(cfg2).senderId("default-" + idx).build();
                    if (idx == 0 && forged.compareAndSet(null, real)) {
                        try {
                            ((QwpWebSocketSender) real).setClosedForTesting(true);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return real;
                };

                try (SenderPool pool = newPoolWithFactory(cfg2, 0, 1, 100, factory)) {
                    Assert.assertNotNull("recovery must have built slot 0", forged.get());
                    Assert.assertEquals("precondition: startup recovery must retire the slot",
                            1, pool.leakedSlotCount());

                    AtomicBoolean waitExpired = new AtomicBoolean();
                    pool.setBorrowWaitExpiredHook(() -> {
                        Assert.assertTrue("expired-wait hook must run exactly once",
                                waitExpired.compareAndSet(false, true));
                        Sender recoverer = forged.get();
                        try {
                            ((QwpWebSocketSender) recoverer).setClosedForTesting(false);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        // The flock drops only after the positive awaitNanos()
                        // budget is exhausted. This delegate-side release does
                        // not signal slotReleased.
                        recoverer.close();
                    });
                    try {
                        PooledSender b = pool.borrow();
                        try {
                            Assert.assertTrue("borrow must exhaust its positive wait budget",
                                    waitExpired.get());
                            Assert.assertEquals("wake-up probe must recover the retired slot's capacity",
                                    0, pool.leakedSlotCount());
                        } finally {
                            b.close();
                        }
                    } finally {
                        pool.setBorrowWaitExpiredHook(null);
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

    @Test
    public void testFirstBorrowToleratesPreExistingSfDir() throws Exception {
        // Deterministic complement to testConcurrentFirstBorrowsWithMinZeroRaceOnSfDir.
        // That test only RAISES the probability of two threads racing into
        // Files.mkdir(sfDir); on a run where one thread wins the mkdir cleanly
        // before the others reach the exists() check, the benign-race branch
        // in Sender.build() is never hit and the test would pass even on
        // pre-fix code. This test removes the timing dependency: it pre-creates
        // sfDir so EVERY first borrow finds the parent already present and must
        // build successfully without throwing "could not create sf_dir".
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Pre-create the shared parent: now build()'s mkdir guard is
                // skipped on every borrow, deterministically asserting that a
                // pre-existing sf_dir is tolerated rather than fatal.
                Assert.assertEquals("pre-create sf_dir must succeed",
                        0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
                Assert.assertTrue(Files.exists(sfDir));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(config, 0, 4, 10_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertEquals(2, pool.totalSize());
                        Assert.assertTrue(Files.exists(slot("default-0")));
                        Assert.assertTrue(Files.exists(slot("default-1")));
                    } finally {
                        a.close();
                        b.close();
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

    @Test
    public void testShrinkingMaxSizeDrainsStrandedOutOfRangeSlots() throws Exception {
        // The bug: a deployment that previously ran at maxSize=4 leaves unacked
        // data in default-0..3. Restarting at maxSize=2 means default-2 and
        // default-3 are out of the new [0,2) index range forever -- the pool
        // never re-creates them. Before the fix the pool also fenced off the
        // WHOLE "default-" prefix from draining, so default-2/3 were neither
        // re-created nor drained: their store-and-forward data was silently
        // stranded even with drain_orphans=on. After the fix the exclusion is
        // bounded to [0,maxSize), so the out-of-range slots are adopted by a
        // background drainer and recovered.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: seed unacked data into default-0..3 via a maxSize=4 pool
            // against a silent (never-acks) server. close_flush_timeout_millis=0
            // so close() leaves the flushed-but-unacked .sfa on disk.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, 4, 4, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] s = new PooledSender[4];
                    for (int i = 0; i < 4; i++) {
                        s[i] = seed.borrow();
                    }
                    for (int i = 0; i < 4; i++) {
                        s[i].table("recover").longColumn("v", i).atNow();
                        s[i].flush();
                    }
                    for (int i = 3; i >= 0; i--) {
                        s[i].close();
                    }
                }
            }
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue("default-" + i + " must hold unacked data",
                        hasSegmentFile(slot("default-" + i)));
            }

            // Phase 2: restart at maxSize=2 with drain_orphans=on against an ack
            // server. The pool re-creates + self-recovers default-0/1; the
            // out-of-range default-2/3 must be drained, not stranded.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir
                        + ";drain_orphans=on;";
                try (SenderPool pool = new SenderPool(cfg, 1, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = pool.borrow();
                    PooledSender b = pool.borrow();
                    try {
                        // The regression: the out-of-range slots must be adopted
                        // by a background drainer and emptied.
                        Assert.assertTrue("default-2 unacked data must be recovered, not stranded",
                                awaitNoSegmentFile(slot("default-2"), 15_000));
                        Assert.assertTrue("default-3 unacked data must be recovered, not stranded",
                                awaitNoSegmentFile(slot("default-3"), 15_000));
                        Assert.assertFalse("out-of-range slot must not be abandoned as .failed",
                                Files.exists(slot("default-2") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                        Assert.assertFalse("out-of-range slot must not be abandoned as .failed",
                                Files.exists(slot("default-3") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                    } finally {
                        b.close();
                        a.close();
                    }
                }
            }
        });
    }

    @Test
    public void testDefaultConfigRecoversOutOfRangeSlotsAfterShrink() throws Exception {
        // Regression for review claim M1: with the out-of-the-box config
        // (drain_orphans defaults to OFF), shrinking maxSize across a restart
        // must STILL deliver the unacked data left in the now-out-of-range
        // slots. recoverOneSlotStep() pass 2 adopts <base>-i for
        // i >= maxSize at construction, independent of drain_orphans.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: seed unacked data into default-0..3 via a maxSize=4 pool
            // against a silent (never-acks) server.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, 4, 4, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] s = new PooledSender[4];
                    for (int i = 0; i < 4; i++) {
                        s[i] = seed.borrow();
                    }
                    for (int i = 0; i < 4; i++) {
                        s[i].table("recover").longColumn("v", i).atNow();
                        s[i].flush();
                    }
                    for (int i = 3; i >= 0; i--) {
                        s[i].close();
                    }
                }
            }
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue("default-" + i + " must hold unacked data",
                        hasSegmentFile(slot("default-" + i)));
            }

            // Phase 2: restart at maxSize=2 with the DEFAULT config (NO
            // drain_orphans) against a healthy ack server. minSize=0 so startup
            // recovery -- not prewarm "normal use" -- is the recovery path for
            // the in-range slots, and pass 2 is the only path for default-2/3.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(cfg, 0, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    // In-range [0,2): startup recovery pass 1 drains these.
                    Assert.assertTrue("in-range default-0 must be recovered at startup",
                            awaitNoSegmentFile(slot("default-0"), 15_000));
                    Assert.assertTrue("in-range default-1 must be recovered at startup",
                            awaitNoSegmentFile(slot("default-1"), 15_000));

                    // Out-of-range [2,4): pass 2 must deliver these too, under
                    // the default config (no drain_orphans).
                    Assert.assertTrue("default-2 unacked data must be recovered, not stranded",
                            awaitNoSegmentFile(slot("default-2"), 15_000));
                    Assert.assertTrue("default-3 unacked data must be recovered, not stranded",
                            awaitNoSegmentFile(slot("default-3"), 15_000));
                    Assert.assertFalse("out-of-range slot must not be abandoned as .failed",
                            Files.exists(slot("default-2") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                    Assert.assertFalse("out-of-range slot must not be abandoned as .failed",
                            Files.exists(slot("default-3") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

                    // Sanity: the pool is still usable for normal borrows.
                    PooledSender a = pool.borrow();
                    a.close();
                }
            }
        });
    }

    @Test
    public void testFailedOutOfRangeRecoveryRetriesAfterPrimaryReturns() throws Exception {
        // A deferred pool can already have its sole in-range sender borrowed when
        // startup recovery reaches an out-of-range slot left by a larger pool.
        // A transient recoverer build failure must leave that candidate pending:
        // after the primary lease returns, the SAME pool must retry and drain it.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedConfig = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir
                        + ";sender_id=default-1;close_flush_timeout_millis=0;";
                try (Sender seed = Sender.fromConfig(seedConfig)) {
                    seed.table("recover").longColumn("v", 1L).atNow();
                    seed.flush();
                }
            }
            Assert.assertTrue("out-of-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-1")));

            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";";
                AtomicBoolean primaryReturned = new AtomicBoolean();
                AtomicInteger recoveryAttempts = new AtomicInteger();
                IntFunction<Sender> factory = idx -> {
                    if (idx == 1) {
                        recoveryAttempts.incrementAndGet();
                        if (!primaryReturned.get()) {
                            throw new LineSenderException("transient out-of-range recovery failure");
                        }
                    }
                    return Sender.builder(config).senderId("default-" + idx).build();
                };

                try (SenderPool pool = newDeferredPoolWithFactory(config, 0, 1, 5_000, factory)) {
                    PooledSender primary = pool.borrow();

                    Assert.assertFalse("first recovery attempt must stop at the transient failure",
                            pool.runStartupRecoveryStepForTesting());
                    Assert.assertEquals("exactly one out-of-range recovery attempt", 1,
                            recoveryAttempts.get());
                    Assert.assertTrue("failed recovery must preserve the candidate",
                            hasSegmentFile(slot("default-1")));
                    Assert.assertEquals("out-of-range failure must not consume in-range capacity",
                            0, pool.leakedSlotCount());
                    Assert.assertTrue("out-of-range recoverer must not enter retired-slot bookkeeping",
                            pool.getRetiredSlotCountForTesting() == 0);

                    primary.close();
                    primaryReturned.set(true);
                    pool.runStartupRecoveryToCompletionForTesting();

                    Assert.assertEquals("same live pool must retry the out-of-range candidate", 2,
                            recoveryAttempts.get());
                    Assert.assertFalse("retry must drain the preserved out-of-range data",
                            hasSegmentFile(slot("default-1")));
                    Assert.assertTrue("retry must deliver the recovered frame", handler.frames.get() >= 1);
                    Assert.assertEquals("successful out-of-range retry must not consume capacity",
                            0, pool.leakedSlotCount());
                    Assert.assertTrue("out-of-range retry must leave retired slots untouched",
                            pool.getRetiredSlotCountForTesting() == 0);

                    PooledSender next = pool.borrow();
                    try {
                        Assert.assertTrue("normal borrow must reuse the returned primary slot",
                                primary.hasSameSlotForTesting(next));
                    } finally {
                        next.close();
                    }
                }
            }
        });
    }

    @Test
    public void testDrainFailureRetriesInRangeAndOutOfRangeCandidates() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                for (int i = 0; i < 2; i++) {
                    String seedConfig = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir
                            + ";sender_id=default-" + i + ";close_flush_timeout_millis=0;";
                    try (Sender seed = Sender.fromConfig(seedConfig)) {
                        seed.table("recover").longColumn("v", i).atNow();
                        seed.flush();
                    }
                }
            }
            Assert.assertTrue("in-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-0")));
            Assert.assertTrue("out-of-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-1")));

            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";";
                AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
                IntFunction<Sender> factory = idx -> {
                    if (attempts[idx].incrementAndGet() == 1) {
                        return (Sender) Proxy.newProxyInstance(
                                Sender.class.getClassLoader(),
                                new Class[]{Sender.class},
                                (proxy, method, args) -> {
                                    if ("drain".equals(method.getName())) {
                                        throw new LineSenderException("transient drain failure for slot " + idx);
                                    }
                                    if ("close".equals(method.getName())) {
                                        return null;
                                    }
                                    throw new AssertionError("unexpected recovery sender call: " + method.getName());
                                });
                    }
                    return Sender.builder(config).senderId("default-" + idx).build();
                };

                try (SenderPool pool = newDeferredPoolWithFactory(config, 0, 1, 5_000, factory)) {
                    Assert.assertFalse("failed in-range drain must defer the same candidate",
                            pool.runStartupRecoveryStepForTesting());
                    Assert.assertEquals(1, attempts[0].get());
                    Assert.assertEquals(0, attempts[1].get());
                    Assert.assertTrue("failed in-range drain must preserve durable data",
                            hasSegmentFile(slot("default-0")));

                    Assert.assertTrue("successful in-range retry may continue scanning",
                            pool.runStartupRecoveryStepForTesting());
                    Assert.assertEquals("same live pool must retry the in-range candidate",
                            2, attempts[0].get());
                    Assert.assertFalse("in-range retry must drain the preserved data",
                            hasSegmentFile(slot("default-0")));

                    Assert.assertFalse("failed out-of-range drain must defer the same candidate",
                            pool.runStartupRecoveryStepForTesting());
                    Assert.assertEquals(1, attempts[1].get());
                    Assert.assertTrue("failed out-of-range drain must preserve durable data",
                            hasSegmentFile(slot("default-1")));

                    Assert.assertTrue("successful out-of-range retry may finish the candidate",
                            pool.runStartupRecoveryStepForTesting());
                    Assert.assertEquals("same live pool must retry the out-of-range candidate",
                            2, attempts[1].get());
                    Assert.assertFalse("out-of-range retry must drain the preserved data",
                            hasSegmentFile(slot("default-1")));
                    Assert.assertFalse("final scan step must mark recovery complete",
                            pool.runStartupRecoveryStepForTesting());
                    Assert.assertTrue("both recovered frames must be delivered", handler.frames.get() >= 2);
                }
            }
        });
    }

    @Test
    public void testInRangeIdleSlotIsRecoveredAtStartupUnderSteadyLowLoad() throws Exception {
        // The drain exclusion is bounded to [0, maxSize) so a sibling's drainer
        // never adopts a slot dir the pool intends to (re)create -- that is what
        // prevents "sf slot already in use" (see
        // testDrainOrphansPoolDoesNotCannibalizeSiblingSlots). The trade-off was
        // that an in-range slot left holding unacked data by a previous run was
        // recovered ONLY when the pool happened to (re)create that index: the
        // pool pre-warms [0, minSize) and builds [minSize, maxSize) lazily on
        // demand, so under steady low load a high in-range index was never
        // rebuilt -- neither drained (excluded) nor recovered -- and its data
        // was stranded on disk until a restart or load spike.
        //
        // The fix has the pool recover its own stranded managed slots once, at
        // construction, under its own slot reservation (so the cannibalization
        // race the exclusion guards against still cannot happen). This test
        // seeds a busy run, then restarts under steady low load and asserts the
        // idle in-range slots are recovered anyway.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: a busy run at maxSize=4 seeds unacked data into
            // default-0..3 (silent server never acks; close_flush_timeout=0 so
            // close() leaves the flushed-but-unacked .sfa on disk).
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, 4, 4, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] s = new PooledSender[4];
                    for (int i = 0; i < 4; i++) {
                        s[i] = seed.borrow();
                    }
                    for (int i = 0; i < 4; i++) {
                        s[i].table("recover").longColumn("v", i).atNow();
                        s[i].flush();
                    }
                    for (int i = 3; i >= 0; i--) {
                        s[i].close();
                    }
                }
            }
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue("default-" + i + " must hold unacked data",
                        hasSegmentFile(slot("default-" + i)));
            }

            // Phase 2: restart at the SAME maxSize=4 (so default-0..3 stay in
            // range) with steady low load. minSize=0 means prewarm builds
            // nothing and the lowest-free allocator would never reach the high
            // indices under a single in-flight borrow -- yet startup recovery
            // must still empty every in-range slot.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";
                try (SenderPool pool = new SenderPool(cfg, 0, 4, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    // All four in-range slots must be recovered by the startup
                    // pass, even though steady low load never grows the pool to
                    // their indices.
                    for (int i = 0; i < 4; i++) {
                        Assert.assertTrue("in-range idle default-" + i
                                        + " must be recovered at startup, not stranded",
                                awaitNoSegmentFile(slot("default-" + i), 15_000));
                        Assert.assertFalse("recovered slot must not be flagged .failed",
                                Files.exists(slot("default-" + i) + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                    }
                    // The recovered frames must have actually reached the server.
                    Assert.assertTrue("recovered frames must be replayed to the server",
                            awaitAtLeast(handler.frames, 4, 15_000));

                    // Sanity: the pool is still usable for normal borrows.
                    PooledSender a = pool.borrow();
                    a.close();
                }
            }
        });
    }

    @Test
    public void testDirectRecoveryContinuesAfterFiniteBudgetExhaustion() throws Exception {
        createCandidateSlot("default-0");
        createCandidateSlot("default-1");
        CountDownLatch drained = new CountDownLatch(2);
        AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
        IntFunction<Sender> factory = idx -> {
            attempts[idx].incrementAndGet();
            return successfulRecoverySender(idx, drained);
        };
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";

        try (SenderPool pool = newPoolWithFactory(config, 0, 1, 0, factory)) {
            Assert.assertTrue("direct pool must continue recovery after its inline budget expires",
                    drained.await(5, TimeUnit.SECONDS));
            Assert.assertEquals("in-range candidate must be recovered", 1, attempts[0].get());
            Assert.assertEquals("out-of-range candidate must be recovered", 1, attempts[1].get());
        }
    }

    @Test
    public void testDirectRecoveryCloseJoinsDriverAndStopsRemainingCandidates() throws Exception {
        createCandidateSlot("default-0");
        createCandidateSlot("default-1");
        CountDownLatch afterJoin = new CountDownLatch(1);
        CountDownLatch beforeJoin = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        CountDownLatch delegateCloseStarted = new CountDownLatch(1);
        CountDownLatch drainStarted = new CountDownLatch(1);
        CountDownLatch releaseDelegateClose = new CountDownLatch(1);
        CountDownLatch releaseDrain = new CountDownLatch(1);
        AtomicBoolean driverAliveAfterJoin = new AtomicBoolean();
        AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        IntFunction<Sender> senderFactory = idx -> {
            attempts[idx].incrementAndGet();
            return blockingFakeSender(
                    idx, drainStarted, releaseDrain, delegateCloseStarted, releaseDelegateClose);
        };

        SenderPool pool = newPoolWithFactory(
                "ws::addr=localhost:1;sf_dir=" + sfDir + ";",
                0, 2, 0, senderFactory);
        Thread recoveryThread = pool.getStartupRecoveryThreadForTesting();
        pool.setStartupRecoveryJoinHooksForTesting(
                beforeJoin::countDown,
                () -> {
                    driverAliveAfterJoin.set(recoveryThread.isAlive());
                    afterJoin.countDown();
                });
        Thread closeThread = new Thread(() -> {
            try {
                pool.close();
            } catch (Throwable t) {
                closeFailure.set(t);
            } finally {
                closeReturned.countDown();
            }
        }, "test-direct-pool-close");
        try {
            Assert.assertTrue("direct recovery driver must enter the first drain",
                    drainStarted.await(10, TimeUnit.SECONDS));
            Assert.assertTrue("direct recovery driver must be running", recoveryThread.isAlive());

            closeThread.start();
            Assert.assertTrue("close must enter its direct-driver join operation",
                    beforeJoin.await(10, TimeUnit.SECONDS));
            Assert.assertTrue("close itself must raise the shutdown signal before joining",
                    pool.isClosedForTesting());

            releaseDrain.countDown();
            Assert.assertTrue("driver must remain deliberately held before termination",
                    delegateCloseStarted.await(10, TimeUnit.SECONDS));
            Assert.assertEquals("close must not return while its driver is held",
                    1L, closeReturned.getCount());

            releaseDelegateClose.countDown();
            Assert.assertTrue("close must return after the driver is released",
                    closeReturned.await(10, TimeUnit.SECONDS));
            Assert.assertTrue("close must complete its join operation",
                    afterJoin.await(10, TimeUnit.SECONDS));
            if (closeFailure.get() != null) {
                throw new AssertionError("close failed", closeFailure.get());
            }
            Assert.assertFalse("the recovery driver must be dead when close's join returns",
                    driverAliveAfterJoin.get());
            Assert.assertFalse("close must leave the direct recovery driver quiescent",
                    recoveryThread.isAlive());
            Assert.assertEquals("the in-flight candidate must have been built", 1, attempts[0].get());
            Assert.assertEquals("shutdown must prevent a later candidate build", 0, attempts[1].get());
        } finally {
            releaseDrain.countDown();
            releaseDelegateClose.countDown();
            closeThread.join(TimeUnit.SECONDS.toMillis(10));
            pool.close();
        }
    }

    @Test
    public void testDirectRecoveryFailedAttemptEntersRetryWait() throws Throwable {
        createCandidateSlot("default-0");
        CountDownLatch beforeJoin = new CountDownLatch(1);
        CountDownLatch closeReturned = new CountDownLatch(1);
        CountDownLatch recoveryOperationObserved = new CountDownLatch(1);
        CountDownLatch releaseWait = new CountDownLatch(1);
        CountDownLatch waitEntered = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        IntFunction<Sender> senderFactory = idx -> {
            if (attempts.incrementAndGet() > 1) {
                recoveryOperationObserved.countDown();
            }
            throw new LineSenderException("injected recovery failure");
        };
        Runnable recoveryWaiter = () -> {
            waitEntered.countDown();
            recoveryOperationObserved.countDown();
            try {
                releaseWait.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        SenderPool pool = newPoolWithRecoveryControls(
                "ws::addr=localhost:1;sf_dir=" + sfDir + ";",
                0, 1, 0, senderFactory, null, recoveryWaiter, null);
        pool.setStartupRecoveryJoinHooksForTesting(beforeJoin::countDown, null);
        Thread closeThread = new Thread(() -> {
            try {
                pool.close();
            } catch (Throwable t) {
                closeFailure.set(t);
            } finally {
                closeReturned.countDown();
            }
        }, "test-retry-wait-close");
        try {
            Assert.assertTrue("recovery must either wait or retry after the failed attempt",
                    recoveryOperationObserved.await(10, TimeUnit.SECONDS));
            Assert.assertEquals("a failed recovery attempt must enter the retry-wait operation",
                    0L, waitEntered.getCount());
            Assert.assertEquals("the driver must wait immediately after its first failed attempt",
                    1, attempts.get());

            closeThread.start();
            Assert.assertTrue("close must reach the driver join while the waiter is held",
                    beforeJoin.await(10, TimeUnit.SECONDS));
            releaseWait.countDown();
            Assert.assertTrue("close must finish after the retry waiter is released",
                    closeReturned.await(10, TimeUnit.SECONDS));
            if (closeFailure.get() != null) {
                throw new AssertionError("close failed", closeFailure.get());
            }
            Assert.assertEquals("shutdown must prevent another recovery attempt", 1, attempts.get());
        } finally {
            releaseWait.countDown();
            closeThread.join(TimeUnit.SECONDS.toMillis(10));
            pool.close();
        }
    }

    @Test
    public void testDirectRecoveryThreadCreationFailureClosesPrewarmedDelegates() throws Throwable {
        createCandidateSlot("default-2");
        AssertionError failure = new AssertionError("injected recovery thread creation failure");
        AtomicInteger[] closeCalls = {new AtomicInteger(), new AtomicInteger()};
        IntFunction<Sender> senderFactory = idx -> closeCountingSender(idx, closeCalls, idx == 0);
        ThreadFactory threadFactory = runnable -> {
            throw failure;
        };

        try {
            newPoolWithRecoveryThreadFactory(
                    "ws::addr=localhost:1;sf_dir=" + sfDir + ";",
                    2, 2, 0, senderFactory, threadFactory);
            Assert.fail("construction must propagate the recovery thread creation failure");
        } catch (AssertionError actual) {
            Assert.assertSame("construction must preserve throwable identity", failure, actual);
        }
        Assert.assertEquals("cleanup must attempt the first prewarmed delegate", 1, closeCalls[0].get());
        Assert.assertEquals("one close failure must not strand the next delegate", 1, closeCalls[1].get());
    }

    @Test
    public void testDirectRecoveryThreadCreationFailureReleasesPrewarmedSfFlocks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createCandidateSlot("default-2");
            AssertionError failure = new AssertionError("injected recovery thread creation failure");
            ThreadFactory threadFactory = runnable -> {
                throw failure;
            };

            try (TestWebSocketServer ack = new TestWebSocketServer(new CountingAckHandler())) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";";
                try {
                    newPoolWithRecoveryThreadFactory(config, 2, 2, 0, null, threadFactory);
                    Assert.fail("construction must propagate the recovery thread creation failure");
                } catch (AssertionError actual) {
                    Assert.assertSame("construction must preserve throwable identity", failure, actual);
                } catch (Throwable unexpected) {
                    throw new AssertionError("unexpected construction failure", unexpected);
                }

                try (SlotLock ignored0 = SlotLock.acquire(slot("default-0"));
                     SlotLock ignored1 = SlotLock.acquire(slot("default-1"))) {
                    // Reacquisition proves failed construction closed both real
                    // prewarmed delegates and released their SF flocks.
                }
            }
        });
    }

    @Test
    public void testDirectRecoveryThreadStartFailureClosesPrewarmedDelegates() throws Throwable {
        createCandidateSlot("default-2");
        AssertionError failure = new AssertionError("injected recovery thread start failure");
        AtomicBoolean cleanupBeforeDriverQuiescence = new AtomicBoolean();
        AtomicInteger failedJoinCalls = new AtomicInteger();
        AtomicInteger[] closeCalls = {new AtomicInteger(), new AtomicInteger()};
        AtomicReference<Thread> recoveryThread = new AtomicReference<>();
        CountDownLatch releaseDriver = new CountDownLatch(1);
        CountDownLatch running = new CountDownLatch(1);
        IntFunction<Sender> senderFactory = idx -> closeCountingSender(
                idx, closeCalls, false, recoveryThread, cleanupBeforeDriverQuiescence, releaseDriver);
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(() -> {
                running.countDown();
                try {
                    releaseDriver.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                runnable.run();
            }, "test-started-recovery-driver") {
                @Override
                public synchronized void start() {
                    super.start();
                    try {
                        if (!running.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("recovery driver did not start");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("interrupted waiting for recovery driver", e);
                    }
                    throw failure;
                }
            };
            recoveryThread.set(thread);
            return thread;
        };
        Runnable beforeFailedJoin = () -> {
            failedJoinCalls.incrementAndGet();
            releaseDriver.countDown();
        };

        try {
            newPoolWithRecoveryControls(
                    "ws::addr=localhost:1;sf_dir=" + sfDir + ";",
                    2, 2, 0, senderFactory, threadFactory, null, beforeFailedJoin);
            Assert.fail("construction must propagate the recovery thread start failure");
        } catch (AssertionError actual) {
            Assert.assertSame("construction must preserve throwable identity", failure, actual);
        } finally {
            releaseDriver.countDown();
        }
        Assert.assertEquals("constructor cleanup must enter the failed-driver join", 1, failedJoinCalls.get());
        Assert.assertFalse("delegate cleanup must not begin while the failed driver is alive",
                cleanupBeforeDriverQuiescence.get());
        Assert.assertFalse("a possibly-started failed driver must terminate before delegate cleanup",
                recoveryThread.get().isAlive());
        Assert.assertEquals("cleanup must close the first prewarmed delegate", 1, closeCalls[0].get());
        Assert.assertEquals("cleanup must close the second prewarmed delegate", 1, closeCalls[1].get());
    }

    @Test
    public void testDirectRecoveryRetriesTransientFailureAndRemainingSlots() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                for (int i = 0; i < 2; i++) {
                    String seedConfig = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir
                            + ";sender_id=default-" + i + ";close_flush_timeout_millis=0;";
                    try (Sender seed = Sender.fromConfig(seedConfig)) {
                        seed.table("recover").longColumn("v", i).atNow();
                        seed.flush();
                    }
                }
            }
            Assert.assertTrue("in-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-0")));
            Assert.assertTrue("out-of-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-1")));

            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";";
                AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
                CountDownLatch drained = new CountDownLatch(2);
                IntFunction<Sender> factory = idx -> {
                    int attempt = attempts[idx].incrementAndGet();
                    if (idx == 0 && attempt == 1) {
                        throw new LineSenderException("transient direct recovery failure");
                    }
                    Sender delegate = Sender.builder(config).senderId("default-" + idx).build();
                    return notifyingCloseSender(delegate, drained);
                };

                try (SenderPool pool = newPoolWithFactory(config, 0, 1, 5_000, factory)) {
                    Assert.assertTrue("both recovery delegates must drain and close",
                            drained.await(15, TimeUnit.SECONDS));
                    Assert.assertFalse("failed in-range candidate must be retried and delivered",
                            hasSegmentFile(slot("default-0")));
                    Assert.assertFalse("remaining out-of-range candidate must also be delivered",
                            hasSegmentFile(slot("default-1")));
                    Assert.assertEquals("failed in-range candidate must be retried", 2, attempts[0].get());
                    Assert.assertEquals("remaining out-of-range candidate must also be recovered",
                            1, attempts[1].get());
                    Assert.assertTrue("both recovered frames must reach the server",
                            handler.frames.get() >= 2);
                }
            }
        });
    }

    @Test
    public void testDirectRecoveryContendedSlotDoesNotStarveRemainingSlots() throws Exception {
        // C2 regression, end to end: a slot whose createRecoverer PERSISTENTLY
        // throws SlotLockContentionException (its flock is held by another live
        // owner, e.g. a sibling process sharing the slot dir) must not pin the
        // startup-recovery cursor: pre-fix the driver retried that one slot
        // every second forever and the higher-index slot's durable orphan data
        // was never forwarded under idle load. The transient-failure twin
        // (testDirectRecoveryRetriesTransientFailureAndRemainingSlots) covers a
        // failure that CLEARS; this covers one that never does.
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                for (int i = 0; i < 2; i++) {
                    String seedConfig = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir
                            + ";sender_id=default-" + i + ";close_flush_timeout_millis=0;";
                    try (Sender seed = Sender.fromConfig(seedConfig)) {
                        seed.table("recover").longColumn("v", i).atNow();
                        seed.flush();
                    }
                }
            }
            Assert.assertTrue("in-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-0")));
            Assert.assertTrue("out-of-range fixture must contain unacked data",
                    hasSegmentFile(slot("default-1")));

            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";";
                AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
                CountDownLatch drained = new CountDownLatch(1);
                IntFunction<Sender> factory = idx -> {
                    attempts[idx].incrementAndGet();
                    if (idx == 0) {
                        // Persistent: never clears for the life of the pool.
                        throw new SlotLockContentionException(
                                "sf slot already in use by another process [slot="
                                        + slot("default-0") + ", holder=pid=test]");
                    }
                    Sender delegate = Sender.builder(config).senderId("default-" + idx).build();
                    return notifyingCloseSender(delegate, drained);
                };

                try (SenderPool pool = newPoolWithFactory(config, 0, 1, 5_000, factory)) {
                    Assert.assertTrue(
                            "a persistently contended slot 0 must not starve slot 1's recovery",
                            drained.await(15, TimeUnit.SECONDS));
                    Assert.assertFalse("higher-index slot's orphan data must be delivered",
                            hasSegmentFile(slot("default-1")));
                    Assert.assertEquals("recovered slot must be drained exactly once",
                            1, attempts[1].get());
                    Assert.assertTrue("contended slot must have been probed", attempts[0].get() >= 1);
                    Assert.assertTrue("contended slot's durable data must be preserved on disk",
                            hasSegmentFile(slot("default-0")));
                    Assert.assertTrue("recovered frame must reach the server", handler.frames.get() >= 1);
                    Assert.assertFalse("recovery must not report complete while the contended slot holds data",
                            pool.isRecoveryCompleteForTesting());
                }
            }
        });
    }

    @Test
    public void testStartupRecoveryParksContendedSlotAndContinuesScan() throws Exception {
        // C2, white-box and fully step-driven (no live driver, no wall clock):
        // one recovery step must park a contended in-range slot 0, continue to
        // the out-of-range slot 1 within the SAME step, keep the parked slot
        // retryable across scan cycles (never abandoned, never complete), and
        // WARN about the contended slot exactly once, not once per retry.
        createCandidateSlot("default-0");
        createCandidateSlot("default-1");
        AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
        CountDownLatch drained = new CountDownLatch(1);
        IntFunction<Sender> factory = idx -> {
            attempts[idx].incrementAndGet();
            if (idx == 0) {
                throw new SlotLockContentionException(
                        "sf slot already in use by another process [slot="
                                + slot("default-0") + ", holder=pid=test]");
            }
            return successfulRecoverySender(idx, drained);
        };
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";

        Logger poolLogger = (Logger) LoggerFactory.getLogger(SenderPool.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = poolLogger.getLevel();
        poolLogger.setLevel(Level.ALL);
        poolLogger.addAppender(appender);
        try (SenderPool pool = newDeferredPoolWithFactory(config, 0, 1, 0, factory)) {
            // Cycle 1: park the contended slot 0, then drain slot 1 in the
            // same step (the park must not consume the step's single drain).
            Assert.assertTrue("step must park the contended slot and drain the next candidate",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertEquals(1, attempts[0].get());
            Assert.assertEquals("higher-index slot must be recovered despite the parked slot",
                    1, attempts[1].get());
            Assert.assertEquals(0, drained.getCount());
            Assert.assertFalse("cycle with a parked slot must defer, not complete",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertFalse("recovery must not report complete while the contended slot holds data",
                    pool.isRecoveryCompleteForTesting());

            // Cycle 2: the parked slot is re-probed (retryable, not abandoned).
            Assert.assertTrue(pool.runStartupRecoveryStepForTesting());
            Assert.assertEquals("parked slot must be re-probed on the next cycle",
                    2, attempts[0].get());
            Assert.assertFalse(pool.runStartupRecoveryStepForTesting());

            // Bounded logging: the contended slot warned once, not per retry.
            long contentionWarns = appender.list.stream().filter(e ->
                    e.getLevel().isGreaterOrEqual(Level.WARN)
                            && e.getFormattedMessage().contains("default-0")).count();
            Assert.assertEquals("contended slot must WARN once per episode, not per retry; captured="
                    + appender.list, 1, contentionWarns);
        } finally {
            poolLogger.detachAppender(appender);
            poolLogger.setLevel(savedLevel);
            appender.stop();
        }
    }

    @Test
    public void testStartupRecoveryParksPersistentlyFailingSlotAfterBoundedRetries() throws Exception {
        // C2, generic-failure flavor, step-driven: a slot whose recovery build
        // persistently fails with a NON-contention error keeps the existing
        // retry-in-place behavior for the first attempts (the server-wide
        // transient heuristic) but must be parked after a bounded streak so it
        // cannot starve the higher-index in-range slot, and must be re-probed
        // on the next scan cycle rather than abandoned.
        createCandidateSlot("default-0");
        createCandidateSlot("default-1");
        AtomicInteger[] attempts = {new AtomicInteger(), new AtomicInteger()};
        CountDownLatch drained = new CountDownLatch(1);
        IntFunction<Sender> factory = idx -> {
            attempts[idx].incrementAndGet();
            if (idx == 0) {
                throw new LineSenderException("persistent per-slot recovery failure");
            }
            return successfulRecoverySender(idx, drained);
        };
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";

        try (SenderPool pool = newDeferredPoolWithFactory(config, 0, 2, 0, factory)) {
            // Attempts 1 and 2: presumed transient, same candidate retried.
            Assert.assertFalse("first failure must defer the same candidate",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertFalse("second failure must defer the same candidate",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertEquals(2, attempts[0].get());
            Assert.assertEquals("failing slot must not have blocked past its streak yet",
                    0, attempts[1].get());

            // Attempt 3 exhausts the streak: the slot is parked and the scan
            // may continue to the next candidate.
            Assert.assertTrue("streak exhaustion must park the slot and continue the scan",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertEquals(3, attempts[0].get());
            Assert.assertTrue("higher-index slot must be recovered despite the parked slot",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertEquals("higher-index slot must be drained exactly once",
                    1, attempts[1].get());
            Assert.assertEquals(0, drained.getCount());

            // End of cycle: parked slot outstanding -> defer, not complete.
            Assert.assertFalse("cycle with a parked slot must defer, not complete",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertFalse("recovery must not report complete while the parked slot holds data",
                    pool.isRecoveryCompleteForTesting());

            // Next cycle: the parked slot is re-probed with a fresh streak.
            Assert.assertFalse("re-probed slot restarts its bounded retry streak",
                    pool.runStartupRecoveryStepForTesting());
            Assert.assertEquals(4, attempts[0].get());
        }
    }

    @Test
    public void testLongMaxStartupRecoveryBudgetDoesNotOverflow() throws Exception {
        createCandidateSlot("default-0");
        AtomicInteger attempts = new AtomicInteger();
        IntFunction<Sender> factory = idx -> {
            attempts.incrementAndGet();
            return successfulRecoverySender(idx, new CountDownLatch(0));
        };
        String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";

        try (SenderPool pool = newPoolWithFactory(config, 0, 1, Long.MAX_VALUE, factory)) {
            Assert.assertEquals("Long.MAX_VALUE must leave a positive inline recovery budget",
                    1, attempts.get());
            Assert.assertTrue("the inline scan must complete", pool.isRecoveryCompleteForTesting());
        }
    }

    @Test
    public void testStartupRecoveryIsBoundedByASharedBudget() throws Exception {
        // Regression for the startup-recovery budget (M1).
        // recoverOneSlotStep() runs synchronously in the SenderPool
        // constructor. A previous run can strand unacked data in EVERY in-range
        // slot, and if the server is reachable but does not ack, each slot's
        // drain blocks for the full acquireTimeoutMillis. Without a shared,
        // whole-scan budget (and a short-circuit on the first drain that fails
        // to ack) construction blocks for (maxSize - minSize) *
        // acquireTimeoutMillis -- here 4 * 1s = 4s -- so QuestDB.build() stalls
        // proportionally to the recovery backlog. The fix caps the TOTAL
        // recovery at ~one acquireTimeoutMillis and stops scanning the moment a
        // drain fails to ack, so construction must return well inside that
        // ceiling no matter how many slots are stranded.
        final long acquireTimeoutMillis = 1_000L;
        final int maxSize = 4;

        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: seed unacked data into default-0..3 against a silent
            // server (never acks; close_flush_timeout=0 leaves the
            // flushed-but-unacked .sfa on disk).
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, maxSize, maxSize, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] s = new PooledSender[maxSize];
                    for (int i = 0; i < maxSize; i++) {
                        s[i] = seed.borrow();
                    }
                    for (int i = 0; i < maxSize; i++) {
                        s[i].table("recover").longColumn("v", i).atNow();
                        s[i].flush();
                    }
                    for (int i = maxSize - 1; i >= 0; i--) {
                        s[i].close();
                    }
                }
            }
            for (int i = 0; i < maxSize; i++) {
                Assert.assertTrue("default-" + i + " must hold unacked data",
                        hasSegmentFile(slot("default-" + i)));
            }

            // Phase 2: restart against a STILL-silent (reachable but
            // never-acking) server. Construction triggers startup recovery over
            // all four stranded slots. close_flush_timeout=0 makes each
            // recoverer's close() a fast close, so the measured window isolates
            // the drain budget: pre-fix it is maxSize * acquireTimeoutMillis;
            // post-fix it is bounded by ~one acquireTimeoutMillis.
            try (TestWebSocketServer silent2 = new TestWebSocketServer(new SilentHandler())) {
                int port = silent2.getPort();
                silent2.start();
                Assert.assertTrue(silent2.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";

                long startNanos = System.nanoTime();
                SenderPool pool = new SenderPool(cfg, 0, maxSize, acquireTimeoutMillis, Long.MAX_VALUE, Long.MAX_VALUE);
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                try {
                    // Headline guarantee: total recovery is bounded by the
                    // shared budget, NOT maxSize * acquireTimeoutMillis. The 3x
                    // ceiling leaves generous CI margin over the ~1x post-fix
                    // cost while still decisively failing the pre-fix 4x stall.
                    Assert.assertTrue(
                            "startup recovery must be bounded by a shared budget, not per-slot: took "
                                    + elapsedMillis + "ms with acquireTimeout=" + acquireTimeoutMillis
                                    + "ms over " + maxSize + " stranded slots (pre-fix ~"
                                    + (maxSize * acquireTimeoutMillis) + "ms)",
                            elapsedMillis < 3 * acquireTimeoutMillis);

                    // Durability, not loss: the silent server never acked, so
                    // the stranded data is deferred (still on disk for a later
                    // attempt), never dropped.
                    for (int i = 0; i < maxSize; i++) {
                        Assert.assertTrue(
                                "stranded data must be preserved on disk, not lost: default-" + i,
                                hasSegmentFile(slot("default-" + i)));
                    }
                } finally {
                    pool.close();
                }
            }
        });
    }

    @Test
    public void testRecoveryStepStaysBoundedWithDrainOrphansAgainstNonAckingServer() throws Exception {
        // M-A regression: a startup-recovery delegate must NOT inherit
        // drain_orphans=on. If it did, building the recoverer (which connects
        // OK against a reachable server because initial_connect_mode is forced
        // OFF) would run an orphan scan and dispatch a BackgroundDrainerPool at
        // any foreign/out-of-range orphan. Against a reachable-but-not-acking
        // server those background drainers never reach their target, so the
        // recoverer's close() -- called inside the recovery step, on the
        // housekeeper thread, BEFORE cursorEngine.close() releases the slot
        // flock -- blocks in BackgroundDrainerPool.close() for ~3s
        // (GRACEFUL_DRAIN_MILLIS + STOP_GRACE_MILLIS). That makes one step
        // ~1s drain + ~3s drainerPool.close ~= 4s, far past
        // RECOVERY_DRAIN_BUDGET_MILLIS and PoolHousekeeper.STOP_TIMEOUT_MILLIS
        // (2s) -- and a close() landing mid-step would return with the flock
        // still held. After the fix recovery delegates force drain_orphans=off,
        // so no drainer pool is created and the step stays bounded by the drain
        // budget alone.
        final long acquireTimeoutMillis = 1_000L;
        final int maxSize = 2;

        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String addr = "localhost:" + silentPort;

                // Phase 1a: strand unacked data in an in-range managed slot
                // (default-0) so the recovery step has a slot to process.
                String seedCfg = "ws::addr=" + addr + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, 1, maxSize, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender s = seed.borrow();
                    s.table("recover").longColumn("v", 1L).atNow();
                    s.flush();
                    s.close();
                }
                Assert.assertTrue("default-0 must hold unacked data", hasSegmentFile(slot("default-0")));

                // Phase 1b: strand a FOREIGN orphan (different base) so a
                // recovery delegate that inherited drain_orphans=on would
                // dispatch a background drainer at it.
                String ghostCfg = "ws::addr=" + addr + ";sf_dir=" + sfDir
                        + ";sender_id=legacy;close_flush_timeout_millis=0;";
                try (Sender ghost = Sender.fromConfig(ghostCfg)) {
                    for (int i = 0; i < 3; i++) {
                        ghost.table("foreign").longColumn("v", i).atNow();
                        ghost.flush();
                    }
                } catch (Exception ignored) {
                    // best-effort: we only need the unacked .sfa on disk
                }
                Assert.assertTrue("foreign leftover must hold unacked data", hasSegmentFile(slot("legacy")));

                // Phase 2: a deferred drain_orphans=on pool against the SAME
                // reachable-but-not-acking server. Drive ONE recovery step and
                // time it: the step builds a recovery delegate on default-0,
                // which pre-fix would dispatch a drainer at the foreign orphan
                // and then block ~3s in drainerPool.close().
                String cfg = "ws::addr=" + addr + ";sf_dir=" + sfDir
                        + ";drain_orphans=on;close_flush_timeout_millis=0;";
                SenderPool pool = newDeferredPool(cfg, 0, maxSize, acquireTimeoutMillis);
                try {
                    long startNanos = System.nanoTime();
                    pool.runStartupRecoveryStepForTesting();
                    long stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                    // Headline guarantee: a recovery delegate must not stand up a
                    // BackgroundDrainerPool, so the step is bounded by the drain
                    // budget (~1s) -- comfortably under STOP_TIMEOUT_MILLIS. The
                    // 2.5s ceiling clears the ~1s post-fix cost with CI margin
                    // while decisively failing the pre-fix ~4s overrun.
                    Assert.assertTrue(
                            "recovery step must stay bounded with drain_orphans=on against a "
                                    + "non-acking server (no BackgroundDrainerPool overrun): took "
                                    + stepMillis + "ms",
                            stepMillis < 2_500L);

                    // Durability, not loss: the foreign orphan stays on disk for
                    // a later live-sender drainer; the recovery delegate must
                    // not have abandoned it as .failed either.
                    Assert.assertTrue("foreign orphan data must be preserved on disk, not lost",
                            hasSegmentFile(slot("legacy")));
                    Assert.assertFalse("foreign orphan must not be flagged .failed by a recovery delegate",
                            Files.exists(slot("legacy") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                } finally {
                    pool.close();
                }
            }
        });
    }

    @Test
    public void testDeferredStartupRecoveryDoesNotBlockConstruction() throws Exception {
        // M2 fix: when recovery is deferred (the pooled QuestDB handle's path),
        // constructing the SenderPool must NOT run startup recovery inline, so
        // build() never blocks on a reachable-but-not-acking server -- not even
        // when every in-range slot holds stranded data. Recovery is driven later,
        // off the build() thread (by the housekeeper in production; explicitly
        // here).
        final long acquireTimeoutMillis = 1_000L;
        final int maxSize = 4;

        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: seed unacked data into default-0..3 against a silent
            // server (never acks; close_flush_timeout=0 leaves the .sfa on disk).
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, maxSize, maxSize, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender[] s = new PooledSender[maxSize];
                    for (int i = 0; i < maxSize; i++) {
                        s[i] = seed.borrow();
                    }
                    for (int i = 0; i < maxSize; i++) {
                        s[i].table("recover").longColumn("v", i).atNow();
                        s[i].flush();
                    }
                    for (int i = maxSize - 1; i >= 0; i--) {
                        s[i].close();
                    }
                }
            }
            for (int i = 0; i < maxSize; i++) {
                Assert.assertTrue("default-" + i + " must hold unacked data",
                        hasSegmentFile(slot("default-" + i)));
            }

            // Phase 2: construct a DEFERRED pool against a STILL-silent (reachable
            // but never-acking) server. Pre-fix, inline recovery would block the
            // constructor for ~one acquireTimeoutMillis here; deferred, it returns
            // effectively immediately because recovery has not run yet.
            try (TestWebSocketServer silent2 = new TestWebSocketServer(new SilentHandler())) {
                int port = silent2.getPort();
                silent2.start();
                Assert.assertTrue(silent2.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";

                long startNanos = System.nanoTime();
                SenderPool pool = newDeferredPool(cfg, 0, maxSize, acquireTimeoutMillis);
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                try {
                    // Headline: deferred construction does not pay the recovery
                    // drain budget. A whole acquireTimeout of margin is plenty --
                    // pre-fix this would have been ~acquireTimeoutMillis.
                    Assert.assertTrue(
                            "deferred construction must not block on recovery: took " + elapsedMillis
                                    + "ms with acquireTimeout=" + acquireTimeoutMillis + "ms",
                            elapsedMillis < acquireTimeoutMillis);

                    // Recovery has not been driven yet, so every stranded slot is
                    // still on disk -- proving construction skipped inline recovery.
                    for (int i = 0; i < maxSize; i++) {
                        Assert.assertTrue(
                                "deferred construction must NOT recover inline: default-" + i,
                                hasSegmentFile(slot("default-" + i)));
                    }

                    // Driving recovery against the still-silent server is bounded
                    // by the shared budget and preserves the data (durable, not
                    // lost) for a later attempt -- exercising the deferred path's
                    // concurrency-safe slot reservation too.
                    long recoverStart = System.nanoTime();
                    pool.runStartupRecoveryToCompletionForTesting();
                    long recoverMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - recoverStart);
                    Assert.assertTrue(
                            "driven recovery must be bounded by the shared budget: took " + recoverMillis
                                    + "ms (acquireTimeout=" + acquireTimeoutMillis + "ms)",
                            recoverMillis < 3 * acquireTimeoutMillis);
                    for (int i = 0; i < maxSize; i++) {
                        Assert.assertTrue(
                                "stranded data must be preserved on disk, not lost: default-" + i,
                                hasSegmentFile(slot("default-" + i)));
                    }
                } finally {
                    pool.close();
                }
            }
        });
    }

    @Test
    public void testDeferredStartupRecoveryDeliversWhenDriven() throws Exception {
        // Deferring recovery off the constructor must not lose it: once driven
        // (by the housekeeper in production; explicitly here) against an acking
        // server, a deferred pool recovers its stranded managed slots exactly as
        // the inline path would. The drive is also idempotent.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: seed unacked data into default-0 (silent server).
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, 1, 1, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender s = seed.borrow();
                    s.table("recover").longColumn("v", 1).atNow();
                    s.flush();
                    s.close();
                }
            }
            Assert.assertTrue("default-0 must hold unacked data", hasSegmentFile(slot("default-0")));

            // Phase 2: deferred pool against an ACKING server.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                int ackPort = ack.getPort();
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir + ";";
                SenderPool pool = newDeferredPool(cfg, 0, 2, 5_000);
                try {
                    // Deferred: not recovered until driven.
                    Assert.assertTrue("deferred construction must NOT recover inline: default-0",
                            hasSegmentFile(slot("default-0")));

                    // Drive it (what the housekeeper does on its first tick).
                    pool.runStartupRecoveryToCompletionForTesting();
                    Assert.assertTrue("driven recovery must empty default-0",
                            awaitNoSegmentFile(slot("default-0"), 15_000));
                    Assert.assertTrue("recovered frames must reach the server",
                            awaitAtLeast(handler.frames, 1, 15_000));
                    Assert.assertFalse("recovered slot must not be flagged .failed",
                            Files.exists(slot("default-0") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

                    // Idempotent: a second drive is a no-op and must not throw.
                    pool.runStartupRecoveryToCompletionForTesting();
                    Assert.assertFalse("default-0 stays recovered", hasSegmentFile(slot("default-0")));

                    // Pool still usable for normal borrows.
                    PooledSender a = pool.borrow();
                    a.close();
                } finally {
                    pool.close();
                }
            }
        });
    }

    @Test(timeout = 60_000)
    public void testCloseDuringDeferredRecoveryStopsBuildingOnClosingPool() throws Exception {
        // C1 regression. The housekeeper drives startup recovery one slot per
        // step on its own thread. QuestDBImpl.close() raises the pool's shutdown
        // signal (markClosing) BEFORE stopping the housekeeper, and every step
        // re-checks it, so a close() landing while a recoverer is mid-drain must
        // stop recovery from building the NEXT slot -- no more "keeps building
        // senders on a logically-closed pool". A fake recoverer parks slot-0's
        // drain so we can raise the signal deterministically inside the window.
        TestUtils.assertMemoryLeak(() -> {
            // Seed REAL unacked data into default-0 and default-1 (two candidates).
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int silentPort = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (SenderPool seed = new SenderPool(seedCfg, 2, 2, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    PooledSender a = seed.borrow();
                    PooledSender b = seed.borrow();
                    a.table("recover").longColumn("v", 0).atNow();
                    a.flush();
                    b.table("recover").longColumn("v", 1).atNow();
                    b.flush();
                    b.close();
                    a.close();
                }
            }
            Assert.assertTrue(hasSegmentFile(slot("default-0")));
            Assert.assertTrue(hasSegmentFile(slot("default-1")));

            final CountDownLatch slot0DrainStarted = new CountDownLatch(1);
            final CountDownLatch releaseSlot0Drain = new CountDownLatch(1);
            final java.util.List<Integer> builtSlots =
                    java.util.Collections.synchronizedList(new java.util.ArrayList<>());
            IntFunction<Sender> factory = idx -> {
                builtSlots.add(idx);
                return blockingFakeSender(idx, slot0DrainStarted, releaseSlot0Drain);
            };

            // minSize=0 -> recovery is the only factory caller. Generous
            // acquireTimeout so the ONLY reason slot 1 could be skipped is the
            // shutdown signal being honoured.
            final SenderPool pool = newDeferredPoolWithFactory(
                    "ws::addr=localhost:1;sf_dir=" + sfDir + ";", 0, 2, 30_000, factory);
            // Mimic the housekeeper: drive steps back-to-back until done/closing.
            Thread recovery = new Thread(() -> {
                try {
                    while (pool.runStartupRecoveryStepForTesting()) {
                        // keep stepping
                    }
                } catch (Exception ignored) {
                }
            }, "test-recovery");
            recovery.start();

            Assert.assertTrue("slot-0 recoverer must reach drain()",
                    slot0DrainStarted.await(10, TimeUnit.SECONDS));

            // Raise the shutdown signal mid-drain, exactly as QuestDBImpl.close()
            // does before stopping the housekeeper.
            pool.markClosingForTesting();
            releaseSlot0Drain.countDown();
            recovery.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("recovery thread must finish", recovery.isAlive());

            Assert.assertTrue("sanity: the in-flight slot-0 recoverer was built",
                    builtSlots.contains(0));
            Assert.assertFalse(
                    "recovery built a recoverer for slot 1 after the pool was signalled "
                            + "closing; builtSlots=" + builtSlots,
                    builtSlots.contains(1));

            pool.close();
        });
    }

    // ----------------------------------------------------------------------
    // Helpers.
    // ----------------------------------------------------------------------

    private static Sender closeCountingSender(
            int idx, AtomicInteger[] closeCalls, boolean throwOnClose
    ) {
        return closeCountingSender(idx, closeCalls, throwOnClose, null, null, null);
    }

    private static Sender closeCountingSender(
            int idx,
            AtomicInteger[] closeCalls,
            boolean throwOnClose,
            AtomicReference<Thread> recoveryThread,
            AtomicBoolean cleanupBeforeDriverQuiescence,
            CountDownLatch releaseDriver
    ) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            if (recoveryThread != null && recoveryThread.get().isAlive()) {
                                cleanupBeforeDriverQuiescence.set(true);
                                releaseDriver.countDown();
                            }
                            closeCalls[idx].incrementAndGet();
                            if (throwOnClose) {
                                throw new AssertionError("injected delegate close failure");
                            }
                            return null;
                        case "toString":
                            return "CloseCountingSender-" + idx;
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new AssertionError("unexpected prewarmed sender call: " + method.getName());
                    }
                });
    }

    private void createCandidateSlot(String name) throws IOException {
        java.nio.file.Path dir = Paths.get(slot(name));
        java.nio.file.Files.createDirectories(dir);
        java.nio.file.Files.write(dir.resolve("0.sfa"), new byte[]{1});
    }

    private static Sender notifyingCloseSender(Sender delegate, CountDownLatch closed) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(delegate, args);
                        if ("close".equals(method.getName())) {
                            closed.countDown();
                        }
                        return result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private String slot(String name) {
        return sfDir + "/" + name;
    }

    private static Sender successfulRecoverySender(int idx, CountDownLatch drained) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "drain":
                            drained.countDown();
                            return true;
                        case "close":
                            return null;
                        case "toString":
                            return "SuccessfulRecoverySender-" + idx;
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            throw new AssertionError("unexpected recovery sender call: " + method.getName());
                    }
                });
    }

    private void assertPreallocatedExitHandoffCleansStartupRecoverer(
            int strandedIndex, int maxSize) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String strandedId = "default-" + strandedIndex;
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=100;";
                Sender sender = Sender.builder(config).senderId(strandedId).build();
                sender.table("recover").longColumn("v", strandedIndex).atNow();
                sender.flush();
                try {
                    sender.close();
                } catch (LineSenderException expected) {
                    Assert.assertTrue(expected.getMessage(),
                            expected.getMessage().contains("drain timed out"));
                }
            }
            Assert.assertTrue("startup-recovery fixture must contain an unacked segment",
                    hasSegmentFile(slot(strandedId)));

            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";";
                AtomicBoolean instrumented = new AtomicBoolean();
                AtomicReference<CursorSendEngine> engineRef = new AtomicReference<>();
                AtomicReference<SegmentManager> managerRef = new AtomicReference<>();
                AtomicReference<Throwable> hookErr = new AtomicReference<>();
                CountDownLatch releaseWorker = new CountDownLatch(1);
                IntFunction<Sender> factory = idx -> {
                    Sender sender = Sender.builder(config).senderId("default-" + idx).build();
                    if (idx == strandedIndex && instrumented.compareAndSet(false, true)) {
                        try {
                            CursorSendEngine engine = ((QwpWebSocketSender) sender).getCursorEngineForTesting();
                            SegmentManager manager = engine.getManagerForTesting();
                            CountDownLatch workerBlocked = new CountDownLatch(1);
                            AtomicBoolean fired = new AtomicBoolean();
                            manager.setBeforeTrimSyncHook(() -> {
                                if (!fired.compareAndSet(false, true)) {
                                    return;
                                }
                                workerBlocked.countDown();
                                try {
                                    if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                                        hookErr.compareAndSet(null,
                                                new AssertionError("timed out waiting to release recovery worker"));
                                    }
                                } catch (Throwable t) {
                                    hookErr.compareAndSet(null, t);
                                }
                            });
                            if (!workerBlocked.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("recovery manager never entered a service pass");
                            }
                            manager.setBeforeExitCleanupRegistrationHook(() -> {
                                throw new OutOfMemoryError("simulated callback allocation failure");
                            });
                            manager.setWorkerJoinTimeoutMillis(50L);
                            engineRef.set(engine);
                            managerRef.set(manager);
                        } catch (Throwable t) {
                            try {
                                sender.close();
                            } catch (Throwable ignored) {
                            }
                            throw new RuntimeException(t);
                        }
                    }
                    return sender;
                };

                SenderPool pool = null;
                try {
                    pool = newPoolWithFactory(config, 0, maxSize, 2_000, factory);
                    CursorSendEngine engine = engineRef.get();
                    SegmentManager manager = managerRef.get();
                    Assert.assertNotNull("startup recovery must build the stranded slot", engine);
                    if (strandedIndex < maxSize) {
                        Assert.assertEquals("in-range recoverer must remain retired while worker is live",
                                1, pool.leakedSlotCount());
                    } else {
                        Assert.assertEquals("out-of-range recovery must not consume pool capacity",
                                0, pool.leakedSlotCount());
                    }
                    Assert.assertFalse("cleanup must remain pending while the worker is live",
                            engine.isCloseCompleted());

                    releaseWorker.countDown();
                    manager.close();
                    Assert.assertTrue("recovery manager worker must be reaped", manager.isWorkerReaped());
                    Assert.assertTrue("worker exit must complete startup-recoverer cleanup without "
                                    + "a sender or engine close retry [index=" + strandedIndex + "]",
                            engine.isCloseCompleted());
                    if (hookErr.get() != null) {
                        throw new AssertionError("recovery worker hook failed", hookErr.get());
                    }
                    if (strandedIndex < maxSize) {
                        pool.reapIdle();
                        Assert.assertEquals("late cleanup must restore in-range pool capacity",
                                0, pool.leakedSlotCount());
                    }
                    try (SlotLock ignored = SlotLock.acquire(slot(strandedId))) {
                        // Completion must mean the real slot flock is reusable.
                    }
                } finally {
                    releaseWorker.countDown();
                    SegmentManager manager = managerRef.get();
                    if (manager != null) {
                        manager.setBeforeExitCleanupRegistrationHook(null);
                        manager.setBeforeTrimSyncHook(null);
                        manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                        manager.close();
                    }
                    CursorSendEngine engine = engineRef.get();
                    if (engine != null && !engine.isCloseCompleted()) {
                        engine.close();
                    }
                    if (pool != null) {
                        pool.close();
                    }
                }
            }
        });
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

    @Test
    public void testContendedSlotReprobeUsesFlockProbeNotFullBuild() throws Exception {
        // A slot whose flock is held by another LIVE owner is parked and
        // re-probed on every retry cycle -- potentially for the owner's whole
        // lifetime. The re-probe must ask the flock directly (O(1) probe,
        // a few syscalls), not pay a full recovery build (config re-parse,
        // builder graph, parent-dir fsync barriers in periodic durability,
        // owned SegmentManager allocation) per cycle just to reach
        // SlotLock.acquire and throw.
        TestUtils.assertMemoryLeak(() -> {
            String config = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";
            String slot0 = slot("default-0");

            // Seed one unacked frame so slot 0 is a candidate orphan. The
            // group root normally comes from Sender.build(); this test seeds
            // the slot directly, so create the parent first (mkdir in
            // SlotLock.acquire is non-recursive).
            Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine seed = new CursorSendEngine(slot0, 1 << 20)) {
                Unsafe.getUnsafe().setMemory(buf, 16, (byte) 1);
                Assert.assertEquals(0L, seed.appendBlocking(buf, 16));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
            Assert.assertTrue("seeded slot must be a candidate orphan",
                    OrphanScanner.isCandidateOrphan(slot0));

            AtomicInteger builds = new AtomicInteger();
            IntFunction<Sender> factory = idx -> {
                builds.incrementAndGet();
                // Fidelity with the production build: the recovery build's
                // fate is decided by the real slot flock, exactly like
                // defaultRecoverySender's engine construction.
                SlotLock lock = SlotLock.acquire(slot("default-" + idx));
                return (Sender) Proxy.newProxyInstance(
                        Sender.class.getClassLoader(),
                        new Class[]{Sender.class},
                        (proxy, method, args) -> {
                            if ("drain".equals(method.getName())) {
                                return Boolean.TRUE;
                            }
                            if ("close".equals(method.getName())) {
                                lock.close();
                                return null;
                            }
                            throw new AssertionError(
                                    "unexpected recovery sender call: " + method.getName());
                        });
            };

            SlotLock held = SlotLock.acquire(slot0);
            try (SenderPool pool = newDeferredPoolWithFactory(config, 0, 1, 5_000, factory)) {
                // Steady-state re-probe of a parked contended slot: three
                // full cycles while the flock is held.
                for (int cycle = 0; cycle < 3; cycle++) {
                    Assert.assertFalse("a cycle with only a contended slot must defer",
                            pool.runStartupRecoveryStepForTesting());
                }
                Assert.assertEquals("re-probing a contended slot must be a flock probe, "
                        + "not a full recovery build per cycle", 0, builds.get());
                Assert.assertTrue("parked slot must keep its durable data",
                        hasSegmentFile(slot0));

                // The probe must not dampen recovery: once the owner lets
                // go, the very next cycle pays exactly one real build and
                // drains the slot.
                held.close();
                Assert.assertTrue("released slot must be recovered on the next cycle",
                        pool.runStartupRecoveryStepForTesting());
                Assert.assertEquals("released slot must be recovered with exactly one build",
                        1, builds.get());
                Assert.assertFalse("final scan step must mark recovery complete",
                        pool.runStartupRecoveryStepForTesting());
            } finally {
                held.close(); // idempotent when already released
            }
        });
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

    private static Sender getDelegate(PooledSender ps) {
        return ps.getDelegateForTesting();
    }

    // Uses the @TestOnly senderFactory seam so a test can inject a fake/forged
    // delegate (mirrors SenderPoolErrorSafetyTest).
    private static SenderPool newPoolWithFactory(
            String cfg, int min, int max, long acquireMs, IntFunction<Sender> senderFactory
    ) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, senderFactory);
    }

    private static SenderPool newPoolWithRecoveryControls(
            String cfg,
            int min,
            int max,
            long acquireMs,
            IntFunction<Sender> senderFactory,
            ThreadFactory threadFactory,
            Runnable recoveryWaiter,
            Runnable beforeFailedRecoveryJoinHook
    ) {
        return SenderPool.createWithRecoveryControlsForTesting(
                cfg, min, max, acquireMs, senderFactory, threadFactory,
                recoveryWaiter, beforeFailedRecoveryJoinHook);
    }

    private static SenderPool newPoolWithRecoveryThreadFactory(
            String cfg,
            int min,
            int max,
            long acquireMs,
            IntFunction<Sender> senderFactory,
            ThreadFactory threadFactory
    ) throws Throwable {
        return newPoolWithRecoveryControls(
                cfg, min, max, acquireMs, senderFactory, threadFactory, null, null);
    }

    // Uses the @TestOnly 8-arg constructor (deferStartupRecovery=true) so a test
    // can build a pool whose SF startup recovery is NOT run inline -- mirroring
    // the pooled QuestDB handle, which defers it to the housekeeper.
    // senderFactory=null -> the real defaultSender().
    private static SenderPool newDeferredPool(String cfg, int min, int max, long acquireMs) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, null, true);
    }

    // Deferred pool (deferStartupRecovery=true) WITH an injected factory, so a
    // test can drive the housekeeper recovery path against fully controlled
    // (fake) recoverers.
    private static SenderPool newDeferredPoolWithFactory(
            String cfg, int min, int max, long acquireMs, IntFunction<Sender> factory) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, factory, true);
    }

    private static Sender blockingFakeSender(
            int idx, CountDownLatch drainStarted, CountDownLatch releaseDrain
    ) {
        return blockingFakeSender(
                idx, drainStarted, releaseDrain, new CountDownLatch(0), new CountDownLatch(0));
    }

    // Fake Sender whose drain() and close() (for slot 0 only) park until
    // released, opening deterministic shutdown and pre-termination windows.
    // Holds no native resources.
    private static Sender blockingFakeSender(
            int idx,
            CountDownLatch drainStarted,
            CountDownLatch releaseDrain,
            CountDownLatch closeStarted,
            CountDownLatch releaseClose
    ) {
        return (Sender) java.lang.reflect.Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "drain":
                            if (idx == 0) {
                                drainStarted.countDown();
                                releaseDrain.await();
                            }
                            return true;
                        case "close":
                            if (idx == 0) {
                                closeStarted.countDown();
                                releaseClose.await();
                            }
                            return null;
                        case "toString":
                            return "BlockingFakeSender-" + idx;
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) return false;
                            if (rt == int.class) return 0;
                            if (rt == long.class) return 0L;
                            if (rt.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
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
