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
                        Assert.assertNotSame("two borrows must hold distinct slots",
                                slotOf(a), slotOf(b));
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
                        Assert.assertSame("returned slot must be recycled",
                                getField(first, "slot"), getField(second, "slot"));
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
                    setBooleanField(delegate, "slotLockReleased", false);

                    // Drive the sweep: the idle timeout has elapsed.
                    Thread.sleep(10);
                    pool.reapIdle();

                    // The reap leaked branch must have fired.
                    Assert.assertEquals("reapIdle must retire the still-locked slot as leaked",
                            1, getIntField(pool, "leakedSlots"));
                    boolean[] slotInUse = (boolean[]) getField(pool, "slotInUse");
                    Assert.assertTrue("leaked slot index 0 must stay reserved", slotInUse[0]);
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
                    Sender normal = invokeBuildSlotDelegate(pool, "defaultSender", 0);
                    try {
                        Assert.assertEquals(
                                "ordinary pooled sender must honour the user's promoted SYNC mode",
                                Sender.InitialConnectMode.SYNC, readInitialConnectMode(normal));
                    } finally {
                        normal.close();
                    }
                    // Recovery delegate on a different slot: forced OFF.
                    Sender recoverer = invokeBuildSlotDelegate(pool, "defaultRecoverySender", 1);
                    try {
                        Assert.assertEquals(
                                "recovery delegate must force OFF so build() makes at most one connect attempt",
                                Sender.InitialConnectMode.OFF, readInitialConnectMode(recoverer));
                    } finally {
                        recoverer.close();
                    }
                }
            }
        });
    }

    @Test
    public void testStartupRecoveryRetiresSlotWhenRecovererCloseLeavesFlockHeld() throws Exception {
        // C1 regression: the startup recovery loop MUST mirror discardBroken /
        // reapIdle. When a recoverer's delegate close() returns with the SF
        // flock still held (the I/O thread refused to stop), the recovered slot
        // index must be retired permanently (leakedSlots++, slotInUse stays
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
                            setBooleanField(real, "closed", true);
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
                            1, getIntField(pool, "leakedSlots"));
                    boolean[] slotInUse = (boolean[]) getField(pool, "slotInUse");
                    Assert.assertTrue("retired slot 0 must stay reserved", slotInUse[0]);
                    Assert.assertFalse("slot 1 must remain free", slotInUse[1]);

                    // A later borrow must take the fresh slot 1, never re-pick
                    // the still-locked default-0 (which would throw "sf slot
                    // already in use").
                    PooledSender b = pool.borrow();
                    try {
                        Assert.assertTrue("borrow must use a fresh slot dir",
                                Files.exists(slot("default-1")));
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
                } finally {
                    pool.close();
                    // Release the forged recoverer's real flock + native
                    // resources: pool.close() never saw it (recovery never
                    // added the recoverer to `all`), so un-forge closed and
                    // close it for real, otherwise assertMemoryLeak trips.
                    Sender leaked = forged.get();
                    if (leaked != null) {
                        setBooleanField(leaked, "closed", false);
                        leaked.close();
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
                    invokeRunStartupRecoveryStep(pool);
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
                    invokeRunStartupRecoveryOnce(pool);
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
                    invokeRunStartupRecoveryOnce(pool);
                    Assert.assertTrue("driven recovery must empty default-0",
                            awaitNoSegmentFile(slot("default-0"), 15_000));
                    Assert.assertTrue("recovered frames must reach the server",
                            awaitAtLeast(handler.frames, 1, 15_000));
                    Assert.assertFalse("recovered slot must not be flagged .failed",
                            Files.exists(slot("default-0") + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

                    // Idempotent: a second drive is a no-op and must not throw.
                    invokeRunStartupRecoveryOnce(pool);
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
                    while (invokeRunStartupRecoveryStep(pool)) {
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
            invokeMarkClosing(pool);
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
        Field slotF = PooledSender.class.getDeclaredField("slot");
        slotF.setAccessible(true);
        Object slot = slotF.get(ps);
        Field f = slot.getClass().getDeclaredField("delegate");
        f.setAccessible(true);
        return (Sender) f.get(slot);
    }

    // Invokes one of the pool's private managed-slot delegate factories
    // (defaultSender / defaultRecoverySender) so a test can inspect the raw
    // delegate it would build for a given slot index.
    private static Sender invokeBuildSlotDelegate(SenderPool pool, String methodName, int slotIndex)
            throws Exception {
        Method m = SenderPool.class.getDeclaredMethod(methodName, int.class);
        m.setAccessible(true);
        return (Sender) m.invoke(pool, slotIndex);
    }

    // Reads the resolved initial-connect mode a built QwpWebSocketSender delegate
    // is using (the value after the builder's SYNC auto-promotion / explicit
    // override has been applied).
    private static Sender.InitialConnectMode readInitialConnectMode(Sender delegate) throws Exception {
        Field f = delegate.getClass().getDeclaredField("initialConnectMode");
        f.setAccessible(true);
        return (Sender.InitialConnectMode) f.get(delegate);
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

    // Reads the package-private PooledSender.slot -- the identity that the pool
    // actually recycles. Wrapper identity is useless for aliasing checks because
    // borrow() allocates a fresh wrapper every call (mirrors SenderPoolTest and
    // SenderPoolErrorSafetyTest).
    private static Object slotOf(PooledSender pooledWrapper) throws Exception {
        Field f = PooledSender.class.getDeclaredField("slot");
        f.setAccessible(true);
        return f.get(pooledWrapper);
    }

    private static void invokeDiscardBroken(SenderPool pool, PooledSender ps) throws Exception {
        Method m = SenderPool.class.getDeclaredMethod("discardBroken", PooledSender.class);
        m.setAccessible(true);
        m.invoke(pool, ps);
    }

    // Uses the @TestOnly senderFactory seam so a test can inject a fake/forged
    // delegate (mirrors SenderPoolErrorSafetyTest).
    private static SenderPool newPoolWithFactory(
            String cfg, int min, int max, long acquireMs, IntFunction<Sender> senderFactory
    ) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, senderFactory);
    }

    // Uses the @TestOnly 8-arg constructor (deferStartupRecovery=true) so a test
    // can build a pool whose SF startup recovery is NOT run inline -- mirroring
    // the pooled QuestDB handle, which defers it to the housekeeper.
    // senderFactory=null -> the real defaultSender().
    private static SenderPool newDeferredPool(String cfg, int min, int max, long acquireMs) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, null, true);
    }

    // Drives a deferred pool's startup recovery to completion (the housekeeper
    // drives it one slot per tick; tests drive the whole backlog in one call).
    private static void invokeRunStartupRecoveryOnce(SenderPool pool) throws Exception {
        Method m = SenderPool.class.getDeclaredMethod("runStartupRecoveryToCompletion");
        m.setAccessible(true);
        m.invoke(pool);
    }

    // Drives a SINGLE recovery step (the housekeeper's per-tick unit); returns
    // whether more stranded slots remain.
    private static boolean invokeRunStartupRecoveryStep(SenderPool pool) throws Exception {
        Method m = SenderPool.class.getDeclaredMethod("runStartupRecoveryStep");
        m.setAccessible(true);
        return (Boolean) m.invoke(pool);
    }

    // Raises the pool's shutdown signal early, exactly as QuestDBImpl.close()
    // does before stopping the housekeeper.
    private static void invokeMarkClosing(SenderPool pool) throws Exception {
        Method m = SenderPool.class.getDeclaredMethod("markClosing");
        m.setAccessible(true);
        m.invoke(pool);
    }

    // Deferred pool (deferStartupRecovery=true) WITH an injected factory, so a
    // test can drive the housekeeper recovery path against fully controlled
    // (fake) recoverers.
    private static SenderPool newDeferredPoolWithFactory(
            String cfg, int min, int max, long acquireMs, IntFunction<Sender> factory) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, factory, true);
    }

    // Fake Sender whose drain() (for slot 0 only) parks until released, opening a
    // deterministic shutdown-during-recovery window. Holds no native resources.
    private static Sender blockingFakeSender(int idx, CountDownLatch drainStarted, CountDownLatch release) {
        return (Sender) java.lang.reflect.Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "drain":
                            if (idx == 0) {
                                drainStarted.countDown();
                                release.await();
                            }
                            return true;
                        case "close":
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
