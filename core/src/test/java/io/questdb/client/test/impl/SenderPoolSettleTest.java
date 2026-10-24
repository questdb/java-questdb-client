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
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link SenderPool#settleIdle()}: ships rows left in idle slots through an
 * internal lease, retires dead delegates, and never shares a slot with a
 * concurrent borrower.
 */
public class SenderPoolSettleTest {

    @Test
    public void testReapSkipsSlotUntilSettleShipsItsRows() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPoolWithDefaults(0, 1, 1L, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            Thread.sleep(20);
            pool.reapIdle();
            Assert.assertEquals("an idle-expired slot holding rows is not reaped", 1, pool.totalSize());
            Assert.assertEquals(0, delegate.closeCount.get());
            Assert.assertEquals(1, delegate.pendingRowCount());

            delegate.deferredFramesOpen = true;
            delegate.pendingRows = 0;
            pool.reapIdle();
            Assert.assertEquals("an open commit group is unshipped work too", 1, pool.totalSize());

            delegate.deferredFramesOpen = false;
            delegate.pendingRows = 1;
            Assert.assertEquals(1, pool.settleIdle());
            pool.reapIdle();
            Assert.assertEquals("the reap closes the slot once the settle shipped it", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testSettleRetiresSlotHoldingAnOverCapBatch() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            delegate.flushError = new BatchTooLargeForCapException("single table batch too large for server batch cap");
            Assert.assertEquals(0, pool.settleIdle());
            Assert.assertEquals("a batch the cap rejects never ships: the slot is retired", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
            try (Sender next = pool.borrow()) {
                Assert.assertEquals("the next borrow builds a fresh delegate", 2, built.size());
                Assert.assertNotSame(delegate, ((PooledSender) next).getDelegateForTesting());
            }
        }
    }

    @Test
    public void testSettleRetiresSlotWhenFlushThrowsAnError() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            delegate.flushFatal = new AssertionError("buffer invariant broken");
            Assert.assertEquals(0, pool.settleIdle());
            Assert.assertEquals("an Error is not transient: the slot is retired", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testSettleShipsRowsLeftInIdleSlot() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            long idleSince = idleSinceOf(pool);
            Assert.assertEquals(0, delegate.flushCount.get());
            Thread.sleep(20);
            Assert.assertEquals(1, pool.settleIdle());
            Assert.assertEquals(1, delegate.flushCount.get());
            Assert.assertEquals(0, delegate.pendingRowCount());
            Assert.assertEquals("slot must be back on the free list", 1, pool.availableSize());
            Assert.assertEquals("settling must not reset the idle clock", idleSince, idleSinceOf(pool));
            Assert.assertEquals("nothing left to settle", 0, pool.settleIdle());
        }
    }

    @Test
    public void testSettleSkipsSlotsWithoutRows() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(2, 2, built)) {
            Assert.assertEquals(0, pool.settleIdle());
            Assert.assertEquals(0, built.get(0).flushCount.get());
            Assert.assertEquals(0, built.get(1).flushCount.get());
        }
    }

    @Test
    public void testSettleRetiresDeadDelegate() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            delegate.flushError = new LineSenderException("wire is dead");
            delegate.latchedError = new LineSenderException("latched terminal");
            Assert.assertEquals(0, pool.settleIdle());
            Assert.assertEquals("dead delegate must be retired", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testSettleKeepsSlotAfterTransientFlushFailure() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            delegate.flushError = new LineSenderException("ring full, try later");
            Assert.assertEquals(0, pool.settleIdle());
            Assert.assertEquals("healthy delegate stays in the pool", 1, pool.totalSize());
            Assert.assertEquals(1, pool.availableSize());
            Assert.assertEquals("rows stay for the next settle", 1, delegate.pendingRowCount());
            delegate.flushError = null;
            Assert.assertEquals(1, pool.settleIdle());
            Assert.assertEquals(0, delegate.pendingRowCount());
        }
    }

    @Test
    public void testBorrowerNeverSharesSlotWithSettle() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            CountDownLatch gate = new CountDownLatch(1);
            delegate.flushGate = gate;
            Thread settler = new Thread(pool::settleIdle, "settler");
            settler.start();
            try {
                // wait until the settle holds the slot (free list empty)
                long deadline = System.currentTimeMillis() + 2_000;
                while (pool.availableSize() != 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                Assert.assertEquals("settle must hold the slot as an internal lease", 0, pool.availableSize());
                // a borrower arriving now must wait (max=1), not get the slot being flushed
                try {
                    pool.borrow();
                    Assert.fail("borrow must time out while the only slot is being settled");
                } catch (LineSenderException expected) {
                    // acquire timeout (150 ms in newPool)
                }
            } finally {
                gate.countDown();
            }
            settler.join(2_000);
            Assert.assertFalse(settler.isAlive());
            Assert.assertEquals(1, delegate.flushCount.get());
            try (Sender s = pool.borrow()) {
                Assert.assertNotNull(s);
            }
        }
    }

    @Test
    public void testPoolCloseDoesNotWaitForASettleOnABusyRing() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built);
        try (Sender s = pool.borrow()) {
            s.table("t").longColumn("v", 1L).atNow();
        }
        built.get(0).ringBusy = true;
        Assert.assertEquals(0, pool.settleIdle());
        long start = System.nanoTime();
        pool.close();
        Assert.assertTrue("close() must not wait for a settle that never held the slot",
                System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(1_000));
        Assert.assertEquals(1, built.get(0).closeCount.get());
    }

    @Test
    public void testPoolCloseWarnNamesTheSettleLease() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built);
        try (Sender s = pool.borrow()) {
            s.table("t").longColumn("v", 1L).atNow();
        }
        CountDownLatch gate = new CountDownLatch(1);
        built.get(0).flushGate = gate;
        Thread settler = new Thread(pool::settleIdle, "settler");
        Logger poolLogger = (Logger) LoggerFactory.getLogger(SenderPool.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = poolLogger.getLevel();
        poolLogger.setLevel(Level.ALL);
        poolLogger.addAppender(appender);
        try {
            settler.start();
            long deadline = System.currentTimeMillis() + 2_000;
            while (pool.availableSize() != 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            Assert.assertEquals(0, pool.availableSize());
            pool.close();
            int matching = 0;
            for (ILoggingEvent e : appender.list) {
                String msg = e.getFormattedMessage();
                if (e.getLevel() == Level.WARN && msg.contains("1 held by the housekeeper's settle")
                        && msg.contains("0 borrowed by the application")) {
                    matching++;
                }
            }
            Assert.assertEquals("close() must name the settle lease in its WARN", 1, matching);
            gate.countDown();
            settler.join(2_000);
            Assert.assertFalse(settler.isAlive());
            Assert.assertEquals(0, pool.totalSize());
            Assert.assertEquals(1, built.get(0).closeCount.get());
        } finally {
            gate.countDown();
            poolLogger.detachAppender(appender);
            poolLogger.setLevel(savedLevel);
        }
    }

    @Test
    public void testPoolCloseDuringSettleRetiresSlotOnSettleThread() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        SenderPool pool = new SenderPool("http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;", 1, 1, 2_000L, Long.MAX_VALUE, Long.MAX_VALUE,
                slotIndex -> {
                    RecordingSender rs = new RecordingSender();
                    built.add(rs);
                    return rs;
                });
        try {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            CountDownLatch gate = new CountDownLatch(1);
            delegate.flushGate = gate;
            AtomicReference<Throwable> settleFailure = new AtomicReference<>();
            AtomicInteger settleResult = new AtomicInteger(-1);
            Thread settler = new Thread(() -> {
                try {
                    settleResult.set(pool.settleIdle());
                } catch (Throwable t) {
                    settleFailure.set(t);
                }
            }, "settler");
            Thread closer = new Thread(pool::close, "closer");
            settler.start();
            try {
                long deadline = System.currentTimeMillis() + 2_000;
                while (pool.availableSize() != 0 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                Assert.assertEquals(0, pool.availableSize());
                closer.start();
                // close() parks in its timed wait for the internal lease
                deadline = System.currentTimeMillis() + 2_000;
                while (closer.getState() != Thread.State.TIMED_WAITING && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                Assert.assertEquals("close() must be waiting for the internal lease",
                        Thread.State.TIMED_WAITING, closer.getState());
                Assert.assertTrue("close() must be waiting for the internal lease", closer.isAlive());
            } finally {
                gate.countDown();
            }
            settler.join(2_000);
            closer.join(2_000);
            Assert.assertFalse(settler.isAlive());
            Assert.assertFalse(closer.isAlive());
            Assert.assertNull(settleFailure.get());
            Assert.assertEquals(1, settleResult.get());
            Assert.assertEquals("the settle thread must retire the slot once the pool is closing", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        } finally {
            pool.close();
        }
    }

    @Test
    public void testSettleKeepsOtherSlotsBorrowableWhileOneFlushBlocks() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(2, 2, built)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            a.table("t").longColumn("v", 1L).atNow();
            b.table("t").longColumn("v", 2L).atNow();
            a.close();
            b.close();
            CountDownLatch gate = new CountDownLatch(1);
            for (RecordingSender rs : built) {
                rs.flushGate = gate;
            }
            Thread settler = new Thread(pool::settleIdle, "settler");
            settler.start();
            try {
                long deadline = System.currentTimeMillis() + 2_000;
                while (pool.availableSize() == 2 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                Assert.assertEquals("settle holds one slot at a time", 1, pool.availableSize());
                try (Sender borrowed = pool.borrow()) {
                    RecordingSender other = (RecordingSender) ((PooledSender) borrowed).getDelegateForTesting();
                    gate.countDown();
                    settler.join(2_000);
                    Assert.assertFalse(settler.isAlive());
                    Assert.assertEquals("settle never flushes a slot a borrower took mid-pass", 0, other.flushCount.get());
                }
            } finally {
                gate.countDown();
            }
        }
    }

    @Test
    public void testSettleStopsTakingSlotsOnceThePoolCloses() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        SenderPool pool = SenderPoolCloseSemanticsTest.newPool(2, 2, built);
        try {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            a.table("t").longColumn("v", 1L).atNow();
            b.table("t").longColumn("v", 2L).atNow();
            a.close();
            b.close();
            CountDownLatch gate = new CountDownLatch(1);
            for (RecordingSender rs : built) {
                rs.flushGate = gate;
            }
            AtomicReference<Throwable> settleFailure = new AtomicReference<>();
            Thread settler = new Thread(() -> {
                try {
                    pool.settleIdle();
                } catch (Throwable t) {
                    settleFailure.set(t);
                }
            }, "settler");
            Thread closer = new Thread(pool::close, "closer");
            settler.start();
            try {
                long deadline = System.currentTimeMillis() + 2_000;
                while (pool.availableSize() == 2 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                Assert.assertTrue(pool.availableSize() < 2);
                closer.start();
                deadline = System.currentTimeMillis() + 2_000;
                while (closer.getState() != Thread.State.TIMED_WAITING && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                Assert.assertEquals("close() must be waiting for the internal lease",
                        Thread.State.TIMED_WAITING, closer.getState());
            } finally {
                gate.countDown();
            }
            settler.join(2_000);
            closer.join(2_000);
            Assert.assertFalse(settler.isAlive());
            Assert.assertFalse(closer.isAlive());
            Assert.assertNull(settleFailure.get());
            Assert.assertEquals("a closing pool stops the settle after the slot in flight", 1,
                    built.get(0).flushCount.get() + built.get(1).flushCount.get());
        } finally {
            pool.close();
        }
    }

    @Test
    public void testSettleHandlesSeveralCandidatesIndependently() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(2, 2, built)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            a.table("t").longColumn("v", 1L).atNow();
            b.table("t").longColumn("v", 2L).atNow();
            a.close();
            b.close();
            RecordingSender dead = built.get(0);
            RecordingSender healthy = built.get(1);
            dead.flushError = new LineSenderException("wire is dead");
            dead.latchedError = new LineSenderException("latched terminal");
            Assert.assertEquals(1, pool.settleIdle());
            Assert.assertEquals(1, healthy.flushCount.get());
            Assert.assertEquals(1, pool.availableSize());
            Assert.assertEquals(1, pool.totalSize());
            Assert.assertEquals(1, dead.closeCount.get());
            Assert.assertEquals(0, healthy.closeCount.get());
        }
    }

    @Test
    public void testSettleOnABusyRingKeepsTheRowsAndRequeuesTheSlot() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            long idleSince = idleSinceOf(pool);
            delegate.ringBusy = true;
            Logger poolLogger = (Logger) LoggerFactory.getLogger(SenderPool.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            Level savedLevel = poolLogger.getLevel();
            poolLogger.setLevel(Level.ALL);
            poolLogger.addAppender(appender);
            try {
                Assert.assertEquals(0, pool.settleIdle());
                for (ILoggingEvent e : appender.list) {
                    Assert.assertTrue("a busy ring is not a failure, but logged: " + e.getFormattedMessage(),
                            e.getLevel().toInt() < Level.WARN.toInt());
                }
            } finally {
                poolLogger.detachAppender(appender);
                poolLogger.setLevel(savedLevel);
            }
            Assert.assertEquals(1, pool.availableSize());
            Assert.assertEquals(1, delegate.pendingRows);
            Assert.assertEquals(0, delegate.flushCount.get());
            Assert.assertEquals("a busy ring must not touch the idle clock", idleSince, idleSinceOf(pool));
            delegate.ringBusy = false;
            Assert.assertEquals(1, pool.settleIdle());
            Assert.assertEquals(1, delegate.flushCount.get());
            Assert.assertEquals(0, delegate.pendingRows);
        }
    }

    @SuppressWarnings("unchecked")
    private static long idleSinceOf(SenderPool pool) throws Exception {
        Field availableF = SenderPool.class.getDeclaredField("available");
        availableF.setAccessible(true);
        Object slot = ((ArrayDeque<Object>) availableF.get(pool)).peekLast();
        Method m = slot.getClass().getDeclaredMethod("idleSinceMillis");
        m.setAccessible(true);
        return (long) m.invoke(slot);
    }
}
