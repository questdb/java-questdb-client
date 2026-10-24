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
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The pooled close() contract: close makes the rows recoverable and releases,
 * flush ships, drain waits. Drives {@link SenderPool} through its
 * {@code senderFactory} seam with {@link RecordingSender} doubles, so nothing
 * touches the network. The config string is parsed for store-and-forward
 * settings only.
 */
public class SenderPoolCloseSemanticsTest {
    private static final String CONFIG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    @Test
    public void testAutoFlushThrowingAtCloseRetiresSlot() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            RecordingSender delegate = built.get(0);
            LineSenderException original = new LineSenderException("auto-flush ship failed");
            delegate.autoFlushDue = true;
            delegate.flushError = original;
            first.table("t").longColumn("v", 1L).atNow();
            try {
                first.close();
                Assert.fail("close() must propagate the ship failure");
            } catch (LineSenderException e) {
                Assert.assertSame(original, e.getCause());
                // the double has no open record, so the rows are not claimed recoverable
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("slot retired with 1 row(s) not yet shipped: auto-flush ship failed"));
            }
            Assert.assertEquals("the release published the rows before the ship failed", 1, delegate.publishCount.get());
            Assert.assertEquals("slot must be retired, not stranded", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testCancelRowThrowingAtCloseRetiresSlot() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            RecordingSender delegate = built.get(0);
            // A terminal latched between the probe and the cancel: model it as
            // cancelRow() itself failing. The lease must still come home.
            delegate.rowInProgress = true;
            LineSenderException original = new LineSenderException("latched during cancel");
            delegate.cancelError = original;
            try {
                first.close();
                Assert.fail("close() must propagate the cancel failure");
            } catch (LineSenderException e) {
                Assert.assertSame(original, e.getCause());
                Assert.assertTrue(e.getMessage().contains("latched during cancel"));
            }
            Assert.assertEquals("slot must be retired, not stranded", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testCapRollbackThatStillFailsRetiresTheSlotAndCountsTheDroppedRows() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            try (Sender a = pool.borrow()) {
                a.table("t").longColumn("v", 1L).atNow();       // published at A's release
            }
            RecordingSender delegate = built.get(0);
            Sender b = pool.borrow();
            b.table("t").longColumn("v", 2L).atNow();
            b.table("t").longColumn("v", 3L).atNow();
            // A frame-path sender: the release's publish is a flush the cap rejects,
            // and the retry after the rollback is rejected too.
            delegate.publishError = new BatchTooLargeForCapException("batch too large for server batch cap");
            delegate.flushError = new BatchTooLargeForCapException("batch too large for server batch cap");
            try {
                b.close();
                Assert.fail("close() must report the retirement");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getCause() instanceof BatchTooLargeForCapException);
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(
                        "slot retired with 1 row(s) not yet shipped, 2 row(s) of this lease dropped by the cap rollback: "));
                Assert.assertFalse("rows of a sender without an open record are not claimed recoverable",
                        e.getMessage().contains("recoverable"));
            }
            Assert.assertEquals("slot must be retired, not stranded", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testClosePublishesAndDoesNotFlush() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            Assert.assertEquals("close() publishes the lease's rows", 1, delegate.publishCount.get());
            Assert.assertEquals("close() must not ship rows", 0, delegate.flushCount.get());
            Assert.assertEquals("rows stay in the slot's open record", 1, delegate.pendingRowCount());
            Assert.assertEquals("slot must be back on the free list", 1, pool.availableSize());
        }
    }

    @Test
    public void testCloseWithAgedRowsFlushesThroughAutoFlushIfDue() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            try (Sender a = pool.borrow()) {
                a.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            Assert.assertEquals("no threshold due: the release only publishes", 0, delegate.flushCount.get());
            delegate.autoFlushDue = true;                        // A's row is now older than auto_flush_interval
            try (Sender b = pool.borrow()) {
                b.table("t").longColumn("v", 2L).atNow();
            }
            Assert.assertEquals(2, delegate.publishCount.get());
            Assert.assertEquals("a due threshold ships both leases' rows at the release", 1, delegate.flushCount.get());
            Assert.assertEquals(0, delegate.pendingRowCount());
            Assert.assertEquals(1, pool.availableSize());
        }
    }

    @Test
    public void testCloseWrapperKeepsTheDelegatesRetryableFlag() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            built.get(0).latchedError = new LineSenderException("transient wire", true);
            try {
                first.close();
                Assert.fail("close() must rethrow the latched error");
            } catch (LineSenderException e) {
                Assert.assertTrue("a retryable cause makes a retryable wrapper", e.isRetryable());
            }
            Sender second = pool.borrow();
            built.get(1).latchedError = new LineSenderException("permanent", false);
            try {
                second.close();
                Assert.fail("close() must rethrow the latched error");
            } catch (LineSenderException e) {
                Assert.assertFalse("a non-retryable cause makes a non-retryable wrapper", e.isRetryable());
            }
        }
    }

    @Test
    public void testFreeListIsLifo() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(2, 2, built)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            Object slotB = slotOf(b);
            a.close();
            b.close();
            try (Sender next = pool.borrow()) {
                Assert.assertSame("most recently returned slot is borrowed first", slotB, slotOf(next));
            }
        }
    }

    @Test
    public void testLatchedErrorAtCloseRetiresSlotAndThrows() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            RecordingSender delegate = built.get(0);
            LineSenderException original = new LineSenderException("latched terminal");
            delegate.latchedError = original;
            try {
                first.close();
                Assert.fail("close() must rethrow the latched terminal");
            } catch (LineSenderException e) {
                Assert.assertSame(original, e.getCause());
                Assert.assertTrue(e.getMessage().contains("latched terminal"));
            }
            Assert.assertEquals("dead slot must be retired", 0, pool.totalSize());
            Assert.assertEquals("retired delegate must be closed", 1, delegate.closeCount.get());
            try (Sender second = pool.borrow()) {
                Assert.assertEquals("next borrow must build a fresh delegate", 2, built.size());
                Assert.assertNotSame(delegate, ((PooledSender) second).getDelegateForTesting());
            }
        }
    }

    @Test
    public void testLeaseReturnedAfterPoolCloseClosesDelegate() {
        List<RecordingSender> built = new ArrayList<>();
        SenderPool pool = newPool(1, 1, built);
        Sender s = pool.borrow();
        s.table("t").longColumn("v", 1L).atNow();
        pool.close();                                        // bounded wait, lease outstanding
        Assert.assertEquals("borrowed slot is left for the returning lease", 1, pool.totalSize());
        s.close();
        RecordingSender delegate = built.get(0);
        Assert.assertEquals("returning lease must close its delegate", 1, delegate.closeCount.get());
        Assert.assertEquals("a lease closing into a shutting-down pool flushes so ship failures propagate", 1, delegate.flushCount.get());
        Assert.assertEquals(0, pool.totalSize());
    }

    @Test
    public void testPartialRowAtCloseIsCancelledAndSlotKept() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            first.table("t").longColumn("v", 1L);          // no at(): row in progress
            try {
                first.close();
                Assert.fail("close() with a row in progress must throw after releasing");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("row in progress"));
            }
            RecordingSender delegate = built.get(0);
            Assert.assertEquals(1, delegate.cancelCount.get());
            Assert.assertEquals("slot stays in service", 1, pool.availableSize());
            try (Sender second = pool.borrow()) {
                Assert.assertSame("same slot is reused", delegate, ((PooledSender) second).getDelegateForTesting());
            }
        }
    }

    @Test
    public void testLatchedServerErrorAtCloseKeepsItsType() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            RecordingSender delegate = built.get(0);
            SenderError error = SenderError.dataLoss("rejected", "/tmp/q");
            LineSenderServerException original = new LineSenderServerException(error);
            delegate.latchedError = original;
            try {
                first.close();
                Assert.fail("close() must propagate the latched error");
            } catch (LineSenderServerException e) {
                Assert.assertNotSame(original, e);
                Assert.assertSame(error, e.getServerError());
                Assert.assertSame(original, e.getCause());
            }
            Assert.assertEquals(0, pool.totalSize());
        }
    }

    @Test
    public void testPoolCloseLogsRowsAFailedIdleCloseDrops() {
        Logger poolLogger = (Logger) LoggerFactory.getLogger(SenderPool.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        Level savedLevel = poolLogger.getLevel();
        poolLogger.setLevel(Level.ALL);
        poolLogger.addAppender(appender);
        try {
            List<RecordingSender> built = new ArrayList<>();
            SenderPool pool = new SenderPool(CONFIG, 1, 1, 150L, Long.MAX_VALUE, Long.MAX_VALUE, idx -> {
                RecordingSender s = new RecordingSender() {
                    @Override
                    public void close() {
                        super.close();
                        throw new LineSenderException("cursor SF append failed");
                    }
                };
                built.add(s);
                return s;
            });
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
                s.table("t").longColumn("v", 2L).atNow();
            }
            pool.close();
            Assert.assertEquals(1, built.get(0).closeCount.get());
            boolean logged = false;
            for (ILoggingEvent e : appender.list) {
                String msg = e.getFormattedMessage();
                if (e.getLevel() == Level.ERROR && msg.contains("2 unshipped row(s)")
                        && msg.contains("cursor SF append failed")) {
                    logged = true;
                }
            }
            Assert.assertTrue("a failed idle close must log the rows it drops", logged);
        } finally {
            poolLogger.detachAppender(appender);
            poolLogger.setLevel(savedLevel);
        }
    }

    @Test
    public void testPublishPendingOnALeasePublishesWithoutShipping() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            RecordingSender delegate;
            try (Sender s = pool.borrow()) {
                delegate = built.get(0);
                s.table("t").longColumn("v", 1L).atNow();
                s.publishPending();
                Assert.assertEquals("the lease's publishPending() reaches the delegate", 1, delegate.publishCount.get());
                Assert.assertEquals("and ships nothing", 0, delegate.flushCount.get());
                s.table("t").longColumn("v", 2L).atNow();
                s.reset();
                Assert.assertEquals("reset() keeps the row the lease published", 1, delegate.pendingRowCount());
            }
            Assert.assertEquals("close() publishes once more", 2, delegate.publishCount.get());
            Assert.assertEquals("neither publish shipped", 0, delegate.flushCount.get());
            Assert.assertEquals(1, delegate.pendingRowCount());
            Sender stale = null;
            try (Sender s = pool.borrow()) {
                stale = s;
            }
            try {
                stale.publishPending();
                Assert.fail("a stale lease must not publish into the slot");
            } catch (IllegalStateException expected) {
                // "sender handle is closed (returned to the pool)"
            }
            Assert.assertEquals("only the second lease's close published; the stale call never reached the delegate",
                    3, delegate.publishCount.get());
        }
    }

    @Test
    public void testPublishThrowingAtCloseRetiresSlot() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            RecordingSender delegate = built.get(0);
            LineSenderException original = new LineSenderException("publish failed");
            delegate.publishError = original;
            delegate.autoFlushDue = true;                        // a release past the publish would flush here
            first.table("t").longColumn("v", 1L).atNow();
            try {
                first.close();
                Assert.fail("close() must propagate the publish failure");
            } catch (LineSenderException e) {
                Assert.assertSame(original, e.getCause());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("slot retired with 1 row(s) not yet shipped: publish failed"));
            }
            Assert.assertEquals("the failed publish stops the release before autoFlushIfDue()", 0, delegate.flushCount.get());
            Assert.assertEquals("slot must be retired, not stranded", 0, pool.totalSize());
            Assert.assertEquals(1, delegate.closeCount.get());
        }
    }

    @Test
    public void testResetAfterAFlushInsideTheLeaseDropsEveryPendingRow() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            try (Sender a = pool.borrow()) {
                a.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            try (Sender b = pool.borrow()) {
                b.table("t").longColumn("v", 2L).atNow();
                b.flush();
                Assert.assertEquals("the flush shipped both rows", 1, delegate.flushCount.get());
                b.table("t").longColumn("v", 3L).atNow();
                b.reset();
                Assert.assertEquals("the flush emptied the savepoint, so reset drops every pending row", 0, delegate.pendingRowCount());
            }
        }
    }

    @Test
    public void testResetAfterPublishKeepsPublishedRows() {
        List<RecordingSender> built = new ArrayList<>();
        // markSavepoint() at borrow is a no-op here, as on a sender whose release
        // already published every row: the release's publish alone keeps A's rows.
        try (SenderPool pool = new SenderPool(CONFIG, 1, 1, 150L, Long.MAX_VALUE, Long.MAX_VALUE, idx -> {
            RecordingSender s = new RecordingSender() {
                @Override
                public void markSavepoint() {
                }
            };
            built.add(s);
            return s;
        })) {
            try (Sender a = pool.borrow()) {
                a.table("t").longColumn("v", 1L).atNow();
                a.table("t").longColumn("v", 2L).atNow();
            }
            RecordingSender delegate = built.get(0);
            try (Sender b = pool.borrow()) {
                b.table("t").longColumn("v", 3L).atNow();
                b.reset();
                Assert.assertEquals("B's reset keeps the two rows A's release published", 2, delegate.pendingRowCount());
            }
            Assert.assertEquals("nothing shipped", 0, delegate.flushCount.get());
        }
    }

    @Test
    public void testResetDiscardsOnlyTheResettingLeasesRows() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            try (Sender a = pool.borrow()) {
                a.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender delegate = built.get(0);
            Assert.assertEquals("close() shipped nothing", 1, delegate.pendingRowCount());
            try (Sender b = pool.borrow()) {
                Assert.assertEquals("same slot", 1, built.size());
                b.table("t").longColumn("v", 2L).atNow();
                Assert.assertEquals(2, delegate.pendingRowCount());
                b.reset();
                Assert.assertEquals("A's row survives B's reset", 1, delegate.pendingRowCount());
            }
            Assert.assertEquals(1, pool.settleIdle());
            Assert.assertEquals(1, delegate.flushCount.get());
            Assert.assertEquals(0, delegate.pendingRowCount());
        }
    }

    @Test
    public void testStaleCloseDoesNotTouchReborrowedSlot() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            first.close();
            Sender second = pool.borrow();
            second.table("t").longColumn("v", 1L);          // second's row in progress
            RecordingSender delegate = built.get(0);
            delegate.latchedError = new LineSenderException("must not be probed by a stale close");
            first.close();                                   // stale: must be a no-op, must not probe
            Assert.assertEquals("stale close must not retire the live slot", 1, pool.totalSize());
            delegate.latchedError = null;
            Assert.assertEquals("stale close must not cancel the live borrower's row", 0, delegate.cancelCount.get());
            Assert.assertTrue(delegate.hasRowInProgress());
            Assert.assertEquals("stale close must not re-enqueue the slot", 0, pool.availableSize());
            second.atNow();
            second.close();
            Assert.assertEquals(1, pool.availableSize());
        }
    }

    @Test
    public void testMarkSavepointRunsOutsideThePoolLock() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(2, 2, built)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            a.close();
            b.close();
            CountDownLatch gate = new CountDownLatch(1);
            for (RecordingSender rs : built) {
                rs.markSavepointGate = gate;
            }
            AtomicReference<Sender> first = new AtomicReference<>();
            AtomicReference<Sender> second = new AtomicReference<>();
            Thread t1 = new Thread(() -> first.set(pool.borrow()), "first");
            Thread t2 = new Thread(() -> second.set(pool.borrow()), "second");
            try {
                t1.start();
                awaitAvailable(pool, 1);
                t2.start();
                awaitAvailable(pool, 0);
                Assert.assertEquals("a second borrower takes the other slot while the first is still marking", 0, pool.availableSize());
            } finally {
                gate.countDown();
                t1.join(2_000);
                t2.join(2_000);
            }
            first.get().close();
            second.get().close();
        }
    }

    @Test
    public void testStaleResetDoesNotTouchReborrowedSlot() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = newPool(1, 1, built)) {
            Sender first = pool.borrow();
            first.close();
            try (Sender second = pool.borrow()) {
                second.table("t").longColumn("v", 1L).atNow();
                try {
                    first.reset();
                    Assert.fail("a stale lease must not reset the slot");
                } catch (IllegalStateException expected) {
                    // "sender handle is closed (returned to the pool)"
                }
                Assert.assertEquals(1, built.get(0).pendingRowCount());
            }
        }
    }

    static SenderPool newPool(int min, int max, List<RecordingSender> built) {
        return newPoolWithDefaults(min, max, Long.MAX_VALUE, built);
    }

    static SenderPool newPoolWithDefaults(int min, int max, long idleTimeoutMillis, List<RecordingSender> built) {
        return new SenderPool(CONFIG, min, max, 150L, idleTimeoutMillis, Long.MAX_VALUE, idx -> {
            RecordingSender s = new RecordingSender();
            built.add(s);
            return s;
        });
    }

    static Object slotOf(Sender pooledWrapper) {
        try {
            Field f = PooledSender.class.getDeclaredField("slot");
            f.setAccessible(true);
            return f.get(pooledWrapper);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static void awaitAvailable(SenderPool pool, int target) throws InterruptedException {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (pool.availableSize() != target && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
    }
}
