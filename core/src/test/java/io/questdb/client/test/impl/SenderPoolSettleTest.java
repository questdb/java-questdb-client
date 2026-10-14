package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.impl.SenderPool;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link SenderPool#settleIdle()}: ships rows left in idle slots through an
 * internal lease, retires dead delegates, and never shares a slot with a
 * concurrent borrower.
 */
public class SenderPoolSettleTest {

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
            Thread.sleep(5);
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
            gate.countDown();
            settler.join(2_000);
            Assert.assertFalse(settler.isAlive());
            Assert.assertEquals(1, delegate.flushCount.get());
            try (Sender s = pool.borrow()) {
                Assert.assertNotNull(s);
            }
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
        try (Sender s = pool.borrow()) {
            s.table("t").longColumn("v", 1L).atNow();
        }
        RecordingSender delegate = built.get(0);
        CountDownLatch gate = new CountDownLatch(1);
        delegate.flushGate = gate;
        AtomicReference<Throwable> settleFailure = new AtomicReference<>();
        Thread settler = new Thread(() -> {
            try {
                pool.settleIdle();
            } catch (Throwable t) {
                settleFailure.set(t);
            }
        }, "settler");
        settler.start();
        long deadline = System.currentTimeMillis() + 2_000;
        while (pool.availableSize() != 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        Assert.assertEquals(0, pool.availableSize());
        Thread closer = new Thread(pool::close, "closer");
        closer.start();
        Thread.sleep(50);                      // close() is now waiting for the internal lease
        gate.countDown();
        settler.join(2_000);
        closer.join(2_000);
        Assert.assertFalse(settler.isAlive());
        Assert.assertFalse(closer.isAlive());
        Assert.assertNull(settleFailure.get());
        Assert.assertEquals("the settle thread must retire the slot once the pool is closing", 0, pool.totalSize());
        Assert.assertEquals(1, delegate.closeCount.get());
    }

    @SuppressWarnings("unchecked")
    private static long idleSinceOf(SenderPool pool) throws Exception {
        Field availableF = SenderPool.class.getDeclaredField("available");
        availableF.setAccessible(true);
        Object slot = ((java.util.ArrayDeque<Object>) availableF.get(pool)).peekLast();
        Method m = slot.getClass().getDeclaredMethod("idleSinceMillis");
        m.setAccessible(true);
        return (long) m.invoke(slot);
    }
}
