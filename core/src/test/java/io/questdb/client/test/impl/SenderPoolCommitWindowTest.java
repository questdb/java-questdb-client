package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.impl.SenderPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The pool's commit window with {@code flush_on_close=on}: each close ships a
 * deferred frame, the close that fills the window ships a committing frame
 * instead, and the settle step commits an open group left idle.
 */
public class SenderPoolCommitWindowTest {

    @Test
    public void testAgeWindowClosesAtNextRelease() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(1_000, 30L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.deferredFlushCount.get());
            Thread.sleep(60);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
            }
            Assert.assertEquals("group older than the interval: this close commits", 1, d.flushCount.get());
            Assert.assertEquals(1, d.deferredFlushCount.get());
        }
    }

    @Test
    public void testLeaseFlushResetsTheWindow() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(2, 60_000L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
                s.flush();                          // app commits explicitly: 2 rows, window would close here
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.flushCount.get());
            // the close after the explicit flush found nothing to ship and the
            // window restarted at 2 rows, so this third close is deferred
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 3L).atNow();
            }
            Assert.assertEquals(2, d.deferredFlushCount.get());
            Assert.assertEquals(1, d.flushCount.get());
        }
    }

    @Test
    public void testReleaseWithoutRowsDoesNotOpenAWindow() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(1_000, 30L);
            try (Sender s = pool.borrow()) {
                Assert.assertNotNull(s);           // no rows
            }
            Thread.sleep(60);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals("the empty release must not have started the clock", 1, d.deferredFlushCount.get());
            Assert.assertEquals(0, d.flushCount.get());
        }
    }

    @Test
    public void testRowsWindowClosesWithCommittingFlush() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(3, 60_000L);
            RecordingSender d = null;
            for (int i = 0; i < 3; i++) {
                try (Sender s = pool.borrow()) {
                    s.table("t").longColumn("v", i).atNow();
                }
                d = built.get(0);
            }
            Assert.assertEquals("first two closes ship deferred", 2, d.deferredFlushCount.get());
            Assert.assertEquals("third close fills the window and commits", 1, d.flushCount.get());
            Assert.assertFalse(d.hasDeferredFrames());
            // the window restarts: a fourth close is deferred again
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 4L).atNow();
            }
            Assert.assertEquals(3, d.deferredFlushCount.get());
            Assert.assertEquals(1, d.flushCount.get());
        }
    }

    @Test
    public void testSettleCommitsIdleOpenGroup() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(1_000, 60_000L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertTrue(d.hasDeferredFrames());
            Assert.assertEquals(0, d.pendingRowCount());
            Assert.assertEquals("an open group is unshipped work for the settle", 1, pool.settleIdle());
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertFalse(d.hasDeferredFrames());
            Assert.assertEquals("window restarted by the settle", 0, pool.settleIdle());
            // the next close after a settle commit is deferred again, not committing
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
            }
            Assert.assertEquals(2, d.deferredFlushCount.get());
            Assert.assertEquals(1, d.flushCount.get());
        }
    }

    @Test
    public void testWindowOffMeansEveryCloseCommits() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            Assert.assertFalse(pool.hasCommitWindow());
            for (int i = 0; i < 2; i++) {
                try (Sender s = pool.borrow()) {
                    s.table("t").longColumn("v", i).atNow();
                }
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(2, d.flushCount.get());
            Assert.assertEquals(0, d.deferredFlushCount.get());
        }
    }
}
