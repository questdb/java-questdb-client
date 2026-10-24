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
import io.questdb.client.impl.SenderPool;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * The pool's commit window: every close publishes the lease's rows, the close
 * that fills the window (rows appended or age of the open group) flushes, which
 * commits the group, and the settle step commits a group left idle.
 */
public class SenderPoolCommitWindowTest {

    @Test
    public void testAgeClockRestartsAfterACommit() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(1_000, 30L);
            borrowAppendClose(pool, 1L);                 // publishes: the window opens
            Thread.sleep(60);
            borrowAppendClose(pool, 2L);                 // older than 30 ms: commits
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertEquals(2, d.publishCount.get());
            borrowAppendClose(pool, 3L);                 // a fresh window: this close must only publish
            Assert.assertEquals("a clock that was not restarted at the commit would commit every close", 1, d.flushCount.get());
            Assert.assertEquals(3, d.publishCount.get());
        }
    }

    @Test
    public void testAgeWindowClosesAtNextRelease() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(1_000, 30L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.publishCount.get());
            Assert.assertEquals(0, d.flushCount.get());
            Thread.sleep(60);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
            }
            Assert.assertEquals("group older than the interval: this close commits", 1, d.flushCount.get());
            Assert.assertEquals(0, d.pendingRowCount());
        }
    }

    @Test
    public void testDrainResetsTheWindow() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(3, 60_000L);
            borrowAppendClose(pool, 1L);            // publishes: the window is open with 1 row
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
                Assert.assertTrue(s.drain(0));      // app commits at 2 rows: the window restarts here
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.flushCount.get());
            // 3 rows in total: a counter that ignored the drain would commit here
            borrowAppendClose(pool, 3L);
            borrowAppendClose(pool, 4L);
            Assert.assertEquals(4, d.publishCount.get());
            Assert.assertEquals(1, d.flushCount.get());
            // 3 rows since the drain: the window closes with a committing flush
            borrowAppendClose(pool, 5L);
            Assert.assertEquals(5, d.publishCount.get());
            Assert.assertEquals(2, d.flushCount.get());
        }
    }

    @Test
    public void testLeaseFlushAndGetSequenceResetsTheWindow() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(3, 60_000L);
            borrowAppendClose(pool, 1L);                 // publishes: the window is open with 1 row
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
                s.flushAndGetSequence();                 // app commits at 2 rows: the window restarts here
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.flushCount.get());
            borrowAppendClose(pool, 3L);                 // 3 rows in total: a counter that ignored the flush would commit here
            borrowAppendClose(pool, 4L);
            Assert.assertEquals(4, d.publishCount.get());
            Assert.assertEquals(1, d.flushCount.get());
            borrowAppendClose(pool, 5L);                 // 3 rows since the flush: the window closes
            Assert.assertEquals(5, d.publishCount.get());
            Assert.assertEquals(2, d.flushCount.get());
        }
    }

    @Test
    public void testLeaseFlushResetsTheWindow() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(3, 60_000L);
            borrowAppendClose(pool, 1L);            // publishes: the window is open with 1 row
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
                s.flush();                          // app commits at 2 rows: the window restarts here
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.flushCount.get());
            // 3 rows in total: a counter that ignored the flush would commit here
            borrowAppendClose(pool, 3L);
            borrowAppendClose(pool, 4L);
            Assert.assertEquals(4, d.publishCount.get());
            Assert.assertEquals(1, d.flushCount.get());
            // 3 rows since the flush: the window closes with a committing flush
            borrowAppendClose(pool, 5L);
            Assert.assertEquals(5, d.publishCount.get());
            Assert.assertEquals(2, d.flushCount.get());
        }
    }

    @Test
    public void testReleaseWithoutRowsDoesNotOpenAWindow() throws Exception {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(1_000, 30L);
            try (Sender s = pool.borrow()) {
                Assert.assertNotNull(s);           // no rows
            }
            Thread.sleep(60);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals("the empty release must not have started the clock", 0, d.flushCount.get());
            Assert.assertEquals(1, d.pendingRowCount());
        }
    }

    @Test
    public void testRowsWindowClosesWithCommittingFlush() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(3, 60_000L);
            for (int i = 0; i < 3; i++) {
                try (Sender s = pool.borrow()) {
                    s.table("t").longColumn("v", i).atNow();
                }
                RecordingSender d = built.get(0);
                Assert.assertEquals("every close publishes", i + 1, d.publishCount.get());
                Assert.assertEquals("only the third close fills the window and commits", i < 2 ? 0 : 1, d.flushCount.get());
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(0, d.pendingRowCount());
            Assert.assertFalse(d.hasDeferredFrames());
            // the window restarts: a fourth close only publishes again
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 4L).atNow();
            }
            Assert.assertEquals(4, d.publishCount.get());
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertEquals(1, d.pendingRowCount());
        }
    }

    @Test
    public void testSetCommitWindowRejectsNegativeOrMixedValues() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            int[][] bad = {{-1, -1}, {0, -5}, {5, 0}, {0, 5}};
            for (int[] b : bad) {
                try {
                    pool.setCommitWindow(b[0], b[1]);
                    Assert.fail("expected IllegalArgumentException for " + b[0] + ", " + b[1]);
                } catch (IllegalArgumentException expected) {
                    Assert.assertFalse(pool.hasCommitWindow());
                }
            }
            pool.setCommitWindow(0, 0);
            Assert.assertFalse(pool.hasCommitWindow());
        }
    }

    @Test
    public void testSettleCommitsIdleOpenGroup() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(2, 60_000L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals("the release published the group's row and shipped nothing", 1, d.pendingRowCount());
            Assert.assertEquals("the group's published rows are unshipped work for the settle", 1, pool.settleIdle());
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertEquals(0, d.pendingRowCount());
            Assert.assertEquals("window restarted by the settle", 0, pool.settleIdle());
            // the settle marked the commit at 1 row, so the next close (2 rows
            // appended in total, 1 in the window) only publishes; had the mark
            // been missed, the window would count 2 rows and this close would commit
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 2L).atNow();
            }
            Assert.assertEquals(2, d.publishCount.get());
            Assert.assertEquals(1, d.flushCount.get());
        }
    }

    @Test
    public void testSettleCommitsIdleOpenGroupOfDeferredFrames() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(2, 60_000L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
                s.flushDeferred();                       // an auto-flush inside the lease ships a flagged frame
            }
            RecordingSender d = built.get(0);
            Assert.assertTrue(d.hasDeferredFrames());
            Assert.assertEquals(0, d.pendingRowCount());
            Assert.assertEquals("an open group is unshipped work for the settle", 1, pool.settleIdle());
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertFalse(d.hasDeferredFrames());
            Assert.assertEquals("window restarted by the settle", 0, pool.settleIdle());
        }
    }

    @Test
    public void testWindowOffMeansNoCloseCommits() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            Assert.assertFalse(pool.hasCommitWindow());
            for (int i = 0; i < 2; i++) {
                try (Sender s = pool.borrow()) {
                    s.table("t").longColumn("v", i).atNow();
                }
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(2, d.publishCount.get());
            Assert.assertEquals("without a window a close commits only when an auto-flush threshold is due", 0, d.flushCount.get());
            Assert.assertEquals(0, d.deferredFlushCount.get());
            Assert.assertEquals(2, d.pendingRowCount());
        }
    }

    private static void borrowAppendClose(SenderPool pool, long v) {
        try (Sender s = pool.borrow()) {
            s.table("t").longColumn("v", v).atNow();
        }
    }
}
