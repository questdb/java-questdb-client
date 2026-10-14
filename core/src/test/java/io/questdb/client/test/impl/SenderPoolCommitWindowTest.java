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
            pool.setFlushOnClose(true);
            pool.setCommitWindow(2, 60_000L);
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
            // the settle marked the commit at 1 row, so the next close (2 rows
            // appended in total, 1 in the window) defers; had the mark been
            // missed, the window would count 2 rows and this close would commit
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
