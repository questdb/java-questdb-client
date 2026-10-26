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
 * What a pooled release asks of its delegate: under {@code flush_on_close=on}
 * one window-member flush, which the delegate's I/O thread may defer within
 * the pool's commit window; under {@code off} an auto-flush check. The settle
 * ships rows an {@code off} release left in an idle slot.
 */
public class SenderPoolCommitWindowTest {

    @Test
    public void testFlushAsWindowMemberOnALeaseFlushes() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(1_000, 100L);
            try (Sender s = pool.borrow()) {
                s.table("t").longColumn("v", 1L).atNow();
                s.flushAsWindowMember();
                RecordingSender d = built.get(0);
                Assert.assertEquals("a direct call on a lease is a flush", 1, d.flushCount.get());
                Assert.assertEquals("and ships no window member", 0, d.windowMemberFlushCount.get());
                s.table("t").longColumn("v", 2L).atNow();
            }
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertEquals("the release ships the one window member", 1, d.windowMemberFlushCount.get());
            Assert.assertEquals(0, d.pendingRowCount());
        }
    }

    @Test
    public void testReleaseUnderFlushOnCloseOffChecksAutoFlush() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(1_000, 100L);
            borrowAppendClose(pool, 1L);
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.autoFlushIfDueCount.get());
            Assert.assertEquals(0, d.windowMemberFlushCount.get());
            Assert.assertEquals(0, d.flushCount.get());
            Assert.assertEquals("the rows stay in the slot", 1, d.pendingRowCount());
        }
    }

    @Test
    public void testReleaseUnderFlushOnCloseShipsAWindowMember() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setFlushOnClose(true);
            pool.setCommitWindow(1_000, 100L);
            RecordingSender d = null;
            for (int i = 0; i < 3; i++) {
                borrowAppendClose(pool, i);
                d = built.get(0);
                Assert.assertEquals("one window-member flush per release", i + 1, d.windowMemberFlushCount.get());
            }
            Assert.assertEquals("a release never flushes", 0, d.flushCount.get());
            Assert.assertEquals(0, d.autoFlushIfDueCount.get());
            Assert.assertEquals(0, d.pendingRowCount());
        }
    }

    @Test
    public void testSetCommitWindowRejectsNegativeValues() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            long[][] bad = {{-1, 0}, {0, -1}};
            for (long[] b : bad) {
                try {
                    pool.setCommitWindow((int) b[0], b[1]);
                    Assert.fail("expected IllegalArgumentException for " + b[0] + ", " + b[1]);
                } catch (IllegalArgumentException expected) {
                    Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("must not be negative"));
                }
            }
            pool.setCommitWindow(0, 0L);
            pool.setCommitWindow(0, 50L);
            pool.setCommitWindow(10, 0L);
        }
    }

    @Test
    public void testSettleShipsIdleRowsWithTryFlush() {
        List<RecordingSender> built = new ArrayList<>();
        try (SenderPool pool = SenderPoolCloseSemanticsTest.newPool(1, 1, built)) {
            pool.setCommitWindow(1_000, 100L);
            borrowAppendClose(pool, 1L);
            RecordingSender d = built.get(0);
            Assert.assertEquals(1, d.pendingRowCount());
            Assert.assertEquals(1, pool.settleIdle());
            Assert.assertEquals(1, d.tryFlushCount.get());
            Assert.assertEquals(1, d.flushCount.get());
            Assert.assertEquals(0, d.pendingRowCount());
            Assert.assertEquals("nothing left to settle", 0, pool.settleIdle());
            Assert.assertEquals(1, d.tryFlushCount.get());
        }
    }

    private static void borrowAppendClose(SenderPool pool, long v) {
        try (Sender s = pool.borrow()) {
            s.table("t").longColumn("v", v).atNow();
        }
    }
}
