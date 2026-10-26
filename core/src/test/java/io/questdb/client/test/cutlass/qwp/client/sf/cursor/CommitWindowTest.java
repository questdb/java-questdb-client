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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow;
import org.junit.Test;

import static io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow.CLOSED_BY_AGE;
import static io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow.CLOSED_BY_DEMAND;
import static io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow.CLOSED_BY_FRAME;
import static io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow.CLOSED_BY_ROWS;
import static io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow.DEFERRED;
import static io.questdb.client.cutlass.qwp.client.sf.cursor.CommitWindow.NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitWindowTest {
    private static final long MS = 1_000_000L;
    private final CommitWindow cw = new CommitWindow();

    @Test
    public void testAgeBoundIsCheckedWhenAMemberIsSent() {
        cw.configure(1000, 50);
        assertEquals(DEFERRED, cw.onMember(0, 1, 0));
        assertEquals(DEFERRED, cw.onMember(1, 1, 49 * MS));
        assertEquals(CLOSED_BY_AGE, cw.onMember(2, 1, 50 * MS));
    }

    @Test
    public void testCloseRequestAlreadyCoveredIsIgnored() {
        cw.configure(1000, 0);
        assertEquals(DEFERRED, cw.onMember(0, 1, 0));
        assertEquals(CLOSED_BY_FRAME, cw.onUnmarked(1, false));
        cw.requestCloseThrough(1);
        assertEquals(DEFERRED, cw.onMember(2, 1, 0));
        assertEquals(NONE, cw.pollQuiet(0, 2));
    }

    @Test
    public void testCloseRequestCommitsThroughTheRequestedFsnOnly() {
        cw.configure(1000, 0);
        assertEquals(DEFERRED, cw.onMember(0, 1, 0));
        assertEquals(DEFERRED, cw.onMember(1, 1, 0));
        assertEquals(DEFERRED, cw.onMember(2, 1, 0));
        cw.requestCloseThrough(4);
        assertEquals(NONE, cw.pollQuiet(0, 2));
        assertEquals(DEFERRED, cw.onMember(3, 1, 0));
        assertEquals(CLOSED_BY_DEMAND, cw.onMember(4, 1, 0));
        assertEquals(DEFERRED, cw.onMember(5, 1, 0));
    }

    @Test
    public void testCloseRequestOnAQuietGroup() {
        cw.configure(1000, 0);
        assertEquals(DEFERRED, cw.onMember(0, 1, 0));
        assertEquals(DEFERRED, cw.onMember(1, 1, 0));
        cw.requestCloseThrough(1);
        assertEquals(CLOSED_BY_DEMAND, cw.pollQuiet(0, 1));
        assertFalse(cw.isGroupOpen());
        assertEquals(NONE, cw.pollQuiet(0, 1));
    }

    @Test
    public void testConfigureRejectsNegativeBounds() {
        try {
            cw.configure(-1, 0);
            fail();
        } catch (IllegalArgumentException expected) {
        }
        try {
            cw.configure(0, -1);
            fail();
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testInactiveUntilABoundIsSet() {
        assertFalse(cw.isActive());
        cw.configure(0, 0);
        assertFalse(cw.isActive());
        cw.configure(0, 50);
        assertTrue(cw.isActive());
        cw.configure(10, 0);
        assertTrue(cw.isActive());
    }

    @Test
    public void testMemberAloneAtTheRowBoundCommits() {
        cw.configure(2, 0);
        assertEquals(CLOSED_BY_ROWS, cw.onMember(0, 5, 0));
        assertFalse(cw.isGroupOpen());
    }

    @Test
    public void testQuietGroupClosesByAge() {
        cw.configure(1000, 50);
        cw.onMember(0, 1, 0);
        assertEquals(NONE, cw.pollQuiet(49 * MS, 0));
        assertEquals(CLOSED_BY_AGE, cw.pollQuiet(50 * MS, 0));
        assertFalse(cw.isGroupOpen());
        assertEquals(NONE, cw.pollQuiet(500 * MS, 0));
    }

    @Test
    public void testRequestCloseThroughKeepsTheHighestFsn() {
        cw.configure(1000, 0);
        cw.requestCloseThrough(5);
        cw.requestCloseThrough(3);
        for (int i = 0; i < 5; i++) {
            assertEquals(DEFERRED, cw.onMember(i, 1, 0));
        }
        assertEquals(CLOSED_BY_DEMAND, cw.onMember(5, 1, 0));
    }

    @Test
    public void testResetForConnectionDropsTheGroupAndCoveredRequests() {
        cw.configure(1000, 0);
        cw.onMember(0, 1, 0);
        cw.onMember(1, 1, 0);
        cw.requestCloseThrough(1);
        cw.resetForConnection(1);
        assertFalse(cw.isGroupOpen());
        assertEquals(NONE, cw.pollQuiet(0, 1));
        assertEquals(DEFERRED, cw.onMember(2, 1, 0));
    }

    @Test
    public void testRowBoundCommitsTheCrossingMember() {
        cw.configure(3, 0);
        assertEquals(DEFERRED, cw.onMember(0, 1, 0));
        assertTrue(cw.isGroupOpen());
        assertEquals(0, cw.groupStartFsn());
        assertEquals(DEFERRED, cw.onMember(1, 1, 0));
        assertEquals(CLOSED_BY_ROWS, cw.onMember(2, 1, 0));
        assertFalse(cw.isGroupOpen());
        assertEquals(-1, cw.groupStartFsn());
        assertEquals(DEFERRED, cw.onMember(3, 1, 0));
        assertEquals(3, cw.groupStartFsn());
    }

    @Test
    public void testStoredDeferredFrameHoldsTheQuietClose() {
        cw.configure(1000, 50);
        assertEquals(DEFERRED, cw.onMember(0, 1, 0));
        assertEquals(NONE, cw.onUnmarked(1, true));
        assertTrue(cw.isGroupOpen());
        assertEquals(NONE, cw.pollQuiet(100 * MS, 1));
        assertEquals(CLOSED_BY_AGE, cw.onMember(2, 1, 100 * MS));
    }

    @Test
    public void testUnmarkedDeferredFrameOpensNoGroup() {
        cw.configure(1000, 50);
        assertEquals(NONE, cw.onUnmarked(0, true));
        assertFalse(cw.isGroupOpen());
        assertEquals(NONE, cw.pollQuiet(100 * MS, 0));
    }

    @Test
    public void testZeroRowBoundNeverClosesByRows() {
        cw.configure(0, 50);
        for (int i = 0; i < 3; i++) {
            assertEquals(DEFERRED, cw.onMember(i, 1_000_000, 0));
        }
    }
}
