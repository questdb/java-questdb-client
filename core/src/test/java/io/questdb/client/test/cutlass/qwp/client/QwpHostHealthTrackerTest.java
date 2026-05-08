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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpHostHealthTracker;
import org.junit.Assert;
import org.junit.Test;

public class QwpHostHealthTrackerTest {

    @Test
    public void testAllUnknown_PicksByIdxOrder() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        Assert.assertEquals(0, t.pickNext());
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(0));
    }

    @Test
    public void testBeginRoundFalseClearsAttemptedKeepsClassifications() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        t.recordTransportError(0);
        t.recordSuccess(1);
        Assert.assertTrue(t.isRoundExhausted());

        t.beginRound(false);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TRANSPORT_ERROR, t.getState(0));
        Assert.assertEquals(QwpHostHealthTracker.HostState.HEALTHY, t.getState(1));
        Assert.assertFalse(t.isRoundExhausted());
        Assert.assertEquals(1, t.pickNext());
    }

    @Test
    public void testBeginRoundTrue_PreservesHealthyResetsRest() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        t.recordSuccess(0);
        t.recordTransportError(1);
        t.recordRoleReject(2, false);

        t.beginRound(true);

        Assert.assertEquals(QwpHostHealthTracker.HostState.HEALTHY, t.getState(0));
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(1));
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(2));
        Assert.assertEquals(0, t.pickNext());
    }

    @Test
    public void testCount() {
        Assert.assertEquals(4, new QwpHostHealthTracker(4).count());
    }

    @Test
    public void testCtorRejectsZeroHosts() {
        try {
            new QwpHostHealthTracker(0);
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testIsRoundExhausted() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        Assert.assertFalse(t.isRoundExhausted());
        t.recordSuccess(0);
        Assert.assertFalse(t.isRoundExhausted());
        t.recordTransportError(1);
        Assert.assertTrue(t.isRoundExhausted());
    }

    @Test
    public void testMidStreamFailure_HealthyDemoted() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        t.recordSuccess(0);
        t.recordMidStreamFailure(0);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TRANSPORT_ERROR, t.getState(0));
    }

    @Test
    public void testMidStreamFailure_DoesNotTouchAttempted() {
        // Spec failover.md §2.1: recordMidStreamFailure mutates classification
        // only; the round's attempted bit belongs to the round lifecycle.
        // After the demote the host stays pickable in the current round under
        // TRANSPORT_ERROR priority.
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        t.recordSuccess(0);                 // state=HEALTHY, attempted=true
        t.beginRound(false);                // attempted cleared, classifications preserved
        Assert.assertEquals(QwpHostHealthTracker.HostState.HEALTHY, t.getState(0));
        Assert.assertFalse(t.isRoundExhausted());

        t.recordMidStreamFailure(0);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TRANSPORT_ERROR, t.getState(0));
        Assert.assertFalse(t.isRoundExhausted());

        // Host 1 (UNKNOWN) outranks host 0 (TRANSPORT_ERROR), so pickNext
        // returns 1 first. Host 0 is still pickable afterwards because
        // recordMidStreamFailure left attempted[0]=false.
        Assert.assertEquals(1, t.pickNext());
        t.recordTransportError(1);
        Assert.assertFalse(t.isRoundExhausted());
        Assert.assertEquals(0, t.pickNext());
        t.recordTransportError(0);
        Assert.assertTrue(t.isRoundExhausted());
    }

    @Test
    public void testMidStreamFailure_NonHealthyUnchanged() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        t.recordRoleReject(0, false);
        t.recordMidStreamFailure(0);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TOPOLOGY_REJECT, t.getState(0));
    }

    @Test
    public void testPickNextReturnsMinusOneOnExhaustion() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(1);
        Assert.assertEquals(0, t.pickNext());
        t.recordSuccess(0);
        Assert.assertEquals(-1, t.pickNext());
    }

    @Test
    public void testPriorityOrder_HealthyBeforeUnknown() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        t.recordSuccess(2);
        t.beginRound(false);
        Assert.assertEquals(2, t.pickNext());
    }

    @Test
    public void testPriorityOrder_TopologyRejectLast() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        t.recordRoleReject(0, false);     // TOPOLOGY_REJECT
        t.recordTransportError(1);        // TRANSPORT_ERROR
        // 2 stays UNKNOWN
        t.beginRound(false);
        Assert.assertEquals(2, t.pickNext());     // UNKNOWN first
        t.recordSuccess(2);
        Assert.assertEquals(1, t.pickNext());     // TRANSPORT_ERROR before TOPOLOGY_REJECT
        t.recordTransportError(1);
        Assert.assertEquals(0, t.pickNext());     // TOPOLOGY_REJECT last
    }

    @Test
    public void testRecordRoleReject_Transient_TransientCategory() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        t.recordRoleReject(0, true);
        t.recordRoleReject(1, false);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TRANSIENT_REJECT, t.getState(0));
        Assert.assertEquals(QwpHostHealthTracker.HostState.TOPOLOGY_REJECT, t.getState(1));
    }

    @Test
    public void testStickyHealthyAcrossRounds() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        t.recordTransportError(0);
        t.recordSuccess(1);
        t.beginRound(true);
        Assert.assertEquals(1, t.pickNext());     // sticky-Healthy
    }

    @Test
    public void testStickyHealthyPicksMostRecentSuccess_NotHighestIndex() {
        // Two hosts simultaneously HEALTHY (consecutive recordSuccess without
        // intervening demotion). beginRound(true) must keep the most recently
        // successful entry, not the highest index.
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        t.recordSuccess(2);
        t.recordSuccess(0);
        t.beginRound(true);
        Assert.assertEquals(QwpHostHealthTracker.HostState.HEALTHY, t.getState(0));
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(2));
        Assert.assertEquals(0, t.pickNext());
    }
}
