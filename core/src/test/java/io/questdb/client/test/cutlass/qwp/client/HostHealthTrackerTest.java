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

import io.questdb.client.cutlass.qwp.client.HostHealthTracker;
import io.questdb.client.cutlass.qwp.client.HostHealthTracker.State;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct unit tests for the per-host health classifier (.NET QWP ingress
 * spec §1.2). The integration tests in {@code WriteFailoverTest} cover
 * round-tripping through a sender; these isolate the picker semantics.
 */
public class HostHealthTrackerTest {

    @Test
    public void testFreshTrackerPicksInIndexOrder() {
        // All hosts start in UNKNOWN; pickNext returns the lowest index
        // first because they all share the same priority and zero
        // success-epoch (so the tiebreaker doesn't change order).
        HostHealthTracker t = new HostHealthTracker(3);
        Assert.assertEquals(0, t.pickNext());
        // pickNext alone doesn't mark attempted — only Record* does.
        Assert.assertEquals(0, t.pickNext());
        t.recordTransportError(0);
        Assert.assertEquals(1, t.pickNext());
        t.recordTransportError(1);
        Assert.assertEquals(2, t.pickNext());
        t.recordTransportError(2);
        Assert.assertEquals(-1, t.pickNext());
        Assert.assertTrue(t.isRoundExhausted());
    }

    @Test
    public void testPickNextPriority_HealthyBeforeUnknownBeforeRejects() {
        HostHealthTracker t = new HostHealthTracker(5);
        t.recordRoleReject(0, false);  // TopologyReject
        t.recordTransportError(1);
        t.recordRoleReject(2, true);   // TransientReject
        t.recordSuccess(3);            // Healthy
        // host 4 stays UNKNOWN

        // Round 1 ended (every except 4 marked attempted). Begin a fresh
        // round preserving classifications. Now host 3 (Healthy) wins,
        // then host 4 (Unknown), then 2 (Transient), 1 (TransportError),
        // 0 (Topology).
        t.beginRound(false);
        Assert.assertEquals(3, t.pickNext());
        t.recordTransportError(3); // simulate failure to advance
        Assert.assertEquals(4, t.pickNext());
        t.recordTransportError(4);
        Assert.assertEquals(2, t.pickNext());
        t.recordTransportError(2);
        Assert.assertEquals(1, t.pickNext());
        t.recordTransportError(1);
        Assert.assertEquals(0, t.pickNext());
    }

    @Test
    public void testBeginRoundForgetKeepsStickyHealthy() {
        HostHealthTracker t = new HostHealthTracker(3);
        t.recordTransportError(0);
        t.recordSuccess(1);            // sticky-Healthy
        t.recordRoleReject(2, false);  // TopologyReject

        t.beginRound(true);

        // Sticky-Healthy preserved
        Assert.assertEquals(State.HEALTHY, t.stateOf(1));
        // Others reset to UNKNOWN
        Assert.assertEquals(State.UNKNOWN, t.stateOf(0));
        Assert.assertEquals(State.UNKNOWN, t.stateOf(2));
        // Healthy picked first
        Assert.assertEquals(1, t.pickNext());
    }

    @Test
    public void testRecordMidStreamFailureOnlyAffectsHealthy() {
        HostHealthTracker t = new HostHealthTracker(3);
        t.recordSuccess(0);
        t.recordTransportError(1);
        t.recordRoleReject(2, false);  // TopologyReject

        // Mid-stream failure on the previously-Healthy host demotes it.
        t.recordMidStreamFailure(0);
        Assert.assertEquals(State.TRANSPORT_ERROR, t.stateOf(0));

        // No-op on hosts not currently HEALTHY — preserves prior
        // classification so a single hiccup doesn't erase a topology
        // reject that's still informative.
        t.recordMidStreamFailure(2);
        Assert.assertEquals(State.TOPOLOGY_REJECT, t.stateOf(2));
        t.recordMidStreamFailure(1);
        Assert.assertEquals(State.TRANSPORT_ERROR, t.stateOf(1));
    }

    @Test
    public void testIsRoundExhausted_TransitionsAtRoundBoundary() {
        HostHealthTracker t = new HostHealthTracker(2);
        Assert.assertFalse(t.isRoundExhausted());
        t.recordTransportError(0);
        Assert.assertFalse(t.isRoundExhausted());
        t.recordTransportError(1);
        Assert.assertTrue(t.isRoundExhausted());
        t.beginRound(false);
        Assert.assertFalse(t.isRoundExhausted());
    }

    @Test
    public void testTransientRejectBeatsTopologyReject() {
        // 421 + PRIMARY_CATCHUP gets retry priority over 421 + REPLICA
        // because catchup nodes typically become writable within seconds.
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordRoleReject(0, false); // REPLICA
        t.recordRoleReject(1, true);  // PRIMARY_CATCHUP
        t.beginRound(false);
        Assert.assertEquals(1, t.pickNext());
    }

    @Test
    public void testSuccessEpochTiebreaksAcrossSimultaneousHealthy() {
        // Two hosts that are both Healthy: the one whose recordSuccess
        // came LAST should be picked first. Sticky-most-recent.
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordSuccess(0);
        t.recordSuccess(1); // newer epoch
        t.beginRound(false);
        Assert.assertEquals(1, t.pickNext());
    }
}
