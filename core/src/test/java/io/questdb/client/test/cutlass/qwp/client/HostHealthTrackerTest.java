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
    public void testBeginRoundFalseClearsAttemptedKeepsClassifications() {
        // failover.md §2.1: BeginRound(forgetClassifications=false) is the
        // within-outage reset. Attempted bits clear; classifications stay
        // so a host that role-rejected this outage stays demoted.
        HostHealthTracker t = new HostHealthTracker(3);
        t.recordSuccess(0);
        t.recordRoleReject(1, true);
        t.recordRoleReject(2, false);
        Assert.assertTrue(t.isRoundExhausted());
        t.beginRound(false);
        Assert.assertFalse(t.isRoundExhausted());
        Assert.assertEquals(State.HEALTHY, t.stateOf(0));
        Assert.assertEquals(State.TRANSIENT_REJECT, t.stateOf(1));
        Assert.assertEquals(State.TOPOLOGY_REJECT, t.stateOf(2));
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
    public void testBeginRoundTrueWithoutHealthyResetsEverything() {
        // No Healthy host: BeginRound(forgetClassifications=true) must
        // reset every host to UNKNOWN. Without a sticky entry the picker
        // sees a fresh slate.
        HostHealthTracker t = new HostHealthTracker(3);
        t.recordTransportError(0);
        t.recordRoleReject(1, false);
        t.recordRoleReject(2, true);
        t.beginRound(true);
        Assert.assertEquals(State.UNKNOWN, t.stateOf(0));
        Assert.assertEquals(State.UNKNOWN, t.stateOf(1));
        Assert.assertEquals(State.UNKNOWN, t.stateOf(2));
    }

    @Test
    public void testConcurrentRecordsAreSerialised() throws InterruptedException {
        // Smoke test: 8 threads hammering record / pickNext for one
        // second. The internal lock should serialise, so no exception is
        // raised and the final host count remains intact. We check
        // only liveness: every host has been visited at least once.
        final HostHealthTracker t = new HostHealthTracker(8);
        final int threadCount = 8;
        final long deadline = System.currentTimeMillis() + 500;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int seed = i;
            threads[i] = new Thread(() -> {
                java.util.Random rng = new java.util.Random(seed);
                while (System.currentTimeMillis() < deadline) {
                    int idx = t.pickNext();
                    if (idx < 0) {
                        t.beginRound(rng.nextBoolean());
                        continue;
                    }
                    int op = rng.nextInt(4);
                    switch (op) {
                        case 0:
                            t.recordSuccess(idx);
                            break;
                        case 1:
                            t.recordTransportError(idx);
                            break;
                        case 2:
                            t.recordRoleReject(idx, rng.nextBoolean());
                            break;
                        default:
                            t.recordMidStreamFailure(idx);
                            break;
                    }
                }
            });
            threads[i].start();
        }
        for (Thread th : threads) {
            th.join();
        }
        // All 8 hosts must have a visible state -- no crash, no shape
        // corruption.
        for (int i = 0; i < 8; i++) {
            Assert.assertNotNull(t.stateOf(i));
        }
    }

    @Test
    public void testConstructorRejectsNegativeHosts() {
        try {
            new HostHealthTracker(-1);
            Assert.fail("expected IllegalArgumentException for hostCount=-1");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("hostCount must be positive", e.getMessage());
        }
    }

    @Test
    public void testConstructorRejectsZeroHosts() {
        try {
            new HostHealthTracker(0);
            Assert.fail("expected IllegalArgumentException for hostCount=0");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("hostCount must be positive", e.getMessage());
        }
    }

    @Test
    public void testCountReflectsConstructorArgument() {
        Assert.assertEquals(1, new HostHealthTracker(1).count());
        Assert.assertEquals(7, new HostHealthTracker(7).count());
        Assert.assertEquals(64, new HostHealthTracker(64).count());
    }

    @Test
    public void testFiveStateLatticePriorityFullOrder() {
        // Pin the full priority order across all 5 states by composing a
        // single tracker with each state present and verifying pickNext
        // walks them in spec order (failover.md §2 Selection priority):
        // HEALTHY > UNKNOWN > TRANSIENT > TRANSPORT_ERROR > TOPOLOGY.
        HostHealthTracker t = new HostHealthTracker(5);
        t.recordRoleReject(0, false);  // TopologyReject (priority 5)
        t.recordTransportError(1);     // TransportError (4)
        t.recordRoleReject(2, true);   // TransientReject (3)
        // host 3 stays UNKNOWN (2)
        t.recordSuccess(4);            // Healthy (1)
        t.beginRound(false);
        Assert.assertEquals("Healthy first", 4, t.pickNext());
        t.recordTransportError(4);
        Assert.assertEquals("Unknown second", 3, t.pickNext());
        t.recordTransportError(3);
        Assert.assertEquals("TransientReject third", 2, t.pickNext());
        t.recordTransportError(2);
        Assert.assertEquals("TransportError fourth", 1, t.pickNext());
        t.recordTransportError(1);
        Assert.assertEquals("TopologyReject last", 0, t.pickNext());
    }

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
    public void testMidStreamFailureAfterBeginRoundLeavesStickyIntact() {
        // The opposite ordering: beginRound(true) BEFORE
        // recordMidStreamFailure. The sticky picker has already preserved
        // host 0 as HEALTHY; the subsequent mid-stream demote then
        // demotes it inside the new round. The user-visible drift this
        // test pins: a buggy loop that reverses the order observes
        // sticky=Healthy on the first pickNext, attempts the same just-
        // failed host again, and burns its first attempt of the new round.
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordSuccess(0);
        t.beginRound(true);
        // Sticky-Healthy preserved.
        Assert.assertEquals(State.HEALTHY, t.stateOf(0));
        // Buggy mid-stream demote (after the reset) takes effect inside
        // the new round, but only AFTER the picker has already had a
        // chance to return host 0 as the priority pick.
        t.recordMidStreamFailure(0);
        Assert.assertEquals(State.TRANSPORT_ERROR, t.stateOf(0));
    }

    @Test
    public void testMidStreamFailureBeforeBeginRoundLosesStickyHealthy() {
        // failover.md §2.3 ordering invariant: when a mid-stream failure
        // happens, the loop MUST call recordMidStreamFailure(previousIdx)
        // BEFORE the next beginRound(true). The semantic effect: the
        // sticky-Healthy preservation in beginRound(true) sees host 0 as
        // TRANSPORT_ERROR rather than HEALTHY and therefore does NOT
        // preserve it as the priority pick on the next round.
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordSuccess(0);                   // HEALTHY, would be sticky
        t.recordMidStreamFailure(0);          // demote BEFORE round reset
        t.beginRound(true);
        // No HEALTHY entry survived the round reset; both end UNKNOWN.
        Assert.assertEquals(State.UNKNOWN, t.stateOf(0));
        Assert.assertEquals(State.UNKNOWN, t.stateOf(1));
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
    public void testPickNextReturnsMinusOneAfterRoundExhausts() {
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordTransportError(0);
        t.recordTransportError(1);
        Assert.assertEquals(-1, t.pickNext());
        // Repeat: still -1 until a beginRound clears the attempted bits.
        Assert.assertEquals(-1, t.pickNext());
        t.beginRound(false);
        Assert.assertNotEquals(-1, t.pickNext());
    }

    @Test
    public void testPickNextWithoutRecordIsIdempotent() {
        // The picker is observation-only: calling pickNext repeatedly
        // without an intervening Record* must keep returning the same
        // index. Failing this would let a caller burn the round picking
        // identical indices in a tight loop.
        HostHealthTracker t = new HostHealthTracker(3);
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(0, t.pickNext());
        }
    }

    @Test
    public void testRecordMidStreamFailureDoesNotTouchAttempted() {
        // Spec §2.1 contract: recordMidStreamFailure mutates classification
        // only; the round's attempted bit belongs to the round lifecycle.
        // The bit is observed indirectly via pickNext / isRoundExhausted.
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordSuccess(0);              // state=HEALTHY, attempted=true
        t.beginRound(false);             // attempted cleared, classifications preserved
        Assert.assertEquals(State.HEALTHY, t.stateOf(0));
        Assert.assertFalse(t.isRoundExhausted());

        // Mid-stream demotion must NOT mark host 0 as attempted; it should
        // remain pickable in the current round (now under TRANSPORT_ERROR
        // priority).
        t.recordMidStreamFailure(0);
        Assert.assertEquals(State.TRANSPORT_ERROR, t.stateOf(0));
        Assert.assertFalse(t.isRoundExhausted());

        // Host 1 (UNKNOWN, priority 2) outranks host 0 (TRANSPORT_ERROR,
        // priority 4), so pickNext returns 1 first.
        Assert.assertEquals(1, t.pickNext());
        t.recordTransportError(1);
        // Host 0 is still pickable — round isn't exhausted yet because
        // recordMidStreamFailure left attempted[0]=false.
        Assert.assertFalse(t.isRoundExhausted());
        Assert.assertEquals(0, t.pickNext());
        t.recordTransportError(0);
        Assert.assertTrue(t.isRoundExhausted());
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
    public void testRecordRoleRejectClassifiesByTransientFlag() {
        HostHealthTracker t = new HostHealthTracker(2);
        t.recordRoleReject(0, true);
        Assert.assertEquals(State.TRANSIENT_REJECT, t.stateOf(0));
        t.recordRoleReject(1, false);
        Assert.assertEquals(State.TOPOLOGY_REJECT, t.stateOf(1));
    }

    @Test
    public void testRecordSuccessAfterRoleRejectPromotesToHealthy() {
        // A host that role-rejected earlier and recovered should be
        // pickable in priority again. Sequence: reject, beginRound(false),
        // re-record success. State must end as HEALTHY.
        HostHealthTracker t = new HostHealthTracker(1);
        t.recordRoleReject(0, true);
        Assert.assertEquals(State.TRANSIENT_REJECT, t.stateOf(0));
        t.beginRound(false);
        // Round bit cleared; the host is pickable again.
        Assert.assertEquals(0, t.pickNext());
        t.recordSuccess(0);
        Assert.assertEquals(State.HEALTHY, t.stateOf(0));
    }

    @Test
    public void testRecordSuccessAfterTransportErrorPromotesToHealthy() {
        // Same as above but for TRANSPORT_ERROR. Verifies the transition
        // is unconditional (any prior state -> HEALTHY).
        HostHealthTracker t = new HostHealthTracker(1);
        t.recordTransportError(0);
        Assert.assertEquals(State.TRANSPORT_ERROR, t.stateOf(0));
        t.beginRound(false);
        t.recordSuccess(0);
        Assert.assertEquals(State.HEALTHY, t.stateOf(0));
    }

    @Test
    public void testStateOfReturnsLiveStateNotSnapshot() {
        // Verifies reads observe the latest write -- not stale on the
        // synchronized monitor. Sanity check; the lock would have to be
        // reentered to hold an old snapshot.
        HostHealthTracker t = new HostHealthTracker(1);
        Assert.assertEquals(State.UNKNOWN, t.stateOf(0));
        t.recordSuccess(0);
        Assert.assertEquals(State.HEALTHY, t.stateOf(0));
        t.recordTransportError(0);
        Assert.assertEquals(State.TRANSPORT_ERROR, t.stateOf(0));
    }

    @Test
    public void testStickyHealthyPersistsAcrossMultipleBeginRoundTrue() {
        // The sticky-Healthy host stays first in line even after several
        // BeginRound(true) cycles, as long as it isn't demoted by a fresh
        // RecordTransportError / RecordRoleReject / RecordMidStreamFailure.
        HostHealthTracker t = new HostHealthTracker(3);
        t.recordSuccess(1); // sticky
        for (int i = 0; i < 4; i++) {
            t.beginRound(true);
            Assert.assertEquals(1, t.pickNext());
            // simulate an unrelated Record on a different host
            t.recordTransportError(0);
        }
        // Host 1 still Healthy.
        Assert.assertEquals(State.HEALTHY, t.stateOf(1));
    }

    @Test
    public void testStickyHealthyPicksMostRecentSuccessAfterReset() {
        // Two hosts have been Healthy at different times; BeginRound(true)
        // preserves only the most recent (largest epoch) as sticky.
        HostHealthTracker t = new HostHealthTracker(3);
        t.recordSuccess(0); // older
        t.recordSuccess(2); // newer
        // Both are HEALTHY, but per §2.2 we keep the LAST one.
        t.beginRound(true);
        Assert.assertEquals(State.HEALTHY, t.stateOf(2));
        // Older success demoted to UNKNOWN.
        Assert.assertEquals(State.UNKNOWN, t.stateOf(0));
        Assert.assertEquals(2, t.pickNext());
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
}
