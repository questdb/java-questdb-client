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

    @Test
    public void testZone_ConfiguredZoneUnsetCollapsesAllToSame() {
        // Per failover.md §1.1: when client zone= is unset, every host's tier
        // is SAME from construction time and recordZone() leaves it SAME
        // regardless of the server-advertised zone.
        QwpHostHealthTracker t = new QwpHostHealthTracker(3, null, false);
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(1));
        t.recordZone(0, "eu-west-1a");
        t.recordZone(1, "us-east-2");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(1));
    }

    @Test
    public void testZone_NullOrEmptyZoneIdIsNoop() {
        // recordZone with null/empty preserves the existing tier (failover.md
        // §2.1) so a missing X-QuestDB-Zone on a 421 reject does not erase a
        // tier set by an earlier successful SERVER_INFO observation.
        QwpHostHealthTracker t = new QwpHostHealthTracker(2, "eu-west-1a", false);
        t.recordZone(0, "eu-west-1a");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));

        t.recordZone(0, null);
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        t.recordZone(0, "");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        t.recordZone(0, "   ");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
    }

    @Test
    public void testZone_PersistsAcrossBeginRound() {
        // Per failover.md §2.1, zone_tier is NOT cleared by BeginRound -- once
        // observed it persists across rounds so observations don't have to be
        // re-issued on every walk.
        QwpHostHealthTracker t = new QwpHostHealthTracker(2, "eu-west-1a", false);
        t.recordZone(0, "eu-west-1a");
        t.recordZone(1, "us-east-2");
        t.recordSuccess(0);
        t.recordTransportError(1);

        t.beginRound(true);
        // States may have been reset by the forget-classifications round, but
        // zone tiers must persist.
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.OTHER, t.getZoneTier(1));
    }

    @Test
    public void testZone_PrefersSameZoneWhenStateTied() {
        // Two UNKNOWN hosts: same-zone (1) wins over cross-zone (0). State
        // ties break by zone tier per failover.md §2.
        QwpHostHealthTracker t = new QwpHostHealthTracker(2, "eu-west-1a", false);
        t.recordZone(0, "us-east-2");
        t.recordZone(1, "eu-west-1a");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.OTHER, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(1));
        Assert.assertEquals(1, t.pickNext());
    }

    @Test
    public void testZone_StateOutranksZoneTier() {
        // Per failover.md §2.1: state outranks zone, so a known-good cross-zone
        // host is picked before an untried local host (the alternative is to
        // gamble on the unknown host versus accept a working connection
        // already in hand).
        QwpHostHealthTracker t = new QwpHostHealthTracker(2, "eu-west-1a", false);
        // Host 0: HEALTHY, OTHER (cross-zone). Host 1: UNKNOWN, SAME (local).
        t.recordZone(0, "us-east-2");
        t.recordSuccess(0);
        t.recordZone(1, "eu-west-1a");
        t.beginRound(false);   // clear attempted, keep classifications
        Assert.assertEquals(0, t.pickNext());   // HEALTHY > UNKNOWN regardless of zone
    }

    @Test
    public void testZone_StickyHealthyPreservesOnlySameZone() {
        // Per failover.md §2.2: cross-zone HEALTHY entries are reset to
        // UNKNOWN on beginRound(true). Only the most recent same-zone HEALTHY
        // entry is preserved; otherwise a sticky pin in another zone would
        // defeat same-zone preference.
        QwpHostHealthTracker t = new QwpHostHealthTracker(3, "eu-west-1a", false);
        t.recordZone(0, "us-east-2");
        t.recordSuccess(0);            // HEALTHY, OTHER -- most recent
        t.recordZone(1, "eu-west-1a");
        t.recordSuccess(1);            // HEALTHY, SAME
        t.recordZone(2, "us-east-2");
        t.recordSuccess(2);            // HEALTHY, OTHER -- even more recent than 1

        t.beginRound(true);

        // Despite host 2 being the most recent overall HEALTHY, it's
        // cross-zone and must be reset; host 1 (most recent same-zone HEALTHY)
        // is the sticky-Healthy.
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(0));
        Assert.assertEquals(QwpHostHealthTracker.HostState.HEALTHY, t.getState(1));
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(2));
        Assert.assertEquals(1, t.pickNext());
    }

    @Test
    public void testZone_TargetPrimaryCollapsesAllToSame() {
        // Per failover.md §2: target=primary collapses every host's zone tier
        // to SAME because the master must be followed across zones.
        QwpHostHealthTracker t = new QwpHostHealthTracker(2, "eu-west-1a", true);
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(1));
        // Even after recording a mismatched zone, tier stays SAME.
        t.recordZone(0, "us-east-2");
        t.recordZone(1, "ap-south-1");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(1));
    }

    @Test
    public void testZone_ZoneIdComparisonIsCaseInsensitive() {
        // failover.md §1.1: zone identifiers are opaque, case-insensitive.
        QwpHostHealthTracker t = new QwpHostHealthTracker(2, "EU-West-1a", false);
        t.recordZone(0, "eu-WEST-1a");
        t.recordZone(1, "us-east-2");
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.SAME, t.getZoneTier(0));
        Assert.assertEquals(QwpHostHealthTracker.ZoneTier.OTHER, t.getZoneTier(1));
    }

    @Test
    public void testRoundCursor_FullSweepInLivePriorityOrderThenExhausted() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        t.recordSuccess(2);          // HEALTHY -> first
        t.recordTransportError(0);   // TRANSPORT_ERROR -> last
        // host 1 stays UNKNOWN -> middle
        QwpHostHealthTracker.RoundCursor c = t.newRoundCursor();
        Assert.assertEquals(2, c.next());
        Assert.assertEquals(1, c.next());
        Assert.assertEquals(0, c.next());
        Assert.assertEquals("cursor must be exhausted after a full sweep", -1, c.next());
        Assert.assertEquals("cursor exhaustion is sticky", -1, c.next());
    }

    @Test
    public void testRoundCursor_ReRanksRemainingHostsOnLiveStateChange() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(3);
        QwpHostHealthTracker.RoundCursor c = t.newRoundCursor();
        Assert.assertEquals(0, c.next()); // all UNKNOWN -> idx order
        // Another walker observes host 2 healthy mid-sweep: it must now
        // outrank the still-UNKNOWN host 1 for THIS cursor's next pick.
        t.recordSuccess(2, false);
        Assert.assertEquals(2, c.next());
        Assert.assertEquals(1, c.next());
        Assert.assertEquals(-1, c.next());
    }

    @Test
    public void testRoundCursor_DoesNotConsumeOrDependOnSharedRound() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        // Exhaust the SHARED round completely...
        t.recordTransportError(0);
        t.recordTransportError(1);
        Assert.assertTrue(t.isRoundExhausted());
        Assert.assertEquals(-1, t.pickNext());
        // ...a fresh cursor still gets a FULL sweep (its attempted set is
        // private), ordered by the live states.
        QwpHostHealthTracker.RoundCursor c = t.newRoundCursor();
        Assert.assertEquals(0, c.next());
        Assert.assertEquals(1, c.next());
        Assert.assertEquals(-1, c.next());
        // ...and the cursor's sweep left the shared round untouched.
        Assert.assertTrue(t.isRoundExhausted());
        Assert.assertEquals(-1, t.pickNext());
    }

    @Test
    public void testRoundCursors_AreIndependentNoEndpointStealing() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        QwpHostHealthTracker.RoundCursor a = t.newRoundCursor();
        QwpHostHealthTracker.RoundCursor b = t.newRoundCursor();
        // Interleaved: each cursor must sweep EVERY host exactly once;
        // a's claims must not consume b's sweep or vice versa.
        Assert.assertEquals(0, a.next());
        Assert.assertEquals(0, b.next());
        Assert.assertEquals(1, a.next());
        Assert.assertEquals(1, b.next());
        Assert.assertEquals(-1, a.next());
        Assert.assertEquals(-1, b.next());
    }

    @Test
    public void testHealthOnlyRecords_UpdateStateButNeverTheSharedRoundBit() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(1);
        // Health-only variants (markRoundAttempted=false): state flips...
        t.recordTransportError(0, false);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TRANSPORT_ERROR, t.getState(0));
        t.recordRoleReject(0, true, false);
        Assert.assertEquals(QwpHostHealthTracker.HostState.TRANSIENT_REJECT, t.getState(0));
        t.recordSuccess(0, false);
        Assert.assertEquals(QwpHostHealthTracker.HostState.HEALTHY, t.getState(0));
        // ...but the shared round never sees an attempt: the host is still
        // pickable and the round is not exhausted. This is what keeps a
        // background drainer's sweep invisible to the foreground's round.
        Assert.assertFalse(t.isRoundExhausted());
        Assert.assertEquals(0, t.pickNext());
    }

    @Test
    public void testHealthOnlySuccess_StillFeedsStickyHealthyRecency() {
        QwpHostHealthTracker t = new QwpHostHealthTracker(2);
        // Foreground succeeded on 0 (round-marking), a background walker
        // later succeeded on 1 (health-only): the background success is the
        // most RECENT and must win the sticky-Healthy pin across
        // beginRound(true).
        t.recordSuccess(0);
        t.recordSuccess(1, false);
        t.beginRound(true);
        Assert.assertEquals("most recent success (health-only or not) is sticky",
                QwpHostHealthTracker.HostState.HEALTHY, t.getState(1));
        Assert.assertEquals(QwpHostHealthTracker.HostState.UNKNOWN, t.getState(0));
        Assert.assertEquals(1, t.pickNext());
    }
}
