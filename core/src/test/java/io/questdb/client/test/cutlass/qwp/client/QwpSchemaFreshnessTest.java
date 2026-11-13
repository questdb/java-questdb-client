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

import io.questdb.client.cutlass.qwp.client.QwpSchemaFreshness;
import io.questdb.client.std.Rnd;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Pins when a cached table schema counts as confirmed by the server. The
 * property test checks the guarantee the sender relies on against a log of
 * every confirmation the test handed out: a snapshot is never reported fresh
 * without a confirmation, or a failed re-check, within the age limit.
 */
public class QwpSchemaFreshnessTest {
    private static final long MAX_AGE = 1_000;

    @Test
    public void testAcceptedRowEndsRunOfRejectedRows() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        for (int i = 1; i < QwpSchemaFreshness.REJECTIONS_BEFORE_RECHECK; i++) {
            freshness.onRowRejected();
        }
        freshness.onRowAccepted();
        for (int i = 1; i < QwpSchemaFreshness.REJECTIONS_BEFORE_RECHECK; i++) {
            freshness.onRowRejected();
        }
        Assert.assertFalse(freshness.isRecheckDue());
        Assert.assertFalse(freshness.takeRecheck(0, MAX_AGE));
        freshness.onRowRejected();
        Assert.assertTrue(freshness.isRecheckDue());
    }

    @Test
    public void testConfirmationAtAgeLimitIsFreshAndOneNanoLaterExpired() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.confirm(100);
        Assert.assertFalse(freshness.isExpired(100, MAX_AGE, -1));
        Assert.assertFalse(freshness.isExpired(100 + MAX_AGE, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(101 + MAX_AGE, MAX_AGE, -1));
    }

    @Test
    public void testConfirmationsSurviveClockWrap() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.confirm(Long.MAX_VALUE - 10);
        // System.nanoTime() may wrap; only differences carry meaning.
        Assert.assertFalse(freshness.isExpired(Long.MIN_VALUE + 10, MAX_AGE, -1));
        freshness.confirm(Long.MIN_VALUE + 5);
        Assert.assertFalse(freshness.isExpired(Long.MIN_VALUE + 5 + MAX_AGE, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(Long.MIN_VALUE + 6 + MAX_AGE, MAX_AGE, -1));
    }

    @Test
    public void testCoveredFrameConfirmsAsOfItsFlush() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.markConsulted();
        freshness.onFramePublished(5, 200, 4);
        Assert.assertTrue("an unacknowledged frame confirms nothing", freshness.isExpired(200, MAX_AGE, 4));
        Assert.assertFalse(freshness.isExpired(200 + MAX_AGE, MAX_AGE, 5));
        Assert.assertTrue(freshness.isExpired(201 + MAX_AGE, MAX_AGE, 5));
    }

    @Test
    public void testEarlierConfirmationNeverReplacesLaterOne() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.confirm(500);
        freshness.confirm(100);
        Assert.assertFalse(freshness.isExpired(500 + MAX_AGE, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(501 + MAX_AGE, MAX_AGE, -1));

        // A covered frame flushed before the latest confirmation does not age it either.
        freshness.onFramePublished(7, 50, 6);
        Assert.assertFalse(freshness.isExpired(500 + MAX_AGE, MAX_AGE, 7));
        Assert.assertTrue(freshness.isExpired(501 + MAX_AGE, MAX_AGE, 7));
    }

    @Test
    public void testFailedCheckDefersNextCheckByAgeLimit() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.deferCheck(300);
        Assert.assertFalse(freshness.isExpired(300 + MAX_AGE, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(301 + MAX_AGE, MAX_AGE, -1));

        freshness.confirm(0);
        freshness.deferCheck(5_000);
        Assert.assertFalse(freshness.isExpired(5_000 + MAX_AGE, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(5_001 + MAX_AGE, MAX_AGE, -1));
        // A confirmation after the failed check counts from its own time.
        freshness.confirm(7_000);
        Assert.assertFalse(freshness.isExpired(7_000 + MAX_AGE, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(7_001 + MAX_AGE, MAX_AGE, -1));
    }

    @Test
    public void testFrameAfterCoveredFrameWaitsInTurn() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.markConsulted();
        freshness.onFramePublished(5, 200, 4);
        freshness.markConsulted();
        // Publishing the next frame first confirms the covered one, then waits itself.
        freshness.onFramePublished(6, 300, 5);
        Assert.assertFalse(freshness.isExpired(200 + MAX_AGE, MAX_AGE, 5));
        Assert.assertTrue(freshness.isExpired(201 + MAX_AGE, MAX_AGE, 5));
        Assert.assertFalse(freshness.isExpired(300 + MAX_AGE, MAX_AGE, 6));
        Assert.assertTrue(freshness.isExpired(301 + MAX_AGE, MAX_AGE, 6));
    }

    @Test
    public void testNeverConfirmedIsExpired() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        Assert.assertTrue(freshness.isExpired(0, MAX_AGE, -1));
        Assert.assertTrue(freshness.isExpired(0, Long.MAX_VALUE, Long.MAX_VALUE));
        Assert.assertFalse(freshness.isConsulted());
    }

    @Test
    public void testOldestUnacknowledgedFrameWaits() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.markConsulted();
        freshness.onFramePublished(5, 200, 4);
        freshness.markConsulted();
        freshness.onFramePublished(6, 300, 4);
        // Frame 6 was published while frame 5 waited, so only frame 5 confirms.
        Assert.assertFalse(freshness.isExpired(200 + MAX_AGE, MAX_AGE, 6));
        Assert.assertTrue(freshness.isExpired(201 + MAX_AGE, MAX_AGE, 6));
    }

    @Test
    public void testPublishClearsConsultedMark() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        freshness.markConsulted();
        Assert.assertTrue(freshness.isConsulted());
        freshness.onFramePublished(0, 0, -1);
        Assert.assertFalse(freshness.isConsulted());
    }

    @Test
    public void testRecheckFollowsRunOfRejectedRows() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        for (int i = 1; i < QwpSchemaFreshness.REJECTIONS_BEFORE_RECHECK; i++) {
            freshness.onRowRejected();
            Assert.assertFalse(freshness.isRecheckDue());
            Assert.assertFalse(freshness.takeRecheck(0, MAX_AGE));
        }
        freshness.onRowRejected();
        Assert.assertTrue(freshness.isRecheckDue());
        Assert.assertTrue(freshness.takeRecheck(0, MAX_AGE));
        Assert.assertFalse("taking the re-check ends the run", freshness.isRecheckDue());
        Assert.assertFalse(freshness.takeRecheck(0, MAX_AGE));
    }

    @Test
    public void testRecheckRunsAtMostOncePerInterval() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        rejectRun(freshness);
        Assert.assertTrue(freshness.takeRecheck(Long.MAX_VALUE - 10, MAX_AGE));
        rejectRun(freshness);
        // Differences only, so the interval holds across a clock wrap.
        Assert.assertFalse(freshness.takeRecheck(Long.MAX_VALUE - 10 + MAX_AGE, MAX_AGE));
        Assert.assertTrue("a run deferred by the interval stays due", freshness.isRecheckDue());
        Assert.assertTrue(freshness.takeRecheck(Long.MAX_VALUE - 9 + MAX_AGE, MAX_AGE));
        Assert.assertFalse(freshness.isRecheckDue());
    }

    @Test
    public void testRecheckWaitsIntervalAfterUnansweredCheck() {
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        rejectRun(freshness);
        Assert.assertTrue(freshness.takeRecheck(0, MAX_AGE));
        // The lookup waited past the interval and got no answer.
        freshness.deferCheck(MAX_AGE + 1);
        rejectRun(freshness);
        Assert.assertFalse(freshness.takeRecheck(2 * MAX_AGE + 1, MAX_AGE));
        Assert.assertTrue(freshness.isRecheckDue());
        Assert.assertTrue(freshness.takeRecheck(2 * MAX_AGE + 2, MAX_AGE));
    }

    @Test
    public void testRunOfRejectedRowsOutlastsIntRange() {
        // A sender that keeps rejecting rows while the wire is down takes no re-check,
        // so the run grows until a connection comes back. It must still be due then.
        QwpSchemaFreshness freshness = new QwpSchemaFreshness();
        for (long i = 0; i <= Integer.MAX_VALUE; i++) {
            freshness.onRowRejected();
        }
        Assert.assertTrue(freshness.isRecheckDue());
        Assert.assertTrue(freshness.takeRecheck(0, MAX_AGE));
        Assert.assertFalse(freshness.isRecheckDue());
    }

    @Test
    public void testRandomSequencesNeverReportUnconfirmedSnapshotFresh() {
        Rnd rnd = TestUtils.generateRandom(null);
        for (int run = 0; run < 2_000; run++) {
            QwpSchemaFreshness freshness = new QwpSchemaFreshness();
            List<long[]> frames = new ArrayList<>();
            List<Long> confirmations = new ArrayList<>();
            List<Long> failedChecks = new ArrayList<>();
            long now = rnd.nextLong();
            long okFsn = -1;
            long nextFsn = 0;
            int steps = 1 + rnd.nextInt(40);
            for (int step = 0; step < steps; step++) {
                now += rnd.nextInt(700);
                switch (rnd.nextInt(5)) {
                    case 0:
                        // A DESCRIBE confirms as of a moment no later than now.
                        long describedAt = now - rnd.nextInt(50);
                        freshness.confirm(describedAt);
                        confirmations.add(describedAt);
                        break;
                    case 1:
                        long flushedAt = now - rnd.nextInt(50);
                        nextFsn += 1 + rnd.nextInt(3);
                        freshness.markConsulted();
                        freshness.onFramePublished(nextFsn, flushedAt, okFsn);
                        frames.add(new long[]{nextFsn, flushedAt});
                        break;
                    case 2:
                        okFsn = Math.min(nextFsn, okFsn + rnd.nextInt(4));
                        break;
                    case 3:
                        freshness.deferCheck(now);
                        failedChecks.add(now);
                        break;
                    default:
                        break;
                }
                if (!freshness.isExpired(now, MAX_AGE, okFsn)) {
                    Assert.assertTrue("run " + run + " step " + step + ": reported fresh without a confirmation",
                            isJustified(now, okFsn, frames, confirmations, failedChecks));
                }
            }
        }
    }

    private static boolean isJustified(
            long now,
            long okFsn,
            List<long[]> frames,
            List<Long> confirmations,
            List<Long> failedChecks
    ) {
        for (int i = 0, n = confirmations.size(); i < n; i++) {
            if (now - confirmations.get(i) <= MAX_AGE) {
                return true;
            }
        }
        for (int i = 0, n = failedChecks.size(); i < n; i++) {
            if (now - failedChecks.get(i) <= MAX_AGE) {
                return true;
            }
        }
        for (int i = 0, n = frames.size(); i < n; i++) {
            long[] frame = frames.get(i);
            if (frame[0] <= okFsn && now - frame[1] <= MAX_AGE) {
                return true;
            }
        }
        return false;
    }

    private static void rejectRun(QwpSchemaFreshness freshness) {
        for (int i = 0; i < QwpSchemaFreshness.REJECTIONS_BEFORE_RECHECK; i++) {
            freshness.onRowRejected();
        }
    }
}
