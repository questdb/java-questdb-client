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

package io.questdb.client.cutlass.qwp.client;

/**
 * Tracks when the server last confirmed the schema snapshot a sender caches
 * for one table. The sender converts values against that snapshot, so after a
 * column type change it converts to the old type until it learns of the change.
 * <p>
 * The server reports a change in its reply to a frame that writes the table,
 * and the client applies that feedback before it advances the OK watermark.
 * Once the watermark covers such a frame, the cached snapshot was current when
 * the frame was flushed. A DESCRIBE confirms the snapshot as of the moment
 * before its request. Both times precede the server's read of the schema, so
 * the computed age never understates the real one.
 * <p>
 * One frame at a time waits for confirmation, the oldest uncovered one, since
 * its reply arrives first. Frames published while it waits are not recorded.
 * <p>
 * A re-check the server did not answer defers the next one by the age limit.
 * <p>
 * A column type change can also show as rows the snapshot rejects. While the
 * table holds pending rows, its batch keeps the snapshot, and rows that fail
 * reach no {@code at()} that could flush the batch, so no reply reports the
 * change. A run of {@link #REJECTIONS_BEFORE_RECHECK} rejected rows with no
 * accepted one in between asks for a re-check before the next row; see
 * {@link #takeRecheck}.
 * <p>
 * The producer thread owns this object; it is not thread-safe.
 */
public final class QwpSchemaFreshness {
    /**
     * Consecutive rows the snapshot rejects before the next row re-checks it.
     * More than one, so a single bad value in a flowing batch costs no lookup.
     */
    public static final int REJECTIONS_BEFORE_RECHECK = 2;
    private static final long NO_FRAME = -1L;
    private long confirmedNanos;
    private long failedCheckNanos;
    private long frameFsn = NO_FRAME;
    private long frameNanos;
    private boolean hasConfirmation;
    private boolean hasFailedCheck;
    private boolean hasRechecked;
    private boolean isConsulted;
    private long recheckNanos;
    // Rows the snapshot rejected since the table last accepted one.
    private int rejectedRows;

    /**
     * Records that the server confirmed the snapshot as of {@code nanos}. An
     * earlier confirmation never replaces a later one.
     */
    public void confirm(long nanos) {
        if (!hasConfirmation || nanos - confirmedNanos > 0) {
            confirmedNanos = nanos;
            hasConfirmation = true;
        }
    }

    /**
     * Records that a re-check ending at {@code nowNanos} got no answer, which
     * defers the next re-check by the age limit.
     */
    public void deferCheck(long nowNanos) {
        failedCheckNanos = nowNanos;
        hasFailedCheck = true;
    }

    /**
     * Whether the current batch of the table consulted the cached schema, so
     * that the frame carrying it can confirm the cache.
     */
    public boolean isConsulted() {
        return isConsulted;
    }

    /**
     * Reports whether the snapshot needs a re-check at {@code nowNanos}: no
     * confirmation and no failed re-check lies within {@code maxAgeNanos}. A
     * waiting frame that {@code okFsn} covers confirms the snapshot first.
     */
    public boolean isExpired(long nowNanos, long maxAgeNanos, long okFsn) {
        confirmCoveredFrame(okFsn);
        return (!hasConfirmation || nowNanos - confirmedNanos > maxAgeNanos)
                && (!hasFailedCheck || nowNanos - failedCheckNanos > maxAgeNanos);
    }

    /**
     * Reports whether a run of rejected rows asks for a re-check, before the
     * interval of {@link #takeRecheck} applies.
     */
    public boolean isRecheckDue() {
        return rejectedRows >= REJECTIONS_BEFORE_RECHECK;
    }

    /**
     * Marks that the current batch of the table consults the cached schema.
     */
    public void markConsulted() {
        isConsulted = true;
    }

    /**
     * Records that a frame up to {@code fsn} carried the consulted batch,
     * flushed at {@code flushNanos}, and clears the consulted mark. The frame
     * waits for confirmation unless an earlier frame still does.
     */
    public void onFramePublished(long fsn, long flushNanos, long okFsn) {
        isConsulted = false;
        confirmCoveredFrame(okFsn);
        if (frameFsn == NO_FRAME) {
            frameFsn = fsn;
            frameNanos = flushNanos;
        }
    }

    /**
     * Records a row the table accepted, which ends a run of rejected rows.
     */
    public void onRowAccepted() {
        rejectedRows = 0;
    }

    /**
     * Records a row the snapshot rejected, for a column it knows.
     */
    public void onRowRejected() {
        if (rejectedRows < REJECTIONS_BEFORE_RECHECK) {
            rejectedRows++;
        }
    }

    /**
     * Reports whether to re-check the snapshot at {@code nowNanos}: a run of
     * rejected rows is due, no re-check started within {@code minIntervalNanos},
     * and no re-check that got no answer ended within it; see
     * {@link #deferCheck}. Taking the re-check ends the run; a run the interval
     * defers stays due.
     */
    public boolean takeRecheck(long nowNanos, long minIntervalNanos) {
        if (!isRecheckDue()
                || (hasRechecked && nowNanos - recheckNanos <= minIntervalNanos)
                || (hasFailedCheck && nowNanos - failedCheckNanos <= minIntervalNanos)) {
            return false;
        }
        rejectedRows = 0;
        recheckNanos = nowNanos;
        hasRechecked = true;
        return true;
    }

    private void confirmCoveredFrame(long okFsn) {
        if (frameFsn != NO_FRAME && okFsn >= frameFsn) {
            confirm(frameNanos);
            frameFsn = NO_FRAME;
        }
    }
}
