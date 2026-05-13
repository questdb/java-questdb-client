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

package io.questdb.client;

/**
 * User-supplied callback invoked when the asynchronous SF send loop observes a
 * server acknowledgement that advances the per-sender ack watermark. Registered
 * on {@code QwpWebSocketSender} via {@code setProgressHandler(...)} or on the
 * builder via {@code LineSenderBuilder.progressHandler(...)}.
 *
 * <h2>WARNING -- settled is not durable</h2>
 * {@code ackedFsn} is a <em>settled</em> watermark, not a <em>durable</em> one.
 * A {@code DROP_AND_CONTINUE} server rejection (the default policy for the
 * {@code SCHEMA_MISMATCH} and {@code WRITE_ERROR} categories) advances
 * {@code ackedFsn} past the dropped FSN range exactly as a successful OK
 * does -- the loop cannot leave a dropped FSN unsettled without leaking
 * storage and stalling the wire. This handler therefore CANNOT distinguish
 * a batch that the server committed to the WAL from one that the server
 * discarded.
 *
 * <p>Code that gates a downstream side effect on {@code onAcked} without
 * also tracking {@link SenderErrorHandler} drops will treat dropped batches
 * as durable. The result is silent data loss: rows discarded by the server
 * are marked "saved" by the user's outbox, locks released, source records
 * deleted, downstream confirmations emitted -- for data that no longer
 * exists on the server.
 *
 * <p>Required pattern when durability matters: register a
 * {@link SenderErrorHandler}, record the {@code [fromFsn, toFsn]} range of
 * every error whose {@code getAppliedPolicy()} is
 * {@link SenderError.Policy#DROP_AND_CONTINUE}, and exclude those FSNs from
 * the "durable" set you derive from the watermark.
 *
 * <h2>Watermark semantics</h2>
 * The handler fires only when the watermark <em>advances</em>:
 * <ul>
 *   <li>delivered values are strictly increasing,</li>
 *   <li>the handler may be called many times during the lifetime of a sender,</li>
 *   <li>a single call may skip multiple FSNs if the server batches several
 *       frames into one OK frame.</li>
 * </ul>
 *
 * <p>Callers polling for "is everything up to FSN N settled?" should compare
 * {@code ackedFsn} against their target and act once the inequality is
 * satisfied, not assume one call per sent batch. The "settled" wording is
 * deliberate -- see the WARNING above for the distinction from "durable".
 *
 * <h2>Threading</h2>
 * Implementations are invoked on a dedicated daemon dispatcher thread, never on
 * the I/O thread or the producer thread. Slow handlers cannot stall publishing.
 * If the bounded inbox fills, surplus notifications are dropped -- visible via
 * {@code QwpWebSocketSender.getDroppedProgressNotifications()}. Drops are
 * tolerable because the next delivered call carries an equal-or-higher FSN, so
 * watchers comparing against a target threshold catch up automatically.
 *
 * <h2>Exceptions</h2>
 * Any {@link Throwable} thrown by the handler is caught and logged by the
 * dispatcher. The dispatcher and the sender continue running.
 *
 * <h2>What this callback is for</h2>
 * Marking application state settled (see the WARNING for the distinction from
 * durable), releasing producer-side latches, fan-out to journals tagged with
 * {@code (fsn, domainContext)} pairs returned by {@code flushAndGetSequence()}.
 * For an "is this batch rejected" question on the producer thread, see
 * {@link SenderErrorHandler} and
 * {@link io.questdb.client.cutlass.line.LineSenderException}.
 *
 * @see SenderErrorHandler
 */
@FunctionalInterface
public interface SenderProgressHandler {
    /**
     * Called when the settled watermark advances. Strictly monotonic:
     * {@code ackedFsn} on call N+1 is greater than on call N. "Settled" covers
     * both successful OK frames and {@code DROP_AND_CONTINUE} rejections --
     * see the class javadoc WARNING before treating this as a durability
     * signal.
     */
    void onAcked(long ackedFsn);
}
