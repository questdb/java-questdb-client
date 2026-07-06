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
 * <h2>Watermark advances only on server acceptance</h2>
 * {@code ackedFsn} advances exclusively on server OK frames. A server
 * rejection NEVER advances the watermark: retriable rejections recycle the
 * connection and replay the batch, terminal rejections halt the sender with
 * the bytes preserved in the store-and-forward log. There is no drop policy.
 *
 * <p>Note that in non-durable-ack mode an OK acknowledges server-side commit,
 * not object-store durability; opt in to durable acks when replication-grade
 * durability gates downstream side effects.
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
     * {@code ackedFsn} on call N+1 is greater than on call N. The watermark
     * advances only on server OK frames -- rejections never advance it (see
     * the class javadoc for durable-ack considerations).
     */
    void onAcked(long ackedFsn);
}
