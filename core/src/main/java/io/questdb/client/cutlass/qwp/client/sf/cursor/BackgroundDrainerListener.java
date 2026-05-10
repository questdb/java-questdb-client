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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

/**
 * Async observer hook for {@link BackgroundDrainer} events that user code
 * may want to surface but that are not terminal for the drain. The drainer
 * stays asymmetric to the foreground sender: a foreground sender that
 * lands on an endpoint without {@code X-QWP-Durable-Ack} support fails
 * loudly because the producer is actively pushing data; the drainer
 * tolerates the same condition transiently because the source data is
 * already pinned (durable-ack-mode trims only on STATUS_DURABLE_ACK
 * frames, so the engine watermark cannot advance) and rolling upgrades
 * routinely produce brief windows where no current endpoint advertises
 * durable ack.
 * <p>
 * Listener methods run on the drainer's own thread. Implementations must
 * not block — hand off to a queue or metrics sink and return.
 */
public interface BackgroundDrainerListener {

    /**
     * Fired when the drainer has retried past its budget on consecutive
     * durable-ack-unavailable failures. The drainer drops a {@code .failed}
     * sentinel and exits. Treat as cluster-wide misconfiguration and
     * surface to operators.
     *
     * @param slotPath      slot the drainer was processing
     * @param totalAttempts how many connect attempts hit the same failure
     * @param elapsedMillis wall time spent on this failure mode
     */
    void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis);

    /**
     * Fired when {@code clientFactory.reconnect()} threw
     * {@code QwpDurableAckMismatchException} — i.e. every endpoint in the
     * current sweep failed to advertise durable ack. The drainer will
     * back off and retry; this callback is purely observability. Source
     * data stays pinned regardless because the loop runs in
     * {@code durableAckMode=true} and only trims on STATUS_DURABLE_ACK.
     *
     * @param slotPath      slot the drainer is processing
     * @param attemptNumber 1-based count of consecutive durable-ack-unavailable
     *                      failures for this drainer
     */
    void onDurableAckUnavailable(String slotPath, int attemptNumber);
}
