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
     * durable-ack capability-gap failures. The drainer drops a
     * {@code .failed} sentinel and exits. Treat as cluster-wide
     * misconfiguration and surface to operators.
     *
     * @param slotPath      slot the drainer was processing
     * @param totalAttempts capability-gap attempts in the final episode;
     *                      transient sweeps (role reject, transport) are
     *                      never counted
     * @param elapsedMillis wall time of the final capability-gap episode,
     *                      anchored at its first capability-gap error
     */
    void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis);

    /**
     * Fired when a connect sweep hit a genuine durable-ack capability gap
     * ({@code QwpDurableAckMismatchException}: an endpoint upgrades but does
     * not advertise durable ack). The drainer will back off and retry within
     * its settle budget; this callback is purely observability. Source data
     * stays pinned regardless because the loop runs in
     * {@code durableAckMode=true} and only trims on STATUS_DURABLE_ACK.
     * A transient all-replica failover window (role reject) never fires this
     * callback — it is surfaced through {@link #onPrimaryUnavailable}.
     *
     * @param slotPath      slot the drainer is processing
     * @param attemptNumber 1-based attempt number within the current
     *                      capability-gap EPISODE. The counter restarts when
     *                      an intervening role reject resets the episode —
     *                      topology churn grants the next gap a fresh settle
     *                      budget, which is correct behavior — and with the
     *                      streams separated the reset's cause is visible as
     *                      an {@link #onPrimaryUnavailable} delivery between
     *                      the two episodes
     */
    void onDurableAckUnavailable(String slotPath, int attemptNumber);

    /**
     * Fired when a connect sweep found every reachable endpoint to be a
     * REPLICA — a transient all-replica failover window (role reject). A
     * replica is promotable and a primary will reappear, so the drainer
     * retries indefinitely under Invariant B: this condition NEVER escalates
     * and is never followed by {@link #onDurableAckPersistentFailure}. Runs
     * on the drainer thread; implementations must not block. The no-op
     * default keeps every implementor of the released 1.3.4 contract source-
     * and binary-compatible.
     *
     * @param slotPath      slot the drainer is processing
     * @param attemptNumber 1-based running role-reject count within the
     *                      current connect loop (resets across connect
     *                      re-entries)
     */
    default void onPrimaryUnavailable(String slotPath, int attemptNumber) {
    }
}
