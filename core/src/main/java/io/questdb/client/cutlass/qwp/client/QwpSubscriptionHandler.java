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
 * Callback contract delivered to one subscription on a {@link QwpSubscribeClient}.
 * <p>
 * {@link #onAck} fires once when the server accepts the subscription;
 * {@link #onBatch} fires for every committed transaction the server forwards;
 * {@link #onEnd} fires exactly once when the subscription terminates (client
 * cancel, table drop, schema change, server shutdown, error).
 * <p>
 * Implementations run on the client's IO thread (whichever thread is calling
 * {@link QwpSubscribeClient#poll(int)} or
 * {@link QwpSubscribeClient#pollFor(QwpSubscription, int)}). Avoid blocking
 * inside callbacks for longer than the connection's flow-control budget can
 * absorb; the server will pause delivery once credit is exhausted but cannot
 * speed up a slow consumer.
 */
public interface QwpSubscriptionHandler {

    /**
     * Server has accepted the subscription. {@code startTxn} is the
     * sequencer txn at which streaming begins. Subsequent
     * {@link #onBatch} callbacks carry txn values at or above this.
     *
     * @param startTxn the resolved start txn (always positive)
     * @param schemaId connection-scoped schema id assigned to the table
     */
    void onAck(long startTxn, int schemaId);

    /**
     * Server is forwarding the row delta committed at {@code txn}.
     * <p>
     * The callback is invoked synchronously while the receive thread holds
     * the wire bytes pinned in the receive buffer; the body pointer
     * ({@code bodyAddr}) is only valid for the duration of the call.
     * Implementations that need to retain row data must copy the bytes out.
     *
     * @param txn       the sequencer txn this batch corresponds to
     * @param batchSeq  per-subscription monotonic batch sequence number
     * @param bodyAddr  native pointer to the delta-section + table-block
     *                  bytes that follow the per-batch prelude
     * @param bodyLen   length of {@code bodyAddr}'s region in bytes
     */
    void onBatch(long txn, long batchSeq, long bodyAddr, int bodyLen);

    /**
     * Server has terminated the subscription. {@code reason} is one of the
     * {@code QwpSubscribeMsgKind.SUB_END_*} byte codes. After this callback
     * the subscription is no longer active and no further {@link #onBatch}
     * deliveries will arrive.
     *
     * @param reason  reason code from
     *                {@link QwpSubscribeMsgKind#reasonName(byte)}
     * @param lastTxn last txn the server delivered to this subscription
     *                (zero if no batches were delivered)
     * @param message human-readable detail; may be empty
     */
    void onEnd(byte reason, long lastTxn, String message);
}
