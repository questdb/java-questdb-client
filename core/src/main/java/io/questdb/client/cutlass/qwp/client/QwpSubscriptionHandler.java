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
     * The {@link QwpColumnBatch} passed in is a column-major decoded view of
     * the txn's rows: {@code batch.getRowCount()},
     * {@code batch.getColumnCount()}, {@code batch.getString(col, row)},
     * {@code batch.getDoubleValue(col, row)}, {@code batch.getSymbol(col,
     * row)}, etc. The view is valid only for the duration of this callback -
     * its column pointers reference the receive-buffer bytes the IO thread
     * is about to overwrite. Copy any values you need to retain.
     * <p>
     * Multi-chunk delivery: if a single txn's row count exceeds the
     * per-batch row cap (default 16,384, configurable via
     * {@link QwpSubscribeClient.Builder#withMaxBatchRows(int)}) the server
     * splits it across multiple {@code SUBSCRIBE_BATCH} frames sharing the
     * same {@code txn} value but with monotonically-increasing
     * {@code batch.batchSeq()}. A consumer that wants per-txn atomicity can
     * buffer until {@code txn} changes; consumers that treat each chunk as
     * an independent slab of rows see no observable difference from the
     * single-chunk case.
     *
     * @param txn   the sequencer txn this batch corresponds to (or partial
     *              chunk of)
     * @param batch column-major view of the rows in this chunk; valid until
     *              this method returns
     */
    void onBatch(long txn, QwpColumnBatch batch);

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
