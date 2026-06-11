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
 * Callback interface for consuming a streamed QWP egress query result.
 * <p>
 * Invoked by {@link QwpQueryClient#execute(String, QwpColumnBatchHandler)}:
 * once per {@code RESULT_BATCH} frame via {@link #onBatch(QwpColumnBatch)},
 * then exactly once via either {@link #onEnd(long)} or
 * {@link #onError(byte, String)} (or {@link #onExecDone} for non-SELECT queries).
 * <p>
 * The {@link QwpColumnBatch} passed to {@link #onBatch} is valid only for the
 * duration of that call. Copy any values you need to retain.
 * <p>
 * <strong>Exception contract:</strong> if any callback method throws, the
 * exception propagates out of the {@link QwpQueryClient#execute} call on the
 * caller's thread and no further callbacks fire for that query. The connection
 * remains usable for subsequent queries.
 */
public interface QwpColumnBatchHandler {

    /**
     * Invoked for each {@code RESULT_BATCH} received. Throwing from here aborts
     * the query (see the class-level exception contract).
     *
     * @param batch column-major view over the batch; valid until {@code onBatch} returns
     */
    void onBatch(QwpColumnBatch batch);

    /**
     * Invoked exactly once after the last batch, upon successful completion of the query.
     *
     * @param totalRows server-reported total row count (0 if not tracked)
     */
    void onEnd(long totalRows);

    /**
     * Correlation-aware overload of {@link #onEnd(long)}. Receives the
     * client-assigned request id of the just-completed query so callers can
     * join completion records to earlier {@code onBatch} or log entries.
     * The default delegates to {@link #onEnd(long)} for backwards
     * compatibility; override this method when you need the request id.
     *
     * @param requestId client-assigned request id (matches {@link QwpColumnBatch#requestId()})
     * @param totalRows server-reported total row count (0 if not tracked)
     */
    default void onEnd(long requestId, long totalRows) {
        onEnd(totalRows);
    }

    /**
     * Invoked exactly once if the query fails at any point, instead of
     * {@link #onEnd} / {@link #onExecDone}.
     *
     * @param status  one of the QWP status codes (e.g., {@code STATUS_PARSE_ERROR})
     * @param message server-supplied error message (may be empty)
     */
    void onError(byte status, String message);

    /**
     * Correlation-aware overload of {@link #onError(byte, String)}. Receives
     * the client-assigned request id of the failed query so callers can join
     * the error to earlier {@code onBatch} or log entries without having to
     * stash the id from a prior callback. The default delegates to
     * {@link #onError(byte, String)} for backwards compatibility; override
     * this method when you need the request id.
     * <p>
     * {@code requestId} is {@code -1} when the failure is raised before a
     * request was assigned -- e.g. a {@code QwpQueryClient} that is already
     * closed.
     *
     * @param requestId client-assigned request id, or {@code -1} if no request was in flight
     * @param status    one of the QWP status codes (e.g., {@code STATUS_PARSE_ERROR})
     * @param message   server-supplied error message (may be empty)
     */
    default void onError(long requestId, byte status, String message) {
        onError(status, message);
    }

    /**
     * Invoked when {@link QwpQueryClient#execute} has transparently reconnected
     * to another endpoint after a transport failure and is about to re-submit
     * the query (with {@code failover=on}, the default).
     * <p>
     * {@code newNode} is the {@link QwpServerInfo} of the endpoint the client
     * just bound to.
     * <p>
     * After this callback fires, {@link #onBatch} will be invoked again with
     * {@code batch_seq} restarting at 0 on the new connection. Handlers that
     * accumulate rows across batches should discard whatever they built up
     * from the previous attempt. The default implementation is a no-op, safe
     * for handlers that don't care (for example, simple row-count aggregators
     * that are idempotent against replays) and for applications that set
     * {@code failover=off}.
     */
    default void onFailoverReset(QwpServerInfo newNode) {
    }

    /**
     * Correlation-aware overload of {@link #onFailoverReset(QwpServerInfo)}.
     * Receives the client-assigned request id of the query that is about to
     * be re-submitted. The default delegates to
     * {@link #onFailoverReset(QwpServerInfo)} for backwards compatibility.
     *
     * @param requestId client-assigned request id of the query being replayed
     * @param newNode   the {@link QwpServerInfo} of the endpoint just bound to
     */
    default void onFailoverReset(long requestId, QwpServerInfo newNode) {
        onFailoverReset(newNode);
    }

    /**
     * Invoked in place of {@link #onBatch} + {@link #onEnd} when the query was
     * a non-SELECT (DDL, INSERT, UPDATE, etc.). No batches are delivered for
     * such queries -- the server executes the statement and replies with a
     * single {@code EXEC_DONE}.
     * <p>
     * Override this method in any handler that may see non-SELECT SQL (e.g. a
     * generic query runner or a test harness that issues {@code CREATE TABLE}
     * before querying). SELECT-only handlers can rely on the default no-op and
     * will only ever see {@link #onBatch} + {@link #onEnd} / {@link #onError}.
     *
     * @param opType       matches one of {@code CompiledQuery.SELECT} / {@code INSERT} /
     *                     {@code UPDATE} / {@code CREATE_TABLE} / etc. (server-side constants)
     * @param rowsAffected rows inserted / updated / deleted; 0 for pure DDL
     */
    default void onExecDone(short opType, long rowsAffected) {
    }

    /**
     * Correlation-aware overload of {@link #onExecDone(short, long)}. Receives
     * the client-assigned request id of the completed non-SELECT query. The
     * default delegates to {@link #onExecDone(short, long)} for backwards
     * compatibility.
     *
     * @param requestId    client-assigned request id (matches {@link QwpColumnBatch#requestId()})
     * @param opType       server-side opType constant (CompiledQuery.SELECT / INSERT / UPDATE / CREATE_TABLE / etc.)
     * @param rowsAffected rows inserted / updated / deleted; 0 for pure DDL
     */
    default void onExecDone(long requestId, short opType, long rowsAffected) {
        onExecDone(opType, rowsAffected);
    }
}
