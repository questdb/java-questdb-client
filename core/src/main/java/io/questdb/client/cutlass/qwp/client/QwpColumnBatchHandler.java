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
 * {@link #onError(byte, String)}.
 * <p>
 * The {@link QwpColumnBatch} passed to {@link #onBatch} is valid only for the
 * duration of the callback. Copy any values you need to retain.
 */
public interface QwpColumnBatchHandler {

    /**
     * Invoked for each {@code RESULT_BATCH} received.
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
     * Invoked exactly once if the query fails at any point.
     *
     * @param status  one of the QWP status codes (e.g., {@code STATUS_PARSE_ERROR})
     * @param message server-supplied error message (may be empty)
     */
    void onError(byte status, String message);

    /**
     * Invoked in place of {@link #onBatch} + {@link #onEnd} when the query was
     * a non-SELECT (DDL, INSERT, UPDATE, etc.). No batches are delivered for
     * such queries -- the server executes the statement and replies with a
     * single {@code EXEC_DONE}.
     *
     * @param opType       matches one of {@code CompiledQuery.SELECT} / {@code INSERT} /
     *                     {@code UPDATE} / {@code CREATE_TABLE} / etc. (server-side constants)
     * @param rowsAffected rows inserted / updated / deleted; 0 for pure DDL
     */
    default void onExecDone(short opType, long rowsAffected) {
        // Default no-op lets existing SELECT-only implementations stay source-
        // compatible. A handler that ever sees a non-SELECT query should
        // override this method.
    }
}
