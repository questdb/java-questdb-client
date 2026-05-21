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

import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Per-thread, reusable builder for one query. Obtained from
 * {@link QuestDB#query()}: every call on the same thread returns the same
 * instance, reset to empty.
 * <p>
 * Lifecycle: configure with {@link #sql}, optional {@link #binds}, and
 * {@link #handler}, then call {@link #submit()} to obtain a {@link Completion}.
 * After the Completion terminates, the next {@code QuestDB.query()} call on
 * the same thread returns this same instance with its state reset.
 * <p>
 * Thread safety: not thread-safe. One in-flight query per thread.
 */
public interface Query {

    /** Discards the current configuration without submitting. */
    void abandon();

    /**
     * Sets the bind-value setter, invoked by the pooled query client when the
     * QUERY_REQUEST frame is being prepared. Pass a reusable
     * {@link QwpBindSetter} instance (or a stateless lambda hoisted to a
     * field) to keep submission zero-allocation.
     */
    Query binds(QwpBindSetter binds);

    /**
     * Sets the result-batch handler. The handler is invoked on the pooled
     * query client's I/O thread; if it touches caller state, it is
     * responsible for its own synchronization.
     */
    Query handler(QwpColumnBatchHandler handler);

    /**
     * Sets the SQL text. The buffer is not retained past {@link #submit()}.
     */
    Query sql(CharSequence sql);

    /**
     * Submits the query for execution. Returns the {@link Completion} field
     * cached on this instance; never allocates. Blocks up to the builder's
     * configured acquire timeout if the query pool is exhausted.
     *
     * @return the single-flight Completion bound to this Query instance
     */
    Completion submit();
}
