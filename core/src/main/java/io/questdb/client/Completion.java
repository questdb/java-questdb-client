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

import java.util.concurrent.TimeUnit;

/**
 * Async handle for a submitted {@link Query}. Returned by {@link Query#submit()}.
 * <p>
 * Lifecycle: the Completion is allocated once as a field on the per-thread
 * {@link Query} instance and is reused on every {@code submit()}. It is
 * single-flight: a new {@code submit()} cannot be issued on the same {@link Query}
 * until the previous Completion resolves (via {@link #await()},
 * {@link #await(long, TimeUnit)} returning {@code true}, or an explicit
 * {@link #cancel()} that races to terminal).
 * <p>
 * Signaling: the Completion is signaled on the worker (dispatch) thread of the
 * pooled query client when the handler's terminal callback ({@code onEnd},
 * {@code onError}, or {@code onExecDone}) returns -- that callback runs inline
 * on the worker thread, not on the I/O thread. Because of this, {@code await()}
 * must never be called from inside a handler (it would self-deadlock on the
 * worker thread); use {@link #cancel()} to stop a query from inside a handler.
 */
public interface Completion {

    /**
     * Blocks until the query completes. Rethrows any server-reported failure
     * as a {@link QueryException}. Returns normally on success.
     * <p>
     * Must NOT be called from a result handler (it runs on the worker thread
     * and would self-deadlock); calling it there throws
     * {@link IllegalStateException}. Use {@link #cancel()} instead.
     *
     * @throws QueryException       if the server reported an error or
     *                              {@link #cancel()} won the race
     * @throws InterruptedException if the calling thread is interrupted
     *                              while waiting
     */
    void await() throws InterruptedException;

    /**
     * Blocks up to the given timeout. Returns {@code true} if the query
     * completed, {@code false} on timeout.
     *
     * @throws QueryException       if the server reported an error or
     *                              {@link #cancel()} won the race
     * @throws InterruptedException if the calling thread is interrupted
     *                              while waiting
     */
    boolean await(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Requests cancellation of the in-flight query. The handler's
     * {@code onError} fires with a cancellation status. No-op if the query
     * has already completed.
     */
    void cancel();

    /**
     * Returns true once the query has terminated (success, error, or cancel
     * acknowledged).
     */
    boolean isDone();
}
