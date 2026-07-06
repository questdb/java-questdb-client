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

import java.io.Closeable;

/**
 * High-level handle to a QuestDB deployment. Owns connection pools for both
 * ingest (via {@link Sender}) and egress (via {@link Query}). Construct once,
 * share across threads.
 * <p>
 * Steady-state allocation is zero: pooled instances are pre-allocated and
 * reused, each borrowed {@link Query} handle is a pre-allocated front bound to
 * its pool slot, and the {@link Completion} associated with each query is a
 * field on that handle.
 * <p>
 * Configuration: one {@code ws}/{@code wss} string describes the whole cluster
 * (a single {@code addr} server list) and both the ingest and query pools
 * connect across it. Use {@link #connect(CharSequence)} for the common case, or
 * {@link #builder()} for pool sizing and the ingest callbacks. To tolerate the
 * server being down at startup, set {@code lazy_connect=true} in the config
 * (async ingest + lazy reads; reads stay enabled and connect once the server
 * is up).
 * <p>
 * Thread safety: instances are safe to share. {@link #borrowSender()} and
 * {@link #borrowQuery()} may be called concurrently from any thread; the pool
 * guarantees mutual exclusion of pooled resources.
 */
public interface QuestDB extends Closeable {

    /**
     * Builder for advanced configuration (pool sizes, acquisition timeouts,
     * ingest callbacks).
     */
    static QuestDBBuilder builder() {
        return new QuestDBBuilder();
    }

    /**
     * Connects with a single configuration string for the whole QuestDB cluster,
     * used for both ingest and egress. The schema must be {@code ws} or
     * {@code wss}: QuestDB ingests and queries over QWP (the QuestDB WebSocket
     * protocol), so one string configures both clients. List every cluster node
     * in a single {@code addr} server list and both pools connect across it.
     * <p>
     * Use {@link #builder()} for pool sizing and the ingest callbacks. To
     * tolerate the server being down at startup, set {@code lazy_connect=true}
     * in the config (async ingest + lazy reads, reads still enabled).
     *
     * @param configurationString a {@code ws}/{@code wss} config string (see
     *                            {@link Sender#fromConfig} or
     *                            {@link io.questdb.client.cutlass.qwp.client.QwpQueryClient#fromConfig})
     * @return a connected QuestDB handle
     */
    static QuestDB connect(CharSequence configurationString) {
        return builder().fromConfig(configurationString).build();
    }

    /**
     * Borrows a {@link Query} handle from the pool. The caller MUST call
     * {@link Query#close()} on the returned instance to release it back to the
     * pool (typically via try-with-resources). The handle leases one pooled
     * query client (one WebSocket + I/O thread) for the borrow's lifetime;
     * submit one or more queries on it, then close it.
     * <p>
     * Allocation: zero at steady state -- the returned instance is a
     * pre-allocated handle bound to the leased pool slot.
     * <p>
     * Blocking: blocks up to the builder's
     * {@link QuestDBBuilder#acquireTimeoutMillis(long) acquire timeout} when
     * the pool is exhausted; throws on timeout.
     * <p>
     * Concurrency: a single handle is single-flight. To run queries
     * concurrently, borrow one handle per concurrent query (up to
     * {@code query_pool_max}).
     *
     * @return a Query handle leased from the pool; release with
     * {@link Query#close()}
     * @throws QueryException if the pool is exhausted beyond the acquire
     *                        timeout, or if this handle is closed
     */
    Query borrowQuery();

    /**
     * Borrows a {@link Sender} from the pool. The caller MUST call
     * {@link Sender#close()} on the returned instance to release it back to
     * the pool. {@code close()} on a pooled Sender flushes pending rows
     * before returning to the pool; a real disconnect only happens at
     * {@link #close()} on this {@code QuestDB} handle.
     * <p>
     * Allocation: zero at steady state -- the returned instance is a
     * pre-allocated decorator backed by a pre-allocated underlying Sender.
     * <p>
     * Blocking: blocks up to the builder's
     * {@link QuestDBBuilder#acquireTimeoutMillis(long) acquire timeout} when
     * the pool is exhausted; throws on timeout.
     *
     * @return a Sender leased from the pool; release with {@link Sender#close()}
     * @throws io.questdb.client.cutlass.line.LineSenderException if the pool
     *                                                            is exhausted
     *                                                            beyond the
     *                                                            acquire
     *                                                            timeout, or
     *                                                            if this
     *                                                            handle is
     *                                                            closed
     */
    Sender borrowSender();

    /**
     * Shuts down the pools, closing every underlying {@link Sender} and
     * query client. Idempotent. Threads currently blocked in
     * {@link #borrowSender()} or {@link Query#submit()} are released with an
     * error.
     * <p>
     * Outstanding leases: a borrowed {@link Sender} is never torn down
     * underneath the thread using it. Instead, close() waits up to the
     * builder's {@link QuestDBBuilder#acquireTimeoutMillis(long) acquire
     * timeout} (hard-capped at 5 seconds, so an unbounded acquire timeout can
     * never hang shutdown) for borrowed senders to be closed; a lease closed
     * during (or
     * after) that window is flushed and its connection released safely on the
     * returning thread, while a lease that is never closed leaks its
     * connection (logged). Avoid calling close() while holding an unclosed
     * borrowed sender on the same thread: the lease cannot come home, so the
     * close stalls for the full timeout and the connection is only released
     * when that sender is eventually closed.
     */
    @Override
    void close();
}
