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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.io.Closeable;

/**
 * High-level handle to a QuestDB deployment. Owns connection pools for both
 * ingest (via {@link Sender}) and egress (via {@link Query}). Construct once,
 * share across threads.
 * <p>
 * Steady-state allocation is zero: pooled instances are pre-allocated and
 * reused, the per-thread {@link Query} handle is cached in a {@code ThreadLocal},
 * and the {@link Completion} associated with each query is a field on that
 * cached handle.
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
 * {@link #query()} may be called concurrently from any thread; the pool
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
     */
    @Override
    void close();

    /**
     * One-shot convenience for queries with no bind parameters. Equivalent to
     * {@code query().sql(sql).handler(handler).submit()}. Returns the same
     * thread-local {@link Completion} instance that {@link #query()} would,
     * so this method is also zero-allocation at steady state.
     *
     * @param sql     the SQL text; the buffer is not retained after submit
     * @param handler the result-batch handler; invoked on the pooled query
     *                client's I/O thread
     * @return a single-flight handle for the in-flight query
     */
    Completion executeSql(CharSequence sql, QwpColumnBatchHandler handler);

    /**
     * Allocates a fresh {@link Query} handle. Unlike {@link #query()}, this
     * does NOT return the per-thread cached instance; every call allocates.
     * <p>
     * Use this when one thread needs to hold multiple in-flight queries
     * concurrently (each {@code submit()} acquires its own worker from the
     * query pool, so up to {@code queryPoolSize} concurrent queries on a
     * single thread is fine). For the common case of one query at a time,
     * prefer {@link #query()} -- it is allocation-free.
     */
    Query newQuery();

    /**
     * Opens a query builder for the calling thread. Returns the same
     * thread-local instance on every call: callers do not need to cache it
     * themselves. The returned {@code Query} is in a reset state and is not
     * thread-safe -- one in-flight query per thread.
     * <p>
     * For multiple concurrent in-flight queries from a single thread, use
     * {@link #newQuery()} instead.
     */
    Query query();

    /**
     * Releases the thread-affine {@link Sender} (if any) currently attached
     * to the calling thread back to the pool. Call this on threads borrowed
     * from pools you do not own (for example, Netty event loops) before they
     * are recycled, to avoid pinning a {@link Sender} for the lifetime of
     * a thread that no longer needs it.
     */
    void releaseSender();

    /**
     * Returns a {@link Sender} pinned to the calling thread. First call on
     * a thread takes one from the pool and pins it; subsequent calls on the
     * same thread return the same instance. The pin is released by
     * {@link #releaseSender()} or by {@link #close()} on this handle.
     * <p>
     * Use this for long-lived, dedicated producer threads where borrow/return
     * overhead would dominate. For short-lived or event-loop callers, prefer
     * {@link #borrowSender()}.
     */
    Sender sender();
}
