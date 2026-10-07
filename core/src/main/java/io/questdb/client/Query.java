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
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * A query handle leased from the {@link QuestDB} pool via
 * {@link QuestDB#borrowQuery()}. The handle holds one pooled query client (one
 * WebSocket + I/O thread) for the lifetime of the borrow; the caller MUST
 * {@link #close()} it to release the client back to the pool (typically via
 * try-with-resources).
 * <p>
 * Allocation: the per-submit path is allocation-free -- the heavy query state
 * is pre-allocated on the leased pool slot and reused, and {@link #submit()}
 * returns this same handle as its {@link Completion}. {@code borrowQuery()}
 * creates one small lease handle per borrow (often scalar-replaced by the JIT
 * when used with try-with-resources).
 * <p>
 * Lifecycle: configure with {@link #sql}, optional {@link #binds} and
 * {@link #timeout}, and {@link #handler}, then call {@link #submit()} to obtain
 * a {@link Completion} and {@code await()} it before the next {@link #submit()}.
 * <p>
 * Thread safety: not thread-safe and single-flight -- one in-flight query per
 * handle. To run queries concurrently, borrow one handle per concurrent query.
 */
public interface Query extends Closeable {

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
     * Releases the leased pooled query client back to the pool. The caller
     * MUST call this (typically via try-with-resources). A real disconnect only
     * happens at {@link QuestDB#close()}. Idempotent.
     * <p>
     * If a submit is still in flight (the caller never awaited, or its
     * {@code await(timeout)} expired), {@code close()} cancels it and waits for
     * the terminal event so the client is idle before it returns to the pool.
     * That wait is bounded by {@code query_close_timeout_ms} (default 5000ms,
     * see {@link QuestDBBuilder#queryCloseTimeoutMillis(long)}) and is
     * interruptible -- interrupting the calling thread aborts it. If the query
     * does not drain within the budget, the client is discarded rather than
     * returned (its connection may carry late frames for the abandoned query),
     * and the pool grows a fresh one on the next borrow. {@code close()}
     * therefore never blocks the caller unbounded, even when the server is slow
     * to honor the cancel.
     * <p>
     * Must NOT be called from a result handler: handlers run on the worker
     * thread, so {@code close()} would block waiting for a terminal event that
     * only that thread can deliver. Calling it there throws
     * {@link IllegalStateException}. Use {@link #cancel()} (non-blocking) to
     * stop a query from inside a handler.
     */
    @Override
    void close();

    /**
     * Sets the result-batch handler. The handler is invoked on the worker
     * (dispatch) thread that drives {@code execute()} -- it consumes the pooled
     * query client's I/O-thread event queue inline, it does NOT run on the I/O
     * thread. If it touches caller state, it is responsible for its own
     * synchronization. A handler must not call the blocking {@link #close()} or
     * {@link Completion#await()} (they would self-deadlock on the worker
     * thread); use {@link #cancel()} to stop from inside a handler.
     */
    Query handler(QwpColumnBatchHandler handler);

    /**
     * Sets the SQL text. The buffer is not retained past {@link #submit()}.
     */
    Query sql(CharSequence sql);

    /**
     * Submits the query for execution on the leased client. Returns this handle
     * as its own {@link Completion}; never allocates. The handle is
     * single-flight: {@code await()} the returned Completion before the next
     * {@code submit()}.
     *
     * @return the single-flight Completion bound to this Query handle
     */
    Completion submit();

    /**
     * Returns the {@link QwpServerInfo} the leased pooled query client cached on
     * its most recent successful bind -- the decoded {@code SERVER_INFO} frame:
     * the live role, zone, cluster/node ids and capabilities of the endpoint
     * this handle is currently bound to.
     * <p>
     * Read-only and non-perturbing: it returns the client's cached snapshot
     * without issuing a query, so it does NOT drive the per-{@code submit()}
     * failover reconnect loop. After the bound endpoint dies it keeps reporting
     * the pre-failover snapshot until the next {@link #submit()} rebinds. This
     * is what lets a caller observe "which endpoint am I on" without itself
     * causing a failover walk.
     *
     * @return the cached server info for the bound endpoint, or {@code null}
     * before the first successful bind
     */
    QwpServerInfo serverInfo();

    /**
     * Sets the query timeout for subsequent {@link #submit()} calls on this
     * handle, overriding the {@code query_timeout_ms} default from the
     * connection string; {@code 0} runs queries without a timeout. It stays in
     * effect until changed, and every borrow starts from the configured
     * default.
     * <p>
     * The timeout is measured from {@code submit()} and bounds the whole query:
     * server execution, any failover, and the time the handler spends in its
     * callbacks (it is checked between result batches, so a handler blocked
     * inside {@code onBatch} is not interrupted). When it expires, the query is
     * stopped -- by the server when it supports per-query timeouts, otherwise by
     * a client-side cancel -- no further result batch reaches the handler, the
     * handler's {@code onError} receives
     * {@link QwpConstants#STATUS_QUERY_TIMEOUT}, and {@link Completion#await()}
     * throws a {@link QueryException} whose {@link QueryException#isTimeout()}
     * is {@code true}. The pooled connection stays open and authenticated, and
     * serves the next query.
     * <p>
     * Should the server not end the query within {@code query_close_timeout_ms}
     * (see {@link QuestDBBuilder#queryCloseTimeoutMillis(long)}) of the timeout,
     * the caller is released with the timeout anyway while the connection
     * finishes draining the aborted query; only a connection that stays silent
     * for that long once more is closed and replaced.
     * <p>
     * Unlike {@link Completion#await(long, TimeUnit)}, which only bounds how
     * long the caller waits, the query timeout stops the query.
     *
     * @param timeout the timeout, {@code 0} for none; a positive value below
     *                one millisecond counts as one millisecond
     * @param unit    the unit of {@code timeout}
     * @return this handle
     * @throws IllegalArgumentException when {@code timeout} is negative
     */
    Query timeout(long timeout, TimeUnit unit);
}
