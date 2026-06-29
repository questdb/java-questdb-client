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

package io.questdb.client.impl;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Pairs one {@link QwpQueryClient} with one dedicated thread. The worker thread
 * loops, waiting for {@link #dispatch} to hand it a {@link QueryImpl}, then
 * runs {@code execute()} synchronously and releases itself back to the pool
 * when the call returns.
 * <p>
 * The pooled query client's own I/O thread continues to drive the wire; the
 * worker thread exists only to keep {@code execute()} off the application's
 * submitting thread. Handler callbacks ({@code onBatch}, {@code onEnd},
 * {@code onError}) still run on the client's I/O thread.
 */
public final class QueryWorker {

    static final long SHUTDOWN_JOIN_MILLIS = 5_000;
    private final QwpQueryClient client;
    private final long createdAtMillis;
    private final QueryClientPool pool;
    private final QueryImpl query;
    private final Condition signalCondition;
    private final ReentrantLock signalLock = new ReentrantLock();
    private final Thread thread;
    private volatile QueryImpl current;
    // Monotonic lease id. Mutated only under the QueryClientPool lock
    // (bumped once in acquire() when the worker is handed out and once in
    // release() when it is returned), so successive borrows of the same
    // worker get distinct ids. A QueryLease captures the value live during
    // its borrow; once the worker is released or re-borrowed the captured id
    // no longer matches, which is how a stale handle's close()/cancel()/
    // submit() are detected and dropped. Volatile so a stale handle on another
    // thread observes the latest value without taking the pool lock.
    private volatile long generation;
    private volatile long idleSinceMillis;
    private volatile boolean shuttingDown;

    public QueryWorker(QwpQueryClient client, QueryClientPool pool, int slotIndex) {
        this.client = client;
        this.pool = pool;
        this.query = new QueryImpl(this);
        this.signalCondition = signalLock.newCondition();
        this.thread = new Thread(this::runLoop, "questdb-query-worker-" + slotIndex);
        this.thread.setDaemon(true);
        this.createdAtMillis = System.currentTimeMillis();
        this.idleSinceMillis = this.createdAtMillis;
    }

    long createdAtMillis() {
        return createdAtMillis;
    }

    /**
     * Advances the lease generation. Called by {@link QueryClientPool} under
     * the pool lock when this worker is handed out (acquire) and when it is
     * returned (release).
     */
    void bumpGeneration() {
        generation++;
    }

    /**
     * Current lease generation. See {@link #generation} for the visibility and
     * mutation contract.
     */
    long generation() {
        return generation;
    }

    long idleSinceMillis() {
        return idleSinceMillis;
    }

    void markIdleAt(long nowMillis) {
        idleSinceMillis = nowMillis;
    }

    /**
     * Cancels the in-flight query on this worker's client. Safe to call from
     * any thread; harmless if the worker is idle.
     */
    void cancelInFlight() {
        try {
            client.cancel();
        } catch (RuntimeException ignored) {
            // cancel() is best-effort; an already-completed query is fine.
        }
    }

    /**
     * Returns the {@link QwpQueryClient} this worker drives. Exposed for
     * introspection and tests; callers must not invoke {@code execute()} on
     * it directly because that would race the worker's own dispatch loop.
     */
    public QwpQueryClient client() {
        return client;
    }

    /**
     * Resets the worker's reused {@link QueryImpl} and returns a fresh
     * {@link QueryLease} stamped with the current lease {@link #generation}.
     * Called by {@link QuestDBImpl#borrowQuery()} right after
     * {@link QueryClientPool#acquire()} hands this worker out (which bumped the
     * generation under the pool lock). The lease is a small per-borrow handle;
     * the heavy state stays on the reused {@link QueryImpl}, and the per-submit
     * path remains allocation-free.
     */
    Query lease() {
        query.resetForBorrow();
        return new QueryLease(query, generation);
    }

    /**
     * Returns this worker to the pool. Called by {@link QueryImpl#close(long)}
     * when the borrowed lease is released; the captured lease {@code gen} lets
     * the pool reject a stale release whose worker has already been re-borrowed.
     */
    void releaseToPool(long gen) {
        pool.release(this, gen);
    }

    void shutdown() {
        shuttingDown = true;
        signalLock.lock();
        try {
            signalCondition.signalAll();
        } finally {
            signalLock.unlock();
        }
        try {
            // If a query is in flight on this worker, ask the client to abort so
            // execute() returns promptly and the thread can exit before join
            // times out. cancel() is documented as thread-safe and is a no-op
            // when idle.
            try {
                client.cancel();
            } catch (Throwable ignored) {
                // Best-effort. Catch Throwable, not just RuntimeException: an
                // Error (e.g. an -ea AssertionError) thrown here must not skip
                // the join() below or the close() in finally, which would leak
                // the worker thread and the client's native socket/buffers.
            }
            try {
                thread.join(SHUTDOWN_JOIN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } finally {
            // close() must run even if cancel()/join() threw, otherwise the
            // client's native buffer pool and socket leak for the lifetime of
            // the process. Catch Throwable so shutdown() itself never propagates
            // a teardown Error to its callers.
            try {
                client.close();
            } catch (Throwable ignored) {
            }
        }
    }

    void start() {
        thread.start();
    }

    /**
     * Hands a configured {@link QueryImpl} to this worker for execution. The
     * worker is held by an open {@link io.questdb.client.Query} lease (see
     * {@link #lease()}), so a lease may dispatch repeatedly (single-flight)
     * until it is closed.
     */
    void dispatch(QueryImpl q) {
        signalLock.lock();
        try {
            if (shuttingDown) {
                // shutdown() has already flipped the flag and signalled the
                // worker thread, so the run loop will not pick this up. Signal
                // the caller directly so its Completion.await() does not hang.
                q.signalUnexpected(new QueryException((byte) 0, "QuestDB handle is closed"));
                return;
            }
            current = q;
            signalCondition.signal();
        } finally {
            signalLock.unlock();
        }
    }

    private void runLoop() {
        while (!shuttingDown) {
            QueryImpl q;
            signalLock.lock();
            try {
                while (current == null && !shuttingDown) {
                    signalCondition.awaitUninterruptibly();
                }
                if (shuttingDown) {
                    // shutdown() raced an in-flight dispatch(): current was set
                    // by a caller but the loop never got to runOn(). Signal the
                    // caller so its Completion.await() does not hang.
                    QueryImpl stranded = current;
                    current = null;
                    if (stranded != null) {
                        stranded.signalUnexpected(
                                new QueryException((byte) 0, "QuestDB handle is closed"));
                    }
                    return;
                }
                q = current;
            } finally {
                signalLock.unlock();
            }
            try {
                q.runOn(client);
            } catch (Throwable t) {
                q.signalUnexpected(t);
            } finally {
                current = null;
            }
        }
    }
}
