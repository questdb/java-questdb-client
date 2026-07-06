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

import io.questdb.client.QueryException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Elastic pool of {@link QueryWorker}s. Each worker pairs one
 * {@link QwpQueryClient} (one WebSocket, one I/O thread) with one daemon
 * dispatch thread. The pool keeps at least {@code minSize} workers warm and
 * grows up to {@code maxSize} on demand; idle and over-age workers are
 * reaped by the housekeeper.
 * <p>
 * Worker creation involves a WebSocket upgrade (slow), so it happens
 * outside the pool lock; an {@code inFlightCreations} counter keeps the
 * cap check honest under concurrent acquires.
 */
public final class QueryClientPool implements AutoCloseable {

    // Default upper bound, in milliseconds, on how long Query.close() waits for
    // an in-flight query to drain (after issuing a cancel) before discarding the
    // worker. Mirrors the ingest side's close_flush_timeout_millis default so a
    // close() can never block the caller unbounded. Tunable per pool via
    // closeQueryTimeoutMillis(long).
    static final long DEFAULT_CLOSE_QUERY_TIMEOUT_MILLIS = 5_000;
    private final long acquireTimeoutMillis;
    private final ArrayList<QueryWorker> all;
    private final ArrayDeque<QueryWorker> available;
    private final String configurationString;
    // Test seam. Production connects via QwpQueryClient.connect(); white-box
    // tests in io.questdb.client.test.impl reach the package-private constructor
    // by reflection to inject a hook that throws a non-RuntimeException
    // Throwable (e.g. an -ea AssertionError) from the native connect path,
    // exercising the Error-safe cleanup on the prewarm and acquire paths.
    private final Consumer<QwpQueryClient> connectHook;
    // Test seam. Production starts the worker's dispatch thread via
    // QueryWorker.start(); white-box tests in io.questdb.client.test.impl reach
    // the package-private constructor by reflection to inject a hook that throws
    // a Throwable (modelling OutOfMemoryError "unable to create native thread")
    // *after* createUnlocked() has returned a fully connected client, exercising
    // the Error-safe client teardown on the prewarm and acquire paths -- the
    // start()-throws path connectHook cannot reach.
    private final Consumer<QueryWorker> startHook;
    private final long idleTimeoutMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxLifetimeMillis;
    private final int maxSize;
    private final int minSize;
    private final AtomicInteger nextSlotIndex = new AtomicInteger();
    private final Condition workerReleased;
    private volatile boolean closed;
    // Upper bound on the Query.close() drain wait; see
    // DEFAULT_CLOSE_QUERY_TIMEOUT_MILLIS. Volatile because QuestDBImpl sets it
    // once at build time on a different thread than the borrowers that read it.
    private volatile long closeQueryTimeoutMillis = DEFAULT_CLOSE_QUERY_TIMEOUT_MILLIS;
    private int inFlightCreations;

    public QueryClientPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, null);
    }

    // Constructor exposing the connectHook seam. Production (QuestDBImpl) passes
    // null -> the real QwpQueryClient.connect(); white-box tests pass a hook that
    // throws a non-RuntimeException Throwable from the native connect path. This
    // is the construction path QuestDBImpl uses, so it is a real (public) ctor,
    // not test-only.
    public QueryClientPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            Consumer<QwpQueryClient> connectHook
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, connectHook, null);
    }

    // Constructor exposing both the connectHook and startHook seams. Production
    // reaches it via the overload above (both null -> the real
    // QwpQueryClient.connect() and QueryWorker.start()); white-box tests pass a
    // hook that throws a Throwable from either the native connect path
    // (connectHook) or the worker thread-start path (startHook).
    public QueryClientPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            Consumer<QwpQueryClient> connectHook,
            Consumer<QueryWorker> startHook
    ) {
        if (minSize < 0 || maxSize < 1 || minSize > maxSize) {
            throw new IllegalArgumentException("invalid pool sizing: min=" + minSize + ", max=" + maxSize);
        }
        this.connectHook = connectHook != null ? connectHook : QwpQueryClient::connect;
        this.startHook = startHook != null ? startHook : QueryWorker::start;
        this.configurationString = configurationString;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.maxLifetimeMillis = maxLifetimeMillis;
        this.all = new ArrayList<>(maxSize);
        this.available = new ArrayDeque<>(maxSize);
        this.workerReleased = lock.newCondition();
        int built = 0;
        // Tracks a worker built by createUnlocked() but not yet added to `all`:
        // it is fully connected (socket + native scratch + I/O thread) the
        // instant createUnlocked() returns, yet the following start() can still
        // throw (e.g. OutOfMemoryError creating the dispatch thread). Without
        // this handle the cleanup loop below -- which only walks `all` -- would
        // never close it, stranding exactly the I/O thread and native
        // allocations this catch exists to reclaim.
        QueryWorker pending = null;
        try {
            for (int i = 0; i < minSize; i++) {
                pending = createUnlocked();
                this.startHook.accept(pending);
                all.add(pending);
                available.add(pending);
                pending = null;
                built++;
            }
        } catch (Throwable e) {
            // Catch Throwable, not just RuntimeException: createUnlocked()/start()
            // run a heavy native build path that can throw an Error -- e.g. an
            // -ea AssertionError or OutOfMemoryError -- mid-prewarm. If we only
            // caught RuntimeException the Error would propagate without running
            // the cleanup below, stranding every already-built worker's I/O
            // thread and native allocations.
            for (int i = 0; i < built; i++) {
                try {
                    all.get(i).shutdown();
                } catch (Throwable ignored) {
                    // Best-effort cleanup: an Error (e.g. -ea AssertionError)
                    // from one worker's shutdown must not strand the remaining
                    // pre-warmed workers nor mask the original failure below.
                }
            }
            // Close the worker that was built but never made it into `all`
            // (start() threw after createUnlocked() returned a live client).
            // createUnlocked() already self-cleans when connect() throws, so
            // pending is only non-null on the start()-throws path.
            if (pending != null) {
                try {
                    pending.shutdown();
                } catch (Throwable ignored) {
                    // Best-effort: must not mask the original failure below.
                }
            }
            throw e;
        }
    }

    public QueryWorker acquire() {
        // Track remaining wait via awaitNanos's return value (canonical pattern):
        // awaitNanos consumes from the budget on each wait and reports what is
        // left; <= 0 means the budget is exhausted.
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        lock.lock();
        try {
            while (true) {
                if (closed) {
                    throw new QueryException((byte) 0, "QuestDB handle is closed");
                }
                if (!available.isEmpty()) {
                    QueryWorker w = available.pollFirst();
                    // Stamp a fresh lease id under the lock so the QueryLease
                    // about to be handed out can be distinguished from any
                    // prior, now-stale borrow of the same worker.
                    w.bumpGeneration();
                    return w;
                }
                if (all.size() + inFlightCreations < maxSize) {
                    inFlightCreations++;
                    lock.unlock();
                    QueryWorker created = null;
                    try {
                        created = createUnlocked();
                        startHook.accept(created);
                    } catch (Throwable e) {
                        // Catch Throwable, not just RuntimeException:
                        // createUnlocked()/start() run a heavy native build path
                        // that can throw an Error -- e.g. an -ea AssertionError
                        // or OutOfMemoryError. If we only caught RuntimeException
                        // the Error would propagate with inFlightCreations still
                        // incremented, permanently shrinking pool capacity until
                        // every acquire() times out. Restoring the reservation
                        // for any throwable is safe.
                        lock.lock();
                        inFlightCreations--;
                        workerReleased.signal();
                        lock.unlock();
                        // createUnlocked() returns a fully connected client
                        // (socket + native scratch + I/O thread), so if start()
                        // threw afterwards we must close it here -- nothing else
                        // references it. createUnlocked() already self-cleans
                        // when connect() throws, leaving created == null, so
                        // this only fires on the start()-throws path.
                        if (created != null) {
                            try {
                                created.shutdown();
                            } catch (Throwable ignored) {
                                // Best-effort: a teardown Error must not mask the
                                // original creation failure rethrown below.
                            }
                        }
                        throw new QueryException((byte) 0,
                                "failed to create query client: " + e.getMessage(), e);
                    }
                    lock.lock();
                    inFlightCreations--;
                    if (closed) {
                        // Pool was closed mid-creation -- tear the fresh worker
                        // down rather than leaking it, but OUTSIDE the lock:
                        // shutdown() joins the dispatch thread for up to
                        // SHUTDOWN_JOIN_MILLIS, and close()/release()/discard()/
                        // cancelIfCurrent() all contend on this lock (whose
                        // contract is "held only briefly"). The accounting above
                        // already ran under the lock, and the worker never
                        // entered `all`, so close()'s snapshot loop cannot race
                        // this teardown.
                        lock.unlock();
                        try {
                            created.shutdown();
                        } catch (Throwable ignored) {
                            // Best-effort: an Error from teardown must not mask
                            // the closed-pool signal.
                        }
                        throw new QueryException((byte) 0, "QuestDB handle is closed");
                    }
                    all.add(created);
                    // Stamp the first lease id for this freshly built worker.
                    created.bumpGeneration();
                    return created;
                }
                if (remainingNanos <= 0) {
                    throw new QueryException((byte) 0,
                            "timed out waiting for a query client from the pool after "
                                    + acquireTimeoutMillis + "ms");
                }
                try {
                    remainingNanos = workerReleased.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new QueryException((byte) 0,
                            "interrupted while waiting for a query client from the pool");
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    public void close() {
        ArrayList<QueryWorker> snapshot;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            workerReleased.signalAll();
            snapshot = new ArrayList<>(all);
        } finally {
            lock.unlock();
        }
        // Cancel any in-flight queries first so execute() returns promptly, then
        // join the worker threads and close their clients. Done outside the lock
        // so a slow join doesn't keep the pool latched.
        for (int i = 0; i < snapshot.size(); i++) {
            try {
                snapshot.get(i).shutdown();
            } catch (Throwable ignored) {
                // Best-effort: a single worker's shutdown failure (including an
                // Error such as an -ea AssertionError) must not abort the loop
                // and strand the remaining workers unclosed.
            }
        }
    }

    /**
     * Cancels the in-flight query on {@code w} only while its lease generation
     * still equals {@code gen}, holding the pool lock across both the check and
     * the wire cancel. acquire() and release() bump the generation under this
     * same lock, so once this method holds it the generation cannot change: a
     * cancel whose lease has already gone stale (the worker was released and
     * re-borrowed) is dropped instead of aborting the new borrower's query. The
     * cancel itself is non-blocking -- a volatile flag plus an AtomicLong set --
     * so the lock is held only briefly.
     */
    void cancelIfCurrent(QueryWorker w, long gen) {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            if (w.generation() != gen) {
                return;
            }
            w.cancelInFlight();
        } finally {
            lock.unlock();
        }
    }

    long closeQueryTimeoutMillis() {
        return closeQueryTimeoutMillis;
    }

    void closeQueryTimeoutMillis(long millis) {
        this.closeQueryTimeoutMillis = millis;
    }

    /**
     * Evicts a worker whose lease {@link QueryImpl#close(long)} could not drain
     * the in-flight query within {@link #closeQueryTimeoutMillis} (the cancel
     * was not honored in time, or the caller was interrupted). The worker's
     * connection is left in an unknown protocol state -- a late {@code RESULT_*}
     * frame for the abandoned query could corrupt the next borrower's stream --
     * so it must NOT return to the pool. Removes it from {@code all} (freeing
     * capacity for a fresh worker) and tears it down outside the lock via
     * {@link QueryWorker#shutdown()}, which interrupts the dispatch thread so a
     * stuck {@code execute()} returns promptly.
     * <p>
     * Bails when the pool is already closed: {@link #close()} owns the teardown
     * of every worker via its snapshot loop, so mutating {@code all} here would
     * race that iteration on a non-thread-safe {@code ArrayList}. Also bails on a
     * stale generation -- the worker was already released/discarded and possibly
     * re-borrowed, so discarding it would evict a worker a different borrower now
     * owns. Mirrors {@link SenderPool#discardBroken} on the ingest side.
     */
    void discard(QueryWorker w, long gen) {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            if (w.generation() != gen) {
                return;
            }
            // Invalidate the lease so a duplicate close()/release with the same
            // generation is dropped and the in-flight handle can no longer drive
            // this worker.
            w.bumpGeneration();
            all.remove(w);
            // Capacity freed -- a waiter in acquire() may now create a fresh
            // worker in this slot's place.
            workerReleased.signal();
        } finally {
            lock.unlock();
        }
        // Tear down outside the lock so a slow join doesn't keep the pool
        // latched. shutdown() is best-effort and idempotent.
        try {
            w.shutdown();
        } catch (Throwable ignored) {
            // Best-effort: a teardown Error (e.g. an -ea AssertionError) must
            // not propagate out of Query.close().
        }
    }

    void reapIdle() {
        if (closed) {
            return;
        }
        long now = System.currentTimeMillis();
        ArrayList<QueryWorker> toShutdown = null;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            Iterator<QueryWorker> it = available.iterator();
            while (it.hasNext() && all.size() > minSize) {
                QueryWorker w = it.next();
                boolean idleExpired = idleTimeoutMillis < Long.MAX_VALUE
                        && (now - w.idleSinceMillis()) >= idleTimeoutMillis;
                boolean overAge = maxLifetimeMillis < Long.MAX_VALUE
                        && (now - w.createdAtMillis()) >= maxLifetimeMillis;
                if (idleExpired || overAge) {
                    it.remove();
                    all.remove(w);
                    if (toShutdown == null) {
                        toShutdown = new ArrayList<>();
                    }
                    toShutdown.add(w);
                }
            }
        } finally {
            lock.unlock();
        }
        if (toShutdown != null) {
            for (int i = 0, n = toShutdown.size(); i < n; i++) {
                try {
                    toShutdown.get(i).shutdown();
                } catch (Throwable ignored) {
                    // Best-effort: a single worker's shutdown failure (including
                    // an Error such as an -ea AssertionError) must not abort the
                    // reap loop and strand the remaining reaped workers.
                }
            }
        }
    }

    void release(QueryWorker w, long gen) {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            if (w.generation() != gen) {
                // Stale release: this lease was already returned and the worker
                // has since been re-borrowed (or this is a duplicate close of an
                // already-released lease). Dropping it is what makes
                // Query.close() idempotent even under a concurrent re-borrow --
                // without this guard a double close would enqueue the worker
                // twice and hand it to two borrowers at once, corrupting the
                // whole pool. The flag a stale close() reads is no longer its
                // own lease's, so a non-validated release path could not catch
                // this; the generation captured at borrow time can.
                return;
            }
            // Invalidate the just-returned lease so a duplicate release with the
            // same generation is also dropped and the in-flight handle can no
            // longer drive this worker.
            w.bumpGeneration();
            w.markIdleAt(System.currentTimeMillis());
            assert !available.contains(w) : "worker already present in available deque on release";
            available.addLast(w);
            workerReleased.signal();
        } finally {
            lock.unlock();
        }
    }

    // White-box accessor for tests: reports the current in-flight creation count
    // under the pool lock. A non-zero value after a failed acquire() means the
    // slot reservation was never released -- the capacity-shrink bug this guards
    // against.
    @TestOnly
    public int inFlightCreations() {
        lock.lock();
        try {
            return inFlightCreations;
        } finally {
            lock.unlock();
        }
    }

    private QueryWorker createUnlocked() {
        QwpQueryClient client = QwpQueryClient.fromConfig(configurationString);
        try {
            connectHook.accept(client);
        } catch (Throwable e) {
            // Catch Throwable, not just RuntimeException: connect() runs a heavy
            // native path that can throw an Error (e.g. an -ea AssertionError or
            // OutOfMemoryError) after QwpQueryClient.fromConfig() has already
            // allocated native scratch (the QwpBindValues NativeBufferWriter is
            // field-initialised). Close the half-built client so its allocations
            // are released, otherwise an Error during pool growth leaks the
            // NATIVE_DEFAULT bytes that only this cleanup would reclaim.
            try {
                client.close();
            } catch (Throwable ignored) {
                // Best-effort: an Error from closing the half-built client must
                // not mask the original connect failure being rethrown below.
            }
            throw e;
        }
        return new QueryWorker(client, this, nextSlotIndex.getAndIncrement());
    }
}
