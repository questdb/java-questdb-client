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

    // Package-private constructor exposing the connectHook test seam: production
    // passes null (-> the real QwpQueryClient.connect()). White-box tests in
    // io.questdb.client.test.impl reach this by reflection to inject a hook that
    // throws a non-RuntimeException Throwable from the native connect path.
    QueryClientPool(
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

    // Package-private constructor exposing both the connectHook and startHook
    // test seams: production passes null for each (-> the real
    // QwpQueryClient.connect() and QueryWorker.start()). White-box tests in
    // io.questdb.client.test.impl reach this by reflection to inject a hook that
    // throws a Throwable from either the native connect path (connectHook) or
    // the worker thread-start path (startHook).
    QueryClientPool(
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
                    return available.pollFirst();
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
                        try {
                            created.shutdown();
                        } catch (Throwable ignored) {
                            // Best-effort: an Error from teardown must not mask
                            // the closed-pool signal.
                        }
                        throw new QueryException((byte) 0, "QuestDB handle is closed");
                    }
                    all.add(created);
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

    void release(QueryWorker w) {
        long now = System.currentTimeMillis();
        w.markIdleAt(now);
        lock.lock();
        try {
            if (closed) {
                return;
            }
            available.addLast(w);
            workerReleased.signal();
        } finally {
            lock.unlock();
        }
    }

    // Package-private white-box accessor for tests: reports the current
    // in-flight creation count under the pool lock. A non-zero value after a
    // failed acquire() means the slot reservation was never released -- the
    // capacity-shrink bug this guards against.
    int inFlightCreations() {
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
