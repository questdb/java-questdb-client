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
public final class QueryClientPool {

    private final long acquireTimeoutMillis;
    private final ArrayList<QueryWorker> all;
    private final ArrayDeque<QueryWorker> available;
    private final String configurationString;
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
        if (minSize < 0 || maxSize < 1 || minSize > maxSize) {
            throw new IllegalArgumentException("invalid pool sizing: min=" + minSize + ", max=" + maxSize);
        }
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
        try {
            for (int i = 0; i < minSize; i++) {
                QueryWorker w = createUnlocked();
                w.start();
                all.add(w);
                available.add(w);
                built++;
            }
        } catch (RuntimeException e) {
            for (int i = 0; i < built; i++) {
                try {
                    all.get(i).shutdown();
                } catch (Throwable ignored) {
                    // Best-effort cleanup: an Error (e.g. -ea AssertionError)
                    // from one worker's shutdown must not strand the remaining
                    // pre-warmed workers nor mask the original failure below.
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
                    QueryWorker created;
                    try {
                        created = createUnlocked();
                        created.start();
                    } catch (RuntimeException e) {
                        lock.lock();
                        inFlightCreations--;
                        workerReleased.signal();
                        lock.unlock();
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

    private QueryWorker createUnlocked() {
        QwpQueryClient client = QwpQueryClient.fromConfig(configurationString);
        try {
            client.connect();
        } catch (RuntimeException e) {
            // connect() may throw after QwpQueryClient.fromConfig() has already
            // allocated native scratch (the QwpBindValues NativeBufferWriter is
            // field-initialised). Close the half-built client so its allocations
            // are released, otherwise every connect failure during pool growth
            // leaks NATIVE_DEFAULT bytes.
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
