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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.std.QuietCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Bounded thread pool that runs {@link BackgroundDrainer} tasks. One pool
 * per foreground sender; size capped by {@code max_background_drainers}.
 * <p>
 * Each drainer gets its own thread out of the pool. Excess orphans queue
 * up — finished drainers free a slot for the next queued one. Idle pool
 * (no orphans submitted) costs one core thread; submitted-and-finished
 * drainers are GC'd after they complete.
 * <p>
 * Closing the pool requests every still-running drainer to stop and
 * waits up to a few seconds for them to exit cleanly. Drainers that
 * don't exit in time are left to finish on their own — the pool's
 * underlying executor uses daemon threads so they don't block JVM exit.
 */
public final class BackgroundDrainerPool implements QuietCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BackgroundDrainerPool.class);
    private static final long CLOSE_GRACE_MILLIS = 3_000L;

    private final ExecutorService executor;
    private final CopyOnWriteArrayList<BackgroundDrainer> active = new CopyOnWriteArrayList<>();
    private final int maxConcurrent;
    private volatile boolean closed;

    public BackgroundDrainerPool(int maxConcurrent) {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("maxConcurrent must be > 0: " + maxConcurrent);
        }
        this.maxConcurrent = maxConcurrent;
        this.executor = Executors.newFixedThreadPool(maxConcurrent, r -> {
            Thread t = new Thread(r, "qdb-orphan-drainer");
            t.setDaemon(true);
            return t;
        });
    }

    public int maxConcurrent() {
        return maxConcurrent;
    }

    /**
     * Submits a drainer for background execution. The pool tracks it so
     * {@link #close} can request a stop. Safe to call any number of
     * times; excess submissions queue inside the pool's executor.
     */
    public void submit(BackgroundDrainer drainer) {
        if (closed) {
            throw new IllegalStateException("pool closed");
        }
        active.add(drainer);
        executor.submit(() -> {
            try {
                drainer.run();
            } finally {
                active.remove(drainer);
            }
        });
    }

    /**
     * Snapshot of currently-tracked drainers. May include drainers that
     * finished moments ago — the cleanup race is intentionally lax.
     * Useful for visibility / status accessors.
     */
    public java.util.List<BackgroundDrainer> snapshot() {
        return new java.util.ArrayList<>(active);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        for (BackgroundDrainer d : active) {
            d.requestStop();
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(CLOSE_GRACE_MILLIS, TimeUnit.MILLISECONDS)) {
                LOG.warn("drainer pool did not finish in {}ms; "
                                + "remaining drainers will exit on their own",
                        CLOSE_GRACE_MILLIS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
