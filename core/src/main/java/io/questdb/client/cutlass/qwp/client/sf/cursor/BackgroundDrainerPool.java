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

import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
    // CAS gate. Single AtomicInteger packs the closed flag (sign bit) and
    // the in-flight submit count (low 31 bits):
    //   state >= 0       → open, value is the in-flight submit count
    //   state < 0        → closed bit set, low bits still track in-flight
    //                      count waiting to drain
    // submit() CASes state+1 only if state >= 0; close() CASes the CLOSED
    // bit on, then waits for state to reach exactly CLOSED_BIT (no
    // in-flight). This eliminates the "submit reads closed=false then
    // close shuts the executor down" race window: the closed-bit CAS
    // contends with the increment CAS on the same atomic, so submit
    // either lands before close (and close waits for it to finish) or
    // sees the closed bit and throws.
    private static final int CLOSED_BIT = Integer.MIN_VALUE;
    private final AtomicInteger state = new AtomicInteger();

    private final ExecutorService executor;
    private final CopyOnWriteArrayList<BackgroundDrainer> active = new CopyOnWriteArrayList<>();
    private final int maxConcurrent;

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
     * <p>
     * Reserves a "submit slot" on the {@link #state} CAS gate first; if
     * the closed bit is already set, throws immediately. Otherwise the
     * gate guarantees {@code close()} cannot shut the executor down until
     * after we release the slot, so {@code executor.submit} always lands.
     */
    public void submit(BackgroundDrainer drainer) {
        // Reserve a slot on the gate. Spin on CAS until either we win
        // (state was non-negative) or we observe the closed bit.
        for (;;) {
            int s = state.get();
            if (s < 0) {
                throw new IllegalStateException("pool closed");
            }
            if (state.compareAndSet(s, s + 1)) break;
        }
        boolean accepted = false;
        try {
            active.add(drainer);
            executor.submit(() -> {
                try {
                    drainer.run();
                } finally {
                    active.remove(drainer);
                }
            });
            accepted = true;
        } finally {
            if (!accepted) {
                active.remove(drainer);
            }
            // Release our slot. Decrement is safe regardless of the
            // closed bit's state — the bit lives in position 31 and
            // only the low 31 bits move.
            state.decrementAndGet();
        }
    }

    /**
     * Snapshot of currently-tracked drainers. May include drainers that
     * finished moments ago — the cleanup race is intentionally lax.
     * Useful for visibility / status accessors.
     */
    public ObjList<BackgroundDrainer> snapshot() {
        ObjList<BackgroundDrainer> result = new ObjList<>(active.size());
        for (BackgroundDrainer d : active) {
            result.add(d);
        }
        return result;
    }

    @Override
    public void close() {
        // Set the closed bit. CAS-loop because the in-flight count can be
        // changing under us. Subsequent submit() calls will fail their
        // CAS check (state < 0) and throw.
        for (;;) {
            int s = state.get();
            if (s < 0) return; // already closed (idempotent)
            if (state.compareAndSet(s, s | CLOSED_BIT)) break;
        }
        // Wait for in-flight submits to release their slots — i.e. for
        // state to drain to exactly CLOSED_BIT (no low bits set). This
        // ensures every submit's executor.submit has already returned
        // before we shut the executor down.
        while (state.get() != CLOSED_BIT) {
            Thread.onSpinWait();
        }
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
