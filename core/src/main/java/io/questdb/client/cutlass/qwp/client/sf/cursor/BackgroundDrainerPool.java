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

import io.questdb.client.std.Compat;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded thread pool that runs {@link BackgroundDrainer} tasks. One pool
 * per foreground sender; size capped by {@code max_background_drainers}.
 * <p>
 * Each drainer gets its own thread out of the pool. Excess orphans queue
 * up — finished drainers free a slot for the next queued one. Idle pool
 * (no orphans submitted) costs one core thread; submitted-and-finished
 * drainers are GC'd after they complete.
 * <p>
 * Closing the pool uses a split stop policy: drainers that never started
 * draining (still inside their connect-retry loop — e.g. the cluster is
 * unreachable) are stop-signaled immediately, because no grace window can
 * help them finish; drainers actively replaying frames get a graceful
 * window to reach {@code acked >= target} before being signaled. Drainers
 * that don't exit in time (typically parked in a blocking native connect
 * that neither unpark nor interrupt cancels) are left to finish on their
 * own — the pool's underlying executor uses daemon threads so they don't
 * block JVM exit. An interrupted {@code close()} skips the graceful
 * window: every active drainer is stop-signaled immediately, then the
 * executor is shut down hard. A drainer cut down mid-drain exits STOPPED
 * with its unacked rows still in SF — re-adopted by the next orphan scan,
 * never dropped.
 */
public final class BackgroundDrainerPool implements QuietCloseable {

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
    // Time we let ACTIVELY DRAINING drainers finish naturally before
    // signaling stop. Connect-phase drainers are stop-signaled before this
    // window even starts (see close()), so during an outage — when no
    // drainer can be draining — close() does not pay this in full.
    // awaitTermination returns as soon as the last drainer exits, so this
    // only matters when something is genuinely stuck.
    private static final long GRACEFUL_DRAIN_MILLIS = 2_500L;
    private static final Logger LOG = LoggerFactory.getLogger(BackgroundDrainerPool.class);
    // After signaling stop, give drainers a brief window to unwind cleanly
    // (release slot lock, close engine) before forcing shutdownNow.
    private static final long STOP_GRACE_MILLIS = 500L;
    private final CopyOnWriteArrayList<BackgroundDrainer> active = new CopyOnWriteArrayList<>();
    private final ExecutorService executor;
    private final AtomicInteger state = new AtomicInteger();
    /**
     * Cumulative count of drainers whose runnable returned with
     * {@link BackgroundDrainer.DrainOutcome#FAILED}. Bumped once per
     * drainer in the finally block of {@link #submit}.
     */
    private final AtomicLong totalFailed = new AtomicLong();
    /**
     * Cumulative count of drainers whose runnable returned with
     * {@link BackgroundDrainer.DrainOutcome#SUCCESS}. Bumped once per
     * drainer in the finally block of {@link #submit}.
     */
    private final AtomicLong totalSucceeded = new AtomicLong();
    /**
     * Pool-level listener applied to drainers at submit time when the
     * drainer doesn't already carry one. Volatile so callers can install
     * (or rotate) the listener at any point relative to {@link #submit};
     * each submit reads it once into a local.
     */
    private volatile BackgroundDrainerListener listener;

    public BackgroundDrainerPool(int maxConcurrent) {
        if (maxConcurrent <= 0) {
            throw new IllegalArgumentException("maxConcurrent must be > 0: " + maxConcurrent);
        }
        this.executor = Executors.newFixedThreadPool(maxConcurrent, r -> {
            Thread t = new Thread(r, "qdb-orphan-drainer");
            t.setDaemon(true);
            return t;
        });
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
            Compat.onSpinWait();
        }
        // Split stop policy. The graceful window below exists so a drainer
        // that is seconds away from acked >= target is not cut down
        // mid-drain (its engine.close() would see fullyDrained=false and
        // leave the slot's .sfa files behind, defeating drain_orphans). A
        // drainer that never started draining — still inside its
        // connect-retry loop, e.g. the cluster is unreachable and
        // Invariant B retries forever — cannot possibly use that window
        // productively, so stop it NOW: it wakes from its backoff park
        // within ~50ms (STOP_CHECK_PARK_CHUNK_NANOS) and exits as STOPPED,
        // cutting close() latency during an outage from
        // GRACEFUL_DRAIN_MILLIS + STOP_GRACE_MILLIS (~3s) to roughly one
        // stop-check park chunk. ackedFsn stays -1 until the drain loop's
        // first poll, so `< 0` discriminates "never connected/started
        // draining" from "actively draining"; the moments-wide race with a
        // just-connected drainer is benign — it exits as STOPPED and the
        // slot is re-adopted by the next scan. A drainer parked inside a
        // blocking native connect ignores the stop until its background
        // connect deadline resolves; that one still burns the full grace +
        // stop windows below and is then abandoned to exit on its own
        // (daemon thread).
        for (BackgroundDrainer d : active) {
            if (d.outcome() == BackgroundDrainer.DrainOutcome.PENDING && d.getAckedFsn() < 0) {
                d.requestStop();
            }
        }
        // Reject new tasks but let actively-draining drainers finish
        // naturally.
        executor.shutdown();
        try {
            if (!executor.awaitTermination(GRACEFUL_DRAIN_MILLIS, TimeUnit.MILLISECONDS)) {
                LOG.warn("orphan drainers still running after {}ms — signaling stop",
                        GRACEFUL_DRAIN_MILLIS);
                for (BackgroundDrainer d : active) {
                    d.requestStop();
                }
                if (!executor.awaitTermination(STOP_GRACE_MILLIS, TimeUnit.MILLISECONDS)) {
                    LOG.warn("drainer pool did not exit in {}ms after stop; "
                                    + "remaining drainers will exit on their own",
                            STOP_GRACE_MILLIS);
                    executor.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Signal stop BEFORE shutdownNow's interrupts land. Actively-
            // draining drainers were spared by the split-stop above, and the
            // graceful-timeout sweep never runs on this path -- without this
            // sweep they would rely solely on the drainer-side fallback that
            // folds a pending interrupt into stopRequested
            // (BackgroundDrainer.stopRequestedOrInterrupted). This sweep is
            // the authoritative signal: it is synchronous with close() (the
            // caller observes fully stop-signaled drainers on return) and
            // covers a drainer that is between park-loop checks when the
            // interrupt lands. Cutting a mid-drain drainer short here is
            // deliberate -- an interrupted close() means "leave now", and
            // its unacked rows stay in SF for the next orphan scan (no data
            // loss), exactly like the graceful-timeout sweep.
            for (BackgroundDrainer d : active) {
                d.requestStop();
            }
            executor.shutdownNow();
        }
    }

    /**
     * Number of drainers currently tracked by the pool. Same lax-cleanup
     * race as {@link #snapshot} — a drainer that finished moments ago may
     * still count for a few ms before its executor task removes it.
     */
    public int getActiveCount() {
        return active.size();
    }

    /**
     * Cumulative count of drainers that exited with
     * {@link BackgroundDrainer.DrainOutcome#FAILED}, since pool creation.
     */
    public long getTotalFailed() {
        return totalFailed.get();
    }

    /**
     * Cumulative count of drainers that exited with
     * {@link BackgroundDrainer.DrainOutcome#SUCCESS}, since pool creation.
     */
    public long getTotalSucceeded() {
        return totalSucceeded.get();
    }

    /**
     * Plug a default {@link BackgroundDrainerListener} for drainers
     * submitted through this pool. {@code null} clears the default.
     * Drainers that already have a listener set by the caller before
     * {@link #submit} are not overridden — the pool default is a
     * fallback, not an override. Subsequent submits pick up the most
     * recently set value.
     */
    public void setListener(BackgroundDrainerListener listener) {
        this.listener = listener;
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
        // Apply the pool-level listener only if the drainer doesn't already
        // carry one. Per-drainer listeners (set by the caller before submit)
        // win — the pool listener is a fallback default, not an override.
        BackgroundDrainerListener poolListener = listener;
        if (poolListener != null && drainer.getListener() == null) {
            drainer.setListener(poolListener);
        }
        boolean accepted = false;
        try {
            active.add(drainer);
            executor.submit(() -> {
                try {
                    drainer.run();
                } finally {
                    active.remove(drainer);
                    BackgroundDrainer.DrainOutcome out = drainer.outcome();
                    if (out == BackgroundDrainer.DrainOutcome.SUCCESS) {
                        totalSucceeded.incrementAndGet();
                    } else if (out == BackgroundDrainer.DrainOutcome.FAILED) {
                        totalFailed.incrementAndGet();
                    }
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
}
