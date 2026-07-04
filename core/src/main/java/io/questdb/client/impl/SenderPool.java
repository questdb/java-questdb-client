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

import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.IntList;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;

/**
 * Elastic pool of {@link Sender} instances, each wrapped in a
 * {@link PooledSender} decorator. The pool keeps at least {@code minSize}
 * connections warm, grows on demand up to {@code maxSize}, and lets the
 * housekeeper reap slots that have idled longer than {@code idleTimeoutMillis}
 * or aged past {@code maxLifetimeMillis} (with {@code minSize} respected at
 * all times).
 * <p>
 * The hot borrow / return path takes a {@link ReentrantLock} but does no
 * per-call allocation; the underlying {@link ArrayDeque} of free decorators
 * is pre-sized to {@code maxSize}.
 * <p>
 * Connection creation happens outside the lock so a slow connect (TLS
 * handshake, DNS) does not block other borrowers or the housekeeper. The
 * pool tracks in-flight creations via {@code inFlightCreations} so the cap
 * check ({@code allSize + inFlightCreations + closingSlots + leakedSlots <
 * maxSize}) stays correct under concurrent borrows.
 * <p>
 * <b>Store-and-forward slots.</b> When the configuration enables SF
 * ({@code sf_dir} set), every sender owns an exclusive on-disk slot
 * {@code <sf_dir>/<sender_id>} guarded by a {@code flock}. A pool reuses one
 * immutable config string for every sender, so without intervention all
 * senders would inherit the same {@code sender_id}, point at the same slot,
 * and every sender after the first would die with "sf slot already in use".
 * The pool therefore hands each slot a distinct id {@code <base>-<index>},
 * where {@code <base>} is the configured {@code sender_id} (default
 * {@code "default"}) and {@code <index>} is a stable pool slot index in
 * {@code [0, maxSize)}. Indices are reused deterministically (lowest free
 * first), so across a restart the same slot dirs are re-adopted and any
 * unacked data they hold is recovered on creation. A slot is only returned
 * to the free set once its delegate has released the {@code flock}, tracked
 * via {@code closingSlots} so a concurrent borrow can never reclaim a slot
 * dir whose lock is still held.
 */
public final class SenderPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SenderPool.class);
    // Per-slot wall-clock cap on a single startup-recovery drain. Kept BELOW the
    // PoolHousekeeper stop/join budget (PoolHousekeeper.STOP_TIMEOUT_MILLIS) so a
    // recovery drain still in flight when close() arrives cannot outlive the
    // housekeeper join -- the residual-budget bound that, together with the
    // early markClosing() signal, keeps close() prompt (C1 fix).
    //
    // This caps only the DRAIN. The recovery build that precedes it is bounded
    // separately: recovery delegates force initial_connect_mode=OFF (see
    // defaultRecoverySender) so the build does at most ONE connect attempt
    // rather than a SYNC reconnect-budget retry loop (M1). One in-flight
    // connect against a black-holed host still blocks on the OS connect timeout
    // -- the residual window documented on recoverOneSlotStep -- because the
    // transport has no application-level connect timeout to clamp it.
    private static final long RECOVERY_DRAIN_BUDGET_MILLIS = 1_000;
    // Hard cap on close()'s outstanding-lease wait. The acquire timeout is a
    // BORROW policy -- Long.MAX_VALUE legitimately means "block until a slot
    // frees" -- and must never unbound SHUTDOWN: without this cap a forgotten
    // lease would hang close() forever. Mirrors the egress twin, whose
    // shutdown join is capped at QueryWorker.SHUTDOWN_JOIN_MILLIS (same value)
    // regardless of user config.
    static final long MAX_CLOSE_LEASE_WAIT_MILLIS = 5_000;
    private final long acquireTimeoutMillis;
    private final ArrayList<SenderSlot> all;
    private final ArrayDeque<SenderSlot> available;
    private final String configurationString;
    // User-supplied ingest callbacks, shared across every pooled Sender this
    // pool builds. Null -> each sender keeps its loud-not-silent default.
    private final SenderConnectionListener connectionListener;
    private final BackgroundDrainerListener drainerListener;
    private final SenderErrorHandler errorHandler;
    private final long idleTimeoutMillis;
    // Test seam. Production builds delegates via defaultSender(); white-box
    // tests in io.questdb.client.test.impl reach the package-private
    // constructor by reflection to inject a factory that throws a non-
    // RuntimeException Throwable (e.g. an -ea AssertionError) mid-prewarm,
    // exercising the Error-safe delegate cleanup loop.
    private final IntFunction<Sender> senderFactory;
    // Factory for startup-recovery delegates. Distinct from senderFactory so a
    // recoverer can force a non-blocking initial connect (initial_connect_mode=
    // OFF) regardless of user config: a recovery build runs on the
    // PoolHousekeeper thread and must NOT inherit SYNC (auto-enabled by any
    // reconnect_* knob), which would retry the connect for the whole reconnect
    // budget inside build() -- far past PoolHousekeeper.STOP_TIMEOUT_MILLIS, so
    // a close() landing during that build would make housekeeper.stop()'s join
    // time out and leave the recoverer holding the slot flock after close()
    // returned (M1). Mirrors senderFactory's test seam: an injected factory
    // (non-null) drives BOTH paths so white-box recovery tests keep control.
    private final IntFunction<Sender> recoverySenderFactory;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxLifetimeMillis;
    private final int maxSize;
    private final int minSize;
    // SF slot base id (configured sender_id, default "default") when SF is
    // enabled; null otherwise. Each pooled sender's slot id is
    // {@code slotBaseId + "-" + slotIndex}.
    private final String slotBaseId;
    // SF group root (sf_dir) when SF is enabled; null otherwise. Used to
    // locate this pool's own managed slot dirs <sfDir>/<slotBaseId>-<index>
    // for startup recovery of unacked data left by a previous run.
    private final String sfDir;
    // Reservation bitmap for SF slot indices [0, maxSize). Guarded by lock.
    // null when SF is disabled (no per-slot identity needed).
    private final boolean[] slotInUse;
    private final Condition slotReleased;
    // True iff the configuration enables store-and-forward (sf_dir set).
    private final boolean storeAndForward;
    // Slots removed from `all` whose delegate is still releasing its flock.
    // They keep reserving capacity (and their slotInUse mark) until the
    // flock drops, so the cap check and the slot allocator stay consistent
    // and no concurrent borrow can reclaim a slot dir that is still locked.
    // Guarded by lock. Only ever ticks for SF slots.
    private int closingSlots;
    // Shutdown signal: "the pool is shutting down". markClosing() raises it early
    // (without tearing down delegates) so an in-flight startup-recovery step
    // stops promptly between slots; close() also raises it. Read on the hot
    // paths (borrow/giveBack/discardBroken/reapIdle/recovery).
    private volatile boolean closed;
    // True once close() has begun the one-and-only delegate teardown. Distinct
    // from `closed` so markClosing() can raise the shutdown signal early
    // (cancelling recovery) WITHOUT making a later close() short-circuit the
    // teardown. Guarded by lock.
    private boolean closeStarted;
    private int inFlightCreations;
    // Lease teardowns currently running on borrower threads (retireLease's
    // delegate-close section, outside the lock). close() counts these as
    // outstanding so it does not return while a delegate is still being torn
    // down on another thread. Guarded by lock.
    private int pendingLeaseTeardowns;
    // Slots whose delegate close() returned with the SF flock still held
    // (the I/O thread refused to stop). Permanently consumed: the index is
    // never freed and never reused, so no borrow ever hands out a still-
    // locked slot dir. Counted in the cap check so the lost capacity is
    // accounted for. Guarded by lock; only ever ticks for SF slots.
    private int leakedSlots;
    // SF slots currently held by the in-range startup-recovery pass
    // (recoverOneSlotStep): each is reserved under `lock` for the
    // duration of its drain and counted in the borrow() cap check so a
    // concurrent borrow can neither over-allocate past maxSize nor target a
    // dir being recovered. Only ever non-zero on the deferred (housekeeper-
    // driven) recovery path, where recovery overlaps borrow()/return; on the
    // inline construction path the pool is still single-threaded. Guarded by
    // lock; only ever ticks for SF slots.
    private int recoveringSlots;
    // Resumable startup-recovery scan cursor. Advanced only by the single
    // recovery driver -- the inline constructor loop (single-threaded,
    // unpublished) or the PoolHousekeeper thread (the sole deferred driver) --
    // so the cursor itself needs no lock; the per-slot reservation it performs
    // (slotInUse/recoveringSlots) is still taken under `lock` because borrow()
    // races it. recoveryInRangeNext is the next in-range index in [0, maxSize)
    // for pass 1; recoveryOutOfRange / recoveryOutOfRangeNext are the lazily
    // built pass-2 work list (same-base slots at index >= maxSize) and its
    // cursor; recoveryComplete latches true when the whole scan finishes or is
    // aborted, making runStartupRecoveryStep()/...ToCompletion() idempotent.
    private int recoveryInRangeNext;
    private IntList recoveryOutOfRange;
    private int recoveryOutOfRangeNext;
    private boolean recoveryComplete;

    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, null, false, null, null, null);
    }

    // Test-only constructor exposing the senderFactory seam: production builds
    // via the full constructor below (senderFactory null -> the real
    // defaultSender()). White-box tests inject a factory that throws a
    // non-RuntimeException Throwable mid-prewarm. Recovery runs inline here
    // (deferStartupRecovery=false); the pooled QuestDB handle uses the 8-arg
    // overload to defer it to the housekeeper thread.
    @TestOnly
    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, senderFactory, false);
    }

    // Test-only constructor adding the deferStartupRecovery toggle.
    // deferStartupRecovery=true skips the inline, construction-time SF recovery
    // (recoverOneSlotStep) so QuestDB.build() never blocks on a slow or
    // reachable-but-not-acking server; the owner (QuestDBImpl) then drives
    // recovery one slot per tick on the PoolHousekeeper thread via
    // runStartupRecoveryStep(). White-box SF tests call this directly; the
    // in-range recovery pass is concurrency-safe against borrow()/return on the
    // deferred path -- see recoverOneSlotStep().
    @TestOnly
    public SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery
    ) {
        this(configurationString, minSize, maxSize, acquireTimeoutMillis,
                idleTimeoutMillis, maxLifetimeMillis, senderFactory,
                deferStartupRecovery, null, null, null);
    }

    // Full constructor adding the user-supplied ingest callbacks (error
    // handler, connection listener and background-drainer listener), applied
    // to every Sender the pool builds (see buildManagedSlotSender). The public
    // 6-arg ctor and the test-only senderFactory overloads above both delegate
    // here with null callbacks; the pooled QuestDB handle calls this directly.
    SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery,
            SenderErrorHandler errorHandler,
            SenderConnectionListener connectionListener,
            BackgroundDrainerListener drainerListener
    ) {
        if (minSize < 0 || maxSize < 1 || minSize > maxSize) {
            throw new IllegalArgumentException("invalid pool sizing: min=" + minSize + ", max=" + maxSize);
        }
        this.errorHandler = errorHandler;
        this.connectionListener = connectionListener;
        this.drainerListener = drainerListener;
        this.senderFactory = senderFactory != null ? senderFactory : this::defaultSender;
        // An injected factory (tests) drives recovery too, preserving the
        // white-box recovery seam; production recovery forces OFF-mode connects
        // via defaultRecoverySender (see field comment / createRecoverer).
        this.recoverySenderFactory = senderFactory != null ? senderFactory : this::defaultRecoverySender;
        this.configurationString = configurationString;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.maxLifetimeMillis = maxLifetimeMillis;
        this.all = new ArrayList<>(maxSize);
        this.available = new ArrayDeque<>(maxSize);
        this.slotReleased = lock.newCondition();
        // Probe the config once, up front: this validates it eagerly (so a
        // bad config fails at construction even when minSize == 0) and tells
        // us whether SF is on and, if so, the base slot id to derive
        // per-sender ids from.
        Sender.LineSenderBuilder probe = Sender.builder(configurationString);
        this.storeAndForward = probe.isStoreAndForwardEnabled();
        this.slotBaseId = this.storeAndForward ? probe.getConfiguredSenderId() : null;
        this.sfDir = this.storeAndForward ? probe.getConfiguredSfDir() : null;
        this.slotInUse = this.storeAndForward ? new boolean[maxSize] : null;
        // Pre-warm minSize connections. Pre-warm runs single-threaded in the
        // constructor, so slots 0..minSize-1 are reserved directly.
        int built = 0;
        try {
            for (int i = 0; i < minSize; i++) {
                if (storeAndForward) {
                    slotInUse[i] = true;
                }
                SenderSlot ps = createUnlocked(storeAndForward ? i : -1);
                all.add(ps);
                available.add(ps);
                built++;
            }
        } catch (Throwable e) {
            // Catch Throwable, not just RuntimeException: createUnlocked() runs a
            // heavy native build path (mmap, flock, WebSocket connect) that can
            // throw an Error -- e.g. an -ea AssertionError or OutOfMemoryError --
            // mid-prewarm. If we only caught RuntimeException the Error would
            // propagate without running the cleanup below, leaking every
            // already-built delegate's flock + mmap'd ring + I/O thread and
            // resurrecting "sf slot already in use" on the next attempt.
            for (int i = 0; i < built; i++) {
                try {
                    all.get(i).delegate().close();
                } catch (Throwable ignored) {
                    // Best-effort cleanup: a delegate close() can throw an
                    // Error (e.g. an -ea AssertionError) as well as a
                    // RuntimeException. Swallow either so we still close the
                    // remaining pre-warmed slots and rethrow the original
                    // construction failure below.
                }
            }
            throw e;
        }
        // Prewarm succeeded. Recover any unacked data a previous run left in
        // this pool's own managed slots that prewarm did not already re-adopt.
        // The pooled QuestDB handle defers this to the housekeeper thread
        // (deferStartupRecovery=true) so QuestDB.build() never blocks on a slow
        // or reachable-but-not-acking server; direct constructions run it inline
        // here, while still single-threaded and unpublished.
        if (!deferStartupRecovery) {
            runStartupRecoveryToCompletion();
        }
    }

    /**
     * Drives startup SF recovery to completion in a single call, bounded by one
     * shared {@code acquireTimeoutMillis} wall-clock budget (and each individual
     * drain by {@link #RECOVERY_DRAIN_BUDGET_MILLIS}). Used by the inline
     * construction path -- single-threaded and unpublished -- and by manual /
     * test drives. No-op when SF is off, the pool is shutting down, or recovery
     * has already finished. Idempotent.
     */
    void runStartupRecoveryToCompletion() {
        if (!storeAndForward) {
            return;
        }
        // One shared wall-clock budget for the WHOLE scan, not per slot: without
        // it a reachable-but-not-acking server would pay a full drain timeout on
        // every stranded slot. One acquire timeout is the ceiling already
        // accepted for a single borrow, so we reuse it as the total budget; once
        // spent, the remaining slots wait for a later attempt (data stays
        // durable on disk).
        final long deadlineNanos =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        while (!closed && !recoveryComplete) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            if (remainingMillis <= 0) {
                LOG.warn("startup SF recovery: {}ms budget exhausted; "
                        + "skipping remaining slots", acquireTimeoutMillis);
                recoveryComplete = true;
                return;
            }
            if (!recoverOneSlotStep(Math.min(remainingMillis, RECOVERY_DRAIN_BUDGET_MILLIS))) {
                return;
            }
        }
    }

    /**
     * Recovers at most ONE stranded managed slot and reports whether more remain.
     * Driven by the {@link PoolHousekeeper} one slot per step, back-to-back, on
     * the reap-loop thread: each step performs at most one drain bounded by
     * {@link #RECOVERY_DRAIN_BUDGET_MILLIS} -- kept below the housekeeper
     * stop/join budget -- on a delegate whose initial connect is forced OFF
     * ({@link #defaultRecoverySender}) so the build makes at most one connect
     * attempt. The housekeeper re-checks {@code stop} between steps while
     * {@link QuestDBImpl#close()} raises the {@code closed} shutdown signal (via
     * {@link #markClosing()}) BEFORE stopping it, so a {@code close()} landing
     * mid-recovery normally only has to wait out a single bounded drain. The one
     * exception is an in-flight connect to a black-holed host, which blocks on
     * the OS connect timeout -- see the residual-window note on
     * {@link #recoverOneSlotStep}.
     * No-op (returns {@code false}) when SF is off, the pool is shutting down, or
     * recovery has already finished.
     *
     * @return {@code true} if recovery has more work (call again), {@code false}
     * when recovery is complete or the pool is shutting down
     */
    boolean runStartupRecoveryStep() {
        if (!storeAndForward || closed || recoveryComplete) {
            return false;
        }
        return recoverOneSlotStep(RECOVERY_DRAIN_BUDGET_MILLIS);
    }

    /**
     * Best-effort recovery of unacked data left in this pool's own managed SF
     * slots by a previous run -- both the in-range slots {@code [0, maxSize)}
     * (pass 1) and the same-base slots a larger previous run left OUT of range
     * ({@code <base>-i} with {@code i >= maxSize}, pass 2). Performs at most ONE
     * actual drain per call, advancing the resumable scan cursor, and returns
     * whether more stranded slots remain.
     * <p>
     * Every pooled SF sender's orphan drainer deliberately excludes the whole
     * {@code [0, maxSize)} managed range (via
     * {@code orphanDrainExcludeManagedSlots}) so a sibling never adopts a slot
     * dir/lock the pool intends to (re)create -- that exclusion is what keeps the
     * per-slot ids from resurfacing "sf slot already in use". The trade-off is
     * that an in-range slot left holding unacked data is otherwise recovered ONLY
     * when the pool happens to (re)create that index; under steady low load the
     * pool may never grow to a high index, stranding that slot's data (durable on
     * disk, but undelivered) until a later restart or load spike. An out-of-range
     * slot is worse off still: the pool never re-creates its index, and the
     * per-sender drainer only adopts it when {@code drain_orphans=on}. This scan
     * closes both gaps regardless of {@code drain_orphans}.
     * <p>
     * The in-range pass reserves each slot index under {@code lock} for the
     * duration of its recovery AND counts it in the borrow() capacity check (via
     * {@code recoveringSlots}), so a concurrent borrow on the deferred path can
     * neither target the dir being recovered nor over-allocate past
     * {@code maxSize}. Prewarmed/borrowed slots (already live, holding their
     * flock) are skipped, as are empty slots (a cheap directory probe); only a
     * slot that actually holds stranded data spends the step's single drain. The
     * out-of-range pass needs no reservation: those indices have no
     * {@code slotInUse} entry and are never allocated by borrow().
     * <p>
     * Best-effort throughout: a build/close Error or a slow drain is logged and
     * never propagates, since the data stays durable on disk for a later attempt;
     * the first build failure or drain timeout latches {@code recoveryComplete}
     * (the failure will very likely repeat for every remaining slot).
     * <p>
     * <b>Boundedness / residual window.</b> Recovery is driven on the
     * PoolHousekeeper thread, and {@code close()} relies on a step finishing
     * within {@code PoolHousekeeper.STOP_TIMEOUT_MILLIS}. A step is build +
     * drain + close. The drain is capped by {@link #RECOVERY_DRAIN_BUDGET_MILLIS}
     * and the build forces {@code initial_connect_mode=OFF} (see
     * {@link #defaultRecoverySender}), so it makes at most ONE connect attempt
     * instead of a SYNC reconnect-budget retry loop. That removes the
     * minutes-long block a {@code reconnect_*}-tuned config used to cause (M1).
     * One residual window remains and is NOT closed here: a single in-flight
     * connect to a black-holed/firewalled host blocks on the OS connect timeout
     * (the transport exposes no application-level connect timeout to clamp it).
     * If {@code close()} lands during that one connect the housekeeper join can
     * still time out and the detached build releases the slot flock shortly
     * after {@code close()} returns. No data is lost (the slot stays durable on
     * disk); the exposure is a brief "sf slot already in use" window on an
     * immediate reopen, bounded by a single OS connect timeout.
     *
     * @return {@code true} if a drain was performed and more slots may remain;
     * {@code false} once the scan is complete or the pool is shutting down
     */
    private boolean recoverOneSlotStep(long stepBudgetMillis) {
        if (sfDir == null || !Files.exists(sfDir)) {
            recoveryComplete = true;
            return false;
        }
        final boolean[] flockHeld = new boolean[1];

        // Pass 1: in-range managed slots [0, maxSize). Skip live and empty slots
        // cheaply; spend the step on the first slot that actually holds data.
        while (recoveryInRangeNext < maxSize) {
            if (closed) {
                return false;
            }
            int i = recoveryInRangeNext;
            // Reserve this index unless prewarm (or a concurrent borrow, on the
            // deferred path) already holds it live. Count the reservation in
            // recoveringSlots so the borrow() cap check cannot over-allocate
            // while this slot is held for recovery.
            boolean reserved;
            lock.lock();
            try {
                reserved = slotInUse[i];
                if (!reserved) {
                    slotInUse[i] = true;
                    recoveringSlots++;
                }
            } finally {
                lock.unlock();
            }
            if (reserved) {
                recoveryInRangeNext++;
                continue;
            }
            String slotPath = sfDir + "/" + slotBaseId + "-" + i;
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                // No stranded data: release the reservation and keep scanning;
                // an empty slot must not cost a whole step.
                lock.lock();
                try {
                    recoveringSlots--;
                    slotInUse[i] = false;
                    slotReleased.signal();
                } finally {
                    lock.unlock();
                }
                recoveryInRangeNext++;
                continue;
            }
            // A real candidate -> spend the step on it. Advance the cursor first
            // so a resume never reprocesses this index.
            recoveryInRangeNext++;
            boolean stopScan = drainCandidateSlotForRecovery(i, slotPath, stepBudgetMillis, flockHeld);
            lock.lock();
            try {
                // Release the recovery reservation accounting; from here either
                // leakedSlots (retire) or the freed index carries the cap math.
                recoveringSlots--;
                if (flockHeld[0]) {
                    // close() bailed early with the I/O thread still running and
                    // the flock still held. Retire the slot permanently (mirror
                    // discardBroken/reapIdle): keep slotInUse[i] set and count it
                    // in leakedSlots so the borrow() cap math accounts for the
                    // lost capacity and no later borrow ever reuses the
                    // still-locked dir.
                    leakedSlots++;
                    LOG.warn("startup SF recovery: slot {} retired permanently: delegate close() returned with "
                                    + "the flock still held (I/O thread refused to stop); pool capacity reduced by 1, "
                                    + "now {} of {} usable [leakedSlots={}]",
                            i, maxSize - leakedSlots, maxSize, leakedSlots);
                } else {
                    slotInUse[i] = false;
                    // On the deferred path a borrow may be waiting on capacity
                    // this recovery held; the freed index can now admit a
                    // creation.
                    slotReleased.signal();
                }
            } finally {
                lock.unlock();
            }
            if (stopScan) {
                // A build failure or drain timeout that will very likely repeat
                // for every remaining slot -- abort the scan; the data stays
                // durable on disk for a later attempt. Do not start pass 2.
                recoveryComplete = true;
                return false;
            }
            return true;
        }

        // Pass 1 done. Build the pass-2 work list once: same-base slots a
        // previous run left OUT of the current index range (<base>-i with
        // i >= maxSize, from a run with a larger maxSize). The pool never
        // re-creates these indices, and the per-sender drainer only adopts them
        // when drain_orphans=on, so without this pass their unacked data would
        // sit durable-but-undelivered under the default config. They are outside
        // [0, maxSize), have no slotInUse entry, and never affect the borrow()
        // cap math, so no reservation is needed.
        if (recoveryOutOfRange == null) {
            recoveryOutOfRange = OrphanScanner.listStrandedOutOfRangeManagedSlots(
                    sfDir, slotBaseId, maxSize);
            recoveryOutOfRangeNext = 0;
        }
        while (recoveryOutOfRangeNext < recoveryOutOfRange.size()) {
            if (closed) {
                return false;
            }
            int idx = recoveryOutOfRange.getQuick(recoveryOutOfRangeNext++);
            String slotPath = sfDir + "/" + slotBaseId + "-" + idx;
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                continue;
            }
            boolean stopScan = drainCandidateSlotForRecovery(idx, slotPath, stepBudgetMillis, flockHeld);
            if (flockHeld[0]) {
                // Out of the pool's [0, maxSize) capacity range: there is no
                // slotInUse entry to retire and no future borrow targets this
                // dir, so a still-held flock only leaks this recoverer's I/O
                // thread (a best-effort teardown loss, logged). Crucially we do
                // NOT touch leakedSlots -- that would wrongly shrink the
                // in-range pool capacity.
                LOG.warn("startup SF recovery: out-of-range slot {} closed with the flock still held "
                                + "(I/O thread refused to stop); its data is durable on disk for a later attempt",
                        slotPath);
            }
            if (stopScan) {
                recoveryComplete = true;
                return false;
            }
            return true;
        }

        recoveryComplete = true;
        return false;
    }

    /**
     * Drains one candidate orphan slot dir within {@code remainingMillis},
     * best-effort and never throwing. Builds a recoverer on {@code slotIndex}
     * (whose {@link #defaultSender} derives the dir {@code <base>-slotIndex}),
     * drains its unacked data, and closes the delegate. Shared by both recovery
     * passes -- the in-range pass and the out-of-range pass -- which differ only
     * in their slot bookkeeping, handled by the caller via {@code flockHeld}.
     *
     * @param flockHeld single-element out-param set to {@code true} iff a
     *                  recoverer was built and its {@code close()} returned with
     *                  the flock still held (the I/O thread refused to stop)
     * @return {@code true} if a build/drain failure occurred that will very
     * likely repeat for every remaining slot, so the caller should stop scanning
     */
    private boolean drainCandidateSlotForRecovery(int slotIndex, String slotPath,
                                                  long remainingMillis, boolean[] flockHeld) {
        flockHeld[0] = false;
        // Hoisted so the flock check after the try can consult it:
        // createRecoverer() takes the slot flock on <base>-slotIndex, and
        // delegate().close() can early-return with the I/O thread still running
        // (flock still held).
        SenderSlot recoverer = null;
        boolean stopScan = false;
        try {
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                return false;
            }
            try {
                // Recovery delegate: forced OFF-mode initial connect (see
                // createRecoverer / defaultRecoverySender), so this build does
                // at most ONE connect attempt -- it never inherits the SYNC
                // reconnect-budget retry loop that would block this
                // (PoolHousekeeper) thread for minutes (M1).
                recoverer = createRecoverer(slotIndex);
            } catch (Throwable buildErr) {
                // A build/connect failure (e.g. server unreachable) will very
                // likely repeat for every remaining slot, so stop here rather
                // than pay a connect timeout per slot.
                LOG.warn("startup SF recovery: could not open slot {} ({}); "
                        + "skipping remaining slots", slotPath, buildErr.toString());
                return true;
            }
            try {
                // Cap the drain at the remaining shared budget and short-circuit
                // on a timeout: a server that fails to ack within the budget
                // will very likely do the same for every remaining slot -- the
                // same reasoning as the build-failure case above.
                if (!recoverer.delegate().drain(remainingMillis)) {
                    LOG.warn("startup SF recovery: drain did not ack slot {} "
                            + "within {}ms; skipping remaining slots",
                            slotPath, remainingMillis);
                    stopScan = true;
                }
            } catch (Throwable drainErr) {
                LOG.warn("startup SF recovery: drain failed for slot {} ({})",
                        slotPath, drainErr.toString());
            } finally {
                try {
                    recoverer.delegate().close();
                } catch (Throwable ignored) {
                    // Best-effort close: a teardown Error must not abort
                    // recovery of the remaining slots.
                }
            }
        } catch (Throwable scanErr) {
            LOG.warn("startup SF recovery: scan failed for slot {} ({})",
                    slotPath, scanErr.toString());
        }
        if (recoverer != null) {
            flockHeld[0] = !flockReleased(recoverer);
        }
        return stopScan;
    }

    public PooledSender borrow() {
        // Track remaining wait via awaitNanos's return value (canonical pattern):
        // awaitNanos consumes from the budget on each wait and reports what is
        // left; <= 0 means the budget is exhausted.
        long remainingNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        lock.lock();
        try {
            while (true) {
                if (closed) {
                    throw new LineSenderException("QuestDB handle is closed");
                }
                if (!available.isEmpty()) {
                    SenderSlot s = available.pollFirst();
                    // Stamp a fresh lease id under the lock so the PooledSender
                    // wrapper handed out can be told apart from any prior,
                    // now-stale borrow of the same slot.
                    s.bumpGeneration();
                    return new PooledSender(s, s.generation());
                }
                if (all.size() + inFlightCreations + closingSlots + leakedSlots + recoveringSlots < maxSize) {
                    inFlightCreations++;
                    // Reserve a slot index under the lock so concurrent
                    // creations never target the same SF slot dir. -1 when
                    // SF is off (no per-slot identity needed).
                    int slotIndex = storeAndForward ? allocateSlotIndex() : -1;
                    lock.unlock();
                    SenderSlot created;
                    try {
                        created = createUnlocked(slotIndex);
                    } catch (Throwable e) {
                        // Catch Throwable, not just RuntimeException:
                        // createUnlocked() runs a heavy native build path
                        // (mmap, flock, WebSocket connect) that can throw an
                        // Error -- e.g. an -ea AssertionError or
                        // OutOfMemoryError. If we only caught RuntimeException
                        // the Error would propagate with inFlightCreations
                        // still incremented and the SF slot index still
                        // reserved (slotInUse[idx] stuck true), permanently
                        // lowering pool capacity. The cleanup below is
                        // idempotent, so undoing the reservation for any
                        // throwable is safe.
                        lock.lock();
                        inFlightCreations--;
                        freeSlotIndex(slotIndex);
                        slotReleased.signal();
                        lock.unlock();
                        throw e;
                    }
                    lock.lock();
                    inFlightCreations--;
                    if (closed) {
                        // Pool was closed mid-creation -- destroy the new connection
                        // rather than leaking it. Other waiters have been signaled
                        // by close() already. The delegate is closed OUTSIDE the
                        // lock (mirroring retireLease): its close() can block for
                        // seconds (bounded ack drain, drainer-pool wind-down) or
                        // longer (unbounded I/O-thread latch await behind an
                        // OS-level connect), which held here would stall close(),
                        // giveBack/retireLease and reapIdle behind the pool lock.
                        // Accounting first, under the lock: for an SF slot the
                        // index reservation moves from inFlightCreations to
                        // closingSlots until the close below releases the flock,
                        // and pendingLeaseTeardowns keeps the out-of-lock close
                        // visible to close()'s outstanding-teardown wait.
                        boolean reserved = created.slotIndex() >= 0;
                        if (reserved) {
                            closingSlots++;
                        }
                        pendingLeaseTeardowns++;
                        lock.unlock();
                        try {
                            created.delegate().close();
                        } catch (Throwable ignored) {
                            // Best-effort: an Error (e.g. -ea AssertionError)
                            // from teardown must not mask the closed-pool signal.
                        } finally {
                            // Re-lock to reclaim the SF slot index and signal a
                            // close() waiting on this teardown. MUST run even if
                            // the delegate close threw, otherwise the slot stays
                            // reserved forever and close() waits out its full
                            // budget on a teardown that already happened.
                            lock.lock();
                            pendingLeaseTeardowns--;
                            if (reserved) {
                                reclaimSlot(created, " after closed-mid-creation teardown");
                            }
                            slotReleased.signalAll();
                            lock.unlock();
                        }
                        throw new LineSenderException("QuestDB handle is closed");
                    }
                    all.add(created);
                    created.bumpGeneration();
                    return new PooledSender(created, created.generation());
                }
                if (remainingNanos <= 0) {
                    throw new LineSenderException(
                            "timed out waiting for a Sender from the pool after " + acquireTimeoutMillis + "ms");
                }
                try {
                    remainingNanos = slotReleased.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("interrupted while waiting for a Sender from the pool");
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * Raises the shutdown signal early -- without tearing down live delegates --
     * so an in-flight startup-recovery step driven on the {@link PoolHousekeeper}
     * thread stops promptly between slots. {@link QuestDBImpl#close()} calls this
     * BEFORE stopping the housekeeper, so the housekeeper join cannot time out
     * waiting on a fresh slot's recovery drain. Idle-delegate teardown and the
     * outstanding-lease wait still happen in {@link #close()} (guarded by
     * {@code closeStarted}, so this early signal never short-circuits them);
     * borrowed delegates returned after this signal are torn down on the
     * returning thread via {@link #retireLease}. Idempotent; safe to call
     * repeatedly.
     */
    void markClosing() {
        closed = true;
    }

    /**
     * Shuts the pool down. NEVER tears down a borrowed delegate: a producer
     * thread may be inside one right now (mid-append, mid-flush), and closing
     * it from here would flush table buffers that thread is mutating and then
     * free their native memory under its feet -- a use-after-free / SEGV, not
     * an exception (C1). Instead:
     * <ol>
     * <li>waits boundedly (up to {@code acquireTimeoutMillis}, hard-capped at
     * {@link #MAX_CLOSE_LEASE_WAIT_MILLIS}) for outstanding leases to come home -- {@link #giveBack} and {@link #discardBroken}
     * observe {@code closed} and tear each delegate down on the returning
     * borrower's own thread, its exclusive user at that point
     * ({@link #retireLease}); then</li>
     * <li>closes the delegates of idle slots only, outside the lock.</li>
     * </ol>
     * A lease that never returns leaks its delegate (logged): a logged leak is
     * recoverable, a freed buffer under a live producer is a JVM crash. This
     * mirrors the egress twin, {@code QueryWorker.shutdown()}'s bounded
     * interrupt+join before {@code client.close()}. Idempotent.
     */
    @Override
    public void close() {
        SenderSlot[] idleSnapshot;
        lock.lock();
        try {
            if (closeStarted) {
                return;
            }
            closeStarted = true;
            // Raise the shutdown signal too (a direct, non-pooled caller may
            // close() without a prior markClosing()); harmless if already set.
            closed = true;
            // Wake parked borrowers so they observe the shutdown and throw.
            slotReleased.signalAll();
            // Bounded graceful wait for outstanding leases. A slot is borrowed
            // iff it is in `all` but not in `available`; retireLease's
            // delegate-close section (running outside the lock on a returning
            // borrower's thread) is tracked by pendingLeaseTeardowns so this
            // method does not return while a teardown is still in flight.
            // The budget is the acquire timeout hard-capped at
            // MAX_CLOSE_LEASE_WAIT_MILLIS: a huge/infinite acquire timeout is
            // a borrow policy, not a licence for close() to hang forever on a
            // lease that never comes home.
            final long waitMillis = Math.min(acquireTimeoutMillis, MAX_CLOSE_LEASE_WAIT_MILLIS);
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(waitMillis);
            while ((all.size() > available.size() || pendingLeaseTeardowns > 0) && remainingNanos > 0) {
                try {
                    remainingNanos = slotReleased.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    // Preserve the interrupt and stop waiting: idle delegates
                    // are still torn down below, and stragglers take the
                    // delegated-teardown path whenever they return.
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            int outstanding = all.size() - available.size() + pendingLeaseTeardowns;
            if (outstanding > 0) {
                LOG.warn("SenderPool.close(): {} borrowed sender lease(s) still outstanding after {}ms; "
                                + "each connection is torn down when its lease is closed, or leaks if it never is",
                        outstanding, waitMillis);
            }
            // Only idle slots are safe to close from this thread: no lease
            // means no user thread inside the delegate. `available` cannot
            // grow after this point -- borrow() throws on `closed` and
            // giveBack() retires instead of requeueing -- so the snapshot is
            // complete.
            idleSnapshot = available.toArray(new SenderSlot[0]);
        } finally {
            lock.unlock();
        }
        // Close each idle delegate outside the lock so a slow real-close()
        // doesn't keep the pool latched.
        for (int i = 0; i < idleSnapshot.length; i++) {
            try {
                idleSnapshot[i].delegate().close();
            } catch (Throwable ignored) {
                // Best-effort: an Error from one delegate's teardown must not
                // abort the loop and strand the remaining delegates unclosed.
            }
        }
    }

    /**
     * Evicts a slot whose delegate has failed (typically a {@code flush()}
     * failure observed in {@link PooledSender#close()}). The slot is removed
     * from {@code all} so the pool can grow back into a fresh slot on demand.
     * The underlying delegate is closed outside the lock so a slow real-close
     * does not stall other borrowers.
     * <p>
     * Safe during shutdown too: {@link #close()} never touches borrowed
     * delegates, so the calling (borrower's) thread is the delegate's
     * exclusive user and {@link #retireLease} can tear it down without racing
     * the close() loop.
     */
    void discardBroken(PooledSender ps) {
        retireLease(ps, "");
    }

    public void giveBack(PooledSender ps) {
        SenderSlot s = ps.slot();
        long gen = ps.generation();
        lock.lock();
        try {
            if (!closed) {
                if (s.generation() != gen) {
                    // Stale return: this lease was already given back and the slot
                    // possibly re-borrowed (or this is a duplicate close). Dropping
                    // it keeps Sender.close() idempotent under a concurrent
                    // re-borrow -- without it a double close would enqueue the slot
                    // twice and hand it to two borrowers writing into one delegate.
                    return;
                }
                s.bumpGeneration();
                s.markIdleAt(System.currentTimeMillis());
                assert !available.contains(s) : "slot already present in available deque on giveBack";
                available.addLast(s);
                slotReleased.signal();
                return;
            }
        } finally {
            lock.unlock();
        }
        // Pool is shutting down: never requeue. close() deliberately does not
        // close borrowed delegates -- a producer thread could still be inside
        // one, and freeing its native buffers mid-append is a use-after-free /
        // SEGV (C1) -- so teardown is delegated HERE, to the returning
        // borrower's thread, the delegate's exclusive user at this point.
        // retireLease re-validates the lease generation under the lock and
        // signals the close() thread waiting for outstanding leases.
        retireLease(ps, " during pool shutdown");
    }

    /**
     * Retires one lease on the calling (borrower's) thread: validates the
     * lease generation under the lock, removes the slot from {@code all},
     * closes the delegate OUTSIDE the lock, and reclaims the SF slot index.
     * Shared by {@link #discardBroken} (broken delegate) and by
     * {@link #giveBack} when the pool is shutting down (delegated teardown --
     * see {@link #close()}).
     * <p>
     * Single-owner teardown: the caller holds the only live lease on this
     * slot and {@link #close()} never touches borrowed delegates, so no other
     * thread can be inside the delegate when it is closed here.
     * {@code pendingLeaseTeardowns} keeps the out-of-lock close visible to
     * close()'s outstanding-lease wait, so the pool does not report itself
     * closed while a delegate is still being torn down.
     *
     * @param ps      the lease being retired
     * @param context phrase woven into the SF retire WARN naming the reclaim
     *                path (e.g. {@code ""} or {@code " during pool shutdown"})
     */
    private void retireLease(PooledSender ps, String context) {
        SenderSlot s = ps.slot();
        long gen = ps.generation();
        boolean reserved = false;
        lock.lock();
        try {
            if (s.generation() != gen) {
                // Stale retire: the slot was already returned/discarded and
                // possibly re-borrowed. Dropping it avoids evicting a slot a
                // different borrower now owns and double-closing its delegate.
                return;
            }
            s.bumpGeneration();
            boolean removed = all.remove(s);
            // For an SF slot, keep its index reserved (move the reservation
            // from `all` to `closingSlots`) until the delegate below releases
            // the flock. Capacity stays accounted for, so a concurrent borrow
            // cannot reclaim this slot dir while its lock is still held.
            if (removed && s.slotIndex() >= 0) {
                closingSlots++;
                reserved = true;
            }
            pendingLeaseTeardowns++;
            // Wake all waiters: the cap check in borrow() may now admit a
            // creation attempt (on a *different* slot), and a close() in
            // progress must re-check its outstanding-lease count.
            slotReleased.signalAll();
        } finally {
            lock.unlock();
        }
        // Close the delegate outside the lock (releases the SF flock). Always
        // attempt it so native resources are freed even on the defensive path
        // where the wrapper had already left `all`.
        try {
            s.delegate().close();
        } catch (Throwable ignored) {
            // Best-effort teardown: a delegate close() can throw an Error
            // (e.g. an -ea AssertionError) as well as a RuntimeException.
            // Either way the accounting in the finally below MUST run,
            // otherwise an SF slot stays reserved forever (slotInUse stuck
            // true, closingSlots over-counted), the pool leaks capacity until
            // borrow() can only ever time out, and a concurrent close() would
            // wait out its full budget on a teardown that already happened.
        } finally {
            lock.lock();
            try {
                pendingLeaseTeardowns--;
                if (reserved) {
                    // Free the index only when the flock was released; a slot
                    // left locked is retired permanently.
                    reclaimSlot(s, context);
                }
                slotReleased.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Closes idle slots that have exceeded {@code idleTimeoutMillis} or that
     * have aged past {@code maxLifetimeMillis}. Never shrinks below
     * {@code minSize}. Called by the {@link PoolHousekeeper} on its tick.
     */
    public void reapIdle() {
        if (closed) {
            return;
        }
        long now = System.currentTimeMillis();
        ArrayList<SenderSlot> toClose = null;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            Iterator<SenderSlot> it = available.iterator();
            while (it.hasNext() && all.size() > minSize) {
                SenderSlot s = it.next();
                boolean idleExpired = idleTimeoutMillis < Long.MAX_VALUE
                        && (now - s.idleSinceMillis()) >= idleTimeoutMillis;
                boolean overAge = maxLifetimeMillis < Long.MAX_VALUE
                        && (now - s.createdAtMillis()) >= maxLifetimeMillis;
                if (idleExpired || overAge) {
                    it.remove();
                    all.remove(s);
                    // Keep the SF slot reserved until its flock is released
                    // below (see discardBroken for the rationale).
                    if (s.slotIndex() >= 0) {
                        closingSlots++;
                    }
                    if (toClose == null) {
                        toClose = new ArrayList<>();
                    }
                    toClose.add(s);
                }
            }
        } finally {
            lock.unlock();
        }
        if (toClose != null) {
            for (int i = 0, n = toClose.size(); i < n; i++) {
                try {
                    toClose.get(i).delegate().close();
                } catch (Throwable ignored) {
                    // Best-effort: a single delegate close() failure (including
                    // an Error such as an -ea AssertionError) must not abort the
                    // loop -- that would leave sibling flocks unreleased -- nor
                    // skip the slot-accounting release block below, which would
                    // strand every reaped index (slotInUse stuck true,
                    // closingSlots over-counted) and leak pool capacity.
                }
            }
            // Return reserved SF slot indices to the free set -- but only for
            // slots whose delegate confirmed the flock was released. A slot
            // left locked (I/O thread refused to stop) is retired permanently.
            if (storeAndForward) {
                lock.lock();
                try {
                    for (int i = 0, n = toClose.size(); i < n; i++) {
                        SenderSlot s = toClose.get(i);
                        if (s.slotIndex() >= 0) {
                            reclaimSlot(s, " during idle reaping");
                        }
                    }
                    slotReleased.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    /** Snapshot of the number of idle slots. For tests and introspection. */
    public int availableSize() {
        lock.lock();
        try {
            return available.size();
        } finally {
            lock.unlock();
        }
    }

    /** Snapshot of the total number of live slots (idle + in-use). For tests and introspection. */
    public int totalSize() {
        lock.lock();
        try {
            return all.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Snapshot of the number of SF slots permanently retired because a
     * delegate {@code close()} returned with the slot flock still held (the
     * I/O thread refused to stop). Each leaked slot permanently lowers the
     * pool's effective capacity ({@code maxSize - leakedSlotCount()}). A
     * non-zero, growing value explains a pool that has started timing out
     * every {@code borrow()}. For metrics and tests.
     */
    public int leakedSlotCount() {
        lock.lock();
        try {
            return leakedSlots;
        } finally {
            lock.unlock();
        }
    }

    private SenderSlot createUnlocked(int slotIndex) {
        return new SenderSlot(senderFactory.apply(slotIndex), this, slotIndex);
    }

    /**
     * Builds a {@link SenderSlot} for startup recovery of one stranded slot.
     * Routes through {@link #recoverySenderFactory}, which in production forces
     * a non-blocking initial connect ({@link #defaultRecoverySender}) so a
     * single recovery step stays bounded -- see that method and
     * {@link #drainCandidateSlotForRecovery}.
     */
    private SenderSlot createRecoverer(int slotIndex) {
        return new SenderSlot(recoverySenderFactory.apply(slotIndex), this, slotIndex);
    }

    private Sender defaultSender(int slotIndex) {
        return buildManagedSlotSender(slotIndex, false);
    }

    /**
     * Same managed-slot delegate as {@link #defaultSender}, but with the
     * initial connect forced to {@link Sender.InitialConnectMode#OFF}. Used
     * only for startup recovery, which runs on the PoolHousekeeper thread: OFF
     * makes the build do at most ONE connect attempt instead of retrying for
     * the whole reconnect budget (SYNC, auto-enabled by any reconnect_* knob),
     * keeping a recovery step bounded below
     * {@code PoolHousekeeper.STOP_TIMEOUT_MILLIS}. See M1 / the residual-window
     * note on {@link #recoverOneSlotStep}.
     * <p>
     * Also forces {@code drain_orphans=off} (see
     * {@link #buildManagedSlotSender}): a recovery delegate must never spin up a
     * BackgroundDrainerPool, whose {@code close()} could block ~3s and overrun
     * the step / {@code STOP_TIMEOUT_MILLIS} budget while still holding the slot
     * flock.
     */
    private Sender defaultRecoverySender(int slotIndex) {
        return buildManagedSlotSender(slotIndex, true);
    }

    // Applies the user-supplied ingest callbacks to a sender builder. Null
    // callbacks are skipped so the sender keeps its loud-not-silent default.
    private Sender.LineSenderBuilder applyUserCallbacks(Sender.LineSenderBuilder builder) {
        if (errorHandler != null) {
            builder.errorHandler(errorHandler);
        }
        if (connectionListener != null) {
            builder.connectionListener(connectionListener);
        }
        if (drainerListener != null) {
            builder.drainerListener(drainerListener);
        }
        return builder;
    }

    private Sender buildManagedSlotSender(int slotIndex, boolean forRecovery) {
        if (!storeAndForward) {
            return applyUserCallbacks(Sender.builder(configurationString)).build();
        }
        // Give this pooled sender its own slot dir <sf_dir>/<base>-<index>
        // so concurrent SF senders sharing one sf_dir never collide on
        // the slot flock. senderId() is only legal on WebSocket transport,
        // which is exactly when storeAndForward is true.
        //
        // Also fence off the pool's own live slot set [0, maxSize) from
        // orphan draining: the pool co-manages every <base>-<index> slot it
        // can re-create and recovers each slot's unacked data when it
        // (re)creates it, so a sibling's startup drainer must never adopt
        // another live pool slot's dir/lock (that would resurrect "sf slot
        // already in use"). The bound is maxSize, NOT the whole "<base>-"
        // prefix: a same-base slot at an index >= maxSize (left behind when
        // a previous run used a larger maxSize) is out of the pool's index
        // range forever, so it is left drainable here. Its unacked data is
        // delivered by the pool's own startup recovery
        // (recoverOneSlotStep, pass 2), which adopts these
        // out-of-range same-base slots at construction REGARDLESS of
        // drain_orphans -- so the default config never strands it. The
        // per-sender drainer is an additional path that only runs when
        // drain_orphans=on; foreign leftovers under other names are drained
        // only by that path.
        Sender.LineSenderBuilder builder = Sender.builder(configurationString)
                .senderId(slotBaseId + "-" + slotIndex)
                .orphanDrainExcludeManagedSlots(slotBaseId, maxSize);
        if (forRecovery) {
            // Force OFF so the recovery build never blocks on the reconnect
            // budget (see defaultRecoverySender). An explicit mode wins over
            // the SYNC auto-promotion the user's reconnect_* knobs would
            // otherwise trigger.
            builder.initialConnectMode(Sender.InitialConnectMode.OFF);
            // Force drain_orphans OFF on recovery delegates regardless of the
            // shared config string. A recovery delegate's sole job is to drain
            // its OWN slot (the one recoverOneSlotStep is processing); it must
            // never start a BackgroundDrainerPool for sibling/foreign orphans.
            // If it did, the delegate's close() -- called from
            // drainCandidateSlotForRecovery() on the PoolHousekeeper thread,
            // BEFORE its cursorEngine.close() releases the slot flock -- would
            // block in BackgroundDrainerPool.close() for up to
            // GRACEFUL_DRAIN_MILLIS + STOP_GRACE_MILLIS (3s) against a
            // reachable-but-not-acking server. That overruns a recovery step's
            // budget (RECOVERY_DRAIN_BUDGET_MILLIS) and PoolHousekeeper
            // .STOP_TIMEOUT_MILLIS, so a close() landing mid-step times out its
            // join and returns while the recoverer still holds the slot flock
            // -- resurrecting the "sf slot already in use" window this pool's
            // per-slot ids exist to eliminate. Sibling in-range slots are
            // covered by recoverOneSlotStep's own passes; foreign/out-of-range
            // orphans are covered by the LIVE pooled senders' drainers (which
            // keep drain_orphans=on and whose close() senderPool.close() awaits
            // synchronously, so they release their flock before close()
            // returns).
            builder.drainOrphans(false);
        }
        // Recovery delegates are internal, short-lived, OFF-mode drain senders;
        // don't surface their connect/error events to the user's callbacks.
        return (forRecovery ? builder : applyUserCallbacks(builder)).build();
    }

    /**
     * Reserves and returns the lowest free SF slot index. The borrow() cap
     * check ({@code all.size() + inFlightCreations + closingSlots + leakedSlots
     * < maxSize}) guarantees a free index exists whenever a creation is
     * admitted, so this never fails in practice; the guard throws defensively rather than
     * silently colliding two senders on one slot dir. Caller must hold
     * {@code lock}.
     */
    private int allocateSlotIndex() {
        for (int i = 0; i < slotInUse.length; i++) {
            if (!slotInUse[i]) {
                slotInUse[i] = true;
                return i;
            }
        }
        throw new IllegalStateException(
                "no free SF slot index -- pool capacity invariant violated");
    }

    /**
     * Returns an SF slot index to the free set. No-op for non-SF pools and
     * for the {@code -1} sentinel. Caller must hold {@code lock}.
     */
    private void freeSlotIndex(int idx) {
        if (idx >= 0 && slotInUse != null) {
            slotInUse[idx] = false;
        }
    }

    /**
     * Whether the delegate's {@code close()} released the SF slot flock. A
     * non-QWP delegate never holds an SF flock, so it is always treated as
     * released. A {@link QwpWebSocketSender} reports it via
     * {@link QwpWebSocketSender#isSlotLockReleased()} -- false means close()
     * bailed early with the I/O thread still running and the flock still held.
     */
    private static boolean flockReleased(SenderSlot s) {
        Sender d = s.delegate();
        return !(d instanceof QwpWebSocketSender) || ((QwpWebSocketSender) d).isSlotLockReleased();
    }

    /**
     * Reclaims one SF slot after its delegate's {@code close()} has been
     * attempted. When the flock was released the index returns to the free
     * set; when {@code close()} returned with the flock still held (the I/O
     * thread refused to stop) the slot is retired permanently --
     * {@code leakedSlots++} and {@code slotInUse[idx]} stays set -- so the cap
     * math accounts for the lost capacity and no later borrow ever reuses the
     * still-locked dir. Either way {@code closingSlots} is decremented.
     * <p>
     * Caller must hold {@code lock} and is responsible for signalling waiters
     * (only the free path admits a new creation). Shared by
     * {@link #discardBroken} and {@link #reapIdle}.
     *
     * @param s       sender whose slot is being reclaimed ({@code slotIndex() >= 0})
     * @param context phrase woven into the retire WARN to name the reclaim
     *                path (e.g. {@code ""} or {@code " during idle reaping"})
     * @return {@code true} if the index was freed, {@code false} if retired
     */
    private boolean reclaimSlot(SenderSlot s, String context) {
        closingSlots--;
        if (flockReleased(s)) {
            freeSlotIndex(s.slotIndex());
            return true;
        }
        leakedSlots++;
        LOG.warn("SF slot {} retired permanently{}: delegate close() returned with the flock still held " +
                        "(I/O thread refused to stop); pool capacity reduced by 1, now {} of {} usable [leakedSlots={}]",
                s.slotIndex(), context, maxSize - leakedSlots, maxSize, leakedSlots);
        return false;
    }
}
