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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.IntList;
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
    private final long acquireTimeoutMillis;
    private final ArrayList<PooledSender> all;
    private final ArrayDeque<PooledSender> available;
    private final String configurationString;
    private final long idleTimeoutMillis;
    // Test seam. Production builds delegates via defaultSender(); white-box
    // tests in io.questdb.client.test.impl reach the package-private
    // constructor by reflection to inject a factory that throws a non-
    // RuntimeException Throwable (e.g. an -ea AssertionError) mid-prewarm,
    // exercising the Error-safe delegate cleanup loop.
    private final IntFunction<Sender> senderFactory;
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
    private final ThreadLocal<PooledSender> threadAffine = new ThreadLocal<>();
    // Slots removed from `all` whose delegate is still releasing its flock.
    // They keep reserving capacity (and their slotInUse mark) until the
    // flock drops, so the cap check and the slot allocator stay consistent
    // and no concurrent borrow can reclaim a slot dir that is still locked.
    // Guarded by lock. Only ever ticks for SF slots.
    private int closingSlots;
    private volatile boolean closed;
    private int inFlightCreations;
    // Slots whose delegate close() returned with the SF flock still held
    // (the I/O thread refused to stop). Permanently consumed: the index is
    // never freed and never reused, so no borrow ever hands out a still-
    // locked slot dir. Counted in the cap check so the lost capacity is
    // accounted for. Guarded by lock; only ever ticks for SF slots.
    private int leakedSlots;
    // SF slots currently held by the in-range startup-recovery pass
    // (recoverStrandedManagedSlots): each is reserved under `lock` for the
    // duration of its drain and counted in the borrow() cap check so a
    // concurrent borrow can neither over-allocate past maxSize nor target a
    // dir being recovered. Only ever non-zero on the deferred (housekeeper-
    // driven) recovery path, where recovery overlaps borrow()/return; on the
    // inline construction path the pool is still single-threaded. Guarded by
    // lock; only ever ticks for SF slots.
    private int recoveringSlots;
    // True once recoverStrandedManagedSlots() has been started for this pool
    // (inline at construction, or on the first PoolHousekeeper tick when the
    // pooled handle defers it). Guarded by lock; makes runStartupRecoveryOnce()
    // idempotent.
    private boolean startupRecoveryDone;

    public SenderPool(
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

    // Package-private constructor exposing the senderFactory test seam:
    // production passes null (-> the real defaultSender()). White-box tests in
    // io.questdb.client.test.impl reach this by reflection to inject a factory
    // that throws a non-RuntimeException Throwable mid-prewarm. Recovery runs
    // inline here (deferStartupRecovery=false); the pooled QuestDB handle uses
    // the 8-arg overload to defer it to the housekeeper thread.
    SenderPool(
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

    // Full constructor. deferStartupRecovery=true skips the inline,
    // construction-time SF recovery (recoverStrandedManagedSlots) so
    // QuestDB.build() never blocks on a slow or reachable-but-not-acking
    // server; the owner (QuestDBImpl) then drives recovery on the
    // PoolHousekeeper thread via runStartupRecoveryOnce(). The in-range
    // recovery pass is concurrency-safe against borrow()/return on that
    // deferred path -- see recoverStrandedManagedSlots().
    SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory,
            boolean deferStartupRecovery
    ) {
        if (minSize < 0 || maxSize < 1 || minSize > maxSize) {
            throw new IllegalArgumentException("invalid pool sizing: min=" + minSize + ", max=" + maxSize);
        }
        this.senderFactory = senderFactory != null ? senderFactory : this::defaultSender;
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
                PooledSender ps = createUnlocked(storeAndForward ? i : -1);
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
            runStartupRecoveryOnce();
        }
    }

    /**
     * Runs {@link #recoverStrandedManagedSlots()} at most once for this pool.
     * No-op when SF is off, when the pool is already closed, or when recovery
     * has already been started (inline at construction, or on an earlier
     * housekeeper tick).
     * <p>
     * The pooled {@code QuestDB} handle constructs its {@link SenderPool} with
     * {@code deferStartupRecovery=true} and drives recovery through this method
     * from the {@link PoolHousekeeper} thread, so {@code QuestDB.build()}
     * returns without blocking on a slow or reachable-but-not-acking server.
     * Safe to call while {@link #borrow()} and return run concurrently -- see
     * the concurrency note on {@link #recoverStrandedManagedSlots()}.
     */
    void runStartupRecoveryOnce() {
        if (!storeAndForward || closed) {
            return;
        }
        lock.lock();
        try {
            if (startupRecoveryDone) {
                return;
            }
            startupRecoveryDone = true;
        } finally {
            lock.unlock();
        }
        recoverStrandedManagedSlots();
    }

    /**
     * Best-effort, one-shot recovery of unacked data left in this pool's own
     * managed SF slots by a previous run -- both the in-range slots
     * {@code [0, maxSize)} and the same-base slots a larger previous run left
     * OUT of range ({@code <base>-i} with {@code i >= maxSize}).
     * <p>
     * Every pooled SF sender's orphan drainer deliberately excludes the whole
     * {@code [0, maxSize)} managed range (via
     * {@code orphanDrainExcludeManagedSlots}) so a sibling never adopts a slot
     * dir/lock the pool intends to (re)create -- that exclusion is what keeps
     * the per-slot ids from resurfacing "sf slot already in use". The
     * trade-off is that an in-range slot left holding unacked data is otherwise
     * recovered ONLY when the pool happens to (re)create that index; under
     * steady low load the pool may never grow to a high index, stranding that
     * slot's data (durable on disk, but undelivered) until a later restart or
     * a load spike. An out-of-range slot is worse off still: the pool never
     * re-creates its index at all, and the per-sender drainer only adopts it
     * when {@code drain_orphans=on}, so under the default config its data would
     * sit durable-but-undelivered indefinitely. This method closes both gaps by
     * recovering such slots once, here at construction, regardless of
     * {@code drain_orphans}.
     * <p>
     * It runs either inline at construction (single-threaded, unpublished) or,
     * for the pooled {@code QuestDB} handle, on the {@link PoolHousekeeper}
     * thread shortly after publication -- concurrently with {@link #borrow()}
     * and return. Either way the in-range pass reserves each slot index under
     * {@code lock} for the duration of its recovery AND counts it in the
     * borrow() capacity check (via {@code recoveringSlots}), so a concurrent
     * borrow can neither target the dir being recovered nor over-allocate past
     * {@code maxSize} -- the cannibalization race the drainer exclusion prevents
     * cannot occur here either. Prewarmed slots (already live, holding their
     * flock) are skipped; they deliver their recovered data through normal use.
     * The out-of-range pass needs no reservation: those indices are outside
     * {@code [0, maxSize)}, have no {@code slotInUse} entry, and are never
     * allocated by borrow().
     * <p>
     * Every step is best-effort: a slot with no unacked data is a cheap
     * directory probe; an unreachable server, a slow drain, or a build/close
     * Error is logged and never fails construction, since the data stays
     * durable on disk for a later attempt. A single shared wall-clock budget
     * (one {@code acquireTimeoutMillis}) caps the WHOLE recovery -- both passes
     * -- and the first build/drain failure stops the scan.
     */
    private void recoverStrandedManagedSlots() {
        if (!storeAndForward || sfDir == null || !Files.exists(sfDir)) {
            return;
        }
        // Shared wall-clock budget for the WHOLE scan, not per slot. Without
        // it a reachable-but-not-acking server pays a full drain timeout on
        // every stranded slot, so construction could block for up to
        // (maxSize - minSize) * acquireTimeoutMillis. One acquire timeout is
        // the ceiling already accepted for a single borrow, so we reuse it as
        // the total recovery budget: once it is spent the scan stops and the
        // remaining slots wait for a later attempt (their data stays durable
        // on disk).
        final long recoveryDeadlineNanos =
                System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
        final boolean[] flockHeld = new boolean[1];
        boolean stopScan = false;

        // Pass 1: in-range managed slots [0, maxSize). Reserve each index so no
        // concurrent borrow can target the dir; prewarmed slots are skipped.
        for (int i = 0; i < maxSize; i++) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(
                    recoveryDeadlineNanos - System.nanoTime());
            if (remainingMillis <= 0) {
                LOG.warn("startup SF recovery: {}ms budget exhausted; "
                        + "skipping remaining slots", acquireTimeoutMillis);
                stopScan = true;
                break;
            }
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
                continue;
            }
            String slotPath = sfDir + "/" + slotBaseId + "-" + i;
            stopScan = drainCandidateSlotForRecovery(i, slotPath, remainingMillis, flockHeld);
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
                break;
            }
        }

        if (stopScan) {
            // Budget exhausted, or a build/drain failure that will very likely
            // repeat -- do not start the out-of-range pass; that data stays
            // durable on disk for a later attempt.
            return;
        }

        // Pass 2: same-base slots a previous run left OUT of the current index
        // range (<base>-i with i >= maxSize, from a run with a larger maxSize).
        // The pool never re-creates these indices, and the per-sender orphan
        // drainer only adopts them when drain_orphans=on -- so without this pass
        // their unacked data would sit durable-but-undelivered under the default
        // config. They are outside [0, maxSize), so they have no slotInUse entry
        // and never affect the borrow() cap math; the pool is single-threaded
        // and unpublished here and never allocates these indices, so no
        // reservation is needed and the cannibalization race cannot occur.
        IntList outOfRange = OrphanScanner.listStrandedOutOfRangeManagedSlots(
                sfDir, slotBaseId, maxSize);
        for (int k = 0, n = outOfRange.size(); k < n; k++) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(
                    recoveryDeadlineNanos - System.nanoTime());
            if (remainingMillis <= 0) {
                LOG.warn("startup SF recovery: {}ms budget exhausted; "
                        + "skipping remaining out-of-range slots", acquireTimeoutMillis);
                break;
            }
            int idx = outOfRange.getQuick(k);
            String slotPath = sfDir + "/" + slotBaseId + "-" + idx;
            boolean stop = drainCandidateSlotForRecovery(idx, slotPath, remainingMillis, flockHeld);
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
            if (stop) {
                break;
            }
        }
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
        // createUnlocked() takes the slot flock on <base>-slotIndex, and
        // delegate().close() can early-return with the I/O thread still running
        // (flock still held).
        PooledSender recoverer = null;
        boolean stopScan = false;
        try {
            if (!OrphanScanner.isCandidateOrphan(slotPath)) {
                return false;
            }
            try {
                recoverer = createUnlocked(slotIndex);
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
                if (!recoverer.drain(remainingMillis)) {
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
                    PooledSender s = available.pollFirst();
                    s.markInUse();
                    return s;
                }
                if (all.size() + inFlightCreations + closingSlots + leakedSlots + recoveringSlots < maxSize) {
                    inFlightCreations++;
                    // Reserve a slot index under the lock so concurrent
                    // creations never target the same SF slot dir. -1 when
                    // SF is off (no per-slot identity needed).
                    int slotIndex = storeAndForward ? allocateSlotIndex() : -1;
                    lock.unlock();
                    PooledSender created;
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
                        // by close() already.
                        freeSlotIndex(slotIndex);
                        try {
                            created.delegate().close();
                        } catch (Throwable ignored) {
                            // Best-effort: an Error (e.g. -ea AssertionError)
                            // from teardown must not mask the closed-pool signal.
                        }
                        throw new LineSenderException("QuestDB handle is closed");
                    }
                    all.add(created);
                    created.markInUse();
                    return created;
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

    @Override
    public void close() {
        PooledSender[] snapshot;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            // Mark every pooled wrapper invalidated so pinToCurrentThread()
            // on other threads -- which never takes this lock -- can detect
            // that its cached entry no longer wraps a live delegate. Removing
            // the calling thread's ThreadLocal only clears one slot; other
            // threads' slots survive until they read the flag.
            for (int i = 0; i < all.size(); i++) {
                all.get(i).markInvalidated();
            }
            // Snapshot under the lock so the delegate-close loop below is
            // immune to concurrent mutation of `all`. discardBroken running
            // on another thread can still bail thanks to the `closed` check
            // it now performs; the snapshot is belt-and-braces for any
            // future code path that mutates `all` outside this lock's
            // happens-before chain.
            snapshot = all.toArray(new PooledSender[0]);
            threadAffine.remove();
            slotReleased.signalAll();
        } finally {
            lock.unlock();
        }
        // Close each delegate from the snapshot, outside the lock so a slow
        // real-close() doesn't keep the pool latched.
        for (int i = 0; i < snapshot.length; i++) {
            try {
                snapshot[i].delegate().close();
            } catch (Throwable ignored) {
                // Best-effort: an Error from one delegate's teardown must not
                // abort the loop and strand the remaining delegates unclosed.
            }
        }
    }

    /**
     * Clears the current thread's pin if it currently references {@code s}.
     * Invoked from {@link PooledSender#close()} before the wrapper is
     * returned to the pool, so a subsequent {@link #pinToCurrentThread()}
     * on this thread cannot hand the wrapper back after another consumer
     * has borrowed the slot. No-op when the caller never pinned, or pinned
     * a different wrapper.
     */
    void clearPinIfCurrent(PooledSender s) {
        if (threadAffine.get() == s) {
            threadAffine.remove();
        }
    }

    /**
     * Evicts a slot whose delegate has failed (typically a {@code flush()}
     * failure observed in {@link PooledSender#close()}). The wrapper is
     * marked invalidated so any thread-pinned reference gets rejected on the
     * next {@link #pinToCurrentThread()} call; the slot is removed from
     * {@code all} so the pool can grow back into a fresh slot on demand. The
     * underlying delegate is closed outside the lock so a slow real-close
     * does not stall other borrowers.
     * <p>
     * Bails when the pool is already closed: {@link #close()} owns the
     * teardown of every delegate via its snapshot loop, so mutating
     * {@code all} here would race that iteration on a non-thread-safe
     * {@code ArrayList} and the {@code delegate.close()} below would be a
     * double-close on a delegate {@code close()} has already shut down.
     */
    void discardBroken(PooledSender s) {
        s.markInvalidated();
        boolean reserved = false;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            boolean removed = all.remove(s);
            // For an SF slot, keep its index reserved (move the reservation
            // from `all` to `closingSlots`) until the delegate below releases
            // the flock. Capacity stays accounted for, so a concurrent borrow
            // cannot reclaim this slot dir while its lock is still held.
            if (removed && s.slotIndex() >= 0) {
                closingSlots++;
                reserved = true;
            }
            // Wake one waiter -- the cap check in borrow() may now admit a
            // creation attempt (on a *different* slot).
            slotReleased.signal();
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
            // Either way the slot accounting in the finally below MUST run,
            // otherwise an SF slot stays reserved forever (slotInUse stuck
            // true, closingSlots over-counted) and the pool leaks capacity
            // until borrow() can only ever time out.
        } finally {
            if (reserved) {
                lock.lock();
                try {
                    // Free the index only when the flock was released; a slot
                    // left locked is retired permanently. Signal a waiter only
                    // on the free path, where a new creation can now be admitted.
                    if (reclaimSlot(s, "")) {
                        slotReleased.signal();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    public void giveBack(PooledSender s) {
        long now = System.currentTimeMillis();
        s.markIdleAt(now);
        lock.lock();
        try {
            if (closed) {
                // Pool already shut down: don't requeue; let close() finish destroying.
                return;
            }
            available.addLast(s);
            slotReleased.signal();
        } finally {
            lock.unlock();
        }
    }

    public PooledSender pinToCurrentThread() {
        PooledSender pinned = threadAffine.get();
        if (pinned != null && !pinned.isInvalidated()) {
            return pinned;
        }
        if (pinned != null) {
            threadAffine.remove();
        }
        PooledSender s = borrow();
        threadAffine.set(s);
        return s;
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
        ArrayList<PooledSender> toClose = null;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            Iterator<PooledSender> it = available.iterator();
            while (it.hasNext() && all.size() > minSize) {
                PooledSender s = it.next();
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
                        PooledSender s = toClose.get(i);
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

    public void releaseCurrentThread() {
        PooledSender pinned = threadAffine.get();
        if (pinned == null) {
            return;
        }
        threadAffine.remove();
        if (pinned.isInvalidated()) {
            // Pool was closed: delegate is already closed, skip flush/giveBack.
            return;
        }
        pinned.close();
    }

    private PooledSender createUnlocked(int slotIndex) {
        return new PooledSender(senderFactory.apply(slotIndex), this, slotIndex);
    }

    private Sender defaultSender(int slotIndex) {
        final Sender raw;
        if (storeAndForward) {
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
            // (recoverStrandedManagedSlots, pass 2), which adopts these
            // out-of-range same-base slots at construction REGARDLESS of
            // drain_orphans -- so the default config never strands it. The
            // per-sender drainer is an additional path that only runs when
            // drain_orphans=on; foreign leftovers under other names are drained
            // only by that path.
            raw = Sender.builder(configurationString)
                    .senderId(slotBaseId + "-" + slotIndex)
                    .orphanDrainExcludeManagedSlots(slotBaseId, maxSize)
                    .build();
        } else {
            raw = Sender.fromConfig(configurationString);
        }
        return raw;
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
    private static boolean flockReleased(PooledSender s) {
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
    private boolean reclaimSlot(PooledSender s, String context) {
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
