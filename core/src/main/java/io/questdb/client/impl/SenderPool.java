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
    // that throws a non-RuntimeException Throwable mid-prewarm.
    SenderPool(
            String configurationString,
            int minSize,
            int maxSize,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            IntFunction<Sender> senderFactory
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
                if (all.size() + inFlightCreations + closingSlots + leakedSlots < maxSize) {
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
                    if (flockReleased(s)) {
                        // Flock is released now: return the reserved slot
                        // index to the free set.
                        freeSlotIndex(s.slotIndex());
                        closingSlots--;
                        slotReleased.signal();
                    } else {
                        // close() leaked the still-running I/O thread; the
                        // flock is still held. Retire the slot permanently:
                        // keep slotInUse[idx] set and move it from
                        // closingSlots to leakedSlots so the cap math
                        // accounts for the lost capacity and no borrow ever
                        // reuses the still-locked dir.
                        closingSlots--;
                        leakedSlots++;
                        LOG.warn("SF slot {} retired permanently: delegate close() returned with the flock still held " +
                                        "(I/O thread refused to stop); pool capacity reduced by 1, now {} of {} usable [leakedSlots={}]",
                                s.slotIndex(), maxSize - leakedSlots, maxSize, leakedSlots);
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
                            if (flockReleased(s)) {
                                freeSlotIndex(s.slotIndex());
                                closingSlots--;
                            } else {
                                closingSlots--;
                                leakedSlots++;
                                LOG.warn("SF slot {} retired permanently during idle reaping: delegate close() returned " +
                                                "with the flock still held (I/O thread refused to stop); pool capacity reduced by 1, " +
                                                "now {} of {} usable [leakedSlots={}]",
                                        s.slotIndex(), maxSize - leakedSlots, maxSize, leakedSlots);
                            }
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
            // Also fence off the pool's own "<base>-" namespace from orphan
            // draining: the pool co-manages every <base>-<index> slot and
            // recovers each slot's unacked data when it (re)creates it, so a
            // sibling's startup drainer must never adopt another pool slot's
            // dir/lock (that would resurrect "sf slot already in use"). This
            // is a no-op unless the config also set drain_orphans=on; foreign
            // leftovers under other names are still drained.
            raw = Sender.builder(configurationString)
                    .senderId(slotBaseId + "-" + slotIndex)
                    .orphanDrainExcludePrefix(slotBaseId + "-")
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
}
