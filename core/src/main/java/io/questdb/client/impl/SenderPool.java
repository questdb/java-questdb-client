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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

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
 * check ({@code allSize + inFlightCreations < maxSize}) stays correct under
 * concurrent borrows.
 */
public final class SenderPool implements AutoCloseable {

    private final long acquireTimeoutMillis;
    private final ArrayList<PooledSender> all;
    private final ArrayDeque<PooledSender> available;
    private final String configurationString;
    private final long idleTimeoutMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxLifetimeMillis;
    private final int maxSize;
    private final int minSize;
    private final Condition slotReleased;
    private final ThreadLocal<PooledSender> threadAffine = new ThreadLocal<>();
    private volatile boolean closed;
    private int inFlightCreations;

    public SenderPool(
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
        this.slotReleased = lock.newCondition();
        // Pre-warm minSize connections.
        int built = 0;
        try {
            for (int i = 0; i < minSize; i++) {
                PooledSender ps = createUnlocked();
                all.add(ps);
                available.add(ps);
                built++;
            }
        } catch (RuntimeException e) {
            for (int i = 0; i < built; i++) {
                try {
                    all.get(i).delegate().close();
                } catch (RuntimeException ignored) {
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
                if (all.size() + inFlightCreations < maxSize) {
                    inFlightCreations++;
                    lock.unlock();
                    PooledSender created;
                    try {
                        created = createUnlocked();
                    } catch (RuntimeException e) {
                        lock.lock();
                        inFlightCreations--;
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
                        try {
                            created.delegate().close();
                        } catch (RuntimeException ignored) {
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
            } catch (RuntimeException ignored) {
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
        lock.lock();
        try {
            if (closed) {
                return;
            }
            all.remove(s);
            // Wake one waiter -- the cap check in borrow() uses all.size(),
            // so a freed slot may now allow a creation attempt.
            slotReleased.signal();
        } finally {
            lock.unlock();
        }
        try {
            s.delegate().close();
        } catch (RuntimeException ignored) {
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
                } catch (RuntimeException ignored) {
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

    private PooledSender createUnlocked() {
        Sender raw = Sender.fromConfig(configurationString);
        return new PooledSender(raw, this);
    }
}
