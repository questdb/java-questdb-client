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

/**
 * One reusable {@link SenderPool} slot: owns a real {@link Sender} delegate, its
 * store-and-forward slot index, and the idle/age bookkeeping the pool needs.
 * Pre-allocated once per slot and held in the pool's {@code all}/{@code
 * available} collections across borrows; it is never handed to callers
 * directly.
 * <p>
 * Each borrow wraps the slot in a fresh {@link PooledSender} stamped with the
 * slot's current lease {@link #generation}. Because the slot is shared across
 * borrows, a stale handle's {@code close()} or data write must not release, or
 * write through, a slot a later borrower now owns. The generation -- mutated
 * only under the pool lock when the slot is handed out and returned -- is what
 * lets {@link #live(long)} and {@link SenderPool#giveBack}/{@link
 * SenderPool#discardBroken} detect and drop such stale calls. This is the
 * ingest-side mirror of the egress {@code QueryWorker} generation guard.
 */
final class SenderSlot {

    // The idle mark of a slot whose release read no clock, a release after
    // appends without a commit window. The next SenderPool.reapIdle() sweep
    // stamps such a slot with its own reading.
    static final long UNSTAMPED = Long.MIN_VALUE;
    private final long createdAtMillis;
    private final Sender delegate;
    private final SenderPool pool;
    private final int slotIndex;
    // Commit-window state, guarded by the lease: only the thread holding the
    // slot's current lease (a borrower or the settle step) reads or writes it.
    // Long.MIN_VALUE = no group open since the last commit.
    private long groupOpenedAtNanos = Long.MIN_VALUE;
    // Index in SenderPool.retiredSlots, or -1 while this slot is not retired.
    // Guarded by the pool lock. The pool maintains this index across swap
    // removals so the delegate's release callback can recover this exact slot
    // without scanning every other retired slot.
    private int retiredIndex = -1;
    // Commit-window state, guarded by the lease (see groupOpenedAtNanos).
    private long rowsAppendedAtLastCommit;
    private boolean settleFailureLogged;
    // Monotonic lease id. Mutated only under the SenderPool lock (bumped in
    // borrow() when the slot is handed out and in giveBack()/discardBroken()
    // when it is returned). A PooledSender wrapper captures it live for its
    // borrow; once the slot is released or re-borrowed the captured id no
    // longer matches. Volatile so a stale handle on another thread observes
    // the latest value without taking the pool lock.
    private volatile long generation;
    // System.nanoTime() reading the idle timeout counts from, or UNSTAMPED.
    // After construction only giveBack() and reapIdle() write it, under the
    // pool lock.
    private volatile long idleSinceNanos;

    SenderSlot(Sender delegate, SenderPool pool, int slotIndex) {
        this.delegate = delegate;
        this.pool = pool;
        this.slotIndex = slotIndex;
        this.createdAtMillis = System.currentTimeMillis();
        this.idleSinceNanos = System.nanoTime();
    }

    /**
     * Advances the lease generation. Called by {@link SenderPool} under the
     * pool lock when the slot is handed out (borrow) and when it is returned
     * (giveBack/discardBroken).
     */
    void bumpGeneration() {
        generation++;
    }

    long createdAtMillis() {
        return createdAtMillis;
    }

    Sender delegate() {
        return delegate;
    }

    long generation() {
        return generation;
    }

    long idleSinceNanos() {
        return idleSinceNanos;
    }

    /**
     * Validates the borrowing lease's {@code gen} and returns the underlying
     * delegate for a data-plane call. Throws if the lease is stale (the slot
     * was returned to the pool or re-borrowed), so a stale handle cannot write
     * into a slot a later borrower owns. Called by {@link PooledSender} on
     * every operation.
     */
    Sender live(long gen) {
        if (gen != generation) {
            throw new IllegalStateException("sender handle is closed (returned to the pool)");
        }
        return delegate;
    }

    /**
     * Records that everything appended so far is committed. Called after a
     * committing {@code flush()} on the delegate, by a release or by the settle.
     */
    void markCommitted(long rowsAppended) {
        rowsAppendedAtLastCommit = rowsAppended;
        groupOpenedAtNanos = Long.MIN_VALUE;
    }

    void markIdleAt(long nowNanos) {
        idleSinceNanos = nowNanos;
    }

    SenderPool pool() {
        return pool;
    }

    int retiredIndex() {
        return retiredIndex;
    }

    void retiredIndex(int retiredIndex) {
        this.retiredIndex = retiredIndex;
    }

    void settleFailureLogged(boolean logged) {
        this.settleFailureLogged = logged;
    }

    boolean settleFailureLogged() {
        return settleFailureLogged;
    }

    int slotIndex() {
        return slotIndex;
    }

    /**
     * Whether the commit window should close at this release. Rows in the
     * window are counted from appends, so the answer does not depend on
     * whether rows shipped at close or by auto-flush. The first release that
     * finds work after a commit opens the group's clock.
     */
    boolean windowCloses(long rowsAppended, boolean deferredOpen, long nowNanos, int commitRows, long commitIntervalNanos) {
        long inWindow = rowsAppended - rowsAppendedAtLastCommit;
        if (inWindow <= 0 && !deferredOpen) {
            return false;
        }
        if (groupOpenedAtNanos == Long.MIN_VALUE) {
            groupOpenedAtNanos = nowNanos;
        }
        return inWindow >= commitRows || nowNanos - groupOpenedAtNanos >= commitIntervalNanos;
    }
}
