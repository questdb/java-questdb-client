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

    private final long createdAtMillis;
    private final Sender delegate;
    private final SenderPool pool;
    private final int slotIndex;
    // Monotonic lease id. Mutated only under the SenderPool lock (bumped in
    // borrow() when the slot is handed out and in giveBack()/discardBroken()
    // when it is returned). A PooledSender wrapper captures it live for its
    // borrow; once the slot is released or re-borrowed the captured id no
    // longer matches. Volatile so a stale handle on another thread observes
    // the latest value without taking the pool lock.
    private volatile long generation;
    private volatile long idleSinceMillis;

    SenderSlot(Sender delegate, SenderPool pool, int slotIndex) {
        this.delegate = delegate;
        this.pool = pool;
        this.slotIndex = slotIndex;
        this.createdAtMillis = System.currentTimeMillis();
        this.idleSinceMillis = this.createdAtMillis;
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

    long idleSinceMillis() {
        return idleSinceMillis;
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

    void markIdleAt(long nowMillis) {
        idleSinceMillis = nowMillis;
    }

    SenderPool pool() {
        return pool;
    }

    int slotIndex() {
        return slotIndex;
    }
}
