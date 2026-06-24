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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.std.Compat;

import java.util.concurrent.locks.LockSupport;

/**
 * Bounded single-producer single-consumer queue with spin-then-park blocking.
 * <p>
 * Purpose-built for the QwpEgressIoThread hand-off hot paths where
 * {@code java.util.concurrent.ArrayBlockingQueue} costs ~2 microseconds per
 * offer/take cycle under an uncontended lock + park pattern. This queue is
 * lock-free on the fast path and parks only after a short spin window (default
 * ~50 microseconds at current CPU speeds), so queries whose producer arrives
 * within the budget skip park/unpark entirely.
 * <p>
 * Contract:
 * <ul>
 *   <li>Exactly one producer thread may call {@link #offer}.</li>
 *   <li>Exactly one consumer thread may call {@link #poll} or {@link #take}.</li>
 *   <li>Capacity is rounded up to the next power of two.</li>
 * </ul>
 * Behaviour outside these assumptions is undefined.
 */
public final class QwpSpscQueue<T> {

    // Tuned for latency-sensitive localhost workloads: long enough to catch a
    // typical round-trip inside the spin window (~30 microseconds on loopback),
    // short enough not to dominate CPU on idle queues. The spin loop exits
    // early on every iteration that observes a non-empty ring, so this is an
    // upper bound, not a fixed cost.
    private static final int SPIN_ITERATIONS = 2048;
    private final int mask;
    // One-slot handshake so the producer knows whether to unpark the consumer.
    // Null when the consumer is running (no unpark needed); non-null when
    // parked (producer must unpark to release). Volatile write on set ensures
    // the consumer's prior tail read is visible to the producer's queue check.
    private final Object[] slots;
    private volatile Thread consumerThread;
    private volatile long head;
    private volatile long tail;

    public QwpSpscQueue(int capacity) {
        int pow2 = 1;
        while (pow2 < capacity) pow2 <<= 1;
        this.slots = new Object[pow2];
        this.mask = pow2 - 1;
    }

    /**
     * Publishes {@code value} to the consumer. Returns {@code false} when the
     * ring is full (caller may retry or spin externally). Never blocks.
     */
    public boolean offer(T value) {
        final long h = head;
        // Producer-only read of tail -- consumer's volatile write to tail
        // happens-before this read via the queue discipline.
        if (h - tail >= slots.length) {
            return false;
        }
        slots[(int) (h & mask)] = value;
        head = h + 1;  // volatile write publishes the slot to the consumer
        // Wake the consumer if it parked between its last poll and our offer.
        // consumerThread is non-null only when the consumer is inside the park
        // phase of take(); unpark while it is null is a no-op so we stay cheap
        // on the fast path.
        Thread consumer = consumerThread;
        if (consumer != null) {
            LockSupport.unpark(consumer);
        }
        return true;
    }

    /** Non-blocking read. Returns {@code null} when the ring is empty. */
    @SuppressWarnings("unchecked")
    public T poll() {
        final long t = tail;
        if (t == head) {
            return null;
        }
        final int idx = (int) (t & mask);
        T value = (T) slots[idx];
        slots[idx] = null;
        tail = t + 1;
        return value;
    }

    /**
     * Spin-then-park take. Returns the next value, or throws
     * {@link InterruptedException} when the consumer thread was interrupted
     * while waiting.
     */
    public T take() throws InterruptedException {
        // Fast path: the producer beat us here and the value is already visible.
        T value = poll();
        if (value != null) {
            return value;
        }
        // Spin phase: burn a bounded amount of CPU hoping the producer arrives
        // inside the window. onSpinWait lets the CPU slow its pipeline while
        // waiting -- meaningful on hyperthreaded cores.
        for (int i = 0; i < SPIN_ITERATIONS; i++) {
            Compat.onSpinWait();
            if ((value = poll()) != null) {
                return value;
            }
        }
        // Park phase: publish ourselves as the consumer so a subsequent offer
        // sees a non-null reference and unparks us. Re-poll after publishing
        // to close the race where the producer offered between our last poll
        // and the consumerThread assignment.
        consumerThread = Thread.currentThread();
        try {
            while ((value = poll()) == null) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                LockSupport.park();
            }
            return value;
        } finally {
            consumerThread = null;
        }
    }
}
