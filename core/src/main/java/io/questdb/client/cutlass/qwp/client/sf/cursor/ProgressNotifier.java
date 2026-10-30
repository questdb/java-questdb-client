/*******************************************************************************
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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * Generation counter plus a single parked-waiter slot. Producers of progress
 * (the I/O loop after an acked-watermark advance or a terminal error, the
 * segment ring after a durability failure) call {@link #signal()}. A waiter
 * snapshots {@link #generation()}, re-checks its predicate, then calls
 * {@link #awaitChange(long, long)} and loops.
 * <p>
 * No lost wakeup: {@code signal()} increments the generation (a volatile
 * write) and only then reads the waiter slot (a volatile read). The waiter
 * CASes itself into the slot (a volatile write) and only then re-reads the
 * generation (a volatile read). All four accesses are volatile, hence totally
 * ordered. If the signaller's slot read does not see the waiter, then either
 * the waiter's CAS is ordered after that read, and so after the increment, so
 * the waiter's generation read sees the change and it does not park, or the
 * waiter already CASed in and out before that read, so its park has ended and
 * its caller re-checks the predicate on the next pass. Otherwise the
 * signaller sees the waiter and unparks it; a permit granted before the park
 * makes the park return immediately. A stale permit left over from an earlier
 * signal only makes a later park return early, so callers must re-check their
 * predicate in a loop.
 * <p>
 * Only one thread holds the slot at a time. A second concurrent waiter does
 * not register and falls back to a short bounded park, so it still observes
 * progress within {@link #CONTENDED_PARK_NANOS}. The wait and signal paths do
 * not allocate.
 */
public final class ProgressNotifier {
    /**
     * Park bound for a waiter that could not take the waiter slot.
     */
    public static final long CONTENDED_PARK_NANOS = 50_000L;
    /**
     * Safety cap for one park.
     */
    public static final long MAX_PARK_NANOS = 10_000_000L;
    private static final AtomicReferenceFieldUpdater<ProgressNotifier, Thread> WAITER =
            AtomicReferenceFieldUpdater.newUpdater(ProgressNotifier.class, Thread.class, "waiter");
    private final AtomicLong awaitCalls = new AtomicLong();
    private final AtomicLong generation = new AtomicLong();
    private volatile Thread waiter;

    /**
     * Parks the calling thread until the generation differs from
     * {@code expectedGeneration}, or until the park bound elapses. The bound is
     * {@code min(maxParkNanos, MAX_PARK_NANOS)}, or
     * {@code min(maxParkNanos, CONTENDED_PARK_NANOS)} when another thread holds
     * the waiter slot. A non-positive {@code maxParkNanos} does not park.
     *
     * @return {@code true} if the generation differs from
     * {@code expectedGeneration} as read on exit
     */
    public boolean awaitChange(long expectedGeneration, long maxParkNanos) {
        awaitCalls.incrementAndGet();
        if (maxParkNanos <= 0) {
            return generation.get() != expectedGeneration;
        }
        Thread self = Thread.currentThread();
        if (WAITER.compareAndSet(this, null, self)) {
            try {
                if (generation.get() == expectedGeneration) {
                    LockSupport.parkNanos(this, Math.min(maxParkNanos, MAX_PARK_NANOS));
                }
            } finally {
                WAITER.compareAndSet(this, self, null);
            }
        } else {
            if (generation.get() == expectedGeneration) {
                LockSupport.parkNanos(this, Math.min(maxParkNanos, CONTENDED_PARK_NANOS));
            }
        }
        return generation.get() != expectedGeneration;
    }

    /**
     * Number of {@link #awaitChange(long, long)} calls so far (test diagnostics).
     */
    public long awaitCount() {
        return awaitCalls.get();
    }

    public long generation() {
        return generation.get();
    }

    /**
     * Increments the generation, then unparks the registered waiter, if any.
     */
    public void signal() {
        generation.incrementAndGet();
        Thread t = waiter;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }
}
