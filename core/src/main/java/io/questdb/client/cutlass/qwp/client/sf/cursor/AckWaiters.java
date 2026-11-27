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

import java.util.concurrent.locks.LockSupport;

/**
 * Threads parked for ack progress: the producer in a close drain plus any
 * monitor threads in {@code awaitAckedFsn}. Owned by the Sender, so it outlives
 * the I/O loops and engines a recycle replaces. A waiter registers once per
 * call, then loops check / park until done, and deregisters; the I/O thread
 * advances the watermark and then calls {@link #signal()}. Either the waiter's
 * check sees the new value or the signal sees the waiter, and an unpark that
 * lands between check and park is kept as the park permit.
 * <p>
 * Waiters live in a fixed slot array: {@link #add} takes a null slot and
 * {@link #remove} nulls it again, so steady-state registration allocates
 * nothing; the array only grows (doubles, under the lock) when more threads
 * wait at once than ever before. {@link #signal()} is lock-free and
 * allocation-free: it reads the volatile array and unparks every non-null
 * slot. A slot nulled concurrently is read as null (unpark(null) is a no-op)
 * or as the departing thread, which then receives one extra permit; that is
 * harmless because a permit is per thread, consumed by its next park, and
 * every park in this codebase sits in a re-check loop.
 */
public final class AckWaiters {
    private volatile Thread[] slots = new Thread[4];

    public synchronized void add(Thread t) {
        Thread[] s = slots;
        for (int i = 0; i < s.length; i++) {
            if (s[i] == null) {
                s[i] = t;
                // Volatile self-store: orders the slot write before the waiter's
                // first check, so a signal() that misses the slot (its volatile
                // read of the array predates this store) is one whose watermark
                // write that check is guaranteed to see.
                slots = s;
                return;
            }
        }
        Thread[] n = new Thread[s.length * 2];
        System.arraycopy(s, 0, n, 0, s.length);
        n[s.length] = t;
        slots = n;
    }

    public synchronized void remove(Thread t) {
        Thread[] s = slots;
        for (int i = 0; i < s.length; i++) {
            if (s[i] == t) {
                s[i] = null;
                return;
            }
        }
    }

    /**
     * Unparks every registered waiter. Safe from any thread; spurious wakes are
     * fine because waiters re-check their condition.
     */
    public void signal() {
        Thread[] s = slots;
        for (int i = 0; i < s.length; i++) {
            Thread t = s[i];
            if (t != null) {
                LockSupport.unpark(t);
            }
        }
    }
}
