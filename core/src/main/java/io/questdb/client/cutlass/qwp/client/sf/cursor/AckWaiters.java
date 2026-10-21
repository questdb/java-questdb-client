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

import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

/**
 * Threads parked for ack progress: the producer in a close drain plus any
 * monitor threads in {@code awaitAckedFsn}. Owned by the Sender, so it outlives
 * the I/O loops and engines a recycle replaces. A waiter registers, re-checks
 * its condition, parks, deregisters; the I/O thread advances the watermark and
 * then calls {@link #signal()}. Either the waiter sees the new value or the
 * signal sees the waiter. {@link #signal()} allocates nothing.
 */
public final class AckWaiters {
    private static final Thread[] NONE = new Thread[0];
    private volatile Thread[] waiters = NONE;

    public synchronized void add(Thread t) {
        Thread[] w = waiters;
        Thread[] n = Arrays.copyOf(w, w.length + 1);
        n[w.length] = t;
        waiters = n;
    }

    public synchronized void remove(Thread t) {
        Thread[] w = waiters;
        for (int i = 0; i < w.length; i++) {
            if (w[i] == t) {
                Thread[] n = new Thread[w.length - 1];
                System.arraycopy(w, 0, n, 0, i);
                System.arraycopy(w, i + 1, n, i, w.length - i - 1);
                waiters = n;
                return;
            }
        }
    }

    /**
     * Unparks every registered waiter. Safe from any thread; spurious wakes are
     * fine because waiters re-check their condition.
     */
    public void signal() {
        Thread[] w = waiters;
        for (int i = 0; i < w.length; i++) {
            LockSupport.unpark(w[i]);
        }
    }
}
