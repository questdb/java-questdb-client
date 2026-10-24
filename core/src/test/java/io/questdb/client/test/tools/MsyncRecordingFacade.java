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

package io.questdb.client.test.tools;

import java.util.Arrays;

/**
 * Records the address and length of every {@code msync} it forwards, so a test
 * can check which range a barrier covered. Thread-safe: the periodic durability
 * worker calls {@code msync} from its own thread.
 */
public final class MsyncRecordingFacade extends DelegatingFilesFacade {
    private long[] addrs = new long[64];
    private long[] lens = new long[64];
    private int msyncCalls;

    /**
     * Number of {@code msync} calls so far.
     */
    public synchronized int calls() {
        return msyncCalls;
    }

    /**
     * Address of the last {@code msync}; -1 when none happened.
     */
    public synchronized long lastAddr() {
        return msyncCalls > 0 ? addrs[msyncCalls - 1] : -1L;
    }

    /**
     * Length of the last {@code msync}; -1 when none happened.
     */
    public synchronized long lastLen() {
        return msyncCalls > 0 ? lens[msyncCalls - 1] : -1L;
    }

    /**
     * Length of the last {@code msync} that started at {@code addr}; -1 when none
     * did.
     */
    public synchronized long lastLenFor(long addr) {
        for (int i = msyncCalls - 1; i >= 0; i--) {
            if (addrs[i] == addr) {
                return lens[i];
            }
        }
        return -1L;
    }

    /**
     * Longest {@code msync} that started at {@code addr} among the calls from the
     * {@code fromCall}-th on; -1 when none did.
     */
    public synchronized long maxLenSince(long addr, int fromCall) {
        long max = -1L;
        for (int i = fromCall; i < msyncCalls; i++) {
            if (addrs[i] == addr && lens[i] > max) {
                max = lens[i];
            }
        }
        return max;
    }

    @Override
    public synchronized int msync(long addr, long len, boolean async) {
        if (msyncCalls == addrs.length) {
            addrs = Arrays.copyOf(addrs, msyncCalls * 2);
            lens = Arrays.copyOf(lens, msyncCalls * 2);
        }
        addrs[msyncCalls] = addr;
        lens[msyncCalls] = len;
        msyncCalls++;
        return super.msync(addr, len, async);
    }
}
