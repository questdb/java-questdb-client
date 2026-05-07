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

/**
 * Per-client bookkeeping that ranks the configured endpoint list when picking
 * the next host to try. Mirrors the .NET client's QwpHostHealthTracker.
 * <p>
 * Within a round, {@link #pickNext()} returns the highest-priority host that
 * has not yet been attempted; the caller advances the round via
 * {@link #beginRound(boolean)}.
 * <p>
 * Each method is internally synchronized, but pickNext + recordX is not atomic
 * across the pair. Callers must externally serialize a pick → record sequence
 * (the QWP clients do this via the sender's {@code synchronized buildAndConnect}
 * and the query client's documented one-execute-at-a-time contract).
 */
public final class QwpHostHealthTracker {
    public enum HostState {
        UNKNOWN,
        HEALTHY,
        TRANSIENT_REJECT,
        TRANSPORT_ERROR,
        TOPOLOGY_REJECT,
    }

    private static final HostState[] PRIORITY_ORDER = {
            HostState.HEALTHY,
            HostState.UNKNOWN,
            HostState.TRANSIENT_REJECT,
            HostState.TRANSPORT_ERROR,
            HostState.TOPOLOGY_REJECT,
    };

    private final boolean[] attemptedThisRound;
    private final int hostCount;
    private final long[] lastSuccessEpoch;
    private final Object lock = new Object();
    private final HostState[] states;
    private long successEpoch;

    public QwpHostHealthTracker(int hostCount) {
        if (hostCount <= 0) {
            throw new IllegalArgumentException("hostCount must be > 0");
        }
        this.hostCount = hostCount;
        this.states = new HostState[hostCount];
        this.attemptedThisRound = new boolean[hostCount];
        this.lastSuccessEpoch = new long[hostCount];
        for (int i = 0; i < hostCount; i++) {
            states[i] = HostState.UNKNOWN;
        }
    }

    /**
     * Resets attempted flags. With {@code forgetClassifications}, every host
     * except the most-recently-successful {@link HostState#HEALTHY} entry is
     * reset to {@link HostState#UNKNOWN}; the sticky-Healthy keeps the last
     * successful host first in line on the next round. Recency uses the
     * {@code recordSuccess} epoch counter, not array order.
     */
    public void beginRound(boolean forgetClassifications) {
        synchronized (lock) {
            int stickyIndex = -1;
            if (forgetClassifications) {
                long bestEpoch = -1L;
                for (int i = 0; i < hostCount; i++) {
                    if (states[i] == HostState.HEALTHY && lastSuccessEpoch[i] > bestEpoch) {
                        bestEpoch = lastSuccessEpoch[i];
                        stickyIndex = i;
                    }
                }
            }
            for (int i = 0; i < hostCount; i++) {
                attemptedThisRound[i] = false;
                if (forgetClassifications && i != stickyIndex) {
                    states[i] = HostState.UNKNOWN;
                }
            }
        }
    }

    public int count() {
        return hostCount;
    }

    public HostState getState(int idx) {
        synchronized (lock) {
            return states[idx];
        }
    }

    public boolean isRoundExhausted() {
        synchronized (lock) {
            for (int i = 0; i < hostCount; i++) {
                if (!attemptedThisRound[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Returns the highest-priority host not yet attempted this round, or -1
     * when the round is exhausted. The caller is expected to be externally
     * serialized (see class doc): the returned index is intended to be paired
     * with a follow-up {@code recordX(idx)} on the same logical thread.
     */
    public int pickNext() {
        synchronized (lock) {
            for (HostState p : PRIORITY_ORDER) {
                for (int i = 0; i < hostCount; i++) {
                    if (!attemptedThisRound[i] && states[i] == p) {
                        return i;
                    }
                }
            }
            return -1;
        }
    }

    /**
     * Demotes a previously-healthy host on send/receive failure so a subsequent
     * sticky-Healthy reset doesn't preserve it as the priority entry.
     */
    public void recordMidStreamFailure(int idx) {
        synchronized (lock) {
            if (states[idx] == HostState.HEALTHY) {
                states[idx] = HostState.TRANSPORT_ERROR;
            }
        }
    }

    public void recordRoleReject(int idx, boolean isTransient) {
        synchronized (lock) {
            states[idx] = isTransient ? HostState.TRANSIENT_REJECT : HostState.TOPOLOGY_REJECT;
            attemptedThisRound[idx] = true;
        }
    }

    public void recordSuccess(int idx) {
        synchronized (lock) {
            states[idx] = HostState.HEALTHY;
            attemptedThisRound[idx] = true;
            lastSuccessEpoch[idx] = ++successEpoch;
        }
    }

    public void recordTransportError(int idx) {
        synchronized (lock) {
            states[idx] = HostState.TRANSPORT_ERROR;
            attemptedThisRound[idx] = true;
        }
    }
}
