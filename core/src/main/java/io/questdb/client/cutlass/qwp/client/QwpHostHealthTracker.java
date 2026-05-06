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

package io.questdb.client.cutlass.qwp.client;

/**
 * Per-client bookkeeping that ranks the configured endpoint list when picking
 * the next host to try. Mirrors the .NET client's QwpHostHealthTracker.
 * <p>
 * Within a round, {@link #pickNext()} returns the highest-priority host that
 * has not yet been attempted; the caller advances the round via
 * {@link #beginRound(boolean)}.
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
    private final Object lock = new Object();
    private final HostState[] states;

    public QwpHostHealthTracker(int hostCount) {
        if (hostCount <= 0) {
            throw new IllegalArgumentException("hostCount must be > 0");
        }
        this.hostCount = hostCount;
        this.states = new HostState[hostCount];
        this.attemptedThisRound = new boolean[hostCount];
        for (int i = 0; i < hostCount; i++) {
            states[i] = HostState.UNKNOWN;
        }
    }

    /**
     * Resets attempted flags. With {@code forgetClassifications}, every host
     * except the last-known {@link HostState#HEALTHY} entry is reset to
     * {@link HostState#UNKNOWN}; the sticky-Healthy keeps the last successful
     * host first in line on the next round.
     */
    public void beginRound(boolean forgetClassifications) {
        synchronized (lock) {
            int stickyIndex = -1;
            if (forgetClassifications) {
                for (int i = 0; i < hostCount; i++) {
                    if (states[i] == HostState.HEALTHY) {
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
     * when the round is exhausted.
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
        }
    }

    public void recordTransportError(int idx) {
        synchronized (lock) {
            states[idx] = HostState.TRANSPORT_ERROR;
            attemptedThisRound[idx] = true;
        }
    }
}
