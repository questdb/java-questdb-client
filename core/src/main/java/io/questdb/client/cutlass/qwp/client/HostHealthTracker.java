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

import java.util.Arrays;

/**
 * Per-host health classification tracker. Mirrors the .NET
 * {@code QwpHostHealthTracker} from the QWP ingress failover spec §1.2.
 * <p>
 * Each address starts in {@link State#UNKNOWN}. As connect attempts
 * resolve, the tracker records the outcome and ranks hosts by priority
 * when the next attempt asks for an unvisited host. Within a "round"
 * (every host attempted at most once) {@link #pickNext()} returns the
 * highest-priority not-yet-attempted host, or {@code -1} when the round
 * is exhausted.
 * <p>
 * State priority order, highest first:
 * <ol>
 *   <li>{@link State#HEALTHY} — last attempt succeeded</li>
 *   <li>{@link State#UNKNOWN} — never attempted, or forgotten by
 *       {@link #beginRound(boolean)} with {@code forgetClassifications=true}</li>
 *   <li>{@link State#TRANSIENT_REJECT} — 421 + {@code PRIMARY_CATCHUP}</li>
 *   <li>{@link State#TRANSPORT_ERROR} — connect / mid-stream failure</li>
 *   <li>{@link State#TOPOLOGY_REJECT} — 421 + {@code REPLICA}</li>
 * </ol>
 * Ties within a state break by most-recent-success epoch (a host that
 * succeeded most recently wins), so a flapping primary stays sticky
 * until {@link #recordMidStreamFailure(int)} demotes it.
 * <p>
 * Thread-safe via an internal monitor; the SF reconnect path may share
 * the tracker with the orphan drainer pool.
 */
public final class HostHealthTracker {

    private final boolean[] attempted;
    private final long[] lastSuccessEpoch;
    private final Object lock = new Object();
    private final State[] states;
    private long successEpochCounter;

    public HostHealthTracker(int hostCount) {
        if (hostCount <= 0) {
            throw new IllegalArgumentException("hostCount must be positive");
        }
        this.states = new State[hostCount];
        this.attempted = new boolean[hostCount];
        this.lastSuccessEpoch = new long[hostCount];
        for (int i = 0; i < hostCount; i++) {
            states[i] = State.UNKNOWN;
        }
    }

    /**
     * Start a new round of attempts.
     *
     * @param forgetClassifications when true, every host except the
     *                              most-recently-{@link State#HEALTHY} entry is reset to
     *                              {@link State#UNKNOWN} so previously-failed hosts
     *                              get a clean re-evaluation. The sticky-healthy entry
     *                              stays first in line.
     */
    public void beginRound(boolean forgetClassifications) {
        synchronized (lock) {
            Arrays.fill(attempted, false);
            if (!forgetClassifications) {
                return;
            }
            int stickyHealthy = -1;
            long stickyEpoch = -1L;
            for (int i = 0; i < states.length; i++) {
                if (states[i] == State.HEALTHY && lastSuccessEpoch[i] > stickyEpoch) {
                    stickyEpoch = lastSuccessEpoch[i];
                    stickyHealthy = i;
                }
            }
            for (int i = 0; i < states.length; i++) {
                if (i == stickyHealthy) {
                    continue;
                }
                states[i] = State.UNKNOWN;
            }
        }
    }

    public int count() {
        return states.length;
    }

    /** {@code true} once every host has been attempted in the current round. */
    public boolean isRoundExhausted() {
        synchronized (lock) {
            for (boolean a : attempted) {
                if (!a) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Returns the index of the highest-priority host that has not yet
     * been attempted in the current round, or {@code -1} when every host
     * has been attempted (round exhausted).
     */
    public int pickNext() {
        synchronized (lock) {
            int best = -1;
            int bestPriority = Integer.MAX_VALUE;
            long bestEpoch = -1L;
            for (int i = 0; i < states.length; i++) {
                if (attempted[i]) {
                    continue;
                }
                int p = states[i].priority;
                if (p < bestPriority
                        || (p == bestPriority && lastSuccessEpoch[i] > bestEpoch)) {
                    bestPriority = p;
                    bestEpoch = lastSuccessEpoch[i];
                    best = i;
                }
            }
            return best;
        }
    }

    /**
     * Demote a host from {@link State#HEALTHY} to {@link State#TRANSPORT_ERROR}
     * after a mid-stream failure. No-op if the prior state was anything
     * other than {@link State#HEALTHY}; we don't want a single hiccup to
     * cancel out a transient or topological classification we already
     * captured.
     */
    public void recordMidStreamFailure(int idx) {
        synchronized (lock) {
            if (states[idx] == State.HEALTHY) {
                states[idx] = State.TRANSPORT_ERROR;
                attempted[idx] = true;
            }
        }
    }

    /**
     * Record a 421 role rejection.
     *
     * @param transient_ {@code true} when the role was {@code PRIMARY_CATCHUP}
     *                   (transient — node will become writable shortly);
     *                   {@code false} when {@code REPLICA} (topological — won't
     *                   become writable without an operator-driven failover).
     */
    public void recordRoleReject(int idx, boolean transient_) {
        synchronized (lock) {
            states[idx] = transient_ ? State.TRANSIENT_REJECT : State.TOPOLOGY_REJECT;
            attempted[idx] = true;
        }
    }

    public void recordSuccess(int idx) {
        synchronized (lock) {
            states[idx] = State.HEALTHY;
            lastSuccessEpoch[idx] = ++successEpochCounter;
            attempted[idx] = true;
        }
    }

    public void recordTransportError(int idx) {
        synchronized (lock) {
            states[idx] = State.TRANSPORT_ERROR;
            attempted[idx] = true;
        }
    }

    public State stateOf(int idx) {
        synchronized (lock) {
            return states[idx];
        }
    }

    public enum State {
        // Lower priority value = picked first.
        HEALTHY(0),
        UNKNOWN(1),
        TRANSIENT_REJECT(2),
        TRANSPORT_ERROR(3),
        TOPOLOGY_REJECT(4);

        final int priority;

        State(int priority) {
            this.priority = priority;
        }
    }
}
