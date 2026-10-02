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

package io.questdb.client;

import java.time.Instant;

/**
 * An immutable snapshot of a QWP client's connection health (design/qwp-token-provider-spec.md, section 8.4).
 * A sender in store-and-forward mode retries an outage indefinitely - a revoked credential, an unreachable
 * cluster - while the application keeps writing; this snapshot is how an application, or a health endpoint,
 * finds out that it has not reached the server for an hour even when no error handler is registered.
 * <p>
 * Reading it never waits on I/O or on a connection thread: {@link Sender#health()},
 * {@code QwpQueryClient.health()} and {@link QuestDB#health()} return a published snapshot, safe to call from any
 * thread at any rate. It never contains a credential.
 */
public final class ConnectionHealth {
    /**
     * Value of the timestamp getters when there is nothing to report.
     */
    public static final long NONE = -1L;
    private final long failedRounds;
    private final Failure lastFailure;
    private final long lastConnectedAtEpochMillis;
    private final long outageSinceEpochMillis;
    private final State state;

    public ConnectionHealth(
            State state,
            long lastConnectedAtEpochMillis,
            long outageSinceEpochMillis,
            long failedRounds,
            Failure lastFailure
    ) {
        this.state = state;
        this.lastConnectedAtEpochMillis = lastConnectedAtEpochMillis;
        this.outageSinceEpochMillis = outageSinceEpochMillis;
        this.failedRounds = failedRounds;
        this.lastFailure = lastFailure;
    }

    private static String instant(long epochMillis) {
        return epochMillis == NONE ? "none" : Instant.ofEpochMilli(epochMillis).toString();
    }

    /**
     * Number of failed connect rounds - walks over the configured endpoints that ended without a connection -
     * since {@link #getOutageSinceEpochMillis()}. Reset when a connection is established.
     */
    public long getFailedRounds() {
        return failedRounds;
    }

    /**
     * The most recent failed connect round: its class, a sanitized message and its time. Kept after the
     * connection recovers. Null when no round has failed.
     */
    public Failure getLastFailure() {
        return lastFailure;
    }

    /**
     * Wall-clock time of the most recent successful upgrade, or {@link #NONE}.
     */
    public long getLastConnectedAtEpochMillis() {
        return lastConnectedAtEpochMillis;
    }

    /**
     * Start of the current period without a connection, or {@link #NONE} while connected.
     */
    public long getOutageSinceEpochMillis() {
        return outageSinceEpochMillis;
    }

    public State getState() {
        return state;
    }

    @Override
    public String toString() {
        return "ConnectionHealth{state=" + state
                + ", lastConnectedAt=" + instant(lastConnectedAtEpochMillis)
                + ", outageSince=" + instant(outageSinceEpochMillis)
                + ", failedRounds=" + failedRounds
                + ", lastFailure=" + lastFailure + '}';
    }

    /**
     * The class of a failed connect round.
     */
    public enum FailureClass {
        /**
         * The token provider could not supply a credential; no endpoint was contacted.
         */
        CREDENTIAL_UNAVAILABLE,
        /**
         * An endpoint rejected the credential with {@code 401} or {@code 403}; see {@link Failure#getStatusCode()}.
         */
        AUTH_REJECTED,
        /**
         * Every reachable endpoint has the wrong role, such as a replica during a failover.
         */
        ROLE_REJECTED,
        /**
         * No endpoint could be reached: connection refused, timed out, or dropped.
         */
        TRANSPORT,
        /**
         * Anything else: another HTTP status at the upgrade, a protocol or capability mismatch.
         */
        OTHER
    }

    /**
     * The connection state.
     */
    public enum State {
        /**
         * No successful upgrade yet.
         */
        CONNECTING,
        /**
         * Connected.
         */
        CONNECTED,
        /**
         * The connection was lost and is being re-established. For a query client, which connects on demand: the
         * last connect or failover reconnect failed and the next operation will try again.
         */
        RECONNECTING,
        /**
         * Terminal: the client will not connect again.
         */
        FAILED,
        /**
         * Closed by the application.
         */
        CLOSED
    }

    /**
     * Health across many connections - every pooled connection of a {@link QuestDB} handle: the number of
     * connections in each state, the oldest outage and the most recent failure.
     */
    public static final class Aggregate {
        private final int[] counts;
        private final Failure lastFailure;
        private final long oldestOutageSinceEpochMillis;

        private Aggregate(int[] counts, long oldestOutageSinceEpochMillis, Failure lastFailure) {
            this.counts = counts;
            this.oldestOutageSinceEpochMillis = oldestOutageSinceEpochMillis;
            this.lastFailure = lastFailure;
        }

        /**
         * Aggregates the given snapshots.
         */
        public static Aggregate of(Iterable<ConnectionHealth> healths) {
            int[] counts = new int[State.values().length];
            long oldest = NONE;
            Failure last = null;
            for (ConnectionHealth h : healths) {
                counts[h.state.ordinal()]++;
                if (h.outageSinceEpochMillis != NONE && (oldest == NONE || h.outageSinceEpochMillis < oldest)) {
                    oldest = h.outageSinceEpochMillis;
                }
                if (h.lastFailure != null && (last == null || h.lastFailure.epochMillis > last.epochMillis)) {
                    last = h.lastFailure;
                }
            }
            return new Aggregate(counts, oldest, last);
        }

        /**
         * Number of connections in {@code state}.
         */
        public int count(State state) {
            return counts[state.ordinal()];
        }

        /**
         * The most recent failed connect round across all connections, or null.
         */
        public Failure getLastFailure() {
            return lastFailure;
        }

        /**
         * The oldest {@code outage_since} across all connections, or {@link #NONE} when all are connected.
         */
        public long getOldestOutageSinceEpochMillis() {
            return oldestOutageSinceEpochMillis;
        }

        /**
         * Number of connections aggregated.
         */
        public int total() {
            int n = 0;
            for (int c : counts) {
                n += c;
            }
            return n;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("ConnectionHealth.Aggregate{");
            State[] states = State.values();
            for (int i = 0; i < states.length; i++) {
                sb.append(states[i].name().toLowerCase(java.util.Locale.ROOT)).append('=').append(counts[i]).append(", ");
            }
            return sb.append("oldestOutageSince=").append(instant(oldestOutageSinceEpochMillis))
                    .append(", lastFailure=").append(lastFailure).append('}').toString();
        }
    }

    /**
     * A failed connect round. Never carries a credential.
     */
    public static final class Failure {
        private final long epochMillis;
        private final FailureClass failureClass;
        private final String message;
        private final int statusCode;

        public Failure(FailureClass failureClass, int statusCode, String message, long epochMillis) {
            this.failureClass = failureClass;
            this.statusCode = statusCode;
            this.message = message;
            this.epochMillis = epochMillis;
        }

        /**
         * Wall-clock time of the failure.
         */
        public long getEpochMillis() {
            return epochMillis;
        }

        public FailureClass getFailureClass() {
            return failureClass;
        }

        /**
         * A sanitized description of the failure, at most 256 characters.
         */
        public String getMessage() {
            return message;
        }

        /**
         * The HTTP status of the upgrade rejection - {@code 401} or {@code 403} for
         * {@link FailureClass#AUTH_REJECTED} - or {@code 0} when none applies.
         */
        public int getStatusCode() {
            return statusCode;
        }

        @Override
        public String toString() {
            return "Failure{class=" + failureClass + (statusCode > 0 ? ", status=" + statusCode : "")
                    + ", at=" + instant(epochMillis) + ", message=" + message + '}';
        }
    }
}
