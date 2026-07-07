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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable record describing a single connection-state transition observed by
 * the QWP ingress client. Delivered to a {@link SenderConnectionListener}
 * registered via {@code LineSenderBuilder.connectionListener(...)}.
 *
 * <p>See {@link Kind} for the full set of transitions and when each fires.
 */
public final class SenderConnectionEvent {

    /**
     * Sentinel for {@link #attemptNumber} when the event does not correspond to
     * a per-attempt counter (e.g. the initial CONNECTED event before the
     * reconnect loop has run).
     */
    public static final long NO_ATTEMPT_NUMBER = -1L;

    /** Sentinel for {@link #port} / {@link #previousPort} when none applies. */
    public static final int NO_PORT = -1;

    /**
     * Sentinel for {@link #roundNumber} when the event does not correspond to a
     * full address-list sweep.
     */
    public static final long NO_ROUND_NUMBER = -1L;

    private final long attemptNumber;
    private final Throwable cause;
    private final String host;
    private final Kind kind;
    private final int port;
    private final String previousHost;
    private final int previousPort;
    private final long roundNumber;
    private final long timestampMillis;

    public SenderConnectionEvent(
            @NotNull Kind kind,
            @Nullable String host,
            int port,
            @Nullable String previousHost,
            int previousPort,
            long attemptNumber,
            long roundNumber,
            @Nullable Throwable cause,
            long timestampMillis
    ) {
        this.kind = kind;
        this.host = host;
        this.port = port;
        this.previousHost = previousHost;
        this.previousPort = previousPort;
        this.attemptNumber = attemptNumber;
        this.roundNumber = roundNumber;
        this.cause = cause;
        this.timestampMillis = timestampMillis;
    }

    /**
     * Monotonically-increasing reconnect-attempt counter, snapshotted at the
     * moment this event fired. {@link #NO_ATTEMPT_NUMBER} for events that do
     * not have a per-attempt counter (the initial CONNECTED event).
     */
    public long getAttemptNumber() {
        return attemptNumber;
    }

    /**
     * The classified cause of the event, or {@code null} for success/info
     * events ({@link Kind#CONNECTED}, {@link Kind#FAILED_OVER},
     * {@link Kind#RECONNECTED}). For the terminal kind
     * ({@link Kind#AUTH_FAILED}) this
     * carries the typed exception that caused the sender to halt.
     */
    @Nullable
    public Throwable getCause() {
        return cause;
    }

    /**
     * The endpoint host involved in this event. Null for
     * {@link Kind#ALL_ENDPOINTS_UNREACHABLE} when no last-attempted endpoint is
     * recorded.
     */
    @Nullable
    public String getHost() {
        return host;
    }

    /** The kind of transition. Never null. */
    @NotNull
    public Kind getKind() {
        return kind;
    }

    /** The endpoint port involved, or {@link #NO_PORT} if not applicable. */
    public int getPort() {
        return port;
    }

    /**
     * For {@link Kind#FAILED_OVER}, the host the client was previously
     * connected to. {@code null} for events that do not have a "previous"
     * counterpart.
     */
    @Nullable
    public String getPreviousHost() {
        return previousHost;
    }

    /**
     * For {@link Kind#FAILED_OVER}, the port the client was previously
     * connected to. {@link #NO_PORT} when not applicable.
     */
    public int getPreviousPort() {
        return previousPort;
    }

    /**
     * Number of full address-list sweeps attempted since this sender started,
     * or {@link #NO_ROUND_NUMBER} if not applicable.
     */
    public long getRoundNumber() {
        return roundNumber;
    }

    /** Wall-clock time of the event, milliseconds since the Unix epoch. */
    public long getTimestampMillis() {
        return timestampMillis;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SenderConnectionEvent{");
        sb.append("kind=").append(kind);
        if (host != null) {
            sb.append(", endpoint=").append(host).append(':').append(port);
        }
        if (previousHost != null) {
            sb.append(", previous=").append(previousHost).append(':').append(previousPort);
        }
        if (attemptNumber != NO_ATTEMPT_NUMBER) {
            sb.append(", attempt=").append(attemptNumber);
        }
        if (roundNumber != NO_ROUND_NUMBER) {
            sb.append(", round=").append(roundNumber);
        }
        if (cause != null) {
            sb.append(", cause=").append(cause.getClass().getSimpleName())
                    .append(':').append(cause.getMessage());
        }
        sb.append(", t=").append(timestampMillis);
        return sb.append('}').toString();
    }

    /**
     * The set of connection-state transitions that fire as discrete events.
     */
    public enum Kind {
        /**
         * The very first successful connect of this sender's lifetime.
         * Fired exactly once, before any data has been sent.
         */
        CONNECTED,

        /**
         * The active wire connection dropped and the reconnect loop is about
         * to start. Fired once per outage, before any retry attempts.
         */
        DISCONNECTED,

        /**
         * A reconnect attempt succeeded against the same endpoint that was
         * previously active. Fired once per successful reconnect to the same
         * endpoint; {@link Kind#FAILED_OVER} is fired instead when the new
         * endpoint differs.
         */
        RECONNECTED,

        /**
         * A reconnect attempt succeeded against a different endpoint than the
         * previously-active one. Mutually exclusive with {@link Kind#RECONNECTED}
         * for any given successful reconnect.
         */
        FAILED_OVER,

        /**
         * A single endpoint connect or upgrade attempt failed; the client will
         * try the next endpoint in the address list (if any). Fired once per
         * failed endpoint within a sweep. {@link #getCause()} carries the
         * classified exception.
         */
        ENDPOINT_ATTEMPT_FAILED,

        /**
         * Every endpoint in the configured address list was attempted and none
         * accepted the connection in this sweep. The client will back off and
         * retry the sweep — bounded by {@code reconnect_max_duration_millis}
         * during a blocking (sync) initial connect, indefinitely otherwise
         * (Invariant B: the background loop never gives up on a wall-clock
         * budget). Fired once per failed sweep.
         */
        ALL_ENDPOINTS_UNREACHABLE,

        /**
         * Terminal: server-rejected credentials (or auth handshake failed
         * uniformly across the cluster). The sender will halt; the next
         * producer-thread API call surfaces a {@code LineSenderException}.
         * {@link #getCause()} carries the {@code QwpAuthFailedException}.
         */
        AUTH_FAILED
    }
}
