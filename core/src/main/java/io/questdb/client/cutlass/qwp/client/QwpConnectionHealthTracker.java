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

import io.questdb.client.ConnectionHealth;
import io.questdb.client.cutlass.auth.CredentialRedaction;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.line.LineSenderException;

/**
 * Tracks one QWP client's connection health (design/qwp-token-provider-spec.md, section 8.4) and publishes it as
 * an immutable {@link ConnectionHealth} snapshot. Connection code reports transitions - a successful upgrade, a
 * lost connection, a failed connect round, a terminal failure, close - from whichever thread observes them;
 * readers get the last published snapshot with a single volatile read and never wait on connection threads.
 * Writers serialize on a private monitor that is never held across I/O.
 */
public final class QwpConnectionHealthTracker {
    private static final int MAX_CAUSE_DEPTH = 16;
    private final Object lock = new Object();
    private long failedRounds;
    private ConnectionHealth.Failure lastFailure;
    private long lastConnectedAt = ConnectionHealth.NONE;
    private long outageSince;
    private volatile ConnectionHealth snapshot;
    private ConnectionHealth.State state = ConnectionHealth.State.CONNECTING;

    public QwpConnectionHealthTracker() {
        // A client that has never connected has been without a connection since it was created.
        this.outageSince = System.currentTimeMillis();
        publish();
    }

    /**
     * Classifies a failed connect round's exception into the failure classes of section 8.4.
     */
    public static ConnectionHealth.Failure classify(Throwable failure, long nowMillis) {
        Throwable t = failure;
        for (int depth = 0; t != null && depth < MAX_CAUSE_DEPTH; depth++, t = t.getCause()) {
            if (t instanceof QwpCredentialUnavailableException) {
                return failure(ConnectionHealth.FailureClass.CREDENTIAL_UNAVAILABLE, 0, t, nowMillis);
            }
            if (t instanceof QwpAuthFailedException) {
                return failure(ConnectionHealth.FailureClass.AUTH_REJECTED,
                        ((QwpAuthFailedException) t).getStatusCode(), t, nowMillis);
            }
            if (t instanceof QwpRoleMismatchException || t instanceof QwpIngressRoleRejectedException) {
                return failure(ConnectionHealth.FailureClass.ROLE_REJECTED, 0, t, nowMillis);
            }
            if (t instanceof WebSocketUpgradeException) {
                WebSocketUpgradeException e = (WebSocketUpgradeException) t;
                return e.isRoleMismatch()
                        ? failure(ConnectionHealth.FailureClass.ROLE_REJECTED, 0, t, nowMillis)
                        : failure(ConnectionHealth.FailureClass.OTHER, Math.max(0, e.getStatusCode()), t, nowMillis);
            }
            if (t instanceof QwpVersionMismatchException || t instanceof QwpDurableAckMismatchException) {
                return failure(ConnectionHealth.FailureClass.OTHER, 0, t, nowMillis);
            }
        }
        return failure(failure instanceof HttpClientException || failure instanceof LineSenderException
                ? ConnectionHealth.FailureClass.TRANSPORT
                : ConnectionHealth.FailureClass.OTHER, 0, failure, nowMillis);
    }

    private static ConnectionHealth.Failure failure(
            ConnectionHealth.FailureClass failureClass,
            int statusCode,
            Throwable t,
            long nowMillis
    ) {
        String message = CredentialRedaction.sanitizeErrorText(t.getMessage());
        return new ConnectionHealth.Failure(failureClass, statusCode,
                message == null || message.isEmpty() ? t.getClass().getSimpleName() : message, nowMillis);
    }

    /**
     * The client was closed. Sticky.
     */
    public void closed() {
        synchronized (lock) {
            state = ConnectionHealth.State.CLOSED;
            publish();
        }
    }

    /**
     * An established connection was lost and is being re-established.
     */
    public void connectionLost() {
        synchronized (lock) {
            if (state == ConnectionHealth.State.CONNECTED) {
                state = ConnectionHealth.State.RECONNECTING;
                outageSince = System.currentTimeMillis();
                failedRounds = 0;
                publish();
            }
        }
    }

    /**
     * The client will not connect again. Sticky; keeps the last connect-round failure.
     */
    public void failed() {
        synchronized (lock) {
            if (state != ConnectionHealth.State.CLOSED) {
                state = ConnectionHealth.State.FAILED;
                publish();
            }
        }
    }

    /**
     * A connect round - one walk over the configured endpoints - ended without a connection.
     */
    public void roundFailed(Throwable failure) {
        final long now = System.currentTimeMillis();
        final ConnectionHealth.Failure classified = classify(failure, now);
        synchronized (lock) {
            if (state == ConnectionHealth.State.FAILED || state == ConnectionHealth.State.CLOSED) {
                return;
            }
            if (state == ConnectionHealth.State.CONNECTED) {
                state = ConnectionHealth.State.RECONNECTING;
                outageSince = now;
                failedRounds = 0;
            }
            failedRounds++;
            lastFailure = classified;
            publish();
        }
    }

    /**
     * @return the current snapshot; never null
     */
    public ConnectionHealth snapshot() {
        return snapshot;
    }

    /**
     * A connect round ended with a successful upgrade.
     */
    public void upgraded() {
        synchronized (lock) {
            if (state == ConnectionHealth.State.FAILED || state == ConnectionHealth.State.CLOSED) {
                return;
            }
            state = ConnectionHealth.State.CONNECTED;
            lastConnectedAt = System.currentTimeMillis();
            outageSince = ConnectionHealth.NONE;
            failedRounds = 0;
            publish();
        }
    }

    // caller holds lock
    private void publish() {
        snapshot = new ConnectionHealth(state, lastConnectedAt, outageSince, failedRounds, lastFailure);
    }
}
