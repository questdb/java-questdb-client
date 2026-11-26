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

import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;

/**
 * Server completed the WebSocket upgrade (HTTP 101) without confirming the {@code owned_by} the client named
 * in {@code X-QWP-Owned-By}: it echoed no owner, as a server without the feature does, or another one. The
 * tables and columns the connection auto-created there would not get the owner, so the upgrade fails: the
 * initial connect fails, while a sender that has connected before keeps retrying it. Like a missing
 * durable-ack echo, it comes from a writable endpoint that lacks the capability, which a rolling upgrade
 * clears: an orphan drainer gives it the same settle budget before it quarantines the slot, and a round that
 * also met a 421 role reject does not take it for part of a failover, unlike a plain
 * {@link WebSocketUpgradeException}, but reports it instead of a missing primary. Carries no status
 * ({@link #STATUS_NONE}), as the server answered 101. Names the endpoint that answered, in its message and in
 * {@link #getHost()} and {@link #getPort()}, as a client that walks several addresses may meet the gap on any of
 * them.
 */
public final class QwpOwnedByMismatchException extends WebSocketUpgradeException {
    private final String host;
    private final int port;

    /**
     * @param host    host of the endpoint that answered 101 without confirming the owner
     * @param port    port of that endpoint
     * @param ownedBy the owner the client named
     * @param echo    the {@code X-QWP-Owned-By} value the server echoed, or null when it sent none
     */
    public QwpOwnedByMismatchException(String host, int port, String ownedBy, String echo) {
        super(STATUS_NONE, null, "WebSocket upgrade failed: server did not confirm owned_by, "
                + "it cannot set the owner of auto-created tables and columns [host=");
        put(host).put(", port=").put(port)
                .put(", owned_by=").put(ownedBy)
                .put(", echoed=").put(echo != null ? echo : "<no X-QWP-Owned-By header>").put(']');
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
