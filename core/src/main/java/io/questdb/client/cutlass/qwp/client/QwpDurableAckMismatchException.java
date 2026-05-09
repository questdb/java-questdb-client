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

import io.questdb.client.cutlass.http.client.HttpClientException;

/**
 * The client opted in via {@code request_durable_ack=on} but the server cannot
 * honour it. Terminal: retrying the same configured endpoints will not turn a
 * non-primary node into a durable-ack-capable primary, so the connect path
 * fails fast instead of burning the full reconnect budget.
 * <p>
 * Two flavours collapse into this exception:
 * <ul>
 *   <li>The server completed the upgrade but did not echo
 *       {@code X-QWP-Durable-Ack: enabled} -- {@link #getRole()} is {@code null}.</li>
 *   <li>Every configured endpoint role-rejected the upgrade (replica only),
 *       so no primary was reached -- {@link #getRole()} carries the last
 *       reject's role.</li>
 * </ul>
 */
public final class QwpDurableAckMismatchException extends HttpClientException {
    private final String host;
    private final int port;
    private final String role;

    public QwpDurableAckMismatchException(String host, int port, String role) {
        super("WebSocket upgrade failed: server does not support durable ack [host=");
        put(host).put(", port=").put(port);
        if (role != null) {
            put(", role=").put(role);
        }
        put(']');
        this.host = host;
        this.port = port;
        this.role = role;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    /**
     * Server role from the rejecting endpoint when the failure stemmed from a
     * cluster of role-rejecting nodes; {@code null} when the failure was that a
     * single primary completed the upgrade without advertising durable ack.
     */
    public String getRole() {
        return role;
    }
}
