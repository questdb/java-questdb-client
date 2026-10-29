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
import org.jetbrains.annotations.Nullable;

/**
 * The client requested durable-ack tiers via {@code request_durable_ack}
 * ({@code local}, {@code replicated}, {@code local,replicated}, or the legacy
 * {@code on}) but the server did not grant that exact set. Terminal: retrying
 * the same configured endpoints will not turn a server that cannot grant the
 * set into one that can, so the connect path fails fast instead of burning the
 * full reconnect budget.
 * <p>
 * The server grants all of the requested tiers or none of them: it confirms a
 * grant by echoing the requested token set in {@code X-QWP-Durable-Ack}
 * ({@code enabled} for a legacy {@code on} request). A missing header, or an
 * echo other than the expected token, is a denial. The message names the
 * requested set and what the server echoed, or that it sent no header.
 * {@link #getRole()} is {@code null} for this denial.
 */
public final class QwpDurableAckMismatchException extends HttpClientException {
    private final String host;
    private final int port;
    private final String role;

    /**
     * The upgrade completed but the server did not grant the requested tier
     * set. {@code serverEcho} is the server's {@code X-QWP-Durable-Ack} header
     * value, or {@code null} when the server sent no such header.
     */
    public QwpDurableAckMismatchException(String host, int port, int requestedTiers, @Nullable String serverEcho) {
        super("WebSocket upgrade failed: server does not support durable ack [host=");
        put(host).put(", port=").put(port)
                .put(", requested=").put(DurableAckTiers.configValue(requestedTiers))
                .put(", echoed=");
        if (serverEcho != null) {
            put(serverEcho);
        } else {
            put("<no X-QWP-Durable-Ack header>");
        }
        put(']');
        this.host = host;
        this.port = port;
        this.role = null;
    }

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
     * Server role passed to {@link #QwpDurableAckMismatchException(String, int, String)};
     * {@code null} when the failure was that the server completed the upgrade
     * without granting the requested tier set.
     */
    public String getRole() {
        return role;
    }
}
