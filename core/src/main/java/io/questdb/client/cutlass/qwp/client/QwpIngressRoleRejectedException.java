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
 * Raised when the server rejects a {@code /write/v4} WebSocket upgrade with a
 * {@code 421 Misdirected Request} carrying an {@code X-QuestDB-Role} header.
 * Carries the role name so the host-health tracker can classify the endpoint
 * as transiently unavailable ({@code PRIMARY_CATCHUP}) versus structurally
 * unwritable ({@code REPLICA}).
 */
public final class QwpIngressRoleRejectedException extends HttpClientException {
    public static final String ROLE_PRIMARY = "PRIMARY";
    public static final String ROLE_PRIMARY_CATCHUP = "PRIMARY_CATCHUP";
    public static final String ROLE_REPLICA = "REPLICA";
    public static final String ROLE_STANDALONE = "STANDALONE";

    private final String host;
    private final int port;
    private final String role;
    // Server-advertised zone identifier from the upgrade reject response's
    // X-QuestDB-Zone header. Null when the header was absent or empty.
    // Used by the host tracker to record the host's zone tier (failover.md
    // §5) without a successful upgrade.
    private final String zoneId;

    public QwpIngressRoleRejectedException(String role, String host, int port) {
        this(role, host, port, null);
    }

    public QwpIngressRoleRejectedException(String role, String host, int port, String zoneId) {
        super("WebSocket ingress upgrade rejected by role=");
        put(role).put(" at ").put(host).put(':').put(port);
        this.role = role;
        this.host = host;
        this.port = port;
        this.zoneId = zoneId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getRole() {
        return role;
    }

    public String getZoneId() {
        return zoneId;
    }

    public boolean isTopological() {
        return ROLE_REPLICA.equalsIgnoreCase(role);
    }

    public boolean isTransient() {
        return ROLE_PRIMARY_CATCHUP.equalsIgnoreCase(role);
    }
}
