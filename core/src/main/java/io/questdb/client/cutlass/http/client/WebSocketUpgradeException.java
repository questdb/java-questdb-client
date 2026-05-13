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

package io.questdb.client.cutlass.http.client;

/**
 * Thrown by {@link WebSocketClient#upgrade} when the server returns a non-101
 * status. Carries the status code and the server role (parsed from the
 * {@code X-QuestDB-Role} header) so callers can distinguish a transient
 * role-mismatch (status 421 against a REPLICA / PRIMARY_CATCHUP node) from
 * a terminal upgrade rejection (e.g. 401, 403, 426).
 */
public class WebSocketUpgradeException extends HttpClientException {

    public static final int STATUS_NONE = -1;
    private final String serverRole;
    private final int statusCode;

    public WebSocketUpgradeException(int statusCode, String serverRole, String message) {
        super(message);
        this.statusCode = statusCode;
        this.serverRole = serverRole;
    }

    /**
     * The {@code X-QuestDB-Role} header value the server returned alongside
     * the upgrade response, or {@code null} if the header was absent (legacy
     * server, OSS without replication, or non-QWP endpoint).
     */
    public String getServerRole() {
        return serverRole;
    }

    /**
     * The HTTP status code from the upgrade response, or {@link #STATUS_NONE}
     * if the response could not be parsed.
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * True when the response was {@code 421 Misdirected Request} with a
     * non-writable role ({@code REPLICA} or {@code PRIMARY_CATCHUP}). Callers
     * use this to rotate to the next configured address rather than treat
     * the failure as terminal.
     */
    public boolean isRoleMismatch() {
        return statusCode == 421;
    }

    /**
     * True when the role mismatch reflects a *topological* condition: the
     * node is a read-only replica that pulls WAL segments from the shared
     * object store ({@code X-QuestDB-Role: REPLICA}). Per failover.md §5
     * this is the lowest-priority retry target because a replica won't
     * become writable without an operator-driven failover.
     */
    public boolean isTopologicalRoleReject() {
        return isRoleMismatch() && "REPLICA".equals(serverRole);
    }

    /**
     * True when the role mismatch reflects a *transient* cluster condition:
     * the node believes it is primary but is still uploading WAL segments
     * to the shared object store before accepting writes
     * ({@code X-QuestDB-Role: PRIMARY_CATCHUP}). Per failover.md §5 this
     * gets higher retry priority than a {@code REPLICA} reject because
     * a catchup node typically becomes writable within seconds.
     */
    public boolean isTransientRoleReject() {
        return isRoleMismatch() && "PRIMARY_CATCHUP".equals(serverRole);
    }
}
