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

/**
 * Decoded {@code SERVER_INFO} frame delivered by a QWP egress server as the
 * first WebSocket frame after the upgrade handshake. Immutable; safe to publish
 * across threads without synchronisation once assigned.
 */
public final class QwpServerInfo {

    private final int capabilities;
    private final String clusterId;
    private final long epoch;
    private final String nodeId;
    private final byte role;
    private final long serverWallNs;
    private final String zoneId;

    public QwpServerInfo(
            byte role,
            long epoch,
            int capabilities,
            long serverWallNs,
            String clusterId,
            String nodeId,
            String zoneId
    ) {
        this.role = role;
        this.epoch = epoch;
        this.capabilities = capabilities;
        this.serverWallNs = serverWallNs;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.zoneId = zoneId;
    }

    /**
     * Returns the human-readable name for the given role byte. Useful when
     * constructing error messages from {@link QwpQueryClient#getServerRole()}
     * or logging role-mismatch diagnostics. Unknown values (outside the
     * published enum) are returned as {@code "UNKNOWN(n)"}.
     */
    public static String roleName(byte role) {
        switch (role) {
            case QwpEgressMsgKind.ROLE_STANDALONE:
                return "STANDALONE";
            case QwpEgressMsgKind.ROLE_PRIMARY:
                return "PRIMARY";
            case QwpEgressMsgKind.ROLE_REPLICA:
                return "REPLICA";
            case QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP:
                return "PRIMARY_CATCHUP";
            default:
                return "UNKNOWN(" + (role & 0xFF) + ")";
        }
    }

    public int getCapabilities() {
        return capabilities;
    }

    public String getClusterId() {
        return clusterId;
    }

    public long getEpoch() {
        return epoch;
    }

    public String getNodeId() {
        return nodeId;
    }

    public byte getRole() {
        return role;
    }

    public long getServerWallNs() {
        return serverWallNs;
    }

    /**
     * Server-advertised zone identifier (e.g. {@code eu-west-1a}). {@code null}
     * when the {@link QwpEgressMsgKind#CAP_ZONE} bit is unset on
     * {@link #getCapabilities()}; a possibly-empty string otherwise. Clients
     * that pass {@code zone=} on the connect string compare this case-insensitively
     * against their configured zone to prefer same-zone endpoints.
     */
    public String getZoneId() {
        return zoneId;
    }

    @Override
    public String toString() {
        return "QwpServerInfo{role=" + roleName(role)
                + ", epoch=" + epoch
                + ", clusterId='" + clusterId + '\''
                + ", nodeId='" + nodeId + '\''
                + ", zoneId=" + (zoneId == null ? "null" : "'" + zoneId + '\'')
                + ", capabilities=0x" + Integer.toHexString(capabilities)
                + ", serverWallNs=" + serverWallNs
                + '}';
    }
}
