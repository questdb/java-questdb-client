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
 * QWP egress message-kind discriminator bytes. Mirrors the server-side constants
 * in {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind}. First byte of every
 * egress payload identifies which of the egress message types it carries.
 */
public final class QwpEgressMsgKind {
    /**
     * Server -> client. Connection-scoped cache reset. Body: {@code reset_mask:u8}
     * with bit 0 = SYMBOL dict (the only bit defined). Sent between queries when
     * the dict hits its server-side soft cap. Recipient clears the dict;
     * subsequent RESULT_BATCH delta sections start fresh.
     */
    public static final byte CACHE_RESET = 0x17;
    public static final byte CANCEL = 0x14;
    /**
     * {@code SERVER_INFO.capabilities} bit: the server parses the optional
     * {@code query_flags:varint} trailer on {@code QUERY_REQUEST}. The client
     * appends the trailer only when this bit is set. Mirrors the server-side
     * constant {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#CAP_QUERY_FLAGS}.
     */
    public static final int CAP_QUERY_FLAGS = 0x00000002;
    /**
     * {@code SERVER_INFO.capabilities} bit advertising that the frame ends with
     * an additional {@code zone_id:u16_len+utf8} field after {@code node_id}.
     * Mirrors the server-side constant in
     * {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#CAP_ZONE}; the
     * decoder reads the trailer iff this bit is set, otherwise the frame ends
     * after {@code node_id}.
     */
    public static final int CAP_ZONE = 0x00000001;
    public static final byte CREDIT = 0x15;
    /**
     * Server -> client. Ack for a successful non-SELECT query. Body:
     * {@code request_id:u64, op_type:u8, rows_affected:varint}.
     */
    public static final byte EXEC_DONE = 0x16;
    public static final byte QUERY_ERROR = 0x13;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: reset the connection-scoped SYMBOL
     * dict before this query, scoping it to the query. Sent only when the server
     * advertised {@link #CAP_QUERY_FLAGS}.
     */
    public static final int QUERY_FLAG_RESET_DICT = 0x01;
    public static final byte QUERY_REQUEST = 0x10;
    /**
     * Reset mask bit: clear the connection-scoped SYMBOL dict.
     */
    public static final byte RESET_MASK_DICT = 0x01;
    public static final byte RESULT_BATCH = 0x11;
    public static final byte RESULT_END = 0x12;
    /**
     * Role value on {@code SERVER_INFO.role}: the authoritative write node.
     */
    public static final byte ROLE_PRIMARY = 1;
    /**
     * Role value on {@code SERVER_INFO.role}: promotion-in-progress. Clients
     * insisting on primary-only reads may still route here; clients needing
     * write-visible reads should wait for {@link #ROLE_PRIMARY}.
     */
    public static final byte ROLE_PRIMARY_CATCHUP = 3;
    /**
     * Role value on {@code SERVER_INFO.role}: the node is a read-only replica
     * that pulls WAL segments from the shared object store. Reads may lag the
     * primary by the replication poll interval plus transport time.
     */
    public static final byte ROLE_REPLICA = 2;
    /**
     * Role value on {@code SERVER_INFO.role}: no replication is configured. The
     * standalone OSS default; behaves like a primary for routing purposes.
     */
    public static final byte ROLE_STANDALONE = 0;
    /**
     * Server -> client. Unsolicited frame delivered as the first QWP message
     * on every WebSocket connection. Body (little-endian): {@code
     * msg_kind:u8, role:u8, epoch:u64, capabilities:u32, server_wall_ns:i64,
     * cluster_id:u16_len+utf8, node_id:u16_len+utf8} followed by an optional
     * {@code zone_id:u16_len+utf8} when the {@link #CAP_ZONE} bit is set in
     * {@code capabilities}. The byte-value 0x17 is claimed by
     * {@link #CACHE_RESET}; SERVER_INFO lives at 0x18.
     */
    public static final byte SERVER_INFO = 0x18;

    private QwpEgressMsgKind() {
    }
}
