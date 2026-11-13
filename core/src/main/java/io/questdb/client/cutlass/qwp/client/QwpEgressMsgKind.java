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
     * {@code SERVER_INFO.capabilities} bit: the server honours the query-details
     * flags of {@code QUERY_REQUEST.query_flags} -- {@link #QUERY_FLAG_ERROR_POSITION},
     * {@link #QUERY_FLAG_STATS}, {@link #QUERY_FLAG_COUNT_ROWS},
     * {@link #QUERY_FLAG_SKIP_ROWS} and {@link #QUERY_FLAG_MAX_ROWS}. The client
     * sends those bits only when this bit is set: a server without it ignores
     * them and answers with the baseline frames. Mirrors the server-side constant
     * {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#CAP_QUERY_DETAILS}.
     */
    public static final int CAP_QUERY_DETAILS = 0x00000010;
    /**
     * {@code SERVER_INFO.capabilities} bit: the server parses the optional
     * {@code query_flags:varint} trailer on {@code QUERY_REQUEST}. The client
     * appends the trailer only when this bit is set. Mirrors the server-side
     * constant {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#CAP_QUERY_FLAGS}.
     */
    public static final int CAP_QUERY_FLAGS = 0x00000002;
    /**
     * {@code SERVER_INFO.capabilities} bit reserved for per-query timeouts, which
     * this client does not send. Released clients (c-questdb-client 7.2.0 and
     * later, and the Python client built on it) read the bit as "the server
     * honours {@link #QUERY_FLAG_TIMEOUT}", so no other capability may take it.
     * Mirrors the server-side constant
     * {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#CAP_QUERY_TIMEOUT}.
     */
    public static final int CAP_QUERY_TIMEOUT = 0x00000008;
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
    /**
     * Error position that points nowhere in the SQL text: the position of an
     * error that does not come from the SQL text, such as a {@code CANCEL} the
     * client sent, and of every error when the position was not requested or
     * not reported. A query timeout and a cancellation on the server, such as
     * {@code CANCEL QUERY}, report position 0, as the {@code /exec} HTTP endpoint
     * does.
     */
    public static final int NO_ERROR_POSITION = -1;
    public static final byte QUERY_ERROR = 0x13;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: report the row count of the whole
     * result set, whatever {@link #QUERY_FLAG_SKIP_ROWS} and
     * {@link #QUERY_FLAG_MAX_ROWS} leave out of the stream. {@code RESULT_END}
     * then carries {@code row_count:varint} after {@code total_rows}. Statements
     * that do not produce rows ignore it. Sent only when the server advertised
     * {@link #CAP_QUERY_DETAILS}.
     */
    public static final int QUERY_FLAG_COUNT_ROWS = 0x10;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: report the error position. A
     * {@code QUERY_ERROR} then carries the bare message, without the
     * {@code [n]} prefix of the baseline frame, and ends with
     * {@code position:i32}, the UTF-16 offset into the SQL text the error points
     * at, or {@link #NO_ERROR_POSITION}. The server answers with the baseline
     * frame when it could not read the request's flags. Sent only when the
     * server advertised {@link #CAP_QUERY_DETAILS}.
     */
    public static final int QUERY_FLAG_ERROR_POSITION = 0x04;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: stream at most {@code max_rows}
     * rows. The request carries {@code max_rows:varint} after
     * {@code query_flags}, behind the fields of the lower flag bits: after
     * {@code skip_rows} when {@link #QUERY_FLAG_SKIP_ROWS} is set too. Zero
     * streams only the schema. Statements that do not produce rows ignore it.
     * Sent only when the server advertised {@link #CAP_QUERY_DETAILS}.
     */
    public static final int QUERY_FLAG_MAX_ROWS = 0x40;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: reset the connection-scoped SYMBOL
     * dict before this query, scoping it to the query. Sent only when the server
     * advertised {@link #CAP_QUERY_FLAGS}.
     */
    public static final int QUERY_FLAG_RESET_DICT = 0x01;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: leave out the first
     * {@code skip_rows} rows of the result set. The request carries
     * {@code skip_rows:varint} after {@code query_flags}, behind the fields of
     * the lower flag bits, of which this client sends none. Statements that do
     * not produce rows ignore it. Sent only when the server advertised
     * {@link #CAP_QUERY_DETAILS}.
     */
    public static final int QUERY_FLAG_SKIP_ROWS = 0x20;
    /**
     * {@code QUERY_REQUEST.query_flags} bit: report execution statistics.
     * {@code RESULT_END} and {@code EXEC_DONE} then end with
     * {@code compile_ns:varint, execute_ns:varint, count_ns:varint,
     * client_wait_ns:varint, stats_flags:u8}, after {@code row_count} when
     * {@link #QUERY_FLAG_COUNT_ROWS} is set too. See
     * {@link #STATS_FLAG_JIT_COMPILED}. Sent only when the server advertised
     * {@link #CAP_QUERY_DETAILS}.
     */
    public static final int QUERY_FLAG_STATS = 0x08;
    /**
     * {@code QUERY_REQUEST.query_flags} bit reserved for per-query timeouts, which
     * this client does not send. Released clients (c-questdb-client 7.2.0 and
     * later, and the Python client built on it) send it, followed by a
     * {@code timeout_ms:varint}, to a server that advertises
     * {@link #CAP_QUERY_TIMEOUT}, so no other flag may take this bit. A flag that
     * carries a field appends it after {@code query_flags} in ascending order of
     * the flag bits, so {@code timeout_ms} goes ahead of {@code skip_rows} and
     * {@code max_rows}. Mirrors the server-side constant
     * {@code io.questdb.cutlass.qwp.codec.QwpEgressMsgKind#QUERY_FLAG_TIMEOUT}.
     */
    public static final int QUERY_FLAG_TIMEOUT = 0x02;
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
    /**
     * {@code stats_flags} bit of the {@link #QUERY_FLAG_STATS} block: the query
     * filter runs as JIT-compiled native code.
     */
    public static final byte STATS_FLAG_JIT_COMPILED = 0x01;

    private QwpEgressMsgKind() {
    }
}
