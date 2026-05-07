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
 * Wire opcodes for QWP table commit-tail subscriptions. The opcodes occupy
 * the 0x19-0x1E range. Subscribing is an Enterprise-only feature on the
 * server side; the opcodes ship with the OSS client so a single client jar
 * can talk to both OSS (queries only) and Enterprise (queries +
 * subscriptions) servers.
 * <p>
 * Frame body layouts (little-endian):
 * <ul>
 *   <li>{@link #SUBSCRIBE_REQUEST} (C-&gt;S): {@code msg_kind:u8,
 *       subscription_id:u64, table_name:u16_len+utf8, start_txn:varint,
 *       credit_bytes:varint, batch_max_rows:u32, flags:u32}.
 *       {@code start_txn=0} means tail-from-now; non-zero requests resume
 *       from an explicit sequencer txn (server may reject with STALE).</li>
 *   <li>{@link #SUBSCRIBE_ACK} (S-&gt;C): {@code msg_kind:u8,
 *       subscription_id:u64, start_txn:u64, schema_id:u32, schema_full:bytes}.
 *       Carries the resolved start_txn and the table's full schema.</li>
 *   <li>{@link #SUBSCRIBE_BATCH} (S-&gt;C): {@code msg_kind:u8,
 *       subscription_id:u64, batch_seq:varint, txn:u64,
 *       symbol_dict_delta:bytes, table_block:bytes}. Same column-major payload
 *       as RESULT_BATCH; the schema is shipped by reference (the id was
 *       supplied in SUBSCRIBE_ACK).</li>
 *   <li>{@link #SUBSCRIPTION_END} (S-&gt;C): {@code msg_kind:u8,
 *       subscription_id:u64, reason:u8, last_txn:u64, message:u16_len+utf8}.</li>
 *   <li>{@link #SUB_CANCEL} (C-&gt;S): {@code msg_kind:u8, subscription_id:u64}.</li>
 *   <li>{@link #SUB_CREDIT} (C-&gt;S): {@code msg_kind:u8, subscription_id:u64,
 *       additional_bytes:varint}.</li>
 * </ul>
 */
public final class QwpSubscribeMsgKind {

    /** Server -&gt; client subscribe acknowledgement. */
    public static final byte SUBSCRIBE_ACK = 0x1A;
    /** Server -&gt; client subscription row batch. */
    public static final byte SUBSCRIBE_BATCH = 0x1B;
    /** Client -&gt; server subscribe request. */
    public static final byte SUBSCRIBE_REQUEST = 0x19;
    /** Server -&gt; client subscription terminator. */
    public static final byte SUBSCRIPTION_END = 0x1C;
    /** Reason: client sent SUB_CANCEL. */
    public static final byte SUB_END_CLIENT_UNSUBSCRIBE = 0x00;
    /** Reason: server-side error or unexpected condition. */
    public static final byte SUB_END_ERROR = 0x06;
    /** Reason: table structure changed; client must resubscribe. */
    public static final byte SUB_END_SCHEMA_CHANGED = 0x02;
    /** Reason: principal lost SELECT permission on the table. */
    public static final byte SUB_END_SECURITY_REVOKED = 0x04;
    /** Reason: server is shutting down. */
    public static final byte SUB_END_SERVER_SHUTDOWN = 0x05;
    /**
     * Reason: requested resume txn is older than the oldest WAL segment
     * still on disk, OR per-subscription event queue overflowed.
     */
    public static final byte SUB_END_STALE = 0x03;
    /** Reason: table was dropped or never existed. */
    public static final byte SUB_END_TABLE_DROPPED = 0x01;
    /** Client -&gt; server subscription cancel. */
    public static final byte SUB_CANCEL = 0x1D;
    /** Client -&gt; server subscription credit replenishment. */
    public static final byte SUB_CREDIT = 0x1E;

    private QwpSubscribeMsgKind() {
    }

    public static String reasonName(byte reason) {
        switch (reason) {
            case SUB_END_CLIENT_UNSUBSCRIBE:
                return "CLIENT_UNSUBSCRIBE";
            case SUB_END_TABLE_DROPPED:
                return "TABLE_DROPPED";
            case SUB_END_SCHEMA_CHANGED:
                return "SCHEMA_CHANGED";
            case SUB_END_STALE:
                return "STALE";
            case SUB_END_SECURITY_REVOKED:
                return "SECURITY_REVOKED";
            case SUB_END_SERVER_SHUTDOWN:
                return "SERVER_SHUTDOWN";
            case SUB_END_ERROR:
                return "ERROR";
            default:
                return "UNKNOWN";
        }
    }
}
