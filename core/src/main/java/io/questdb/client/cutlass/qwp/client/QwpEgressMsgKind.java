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
     * with bit 0 = SYMBOL dict, bit 1 = schema-fingerprint cache. Sent between
     * queries when a cache hits its server-side soft cap. Recipient clears the
     * indicated caches; subsequent RESULT_BATCH delta sections start fresh.
     */
    public static final byte CACHE_RESET = 0x17;
    public static final byte CANCEL = 0x14;
    public static final byte CREDIT = 0x15;
    /**
     * Server -> client. Ack for a successful non-SELECT query. Body:
     * {@code request_id:u64, op_type:u8, rows_affected:varint}.
     */
    public static final byte EXEC_DONE = 0x16;
    public static final byte QUERY_ERROR = 0x13;
    public static final byte QUERY_REQUEST = 0x10;
    /**
     * Reset mask bit: clear the connection-scoped SYMBOL dict.
     */
    public static final byte RESET_MASK_DICT = 0x01;
    /**
     * Reset mask bit: clear the connection-scoped schema-fingerprint cache.
     */
    public static final byte RESET_MASK_SCHEMAS = 0x02;
    public static final byte RESULT_BATCH = 0x11;
    public static final byte RESULT_END = 0x12;

    private QwpEgressMsgKind() {
    }
}
