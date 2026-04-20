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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QueryEvent;
import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.client.QwpEgressIoThread;
import io.questdb.client.cutlass.qwp.client.QwpResultBatchDecoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Hardening tests for {@link QwpResultBatchDecoder} against malformed RESULT_BATCH
 * frames from a hostile or buggy server. Each test crafts a wire payload directly
 * in native memory and asserts that the decoder rejects it cleanly with a
 * {@link QwpDecodeException} rather than reading out of bounds, growing the
 * schema registry without bound, or returning negative offsets that propagate
 * into accessors.
 */
public class QwpResultBatchDecoderHardeningTest {

    /**
     * Regression for C5: a server-supplied {@code schema_id} above the per-connection
     * cap must be rejected. Without the fix, {@code ensureSchemaSlot} would happily
     * append nulls until OOM (or AIOOBE for negative ids cast from a high varint).
     */
    @Test
    public void testHugeSchemaIdIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // schema_id = 1_000_000_000, well above the 65_535 cap.
                int len = writeMinimalResultBatch(staging, /*schemaId=*/ 1_000_000_000L);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject huge schema_id");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error message should mention schema_id: " + expected.getMessage(),
                            expected.getMessage().contains("schema_id"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression for C5: a varint that long-to-int casts to a negative value
     * (a hostile high varint with the sign bit set after the cast) must be
     * rejected, not silently passed to {@code getQuick(negativeIndex)}.
     */
    @Test
    public void testNegativeSchemaIdIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // 5-byte varint encoding 0x80000000 (which casts to Integer.MIN_VALUE).
                // varint bytes for 0x80000000:
                //   value bits 7..0:  0x00 -> byte: 0x80 (continuation)
                //   value bits 14..8: 0x00 -> byte: 0x80
                //   value bits 21..15:0x00 -> byte: 0x80
                //   value bits 28..22:0x00 -> byte: 0x80
                //   value bits 35..29:0x08 -> byte: 0x08 (no continuation)
                int len = writeMinimalResultBatchWithRawSchemaIdVarint(
                        staging, new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x08});
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject huge/negative schema_id");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error message should mention schema_id: " + expected.getMessage(),
                            expected.getMessage().contains("schema_id"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression for C3: a hostile or buggy server can send a QUERY_ERROR frame
     * that claims a 65535-byte message but supplies a tiny payload. Without the
     * fix, the client reads up to ~65 KiB of native memory beyond the frame and
     * surfaces it to the user callback as a String. With the fix, the client
     * detects the overrun and reports a bounded error.
     */
    @Test
    public void testQueryErrorMsgLenOverrunIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Frame contents:
            //   12 bytes header (uninspected by decodeError)
            //   1 byte msg_kind
            //   8 bytes request_id
            //   1 byte status
            //   2 bytes msgLen (we set 0xFFFF)
            //   0 bytes of actual message body
            // Total payload: 24 bytes; msgLen would otherwise force reading 65535 bytes.
            int payloadLen = 12 + 1 + 8 + 1 + 2;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Zero out
                for (int i = 0; i < payloadLen; i++) Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
                // Write an obviously bogus msgLen at the right offset (header + msg_kind + reqId + status).
                long msgLenOffset = buf + 12 + 1 + 8 + 1;
                Unsafe.getUnsafe().putShort(msgLenOffset, (short) 0xFFFF);

                QueryEvent ev = QwpEgressIoThread.decodeError(buf, payloadLen);
                Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
                Assert.assertNotNull(ev.errorMessage);
                Assert.assertTrue("error must mention msg_len overrun: " + ev.errorMessage,
                        ev.errorMessage.contains("msg_len") && ev.errorMessage.contains("exceeds"));
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Regression for C3: a QUERY_ERROR frame with valid msgLen and bytes must be
     * decoded correctly. Pins the wire format so the rejection test above is
     * confirming a real defensive guard, not a broken decoder.
     */
    @Test
    public void testQueryErrorValidMessageDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] msgBytes = "boom".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int payloadLen = 12 + 1 + 8 + 1 + 2 + msgBytes.length;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payloadLen; i++) Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
                long statusOffset = buf + 12 + 1 + 8;
                Unsafe.getUnsafe().putByte(statusOffset, (byte) 0x05);
                long msgLenOffset = statusOffset + 1;
                Unsafe.getUnsafe().putShort(msgLenOffset, (short) msgBytes.length);
                long bytesOffset = msgLenOffset + 2;
                for (int i = 0; i < msgBytes.length; i++) {
                    Unsafe.getUnsafe().putByte(bytesOffset + i, msgBytes[i]);
                }

                QueryEvent ev = QwpEgressIoThread.decodeError(buf, payloadLen);
                Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
                Assert.assertEquals((byte) 0x05, ev.errorStatus);
                Assert.assertEquals("boom", ev.errorMessage);
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Regression for C4: STRING column with a negative {@code totalBytes} field.
     * Without the fix, "stringBytesAddr + totalBytes > limit" passes (the sum
     * stays below limit), and {@code parseStringColumn} returns a position
     * before {@code stringBytesAddr} — subsequent column parsing reads native
     * memory backwards.
     */
    @Test
    public void testStringColumnNegativeTotalBytesIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeStringResultBatch(staging, /*nonNull=*/ 1, /*totalBytes=*/ -1);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject negative totalBytes");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error message should describe invalid total bytes: " + expected.getMessage(),
                            expected.getMessage().contains("total bytes"));
                }
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Sanity: with a sane (non-negative, in-range) {@code totalBytes}, the same
     * wire layout decodes successfully (no exception). Pins the wire format so
     * the negative-value rejection above is testing the right code path.
     */
    @Test
    public void testStringColumnValidTotalBytesIsAccepted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeStringResultBatch(staging, /*nonNull=*/ 1, /*totalBytes=*/ 5);
                buffer.copyFromPayload(staging, len);
                decoder.decode(buffer);
                // no exception => the decoder accepts the valid wire bytes
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    // -----------------------------------------------------------------------
    // Wire-format helpers: write a minimal RESULT_BATCH frame to native memory.
    // Layout (matches QwpResultBatchDecoder.decodePayload + parseStringColumn):
    //   header (12 bytes)
    //   msg_kind (0x11)
    //   request_id (8 bytes)
    //   batch_seq (varint)
    //   table-block:
    //     name_len (varint), name bytes (none)
    //     row_count (varint)
    //     column_count (varint)
    //     schema_mode (1 byte) + schema_id (varint)
    //     [if FULL] per column: name_len varint, name bytes, wire_type byte
    //     per column: null_flag byte (+optional bitmap), then column body
    // -----------------------------------------------------------------------

    private static long putByte(long p, byte v) {
        Unsafe.getUnsafe().putByte(p, v);
        return p + 1;
    }

    private static long putInt(long p, int v) {
        Unsafe.getUnsafe().putInt(p, v);
        return p + 4;
    }

    private static long putLong(long p, long v) {
        Unsafe.getUnsafe().putLong(p, v);
        return p + 8;
    }

    private static long putVarint(long p, long value) {
        while ((value & ~0x7FL) != 0) {
            Unsafe.getUnsafe().putByte(p++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(p++, (byte) (value & 0x7F));
        return p;
    }

    private static int writeMinimalResultBatch(long buf, long schemaId) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION_1);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);                         // batch_seq
        p = putVarint(p, 0L);                         // table_name_len
        p = putVarint(p, 0L);                         // row_count = 0 (no body needed)
        p = putVarint(p, 0L);                         // column_count = 0
        p = putByte(p, QwpConstants.SCHEMA_MODE_FULL);
        p = putVarint(p, schemaId);
        return (int) (p - buf);
    }

    /**
     * Variant that writes a custom raw varint sequence for schema_id. Lets us
     * inject a multi-byte varint that decodes to a value with the int sign bit
     * set after long-to-int truncation.
     */
    private static int writeMinimalResultBatchWithRawSchemaIdVarint(long buf, byte[] schemaIdVarint) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION_1);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);
        p = putVarint(p, 0L);
        p = putVarint(p, 0L);
        p = putVarint(p, 0L);
        p = putByte(p, QwpConstants.SCHEMA_MODE_FULL);
        for (byte b : schemaIdVarint) p = putByte(p, b);
        return (int) (p - buf);
    }

    private static int writeStringResultBatch(long buf, int nonNull, int totalBytes) {
        long p = buf;
        // Header: magic + version + msg_kind + flags + table_count + payload_length
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);   // 4
        p = putByte(p, QwpConstants.VERSION_1);       // 1
        p = putByte(p, (byte) 0);                     // msg_kind in header (unused by client)
        p = putByte(p, (byte) 0);                     // flags
        p = putByte(p, (byte) 1);                     // table_count
        p = putInt(p, 0);                             // payload_length placeholder (unused)

        // Body:
        p = putByte(p, (byte) 0x11);                  // msg_kind = RESULT_BATCH
        p = putLong(p, 7L);                           // request_id
        p = putVarint(p, 0L);                         // batch_seq
        p = putVarint(p, 0L);                         // table_name_len = 0
        p = putVarint(p, nonNull);                    // row_count
        p = putVarint(p, 1L);                         // column_count
        p = putByte(p, QwpConstants.SCHEMA_MODE_FULL);
        p = putVarint(p, 0L);                         // schema_id
        // Schema entries (full): one column "s" of TYPE_STRING
        p = putVarint(p, 1L);                         // column name length
        p = putByte(p, (byte) 's');
        p = putByte(p, QwpConstants.TYPE_STRING);
        // Column body: null_flag = 0 (no nulls), offsets[nonNull+1] u32, then bytes.
        p = putByte(p, (byte) 0);                     // null_flag
        for (int i = 0; i < nonNull; i++) {
            p = putInt(p, i * 5);                     // offset[i]
        }
        p = putInt(p, totalBytes);                    // offset[nonNull] = totalBytes
        // Followed by 'totalBytes' string bytes — for the success case we write "hello"
        // (5 bytes). For the negative-totalBytes case we still write 5 bytes; the
        // decoder must reject before consuming them.
        byte[] s = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (byte b : s) p = putByte(p, b);
        return (int) (p - buf);
    }
}
