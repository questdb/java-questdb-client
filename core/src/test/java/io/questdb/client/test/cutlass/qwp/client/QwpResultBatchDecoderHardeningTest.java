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
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpProtocolVersionException;
import io.questdb.client.cutlass.qwp.client.QwpResultBatchDecoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Hardening tests for {@link QwpResultBatchDecoder} against malformed RESULT_BATCH
 * frames from a hostile or buggy server. Each test crafts a wire payload directly
 * in native memory and asserts that the decoder rejects it cleanly with a
 * {@link QwpDecodeException} rather than reading out of bounds, accepting an
 * out-of-range column_count, or returning negative offsets that propagate
 * into accessors.
 */
public class QwpResultBatchDecoderHardeningTest {

    /**
     * The per-dimension bound matches the server's {@code DIM_MAX_LEN}
     * ({@code (1 << 28) - 1}): an empty array carrying a dimension exactly at
     * that ceiling decodes, while one element past it is rejected. Pins the
     * boundary so the client accepts every shape the server can emit and no
     * more. The 0-length sibling keeps the element-count product at 0, so this
     * exercises the per-dimension cap rather than the product cap.
     */
    @Test
    public void testArrayDimAtServerLimitBoundary() throws Exception {
        final int dimMaxLen = (1 << 28) - 1;
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // {0, DIM_MAX_LEN}: empty array (product 0) with a sibling at the
                // server's per-dimension ceiling -- decodes without error.
                int len = writeArrayResultBatchWithDims(staging, new int[]{0, dimMaxLen});
                buffer.copyFromPayload(staging, len);
                decoder.decode(buffer);

                // {0, DIM_MAX_LEN + 1}: one past the ceiling -- rejected.
                len = writeArrayResultBatchWithDims(staging, new int[]{0, dimMaxLen + 1});
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject an ARRAY dim above DIM_MAX_LEN");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must blame the ARRAY dim: " + expected.getMessage(),
                            expected.getMessage().contains("ARRAY dim"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * A 0-length dimension is a valid empty array (cardinality 0), distinct
     * from a NULL array (carried in the null bitmap), so {@code {0, 5}} must
     * decode without error. The original concern -- that a 0 collapses the
     * element-count product and lets a sibling dimension smuggle an arbitrary
     * value past the {@code MAX_ARRAY_ELEMENTS} cap -- is now handled by a
     * per-dimension bound, so {@code {0, Integer.MAX_VALUE}} is still rejected.
     */
    @Test
    public void testArrayDimZeroIsEmptyArrayButHostileDimRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // {0, 5}: a legitimate empty array -- decodes without error.
                int len = writeArrayResultBatchWithDims(staging, new int[]{0, 5});
                buffer.copyFromPayload(staging, len);
                decoder.decode(buffer);

                // {0, Integer.MAX_VALUE}: the per-dimension cap must still fire
                // even though the element-count product collapses to 0.
                len = writeArrayResultBatchWithDims(staging, new int[]{0, Integer.MAX_VALUE});
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject a hostile oversized ARRAY dim");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must blame the ARRAY dim: " + expected.getMessage(),
                            expected.getMessage().contains("ARRAY dim"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Sanity: a well-formed single-row ARRAY column with all dimensions
     * >= 1 still decodes successfully. Pins the wire layout so the
     * dim-zero rejection above is testing the right code path rather
     * than a generic frame-shape bug.
     */
    @Test
    public void testArrayValidDimensionsAreAccepted() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeArrayResultBatchWithDims(staging, new int[]{2, 3});
                buffer.copyFromPayload(staging, len);
                decoder.decode(buffer);
                // no exception => decoder accepts the well-formed wire bytes
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression for the surviving table-block guard: a {@code column_count}
     * above {@link QwpConstants#MAX_COLUMNS_PER_TABLE} on the schema-bearing
     * batch_seq == 0 must be rejected before the decoder tries to parse that
     * many column descriptors. This guard moved into the batch_seq == 0 branch
     * when schema-reference mode was removed; the old schema_id range check that
     * used to sit beside it is gone, so this is the only remaining bound here.
     */
    @Test
    public void testColumnCountOutOfRangeIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(64);
            long staging = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = putInt(p, QwpConstants.MAGIC_MESSAGE);
                p = putByte(p, QwpConstants.VERSION);
                p = putByte(p, (byte) 0);                     // flags
                p = putByte(p, (byte) 0);                     // msg_kind in header (unused)
                p = putByte(p, (byte) 1);                     // table_count
                p = putInt(p, 0);                             // payload_length (unused)
                p = putByte(p, (byte) 0x11);                  // RESULT_BATCH
                p = putLong(p, 1L);                           // request_id
                p = putVarint(p, 0L);                         // batch_seq = 0 (schema-bearing)
                p = putVarint(p, 0L);                         // table_name_len
                p = putVarint(p, 0L);                         // row_count
                p = putVarint(p, 1_000_000_000L);            // column_count: far above the cap
                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject an out-of-range column_count");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must reference column_count: " + expected.getMessage(),
                            expected.getMessage().contains("column_count out of range"));
                }
            } finally {
                Unsafe.free(staging, 64, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testUnsupportedVersionThrowsProtocolVersionException() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(128);
            long staging = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeMinimalResultBatch(staging);
                Unsafe.getUnsafe().putByte(staging + 4, (byte) 99);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must throw on unsupported version");
                } catch (QwpProtocolVersionException expected) {
                    Assert.assertTrue("error must reference unsupported version: " + expected.getMessage(),
                            expected.getMessage().contains("unsupported version"));
                    Assert.assertTrue("must extend QwpDecodeException",
                            expected instanceof QwpDecodeException);
                }
            } finally {
                Unsafe.free(staging, 128, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * The protocol collapsed to a single version. Version 2 -- accepted by the
     * pre-collapse range check ({@code >= VERSION_1 && <= MAX_SUPPORTED_VERSION})
     * -- must now be rejected by the flattened {@code != VERSION} check, the same
     * as any other non-1 version byte. Pins the boundary that actually changed.
     */
    @Test
    public void testVersionTwoIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(128);
            long staging = Unsafe.malloc(128, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeMinimalResultBatch(staging);
                Unsafe.getUnsafe().putByte(staging + 4, (byte) 2);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject protocol version 2");
                } catch (QwpProtocolVersionException expected) {
                    Assert.assertTrue("error must reference unsupported version: " + expected.getMessage(),
                            expected.getMessage().contains("unsupported version"));
                    Assert.assertTrue("must extend QwpDecodeException",
                            expected instanceof QwpDecodeException);
                }
            } finally {
                Unsafe.free(staging, 128, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression: a delta SYMBOL dict entry whose length exceeds
     * {@link Integer#MAX_VALUE} must be rejected. Prior to the fix, the
     * per-entry guard checked {@code entryLen < 0} (on the long varint value)
     * and {@code p + entryLen > limit}; neither catches a value that is
     * positive-as-long but wraps to a negative int after the subsequent
     * {@code (int) entryLen} cast, which then fed a negative length into
     * {@code ensureConnDictHeapCapacity} (no-op) and finally into
     * {@code copyMemory} (undefined behaviour).
     */
    @Test
    public void testDeltaSymbolDictHugeEntryLenIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // 5-byte varint for 0x1_0000_0000L (2^32). That is positive as
                // long but (int) wraps to 0. A wider check is needed to catch
                // the int-cast hazard; a varint of 2^32 exceeds Integer.MAX_VALUE
                // and must be rejected before the cast.
                byte[] entryLenVarint = new byte[]{
                        (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x10
                };
                int len = writeDeltaSymbolDictFrame(staging, entryLenVarint);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject delta dict entryLen > Integer.MAX_VALUE");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must mention truncated delta symbol entry: " + expected.getMessage(),
                            expected.getMessage().contains("truncated delta symbol entry"));
                }
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression: a hostile delta-symbol section that picks
     * {@code (deltaStart, deltaCount)} so their long sum overflows negative
     * must be rejected by the range guard, NOT silently bypassed and caught
     * later by the secondary "out of sync" check (or, on a connection where
     * deltaStart matches an already-grown connDictSize, written into native
     * memory past connDictEntriesAddr).
     * <p>
     * With deltaStart=4M and deltaCount=Long.MAX_VALUE: the additive form
     * {@code deltaStart + deltaCount > MAX_CONN_DICT_SIZE} wraps to a
     * negative long, fails the {@code > MAX} comparison, and lets the value
     * fall through to the sync check (which, on a fresh decoder, throws
     * "out of sync"). The fix validates each operand separately, so the
     * range guard fires first with "out of range" -- which this test pins.
     */
    @Test
    public void testDeltaSymbolDictStartPlusCountOverflowIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeDeltaSymbolDictFrameWithRange(staging,
                        /*deltaStart=*/ 4L * 1024L * 1024L,
                        /*deltaCount=*/ Long.MAX_VALUE);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject overflowing delta range");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("range guard must fire before sync guard so the additive "
                                    + "overflow is caught up-front: " + expected.getMessage(),
                            expected.getMessage().contains("out of range"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression: an EXEC_DONE frame whose {@code rows_affected} varint runs
     * past the end of the payload (continuation bit set on the last byte we
     * have, with no more bytes to follow) must be rejected as a transport
     * failure. Prior to the fix the loop exited via {@code p < limit}
     * without observing a clear continuation bit and emitted the
     * partially-decoded value to the user as a successful EXEC_DONE.
     */
    @Test
    public void testExecDoneTruncatedRowsAffectedVarintIsRejected() throws Exception {
        AtomicReference<String> failure = new AtomicReference<>();
        TestUtils.assertMemoryLeak(() -> {
            QwpEgressIoThread io = new QwpEgressIoThread(null, /*bufferPoolSize=*/ 2,
                    (status, message) -> failure.compareAndSet(null, message));
            try {
                int cap = 64;
                long buf = Unsafe.malloc(cap, MemoryTag.NATIVE_DEFAULT);
                try {
                    int len = writeExecDoneTruncatedRowsAffected(buf);
                    io.onBinaryMessage(buf, len);
                } finally {
                    Unsafe.free(buf, cap, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                closePool(io);
            }
        });
        Assert.assertNotNull("terminal failure listener must fire on truncated EXEC_DONE varint",
                failure.get());
        Assert.assertTrue("error must blame truncation: " + failure.get(),
                failure.get().contains("truncated"));
    }

    /**
     * Regression: GEOHASH precisionBits on the wire must be in {@code [1, 60]}.
     * A hostile varint decoding to 0, 61, or anything above 60 used to
     * flow through as-is into {@code bytesPerValue = (precisionBits + 7) >>> 3},
     * generating nonsense bytesPerValue (e.g. 0 bytes per value for
     * precision=0, or 8+ bytes for a large precision) that skewed the
     * subsequent truncated-column check.
     */
    @Test
    public void testGeohashPrecisionAboveMaxIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeGeohashResultBatch(staging, /*precisionBits=*/ 61);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject GEOHASH precision above 60");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must mention GEOHASH precision: " + expected.getMessage(),
                            expected.getMessage().contains("GEOHASH precision"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Complementary regression: precision 0 (below the [1, 60] range) must
     * also be rejected. Without a lower-bound check the subsequent length
     * computation degenerates to zero bytes per value, masking the corruption.
     */
    @Test
    public void testGeohashPrecisionBelowMinIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeGeohashResultBatch(staging, /*precisionBits=*/ 0);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject GEOHASH precision 0");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must mention GEOHASH precision: " + expected.getMessage(),
                            expected.getMessage().contains("GEOHASH precision"));
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
     * Regression: a RESULT_END frame whose {@code final_seq} varint is
     * truncated (the frame ends before any byte with a clear continuation
     * bit) must be rejected. The previous code's seq-skip loop exited on
     * {@code p == limit} silently and the subsequent {@code total_rows}
     * loop saw no bytes, so the user received a successful
     * {@code asEnd(0)} for a malformed frame.
     */
    @Test
    public void testResultEndTruncatedFinalSeqVarintIsRejected() throws Exception {
        AtomicReference<String> failure = new AtomicReference<>();
        TestUtils.assertMemoryLeak(() -> {
            QwpEgressIoThread io = new QwpEgressIoThread(null, /*bufferPoolSize=*/ 2,
                    (status, message) -> failure.compareAndSet(null, message));
            try {
                int cap = 64;
                long buf = Unsafe.malloc(cap, MemoryTag.NATIVE_DEFAULT);
                try {
                    int len = writeResultEndTruncatedFinalSeq(buf);
                    io.onBinaryMessage(buf, len);
                } finally {
                    Unsafe.free(buf, cap, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                closePool(io);
            }
        });
        Assert.assertNotNull("terminal failure listener must fire on truncated RESULT_END final_seq",
                failure.get());
        Assert.assertTrue("error must mention RESULT_END final_seq: " + failure.get(),
                failure.get().contains("final_seq") && failure.get().contains("truncated"));
    }

    /**
     * Regression: a RESULT_END frame whose {@code total_rows} varint is
     * truncated must be rejected as a transport failure rather than emitted
     * as a successful end-of-result with a partially-decoded total. Before
     * the fix, the old {@code decodeResultEnd} returned the partial value
     * and the I/O thread happily delivered {@code asEnd(partial)} to the
     * user.
     */
    @Test
    public void testResultEndTruncatedTotalRowsVarintIsRejected() throws Exception {
        AtomicReference<String> failure = new AtomicReference<>();
        TestUtils.assertMemoryLeak(() -> {
            QwpEgressIoThread io = new QwpEgressIoThread(null, /*bufferPoolSize=*/ 2,
                    (status, message) -> failure.compareAndSet(null, message));
            try {
                int cap = 64;
                long buf = Unsafe.malloc(cap, MemoryTag.NATIVE_DEFAULT);
                try {
                    int len = writeResultEndTruncatedTotalRows(buf);
                    io.onBinaryMessage(buf, len);
                } finally {
                    Unsafe.free(buf, cap, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                closePool(io);
            }
        });
        Assert.assertNotNull("terminal failure listener must fire on truncated RESULT_END total_rows",
                failure.get());
        Assert.assertTrue("error must mention RESULT_END total_rows: " + failure.get(),
                failure.get().contains("total_rows") && failure.get().contains("truncated"));
    }

    /**
     * Regression: a STRING/VARCHAR offset that is negative (cast from a
     * hostile int32 with the sign bit set) must be rejected at parse time.
     * Without per-offset validation, parseStringColumn only checks
     * offset[nonNull]; the negative intermediate offset survives, and
     * lookupStringBytes computes {@code stringBytesAddr + (negative)} -- a
     * native address before the bytes region, leaking unrelated native
     * memory through the user callback.
     */
    @Test
    public void testStringColumnNegativeIntermediateOffsetIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeStringResultBatchWithOffsets(staging,
                        new int[]{-16, 5},
                        /*totalBytes=*/ 5,
                        /*bytesLen=*/ 5);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject negative intermediate offset");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must blame an offset, not totalBytes: " + expected.getMessage(),
                            expected.getMessage().contains("offset["));
                }
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
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
     * Regression: STRING/VARCHAR offsets must be monotonically non-decreasing.
     * Without this, lookupBinaryBytes computes {@code endOff - startOff} as a
     * negative int and {@code getBinary} surfaces NegativeArraySizeException.
     * Worse, with a startOff in the safe zone and a tiny negative-cast
     * endOff, the view's address range can wrap into unrelated native memory.
     */
    @Test
    public void testStringColumnNonMonotonicOffsetsAreRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeStringResultBatchWithOffsets(staging,
                        new int[]{3, 1},
                        /*totalBytes=*/ 5,
                        /*bytesLen=*/ 5);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject non-monotonic offsets");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must blame an offset: " + expected.getMessage(),
                            expected.getMessage().contains("offset["));
                }
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression: an intermediate offset that exceeds {@code totalBytes} must
     * be rejected. Without it, the cell view at that row would read past the
     * end of the bytes region into the next column or beyond the frame.
     */
    @Test
    public void testStringColumnOffsetExceedingTotalBytesIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // offset[0] = 0, offset[1] = 99 (well past totalBytes = 5).
                // offset[2] = totalBytes = 5 (but offset[1] > offset[2] also
                // violates monotonicity). The intent of the test is the
                // out-of-range value at offset[1]; the parser fires on the
                // first violation, so accept either rejection wording.
                int len = writeStringResultBatchWithOffsets(staging,
                        new int[]{0, 99},
                        /*totalBytes=*/ 5,
                        /*bytesLen=*/ 5);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject offset > totalBytes");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must blame an offset: " + expected.getMessage(),
                            expected.getMessage().contains("offset["));
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

    /**
     * Regression: a non-delta SYMBOL column with a dict size above
     * {@code rowCount} must be rejected. Prior to the fix, {@code dictSize}
     * was read as a raw int with no bound -- a hostile varint could drive
     * {@code dictSize * 8} to overflow silently, causing
     * {@code ensureOwnedEntriesAddr} to skip the realloc (because the
     * overflowed-negative requested size is less than any non-negative
     * capacity) and the subsequent {@code Unsafe.putLong} to write past the
     * allocated buffer.
     */
    @Test
    public void testSymbolColumnNonDeltaHugeDictSizeIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // rowCount=1 column carrying dictSize=Integer.MAX_VALUE (which
                // would drive dictSize*8 to overflow). Must reject on the bound
                // rather than letting the overflowed allocation through.
                int len = writeSymbolResultBatch(staging, /*rowCount=*/ 1, /*dictSize=*/ Integer.MAX_VALUE);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject SYMBOL dictSize above rowCount");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error must mention SYMBOL dict size: " + expected.getMessage(),
                            expected.getMessage().contains("SYMBOL dict size"));
                }
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression for CR-3: the table {@code name_len} varint must reject
     * negative-when-cast values (10-byte varint with bit 63 set on the final
     * data byte). Without the fix, the bound check {@code p + nameLen > limit}
     * passes (negative addend wraps {@code p} below {@code limit}), the
     * decoder advances {@code p} backwards, and the next decode reads garbage
     * from already-consumed bytes -- silent corruption rather than a clean
     * rejection.
     */
    @Test
    public void testTableNameLengthOverflowVarintIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeMinimalResultBatchWithRawNameLenVarint(staging,
                        // 10-byte varint encoding 0x8000_0000_0000_0000 (Long.MIN_VALUE).
                        // Each 0x80 byte sets the continuation bit and contributes
                        // 7 zero data bits; the final 0x01 byte sets bit 63.
                        new byte[]{
                                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                                (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01
                        });
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject negative-when-cast nameLen");
                } catch (QwpDecodeException expected) {
                    String msg = expected.getMessage();
                    Assert.assertTrue("error must blame varint overflow or table name length: " + msg,
                            msg.contains("varint overflow") || msg.contains("table name length"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Regression for CR-2: a RESULT_BATCH frame with FLAG_ZSTD set and a body
     * that is not a valid zstd frame must be rejected via the up-front
     * frame-header check, NOT by the old grow-the-scratch-and-retry loop. The
     * old code would double the scratch up to the 64 MiB cap on any negative
     * decompress return -- a single corrupt frame caused ~127 MiB of native
     * malloc/free churn and pinned 64 MiB resident for the rest of the
     * connection. The fix reads ZSTD_getFrameContentSize before allocating
     * anything; an invalid frame returns -2 and the decoder throws.
     */
    @Test
    public void testZstdCorruptBodyIsRejectedBeforeScratchGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                int len = writeResultBatchWithCorruptZstdBody(staging);
                buffer.copyFromPayload(staging, len);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject corrupt zstd frame");
                } catch (QwpDecodeException expected) {
                    String msg = expected.getMessage();
                    Assert.assertTrue("error must blame the frame header (rejected before decompress): " + msg,
                            msg.contains("frame header") || msg.contains("frame missing"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Reflective wrapper for the package-private {@code closePool()} on
     * {@link QwpEgressIoThread}, used to free the constructor-allocated
     * {@link QwpBatchBuffer} pool and ensure {@link TestUtils#assertMemoryLeak}
     * sees a clean delta. Mirrors the same pattern used by
     * {@link QwpEgressIoThreadCloseRaceTest}.
     */
    private static void closePool(QwpEgressIoThread io) {
        try {
            Method m = QwpEgressIoThread.class.getDeclaredMethod("closePool");
            m.setAccessible(true);
            m.invoke(io);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not invoke QwpEgressIoThread.closePool via reflection", e);
        }
    }

    /**
     * Writes a single byte and returns the advanced position. Part of the
     * wire-format helper set used to hand-craft a minimal RESULT_BATCH frame
     * in native memory, matching {@code QwpResultBatchDecoder.decodePayload +
     * parseStringColumn}:
     * <pre>
     *   header (12 bytes)
     *   msg_kind (0x11)
     *   request_id (8 bytes)
     *   batch_seq (varint)
     *   table-block:
     *     name_len (varint), name bytes (none)
     *     row_count (varint)
     *     column_count (varint, batch_seq == 0 only)
     *     per column (batch_seq == 0 only): name_len varint, name bytes, wire_type byte
     *     per column: null_flag byte (+optional bitmap), then column body
     * </pre>
     */
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

    /**
     * Crafts a RESULT_BATCH carrying a single TYPE_DOUBLE_ARRAY column with
     * one row whose dimensions are caller-supplied. Each dimension takes
     * 4 bytes on the wire; the row's payload follows as
     * {@code product(dims)} 8-byte slots, all zero. With {@code dims}
     * containing a 0 entry the helper writes 0 payload bytes, exercising
     * the dim-zero rejection in {@link QwpResultBatchDecoder}.
     */
    private static int writeArrayResultBatchWithDims(long buf, int[] dims) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);                         // batch_seq
        p = putVarint(p, 0L);                         // table_name_len
        p = putVarint(p, 1L);                         // row_count = 1
        p = putVarint(p, 1L);                         // column_count
        p = putVarint(p, 1L);                         // column name length
        p = putByte(p, (byte) 'a');
        p = putByte(p, QwpConstants.TYPE_DOUBLE_ARRAY);
        p = putByte(p, (byte) 0);                     // null_flag = 0
        p = putByte(p, (byte) dims.length);           // nDims
        long elements = 1;
        for (int dl : dims) {
            p = putInt(p, dl);
            elements *= dl;
        }
        for (long e = 0; e < elements; e++) {
            p = putLong(p, 0L);                       // zero-valued payload
        }
        return (int) (p - buf);
    }

    /**
     * Crafts a RESULT_BATCH frame with FLAG_DELTA_SYMBOL_DICT set and a
     * delta-dict section of one entry whose length varint is supplied as
     * raw bytes. No further table block is emitted; the decoder must fail
     * in {@code parseDeltaSymbolDict} before reaching it.
     */
    private static int writeDeltaSymbolDictFrame(long buf, byte[] entryLenVarint) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        // Flags byte lives at HEADER_OFFSET_FLAGS (=5). Existing helpers with
        // flags=0 don't care about the exact slot; a delta-mode frame does.
        p = putByte(p, QwpConstants.FLAG_DELTA_SYMBOL_DICT);
        p = putByte(p, (byte) 0);       // reserved slot at offset 6
        p = putByte(p, (byte) 1);       // table_count
        p = putInt(p, 0);               // payload_length placeholder
        p = putByte(p, (byte) 0x11);    // msg_kind = RESULT_BATCH
        p = putLong(p, 1L);             // request_id
        p = putVarint(p, 0L);           // batch_seq
        // Delta dict section: deltaStart=0, deltaCount=1, then the raw entry
        // length varint the test wants to probe.
        p = putVarint(p, 0L);
        p = putVarint(p, 1L);
        for (byte b : entryLenVarint) p = putByte(p, b);
        return (int) (p - buf);
    }

    /**
     * Crafts a RESULT_BATCH frame with FLAG_DELTA_SYMBOL_DICT set, carrying
     * caller-supplied {@code deltaStart} / {@code deltaCount} as varints. No
     * entry payload follows; the decoder must fail in
     * {@code parseDeltaSymbolDict} on the range guard before reaching it.
     * Used to probe the range-guard for additive-overflow attacks.
     */
    private static int writeDeltaSymbolDictFrameWithRange(long buf, long deltaStart, long deltaCount) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, QwpConstants.FLAG_DELTA_SYMBOL_DICT);  // flags at HEADER_OFFSET_FLAGS
        p = putByte(p, (byte) 0);                              // reserved
        p = putByte(p, (byte) 1);                              // table_count
        p = putInt(p, 0);                                      // payload_length placeholder
        p = putByte(p, (byte) 0x11);                           // msg_kind = RESULT_BATCH
        p = putLong(p, 1L);                                    // request_id
        p = putVarint(p, 0L);                                  // batch_seq
        p = putVarint(p, deltaStart);
        p = putVarint(p, deltaCount);
        return (int) (p - buf);
    }

    /**
     * Header (12 bytes) + msg_kind(1) + request_id(8) + op_type(1) + a single
     * 0x80 byte with the continuation bit set and no terminator. The varint
     * loop must reject this rather than emit the partial rows_affected value
     * to the user.
     */
    private static int writeExecDoneTruncatedRowsAffected(long buf) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, QwpEgressMsgKind.EXEC_DONE);
        p = putLong(p, 1L);
        p = putByte(p, (byte) 0);            // op_type
        p = putByte(p, (byte) 0x80);         // continuation set, no terminator follows
        return (int) (p - buf);
    }

    /**
     * Crafts a minimal RESULT_BATCH frame carrying a single GEOHASH column
     * with {@code rowCount=0} and caller-chosen {@code precisionBits}.
     * Because rowCount is zero, no per-row value bytes follow the precision
     * varint; the decoder still decodes the precision up front and the range
     * check must fire there.
     */
    private static int writeGeohashResultBatch(long buf, long precisionBits) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);       // header msg_kind (unused)
        p = putByte(p, (byte) 0);       // flags (no delta dict)
        p = putByte(p, (byte) 1);       // table_count
        p = putInt(p, 0);               // payload_length placeholder
        p = putByte(p, (byte) 0x11);    // msg_kind = RESULT_BATCH
        p = putLong(p, 1L);             // request_id
        p = putVarint(p, 0L);           // batch_seq
        p = putVarint(p, 0L);           // table_name_len
        p = putVarint(p, 0L);           // row_count (no data rows)
        p = putVarint(p, 1L);           // column_count
        // Schema: one column "g" of TYPE_GEOHASH.
        p = putVarint(p, 1L);
        p = putByte(p, (byte) 'g');
        p = putByte(p, QwpConstants.TYPE_GEOHASH);
        // Column body: null_flag=0 (no nulls), then precision_bits varint.
        // With row_count=0 no value bytes follow.
        p = putByte(p, (byte) 0);       // null_flag
        p = putVarint(p, precisionBits);
        return (int) (p - buf);
    }

    private static int writeMinimalResultBatch(long buf) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
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
        return (int) (p - buf);
    }

    /**
     * Variant of {@link #writeMinimalResultBatch} that injects a raw varint
     * sequence for the table {@code name_len} field. Used by the CR-3
     * regression test to drive the negative-nameLen bound-check bypass.
     */
    private static int writeMinimalResultBatchWithRawNameLenVarint(long buf, byte[] nameLenVarint) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);                         // batch_seq
        for (byte b : nameLenVarint) p = putByte(p, b);
        return (int) (p - buf);
    }

    /**
     * Crafts a RESULT_BATCH frame whose flags byte advertises FLAG_ZSTD but
     * whose body is junk (not a valid zstd frame). Used by
     * {@link #testZstdCorruptBodyIsRejectedBeforeScratchGrowth}.
     */
    private static int writeResultBatchWithCorruptZstdBody(long buf) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, QwpConstants.FLAG_ZSTD);       // flags byte (offset 5) -- FLAG_ZSTD set
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);                  // msg_kind = RESULT_BATCH
        p = putLong(p, 1L);                           // request_id
        p = putVarint(p, 0L);                         // batch_seq
        // Following bytes claim to be a zstd frame but aren't -- not the magic
        // (0x28 0xB5 0x2F 0xFD), no header, just a few junk bytes.
        for (int i = 0; i < 16; i++) p = putByte(p, (byte) (0xAA ^ i));
        return (int) (p - buf);
    }

    /**
     * Header (12 bytes) + msg_kind(1) + request_id(8) + final_seq truncated
     * (single 0x80 byte, frame ends before any clear-continuation byte). The
     * final_seq skip loop must reject the truncation rather than fall
     * through to total_rows with a desynced cursor.
     */
    private static int writeResultEndTruncatedFinalSeq(long buf) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, QwpEgressMsgKind.RESULT_END);
        p = putLong(p, 1L);
        p = putByte(p, (byte) 0x80);         // final_seq truncated mid-varint
        return (int) (p - buf);
    }

    /**
     * Header (12 bytes) + msg_kind(1) + request_id(8) + final_seq(1 byte
     * varint = 0) + total_rows truncated (single 0x80 byte). The total_rows
     * loop must reject the truncation.
     */
    private static int writeResultEndTruncatedTotalRows(long buf) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, QwpEgressMsgKind.RESULT_END);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);                // final_seq = 0 (single 0x00 byte)
        p = putByte(p, (byte) 0x80);         // total_rows truncated mid-varint
        return (int) (p - buf);
    }

    private static int writeStringResultBatch(long buf, int nonNull, int totalBytes) {
        long p = buf;
        // Header: magic + version + msg_kind + flags + table_count + payload_length
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);   // 4
        p = putByte(p, QwpConstants.VERSION);       // 1
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
        // Schema entries: one column "s" of TYPE_VARCHAR
        p = putVarint(p, 1L);                         // column name length
        p = putByte(p, (byte) 's');
        p = putByte(p, QwpConstants.TYPE_VARCHAR);
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

    /**
     * Crafts a RESULT_BATCH carrying a single VARCHAR column with
     * caller-supplied per-row offsets, an explicit {@code totalBytes}, and a
     * payload tail of {@code bytesLen} arbitrary bytes. Lets tests probe the
     * per-offset validation in {@link QwpResultBatchDecoder} (negative,
     * non-monotonic, or out-of-range entries).
     * <p>
     * {@code intOffsets.length} must equal the column's non-null row count;
     * the helper appends offset[N] = totalBytes for the (N+1)th slot the
     * decoder requires.
     */
    private static int writeStringResultBatchWithOffsets(long buf, int[] intOffsets, int totalBytes, int bytesLen) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 7L);
        p = putVarint(p, 0L);
        p = putVarint(p, 0L);                         // table_name_len
        p = putVarint(p, intOffsets.length);          // row_count = nonNull
        p = putVarint(p, 1L);                         // column_count
        p = putVarint(p, 1L);                         // column name length
        p = putByte(p, (byte) 's');
        p = putByte(p, QwpConstants.TYPE_VARCHAR);
        p = putByte(p, (byte) 0);                     // null_flag = 0
        for (int off : intOffsets) {
            p = putInt(p, off);                       // offset[i]
        }
        p = putInt(p, totalBytes);                    // offset[nonNull]
        for (int i = 0; i < bytesLen; i++) {
            p = putByte(p, (byte) 'a');
        }
        return (int) (p - buf);
    }

    /**
     * Crafts a RESULT_BATCH carrying a single SYMBOL column in non-delta mode
     * (FLAG_DELTA_SYMBOL_DICT unset). The caller supplies the raw
     * {@code dictSize} varint so bounds tests can push values that would
     * otherwise flow through untouched.
     */
    private static int writeSymbolResultBatch(long buf, long rowCount, long dictSize) {
        long p = buf;
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 0);       // flags: no delta dict, so non-delta SYMBOL path
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);           // batch_seq
        p = putVarint(p, 0L);           // table_name_len
        p = putVarint(p, rowCount);
        p = putVarint(p, 1L);           // column_count
        // Schema: one column "s" of TYPE_SYMBOL.
        p = putVarint(p, 1L);
        p = putByte(p, (byte) 's');
        p = putByte(p, QwpConstants.TYPE_SYMBOL);
        // Column body: null_flag=0 (no nulls), then the dict_size varint.
        // The decoder reads dict_size and must reject before attempting to
        // decode dict_size entries (which don't even exist in this frame).
        p = putByte(p, (byte) 0);
        p = putVarint(p, dictSize);
        return (int) (p - buf);
    }
}
