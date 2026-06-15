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

import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpResultBatchDecoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * Per-wire-type and per-feature coverage for {@link QwpResultBatchDecoder}.
 * Sister to {@link QwpResultBatchDecoderHardeningTest}: the hardening file
 * pins the rejection paths for malformed frames, this file pins the
 * happy-path decoders for every wire type plus the connection-scoped state
 * machines (delta SYMBOL dict, FLAG_GORILLA TIMESTAMP, FLAG_DELTA_SYMBOL_DICT,
 * CACHE_RESET, batch_seq schema reuse). Each test crafts a single-column or
 * minimal multi-column RESULT_BATCH in native memory and verifies the values
 * surface correctly through {@link io.questdb.client.cutlass.qwp.client.QwpColumnBatch}
 * accessors, mirroring the production end-to-end flow without a server.
 */
public class QwpResultBatchDecoderColumnTypesTest {

    @Test
    public void testBadMagicRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(64);
            long staging = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = putInt(p, 0xDEADBEEF);                       // wrong magic
                for (int i = 4; i < 32; i++) p = putByte(p, (byte) 0);
                buffer.copyFromPayload(staging, 32);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject bad magic");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions magic: " + expected.getMessage(),
                            expected.getMessage().contains("magic"));
                }
            } finally {
                Unsafe.free(staging, 64, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testBooleanColumnDecodesBitPacked() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            // 10 booleans across 2 bytes (LSB-first within each byte). Bits:
            //   row:    0 1 2 3 4 5 6 7 8 9
            //   value:  T F T F F T T F T F
            // byte 0 = 0b01100101 = 0x65, byte 1 = 0b00000001 = 0x01
            int rows = 10;
            long p = startSingleColumnFrame(staging, "b", QwpConstants.TYPE_BOOLEAN, rows);
            p = putByte(p, (byte) 0);
            p = putByte(p, (byte) 0x65);
            p = putByte(p, (byte) 0x01);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            boolean[] expected = {true, false, true, false, false, true, true, false, true, false};
            for (int i = 0; i < rows; i++) {
                Assert.assertEquals("row " + i, expected[i], batchOf(buffer).getBoolValue(0, i));
            }
        }));
    }

    @Test
    public void testByteColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            byte[] values = {Byte.MIN_VALUE, -1, 0, 7, Byte.MAX_VALUE};
            long p = startSingleColumnFrame(staging, "b", QwpConstants.TYPE_BYTE, values.length);
            p = putByte(p, (byte) 0);
            for (byte v : values) p = putByte(p, v);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            for (int i = 0; i < values.length; i++) {
                Assert.assertEquals("row " + i, values[i], batchOf(buffer).getByteValue(0, i));
            }
        }));
    }

    @Test
    public void testCacheResetDictMaskClearsConnectionDict() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // Seed the dict with one entry via a delta-mode batch.
                long p = staging;
                p = writeHeader(p, QwpConstants.FLAG_DELTA_SYMBOL_DICT);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);
                p = putVarint(p, 0L);
                p = putVarint(p, 1L);
                byte[] aaa = "aaa".getBytes(StandardCharsets.UTF_8);
                p = putVarint(p, aaa.length);
                for (byte b : aaa) p = putByte(p, b);
                p = putVarint(p, 0L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 's');
                p = putByte(p, QwpConstants.TYPE_SYMBOL);
                p = putByte(p, (byte) 0);
                p = putVarint(p, 0L);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals("aaa", batchOf(buffer).getSymbol(0, 0));

                // Apply CACHE_RESET with the dict mask -- the connection dict
                // empties and the next batch must restart deltaStart from 0.
                decoder.applyCacheReset(QwpEgressMsgKind.RESET_MASK_DICT);

                // Batch 2: deltaStart = 0 again. Re-add a different entry.
                p = staging;
                p = writeHeader(p, QwpConstants.FLAG_DELTA_SYMBOL_DICT);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);                          // batch_seq = 0 (independent batch)
                p = putVarint(p, 0L);                          // deltaStart = 0 after reset
                p = putVarint(p, 1L);
                byte[] bbb = "bbb".getBytes(StandardCharsets.UTF_8);
                p = putVarint(p, bbb.length);
                for (byte b : bbb) p = putByte(p, b);
                p = putVarint(p, 0L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 's');
                p = putByte(p, QwpConstants.TYPE_SYMBOL);
                p = putByte(p, (byte) 0);
                p = putVarint(p, 0L);

                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals("bbb", batchOf(buffer).getSymbol(0, 0));
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testCharColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            char[] values = {'a', 'Z', '0', '\u0000', '\uffff'};
            long p = startSingleColumnFrame(staging, "c", QwpConstants.TYPE_CHAR, values.length);
            p = putByte(p, (byte) 0);
            for (char v : values) p = putShort(p, (short) v);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            for (int i = 0; i < values.length; i++) {
                Assert.assertEquals("row " + i, values[i], batchOf(buffer).getCharValue(0, i));
            }
        }));
    }

    @Test
    public void testDecimal128ColumnDecodesScaleByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            int rows = 1;
            long p = startSingleColumnFrame(staging, "d", QwpConstants.TYPE_DECIMAL128, rows);
            p = putByte(p, (byte) 0);
            p = putByte(p, (byte) 4);                  // scale
            p = putLong(p, 0xCAFEBABEDEADBEEFL);       // low 8 bytes
            p = putLong(p, 0x0102030405060708L);       // high 8 bytes
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals(4, batchOf(buffer).getDecimalScale(0));
            Assert.assertEquals(0xCAFEBABEDEADBEEFL, batchOf(buffer).getDecimal128Low(0, 0));
            Assert.assertEquals(0x0102030405060708L, batchOf(buffer).getDecimal128High(0, 0));
        }));
    }

    @Test
    public void testDoubleColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            double[] values = {Double.NEGATIVE_INFINITY, -1.5, 0.0, 1.5, Double.POSITIVE_INFINITY, Double.NaN};
            long p = startSingleColumnFrame(staging, "d", QwpConstants.TYPE_DOUBLE, values.length);
            p = putByte(p, (byte) 0);
            for (double v : values) {
                p = putLong(p, Double.doubleToRawLongBits(v));
            }
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            for (int i = 0; i < values.length; i++) {
                double got = batchOf(buffer).getDoubleValue(0, i);
                if (Double.isNaN(values[i])) {
                    Assert.assertTrue("row " + i + " NaN", Double.isNaN(got));
                } else {
                    Assert.assertEquals("row " + i, values[i], got, 0.0);
                }
            }
        }));
    }

    @Test
    public void testEmptyBatchDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(128, (decoder, buffer, staging) -> {
            long p = staging;
            p = writeHeader(p, /*flags=*/(byte) 0);
            p = putByte(p, (byte) 0x11);                 // RESULT_BATCH
            p = putLong(p, 1L);                          // request_id
            p = putVarint(p, 0L);                        // batch_seq
            p = putVarint(p, 0L);                        // table_name_len
            p = putVarint(p, 0L);                        // row_count = 0
            p = putVarint(p, 0L);                        // column_count = 0

            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals(0, batchOf(buffer).getRowCount());
            Assert.assertEquals(0, batchOf(buffer).getColumnCount());
        }));
    }

    @Test
    public void testEmptyResultSetCarriesSchema() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(128, (decoder, buffer, staging) -> {
            // Realistic empty result: batch_seq == 0, row_count == 0, but the schema
            // is present (column_count > 0 with descriptors, no row bodies). The
            // server always ships the schema on batch 0 even when the result is empty.
            long p = staging;
            p = writeHeader(p, (byte) 0);
            p = putByte(p, (byte) 0x11);                  // RESULT_BATCH
            p = putLong(p, 1L);                           // request_id
            p = putVarint(p, 0L);                         // batch_seq = 0
            p = putVarint(p, 0L);                         // table_name_len
            p = putVarint(p, 0L);                         // row_count = 0
            p = putVarint(p, 2L);                         // column_count = 2
            p = putVarint(p, 1L);
            p = putByte(p, (byte) 'a');
            p = putByte(p, QwpConstants.TYPE_INT);
            p = putVarint(p, 1L);
            p = putByte(p, (byte) 'b');
            p = putByte(p, QwpConstants.TYPE_LONG);
            // Each column still carries its null-flag byte even at row_count == 0
            // (no nulls -> 0x00); the decoder reads one per column. No row bodies.
            p = putByte(p, (byte) 0);                    // col "a" null_flag
            p = putByte(p, (byte) 0);                    // col "b" null_flag
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals(0, batchOf(buffer).getRowCount());
            Assert.assertEquals(2, batchOf(buffer).getColumnCount());
            Assert.assertEquals("a", batchOf(buffer).getColumnName(0));
            Assert.assertEquals("b", batchOf(buffer).getColumnName(1));
        }));
    }

    @Test
    public void testGeohashColumnReadsPrecisionVarint() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            int rows = 2;
            long p = startSingleColumnFrame(staging, "g", QwpConstants.TYPE_GEOHASH, rows);
            p = putByte(p, (byte) 0);
            // Precision = 20 bits -> ceil(20/8) = 3 bytes per value.
            p = putVarint(p, 20L);
            // Two values, 3 bytes each.
            p = putByte(p, (byte) 0x01);
            p = putByte(p, (byte) 0x02);
            p = putByte(p, (byte) 0x03);
            p = putByte(p, (byte) 0x04);
            p = putByte(p, (byte) 0x05);
            p = putByte(p, (byte) 0x06);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals(rows, batchOf(buffer).getRowCount());
            // We only need to verify the decoder accepted the layout; the
            // GEOHASH accessor is not in scope here.
        }));
    }

    @Test
    public void testIntColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            int[] values = {Integer.MIN_VALUE, -1, 0, 42, Integer.MAX_VALUE};
            long p = startSingleColumnFrame(staging, "n", QwpConstants.TYPE_INT, values.length);
            p = putByte(p, (byte) 0);
            for (int v : values) p = putInt(p, v);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            for (int i = 0; i < values.length; i++) {
                Assert.assertEquals("row " + i, values[i], batchOf(buffer).getIntValue(0, i));
            }
        }));
    }

    @Test
    public void testLongArrayColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(512, (decoder, buffer, staging) -> {
            // 2 rows, each a 1-D array of 3 longs.
            int rows = 2;
            long p = startSingleColumnFrame(staging, "a", QwpConstants.TYPE_LONG_ARRAY, rows);
            p = putByte(p, (byte) 0);
            // row 0: nDims=1, dim=3, [10, 20, 30]
            p = putByte(p, (byte) 1);
            p = putInt(p, 3);
            p = putLong(p, 10L);
            p = putLong(p, 20L);
            p = putLong(p, 30L);
            // row 1: nDims=1, dim=3, [40, 50, 60]
            p = putByte(p, (byte) 1);
            p = putInt(p, 3);
            p = putLong(p, 40L);
            p = putLong(p, 50L);
            p = putLong(p, 60L);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals(rows, batchOf(buffer).getRowCount());
        }));
    }

    @Test
    public void testLongColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(512, (decoder, buffer, staging) -> {
            long[] values = {Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE};
            long p = startSingleColumnFrame(staging, "v", QwpConstants.TYPE_LONG, values.length);
            p = putByte(p, (byte) 0); // null_flag = 0 (no nulls)
            for (long v : values) p = putLong(p, v);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            for (int i = 0; i < values.length; i++) {
                Assert.assertFalse(batchOf(buffer).isNull(0, i));
                Assert.assertEquals("row " + i, values[i], batchOf(buffer).getLongValue(0, i));
            }
        }));
    }

    @Test
    public void testNegativeArrayDimRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = startSingleColumnFrame(staging, "a", QwpConstants.TYPE_LONG_ARRAY, 1);
                p = putByte(p, (byte) 0);
                p = putByte(p, (byte) 1);
                p = putInt(p, -1);                              // hostile negative dim

                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject negative ARRAY dim");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions negative dim: " + expected.getMessage(),
                            expected.getMessage().contains("ARRAY dim"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testNullBitmapPopulatesNonNullIdx() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            // 4 rows: row 0 non-null, row 1 NULL, row 2 non-null, row 3 NULL.
            // bitmap byte 0: bit 1 + bit 3 set = 0b00001010 = 0x0A.
            // nonNullCount = 2, dense values = 2 longs.
            int rows = 4;
            long p = startSingleColumnFrame(staging, "v", QwpConstants.TYPE_LONG, rows);
            p = putByte(p, (byte) 1);        // null_flag = 1 -> bitmap follows
            p = putByte(p, (byte) 0x0A);     // bitmap byte
            p = putLong(p, 100L);            // dense[0] -> row 0
            p = putLong(p, 200L);            // dense[1] -> row 2
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);

            Assert.assertFalse(batchOf(buffer).isNull(0, 0));
            Assert.assertTrue(batchOf(buffer).isNull(0, 1));
            Assert.assertFalse(batchOf(buffer).isNull(0, 2));
            Assert.assertTrue(batchOf(buffer).isNull(0, 3));
            Assert.assertEquals(100L, batchOf(buffer).getLongValue(0, 0));
            Assert.assertEquals(200L, batchOf(buffer).getLongValue(0, 2));
        }));
    }

    @Test
    public void testResetQuerySchemaRejectsContinuationFromPriorQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // Query A delivers its schema on batch_seq == 0.
                long p = startSingleColumnFrame(staging, "n", QwpConstants.TYPE_INT, 1);
                p = putByte(p, (byte) 0);
                p = putInt(p, 99);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(99, batchOf(buffer).getIntValue(0, 0));

                // The IoThread calls resetQuerySchema() when the next query starts.
                // After that, query A's schema must no longer satisfy a continuation:
                // a batch_seq > 0 arriving before the new query's batch_seq == 0 must
                // be rejected, not bound to the prior query's schema.
                decoder.resetQuerySchema();

                p = staging;
                p = writeHeader(p, (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 1L);                          // batch_seq = 1 (continuation)
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 1L);                          // row_count
                p = putByte(p, (byte) 0);                      // null_flag
                p = putInt(p, 1234);                           // value
                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject a continuation after resetQuerySchema()");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions the missing schema batch: " + expected.getMessage(),
                            expected.getMessage().contains("batch_seq"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testSchemaMissingOnContinuationRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // A continuation batch (batch_seq > 0) arriving before any
                // schema-bearing batch_seq == 0 must be rejected, not bound to a
                // stale schema.
                long p = staging;
                p = writeHeader(p, (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 1L);                          // batch_seq = 1 (continuation)
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 0L);                          // row_count
                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject a continuation batch with no prior schema");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions the missing schema batch: " + expected.getMessage(),
                            expected.getMessage().contains("batch_seq"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testSchemaReusedAcrossContinuationBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // Batch 1 (batch_seq == 0) carries the schema: one INT column "n".
                long p = startSingleColumnFrame(staging, "n", QwpConstants.TYPE_INT, 1);
                p = putByte(p, (byte) 0);
                p = putInt(p, 99);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(99, batchOf(buffer).getIntValue(0, 0));

                // Batch 2 (batch_seq == 1) carries rows only and reuses the schema
                // parsed from batch 0 -- no column_count / column descriptors inline.
                p = staging;
                p = writeHeader(p, /*flags=*/ (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 1L);                          // batch_seq = 1 (continuation)
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 1L);                          // row_count
                p = putByte(p, (byte) 0);                      // null_flag
                p = putInt(p, 1234);                           // value
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(1234, batchOf(buffer).getIntValue(0, 0));
                Assert.assertEquals("n", batchOf(buffer).getColumnName(0));
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testSchemaSlotsReusedAcrossQueriesWithDifferentTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // Query A: one INT column "i". Seeds pooled schema slot 0 with
                // name "i" / wire-type INT.
                long p = startSingleColumnFrame(staging, "i", QwpConstants.TYPE_INT, 1);
                p = putByte(p, (byte) 0);
                p = putInt(p, 5);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(5, batchOf(buffer).getIntValue(0, 0));

                decoder.resetQuerySchema();

                // Query B batch_seq == 0: two columns LONG "p", INT "q". Slot 0 is
                // reused from query A and must be fully overwritten (name "p",
                // wire-type LONG), not retain A's "i"/INT; slot 1 is freshly grown.
                p = staging;
                p = writeHeader(p, (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);                          // batch_seq = 0
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 1L);                          // row_count
                p = putVarint(p, 2L);                          // column_count
                p = putVarint(p, 1L);                          // col 0 name length
                p = putByte(p, (byte) 'p');
                p = putByte(p, QwpConstants.TYPE_LONG);
                p = putVarint(p, 1L);                          // col 1 name length
                p = putByte(p, (byte) 'q');
                p = putByte(p, QwpConstants.TYPE_INT);
                p = putByte(p, (byte) 0);                      // col 0 null_flag
                p = putLong(p, 9_876_543_210L);               // col 0 value (LONG)
                p = putByte(p, (byte) 0);                      // col 1 null_flag
                p = putInt(p, 42);                             // col 1 value (INT)
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);

                Assert.assertEquals(2, batchOf(buffer).getColumnCount());
                Assert.assertEquals("p", batchOf(buffer).getColumnName(0));
                Assert.assertEquals(QwpConstants.TYPE_LONG, batchOf(buffer).getColumnWireType(0));
                Assert.assertEquals(9_876_543_210L, batchOf(buffer).getLongValue(0, 0));
                Assert.assertEquals("q", batchOf(buffer).getColumnName(1));
                Assert.assertEquals(42, batchOf(buffer).getIntValue(1, 0));
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testSchemaSwitchesAcrossQueriesWithDifferentColumnCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // Query A batch_seq == 0: two INT columns "a", "b" -- grows the
                // pooled schema to 2 slots.
                long p = staging;
                p = writeHeader(p, (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);                          // batch_seq = 0
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 1L);                          // row_count
                p = putVarint(p, 2L);                          // column_count
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 'a');
                p = putByte(p, QwpConstants.TYPE_INT);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 'b');
                p = putByte(p, QwpConstants.TYPE_INT);
                p = putByte(p, (byte) 0);                      // col a null_flag
                p = putInt(p, 10);
                p = putByte(p, (byte) 0);                      // col b null_flag
                p = putInt(p, 20);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(2, batchOf(buffer).getColumnCount());
                Assert.assertEquals(10, batchOf(buffer).getIntValue(0, 0));
                Assert.assertEquals(20, batchOf(buffer).getIntValue(1, 0));

                decoder.resetQuerySchema();

                // Query B batch_seq == 0: a single INT column "x" -- shrinks the
                // pooled schema back to 1 slot.
                p = startSingleColumnFrame(staging, "x", QwpConstants.TYPE_INT, 1);
                p = putByte(p, (byte) 0);
                p = putInt(p, 77);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(1, batchOf(buffer).getColumnCount());
                Assert.assertEquals(77, batchOf(buffer).getIntValue(0, 0));

                // Query B continuation (batch_seq == 1): rows only. It binds to
                // query B's 1-column schema -- columnCount derives from
                // querySchema.size(), which must have shrunk to 1 (not stale at 2).
                p = staging;
                p = writeHeader(p, (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 1L);                          // batch_seq = 1
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 1L);                          // row_count
                p = putByte(p, (byte) 0);                      // null_flag
                p = putInt(p, 88);
                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals(1, batchOf(buffer).getColumnCount());
                Assert.assertEquals("x", batchOf(buffer).getColumnName(0));
                Assert.assertEquals(88, batchOf(buffer).getIntValue(0, 0));
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testSymbolColumnDeltaModeAccumulatesAcrossBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            try {
                // Batch 1: dict starts empty, add ["alpha", "beta"], 2 rows referencing them.
                long p = staging;
                p = writeHeader(p, QwpConstants.FLAG_DELTA_SYMBOL_DICT);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);                          // batch_seq
                // Delta dict section: deltaStart=0, deltaCount=2, then len+bytes.
                p = putVarint(p, 0L);
                p = putVarint(p, 2L);
                byte[] alpha = "alpha".getBytes(StandardCharsets.UTF_8);
                byte[] beta = "beta".getBytes(StandardCharsets.UTF_8);
                p = putVarint(p, alpha.length);
                for (byte b : alpha) p = putByte(p, b);
                p = putVarint(p, beta.length);
                for (byte b : beta) p = putByte(p, b);
                // Table block.
                p = putVarint(p, 0L);                          // table_name_len
                p = putVarint(p, 2L);                          // row_count
                p = putVarint(p, 1L);                          // column_count
                p = putVarint(p, 1L);                          // colName length
                p = putByte(p, (byte) 's');
                p = putByte(p, QwpConstants.TYPE_SYMBOL);
                // Column body: null_flag = 0, then per-row varint dict ids.
                p = putByte(p, (byte) 0);
                p = putVarint(p, 0L);                          // row 0 -> "alpha"
                p = putVarint(p, 1L);                          // row 1 -> "beta"

                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals("alpha", batchOf(buffer).getSymbol(0, 0));
                Assert.assertEquals("beta", batchOf(buffer).getSymbol(0, 1));

                // Batch 2: dict already has 2 entries; add 1 more ["gamma"], 1 row referencing it.
                p = staging;
                p = writeHeader(p, QwpConstants.FLAG_DELTA_SYMBOL_DICT);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 1L);
                p = putVarint(p, 2L);                          // deltaStart = current size
                p = putVarint(p, 1L);
                byte[] gamma = "gamma".getBytes(StandardCharsets.UTF_8);
                p = putVarint(p, gamma.length);
                for (byte b : gamma) p = putByte(p, b);
                p = putVarint(p, 0L);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 0);
                p = putVarint(p, 2L);                          // row 0 -> "gamma" (id=2)

                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                Assert.assertEquals("gamma", batchOf(buffer).getSymbol(0, 0));
            } finally {
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testSymbolColumnNonDeltaInlineDictionary() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(512, (decoder, buffer, staging) -> {
            long p = staging;
            p = writeHeader(p, /*flags=*/ (byte) 0);
            p = putByte(p, (byte) 0x11);
            p = putLong(p, 1L);
            p = putVarint(p, 0L);
            p = putVarint(p, 0L);
            p = putVarint(p, 3L);                          // row_count
            p = putVarint(p, 1L);                          // column_count
            p = putVarint(p, 1L);
            p = putByte(p, (byte) 's');
            p = putByte(p, QwpConstants.TYPE_SYMBOL);
            // Non-delta SYMBOL: null_flag, dict_size varint, len+bytes per entry, then per-row ids.
            p = putByte(p, (byte) 0);
            p = putVarint(p, 2L);                          // dictSize
            byte[] x = "xx".getBytes(StandardCharsets.UTF_8);
            byte[] y = "yyyy".getBytes(StandardCharsets.UTF_8);
            p = putVarint(p, x.length);
            for (byte b : x) p = putByte(p, b);
            p = putVarint(p, y.length);
            for (byte b : y) p = putByte(p, b);
            p = putVarint(p, 0L);
            p = putVarint(p, 1L);
            p = putVarint(p, 0L);

            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals("xx", batchOf(buffer).getSymbol(0, 0));
            Assert.assertEquals("yyyy", batchOf(buffer).getSymbol(0, 1));
            Assert.assertEquals("xx", batchOf(buffer).getSymbol(0, 2));
        }));
    }

    @Test
    public void testSymbolIndexOutOfRangeRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = writeHeader(p, /*flags=*/ (byte) 0);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);
                p = putVarint(p, 0L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 's');
                p = putByte(p, QwpConstants.TYPE_SYMBOL);
                p = putByte(p, (byte) 0);
                p = putVarint(p, 1L);                          // dictSize = 1 (only id 0 valid)
                byte[] only = "only".getBytes(StandardCharsets.UTF_8);
                p = putVarint(p, only.length);
                for (byte b : only) p = putByte(p, b);
                p = putVarint(p, 5L);                          // hostile id 5

                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject symbol id beyond dictSize");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions symbol index: " + expected.getMessage(),
                            expected.getMessage().contains("symbol index"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testTimestampColumnGorillaEncoding() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(512);
            long staging = Unsafe.malloc(512, MemoryTag.NATIVE_DEFAULT);
            // Pre-encode timestamps via the matching encoder, then splice the
            // result into a RESULT_BATCH frame whose flags advertise FLAG_GORILLA.
            long[] values = {1_700_000_000L, 1_700_000_100L, 1_700_000_205L,
                    1_700_000_320L, 1_700_000_440L};
            int srcLen = values.length * 8;
            int gorillaCap = 256;
            long src = Unsafe.malloc(srcLen, MemoryTag.NATIVE_DEFAULT);
            long gorillaScratch = Unsafe.malloc(gorillaCap, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < values.length; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, values[i]);
                }
                QwpGorillaEncoder enc = new QwpGorillaEncoder();
                int gorillaWritten = enc.encodeTimestamps(gorillaScratch, gorillaCap, src, values.length);

                long p = staging;
                p = writeHeader(p, QwpConstants.FLAG_GORILLA);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);
                p = putVarint(p, 0L);
                p = putVarint(p, values.length);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 't');
                p = putByte(p, QwpConstants.TYPE_TIMESTAMP);
                p = putByte(p, (byte) 0);                      // null_flag
                p = putByte(p, (byte) 0x01);                   // encoding = Gorilla
                for (int i = 0; i < gorillaWritten; i++) {
                    p = putByte(p, Unsafe.getUnsafe().getByte(gorillaScratch + i));
                }

                buffer.copyFromPayload(staging, (int) (p - staging));
                decoder.decode(buffer);
                for (int i = 0; i < values.length; i++) {
                    Assert.assertEquals("row " + i, values[i], batchOf(buffer).getLongValue(0, i));
                }
            } finally {
                Unsafe.free(src, srcLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(gorillaScratch, gorillaCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(staging, 512, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testTimestampColumnRawWithoutGorillaFlag() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            long[] values = {1_000_000L, 2_000_000L, 3_000_000L};
            long p = startSingleColumnFrame(staging, "t", QwpConstants.TYPE_TIMESTAMP, values.length);
            p = putByte(p, (byte) 0);
            for (long v : values) p = putLong(p, v);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            for (int i = 0; i < values.length; i++) {
                Assert.assertEquals("row " + i, values[i], batchOf(buffer).getLongValue(0, i));
            }
        }));
    }

    @Test
    public void testTimestampUnknownEncodingByteRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = writeHeader(p, QwpConstants.FLAG_GORILLA);
                p = putByte(p, (byte) 0x11);
                p = putLong(p, 1L);
                p = putVarint(p, 0L);
                p = putVarint(p, 0L);
                p = putVarint(p, 3L);
                p = putVarint(p, 1L);
                p = putVarint(p, 1L);
                p = putByte(p, (byte) 't');
                p = putByte(p, QwpConstants.TYPE_TIMESTAMP);
                p = putByte(p, (byte) 0);                       // null_flag
                p = putByte(p, (byte) 0x77);                    // bogus encoding byte

                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject unknown TIMESTAMP encoding byte");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions TIMESTAMP encoding: " + expected.getMessage(),
                            expected.getMessage().contains("TIMESTAMP encoding"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testTooManyArrayDimsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = startSingleColumnFrame(staging, "a", QwpConstants.TYPE_LONG_ARRAY, 1);
                p = putByte(p, (byte) 0);
                p = putByte(p, (byte) (ColumnType.ARRAY_NDIMS_LIMIT + 1));

                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject out-of-range ARRAY nDims");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions invalid dimensions: " + expected.getMessage(),
                            expected.getMessage().contains("invalid array dimensions"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testTruncatedFixedColumnRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                // Claim 3 LONG rows but supply only 2 longs of payload.
                long p = startSingleColumnFrame(staging, "v", QwpConstants.TYPE_LONG, 3);
                p = putByte(p, (byte) 0);
                p = putLong(p, 1L);
                p = putLong(p, 2L);
                // (no third long)
                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject truncated fixed-width column");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions truncation: " + expected.getMessage(),
                            expected.getMessage().contains("truncated"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testUnsupportedVersionRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(64);
            long staging = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = putInt(p, QwpConstants.MAGIC_MESSAGE);
                p = putByte(p, (byte) 99);                       // future version client doesn't speak
                for (int i = 5; i < 32; i++) p = putByte(p, (byte) 0);
                buffer.copyFromPayload(staging, 32);
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject unsupported version");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue(expected.getMessage().contains("unsupported version"));
                }
            } finally {
                Unsafe.free(staging, 64, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testUnsupportedWireTypeRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = startSingleColumnFrame(staging, "x", (byte) 0x7F, 1);
                p = putByte(p, (byte) 0);
                p = putLong(p, 0L);
                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject unknown wire type");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions unsupported wire type: " + expected.getMessage(),
                            expected.getMessage().contains("unsupported wire type"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testUuidColumnDecodes() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(256, (decoder, buffer, staging) -> {
            int rows = 2;
            long p = startSingleColumnFrame(staging, "u", QwpConstants.TYPE_UUID, rows);
            p = putByte(p, (byte) 0);
            // UUID 0: low=0x1122334455667788, high=0x99AABBCCDDEEFF00
            p = putLong(p, 0x1122334455667788L);
            p = putLong(p, 0x99AABBCCDDEEFF00L);
            // UUID 1: low=0, high=0
            p = putLong(p, 0L);
            p = putLong(p, 0L);
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);
            Assert.assertEquals(0x1122334455667788L, batchOf(buffer).getUuidLo(0, 0));
            Assert.assertEquals(0x99AABBCCDDEEFF00L, batchOf(buffer).getUuidHi(0, 0));
            Assert.assertEquals(0L, batchOf(buffer).getUuidLo(0, 1));
            Assert.assertEquals(0L, batchOf(buffer).getUuidHi(0, 1));
        }));
    }

    @Test
    public void testVarcharColumnRoundTrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> withDecoder(512, (decoder, buffer, staging) -> {
            String[] strings = {"alpha", "", "héllo", "🚀"};
            long p = startSingleColumnFrame(staging, "s", QwpConstants.TYPE_VARCHAR, strings.length);
            p = putByte(p, (byte) 0); // no nulls
            int totalBytes = 0;
            int[] offsets = new int[strings.length + 1];
            offsets[0] = 0;
            byte[][] payloads = new byte[strings.length][];
            for (int i = 0; i < strings.length; i++) {
                payloads[i] = strings[i].getBytes(StandardCharsets.UTF_8);
                totalBytes += payloads[i].length;
                offsets[i + 1] = totalBytes;
            }
            for (int o : offsets) p = putInt(p, o);
            for (byte[] payload : payloads) {
                for (byte b : payload) p = putByte(p, b);
            }
            buffer.copyFromPayload(staging, (int) (p - staging));
            decoder.decode(buffer);

            for (int i = 0; i < strings.length; i++) {
                Assert.assertFalse(batchOf(buffer).isNull(0, i));
                String got = batchOf(buffer).getStrA(0, i).toString();
                Assert.assertEquals("row " + i, strings[i], got);
            }
        }));
    }

    @Test
    public void testZeroArrayDimsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(256);
            long staging = Unsafe.malloc(256, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = startSingleColumnFrame(staging, "a", QwpConstants.TYPE_LONG_ARRAY, 1);
                p = putByte(p, (byte) 0);
                p = putByte(p, (byte) 0);                       // nDims = 0 (null on the wire is signalled via the bitmap, not 0-dim)

                buffer.copyFromPayload(staging, (int) (p - staging));
                try {
                    decoder.decode(buffer);
                    Assert.fail("decoder must reject 0-dimensional ARRAY value");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions invalid dimensions: " + expected.getMessage(),
                            expected.getMessage().contains("invalid array dimensions"));
                }
            } finally {
                Unsafe.free(staging, 256, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    /**
     * Reaches into {@link QwpBatchBuffer} via reflection to grab the
     * package-private {@code batch} view. Production callers receive the same
     * batch via the {@code QwpColumnBatchHandler.onBatch} callback dispatched
     * by {@code QwpEgressIoThread}; we don't have that path available in a
     * unit test, so we read the field directly.
     */
    private static QwpColumnBatch batchOf(QwpBatchBuffer buffer) {
        try {
            Field f = QwpBatchBuffer.class.getDeclaredField("batch");
            f.setAccessible(true);
            return (QwpColumnBatch) f.get(buffer);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not access QwpBatchBuffer.batch via reflection", e);
        }
    }

    /**
     * Wire-format helpers.
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

    private static long putShort(long p, short v) {
        Unsafe.getUnsafe().putShort(p, v);
        return p + 2;
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
     * Begins a single-column RESULT_BATCH frame and writes everything up to but
     * NOT including the column body (caller writes null_flag + values). The
     * returned position points at where the column body starts.
     */
    private static long startSingleColumnFrame(long buf, String colName, byte wireType, int rowCount) {
        long p = buf;
        p = writeHeader(p, (byte) 0);
        p = putByte(p, (byte) 0x11);
        p = putLong(p, 1L);
        p = putVarint(p, 0L);                      // batch_seq
        p = putVarint(p, 0L);                      // table_name_len
        p = putVarint(p, rowCount);
        p = putVarint(p, 1L);                      // column_count
        byte[] nameBytes = colName.getBytes(StandardCharsets.UTF_8);
        p = putVarint(p, nameBytes.length);
        for (byte b : nameBytes) p = putByte(p, b);
        p = putByte(p, wireType);
        return p;
    }

    private static void withDecoder(int bufferSize, BatchTest body) throws Exception {
        QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
        QwpBatchBuffer buffer = new QwpBatchBuffer(bufferSize);
        long staging = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        try {
            body.run(decoder, buffer, staging);
        } finally {
            Unsafe.free(staging, bufferSize, MemoryTag.NATIVE_DEFAULT);
            buffer.close();
            decoder.close();
        }
    }

    private static long writeHeader(long p, byte flags) {
        // Magic (4) + version (1) + flags (1) + msg_kind-in-header unused (1)
        // + table_count (1) + payload_length (4).
        // The decoder reads flags at HEADER_OFFSET_FLAGS == 5, so flags MUST sit
        // immediately after version. Putting them later (e.g. at offset 6) makes
        // the decoder see flags == 0 and silently take the non-delta / non-gorilla
        // / non-zstd path, leaving the delta/gorilla payload bytes at the position
        // the table-block parser then advances over -- producing inscrutable
        // table-block parse errors (bad varint, out-of-range column_count) mid-frame.
        p = putInt(p, QwpConstants.MAGIC_MESSAGE);
        p = putByte(p, QwpConstants.VERSION);
        p = putByte(p, flags);
        p = putByte(p, (byte) 0);
        p = putByte(p, (byte) 1);
        p = putInt(p, 0);
        return p;
    }

    @FunctionalInterface
    private interface BatchTest {
        void run(QwpResultBatchDecoder decoder, QwpBatchBuffer buffer, long staging) throws Exception;
    }
}
