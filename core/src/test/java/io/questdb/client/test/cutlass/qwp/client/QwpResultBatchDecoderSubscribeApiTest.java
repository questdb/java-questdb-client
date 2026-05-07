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

import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.client.QwpResultBatchDecoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the SUBSCRIBE_BATCH-facing entry points on {@link
 * QwpResultBatchDecoder}: {@link QwpResultBatchDecoder#registerSchemaFull(long, long)}
 * for seeding the connection-scoped schema registry from a SUBSCRIBE_ACK
 * body, and {@link QwpResultBatchDecoder#decodeAfterPrelude(QwpBatchBuffer,
 * long, long, byte, long, long, long, int)} for decoding a body whose
 * non-RESULT_BATCH prelude has already been stripped. These are the only
 * decoder hooks the Enterprise subscribe client uses; the OSS query path
 * is unchanged.
 */
public class QwpResultBatchDecoderSubscribeApiTest {

    private static final int STAGING = 1024;

    @Test
    public void testDecodeAfterPreludeWithRegisteredSchemaProducesRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(STAGING);
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                // Seed the schema (one INT column "n") via the public API.
                long sLen = writeSchemaFull(staging, 7, "n", QwpConstants.TYPE_INT);
                decoder.registerSchemaFull(staging, staging + sLen);

                // Body of a hypothetical SUBSCRIBE_BATCH (delta_section +
                // table_block) referencing schema id 7 with two INT rows.
                long bLen = writeSubscribeBatchBody(staging, /*schemaId*/ 7,
                        /*rowCount*/ 2, /*columnCount*/ 1,
                        /*nullFlag*/ (byte) 0, /*intValues*/ new int[]{42, 99});

                byte flags = (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT | QwpConstants.FLAG_GORILLA);
                decoder.decodeAfterPrelude(
                        buffer, staging, staging + bLen,
                        flags, /*correlationId=sub_id*/ 555L, /*batchSeq*/ 3L,
                        staging, (int) bLen
                );
                QwpColumnBatch batch = buffer.getBatch();
                Assert.assertEquals(555L, batch.requestId());
                Assert.assertEquals(3L, batch.batchSeq());
                Assert.assertEquals(2, batch.getRowCount());
                Assert.assertEquals(1, batch.getColumnCount());
                Assert.assertEquals("n", batch.getColumnName(0));
                Assert.assertEquals(QwpConstants.TYPE_INT, batch.getColumnWireType(0));
                Assert.assertEquals(42, batch.getIntValue(0, 0));
                Assert.assertEquals(99, batch.getIntValue(0, 1));
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testRegisterSchemaFullEmptyPayloadRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            try {
                decoder.registerSchemaFull(0L, 0L);
                Assert.fail("must reject empty schema payload");
            } catch (QwpDecodeException expected) {
                Assert.assertTrue(expected.getMessage().contains("empty"));
            } finally {
                decoder.close();
            }
        });
    }

    @Test
    public void testRegisterSchemaFullExoticColumnTypeRoundTrips() throws Exception {
        // Ensure registerSchemaFull doesn't hard-code the wire-type byte: a
        // subsequent decodeAfterPrelude with a TYPE_DOUBLE column resolves
        // the right per-column wire type from the registry.
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(STAGING);
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                long sLen = writeSchemaFull(staging, 4, "v", QwpConstants.TYPE_DOUBLE);
                decoder.registerSchemaFull(staging, staging + sLen);
                long bLen = writeSubscribeBatchBodyDouble(staging, 4, 1, 1, (byte) 0,
                        new double[]{3.1415});
                decoder.decodeAfterPrelude(
                        buffer, staging, staging + bLen,
                        (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT | QwpConstants.FLAG_GORILLA),
                        1L, 0L, staging, (int) bLen);
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE, buffer.getBatch().getColumnWireType(0));
                Assert.assertEquals(3.1415, buffer.getBatch().getDoubleValue(0, 0), 1e-12);
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    @Test
    public void testRegisterSchemaFullNegativeSchemaIdRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = putByte(p, QwpConstants.SCHEMA_MODE_FULL);
                // 10-byte negative-encoding varint (sign bit set)
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0xFF);
                p = putByte(p, (byte) 0x01); // last byte's bit 0 set -> overflow guard fires
                try {
                    decoder.registerSchemaFull(staging, p);
                    Assert.fail("must reject negative / overflow schema_id");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue(expected.getMessage().toLowerCase().contains("overflow")
                            || expected.getMessage().toLowerCase().contains("schema_id out of range"));
                }
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                decoder.close();
            }
        });
    }

    @Test
    public void testRegisterSchemaFullOversizedColumnNameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                long p = staging;
                p = putByte(p, QwpConstants.SCHEMA_MODE_FULL);
                p = putVarint(p, 0L);
                // Column name length way past MAX_COLUMN_NAME_LENGTH.
                p = putVarint(p, (long) (QwpConstants.MAX_COLUMN_NAME_LENGTH + 1));
                try {
                    decoder.registerSchemaFull(staging, p);
                    Assert.fail("must reject oversized column name length");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue(expected.getMessage().contains("column name length"));
                }
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                decoder.close();
            }
        });
    }

    @Test
    public void testRegisterSchemaFullTrailingBytesRejected() throws Exception {
        // Pin the contract: registerSchemaFull's payload must end exactly at
        // the last column's wire-type byte. Trailing bytes get interpreted
        // as the start of another column (varint name length + name +
        // wire-type byte); since they don't form a complete additional
        // column the parser throws "truncated column def" or
        // "truncated varint" depending on the exact garbage. The test just
        // pins "any QwpDecodeException with a truncated/trailing message".
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                long sLen = writeSchemaFull(staging, 1, "n", QwpConstants.TYPE_INT);
                // 0x00 = "varint of value zero -> nameLen=0", then the
                // parser expects a wire-type byte that isn't there.
                Unsafe.getUnsafe().putByte(staging + sLen, (byte) 0x00);
                try {
                    decoder.registerSchemaFull(staging, staging + sLen + 1);
                    Assert.fail("must reject trailing bytes after last column");
                } catch (QwpDecodeException expected) {
                    String msg = expected.getMessage();
                    Assert.assertTrue("unexpected message: " + msg,
                            msg.contains("truncated") || msg.contains("trailing"));
                }
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                decoder.close();
            }
        });
    }

    @Test
    public void testRegisterSchemaFullWrongModeRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(staging, (byte) 0x01); // SCHEMA_MODE_REFERENCE
                try {
                    decoder.registerSchemaFull(staging, staging + 1);
                    Assert.fail("must reject non-FULL schema mode");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue(expected.getMessage().contains("SCHEMA_MODE_FULL"));
                }
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                decoder.close();
            }
        });
    }

    @Test
    public void testSchemaSurvivesAcrossManyDecodeAfterPreludeCalls() throws Exception {
        // Connection-scoped schema registry: register once via ACK, decode
        // many BATCHes referencing the same schema id without re-seeding.
        TestUtils.assertMemoryLeak(() -> {
            QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
            QwpBatchBuffer buffer = new QwpBatchBuffer(STAGING);
            long staging = Unsafe.malloc(STAGING, MemoryTag.NATIVE_DEFAULT);
            try {
                long sLen = writeSchemaFull(staging, 9, "n", QwpConstants.TYPE_INT);
                decoder.registerSchemaFull(staging, staging + sLen);

                byte flags = (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT | QwpConstants.FLAG_GORILLA);
                for (int batchSeq = 0; batchSeq < 5; batchSeq++) {
                    long bLen = writeSubscribeBatchBody(staging, 9, 1, 1, (byte) 0,
                            new int[]{batchSeq * 10});
                    decoder.decodeAfterPrelude(
                            buffer, staging, staging + bLen,
                            flags, /*sub_id*/ 1L, batchSeq, staging, (int) bLen
                    );
                    Assert.assertEquals(1, buffer.getBatch().getRowCount());
                    Assert.assertEquals(batchSeq * 10, buffer.getBatch().getIntValue(0, 0));
                    Assert.assertEquals(batchSeq, buffer.getBatch().batchSeq());
                }
            } finally {
                Unsafe.free(staging, STAGING, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
                decoder.close();
            }
        });
    }

    private static long putByte(long p, byte v) {
        Unsafe.getUnsafe().putByte(p, v);
        return p + 1;
    }

    private static long putInt(long p, int v) {
        Unsafe.getUnsafe().putInt(p, v);
        return p + 4;
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
     * Crafts a minimal SUBSCRIBE_BATCH-shaped body: deltaStart=0,
     * deltaCount=0, table_block (anonymous name + row_count + col_count +
     * SCHEMA_MODE_REFERENCE + schema_id + per-column INT data).
     */
    private static long writeSubscribeBatchBody(long buf, int schemaId, int rowCount,
                                                int columnCount, byte nullFlag, int[] intValues) {
        long p = buf;
        // delta_section: deltaStart=0, deltaCount=0
        p = putVarint(p, 0L);
        p = putVarint(p, 0L);
        // table_block: name_len(0) + row_count + col_count + ref-mode + schema_id
        p = putByte(p, (byte) 0); // name_len = 0 (anonymous)
        p = putVarint(p, rowCount);
        p = putVarint(p, columnCount);
        p = putByte(p, (byte) 0x01); // SCHEMA_MODE_REFERENCE
        p = putVarint(p, schemaId);
        // INT column: null_flag + nonNullCount placeholder + values.
        // Without nulls the wire stores nonNull values densely. Layout for
        // TYPE_INT in this client: null_flag(1) + values(4 each).
        p = putByte(p, nullFlag);
        for (int v : intValues) {
            p = putInt(p, v);
        }
        return p - buf;
    }

    private static long writeSubscribeBatchBodyDouble(long buf, int schemaId, int rowCount,
                                                      int columnCount, byte nullFlag, double[] doubleValues) {
        long p = buf;
        p = putVarint(p, 0L);
        p = putVarint(p, 0L);
        p = putByte(p, (byte) 0);
        p = putVarint(p, rowCount);
        p = putVarint(p, columnCount);
        p = putByte(p, (byte) 0x01);
        p = putVarint(p, schemaId);
        p = putByte(p, nullFlag);
        for (double v : doubleValues) {
            Unsafe.getUnsafe().putDouble(p, v);
            p += 8;
        }
        return p - buf;
    }

    /**
     * Writes a single-column SCHEMA_MODE_FULL block as a SUBSCRIBE_ACK
     * carries it (no column count - reader walks until limit).
     */
    private static long writeSchemaFull(long buf, int schemaId, String columnName, byte wireType) {
        long p = buf;
        p = putByte(p, QwpConstants.SCHEMA_MODE_FULL);
        p = putVarint(p, schemaId);
        byte[] nameBytes = columnName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        p = putVarint(p, nameBytes.length);
        for (byte b : nameBytes) {
            p = putByte(p, b);
        }
        p = putByte(p, wireType);
        return p - buf;
    }
}
