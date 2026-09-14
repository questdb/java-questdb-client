/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class QwpSchemaProtocolTest {
    @Test
    public void testAcceptsMaximumResponseColumnNameBoundaries() {
        String name = repeat('\u0800', QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH);
        QwpSchemaResponse response = decode(knownResponse(1, 1, 1, 0, column(name, 5, new byte[0])));
        Assert.assertEquals(name, response.getColumnName(0));
        Assert.assertEquals(QwpSchemaProtocol.MAX_NAME_UTF8_LENGTH,
                name.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void testDecodeKnownSchemaAndSkipsUnknownParams() {
        byte[] frame = knownResponse(7, 42, 19, 1,
                column("id", 32, new byte[]{1, 2, 3}),
                column("métric🚀", 0x71234567, new byte[0]));
        QwpSchemaResponse response = decode(frame);
        Assert.assertEquals(7, response.getRequestId());
        Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
        Assert.assertTrue(response.hasSchema());
        Assert.assertEquals(42, response.getTableId());
        Assert.assertEquals(19, response.getMetadataVersion());
        Assert.assertEquals(1, response.getDesignatedIndex());
        Assert.assertEquals(2, response.getColumnCount());
        Assert.assertTrue(response.hasColumnExtensionParameters(0));
        Assert.assertFalse(response.hasColumnExtensionParameters(1));
        Assert.assertEquals("métric🚀", response.getColumnName(1));
        Assert.assertEquals(0x71234567, response.getColumnType(1));
    }

    @Test
    public void testRejectsInvalidAndDuplicateColumnNames() {
        assertMalformed(knownResponse(1, 1, 0, -1, column("x", 5, new byte[0]), column("x", 5, new byte[0])));
        assertMalformed(knownResponse(1, 1, 0, -1, column("x", 5, new byte[0]), column("X", 5, new byte[0])));
        assertMalformed(knownResponse(1, 1, 0, -1, column("Ä", 5, new byte[0]), column("ä", 5, new byte[0])));
        assertMalformed(knownResponse(1, 1, 0, -1, column("a/b", 5, new byte[0])));
        assertMalformed(knownResponse(1, 1, 0, -1, column("a\nb", 5, new byte[0])));
    }

    @Test
    public void testAcceptsValidColumnNameCompatibilityBoundaries() {
        String max = repeat('\u0800', QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH);
        QwpSchemaResponse response = decode(knownResponse(1, 1, 0, -1,
                column("metric name", 5, new byte[0]),
                column("метрика", 5, new byte[0]),
                column("!^[$", 5, new byte[0]),
                column(max, 5, new byte[0])));
        Assert.assertEquals(4, response.getColumnCount());
        Assert.assertEquals(max, response.getColumnName(3));
    }

    @Test
    public void testDecodeEverySchemaLessResult() {
        for (int result = QwpSchemaProtocol.RESULT_MISSING; result <= QwpSchemaProtocol.RESULT_TOO_LARGE; result++) {
            QwpSchemaResponse response = decode(resultResponse(11, result));
            Assert.assertEquals(result, response.getResult());
            Assert.assertFalse(response.hasSchema());
            Assert.assertEquals(0, response.getColumnCount());
            Assert.assertEquals(-1, response.getTableId());
        }
    }

    @Test
    public void testEncodeDescribeExactAsciiWire() {
        byte[] encoded = QwpSchemaProtocol.encodeDescribe(0x0102030405060708L, "trades");
        ByteBuffer b = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, b.getInt());
        Assert.assertEquals(1, b.get() & 0xff);
        Assert.assertEquals(QwpSchemaProtocol.FLAG_CONTROL, b.get());
        Assert.assertEquals(0, b.getShort());
        Assert.assertEquals(encoded.length - QwpConstants.HEADER_SIZE, b.getInt());
        Assert.assertEquals(QwpSchemaProtocol.KIND_DESCRIBE, b.get());
        Assert.assertEquals(0x0102030405060708L, b.getLong());
        Assert.assertEquals(6, b.getShort());
        byte[] name = new byte[6];
        b.get(name);
        Assert.assertArrayEquals("trades".getBytes(StandardCharsets.UTF_8), name);
        Assert.assertFalse(b.hasRemaining());
    }

    @Test
    public void testEncodeDescribeUnicodeBoundariesAndRejectsInvalidNames() {
        String maxUtf16AndUtf8 = repeat('\u0800', 127);
        byte[] encoded = QwpSchemaProtocol.encodeDescribe(1, maxUtf16AndUtf8);
        Assert.assertEquals(381, Short.toUnsignedInt(ByteBuffer.wrap(encoded, 21, 2).order(ByteOrder.LITTLE_ENDIAN).getShort()));

        assertEncodeRejected(0, "table");
        assertEncodeRejected(1, "");
        assertEncodeRejected(1, repeat('a', 128));
        assertEncodeRejected(1, repeat('\u0800', 126) + "🚀"); // 128 UTF-16 units
        assertEncodeRejected(1, "bad\uD800name");
        assertEncodeRejected(1, "bad\uDC00name");
        assertEncodeRejected(1, "bad/name");
    }

    @Test
    public void testRejectsMalformedEnvelopesAndTrailingData() {
        byte[] valid = knownResponse(1, 1, 0, -1, column("x", 5, new byte[0]));
        assertMalformed(mutate(valid, 0, (byte) 'X'));
        assertMalformed(mutate(valid, 4, (byte) 2));
        assertMalformed(mutate(valid, 5, (byte) (QwpSchemaProtocol.FLAG_CONTROL | 1)));
        assertMalformed(mutate(valid, 6, (byte) 1));
        assertMalformed(mutate(valid, 12, QwpSchemaProtocol.KIND_DESCRIBE));
        assertMalformed(Arrays.copyOf(valid, valid.length - 1));

        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        ByteBuffer.wrap(trailing).order(ByteOrder.LITTLE_ENDIAN).putInt(8, trailing.length - 12);
        assertMalformed(trailing);
        byte[] oversized = new byte[QwpSchemaProtocol.MAX_FRAME_SIZE + 1];
        assertMalformed(oversized);
    }

    @Test
    public void testPeekResponseRequestIdValidatesOnlyFixedEnvelopeAndIdentity() {
        byte[] valid = resultResponse(7, QwpSchemaProtocol.RESULT_MISSING);
        byte[] noResult = responseIdentity(7);
        Assert.assertEquals(7, peek(noResult));
        Assert.assertEquals(7, peek(valid));
        assertMalformed(Arrays.copyOf(noResult, 20), true);
        assertMalformed(mutate(valid, 0, (byte) 'X'), true);
        assertMalformed(mutate(valid, 4, (byte) 2), true);
        assertMalformed(mutate(valid, 5, (byte) (QwpSchemaProtocol.FLAG_CONTROL | 1)), true);
        assertMalformed(mutate(valid, 6, (byte) 1), true);
        assertMalformed(mutate(valid, 12, QwpSchemaProtocol.KIND_DESCRIBE), true);
        assertMalformed(resultResponse(0, QwpSchemaProtocol.RESULT_MISSING), true);

        byte[] badLength = valid.clone();
        ByteBuffer.wrap(badLength).order(ByteOrder.LITTLE_ENDIAN).putInt(8, 9);
        assertMalformed(badLength, true);

        // Identification deliberately stops before the result/body, while full decoding stays strict.
        assertMalformed(noResult);
    }

    @Test
    public void testRejectsEveryTruncatedKnownResponsePrefix() {
        byte[] valid = knownResponse(9, 7, 6, 1,
                column("first", 5, new byte[]{1, 2, 3}),
                column("секунд", 0x71234567, new byte[0]));
        Assert.assertEquals(2, decode(valid).getColumnCount());
        for (int length = 0; length < valid.length; length++) {
            byte[] prefix = Arrays.copyOf(valid, length);
            if (length >= QwpConstants.HEADER_SIZE) {
                ByteBuffer.wrap(prefix).order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(8, length - QwpConstants.HEADER_SIZE);
            }
            assertMalformed(prefix);
        }
    }

    @Test
    public void testRejectsInvalidResultsIdentitiesCountsAndIndexes() {
        assertMalformed(resultResponse(0, QwpSchemaProtocol.RESULT_MISSING));
        assertMalformed(resultResponse(1, 5));
        assertMalformed(knownResponse(1, -1, 0, -1));
        assertMalformed(knownResponse(1, 1, -1, -1));
        assertMalformed(knownResponse(1, 1, 0, 0));
        assertMalformed(knownResponse(1, 1, 0, -2));

        byte[] count = knownResponse(1, 1, 0, -1);
        ByteBuffer.wrap(count).order(ByteOrder.LITTLE_ENDIAN).putShort(36, (short) 2049);
        assertMalformed(count);
    }

    @Test
    public void testRejectsInvalidUtf8AndFieldBoundsBeforeAllocation() {
        byte[] invalidUtf8 = knownResponse(1, 1, 0, -1, columnBytes(new byte[]{(byte) 0xc3, 0x28}, 5, new byte[0]));
        assertMalformed(invalidUtf8);
        assertMalformed(knownResponse(1, 1, 0, -1, column("", 5, new byte[0])));
        assertMalformed(knownResponse(1, 1, 0, -1, column(repeat('a', 128), 5, new byte[0])));
        assertMalformed(knownResponse(1, 1, 0, -1, column("x", 5, new byte[1025])));

        byte[] truncatedName = knownResponse(1, 1, 0, -1, column("abc", 5, new byte[0]));
        ByteBuffer.wrap(truncatedName).order(ByteOrder.LITTLE_ENDIAN).putShort(38, (short) 381);
        assertMalformed(truncatedName);
        byte[] truncatedParams = knownResponse(1, 1, 0, -1, column("x", 5, new byte[0]));
        ByteBuffer.wrap(truncatedParams).order(ByteOrder.LITTLE_ENDIAN).putShort(45, (short) 1024);
        assertMalformed(truncatedParams);
    }

    private static void assertEncodeRejected(long requestId, String name) {
        try {
            QwpSchemaProtocol.encodeDescribe(requestId, name);
            Assert.fail("expected invalid describe request");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static void assertMalformed(byte[] frame) {
        try {
            decode(frame);
            Assert.fail("expected malformed frame");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().startsWith("Malformed QWP schema response:"));
        }
    }

    private static void assertMalformed(byte[] frame, boolean peek) {
        try {
            if (peek) {
                peek(frame);
            } else {
                decode(frame);
            }
            Assert.fail("expected malformed frame");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().startsWith("Malformed QWP schema response:"));
        }
    }

    private static byte[] column(String name, int type, byte[] params) {
        return columnBytes(name.getBytes(StandardCharsets.UTF_8), type, params);
    }

    private static byte[] columnBytes(byte[] name, int type, byte[] params) {
        ByteBuffer b = ByteBuffer.allocate(2 + name.length + 4 + 2 + params.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putShort((short) name.length).put(name).putInt(type).putShort((short) params.length).put(params);
        return b.array();
    }

    private static QwpSchemaResponse decode(byte[] frame) {
        long address = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, frame[i]);
            }
            return QwpSchemaProtocol.decodeResponse(address, frame.length);
        } finally {
            Unsafe.free(address, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long peek(byte[] frame) {
        long address = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, frame[i]);
            }
            return QwpSchemaProtocol.peekResponseRequestId(address, frame.length);
        } finally {
            Unsafe.free(address, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] frame(byte[] payload) {
        ByteBuffer b = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload);
        return b.array();
    }

    private static byte[] knownResponse(long requestId, int tableId, long version, int designated, byte[]... columns) {
        int length = 1 + 8 + 1 + 4 + 8 + 2 + 2;
        for (byte[] column : columns) length += column.length;
        ByteBuffer b = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
        b.put(QwpSchemaProtocol.KIND_SCHEMA).putLong(requestId).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(tableId).putLong(version).putShort((short) designated).putShort((short) columns.length);
        for (byte[] column : columns) b.put(column);
        return frame(b.array());
    }

    private static byte[] mutate(byte[] source, int offset, byte value) {
        byte[] copy = source.clone();
        copy[offset] = value;
        return copy;
    }

    private static String repeat(char c, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    private static byte[] resultResponse(long requestId, int result) {
        ByteBuffer b = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN);
        b.put(QwpSchemaProtocol.KIND_SCHEMA).putLong(requestId).put((byte) result);
        return frame(b.array());
    }

    private static byte[] responseIdentity(long requestId) {
        ByteBuffer b = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        b.put(QwpSchemaProtocol.KIND_SCHEMA).putLong(requestId);
        return frame(b.array());
    }
}
