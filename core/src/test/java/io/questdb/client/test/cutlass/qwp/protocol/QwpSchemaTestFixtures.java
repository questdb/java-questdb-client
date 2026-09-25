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

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Shared mechanics for the {@code QwpSchemaBinding*Test} family: schema
 * response construction and encoded-table header checks. The fixtures write
 * the schema envelope by hand, independently of any production encoder, and
 * leave every conversion expectation to the individual tests.
 *
 * <p>All fixtures use table {@code "t"}, table id 1 and metadata version 1
 * unless a parameter says otherwise.</p>
 */
final class QwpSchemaTestFixtures {
    private static final byte[] NO_PARAMS = new byte[0];

    private QwpSchemaTestFixtures() {
    }

    /** Asserts a non-retryable schema rejection whose message contains every part. */
    static void assertReason(LineSenderSchemaException.Reason reason, Runnable action, String... messageParts) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, action::run);
        Assert.assertEquals(error.getMessage(), reason, error.getReason());
        Assert.assertFalse(error.getMessage(), error.isRetryable());
        for (String part : messageParts) {
            Assert.assertTrue(error.getMessage(), error.getMessage().contains(part));
        }
    }

    static QwpSchemaBinding binding(QwpTableBuffer buffer, byte[]... columns) {
        return binding(buffer, -1, columns);
    }

    static QwpSchemaBinding binding(QwpTableBuffer buffer, int designatedIndex, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(designatedIndex, columns));
    }

    static byte[] column(String name, int type) {
        return column(name, type, NO_PARAMS);
    }

    static byte[] column(String name, int type, byte[] params) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 4 + 2 + params.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type)
                .putShort((short) params.length).put(params).array();
    }

    /** Wraps a control payload in a message frame and decodes it. */
    static QwpSchemaResponse decode(byte[] payload) {
        byte[] frame = frame(payload);
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

    static byte[] frame(byte[] payload) {
        return ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put(QwpConstants.VERSION).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload).array();
    }

    static QwpSchemaResponse known(byte[]... columns) {
        return known(-1, columns);
    }

    static QwpSchemaResponse known(int designatedIndex, byte[]... columns) {
        int length = 1 + 8 + 1 + 4 + 8 + 2 + 2;
        for (byte[] column : columns) {
            length += column.length;
        }
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) designatedIndex).putShort((short) columns.length);
        for (byte[] column : columns) {
            payload.put(column);
        }
        return decode(payload.array());
    }

    static QwpSchemaResponse missing() {
        return schemaResult(QwpSchemaProtocol.RESULT_MISSING);
    }

    /** A column whose type carries a non-empty extension parameter block. */
    static byte[] parameterizedColumn(String name, int type) {
        return column(name, type, new byte[]{1});
    }

    /** Decodes a schema-less response carrying only {@code result}. */
    static QwpSchemaResponse schemaResult(int result) {
        return decode(ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) result).array());
    }

    /** Consumes the table prefix and the header of a single column named "value". */
    static void skipTableHeader(QwpTestWireReader reader, byte wireType, int rows) {
        skipTablePrefix(reader, rows, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(wireType, reader.u8());
    }

    static void skipTablePrefix(QwpTestWireReader reader, int rows, int columns) {
        skipTablePrefix(reader, rows, columns, 1, 1);
    }

    /** Asserts the message header and table prefix, leaving the reader at the first column. */
    static QwpTestWireReader tableHeader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        return tableHeader(encoder, size, rows, columns, 1, 1);
    }

    /** A negative {@code tableId} expects a schema-framed table block without a pinned identity. */
    static QwpTestWireReader tableHeader(
            QwpWebSocketEncoder encoder,
            int size,
            int rows,
            int columns,
            int tableId,
            long metadataVersion
    ) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, reader.i32());
        Assert.assertEquals(QwpConstants.VERSION, reader.u8());
        Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_SCHEMA, reader.u8());
        Assert.assertEquals(1, reader.u16());
        Assert.assertEquals(size - QwpConstants.HEADER_SIZE, reader.i32());
        skipTablePrefix(reader, rows, columns, tableId, metadataVersion);
        return reader;
    }

    private static void skipTablePrefix(
            QwpTestWireReader reader,
            int rows,
            int columns,
            int tableId,
            long metadataVersion
    ) {
        Assert.assertEquals("t", reader.string());
        if (tableId < 0) {
            Assert.assertEquals(0, reader.u8());
        } else {
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(tableId, reader.i32());
            Assert.assertEquals(metadataVersion, reader.i64());
        }
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
    }
}
