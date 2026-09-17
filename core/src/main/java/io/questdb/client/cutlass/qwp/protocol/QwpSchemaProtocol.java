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

package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.cairo.TableUtils;
import io.questdb.client.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.client.std.Unsafe;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Codec and wire constants for negotiated QWP schema control messages. */
public final class QwpSchemaProtocol {
    public static final byte FLAG_CONTROL = 0x20;
    public static final byte KIND_DESCRIBE = 1;
    public static final byte KIND_SCHEMA = 2;
    public static final int MAX_COLUMN_COUNT = 2048;
    public static final int MAX_FRAME_SIZE = 1024 * 1024;
    public static final int MAX_NAME_UTF16_LENGTH = 127;
    public static final int MAX_NAME_UTF8_LENGTH = 381;
    public static final int MAX_PARAMS_LENGTH = 1024;
    public static final int RESULT_DENIED = 2;
    public static final int RESULT_KNOWN = 0;
    public static final int RESULT_MISSING = 1;
    public static final int RESULT_TOO_LARGE = 4;
    public static final int RESULT_UNAVAILABLE = 3;

    private static final int KNOWN_FIXED_PAYLOAD_SIZE = 1 + 8 + 1 + 4 + 8 + 2 + 2;
    private static final int MIN_PAYLOAD_SIZE = 1 + 8 + 1;

    private QwpSchemaProtocol() {
    }

    public static QwpSchemaResponse decodeResponse(long address, int length) {
        int payloadLength = validateResponseEnvelope(address, length, MIN_PAYLOAD_SIZE);
        return decodePayload(address + QwpConstants.HEADER_SIZE, payloadLength, false);
    }

    /** Validates a complete schema-response envelope and returns its positive request ID. */
    public static long peekResponseRequestId(long address, int length) {
        validateResponseEnvelope(address, length, 1 + Long.BYTES);
        long payloadAddress = address + QwpConstants.HEADER_SIZE;
        if (getUnsignedByte(payloadAddress) != KIND_SCHEMA) {
            throw malformed("unexpected control response kind");
        }
        long requestId = getLong(payloadAddress + 1);
        if (requestId <= 0) {
            throw malformed("request id must be positive");
        }
        return requestId;
    }

    private static int validateResponseEnvelope(long address, int length, int minimumPayloadSize) {
        if (address == 0) {
            throw malformed("null frame address");
        }
        if (length < QwpConstants.HEADER_SIZE + minimumPayloadSize || length > MAX_FRAME_SIZE) {
            throw malformed("invalid frame length: " + length);
        }
        if (getInt(address) != QwpConstants.MAGIC_MESSAGE) {
            throw malformed("invalid QWP magic");
        }
        if (getUnsignedByte(address + 4) != 1) {
            throw malformed("unsupported QWP version");
        }
        if (getUnsignedByte(address + 5) != (FLAG_CONTROL & 0xff)) {
            throw malformed("invalid control flags");
        }
        if (getUnsignedShort(address + 6) != 0) {
            throw malformed("control frame must have zero tables");
        }
        int payloadLength = getInt(address + 8);
        if (payloadLength != length - QwpConstants.HEADER_SIZE) {
            throw malformed("payload length mismatch");
        }
        return payloadLength;
    }

    /** Decodes a length-delimited schema payload carried by response feedback. */
    public static QwpSchemaResponse decodeFeedbackPayload(long address, int length) {
        if (address == 0) {
            throw malformed("null payload address");
        }
        if (length < MIN_PAYLOAD_SIZE || length > MAX_FRAME_SIZE) {
            throw malformed("invalid payload length: " + length);
        }
        return decodePayload(address, length, true);
    }

    private static QwpSchemaResponse decodePayload(long address, int length, boolean feedback) {
        long p = address;
        long limit = address + length;
        if (getUnsignedByte(p++) != KIND_SCHEMA) {
            throw malformed("unexpected control response kind");
        }
        long requestId = getLong(p);
        p += 8;
        if (feedback ? requestId != 0 : requestId <= 0) {
            throw malformed(feedback ? "feedback request id must be zero" : "request id must be positive");
        }
        int result = getUnsignedByte(p++);
        if (result < RESULT_KNOWN || result > RESULT_TOO_LARGE) {
            throw malformed("unknown schema result: " + result);
        }
        if (result != RESULT_KNOWN) {
            if (p != limit) {
                throw malformed("trailing bytes after schema result");
            }
            return new QwpSchemaResponse(requestId, result, -1, -1, -1, new QwpSchemaResponse.Column[0],
                    new LowerCaseCharSequenceIntHashMap());
        }
        if (length < KNOWN_FIXED_PAYLOAD_SIZE) {
            throw malformed("truncated known schema");
        }
        require(p, 16, limit, "schema identity");
        int tableId = getInt(p);
        p += 4;
        long metadataVersion = getLong(p);
        p += 8;
        int designatedIndex = getShort(p);
        p += 2;
        int columnCount = getUnsignedShort(p);
        p += 2;
        if (tableId < 0 || metadataVersion < 0) {
            throw malformed("negative schema identity");
        }
        if (columnCount > MAX_COLUMN_COUNT) {
            throw malformed("too many columns: " + columnCount);
        }
        if (designatedIndex < -1 || designatedIndex >= columnCount) {
            throw malformed("invalid designated index: " + designatedIndex);
        }

        QwpSchemaResponse.Column[] columns = new QwpSchemaResponse.Column[columnCount];
        LowerCaseCharSequenceIntHashMap columnNames = new LowerCaseCharSequenceIntHashMap(columnCount);
        for (int i = 0; i < columnCount; i++) {
            require(p, 2, limit, "column name length");
            int nameLength = getUnsignedShort(p);
            p += 2;
            if (nameLength == 0 || nameLength > MAX_NAME_UTF8_LENGTH) {
                throw malformed("invalid column name length: " + nameLength);
            }
            require(p, nameLength, limit, "column name");
            String name = decodeUtf8(p, nameLength, "column name");
            if (!TableUtils.isValidColumnName(name, MAX_NAME_UTF16_LENGTH)) {
                throw malformed("invalid column name");
            }
            if (!columnNames.put(name, i)) {
                throw malformed("duplicate case-insensitive column name");
            }
            p += nameLength;
            require(p, 6, limit, "column type and parameter length");
            int type = getInt(p);
            p += 4;
            int paramsLength = getUnsignedShort(p);
            p += 2;
            if (paramsLength > MAX_PARAMS_LENGTH) {
                throw malformed("column parameters too large: " + paramsLength);
            }
            require(p, paramsLength, limit, "column parameters");
            p += paramsLength;
            columns[i] = new QwpSchemaResponse.Column(name, type, paramsLength != 0);
        }
        if (p != limit) {
            throw malformed("trailing bytes after schema");
        }
        return new QwpSchemaResponse(requestId, result, tableId, metadataVersion, designatedIndex, columns, columnNames);
    }

    public static byte[] encodeDescribe(long requestId, CharSequence tableName) {
        if (requestId <= 0) {
            throw new IllegalArgumentException("request id must be positive");
        }
        if (tableName == null || !TableUtils.isValidTableName(tableName, MAX_NAME_UTF16_LENGTH)) {
            throw new IllegalArgumentException("invalid table name");
        }
        byte[] name = encodeName(tableName, "table name");
        int payloadLength = 1 + 8 + 2 + name.length;
        ByteBuffer buffer = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLength).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(QwpConstants.MAGIC_MESSAGE);
        buffer.put((byte) 1);
        buffer.put(FLAG_CONTROL);
        buffer.putShort((short) 0);
        buffer.putInt(payloadLength);
        buffer.put(KIND_DESCRIBE);
        buffer.putLong(requestId);
        buffer.putShort((short) name.length);
        buffer.put(name);
        return buffer.array();
    }

    private static byte[] copyBytes(long address, int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(address + i);
        }
        return bytes;
    }

    private static String decodeUtf8(long address, int length, String what) {
        byte[] bytes = copyBytes(address, length);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw malformed("invalid UTF-8 " + what);
        }
    }

    private static byte[] encodeName(CharSequence value, String what) {
        if (value == null || value.length() == 0 || value.length() > MAX_NAME_UTF16_LENGTH) {
            throw new IllegalArgumentException(what + " must contain 1 to " + MAX_NAME_UTF16_LENGTH + " UTF-16 units");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) {
                    throw new IllegalArgumentException(what + " contains an unpaired surrogate");
                }
            } else if (Character.isLowSurrogate(c)) {
                throw new IllegalArgumentException(what + " contains an unpaired surrogate");
            }
        }
        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_NAME_UTF8_LENGTH) {
            throw new IllegalArgumentException(what + " exceeds " + MAX_NAME_UTF8_LENGTH + " UTF-8 bytes");
        }
        return bytes;
    }

    private static int getInt(long address) {
        return Unsafe.getUnsafe().getInt(address);
    }

    private static long getLong(long address) {
        return Unsafe.getUnsafe().getLong(address);
    }

    private static short getShort(long address) {
        return Unsafe.getUnsafe().getShort(address);
    }

    private static int getUnsignedByte(long address) {
        return Unsafe.getUnsafe().getByte(address) & 0xff;
    }

    private static int getUnsignedShort(long address) {
        return getShort(address) & 0xffff;
    }

    private static IllegalArgumentException malformed(String message) {
        return new IllegalArgumentException("Malformed QWP schema response: " + message);
    }

    private static void require(long position, int needed, long limit, String what) {
        if (needed < 0 || position > limit || needed > limit - position) {
            throw malformed("truncated " + what);
        }
    }
}
