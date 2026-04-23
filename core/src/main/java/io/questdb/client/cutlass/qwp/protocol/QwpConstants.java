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

package io.questdb.client.cutlass.qwp.protocol;

/**
 * Constants for the QWP v1 binary protocol.
 * <p>
 * All little-endian encoding in this protocol is byte-level: multi-byte and
 * multi-word values are stored with the least significant byte at the lowest
 * address.
 */
public final class QwpConstants {
    /**
     * Client identifier sent in the X-QWP-Client-Id upgrade header.
     */
    public static final String CLIENT_ID = "java/1.0.2";
    /**
     * Flag bit: Delta symbol dictionary encoding enabled.
     * When set, symbol columns use global IDs and send only new dictionary entries.
     */
    public static final byte FLAG_DELTA_SYMBOL_DICT = 0x08;
    /**
     * Flag bit: Gorilla timestamp encoding enabled.
     */
    public static final byte FLAG_GORILLA = 0x04;
    /**
     * Flag bit: payload region after the prelude is zstd-compressed. Set only
     * when the handshake negotiated zstd compression. Mirror of the server-side
     * constant; see the server QwpConstants for the full description.
     */
    public static final byte FLAG_ZSTD = 0x10;
    /**
     * Offset of flags byte in header.
     */
    public static final int HEADER_OFFSET_FLAGS = 5;
    /**
     * Size of the message header in bytes.
     */
    public static final int HEADER_SIZE = 12;
    /**
     * Magic bytes for QWP v1 message: "QWP1" (ASCII).
     */
    public static final int MAGIC_MESSAGE = 0x31505751; // "QWP1" in little-endian
    /**
     * Maximum columns per table. Must match the server-side constant of
     * the same name so the client can produce an error before sending to
     * the server and finding out.
     */
    public static final int MAX_COLUMNS_PER_TABLE = 2048;
    /**
     * Maximum column name length in bytes. Mirrors the server's same-named
     * constant so the decoder can reject hostile or malformed wire bytes that
     * advertise an oversized name length.
     */
    public static final int MAX_COLUMN_NAME_LENGTH = 127;
    /**
     * Maximum table name length in bytes. Mirrors the server's same-named
     * constant; used by the decoder to reject malformed wire bytes.
     */
    public static final int MAX_TABLE_NAME_LENGTH = 127;
    /**
     * Schema mode: Full schema included.
     */
    public static final byte SCHEMA_MODE_FULL = 0x00;
    /**
     * Schema mode: Schema reference (ID lookup).
     */
    public static final byte SCHEMA_MODE_REFERENCE = 0x01;
    /**
     * Column type: BINARY (length-prefixed opaque bytes).
     * Wire format: identical to VARCHAR — (N+1) x uint32 offsets + concatenated bytes.
     */
    public static final byte TYPE_BINARY = 0x17;
    /**
     * Column type: BOOLEAN (1 bit per value, packed).
     */
    public static final byte TYPE_BOOLEAN = 0x01;
    /**
     * Column type: BYTE (int8).
     */
    public static final byte TYPE_BYTE = 0x02;
    /**
     * Column type: CHAR (2-byte UTF-16 code unit).
     */
    public static final byte TYPE_CHAR = 0x16;
    /**
     * Column type: DATE (int64 milliseconds since epoch).
     */
    public static final byte TYPE_DATE = 0x0B;
    /**
     * Column type: DECIMAL128 (16 bytes, 38 digits precision).
     * Wire format: [scale (1B in schema)] + [little-endian unscaled value (16B)]
     */
    public static final byte TYPE_DECIMAL128 = 0x14;
    /**
     * Column type: DECIMAL256 (32 bytes, 77 digits precision).
     * Wire format: [scale (1B in schema)] + [little-endian unscaled value (32B)]
     */
    public static final byte TYPE_DECIMAL256 = 0x15;
    /**
     * Column type: DECIMAL64 (8 bytes, 18 digits precision).
     * Wire format: [scale (1B in schema)] + [little-endian unscaled value (8B)]
     */
    public static final byte TYPE_DECIMAL64 = 0x13;
    /**
     * Column type: DOUBLE (IEEE 754 float64).
     */
    public static final byte TYPE_DOUBLE = 0x07;
    /**
     * Column type: DOUBLE_ARRAY (N-dimensional array of IEEE 754 float64).
     * Wire format: [nDims (1B)] [dim1_len (4B)]...[dimN_len (4B)] [flattened values (LE)]
     */
    public static final byte TYPE_DOUBLE_ARRAY = 0x11;
    /**
     * Column type: FLOAT (IEEE 754 float32).
     */
    public static final byte TYPE_FLOAT = 0x06;
    /**
     * Column type: GEOHASH (varint bits + packed geohash).
     */
    public static final byte TYPE_GEOHASH = 0x0E;
    /**
     * Column type: INT (int32, little-endian).
     */
    public static final byte TYPE_INT = 0x04;
    /**
     * Column type: IPv4 (32-bit address). Wire format: 4 bytes LE, identical to INT.
     * NULL is signalled via the standard null bitmap.
     */
    public static final byte TYPE_IPv4 = 0x18;
    /**
     * Column type: LONG (int64, little-endian).
     */
    public static final byte TYPE_LONG = 0x05;
    /**
     * Column type: LONG256 (32 bytes, little-endian).
     */
    public static final byte TYPE_LONG256 = 0x0D;
    /**
     * Column type: LONG_ARRAY (N-dimensional array of int64).
     * Wire format: [nDims (1B)] [dim1_len (4B)]...[dimN_len (4B)] [flattened values (LE)]
     */
    public static final byte TYPE_LONG_ARRAY = 0x12;
    /**
     * Column type: SHORT (int16, little-endian).
     */
    public static final byte TYPE_SHORT = 0x03;
    /**
     * Column type: SYMBOL (dictionary-encoded string).
     */
    public static final byte TYPE_SYMBOL = 0x09;
    /**
     * Column type: TIMESTAMP (int64 microseconds since epoch).
     * Use this for timestamps beyond nanosecond range (year > 2262).
     */
    public static final byte TYPE_TIMESTAMP = 0x0A;
    /**
     * Column type: TIMESTAMP_NANOS (int64 nanoseconds since epoch).
     * Use this for full nanosecond precision (limited to years 1677-2262).
     */
    public static final byte TYPE_TIMESTAMP_NANOS = 0x10;
    /**
     * Column type: UUID (16 bytes, little-endian).
     */
    public static final byte TYPE_UUID = 0x0C;
    /**
     * Column type: VARCHAR (length-prefixed UTF-8, aux storage).
     */
    public static final byte TYPE_VARCHAR = 0x0F;
    /**
     * Current protocol version.
     */
    public static final byte VERSION_1 = 1;
    /**
     * Protocol v2 adds an unsolicited {@code SERVER_INFO} control frame
     * delivered as the first WebSocket frame after the 101 upgrade. Servers that
     * advertise v2 send the frame automatically; v2 clients must consume it
     * before submitting the first query and may use the role value it carries
     * to route reads to primary vs replica.
     */
    public static final byte VERSION_2 = 2;
    /**
     * Maximum protocol version the ingest path advertises / accepts. Pinned to
     * v1 because the v2 bump only adds an egress control frame; bumping the
     * ingest wire to v2 would silently round-trip a version byte that no v1
     * server can accept.
     */
    public static final byte MAX_SUPPORTED_INGEST_VERSION = VERSION_1;
    /**
     * Maximum protocol version supported by this client on the egress path.
     * Ingest pins to {@link #MAX_SUPPORTED_INGEST_VERSION}.
     */
    public static final byte MAX_SUPPORTED_VERSION = VERSION_2;

    private QwpConstants() {
        // utility class
    }

    /**
     * Returns the per-value size in bytes as encoded on the wire. BOOLEAN returns 0
     * because it is bit-packed (1 bit per value). GEOHASH returns -1 because it uses
     * variable-width encoding (varint precision + ceil(precision/8) bytes per value).
     * <p>
     * This is distinct from the in-memory buffer stride used by the client's
     * {@code QwpTableBuffer.elementSizeInBuffer()}.
     *
     * @param typeCode the column type code (without nullable flag)
     * @return size in bytes, 0 for bit-packed (BOOLEAN), or -1 for variable-width types
     */
    public static int getFixedTypeSize(byte typeCode) {
        switch ((int) typeCode) {
            case TYPE_BOOLEAN:
                return 0; // Special: bit-packed
            case TYPE_BYTE:
                return 1;
            case TYPE_SHORT:
            case TYPE_CHAR:
                return 2;
            case TYPE_INT:
            case TYPE_FLOAT:
                return 4;
            case TYPE_LONG:
            case TYPE_DOUBLE:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
            case TYPE_DATE:
            case TYPE_DECIMAL64:
                return 8;
            case TYPE_UUID:
            case TYPE_DECIMAL128:
                return 16;
            case TYPE_LONG256:
            case TYPE_DECIMAL256:
                return 32;
            case TYPE_GEOHASH:
                return -1; // Variable width: varint precision + packed values
            default:
                return -1; // Variable width
        }
    }

    /**
     * Returns a human-readable name for the type code.
     *
     * @param typeCode the column type code
     * @return type name
     */
    public static String getTypeName(byte typeCode) {
        String name;
        switch ((int) typeCode) {
            case TYPE_BOOLEAN:
                name = "BOOLEAN";
                break;
            case TYPE_BYTE:
                name = "BYTE";
                break;
            case TYPE_SHORT:
                name = "SHORT";
                break;
            case TYPE_CHAR:
                name = "CHAR";
                break;
            case TYPE_INT:
                name = "INT";
                break;
            case TYPE_LONG:
                name = "LONG";
                break;
            case TYPE_FLOAT:
                name = "FLOAT";
                break;
            case TYPE_DOUBLE:
                name = "DOUBLE";
                break;
            case TYPE_SYMBOL:
                name = "SYMBOL";
                break;
            case TYPE_TIMESTAMP:
                name = "TIMESTAMP";
                break;
            case TYPE_TIMESTAMP_NANOS:
                name = "TIMESTAMP_NANOS";
                break;
            case TYPE_DATE:
                name = "DATE";
                break;
            case TYPE_UUID:
                name = "UUID";
                break;
            case TYPE_LONG256:
                name = "LONG256";
                break;
            case TYPE_GEOHASH:
                name = "GEOHASH";
                break;
            case TYPE_VARCHAR:
                name = "VARCHAR";
                break;
            case TYPE_DOUBLE_ARRAY:
                name = "DOUBLE_ARRAY";
                break;
            case TYPE_LONG_ARRAY:
                name = "LONG_ARRAY";
                break;
            case TYPE_DECIMAL64:
                name = "DECIMAL64";
                break;
            case TYPE_DECIMAL128:
                name = "DECIMAL128";
                break;
            case TYPE_DECIMAL256:
                name = "DECIMAL256";
                break;
            default:
                name = "UNKNOWN(" + (int) typeCode + ")";
                break;
        }
        return name;
    }

    /**
     * Returns true if the type code represents a fixed-width type.
     *
     * @param typeCode the column type code (without nullable flag)
     * @return true if fixed-width
     */
    public static boolean isFixedWidthType(byte typeCode) {
        return (int) typeCode == TYPE_BOOLEAN ||
                (int) typeCode == TYPE_BYTE ||
                (int) typeCode == TYPE_SHORT ||
                (int) typeCode == TYPE_CHAR ||
                (int) typeCode == TYPE_INT ||
                (int) typeCode == TYPE_LONG ||
                (int) typeCode == TYPE_FLOAT ||
                (int) typeCode == TYPE_DOUBLE ||
                (int) typeCode == TYPE_TIMESTAMP ||
                (int) typeCode == TYPE_TIMESTAMP_NANOS ||
                (int) typeCode == TYPE_DATE ||
                (int) typeCode == TYPE_UUID ||
                (int) typeCode == TYPE_LONG256 ||
                (int) typeCode == TYPE_DECIMAL64 ||
                (int) typeCode == TYPE_DECIMAL128 ||
                (int) typeCode == TYPE_DECIMAL256;
    }
}
