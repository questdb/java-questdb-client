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

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cairo.MicrosTimestampDriver;
import io.questdb.client.cairo.NanosTimestampDriver;
import io.questdb.client.cairo.TableUtils;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Decimals;
import io.questdb.client.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.std.fastdouble.FastDoubleParser;
import io.questdb.client.std.fastdouble.FastFloatParser;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8StringSink;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.LineSenderSchemaException.Reason.ACCESS_DENIED;
import static io.questdb.client.LineSenderSchemaException.Reason.INVALID_VALUE;
import static io.questdb.client.LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE;
import static io.questdb.client.LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE;

/**
 * Converts supported input values against one immutable server schema result
 * and appends them to a caller-owned {@link QwpTableBuffer}. A confirmed
 * missing table, or a column absent from a known schema, is inferred from the
 * first effective supported input value.
 *
 * <p>This binding does not own the buffer or its row lifecycle and is not
 * thread-safe. The caller completes rows, encodes data and closes the buffer.
 * After a setter failure, the caller must invoke both
 * {@link QwpTableBuffer#cancelCurrentRow()} and
 * {@link QwpTableBuffer#rollbackUncommittedColumns()} before continuing or
 * completing another row. {@link QwpTableBuffer#reset()} preserves the binding;
 * {@link QwpTableBuffer#clear()} invalidates it.</p>
 *
 * <p>The caller must encode the buffer with the matching symbol-dictionary
 * family: local dictionaries for ownerless buffers, or the owning sender's
 * corresponding global dictionary for sender-backed buffers.</p>
 */
public final class QwpSchemaBinding {
    private static final int FIRST_DASH = 8;
    private static final int SECOND_DASH = 13;
    private static final int THIRD_DASH = 18;
    private static final int FOURTH_DASH = 23;
    private static final int UUID_LENGTH = 36;

    private final QwpTableBuffer buffer;
    private final LowerCaseCharSequenceIntHashMap columns = new LowerCaseCharSequenceIntHashMap();
    private final QwpSchemaResponse schema;
    private final String tableName;
    private Decimal256 decimalTextScratch;
    private int[] floatingTextExponentScratch;
    private StringSink floatingTextSink;
    private StringSink numericTextSink;
    private Utf8StringSink timestampTextSink;
    private StringSink uuidTextSink;

    /**
     * Attaches {@code schema} to an empty, unbound caller-owned buffer. The
     * caller must correlate a
     * positive request id with its DESCRIBE table, or obtain request-id-zero
     * schema from the matching named write-feedback entry.
     */
    public QwpSchemaBinding(QwpTableBuffer buffer, QwpSchemaResponse schema) {
        if (buffer == null) {
            throw new NullPointerException("buffer");
        }
        String tableName = buffer.getTableName();
        if (tableName == null || !TableUtils.isValidTableName(tableName, QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH)) {
            throw new IllegalArgumentException("invalid table name");
        }
        if (schema == null) {
            throw new NullPointerException("schema");
        }
        this.tableName = tableName;
        this.schema = schema;
        requireUsableSchema();
        for (int i = 0, n = schema.getColumnCount(); i < n; i++) {
            columns.put(schema.getColumnName(i), i);
        }
        this.buffer = buffer;
        buffer.attachSchemaBinding(this);
    }

    /**
     * Appends an owned copy of a BINARY value to an exact BINARY target.
     * The caller retains the class-level row-cancellation and uncommitted-column rollback
     * responsibility on failure.
     */
    public QwpSchemaBinding binaryColumn(CharSequence name, byte[] value) {
        buffer.requireSchemaBinding(this);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "BINARY", ColumnType.BINARY);
        if (column == null) {
            return this;
        }
        if (value == null) {
            throw error(INVALID_VALUE, name, "BINARY", (int) ColumnType.BINARY, "binary value cannot be null");
        }
        column.addBinary(value);
        return this;
    }

    public QwpSchemaBinding decimalColumn(CharSequence name, Decimal64 value) {
        return decimalColumn(name, value, "DECIMAL64", QwpConstants.TYPE_DECIMAL64);
    }

    public QwpSchemaBinding decimalColumn(CharSequence name, Decimal128 value) {
        return decimalColumn(name, value, "DECIMAL128", QwpConstants.TYPE_DECIMAL128);
    }

    public QwpSchemaBinding decimalColumn(CharSequence name, Decimal256 value) {
        return decimalColumn(name, value, "DECIMAL256", QwpConstants.TYPE_DECIMAL256);
    }

    /**
     * Parses the public textual-decimal overload after target selection and
     * duplicate suppression, then applies the same DECIMAL256 conversion rules
     * as the typed overload. The caller owns {@code scratch}; this binding does
     * not retain it.
     */
    public QwpSchemaBinding decimalColumn(CharSequence name, CharSequence value, Decimal256 scratch) {
        buffer.requireSchemaBinding(this);
        if (value == null || value.length() == 0) {
            return this;
        }
        int index = targetIndex(name, "DECIMAL256");
        int targetType = targetType(index, ColumnType.DECIMAL256);
        if (index < 0) {
            QwpTableBuffer.ColumnBuffer inferred = buffer.getExistingInferredColumn(name);
            if (inferred != null && inferred.getSize() > buffer.getRowCount()) {
                return this;
            }
            if (inferred != null && inferred.getType() != QwpConstants.TYPE_DECIMAL256) {
                throw unsupported(name, "DECIMAL256", -1, "inferred column type conflict [inferredType="
                        + QwpConstants.getTypeName(inferred.getType()) + ']');
            }
            parseDecimalText(name, value, scratch, targetType);
            inferred = buffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL256, true);
            if (inferred != null) {
                // Keep the legacy public-overload contract: infer DECIMAL256 at
                // the parsed natural scale. A parsed special is an effective
                // null but does not pin the scale of a later finite value.
                inferred.addDecimal256(scratch);
            }
            return this;
        }
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "DECIMAL256", index, targetType);
        if (column == null) {
            return this;
        }
        if (!ColumnType.isDecimal(targetType)
                && targetType != ColumnType.STRING && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "DECIMAL256", targetType, "conversion is not implemented");
        }
        parseDecimalText(name, value, scratch, targetType);
        if (scratch.isNull()) {
            if (ColumnType.isDecimal(targetType)) {
                column.addSchemaDecimalNull(ColumnType.getDecimalScale(targetType));
            } else {
                column.addNull();
            }
            return this;
        }
        return appendDecimal(column, name, scratch, "DECIMAL256", targetType);
    }

    /** Appends an owned copy of the bytes addressed by {@code slice}. */
    public QwpSchemaBinding binaryColumn(CharSequence name, DirectByteSlice slice) {
        buffer.requireSchemaBinding(this);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "BINARY", ColumnType.BINARY);
        if (column == null) {
            return this;
        }
        if (slice == null) {
            throw error(INVALID_VALUE, name, "BINARY", (int) ColumnType.BINARY, "binary slice cannot be null");
        }
        appendBinary(name, column, slice.ptr(), slice.size());
        return this;
    }

    /**
     * Appends an owned copy of {@code [ptr, ptr + len)}. The source memory need only remain
     * valid for the duration of this call.
     */
    public QwpSchemaBinding binaryColumn(CharSequence name, long ptr, long len) {
        buffer.requireSchemaBinding(this);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "BINARY", ColumnType.BINARY);
        if (column == null) {
            return this;
        }
        appendBinary(name, column, ptr, len);
        return this;
    }

    public QwpSchemaBinding boolColumn(CharSequence name, boolean value) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "BOOLEAN");
        int targetType = targetType(index, ColumnType.BOOLEAN);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "BOOLEAN", index, targetType);
        if (column == null) {
            return this;
        }
        switch (targetType) {
            case ColumnType.BOOLEAN:
                column.addBoolean(value);
                break;
            case ColumnType.BYTE:
                column.addByte((byte) (value ? 1 : 0));
                break;
            case ColumnType.SHORT:
                column.addShort((short) (value ? 1 : 0));
                break;
            case ColumnType.INT:
                column.addInt(value ? 1 : 0);
                break;
            case ColumnType.LONG:
                column.addLong(value ? 1 : 0);
                break;
            case ColumnType.FLOAT:
                column.addFloat(value ? 1 : 0);
                break;
            case ColumnType.DOUBLE:
                column.addDouble(value ? 1 : 0);
                break;
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
                column.addString(value ? "true" : "false");
                break;
            default:
                throw unsupported(name, "BOOLEAN", targetType, "conversion is not implemented");
        }
        return this;
    }

    public QwpSchemaBinding byteColumn(CharSequence name, byte value) {
        return integerNumericColumn(name, value, "BYTE", ColumnType.BYTE, false);
    }

    public QwpSchemaBinding doubleColumn(CharSequence name, double value) {
        return floatingNumericColumn(name, value, "DOUBLE");
    }

    /** Appends and pins the row's designated timestamp using the schema target precision. */
    public QwpSchemaBinding designatedTimestamp(long value, ChronoUnit unit) {
        buffer.requireSchemaBinding(this);
        int targetType = schema.getResult() == QwpSchemaProtocol.RESULT_MISSING
                ? (unit == ChronoUnit.NANOS ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO)
                : schema.getColumnType(requireDesignatedTimestamp("TIMESTAMP"));
        if (unit == null) {
            throw error(INVALID_VALUE, null, "TIMESTAMP", targetType, "timestamp unit is null");
        }
        if (!isSupportedTimestampUnit(unit)) {
            throw unsupported(null, "TIMESTAMP", targetType, "timestamp unit is not supported: " + unit);
        }
        final long converted;
        try {
            converted = targetType == ColumnType.TIMESTAMP_MICRO
                    ? MicrosTimestampDriver.INSTANCE.from(value, unit)
                    : NanosTimestampDriver.INSTANCE.from(value, unit);
        } catch (ArithmeticException e) {
            throw error(INVALID_VALUE, null, "TIMESTAMP", targetType, "value is outside target timestamp range");
        }
        designatedColumn(targetType, "TIMESTAMP").addLong(converted);
        return this;
    }

    /** Appends and pins an Instant as the row's designated timestamp. */
    public QwpSchemaBinding designatedTimestamp(Instant value) {
        buffer.requireSchemaBinding(this);
        int targetType = schema.getResult() == QwpSchemaProtocol.RESULT_MISSING
                ? ColumnType.TIMESTAMP_MICRO
                : schema.getColumnType(requireDesignatedTimestamp("TIMESTAMP"));
        if (value == null) {
            throw error(INVALID_VALUE, null, "TIMESTAMP", targetType, "timestamp value is null");
        }
        final long converted;
        try {
            converted = targetType == ColumnType.TIMESTAMP_MICRO
                    ? instantTimestamp(value, 1_000_000L, value.getNano() / 1_000L)
                    : instantTimestamp(value, 1_000_000_000L, value.getNano());
        } catch (ArithmeticException e) {
            throw error(INVALID_VALUE, null, "TIMESTAMP", targetType, "value is outside target timestamp range");
        }
        designatedColumn(targetType, "TIMESTAMP").addLong(converted);
        return this;
    }

    private QwpTableBuffer.ColumnBuffer designatedColumn(int targetType, String inputType) {
        byte wireType = timestampWireType(targetType);
        if (schema.getResult() == QwpSchemaProtocol.RESULT_MISSING) {
            QwpTableBuffer.ColumnBuffer inferred = buffer.getExistingInferredColumn("");
            if (inferred != null && inferred.getType() != wireType) {
                throw unsupported(null, inputType, -1, "inferred designated timestamp type conflict [inferredType="
                        + QwpConstants.getTypeName(inferred.getType()) + ']');
            }
        }
        return buffer.getOrCreateDesignatedTimestampColumn(wireType);
    }

    public QwpSchemaBinding floatColumn(CharSequence name, float value) {
        return floatingNumericColumn(name, value, "FLOAT");
    }

    public QwpSchemaBinding geoHashColumn(CharSequence name, long bits, int precisionBits) {
        buffer.requireSchemaBinding(this);
        if (precisionBits < 1 || precisionBits > 60) {
            throw error(INVALID_VALUE, name, "GEOHASH", null,
                    "invalid GEOHASH precision: " + precisionBits + " (must be 1-60)");
        }
        int sourceType = ColumnType.getGeoHashTypeWithBits(precisionBits);
        int index = targetIndex(name, "GEOHASH");
        int targetType = targetType(index, sourceType);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "GEOHASH", index, targetType);
        if (column == null) {
            return this;
        }
        long value = bits & ((1L << precisionBits) - 1L);
        if (ColumnType.isGeoHash(targetType)) {
            int targetBits = ColumnType.getGeoHashBits(targetType);
            if (targetBits != precisionBits) {
                throw unsupported(name, "GEOHASH", targetType,
                        "GEOHASH precision mismatch [sourceBits=" + precisionBits + ", targetBits=" + targetBits + ']');
            }
            column.addGeoHash(value, precisionBits);
        } else if (targetType == ColumnType.STRING || targetType == ColumnType.VARCHAR) {
            column.addString(formatGeoHash(value, precisionBits));
        } else {
            throw unsupported(name, "GEOHASH", targetType, "conversion is not implemented");
        }
        return this;
    }

    public QwpSchemaBinding geoHashColumn(CharSequence name, CharSequence value) {
        buffer.requireSchemaBinding(this);
        if (value == null) {
            throw error(INVALID_VALUE, name, "GEOHASH", null,
                    "GEOHASH string cannot be null; omit the setter to write NULL");
        }
        int length = value.length();
        if (length == 0) {
            throw error(INVALID_VALUE, name, "GEOHASH", null, "GEOHASH string cannot be empty");
        }
        if (length > 12) {
            throw error(INVALID_VALUE, name, "GEOHASH", null,
                    "GEOHASH string exceeds 12 characters: " + length);
        }
        final long bits;
        try {
            bits = Numbers.parseGeoHashBase32(value, 0, length);
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, "GEOHASH", null, "invalid GEOHASH string");
        }
        return geoHashColumn(name, bits, length * 5);
    }

    public QwpSchemaBinding intColumn(CharSequence name, int value) {
        return integerNumericColumn(name, value, "INT", ColumnType.INT, value == Integer.MIN_VALUE);
    }

    public QwpSchemaBinding ipv4Column(CharSequence name, int address) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "IPv4");
        int targetType = targetType(index, ColumnType.IPv4);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "IPv4", index, targetType);
        if (column == null) {
            return this;
        }
        if (targetType != ColumnType.IPv4
                && targetType != ColumnType.STRING
                && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "IPv4", targetType, "conversion is not implemented");
        }
        if (address == Numbers.IPv4_NULL) {
            column.addNull();
        } else if (targetType == ColumnType.IPv4) {
            column.addIPv4(address);
        } else {
            column.addString(formatIPv4(address));
        }
        return this;
    }

    public QwpSchemaBinding ipv4Column(CharSequence name, CharSequence address) {
        buffer.requireSchemaBinding(this);
        if (address == null) {
            return this;
        }
        int index = targetIndex(name, "IPv4");
        int targetType = targetType(index, ColumnType.IPv4);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "IPv4", index, targetType);
        if (column == null) {
            return this;
        }
        if (targetType != ColumnType.IPv4
                && targetType != ColumnType.STRING
                && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "IPv4", targetType, "conversion is not implemented");
        }
        if (Chars.equalsIgnoreCase("null", address) || Chars.equals("0.0.0.0", address)) {
            throw error(INVALID_VALUE, name, "IPv4", targetType,
                    "NULL sentinel inputs are rejected; pass a null reference or omit the setter");
        }
        final int packed;
        try {
            packed = Numbers.parseIPv4(address);
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, "IPv4", targetType, "invalid IPv4 address");
        }
        if (packed == Numbers.IPv4_NULL) {
            column.addNull();
        } else if (targetType == ColumnType.IPv4) {
            column.addIPv4(packed);
        } else {
            column.addString(formatIPv4(packed));
        }
        return this;
    }

    public QwpSchemaBinding longColumn(CharSequence name, long value) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "LONG");
        int targetType = targetType(index, ColumnType.LONG);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "LONG", index, targetType);
        if (column == null) {
            return this;
        }
        if (!isNumericTarget(targetType)
                && !isTextTarget(targetType)
                && !ColumnType.isDecimal(targetType)
                && targetType != ColumnType.DATE
                && targetType != ColumnType.TIMESTAMP_MICRO
                && targetType != ColumnType.TIMESTAMP_NANO) {
            throw unsupported(name, "LONG", targetType, "conversion is not implemented");
        }
        if (ColumnType.isDecimal(targetType)) {
            int targetPrecision = ColumnType.getDecimalPrecision(targetType);
            int targetScale = ColumnType.getDecimalScale(targetType);
            if (value == Long.MIN_VALUE) {
                column.addSchemaDecimalNull(targetScale);
                return this;
            }
            try {
                if (!column.addSchemaLongDecimal(value, targetPrecision, targetScale)) {
                    throw error(INVALID_VALUE, name, "LONG", targetType, "decimal value exceeds target precision");
                }
            } catch (NumericException e) {
                throw error(INVALID_VALUE, name, "LONG", targetType, "decimal value cannot be rescaled exactly");
            }
            return this;
        }
        if (value == Long.MIN_VALUE) {
            column.addNull();
            return this;
        }
        switch (targetType) {
            case ColumnType.BYTE:
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw invalidRange(name, "LONG", targetType);
                }
                column.addByte((byte) value);
                break;
            case ColumnType.SHORT:
                if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                    throw invalidRange(name, "LONG", targetType);
                }
                column.addShort((short) value);
                break;
            case ColumnType.INT:
                if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                    throw invalidRange(name, "LONG", targetType);
                }
                column.addInt((int) value);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP_MICRO:
            case ColumnType.TIMESTAMP_NANO:
                column.addLong(value);
                break;
            case ColumnType.FLOAT:
                column.addFloat((float) value);
                break;
            case ColumnType.DOUBLE:
                column.addDouble((double) value);
                break;
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
                column.addString(formatLong(value));
                break;
            case ColumnType.SYMBOL:
                column.addSymbol(formatLong(value));
                break;
            default:
                throw unsupported(name, "LONG", targetType, "conversion is not implemented");
        }
        return this;
    }

    public QwpSchemaBinding long256Column(CharSequence name, long l0, long l1, long l2, long l3) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "LONG256");
        int targetType = targetType(index, ColumnType.LONG256);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "LONG256", index, targetType);
        if (column == null) {
            return this;
        }
        if (targetType != ColumnType.LONG256
                && targetType != ColumnType.STRING
                && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "LONG256", targetType, "conversion is not implemented");
        }
        if (l0 == Long.MIN_VALUE && l1 == Long.MIN_VALUE
                && l2 == Long.MIN_VALUE && l3 == Long.MIN_VALUE) {
            column.addNull();
        } else if (targetType == ColumnType.LONG256) {
            column.addLong256(l0, l1, l2, l3);
        } else {
            column.addString(formatLong256(l0, l1, l2, l3));
        }
        return this;
    }

    public QwpSchemaBinding shortColumn(CharSequence name, short value) {
        return integerNumericColumn(name, value, "SHORT", ColumnType.SHORT, false);
    }

    public QwpSchemaBinding stringColumn(CharSequence name, CharSequence value) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "STRING");
        int targetType = targetType(index, ColumnType.VARCHAR);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "STRING", index, targetType);
        if (column == null) {
            return this;
        }
        if (ColumnType.isGeoHash(targetType)) {
            int precision = ColumnType.getGeoHashBits(targetType);
            column.initGeoHashPrecision(precision);
            if (value == null || value.length() == 0) {
                column.addNull();
            } else {
                appendStringGeoHash(column, name, value, precision, targetType);
            }
            return this;
        }
        if (ColumnType.isDecimal(targetType)) {
            int targetPrecision = ColumnType.getDecimalPrecision(targetType);
            int targetScale = ColumnType.getDecimalScale(targetType);
            if (value == null) {
                column.addSchemaDecimalNull(targetScale);
                return this;
            }
            try {
                if (!column.addSchemaStringDecimal(value, targetPrecision, targetScale)) {
                    throw error(INVALID_VALUE, name, "STRING", targetType, "decimal value exceeds target precision");
                }
            } catch (NumericException e) {
                throw error(INVALID_VALUE, name, "STRING", targetType, "invalid decimal text");
            }
            return this;
        }
        if (value == null) {
            column.addNull();
        } else {
            switch (targetType) {
                case ColumnType.BYTE:
                case ColumnType.SHORT:
                case ColumnType.INT:
                case ColumnType.LONG:
                case ColumnType.FLOAT:
                case ColumnType.DOUBLE:
                    appendStringNumeric(column, name, value, targetType);
                    break;
                case ColumnType.BOOLEAN:
                    if (value.length() == 1 && value.charAt(0) == '1') {
                        column.addBoolean(true);
                    } else if (value.length() == 1 && value.charAt(0) == '0') {
                        column.addBoolean(false);
                    } else if (Chars.equalsLowerCaseAscii(value, "true")) {
                        column.addBoolean(true);
                    } else if (Chars.equalsLowerCaseAscii(value, "false")) {
                        column.addBoolean(false);
                    } else {
                        throw error(INVALID_VALUE, name, "STRING", targetType, "invalid BOOLEAN text");
                    }
                    break;
                case ColumnType.STRING:
                case ColumnType.VARCHAR:
                    column.addString(value);
                    break;
                case ColumnType.SYMBOL:
                    column.addSymbol(value);
                    break;
                case ColumnType.BINARY:
                    column.addString(value);
                    break;
                case ColumnType.CHAR:
                    column.addShort((short) stringToChar(value));
                    break;
                case ColumnType.TIMESTAMP_MICRO:
                case ColumnType.TIMESTAMP_NANO:
                    appendStringTimestamp(column, name, value, targetType);
                    break;
                case ColumnType.UUID:
                    long lo;
                    long hi;
                    try {
                        checkUuid(value);
                        lo = parseUuidLo(value);
                        hi = parseUuidHi(value);
                    } catch (NumericException e) {
                        throw error(INVALID_VALUE, name, "STRING", targetType, "invalid UUID text");
                    }
                    column.addUuid(hi, lo);
                    break;
                case ColumnType.LONG256:
                    appendStringLong256(column, name, value);
                    break;
                default:
                    throw unsupported(name, "STRING", targetType, "conversion is not implemented");
            }
        }
        return this;
    }

    public QwpSchemaBinding charColumn(CharSequence name, char value) {
        buffer.requireSchemaBinding(this);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "CHAR", ColumnType.CHAR);
        if (column != null) {
            column.addShort((short) value);
        }
        return this;
    }

    public QwpSchemaBinding symbol(CharSequence name, CharSequence value) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "SYMBOL");
        int targetType = targetType(index, ColumnType.SYMBOL);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "SYMBOL", index, targetType);
        if (column == null) {
            return this;
        }
        switch (targetType) {
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
                column.addString(value);
                break;
            case ColumnType.SYMBOL:
                column.addSymbol(value);
                break;
            default:
                throw unsupported(name, "SYMBOL", targetType, "conversion is not implemented");
        }
        return this;
    }

    /**
     * Appends a non-designated timestamp supplied in an explicitly supported fixed-duration unit
     * to an exact TIMESTAMP, TIMESTAMP_NS, STRING or VARCHAR schema target. Text targets normalize
     * through checked microseconds and use the server's UTC millisecond presentation. Conversion
     * fails on overflow; nanos converted to micros truncate toward zero. The caller retains the
     * class-level row-cancellation and uncommitted-column rollback responsibility on failure.
     */
    public QwpSchemaBinding timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "TIMESTAMP");
        int targetType = targetType(index,
                unit == ChronoUnit.NANOS ? ColumnType.TIMESTAMP_NANO : ColumnType.TIMESTAMP_MICRO);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "TIMESTAMP", index, targetType);
        if (column == null) {
            return this;
        }
        if (targetType != ColumnType.TIMESTAMP_MICRO
                && targetType != ColumnType.TIMESTAMP_NANO
                && targetType != ColumnType.STRING
                && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "TIMESTAMP", targetType, "conversion is not implemented");
        }
        if (unit == null) {
            throw error(INVALID_VALUE, name, "TIMESTAMP", targetType, "timestamp unit is null");
        }
        if (!isSupportedTimestampUnit(unit)) {
            throw unsupported(name, "TIMESTAMP", targetType, "timestamp unit is not supported: " + unit);
        }
        final long converted;
        try {
            converted = targetType == ColumnType.TIMESTAMP_NANO
                    ? NanosTimestampDriver.INSTANCE.from(value, unit)
                    : MicrosTimestampDriver.INSTANCE.from(value, unit);
        } catch (ArithmeticException e) {
            throw error(INVALID_VALUE, name, "TIMESTAMP", targetType, "value is outside target timestamp range");
        }
        if (targetType == ColumnType.STRING || targetType == ColumnType.VARCHAR) {
            appendTimestampText(column, converted);
        } else {
            column.addLong(converted);
        }
        return this;
    }

    /**
     * Appends a non-designated Instant to an exact TIMESTAMP, TIMESTAMP_NS, STRING or VARCHAR
     * schema target. Timestamp targets preserve their precision; text targets normalize through
     * checked microseconds and use the server's UTC millisecond presentation. Final values outside
     * the selected signed-long range are rejected. The caller retains the class-level rollback
     * responsibility on failure.
     */
    public QwpSchemaBinding timestampColumn(CharSequence name, Instant value) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "TIMESTAMP");
        int targetType = targetType(index, ColumnType.TIMESTAMP_MICRO);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "TIMESTAMP", index, targetType);
        if (column == null) {
            return this;
        }
        if (targetType != ColumnType.TIMESTAMP_MICRO
                && targetType != ColumnType.TIMESTAMP_NANO
                && targetType != ColumnType.STRING
                && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "TIMESTAMP", targetType, "conversion is not implemented");
        }
        if (value == null) {
            throw error(INVALID_VALUE, name, "TIMESTAMP", targetType, "timestamp value is null");
        }
        final long converted;
        try {
            converted = targetType == ColumnType.TIMESTAMP_NANO
                    ? instantTimestamp(value, 1_000_000_000L, value.getNano())
                    : instantTimestamp(value, 1_000_000L, value.getNano() / 1_000L);
        } catch (ArithmeticException e) {
            throw error(INVALID_VALUE, name, "TIMESTAMP", targetType, "value is outside target timestamp range");
        }
        if (targetType == ColumnType.STRING || targetType == ColumnType.VARCHAR) {
            appendTimestampText(column, converted);
        } else {
            column.addLong(converted);
        }
        return this;
    }

    public QwpSchemaBinding uuidColumn(CharSequence name, long lo, long hi) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, "UUID");
        int targetType = targetType(index, ColumnType.UUID);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, "UUID", index, targetType);
        if (column == null) {
            return this;
        }
        if (targetType == ColumnType.UUID) {
            column.addUuid(hi, lo);
            return this;
        }
        if (targetType != ColumnType.STRING && targetType != ColumnType.VARCHAR) {
            throw unsupported(name, "UUID", targetType, "conversion is not implemented");
        }
        if (lo == Long.MIN_VALUE && hi == Long.MIN_VALUE) {
            column.addNull();
        } else {
            column.addString(formatUuid(lo, hi));
        }
        return this;
    }

    /** Rejects an effective input family whose schema-mode conversion is not implemented. */
    public QwpSchemaBinding unsupportedColumn(CharSequence name, String inputType) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, inputType);
        QwpTableBuffer.ColumnBuffer inferred = index < 0 ? buffer.getExistingInferredColumn(name) : null;
        if (inferred != null && inferred.getSize() > buffer.getRowCount()) {
            return this;
        }
        if (index < 0) {
            throw unsupported(name, inputType, -1, "input conversion is not implemented");
        }
        int targetType = schema.getColumnType(index);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, inputType, index, targetType);
        if (column != null) {
            throw unsupported(name, inputType, targetType, "input conversion is not implemented");
        }
        return this;
    }

    private static void checkUuid(CharSequence value) throws NumericException {
        if (value.length() != UUID_LENGTH
                || value.charAt(FIRST_DASH) != '-'
                || value.charAt(SECOND_DASH) != '-'
                || value.charAt(THIRD_DASH) != '-'
                || value.charAt(FOURTH_DASH) != '-') {
            throw NumericException.instance();
        }
    }

    private static long parseUuidHi(CharSequence value) throws NumericException {
        return (Numbers.parseHexLong(value, 0, FIRST_DASH) << 32)
                | (Numbers.parseHexLong(value, FIRST_DASH + 1, SECOND_DASH) << 16)
                | Numbers.parseHexLong(value, SECOND_DASH + 1, THIRD_DASH);
    }

    private static long parseUuidLo(CharSequence value) throws NumericException {
        return (Numbers.parseHexLong(value, THIRD_DASH + 1, FOURTH_DASH) << 48)
                | Numbers.parseHexLong(value, FOURTH_DASH + 1, UUID_LENGTH);
    }

    public int getTableId() {
        return schema.getTableId();
    }

    public long getMetadataVersion() {
        return schema.getMetadataVersion();
    }

    public boolean hasSameRelevantTarget(QwpSchemaResponse other, CharSequence name) {
        int current = name == null ? schema.getDesignatedIndex() : columns.get(name);
        int next = -1;
        if (name == null) {
            next = other.getDesignatedIndex();
        } else {
            for (int i = 0, n = other.getColumnCount(); i < n; i++) {
                if (Chars.equalsIgnoreCase(other.getColumnName(i), name)) {
                    next = i;
                    break;
                }
            }
        }
        if (current < 0 || next < 0) {
            return current < 0 && next < 0;
        }
        return (current == schema.getDesignatedIndex()) == (next == other.getDesignatedIndex())
                && schema.getColumnType(current) == other.getColumnType(next)
                && schema.hasColumnExtensionParameters(current) == other.hasColumnExtensionParameters(next);
    }

    private LineSenderSchemaException error(
            LineSenderSchemaException.Reason reason,
            CharSequence column,
            CharSequence inputType,
            Integer targetType,
            String detail
    ) {
        StringSink message = new StringSink();
        message.put("schema row error [reason=").put(reason.name()).put(", table=").putAsPrintable(tableName);
        if (column != null) {
            message.put(", column=").putAsPrintable(column);
        }
        if (inputType != null) {
            message.put(", inputType=").put(inputType);
        }
        if (targetType != null) {
            message.put(", targetType=").put(ColumnType.nameOf(targetType)).put('(').put(targetType).put(')');
        }
        message.put(", detail=").put(detail).put(']');
        return new LineSenderSchemaException(reason, message);
    }

    private void requireUsableSchema() {
        switch (schema.getResult()) {
            case QwpSchemaProtocol.RESULT_KNOWN:
            case QwpSchemaProtocol.RESULT_MISSING:
                return;
            case QwpSchemaProtocol.RESULT_DENIED:
                throw error(ACCESS_DENIED, null, null, null, "schema access denied");
            case QwpSchemaProtocol.RESULT_UNAVAILABLE:
                throw error(SCHEMA_UNAVAILABLE, null, null, null, "schema is temporarily unavailable");
            case QwpSchemaProtocol.RESULT_TOO_LARGE:
                throw error(UNSUPPORTED_FEATURE, null, null, null, "schema could not be returned within the discovery response limit");
            default:
                throw error(UNSUPPORTED_FEATURE, null, null, null, "unknown schema result");
        }
    }

    private QwpTableBuffer.ColumnBuffer targetColumn(CharSequence name, String inputType, int expectedTargetType) {
        int index = targetIndex(name, inputType);
        int targetType = targetType(index, expectedTargetType);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, inputType, index, targetType);
        if (column != null && targetType != expectedTargetType) {
            throw unsupported(name, inputType, targetType, "conversion is not implemented");
        }
        return column;
    }

    private QwpTableBuffer.ColumnBuffer targetColumn(
            CharSequence name,
            String inputType,
            int index,
            int targetType
    ) {
        if (index < 0) {
            QwpTableBuffer.ColumnBuffer inferred = buffer.getExistingInferredColumn(name);
            if (inferred != null && inferred.getSize() > buffer.getRowCount()) {
                return null;
            }
            if (inferred != null && inferred.getType() != wireType(targetType)) {
                throw unsupported(name, inputType, -1, "inferred column type conflict [inferredType="
                        + QwpConstants.getTypeName(inferred.getType()) + ']');
            }
        }
        if (index >= 0 && index == schema.getDesignatedIndex()) {
            throw unsupported(name, inputType, targetType, "designated timestamp writes are not implemented");
        }
        final byte wireType = wireType(targetType);
        if (wireType == 0) {
            throw unsupported(name, inputType, targetType, "conversion is not implemented");
        }
        QwpTableBuffer.ColumnBuffer column = buffer.getOrCreateColumn(name, wireType, true);
        if (column == null) {
            return null;
        }
        if (index >= 0 && schema.hasColumnExtensionParameters(index)) {
            throw unsupported(name, inputType, targetType, "parameterized target type");
        }
        return column;
    }

    private byte wireType(int targetType) {
        if (ColumnType.isGeoHash(targetType)) {
            int bits = ColumnType.getGeoHashBits(targetType);
            return bits >= 1 && bits <= 60 && targetType == ColumnType.getGeoHashTypeWithBits(bits)
                    ? QwpConstants.TYPE_GEOHASH
                    : 0;
        }
        if (ColumnType.isDecimal(targetType)) {
            int precision = ColumnType.getDecimalPrecision(targetType);
            int scale = ColumnType.getDecimalScale(targetType);
            if (precision < 1 || precision > Decimals.MAX_PRECISION || scale > precision
                    || targetType != ColumnType.getDecimalType(precision, scale)) {
                return 0;
            }
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.DECIMAL8:
                case ColumnType.DECIMAL16:
                case ColumnType.DECIMAL32:
                case ColumnType.DECIMAL64:
                    return QwpConstants.TYPE_DECIMAL64;
                case ColumnType.DECIMAL128:
                    return QwpConstants.TYPE_DECIMAL128;
                case ColumnType.DECIMAL256:
                    return QwpConstants.TYPE_DECIMAL256;
                default:
                    return 0;
            }
        }
        switch (targetType) {
            case ColumnType.BOOLEAN:
                return QwpConstants.TYPE_BOOLEAN;
            case ColumnType.BYTE:
                return QwpConstants.TYPE_BYTE;
            case ColumnType.SHORT:
                return QwpConstants.TYPE_SHORT;
            case ColumnType.CHAR:
                return QwpConstants.TYPE_CHAR;
            case ColumnType.INT:
                return QwpConstants.TYPE_INT;
            case ColumnType.LONG:
                return QwpConstants.TYPE_LONG;
            case ColumnType.DATE:
                return QwpConstants.TYPE_DATE;
            case ColumnType.FLOAT:
                return QwpConstants.TYPE_FLOAT;
            case ColumnType.DOUBLE:
                return QwpConstants.TYPE_DOUBLE;
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
                return QwpConstants.TYPE_VARCHAR;
            case ColumnType.SYMBOL:
                return QwpConstants.TYPE_SYMBOL;
            case ColumnType.UUID:
                return QwpConstants.TYPE_UUID;
            case ColumnType.LONG256:
                return QwpConstants.TYPE_LONG256;
            case ColumnType.IPv4:
                return QwpConstants.TYPE_IPv4;
            case ColumnType.BINARY:
                return QwpConstants.TYPE_BINARY;
            case ColumnType.TIMESTAMP_MICRO:
                return QwpConstants.TYPE_TIMESTAMP;
            case ColumnType.TIMESTAMP_NANO:
                return QwpConstants.TYPE_TIMESTAMP_NANOS;
            default:
                return 0;
        }
    }

    private void appendBinary(
            CharSequence name,
            QwpTableBuffer.ColumnBuffer column,
            long ptr,
            long len
    ) {
        if (len < 0) {
            throw error(INVALID_VALUE, name, "BINARY", (int) ColumnType.BINARY, "binary length must be non-negative");
        }
        if (len > Integer.MAX_VALUE) {
            throw error(INVALID_VALUE, name, "BINARY", (int) ColumnType.BINARY, "binary length exceeds the supported maximum");
        }
        if (len > 0 && ptr == 0) {
            throw error(INVALID_VALUE, name, "BINARY", (int) ColumnType.BINARY, "binary pointer cannot be zero for a non-empty value");
        }
        column.addBinary(ptr, len);
    }

    private QwpSchemaBinding decimalColumn(CharSequence name, Decimal value, String inputType, byte inferredWireType) {
        buffer.requireSchemaBinding(this);
        if (value == null || value.isNull()) {
            return this;
        }
        int index = targetIndex(name, inputType);
        if (index < 0) {
            QwpTableBuffer.ColumnBuffer inferred = buffer.getExistingInferredColumn(name);
            if (inferred != null && inferred.getSize() > buffer.getRowCount()) {
                return this;
            }
            if (inferred != null && inferred.getType() != inferredWireType) {
                throw unsupported(name, inputType, -1, "inferred column type conflict [inferredType="
                        + QwpConstants.getTypeName(inferred.getType()) + ']');
            }
            inferred = buffer.getOrCreateColumn(name, inferredWireType, true);
            if (inferred == null) {
                return this;
            }
            if (value instanceof Decimal64) {
                inferred.addDecimal64((Decimal64) value);
            } else if (value instanceof Decimal128) {
                inferred.addDecimal128((Decimal128) value);
            } else {
                inferred.addDecimal256((Decimal256) value);
            }
            return this;
        }
        int targetType = targetType(index, ColumnType.DECIMAL256);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, inputType, index, targetType);
        if (column == null) {
            return this;
        }
        return appendDecimal(column, name, value, inputType, targetType);
    }

    private QwpSchemaBinding appendDecimal(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            Decimal value,
            String inputType,
            int targetType
    ) {
        if (!ColumnType.isDecimal(targetType)) {
            return decimalNonDecimalColumn(column, name, value, inputType, targetType);
        }
        int targetPrecision = ColumnType.getDecimalPrecision(targetType);
        int targetScale = ColumnType.getDecimalScale(targetType);
        try {
            if (!column.addSchemaDecimal(value, targetPrecision, targetScale)) {
                throw error(INVALID_VALUE, name, inputType, targetType, "decimal value exceeds target precision");
            }
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, inputType, targetType, "decimal value cannot be rescaled exactly");
        }
        return this;
    }

    private QwpSchemaBinding decimalNonDecimalColumn(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            Decimal value,
            String inputType,
            int targetType
    ) {
        if (targetType == ColumnType.STRING || targetType == ColumnType.VARCHAR) {
            Decimal256 decimal = decimalTextScratch;
            if (decimal == null) {
                decimalTextScratch = decimal = new Decimal256();
            }
            value.toDecimal256(decimal);
            decimal.setScale(decimal.getScale() & 0xff);
            StringSink sink = numericTextSink;
            if (sink == null) {
                numericTextSink = sink = new StringSink(80);
            } else {
                sink.clear();
            }
            decimal.toSink(sink);
            column.addString(sink);
            return this;
        }
        throw unsupported(name, inputType, targetType, "conversion is not implemented");
    }

    private LineSenderSchemaException invalidRange(CharSequence name, String inputType, int targetType) {
        return error(INVALID_VALUE, name, inputType, targetType, "value is outside target type range");
    }

    private void parseDecimalText(CharSequence name, CharSequence value, Decimal256 scratch, int targetType) {
        try {
            scratch.ofString(value);
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, "DECIMAL256", targetType, "invalid decimal text");
        }
    }

    private QwpSchemaBinding integerNumericColumn(
            CharSequence name,
            long value,
            String inputType,
            int inferredType,
            boolean sourceNull
    ) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, inputType);
        int targetType = targetType(index, inferredType);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, inputType, index, targetType);
        if (column == null) {
            return this;
        }
        if (!isNumericTarget(targetType)
                && targetType != ColumnType.DATE
                && targetType != ColumnType.TIMESTAMP_MICRO
                && targetType != ColumnType.TIMESTAMP_NANO) {
            throw unsupported(name, inputType, targetType, "conversion is not implemented");
        }
        if (sourceNull) {
            column.addNull();
            return this;
        }
        switch (targetType) {
            case ColumnType.BYTE:
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw invalidRange(name, inputType, targetType);
                }
                column.addByte((byte) value);
                break;
            case ColumnType.SHORT:
                if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
                    throw invalidRange(name, inputType, targetType);
                }
                column.addShort((short) value);
                break;
            case ColumnType.INT:
                column.addInt((int) value);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
            case ColumnType.TIMESTAMP_MICRO:
            case ColumnType.TIMESTAMP_NANO:
                column.addLong(value);
                break;
            case ColumnType.FLOAT:
                column.addFloat((float) value);
                break;
            case ColumnType.DOUBLE:
                column.addDouble((double) value);
                break;
            default:
                throw new AssertionError("unsupported numeric target");
        }
        return this;
    }

    private CharSequence formatLong(long value) {
        StringSink sink = numericTextSink;
        if (sink == null) {
            numericTextSink = sink = new StringSink(20);
        } else {
            sink.clear();
        }
        Numbers.append(sink, value, false);
        return sink;
    }

    private CharSequence formatLong256(long l0, long l1, long l2, long l3) {
        StringSink sink = numericTextSink;
        if (sink == null) {
            numericTextSink = sink = new StringSink(66);
        } else {
            sink.clear();
        }
        sink.putAscii("0x");
        if (l3 != 0) {
            Numbers.appendHex(sink, l3, false);
            Numbers.appendHex(sink, l2, true);
            Numbers.appendHex(sink, l1, true);
            Numbers.appendHex(sink, l0, true);
        } else if (l2 != 0) {
            Numbers.appendHex(sink, l2, false);
            Numbers.appendHex(sink, l1, true);
            Numbers.appendHex(sink, l0, true);
        } else if (l1 != 0) {
            Numbers.appendHex(sink, l1, false);
            Numbers.appendHex(sink, l0, true);
        } else {
            Numbers.appendHex(sink, l0, false);
        }
        return sink;
    }

    private CharSequence formatGeoHash(long value, int precisionBits) {
        StringSink sink = numericTextSink;
        if (sink == null) {
            numericTextSink = sink = new StringSink(60);
        } else {
            sink.clear();
        }
        for (int bit = precisionBits - 1; bit >= 0; bit--) {
            sink.putAscii(((value >>> bit) & 1) == 0 ? '0' : '1');
        }
        return sink;
    }

    private CharSequence formatIPv4(int value) {
        StringSink sink = numericTextSink;
        if (sink == null) {
            numericTextSink = sink = new StringSink(15);
        } else {
            sink.clear();
        }
        Numbers.append(sink, (value >>> 24) & 0xff);
        sink.put('.');
        Numbers.append(sink, (value >>> 16) & 0xff);
        sink.put('.');
        Numbers.append(sink, (value >>> 8) & 0xff);
        sink.put('.');
        Numbers.append(sink, value & 0xff);
        return sink;
    }

    private CharSequence formatFloating(double value) {
        StringSink sink = floatingTextSink;
        if (sink == null) {
            floatingTextSink = sink = new StringSink(24);
            floatingTextExponentScratch = new int[1];
        } else {
            sink.clear();
        }
        QwpSchemaDoubleFormatter.append(sink, value, floatingTextExponentScratch);
        return sink;
    }

    private CharSequence formatUuid(long lo, long hi) {
        StringSink sink = uuidTextSink;
        if (sink == null) {
            uuidTextSink = sink = new StringSink(UUID_LENGTH);
        } else {
            sink.clear();
        }
        appendUuidHex(sink, hi >>> 32, 8);
        sink.put('-');
        appendUuidHex(sink, hi >>> 16, 4);
        sink.put('-');
        appendUuidHex(sink, hi, 4);
        sink.put('-');
        appendUuidHex(sink, lo >>> 48, 4);
        sink.put('-');
        appendUuidHex(sink, lo, 12);
        return sink;
    }

    private static void appendUuidHex(StringSink sink, long value, int digits) {
        for (int shift = (digits - 1) * 4; shift >= 0; shift -= 4) {
            int nibble = (int) ((value >>> shift) & 0xf);
            sink.put((char) (nibble < 10 ? '0' + nibble : 'a' + nibble - 10));
        }
    }

    private LineSenderSchemaException invalidNumericText(CharSequence name, int targetType) {
        return error(INVALID_VALUE, name, "STRING", targetType, "invalid numeric text");
    }

    private void appendStringNumeric(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            CharSequence value,
            int targetType
    ) {
        try {
            switch (targetType) {
                case ColumnType.BYTE:
                    int parsed = Numbers.parseInt(value);
                    if (parsed < Byte.MIN_VALUE || parsed > Byte.MAX_VALUE) {
                        throw NumericException.instance();
                    }
                    column.addByte((byte) parsed);
                    break;
                case ColumnType.SHORT:
                    column.addShort(parseSchemaShort(value));
                    break;
                case ColumnType.INT:
                    column.addInt(Numbers.parseInt(value));
                    break;
                case ColumnType.LONG:
                    column.addLong(parseSchemaLong(value));
                    break;
                case ColumnType.FLOAT:
                    column.addFloat(parseSchemaFloat(value));
                    break;
                case ColumnType.DOUBLE:
                    column.addDouble(parseSchemaDouble(value));
                    break;
                default:
                    throw new AssertionError("unsupported numeric target");
            }
        } catch (NumericException e) {
            throw invalidNumericText(name, targetType);
        }
    }

    private void appendStringLong256(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            CharSequence value
    ) {
        int length = value.length();
        if (length < 4 || length > 66 || (length & 1) != 0
                || value.charAt(0) != '0' || value.charAt(1) != 'x') {
            throw error(INVALID_VALUE, name, "STRING", (int) ColumnType.LONG256, "invalid LONG256 text");
        }
        try {
            int hi = length;
            int lo = Math.max(2, hi - 16);
            long l0 = Numbers.parseHexLong(value, lo, hi);
            hi = lo;
            lo = Math.max(2, hi - 16);
            long l1 = hi > 2 ? Numbers.parseHexLong(value, lo, hi) : 0;
            hi = lo;
            lo = Math.max(2, hi - 16);
            long l2 = hi > 2 ? Numbers.parseHexLong(value, lo, hi) : 0;
            hi = lo;
            long l3 = hi > 2 ? Numbers.parseHexLong(value, 2, hi) : 0;
            if (l0 == Long.MIN_VALUE && l1 == Long.MIN_VALUE
                    && l2 == Long.MIN_VALUE && l3 == Long.MIN_VALUE) {
                throw NumericException.instance();
            }
            column.addLong256(l0, l1, l2, l3);
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, "STRING", (int) ColumnType.LONG256, "invalid LONG256 text");
        }
    }

    private void appendStringGeoHash(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            CharSequence value,
            int precision,
            int targetType
    ) {
        int chars = Math.min(value.length(), 12);
        if (chars * 5 < precision) {
            throw error(INVALID_VALUE, name, "STRING", targetType, "invalid GEOHASH text");
        }
        try {
            long hash = Numbers.parseGeoHashBase32(value, 0, chars);
            column.addGeoHash(hash >>> (chars * 5 - precision), precision);
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, "STRING", targetType, "invalid GEOHASH text");
        }
    }

    private void appendStringTimestamp(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            CharSequence value,
            int targetType
    ) {
        Utf8StringSink sink = timestampTextSink;
        if (sink == null) {
            timestampTextSink = sink = new Utf8StringSink(Math.max(32, value.length()));
        } else {
            sink.clear();
        }
        sink.put(value);
        try {
            long micros = QwpSchemaTimestampParser.parse(sink.asAsciiCharSequence());
            if (targetType == ColumnType.TIMESTAMP_NANO) {
                if (micros > Long.MAX_VALUE / 1000 || micros < Long.MIN_VALUE / 1000) {
                    throw NumericException.instance();
                }
                column.addLong(micros * 1000);
            } else {
                column.addLong(micros);
            }
        } catch (NumericException e) {
            throw error(INVALID_VALUE, name, "STRING", targetType, "invalid timestamp text");
        }
    }

    private void appendTimestampText(QwpTableBuffer.ColumnBuffer column, long micros) {
        Utf8StringSink sink = timestampTextSink;
        if (sink == null) {
            timestampTextSink = sink = new Utf8StringSink(32);
        } else {
            sink.clear();
        }
        QwpSchemaTimestampFormatter.appendDateTime(sink, micros);
        column.addString(sink.asAsciiCharSequence());
    }

    private static double parseSchemaDouble(CharSequence value) throws NumericException {
        int length = oppositeFloatingSuffixLength(value, 'f');
        return length < 0
                ? Numbers.parseDouble(value)
                : FastDoubleParser.parseDouble(value, 0, length, true);
    }

    private static float parseSchemaFloat(CharSequence value) throws NumericException {
        int length = oppositeFloatingSuffixLength(value, 'd');
        return length < 0
                ? FastFloatParser.parseFloat(value, true)
                : FastFloatParser.parseFloat(value, 0, length, true);
    }

    private static int oppositeFloatingSuffixLength(CharSequence value, char suffix) {
        int index = value.length() - 1;
        while (index >= 0 && value.charAt(index) <= ' ') {
            index--;
        }
        if (index <= 0 || (value.charAt(index) | 32) != suffix) {
            return -1;
        }
        char preceding = value.charAt(index - 1);
        return (preceding >= '0' && preceding <= '9') || preceding == '.' ? index : -1;
    }

    private static long parseSchemaLong(CharSequence value) throws NumericException {
        int length = value.length();
        if (length == 0) {
            throw NumericException.instance();
        }
        boolean negative = value.charAt(0) == '-';
        int index = negative ? 1 : 0;
        if (index == length) {
            throw NumericException.instance();
        }
        long limit = negative ? Long.MIN_VALUE : -Long.MAX_VALUE;
        long multiplyLimit = limit / 10;
        long result = 0;
        int digitCount = 0;
        for (; index < length; index++) {
            char c = value.charAt(index);
            if (c == '_' || c == 0x7f) {
                if (digitCount == 0) {
                    throw NumericException.instance();
                }
                digitCount = 0;
            } else if (c == 'L' || c == 'l') {
                if (digitCount == 0 || index + 1 != length) {
                    throw NumericException.instance();
                }
            } else {
                if (c < '0' || c > '9') {
                    throw NumericException.instance();
                }
                int digit = c - '0';
                if (result < multiplyLimit) {
                    throw NumericException.instance();
                }
                result *= 10;
                if (result < limit + digit) {
                    throw NumericException.instance();
                }
                result -= digit;
                digitCount++;
            }
        }
        if (digitCount == 0) {
            throw NumericException.instance();
        }
        return negative ? result : -result;
    }

    private static short parseSchemaShort(CharSequence value) throws NumericException {
        int length = value.length();
        int index = length > 0 && value.charAt(0) == '-' ? 1 : 0;
        if (index == length) {
            throw NumericException.instance();
        }
        for (; index < length; index++) {
            char c = value.charAt(index);
            if (c < '0' || c > '9') {
                throw NumericException.instance();
            }
        }
        int parsed = Numbers.parseInt(value);
        if (parsed < Short.MIN_VALUE || parsed > Short.MAX_VALUE) {
            throw NumericException.instance();
        }
        return (short) parsed;
    }

    private QwpSchemaBinding floatingNumericColumn(CharSequence name, double value, String inputType) {
        buffer.requireSchemaBinding(this);
        int index = targetIndex(name, inputType);
        int targetType = targetType(index,
                "FLOAT".equals(inputType) ? ColumnType.FLOAT : ColumnType.DOUBLE);
        QwpTableBuffer.ColumnBuffer column = targetColumn(name, inputType, index, targetType);
        if (column == null) {
            return this;
        }
        if (!isNumericTarget(targetType)) {
            return floatingNonNumericColumn(column, name, value, inputType, targetType);
        }
        if (Double.isNaN(value)) {
            column.addNull();
            return this;
        }
        switch (targetType) {
            case ColumnType.BYTE:
                requireWholeInRange(name, inputType, targetType, value, Byte.MIN_VALUE, Byte.MAX_VALUE);
                column.addByte((byte) value);
                break;
            case ColumnType.SHORT:
                requireWholeInRange(name, inputType, targetType, value, Short.MIN_VALUE, Short.MAX_VALUE);
                column.addShort((short) value);
                break;
            case ColumnType.INT:
                requireWholeInRange(name, inputType, targetType, value, Integer.MIN_VALUE, Integer.MAX_VALUE);
                column.addInt((int) value);
                break;
            case ColumnType.LONG:
                if (!Double.isFinite(value) || value != Math.rint(value) || value < -0x1.0p63 || value >= 0x1.0p63) {
                    throw error(INVALID_VALUE, name, inputType, targetType, "value cannot be represented exactly by target integer type");
                }
                column.addLong((long) value);
                break;
            case ColumnType.FLOAT:
                column.addFloat((float) value);
                break;
            case ColumnType.DOUBLE:
                column.addDouble(value);
                break;
        }
        return this;
    }

    private QwpSchemaBinding floatingNonNumericColumn(
            QwpTableBuffer.ColumnBuffer column,
            CharSequence name,
            double value,
            String inputType,
            int targetType
    ) {
        if (ColumnType.isDecimal(targetType)) {
            int targetPrecision = ColumnType.getDecimalPrecision(targetType);
            int targetScale = ColumnType.getDecimalScale(targetType);
            if (!Double.isFinite(value)) {
                column.addSchemaDecimalNull(targetScale);
                return this;
            }
            try {
                if (!column.addSchemaStringDecimal(formatFloating(value), targetPrecision, targetScale)) {
                    throw error(INVALID_VALUE, name, inputType, targetType, "decimal value exceeds target precision");
                }
            } catch (NumericException e) {
                throw error(INVALID_VALUE, name, inputType, targetType,
                        "floating value cannot be represented exactly by target decimal precision and scale");
            }
            return this;
        }
        if (!isTextTarget(targetType)) {
            throw unsupported(name, inputType, targetType, "conversion is not implemented");
        }
        if (Double.isNaN(value)) {
            column.addNull();
            return this;
        }
        switch (targetType) {
            case ColumnType.STRING:
            case ColumnType.VARCHAR:
                column.addString(formatFloating(value));
                break;
            case ColumnType.SYMBOL:
                column.addSymbol(formatFloating(value));
                break;
            default:
                throw unsupported(name, inputType, targetType, "conversion is not implemented");
        }
        return this;
    }

    private void requireWholeInRange(
            CharSequence name,
            String inputType,
            int targetType,
            double value,
            double min,
            double max
    ) {
        if (!Double.isFinite(value) || value != Math.rint(value) || value < min || value > max) {
            throw error(INVALID_VALUE, name, inputType, targetType, "value cannot be represented exactly by target integer type");
        }
    }

    private static boolean isNumericTarget(int targetType) {
        return targetType == ColumnType.BYTE
                || targetType == ColumnType.SHORT
                || targetType == ColumnType.INT
                || targetType == ColumnType.LONG
                || targetType == ColumnType.FLOAT
                || targetType == ColumnType.DOUBLE;
    }

    private static boolean isTextTarget(int targetType) {
        return targetType == ColumnType.STRING
                || targetType == ColumnType.VARCHAR
                || targetType == ColumnType.SYMBOL;
    }

    private static boolean isSupportedTimestampUnit(ChronoUnit unit) {
        return unit == ChronoUnit.NANOS
                || unit == ChronoUnit.MICROS
                || unit == ChronoUnit.MILLIS
                || unit == ChronoUnit.SECONDS
                || unit == ChronoUnit.MINUTES
                || unit == ChronoUnit.HOURS
                || unit == ChronoUnit.DAYS;
    }

    // Matches client UTF-8 replacement followed by the server's first-BMP decoder:
    // supplementary code points become NUL and malformed surrogates become '?'.
    private static char stringToChar(CharSequence value) {
        if (value.length() == 0) {
            return 0;
        }
        char first = value.charAt(0);
        if (Character.isHighSurrogate(first)) {
            return value.length() > 1 && Character.isLowSurrogate(value.charAt(1)) ? 0 : '?';
        }
        return Character.isLowSurrogate(first) ? '?' : first;
    }

    private static long instantTimestamp(Instant instant, long unitsPerSecond, long fraction) {
        long seconds = instant.getEpochSecond();
        if (seconds < 0) {
            // Keep both terms non-positive so an intermediate underflow also means the final sum is out of range.
            return Math.addExact(Math.multiplyExact(seconds + 1, unitsPerSecond), fraction - unitsPerSecond);
        }
        return Math.addExact(Math.multiplyExact(seconds, unitsPerSecond), fraction);
    }

    private int requireDesignatedTimestamp(String inputType) {
        int index = schema.getDesignatedIndex();
        if (index < 0) {
            throw unsupported(null, inputType, -1, "target table has no designated timestamp");
        }
        int targetType = schema.getColumnType(index);
        timestampWireType(targetType);
        if (schema.hasColumnExtensionParameters(index)) {
            throw unsupported(null, inputType, targetType, "parameterized target type");
        }
        return index;
    }

    private byte timestampWireType(int targetType) {
        if (targetType == ColumnType.TIMESTAMP_MICRO) {
            return QwpConstants.TYPE_TIMESTAMP;
        }
        if (targetType == ColumnType.TIMESTAMP_NANO) {
            return QwpConstants.TYPE_TIMESTAMP_NANOS;
        }
        throw unsupported(null, "TIMESTAMP", targetType, "designated timestamp conversion is not implemented");
    }

    private int targetIndex(CharSequence name, String inputType) {
        if (name == null || !TableUtils.isValidColumnName(name, QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH)) {
            throw error(INVALID_VALUE, name, inputType, null, "invalid column name");
        }
        int index = columns.get(name);
        return index;
    }

    private int targetType(int index, int inferredType) {
        return index >= 0 ? schema.getColumnType(index) : inferredType;
    }

    private LineSenderSchemaException unsupported(CharSequence name, String inputType, int targetType, String detail) {
        return error(UNSUPPORTED_FEATURE, name, inputType, targetType, detail);
    }
}
