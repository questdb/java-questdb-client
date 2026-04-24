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

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimals;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Typed bind-parameter sink for a single QWP egress query.
 * <p>
 * Writes the per-bind wire layout directly into a reusable native buffer:
 * {@code type_code(1B) | null_flag(1B) | [bitmap(1B) if null_flag != 0] |
 * [value bytes if !null]}.
 * <p>
 * Non-null: {@code type | 0x00 | value}. NULL: {@code type | 0x01 | 0x01}
 * (no value bytes).
 * <p>
 * Indexes must be assigned in strictly ascending order starting at 0. The
 * sink tracks the next expected index and throws {@link IllegalStateException}
 * on gaps or duplicates. Matches the server's positional bind layout; SQL
 * parameter placeholders are 1-based ({@code $1, $2, ...}), indexes here are
 * 0-based and map to {@code $(index + 1)}.
 * <p>
 * Multi-byte values are little-endian. DECIMAL scales are validated against
 * {@link Decimals#MAX_SCALE}; GEOHASH precisions are validated against the
 * [1, 60]-bit range the server enforces.
 * <p>
 * Not thread-safe. One instance per {@link QwpQueryClient}; reset on every
 * {@link QwpQueryClient#execute(String, QwpBindSetter, QwpColumnBatchHandler)}
 * call.
 */
public final class QwpBindValues implements QuietCloseable {

    /**
     * Maximum scale for a DECIMAL128 bind. DECIMAL128 stores up to 38 digits of
     * precision, so scale above 38 is mathematically invalid even though the
     * wire format can carry any byte value.
     */
    private static final int DECIMAL128_MAX_SCALE = 38;
    /**
     * Maximum scale for a DECIMAL256 bind. DECIMAL256 is the widest decimal
     * type and its scale cap matches {@link Decimals#MAX_SCALE} (76).
     */
    private static final int DECIMAL256_MAX_SCALE = Decimals.MAX_SCALE;
    /**
     * Maximum scale for a DECIMAL64 bind. DECIMAL64 stores up to 18 digits of
     * precision, so scale above 18 is mathematically invalid.
     */
    private static final int DECIMAL64_MAX_SCALE = 18;
    /**
     * Maximum GEOHASH precision in bits, matching the server's
     * {@code ColumnType.GEOLONG_MAX_BITS} check.
     */
    private static final int GEOHASH_MAX_BITS = 60;
    private static final int GEOHASH_MIN_BITS = 1;
    private static final byte NULL_BITMAP = 0x01;
    private static final byte NULL_FLAG = 0x01;
    private static final byte NON_NULL_FLAG = 0x00;

    private final NativeBufferWriter writer = new NativeBufferWriter();
    private int count;
    private int expectedNextIndex;

    @Override
    public void close() {
        writer.close();
    }

    public QwpBindValues setBoolean(int index, boolean value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_BOOLEAN, false);
        writer.putByte(value ? (byte) 1 : (byte) 0);
        return this;
    }

    public QwpBindValues setByte(int index, byte value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_BYTE, false);
        writer.putByte(value);
        return this;
    }

    public QwpBindValues setChar(int index, char value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_CHAR, false);
        writer.putShort((short) value);
        return this;
    }

    public QwpBindValues setDate(int index, long millisSinceEpoch) {
        advance(index);
        writeHeader(QwpConstants.TYPE_DATE, false);
        writer.putLong(millisSinceEpoch);
        return this;
    }

    public QwpBindValues setDecimal128(int index, int scale, long lo, long hi) {
        checkScale128(scale);
        advance(index);
        writeHeader(QwpConstants.TYPE_DECIMAL128, false);
        writer.putByte((byte) scale);
        writer.putLong(lo);
        writer.putLong(hi);
        return this;
    }

    /**
     * Convenience overload that takes the scale and limbs from the supplied
     * {@link Decimal128}. If the value is NULL (Decimal128's canonical NULL
     * sentinel), an explicit DECIMAL128 NULL is encoded preserving
     * {@code value.getScale()}; a {@code null} reference encodes with scale 0.
     */
    public QwpBindValues setDecimal128(int index, Decimal128 value) {
        if (value == null) {
            return setNullDecimal128(index, 0);
        }
        if (Decimal128.isNull(value.getHigh(), value.getLow())) {
            return setNullDecimal128(index, value.getScale());
        }
        return setDecimal128(index, value.getScale(), value.getLow(), value.getHigh());
    }

    public QwpBindValues setDecimal256(int index, int scale, long ll, long lh, long hl, long hh) {
        checkScale256(scale);
        advance(index);
        writeHeader(QwpConstants.TYPE_DECIMAL256, false);
        writer.putByte((byte) scale);
        writer.putLong(ll);
        writer.putLong(lh);
        writer.putLong(hl);
        writer.putLong(hh);
        return this;
    }

    /**
     * Convenience overload that takes the scale and limbs from the supplied
     * {@link Decimal256}. If the value is NULL (Decimal256's canonical NULL
     * sentinel), an explicit DECIMAL256 NULL is encoded preserving
     * {@code value.getScale()}; a {@code null} reference encodes with scale 0.
     */
    public QwpBindValues setDecimal256(int index, Decimal256 value) {
        if (value == null) {
            return setNullDecimal256(index, 0);
        }
        if (Decimal256.isNull(value.getHh(), value.getHl(), value.getLh(), value.getLl())) {
            return setNullDecimal256(index, value.getScale());
        }
        return setDecimal256(index, value.getScale(), value.getLl(), value.getLh(), value.getHl(), value.getHh());
    }

    public QwpBindValues setDecimal64(int index, int scale, long unscaledValue) {
        checkScale64(scale);
        advance(index);
        writeHeader(QwpConstants.TYPE_DECIMAL64, false);
        writer.putByte((byte) scale);
        writer.putLong(unscaledValue);
        return this;
    }

    public QwpBindValues setDouble(int index, double value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_DOUBLE, false);
        writer.putLong(Double.doubleToRawLongBits(value));
        return this;
    }

    public QwpBindValues setFloat(int index, float value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_FLOAT, false);
        writer.putInt(Float.floatToRawIntBits(value));
        return this;
    }

    /**
     * Encodes a GEOHASH bind with the given precision (in bits) and packed
     * value. The value is stored little-endian in {@code ceil(precisionBits / 8)}
     * bytes. Precision must be in {@code [1, 60]}.
     * <p>
     * {@code value} is masked to {@code precisionBits} before encoding, so bits
     * above the declared precision cannot leak into the top wire byte (which
     * would otherwise pass through when {@code precisionBits} is not a multiple
     * of 8).
     */
    public QwpBindValues setGeohash(int index, int precisionBits, long value) {
        checkGeohashPrecision(precisionBits);
        advance(index);
        writeHeader(QwpConstants.TYPE_GEOHASH, false);
        writer.putVarint(precisionBits);
        long masked = maskGeohashBits(value, precisionBits);
        int byteCount = (precisionBits + 7) >>> 3;
        for (int b = 0; b < byteCount; b++) {
            writer.putByte((byte) (masked >>> (b * 8)));
        }
        return this;
    }

    public QwpBindValues setInt(int index, int value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_INT, false);
        writer.putInt(value);
        return this;
    }

    public QwpBindValues setLong(int index, long value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_LONG, false);
        writer.putLong(value);
        return this;
    }

    public QwpBindValues setLong256(int index, long l0, long l1, long l2, long l3) {
        advance(index);
        writeHeader(QwpConstants.TYPE_LONG256, false);
        writer.putLong(l0);
        writer.putLong(l1);
        writer.putLong(l2);
        writer.putLong(l3);
        return this;
    }

    /**
     * Binds an explicit NULL with the given QWP wire type. The type code must
     * be one of the supported scalar bind types; ARRAY, BINARY, and IPv4 are
     * rejected because the server decoder does not accept them as binds.
     * <p>
     * DECIMAL64/128/256 NULLs are encoded with scale 0 and GEOHASH NULLs with
     * precision {@value #GEOHASH_MIN_BITS} bit; use {@link #setNullDecimal64},
     * {@link #setNullDecimal128}, {@link #setNullDecimal256}, or
     * {@link #setNullGeohash} when the scale/precision matters (it becomes part
     * of the bound variable's type on the server).
     */
    public QwpBindValues setNull(int index, byte qwpTypeCode) {
        checkBindType(qwpTypeCode);
        if (qwpTypeCode == QwpConstants.TYPE_DECIMAL64) {
            return setNullDecimal64(index, 0);
        }
        if (qwpTypeCode == QwpConstants.TYPE_DECIMAL128) {
            return setNullDecimal128(index, 0);
        }
        if (qwpTypeCode == QwpConstants.TYPE_DECIMAL256) {
            return setNullDecimal256(index, 0);
        }
        if (qwpTypeCode == QwpConstants.TYPE_GEOHASH) {
            return setNullGeohash(index, GEOHASH_MIN_BITS);
        }
        advance(index);
        writeHeader(qwpTypeCode, true);
        return this;
    }

    /**
     * Binds an explicit NULL with DECIMAL128 type and the given scale. The
     * server reads the scale byte regardless of null, so the scale must be
     * supplied even for NULL (it becomes part of the bound variable's type).
     */
    public QwpBindValues setNullDecimal128(int index, int scale) {
        checkScale128(scale);
        advance(index);
        writeHeader(QwpConstants.TYPE_DECIMAL128, true);
        writer.putByte((byte) scale);
        return this;
    }

    /**
     * Binds an explicit NULL with DECIMAL256 type and the given scale. See
     * {@link #setNullDecimal128} for the rationale.
     */
    public QwpBindValues setNullDecimal256(int index, int scale) {
        checkScale256(scale);
        advance(index);
        writeHeader(QwpConstants.TYPE_DECIMAL256, true);
        writer.putByte((byte) scale);
        return this;
    }

    /**
     * Binds an explicit NULL with DECIMAL64 type and the given scale. See
     * {@link #setNullDecimal128} for the rationale.
     */
    public QwpBindValues setNullDecimal64(int index, int scale) {
        checkScale64(scale);
        advance(index);
        writeHeader(QwpConstants.TYPE_DECIMAL64, true);
        writer.putByte((byte) scale);
        return this;
    }

    /**
     * Binds an explicit NULL with GEOHASH type and the given precision (bits).
     * The server reads the precision_bits varint regardless of null, so
     * precision must be supplied even for NULL (it becomes part of the bound
     * variable's type).
     */
    public QwpBindValues setNullGeohash(int index, int precisionBits) {
        checkGeohashPrecision(precisionBits);
        advance(index);
        writeHeader(QwpConstants.TYPE_GEOHASH, true);
        writer.putVarint(precisionBits);
        return this;
    }

    public QwpBindValues setShort(int index, short value) {
        advance(index);
        writeHeader(QwpConstants.TYPE_SHORT, false);
        writer.putShort(value);
        return this;
    }

    public QwpBindValues setTimestampMicros(int index, long microsSinceEpoch) {
        advance(index);
        writeHeader(QwpConstants.TYPE_TIMESTAMP, false);
        writer.putLong(microsSinceEpoch);
        return this;
    }

    public QwpBindValues setTimestampNanos(int index, long nanosSinceEpoch) {
        advance(index);
        writeHeader(QwpConstants.TYPE_TIMESTAMP_NANOS, false);
        writer.putLong(nanosSinceEpoch);
        return this;
    }

    public QwpBindValues setUuid(int index, long lo, long hi) {
        advance(index);
        writeHeader(QwpConstants.TYPE_UUID, false);
        writer.putLong(lo);
        writer.putLong(hi);
        return this;
    }

    /**
     * Convenience overload. Encodes {@link UUID#getLeastSignificantBits()} as
     * the lo limb and {@link UUID#getMostSignificantBits()} as the hi limb,
     * matching how QuestDB's UUID type is laid out internally.
     */
    public QwpBindValues setUuid(int index, UUID uuid) {
        if (uuid == null) {
            return setNull(index, QwpConstants.TYPE_UUID);
        }
        return setUuid(index, uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
    }

    /**
     * Encodes a VARCHAR bind. A {@code null} value is written as a typed NULL.
     * Strings are encoded as: {@code u32 offset0=0 | u32 length_bytes | UTF-8 bytes}.
     * The length must fit in a signed int32 (the server rejects negative
     * lengths).
     */
    public QwpBindValues setVarchar(int index, CharSequence value) {
        if (value == null) {
            return setNull(index, QwpConstants.TYPE_VARCHAR);
        }
        advance(index);
        writeHeader(QwpConstants.TYPE_VARCHAR, false);
        // Fast path for ASCII-only; fall back to UTF-8 re-encode for non-ASCII.
        int charLen = value.length();
        if (isAscii(value, charLen)) {
            writer.putInt(0);          // offset[0]
            writer.putInt(charLen);    // offset[1] = UTF-8 length (== charLen for ASCII)
            writer.ensureCapacity(charLen);
            long addr = writer.getWriteAddress();
            for (int i = 0; i < charLen; i++) {
                Unsafe.getUnsafe().putByte(addr + i, (byte) value.charAt(i));
            }
            writer.skip(charLen);
            return this;
        }
        byte[] utf8 = value.toString().getBytes(StandardCharsets.UTF_8);
        writer.putInt(0);
        writer.putInt(utf8.length);
        writer.ensureCapacity(utf8.length);
        long addr = writer.getWriteAddress();
        for (int i = 0; i < utf8.length; i++) {
            Unsafe.getUnsafe().putByte(addr + i, utf8[i]);
        }
        writer.skip(utf8.length);
        return this;
    }

    /**
     * Number of bytes of encoded bind payload currently in the buffer. Used
     * by {@link QwpQueryClient} to hand the payload to the I/O thread; not
     * intended for user code.
     */
    public long bufferLen() {
        return writer.getPosition();
    }

    /**
     * Native pointer to the encoded bind payload. Used by
     * {@link QwpQueryClient} to hand the payload to the I/O thread; not
     * intended for user code. Valid only until the next {@link #reset()}
     * (implicit at the start of every {@code execute} call).
     */
    public long bufferPtr() {
        return writer.getBufferPtr();
    }

    /**
     * Number of binds that have been written since the last {@link #reset()}.
     * Used by {@link QwpQueryClient} to emit the {@code bind_count} varint;
     * not intended for user code.
     */
    public int count() {
        return count;
    }

    /**
     * Clears prior state so this instance can accumulate binds for a new
     * query. Called by {@link QwpQueryClient} at the start of every
     * {@code execute}; not intended for user code.
     */
    public void reset() {
        writer.reset();
        count = 0;
        expectedNextIndex = 0;
    }

    private static void checkGeohashPrecision(int precisionBits) {
        if (precisionBits < GEOHASH_MIN_BITS || precisionBits > GEOHASH_MAX_BITS) {
            throw new IllegalArgumentException(
                    "GEOHASH precision must be in [" + GEOHASH_MIN_BITS + ", " + GEOHASH_MAX_BITS
                            + "], got " + precisionBits);
        }
    }

    private static boolean isAscii(CharSequence value, int charLen) {
        for (int i = 0; i < charLen; i++) {
            if (value.charAt(i) >= 0x80) {
                return false;
            }
        }
        return true;
    }

    private static long maskGeohashBits(long value, int precisionBits) {
        return precisionBits >= 64 ? value : value & ((1L << precisionBits) - 1L);
    }

    private void advance(int index) {
        if (index != expectedNextIndex) {
            throw new IllegalStateException(
                    "bind index out of order: expected " + expectedNextIndex + ", got " + index);
        }
        if (count >= QwpConstants.MAX_COLUMNS_PER_TABLE) {
            throw new IllegalStateException(
                    "too many binds: exceeds " + QwpConstants.MAX_COLUMNS_PER_TABLE);
        }
        expectedNextIndex++;
        count++;
    }

    private void checkBindType(byte type) {
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN:
            case QwpConstants.TYPE_BYTE:
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_CHAR:
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_FLOAT:
            case QwpConstants.TYPE_DOUBLE:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
            case QwpConstants.TYPE_UUID:
            case QwpConstants.TYPE_LONG256:
            case QwpConstants.TYPE_GEOHASH:
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_DECIMAL64:
            case QwpConstants.TYPE_DECIMAL128:
            case QwpConstants.TYPE_DECIMAL256:
                return;
            default:
                throw new IllegalArgumentException(
                        "unsupported bind type 0x" + Integer.toHexString(type & 0xFF));
        }
    }

    private void checkScale128(int scale) {
        if (scale < 0 || scale > DECIMAL128_MAX_SCALE) {
            throw new IllegalArgumentException(
                    "DECIMAL128 scale must be in [0, " + DECIMAL128_MAX_SCALE + "], got " + scale);
        }
    }

    private void checkScale256(int scale) {
        if (scale < 0 || scale > DECIMAL256_MAX_SCALE) {
            throw new IllegalArgumentException(
                    "DECIMAL256 scale must be in [0, " + DECIMAL256_MAX_SCALE + "], got " + scale);
        }
    }

    private void checkScale64(int scale) {
        if (scale < 0 || scale > DECIMAL64_MAX_SCALE) {
            throw new IllegalArgumentException(
                    "DECIMAL64 scale must be in [0, " + DECIMAL64_MAX_SCALE + "], got " + scale);
        }
    }

    private void writeHeader(byte type, boolean isNull) {
        writer.putByte(type);
        if (isNull) {
            writer.putByte(NULL_FLAG);
            writer.putByte(NULL_BITMAP);
        } else {
            writer.putByte(NON_NULL_FLAG);
        }
    }
}
