/*******************************************************************************
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

package io.questdb.std;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.fastdouble.FastDoubleParser;
import io.questdb.std.fastdouble.FastFloatParser;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8s;
import jdk.internal.math.FDBigInteger;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public final class Numbers {
    public static final int INT_NULL = Integer.MIN_VALUE;
    public static final int IPv4_NULL = 0;
    public static final long LONG_NULL = Long.MIN_VALUE;
    public static final int MAX_DOUBLE_SCALE = 19;
    public static final int MAX_FLOAT_SCALE = 10;
    public static final int SIGNIFICAND_WIDTH = 53;
    public static final long SIGN_BIT_MASK = 0x8000000000000000L;
    public static final char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    public final static int[] hexNumbers;
    public static final long[] pow10;
    public final static int pow10max;
    private static final int EXP_BIAS = 1023;
    private static final long EXP_BIT_MASK = 0x7FF0000000000000L;
    private static final int EXP_SHIFT = SIGNIFICAND_WIDTH - 1;
    static final long EXP_ONE = ((long) EXP_BIAS) << EXP_SHIFT; // exponent of 1.0
    private static final long FRACT_HOB = (1L << EXP_SHIFT); // assumed High-Order bit
    private static final long[] LONG_5_POW = new long[]{1L, 5L, 25L, 125L, 625L, 3125L, 15625L, 78125L, 390625L, 1953125L, 9765625L, 48828125L, 244140625L, 1220703125L, 6103515625L, 30517578125L, 152587890625L, 762939453125L, 3814697265625L, 19073486328125L, 95367431640625L, 476837158203125L, 2384185791015625L, 11920928955078125L, 59604644775390625L, 298023223876953125L, 1490116119384765625L};
    private static final int MAX_SMALL_BIN_EXP = 62;
    private static final int MIN_SMALL_BIN_EXP = -(63 / 3);
    private static final int[] N_5_BITS = new int[]{0, 3, 5, 7, 10, 12, 14, 17, 19, 21, 24, 26, 28, 31, 33, 35, 38, 40, 42, 45, 47, 49, 52, 54, 56, 59, 61};
    private static final long SIGNIF_BIT_MASK = 0x000FFFFFFFFFFFFFL;
    private static final int[] SMALL_5_POW = new int[]{1, 5, 25, 125, 625, 3125, 15625, 78125, 390625, 1953125, 9765625, 48828125, 244140625, 1220703125};
    private static final int[] insignificantDigitsNumber = new int[]{0, 0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 8, 8, 8, 9, 9, 9, 9, 10, 10, 10, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 14, 14, 14, 15, 15, 15, 15, 16, 16, 16, 17, 17, 17, 18, 18, 18, 19};
    private static final LongHexAppender[] longHexAppender = new LongHexAppender[Long.SIZE + 1];
    private static final LongHexAppender[] longHexAppenderPad64 = new LongHexAppender[Long.SIZE + 1];
    private final static ThreadLocal<char[]> tlDoubleDigitsBuffer = new ThreadLocal<>(() -> new char[21]);

    private Numbers() {
    }

    public static void append(CharSink<?> sink, final float value, int scale) {
        float f = value;
        if (f == Float.POSITIVE_INFINITY) {
            sink.putAscii("Infinity");
            return;
        }

        if (f == Float.NEGATIVE_INFINITY) {
            sink.putAscii("-Infinity");
            return;
        }

        if (Float.isNaN(f)) {
            sink.putAscii("NaN");
            return;
        }

        // it is very awkward to distinguish between 0.0 and -0.0
        // -0.0 < 0 is false
        if (f < 0 || 1 / f == Float.NEGATIVE_INFINITY) {
            sink.putAscii('-');
            f = -f;
        }
        int factor = (int) pow10[scale];
        int scaled = (int) (f * factor + 0.5);
        int targetScale = scale + 1;
        int z;
        while (targetScale < 11 && (z = factor * 10) <= scaled) {
            factor = z;
            targetScale++;
        }

        if (targetScale == 11) {
            sink.putAscii(Float.toString(f));
            return;
        }

        while (targetScale > 0) {
            if (targetScale-- == scale) {
                sink.putAscii('.');
            }
            sink.putAscii((char) ('0' + scaled / factor % 10));
            factor /= 10;
        }
    }

    public static void append(CharSink<?> sink, final int value) {
        int i = value;
        if (i < 0) {
            if (i == Numbers.INT_NULL) {
                sink.putAscii("null");
                return;
            }
            sink.putAscii('-');
            i = -i;
        }
        if (i < 10) {
            sink.putAscii((char) ('0' + i));
        } else if (i < 100) {  // two
            appendInt2(sink, i);
        } else if (i < 1000) { // three
            appendInt3(sink, i);
        } else if (i < 10000) { // four
            appendInt4(sink, i);
        } else if (i < 100000) { // five
            appendInt5(sink, i);
        } else if (i < 1000000) { // six
            appendInt6(sink, i);
        } else if (i < 10000000) { // seven
            appendInt7(sink, i);
        } else if (i < 100000000) { // eight
            appendInt8(sink, i);
        } else if (i < 1000000000) { // nine
            appendInt9(sink, i);
        } else {
            // ten
            appendInt10(sink, i);
        }
    }

    public static void append(CharSink<?> sink, final long value) {
        append(sink, value, true);
    }

    public static void append(CharSink<?> sink, final long value, final boolean checkNaN) {
        long i = value;
        if (i < 0) {
            if (i == Long.MIN_VALUE) {
                if (checkNaN) {
                    sink.putAscii("null");
                } else {
                    // we cannot negate Long.MIN_VALUE, so we have to special case it
                    sink.putAscii("-9223372036854775808");
                }
                return;
            }
            sink.putAscii('-');
            i = -i;
        }

        if (i < 10) {
            sink.putAscii((char) ('0' + i));
        } else if (i < 100) {  // two
            appendLong2(sink, i);
        } else if (i < 1000) { // three
            appendLong3(sink, i);
        } else if (i < 10000) { // four
            appendLong4(sink, i);
        } else if (i < 100000) { // five
            appendLong5(sink, i);
        } else if (i < 1000000) { // six
            appendLong6(sink, i);
        } else if (i < 10000000) { // seven
            appendLong7(sink, i);
        } else if (i < 100000000) { // eight
            appendLong8(sink, i);
        } else if (i < 1000000000) { // nine
            appendLong9(sink, i);
        } else if (i < 10000000000L) {
            appendLong10(sink, i);
        } else if (i < 100000000000L) { //  eleven
            appendLong11(sink, i);
        } else if (i < 1000000000000L) { //  twelve
            appendLong12(sink, i);
        } else if (i < 10000000000000L) { //  thirteen
            appendLong13(sink, i);
        } else if (i < 100000000000000L) { //  fourteen
            appendLong14(sink, i);
        } else if (i < 1000000000000000L) { //  fifteen
            appendLong15(sink, i);
        } else if (i < 10000000000000000L) { //  sixteen
            appendLong16(sink, i);
        } else if (i < 100000000000000000L) { //  seventeen
            appendLong17(sink, i);
        } else if (i < 1000000000000000000L) { //  eighteen
            appendLong18(sink, i);
        } else { //  nineteen
            appendLong19(sink, i);
        }
    }

    public static void append(CharSink<?> sink, float value) {
        append(sink, value, MAX_FLOAT_SCALE);
    }

    public static void append(CharSink<?> sink, double value) {
        append(sink, value, MAX_DOUBLE_SCALE);
    }

    public static void append(CharSink<?> sink, double value, int scale) {
        final char[] digits = tlDoubleDigitsBuffer.get();
        final long doubleBits = Double.doubleToRawLongBits(value);
        boolean negative = (doubleBits & SIGN_BIT_MASK) != 0L;
        long significantBitCount = doubleBits & SIGNIF_BIT_MASK;
        int binExp = (int) ((doubleBits & EXP_BIT_MASK) >> EXP_SHIFT);

        if (binExp == 2047) {
            if (significantBitCount == 0L) {
                if (negative) {
                    sink.putAscii("-Infinity");
                } else {
                    sink.putAscii("Infinity");
                }
            } else {
                sink.putAscii("NaN");
            }
        } else {
            int fractionBits;
            if (binExp == 0) {
                if (significantBitCount == 0L) {
                    if (negative) {
                        sink.putAscii("-0.0");
                    } else {
                        sink.putAscii("0.0");
                    }
                    return;
                }

                int leadingZeros = Long.numberOfLeadingZeros(significantBitCount);
                int shift = leadingZeros - (63 - EXP_SHIFT);
                significantBitCount <<= shift;
                binExp = 1 - shift;
                fractionBits = 64 - leadingZeros;
            } else {
                significantBitCount |= FRACT_HOB;
                fractionBits = 53;
            }

            binExp -= EXP_BIAS;

            appendDouble0(binExp, significantBitCount, fractionBits, negative, digits, sink, scale);
        }
    }

    public static void appendHex(CharSink<?> sink, long value, boolean pad) {
        int bit = value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value);
        LongHexAppender[] array = pad ? longHexAppenderPad64 : longHexAppender;
        array[bit].append(sink, value);
    }

    /**
     * Append a long value to a CharSink in hex format.
     *
     * @param sink       the CharSink to append to
     * @param value      the value to append
     * @param padToBytes if non-zero, pad the output to the specified number of bytes
     */
    public static void appendHexPadded(CharSink<?> sink, long value, int padToBytes) {
        assert padToBytes >= 0 && padToBytes <= 8;
        // This code might be unclear, so here are some hints:
        // This method uses longHexAppender() and longHexAppender() is always padding to a whole byte. It never prints
        // just a nibble. It means the longHexAppender() will print value 0xf as "0f". Value 0xff will be printed as "ff".
        // Value 0xfff will be printed as "0fff". Value 0xffff will be printed as "ffff" and so on.
        // So this method needs to pad only from the next whole byte up.
        // In other words: This method always pads with full bytes (=even number of zeros), never with just a nibble.

        // Example 1: Value is 0xF and padToBytes is 2. This means the desired output is 000f.
        // longHexAppender() pads to a full byte. This means it will output is 0f. So this method needs to pad with 2 zeros.

        // Example 2: The value is 0xFF and padToBytes is 2. This means the desired output is 00ff.
        // longHexAppender() will output "ff". This is a full byte so longHexAppender() will not do any padding on its own.
        // So this method needs to pad with 2 zeros.
        int leadingZeroBits = Long.numberOfLeadingZeros(value);
        int padToBits = padToBytes << 3;
        int bitsToPad = padToBits - (Long.SIZE - leadingZeroBits);
        int bytesToPad = (bitsToPad >> 3);
        for (int i = 0; i < bytesToPad; i++) {
            sink.putAscii('0');
            sink.putAscii('0');
        }
        if (value == 0) {
            return;
        }
        int bit = 64 - leadingZeroBits;
        longHexAppender[bit].append(sink, value);
    }

    public static void appendLong256(long a, long b, long c, long d, CharSink<?> sink) {
        if (a == Numbers.LONG_NULL && b == Numbers.LONG_NULL && c == Numbers.LONG_NULL && d == Numbers.LONG_NULL) {
            return;
        }
        sink.putAscii("0x");
        if (d != 0) {
            appendLong256Four(a, b, c, d, sink);
            return;
        }
        if (c != 0) {
            appendLong256Three(a, b, c, sink);
            return;
        }
        if (b != 0) {
            appendLong256Two(a, b, sink);
            return;
        }
        appendHex(sink, a, false);
    }

    public static void appendUuid(long lo, long hi, CharSink<?> sink) {
        appendHexPadded(sink, (hi >> 32) & 0xFFFFFFFFL, 4);
        sink.putAscii('-');
        appendHexPadded(sink, (hi >> 16) & 0xFFFF, 2);
        sink.putAscii('-');
        appendHexPadded(sink, hi & 0xFFFF, 2);
        sink.putAscii('-');
        appendHexPadded(sink, lo >> 48 & 0xFFFF, 2);
        sink.putAscii('-');
        appendHexPadded(sink, lo & 0xFFFFFFFFFFFFL, 6);
    }

    public static int ceilPow2(int value) {
        int i = value;
        if ((i != 0) && (i & (i - 1)) > 0) {
            i |= (i >>> 1);
            i |= (i >>> 2);
            i |= (i >>> 4);
            i |= (i >>> 8);
            i |= (i >>> 16);
            i++;

            if (i < 0) {
                i >>>= 1;
            }
        }

        return i;
    }

    public static int decodeHighInt(long val) {
        return (int) (val >> 32);
    }

    public static int decodeLowInt(long val) {
        return (int) (val & 0xffffffffL);
    }

    public static long encodeLowHighInts(int low, int high) {
        return ((Integer.toUnsignedLong(high)) << 32L) | Integer.toUnsignedLong(low);
    }

    public static int hexToDecimal(int c) throws NumericException {
        if (c > 127) {
            throw NumericException.instance().put("invalid hex character code: ").put(c);
        }
        int r = hexNumbers[c];
        if (r == -1) {
            throw NumericException.instance().put("invalid hex character: '").put((char) c).put('\'');
        }
        return r;
    }

    public static void intToIPv4Sink(CharSink<?> sink, int value) {
        // NULL handling should be done outside, null here will be printed as 0.0.0.0
        append(sink, (value >> 24) & 0xff);
        sink.putAscii('.');
        append(sink, (value >> 16) & 0xff);
        sink.putAscii('.');
        append(sink, (value >> 8) & 0xff);
        sink.putAscii('.');
        append(sink, value & 0xff);
    }

    /**
     * Checks double value for NULL in database sense. NULL is anything that is
     * not "finite". In this sense the return value is directly opposite of
     * the return value of {@link #isFinite(double)}
     *
     * @param value to check
     * @return true if value is "infinite", which includes {@link Double#isNaN(double)}, positive and negative
     * infinities that arise from division by 0.
     */
    public static boolean isNull(double value) {
        return (Double.doubleToRawLongBits(value) & EXP_BIT_MASK) == EXP_BIT_MASK;
    }

    public static boolean isNull(float value) {
        return Float.isNaN(value) || Float.isInfinite(value);
    }

    public static boolean isPow2(int value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    public static int msb(int value) {
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    public static boolean notDigit(char c) {
        return c < '0' || c > '9';
    }

    public static double parseDouble(CharSequence sequence) throws NumericException {
        return FastDoubleParser.parseDouble(sequence, true);
    }

    public static float parseFloat(CharSequence sequence) throws NumericException {
        return FastFloatParser.parseFloat(sequence, true);
    }

    public static int parseHexInt(CharSequence sequence) throws NumericException {
        return parseHexInt(sequence, 0, sequence.length());
    }

    public static int parseHexInt(CharSequence sequence, int lo, int hi) throws NumericException {
        if (hi == 0) {
            throw NumericException.instance().put("empty hex string");
        }

        int val = 0;
        int r;
        for (int i = lo; i < hi; i++) {
            int c = sequence.charAt(i);
            int n = val << 4;
            r = n + hexToDecimal(c);
            val = r;
        }
        return val;
    }

    public static long parseHexLong(CharSequence sequence) throws NumericException {
        return parseHexLong(sequence, 0, sequence.length());
    }

    public static long parseHexLong(CharSequence sequence, int lo, int hi) throws NumericException {
        if (hi == 0) {
            throw NumericException.instance().put("empty hex string");
        }

        long val = 0;
        long r;
        for (int i = lo; i < hi; i++) {
            int c = sequence.charAt(i);
            long n = val << 4;
            r = n + hexToDecimal(c);
            val = r;
        }
        return val;
    }

    public static int parseIPv4(CharSequence sequence) throws NumericException {
        if (sequence == null || Chars.equalsIgnoreCase("null", sequence)) {
            return IPv4_NULL;
        }
        return parseIPv4_0(sequence, 0, sequence.length());
    }

    public static int parseIPv4Quiet(CharSequence sequence) {
        try {
            if (sequence == null || Chars.equals("null", sequence)) {
                return IPv4_NULL;
            }
            return parseIPv4(sequence);
        } catch (NumericException e) {
            return IPv4_NULL;
        }
    }

    public static int parseIPv4_0(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == 0) {
            throw NumericException.instance().put("empty IPv4 address string");
        }

        int hi;
        int lo = p;
        int num;
        int ipv4 = 0;
        int count = 0;

        final char sign = sequence.charAt(lo);

        // removes any leading dots
        if (notDigit(sign)) {
            if (sign == '.') {
                do {
                    lo++;
                } while (sequence.charAt(lo) == '.');
            } else {
                throw NumericException.instance().put("invalid IPv4 address: ").put(sequence);
            }
        }

        while ((hi = Chars.indexOf(sequence, lo, '.')) > -1 && count < 3) {
            num = parseInt(sequence, lo, hi);
            if (num > 255) {
                throw NumericException.instance().put("IPv4 octet out of range [0-255]: ").put(num);
            }
            ipv4 = (ipv4 << 8) | num;
            count++;
            lo = hi + 1;
        }

        if (count != 3) {
            throw NumericException.instance().put("IPv4 address must have 4 octets, found: ").put(count + 1);
        }

        // removes any trailing dots
        if ((hi = Chars.indexOf(sequence, lo, '.')) > -1) {
            num = parseInt(sequence, lo, hi);
            hi++;
            while (hi < lim) {
                if (sequence.charAt(hi) == '.') {
                    hi++;
                } else {
                    throw NumericException.instance().put("invalid character in IPv4 address: ").put(sequence);
                }
            }
        } else {
            num = parseInt(sequence, lo, lim);
        }

        if (num > 255) {
            throw NumericException.instance().put("IPv4 octet out of range [0-255]: ").put(num);
        }

        return (ipv4 << 8) | num;
    }

    public static int parseInt(CharSequence sequence) throws NumericException {
        if (sequence == null) {
            throw NumericException.instance().put("null string");
        }

        return parseInt0(sequence, 0, sequence.length());
    }

    public static int parseInt(CharSequence sequence, int p, int lim) throws NumericException {
        if (sequence == null) {
            throw NumericException.instance().put("null string");
        }
        return parseInt0(sequence, p, lim);
    }

    public static long parseInt000Greedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance().put("empty number string");
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || notDigit(sequence.charAt(i))) {
            throw NumericException.instance().put("not a number: ").put(sequence);
        }

        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);

            if (notDigit(c)) {
                break;
            }

            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.instance().put("number overflow");
            }
            val = r;
        }

        final int len = i - p;

        if (len > 3 || val == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance().put("number overflow");
        }

        while (i - p < 3) {
            val *= 10;
            i++;
        }

        return encodeLowHighInts(negative ? val : -val, len);
    }

    public static int parseIntQuiet(CharSequence sequence) {
        try {
            if (sequence == null || Chars.equals("NaN", sequence)) {
                return Numbers.INT_NULL;
            }
            return parseInt0(sequence, 0, sequence.length());
        } catch (NumericException e) {
            return Numbers.INT_NULL;
        }

    }

    public static long parseIntSafely(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance().put("empty number string");
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || notDigit(sequence.charAt(i))) {
            throw NumericException.instance().put("not a number: ").put(sequence);
        }

        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);

            if (notDigit(c)) {
                break;
            }

            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.instance().put("number overflow");
            }
            val = r;
        }

        if (val == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance().put("number overflow");
        }

        return encodeLowHighInts(negative ? val : -val, i - p);
    }

    public static long parseLong(CharSequence sequence) throws NumericException {
        if (sequence == null) {
            throw NumericException.instance().put("null string");
        }
        return parseLong0(sequence, 0, sequence.length());
    }

    public static long parseLong(Utf8Sequence sequence) throws NumericException {
        if (sequence == null) {
            throw NumericException.instance().put("null string");
        }
        return parseLong0(sequence.asAsciiCharSequence(), 0, sequence.size());
    }

    public static long parseLong000000Greedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance().put("empty number string");
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || notDigit(sequence.charAt(i))) {
            throw NumericException.instance().put("not a number: ").put(sequence);
        }

        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);

            if (notDigit(c)) {
                break;
            }

            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.instance().put("number overflow");
            }
            val = r;
        }

        final int len = i - p;

        if (len > 6 || val == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance().put("number overflow");
        }

        while (i - p < 6) {
            val *= 10;
            i++;
        }

        return encodeLowHighInts(negative ? val : -val, len);
    }

    private static void appendDouble0(
            int binExp,
            long fractionBits,
            int significantBitCount,
            boolean negative,
            char[] digits,
            CharSink<?> out,
            int outScale
    ) {
        assert fractionBits > 0L;
        assert (fractionBits & FRACT_HOB) != 0L;

        final int tailZeroes = Long.numberOfTrailingZeros(fractionBits);
        final int fractBitCount = EXP_SHIFT + 1 - tailZeroes;
        int decExp;
        int firstDigitIndex;
        int nDigits;

        final int tinyBitCount = Math.max(0, fractBitCount - binExp - 1);
        if (binExp < MAX_SMALL_BIN_EXP + 1 && binExp > MIN_SMALL_BIN_EXP - 1 && tinyBitCount < LONG_5_POW.length && fractBitCount + N_5_BITS[tinyBitCount] < 64 && tinyBitCount == 0) {
            int insignificant;
            if (binExp > significantBitCount) {
                insignificant = insignificantDigitsForPow2(binExp - significantBitCount - 1);
            } else {
                insignificant = 0;
            }

            if (binExp >= EXP_SHIFT) {
                fractionBits <<= binExp - EXP_SHIFT;
            } else {
                fractionBits >>>= EXP_SHIFT - binExp;
            }

            //
            int binExp2 = 0;
            if (insignificant != 0) {
                long pow10 = LONG_5_POW[insignificant] << insignificant;
                long residue = fractionBits % pow10;
                fractionBits /= pow10;
                binExp2 += insignificant;
                if (residue >= pow10 >> 1) {
                    ++fractionBits;
                }
            }

            int digitIndex = digits.length - 1;
            int digit;
            if (fractionBits <= Integer.MAX_VALUE) {
                assert fractionBits > 0L : fractionBits;

                int fractRemaining = (int) fractionBits;
                digit = fractRemaining % 10;

                for (fractRemaining /= 10; digit == 0; fractRemaining /= 10) {
                    ++binExp2;
                    digit = fractRemaining % 10;
                }

                while (fractRemaining != 0) {
                    digits[digitIndex--] = (char) (digit + '0');
                    ++binExp2;
                    digit = fractRemaining % 10;
                    fractRemaining /= 10;
                }

            } else {
                digit = (int) (fractionBits % 10L);

                for (fractionBits /= 10L; digit == 0; fractionBits /= 10L) {
                    ++binExp2;
                    digit = (int) (fractionBits % 10L);
                }

                while (fractionBits != 0L) {
                    digits[digitIndex--] = (char) (digit + '0');
                    ++binExp2;
                    digit = (int) (fractionBits % 10L);
                    fractionBits /= 10L;
                }

            }
            digits[digitIndex] = (char) (digit + '0');

            decExp = binExp2 + 1;
            firstDigitIndex = digitIndex;
            nDigits = digits.length - digitIndex;
        } else {
            int estDecExp = estimateDecExpDouble(fractionBits, binExp);
            int B5 = Math.max(0, -estDecExp);
            int B2 = B5 + tinyBitCount + binExp;
            int S5 = Math.max(0, estDecExp);
            int S2 = S5 + tinyBitCount;
            int M2 = B2 - significantBitCount;
            fractionBits >>>= tailZeroes;
            B2 -= fractBitCount - 1;
            int common2factor = Math.min(B2, S2);
            B2 -= common2factor;
            S2 -= common2factor;
            M2 -= common2factor;
            if (fractBitCount == 1) {
                --M2;
            }

            if (M2 < 0) {
                B2 -= M2;
                S2 -= M2;
                M2 = 0;
            }

            int bBits = fractBitCount + B2 + (B5 < N_5_BITS.length ? N_5_BITS[B5] : B5 * 3);
            int tenBits = S2 + 1 + (S5 + 1 < N_5_BITS.length ? N_5_BITS[S5 + 1] : (S5 + 1) * 3);
            boolean low;
            boolean high;
            long lowDigitDifference;
            int q;
            int digitIndex;
            if (bBits < 64 && tenBits < 64) {
                if (bBits < 32 && tenBits < 32) {
                    int b = (int) fractionBits * SMALL_5_POW[B5] << B2;
                    int s = SMALL_5_POW[S5] << S2;
                    int m = SMALL_5_POW[B5] << M2;
                    int tens = s * 10;
                    digitIndex = 0;
                    q = b / s;
                    b = 10 * (b % s);
                    m *= 10;
                    low = b < m;
                    high = b + m > tens;

                    assert q < 10 : q;

                    if (q == 0 && !high) {
                        --estDecExp;
                    } else {
                        digits[digitIndex++] = (char) ('0' + q);
                    }

                    if (estDecExp < -3 || estDecExp >= 8) {
                        low = false;
                        high = false;
                    }

                    for (; !low && !high; digits[digitIndex++] = (char) ('0' + q)) {
                        q = b / s;
                        b = 10 * (b % s);
                        m *= 10;

                        assert q < 10 : q;

                        if ((long) m > 0L) {
                            low = b < m;
                            high = b + m > tens;
                        } else {
                            low = true;
                            high = true;
                        }
                    }

                    lowDigitDifference = ((long) b << 1) - tens;
                } else {
                    long b = fractionBits * LONG_5_POW[B5] << B2;
                    long s = LONG_5_POW[S5] << S2;
                    long m = LONG_5_POW[B5] << M2;
                    long tens = s * 10L;
                    digitIndex = 0;
                    q = (int) (b / s);
                    b = 10L * (b % s);
                    m *= 10L;
                    low = b < m;
                    high = b + m > tens;

                    assert q < 10 : q;

                    if (q == 0 && !high) {
                        --estDecExp;
                    } else {
                        digits[digitIndex++] = (char) ('0' + q);
                    }

                    if (estDecExp < -3 || estDecExp >= 8) {
                        low = false;
                        high = false;
                    }

                    for (; !low && !high; digits[digitIndex++] = (char) ('0' + q)) {
                        q = (int) (b / s);
                        b = 10L * (b % s);
                        m *= 10L;

                        assert q < 10 : q;

                        if (m > 0L) {
                            low = b < m;
                            high = b + m > tens;
                        } else {
                            low = true;
                            high = true;
                        }
                    }
                    lowDigitDifference = (b << 1) - tens;
                }
            } else {
                FDBigInteger sVal = FDBigInteger.valueOfPow52(S5, S2);
                final int shiftBias = sVal.getNormalizationBias();
                sVal = sVal.leftShift(shiftBias);
                FDBigInteger bVal = FDBigInteger.valueOfMulPow52(fractionBits, B5, B2 + shiftBias);
                FDBigInteger mVal = FDBigInteger.valueOfPow52(B5 + 1, M2 + shiftBias + 1);
                FDBigInteger tensVal = FDBigInteger.valueOfPow52(S5 + 1, S2 + shiftBias + 1);
                digitIndex = 0;
                q = bVal.quoRemIteration(sVal);
                low = bVal.cmp(mVal) < 0;
                high = tensVal.addAndCmp(bVal, mVal) <= 0;

                assert q < 10 : q;

                if (q == 0 && !high) {
                    --estDecExp;
                } else {
                    digits[digitIndex++] = (char) ('0' + q);
                }

                if (estDecExp < -3 || estDecExp >= 8) {
                    low = false;
                    high = false;
                }

                while (!low && !high) {
                    q = bVal.quoRemIteration(sVal);

                    assert q < 10 : q;

                    mVal = mVal.multBy10();
                    low = bVal.cmp(mVal) < 0;
                    high = tensVal.addAndCmp(bVal, mVal) <= 0;
                    digits[digitIndex++] = (char) ('0' + q);
                }

                if (high && low) {
                    bVal = bVal.leftShift(1);
                    lowDigitDifference = bVal.cmp(tensVal);
                } else {
                    lowDigitDifference = 0L;
                }
            }

            decExp = estDecExp + 1;
            firstDigitIndex = 0;
            nDigits = digitIndex;
            if (high) {
                if (low) {
                    if (lowDigitDifference == 0L) {
                        if ((digits[firstDigitIndex + nDigits - 1] & 1) != 0) {
                            if (roundupDouble(firstDigitIndex, digits, nDigits)) {
                                decExp++;
                            }
                        }
                    } else if (lowDigitDifference > 0L) {
                        if (roundupDouble(firstDigitIndex, digits, nDigits)) {
                            decExp++;
                        }
                    }
                } else {
                    if (roundupDouble(firstDigitIndex, digits, nDigits)) {
                        decExp++;
                    }
                }
            }
        }

        appendDouble00(digits, firstDigitIndex, nDigits, negative, decExp, out, outScale);
    }

    private static void appendDouble00(
            char[] digits,
            int firstDigitIndex,
            int nDigits,
            boolean isNegative,
            int decExp,
            CharSink<?> sink,
            int outScale
    ) {
        assert nDigits <= MAX_DOUBLE_SCALE : nDigits;
        if (isNegative) {
            sink.putAscii('-');
        }

        int exp;
        if (decExp > 0 && decExp < 8) {
            exp = Math.min(nDigits, decExp);
            sink.putAscii(digits, firstDigitIndex, exp);
            if (exp < decExp) {
                exp = decExp - exp;
                sink.fillAscii('0', exp);
                sink.putAscii('.');
                sink.putAscii('0');
            } else {
                sink.putAscii('.');
                if (exp < nDigits) {
                    sink.putAscii(digits, firstDigitIndex + exp, Math.min(nDigits - exp, outScale));
                } else {
                    sink.putAscii('0');
                }
            }
        } else if (decExp <= 0 && decExp > -3) {
            sink.putAscii('0').putAscii('.');
            if (decExp != 0) {
                sink.fillAscii('0', -decExp);
            }

            sink.putAscii(digits, firstDigitIndex, Math.min(nDigits, outScale));
        } else {
            sink.putAscii(digits[firstDigitIndex]);
            sink.putAscii('.');
            if (nDigits > 1) {
                sink.putAscii(digits, firstDigitIndex + 1, nDigits - 1);
            } else {
                sink.putAscii('0');
            }

            sink.putAscii('E');
            if (decExp <= 0) {
                sink.putAscii('-');
                exp = -decExp + 1;
            } else {
                exp = decExp - 1;
            }

            if (exp < 10) {
                sink.putAscii((char) (exp + '0'));
            } else if (exp < 100) {
                sink.putAscii((char) (exp / 10 + '0'));
                sink.putAscii((char) (exp % 10 + '0'));
            } else {
                sink.putAscii((char) (exp / 100 + '0'));
                exp %= 100;
                sink.putAscii((char) (exp / 10 + '0'));
                sink.putAscii((char) (exp % 10 + '0'));
            }
        }
    }

    private static void appendInt10(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 1000000000));
        sink.putAscii((char) ('0' + (c = i % 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt2(CharSink<?> sink, int i) {
        sink.putAscii((char) ('0' + i / 10));
        sink.putAscii((char) ('0' + i % 10));
    }

    private static void appendInt3(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 100));
        sink.putAscii((char) ('0' + (c = i % 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt4(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 1000));
        sink.putAscii((char) ('0' + (c = i % 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt5(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 10000));
        sink.putAscii((char) ('0' + (c = i % 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt6(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 100000));
        sink.putAscii((char) ('0' + (c = i % 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt7(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 1000000));
        sink.putAscii((char) ('0' + (c = i % 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt8(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 10000000));
        sink.putAscii((char) ('0' + (c = i % 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendInt9(CharSink<?> sink, int i) {
        int c;
        sink.putAscii((char) ('0' + i / 100000000));
        sink.putAscii((char) ('0' + (c = i % 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong10(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 1000000000L));
        sink.putAscii((char) ('0' + (c = i % 1000000000L) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong11(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 10000000000L));
        sink.putAscii((char) ('0' + (c = i % 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong12(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 100000000000L));
        sink.putAscii((char) ('0' + (c = i % 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong13(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 1000000000000L));
        sink.putAscii((char) ('0' + (c = i % 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong14(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 10000000000000L));
        sink.putAscii((char) ('0' + (c = i % 10000000000000L) / 1000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong15(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 100000000000000L));
        sink.putAscii((char) ('0' + (c = i % 100000000000000L) / 10000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong16(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 1000000000000000L));
        sink.putAscii((char) ('0' + (c = i % 1000000000000000L) / 100000000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong17(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 10000000000000000L));
        sink.putAscii((char) ('0' + (c = i % 10000000000000000L) / 1000000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong18(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 100000000000000000L));
        sink.putAscii((char) ('0' + (c = i % 100000000000000000L) / 10000000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000000L) / 1000000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong19(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 1000000000000000000L));
        sink.putAscii((char) ('0' + (c = i % 1000000000000000000L) / 100000000000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000000000L) / 10000000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000000L) / 1000000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000000L) / 100000000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000000L) / 10000000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000000L) / 1000000000000L));
        sink.putAscii((char) ('0' + (c %= 1000000000000L) / 100000000000L));
        sink.putAscii((char) ('0' + (c %= 100000000000L) / 10000000000L));
        sink.putAscii((char) ('0' + (c %= 10000000000L) / 1000000000));
        sink.putAscii((char) ('0' + (c %= 1000000000) / 100000000));
        sink.putAscii((char) ('0' + (c %= 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong2(CharSink<?> sink, long i) {
        sink.putAscii((char) ('0' + i / 10));
        sink.putAscii((char) ('0' + i % 10));
    }

    private static void appendLong256Four(long a, long b, long c, long d, CharSink<?> sink) {
        appendLong256Three(b, c, d, sink);
        appendHex(sink, a, true);
    }

    private static void appendLong256Three(long a, long b, long c, CharSink<?> sink) {
        appendLong256Two(b, c, sink);
        appendHex(sink, a, true);
    }

    private static void appendLong256Two(long a, long b, CharSink<?> sink) {
        appendHex(sink, b, false);
        appendHex(sink, a, true);
    }

    private static void appendLong3(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 100));
        sink.putAscii((char) ('0' + (c = i % 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong4(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 1000));
        sink.putAscii((char) ('0' + (c = i % 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong5(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 10000));
        sink.putAscii((char) ('0' + (c = i % 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong6(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 100000));
        sink.putAscii((char) ('0' + (c = i % 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong7(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 1000000));
        sink.putAscii((char) ('0' + (c = i % 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong8(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 10000000));
        sink.putAscii((char) ('0' + (c = i % 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLong9(CharSink<?> sink, long i) {
        long c;
        sink.putAscii((char) ('0' + i / 100000000));
        sink.putAscii((char) ('0' + (c = i % 100000000) / 10000000));
        sink.putAscii((char) ('0' + (c %= 10000000) / 1000000));
        sink.putAscii((char) ('0' + (c %= 1000000) / 100000));
        sink.putAscii((char) ('0' + (c %= 100000) / 10000));
        sink.putAscii((char) ('0' + (c %= 10000) / 1000));
        sink.putAscii((char) ('0' + (c %= 1000) / 100));
        sink.putAscii((char) ('0' + (c %= 100) / 10));
        sink.putAscii((char) ('0' + (c % 10)));
    }

    private static void appendLongHex12(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 8) & 0xf)]);
        appendLongHex8(sink, value);
    }

    private static void appendLongHex12Pad64(CharSink<?> sink, long value) {
        sink.putAscii("000000000000");
        appendLongHex12(sink, value);
    }

    private static void appendLongHex16(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 12) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 8) & 0xf)]);
        appendLongHex8(sink, value);
    }

    private static void appendLongHex16Pad64(CharSink<?> sink, long value) {
        sink.putAscii("000000000000");
        appendLongHex16(sink, value);
    }

    private static void appendLongHex20(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 16) & 0xf)]);
        appendLongHex16(sink, value);
    }

    private static void appendLongHex20Pad64(CharSink<?> sink, long value) {
        sink.putAscii("0000000000");
        appendLongHex20(sink, value);
    }

    private static void appendLongHex24(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 20) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 16) & 0xf)]);
        appendLongHex16(sink, value);
    }

    private static void appendLongHex24Pad64(CharSink<?> sink, long value) {
        sink.putAscii("0000000000");
        appendLongHex24(sink, value);
    }

    private static void appendLongHex28(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 24) & 0xf)]);
        appendLongHex24(sink, value);
    }

    private static void appendLongHex28Pad64(CharSink<?> sink, long value) {
        sink.putAscii("00000000");
        appendLongHex28(sink, value);
    }

    private static void appendLongHex32(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 28) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 24) & 0xf)]);
        appendLongHex24(sink, value);
    }

    private static void appendLongHex32Pad64(CharSink<?> sink, long value) {
        sink.putAscii("00000000");
        appendLongHex32(sink, value);
    }

    private static void appendLongHex36(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 32) & 0xf)]);
        appendLongHex32(sink, value);
    }

    private static void appendLongHex36Pad64(CharSink<?> sink, long value) {
        sink.putAscii("000000");
        appendLongHex36(sink, value);
    }

    private static void appendLongHex4(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value) & 0xf)]);
    }

    private static void appendLongHex40(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 36) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 32) & 0xf)]);
        appendLongHex32(sink, value);
    }

    private static void appendLongHex40Pad64(CharSink<?> sink, long value) {
        sink.putAscii("000000");
        appendLongHex40(sink, value);
    }

    private static void appendLongHex44(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 40) & 0xf)]);
        appendLongHex40(sink, value);
    }

    private static void appendLongHex44Pad64(CharSink<?> sink, long value) {
        sink.putAscii("0000");
        appendLongHex44(sink, value);
    }

    private static void appendLongHex48(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 44) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 40) & 0xf)]);
        appendLongHex40(sink, value);
    }

    private static void appendLongHex48Pad64(CharSink<?> sink, long value) {
        sink.putAscii("0000");
        appendLongHex48(sink, value);
    }

    private static void appendLongHex4Pad64(CharSink<?> sink, long value) {
        sink.putAscii("00000000000000");
        appendLongHex4(sink, value);
    }

    private static void appendLongHex52(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 48) & 0xf)]);
        appendLongHex48(sink, value);
    }

    private static void appendLongHex52Pad64(CharSink<?> sink, long value) {
        sink.putAscii("00");
        appendLongHex52(sink, value);
    }

    private static void appendLongHex56(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 52) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 48) & 0xf)]);
        appendLongHex48(sink, value);
    }

    private static void appendLongHex56Pad64(CharSink<?> sink, long value) {
        sink.putAscii("00");
        appendLongHex56(sink, value);
    }

    private static void appendLongHex60(CharSink<?> sink, long value) {
        appendLongHexPad(sink, hexDigits[(int) ((value >> 56) & 0xf)]);
        appendLongHex56(sink, value);
    }

    private static void appendLongHex64(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 60) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value >> 56) & 0xf)]);
        appendLongHex56(sink, value);
    }

    private static void appendLongHex8(CharSink<?> sink, long value) {
        sink.putAscii(hexDigits[(int) ((value >> 4) & 0xf)]);
        sink.putAscii(hexDigits[(int) ((value) & 0xf)]);
    }

    private static void appendLongHex8Pad64(CharSink<?> sink, long value) {
        sink.putAscii("00000000000000");
        appendLongHex8(sink, value);
    }

    private static void appendLongHexPad(CharSink<?> sink, char hexDigit) {
        sink.putAscii('0');
        sink.putAscii(hexDigit);
    }

    private static int estimateDecExpDouble(long fractBits, int binExp) {
        double d2 = Double.longBitsToDouble(EXP_ONE | fractBits & SIGNIF_BIT_MASK);
        double d = (d2 - 1.5D) * 0.289529654D + 0.176091259D + (double) binExp * 0.301029995663981D;
        long dBits = Double.doubleToRawLongBits(d);
        int exponent = (int) ((dBits & EXP_BIT_MASK) >> EXP_SHIFT) - EXP_BIAS;
        final boolean isNegative = (dBits & SIGN_BIT_MASK) != 0L;
        if (exponent > -1 && exponent < 52) {
            final long mask = SIGNIF_BIT_MASK >> exponent;
            final int r = (int) ((dBits & SIGNIF_BIT_MASK | FRACT_HOB) >> EXP_SHIFT - exponent);
            return isNegative ? ((mask & dBits) == 0L ? -r : -r - 1) : r;
        } else if (exponent < 0) {
            return (dBits & ~SIGN_BIT_MASK) == 0L ? 0 : (isNegative ? -1 : 0);
        } else {
            return (int) d;
        }
    }

    private static int insignificantDigitsForPow2(int p2) {
        return p2 > 1 && p2 < insignificantDigitsNumber.length ? insignificantDigitsNumber[p2] : 0;
    }

    private static int parseInt0(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance().put("empty integer string");
        }

        final char sign = sequence.charAt(p);
        final boolean negative = sign == '-';
        int i = p;
        if (negative || sign == '+') {
            i++;
        }

        if (i >= lim) {
            throw NumericException.instance().put("empty integer string");
        }

        int digitCounter = 0;
        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);
            if (c == '_') {
                if (digitCounter == 0) {
                    throw NumericException.instance().put("invalid integer format: ").put(sequence, p, lim);
                }
                digitCounter = 0;
            } else if (c < '0' || c > '9') {
                throw NumericException.instance().put("invalid character in integer: ").put(sequence, p, lim);
            } else {
                // val * 10 + (c - '0')
                if (val < (Integer.MIN_VALUE / 10)) {
                    throw NumericException.instance().put("integer overflow: ").put(sequence, p, lim);
                }
                int r = (val << 3) + (val << 1) - (c - '0');
                if (r > val) {
                    throw NumericException.instance().put("integer overflow: ").put(sequence, p, lim);
                }
                val = r;
                digitCounter++;
            }
        }

        if ((val == Integer.MIN_VALUE && !negative) || digitCounter == 0) {
            throw NumericException.instance().put("invalid integer format: ").put(sequence, p, lim);
        }
        return negative ? val : -val;
    }

    private static long parseLong0(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance().put("empty long string");
        }

        boolean negative = sequence.charAt(p) == '-';

        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim) {
            throw NumericException.instance().put("empty long string");
        }

        int digitCounter = 0;
        long val = 0;
        for (; i < lim; i++) {
            int c = sequence.charAt(i);
            switch (c | 32) {
                case 'l':
                    if (i == 0 || i + 1 < lim) {
                        throw NumericException.instance().put("invalid long format: ").put(sequence, p, lim);
                    }
                    break;
                case 127: // '_'
                    if (digitCounter == 0) {
                        throw NumericException.instance().put("invalid long format: ").put(sequence, p, lim);
                    }
                    digitCounter = 0;
                    break;
                default:
                    if (c < '0' || c > '9') {
                        throw NumericException.instance().put("invalid character in long: ").put(sequence, p, lim);
                    }
                    // val * 10 + (c - '0')
                    long r = (val << 3) + (val << 1) - (c - '0');
                    if (r > val) {
                        throw NumericException.instance().put("long overflow: ").put(sequence, p, lim);
                    }
                    val = r;
                    digitCounter++;
            }
        }

        if ((val == Long.MIN_VALUE && !negative) || digitCounter == 0) {
            throw NumericException.instance().put("invalid long format: ").put(sequence, p, lim);
        }
        return negative ? val : -val;
    }

    private static boolean roundupDouble(int firstDigitIndex, char[] digits, int nDigits) {
        int charIndex = firstDigitIndex + nDigits - 1;
        char c = digits[charIndex];
        if (c == '9') {
            while (true) {
                if (c != '9' || charIndex <= firstDigitIndex) {
                    if (c == '9') {
                        digits[firstDigitIndex] = '1';
                        return true;
                    }
                    break;
                }

                digits[charIndex] = '0';
                --charIndex;
                c = digits[charIndex];
            }
        }

        digits[charIndex] = (char) (c + 1);
        return false;
    }

    @FunctionalInterface
    private interface LongHexAppender {
        void append(CharSink<?> sink, long value);
    }

    static {
        Module currentModule = Numbers.class.getModule();
        Unsafe.addExports(Unsafe.JAVA_BASE_MODULE, currentModule, "jdk.internal.math");
    }

    static {
        pow10 = new long[20];
        pow10max = 18;
        pow10[0] = 1;
        for (int i = 1; i < pow10.length; i++) {
            pow10[i] = pow10[i - 1] * 10;
        }

        hexNumbers = new int[128];
        Arrays.fill(hexNumbers, -1);
        hexNumbers['0'] = 0;
        hexNumbers['1'] = 1;
        hexNumbers['2'] = 2;
        hexNumbers['3'] = 3;
        hexNumbers['4'] = 4;
        hexNumbers['5'] = 5;
        hexNumbers['6'] = 6;
        hexNumbers['7'] = 7;
        hexNumbers['8'] = 8;
        hexNumbers['9'] = 9;
        hexNumbers['A'] = 10;
        hexNumbers['a'] = 10;
        hexNumbers['B'] = 11;
        hexNumbers['b'] = 11;
        hexNumbers['C'] = 12;
        hexNumbers['c'] = 12;
        hexNumbers['D'] = 13;
        hexNumbers['d'] = 13;
        hexNumbers['E'] = 14;
        hexNumbers['e'] = 14;
        hexNumbers['F'] = 15;
        hexNumbers['f'] = 15;
    }

    static {
        final LongHexAppender a4 = Numbers::appendLongHex4;
        longHexAppender[0] = a4;
        longHexAppender[1] = a4;
        longHexAppender[2] = a4;
        longHexAppender[3] = a4;
        longHexAppender[4] = a4;

        final LongHexAppender a8 = Numbers::appendLongHex8;
        longHexAppender[5] = a8;
        longHexAppender[6] = a8;
        longHexAppender[7] = a8;
        longHexAppender[8] = a8;

        LongHexAppender a12 = Numbers::appendLongHex12;
        longHexAppender[9] = a12;
        longHexAppender[10] = a12;
        longHexAppender[11] = a12;
        longHexAppender[12] = a12;

        LongHexAppender a16 = Numbers::appendLongHex16;
        longHexAppender[13] = a16;
        longHexAppender[14] = a16;
        longHexAppender[15] = a16;
        longHexAppender[16] = a16;

        LongHexAppender a20 = Numbers::appendLongHex20;
        longHexAppender[17] = a20;
        longHexAppender[18] = a20;
        longHexAppender[19] = a20;
        longHexAppender[20] = a20;

        LongHexAppender a24 = Numbers::appendLongHex24;
        longHexAppender[21] = a24;
        longHexAppender[22] = a24;
        longHexAppender[23] = a24;
        longHexAppender[24] = a24;

        LongHexAppender a28 = Numbers::appendLongHex28;
        longHexAppender[25] = a28;
        longHexAppender[26] = a28;
        longHexAppender[27] = a28;
        longHexAppender[28] = a28;

        LongHexAppender a32 = Numbers::appendLongHex32;
        longHexAppender[29] = a32;
        longHexAppender[30] = a32;
        longHexAppender[31] = a32;
        longHexAppender[32] = a32;

        LongHexAppender a36 = Numbers::appendLongHex36;
        longHexAppender[33] = a36;
        longHexAppender[34] = a36;
        longHexAppender[35] = a36;
        longHexAppender[36] = a36;

        LongHexAppender a40 = Numbers::appendLongHex40;
        longHexAppender[37] = a40;
        longHexAppender[38] = a40;
        longHexAppender[39] = a40;
        longHexAppender[40] = a40;

        LongHexAppender a44 = Numbers::appendLongHex44;
        longHexAppender[41] = a44;
        longHexAppender[42] = a44;
        longHexAppender[43] = a44;
        longHexAppender[44] = a44;

        LongHexAppender a48 = Numbers::appendLongHex48;
        longHexAppender[45] = a48;
        longHexAppender[46] = a48;
        longHexAppender[47] = a48;
        longHexAppender[48] = a48;

        LongHexAppender a52 = Numbers::appendLongHex52;
        longHexAppender[49] = a52;
        longHexAppender[50] = a52;
        longHexAppender[51] = a52;
        longHexAppender[52] = a52;

        LongHexAppender a56 = Numbers::appendLongHex56;
        longHexAppender[53] = a56;
        longHexAppender[54] = a56;
        longHexAppender[55] = a56;
        longHexAppender[56] = a56;

        LongHexAppender a60 = Numbers::appendLongHex60;
        longHexAppender[57] = a60;
        longHexAppender[58] = a60;
        longHexAppender[59] = a60;
        longHexAppender[60] = a60;

        LongHexAppender a64 = Numbers::appendLongHex64;
        longHexAppender[61] = a64;
        longHexAppender[62] = a64;
        longHexAppender[63] = a64;
        longHexAppender[64] = a64;
    }

    static {
        final LongHexAppender a4 = Numbers::appendLongHex4Pad64;
        longHexAppenderPad64[0] = a4;
        longHexAppenderPad64[1] = a4;
        longHexAppenderPad64[2] = a4;
        longHexAppenderPad64[3] = a4;
        longHexAppenderPad64[4] = a4;

        final LongHexAppender a8 = Numbers::appendLongHex8Pad64;
        longHexAppenderPad64[5] = a8;
        longHexAppenderPad64[6] = a8;
        longHexAppenderPad64[7] = a8;
        longHexAppenderPad64[8] = a8;

        LongHexAppender a12 = Numbers::appendLongHex12Pad64;
        longHexAppenderPad64[9] = a12;
        longHexAppenderPad64[10] = a12;
        longHexAppenderPad64[11] = a12;
        longHexAppenderPad64[12] = a12;

        LongHexAppender a16 = Numbers::appendLongHex16Pad64;
        longHexAppenderPad64[13] = a16;
        longHexAppenderPad64[14] = a16;
        longHexAppenderPad64[15] = a16;
        longHexAppenderPad64[16] = a16;

        LongHexAppender a20 = Numbers::appendLongHex20Pad64;
        longHexAppenderPad64[17] = a20;
        longHexAppenderPad64[18] = a20;
        longHexAppenderPad64[19] = a20;
        longHexAppenderPad64[20] = a20;

        LongHexAppender a24 = Numbers::appendLongHex24Pad64;
        longHexAppenderPad64[21] = a24;
        longHexAppenderPad64[22] = a24;
        longHexAppenderPad64[23] = a24;
        longHexAppenderPad64[24] = a24;

        LongHexAppender a28 = Numbers::appendLongHex28Pad64;
        longHexAppenderPad64[25] = a28;
        longHexAppenderPad64[26] = a28;
        longHexAppenderPad64[27] = a28;
        longHexAppenderPad64[28] = a28;

        LongHexAppender a32 = Numbers::appendLongHex32Pad64;
        longHexAppenderPad64[29] = a32;
        longHexAppenderPad64[30] = a32;
        longHexAppenderPad64[31] = a32;
        longHexAppenderPad64[32] = a32;

        LongHexAppender a36 = Numbers::appendLongHex36Pad64;
        longHexAppenderPad64[33] = a36;
        longHexAppenderPad64[34] = a36;
        longHexAppenderPad64[35] = a36;
        longHexAppenderPad64[36] = a36;

        LongHexAppender a40 = Numbers::appendLongHex40Pad64;
        longHexAppenderPad64[37] = a40;
        longHexAppenderPad64[38] = a40;
        longHexAppenderPad64[39] = a40;
        longHexAppenderPad64[40] = a40;

        LongHexAppender a44 = Numbers::appendLongHex44Pad64;
        longHexAppenderPad64[41] = a44;
        longHexAppenderPad64[42] = a44;
        longHexAppenderPad64[43] = a44;
        longHexAppenderPad64[44] = a44;

        LongHexAppender a48 = Numbers::appendLongHex48Pad64;
        longHexAppenderPad64[45] = a48;
        longHexAppenderPad64[46] = a48;
        longHexAppenderPad64[47] = a48;
        longHexAppenderPad64[48] = a48;

        LongHexAppender a52 = Numbers::appendLongHex52Pad64;
        longHexAppenderPad64[49] = a52;
        longHexAppenderPad64[50] = a52;
        longHexAppenderPad64[51] = a52;
        longHexAppenderPad64[52] = a52;

        LongHexAppender a56 = Numbers::appendLongHex56Pad64;
        longHexAppenderPad64[53] = a56;
        longHexAppenderPad64[54] = a56;
        longHexAppenderPad64[55] = a56;
        longHexAppenderPad64[56] = a56;

        LongHexAppender a60 = Numbers::appendLongHex60;
        longHexAppenderPad64[57] = a60;
        longHexAppenderPad64[58] = a60;
        longHexAppenderPad64[59] = a60;
        longHexAppenderPad64[60] = a60;

        LongHexAppender a64 = Numbers::appendLongHex64;
        longHexAppenderPad64[61] = a64;
        longHexAppenderPad64[62] = a64;
        longHexAppenderPad64[63] = a64;
        longHexAppenderPad64[64] = a64;
    }
}