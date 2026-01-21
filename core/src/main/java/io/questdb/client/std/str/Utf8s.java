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

package io.questdb.client.std.str;

import io.questdb.client.cairo.CairoException;
import io.questdb.client.std.Chars;
import io.questdb.client.std.ThreadLocal;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.client.std.Misc.getThreadLocalUtf8Sink;

/**
 * UTF-8 specific variant of the {@link Chars} utility.
 */
public final class Utf8s {
    // We store a prefix of this many bytes in auxiliary memory when the value is too large to inline.
    public static final int VARCHAR_INLINED_PREFIX_BYTES = 6;
    public static final long VARCHAR_INLINED_PREFIX_MASK = (1L << 8 * VARCHAR_INLINED_PREFIX_BYTES) - 1L;

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private Utf8s() {
    }

    public static int encodeUtf16Char(@NotNull Utf8Sink sink, @NotNull CharSequence cs, int hi, int i, char c) {
        if (c < 2048) {
            sink.put((byte) (192 | c >> 6));
            sink.put((byte) (128 | c & 63));
        } else if (Character.isSurrogate(c)) {
            i = encodeUtf16Surrogate(sink, c, cs, i, hi);
        } else {
            sink.put((byte) (224 | c >> 12));
            sink.put((byte) (128 | c >> 6 & 63));
            sink.put((byte) (128 | c & 63));
        }
        return i;
    }

    public static boolean equals(@Nullable Utf8Sequence l, @Nullable Utf8Sequence r) {
        if (l == null && r == null) {
            return true;
        }
        if (l == null || r == null) {
            return false;
        }
        final int lSize = l.size();
        return lSize == r.size()
                && l.zeroPaddedSixPrefix() == r.zeroPaddedSixPrefix()
                && dataEquals(l, r, VARCHAR_INLINED_PREFIX_BYTES, lSize);
    }

    public static boolean equalsAscii(@NotNull CharSequence asciiSeq, @NotNull Utf8Sequence seq) {
        int len;
        if ((len = asciiSeq.length()) != seq.size()) {
            return false;
        }
        for (int index = 0; index < len; index++) {
            if (asciiSeq.charAt(index) != seq.byteAt(index)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsAscii(@NotNull CharSequence asciiSeq, long rLo, long rHi) {
        int rLen = (int) (rHi - rLo);
        if (rLen != asciiSeq.length()) {
            return false;
        }
        for (int i = 0; i < rLen; i++) {
            if (asciiSeq.charAt(i) != (char) Unsafe.getUnsafe().getByte(rLo + i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsIgnoreCaseAscii(@NotNull Utf8Sequence lSeq, @NotNull Utf8Sequence rSeq) {
        int size = lSeq.size();
        if (size != rSeq.size()) {
            return false;
        }
        for (int index = 0; index < size; index++) {
            if (toLowerCaseAscii(lSeq.byteAt(index)) != toLowerCaseAscii(rSeq.byteAt(index))) {
                return false;
            }
        }
        return true;
    }

    public static boolean equalsNcAscii(@NotNull CharSequence asciiSeq, @Nullable Utf8Sequence seq) {
        return seq != null && equalsAscii(asciiSeq, seq);
    }

    public static int getUtf8Codepoint(int b1, int b2, int b3, int b4) {
        return b1 << 18 ^ b2 << 12 ^ b3 << 6 ^ b4 ^ 3678080;
    }

    public static int hashCode(@NotNull Utf8Sequence value) {
        int size = value.size();
        if (size == 0) {
            return 0;
        }
        int h = 0;
        for (int p = 0; p < size; p++) {
            h = 31 * h + value.byteAt(p);
        }
        return h;
    }

    public static int lowerCaseAsciiHashCode(@NotNull Utf8Sequence value) {
        int size = value.size();
        if (size == 0) {
            return 0;
        }
        int h = 0;
        for (int p = 0; p < size; p++) {
            h = 31 * h + toLowerCaseAscii(value.byteAt(p));
        }
        return h;
    }

    public static void putSafe(long lo, long hi, @NotNull Utf8Sink sink) {
        long p = lo;
        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = putMultibyteSafe(p, hi, b, sink);
                p += n;
            } else {
                char c = (char) b;
                if (!Character.isISOControl(c)) {
                    sink.put(c);
                } else {
                    putNonAsciiAsHex(sink, b);
                }
                ++p;
            }
        }
    }

    public static void strCpy(@NotNull Utf8Sequence src, int destLen, long destAddr) {
        for (int i = 0; i < destLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, src.byteAt(i));
        }
    }

    public static void strCpy(long srcLo, long srcHi, @NotNull Utf8Sink dest) {
        for (long i = srcLo; i < srcHi; i++) {
            dest.putAny(Unsafe.getUnsafe().getByte(i));
        }
    }

    public static void strCpyAscii(char @NotNull [] srcChars, int srcLo, int srcLen, long destAddr) {
        for (int i = 0; i < srcLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, (byte) srcChars[i + srcLo]);
        }
    }

    public static void strCpyAscii(@NotNull CharSequence asciiSrc, int srcLen, long destAddr) {
        strCpyAscii(asciiSrc, 0, srcLen, destAddr);
    }

    public static void strCpyAscii(@NotNull CharSequence asciiSrc, int srcLo, int srcLen, long destAddr) {
        for (int i = 0; i < srcLen; i++) {
            Unsafe.getUnsafe().putByte(destAddr + i, (byte) asciiSrc.charAt(srcLo + i));
        }
    }

    public static String stringFromUtf8Bytes(long lo, long hi) {
        if (hi == lo) {
            return "";
        }
        Utf16Sink r = getThreadLocalSink();
        if (!utf8ToUtf16(lo, hi, r)) {
            Utf8StringSink sink = getThreadLocalUtf8Sink();
            CairoException ex = CairoException.nonCritical().put("cannot convert invalid UTF-8 sequence to UTF-16 [seq=");
            putSafe(lo, hi, sink);
            ex.put(sink).put(']');
            throw ex;
        }
        return r.toString();
    }

    public static String stringFromUtf8Bytes(@NotNull Utf8Sequence seq) {
        if (seq.size() == 0) {
            return "";
        }
        Utf16Sink b = getThreadLocalSink();
        if (!utf8ToUtf16(seq, b)) {
            if (seq instanceof DirectUtf8Sequence) {
                Utf8StringSink sink = getThreadLocalUtf8Sink();
                CairoException ex = CairoException.nonCritical().put("cannot convert invalid UTF-8 sequence to UTF-16 [seq=");
                putSafe(seq.ptr(), seq.ptr() + seq.size(), sink);
                ex.put(sink).put(']');
                throw ex;
            }
            throw CairoException.nonCritical().put("cannot convert invalid UTF-8 sequence to UTF-16 [seq=").put(seq).put(']');
        }
        return b.toString();
    }

    public static String stringFromUtf8BytesSafe(@NotNull Utf8Sequence seq) {
        if (seq.size() == 0) {
            return "";
        }
        Utf16Sink b = getThreadLocalSink();
        utf8ToUtf16(seq, b);
        return b.toString();
    }

    public static String toString(@Nullable Utf8Sequence s) {
        return s == null ? null : s.toString();
    }

    public static String toString(@NotNull Utf8Sequence us, int start, int end, byte unescapeAscii) {
        final Utf8Sink sink = getThreadLocalUtf8Sink();
        final int lastChar = end - 1;
        for (int i = start; i < end; i++) {
            byte b = us.byteAt(i);
            sink.putAny(b);
            if (b == unescapeAscii && i < lastChar && us.byteAt(i + 1) == unescapeAscii) {
                i++;
            }
        }
        return sink.toString();
    }

    public static int utf8DecodeMultiByte(long lo, long hi, byte b, Utf16Sink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return utf8Decode2Bytes(lo, hi, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3Bytes(lo, hi, b, sink);
        }
        return utf8Decode4Bytes(lo, hi, b, sink);
    }

    public static char utf8ToChar(byte b1, byte b2, byte b3) {
        return (char) (b1 << 12 ^ b2 << 6 ^ b3 ^ -123008);
    }

    /**
     * Decodes bytes between lo,hi addresses into sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(long lo, long hi, @NotNull Utf16Sink sink) {
        long p = lo;
        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = utf8DecodeMultiByte(p, hi, b, sink);
                if (n == -1) {
                    // UTF8 error
                    return false;
                }
                p += n;
            } else {
                sink.put((char) b);
                ++p;
            }
        }
        return true;
    }

    /**
     * Decodes bytes from the given UTF-8 sink into char sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @param seq   input sequence
     * @param seqLo character bytes start in input sequence
     * @param seqHi character bytes end in input sequence (exclusive)
     * @param sink  destination sink
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(@NotNull Utf8Sequence seq, int seqLo, int seqHi, @NotNull Utf16Sink sink) {
        int i = seqLo;
        while (i < seqHi) {
            byte b = seq.byteAt(i);
            if (b < 0) {
                int n = utf8DecodeMultiByte(seq, i, b, sink);
                if (n == -1) {
                    // UTF-8 error
                    return false;
                }
                i += n;
            } else {
                sink.put((char) b);
                ++i;
            }
        }
        return true;
    }

    /**
     * Decodes bytes from the given UTF-8 sink into char sink.
     * Note: operation might fail in the middle and leave sink in inconsistent state.
     *
     * @return true if input is proper UTF-8 and false otherwise.
     */
    public static boolean utf8ToUtf16(@NotNull Utf8Sequence seq, @NotNull Utf16Sink sink) {
        return utf8ToUtf16(seq, 0, seq.size(), sink);
    }

    public static int validateUtf8(@NotNull Utf8Sequence seq) {
        if (seq.isAscii()) {
            return seq.size();
        }
        int len = 0;
        for (int i = 0, hi = seq.size(); i < hi; ) {
            byte b = seq.byteAt(i);
            if (b < 0) {
                int n = validateUtf8MultiByte(seq, i, b);
                if (n == -1) {
                    // UTF-8 error
                    return -1;
                }
                i += n;
            } else {
                ++i;
            }
            ++len;
        }
        return len;
    }

    /**
     * Returns up to 6 initial bytes of the given UTF-8 sequence (less if it's shorter)
     * packed into a zero-padded long value, in little-endian order. This prefix is
     * stored inline in the auxiliary vector of a VARCHAR column, so asking for it is a
     * matter of optimized data access. This is not a general access method, it
     * shouldn't be called unless looking to optimize the access of the VARCHAR column.
     *
     * @param seq UTF8 sequence
     * @return up to 6 initial bytes
     */
    public static long zeroPaddedSixPrefix(@NotNull Utf8Sequence seq) {
        final int size = seq.size();
        if (size >= Long.BYTES) {
            return seq.longAt(0) & VARCHAR_INLINED_PREFIX_MASK;
        }
        final long limit = Math.min(size, VARCHAR_INLINED_PREFIX_BYTES);
        long result = 0;
        for (int i = 0; i < limit; i++) {
            result |= (seq.byteAt(i) & 0xffL) << (8 * i);
        }
        return result;
    }

    private static boolean dataEquals(@NotNull Utf8Sequence l, @NotNull Utf8Sequence r, int start, int limit) {
        int i = start;
        for (; i <= limit - Long.BYTES; i += Long.BYTES) {
            if (l.longAt(i) != r.longAt(i)) {
                return false;
            }
        }

        if (i <= limit - Integer.BYTES) {
            if (l.intAt(i) != r.intAt(i)) {
                return false;
            }
            i += Integer.BYTES;
        }

        if (i <= limit - Short.BYTES) {
            if (l.shortAt(i) != r.shortAt(i)) {
                return false;
            }
            i += Short.BYTES;
        }

        return i >= limit || l.byteAt(i) == r.byteAt(i);
    }

    private static int encodeUtf16Surrogate(@NotNull Utf8Sink sink, char c, @NotNull CharSequence in, int pos, int hi) {
        int dword;
        if (Character.isHighSurrogate(c)) {
            if (hi - pos < 1) {
                sink.putAscii('?');
                return pos;
            } else {
                char c2 = in.charAt(pos++);
                if (Character.isLowSurrogate(c2)) {
                    dword = Character.toCodePoint(c, c2);
                } else {
                    sink.putAscii('?');
                    return pos;
                }
            }
        } else if (Character.isLowSurrogate(c)) {
            sink.putAscii('?');
            return pos;
        } else {
            dword = c;
        }
        sink.put((byte) (240 | dword >> 18));
        sink.put((byte) (128 | dword >> 12 & 63));
        sink.put((byte) (128 | dword >> 6 & 63));
        sink.put((byte) (128 | dword & 63));
        return pos;
    }

    private static StringSink getThreadLocalSink() {
        StringSink b = tlSink.get();
        b.clear();
        return b;
    }

    private static boolean isMalformed3(int b1, int b2, int b3) {
        return b1 == -32 && (b2 & 224) == 128 || (b2 & 192) != 128 || (b3 & 192) != 128;
    }

    private static boolean isMalformed4(int b2, int b3, int b4) {
        return (b2 & 192) != 128 || (b3 & 192) != 128 || (b4 & 192) != 128;
    }

    private static boolean isNotContinuation(int b) {
        return (b & 192) != 128;
    }

    private static void put2BytesSafe(byte b1, @NotNull Utf8Sink sink, byte b2) {
        if (isNotContinuation(b2)) {
            putNonAsciiAsHex(sink, b1);
            putNonAsciiAsHex(sink, b2);
            return;
        }
        sink.put(b1);
        sink.put(b2);
    }

    private static int put3BytesSafe(byte b1, byte b2, byte b3, @NotNull Utf8Sink sink) {
        if (!isMalformed3(b1, b2, b3)) {
            char c = utf8ToChar(b1, b2, b3);
            if (!Character.isSurrogate(c)) {
                sink.put(b1);
                sink.put(b2);
                sink.put(b3);
            }
        } else {
            putNonAsciiAsHex(sink, b1);
            putNonAsciiAsHex(sink, b2);
            putNonAsciiAsHex(sink, b3);
        }
        return 3;
    }

    private static void put4ByteSafe(byte b1, byte b2, byte b3, byte b4, @NotNull Utf8Sink sink) {
        if (!isMalformed4(b2, b3, b4)) {
            final int codePoint = getUtf8Codepoint(b1, b2, b3, b4);
            if (Character.isSupplementaryCodePoint(codePoint)) {
                sink.put(Character.highSurrogate(codePoint));
                sink.put(Character.lowSurrogate(codePoint));
                return;
            }
        }
        putNonAsciiAsHex(sink, b1);
        putNonAsciiAsHex(sink, b2);
        putNonAsciiAsHex(sink, b3);
        putNonAsciiAsHex(sink, b4);
    }

    private static int putInvalidBytes(long lo, long hi, byte b, @NotNull Utf8Sink sink) {
        putNonAsciiAsHex(sink, b);
        int i = 1;
        for (; lo + i < hi; i++) {
            byte val = Unsafe.getUnsafe().getByte(lo + i);
            if (val >= 0) {
                i--;
                break;
            }
            putNonAsciiAsHex(sink, val);
        }
        return i + 1;
    }

    private static int putMultibyteSafe(long lo, long hi, byte b, @NotNull Utf8Sink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            return putUpTo2BytesSafe(lo, hi, b, sink);
        }
        if (b >> 4 == -2) {
            return putUpTo3BytesSafe(lo, hi, b, sink);
        }
        if (b >> 3 == -2) {
            return putUpTo4BytesSafe(lo, hi, b, sink);
        }
        return putInvalidBytes(lo, hi, b, sink);
    }

    private static void putNonAsciiAsHex(@NotNull Utf8Sink sink, byte b) {
        if (b >= ' ' && b < 127) {
            sink.putAny(b);
            return;
        }
        sink.putAny(((byte) '\\'));
        sink.putAny(((byte) 'x'));
        sink.put(HEX_CHARS[(b & 0xFF) >>> 4]);
        sink.put(HEX_CHARS[b & 0x0F]);
    }

    private static int putUpTo2BytesSafe(long lo, long hi, byte b1, @NotNull Utf8Sink sink) {
        if (hi - lo >= 2) {
            byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
            put2BytesSafe(b1, sink, b2);
            return 2;
        }
        putNonAsciiAsHex(sink, b1);
        return 1;
    }

    private static int putUpTo3BytesSafe(long lo, long hi, byte b1, @NotNull Utf8Sink sink) {
        if (hi - lo >= 3) {
            byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
            byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
            return put3BytesSafe(b1, b2, b3, sink);
        }
        putNonAsciiAsHex(sink, b1);
        if (hi - lo > 1) {
            putNonAsciiAsHex(sink, Unsafe.getUnsafe().getByte(lo + 1));
            return 2;
        }
        return 1;
    }

    private static int putUpTo4BytesSafe(long lo, long hi, byte b, @NotNull Utf8Sink sink) {
        if (hi - lo >= 4) {
            byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
            byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
            byte b4 = Unsafe.getUnsafe().getByte(lo + 3);
            put4ByteSafe(b, b2, b3, b4, sink);
            return 4;
        }
        putNonAsciiAsHex(sink, b);
        if (hi - lo > 1) {
            putNonAsciiAsHex(sink, Unsafe.getUnsafe().getByte(lo + 1));
            if (hi - lo > 2) {
                putNonAsciiAsHex(sink, Unsafe.getUnsafe().getByte(lo + 2));
                if (hi - lo > 3) {
                    putNonAsciiAsHex(sink, Unsafe.getUnsafe().getByte(lo + 3));
                    return 4;
                }
                return 3;
            }
            return 2;
        }
        return 1;
    }

    private static byte toLowerCaseAscii(byte b) {
        return b > 64 && b < 91 ? (byte) (b + 32) : b;
    }

    private static int utf8Decode2Bytes(@NotNull Utf8Sequence seq, int index, int b1, @NotNull Utf16Sink sink) {
        if (seq.size() - index < 2) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static int utf8Decode2Bytes(long lo, long hi, int b1, @NotNull Utf16Sink sink) {
        if (hi - lo < 2) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        sink.put((char) (b1 << 6 ^ b2 ^ 3968));
        return 2;
    }

    private static int utf8Decode3Byte0(byte b1, @NotNull Utf16Sink sink, byte b2, byte b3) {
        if (isMalformed3(b1, b2, b3)) {
            return -1;
        }
        char c = utf8ToChar(b1, b2, b3);
        if (Character.isSurrogate(c)) {
            return -1;
        }
        sink.put(c);
        return 3;
    }

    private static int utf8Decode3Bytes(long lo, long hi, byte b1, @NotNull Utf16Sink sink) {
        if (hi - lo < 3) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode3Bytes(@NotNull Utf8Sequence seq, int index, byte b1, @NotNull Utf16Sink sink) {
        if (seq.size() - index < 3) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        return utf8Decode3Byte0(b1, sink, b2, b3);
    }

    private static int utf8Decode4Bytes(long lo, long hi, int b, @NotNull Utf16Sink sink) {
        if (b >> 3 != -2 || hi - lo < 4) {
            return -1;
        }
        byte b2 = Unsafe.getUnsafe().getByte(lo + 1);
        byte b3 = Unsafe.getUnsafe().getByte(lo + 2);
        byte b4 = Unsafe.getUnsafe().getByte(lo + 3);
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes(@NotNull Utf8Sequence seq, int index, int b, @NotNull Utf16Sink sink) {
        if (b >> 3 != -2 || seq.size() - index < 4) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        byte b4 = seq.byteAt(index + 3);
        return utf8Decode4Bytes0(b, sink, b2, b3, b4);
    }

    private static int utf8Decode4Bytes0(int b, @NotNull Utf16Sink sink, byte b2, byte b3, byte b4) {
        if (isMalformed4(b2, b3, b4)) {
            return -1;
        }
        final int codePoint = getUtf8Codepoint(b, b2, b3, b4);
        if (Character.isSupplementaryCodePoint(codePoint)) {
            sink.put(Character.highSurrogate(codePoint));
            sink.put(Character.lowSurrogate(codePoint));
            return 4;
        }
        return -1;
    }

    private static int utf8DecodeMultiByte(Utf8Sequence seq, int index, byte b, @NotNull Utf16Sink sink) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            // we should allow 11000001, as it is a valid UTF8 byte?
            return utf8Decode2Bytes(seq, index, b, sink);
        }
        if (b >> 4 == -2) {
            return utf8Decode3Bytes(seq, index, b, sink);
        }
        return utf8Decode4Bytes(seq, index, b, sink);
    }

    private static int validateUtf8Decode2Bytes(@NotNull Utf8Sequence seq, int index) {
        if (seq.size() - index < 2) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        if (isNotContinuation(b2)) {
            return -1;
        }
        return 2;
    }

    private static int validateUtf8Decode3Bytes(@NotNull Utf8Sequence seq, int index, byte b1) {
        if (seq.size() - index < 3) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);

        if (isMalformed3(b1, b2, b3)) {
            return -1;
        }

        char c = utf8ToChar(b1, b2, b3);
        if (Character.isSurrogate(c)) {
            return -1;
        }
        return 3;
    }

    private static int validateUtf8Decode4Bytes(@NotNull Utf8Sequence seq, int index, int b) {
        if (b >> 3 != -2 || seq.size() - index < 4) {
            return -1;
        }
        byte b2 = seq.byteAt(index + 1);
        byte b3 = seq.byteAt(index + 2);
        byte b4 = seq.byteAt(index + 3);

        if (isMalformed4(b2, b3, b4)) {
            return -1;
        }
        final int codePoint = getUtf8Codepoint(b, b2, b3, b4);
        if (!Character.isSupplementaryCodePoint(codePoint)) {
            return -1;
        }
        return 4;
    }

    private static int validateUtf8MultiByte(Utf8Sequence seq, int index, byte b) {
        if (b >> 5 == -2 && (b & 30) != 0) {
            // we should allow 11000001, as it is a valid UTF8 byte?
            return validateUtf8Decode2Bytes(seq, index);
        }
        if (b >> 4 == -2) {
            return validateUtf8Decode3Bytes(seq, index, b);
        }
        return validateUtf8Decode4Bytes(seq, index, b);
    }
}