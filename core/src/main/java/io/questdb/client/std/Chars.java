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

package io.questdb.client.std;

import io.questdb.client.std.str.CharSink;
import io.questdb.client.std.str.Utf16Sink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

public final class Chars {
    static final String[] CHAR_STRINGS;
    static final char[] base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
    // inverted alphabets for base64 decoding could be just byte arrays. this would save 3 * 128 bytes per alphabet
    // but benchmarks show that int arrays make decoding faster.
    // it's probably because it does not require any type conversions in the hot decoding loop
    static final int[] base64Inverted = base64CreateInvertedAlphabet(base64);
    static final char[] base64Url = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();
    static final int[] base64UrlInverted = base64CreateInvertedAlphabet(base64Url);

    private Chars() {
    }

    public static void base64Encode(@Nullable BinarySequence sequence, int maxLength, @NotNull CharSink<?> buffer) {
        int pad = base64Encode(sequence, maxLength, buffer, base64);
        for (int j = 0; j < pad; j++) {
            buffer.putAscii("=");
        }
    }

    public static boolean contains(@NotNull CharSequence sequence, @NotNull CharSequence term) {
        return indexOf(sequence, 0, sequence.length(), term) != -1;
    }

    public static boolean equals(@NotNull CharSequence l, @NotNull CharSequence r) {
        if (l == r) {
            return true;
        }

        int ll;
        if ((ll = l.length()) != r.length()) {
            return false;
        }

        return equalsChars(l, r, ll);
    }

    public static boolean equals(@NotNull String l, @NotNull String r) {
        return l.equals(r);
    }

    public static boolean equals(@NotNull CharSequence l, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = l.length();
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (l.charAt(i) != r.charAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    public static boolean equals(@NotNull CharSequence l, int lLo, int lHi, @NotNull CharSequence r, int rLo, int rHi) {
        if (l == r) {
            return true;
        }

        int ll = lHi - lLo;
        if (ll != rHi - rLo) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (l.charAt(i + lLo) != r.charAt(i + rLo)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Case-insensitive comparison of two char sequences.
     *
     * @param l left sequence
     * @param r right sequence
     * @return true if sequences match exactly (ignoring char case)
     */
    public static boolean equalsIgnoreCase(@NotNull CharSequence l, @NotNull CharSequence r) {
        if (l == r) {
            return true;
        }

        int ll = l.length();
        if (ll != r.length()) {
            return false;
        }

        return equalsCharsIgnoreCase(l, r, ll);
    }

    public static boolean equalsLowerCaseAscii(@NotNull CharSequence l, @NotNull CharSequence r) {
        int ll = l.length();
        if (ll != r.length()) {
            return false;
        }

        for (int i = 0; i < ll; i++) {
            if (toLowerCaseAscii(l.charAt(i)) != toLowerCaseAscii(r.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean equalsNc(@NotNull CharSequence l, @Nullable CharSequence r) {
        return r != null && equals(l, r);
    }

    public static int hashCode(char @NotNull [] value, int lo, int hi) {
        if (hi == lo) {
            return 0;
        }

        int h = 0;
        for (int p = lo; p < hi; p++) {
            h = 31 * h + value[p];
        }
        return h;
    }

    public static int hashCode(@NotNull CharSequence value) {
        if (value instanceof String) {
            return value.hashCode();
        }

        int len = value.length();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0; p < len; p++) {
            h = 31 * h + value.charAt(p);
        }
        return h;
    }

    /**
     * Searches for the first occurrence of a character in the entire character sequence.
     * This is a convenience method that searches from the beginning of the sequence.
     *
     * @param seq the character sequence to search within
     * @param c   the character to search for
     * @return the index of the first occurrence of the character, or -1 if not found
     */
    public static int indexOf(CharSequence seq, char c) {
        return indexOf(seq, 0, c);
    }

    /**
     * Searches for the first occurrence of a character starting from a specified position.
     * This is a convenience method that searches from the given starting position to the end of the sequence.
     *
     * @param seq   the character sequence to search within
     * @param seqLo the starting position (inclusive) to begin the search
     * @param c     the character to search for
     * @return the index of the first occurrence of the character, or -1 if not found
     */
    public static int indexOf(CharSequence seq, final int seqLo, char c) {
        return indexOf(seq, seqLo, seq.length(), c);
    }

    /**
     * Searches for the first occurrence of a character within the specified bounds of a character sequence.
     * This is a convenience method that searches for the 1st occurrence of the character.
     *
     * @param seq   the character sequence to search within
     * @param seqLo the lower bound (inclusive) of the search range
     * @param seqHi the upper bound (exclusive) of the search range
     * @param c     the character to search for
     * @return the index of the first occurrence of the character, or -1 if not found within bounds
     */
    public static int indexOf(CharSequence seq, int seqLo, int seqHi, char c) {
        return indexOf(seq, seqLo, seqHi, c, 1);
    }

    /**
     * Searches for the first occurrence of a character sequence within the specified bounds of another character sequence.
     * This method performs a case-sensitive search using an optimized string matching algorithm.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive. The method searches for the complete term within these bounds.
     * <p>
     * If the term is empty (length 0), the method returns 0 as an empty string is considered to be
     * found at the beginning of any sequence.
     *
     * @param seq   the character sequence to search within (must not be null)
     * @param seqLo the lower bound (inclusive) of the search range
     * @param seqHi the upper bound (exclusive) of the search range
     * @param term  the character sequence to search for (must not be null)
     * @return the index of the first occurrence of the term, or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOf(@NotNull CharSequence seq, int seqLo, int seqHi, @NotNull CharSequence term) {
        int termLen = term.length();
        if (termLen == 0) {
            return 0;
        }

        char first = term.charAt(0);
        int max = seqHi - termLen;

        for (int i = seqLo; i <= max; ++i) {
            if (seq.charAt(i) != first) {
                do {
                    ++i;
                } while (i <= max && seq.charAt(i) != first);
            }

            if (i <= max) {
                int j = i + 1;
                int end = j + termLen - 1;

                for (int k = 1; j < end && seq.charAt(j) == term.charAt(k); ++k) {
                    ++j;
                }

                if (j == end) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Searches for the nth occurrence of a character within the specified bounds of a character sequence.
     * This method supports both forward and reverse searching based on the occurrence parameter.
     * <p>
     * The search bounds are defined as {@code [seqLo, seqHi)} where {@code seqLo} is inclusive and
     * {@code seqHi} is exclusive.
     * <p>
     * <strong>Occurrence Parameter:</strong>
     * <ul>
     * <li>Positive values (1, 2, 3, ...): search forward for the 1st, 2nd, 3rd, ... occurrence</li>
     * <li>Negative values (-1, -2, -3, ...): search backward for the 1st, 2nd, 3rd, ... occurrence from the end</li>
     * <li>Zero (0): returns -1 immediately without searching</li>
     * </ul>
     *
     * @param seq        the character sequence to search within
     * @param seqLo      the lower bound (inclusive) of the search range
     * @param seqHi      the upper bound (exclusive) of the search range
     * @param ch         the character to search for
     * @param occurrence the occurrence number to find (positive for forward, negative for reverse, 0 returns -1)
     * @return the index of the specified occurrence of the character, or -1 if not found within bounds
     * @throws StringIndexOutOfBoundsException if bounds are invalid for the given sequence
     */
    public static int indexOf(CharSequence seq, int seqLo, int seqHi, char ch, int occurrence) {
        if (occurrence == 0) {
            return -1;
        }

        int count = 0;
        if (occurrence > 0) {
            for (int i = seqLo; i < seqHi; i++) {
                if (seq.charAt(i) == ch) {
                    count++;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        } else {    // if occurrence is negative, search in reverse
            for (int i = seqHi - 1; i >= seqLo; i--) {
                if (seq.charAt(i) == ch) {
                    count--;
                    if (count == occurrence) {
                        return i;
                    }
                }
            }
        }

        return -1;
    }

    public static boolean isBlank(CharSequence s) {
        if (s == null) {
            return true;
        }

        int len = s.length();
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (!Character.isWhitespace(c)) {
                return false;
            }
        }
        return true;
    }

    public static int lowerCaseAsciiHashCode(CharSequence value) {
        int len = value.length();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0; p < len; p++) {
            h = 31 * h + toLowerCaseAscii(value.charAt(p));
        }
        return h;
    }

    public static int lowerCaseHashCode(CharSequence value) {
        int len = value.length();
        if (len == 0) {
            return 0;
        }

        int h = 0;
        for (int p = 0; p < len; p++) {
            h = 31 * h + Character.toLowerCase(value.charAt(p));
        }
        return h;
    }

    public static boolean noMatch(CharSequence l, int llo, int lhi, CharSequence r, int rlo, int rhi) {
        int lp = llo;
        int rp = rlo;
        while (lp < lhi && rp < rhi) {
            if (Character.toLowerCase(l.charAt(lp++)) != r.charAt(rp++)) {
                return true;
            }

        }
        return lp != lhi || rp != rhi;
    }

    public static boolean startsWith(@Nullable CharSequence cs, @Nullable CharSequence starts) {
        if (cs == null || starts == null) {
            return false;
        }
        int l = starts.length();
        return l == 0 || cs.length() >= l && equalsChars(cs, starts, l);
    }

    public static boolean startsWith(CharSequence _this, char c) {
        return _this.length() > 0 && _this.charAt(0) == c;
    }

    public static String toLowerCaseAscii(@Nullable CharSequence value) {
        if (value == null) {
            return null;
        }
        final int len = value.length();
        if (len == 0) {
            return "";
        }

        final Utf16Sink b = Misc.getThreadLocalSink();
        for (int i = 0; i < len; i++) {
            b.put(toLowerCaseAscii(value.charAt(i)));
        }
        return b.toString();
    }

    public static char toLowerCaseAscii(char character) {
        return character > 64 && character < 91 ? (char) (character + 32) : character;
    }

    public static String toString(CharSequence s) {
        return s == null ? null : s.toString();
    }

    private static int[] base64CreateInvertedAlphabet(char[] alphabet) {
        int[] inverted = new int[128]; // ASCII only
        Arrays.fill(inverted, (byte) -1);
        int length = alphabet.length;
        for (int i = 0; i < length; i++) {
            char letter = alphabet[i];
            assert letter < 128;
            inverted[letter] = (byte) i;
        }
        return inverted;
    }

    private static int base64Encode(@Nullable BinarySequence sequence, int maxLength, @NotNull CharSink<?> buffer, char @NotNull [] alphabet) {
        if (sequence == null) {
            return 0;
        }
        final long len = Math.min(maxLength, sequence.length());
        int pad = 0;
        for (int i = 0; i < len; i += 3) {
            int b = ((sequence.byteAt(i) & 0xFF) << 16) & 0xFFFFFF;
            if (i + 1 < len) {
                b |= (sequence.byteAt(i + 1) & 0xFF) << 8;
            } else {
                pad++;
            }
            if (i + 2 < len) {
                b |= (sequence.byteAt(i + 2) & 0xFF);
            } else {
                pad++;
            }

            for (int j = 0; j < 4 - pad; j++) {
                int c = (b & 0xFC0000) >> 18;
                buffer.putAscii(alphabet[c]);
                b <<= 6;
            }
        }
        return pad;
    }

    private static boolean equalsChars(@NotNull CharSequence l, @NotNull CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (l.charAt(i) != r.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalsCharsIgnoreCase(@NotNull CharSequence l, @NotNull CharSequence r, int len) {
        for (int i = 0; i < len; i++) {
            if (Character.toLowerCase(l.charAt(i)) != Character.toLowerCase(r.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    static {
        CHAR_STRINGS = new String[128];
        for (char c = 0; c < 128; c++) {
            CHAR_STRINGS[c] = Character.toString(c);
        }
    }
}