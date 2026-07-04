/*+*****************************************************************************
 * ___                 _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
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
 *
 ******************************************************************************/

package io.questdb.client.std.str;

import io.questdb.client.std.Misc;
import io.questdb.client.std.Numbers;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A sink that does not expose its storage format. Users of this interface must
 * not make any assumptions about the storage format.
 */
@SuppressWarnings("unchecked")
public interface CharSink<T extends CharSink<?>> {

    /**
     * Assumes the char is ASCII and appends it to the sink n times.
     * If the char is non-ASCII, it may append a corrupted char, depending
     * on the implementation.
     */
    default void fillAscii(char c, int n) {
        for (int i = 0; i < n; i++) {
            putAscii(c);
        }
    }

    default T put(@Nullable Sinkable sinkable) {
        if (sinkable != null) {
            sinkable.toSink(this);
        }
        return (T) this;
    }

    T put(char c);

    default T put(@Nullable CharSequence cs) {
        if (cs != null) {
            for (int i = 0, n = cs.length(); i < n; i++) {
                put(cs.charAt(i));
            }
        }
        return (T) this;
    }

    /**
     * Appends a UTF-8-encoded sequence to this sink.
     * <br>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * If the sequence's `isAscii` status is false, this sink's `isAscii`
     * status drops to false as well.
     */
    T put(@Nullable Utf8Sequence us);

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(int value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(long value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends a string representation of the supplied number to this sink.
     */
    default T put(double value) {
        Numbers.append(this, value);
        return (T) this;
    }

    /**
     * Appends an ASCII char to this sink. If the char is non-ASCII, it may append a
     * corrupted char, depending on the implementation.
     */
    T putAscii(char c);

    /**
     * Appends a sequence of ASCII chars to this sink. If some chars are non-ASCII,
     * it may append corrupted chars, depending on the implementation.
     */
    T putAscii(@Nullable CharSequence cs);

    /**
     * Appends a range of ASCII chars from the supplied array. If some chars are
     * non-ASCII, it may append corrupted chars, depending on the implementation.
     */
    default T putAscii(char @NotNull [] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            putAscii(chars[i + start]);
        }
        return (T) this;
    }

    /**
     * Appends a range of ASCII chars from the supplied sequence to this sink.
     * If some chars are non-ASCII, it may append corrupted chars, depending on
     * the implementation.
     */
    default T putAscii(@NotNull CharSequence cs, int start, int len) {
        for (int i = start; i < len; i++) {
            putAscii(cs.charAt(i));
        }
        return (T) this;
    }

    default T putEOL() {
        return putAscii(Misc.EOL);
    }

    /**
     * Accepts a range of memory addresses from lo to hi (exclusive), expecting it to
     * point to a block of valid UTF-8 bytes, and appends it to this sink.
     * <br>
     * For impls that care about the distinction between ASCII and non-ASCII:
     * Drops the `isAscii` status of this sink.
     */
    T putNonAscii(long lo, long hi);

    default T putSize(long bytes) {
        long b = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (b < 1024L) {
            return (T) put(bytes).put(' ').put('B');
        }

        final String[] units = {" KiB", " MiB", " GiB", " TiB", " PiB", " EiB"};
        final int[] shifts = {10, 20, 30, 40, 40, 40};
        final long baseLimit = 0xfffccccccccccccL;

        for (int i = 0; i < units.length; i++) {
            if (i >= 4 || b <= (baseLimit >> shifts[i])) {
                long inputBytes = bytes;
                if (i == 4) {
                    inputBytes >>= 10;
                } else if (i == 5) {
                    inputBytes >>= 20;
                }
                double divisor = Double.longBitsToDouble(0x3ff0000000000000L + (((long) shifts[i]) << 52)); // 0x1p10, 0x1p20, etc.
                double value = Math.round(inputBytes / divisor * 1000.0) / 1000.0;
                return (T) put(value).put(units[i]);
            }
        }
        return (T) this;
    }
}