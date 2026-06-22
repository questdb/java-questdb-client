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

package io.questdb.client.std.str;

import org.jetbrains.annotations.Nullable;

import static io.questdb.client.std.Numbers.hexDigits;

/**
 * Family of sinks that write out <b>character</b> value as UTF16 encoded bytes. This interface
 * is separate from {@link CharSink} to achieve two goals:
 * <ul>
 *     <li>Avoid using these sinks as the target of UTF16-to-UTF8 conversions</li>
 *     <li>Group implementations in easy to understand hierarchy</li>
 * </ul>
 */
public interface Utf16Sink extends CharSink<Utf16Sink> {
    default Utf16Sink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            Utf8s.utf8ToUtf16(us, this);
        }
        return this;
    }

    default void putAsPrintable(CharSequence nonPrintable) {
        for (int i = 0, n = nonPrintable.length(); i < n; i++) {
            char c = nonPrintable.charAt(i);
            putAsPrintable(c);
        }
    }

    default void putAsPrintable(char c) {
        // escape control chars (C0/C1, DEL) and Unicode format chars - bidi embeddings/overrides/isolates,
        // LRM/RLM marks, zero-width joiners, the BOM - to a visible \\uXXXX. Left raw, attacker-influenced
        // text (an ILP server's JSON error body, a column name) could reorder, hide or forge what a human
        // reads in a terminal or log; escaping rather than stripping keeps it visible for diagnosis. Per
        // UTF-16-unit scanning covers every BMP threat; a supplementary-plane char (emoji surrogate pair) is
        // neither control nor format and passes through. Emitting all four hex digits keeps a format char
        // above U+00FF (e.g. U+202E) correct rather than truncated to its low byte.
        if (!Character.isISOControl(c) && Character.getType(c) != Character.FORMAT) {
            put(c);
        } else {
            put('\\');
            put('u');
            put(hexDigits[(c >> 12) & 0xF]);
            put(hexDigits[(c >> 8) & 0xF]);
            put(hexDigits[(c >> 4) & 0xF]);
            put(hexDigits[c & 0xF]);
        }
    }

    /**
     * UTF16 sink stores ASCII character just like any other, as 16bit representation.
     *
     * @param c ascii character to write out.
     * @return this sink for daisy-chaining
     */
    @Override
    default Utf16Sink putAscii(char c) {
        return put(c);
    }

    /**
     * UTF16 sink does not make any special provisions for ASCII string. It will be stored just like any
     * other UTF16 encoded string.
     *
     * @param cs UTF16 encoded ASCII string
     * @return this sink for daisy-chaining
     */
    @Override
    default Utf16Sink putAscii(@Nullable CharSequence cs) {
        return put(cs);
    }

    default Utf16Sink putNonAscii(long lo, long hi) {
        Utf8s.utf8ToUtf16(lo, hi, this);
        return this;
    }

}