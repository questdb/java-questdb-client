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
        // Scan by code point, not UTF-16 unit. A supplementary-plane format char (e.g. a U+E00xx language
        // tag char) arrives as a surrogate pair whose halves report SURROGATE rather than FORMAT, and a
        // lone surrogate likewise - per-unit scanning would pass both through raw. Judging the whole code
        // point (via DisplaySafe, the shared classifier) escapes them, while a normal supplementary char
        // such as an emoji is neither control nor format and is emitted verbatim.
        for (int i = 0, n = nonPrintable.length(); i < n; ) {
            final int cp = Character.codePointAt(nonPrintable, i);
            final int count = Character.charCount(cp);
            if (DisplaySafe.isDisplaySafe(cp)) {
                for (int j = 0; j < count; j++) {
                    put(nonPrintable.charAt(i + j));
                }
            } else {
                DisplaySafe.putUnicodeEscape(this, cp);
            }
            i += count;
        }
    }

    default void putAsPrintable(char c) {
        // A single UTF-16 unit: escape control chars, Unicode format chars, and a lone surrogate (which has
        // no displayable meaning). Supplementary-plane format chars are caught by the code-point-aware
        // putAsPrintable(CharSequence).
        if (DisplaySafe.isDisplaySafe(c)) {
            put(c);
        } else {
            DisplaySafe.putUnicodeEscape(this, c);
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
