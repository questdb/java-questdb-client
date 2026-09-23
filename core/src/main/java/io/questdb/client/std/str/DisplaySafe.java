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

import static io.questdb.client.std.Numbers.hexDigits;

/**
 * Shared classifier for whether a code point is safe to show in a terminal or a log line. It is the one
 * source of truth for the client's display-escaping: {@link Utf16Sink#putAsPrintable(CharSequence)} escapes
 * everything it rejects, and the OIDC auth layer strips it from untrusted identity-provider text. Left raw,
 * attacker-influenced text - an ILP server's error body, a column name, a verification URL - could reorder,
 * hide, or forge what a human reads (a right-to-left override, a zero-width joiner, an ANSI escape).
 */
public final class DisplaySafe {

    private DisplaySafe() {
    }

    /**
     * Returns {@code true} when {@code cp} can be shown verbatim, {@code false} when it must be escaped or
     * stripped. A code point is unsafe if it is a control char (C0/C1, DEL), a Unicode format char (bidi
     * embeddings/overrides/isolates, LRM/RLM marks, zero-width joiners, the BOM, supplementary-plane tag
     * chars), a Unicode line/paragraph separator (U+2028/U+2029, which break a rendered log line), or a
     * surrogate (a lone half, with no displayable meaning).
     */
    public static boolean isDisplaySafe(int cp) {
        // Printable ASCII is the overwhelmingly common case and is never a control, format or surrogate char,
        // so a single range check returns it without the Character.getType table lookup.
        if (cp >= 0x20 && cp < 0x7f) {
            return true;
        }
        if (Character.isISOControl(cp)) {
            return false;
        }
        final int type = Character.getType(cp);
        // FORMAT covers bidi/zero-width/joiners/BOM/tag chars; SURROGATE a lone half. LINE_SEPARATOR (U+2028)
        // and PARAGRAPH_SEPARATOR (U+2029) are Unicode line breaks that split a rendered log line in
        // ECMAScript/GUI/JSON log consumers, yet they are neither C0/C1 (isISOControl) nor FORMAT, so catch
        // them here rather than let a tampered field forge an apparent extra log line.
        if (type == Character.FORMAT || type == Character.SURROGATE
                || type == Character.LINE_SEPARATOR || type == Character.PARAGRAPH_SEPARATOR) {
            return false;
        }
        // The explicit bidi/BOM set is redundant with the FORMAT category on a conformant JDK, but kept as
        // belt-and-suspenders on one that categorizes these differently. Hex literals (not char escapes) keep
        // this source ASCII, so it carries none of the chars it guards.
        return !(cp >= 0x202A && cp <= 0x202E)   // LRE, RLE, PDF, LRO, RLO
                && !(cp >= 0x2066 && cp <= 0x2069) // LRI, RLI, FSI, PDI
                && cp != 0x200E && cp != 0x200F    // LRM, RLM
                && cp != 0xFEFF;                   // BOM / zero-width no-break space
    }

    /**
     * The inverse of {@link #isDisplaySafe(int)}: {@code true} when {@code cp} must not reach a terminal or
     * log line raw.
     */
    public static boolean isUnsafeForDisplay(int cp) {
        return !isDisplaySafe(cp);
    }

    // Escapes a code point to one (BMP) or two (supplementary, as its surrogate pair) visible \\uXXXX
    // sequences, so the escaped value still names the original char. Emitting all four hex digits keeps a
    // char above U+00FF (e.g. U+202E) correct rather than truncated to its low byte. A static helper here
    // (not a private method on Utf16Sink) keeps the source Java 8 - private interface methods are Java 9.
    static void putUnicodeEscape(Utf16Sink sink, int cp) {
        if (cp > 0xFFFF) {
            putUnicodeEscape(sink, Character.highSurrogate(cp));
            putUnicodeEscape(sink, Character.lowSurrogate(cp));
            return;
        }
        sink.put('\\');
        sink.put('u');
        sink.put(hexDigits[(cp >> 12) & 0xF]);
        sink.put(hexDigits[(cp >> 8) & 0xF]);
        sink.put(hexDigits[(cp >> 4) & 0xF]);
        sink.put(hexDigits[cp & 0xF]);
    }
}
