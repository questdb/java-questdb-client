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

package io.questdb.client.test.std.str;

import io.questdb.client.std.str.DisplaySafe;
import org.junit.Assert;
import org.junit.Test;

/**
 * Direct coverage for {@link DisplaySafe}, the single source of truth for whether a code point may be shown
 * verbatim in a terminal or a log line. Both {@code Utf16Sink.putAsPrintable} and the OIDC display sanitizer
 * delegate to it, so a regression here would silently weaken every display-escaping path - yet the classifier
 * was previously exercised only transitively, for the few code points the integration tests happen to use.
 * <p>
 * The unsafe code points (controls, Unicode format chars, surrogates, bidi controls, the BOM) are written as
 * hex literals so this source stays pure ASCII and carries none of the chars it asserts on.
 */
public class DisplaySafeTest {

    @Test
    public void testC0C1ControlsAndDelAreUnsafe() {
        // C0 (incl. TAB/LF/CR/ESC), DEL, and the C1 block: every ISO control must be escaped
        int[] unsafe = {0x00, 0x07, 0x08, 0x09, 0x0A, 0x0D, 0x1B, 0x1F, 0x7F, 0x80, 0x90, 0x9F};
        for (int cp : unsafe) {
            String hex = "0x" + Integer.toHexString(cp);
            Assert.assertFalse("control " + hex + " must be unsafe", DisplaySafe.isDisplaySafe(cp));
            Assert.assertTrue("control " + hex + " must be unsafe", DisplaySafe.isUnsafeForDisplay(cp));
        }
    }

    @Test
    public void testFormatBidiAndBomAreUnsafe() {
        // Cf format chars and the explicit bidi/BOM set that reorder, hide, or mark text - including the
        // supplementary-plane "tag" chars that arrive as a surrogate pair and must be judged whole
        int[] unsafe = {
                0x00AD, // soft hyphen
                0x200B, // zero-width space
                0x200E, 0x200F, // LRM, RLM
                0x202A, 0x202B, 0x202C, 0x202D, 0x202E, // LRE, RLE, PDF, LRO, RLO
                0x2066, 0x2067, 0x2068, 0x2069, // LRI, RLI, FSI, PDI
                0xFEFF, // BOM / zero-width no-break space
                0xE0001, // language tag
                0xE0020, 0xE007F // tag space, cancel tag
        };
        for (int cp : unsafe) {
            String hex = "0x" + Integer.toHexString(cp);
            Assert.assertTrue("format " + hex + " must be unsafe", DisplaySafe.isUnsafeForDisplay(cp));
            Assert.assertFalse("format " + hex + " must be unsafe", DisplaySafe.isDisplaySafe(cp));
        }
    }

    @Test
    public void testLoneSurrogatesAreUnsafe() {
        // a lone surrogate half has no displayable meaning; the code-point classifier must reject it
        int[] surrogates = {0xD800, 0xDBFF, 0xDC00, 0xDFFF};
        for (int cp : surrogates) {
            Assert.assertFalse("surrogate 0x" + Integer.toHexString(cp) + " must be unsafe", DisplaySafe.isDisplaySafe(cp));
        }
    }

    @Test
    public void testPrintableAsciiIsSafe() {
        // the fast-path range 0x20..0x7e is the overwhelmingly common case and must always pass
        for (int cp = 0x20; cp <= 0x7e; cp++) {
            String hex = "0x" + Integer.toHexString(cp);
            Assert.assertTrue("printable ASCII " + hex + " must be safe", DisplaySafe.isDisplaySafe(cp));
            Assert.assertFalse("printable ASCII " + hex + " must be safe", DisplaySafe.isUnsafeForDisplay(cp));
        }
    }

    @Test
    public void testPrintableSupplementaryCharsAreSafe() {
        // a normal supplementary char (emoji, CJK extension) is neither control, format, nor surrogate and
        // stays safe, so the classifier does not over-escape legitimate non-BMP text
        int[] safe = {
                0x1F600, // grinning face emoji
                0x1F4A9, // pile of poo
                0x20000  // CJK Extension B ideograph
        };
        for (int cp : safe) {
            Assert.assertTrue("supplementary 0x" + Integer.toHexString(cp) + " must be safe", DisplaySafe.isDisplaySafe(cp));
        }
    }
}
