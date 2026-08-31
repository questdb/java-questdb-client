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

package io.questdb.client.test.cutlass.line;

import io.questdb.client.cutlass.line.LineSenderException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LineSenderExceptionTest {

    @Test
    public void testEmptyMessage() {
        LineSenderException e = new LineSenderException(new RuntimeException());
        String message = e.getMessage();
        assertEquals("", message);
    }

    @Test
    public void testEmptyMessage_withErrNo() {
        LineSenderException e = new LineSenderException(new RuntimeException()).errno(10);
        String message = e.getMessage();
        assertEquals("[10]", message);
    }

    @Test
    public void testMessage_PutAsPrintableWithNonPrintableInput() {
        LineSenderException e = new LineSenderException("non-printable char: ").putAsPrintable("āa");
        String message = e.getMessage();
        assertEquals("non-printable char: āa", message);

    }

    @Test
    public void testMessage_putAsPrintableEscapesBidiOverride() {
        // U+202E RIGHT-TO-LEFT OVERRIDE is a BMP format char - regression guard for the existing behavior
        LineSenderException e = new LineSenderException("char: ").putAsPrintable("a\u202Eb");
        assertEquals("char: a\\u202eb", e.getMessage());
    }

    @Test
    public void testMessage_putAsPrintableEscapesLoneSurrogate() {
        // a lone high surrogate has no displayable meaning and must be escaped, not passed through raw
        LineSenderException e = new LineSenderException("char: ").putAsPrintable("a\uD800b");
        assertEquals("char: a\\ud800b", e.getMessage());
    }

    @Test
    public void testMessage_putAsPrintableEscapesSupplementaryFormatChar() {
        // U+E0001 LANGUAGE TAG is a supplementary-plane format char: it arrives as a surrogate pair and must
        // be escaped (as both halves), not passed through raw, or it could hide or forge text in a log
        String tagChar = new String(Character.toChars(0xE0001));
        LineSenderException e = new LineSenderException("char: ").putAsPrintable("a" + tagChar + "b");
        assertEquals("char: a\\udb40\\udc01b", e.getMessage());
    }

    @Test
    public void testMessage_putAsPrintableKeepsEmoji() {
        // U+1F600 GRINNING FACE is a normal supplementary char (not control or format) - emitted verbatim
        String emoji = new String(Character.toChars(0x1F600));
        LineSenderException e = new LineSenderException("char: ").putAsPrintable("a" + emoji + "b");
        assertEquals("char: a" + emoji + "b", e.getMessage());
    }

    @Test
    public void testMessage_putAsPrintableAgreesOnBothPaths() {
        // putAsPrintable now classifies before it copies: an all-printable sequence is handed to
        // put(CharSequence) in one go, and only a sequence carrying something unsafe is walked and escaped
        // character by character. Two paths mean they can drift, so pin that they agree - the same text,
        // with and without one unsafe code point in it, must differ only by that code point's escape.
        String printable = "Could not flush buffer: table 'trades' column 'price' rejected, line 42";
        assertEquals(printable, new LineSenderException("").putAsPrintable(printable).getMessage());

        // the escaping path over the same text, with a bidi override spliced into the middle
        int at = printable.indexOf("column");
        String tampered = printable.substring(0, at) + (char) 0x202e + printable.substring(at);
        assertEquals(printable.substring(0, at) + "\\u202e" + printable.substring(at),
                new LineSenderException("").putAsPrintable(tampered).getMessage());
    }

    @Test
    public void testMessage_withErrNo() {
        LineSenderException e = new LineSenderException("message").errno(10);
        String message = e.getMessage();
        assertEquals("[10] message", message);
    }

    @Test
    public void testMessage_withPutAsPrintable() {
        LineSenderException e = new LineSenderException("non-printable char: ").putAsPrintable("test+");
        String message = e.getMessage();
        assertEquals("non-printable char: test+", message);

    }

    @Test
    public void testMessage_withoutErrNo() {
        LineSenderException e = new LineSenderException("message");
        String message = e.getMessage();
        assertEquals("message", message);
    }
}
