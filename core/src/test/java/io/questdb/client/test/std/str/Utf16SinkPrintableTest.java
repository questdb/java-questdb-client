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

import io.questdb.client.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Rendering coverage for {@code Utf16Sink.putAsPrintable}, at the sink rather than through an exception
 * message.
 * <p>
 * {@code DisplaySafeTest} pins the CLASSIFIER - which code points are safe - and the
 * {@code LineSenderException} tests pin the {@code CharSequence} overload through one caller. Two things
 * fell between them: the single-char overload, whose only production caller
 * ({@code ConfStringParser}) asserts the message prefix and the position but never the escape, and the
 * code points that only the sink can show are emitted correctly - U+2028, U+2029 and the BOM, which the
 * classifier rejects but which no test followed through a sink.
 * <p>
 * Both matter for the same reason the escaping exists at all: these strings are rendered into a log line
 * or a terminal, and an unescaped bidi override or ANSI escape rewrites what a human reads.
 */
public class Utf16SinkPrintableTest {

    @Test
    public void testPutAsPrintableCharEscapesEveryUnsafeClass() {
        // C0, DEL, C1, bidi override, BOM, and a lone surrogate - one per class the classifier rejects
        assertCharRenders((char) 0x00, "\\u0000");
        assertCharRenders((char) 0x1b, "\\u001b");
        assertCharRenders((char) 0x7f, "\\u007f");
        assertCharRenders((char) 0x9f, "\\u009f");
        assertCharRenders((char) 0x202e, "\\u202e");
        assertCharRenders((char) 0xfeff, "\\ufeff");
        assertCharRenders((char) 0xd800, "\\ud800");
    }

    @Test
    public void testPutAsPrintableCharKeepsPrintableAscii() {
        // the boundaries of the printable range, which an off-by-one on either end would escape
        assertCharRenders(' ', " ");
        assertCharRenders('A', "A");
        assertCharRenders('~', "~");
    }

    @Test
    public void testPutAsPrintableCharUsesFourHexDigits() {
        // The escape must name the char, not its low byte. An implementation that truncates renders U+202E
        // as . - a full stop - which is worse than useless: it looks like ordinary text.
        StringSink sink = new StringSink();
        sink.putAsPrintable((char) 0x202e);
        Assert.assertEquals("\\u202e", sink.toString());
        Assert.assertNotEquals("\\u002e", sink.toString());
    }

    @Test
    public void testPutAsPrintableSequenceEscapesLineAndParagraphSeparators() {
        // U+2028 and U+2029 are neither C0/C1 nor Cf, so they need their own arm in the classifier; through
        // a sink they must come out escaped, because a JSON or GUI log consumer treats them as line breaks
        // and a tampered field could forge an apparent extra log line.
        assertSequenceRenders("a" + (char) 0x2028 + "b", "a\\u2028b");
        assertSequenceRenders("a" + (char) 0x2029 + "b", "a\\u2029b");
        assertSequenceRenders("a" + (char) 0xfeff + "b", "a\\ufeffb");
    }

    private static void assertCharRenders(char c, String expected) {
        StringSink sink = new StringSink();
        sink.putAsPrintable(c);
        Assert.assertEquals("rendering of char 0x" + Integer.toHexString(c), expected, sink.toString());
    }

    private static void assertSequenceRenders(CharSequence input, String expected) {
        StringSink sink = new StringSink();
        sink.putAsPrintable(input);
        Assert.assertEquals(expected, sink.toString());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c > 0x7e) {
                Assert.assertTrue("the raw char 0x" + Integer.toHexString(c) + " must not survive: "
                        + sink, sink.toString().indexOf(c) < 0);
            }
        }
    }
}
