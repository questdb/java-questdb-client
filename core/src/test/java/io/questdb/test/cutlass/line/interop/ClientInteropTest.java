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

package io.questdb.test.cutlass.line.interop;

import io.questdb.cairo.ColumnType;
import io.questdb.client.Sender;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.json.JsonLexer;
import io.questdb.cutlass.json.JsonParser;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

/**
 * Client interoperability test that validates ILP line protocol output
 * against a standardized test suite (ilp-client-interop-test.json).
 * <p>
 * This is a unit test that doesn't require a QuestDB server.
 */
public class ClientInteropTest {

    @Test
    public void testInterop() throws Exception {
        String pp = TestUtils.getTestResourcePath("/io/questdb/test/cutlass/line/interop/ilp-client-interop-test.json");
        final long memSize = 1024 * 1024;

        ByteChannel channel = new ByteChannel();
        try (JsonLexer lexer = new JsonLexer(1024, 1024);
             Sender sender = new LineTcpSenderV2(channel, 1024, 127);
             DirectByteSink sink = new DirectByteSink(memSize, MemoryTag.NATIVE_DEFAULT)) {
            final Path path = Paths.get(pp);
            final byte[] b = Files.readAllBytes(path);
            for (int i = 0, n = b.length; i < n; i++) {
                sink.put(b[i]);
            }

            JsonTestSuiteParser parser = new JsonTestSuiteParser(sender, channel);
            lexer.parse(sink.lo(), sink.hi(), parser);
        }
    }

    /**
     * A LineChannel implementation that captures sent bytes for verification.
     */
    private static class ByteChannel implements LineChannel {
        private byte[] buffer = new byte[4096];
        private int position = 0;

        @Override
        public void close() {
        }

        public String encodeBase64String() {
            byte[] data = new byte[position];
            System.arraycopy(buffer, 0, data, 0, position);
            return Base64.getEncoder().encodeToString(data);
        }

        public boolean endWith(byte b) {
            return position > 0 && buffer[position - 1] == b;
        }

        public boolean equals(byte[] expected, int offset, int len) {
            if (position != len) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                if (buffer[i] != expected[offset + i]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public int receive(long ptr, int len) {
            return 0;
        }

        public void reset() {
            position = 0;
        }

        @Override
        public void send(long ptr, int len) {
            ensureCapacity(position + len);
            for (int i = 0; i < len; i++) {
                buffer[position++] = Unsafe.getUnsafe().getByte(ptr + i);
            }
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity > buffer.length) {
                int newCapacity = Math.max(buffer.length * 2, minCapacity);
                byte[] newBuffer = new byte[newCapacity];
                System.arraycopy(buffer, 0, newBuffer, 0, position);
                buffer = newBuffer;
            }
        }
    }

    private static class JsonTestSuiteParser implements JsonParser {

        public static final int TAG_COLUMNS = 8;
        public static final int TAG_COLUMN_NAME = 4;
        public static final int TAG_COLUMN_TYPE = 9;
        public static final int TAG_COLUMN_VALUE = 5;
        public static final int TAG_EXPECTED_RESULT = 6;
        public static final int TAG_LINE = 10;
        public static final int TAG_SYMBOLS = 7;
        public static final int TAG_SYMBOL_NAME = 2;
        public static final int TAG_SYMBOL_VALUE = 3;
        public static final int TAG_TABLE_NAME = 1;
        public static final int TAG_TEST_NAME = 0;
        private final ByteChannel byteChannel;
        private final Sender sender;
        private final StringSink stringSink = new StringSink();
        private int columnType = -1;
        private boolean encounteredError;
        private String name;
        private int tag1Type = -1;
        private int tag2Type = -1;

        public JsonTestSuiteParser(Sender sender, ByteChannel channel) {
            this.sender = sender;
            this.byteChannel = channel;
        }

        @Override
        public void onEvent(int code, CharSequence tag, int position) throws JsonException {
            tag = unescape(tag, stringSink);
            switch (code) {
                case JsonLexer.EVT_NAME:
                    if (Chars.equalsIgnoreCase(tag, "testname")) {
                        tag1Type = TAG_TEST_NAME;
                    } else if (Chars.equalsIgnoreCase(tag, "table")) {
                        tag1Type = TAG_TABLE_NAME;
                    } else if (Chars.equalsIgnoreCase(tag, "symbols")) {
                        tag2Type = TAG_SYMBOLS;
                    } else if (Chars.equalsIgnoreCase(tag, "columns")) {
                        tag2Type = TAG_COLUMNS;
                    } else if (Chars.equalsIgnoreCase(tag, "name")) {
                        if (tag2Type == TAG_SYMBOLS) {
                            tag1Type = TAG_SYMBOL_NAME;
                        } else {
                            tag1Type = TAG_COLUMN_NAME;
                        }
                    } else if (Chars.equalsIgnoreCase(tag, "value")) {
                        if (tag2Type == TAG_SYMBOLS) {
                            tag1Type = TAG_SYMBOL_VALUE;
                        } else {
                            tag1Type = TAG_COLUMN_VALUE;
                        }
                    } else if (Chars.equalsIgnoreCase(tag, "type")) {
                        tag1Type = TAG_COLUMN_TYPE;
                    } else if (Chars.equalsIgnoreCase(tag, "status")) {
                        tag1Type = TAG_EXPECTED_RESULT;
                    } else if (Chars.equalsIgnoreCase(tag, "base64Line")) {
                        tag1Type = TAG_LINE;
                    } else {
                        tag1Type = -1;
                    }
                    break;
                case JsonLexer.EVT_VALUE:
                    switch (tag1Type) {
                        case TAG_TEST_NAME:
                            break;
                        case TAG_TABLE_NAME:
                            try {
                                sender.table(Chars.toString(tag));
                            } catch (LineSenderException e) {
                                encounteredError = true;
                            }
                            break;
                        case TAG_SYMBOL_NAME:
                        case TAG_COLUMN_NAME:
                            name = Chars.toString(tag);
                            break;
                        case TAG_SYMBOL_VALUE:
                            if (encounteredError) {
                                break;
                            }
                            try {
                                sender.symbol(name, Chars.toString(tag));
                            } catch (LineSenderException e) {
                                encounteredError = true;
                            }
                            break;
                        case TAG_COLUMN_TYPE:
                            columnType = ColumnType.typeOf(tag);
                            break;
                        case TAG_COLUMN_VALUE:
                            if (encounteredError) {
                                break;
                            }
                            try {
                                switch (columnType) {
                                    case ColumnType.DOUBLE:
                                        try {
                                            sender.doubleColumn(name, Numbers.parseDouble(tag));
                                        } catch (NumericException e) {
                                            throw JsonException.$(position, "bad double");
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.LONG:
                                        try {
                                            sender.longColumn(name, Numbers.parseLong(tag));
                                        } catch (NumericException e) {
                                            throw JsonException.$(position, "bad long");
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.BOOLEAN:
                                        try {
                                            sender.boolColumn(name, isTrueKeyword(tag));
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.TIMESTAMP:
                                        try {
                                            sender.timestampColumn(name, Numbers.parseLong(tag), ChronoUnit.NANOS);
                                        } catch (NumericException e) {
                                            throw JsonException.$(position, "bad long");
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    case ColumnType.STRING:
                                        try {
                                            sender.stringColumn(name, Chars.toString(tag));
                                        } catch (LineSenderException e) {
                                            encounteredError = true;
                                        }
                                        break;
                                    default:
                                        throw JsonException.$(position, "unexpected state");
                                }
                            } catch (LineSenderException e) {
                                encounteredError = true;
                            }
                            columnType = -1;
                            break;
                        case TAG_EXPECTED_RESULT:
                            if (Chars.equals(tag, "SUCCESS")) {
                                Assert.assertFalse(encounteredError);
                            } else if (Chars.equals(tag, "ERROR")) {
                                if (!encounteredError) {
                                    // there was no error recorded yes. let's try to send the line now
                                    // that's the last chance to get an error. if there is no error
                                    // then the test-case must fail
                                    try {
                                        sender.atNow();
                                        sender.flush();
                                        Assert.fail("Test case '" + name + "' should have failed, but it passed");
                                    } catch (LineSenderException e) {
                                        // expected
                                    }
                                }
                                resetForNextTestCase();
                            } else {
                                throw new AssertionError("unknown status " + tag);
                            }
                            break;
                        case TAG_LINE:
                            Assert.assertFalse(encounteredError);
                            sender.atNow();
                            sender.flush();
                            assertSuccessfulLine(Base64.getDecoder().decode(tag.toString()));
                            resetForNextTestCase();
                            break;
                    }
                    break;
            }
        }

        private static boolean isTrueKeyword(CharSequence tok) {
            return tok.length() == 4
                    && (tok.charAt(0) | 32) == 't'
                    && (tok.charAt(1) | 32) == 'r'
                    && (tok.charAt(2) | 32) == 'u'
                    && (tok.charAt(3) | 32) == 'e';
        }

        private static CharSequence unescape(CharSequence tag, StringSink stringSink) {
            if (tag == null) {
                return null;
            }
            stringSink.clear();

            for (int i = 0, n = tag.length(); i < n; i++) {
                char sourceChar = tag.charAt(i);
                if (sourceChar != '\\') {
                    // happy-path, nothing to unescape
                    stringSink.put(sourceChar);
                } else {
                    // slow path. either there is a code unit sequence. think of this: foo\u0001bar
                    // or a simple escaping: \n, \r, \\, \", etc.
                    // in both cases we will consume more than 1 character from the input,
                    // so we have to adjust "i" accordingly

                    // malformed input could throw IndexOutOfBoundsException, but given we control
                    // the test data then we are OK.
                    char nextChar = tag.charAt(i + 1);
                    if (nextChar == 'u') {
                        // code unit sequence
                        char ch;
                        try {
                            ch = (char) Numbers.parseHexInt(tag, i + 2, i + 6);
                        } catch (NumericException e) {
                            throw new AssertionError("cannot parse code sequence in " + tag);
                        }
                        stringSink.put(ch);
                        i += 5;
                    } else if (nextChar == '\\') {
                        stringSink.put('\\');
                        i++;
                    } else if (nextChar == '\"') {
                        stringSink.put('\"');
                        i++;
                    } else if (nextChar == 'b') {
                        // backspace
                        stringSink.put('\b');
                        i++;
                    } else if (nextChar == 'f') {
                        // form-feed
                        stringSink.put('\f');
                        i++;
                    } else if (nextChar == 'n') {
                        // new line
                        stringSink.put('\n');
                        i++;
                    } else if (nextChar == 'r') {
                        // carriage return
                        stringSink.put('\r');
                        i++;
                    } else if (nextChar == 't') {
                        // tab
                        stringSink.put('\t');
                        i++;
                    } else {
                        throw new AssertionError("Unknown escaping sequence at " + tag);
                    }
                }
            }
            return stringSink.toString();
        }

        private void assertSuccessfulLine(byte[] tag) {
            Assert.assertTrue("Produced line does not end with a new line char", byteChannel.endWith((byte) '\n'));
            Assert.assertTrue("buffer base64[" + byteChannel.encodeBase64String() + "]", byteChannel.equals(tag, 0, tag.length - 1));
        }

        private void resetForNextTestCase() {
            encounteredError = false;
            byteChannel.reset();
        }
    }
}
