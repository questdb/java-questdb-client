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

package io.questdb.client.test.cutlass.qwp.websocket;

import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

/**
 * Tests for the client-side WebSocket frame parser.
 * The client parser expects unmasked frames (from the server)
 * and rejects masked frames.
 */
public class WebSocketFrameParserTest {

    @Test
    public void testControlFrameBetweenFragments() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                WebSocketFrameParser parser = new WebSocketFrameParser();

                // First data fragment
                writeBytes(buf, (byte) 0x01, (byte) 0x02, (byte) 'H', (byte) 'i');
                parser.parse(buf, buf + 4);
                Assert.assertFalse(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

                // Ping in the middle (control frame, FIN must be 1)
                parser.reset();
                writeBytes(buf, (byte) 0x89, (byte) 0x00);
                parser.parse(buf, buf + 2);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.PING, parser.getOpcode());

                // Final data fragment
                parser.reset();
                writeBytes(buf, (byte) 0x80, (byte) 0x01, (byte) '!');
                parser.parse(buf, buf + 3);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

    @Test
    public void testOpcodeIsControlFrame() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertFalse(WebSocketOpcode.isControlFrame(WebSocketOpcode.CONTINUATION));
            Assert.assertFalse(WebSocketOpcode.isControlFrame(WebSocketOpcode.TEXT));
            Assert.assertFalse(WebSocketOpcode.isControlFrame(WebSocketOpcode.BINARY));
            Assert.assertTrue(WebSocketOpcode.isControlFrame(WebSocketOpcode.CLOSE));
            Assert.assertTrue(WebSocketOpcode.isControlFrame(WebSocketOpcode.PING));
            Assert.assertTrue(WebSocketOpcode.isControlFrame(WebSocketOpcode.PONG));
        });
    }

    @Test
    public void testOpcodeIsDataFrame() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(WebSocketOpcode.isDataFrame(WebSocketOpcode.CONTINUATION));
            Assert.assertTrue(WebSocketOpcode.isDataFrame(WebSocketOpcode.TEXT));
            Assert.assertTrue(WebSocketOpcode.isDataFrame(WebSocketOpcode.BINARY));
            Assert.assertFalse(WebSocketOpcode.isDataFrame(WebSocketOpcode.CLOSE));
            Assert.assertFalse(WebSocketOpcode.isDataFrame(WebSocketOpcode.PING));
            Assert.assertFalse(WebSocketOpcode.isDataFrame(WebSocketOpcode.PONG));
        });
    }

    @Test
    public void testOpcodeIsValid() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.CONTINUATION));
            Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.TEXT));
            Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.BINARY));
            Assert.assertFalse(WebSocketOpcode.isValid(3));
            Assert.assertFalse(WebSocketOpcode.isValid(4));
            Assert.assertFalse(WebSocketOpcode.isValid(5));
            Assert.assertFalse(WebSocketOpcode.isValid(6));
            Assert.assertFalse(WebSocketOpcode.isValid(7));
            Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.CLOSE));
            Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.PING));
            Assert.assertTrue(WebSocketOpcode.isValid(WebSocketOpcode.PONG));
            Assert.assertFalse(WebSocketOpcode.isValid(0xB));
            Assert.assertFalse(WebSocketOpcode.isValid(0xF));
        });
    }

    @Test
    public void testParse16BitLength() throws Exception {
        assertMemoryLeak(() -> {
            int payloadLen = 1000;
            long buf = allocateBuffer(payloadLen + 16);
            try {
                writeBytes(buf,
                        (byte) 0x82,           // FIN + BINARY
                        (byte) 126,            // 16-bit length follows
                        (byte) (payloadLen >> 8),   // Length high byte
                        (byte) (payloadLen & 0xFF)  // Length low byte
                );

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 4 + payloadLen);

                Assert.assertEquals(4 + payloadLen, consumed);
                Assert.assertEquals(payloadLen, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, payloadLen + 16);
            }
        });
    }

    @Test
    public void testParse64BitLength() throws Exception {
        assertMemoryLeak(() -> {
            long payloadLen = 70_000L;
            long buf = allocateBuffer((int) payloadLen + 16);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0x82);
                Unsafe.getUnsafe().putByte(buf + 1, (byte) 127);
                Unsafe.getUnsafe().putLong(buf + 2, Long.reverseBytes(payloadLen));

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 10 + payloadLen);

                Assert.assertEquals(10 + payloadLen, consumed);
                Assert.assertEquals(payloadLen, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, (int) payloadLen + 16);
            }
        });
    }

    @Test
    public void testParse7BitLength() throws Exception {
        assertMemoryLeak(() -> {
            for (int len = 0; len <= 125; len++) {
                long buf = allocateBuffer(256);
                try {
                    writeBytes(buf, (byte) 0x82, (byte) len);
                    for (int i = 0; i < len; i++) {
                        Unsafe.getUnsafe().putByte(buf + 2 + i, (byte) i);
                    }

                    WebSocketFrameParser parser = new WebSocketFrameParser();
                    int consumed = parser.parse(buf, buf + 2 + len);

                    Assert.assertEquals(2 + len, consumed);
                    Assert.assertEquals(len, parser.getPayloadLength());
                } finally {
                    freeBuffer(buf, 256);
                }
            }
        });
    }

    @Test
    public void testParseBinaryFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseCloseFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf,
                        (byte) 0x88,  // FIN + CLOSE
                        (byte) 0x02,  // Length 2 (just the code)
                        (byte) 0x03, (byte) 0xE8  // 1000 in big-endian
                );

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 4);

                Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
                Assert.assertEquals(2, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseCloseFrameEmpty() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x88, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
                Assert.assertEquals(0, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseCloseFrameWithReason() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                String reason = "Normal closure";
                byte[] reasonBytes = reason.getBytes(StandardCharsets.UTF_8);

                Unsafe.getUnsafe().putByte(buf, (byte) 0x88);
                Unsafe.getUnsafe().putByte(buf + 1, (byte) (2 + reasonBytes.length));
                Unsafe.getUnsafe().putShort(buf + 2, Short.reverseBytes((short) 1000));
                for (int i = 0; i < reasonBytes.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + 4 + i, reasonBytes[i]);
                }

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 4 + reasonBytes.length);

                Assert.assertEquals(WebSocketOpcode.CLOSE, parser.getOpcode());
                Assert.assertEquals(2 + reasonBytes.length, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

    @Test
    public void testParseContinuationFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x00, (byte) 0x05, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04, (byte) 0x05);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 7);

                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
                Assert.assertFalse(parser.isFin());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseEmptyBuffer() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf);

                Assert.assertEquals(0, consumed);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseEmptyPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 2);

                Assert.assertEquals(2, consumed);
                Assert.assertEquals(0, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseFragmentedMessage() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(64);
            try {
                // First fragment: opcode=TEXT, FIN=0
                writeBytes(buf, (byte) 0x01, (byte) 0x03, (byte) 'H', (byte) 'e', (byte) 'l');

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 5);

                Assert.assertEquals(5, consumed);
                Assert.assertFalse(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());

                // Continuation: opcode=CONTINUATION, FIN=0
                parser.reset();
                writeBytes(buf, (byte) 0x00, (byte) 0x02, (byte) 'l', (byte) 'o');
                consumed = parser.parse(buf, buf + 4);

                Assert.assertEquals(4, consumed);
                Assert.assertFalse(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());

                // Final fragment: opcode=CONTINUATION, FIN=1
                parser.reset();
                writeBytes(buf, (byte) 0x80, (byte) 0x01, (byte) '!');
                consumed = parser.parse(buf, buf + 3);

                Assert.assertEquals(3, consumed);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.CONTINUATION, parser.getOpcode());
            } finally {
                freeBuffer(buf, 64);
            }
        });
    }

    @Test
    public void testParseIncompleteHeader16BitLength() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 126, (byte) 0x01);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 3);

                Assert.assertEquals(0, consumed);
                Assert.assertEquals(WebSocketFrameParser.STATE_NEED_MORE, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompleteHeader1Byte() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 1);

                Assert.assertEquals(0, consumed);
                Assert.assertEquals(WebSocketFrameParser.STATE_NEED_MORE, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompleteHeader64BitLength() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 127, (byte) 0, (byte) 0, (byte) 0, (byte) 0);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 6);

                Assert.assertEquals(0, consumed);
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseIncompletePayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x05, (byte) 0x01, (byte) 0x02);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 4);

                Assert.assertEquals(2, consumed);
                Assert.assertEquals(5, parser.getPayloadLength());
                Assert.assertEquals(WebSocketFrameParser.STATE_NEED_PAYLOAD, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseMaxControlFrameSize() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0x89);  // PING
                Unsafe.getUnsafe().putByte(buf + 1, (byte) 125);
                for (int i = 0; i < 125; i++) {
                    Unsafe.getUnsafe().putByte(buf + 2 + i, (byte) i);
                }

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 127);

                Assert.assertEquals(127, consumed);
                Assert.assertEquals(125, parser.getPayloadLength());
                Assert.assertNotEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }

    @Test
    public void testParseMinimalFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x01, (byte) 0xFF);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + 3);

                Assert.assertEquals(3, consumed);
                Assert.assertTrue(parser.isFin());
                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
                Assert.assertEquals(1, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseMultipleFramesInBuffer() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(32);
            try {
                writeBytes(buf,
                        (byte) 0x82, (byte) 0x02, (byte) 0x01, (byte) 0x02,
                        (byte) 0x81, (byte) 0x03, (byte) 'a', (byte) 'b', (byte) 'c'
                );

                WebSocketFrameParser parser = new WebSocketFrameParser();

                int consumed = parser.parse(buf, buf + 9);
                Assert.assertEquals(4, consumed);
                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
                Assert.assertEquals(2, parser.getPayloadLength());

                parser.reset();
                consumed = parser.parse(buf + 4, buf + 9);
                Assert.assertEquals(5, consumed);
                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());
                Assert.assertEquals(3, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 32);
            }
        });
    }

    @Test
    public void testParsePingFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x89, (byte) 0x04, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 6);

                Assert.assertEquals(WebSocketOpcode.PING, parser.getOpcode());
                Assert.assertEquals(4, parser.getPayloadLength());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParsePongFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x8A, (byte) 0x04, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 6);

                Assert.assertEquals(WebSocketOpcode.PONG, parser.getOpcode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testParseTextFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x81, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketOpcode.TEXT, parser.getOpcode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectCloseFrameWith1BytePayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x88, (byte) 0x01, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 3);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectFragmentedControlFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x09, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectMaskedFrame() throws Exception {
        assertMemoryLeak(() -> {
            // Client-side parser rejects masked frames from the server
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x81, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0xFF);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 7);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectOversizeControlFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(256);
            try {
                writeBytes(buf, (byte) 0x89, (byte) 126, (byte) 0x00, (byte) 0x7E);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 4);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 256);
            }
        });
    }

    @Test
    public void testRejectRSV2Bit() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0xA2, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectRSV3Bit() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x92, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectReservedBits() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0xC2, (byte) 0x00);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 2);

                Assert.assertEquals(WebSocketFrameParser.STATE_ERROR, parser.getState());
                Assert.assertEquals(WebSocketCloseCode.PROTOCOL_ERROR, parser.getErrorCode());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    @Test
    public void testRejectUnknownOpcode() throws Exception {
        assertMemoryLeak(() -> {
            for (int opcode : new int[]{3, 4, 5, 6, 7, 0xB, 0xC, 0xD, 0xE, 0xF}) {
                long buf = allocateBuffer(16);
                try {
                    writeBytes(buf, (byte) (0x80 | opcode), (byte) 0x00);

                    WebSocketFrameParser parser = new WebSocketFrameParser();
                    parser.parse(buf, buf + 2);

                    Assert.assertEquals("Opcode " + opcode + " should be rejected",
                            WebSocketFrameParser.STATE_ERROR, parser.getState());
                } finally {
                    freeBuffer(buf, 16);
                }
            }
        });
    }

    @Test
    public void testReset() throws Exception {
        assertMemoryLeak(() -> {
            long buf = allocateBuffer(16);
            try {
                writeBytes(buf, (byte) 0x82, (byte) 0x02, (byte) 0x01, (byte) 0x02);

                WebSocketFrameParser parser = new WebSocketFrameParser();
                parser.parse(buf, buf + 4);

                Assert.assertEquals(WebSocketOpcode.BINARY, parser.getOpcode());
                Assert.assertEquals(2, parser.getPayloadLength());

                parser.reset();

                Assert.assertEquals(0, parser.getOpcode());
                Assert.assertEquals(0, parser.getPayloadLength());
                Assert.assertEquals(WebSocketFrameParser.STATE_HEADER, parser.getState());
            } finally {
                freeBuffer(buf, 16);
            }
        });
    }

    private static long allocateBuffer(int size) {
        return Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
    }

    private static void freeBuffer(long address, int size) {
        Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
    }

    private static void writeBytes(long address, byte... bytes) {
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
        }
    }
}
