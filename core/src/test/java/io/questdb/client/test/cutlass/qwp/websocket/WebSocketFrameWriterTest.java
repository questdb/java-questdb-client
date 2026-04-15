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

import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class WebSocketFrameWriterTest {

    // headerSize() tests

    @Test
    public void testHeaderSizeSmallPayloadUnmasked() {
        assertEquals(2, WebSocketFrameWriter.headerSize(0, false));
        assertEquals(2, WebSocketFrameWriter.headerSize(1, false));
        assertEquals(2, WebSocketFrameWriter.headerSize(125, false));
    }

    @Test
    public void testHeaderSizeSmallPayloadMasked() {
        assertEquals(6, WebSocketFrameWriter.headerSize(0, true));
        assertEquals(6, WebSocketFrameWriter.headerSize(1, true));
        assertEquals(6, WebSocketFrameWriter.headerSize(125, true));
    }

    @Test
    public void testHeaderSizeMediumPayloadUnmasked() {
        assertEquals(4, WebSocketFrameWriter.headerSize(126, false));
        assertEquals(4, WebSocketFrameWriter.headerSize(1000, false));
        assertEquals(4, WebSocketFrameWriter.headerSize(65_535, false));
    }

    @Test
    public void testHeaderSizeMediumPayloadMasked() {
        assertEquals(8, WebSocketFrameWriter.headerSize(126, true));
        assertEquals(8, WebSocketFrameWriter.headerSize(1000, true));
        assertEquals(8, WebSocketFrameWriter.headerSize(65_535, true));
    }

    @Test
    public void testHeaderSizeLargePayloadUnmasked() {
        assertEquals(10, WebSocketFrameWriter.headerSize(65_536, false));
        assertEquals(10, WebSocketFrameWriter.headerSize(1_000_000, false));
        assertEquals(10, WebSocketFrameWriter.headerSize(Long.MAX_VALUE, false));
    }

    @Test
    public void testHeaderSizeLargePayloadMasked() {
        assertEquals(14, WebSocketFrameWriter.headerSize(65_536, true));
        assertEquals(14, WebSocketFrameWriter.headerSize(1_000_000, true));
        assertEquals(14, WebSocketFrameWriter.headerSize(Long.MAX_VALUE, true));
    }

    @Test
    public void testHeaderSizeBoundaries() {
        // Exact boundaries between length encoding formats
        assertEquals(2, WebSocketFrameWriter.headerSize(125, false));
        assertEquals(4, WebSocketFrameWriter.headerSize(126, false));
        assertEquals(4, WebSocketFrameWriter.headerSize(65_535, false));
        assertEquals(10, WebSocketFrameWriter.headerSize(65_536, false));
    }

    // writeHeader() unmasked tests

    @Test
    public void testWriteHeaderFinBinaryZeroPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 0, false);
                assertEquals(2, written);
                // byte 0: FIN=1 | opcode=0x02 => 0x82
                assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
                // byte 1: mask=0 | length=0 => 0x00
                assertEquals((byte) 0x00, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderNoFinContinuationFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, false, WebSocketOpcode.CONTINUATION, 50, false);
                assertEquals(2, written);
                // byte 0: FIN=0 | opcode=0x00 => 0x00
                assertEquals((byte) 0x00, Unsafe.getUnsafe().getByte(buf));
                // byte 1: mask=0 | length=50 => 0x32
                assertEquals((byte) 50, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderTextFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.TEXT, 100, false);
                assertEquals(2, written);
                assertEquals((byte) 0x81, Unsafe.getUnsafe().getByte(buf)); // FIN | TEXT
                assertEquals((byte) 100, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderPingFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.PING, 5, false);
                assertEquals(2, written);
                assertEquals((byte) 0x89, Unsafe.getUnsafe().getByte(buf)); // FIN | PING
                assertEquals((byte) 5, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderPongFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.PONG, 0, false);
                assertEquals(2, written);
                assertEquals((byte) 0x8A, Unsafe.getUnsafe().getByte(buf)); // FIN | PONG
                assertEquals((byte) 0, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderCloseFrame() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.CLOSE, 2, false);
                assertEquals(2, written);
                assertEquals((byte) 0x88, Unsafe.getUnsafe().getByte(buf)); // FIN | CLOSE
                assertEquals((byte) 2, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderMaxSmallPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 125, false);
                assertEquals(2, written);
                assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
                assertEquals((byte) 125, Unsafe.getUnsafe().getByte(buf + 1));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeader16BitPayloadAtBoundary() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 126, false);
                assertEquals(4, written);
                assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
                assertEquals((byte) 126, Unsafe.getUnsafe().getByte(buf + 1)); // extended length marker
                // 126 in big-endian: 0x00 0x7E
                assertEquals((byte) 0x00, Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) 0x7E, Unsafe.getUnsafe().getByte(buf + 3));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeader16BitPayloadMidRange() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 1000, false);
                assertEquals(4, written);
                assertEquals((byte) 126, Unsafe.getUnsafe().getByte(buf + 1));
                // 1000 = 0x03E8 in big-endian
                assertEquals((byte) 0x03, Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) 0xE8, Unsafe.getUnsafe().getByte(buf + 3));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeader16BitPayloadMax() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 65_535, false);
                assertEquals(4, written);
                assertEquals((byte) 126, Unsafe.getUnsafe().getByte(buf + 1));
                // 65535 = 0xFF 0xFF in big-endian
                assertEquals((byte) 0xFF, Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) 0xFF, Unsafe.getUnsafe().getByte(buf + 3));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeader64BitPayloadAtBoundary() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 65_536, false);
                assertEquals(10, written);
                assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(buf));
                assertEquals((byte) 127, Unsafe.getUnsafe().getByte(buf + 1)); // 64-bit marker
                // 65536 = 0x0000000000010000 in big-endian
                long encodedLen = Long.reverseBytes(Unsafe.getUnsafe().getLong(buf + 2));
                assertEquals(65_536L, encodedLen);
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeader64BitPayloadLargeValue() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                long bigPayload = 1L << 32; // 4GB
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, bigPayload, false);
                assertEquals(10, written);
                assertEquals((byte) 127, Unsafe.getUnsafe().getByte(buf + 1));
                long encodedLen = Long.reverseBytes(Unsafe.getUnsafe().getLong(buf + 2));
                assertEquals(bigPayload, encodedLen);
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderUnmaskedHasNoMaskBit() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 10, false);
                int byte1 = Unsafe.getUnsafe().getByte(buf + 1) & 0xFF;
                assertEquals(0, byte1 & 0x80); // mask bit must be 0
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderMaskedHasMaskBit() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 10, true);
                int byte1 = Unsafe.getUnsafe().getByte(buf + 1) & 0xFF;
                assertEquals(0x80, byte1 & 0x80); // mask bit must be 1
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    // writeHeader() with mask key overload

    @Test
    public void testWriteHeaderWithMaskKeySmallPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int maskKey = 0xDEADBEEF;
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 10, maskKey);
                assertEquals(6, written); // 2 base + 4 mask
                // byte 1: mask=1 | length=10 => 0x8A
                assertEquals((byte) 0x8A, Unsafe.getUnsafe().getByte(buf + 1));
                // mask key in big-endian byte order
                assertEquals((byte) 0xDE, Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) 0xAD, Unsafe.getUnsafe().getByte(buf + 3));
                assertEquals((byte) 0xBE, Unsafe.getUnsafe().getByte(buf + 4));
                assertEquals((byte) 0xEF, Unsafe.getUnsafe().getByte(buf + 5));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderWithMaskKey16BitPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int maskKey = 0x12345678;
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 200, maskKey);
                assertEquals(8, written); // 4 base (16-bit len) + 4 mask
                assertEquals((byte) 126, Unsafe.getUnsafe().getByte(buf + 1) & 0x7F); // length marker
                // 200 in big-endian
                assertEquals((byte) 0x00, Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) 0xC8, Unsafe.getUnsafe().getByte(buf + 3));
                // mask key
                assertEquals((byte) 0x12, Unsafe.getUnsafe().getByte(buf + 4));
                assertEquals((byte) 0x34, Unsafe.getUnsafe().getByte(buf + 5));
                assertEquals((byte) 0x56, Unsafe.getUnsafe().getByte(buf + 6));
                assertEquals((byte) 0x78, Unsafe.getUnsafe().getByte(buf + 7));
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderWithMaskKey64BitPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                int maskKey = 0xAABBCCDD;
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 100_000, maskKey);
                assertEquals(14, written); // 10 base (64-bit len) + 4 mask
                assertEquals((byte) 127, Unsafe.getUnsafe().getByte(buf + 1) & 0x7F);
                long encodedLen = Long.reverseBytes(Unsafe.getUnsafe().getLong(buf + 2));
                assertEquals(100_000L, encodedLen);
                // mask key at offset 10
                assertEquals((byte) 0xAA, Unsafe.getUnsafe().getByte(buf + 10));
                assertEquals((byte) 0xBB, Unsafe.getUnsafe().getByte(buf + 11));
                assertEquals((byte) 0xCC, Unsafe.getUnsafe().getByte(buf + 12));
                assertEquals((byte) 0xDD, Unsafe.getUnsafe().getByte(buf + 13));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderWithMaskKeyZero() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 5, 0x00000000);
                assertEquals(6, written);
                // All mask bytes should be 0
                for (int i = 2; i < 6; i++) {
                    assertEquals((byte) 0, Unsafe.getUnsafe().getByte(buf + i));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderWithMaskKeyAllOnes() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 5, 0xFFFFFFFF);
                assertEquals(6, written);
                for (int i = 2; i < 6; i++) {
                    assertEquals((byte) 0xFF, Unsafe.getUnsafe().getByte(buf + i));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    // maskPayload() tests

    @Test
    public void testMaskPayloadEmptyPayload() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                // Masking zero bytes should be a no-op
                WebSocketFrameWriter.maskPayload(buf, 0, 0xDEADBEEF);
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadSingleByte() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0x42);
                // maskKey big-endian: byte0=0xDE, byte1=0xAD, byte2=0xBE, byte3=0xEF
                // Position 0 XORs with mask byte 0 (0xDE)
                WebSocketFrameWriter.maskPayload(buf, 1, 0xDEADBEEF);
                assertEquals((byte) (0x42 ^ 0xDE), Unsafe.getUnsafe().getByte(buf));
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadFourBytes() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] data = {0x01, 0x02, 0x03, 0x04};
                for (int i = 0; i < 4; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, data[i]);
                }
                int maskKey = 0xAABBCCDD;
                // mask bytes: [0xAA, 0xBB, 0xCC, 0xDD]
                WebSocketFrameWriter.maskPayload(buf, 4, maskKey);

                assertEquals((byte) (0x01 ^ 0xAA), Unsafe.getUnsafe().getByte(buf));
                assertEquals((byte) (0x02 ^ 0xBB), Unsafe.getUnsafe().getByte(buf + 1));
                assertEquals((byte) (0x03 ^ 0xCC), Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) (0x04 ^ 0xDD), Unsafe.getUnsafe().getByte(buf + 3));
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadIsInvolution() throws Exception {
        // XOR masking applied twice restores original data
        assertMemoryLeak(() -> {
            int len = 137; // odd length to hit all code paths (8-byte, 4-byte, byte-by-byte)
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                // Fill with test pattern
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i * 7 + 13));
                }
                // Save original
                byte[] original = new byte[len];
                for (int i = 0; i < len; i++) {
                    original[i] = Unsafe.getUnsafe().getByte(buf + i);
                }

                int maskKey = 0x37A5C9E1;
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);

                // After one mask, data must differ (unless maskKey is 0)
                boolean anyDifferent = false;
                for (int i = 0; i < len; i++) {
                    if (Unsafe.getUnsafe().getByte(buf + i) != original[i]) {
                        anyDifferent = true;
                        break;
                    }
                }
                assertTrue("masking should change the data", anyDifferent);

                // Apply mask again — must restore original
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);
                for (int i = 0; i < len; i++) {
                    assertEquals("byte " + i, original[i], Unsafe.getUnsafe().getByte(buf + i));
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadRfc6455Example() throws Exception {
        // RFC 6455 Section 5.7 example (unmasked "Hello"):
        // mask key = 0x37FA213D => mask bytes [0x37, 0xFA, 0x21, 0x3D]
        // 'H'=0x48, 'e'=0x65, 'l'=0x6C, 'l'=0x6C, 'o'=0x6F
        // masked: 0x48^0x37=0x7F, 0x65^0xFA=0x9F, 0x6C^0x21=0x4D, 0x6C^0x3D=0x51, 0x6F^0x37=0x58
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] hello = "Hello".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
                for (int i = 0; i < hello.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, hello[i]);
                }

                WebSocketFrameWriter.maskPayload(buf, 5, 0x37FA213D);

                assertEquals((byte) 0x7F, Unsafe.getUnsafe().getByte(buf));
                assertEquals((byte) 0x9F, Unsafe.getUnsafe().getByte(buf + 1));
                assertEquals((byte) 0x4D, Unsafe.getUnsafe().getByte(buf + 2));
                assertEquals((byte) 0x51, Unsafe.getUnsafe().getByte(buf + 3));
                assertEquals((byte) 0x58, Unsafe.getUnsafe().getByte(buf + 4));
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadMaskKeyCyclesEvery4Bytes() throws Exception {
        assertMemoryLeak(() -> {
            int len = 13; // tests wrapping: positions 0-3, 4-7, 8-11, 12
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
                }
                int maskKey = 0x01020304;
                // mask bytes: [0x01, 0x02, 0x03, 0x04]
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);

                byte[] expectedMaskBytes = {0x01, 0x02, 0x03, 0x04};
                for (int i = 0; i < len; i++) {
                    assertEquals("byte " + i, expectedMaskBytes[i % 4], Unsafe.getUnsafe().getByte(buf + i));
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadZeroMaskKey() throws Exception {
        // Zero mask means XOR with 0 — data should be unchanged
        assertMemoryLeak(() -> {
            int len = 20;
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i + 1));
                }
                WebSocketFrameWriter.maskPayload(buf, len, 0);
                for (int i = 0; i < len; i++) {
                    assertEquals((byte) (i + 1), Unsafe.getUnsafe().getByte(buf + i));
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaskPayloadExactly8Bytes() throws Exception {
        // Hits the 8-byte chunk path only, no remainder
        assertMaskRoundTrip(8, 0xCAFEBABE);
    }

    @Test
    public void testMaskPayloadExactly4Bytes() throws Exception {
        // Hits the 4-byte chunk path (after 0 8-byte chunks)
        assertMaskRoundTrip(4, 0xDEADC0DE);
    }

    @Test
    public void testMaskPayloadExactly12Bytes() throws Exception {
        // Hits 8-byte chunk (1) then 4-byte chunk (1), no byte remainder
        assertMaskRoundTrip(12, 0x11223344);
    }

    @Test
    public void testMaskPayload1Byte() throws Exception {
        assertMaskRoundTrip(1, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload2Bytes() throws Exception {
        assertMaskRoundTrip(2, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload3Bytes() throws Exception {
        assertMaskRoundTrip(3, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload5Bytes() throws Exception {
        // 0 x 8-byte, 1 x 4-byte, 1 byte remainder
        assertMaskRoundTrip(5, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload7Bytes() throws Exception {
        // 0 x 8-byte, 1 x 4-byte, 3 byte remainder
        assertMaskRoundTrip(7, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload9Bytes() throws Exception {
        // 1 x 8-byte, 0 x 4-byte, 1 byte remainder
        assertMaskRoundTrip(9, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload15Bytes() throws Exception {
        // 1 x 8-byte, 1 x 4-byte, 3 byte remainder
        assertMaskRoundTrip(15, 0xABCDEF01);
    }

    @Test
    public void testMaskPayload16Bytes() throws Exception {
        // 2 x 8-byte, no remainder
        assertMaskRoundTrip(16, 0xABCDEF01);
    }

    @Test
    public void testMaskPayloadLarge() throws Exception {
        // Large payload to stress the 8-byte bulk path
        assertMaskRoundTrip(65_537, 0x55AA55AA);
    }

    // Round-trip: writeHeader + maskPayload -> parser reads back correctly

    @Test
    public void testRoundTripWithParserSmallPayload() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.BINARY, 10, true);
    }

    @Test
    public void testRoundTripWithParserZeroPayload() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.TEXT, 0, true);
    }

    @Test
    public void testRoundTripWithParser16BitPayload() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.BINARY, 200, true);
    }

    @Test
    public void testRoundTripWithParser64BitPayload() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.BINARY, 100_000, true);
    }

    @Test
    public void testRoundTripWithParserUnmaskedSmall() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.BINARY, 50, false);
    }

    @Test
    public void testRoundTripWithParserUnmasked16Bit() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.BINARY, 1000, false);
    }

    @Test
    public void testRoundTripWithParserUnmasked64Bit() throws Exception {
        assertWriteParseRoundTrip(WebSocketOpcode.BINARY, 70_000, false);
    }

    @Test
    public void testRoundTripWithParserPingFrame() throws Exception {
        // Control frames must have FIN=true per RFC 6455
        assertWriteParseRoundTrip(WebSocketOpcode.PING, 5, true);
    }

    @Test
    public void testRoundTripMaskedPayloadDecodes() throws Exception {
        // Write header + mask, write payload, mask it, then verify unmask restores original
        assertMemoryLeak(() -> {
            byte[] payload = "Hello, WebSocket!".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int payloadLen = payload.length;
            int maskKey = 0xBADDCAFE;
            int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
            int totalSize = headerSize + payloadLen;

            long buf = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.TEXT, payloadLen, maskKey);
                assertEquals(headerSize, written);

                // Write payload
                long payloadStart = buf + written;
                for (int i = 0; i < payloadLen; i++) {
                    Unsafe.getUnsafe().putByte(payloadStart + i, payload[i]);
                }

                // Mask it
                WebSocketFrameWriter.maskPayload(payloadStart, payloadLen, maskKey);

                // Unmask to verify original is restored
                WebSocketFrameWriter.maskPayload(payloadStart, payloadLen, maskKey);
                for (int i = 0; i < payloadLen; i++) {
                    assertEquals("payload byte " + i, payload[i], Unsafe.getUnsafe().getByte(payloadStart + i));
                }
            } finally {
                Unsafe.free(buf, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteHeaderReturnValueMatchesHeaderSize() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                // Small unmasked
                assertEquals(
                        WebSocketFrameWriter.headerSize(50, false),
                        WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 50, false)
                );
                // Small masked (with key)
                assertEquals(
                        WebSocketFrameWriter.headerSize(50, true),
                        WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 50, 0x12345678)
                );
                // 16-bit unmasked
                assertEquals(
                        WebSocketFrameWriter.headerSize(300, false),
                        WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 300, false)
                );
                // 16-bit masked (with key)
                assertEquals(
                        WebSocketFrameWriter.headerSize(300, true),
                        WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 300, 0x12345678)
                );
                // 64-bit unmasked
                assertEquals(
                        WebSocketFrameWriter.headerSize(70_000, false),
                        WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 70_000, false)
                );
                // 64-bit masked (with key)
                assertEquals(
                        WebSocketFrameWriter.headerSize(70_000, true),
                        WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.BINARY, 70_000, 0x12345678)
                );
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAllOpcodes() throws Exception {
        // Verify each valid opcode is written correctly in byte 0
        int[] opcodes = {
                WebSocketOpcode.CONTINUATION,  // 0x00
                WebSocketOpcode.TEXT,           // 0x01
                WebSocketOpcode.BINARY,         // 0x02
                WebSocketOpcode.CLOSE,          // 0x08
                WebSocketOpcode.PING,           // 0x09
                WebSocketOpcode.PONG,           // 0x0A
        };
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int opcode : opcodes) {
                    WebSocketFrameWriter.writeHeader(buf, true, opcode, 0, false);
                    int byte0 = Unsafe.getUnsafe().getByte(buf) & 0xFF;
                    assertEquals("opcode " + opcode, opcode, byte0 & 0x0F);
                    assertEquals("FIN bit for opcode " + opcode, 0x80, byte0 & 0x80);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFinBitCombinations() throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // FIN=true
                WebSocketFrameWriter.writeHeader(buf, true, WebSocketOpcode.TEXT, 0, false);
                assertEquals(0x80, Unsafe.getUnsafe().getByte(buf) & 0x80);

                // FIN=false
                WebSocketFrameWriter.writeHeader(buf, false, WebSocketOpcode.TEXT, 0, false);
                assertEquals(0x00, Unsafe.getUnsafe().getByte(buf) & 0x80);
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void assertMaskRoundTrip(int len, int maskKey) throws Exception {
        assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] original = new byte[len];
                for (int i = 0; i < len; i++) {
                    original[i] = (byte) (i * 31 + 17);
                    Unsafe.getUnsafe().putByte(buf + i, original[i]);
                }

                // Mask
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);

                // Verify mask cycling: each byte XORed with mask[(i%4)]
                byte[] maskBytes = {
                        (byte) (maskKey >>> 24),
                        (byte) (maskKey >>> 16),
                        (byte) (maskKey >>> 8),
                        (byte) maskKey
                };
                for (int i = 0; i < len; i++) {
                    byte expected = (byte) (original[i] ^ maskBytes[i % 4]);
                    assertEquals("masked byte " + i, expected, Unsafe.getUnsafe().getByte(buf + i));
                }

                // Unmask (XOR is its own inverse)
                WebSocketFrameWriter.maskPayload(buf, len, maskKey);
                for (int i = 0; i < len; i++) {
                    assertEquals("unmasked byte " + i, original[i], Unsafe.getUnsafe().getByte(buf + i));
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Writes a frame header with writeHeader(), then parses it back with the
     * WebSocketFrameParser to verify round-trip correctness. Unmasked frames
     * are used since the parser rejects masked server frames.
     */
    private static void assertWriteParseRoundTrip(int opcode, int payloadLen, boolean fin) throws Exception {
        assertMemoryLeak(() -> {
            // Parser expects unmasked frames (server->client direction)
            int headerSize = WebSocketFrameWriter.headerSize(payloadLen, false);
            int totalSize = headerSize + payloadLen;
            long buf = Unsafe.malloc(totalSize, MemoryTag.NATIVE_DEFAULT);
            try {
                int written = WebSocketFrameWriter.writeHeader(buf, fin, opcode, payloadLen, false);
                assertEquals(headerSize, written);

                // Fill payload with test data so we allocate the full frame
                for (int i = 0; i < payloadLen; i++) {
                    Unsafe.getUnsafe().putByte(buf + written + i, (byte) (i & 0xFF));
                }

                // Parse the frame
                WebSocketFrameParser parser = new WebSocketFrameParser();
                int consumed = parser.parse(buf, buf + totalSize);
                assertEquals("parser state", WebSocketFrameParser.STATE_COMPLETE, parser.getState());
                assertEquals("consumed bytes", totalSize, consumed);
                assertEquals("opcode", opcode, parser.getOpcode());
                assertEquals("payload length", payloadLen, parser.getPayloadLength());
                assertEquals("fin", fin, parser.isFin());
                assertEquals("header size", headerSize, parser.getHeaderSize());
            } finally {
                Unsafe.free(buf, totalSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
