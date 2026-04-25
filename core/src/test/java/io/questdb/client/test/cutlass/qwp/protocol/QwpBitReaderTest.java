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

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.protocol.QwpBitReader;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Bit-level coverage for {@link QwpBitReader}, the LSB-first reader the QWP
 * Gorilla decoder is built on. Bytes are pushed into the buffer LSB-first, so
 * for the byte {@code 0b1010_0001} the reader yields {@code 1, 0, 0, 0, 0, 1, 0, 1}
 * — bit 0 first. We craft small native byte arrays, exercise every path that
 * the decoder hits in production (single-bit, multi-bit, refill, sign-extend,
 * end-of-stream), and assert the bit position advances correctly so callers
 * downstream can detect truncated columns.
 */
public class QwpBitReaderTest {

    @Test
    public void testReadBitPastEndThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0xFF);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 1);
                for (int i = 0; i < 8; i++) {
                    r.readBit();
                }
                try {
                    r.readBit();
                    Assert.fail("readBit past end must throw");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue("error mentions read past end: " + expected.getMessage(),
                            expected.getMessage().contains("read past end"));
                }
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitYieldsLsbFirstAcrossMultipleBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 0b10100001 then 0b00000010 -- bits LSB-first:
            //   byte 0: 1 0 0 0 0 1 0 1
            //   byte 1: 0 1 0 0 0 0 0 0
            int len = 2;
            long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0b10100001);
                Unsafe.getUnsafe().putByte(buf + 1, (byte) 0b00000010);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, len);
                int[] expected = {1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0};
                for (int i = 0; i < expected.length; i++) {
                    Assert.assertEquals("bit " + i, expected[i], r.readBit());
                    Assert.assertEquals("position after reading " + (i + 1) + " bits",
                            i + 1, r.getBitPosition());
                }
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBits64ReadsFullWord() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Eight bytes assembling, LSB-first, the long 0x0123456789ABCDEFL.
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                long value = 0x0123456789ABCDEFL;
                for (int i = 0; i < 8; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) ((value >>> (8 * i)) & 0xFF));
                }
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 8);
                long read = r.readBits(64);
                Assert.assertEquals("64-bit read must hit the mask==-1L branch and reproduce input",
                        value, read);
                Assert.assertEquals(64L, r.getBitPosition());
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Regression: a full-width {@code readBits(64)} must clear the bit buffer
     * so the next read sees a clean slate. Java masks the right operand of
     * {@code >>>} by 0x3F for {@code long}, making {@code bitBuffer >>>= 64}
     * a no-op. Without the special-case in the reader, the previous 64 bits
     * remain in {@code bitBuffer}; the next ensureBits OR-fills a fresh byte
     * at bit 0 of that stale buffer, silently corrupting every subsequent
     * read.
     * <p>
     * The two halves of the buffer are deliberately disjoint: 8 bytes of
     * {@code 0xFF} followed by 8 bytes of {@code 0x00}. After reading the
     * first 64 bits ({@code 0xFFFF_FFFF_FFFF_FFFFL}), the reader must report
     * the second 64 bits as exactly {@code 0L}; today's bug surfaces them as
     * {@code -1L} because the stale all-ones buffer is OR'd with the
     * incoming zeros.
     */
    @Test
    public void testReadBits64TwiceDoesNotLeakStaleBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 8; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) 0xFF);
                }
                for (int i = 8; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) 0x00);
                }
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 16);

                long first = r.readBits(64);
                Assert.assertEquals(-1L, first);
                Assert.assertEquals(64L, r.getBitPosition());

                long second = r.readBits(64);
                Assert.assertEquals(
                        "second readBits(64) must reflect the bytes that follow, "
                                + "not the stale buffer left by the no-op shift",
                        0L, second);
                Assert.assertEquals(128L, r.getBitPosition());
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitsAcrossLargeRefill() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 16 bytes: read in arbitrary widths totalling 128 bits, verify position
            // and that no exception fires. Smoke test for the refill loop's exit.
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i & 0xFF));
                }
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 16);
                int[] widths = {1, 7, 13, 19, 23, 33, 32}; // sums to 128
                long totalBits = 0;
                for (int w : widths) {
                    r.readBits(w);
                    totalBits += w;
                    Assert.assertEquals(totalBits, r.getBitPosition());
                }
                // Now exhausted.
                try {
                    r.readBit();
                    Assert.fail("read past 16*8 bits must throw");
                } catch (QwpDecodeException expected) {
                    // expected
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitsArbitraryWidths() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Buffer: 0xFF 0x55 0xAA 0x00 -- 32 bits of mixed pattern.
            long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0xFF);
                Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x55);
                Unsafe.getUnsafe().putByte(buf + 2, (byte) 0xAA);
                Unsafe.getUnsafe().putByte(buf + 3, (byte) 0x00);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 4);

                Assert.assertEquals(0b11111L, r.readBits(5));   // 5 bits from 0xFF
                Assert.assertEquals(0b111L, r.readBits(3));     // remaining 3 bits of 0xFF
                Assert.assertEquals(8L, r.getBitPosition());
                Assert.assertEquals(0x55L, r.readBits(8));      // whole 0x55
                Assert.assertEquals(16L, r.getBitPosition());
                // LSB-first: byte 0xAA contributes bits 0-7 of the result, byte 0x00 contributes bits 8-15.
                Assert.assertEquals(0x00AAL, r.readBits(16));
                Assert.assertEquals(32L, r.getBitPosition());
            } finally {
                Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitsMoreThan64Throws() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
                }
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 16);
                try {
                    r.readBits(65);
                    Assert.fail("readBits(>64) must hit the AssertionError guard");
                } catch (AssertionError expected) {
                    Assert.assertTrue("error mentions 64-bit cap: " + expected.getMessage(),
                            expected.getMessage().contains("64"));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitsPastEndThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 1 byte = 8 bits available; asking for 9 must throw before any reads.
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0xFF);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 1);
                try {
                    r.readBits(9);
                    Assert.fail("readBits beyond available must throw");
                } catch (QwpDecodeException expected) {
                    Assert.assertTrue(expected.getMessage().contains("read past end"));
                }
                // Position should not have advanced.
                Assert.assertEquals(0L, r.getBitPosition());
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitsSpansBufferRefills() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Reading 24 bits in one call forces the inner ensureBits loop to
            // refill across at least two boundary points, since the bit buffer
            // is empty at start and refills 8 bits at a time.
            long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0x01);
                Unsafe.getUnsafe().putByte(buf + 1, (byte) 0x02);
                Unsafe.getUnsafe().putByte(buf + 2, (byte) 0x03);
                Unsafe.getUnsafe().putByte(buf + 3, (byte) 0x00);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 4);
                long v = r.readBits(24);
                // LSB-first: 0x01 | (0x02 << 8) | (0x03 << 16) = 0x030201
                Assert.assertEquals(0x030201L, v);
                Assert.assertEquals(24L, r.getBitPosition());
            } finally {
                Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadBitsZeroBitsReturnsZeroWithoutAdvancing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0xFF);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 1);
                Assert.assertEquals(0L, r.readBits(0));
                Assert.assertEquals(0L, r.getBitPosition());
                // Subsequent read still sees the byte intact.
                Assert.assertEquals(1, r.readBit());
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadSigned64BitsBehavesLikeReadBits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Special-case in readSigned: numBits == 64 skips the sign-extend branch
            // because the value already occupies the full long.
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                long value = 0xFFEEDDCCBBAA9988L; // negative as signed long
                for (int i = 0; i < 8; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) ((value >>> (8 * i)) & 0xFF));
                }
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 8);
                Assert.assertEquals(value, r.readSigned(64));
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadSignedDoesNotExtendWhenMsbClear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Encode +5 in 5 bits (0b00101). MSB clear -> no sign extension.
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0b00000101);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 1);
                Assert.assertEquals(5L, r.readSigned(5));
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testReadSignedExtendsWhenMsbSet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Encode -1 in 5 bits (0b11111). Sign-extend should yield -1L.
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf, (byte) 0b00011111);
                QwpBitReader r = new QwpBitReader();
                r.reset(buf, 1);
                Assert.assertEquals(-1L, r.readSigned(5));
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResetClearsAllState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf1 = Unsafe.malloc(2, MemoryTag.NATIVE_DEFAULT);
            long buf2 = Unsafe.malloc(2, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(buf1, (byte) 0xAB);
                Unsafe.getUnsafe().putByte(buf1 + 1, (byte) 0xCD);
                Unsafe.getUnsafe().putByte(buf2, (byte) 0x12);
                Unsafe.getUnsafe().putByte(buf2 + 1, (byte) 0x34);

                QwpBitReader r = new QwpBitReader();
                r.reset(buf1, 2);
                r.readBits(10);
                Assert.assertEquals(10L, r.getBitPosition());

                // Reset to a fresh buffer; position must drop back to 0 and the
                // first read must come from buf2, not the leftover bit buffer.
                r.reset(buf2, 2);
                Assert.assertEquals(0L, r.getBitPosition());
                Assert.assertEquals(0x12L, r.readBits(8));
                Assert.assertEquals(8L, r.getBitPosition());
            } finally {
                Unsafe.free(buf1, 2, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(buf2, 2, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
