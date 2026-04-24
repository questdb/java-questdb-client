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
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaDecoder;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Coverage for {@link QwpGorillaDecoder}. We round-trip timestamp sequences
 * through the matching server-side encoder ({@link QwpGorillaEncoder}) — the
 * encoder is already heavily covered by {@link QwpGorillaEncoderTest}, so it
 * doubles as a known-good wire-format generator that lets us focus on the
 * decoder's read paths. Each test exercises one of the five DoD buckets the
 * encoder picks based on the magnitude of the delta-of-delta value.
 */
public class QwpGorillaDecoderTest {

    @Test
    public void testZeroDodBucket() throws Exception {
        // Constant-stride sequence -> every DoD is 0 -> 1-bit prefix path.
        roundTrip(new long[]{1000L, 2000L, 3000L, 4000L, 5000L, 6000L});
    }

    @Test
    public void test7BitBucket() throws Exception {
        // DoDs in [-64, 63]. Use small jitter on a stable stride.
        roundTrip(new long[]{1000L, 2000L, 3010L, 4030L, 5040L, 6050L});
    }

    @Test
    public void test9BitBucket() throws Exception {
        // DoDs that overflow 7-bit but fit in 9-bit: aim for ~|200|.
        roundTrip(new long[]{1000L, 2000L, 3200L, 4600L, 6200L, 7900L});
    }

    @Test
    public void test12BitBucket() throws Exception {
        // DoDs in (255, 2047]. Multi-thousand-scale jitter.
        roundTrip(new long[]{1000L, 2000L, 4000L, 8000L, 13800L, 19500L, 25500L});
    }

    @Test
    public void test32BitFallback() throws Exception {
        // DoD beyond 12-bit bucket -> 36-bit fallback path.
        roundTrip(new long[]{1_000_000L, 2_000_000L, 3_500_000L, 7_000_000L, 12_000_000L});
    }

    @Test
    public void testNegativeDodSignExtension() throws Exception {
        // Stride that decreases -> negative DoDs -> exercises sign-extension in readSigned.
        roundTrip(new long[]{0L, 10_000L, 19_900L, 29_700L, 39_400L, 48_950L});
    }

    @Test
    public void testMixedBuckets() throws Exception {
        // A heterogeneous sequence forcing the decoder to walk every prefix path
        // within a single column. Specifically:
        //   delta sequence       1000, 1000, 1010, 1210, 5210, 5208
        //   DoD sequence              0,    10,  200, 4000, -2
        long[] timestamps = {0, 1000L, 2000L, 3010L, 4220L, 9430L, 14_638L};
        roundTrip(timestamps);
    }

    @Test
    public void testRandomLongSequenceRoundTrips() throws Exception {
        // Stress-test: 1000 timestamps with a noisy walk. Validates that the
        // decoder produces bit-identical output to the encoder's input across
        // a wide range of bucket transitions and ensureBits refills.
        Random rng = new Random(0xC0FFEE);
        long[] timestamps = new long[1000];
        long t = 1_700_000_000_000_000L;
        long stride = 100;
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = t;
            // jitter the stride to drive different buckets
            stride += rng.nextInt(2001) - 1000;
            t += Math.max(1, stride);
        }
        roundTrip(timestamps);
    }

    @Test
    public void testGetBitPositionPropagatesFromBitReader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long[] timestamps = {0L, 100L, 200L, 300L};
            int srcLen = timestamps.length * 8;
            long src = Unsafe.malloc(srcLen, MemoryTag.NATIVE_DEFAULT);
            int destCap = 256;
            long dest = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < timestamps.length; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, timestamps[i]);
                }
                QwpGorillaEncoder enc = new QwpGorillaEncoder();
                int written = enc.encodeTimestamps(dest, destCap, src, timestamps.length);
                // First two timestamps written raw, bitstream starts at offset 16.
                int bitstreamLen = written - 16;
                QwpGorillaDecoder dec = new QwpGorillaDecoder();
                dec.reset(timestamps[0], timestamps[1], dest + 16, bitstreamLen);
                Assert.assertEquals(0L, dec.getBitPosition());
                dec.decodeNext(); // third timestamp
                long posAfterFirst = dec.getBitPosition();
                Assert.assertTrue("bit position must advance after a decode, was " + posAfterFirst,
                        posAfterFirst > 0);
                dec.decodeNext();
                Assert.assertTrue("bit position must keep advancing on subsequent decodes",
                        dec.getBitPosition() > posAfterFirst);
            } finally {
                Unsafe.free(src, srcLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dest, destCap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDecodePastEndOfEmptyBitstreamThrows() throws Exception {
        // Reset to a zero-length bitstream and verify the very first decodeNext
        // surfaces the bit reader's "read past end" exception. Asking for a
        // value when there are no bytes at all is the unambiguous past-end case;
        // the bit-byte-aligned trailing-zero scenario is not (the trailing 0s
        // happen to be a valid 1-bit "DoD == 0" prefix).
        TestUtils.assertMemoryLeak(() -> {
            QwpGorillaDecoder dec = new QwpGorillaDecoder();
            dec.reset(0L, 100L, 0L, 0);
            try {
                dec.decodeNext();
                Assert.fail("decodeNext on empty bitstream must throw");
            } catch (QwpDecodeException expected) {
                Assert.assertTrue("error mentions read past end: " + expected.getMessage(),
                        expected.getMessage().contains("read past end"));
            }
        });
    }

    @Test
    public void testDecodePastEndOfLargeBucketBitstreamThrows() throws Exception {
        // Sequence whose DoDs land in the 36-bit fallback bucket so each value
        // consumes a known multi-byte chunk. After decoding the encoded values,
        // we keep asking for more until the read past-end check fires. Two
        // successful decodes is enough to land us at end-of-bitstream.
        TestUtils.assertMemoryLeak(() -> {
            long[] timestamps = {1_000_000L, 2_000_000L, 3_500_000L, 7_000_000L};
            int srcLen = timestamps.length * 8;
            long src = Unsafe.malloc(srcLen, MemoryTag.NATIVE_DEFAULT);
            int destCap = 256;
            long dest = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < timestamps.length; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, timestamps[i]);
                }
                QwpGorillaEncoder enc = new QwpGorillaEncoder();
                int written = enc.encodeTimestamps(dest, destCap, src, timestamps.length);
                int bitstreamLen = written - 16;
                QwpGorillaDecoder dec = new QwpGorillaDecoder();
                dec.reset(timestamps[0], timestamps[1], dest + 16, bitstreamLen);
                for (int i = 2; i < timestamps.length; i++) {
                    Assert.assertEquals(timestamps[i], dec.decodeNext());
                }
                // Past-end guarantee depends on the trailing-bit pattern of the
                // last byte. Loop until the bit reader runs out of payload --
                // the bound caps overrun at < 64 attempts, well before a real
                // decoder would ever loop forever.
                boolean threw = false;
                for (int i = 0; i < 64; i++) {
                    try {
                        dec.decodeNext();
                    } catch (QwpDecodeException expected) {
                        threw = true;
                        Assert.assertTrue("error mentions read past end: " + expected.getMessage(),
                                expected.getMessage().contains("read past end"));
                        break;
                    }
                }
                Assert.assertTrue("decodeNext must eventually throw past end of bitstream", threw);
            } finally {
                Unsafe.free(src, srcLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dest, destCap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResetBetweenColumnsForgetsPreviousState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Encode-and-decode column A. Then reset to column B with completely
            // different seed timestamps and verify B decodes correctly -- i.e.
            // that prevDelta / prevTimestamp from A leaked into B.
            long[] colA = {1L, 2L, 3L, 4L};
            long[] colB = {10_000L, 20_000L, 30_000L, 40_000L};

            int srcLen = 8 * Math.max(colA.length, colB.length);
            long src = Unsafe.malloc(srcLen, MemoryTag.NATIVE_DEFAULT);
            int destCap = 64;
            long destA = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            long destB = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            try {
                QwpGorillaEncoder encA = new QwpGorillaEncoder();
                QwpGorillaEncoder encB = new QwpGorillaEncoder();
                for (int i = 0; i < colA.length; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, colA[i]);
                }
                int writtenA = encA.encodeTimestamps(destA, destCap, src, colA.length);
                for (int i = 0; i < colB.length; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, colB[i]);
                }
                int writtenB = encB.encodeTimestamps(destB, destCap, src, colB.length);

                QwpGorillaDecoder dec = new QwpGorillaDecoder();
                dec.reset(colA[0], colA[1], destA + 16, writtenA - 16);
                for (int i = 2; i < colA.length; i++) {
                    Assert.assertEquals(colA[i], dec.decodeNext());
                }
                // Re-bind the decoder to column B with its own seed timestamps.
                dec.reset(colB[0], colB[1], destB + 16, writtenB - 16);
                for (int i = 2; i < colB.length; i++) {
                    Assert.assertEquals("col B index " + i, colB[i], dec.decodeNext());
                }
            } finally {
                Unsafe.free(src, srcLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destA, destCap, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(destB, destCap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Encodes {@code timestamps} with {@link QwpGorillaEncoder}, decodes with
     * {@link QwpGorillaDecoder}, and asserts every value round-trips. Skips the
     * first two timestamps because the encoder ships them uncompressed at the
     * head of the buffer; the decoder starts at the bitstream after them.
     */
    private void roundTrip(long[] timestamps) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int srcLen = timestamps.length * 8;
            long src = Unsafe.malloc(srcLen, MemoryTag.NATIVE_DEFAULT);
            // Bitstream worst-case: 36 bits per timestamp + 16 bytes of seeds.
            int destCap = 16 + (timestamps.length * 36 + 7) / 8 + 16;
            long dest = Unsafe.malloc(destCap, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < timestamps.length; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, timestamps[i]);
                }
                Assert.assertTrue("test sequence must satisfy canUseGorilla precondition",
                        QwpGorillaEncoder.canUseGorilla(src, timestamps.length));
                QwpGorillaEncoder enc = new QwpGorillaEncoder();
                int written = enc.encodeTimestamps(dest, destCap, src, timestamps.length);
                int bitstreamLen = written - 16;

                QwpGorillaDecoder dec = new QwpGorillaDecoder();
                dec.reset(timestamps[0], timestamps[1], dest + 16, bitstreamLen);
                for (int i = 2; i < timestamps.length; i++) {
                    long got = dec.decodeNext();
                    Assert.assertEquals("timestamp index " + i, timestamps[i], got);
                }
            } finally {
                Unsafe.free(src, srcLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(dest, destCap, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
