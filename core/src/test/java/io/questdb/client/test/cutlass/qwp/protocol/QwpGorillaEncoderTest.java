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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class QwpGorillaEncoderTest {

    @Test
    public void testToIntCheckedAtBoundary() {
        // Exactly at Integer.MAX_VALUE — must succeed
        assertEquals(Integer.MAX_VALUE, QwpGorillaEncoder.toIntChecked(Integer.MAX_VALUE));
    }

    @Test
    public void testToIntCheckedJustAboveBoundary() {
        // One above Integer.MAX_VALUE — must throw
        try {
            QwpGorillaEncoder.toIntChecked((long) Integer.MAX_VALUE + 1);
            fail("expected LineSenderException");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("exceeds int range"));
        }
    }

    @Test
    public void testToIntCheckedLargeValue() {
        try {
            QwpGorillaEncoder.toIntChecked(Long.MAX_VALUE);
            fail("expected LineSenderException");
        } catch (LineSenderException e) {
            assertTrue(e.getMessage().contains("exceeds int range"));
        }
    }

    @Test
    public void testBucketBoundariesExact() {
        // Bucket 0: DoD == 0
        assertEquals(0, QwpGorillaEncoder.getBucket(0));

        // Bucket 1: [-64, 63] (7-bit signed)
        assertEquals(1, QwpGorillaEncoder.getBucket(-64));
        assertEquals(1, QwpGorillaEncoder.getBucket(63));
        assertEquals(1, QwpGorillaEncoder.getBucket(-1));
        assertEquals(1, QwpGorillaEncoder.getBucket(1));

        // Bucket 2: [-256, 255] (9-bit signed)
        assertEquals(2, QwpGorillaEncoder.getBucket(-65));
        assertEquals(2, QwpGorillaEncoder.getBucket(64));
        assertEquals(2, QwpGorillaEncoder.getBucket(-256));
        assertEquals(2, QwpGorillaEncoder.getBucket(255));

        // Bucket 3: [-2048, 2047] (12-bit signed)
        assertEquals(3, QwpGorillaEncoder.getBucket(-257));
        assertEquals(3, QwpGorillaEncoder.getBucket(256));
        assertEquals(3, QwpGorillaEncoder.getBucket(-2048));
        assertEquals(3, QwpGorillaEncoder.getBucket(2047));

        // Bucket 4: everything else (32-bit signed)
        assertEquals(4, QwpGorillaEncoder.getBucket(-2049));
        assertEquals(4, QwpGorillaEncoder.getBucket(2048));
        assertEquals(4, QwpGorillaEncoder.getBucket(Integer.MIN_VALUE));
        assertEquals(4, QwpGorillaEncoder.getBucket(Integer.MAX_VALUE));
    }

    @Test
    public void testBitsRequiredPerBucket() {
        assertEquals(1, QwpGorillaEncoder.getBitsRequired(0));        // bucket 0
        assertEquals(9, QwpGorillaEncoder.getBitsRequired(1));        // bucket 1
        assertEquals(9, QwpGorillaEncoder.getBitsRequired(-64));      // bucket 1
        assertEquals(9, QwpGorillaEncoder.getBitsRequired(63));       // bucket 1
        assertEquals(12, QwpGorillaEncoder.getBitsRequired(-65));     // bucket 2
        assertEquals(12, QwpGorillaEncoder.getBitsRequired(255));     // bucket 2
        assertEquals(16, QwpGorillaEncoder.getBitsRequired(-257));    // bucket 3
        assertEquals(16, QwpGorillaEncoder.getBitsRequired(2047));    // bucket 3
        assertEquals(36, QwpGorillaEncoder.getBitsRequired(-2049));   // bucket 4
        assertEquals(36, QwpGorillaEncoder.getBitsRequired(100_000)); // bucket 4
    }

    @Test
    public void testCanUseGorillaZeroTimestamps() {
        assertTrue(QwpGorillaEncoder.canUseGorilla(0, 0));
    }

    @Test
    public void testCanUseGorillaOneTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(ptr, 1_000_000L);
                assertTrue(QwpGorillaEncoder.canUseGorilla(ptr, 1));
            } finally {
                Unsafe.free(ptr, 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaTwoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(16, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(ptr, 1_000_000L);
                Unsafe.getUnsafe().putLong(ptr + 8, 2_000_000L);
                assertTrue(QwpGorillaEncoder.canUseGorilla(ptr, 2));
            } finally {
                Unsafe.free(ptr, 16, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaReturnsFalseWhenDodExceedsIntRange() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(24, MemoryTag.NATIVE_ILP_RSS);
            try {
                // delta0 = 0, delta1 = Long.MAX_VALUE => DoD = Long.MAX_VALUE - 0
                Unsafe.getUnsafe().putLong(ptr, 0);
                Unsafe.getUnsafe().putLong(ptr + 8, 0);
                Unsafe.getUnsafe().putLong(ptr + 16, Long.MAX_VALUE);
                assertFalse(QwpGorillaEncoder.canUseGorilla(ptr, 3));
            } finally {
                Unsafe.free(ptr, 24, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaReturnsFalseForNegativeOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(24, MemoryTag.NATIVE_ILP_RSS);
            try {
                // delta0 = Long.MAX_VALUE, delta1 = 0 => DoD = 0 - Long.MAX_VALUE
                Unsafe.getUnsafe().putLong(ptr, 0);
                Unsafe.getUnsafe().putLong(ptr + 8, Long.MAX_VALUE);
                Unsafe.getUnsafe().putLong(ptr + 16, Long.MAX_VALUE);
                assertFalse(QwpGorillaEncoder.canUseGorilla(ptr, 3));
            } finally {
                Unsafe.free(ptr, 24, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCanUseGorillaReturnsTrueAtIntBoundary() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(24, MemoryTag.NATIVE_ILP_RSS);
            try {
                // delta0 = 0, delta1 = Integer.MAX_VALUE => DoD exactly at int boundary
                Unsafe.getUnsafe().putLong(ptr, 0);
                Unsafe.getUnsafe().putLong(ptr + 8, 0);
                Unsafe.getUnsafe().putLong(ptr + 16, Integer.MAX_VALUE);
                assertTrue(QwpGorillaEncoder.canUseGorilla(ptr, 3));
            } finally {
                Unsafe.free(ptr, 24, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeZeroTimestamps() {
        assertEquals(0, QwpGorillaEncoder.calculateEncodedSize(0, 0));
    }

    @Test
    public void testCalculateEncodedSizeOneTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(ptr, 12345L);
                assertEquals(8, QwpGorillaEncoder.calculateEncodedSize(ptr, 1));
            } finally {
                Unsafe.free(ptr, 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeTwoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(16, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(ptr, 1000L);
                Unsafe.getUnsafe().putLong(ptr + 8, 2000L);
                assertEquals(16, QwpGorillaEncoder.calculateEncodedSize(ptr, 2));
            } finally {
                Unsafe.free(ptr, 16, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeConstantDelta() throws Exception {
        // Constant delta => DoD always 0 => 1 bit per timestamp after first two
        assertMemoryLeak(() -> {
            int count = 100;
            long ptr = Unsafe.malloc((long) count * 8, MemoryTag.NATIVE_ILP_RSS);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(ptr + (long) i * 8, i * 1000L);
                }
                int size = QwpGorillaEncoder.calculateEncodedSize(ptr, count);
                // 16 bytes (two uncompressed) + ceil(98 * 1 / 8) = 16 + 13 = 29
                assertEquals(16 + (98 + 7) / 8, size);
            } finally {
                Unsafe.free(ptr, (long) count * 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testCalculateEncodedSizeDoesNotOverflowWithLargeCount() throws Exception {
        // With int totalBits, overflow occurs at ~59.6M timestamps when every
        // DoD hits the worst-case 36-bit bucket. Use 60M entries to trigger it.
        final int count = 60_000_000;
        final long sizeBytes = (long) count * 8;

        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(sizeBytes, MemoryTag.NATIVE_ILP_RSS);
            try {
                long ts = 0;
                long delta = 1;
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(ptr + (long) i * 8, ts);
                    ts += delta;
                    delta = (i % 2 == 0) ? 10_001 : 1;
                }

                assertTrue(QwpGorillaEncoder.canUseGorilla(ptr, count));

                int encodedSize = QwpGorillaEncoder.calculateEncodedSize(ptr, count);

                long expectedMinBits = 36L * (count - 2);
                long expectedMinSize = 16 + (expectedMinBits + 7) / 8;
                assertTrue("encoded size must be positive, got " + encodedSize, encodedSize > 0);
                assertTrue(
                        "encoded size " + encodedSize + " is smaller than expected minimum " + expectedMinSize,
                        encodedSize >= expectedMinSize
                );
            } finally {
                Unsafe.free(ptr, sizeBytes, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeZeroTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            assertEquals(0, encoder.encodeTimestamps(0, 0, 0, 0));
        });
    }

    @Test
    public void testEncodeOneTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long src = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(64, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(src, 42_000_000L);

                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                int written = encoder.encodeTimestamps(dst, 64, src, 1);

                assertEquals(8, written);
                assertEquals(42_000_000L, Unsafe.getUnsafe().getLong(dst));
            } finally {
                Unsafe.free(src, 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 64, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeTwoTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            long src = Unsafe.malloc(16, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(64, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(src, 1_000_000L);
                Unsafe.getUnsafe().putLong(src + 8, 2_000_000L);

                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                int written = encoder.encodeTimestamps(dst, 64, src, 2);

                assertEquals(16, written);
                assertEquals(1_000_000L, Unsafe.getUnsafe().getLong(dst));
                assertEquals(2_000_000L, Unsafe.getUnsafe().getLong(dst + 8));
            } finally {
                Unsafe.free(src, 16, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 64, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testRoundTripConstantDelta() throws Exception {
        // All DoDs are 0 => bucket 0 (1-bit each)
        long[] timestamps = new long[50];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1_000_000L + i * 1000L;
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripIdenticalTimestamps() throws Exception {
        // All values identical => delta=0, DoD=0
        long[] timestamps = new long[20];
        Arrays.fill(timestamps, 42_000_000L);
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripThreeTimestampsZeroDod() throws Exception {
        // Minimal DoD case: 3 timestamps, constant delta
        assertRoundTrip(new long[]{100, 200, 300});
    }

    @Test
    public void testRoundTripBucket1SmallPositiveDod() throws Exception {
        // DoDs in [1, 63] => bucket 1
        assertRoundTrip(new long[]{0, 100, 201, 303, 406, 510});
    }

    @Test
    public void testRoundTripBucket1SmallNegativeDod() throws Exception {
        // DoDs in [-64, -1] => bucket 1
        assertRoundTrip(new long[]{0, 1000, 1999, 2997, 3994, 4990});
    }

    @Test
    public void testRoundTripBucket1AtBoundaries() throws Exception {
        // DoD exactly -64 and 63
        // t0=0, t1=100 => delta0=100
        // t2=100+163=263 => delta1=163, DoD=63
        // t3=263+99=362 => delta2=99, DoD=-64
        assertRoundTrip(new long[]{0, 100, 263, 362});
    }

    @Test
    public void testRoundTripBucket2MediumDod() throws Exception {
        // DoDs in [-256, -65] and [64, 255] => bucket 2
        // delta0=100, delta1=200 => DoD=100 (bucket 2)
        // delta2=0 => DoD=-200 (bucket 2)
        assertRoundTrip(new long[]{0, 100, 300, 300});
    }

    @Test
    public void testRoundTripBucket2AtBoundaries() throws Exception {
        // DoD exactly -256 and 255
        // t0=0, t1=1000 => delta0=1000
        // t2=1000+1255=2255 => delta1=1255, DoD=255
        // t3=2255+999=3254 => delta2=999, DoD=-256
        assertRoundTrip(new long[]{0, 1000, 2255, 3254});
    }

    @Test
    public void testRoundTripBucket3LargeDod() throws Exception {
        // DoDs in [-2048, -257] and [256, 2047] => bucket 3
        assertRoundTrip(new long[]{0, 1000, 2500, 2500});
    }

    @Test
    public void testRoundTripBucket3AtBoundaries() throws Exception {
        // DoD exactly -2048 and 2047
        // t0=0, t1=10_000 => delta0=10_000
        // t2=10_000+12_047=22_047 => delta1=12_047, DoD=2047
        // t3=22_047+9999=32_046 => delta2=9999, DoD=-2048
        assertRoundTrip(new long[]{0, 10_000, 22_047, 32_046});
    }

    @Test
    public void testRoundTripBucket4VeryLargeDod() throws Exception {
        // DoDs > 2047 or < -2048 => bucket 4 (32-bit)
        assertRoundTrip(new long[]{0, 1000, 1_000_000, 1_000_000});
    }

    @Test
    public void testRoundTripBucket4AtIntMaxBoundary() throws Exception {
        // DoD at Integer.MAX_VALUE
        // t0=0, t1=0 => delta0=0
        // t2=Integer.MAX_VALUE => delta1=Integer.MAX_VALUE, DoD=Integer.MAX_VALUE
        assertRoundTrip(new long[]{0, 0, Integer.MAX_VALUE});
    }

    @Test
    public void testRoundTripBucket4AtIntMinBoundary() throws Exception {
        // DoD at Integer.MIN_VALUE
        // t0=0, t1=Integer.MAX_VALUE => delta0=Integer.MAX_VALUE
        // t2=Integer.MAX_VALUE => delta1=0, DoD = 0 - Integer.MAX_VALUE = -Integer.MAX_VALUE
        // Need DoD = Integer.MIN_VALUE:
        // t0=0, t1=0 => delta0=0
        // t2=Integer.MIN_VALUE => delta1=Integer.MIN_VALUE, DoD=Integer.MIN_VALUE
        assertRoundTrip(new long[]{0, 0, (long) Integer.MIN_VALUE});
    }

    @Test
    public void testRoundTripAllBucketsInOneSequence() throws Exception {
        // Construct a sequence that exercises all 5 bucket sizes
        long[] ts = new long[12];
        ts[0] = 0;
        ts[1] = 1000;          // delta=1000
        ts[2] = 2000;          // delta=1000, DoD=0 (bucket 0)
        ts[3] = 3010;          // delta=1010, DoD=10 (bucket 1)
        ts[4] = 4120;          // delta=1110, DoD=100 (bucket 2)
        ts[5] = 6230;          // delta=2110, DoD=1000 (bucket 3)
        ts[6] = 16_230;        // delta=10_000, DoD=7890 (bucket 4)
        ts[7] = 26_230;        // delta=10_000, DoD=0 (bucket 0)
        ts[8] = 36_200;        // delta=9970, DoD=-30 (bucket 1)
        ts[9] = 45_970;        // delta=9770, DoD=-200 (bucket 2)
        ts[10] = 54_740;       // delta=8770, DoD=-1000 (bucket 3)
        ts[11] = 54_740;       // delta=0, DoD=-8770 (bucket 4)
        assertRoundTrip(ts);
    }

    @Test
    public void testRoundTripNegativeTimestamps() throws Exception {
        assertRoundTrip(new long[]{-3000, -2000, -1000, 0, 1000, 2000});
    }

    @Test
    public void testRoundTripLargeTimestampsRealisticMicros() throws Exception {
        // Realistic timestamps: 2024-01-01 with 1-second intervals
        long base = 1_704_067_200_000_000L; // 2024-01-01 in micros
        long[] timestamps = new long[100];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = base + i * 1_000_000L;
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripRealisticNanosWithJitter() throws Exception {
        // Realistic nanos with small jitter => mostly bucket 1
        long base = 1_704_067_200_000_000_000L;
        long[] timestamps = new long[200];
        long interval = 1_000_000L; // 1ms in nanos
        for (int i = 0; i < timestamps.length; i++) {
            long jitter = (i % 7) * 100 - 300; // -300 to +300 nanos
            timestamps[i] = base + i * interval + jitter;
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripMonotonicDecreasing() throws Exception {
        long[] timestamps = new long[30];
        for (int i = 0; i < timestamps.length; i++) {
            timestamps[i] = 1_000_000L - i * 100L;
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripAlternatingDelta() throws Exception {
        // Delta alternates between two values => constant non-zero DoD
        long[] timestamps = new long[40];
        timestamps[0] = 0;
        for (int i = 1; i < timestamps.length; i++) {
            timestamps[i] = timestamps[i - 1] + (i % 2 == 0 ? 100 : 200);
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripZeroValues() throws Exception {
        assertRoundTrip(new long[]{0, 0, 0, 0, 0});
    }

    @Test
    public void testRoundTripLongMaxValues() throws Exception {
        // Large positive timestamps with constant delta
        long base = Long.MAX_VALUE - 10_000;
        assertRoundTrip(new long[]{base, base + 1000, base + 2000, base + 3000});
    }

    @Test
    public void testRoundTripSingleLargeSpike() throws Exception {
        // Constant delta then one large spike => bucket 4
        long[] timestamps = new long[10];
        for (int i = 0; i < 8; i++) {
            timestamps[i] = i * 1000L;
        }
        timestamps[8] = 7000 + 1_000_000; // spike
        timestamps[9] = 7000 + 1_000_000 + 1000; // back to normal
        assertRoundTrip(timestamps);
    }

    @Test
    public void testEncodeSizeMatchesActualEncodedSize() throws Exception {
        // Verify calculateEncodedSize matches actual encodeTimestamps output
        long[][] testCases = {
                {100, 200, 300},                            // constant delta
                {0, 0, 0, 0},                               // all zeros
                {0, 100, 250, 500, 1000},                   // mixed buckets
                {0, 1000, 1_000_000, 1_000_001, 2_000_000}, // big jumps
        };

        assertMemoryLeak(() -> {
            for (long[] timestamps : testCases) {
                int count = timestamps.length;
                long srcSize = (long) count * 8;
                long src = Unsafe.malloc(srcSize, MemoryTag.NATIVE_ILP_RSS);
                long dst = Unsafe.malloc(srcSize * 2, MemoryTag.NATIVE_ILP_RSS);
                try {
                    for (int i = 0; i < count; i++) {
                        Unsafe.getUnsafe().putLong(src + (long) i * 8, timestamps[i]);
                    }

                    int predicted = QwpGorillaEncoder.calculateEncodedSize(src, count);
                    QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                    int actual = encoder.encodeTimestamps(dst, srcSize * 2, src, count);

                    assertEquals(
                            "calculateEncodedSize != encodeTimestamps for " + java.util.Arrays.toString(timestamps),
                            predicted,
                            actual
                    );
                } finally {
                    Unsafe.free(src, srcSize, MemoryTag.NATIVE_ILP_RSS);
                    Unsafe.free(dst, srcSize * 2, MemoryTag.NATIVE_ILP_RSS);
                }
            }
        });
    }

    @Test
    public void testEncodeThrowsWhenBufferTooSmallForFirstTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long src = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(4, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(src, 1000L);
                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                try {
                    encoder.encodeTimestamps(dst, 4, src, 1);
                    fail("expected LineSenderException");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(src, 8, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 4, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncodeThrowsWhenBufferTooSmallForSecondTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            long src = Unsafe.malloc(16, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(12, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(src, 1000L);
                Unsafe.getUnsafe().putLong(src + 8, 2000L);
                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                try {
                    encoder.encodeTimestamps(dst, 12, src, 2);
                    fail("expected LineSenderException");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(src, 16, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 12, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testEncoderIsReusableAcrossMultipleCalls() throws Exception {
        assertMemoryLeak(() -> {
            QwpGorillaEncoder encoder = new QwpGorillaEncoder();
            long src = Unsafe.malloc(40, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(256, MemoryTag.NATIVE_ILP_RSS);
            try {
                // First encode
                for (int i = 0; i < 5; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, i * 1000L);
                }
                int size1 = encoder.encodeTimestamps(dst, 256, src, 5);
                assertTrue(size1 > 0);

                // Second encode with different data — must not leak state
                for (int i = 0; i < 5; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, i * 9999L);
                }
                int size2 = encoder.encodeTimestamps(dst, 256, src, 5);
                assertTrue(size2 > 0);

                // Verify the second encode is correct via round-trip
                long[] expected = {0, 9999, 19_998, 29_997, 39_996};
                assertDecoded(dst, size2, expected);
            } finally {
                Unsafe.free(src, 40, MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, 256, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testRoundTripExactlyThreeTimestamps() throws Exception {
        // Minimal case with DoD encoding: exactly one DoD value
        assertRoundTrip(new long[]{0, 500, 1500});     // DoD = 500  (bucket 2)
        assertRoundTrip(new long[]{0, 500, 1000});      // DoD = 0    (bucket 0)
        assertRoundTrip(new long[]{0, 500, 510});        // DoD = -490 (bucket 3)
        assertRoundTrip(new long[]{0, 500, 1_000_500});  // DoD = 999_500 (bucket 4)
    }

    @Test
    public void testRoundTripLargeCountAllBucket0() throws Exception {
        // Many timestamps with constant delta => all bucket 0 => very compact
        int count = 10_000;
        long[] timestamps = new long[count];
        for (int i = 0; i < count; i++) {
            timestamps[i] = i * 1_000_000L;
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testRoundTripLargeCountMixedBuckets() throws Exception {
        // Generate timestamps that exercise multiple buckets
        int count = 5000;
        long[] timestamps = new long[count];
        timestamps[0] = 0;
        timestamps[1] = 1000;
        for (int i = 2; i < count; i++) {
            long prevDelta = timestamps[i - 1] - timestamps[i - 2];
            int dod;
            switch (i % 5) {
                case 0:
                    dod = 0;
                    break;       // bucket 0
                case 1:
                    dod = 30;
                    break;      // bucket 1
                case 2:
                    dod = -100;
                    break;   // bucket 2
                case 3:
                    dod = 1500;
                    break;    // bucket 3
                default:
                    dod = 5000;
                    break;    // bucket 4
            }
            timestamps[i] = timestamps[i - 1] + prevDelta + dod;
        }
        assertRoundTrip(timestamps);
    }

    @Test
    public void testCalculateEncodedSizeAllBuckets() throws Exception {
        assertMemoryLeak(() -> {
            // 7 timestamps: 2 uncompressed + 5 DoD values, one per bucket type
            long[] ts = {0, 1000, 2000, 3010, 4120, 6230, 16_230};
            int count = ts.length;
            long srcSize = (long) count * 8;
            long src = Unsafe.malloc(srcSize, MemoryTag.NATIVE_ILP_RSS);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, ts[i]);
                }

                int size = QwpGorillaEncoder.calculateEncodedSize(src, count);

                // DoDs: 0 (1b), 10 (9b), 100 (12b), 1000 (16b), 7890 (36b) = 74 bits
                int expectedDodBytes = (1 + 9 + 12 + 16 + 36 + 7) / 8; // 9 bytes (+ remainder)
                int expectedTotal = 16 + expectedDodBytes; // 25 bytes
                assertEquals(expectedTotal, size);
            } finally {
                Unsafe.free(src, srcSize, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    private static void assertRoundTrip(long[] timestamps) throws Exception {
        assertMemoryLeak(() -> {
            int count = timestamps.length;
            assertTrue("canUseGorilla must return true for test data",
                    count < 3 || writeTimestampsAndCheckGorilla(timestamps));

            long srcSize = (long) count * 8;
            long src = Unsafe.malloc(Math.max(srcSize, 8), MemoryTag.NATIVE_ILP_RSS);
            // Allocate generous dest buffer (worst case: 16 + 36 bits per DoD)
            long dstSize = 16 + (long) count * 5 + 64;
            long dst = Unsafe.malloc(dstSize, MemoryTag.NATIVE_ILP_RSS);
            try {
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(src + (long) i * 8, timestamps[i]);
                }

                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                int written = encoder.encodeTimestamps(dst, dstSize, src, count);
                assertTrue("encoded size must be positive", written > 0);

                assertDecoded(dst, written, timestamps);
            } finally {
                Unsafe.free(src, Math.max(srcSize, 8), MemoryTag.NATIVE_ILP_RSS);
                Unsafe.free(dst, dstSize, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    private static boolean writeTimestampsAndCheckGorilla(long[] timestamps) {
        long srcSize = (long) timestamps.length * 8;
        long src = Unsafe.malloc(srcSize, MemoryTag.NATIVE_ILP_RSS);
        try {
            for (int i = 0; i < timestamps.length; i++) {
                Unsafe.getUnsafe().putLong(src + (long) i * 8, timestamps[i]);
            }
            return QwpGorillaEncoder.canUseGorilla(src, timestamps.length);
        } finally {
            Unsafe.free(src, srcSize, MemoryTag.NATIVE_ILP_RSS);
        }
    }

    /**
     * Decodes Gorilla-encoded timestamps from native memory and verifies
     * they match the expected values. This is a self-contained decoder
     * that mirrors the server-side QwpGorillaDecoder logic.
     */
    private static void assertDecoded(long dst, int encodedSize, long[] expected) {
        int count = expected.length;
        if (count == 0) {
            return;
        }

        // First timestamp: uncompressed int64
        long ts0 = Unsafe.getUnsafe().getLong(dst);
        assertEquals("timestamp[0]", expected[0], ts0);

        if (count == 1) {
            return;
        }

        // Second timestamp: uncompressed int64
        long ts1 = Unsafe.getUnsafe().getLong(dst + 8);
        assertEquals("timestamp[1]", expected[1], ts1);

        if (count == 2) {
            return;
        }

        // Remaining timestamps: decode delta-of-delta from bit stream
        long dataAddr = dst + 16;
        long dataLen = encodedSize - 16;

        // Mini bit reader (mirrors server-side QwpBitReader)
        long[] state = {dataAddr, dataAddr + dataLen, 0L, 0}; // addr, end, bitBuffer, bitsInBuffer

        long prevTs = ts1;
        long prevDelta = ts1 - ts0;

        for (int i = 2; i < count; i++) {
            long dod = readDoD(state);
            long delta = prevDelta + dod;
            long ts = prevTs + delta;
            assertEquals("timestamp[" + i + "]", expected[i], ts);
            prevDelta = delta;
            prevTs = ts;
        }
    }

    private static long readDoD(long[] state) {
        int bit = readBit(state);
        if (bit == 0) {
            return 0; // bucket 0
        }
        bit = readBit(state);
        if (bit == 0) {
            return readSigned(state, 7); // bucket 1
        }
        bit = readBit(state);
        if (bit == 0) {
            return readSigned(state, 9); // bucket 2
        }
        bit = readBit(state);
        if (bit == 0) {
            return readSigned(state, 12); // bucket 3
        }
        return readSigned(state, 32); // bucket 4
    }

    private static int readBit(long[] state) {
        ensureBits(state, 1);
        int bit = (int) (state[2] & 1);
        state[2] >>>= 1;
        state[3]--;
        return bit;
    }

    private static long readSigned(long[] state, int numBits) {
        long unsigned = readBits(state, numBits);
        if (numBits < 64 && (unsigned & (1L << (numBits - 1))) != 0) {
            unsigned |= -1L << numBits;
        }
        return unsigned;
    }

    private static long readBits(long[] state, int numBits) {
        long result = 0;
        int shift = 0;
        int remaining = numBits;
        while (remaining > 0) {
            if (state[3] == 0) {
                ensureBits(state, Math.min(remaining, 64));
            }
            int take = (int) Math.min(remaining, state[3]);
            long mask = take == 64 ? -1L : (1L << take) - 1;
            result |= (state[2] & mask) << shift;
            state[2] >>>= take;
            state[3] -= take;
            remaining -= take;
            shift += take;
        }
        return result;
    }

    // state: [0]=currentAddress, [1]=endAddress, [2]=bitBuffer, [3]=bitsInBuffer
    private static void ensureBits(long[] state, int needed) {
        while (state[3] < needed && state[3] <= 56 && state[0] < state[1]) {
            byte b = Unsafe.getUnsafe().getByte(state[0]++);
            state[2] |= (long) (b & 0xFF) << (int) state[3];
            state[3] += 8;
        }
    }
}
