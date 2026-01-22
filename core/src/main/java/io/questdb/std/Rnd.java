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

package io.questdb.std;

import io.questdb.cairo.GeoHashes;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

public class Rnd {
    private static final long mask = (1L << 48) - 1;
    private static final double DOUBLE_UNIT = 0x1.0p-53; // 1.0 / (1L << 53)
    private final StringSink array = new StringSink();
    private long s0;
    private long s1;

    public Rnd(long s0, long s1) {
        reset(s0, s1);
    }

    public Rnd() {
        reset();
    }

    public int nextInt(int boundary) {
        return nextPositiveInt() % boundary;
    }

    private int nextIntForDouble(int bits) {
        return (int) ((nextLong() & mask) >>> (48 - bits));
    }

    public double nextDouble() {
        return (((long) (nextIntForDouble(26)) << 27) + nextIntForDouble(27)) * DOUBLE_UNIT;
    }

    public long nextGeoHash(int bits) {
        double x = nextDouble() * 180.0 - 90.0;
        double y = nextDouble() * 360.0 - 180.0;
        try {
            return GeoHashes.fromCoordinatesDeg(x, y, bits);
        } catch (NumericException e) {
            // Should never happen
            return GeoHashes.NULL;
        }
    }

    public int nextInt() {
        return (int) nextLong();
    }

    public long nextLong() {
        long l1 = s0;
        long l0 = s1;
        s0 = l0;
        l1 ^= l1 << 23;
        return (s1 = l1 ^ l0 ^ (l1 >> 17) ^ (l0 >> 26)) + l0;
    }

    /**
     * Generate a random Decimal128 into the provided sink.
     * This method generates a mix of small, medium, and large values with random scales.
     *
     * @param sink The Decimal128 instance to populate with random values
     */
    public void nextDecimal128(Decimal128 sink) {
        // Generate random scale between 0 and MAX_SCALE
        int scale = nextInt(Decimal128.MAX_SCALE + 1);
        // Generate random value - mix of small, medium and large values
        int valueType = nextInt(4);
        switch (valueType) {
            case 0: // Small values (-1000 to 1000)
                sink.of(0, nextLong() % 2000 - 1000, scale);
                break;
            case 1: // Medium values (up to int range)
                sink.of(0, nextInt(), scale);
                break;
            case 2: // Large positive values
                sink.of(nextLong() & 0x7FFFFFFFL, nextLong(), scale);
                break;
            default: // Large negative values
                sink.of((1L << 63) | (nextLong() & 0x7FFFFFFFL), nextLong(), scale);
                break;
        }
    }

    /**
     * Generate a random Decimal128 and return a new instance.
     * This method generates a mix of small, medium, and large values with random scales.
     *
     * @return A new Decimal128 instance with random values
     */
    public Decimal128 nextDecimal128() {
        Decimal128 result = new Decimal128();
        nextDecimal128(result);
        return result;
    }

    /**
     * Generate a random Decimal256 into the provided sink.
     * This method generates a mix of small, medium, and large values with random scales.
     *
     * @param sink The Decimal256 instance to populate with random values
     */
    public void nextDecimal256(Decimal256 sink) {
        // Generate random scale between 0 and MAX_SCALE
        int scale = nextInt(Decimal256.MAX_SCALE + 1);

        // Generate random value - mix of small, medium and large values
        int valueType = nextInt(6);  // More types for 256-bit range

        switch (valueType) {
            case 0: // Small values (-1000 to 1000)
                sink.of(0, 0, 0, nextLong() % 2000 - 1000, scale);
                break;
            case 1: // Medium values (up to int range)
                sink.of(0, 0, 0, nextInt(), scale);
                break;
            case 2: // Large 64-bit positive values
                sink.of(0, 0, 0, nextLong() & Long.MAX_VALUE, scale);
                break;
            case 3: // Large 64-bit negative values
                sink.of(0, 0, 0, nextLong() | Long.MIN_VALUE, scale);
                break;
            case 4: // Very large 128-bit values (using mid and low)
                sink.of(0, 0, nextLong(), nextLong(), scale);
                break;
            default: // Ultra large 256-bit values (using all four longs)
                long hh = nextLong();
                while (hh >= Decimal256.MAX_VALUE.getHh() || hh <= Decimal256.MIN_VALUE.getHh()) {
                    hh = nextLong();
                }
                sink.of(hh, nextLong(), nextLong(), nextLong(), scale);
                break;
        }
    }

    /**
     * Generate a random Decimal256 and return a new instance.
     * This method generates a mix of small, medium, and large values with random scales.
     *
     * @return A new Decimal256 instance with random values
     */
    public Decimal256 nextDecimal256() {
        Decimal256 result = new Decimal256();
        nextDecimal256(result);
        return result;
    }

    public void nextDecimal64(Decimal64 sink) {
        // Generate random scale between 0 and MAX_SCALE
        int scale = nextInt(Decimal64.MAX_SCALE + 1);
        // Generate random value - mix of small, medium and large values
        int valueType = nextInt(4);
        switch (valueType) {
            case 0: // Small values (-1000 to 1000)
                sink.of(nextLong() % 2000 - 1000, scale);
                break;
            case 1: // Medium values (up to int range)
                sink.of(nextInt(), scale);
                break;
            case 2: // Large positive values (limited to avoid overflow in operations)
                sink.of(nextLong() / 1000, scale);
                break;
            default: // Large negative values (limited to avoid overflow in operations)
                sink.of(-(nextLong() / 1000), scale);
                break;
        }
    }

    /**
     * Generate a random Decimal64 and return a new instance.
     * This method generates a mix of small, medium, and large values with random scales.
     *
     * @return A new Decimal64 instance with random values
     */
    public Decimal64 nextDecimal64() {
        Decimal64 result = new Decimal64();
        nextDecimal64(result);
        return result;
    }


    public CharSequence nextChars(int len) {
        array.clear();
        nextChars(array, len);
        return array;
    }

    // returns random bytes between 'B' and 'Z' for legacy reasons
    public void nextChars(Utf16Sink array, int len) {
        for (int i = 0; i < len; i++) {
            array.put((char) (nextPositiveInt() % 25 + 66));
        }
    }

    public long nextLong(long boundary) {
        return nextPositiveLong() % boundary;
    }

    public long nextPositiveLong() {
        long l = nextLong();
        return l > 0 ? l : (l == Long.MIN_VALUE ? Long.MAX_VALUE : -l);
    }

    public boolean nextBoolean() {
        return nextLong() >>> (64 - 1) != 0;
    }

    // returns random bytes between 'B' and 'Z' for legacy reasons
    public String nextString(int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            chars[i] = (char) (nextPositiveInt() % 25 + 66);
        }
        return new String(chars);
    }

    public int nextPositiveInt() {
        int n = (int) nextLong();
        return n > 0 ? n : (n == Integer.MIN_VALUE ? Integer.MAX_VALUE : -n);
    }

    public final void reset(long s0, long s1) {
        this.s0 = s0;
        this.s1 = s1;
    }

    public final void reset() {
        reset(0xdeadbeef, 0xdee4c0ed);
    }

}