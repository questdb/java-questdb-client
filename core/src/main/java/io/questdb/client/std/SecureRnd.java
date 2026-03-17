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

package io.questdb.client.std;

import java.security.SecureRandom;

/**
 * Zero-GC cryptographically secure random number generator based on ChaCha20
 * in counter mode (RFC 7539). Seeded once from {@link SecureRandom} at
 * construction time, then produces unpredictable output with no heap
 * allocations.
 * <p>
 * Each {@link #nextInt()} call returns one 32-bit word from the ChaCha20
 * keystream. A single block computation yields 16 words, so the amortized
 * cost is one ChaCha20 block per 16 calls.
 */
public class SecureRnd {

    // "expand 32-byte k" in little-endian
    private static final int CONSTANT_0 = 0x61707865;
    private static final int CONSTANT_1 = 0x3320646e;
    private static final int CONSTANT_2 = 0x79622d32;
    private static final int CONSTANT_3 = 0x6b206574;

    private final int[] output = new int[16];
    private final int[] state = new int[16];
    private int outputPos = 16; // forces block computation on first call

    /**
     * Creates a new instance seeded from {@link SecureRandom}.
     */
    public SecureRnd() {
        SecureRandom seed = new SecureRandom();
        byte[] seedBytes = new byte[44]; // 32 (key) + 12 (nonce)
        seed.nextBytes(seedBytes);
        init(seedBytes, 0);
    }

    /**
     * Creates a new instance with an explicit key, nonce, and initial counter.
     * Useful for testing with known RFC 7539 test vectors.
     *
     * @param key     32-byte key
     * @param nonce   12-byte nonce
     * @param counter initial block counter value
     */
    public SecureRnd(byte[] key, byte[] nonce, int counter) {
        byte[] seedBytes = new byte[44];
        System.arraycopy(key, 0, seedBytes, 0, 32);
        System.arraycopy(nonce, 0, seedBytes, 32, 12);
        init(seedBytes, counter);
    }

    /**
     * Returns the next cryptographically secure random int.
     */
    public int nextInt() {
        if (outputPos >= 16) {
            computeBlock();
            outputPos = 0;
        }
        return output[outputPos++];
    }

    private void computeBlock() {
        int x0 = state[0], x1 = state[1], x2 = state[2], x3 = state[3];
        int x4 = state[4], x5 = state[5], x6 = state[6], x7 = state[7];
        int x8 = state[8], x9 = state[9], x10 = state[10], x11 = state[11];
        int x12 = state[12], x13 = state[13], x14 = state[14], x15 = state[15];

        for (int i = 0; i < 10; i++) {
            // Column rounds
            x0 += x4;  x12 ^= x0;  x12 = Integer.rotateLeft(x12, 16);
            x8 += x12;  x4 ^= x8;  x4 = Integer.rotateLeft(x4, 12);
            x0 += x4;  x12 ^= x0;  x12 = Integer.rotateLeft(x12, 8);
            x8 += x12;  x4 ^= x8;  x4 = Integer.rotateLeft(x4, 7);

            x1 += x5;  x13 ^= x1;  x13 = Integer.rotateLeft(x13, 16);
            x9 += x13;  x5 ^= x9;  x5 = Integer.rotateLeft(x5, 12);
            x1 += x5;  x13 ^= x1;  x13 = Integer.rotateLeft(x13, 8);
            x9 += x13;  x5 ^= x9;  x5 = Integer.rotateLeft(x5, 7);

            x2 += x6;  x14 ^= x2;  x14 = Integer.rotateLeft(x14, 16);
            x10 += x14;  x6 ^= x10;  x6 = Integer.rotateLeft(x6, 12);
            x2 += x6;  x14 ^= x2;  x14 = Integer.rotateLeft(x14, 8);
            x10 += x14;  x6 ^= x10;  x6 = Integer.rotateLeft(x6, 7);

            x3 += x7;  x15 ^= x3;  x15 = Integer.rotateLeft(x15, 16);
            x11 += x15;  x7 ^= x11;  x7 = Integer.rotateLeft(x7, 12);
            x3 += x7;  x15 ^= x3;  x15 = Integer.rotateLeft(x15, 8);
            x11 += x15;  x7 ^= x11;  x7 = Integer.rotateLeft(x7, 7);

            // Diagonal rounds
            x0 += x5;  x15 ^= x0;  x15 = Integer.rotateLeft(x15, 16);
            x10 += x15;  x5 ^= x10;  x5 = Integer.rotateLeft(x5, 12);
            x0 += x5;  x15 ^= x0;  x15 = Integer.rotateLeft(x15, 8);
            x10 += x15;  x5 ^= x10;  x5 = Integer.rotateLeft(x5, 7);

            x1 += x6;  x12 ^= x1;  x12 = Integer.rotateLeft(x12, 16);
            x11 += x12;  x6 ^= x11;  x6 = Integer.rotateLeft(x6, 12);
            x1 += x6;  x12 ^= x1;  x12 = Integer.rotateLeft(x12, 8);
            x11 += x12;  x6 ^= x11;  x6 = Integer.rotateLeft(x6, 7);

            x2 += x7;  x13 ^= x2;  x13 = Integer.rotateLeft(x13, 16);
            x8 += x13;  x7 ^= x8;  x7 = Integer.rotateLeft(x7, 12);
            x2 += x7;  x13 ^= x2;  x13 = Integer.rotateLeft(x13, 8);
            x8 += x13;  x7 ^= x8;  x7 = Integer.rotateLeft(x7, 7);

            x3 += x4;  x14 ^= x3;  x14 = Integer.rotateLeft(x14, 16);
            x9 += x14;  x4 ^= x9;  x4 = Integer.rotateLeft(x4, 12);
            x3 += x4;  x14 ^= x3;  x14 = Integer.rotateLeft(x14, 8);
            x9 += x14;  x4 ^= x9;  x4 = Integer.rotateLeft(x4, 7);
        }

        // Feed-forward: add original state
        output[0] = x0 + state[0];
        output[1] = x1 + state[1];
        output[2] = x2 + state[2];
        output[3] = x3 + state[3];
        output[4] = x4 + state[4];
        output[5] = x5 + state[5];
        output[6] = x6 + state[6];
        output[7] = x7 + state[7];
        output[8] = x8 + state[8];
        output[9] = x9 + state[9];
        output[10] = x10 + state[10];
        output[11] = x11 + state[11];
        output[12] = x12 + state[12];
        output[13] = x13 + state[13];
        output[14] = x14 + state[14];
        output[15] = x15 + state[15];

        // Increment block counter
        state[12]++;
    }

    private void init(byte[] seedBytes, int counter) {
        state[0] = CONSTANT_0;
        state[1] = CONSTANT_1;
        state[2] = CONSTANT_2;
        state[3] = CONSTANT_3;

        // Key: 8 little-endian ints from seedBytes[0..31]
        for (int i = 0; i < 8; i++) {
            int off = i * 4;
            state[4 + i] = (seedBytes[off] & 0xFF)
                    | ((seedBytes[off + 1] & 0xFF) << 8)
                    | ((seedBytes[off + 2] & 0xFF) << 16)
                    | ((seedBytes[off + 3] & 0xFF) << 24);
        }

        state[12] = counter;

        // Nonce: 3 little-endian ints from seedBytes[32..43]
        for (int i = 0; i < 3; i++) {
            int off = 32 + i * 4;
            state[13 + i] = (seedBytes[off] & 0xFF)
                    | ((seedBytes[off + 1] & 0xFF) << 8)
                    | ((seedBytes[off + 2] & 0xFF) << 16)
                    | ((seedBytes[off + 3] & 0xFF) << 24);
        }
    }
}
