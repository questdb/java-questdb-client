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

package io.questdb.client.test.std;

import io.questdb.client.std.SecureRnd;
import org.junit.Assert;
import org.junit.Test;

public class SecureRndTest {

    @Test
    public void testConsecutiveCallsProduceDifferentValues() {
        SecureRnd rnd = new SecureRnd();
        int prev = rnd.nextInt();
        boolean foundDifferent = false;
        for (int i = 0; i < 100; i++) {
            int next = rnd.nextInt();
            if (next != prev) {
                foundDifferent = true;
                break;
            }
        }
        Assert.assertTrue("Expected different values from consecutive calls", foundDifferent);
    }

    @Test
    public void testDifferentInstancesProduceDifferentSequences() {
        SecureRnd rnd1 = new SecureRnd();
        SecureRnd rnd2 = new SecureRnd();
        boolean foundDifferent = false;
        for (int i = 0; i < 16; i++) {
            if (rnd1.nextInt() != rnd2.nextInt()) {
                foundDifferent = true;
                break;
            }
        }
        Assert.assertTrue("Two SecureRnd instances should produce different sequences", foundDifferent);
    }

    @Test
    public void testMultipleBlocksDoNotRepeat() {
        SecureRnd rnd = new SecureRnd();
        // Consume more than one block (16 ints) to trigger block counter increment
        int[] first16 = new int[16];
        for (int i = 0; i < 16; i++) {
            first16[i] = rnd.nextInt();
        }
        // Next 16 should be from a different block
        boolean foundDifferent = false;
        for (int i = 0; i < 16; i++) {
            if (rnd.nextInt() != first16[i]) {
                foundDifferent = true;
                break;
            }
        }
        Assert.assertTrue("Second block should differ from first", foundDifferent);
    }

    // RFC 7539 Section 2.3.2 known-answer test
    @Test
    public void testRfc7539Section232TestVector() {
        // Key: 00:01:02:03:...:1f
        byte[] key = new byte[32];
        for (int i = 0; i < 32; i++) {
            key[i] = (byte) i;
        }

        // Nonce: 00:00:00:09:00:00:00:4a:00:00:00:00
        byte[] nonce = {
                0x00, 0x00, 0x00, 0x09,
                0x00, 0x00, 0x00, 0x4a,
                0x00, 0x00, 0x00, 0x00
        };

        // Block counter = 1
        SecureRnd rnd = new SecureRnd(key, nonce, 1);

        // Expected output words (ChaCha state after adding original input)
        // from RFC 7539 Section 2.3.2
        int[] expected = {
                0xe4e7f110, 0x15593bd1, 0x1fdd0f50, 0xc47120a3,
                0xc7f4d1c7, 0x0368c033, 0x9aaa2204, 0x4e6cd4c3,
                0x466482d2, 0x09aa9f07, 0x05d7c214, 0xa2028bd9,
                0xd19c12b5, 0xb94e16de, 0xe883d0cb, 0x4e3c50a2,
        };

        for (int i = 0; i < 16; i++) {
            int actual = rnd.nextInt();
            Assert.assertEquals(
                    "Mismatch at word " + i + ": expected 0x" + Integer.toHexString(expected[i])
                            + " but got 0x" + Integer.toHexString(actual),
                    expected[i],
                    actual
            );
        }
    }
}
