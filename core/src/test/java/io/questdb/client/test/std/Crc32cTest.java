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

import io.questdb.client.std.Crc32c;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Crc32cTest {

    /**
     * Two distinct inputs must produce distinct CRCs (with overwhelming probability).
     * Single bit-flips at every position must change the CRC.
     */
    @Test
    public void testBitFlipChangesCrc() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] data = new byte[256];
            for (int i = 0; i < data.length; i++) data[i] = (byte) i;
            long buf = Unsafe.malloc(data.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < data.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, data[i]);
                }
                int original = Crc32c.update(Crc32c.INIT, buf, data.length);
                for (int pos = 0; pos < data.length; pos++) {
                    byte saved = data[pos];
                    Unsafe.getUnsafe().putByte(buf + pos, (byte) (saved ^ 1));
                    int flipped = Crc32c.update(Crc32c.INIT, buf, data.length);
                    Assert.assertNotEquals("bit flip at pos=" + pos + " did not change CRC",
                            original, flipped);
                    Unsafe.getUnsafe().putByte(buf + pos, saved);
                }
            } finally {
                Unsafe.free(buf, data.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testChainingMatchesSinglePass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] msg = "the quick brown fox jumps over the lazy dog".getBytes();
            long buf = Unsafe.malloc(msg.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < msg.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, msg[i]);
                }
                int single = Crc32c.update(Crc32c.INIT, buf, msg.length);
                int split = msg.length / 3;
                int chained = Crc32c.update(Crc32c.INIT, buf, split);
                chained = Crc32c.update(chained, buf + split, split);
                chained = Crc32c.update(chained, buf + 2L * split, msg.length - 2L * split);
                assertEquals(single, chained);
            } finally {
                Unsafe.free(buf, msg.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Property-based fuzz: for many random byte sequences and many random split
     * points, {@code chain(crc(prefix), suffix)} must equal {@code crc(prefix||suffix)}.
     * This is the load-bearing property the SF code relies on for replay/scan.
     */
    @Test
    public void testChainingPropertyOverManyRandomInputs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            java.util.Random rnd = new java.util.Random(0x12345678L);
            for (int iter = 0; iter < 200; iter++) {
                int len = 1 + rnd.nextInt(2048);
                byte[] data = new byte[len];
                rnd.nextBytes(data);
                long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < len; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, data[i]);
                    }
                    int single = Crc32c.update(Crc32c.INIT, buf, len);
                    // Try several random split points.
                    for (int s = 0; s < 5; s++) {
                        int split = rnd.nextInt(len + 1);
                        int chained = Crc32c.update(Crc32c.INIT, buf, split);
                        chained = Crc32c.update(chained, buf + split, len - split);
                        Assert.assertEquals(
                                "iter=" + iter + " len=" + len + " split=" + split,
                                single, chained);
                    }
                } finally {
                    Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    /** Length zero with arbitrary seeds returns the seed unchanged. */
    @Test
    public void testEmptyChainingIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            java.util.Random rnd = new java.util.Random(0x42L);
            for (int i = 0; i < 100; i++) {
                int seed = rnd.nextInt();
                Assert.assertEquals(seed, Crc32c.update(seed, 0, 0));
                Assert.assertEquals(seed, Crc32c.update(seed, 0xDEADBEEF, 0));
            }
        });
    }

    @Test
    public void testEmptyReturnsSeed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertEquals(Crc32c.INIT, Crc32c.update(Crc32c.INIT, 0, 0));
            assertEquals(0x12345678, Crc32c.update(0x12345678, 0, 0));
        });
    }

    @Test
    public void testKnownVector() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // CRC-32C of "123456789" = 0xE3069283 (Castagnoli standard test vector)
            byte[] msg = "123456789".getBytes();
            long buf = Unsafe.malloc(msg.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < msg.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, msg[i]);
                }
                int crc = Crc32c.update(Crc32c.INIT, buf, msg.length);
                assertEquals(0xE3069283, crc);
            } finally {
                Unsafe.free(buf, msg.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testZerosHaveStableCrc() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = 1024;
            long buf = Unsafe.calloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                int crc1 = Crc32c.update(Crc32c.INIT, buf, len);
                int crc2 = Crc32c.update(Crc32c.INIT, buf, len);
                assertEquals(crc1, crc2);
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
