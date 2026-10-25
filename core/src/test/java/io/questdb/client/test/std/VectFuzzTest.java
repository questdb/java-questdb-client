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

import io.questdb.client.network.Net;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import org.junit.Assert;
import org.junit.Test;

public class VectFuzzTest {
    @Test
    public void testMemcpyEverySmallLengthAndAlignment() throws Exception {
        assertMemoryLeak(() -> {
            // crosses the small-copy cutoff, at every source/destination misalignment;
            // the guard byte after each copy must stay untouched
            int maxLen = 160;
            int size = maxLen + 16;
            long from = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            long to = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < size; i++) {
                    Unsafe.getUnsafe().putByte(from + i, (byte) (i * 31 + 7));
                }
                for (int len = 0; len <= maxLen; len++) {
                    for (int srcOff = 0; srcOff < 8; srcOff++) {
                        for (int dstOff = 0; dstOff < 8; dstOff++) {
                            Vect.memset(to, size, 0);
                            Vect.memcpy(to + dstOff, from + srcOff, len);
                            for (int i = 0; i < len; i++) {
                                if (Unsafe.getUnsafe().getByte(to + dstOff + i) != Unsafe.getUnsafe().getByte(from + srcOff + i)) {
                                    Assert.fail("len=" + len + " srcOff=" + srcOff + " dstOff=" + dstOff + " byte " + i);
                                }
                            }
                            Assert.assertEquals("guard, len=" + len, 0, Unsafe.getUnsafe().getByte(to + dstOff + len));
                        }
                    }
                }
            } finally {
                Unsafe.free(from, size, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(to, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMemmove() throws Exception {
        assertMemoryLeak(() -> {
            int maxSize = 1024 * 1024;
            int[] sizes = {1024, 4096, maxSize};
            int buffSize = 1024 + 4096 + maxSize;
            long from = Unsafe.malloc(buffSize, MemoryTag.NATIVE_DEFAULT);
            long to = Unsafe.malloc(maxSize, MemoryTag.NATIVE_DEFAULT);

            try {
                // initialize from buffer
                // with 1, 4, 8, 12 ... integers
                for (int i = 0; i < buffSize; i += Integer.BYTES) {
                    Unsafe.getUnsafe().putInt(from + i, i);
                }

                int offset = 0;
                for (int size : sizes) {
                    // move next portion of from into to
                    Vect.memmove(to, from + offset, size);

                    for (int i = 0; i < size; i += Integer.BYTES) {
                        int actual = Unsafe.getUnsafe().getInt(to + i);
                        Assert.assertEquals(i + offset, actual);
                    }

                    offset += size;
                }
            } finally {
                Unsafe.free(from, buffSize, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(to, maxSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    static {
        Os.init();
        Net.init();
    }
}
