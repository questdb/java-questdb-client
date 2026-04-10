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

import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class QwpGorillaEncoderTest {

    @Test
    public void testCalculateEncodedSizeDoesNotOverflowWithLargeCount() throws Exception {
        // With int totalBits, overflow occurs at ~59.6M timestamps when every
        // DoD hits the worst-case 36-bit bucket. Use 60M entries to trigger it.
        // Each entry is 8 bytes => ~480 MB native memory.
        final int count = 60_000_000;
        final long sizeBytes = (long) count * 8;

        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(sizeBytes, MemoryTag.NATIVE_ILP_RSS);
            try {
                // Build timestamps where delta alternates between 1 and 10_001,
                // so DoD = ±10_000 each step — always in the 36-bit bucket.
                long ts = 0;
                long delta = 1;
                for (int i = 0; i < count; i++) {
                    Unsafe.getUnsafe().putLong(ptr + (long) i * 8, ts);
                    ts += delta;
                    delta = (i % 2 == 0) ? 10_001 : 1;
                }

                assertTrue(QwpGorillaEncoder.canUseGorilla(ptr, count));

                int encodedSize = QwpGorillaEncoder.calculateEncodedSize(ptr, count);

                // Before fix: totalBits overflows int and the size goes negative or too small.
                // After fix (long totalBits): size must be positive and at least
                // 16 (two uncompressed timestamps) + ceil(36 * (count - 2) / 8) bytes.
                long expectedMinBits = 36L * (count - 2);
                long expectedMinSize = 16 + (expectedMinBits + 7) / 8;
                assertTrue(
                        "encoded size must be positive, got " + encodedSize,
                        encodedSize > 0
                );
                assertTrue(
                        "encoded size " + encodedSize + " is smaller than expected minimum " + expectedMinSize,
                        encodedSize >= expectedMinSize
                );
            } finally {
                Unsafe.free(ptr, sizeBytes, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }
}
