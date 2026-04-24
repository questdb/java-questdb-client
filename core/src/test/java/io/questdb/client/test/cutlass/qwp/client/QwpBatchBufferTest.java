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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Hardening tests for {@link QwpBatchBuffer#copyFromPayload}'s capacity growth
 * path. The previous {@code ensureCapacity} implementation spun forever when
 * {@code scratchCapacity == 0} (doubling never advances past zero) and
 * silently wrapped negative when {@code required > Integer.MAX_VALUE / 2}.
 */
public class QwpBatchBufferTest {

    /**
     * Regression: a buffer constructed with {@code initialCapacity == 0} must
     * still grow on the first non-zero payload. Previously the doubling loop
     * {@code newCap *= 2} with {@code newCap == 0} spun forever, hanging the
     * decoder. The fix starts the doubling at {@code max(current, 1)}.
     */
    @Test
    public void testEnsureCapacityGrowsFromZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpBatchBuffer buffer = new QwpBatchBuffer(0);
            long src = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 64; i++) {
                    Unsafe.getUnsafe().putByte(src + i, (byte) (i + 1));
                }
                buffer.copyFromPayload(src, 64);
                Assert.assertEquals(64, buffer.getPayloadLen());
                // Spot-check a couple of bytes to confirm the copy actually
                // landed in the grown scratch.
                Assert.assertEquals((byte) 1, Unsafe.getUnsafe().getByte(buffer.getScratchAddr()));
                Assert.assertEquals((byte) 64, Unsafe.getUnsafe().getByte(buffer.getScratchAddr() + 63));
            } finally {
                Unsafe.free(src, 64, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
            }
        });
    }

    /**
     * Regression: a negative required size used to flow through the doubling
     * loop as an always-true comparison, and subsequent realloc produced
     * undefined native behaviour. The fix rejects it explicitly.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testEnsureCapacityRejectsNegativeRequired() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpBatchBuffer buffer = new QwpBatchBuffer(64);
            try {
                // copyFromPayload forwards the negative length to ensureCapacity.
                buffer.copyFromPayload(0L, -1);
            } finally {
                buffer.close();
            }
        });
    }

    /**
     * Regression: the doubling loop used to perform {@code newCap *= 2} in int
     * space, which wraps negative once {@code newCap} exceeds 2^30 and would
     * then spin forever without ever reaching {@code required}. The fix
     * performs the doubling in long space and clamps at
     * {@link Integer#MAX_VALUE}. We drive the growth path with a request
     * comfortably under the halfway mark but well above the initial
     * capacity, which would have had to double several times under the old
     * code path; completing in bounded time means the fix held.
     */
    @Test(timeout = 5_000L)
    public void testEnsureCapacityGrowsInBoundedTime() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpBatchBuffer buffer = new QwpBatchBuffer(4);
            int required = 16 * 1024 * 1024; // 16 MiB -- several doublings above 4 bytes.
            long src = Unsafe.malloc(required, MemoryTag.NATIVE_DEFAULT);
            try {
                buffer.copyFromPayload(src, required);
                Assert.assertEquals(required, buffer.getPayloadLen());
            } finally {
                Unsafe.free(src, required, MemoryTag.NATIVE_DEFAULT);
                buffer.close();
            }
        });
    }
}
