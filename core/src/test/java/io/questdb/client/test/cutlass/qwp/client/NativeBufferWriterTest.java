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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.std.Unsafe;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NativeBufferWriterTest {

    @Test
    public void testPatchIntAtLastValidOffset() {
        try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
            writer.putLong(0L); // 8 bytes, position = 8
            // Patch at offset 4 covers bytes [4..7], exactly at the boundary
            writer.patchInt(4, 0x1234);
            assertEquals(0x1234, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
        }
    }

    @Test
    public void testPatchIntAtValidOffset() {
        try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
            writer.putInt(0); // placeholder at offset 0
            writer.putInt(0xBEEF); // data at offset 4
            // Patch the placeholder
            writer.patchInt(0, 0xCAFE);
            assertEquals(0xCAFE, Unsafe.getUnsafe().getInt(writer.getBufferPtr()));
            assertEquals(0xBEEF, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
        }
    }

    @Test
    public void testSkipAdvancesPosition() {
        try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
            writer.skip(4);
            assertEquals(4, writer.getPosition());
            writer.skip(8);
            assertEquals(12, writer.getPosition());
        }
    }

    @Test
    public void testSkipThenPatchInt() {
        try (NativeBufferWriter writer = new NativeBufferWriter(8)) {
            int patchOffset = writer.getPosition();
            writer.skip(4); // reserve space for a length field
            writer.putInt(0xDEAD);
            // Patch the reserved space
            writer.patchInt(patchOffset, 4);
            assertEquals(0x4, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + patchOffset));
            assertEquals(0xDEAD, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
        }
    }

    @Test
    public void testEnsureCapacityGrowsBuffer() {
        try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
            assertEquals(16, writer.getCapacity());
            writer.ensureCapacity(32);
            assertTrue(writer.getCapacity() >= 32);
        }
    }
}
