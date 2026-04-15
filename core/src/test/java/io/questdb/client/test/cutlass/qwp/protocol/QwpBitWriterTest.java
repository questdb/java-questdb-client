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
import io.questdb.client.cutlass.qwp.protocol.QwpBitWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import org.junit.Test;

import static org.junit.Assert.*;

public class QwpBitWriterTest {

    @Test
    public void testFlushThrowsOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_ILP_RSS);
            try {
                QwpBitWriter writer = new QwpBitWriter();
                writer.reset(ptr, 1);
                // Write 8 bits to fill the single byte
                writer.writeBits(0xFF, 8);
                // Write a few more bits that sit in the bit buffer
                writer.writeBits(0x3, 4);
                // Flush should throw because there's no room for the partial byte
                try {
                    writer.flush();
                    fail("expected LineSenderException on buffer overflow during flush");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(ptr, 1, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testGorillaEncoderThrowsOnInsufficientCapacityForFirstTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Source: 1 timestamp (8 bytes), dest: only 4 bytes
            long src = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(4, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(src, 1_000_000L);
                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                try {
                    encoder.encodeTimestamps(dst, 4, src, 1);
                    fail("expected LineSenderException on buffer overflow");
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
    public void testGorillaEncoderThrowsOnInsufficientCapacityForSecondTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            // Source: 2 timestamps (16 bytes), dest: only 12 bytes (enough for first, not second)
            long src = Unsafe.malloc(16, MemoryTag.NATIVE_ILP_RSS);
            long dst = Unsafe.malloc(12, MemoryTag.NATIVE_ILP_RSS);
            try {
                Unsafe.getUnsafe().putLong(src, 1_000_000L);
                Unsafe.getUnsafe().putLong(src + 8, 2_000_000L);
                QwpGorillaEncoder encoder = new QwpGorillaEncoder();
                try {
                    encoder.encodeTimestamps(dst, 12, src, 2);
                    fail("expected LineSenderException on buffer overflow");
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
    public void testWriteBitsThrowsOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(4, MemoryTag.NATIVE_ILP_RSS);
            try {
                QwpBitWriter writer = new QwpBitWriter();
                writer.reset(ptr, 4);
                // Fill the buffer (32 bits = 4 bytes)
                writer.writeBits(0xFFFF_FFFFL, 32);
                // Next write should throw — buffer is full
                try {
                    writer.writeBits(1, 8);
                    fail("expected LineSenderException on buffer overflow");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(ptr, 4, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testWriteBitsWithinCapacitySucceeds() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            try {
                QwpBitWriter writer = new QwpBitWriter();
                writer.reset(ptr, 8);
                writer.writeBits(0xDEAD_BEEF_CAFE_BABEL, 64);
                writer.flush();
                assertEquals(8, writer.getPosition() - ptr);
                assertEquals(0xDEAD_BEEF_CAFE_BABEL, Unsafe.getUnsafe().getLong(ptr));
            } finally {
                Unsafe.free(ptr, 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testWriteByteThrowsOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_ILP_RSS);
            try {
                QwpBitWriter writer = new QwpBitWriter();
                writer.reset(ptr, 1);
                writer.writeByte(0x42);
                try {
                    writer.writeByte(0x43);
                    fail("expected LineSenderException on buffer overflow");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(ptr, 1, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testWriteIntThrowsOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(4, MemoryTag.NATIVE_ILP_RSS);
            try {
                QwpBitWriter writer = new QwpBitWriter();
                writer.reset(ptr, 4);
                writer.writeInt(42);
                try {
                    writer.writeInt(99);
                    fail("expected LineSenderException on buffer overflow");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(ptr, 4, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }

    @Test
    public void testWriteLongThrowsOnOverflow() throws Exception {
        assertMemoryLeak(() -> {
            long ptr = Unsafe.malloc(8, MemoryTag.NATIVE_ILP_RSS);
            try {
                QwpBitWriter writer = new QwpBitWriter();
                writer.reset(ptr, 8);
                writer.writeLong(42L);
                try {
                    writer.writeLong(99L);
                    fail("expected LineSenderException on buffer overflow");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("buffer overflow"));
                }
            } finally {
                Unsafe.free(ptr, 8, MemoryTag.NATIVE_ILP_RSS);
            }
        });
    }
}
