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

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Tests for SegmentedNativeBufferWriter.getWriteAddress()/getWritableBytes()
 * and Gorilla encoding through a segmented buffer.
 * <p>
 * Uses reflection to access package-private classes from the test module.
 */
public class SegmentedNativeBufferWriterTest {

    @Test
    public void testGetWriteAddressIsChunkLocalAfterFlush() throws Exception {
        assertMemoryLeak(() -> {
            // SegmentedNativeBufferWriter is package-private, create via reflection
            Class<?> segClass = Class.forName("io.questdb.client.cutlass.qwp.client.SegmentedNativeBufferWriter");
            Constructor<?> ctor = segClass.getDeclaredConstructor();
            ctor.setAccessible(true);
            QwpBufferWriter writer = (QwpBufferWriter) ctor.newInstance();
            try {
                // Write some data to the current chunk
                writer.putInt(1);
                writer.putInt(2);
                Assert.assertEquals(8, writer.getPosition());

                // Before flush: getWriteAddress should equal getBufferPtr() + getPosition()
                Assert.assertEquals(writer.getBufferPtr() + writer.getPosition(), writer.getWriteAddress());
                Assert.assertEquals(writer.getCapacity() - writer.getPosition(), writer.getWritableBytes());

                // putBlockOfBytes triggers flushCurrentChunk(), creating flushedBytes > 0
                long tempBuf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    writer.putBlockOfBytes(tempBuf, 16);
                } finally {
                    Unsafe.free(tempBuf, 16, MemoryTag.NATIVE_DEFAULT);
                }

                // After flush: getPosition() is global (includes flushedBytes),
                // but getWriteAddress()/getWritableBytes() must be chunk-local
                writer.putInt(3);
                int globalPos = writer.getPosition();

                // Global position includes: 8 (first chunk) + 16 (block) + 4 (new int) = 28
                Assert.assertEquals(28, globalPos);

                // The critical invariant: getWriteAddress() points into the current chunk
                long writeAddr = writer.getWriteAddress();
                long chunkStart = writer.getBufferPtr();
                int chunkCapacity = writer.getCapacity();
                Assert.assertTrue("Write address must be >= chunk start", writeAddr >= chunkStart);
                Assert.assertTrue("Write address must be within chunk bounds",
                        writeAddr <= chunkStart + chunkCapacity);

                // getWritableBytes() must be positive and chunk-local
                int writable = writer.getWritableBytes();
                Assert.assertTrue("Writable bytes must be positive", writable > 0);
                Assert.assertTrue("Writable bytes must be <= chunk capacity", writable <= chunkCapacity);

                // Verify the broken arithmetic would give wrong results
                long brokenAddr = writer.getBufferPtr() + writer.getPosition();
                Assert.assertNotEquals("getBufferPtr()+getPosition() must differ from getWriteAddress() after flush",
                        brokenAddr, writeAddr);
            } finally {
                ((QuietCloseable) writer).close();
            }
        });
    }

    @Test
    public void testGorillaEncodingWithSegmentedBuffer() throws Exception {
        assertMemoryLeak(() -> {
            // Create SegmentedNativeBufferWriter and QwpColumnWriter via reflection
            Class<?> segClass = Class.forName("io.questdb.client.cutlass.qwp.client.SegmentedNativeBufferWriter");
            Constructor<?> segCtor = segClass.getDeclaredConstructor();
            segCtor.setAccessible(true);
            QwpBufferWriter writer = (QwpBufferWriter) segCtor.newInstance();

            Class<?> cwClass = Class.forName("io.questdb.client.cutlass.qwp.client.QwpColumnWriter");
            Constructor<?> cwCtor = cwClass.getDeclaredConstructor();
            cwCtor.setAccessible(true);
            Object columnWriter = cwCtor.newInstance();

            Method setBuffer = cwClass.getDeclaredMethod("setBuffer", QwpBufferWriter.class);
            setBuffer.setAccessible(true);

            Method encodeTable = cwClass.getDeclaredMethod("encodeTable",
                    QwpTableBuffer.class, boolean.class, boolean.class);
            encodeTable.setAccessible(true);

            try (QwpTableBuffer tableBuffer = new QwpTableBuffer("test")) {
                // Add timestamps with constant delta (best case for Gorilla, needs 3+)
                QwpTableBuffer.ColumnBuffer col = tableBuffer.getOrCreateColumn(
                        "ts", QwpConstants.TYPE_TIMESTAMP, true);
                long baseTs = 1_000_000_000L;
                for (int i = 0; i < 10; i++) {
                    col.addLong(baseTs + i * 1_000_000L);
                    tableBuffer.nextRow();
                }

                // Flush some data first to create flushedBytes > 0.
                // This is the scenario that was broken before the fix:
                // getBufferPtr() + getPosition() would point past the chunk.
                long tempBuf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                try {
                    writer.putBlockOfBytes(tempBuf, 32);
                } finally {
                    Unsafe.free(tempBuf, 32, MemoryTag.NATIVE_DEFAULT);
                }
                Assert.assertTrue("Should have flushed bytes", writer.getPosition() > 0);

                // Encode table with Gorilla enabled into the segmented buffer.
                // Before the fix, this would compute a wrong write address and corrupt memory.
                setBuffer.invoke(columnWriter, writer);
                encodeTable.invoke(columnWriter, tableBuffer, false, true);

                // Verify encoding produced data beyond the initial 32 bytes
                Assert.assertTrue("Encoding should produce data", writer.getPosition() > 32);
            } finally {
                ((QuietCloseable) writer).close();
            }
        });
    }
}
