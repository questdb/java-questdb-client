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

import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MicrobatchBufferTest {

    @Test
    public void testAwaitRecycled() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();

                AtomicBoolean recycled = new AtomicBoolean(false);
                CountDownLatch started = new CountDownLatch(1);

                Thread waiter = new Thread(() -> {
                    started.countDown();
                    buffer.awaitRecycled();
                    recycled.set(true);
                });
                waiter.start();

                started.await();
                awaitThreadBlocked(waiter);
                Assert.assertFalse(recycled.get());

                buffer.markRecycled();
                waiter.join(1000);

                Assert.assertTrue(recycled.get());
            }
        });
    }

    @Test
    public void testAwaitRecycledWithTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();

                // Should timeout
                boolean result = buffer.awaitRecycled(50, TimeUnit.MILLISECONDS);
                Assert.assertFalse(result);

                buffer.markRecycled();

                // Should succeed immediately now
                result = buffer.awaitRecycled(50, TimeUnit.MILLISECONDS);
                Assert.assertTrue(result);
            }
        });
    }

    @Test
    public void testBatchIdIncrementsOnReset() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                long id1 = buffer.getBatchId();

                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();
                buffer.reset();

                long id2 = buffer.getBatchId();
                Assert.assertNotEquals(id1, id2);

                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();
                buffer.reset();

                long id3 = buffer.getBatchId();
                Assert.assertNotEquals(id2, id3);
            }
        });
    }

    @Test
    public void testConcurrentBatchIdUniqueness() throws Exception {
        int threadCount = 8;
        int buffersPerThread = 500;
        int totalBuffers = threadCount * buffersPerThread;
        Set<Long> batchIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    for (int i = 0; i < buffersPerThread; i++) {
                        MicrobatchBuffer buf = new MicrobatchBuffer(64);
                        batchIds.add(buf.getBatchId());
                        buf.close();
                    }
                } finally {
                    doneLatch.countDown();
                }
            });
            threads[t].start();
        }

        startLatch.countDown();
        assertTrue("Threads did not finish in time", doneLatch.await(30, TimeUnit.SECONDS));

        assertEquals(
                "Duplicate batch IDs detected: expected " + totalBuffers + " unique IDs but got " + batchIds.size(),
                totalBuffers,
                batchIds.size()
        );
    }

    @Test
    public void testConcurrentResetBatchIdUniqueness() throws Exception {
        int threadCount = 8;
        int resetsPerThread = 500;
        int totalIds = threadCount * resetsPerThread;
        Set<Long> batchIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    MicrobatchBuffer buf = new MicrobatchBuffer(64);
                    for (int i = 0; i < resetsPerThread; i++) {
                        buf.seal();
                        buf.markSending();
                        buf.markRecycled();
                        buf.reset();
                        batchIds.add(buf.getBatchId());
                    }
                    buf.close();
                } finally {
                    doneLatch.countDown();
                }
            });
            threads[t].start();
        }

        startLatch.countDown();
        assertTrue("Threads did not finish in time", doneLatch.await(30, TimeUnit.SECONDS));

        assertEquals(
                "Duplicate batch IDs from reset(): expected " + totalIds + " unique IDs but got " + batchIds.size(),
                totalIds,
                batchIds.size()
        );
    }

    @Test
    public void testConcurrentStateTransitions() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                AtomicReference<Throwable> error = new AtomicReference<>();
                CountDownLatch userDone = new CountDownLatch(1);
                CountDownLatch ioDone = new CountDownLatch(1);

                // Simulate user thread
                Thread userThread = new Thread(() -> {
                    try {
                        buffer.writeByte((byte) 1);
                        buffer.incrementRowCount();
                        buffer.seal();
                        userDone.countDown();

                        // Wait for I/O thread to recycle
                        buffer.awaitRecycled();

                        // Reset and write again
                        buffer.reset();
                        buffer.writeByte((byte) 2);
                    } catch (Throwable t) {
                        error.set(t);
                    }
                });

                // Simulate I/O thread
                Thread ioThread = new Thread(() -> {
                    try {
                        userDone.await();
                        buffer.markSending();

                        // Simulate sending
                        Os.sleep(10);

                        buffer.markRecycled();
                        ioDone.countDown();
                    } catch (Throwable t) {
                        error.set(t);
                    }
                });

                userThread.start();
                ioThread.start();

                userThread.join(1000);
                ioThread.join(1000);

                Assert.assertNull(error.get());
                Assert.assertTrue(buffer.isFilling());
                Assert.assertEquals(1, buffer.getBufferPos());
            }
        });
    }

    @Test
    public void testConstructionWithDefaultThresholds() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(1024, buffer.getBufferCapacity());
                Assert.assertEquals(0, buffer.getBufferPos());
                Assert.assertEquals(0, buffer.getRowCount());
                Assert.assertTrue(buffer.isFilling());
                Assert.assertFalse(buffer.hasData());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithNegativeCapacity() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer ignored = new MicrobatchBuffer(-1)) {
                Assert.fail("Should have thrown");
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructionWithZeroCapacity() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer ignored = new MicrobatchBuffer(0)) {
                Assert.fail("Should have thrown");
            }
        });
    }

    @Test
    public void testEnsureCapacityGrows() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.ensureCapacity(2000);
                Assert.assertTrue(buffer.getBufferCapacity() >= 2000);
            }
        });
    }

    @Test
    public void testEnsureCapacityNoGrowth() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.ensureCapacity(512);
                Assert.assertEquals(1024, buffer.getBufferCapacity()); // No change
            }
        });
    }

    @Test
    public void testFirstRowTimeIsRecorded() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(0, buffer.getAgeNanos());

                buffer.incrementRowCount();
                long age1 = buffer.getAgeNanos();
                Assert.assertTrue(age1 >= 0);

                Os.sleep(10);

                long age2 = buffer.getAgeNanos();
                Assert.assertTrue(age2 > age1);
            }
        });
    }

    @Test
    public void testFullStateLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                // FILLING
                Assert.assertTrue(buffer.isFilling());
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();

                // FILLING -> SEALED
                buffer.seal();
                Assert.assertTrue(buffer.isSealed());

                // SEALED -> SENDING
                buffer.markSending();
                Assert.assertTrue(buffer.isSending());

                // SENDING -> RECYCLED
                buffer.markRecycled();
                Assert.assertTrue(buffer.isRecycled());

                // RECYCLED -> FILLING (reset)
                buffer.reset();
                Assert.assertTrue(buffer.isFilling());
                Assert.assertFalse(buffer.hasData());
            }
        });
    }

    @Test
    public void testIncrementRowCount() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(0, buffer.getRowCount());
                buffer.incrementRowCount();
                Assert.assertEquals(1, buffer.getRowCount());
                buffer.incrementRowCount();
                Assert.assertEquals(2, buffer.getRowCount());
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testIncrementRowCountWhenSealed() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.incrementRowCount(); // Should throw
            }
        });
    }

    @Test
    public void testInitialState() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                Assert.assertEquals(MicrobatchBuffer.STATE_FILLING, buffer.getState());
                Assert.assertTrue(buffer.isFilling());
                Assert.assertFalse(buffer.isSealed());
                Assert.assertFalse(buffer.isSending());
                Assert.assertFalse(buffer.isRecycled());
                Assert.assertFalse(buffer.isInUse());
            }
        });
    }

    @Test
    public void testMarkRecycledTransition() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();

                Assert.assertEquals(MicrobatchBuffer.STATE_RECYCLED, buffer.getState());
                Assert.assertTrue(buffer.isRecycled());
                Assert.assertFalse(buffer.isInUse());
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testMarkRecycledWhenNotSending() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markRecycled(); // Should throw - not sending
            }
        });
    }

    @Test
    public void testMarkSendingTransition() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();

                Assert.assertEquals(MicrobatchBuffer.STATE_SENDING, buffer.getState());
                Assert.assertTrue(buffer.isSending());
                Assert.assertTrue(buffer.isInUse());
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testMarkSendingWhenNotSealed() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.markSending(); // Should throw - not sealed
            }
        });
    }

    @Test
    public void testResetFromRecycled() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();
                long oldBatchId = buffer.getBatchId();

                buffer.seal();
                buffer.markSending();
                buffer.markRecycled();
                buffer.reset();

                Assert.assertTrue(buffer.isFilling());
                Assert.assertEquals(0, buffer.getBufferPos());
                Assert.assertEquals(0, buffer.getRowCount());
                Assert.assertNotEquals(oldBatchId, buffer.getBatchId());
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testResetWhenSealed() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.reset(); // Should throw
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testResetWhenSending() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.markSending();
                buffer.reset(); // Should throw
            }
        });
    }

    @Test
    public void testRollbackSealForRetry() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();

                buffer.seal();
                Assert.assertTrue(buffer.isSealed());

                buffer.rollbackSealForRetry();
                Assert.assertTrue(buffer.isFilling());

                // Verify the same batch remains writable after rollback.
                buffer.writeByte((byte) 2);
                buffer.incrementRowCount();
                Assert.assertEquals(2, buffer.getBufferPos());
                Assert.assertEquals(2, buffer.getRowCount());
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testRollbackSealWhenNotSealed() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.rollbackSealForRetry(); // Should throw - not sealed
            }
        });
    }

    @Test
    public void testSealTransition() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.seal();

                Assert.assertEquals(MicrobatchBuffer.STATE_SEALED, buffer.getState());
                Assert.assertFalse(buffer.isFilling());
                Assert.assertTrue(buffer.isSealed());
                Assert.assertTrue(buffer.isInUse());
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testSealWhenNotFilling() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.seal(); // Should throw
            }
        });
    }

    @Test
    public void testSetBufferPos() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.setBufferPos(100);
                Assert.assertEquals(100, buffer.getBufferPos());
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferPosNegative() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.setBufferPos(-1);
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetBufferPosOutOfBounds() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.setBufferPos(2000);
            }
        });
    }

    @Test
    public void testStateName() {
        Assert.assertEquals("FILLING", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_FILLING));
        Assert.assertEquals("SEALED", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_SEALED));
        Assert.assertEquals("SENDING", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_SENDING));
        Assert.assertEquals("RECYCLED", MicrobatchBuffer.stateName(MicrobatchBuffer.STATE_RECYCLED));
        Assert.assertEquals("UNKNOWN(99)", MicrobatchBuffer.stateName(99));
    }

    @Test
    public void testToString() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 1);
                buffer.incrementRowCount();

                String str = buffer.toString();
                Assert.assertTrue(str.contains("MicrobatchBuffer"));
                Assert.assertTrue(str.contains("state=FILLING"));
                Assert.assertTrue(str.contains("rows=1"));
                Assert.assertTrue(str.contains("bytes=1"));
            }
        });
    }

    @Test
    public void testWriteBeyondInitialCapacity() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(16)) {
                // Write more than initial capacity
                for (int i = 0; i < 100; i++) {
                    buffer.writeByte((byte) i);
                }
                Assert.assertEquals(100, buffer.getBufferPos());
                Assert.assertTrue(buffer.getBufferCapacity() >= 100);

                // Verify data integrity after growth
                for (int i = 0; i < 100; i++) {
                    byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr() + i);
                    Assert.assertEquals((byte) i, read);
                }
            }
        });
    }

    @Test
    public void testWriteByte() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.writeByte((byte) 0x42);
                Assert.assertEquals(1, buffer.getBufferPos());
                Assert.assertTrue(buffer.hasData());

                byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr());
                Assert.assertEquals((byte) 0x42, read);
            }
        });
    }

    @Test
    public void testWriteFromNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            long src = Unsafe.malloc(10, MemoryTag.NATIVE_DEFAULT);
            try {
                // Fill source with test data
                for (int i = 0; i < 10; i++) {
                    Unsafe.getUnsafe().putByte(src + i, (byte) (i + 100));
                }

                try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                    buffer.write(src, 10);
                    Assert.assertEquals(10, buffer.getBufferPos());

                    // Verify data
                    for (int i = 0; i < 10; i++) {
                        byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr() + i);
                        Assert.assertEquals((byte) (i + 100), read);
                    }
                }
            } finally {
                Unsafe.free(src, 10, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testWriteMultipleBytes() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                for (int i = 0; i < 100; i++) {
                    buffer.writeByte((byte) i);
                }
                Assert.assertEquals(100, buffer.getBufferPos());

                // Verify data
                for (int i = 0; i < 100; i++) {
                    byte read = Unsafe.getUnsafe().getByte(buffer.getBufferPtr() + i);
                    Assert.assertEquals((byte) i, read);
                }
            }
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteWhenSealed() throws Exception {
        assertMemoryLeak(() -> {
            try (MicrobatchBuffer buffer = new MicrobatchBuffer(1024)) {
                buffer.seal();
                buffer.writeByte((byte) 1); // Should throw
            }
        });
    }

    private static void awaitThreadBlocked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Os.sleep(1);
        }
        Assert.fail("Thread did not reach blocked state within 5s, state: " + thread.getState());
    }
}
