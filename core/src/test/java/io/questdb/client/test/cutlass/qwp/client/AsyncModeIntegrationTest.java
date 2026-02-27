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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.InFlightWindow;
import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.WebSocketSendQueue;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Integration tests for async mode: double-buffering, send queue, and
 * in-flight window working together.
 * <p>
 * These tests verify the interaction between the three async mode components
 * ({@link MicrobatchBuffer}, {@link WebSocketSendQueue}, {@link InFlightWindow})
 * without requiring a running QuestDB server. They use {@link FakeWebSocketClient}
 * to simulate server behavior and control ACK timing.
 */
public class AsyncModeIntegrationTest {

    /**
     * Window of 2. Sends 2 batches (fills window), then enqueues a 3rd to
     * occupy the pending slot. The 4th enqueue blocks because the pending
     * slot is occupied and the I/O thread cannot poll it (window full).
     * Delivering ACKs unblocks the pipeline.
     */
    @Test
    public void testBackpressureBlocksEnqueueUntilAck() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(2, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicLong highestAcked = new AtomicLong(-1);
            CountDownLatch twoSent = new CountDownLatch(2);
            AtomicBoolean deliverAcks = new AtomicBoolean(false);

            client.setSendBehavior((ptr, len) -> {
                highestSent.incrementAndGet();
                twoSent.countDown();
            });
            client.setTryReceiveBehavior(handler -> {
                if (deliverAcks.get()) {
                    long sent = highestSent.get();
                    long acked = highestAcked.get();
                    if (sent > acked) {
                        highestAcked.set(sent);
                        emitAck(handler, sent);
                        return true;
                    }
                }
                return false;
            });

            WebSocketSendQueue queue = null;
            MicrobatchBuffer buf0 = new MicrobatchBuffer(256);
            MicrobatchBuffer buf1 = new MicrobatchBuffer(256);

            try {
                queue = new WebSocketSendQueue(client, window, 3_000, 500);

                // Send 2 batches to fill the window.
                buf0.writeByte((byte) 1);
                buf0.incrementRowCount();
                buf0.seal();
                queue.enqueue(buf0);

                buf1.writeByte((byte) 2);
                buf1.incrementRowCount();
                buf1.seal();
                queue.enqueue(buf1);

                assertTrue("Both batches should be sent", twoSent.await(2, TimeUnit.SECONDS));
                assertEquals("Window should be full", 2, window.getInFlightCount());

                // Reuse buf0 (recycled by I/O thread) and enqueue a 3rd batch.
                // The I/O thread cannot poll it because the window is full.
                assertTrue(buf0.awaitRecycled(2, TimeUnit.SECONDS));
                buf0.reset();
                buf0.writeByte((byte) 3);
                buf0.incrementRowCount();
                buf0.seal();
                queue.enqueue(buf0);

                // Reuse buf1 and try to enqueue a 4th batch on a background
                // thread. It should block because the pending slot is still
                // occupied by the 3rd batch.
                assertTrue(buf1.awaitRecycled(2, TimeUnit.SECONDS));
                buf1.reset();
                buf1.writeByte((byte) 4);
                buf1.incrementRowCount();
                buf1.seal();

                CountDownLatch enqueueStarted = new CountDownLatch(1);
                CountDownLatch enqueueDone = new CountDownLatch(1);
                AtomicReference<Throwable> errorRef = new AtomicReference<>();
                WebSocketSendQueue q = queue;

                Thread enqueueThread = new Thread(() -> {
                    enqueueStarted.countDown();
                    try {
                        q.enqueue(buf1);
                    } catch (Throwable t) {
                        errorRef.set(t);
                    } finally {
                        enqueueDone.countDown();
                    }
                });
                enqueueThread.start();

                assertTrue(enqueueStarted.await(1, TimeUnit.SECONDS));
                Thread.sleep(200);
                assertEquals("Enqueue should still be blocked", 1, enqueueDone.getCount());

                // Deliver ACKs to unblock the pipeline.
                deliverAcks.set(true);

                assertTrue("Enqueue should complete after ACK", enqueueDone.await(3, TimeUnit.SECONDS));
                assertNull("No error expected", errorRef.get());

                queue.flush();
                window.awaitEmpty();
            } finally {
                deliverAcks.set(true);
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                buf0.close();
                buf1.close();
                client.close();
            }
        });
    }

    /**
     * Sends 10 batches through 2 alternating buffers with auto-ACK.
     * Each buffer cycles through all states multiple times:
     * FILLING -> SEALED -> SENDING -> RECYCLED -> FILLING.
     */
    @Test
    public void testBatchesCycleThroughDoubleBuffers() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(4, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicLong highestAcked = new AtomicLong(-1);

            client.setSendBehavior((ptr, len) -> highestSent.incrementAndGet());
            client.setTryReceiveBehavior(handler -> {
                long sent = highestSent.get();
                long acked = highestAcked.get();
                if (sent > acked) {
                    highestAcked.set(sent);
                    emitAck(handler, sent);
                    return true;
                }
                return false;
            });

            WebSocketSendQueue queue = null;
            MicrobatchBuffer buf0 = new MicrobatchBuffer(256);
            MicrobatchBuffer buf1 = new MicrobatchBuffer(256);
            int batchCount = 10;

            try {
                queue = new WebSocketSendQueue(client, window, 5_000, 500);
                MicrobatchBuffer active = buf0;

                for (int i = 0; i < batchCount; i++) {
                    if (active.isRecycled()) {
                        active.reset();
                    }
                    assertTrue("Buffer should be FILLING on iteration " + i, active.isFilling());

                    active.writeByte((byte) (i & 0xFF));
                    active.incrementRowCount();
                    active.seal();
                    queue.enqueue(active);

                    // Swap to the other buffer, waiting for it if still in use.
                    MicrobatchBuffer other = (active == buf0) ? buf1 : buf0;
                    if (other.isInUse()) {
                        assertTrue("Other buffer should recycle",
                                other.awaitRecycled(2, TimeUnit.SECONDS));
                    }
                    if (other.isRecycled()) {
                        other.reset();
                    }
                    active = other;
                }

                queue.flush();
                window.awaitEmpty();

                assertEquals(batchCount, queue.getTotalBatchesSent());
                assertEquals(0, window.getInFlightCount());
            } finally {
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                buf0.close();
                buf1.close();
                client.close();
            }
        });
    }

    /**
     * The first send blocks in sendBinary (simulating slow I/O).
     * The user enqueues a second batch, then tries to swap back to the
     * first buffer which is still in SENDING state. The user must wait
     * until the I/O thread finishes and recycles the buffer.
     */
    @Test
    public void testBufferSwapWaitsForSlowSend() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(4, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicLong highestAcked = new AtomicLong(-1);
            CountDownLatch sendStarted = new CountDownLatch(1);
            CountDownLatch sendGate = new CountDownLatch(1);

            client.setSendBehavior((ptr, len) -> {
                long seq = highestSent.incrementAndGet();
                if (seq == 0) {
                    // Block on first send to simulate slow I/O.
                    sendStarted.countDown();
                    try {
                        if (!sendGate.await(5, TimeUnit.SECONDS)) {
                            throw new RuntimeException("sendGate timed out");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            client.setTryReceiveBehavior(handler -> {
                long sent = highestSent.get();
                long acked = highestAcked.get();
                if (sent > acked) {
                    highestAcked.set(sent);
                    emitAck(handler, sent);
                    return true;
                }
                return false;
            });

            WebSocketSendQueue queue = null;
            MicrobatchBuffer buf0 = new MicrobatchBuffer(256);
            MicrobatchBuffer buf1 = new MicrobatchBuffer(256);

            try {
                queue = new WebSocketSendQueue(client, window, 5_000, 500);

                // Enqueue buf0. The I/O thread starts sending and blocks.
                buf0.writeByte((byte) 1);
                buf0.incrementRowCount();
                buf0.seal();
                queue.enqueue(buf0);

                assertTrue("I/O thread should start sending", sendStarted.await(2, TimeUnit.SECONDS));
                assertTrue("buf0 should be in use (SENDING)", buf0.isInUse());

                // Enqueue buf1 into the pending slot (I/O thread is blocked).
                buf1.writeByte((byte) 2);
                buf1.incrementRowCount();
                buf1.seal();
                queue.enqueue(buf1);

                // The user wants to reuse buf0, but it is still SENDING.
                assertTrue("buf0 should still be in use", buf0.isInUse());

                // Release the gate so the I/O thread can finish sending buf0.
                sendGate.countDown();

                // buf0 transitions SENDING -> RECYCLED.
                assertTrue("buf0 should be recycled after send completes",
                        buf0.awaitRecycled(2, TimeUnit.SECONDS));
                assertTrue(buf0.isRecycled());

                // Reset and verify buf0 is reusable.
                buf0.reset();
                assertTrue(buf0.isFilling());

                queue.flush();
                window.awaitEmpty();
                assertEquals(2, queue.getTotalBatchesSent());
            } finally {
                sendGate.countDown();
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                buf0.close();
                buf1.close();
                client.close();
            }
        });
    }

    /**
     * Verifies that {@link WebSocketSendQueue#flush()} returns once the
     * batch has been sent over the wire, even though the server has not
     * ACKed it yet. The caller must separately call
     * {@link InFlightWindow#awaitEmpty()} to wait for the ACK.
     */
    @Test
    public void testFlushWaitsForSendButNotForAcks() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(4, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicBoolean deliverAcks = new AtomicBoolean(false);

            client.setSendBehavior((ptr, len) -> highestSent.incrementAndGet());
            client.setTryReceiveBehavior(handler -> {
                if (deliverAcks.get()) {
                    long sent = highestSent.get();
                    if (sent >= 0 && window.getInFlightCount() > 0) {
                        emitAck(handler, sent);
                        return true;
                    }
                }
                return false;
            });

            WebSocketSendQueue queue = null;
            MicrobatchBuffer buf0 = new MicrobatchBuffer(256);

            try {
                queue = new WebSocketSendQueue(client, window, 2_000, 500);

                buf0.writeByte((byte) 1);
                buf0.incrementRowCount();
                buf0.seal();
                queue.enqueue(buf0);

                // flush() returns once the batch is sent, not when ACKed.
                queue.flush();
                assertEquals(1, queue.getTotalBatchesSent());
                assertEquals("Batch should still be in flight", 1, window.getInFlightCount());

                // Deliver ACK and wait for the window to drain.
                deliverAcks.set(true);
                window.awaitEmpty();
                assertEquals(0, window.getInFlightCount());
            } finally {
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                buf0.close();
                client.close();
            }
        });
    }

    /**
     * Sends 50 batches through 2 buffers with a window of 4.
     * ACKs arrive one-at-a-time (non-cumulative) to test sustained flow
     * control under moderate backpressure.
     */
    @Test
    public void testHighThroughputWithManyBatches() throws Exception {
        assertMemoryLeak(() -> {
            int batchCount = 50;
            int windowSize = 4;

            InFlightWindow window = new InFlightWindow(windowSize, 10_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicLong highestAcked = new AtomicLong(-1);

            client.setSendBehavior((ptr, len) -> highestSent.incrementAndGet());
            client.setTryReceiveBehavior(handler -> {
                long sent = highestSent.get();
                long acked = highestAcked.get();
                if (sent > acked) {
                    // ACK one batch at a time to test sustained flow.
                    long next = acked + 1;
                    highestAcked.set(next);
                    emitAck(handler, next);
                    return true;
                }
                return false;
            });

            WebSocketSendQueue queue = null;
            MicrobatchBuffer buf0 = new MicrobatchBuffer(256);
            MicrobatchBuffer buf1 = new MicrobatchBuffer(256);

            try {
                queue = new WebSocketSendQueue(client, window, 10_000, 2_000);
                MicrobatchBuffer active = buf0;

                for (int i = 0; i < batchCount; i++) {
                    if (!active.isFilling()) {
                        if (active.isInUse()) {
                            assertTrue("Buffer should recycle on iteration " + i,
                                    active.awaitRecycled(5, TimeUnit.SECONDS));
                        }
                        active.reset();
                    }

                    active.writeByte((byte) (i & 0xFF));
                    active.incrementRowCount();
                    active.seal();
                    queue.enqueue(active);

                    active = (active == buf0) ? buf1 : buf0;
                }

                queue.flush();
                window.awaitEmpty();

                assertEquals(batchCount, queue.getTotalBatchesSent());
                assertEquals(0, window.getInFlightCount());
            } finally {
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                buf0.close();
                buf1.close();
                client.close();
            }
        });
    }

    /**
     * The server ACKs the first batch but returns a WRITE_ERROR for the
     * second. {@link WebSocketSendQueue#flush()} completes (both batches
     * were sent) but {@link InFlightWindow#awaitEmpty()} surfaces the error.
     */
    @Test
    public void testServerErrorPropagatesOnFlush() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(4, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicLong highestDelivered = new AtomicLong(-1);

            client.setSendBehavior((ptr, len) -> highestSent.incrementAndGet());
            client.setTryReceiveBehavior(handler -> {
                long sent = highestSent.get();
                long delivered = highestDelivered.get();
                if (sent > delivered) {
                    long next = delivered + 1;
                    highestDelivered.set(next);
                    if (next == 1) {
                        emitError(handler, next, WebSocketResponse.STATUS_WRITE_ERROR, "disk full");
                    } else {
                        emitAck(handler, next);
                    }
                    return true;
                }
                return false;
            });

            WebSocketSendQueue queue = null;
            MicrobatchBuffer buf0 = new MicrobatchBuffer(256);
            MicrobatchBuffer buf1 = new MicrobatchBuffer(256);

            try {
                queue = new WebSocketSendQueue(client, window, 2_000, 500);

                buf0.writeByte((byte) 1);
                buf0.incrementRowCount();
                buf0.seal();
                queue.enqueue(buf0);

                buf1.writeByte((byte) 2);
                buf1.incrementRowCount();
                buf1.seal();
                queue.enqueue(buf1);

                // flush() waits for the queue to drain (both batches sent).
                queue.flush();

                // awaitEmpty() surfaces the server error for batch 1.
                try {
                    window.awaitEmpty();
                    fail("Expected server error to propagate");
                } catch (LineSenderException e) {
                    assertTrue("Error should mention server failure",
                            e.getMessage().contains("disk full") || e.getMessage().contains("Server error"));
                }
            } finally {
                closeQuietly(queue);
                buf0.close();
                buf1.close();
                client.close();
            }
        });
    }

    private static void closeQuietly(WebSocketSendQueue queue) {
        if (queue != null) {
            queue.close();
        }
    }

    private static void emitAck(WebSocketFrameHandler handler, long sequence) {
        WebSocketResponse resp = WebSocketResponse.success(sequence);
        int size = resp.serializedSize();
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            resp.writeTo(ptr);
            handler.onBinaryMessage(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void emitError(WebSocketFrameHandler handler, long sequence, byte status, String message) {
        WebSocketResponse resp = WebSocketResponse.error(sequence, status, message);
        int size = resp.serializedSize();
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            resp.writeTo(ptr);
            handler.onBinaryMessage(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private interface SendBehavior {
        void send(long dataPtr, int length);
    }

    private interface TryReceiveBehavior {
        boolean tryReceive(WebSocketFrameHandler handler);
    }

    private static class FakeWebSocketClient extends WebSocketClient {
        private volatile boolean connected = true;
        private volatile SendBehavior sendBehavior = (dataPtr, length) -> {};
        private volatile TryReceiveBehavior tryReceiveBehavior = handler -> false;

        private FakeWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void close() {
            connected = false;
            super.close();
        }

        @Override
        public boolean isConnected() {
            return connected;
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sendBehavior.send(dataPtr, length);
        }

        public void setSendBehavior(SendBehavior sendBehavior) {
            this.sendBehavior = sendBehavior;
        }

        public void setTryReceiveBehavior(TryReceiveBehavior tryReceiveBehavior) {
            this.tryReceiveBehavior = tryReceiveBehavior;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            return tryReceiveBehavior.tryReceive(handler);
        }

        @Override
        protected void ioWait(int timeout, int op) {
            // no-op
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }
}
