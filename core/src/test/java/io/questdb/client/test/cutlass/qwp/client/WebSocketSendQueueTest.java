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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class WebSocketSendQueueTest {

    @Test
    public void testEnqueueTimeoutWhenPendingSlotOccupied() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(1, 1_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            MicrobatchBuffer batch0 = sealedBuffer((byte) 1);
            MicrobatchBuffer batch1 = sealedBuffer((byte) 2);
            WebSocketSendQueue queue = null;

            try {
                // Keep window full so I/O thread cannot drain pending slot.
                window.addInFlight(0);
                queue = new WebSocketSendQueue(client, window, 100, 500);
                queue.enqueue(batch0);

                try {
                    queue.enqueue(batch1);
                    fail("Expected enqueue timeout");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("Enqueue timeout"));
                }
            } finally {
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                batch0.close();
                batch1.close();
                client.close();
            }
        });
    }

    @Test
    public void testEnqueueWaitsUntilSlotAvailable() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(1, 1_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            MicrobatchBuffer batch0 = sealedBuffer((byte) 1);
            MicrobatchBuffer batch1 = sealedBuffer((byte) 2);
            WebSocketSendQueue queue = null;

            try {
                window.addInFlight(0);
                queue = new WebSocketSendQueue(client, window, 2_000, 500);
                final WebSocketSendQueue finalQueue = queue;
                queue.enqueue(batch0);

                CountDownLatch started = new CountDownLatch(1);
                CountDownLatch finished = new CountDownLatch(1);
                AtomicReference<Throwable> errorRef = new AtomicReference<>();

                Thread t = new Thread(() -> {
                    started.countDown();
                    try {
                        finalQueue.enqueue(batch1);
                    } catch (Throwable t1) {
                        errorRef.set(t1);
                    } finally {
                        finished.countDown();
                    }
                });
                t.start();

                assertTrue(started.await(1, TimeUnit.SECONDS));
                awaitThreadBlocked(t);
                assertEquals("Second enqueue should still be waiting", 1, finished.getCount());

                // Free space so I/O thread can poll pending slot.
                window.acknowledgeUpTo(0);

                assertTrue("Second enqueue should complete", finished.await(2, TimeUnit.SECONDS));
                assertNull(errorRef.get());
            } finally {
                window.acknowledgeUpTo(Long.MAX_VALUE);
                closeQuietly(queue);
                batch0.close();
                batch1.close();
                client.close();
            }
        });
    }

    @Test
    public void testFlushFailsOnInvalidAckPayload() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(8, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            WebSocketSendQueue queue = null;
            CountDownLatch payloadDelivered = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean(false);

            try {
                window.addInFlight(0);
                client.setTryReceiveBehavior(handler -> {
                    if (fired.compareAndSet(false, true)) {
                        emitBinary(handler, new byte[]{1, 2, 3});
                        payloadDelivered.countDown();
                        return true;
                    }
                    return false;
                });

                queue = new WebSocketSendQueue(client, window, 1_000, 500);
                assertTrue("Expected invalid payload callback", payloadDelivered.await(2, TimeUnit.SECONDS));

                try {
                    queue.flush();
                    fail("Expected flush failure on invalid payload");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("Invalid ACK response payload"));
                }
            } finally {
                closeQuietly(queue);
                client.close();
            }
        });
    }

    @Test
    public void testFlushFailsOnReceiveIoError() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(8, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            WebSocketSendQueue queue = null;
            CountDownLatch receiveAttempted = new CountDownLatch(1);

            try {
                window.addInFlight(0);
                client.setTryReceiveBehavior(handler -> {
                    receiveAttempted.countDown();
                    throw new RuntimeException("recv-fail");
                });

                queue = new WebSocketSendQueue(client, window, 1_000, 500);
                assertTrue("Expected receive attempt", receiveAttempted.await(2, TimeUnit.SECONDS));
                long deadline = System.currentTimeMillis() + 2_000;
                while (queue.getLastError() == null && System.currentTimeMillis() < deadline) {
                    Thread.sleep(5);
                }
                assertNotNull("Expected queue error after receive failure", queue.getLastError());

                try {
                    queue.flush();
                    fail("Expected flush failure after receive error");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("Error receiving response"));
                }
            } finally {
                closeQuietly(queue);
                client.close();
            }
        });
    }

    @Test
    public void testFlushFailsOnSendIoError() throws Exception {
        assertMemoryLeak(() -> {
            FakeWebSocketClient client = new FakeWebSocketClient();
            MicrobatchBuffer batch = sealedBuffer((byte) 42);
            WebSocketSendQueue queue = null;

            try {
                client.setSendBehavior((dataPtr, length) -> {
                    throw new RuntimeException("send-fail");
                });
                queue = new WebSocketSendQueue(client, null, 1_000, 500);
                queue.enqueue(batch);

                try {
                    queue.flush();
                    fail("Expected flush failure after send error");
                } catch (LineSenderException e) {
                    assertTrue(
                            e.getMessage().contains("Error sending batch")
                                    || e.getMessage().contains("Error in send queue I/O thread")
                    );
                }
            } finally {
                closeQuietly(queue);
                batch.close();
                client.close();
            }
        });
    }

    @Test
    public void testFlushFailsWhenServerClosesConnection() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(8, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            WebSocketSendQueue queue = null;
            CountDownLatch closeDelivered = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean(false);

            try {
                window.addInFlight(0);
                client.setTryReceiveBehavior(handler -> {
                    if (fired.compareAndSet(false, true)) {
                        handler.onClose(1006, "boom");
                        closeDelivered.countDown();
                        return true;
                    }
                    return false;
                });

                queue = new WebSocketSendQueue(client, window, 1_000, 500);
                assertTrue("Expected close callback", closeDelivered.await(2, TimeUnit.SECONDS));

                try {
                    queue.flush();
                    fail("Expected flush failure after close");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("closed"));
                }
            } finally {
                closeQuietly(queue);
                client.close();
            }
        });
    }

    @Test
    public void testAwaitPendingAcksKeepsDrainNonBlocking() throws Exception {
        assertMemoryLeak(() -> {
            InFlightWindow window = new InFlightWindow(8, 5_000);
            FakeWebSocketClient client = new FakeWebSocketClient();
            WebSocketSendQueue queue = null;
            MicrobatchBuffer batch0 = sealedBuffer((byte) 1);
            MicrobatchBuffer batch1 = sealedBuffer((byte) 2);
            CountDownLatch secondBatchSent = new CountDownLatch(1);
            AtomicBoolean deliverAcks = new AtomicBoolean(false);
            AtomicInteger tryReceivePolls = new AtomicInteger();
            AtomicLong highestSent = new AtomicLong(-1);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            try {
                client.setSendBehavior((dataPtr, length) -> {
                    long sent = highestSent.incrementAndGet();
                    if (sent == 1) {
                        secondBatchSent.countDown();
                    }
                });
                client.setReceiveBehavior((handler, timeout) -> {
                    throw new AssertionError("receiveFrame() must not be used while draining ACKs");
                });
                client.setTryReceiveBehavior(handler -> {
                    tryReceivePolls.incrementAndGet();
                    if (deliverAcks.get()) {
                        long sent = highestSent.get();
                        if (sent >= 0 && window.getInFlightCount() > 0) {
                            emitAck(handler, sent);
                            return true;
                        }
                    }
                    return false;
                });

                queue = new WebSocketSendQueue(client, window, 1_000, 500);
                queue.enqueue(batch0);
                queue.flush();

                CountDownLatch finished = new CountDownLatch(1);
                WebSocketSendQueue finalQueue = queue;
                Thread waiter = new Thread(() -> {
                    try {
                        finalQueue.awaitPendingAcks();
                    } catch (Throwable t) {
                        errorRef.set(t);
                    } finally {
                        finished.countDown();
                    }
                });
                waiter.start();

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (tryReceivePolls.get() == 0 && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                assertTrue("Expected non-blocking ACK polls while draining", tryReceivePolls.get() > 0);

                queue.enqueue(batch1);
                assertTrue("I/O thread should still send new work while ACK drain is active",
                        secondBatchSent.await(1, TimeUnit.SECONDS));

                deliverAcks.set(true);

                assertTrue("awaitPendingAcks should complete once ACK arrives",
                        finished.await(2, TimeUnit.SECONDS));
                assertNull(errorRef.get());
                assertEquals(0, window.getInFlightCount());
            } finally {
                closeQuietly(queue);
                batch0.close();
                batch1.close();
                client.close();
            }
        });
    }

    private static void awaitThreadBlocked(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread.State state = thread.getState();
            if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) {
                return;
            }
            Thread.sleep(1);
        }
        fail("Thread did not reach blocked state within 5s, state: " + thread.getState());
    }

    private static void closeQuietly(WebSocketSendQueue queue) {
        if (queue != null) {
            queue.close();
        }
    }

    private static void emitBinary(WebSocketFrameHandler handler, byte[] payload) {
        long ptr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, payload[i]);
            }
            handler.onBinaryMessage(ptr, payload.length);
        } finally {
            Unsafe.free(ptr, payload.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void emitAck(WebSocketFrameHandler handler, long sequence) {
        WebSocketResponse response = WebSocketResponse.success(sequence);
        int size = response.serializedSize();
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            response.writeTo(ptr);
            handler.onBinaryMessage(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static MicrobatchBuffer sealedBuffer(byte value) {
        MicrobatchBuffer buffer = new MicrobatchBuffer(64);
        buffer.writeByte(value);
        buffer.incrementRowCount();
        buffer.seal();
        return buffer;
    }

    private interface SendBehavior {
        void send(long dataPtr, int length);
    }

    private interface TryReceiveBehavior {
        boolean tryReceive(WebSocketFrameHandler handler);
    }

    private interface ReceiveBehavior {
        boolean receive(WebSocketFrameHandler handler, int timeout);
    }

    private static class FakeWebSocketClient extends WebSocketClient {
        private volatile TryReceiveBehavior behavior = handler -> false;
        private volatile boolean connected = true;
        private volatile ReceiveBehavior receiveBehavior = (handler, timeout) -> false;
        private volatile SendBehavior sendBehavior = (dataPtr, length) -> {
        };

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

        public void setTryReceiveBehavior(TryReceiveBehavior behavior) {
            this.behavior = behavior;
        }

        public void setReceiveBehavior(ReceiveBehavior receiveBehavior) {
            this.receiveBehavior = receiveBehavior;
        }

        @Override
        public boolean receiveFrame(WebSocketFrameHandler handler, int timeout) {
            return receiveBehavior.receive(handler, timeout);
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            return behavior.tryReceive(handler);
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
