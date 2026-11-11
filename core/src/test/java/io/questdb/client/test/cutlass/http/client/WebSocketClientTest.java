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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.test.tools.TestWebSocketClient;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.http.client.WebSocketSendBuffer;
import io.questdb.client.network.IOOperation;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class WebSocketClientTest {

    /**
     * Batched frames go out back to back in ONE send(), each a well-formed
     * masked binary frame with an exact-size header (no MAX_HEADER_SIZE gap
     * between frames); an empty flush writes nothing.
     */
    @Test
    public void testBatchFlushSendsAppendedFramesInOneWrite() throws Exception {
        assertMemoryLeak(() -> {
            CapturingSocket socket = new CapturingSocket();
            // 7: 7-bit length, 200: 16-bit length, 0: empty payload
            int[] lens = {7, 200, 0};
            long payload = Unsafe.malloc(200, MemoryTag.NATIVE_DEFAULT);
            try (CapturingWebSocketClient client = new CapturingWebSocketClient(socket)) {
                setUpgradedTrue(client);
                for (int i = 0; i < 200; i++) {
                    Unsafe.getUnsafe().putByte(payload + i, (byte) i);
                }

                client.batchFlush();
                Assert.assertEquals("an empty flush must not write", 0, socket.sendCalls);

                for (int len : lens) {
                    client.batchAppendBinary(payload, len);
                }
                Assert.assertEquals("appending must not write", 0, socket.sendCalls);
                client.batchFlush();
                Assert.assertEquals("the whole batch must be one write", 1, socket.sendCalls);

                byte[] wire = socket.sent;
                int pos = 0;
                for (int len : lens) {
                    Assert.assertEquals("FIN + BINARY", (byte) 0x82, wire[pos]);
                    int maskPos;
                    if (len <= 125) {
                        Assert.assertEquals((byte) (0x80 | len), wire[pos + 1]);
                        maskPos = pos + 2;
                    } else {
                        Assert.assertEquals((byte) (0x80 | 126), wire[pos + 1]);
                        Assert.assertEquals(len, ((wire[pos + 2] & 0xFF) << 8) | (wire[pos + 3] & 0xFF));
                        maskPos = pos + 4;
                    }
                    int payloadPos = maskPos + 4;
                    for (int i = 0; i < len; i++) {
                        Assert.assertEquals((byte) i, (byte) (wire[payloadPos + i] ^ wire[maskPos + (i & 3)]));
                    }
                    pos = payloadPos + len;
                }
                Assert.assertEquals("no bytes beyond the 3 frames", wire.length, pos);

                client.batchFlush();
                Assert.assertEquals("a flushed batch is empty", 1, socket.sendCalls);
            } finally {
                Unsafe.free(payload, 200, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * close() frees native memory (recv/fragment buffers, send buffers), so
     * its guard must be a CAS, not a volatile check-then-act: two concurrent
     * closers passing the flag check together would both run
     * disconnect()/Unsafe.free -- a native double-free. Closers can race in
     * practice: the owner thread's teardown vs the I/O thread's exit path vs
     * stale duplicate references (see CursorWebSocketSendLoop). The memory
     * counters checked by assertMemoryLeak flag a double-free as a counter
     * mismatch.
     */
    @Test
    public void testConcurrentCloseRunsTeardownExactlyOnce() throws Exception {
        assertMemoryLeak(() -> {
            final int threads = 4;
            final int iterations = 200;
            for (int i = 0; i < iterations; i++) {
                StubWebSocketClient client = new StubWebSocketClient();
                CyclicBarrier barrier = new CyclicBarrier(threads);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread[] closers = new Thread[threads];
                for (int t = 0; t < threads; t++) {
                    closers[t] = new Thread(() -> {
                        try {
                            barrier.await();
                            client.close();
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    });
                    closers[t].start();
                }
                for (Thread closer : closers) {
                    closer.join();
                }
                Throwable t = failure.get();
                if (t != null) {
                    throw new AssertionError("concurrent close failed on iteration " + i, t);
                }
            }
        });
    }

    @Test
    public void testExtractMaxBatchSizeAbsentHeaderReturnsZero() throws Exception {
        String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: x\r\n"
                + "X-QWP-Version: 1\r\n"
                + "\r\n";
        Assert.assertEquals(0, invokeExtractMaxBatchSize(response));
    }

    @Test
    public void testExtractMaxBatchSizeMalformedReturnsZero() throws Exception {
        String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "X-QWP-Max-Batch-Size: not-a-number\r\n"
                + "\r\n";
        Assert.assertEquals(0, invokeExtractMaxBatchSize(response));
    }

    @Test
    public void testExtractMaxBatchSizeNegativeReturnsZero() throws Exception {
        // Negative or zero is a server bug; clamp to 0 so the sender falls
        // back to its configured budget instead of producing a nonsense limit.
        String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "X-QWP-Max-Batch-Size: -1\r\n"
                + "\r\n";
        Assert.assertEquals(0, invokeExtractMaxBatchSize(response));
    }

    @Test
    public void testExtractMaxBatchSizeParsesPositive() throws Exception {
        String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "Upgrade: websocket\r\n"
                + "Connection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: x\r\n"
                + "X-QWP-Version: 1\r\n"
                + "X-QWP-Max-Batch-Size: 16777216\r\n"
                + "\r\n";
        Assert.assertEquals(16 * 1024 * 1024, invokeExtractMaxBatchSize(response));
    }

    @Test
    public void testExtractMaxUncommittedRowsAbsentHeaderReturnsZero() throws Exception {
        String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "X-QWP-Version: 1\r\n"
                + "X-QWP-Max-Batch-Size: 16777216\r\n"
                + "\r\n";
        Assert.assertEquals(0, invokeExtractMaxUncommittedRows(response));
    }

    @Test
    public void testExtractMaxUncommittedRowsMalformedOrNegativeReturnsZero() throws Exception {
        // 0 means "not advertised": a strict transaction then assumes the default.
        Assert.assertEquals(0, invokeExtractMaxUncommittedRows("HTTP/1.1 101 Switching Protocols\r\n"
                + "X-QWP-Max-Uncommitted-Rows: lots\r\n\r\n"));
        Assert.assertEquals(0, invokeExtractMaxUncommittedRows("HTTP/1.1 101 Switching Protocols\r\n"
                + "X-QWP-Max-Uncommitted-Rows: -5\r\n\r\n"));
    }

    @Test
    public void testExtractMaxUncommittedRowsParsesBeyondIntRange() throws Exception {
        String response = "HTTP/1.1 101 Switching Protocols\r\n"
                + "x-qwp-max-uncommitted-rows:  5000000000 \r\n"
                + "\r\n";
        Assert.assertEquals(5_000_000_000L, invokeExtractMaxUncommittedRows(response));
    }

    @Test
    public void testFrameBurstIsParsedInPlace() throws Exception {
        assertFrameBurstIsParsedInPlace(false);
    }

    @Test
    public void testFrameBurstIsParsedInPlaceWithBlockingReceive() throws Exception {
        assertFrameBurstIsParsedInPlace(true);
    }

    /**
     * A frame handler may close() the client from inside its callback:
     * CursorWebSocketSendLoop's NACK-recycle path (handleServerRejection /
     * handlePreSendRejection -> failPaced()/fail() -> connectLoop ->
     * swapClient -> oldClient.close()) runs synchronously on the I/O thread
     * while that thread is still inside this client's tryParseFrame. The
     * post-callback tail must detect the close and touch no recv state:
     * before the guard, it left recvPos negative on the closed client and
     * was one close()-reorder away from a memmove on freed memory.
     */
    @Test
    public void testInCallbackCloseFromBinaryHandlerLeavesStateCoherent() throws Exception {
        assertMemoryLeak(() -> {
            // Unmasked server->client binary frame: FIN|BINARY, len=4, "abcd"
            byte[] frame = {(byte) 0x82, 0x04, 'a', 'b', 'c', 'd'};
            try (FrameFeedWebSocketClient client = new FrameFeedWebSocketClient(frame)) {
                setUpgradedTrue(client);

                boolean received = client.tryReceiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        Assert.assertEquals(4, payloadLen);
                        client.close();
                    }

                    @Override
                    public void onClose(int code, String reason) {
                    }
                });

                Assert.assertTrue("frame must be reported as received", received);
                Assert.assertEquals("recvPos must stay at disconnect()'s reset, not go negative",
                        0, getIntField(client, "recvPos"));
                Assert.assertEquals(0, getIntField(client, "recvReadPos"));
            }
        });
    }

    /**
     * Same contract for the CLOSE-frame branch: onClose handlers routinely
     * recycle the connection (every WS close is reconnect-eligible), which
     * closes this client before tryParseFrame's tail runs.
     */
    @Test
    public void testInCallbackCloseFromCloseHandlerLeavesStateCoherent() throws Exception {
        assertMemoryLeak(() -> {
            // Unmasked server->client close frame: FIN|CLOSE, len=2, code=1000
            byte[] frame = {(byte) 0x88, 0x02, 0x03, (byte) 0xE8};
            try (FrameFeedWebSocketClient client = new FrameFeedWebSocketClient(frame)) {
                setUpgradedTrue(client);

                boolean received = client.tryReceiveFrame(new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        Assert.fail("unexpected binary frame");
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        Assert.assertEquals(1000, code);
                        client.close();
                    }
                });

                Assert.assertTrue("frame must be reported as received", received);
                Assert.assertEquals("recvPos must stay at disconnect()'s reset, not go negative",
                        0, getIntField(client, "recvPos"));
                Assert.assertEquals(0, getIntField(client, "recvReadPos"));
            }
        });
    }

    /**
     * Fragmented-message variant: the CONTINUATION branch runs
     * resetFragmentState() after the handler returns and before the
     * closed-client guard. Pin that an in-callback close from the
     * reassembled-message dispatch stays safe there too: close() frees
     * fragmentBufPtr, so resetFragmentState() must only write fields.
     */
    @Test
    public void testInCallbackCloseFromFragmentedMessageLeavesStateCoherent() throws Exception {
        assertMemoryLeak(() -> {
            // Two unmasked server->client frames: non-FIN BINARY "ab",
            // then FIN CONTINUATION "cd" -> one reassembled message "abcd"
            byte[] frames = {
                    0x02, 0x02, 'a', 'b',
                    (byte) 0x80, 0x02, 'c', 'd'
            };
            try (FrameFeedWebSocketClient client = new FrameFeedWebSocketClient(frames)) {
                setUpgradedTrue(client);
                final int[] messages = {0};
                WebSocketFrameHandler handler = new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        messages[0]++;
                        Assert.assertEquals(4, payloadLen);
                        client.close();
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        Assert.fail("unexpected close frame");
                    }
                };

                // First call consumes the initial fragment: parsed and
                // buffered, no dispatch yet.
                Assert.assertTrue(client.tryReceiveFrame(handler));
                Assert.assertEquals(0, messages[0]);

                // Second call reassembles and dispatches; the handler
                // closes the client in-callback.
                Assert.assertTrue(client.tryReceiveFrame(handler));
                Assert.assertEquals(1, messages[0]);
                Assert.assertEquals(0, getIntField(client, "recvPos"));
                Assert.assertEquals(0, getIntField(client, "recvReadPos"));
            }
        });
    }

    @Test
    public void testRecvOrTimeoutPropagatesNonTimeoutError() throws Exception {
        assertMemoryLeak(() -> {
            try (RecvTestWebSocketClient client = new RecvTestWebSocketClient()) {
                setUpgradedTrue(client);

                // socket.recv() returns 0, triggering the ioWait path
                // ioWait throws a non-timeout error (e.g., queue/poll failure)
                client.ioWaitAction = () -> {
                    throw new HttpClientException("queue error [errno=").put(5).put(']');
                };

                WebSocketFrameHandler noOpHandler = new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                    }

                    @Override
                    public void onClose(int code, String reason) {
                    }
                };

                try {
                    client.receiveFrame(noOpHandler, 1000);
                    Assert.fail("expected HttpClientException for queue error");
                } catch (HttpClientException e) {
                    Assert.assertFalse("non-timeout error must not be flagged as timeout", e.isTimeout());
                    Assert.assertTrue(
                            "expected queue error message, got: " + e.getMessage(),
                            e.getMessage().contains("queue error")
                    );
                }
            }
        });
    }

    @Test
    public void testRecvOrTimeoutReturnsFalseOnTimeout() throws Exception {
        assertMemoryLeak(() -> {
            try (RecvTestWebSocketClient client = new RecvTestWebSocketClient()) {
                setUpgradedTrue(client);

                // socket.recv() returns 0, triggering the ioWait path
                // ioWait throws a timeout error
                client.ioWaitAction = () -> {
                    throw new HttpClientException("timed out [errno=").put(0).put(']').flagAsTimeout();
                };

                WebSocketFrameHandler noOpHandler = new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                    }

                    @Override
                    public void onClose(int code, String reason) {
                    }
                };

                boolean result = client.receiveFrame(noOpHandler, 1000);
                Assert.assertFalse("receiveFrame should return false on timeout", result);
            }
        });
    }

    @Test
    public void testSendBinaryAssemblesTwoPayloadSlicesIntoOneMaskedFrame() throws Exception {
        assertMemoryLeak(() -> {
            CapturingSocket socket = new CapturingSocket();
            long first = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
            long second = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try (CapturingWebSocketClient client = new CapturingWebSocketClient(socket)) {
                setUpgradedTrue(client);
                for (int i = 0; i < 3; i++) {
                    Unsafe.getUnsafe().putByte(first + i, (byte) (i + 1));
                }
                for (int i = 0; i < 4; i++) {
                    Unsafe.getUnsafe().putByte(second + i, (byte) (i + 4));
                }

                client.sendBinary(first, 3, second, 4);

                byte[] frame = socket.sent;
                Assert.assertNotNull(frame);
                Assert.assertEquals((byte) 0x82, frame[0]);
                Assert.assertEquals((byte) (0x80 | 7), frame[1]);
                byte[] payload = new byte[7];
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = (byte) (frame[6 + i] ^ frame[2 + (i & 3)]);
                }
                Assert.assertArrayEquals(
                        new byte[]{1, 2, 3, 4, 5, 6, 7},
                        payload);
            } finally {
                Unsafe.free(first, 3, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(second, 4, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * doSend() must try send() first and poll for writability only when the
     * socket reports full (send returns 0 / ERETRY); a pre-send ioWait costs
     * an epoll_ctl + epoll_wait per frame even when the socket is writable.
     */
    @Test
    public void testSendPollsForWritabilityOnlyOnBackpressure() throws Exception {
        assertMemoryLeak(() -> {
            // whole frame accepted by the first send(): no poll
            assertSendBinaryWriteWaits(new CapturingSocket(), 0);
            // 2 bytes accepted, socket full once, then the rest: exactly one poll
            assertSendBinaryWriteWaits(new CapturingSocket(2, 0), 1);
        });
    }

    @Test
    public void testSendCloseFrameDoesNotClobberSendBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (StubWebSocketClient client = new StubWebSocketClient()) {
                WebSocketSendBuffer sendBuffer = client.getSendBuffer();

                // User starts building a data frame
                sendBuffer.beginFrame();
                sendBuffer.putLong(0xDEADBEEFL);
                int posBeforeClose = sendBuffer.getWritePos();
                Assert.assertTrue("sendBuffer should have data", posBeforeClose > 0);

                // sendCloseFrame() should use controlFrameBuffer, not sendBuffer
                try {
                    client.sendCloseFrame(1000, null, 1000);
                } catch (HttpClientException ignored) {
                    // Expected: doSend() fails because there's no real socket
                }

                // Verify sendBuffer was NOT clobbered
                Assert.assertEquals(
                        "sendCloseFrame() must not reset the main sendBuffer",
                        posBeforeClose,
                        sendBuffer.getWritePos()
                );
            }
        });
    }

    @Test
    public void testSendPingDoesNotClobberSendBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (StubWebSocketClient client = new StubWebSocketClient()) {
                // Set upgraded=true so checkConnected() passes
                setUpgradedTrue(client);

                WebSocketSendBuffer sendBuffer = client.getSendBuffer();

                // User starts building a data frame
                sendBuffer.beginFrame();
                sendBuffer.putLong(0xCAFEBABEL);
                int posBeforePing = sendBuffer.getWritePos();
                Assert.assertTrue("sendBuffer should have data", posBeforePing > 0);

                // sendPing() should use controlFrameBuffer, not sendBuffer
                try {
                    client.sendPing();
                } catch (HttpClientException ignored) {
                    // Expected: doSend() fails because there's no real socket
                }

                // Verify sendBuffer was NOT clobbered
                Assert.assertEquals(
                        "sendPing() must not reset the main sendBuffer",
                        posBeforePing,
                        sendBuffer.getWritePos()
                );
            }
        });
    }

    /**
     * One recv delivering a burst of small frames (the shape of a server's
     * cumulative ACKs) must be parsed in place: compacting the unread tail
     * after every frame memmoved the rest of the burst once per frame. The
     * burst fills the 64 KiB buffer and ends mid-frame, so the next recv must
     * compact that tail BEFORE the grow check -- a tail left at the buffer end
     * would read as "full" and grow the buffer for no reason.
     */
    private static void assertFrameBurstIsParsedInPlace(boolean blocking) throws Exception {
        assertMemoryLeak(() -> {
            final int frameLen = 13; // FIN|BINARY, len=11
            final int frameCount = 5100; // > 64 KiB, so the first recv ends mid-frame
            byte[] data = new byte[frameLen * frameCount];
            for (int i = 0; i < frameCount; i++) {
                data[i * frameLen] = (byte) 0x82;
                data[i * frameLen + 1] = 11;
                data[i * frameLen + 2] = (byte) i;
                data[i * frameLen + 3] = (byte) (i >> 8);
            }
            try (FrameFeedWebSocketClient client = new FrameFeedWebSocketClient(data)) {
                setUpgradedTrue(client);
                final int bufSize = getIntField(client, "recvBufSize");
                final long[] payload = {0, 0}; // first, last
                final int[] frames = {0};
                WebSocketFrameHandler handler = new WebSocketFrameHandler() {
                    @Override
                    public void onBinaryMessage(long payloadPtr, int payloadLen) {
                        Assert.assertEquals(11, payloadLen);
                        int tag = (Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF)
                                | (Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF) << 8;
                        Assert.assertEquals(frames[0], tag);
                        if (frames[0]++ == 0) {
                            payload[0] = payloadPtr;
                        }
                        payload[1] = payloadPtr;
                    }

                    @Override
                    public void onClose(int code, String reason) {
                        Assert.fail("unexpected close frame");
                    }
                };

                int firstRecvFrames = bufSize / frameLen;
                for (int i = 0; i < firstRecvFrames; i++) {
                    Assert.assertTrue(blocking
                            ? client.receiveFrame(handler, 1000)
                            : client.tryReceiveFrame(handler));
                    Assert.assertEquals("frame " + i + " must be parsed where recv put it",
                            (long) i * frameLen, payload[1] - payload[0]);
                }
                for (int i = firstRecvFrames; i < frameCount; i++) {
                    Assert.assertTrue(blocking
                            ? client.receiveFrame(handler, 1000)
                            : client.tryReceiveFrame(handler));
                }
                Assert.assertEquals(frameCount, frames[0]);
                Assert.assertEquals("partial tail must not grow the recv buffer",
                        bufSize, getIntField(client, "recvBufSize"));
            }
        });
    }

    private static void assertSendBinaryWriteWaits(CapturingSocket socket, int expectedWriteWaits) throws Exception {
        long payload = Unsafe.malloc(7, MemoryTag.NATIVE_DEFAULT);
        try (CapturingWebSocketClient client = new CapturingWebSocketClient(socket)) {
            setUpgradedTrue(client);
            for (int i = 0; i < 7; i++) {
                Unsafe.getUnsafe().putByte(payload + i, (byte) (i + 1));
            }

            client.sendBinary(payload, 7);

            byte[] frame = socket.sent;
            Assert.assertEquals(2 + 4 + 7, frame.length);
            Assert.assertEquals((byte) 0x82, frame[0]);
            Assert.assertEquals((byte) (0x80 | 7), frame[1]);
            byte[] unmasked = new byte[7];
            for (int i = 0; i < unmasked.length; i++) {
                unmasked[i] = (byte) (frame[6 + i] ^ frame[2 + (i & 3)]);
            }
            Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7}, unmasked);
            Assert.assertEquals(expectedWriteWaits, client.writeWaits);
        } finally {
            Unsafe.free(payload, 7, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static int getIntField(Object obj, String name) throws Exception {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(name);
                field.setAccessible(true);
                return field.getInt(obj);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static int invokeExtractMaxBatchSize(String response) throws Exception {
        Method m = WebSocketClient.class.getDeclaredMethod("extractMaxBatchSize", String.class);
        m.setAccessible(true);
        return (int) m.invoke(null, response);
    }

    private static long invokeExtractMaxUncommittedRows(String response) throws Exception {
        Method m = WebSocketClient.class.getDeclaredMethod("extractMaxUncommittedRows", String.class);
        m.setAccessible(true);
        return (long) m.invoke(null, response);
    }

    private static void setUpgradedTrue(Object obj) throws Exception {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField("upgraded");
                field.setAccessible(true);
                field.set(obj, (Object) true);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("upgraded");
    }

    /**
     * Minimal Socket that always returns 0 from recv() (no data available),
     * triggering the ioWait path in recvOrTimeout().
     */
    private static class FakeSocket implements Socket {

        @Override
        public void close() {
        }

        @Override
        public int getFd() {
            return 0;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void of(int fd) {
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            return 0;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            return 0;
        }

        @Override
        public void startTlsSession(CharSequence peerName, SocketReadinessWaiter waiter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsTls() {
            return false;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return 0;
        }

        @Override
        public boolean wantsTlsWrite() {
            return false;
        }
    }

    /**
     * Accumulates every byte send() accepts. The i-th send() call accepts at
     * most acceptLimits[i] bytes (0 = socket full); calls past the script
     * accept everything.
     */
    private static class CapturingSocket extends FakeSocket {
        private final int[] acceptLimits;
        private byte[] sent = new byte[0];
        private int sendCalls;

        CapturingSocket(int... acceptLimits) {
            this.acceptLimits = acceptLimits;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            int n = sendCalls < acceptLimits.length ? Math.min(acceptLimits[sendCalls], bufferLen) : bufferLen;
            sendCalls++;
            int offset = sent.length;
            sent = Arrays.copyOf(sent, offset + n);
            for (int i = 0; i < n; i++) {
                sent[offset + i] = Unsafe.getUnsafe().getByte(bufferPtr + i);
            }
            return n;
        }
    }

    private static class CapturingWebSocketClient extends TestWebSocketClient {
        private int writeWaits;

        CapturingWebSocketClient(CapturingSocket socket) {
            super(DefaultHttpClientConfiguration.INSTANCE, (nf, log) -> socket);
        }

        @Override
        protected void ioWait(int timeout, int op) {
            if (op == IOOperation.WRITE) {
                writeWaits++;
            }
        }

        @Override
        protected void setupIoWait() {
        }
    }

    /**
     * Socket that serves a fixed byte sequence from recv() and reports
     * every send() as fully written (so close-frame sends succeed).
     */
    private static class FrameFeedSocket implements Socket {
        private final byte[] data;
        private int readPos;

        FrameFeedSocket(byte[] data) {
            this.data = data;
        }

        @Override
        public void close() {
        }

        @Override
        public int getFd() {
            return 0;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void of(int fd) {
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            int n = Math.min(data.length - readPos, bufferLen);
            for (int i = 0; i < n; i++) {
                Unsafe.getUnsafe().putByte(bufferPtr + i, data[readPos + i]);
            }
            readPos += n;
            return n;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            return bufferLen;
        }

        @Override
        public void startTlsSession(CharSequence peerName, SocketReadinessWaiter waiter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsTls() {
            return false;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return 0;
        }

        @Override
        public boolean wantsTlsWrite() {
            return false;
        }
    }

    /**
     * WebSocketClient over a socket pre-loaded with canned server frames;
     * sends always succeed. Drives both receive APIs through real parsing
     * and handler dispatch on the test thread.
     */
    private static class FrameFeedWebSocketClient extends TestWebSocketClient {

        FrameFeedWebSocketClient(byte[] frames) {
            super(DefaultHttpClientConfiguration.INSTANCE, (nf, log) -> new FrameFeedSocket(frames));
        }

        @Override
        protected void ioWait(int timeout, int op) {
            // no-op: recv delivers data on the first call
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }

    /**
     * WebSocketClient subclass with a fake socket that always returns 0
     * from recv(), forcing the ioWait path in recvOrTimeout().
     */
    private static class RecvTestWebSocketClient extends TestWebSocketClient {
        Runnable ioWaitAction;

        RecvTestWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, (nf, log) -> new FakeSocket());
        }

        @Override
        protected void ioWait(int timeout, int op) {
            ioWaitAction.run();
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }

    /**
     * Minimal concrete WebSocketClient that throws on any I/O,
     * allowing us to test buffer management without a real socket.
     */
    private static class StubWebSocketClient extends TestWebSocketClient {

        StubWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new HttpClientException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }
}
