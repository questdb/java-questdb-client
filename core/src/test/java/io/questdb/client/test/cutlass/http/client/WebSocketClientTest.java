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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.http.client.WebSocketSendBuffer;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class WebSocketClientTest {

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
                    client.sendPing(1000);
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
     * sends always succeed. Used to drive a real tryReceiveFrame ->
     * tryParseFrame -> handler dispatch on the test thread.
     */
    private static class FrameFeedWebSocketClient extends WebSocketClient {

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
    private static class RecvTestWebSocketClient extends WebSocketClient {
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
    private static class StubWebSocketClient extends WebSocketClient {

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
