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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import io.questdb.client.test.tools.TestWebSocketClient;
import org.junit.Assert;
import org.junit.Test;

public class CursorWebSocketSendLoopReplayMetricsTest {
    private static final int FRAME_COUNT = 4;
    private static final int PAYLOAD_LEN = 8;
    private static final int WIRE_LEN = PAYLOAD_LEN + WebSocketFrameWriter.headerSize(PAYLOAD_LEN, true);

    @Test
    public void testAppendFailureDoesNotCountBufferedFramesAsReplays() throws Exception {
        assertFailedBatchDoesNotCountAsSent(0, 3);
    }

    @Test
    public void testPartialFlushDoesNotCountFailedBatchAsSent() throws Exception {
        // Even one complete frame accepted by the socket does not complete the batch.
        assertFailedBatchDoesNotCountAsSent(WIRE_LEN, Integer.MAX_VALUE);
    }

    @Test
    public void testReplayTargetSurvivesAnotherFailedFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedSocket originalSocket = new ScriptedSocket(Integer.MAX_VALUE);
            ScriptedSocket retrySocket = new ScriptedSocket(2 * WIRE_LEN);
            ScriptedSocket replacementSocket = new ScriptedSocket(Integer.MAX_VALUE);
            try (CursorSendEngine engine = new CursorSendEngine(null, 16_384);
                 BatchingClient original = new BatchingClient(originalSocket);
                 BatchingClient retry = new BatchingClient(retrySocket);
                 BatchingClient replacement = new BatchingClient(replacementSocket)) {
                appendFrames(engine);
                retry.batchCap = 2 * WIRE_LEN;
                BatchingClient[] reconnectClients = {retry, replacement};
                int[] reconnectIndex = {0};
                try (CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        original, engine, 0L, () -> reconnectClients[reconnectIndex[0]++], 0L, 0L)) {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(FRAME_COUNT, loop.getTotalFramesSent());
                    Assert.assertEquals(0, loop.getTotalFramesReplayed());

                    loop.connectLoopForTest(new HttpClientException("test disconnect"), "reconnect", 0L);
                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(2, loop.getTotalFramesReplayed());
                    // Appending the remaining two frames clears replayTargetFsn before
                    // this flush fails. The next connection must still replay all four.
                    Assert.assertFalse(loop.trySendBatchForTest());
                    Assert.assertEquals(2 * WIRE_LEN, retrySocket.acceptedBytes);
                    Assert.assertEquals(FRAME_COUNT + 2, loop.getTotalFramesSent());
                    Assert.assertEquals(2, loop.getTotalFramesReplayed());

                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(FRAME_COUNT * WIRE_LEN, replacementSocket.acceptedBytes);
                    Assert.assertEquals(2 * FRAME_COUNT + 2, loop.getTotalFramesSent());
                    Assert.assertEquals(FRAME_COUNT + 2, loop.getTotalFramesReplayed());
                    Assert.assertEquals(2, loop.getTotalReconnects());
                    loop.checkError();
                }
            }
        });
    }

    @Test
    public void testZeroByteFlushFailureDoesNotCountFirstDeliveryAsReplay() throws Exception {
        assertFailedBatchDoesNotCountAsSent(0, Integer.MAX_VALUE);
    }

    private static void appendFrames(CursorSendEngine engine) {
        long payload = Unsafe.malloc(PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < FRAME_COUNT; i++) {
                Unsafe.getUnsafe().putLong(payload, i);
                Assert.assertEquals(i, engine.appendBlocking(payload, PAYLOAD_LEN));
            }
        } finally {
            Unsafe.free(payload, PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertFailedBatchDoesNotCountAsSent(int acceptedBytes, int failAppendAt) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedSocket firstSocket = new ScriptedSocket(acceptedBytes);
            ScriptedSocket replacementSocket = new ScriptedSocket(Integer.MAX_VALUE);
            try (CursorSendEngine engine = new CursorSendEngine(null, 16_384);
                 BatchingClient first = new BatchingClient(firstSocket);
                 BatchingClient replacement = new BatchingClient(replacementSocket)) {
                appendFrames(engine);
                first.failAppendAt = failAppendAt;
                try (CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        first, engine, 0L, () -> replacement, 0L, 0L)) {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    Assert.assertFalse(loop.trySendBatchForTest());
                    Assert.assertEquals(acceptedBytes, firstSocket.acceptedBytes);
                    Assert.assertEquals(Math.min(FRAME_COUNT, failAppendAt), first.appendCalls);
                    if (failAppendAt <= FRAME_COUNT) {
                        Assert.assertEquals("append failure must happen before any socket write", 0, firstSocket.sendCalls);
                    } else {
                        Assert.assertTrue("flush must reach the socket", firstSocket.sendCalls > 0);
                    }
                    Assert.assertEquals(1, loop.getTotalReconnects());
                    Assert.assertEquals(0, loop.getTotalFramesSent());
                    Assert.assertEquals(0, loop.getTotalFramesReplayed());

                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(FRAME_COUNT * WIRE_LEN, replacementSocket.acceptedBytes);
                    Assert.assertEquals(FRAME_COUNT, loop.getTotalFramesSent());
                    Assert.assertEquals("failed batches must not establish a replay boundary",
                            0, loop.getTotalFramesReplayed());
                    Assert.assertFalse("all queued frames must have been sent", loop.trySendBatchForTest());
                    loop.checkError();
                }
            }
        });
    }

    private static class BatchingClient extends TestWebSocketClient {
        private int appendCalls;
        private int batchCap = super.batchByteCap();
        private int failAppendAt = Integer.MAX_VALUE;

        private BatchingClient(ScriptedSocket socket) {
            super(DefaultHttpClientConfiguration.INSTANCE, (nf, log) -> socket);
        }

        @Override
        public void batchAppendBinary(long dataPtr, int length) {
            if (++appendCalls == failAppendAt) {
                throw new HttpClientException("test append failure");
            }
            // Skip the handshake but use the real send buffer and batchFlush/socket path.
            getSendBuffer().appendMaskedBinaryFrame(dataPtr, length);
        }

        @Override
        public int batchByteCap() {
            return batchCap;
        }

        @Override
        protected void ioWait(int timeout, int op) {
            Assert.fail("scripted socket never blocks");
        }

        @Override
        protected void setupIoWait() {
        }
    }

    private static class ScriptedSocket implements Socket {
        private final int bytesBeforeFailure;
        private int acceptedBytes;
        private int sendCalls;

        private ScriptedSocket(int bytesBeforeFailure) {
            this.bytesBeforeFailure = bytesBeforeFailure;
        }

        @Override
        public void close() {
        }

        @Override
        public int getFd() {
            return -1;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void of(int fd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            return 0;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            sendCalls++;
            if (acceptedBytes == bytesBeforeFailure) {
                return -1;
            }
            int count = Math.min(bufferLen, bytesBeforeFailure - acceptedBytes);
            acceptedBytes += count;
            return count;
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
}
