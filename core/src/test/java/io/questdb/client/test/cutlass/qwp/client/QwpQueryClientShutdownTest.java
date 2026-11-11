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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class QwpQueryClientShutdownTest {
    private static final int BATCH_COUNT = 3;

    @Test(timeout = 20_000)
    public void testCompleteResultStillSucceeds() throws Exception {
        assertShutdownResult(0, false);
    }

    @Test(timeout = 20_000)
    public void testShutdownAfterLastBatchStillSucceeds() throws Exception {
        assertShutdownResult(BATCH_COUNT, false);
    }

    @Test(timeout = 20_000)
    public void testShutdownWithSkippedBatchesFails() throws Exception {
        assertShutdownResult(1, false);
    }

    @Test(timeout = 20_000)
    public void testShutdownWithSkippedBatchesReturnsCreditAndFails() throws Exception {
        assertShutdownResult(1, true);
    }

    private static void assertShutdownResult(int shutdownAfterBatch, boolean isCreditEnabled) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch credits = new CountDownLatch(BATCH_COUNT);
            try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
                @Override
                public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                    if (data.length == 0) {
                        return;
                    }
                    if (data[0] == QwpEgressMsgKind.CREDIT) {
                        credits.countDown();
                    } else if (data[0] == QwpEgressMsgKind.QUERY_REQUEST) {
                        try {
                            for (int seq = 0; seq < BATCH_COUNT; seq++) {
                                client.sendBinary(resultBatch(data, seq));
                            }
                            client.sendBinary(resultEnd(data));
                        } catch (IOException ignored) {
                            // The client reports a failed reply through its error callback.
                        }
                    }
                }
            })) {
                server.setSendServerInfo(true);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String config = "ws::addr=localhost:" + server.getPort() + ";"
                        + (isCreditEnabled ? "initial_credit=1_048_576;" : "");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(config)) {
                    client.connect();
                    ResultHandler handler = new ResultHandler(client, shutdownAfterBatch);
                    try {
                        client.execute("SELECT v FROM t", handler);
                        if (shutdownAfterBatch > 0 && shutdownAfterBatch < BATCH_COUNT) {
                            Assert.assertEquals(1, handler.batchCount);
                            Assert.assertEquals("a skipped batch must rule out success", 0, handler.endCount);
                            Assert.assertEquals(1, handler.errorCount);
                            Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                            Assert.assertEquals("query client shut down", handler.errorMessage);
                        } else {
                            Assert.assertEquals(BATCH_COUNT, handler.batchCount);
                            Assert.assertEquals(1, handler.endCount);
                            Assert.assertEquals(BATCH_COUNT, handler.totalRows);
                            Assert.assertEquals(0, handler.errorCount);
                        }
                        if (isCreditEnabled) {
                            Assert.assertTrue("release every batch, including skipped batches",
                                    credits.await(5, TimeUnit.SECONDS));
                        }
                    } finally {
                        client.requestShutdown();
                    }
                }
            }
        });
    }

    private static byte[] frame(byte[] body) {
        return ByteBuffer.allocate(QwpConstants.HEADER_SIZE + body.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put(QwpConstants.VERSION).put((byte) 0).putShort((short) 1)
                .putInt(body.length).put(body).array();
    }

    private static void putVarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static byte[] resultBatch(byte[] queryRequest, int seq) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(QwpEgressMsgKind.RESULT_BATCH);
        body.write(queryRequest, 1, 8);
        putVarint(body, seq);
        putVarint(body, 0); // table name length
        putVarint(body, 1); // row count
        if (seq == 0) {
            putVarint(body, 1); // column count
            putVarint(body, 1); // column name length
            body.write('v');
            body.write(QwpConstants.TYPE_LONG);
        }
        body.write(0); // no nulls
        long value = 100L + seq;
        for (int i = 0; i < 8; i++) {
            body.write((int) ((value >>> (8 * i)) & 0xFF));
        }
        return frame(body.toByteArray());
    }

    private static byte[] resultEnd(byte[] queryRequest) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(QwpEgressMsgKind.RESULT_END);
        body.write(queryRequest, 1, 8);
        putVarint(body, BATCH_COUNT - 1);
        putVarint(body, BATCH_COUNT);
        return frame(body.toByteArray());
    }

    private static final class ResultHandler implements QwpColumnBatchHandler {
        private final QwpQueryClient client;
        private final int shutdownAfterBatch;
        private final Field shutdownRequested;
        private int batchCount;
        private int endCount;
        private int errorCount;
        private String errorMessage;
        private byte errorStatus;
        private long requestId;
        private long totalRows;

        private ResultHandler(QwpQueryClient client, int shutdownAfterBatch) throws NoSuchFieldException {
            this.client = client;
            this.shutdownAfterBatch = shutdownAfterBatch;
            shutdownRequested = QwpQueryClient.class.getDeclaredField("isShutdownRequested");
            shutdownRequested.setAccessible(true);
        }

        @Override
        public void onBatch(QwpColumnBatch batch) {
            Assert.assertEquals(batchCount, batch.batchSeq());
            Assert.assertEquals(1, batch.getRowCount());
            Assert.assertEquals(100L + batchCount, batch.getLongValue(0, 0));
            requestId = batch.requestId();
            if (++batchCount == shutdownAfterBatch) {
                // Model a requester paused after requestShutdown() publishes the client flag but before
                // it stops the I/O thread. Keep I/O live so it can deliver the remaining batches and END.
                try {
                    shutdownRequested.setBoolean(client, true);
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }

        @Override
        public void onEnd(long totalRows) {
            endCount++;
            this.totalRows = totalRows;
        }

        @Override
        public void onEnd(long requestId, long totalRows) {
            Assert.assertEquals(this.requestId, requestId);
            onEnd(totalRows);
        }

        @Override
        public void onError(byte status, String message) {
            errorCount++;
            errorMessage = message;
            errorStatus = status;
        }

        @Override
        public void onError(long requestId, byte status, String message) {
            Assert.assertEquals(this.requestId, requestId);
            onError(status, message);
        }
    }
}
