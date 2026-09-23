/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CursorWebSocketSchemaCoordinatorTest {
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().parentFolder(taskTempRoot()).build();

    @Test
    public void testColdHitCaseAliasAndForcedRefreshUseProductionTransport() throws Exception {
        DescribeHandler handler = new DescribeHandler(true);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("cold").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                try {
                    loop.resolveSchema(new String(new char[QwpSchemaProtocol.MAX_NAME_UTF16_LENGTH + 1]), 5_000);
                    Assert.fail("expected invalid table name");
                } catch (IllegalArgumentException expected) {
                    Assert.assertEquals(0, handler.requests.get());
                }
                QwpSchemaResponse first = loop.resolveSchema("Trades", 5_000);
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, first.getResult());
                Assert.assertEquals(1, handler.requests.get());
                Assert.assertEquals(1, loop.resolveSchema("trades", 5_000).getTableId());
                Assert.assertEquals(1, handler.requests.get());
                Assert.assertEquals(2, loop.refreshSchema("TRADES", 5_000).getTableId());
                Assert.assertEquals(2, handler.requests.get());
            } finally {
                loop.close();
            }
        }
    }

    @Test
    public void testMissingIsCachedButRemoteFailuresAreTyped() throws Exception {
        DescribeHandler missing = new DescribeHandler(true, QwpSchemaProtocol.RESULT_MISSING);
        try (TestWebSocketServer server = server(missing);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("missing").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING,
                        loop.resolveSchema("Absent", 5_000).getResult());
                Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING,
                        loop.resolveSchema("ABSENT", 0).getResult());
                Assert.assertEquals(1, missing.requests.get());
            } finally {
                loop.close();
            }
        }
        assertRemoteReason(QwpSchemaProtocol.RESULT_DENIED, LineSenderSchemaException.Reason.ACCESS_DENIED, "denied");
        assertRemoteReason(QwpSchemaProtocol.RESULT_UNAVAILABLE, LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, "unavailable");
    }

    @Test
    public void testTooLargeIsCachedButUnavailableIsNot() throws Exception {
        // A table too wide to describe stays too wide until its version changes,
        // which ACK feedback reports; re-asking every batch would cost a round trip.
        DescribeHandler tooLarge = new DescribeHandler(true, QwpSchemaProtocol.RESULT_TOO_LARGE);
        try (TestWebSocketServer server = server(tooLarge);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("too-large").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, loop.resolveSchema("Wide", 5_000).getResult());
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, loop.resolveSchema("WIDE", 0).getResult());
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, loop.peekSchema("wide").getResult());
                Assert.assertEquals(1, tooLarge.requests.get());
            } finally {
                loop.close();
            }
        }
        DescribeHandler unavailable = new DescribeHandler(true, QwpSchemaProtocol.RESULT_UNAVAILABLE);
        try (TestWebSocketServer server = server(unavailable);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("unavailable").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                assertReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, () -> loop.resolveSchema("t", 5_000));
                assertReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, () -> loop.resolveSchema("t", 5_000));
                Assert.assertEquals(2, unavailable.requests.get());
                Assert.assertNull(loop.peekSchema("t"));
            } finally {
                loop.close();
            }
        }
    }

    @Test
    public void testTooLargeFeedbackIsCached() throws Exception {
        DescribeHandler handler = new DescribeHandler(true);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("too-large-feedback").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                loop.resolveSchema("seed", 5_000);
                handler.sendFeedback("Wide", QwpSchemaProtocol.RESULT_TOO_LARGE);
                // The marker reply follows the feedback on the wire, so the I/O thread
                // has applied the feedback once the marker lookup returns.
                loop.refreshSchema("marker", 5_000);
                int afterMarker = handler.requests.get();
                Assert.assertEquals(QwpSchemaProtocol.RESULT_TOO_LARGE, loop.resolveSchema("wide", 0).getResult());
                Assert.assertEquals(afterMarker, handler.requests.get());
            } finally {
                loop.close();
            }
        }
    }

    @Test
    public void testOrderedFeedbackInvalidationAndDurableDemultiplexing() throws Exception {
        DescribeHandler handler = new DescribeHandler(true);
        try (TestWebSocketServer server = server(handler, true);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("feedback").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort(), true, true)) {
            Assert.assertTrue(client.isServerDurableAckEnabled());
            CursorWebSocketSendLoop loop = loop(client, engine, true);
            try {
                loop.start();
                loop.resolveSchema("seed", 5_000);

                handler.sendFeedback("Feedback", QwpSchemaProtocol.RESULT_MISSING);
                handler.durableBeforeReply = true;
                loop.refreshSchema("marker", 5_000);
                int afterMarker = handler.requests.get();
                Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING,
                        loop.resolveSchema("feedback", 0).getResult());
                Assert.assertEquals(afterMarker, handler.requests.get());

                handler.sendInvalidation();
                loop.refreshSchema("marker", 5_000);
                loop.resolveSchema("feedback", 5_000);
                Assert.assertEquals(afterMarker + 2, handler.requests.get());
                Assert.assertEquals(-1, engine.ackedFsn());
                Assert.assertEquals(-1, engine.publishedFsn());
            } finally {
                loop.close();
            }
        }
    }

    @Test
    public void testFailedRefreshEvictsPriorCachedSchema() throws Exception {
        DescribeHandler handler = new DescribeHandler(true);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("refresh-eviction").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                loop.start();
                Assert.assertEquals(1, loop.resolveSchema("cached", 5_000).getTableId());
                handler.reply = false;
                Future<QwpSchemaResponse> refresh = executor.submit(() -> loop.refreshSchema("CACHED", 200));
                Assert.assertTrue(handler.awaitRequests(2));
                assertFutureReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, refresh);
                assertReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE,
                        () -> loop.resolveSchema("cached", 0));
                handler.reply = true;
                Assert.assertEquals(3, loop.resolveSchema("cached", 5_000).getTableId());
                Assert.assertEquals(3, handler.requests.get());
            } finally {
                executor.shutdownNow();
                loop.close();
            }
        }
    }

    @Test
    public void testTimeoutIgnoresLateReplyAndNextRequestSucceeds() throws Exception {
        DescribeHandler handler = new DescribeHandler(false);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("late").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                loop.start();
                Future<QwpSchemaResponse> timedOut = executor.submit(() -> loop.resolveSchema("late", 200));
                Assert.assertTrue(handler.awaitFirstRequest());
                assertFutureReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, timedOut);
                handler.replyToFirst();
                handler.reply = true;
                Assert.assertEquals(2, loop.resolveSchema("late", 5_000).getTableId());
                Assert.assertEquals(2, handler.requests.get());
            } finally {
                executor.shutdownNow();
                loop.close();
            }
        }
    }

    @Test
    public void testAbandonOnDisconnectFailsPendingLookupBeforeItsDeadline() throws Exception {
        DescribeHandler handler = new DescribeHandler(false);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("abandon").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            loop.setAbandonSchemaLookupOnDisconnect(true);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                loop.start();
                Assert.assertTrue(loop.isWireUp());
                // A budget far beyond the assertion window: only the drop can end it.
                Future<QwpSchemaResponse> pending = executor.submit(() -> loop.resolveSchema("t", 60_000));
                Assert.assertTrue(handler.awaitFirstRequest());
                server.close();
                try {
                    pending.get(5, TimeUnit.SECONDS);
                    Assert.fail("lookup survived the connection loss");
                } catch (ExecutionException e) {
                    LineSenderSchemaException cause = (LineSenderSchemaException) e.getCause();
                    Assert.assertEquals(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, cause.getReason());
                    Assert.assertTrue(cause.getMessage(), cause.getMessage().contains("connection lost"));
                }
                Assert.assertFalse(loop.isWireUp());
            } finally {
                executor.shutdownNow();
                loop.close();
            }
        }
    }

    @Test
    public void testPeekReturnsOnlyCachedSchema() throws Exception {
        DescribeHandler handler = new DescribeHandler(true);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("peek").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                Assert.assertNull(loop.peekSchema("Trades"));
                Assert.assertEquals(0, handler.requests.get());
                QwpSchemaResponse resolved = loop.resolveSchema("Trades", 5_000);
                Assert.assertSame(resolved, loop.peekSchema("trades"));
                Assert.assertEquals(1, handler.requests.get());
            } finally {
                loop.close();
            }
        }
    }

    @Test
    public void testUnnegotiatedConnectionIsUnavailable() throws Exception {
        io.questdb.client.test.tools.TestUtils.assertMemoryLeak(() -> {
            DescribeHandler handler = new DescribeHandler(true);
            try (TestWebSocketServer server = new TestWebSocketServer(handler);
                 CursorSendEngine engine = new CursorSendEngine(temp.newFolder("legacy").getAbsolutePath(), 1 << 20)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (WebSocketClient client = connect(server.getPort(), false)) {
                    CursorWebSocketSendLoop loop = loop(client, engine);
                    try {
                        loop.start();
                        assertReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE,
                                () -> loop.resolveSchema("t", 5_000));
                        Assert.assertEquals(0, handler.requests.get());
                    } finally {
                        loop.close();
                    }
                }
            }
        });
    }

    @Test
    public void testMalformedControlFailsLookupWithoutAckFallback() throws Exception {
        DescribeHandler handler = new DescribeHandler(true);
        handler.malformedReply = true;
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder("malformed-control").getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                assertReason(LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE,
                        () -> loop.resolveSchema("t", 5_000));
                Assert.assertEquals(1, handler.requests.get());
            } finally {
                loop.close();
            }
        }
    }

    private static CursorWebSocketSendLoop loop(WebSocketClient client, CursorSendEngine engine) {
        return loop(client, engine, false);
    }

    private static CursorWebSocketSendLoop loop(WebSocketClient client, CursorSendEngine engine, boolean durable) {
        return new CursorWebSocketSendLoop(client, engine, 0,
                CursorWebSocketSendLoop.DEFAULT_PARK_NANOS, null, 1, 4, durable);
    }

    private void assertRemoteReason(int result, LineSenderSchemaException.Reason reason, String directory) throws Exception {
        DescribeHandler handler = new DescribeHandler(true, result);
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = new CursorSendEngine(temp.newFolder(directory).getAbsolutePath(), 1 << 20);
             WebSocketClient client = connect(server.getPort())) {
            CursorWebSocketSendLoop loop = loop(client, engine);
            try {
                loop.start();
                assertReason(reason, () -> loop.resolveSchema("t", 5_000));
            } finally {
                loop.close();
            }
        }
    }

    private static TestWebSocketServer server(DescribeHandler handler) throws Exception {
        return server(handler, false);
    }

    private static TestWebSocketServer server(DescribeHandler handler, boolean durable) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler, durable);
        server.setAdvertiseSchema(true);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static WebSocketClient connect(int port) {
        return connect(port, true);
    }

    private static WebSocketClient connect(int port, boolean schema) {
        return connect(port, schema, false);
    }

    private static WebSocketClient connect(int port, boolean schema, boolean durable) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        client.connect("localhost", port);
        if (schema) {
            client.requestQwpSchema();
        }
        client.setQwpRequestDurableAck(durable);
        client.upgrade("/write/v4", 5_000, null);
        return client;
    }

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        try {
            action.run();
            Assert.fail("expected schema failure");
        } catch (LineSenderSchemaException e) {
            Assert.assertEquals(reason, e.getReason());
        }
    }

    private static void assertFutureReason(
            LineSenderSchemaException.Reason reason,
            Future<QwpSchemaResponse> future
    ) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            Assert.fail("expected schema failure");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof LineSenderSchemaException);
            Assert.assertEquals(reason, ((LineSenderSchemaException) e.getCause()).getReason());
        }
    }

    private static File taskTempRoot() {
        File root = new File("target/schema-coordination-2.7/tmp").getAbsoluteFile();
        Assert.assertTrue(root.exists() || root.mkdirs());
        return root;
    }

    private static final class DescribeHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicInteger requests = new AtomicInteger();
        private final CountDownLatch firstRequest = new CountDownLatch(1);
        private volatile boolean reply;
        private volatile boolean durableBeforeReply;
        private volatile boolean malformedReply;
        private TestWebSocketServer.ClientHandler client;
        private long firstRequestId;
        private final int result;

        private DescribeHandler(boolean reply) {
            this(reply, QwpSchemaProtocol.RESULT_KNOWN);
        }

        private DescribeHandler(boolean reply, int result) {
            this.reply = reply;
            this.result = result;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            Assert.assertEquals(QwpConstants.MAGIC_MESSAGE,
                    ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt());
            Assert.assertEquals(QwpSchemaProtocol.FLAG_CONTROL, data[5]);
            long requestId = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong(QwpConstants.HEADER_SIZE + 1);
            this.client = client;
            int requestCount = requests.incrementAndGet();
            notifyAll();
            if (requestCount == 1) {
                firstRequestId = requestId;
                firstRequest.countDown();
            }
            if (reply) {
                if (malformedReply) {
                    ByteBuffer malformed = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                    sendRaw(client, malformed.putInt(QwpConstants.MAGIC_MESSAGE).array());
                    return;
                }
                if (durableBeforeReply) {
                    durableBeforeReply = false;
                    sendRaw(client, new byte[]{WebSocketResponse.STATUS_DURABLE_ACK, 0, 0});
                }
                send(client, requestId, requests.get(), result);
            }
        }

        synchronized void replyToFirst() throws IOException {
            send(client, firstRequestId, 1, result);
        }

        boolean awaitFirstRequest() throws InterruptedException {
            return firstRequest.await(5, TimeUnit.SECONDS);
        }

        synchronized boolean awaitRequests(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (requests.get() < count) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return true;
        }

        synchronized void sendFeedback(String tableName, int result) {
            byte[] name = tableName.getBytes(StandardCharsets.UTF_8);
            byte[] schema = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                    .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(0).put((byte) result).array();
            ByteBuffer b = ByteBuffer.allocate(11 + 2 + 2 + name.length + 4 + schema.length)
                    .order(ByteOrder.LITTLE_ENDIAN);
            b.put((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES))
                    .putLong(0).putShort((short) 0).putShort((short) 1)
                    .putShort((short) name.length).put(name).putInt(schema.length).put(schema);
            sendRaw(client, b.array());
        }

        synchronized void sendInvalidation() {
            ByteBuffer b = ByteBuffer.allocate(11).order(ByteOrder.LITTLE_ENDIAN);
            b.put((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_INVALIDATE_ALL))
                    .putLong(0).putShort((short) 0);
            sendRaw(client, b.array());
        }

        private static void send(TestWebSocketServer.ClientHandler client, long requestId, int tableId, int result) {
            byte[] column = "x".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + 8 + 1;
            if (result == QwpSchemaProtocol.RESULT_KNOWN) {
                payloadLength += 4 + 8 + 2 + 2 + 2 + column.length + 4 + 2;
            }
            ByteBuffer b = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLength).order(ByteOrder.LITTLE_ENDIAN);
            b.putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                    .putShort((short) 0).putInt(payloadLength).put(QwpSchemaProtocol.KIND_SCHEMA)
                    .putLong(requestId).put((byte) result);
            if (result == QwpSchemaProtocol.RESULT_KNOWN) {
                b.putInt(tableId).putLong(1).putShort((short) -1).putShort((short) 1)
                        .putShort((short) column.length).put(column).putInt(ColumnType.LONG).putShort((short) 0);
            }
            sendRaw(client, b.array());
        }

        private static void sendRaw(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(data);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }
}
