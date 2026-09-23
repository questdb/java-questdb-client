/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CursorWebSocketSchemaLifecycleTest {
    @Rule
    public final TestRule memoryLeak = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            TestUtils.assertMemoryLeak(() -> {
                try {
                    base.evaluate();
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            if (failure.get() != null) {
                throw failure.get();
            }
        }
    };
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().parentFolder(taskTempRoot()).build();

    @Test
    public void testCloseReleasesLongDeadlineWaiterBeforeLoopStarts() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("close-before-start");
             WebSocketClient client = connect(server.getPort());
             CursorWebSocketSendLoop loop = loop(client, engine, null)) {
            Lookup lookup = lookup(loop, "never-started", Long.MAX_VALUE, null);
            Assert.assertTrue(lookup.entered.await(5, TimeUnit.SECONDS));
            loop.close();
            lookup.join();
            assertUnavailable(lookup.error.get());
        }
    }

    @Test
    public void testCloseReleasesLookupAlreadySentByActiveLoop() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("close-active");
             WebSocketClient client = connect(server.getPort());
             CursorWebSocketSendLoop loop = loop(client, engine, null)) {
            loop.start();
            Lookup lookup = lookup(loop, "active", Long.MAX_VALUE, null);
            Assert.assertTrue(handler.requestSeen.await(5, TimeUnit.SECONDS));
            loop.close();
            lookup.join();
            assertUnavailable(lookup.error.get());
        }
    }

    @Test
    public void testInterruptionRestoresFlagAndReleasesSlotForNextLookup() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("interrupt");
             WebSocketClient client = connect(server.getPort());
             CursorWebSocketSendLoop loop = loop(client, engine, null)) {
            loop.start();
            Lookup interrupted = lookup(loop, "first", Long.MAX_VALUE, null);
            Assert.assertTrue(handler.requestSeen.await(5, TimeUnit.SECONDS));
            interrupted.thread.interrupt();
            interrupted.join();
            assertUnavailable(interrupted.error.get());
            Assert.assertTrue(interrupted.interrupted.get());

            handler.reply = true;
            QwpSchemaResponse next = loop.resolveSchema("second", 5_000);
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, next.getResult());
            Assert.assertEquals("second", handler.lastName.get());
        }
    }

    @Test
    public void testDisconnectRetriesCurrentLookupRetainsCacheAndRejectsStaleReply() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.reply = true;
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("reconnect");
             WebSocketClient client = connect(server.getPort())) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                try {
                    loop.start();
                    QwpSchemaResponse cached = loop.resolveSchema("cached", 5_000);
                    handler.disconnectName = "lost";
                    handler.disconnectsRemaining = 2;
                    Lookup lost = lookup(loop, "lost", 5_000, null);
                    Assert.assertTrue(reconnect.attempted.await(5, TimeUnit.SECONDS));
                    Assert.assertSame(cached, loop.resolveSchema("CACHED", 5_000));
                    Lookup busy = lookup(loop, "fresh", 5_000, null);
                    busy.join();
                    assertUnavailable(busy.error.get());
                    Assert.assertTrue(busy.error.get().getMessage().contains("another schema lookup"));
                    handler.sendStaleBeforeNextReply = true;
                    reconnect.allow.countDown();
                    lost.join();
                    Assert.assertNull(String.valueOf(lost.error.get()), lost.error.get());
                    Assert.assertNotNull(lost.response.get());
                    Assert.assertEquals(4, lost.response.get().getTableId());
                    Assert.assertSame(lost.response.get(), loop.resolveSchema("lost", 0));
                    Assert.assertEquals(3, server.handshakeCount());

                    int requestsBeforeCacheProbe = handler.requests;
                    QwpSchemaResponse replacement = loop.resolveSchema("cached", 5_000);
                    Assert.assertTrue(replacement.getRequestId() > 0);
                    Assert.assertNotSame(cached, replacement);
                    Assert.assertEquals(requestsBeforeCacheProbe + 1, handler.requests);
                } finally {
                    reconnect.allow.countDown();
                }
            }
        }
    }

    @Test
    public void testQueuedUnsentLookupSurvivesDisconnect() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.reply = true;
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("queued-disconnect");
             DisconnectingClient client = new DisconnectingClient()) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            reconnect.allow.countDown();
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                try {
                    loop.start();
                    Assert.assertTrue(client.receiving.await(5, TimeUnit.SECONDS));
                    Lookup queued = lookup(loop, "queued", 5_000, null);
                    queued.awaitWaiting();
                    client.disconnect.countDown();
                    queued.join();
                    Assert.assertNull(String.valueOf(queued.error.get()), queued.error.get());
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, queued.response.get().getResult());
                    Assert.assertEquals(1, handler.requests);
                } finally {
                    client.disconnect.countDown();
                }
            }
        }
    }

    @Test
    public void testQueuedLookupBecomesUnavailableAtLegacyReplacement() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("queued-legacy-replacement");
             DisconnectingClient client = new DisconnectingClient()) {
            server.setAdvertiseSchema(false);
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            reconnect.allow.countDown();
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                try {
                    loop.start();
                    Assert.assertTrue(client.receiving.await(5, TimeUnit.SECONDS));
                    Lookup queued = lookup(loop, "queued", 5_000, null);
                    queued.awaitWaiting();
                    client.disconnect.countDown();
                    queued.join();
                    assertUnavailable(queued.error.get());
                    Assert.assertTrue(loop.isWireUp());
                    Assert.assertFalse(loop.isSchemaEnabled());
                    Assert.assertFalse(engine.requiresSchema());
                    Assert.assertEquals(0, handler.requests);
                } finally {
                    client.disconnect.countDown();
                }
            }
        }
    }

    @Test
    public void testReconnectWaitCountsAgainstOriginalLookupDeadline() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.reply = true;
        handler.disconnectName = "deadline";
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("reconnect-deadline");
             WebSocketClient client = connect(server.getPort())) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                try {
                    loop.start();
                    Lookup lookup = lookup(loop, "deadline", 1_000, null);
                    Assert.assertTrue(reconnect.attempted.await(5, TimeUnit.SECONDS));
                    lookup.join();
                    assertUnavailable(lookup.error.get());
                    Assert.assertTrue(lookup.error.get().getMessage().contains("timed out"));
                    reconnect.allow.countDown();
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN,
                            loop.resolveSchema("probe", 5_000).getResult());
                    Assert.assertEquals(2, handler.requests);
                    Assert.assertEquals("probe", handler.lastName.get());
                } finally {
                    reconnect.allow.countDown();
                }
            }
        }
    }

    @Test
    public void testInterruptDuringReconnectReleasesSlotForQueuedLookup() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.reply = true;
        handler.disconnectName = "interrupted";
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("reconnect-interrupt");
             WebSocketClient client = connect(server.getPort())) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                try {
                    loop.start();
                    Lookup interrupted = lookup(loop, "interrupted", Long.MAX_VALUE, null);
                    Assert.assertTrue(reconnect.attempted.await(5, TimeUnit.SECONDS));
                    interrupted.thread.interrupt();
                    interrupted.join();
                    assertUnavailable(interrupted.error.get());
                    Assert.assertTrue(interrupted.interrupted.get());
                    Lookup queued = lookup(loop, "queued", 5_000, null);
                    queued.awaitWaiting();
                    reconnect.allow.countDown();
                    queued.join();
                    Assert.assertNull(String.valueOf(queued.error.get()), queued.error.get());
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, queued.response.get().getResult());
                    Assert.assertEquals(2, handler.requests);
                } finally {
                    reconnect.allow.countDown();
                }
            }
        }
    }

    @Test
    public void testCloseDuringReconnectReleasesLookupBeforeFactoryReturns() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.disconnectName = "closed";
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("reconnect-close");
             WebSocketClient client = connect(server.getPort())) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                AtomicReference<Throwable> closeError = new AtomicReference<>();
                Thread closer = new Thread(() -> {
                    try {
                        loop.close();
                    } catch (Throwable e) {
                        closeError.set(e);
                    }
                });
                try {
                    loop.start();
                    Lookup lookup = lookup(loop, "closed", Long.MAX_VALUE, null);
                    Assert.assertTrue(reconnect.attempted.await(5, TimeUnit.SECONDS));
                    closer.start();
                    lookup.join();
                    assertUnavailable(lookup.error.get());
                    Assert.assertTrue(lookup.error.get().getMessage().contains("closed"));
                } finally {
                    reconnect.allow.countDown();
                    closer.join(5_000);
                    Assert.assertFalse(closer.isAlive());
                }
                Assert.assertNull(String.valueOf(closeError.get()), closeError.get());
                Assert.assertEquals(1, handler.requests);
            }
        }
    }

    @Test
    public void testLookupsDuringOutageDoNotShortenReconnectBackoff() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        FailingReconnectFactory reconnect = new FailingReconnectFactory();
        try (CursorSendEngine engine = engine("outage-backoff")) {
            TestWebSocketServer server = server(handler);
            CursorWebSocketSendLoop loop = null;
            try {
                loop = new CursorWebSocketSendLoop(connect(server.getPort()), engine, 0,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS, reconnect, 200, 200);
                loop.start();
                server.close();
                Assert.assertTrue(reconnect.firstAttempt.await(5, TimeUnit.SECONDS));
                int attemptsBefore = reconnect.attempts.get();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
                int lookups = 0;
                while (System.nanoTime() < deadline) {
                    try {
                        loop.resolveSchema("outage", 1);
                        Assert.fail("the wire is down");
                    } catch (LineSenderSchemaException expected) {
                        lookups++;
                    }
                }
                int attempts = reconnect.attempts.get() - attemptsBefore;
                // A 200 ms backoff plus up to 200 ms jitter allows at most 5 attempts per second.
                Assert.assertTrue("lookups=" + lookups + ", attempts=" + attempts, attempts <= 6);
                Assert.assertTrue("lookups=" + lookups, lookups > 50);
            } finally {
                if (loop != null) {
                    loop.close();
                }
                server.close();
            }
        }
    }

    @Test
    public void testMalformedCurrentReplyCancelsLookupEvenWithReconnectAvailable() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.malformedReply = true;
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("reconnect-malformed");
             WebSocketClient client = connect(server.getPort())) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            try (CursorWebSocketSendLoop loop = loop(client, engine, reconnect)) {
                try {
                    loop.start();
                    Lookup lookup = lookup(loop, "malformed", Long.MAX_VALUE, null);
                    Assert.assertTrue(reconnect.attempted.await(5, TimeUnit.SECONDS));
                    lookup.join();
                    assertUnavailable(lookup.error.get());
                    handler.malformedReply = false;
                    handler.reply = true;
                    reconnect.allow.countDown();
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN,
                            loop.resolveSchema("probe", 5_000).getResult());
                    Assert.assertEquals(2, handler.requests);
                } finally {
                    reconnect.allow.countDown();
                }
            }
        }
    }

    @Test
    public void testAsyncInitialConnectWaitCountsAgainstOriginalLookupDeadline() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.reply = true;
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("async-initial")) {
            GatedReconnectFactory reconnect = new GatedReconnectFactory(server.getPort());
            try (CursorWebSocketSendLoop loop = loop(null, engine, reconnect)) {
                loop.start();
                Lookup lookup = lookup(loop, "deadline", 25, null);
                Assert.assertTrue(reconnect.attempted.await(5, TimeUnit.SECONDS));
                lookup.join();
                assertUnavailable(lookup.error.get());
                reconnect.allow.countDown();
                Assert.assertTrue(reconnect.connected.await(5, TimeUnit.SECONDS));
                QwpSchemaResponse probe = loop.resolveSchema("probe", 5_000);
                Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, probe.getResult());
                Assert.assertEquals(1, handler.requests);
                Assert.assertEquals("probe", handler.lastName.get());
            } finally {
                reconnect.allow.countDown();
            }
        }
    }

    @Test
    public void testMalformedTimedOutReplyDoesNotPoisonIdleConnection() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("stale-idle");
             WebSocketClient client = connect(server.getPort());
             CursorWebSocketSendLoop loop = loop(client, engine, null)) {
            loop.start();
            Lookup stale = lookup(loop, "stale", 1_000, null);
            Assert.assertTrue(handler.requestSeen.await(5, TimeUnit.SECONDS));
            stale.join();
            assertUnavailable(stale.error.get());
            handler.sendMalformedLastRequestAndFeedback("idle_barrier");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                try {
                    Assert.assertEquals(QwpSchemaProtocol.RESULT_MISSING,
                            loop.resolveSchema("idle_barrier", 0).getResult());
                    break;
                } catch (LineSenderSchemaException e) {
                    assertUnavailable(e);
                    if (System.nanoTime() >= deadline) {
                        Assert.fail("schema feedback barrier was not observed");
                    }
                    Thread.yield();
                }
            }
            Assert.assertEquals(1, handler.requests);
            handler.reply = true;
            QwpSchemaResponse response = loop.resolveSchema("usable", 5_000);
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, response.getResult());
            Assert.assertEquals("usable", handler.lastName.get());
            Assert.assertEquals(1, server.handshakeCount());
        }
    }

    @Test
    public void testMalformedOldReplyDoesNotFailNewLookupOrPopulateOldCache() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("stale-pending");
             WebSocketClient client = connect(server.getPort());
             CursorWebSocketSendLoop loop = loop(client, engine, null)) {
            loop.start();
            Lookup stale = lookup(loop, "stale", 1_000, null);
            Assert.assertTrue(handler.requestSeen.await(5, TimeUnit.SECONDS));
            stale.join();
            assertUnavailable(stale.error.get());
            handler.sendStaleBeforeNextReply = true;
            QwpSchemaResponse current = loop.resolveSchema("current", 5_000);
            Assert.assertEquals(QwpSchemaProtocol.RESULT_KNOWN, current.getResult());
            Assert.assertEquals(2, current.getTableId());
            Assert.assertEquals(1, server.handshakeCount());

            handler.reply = true;
            QwpSchemaResponse staleProbe = loop.resolveSchema("stale", 5_000);
            Assert.assertEquals(3, staleProbe.getTableId());
            Assert.assertEquals(3, handler.requests);
        }
    }

    @Test
    public void testMalformedMatchingReplyFailsTransportAndReleasesWaiter() throws Exception {
        SchemaHandler handler = new SchemaHandler();
        handler.malformedReply = true;
        try (TestWebSocketServer server = server(handler);
             CursorSendEngine engine = engine("malformed-current");
             WebSocketClient client = connect(server.getPort());
             CursorWebSocketSendLoop loop = loop(client, engine, null)) {
            loop.start();
            Lookup lookup = lookup(loop, "current", Long.MAX_VALUE, null);
            lookup.join();
            assertUnavailable(lookup.error.get());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                try {
                    loop.checkError();
                } catch (LineSenderException expected) {
                    Assert.assertTrue(expected.getMessage().startsWith("Invalid schema control response:"));
                    break;
                }
                if (System.nanoTime() >= deadline) {
                    Assert.fail("malformed schema response did not publish the transport error");
                }
                Thread.yield();
            }
        }
    }

    private CursorSendEngine engine(String name) throws IOException {
        return new CursorSendEngine(temp.newFolder(name).getAbsolutePath(), 1 << 20);
    }

    private static CursorWebSocketSendLoop loop(
            WebSocketClient client,
            CursorSendEngine engine,
            CursorWebSocketSendLoop.ReconnectFactory reconnect
    ) {
        return new CursorWebSocketSendLoop(
                client, engine, 0, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS, reconnect, 1, 4);
    }

    private static Lookup lookup(
            CursorWebSocketSendLoop loop,
            String table,
            long timeoutMillis,
            CountDownLatch completed
    ) {
        Lookup lookup = new Lookup();
        lookup.thread = new Thread(() -> {
            lookup.entered.countDown();
            try {
                lookup.response.set(loop.resolveSchema(table, timeoutMillis));
            } catch (Throwable e) {
                lookup.error.set(e);
            } finally {
                lookup.interrupted.set(Thread.currentThread().isInterrupted());
                if (completed != null) {
                    completed.countDown();
                }
            }
        }, "schema-lookup-" + table);
        lookup.thread.start();
        return lookup;
    }

    private static void assertUnavailable(Throwable error) {
        Assert.assertTrue(String.valueOf(error), error instanceof LineSenderSchemaException);
        Assert.assertEquals(
                LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE,
                ((LineSenderSchemaException) error).getReason());
    }

    private static TestWebSocketServer server(SchemaHandler handler) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.setAdvertiseSchema(true);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static WebSocketClient connect(int port) {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        boolean success = false;
        try {
            client.connect("localhost", port);
            client.requestQwpSchema();
            client.upgrade("/write/v4", 5_000, null);
            success = true;
            return client;
        } finally {
            if (!success) {
                client.close();
            }
        }
    }

    private static File taskTempRoot() {
        File root = new File("target/schema-coordination-2.7/tmp").getAbsoluteFile();
        Assert.assertTrue(root.exists() || root.mkdirs());
        return root;
    }

    private static final class Lookup {
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final AtomicBoolean interrupted = new AtomicBoolean();
        private final AtomicReference<QwpSchemaResponse> response = new AtomicReference<>();
        private Thread thread;

        private void awaitWaiting() throws InterruptedException {
            Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (thread.getState() != Thread.State.TIMED_WAITING) {
                Assert.assertTrue("lookup did not wait", thread.isAlive() && System.nanoTime() < deadline);
                Thread.yield();
            }
        }

        private void join() throws InterruptedException {
            thread.join(5_000);
            Assert.assertFalse("lookup thread did not finish", thread.isAlive());
        }
    }

    private static final class FailingReconnectFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger attempts = new AtomicInteger();
        private final CountDownLatch firstAttempt = new CountDownLatch(1);

        @Override
        public WebSocketClient reconnect() {
            attempts.incrementAndGet();
            firstAttempt.countDown();
            throw new RuntimeException("server is down");
        }
    }

    private static final class GatedReconnectFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final CountDownLatch allow = new CountDownLatch(1);
        private final CountDownLatch attempted = new CountDownLatch(1);
        private final CountDownLatch connected = new CountDownLatch(1);
        private final int port;

        private GatedReconnectFactory(int port) {
            this.port = port;
        }

        @Override
        public WebSocketClient reconnect() {
            attempted.countDown();
            try {
                if (!allow.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("replacement was not released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            WebSocketClient client = connect(port);
            connected.countDown();
            return client;
        }

        @Override
        public void setSchemaRequired(boolean isRequired) {
        }
    }

    // Fault injection only: hold the receive poll until the producer has queued
    // a lookup, then fail before the I/O thread can send that lookup.
    private static final class DisconnectingClient extends WebSocketClient {
        private final CountDownLatch disconnect = new CountDownLatch(1);
        private final CountDownLatch receiving = new CountDownLatch(1);

        private DisconnectingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            receiving.countDown();
            try {
                Assert.assertTrue(disconnect.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            throw new LineSenderException("injected transport disconnect");
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }

    private static final class SchemaHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicReference<String> lastName = new AtomicReference<>();
        private final CountDownLatch requestSeen = new CountDownLatch(1);
        private volatile String disconnectName;
        private int disconnectsRemaining = 1;
        private volatile TestWebSocketServer.ClientHandler lastClient;
        private volatile long lastRequestId;
        private volatile boolean malformedReply;
        private volatile boolean reply;
        private volatile boolean sendStaleBeforeNextReply;
        private volatile int requests;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            ByteBuffer input = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            long requestId = input.getLong(QwpConstants.HEADER_SIZE + 1);
            int nameLength = input.getShort(QwpConstants.HEADER_SIZE + 9) & 0xffff;
            String name = new String(data, QwpConstants.HEADER_SIZE + 11, nameLength, StandardCharsets.UTF_8);
            requests++;
            lastClient = client;
            long staleRequestId = lastRequestId;
            lastRequestId = requestId;
            lastName.set(name);
            requestSeen.countDown();
            if (name.equals(disconnectName) && disconnectsRemaining-- > 0) {
                try {
                    client.sendClose(1001, "test disconnect");
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            } else if (sendStaleBeforeNextReply) {
                sendStaleBeforeNextReply = false;
                sendMalformed(client, staleRequestId);
                send(client, staleRequestId, 999);
                send(client, requestId, requests);
            } else if (malformedReply) {
                sendMalformed(client, requestId);
            } else if (reply) {
                send(client, requestId, requests);
            }
        }

        private synchronized void sendMalformedLastRequestAndFeedback(String tableName) {
            sendMalformed(lastClient, lastRequestId);
            byte[] name = tableName.getBytes(StandardCharsets.UTF_8);
            byte[] schema = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                    .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(0)
                    .put((byte) QwpSchemaProtocol.RESULT_MISSING).array();
            ByteBuffer feedback = ByteBuffer.allocate(11 + 2 + 2 + name.length + 4 + schema.length)
                    .order(ByteOrder.LITTLE_ENDIAN);
            feedback.put((byte) (WebSocketResponse.STATUS_OK | WebSocketResponse.SCHEMA_FEEDBACK_MODE_UPDATES))
                    .putLong(0).putShort((short) 0).putShort((short) 1)
                    .putShort((short) name.length).put(name).putInt(schema.length).put(schema);
            try {
                lastClient.sendBinary(feedback.array());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private static void sendMalformed(TestWebSocketServer.ClientHandler client, long requestId) {
            ByteBuffer out = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + 9).order(ByteOrder.LITTLE_ENDIAN);
            out.putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                    .putShort((short) 0).putInt(9).put(QwpSchemaProtocol.KIND_SCHEMA).putLong(requestId);
            try {
                client.sendBinary(out.array());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private static void send(TestWebSocketServer.ClientHandler client, long requestId, int tableId) {
            byte[] column = "x".getBytes(StandardCharsets.UTF_8);
            int payloadLength = 1 + 8 + 1 + 4 + 8 + 2 + 2 + 2 + column.length + 4 + 2;
            ByteBuffer out = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLength).order(ByteOrder.LITTLE_ENDIAN);
            out.putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                    .putShort((short) 0).putInt(payloadLength).put(QwpSchemaProtocol.KIND_SCHEMA)
                    .putLong(requestId).put((byte) QwpSchemaProtocol.RESULT_KNOWN).putInt(tableId)
                    .putLong(1).putShort((short) -1).putShort((short) 1)
                    .putShort((short) column.length).put(column).putInt(ColumnType.LONG).putShort((short) 0);
            try {
                client.sendBinary(out.array());
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }
}
