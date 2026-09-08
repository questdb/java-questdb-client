/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.test.impl;

import io.questdb.client.QuestDB;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.RejectedMiniSlotArchive;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotEpoch;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SchemaRejectionPoolTest {
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testRecoveredPreservedOrphanReportsAsynchronouslyBeforeRetirement() throws Exception {
        String base = temp.newFolder("recovered").getAbsolutePath();
        String slot = Files.createDirectory(java.nio.file.Paths.get(base, "saved")).toString();
        String archive;
        try (CursorSendEngine engine = new CursorSendEngine(slot, 1 << 20)) {
            String epoch = SlotEpoch.openOrCreate(FilesFacade.INSTANCE, slot, engine.freshFsnNamespace());
            long frame = Unsafe.malloc(QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(frame, QwpConstants.HEADER_SIZE, (byte) 0);
                Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
                Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS, QwpConstants.FLAG_DEFER_COMMIT);
                engine.appendBlocking(frame, QwpConstants.HEADER_SIZE);
            } finally {
                Unsafe.free(frame, QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
            SenderError error = new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                    SenderError.Policy.REJECT_AND_CONTINUE, 3, "schema rejected", 0, 0, 0, null, 1);
            archive = RejectedMiniSlotArchive.preserve(FilesFacade.INSTANCE, engine, null,
                    slot, "saved", epoch, error).path;
        }
        CountDownLatch reported = new CountDownLatch(1);
        AtomicReference<SenderError> error = new AtomicReference<>();
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                throw new AssertionError("orphan frames must be retired without sending");
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.builder("ws::addr=localhost:" + server.getPort() + ";sf_dir=" + base + ";")
                    .senderId("saved").errorHandler(e -> {
                        error.set(e);
                        callbackThread.set(Thread.currentThread());
                        reported.countDown();
                    }).build()) {
                Assert.assertTrue(reported.await(5, TimeUnit.SECONDS));
                Assert.assertNotSame(Thread.currentThread(), callbackThread.get());
                Assert.assertEquals(archive, error.get().getRejectedPath());
                Assert.assertEquals(0, sender.getAckedFsn());
            }
        }
    }

    @Test
    public void testLazyPoolValidatesDestinationBeforeFirstBorrow() throws Exception {
        String file = temp.newFile("not-a-directory").getAbsolutePath();
        try (QuestDB ignored = QuestDB.builder().fromConfig("ws::addr=localhost:1;")
                .senderPoolMin(0).senderPoolMax(1).queryPoolMin(0).queryPoolMax(1)
                .dlqDirectory(file).build()) {
            Assert.fail("build must reject a destination that cannot hold archives");
        } catch (io.questdb.client.cutlass.line.LineSenderException expected) {
            Assert.assertTrue(expected.getMessage().contains("schema preservation destination"));
        }
    }

    @Test
    public void testDiskQueuePreservesBeforeRetirementAndContinuation() throws Exception {
        CountDownLatch reported = new CountDownLatch(1);
        AtomicReference<SenderError> rejection = new AtomicReference<>();
        AtomicReference<TestWebSocketServer.ClientHandler> firstConnection = new AtomicReference<>();
        Map<TestWebSocketServer.ClientHandler, Long> sequences = new ConcurrentHashMap<>();
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                long sequence = sequences.merge(client, 1L, Long::sum) - 1;
                try {
                    if (firstConnection.compareAndSet(null, client)) {
                        client.sendBinary(QwpWireTestUtils.buildNack(sequence, WebSocketResponse.STATUS_SCHEMA_MISMATCH));
                    } else if (firstConnection.get() != client) {
                        client.sendBinary(QwpWireTestUtils.buildAck(sequence));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String sfDir = temp.newFolder("sf").getAbsolutePath();
            try (QuestDB db = QuestDB.builder()
                    .fromConfig("ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir
                            + ";close_flush_timeout_millis=0;")
                    .senderPoolSize(1).queryPoolMin(0).queryPoolMax(1)
                    .errorHandler(error -> { rejection.set(error); reported.countDown(); }).build()) {
                Sender failed = db.borrowSender();
                failed.table("bad").stringColumn("value", "wrong").atNow();
                long rejectedFsn = failed.flushAndGetSequence();
                Assert.assertTrue(reported.await(10, TimeUnit.SECONDS));
                try {
                    failed.awaitAckedFsn(rejectedFsn, 0);
                    Assert.fail("owning handle must fail after preserved rejection");
                } catch (LineSenderServerException expected) {
                    // Mark the lease-local failure observed so close only returns the slot.
                }
                failed.close();
                SenderError error = rejection.get();
                Assert.assertEquals(SenderError.Policy.REJECT_AND_CONTINUE, error.getAppliedPolicy());
                Assert.assertNotNull(error.getRejectedPath());
                Assert.assertTrue(Files.isDirectory(java.nio.file.Paths.get(error.getRejectedPath())));
                try (Sender healthy = db.borrowSender()) {
                    healthy.table("good").longColumn("value", 42).atNow();
                    long target = healthy.flushAndGetSequence();
                    Assert.assertTrue(healthy.awaitAckedFsn(target, 10_000));
                }
            }
        }
    }

    @Test
    public void testObservedFailedHandleCloseReturnsSlot() throws Exception {
        CountDownLatch rejected = new CountDownLatch(1);
        AtomicReference<TestWebSocketServer.ClientHandler> firstConnection = new AtomicReference<>();
        Map<TestWebSocketServer.ClientHandler, Long> sequences = new ConcurrentHashMap<>();
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                long sequence = sequences.merge(client, 1L, Long::sum) - 1;
                try {
                    if (firstConnection.compareAndSet(null, client)) {
                        client.sendBinary(QwpWireTestUtils.buildNack(sequence, WebSocketResponse.STATUS_SCHEMA_MISMATCH));
                        rejected.countDown();
                    } else if (firstConnection.get() != client) {
                        client.sendBinary(QwpWireTestUtils.buildAck(sequence));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = newPool(server)) {
                Sender failed = db.borrowSender();
                failed.table("bad").stringColumn("value", "wrong").atNow();
                long rejectedFsn = failed.flushAndGetSequence();
                Assert.assertTrue(rejected.await(5, TimeUnit.SECONDS));
                try {
                    failed.awaitAckedFsn(rejectedFsn, 10_000);
                    Assert.fail("owning handle must observe schema rejection");
                } catch (LineSenderServerException expected) {
                    Assert.assertEquals(rejectedFsn, expected.getServerError().getRejectedFsn());
                }
                failed.close();

                try (Sender healthy = db.borrowSender()) {
                    healthy.table("good").longColumn("value", 42).atNow();
                    long target = healthy.flushAndGetSequence();
                    Assert.assertTrue("returned slot must remain usable", healthy.awaitAckedFsn(target, 10_000));
                }
            }
        }
    }

    @Test
    public void testTransactionalDeferredRejectionRetiresThroughPublishedTail() throws Exception {
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch rejectNow = new CountDownLatch(1);
        AtomicReference<TestWebSocketServer.ClientHandler> firstConnection = new AtomicReference<>();
        Map<TestWebSocketServer.ClientHandler, Long> sequences = new ConcurrentHashMap<>();
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                long sequence = sequences.merge(client, 1L, Long::sum) - 1;
                try {
                    if (firstConnection.compareAndSet(null, client)) {
                        firstReceived.countDown();
                        if (!rejectNow.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("test did not release NACK");
                        }
                        client.sendBinary(QwpWireTestUtils.buildNack(sequence, WebSocketResponse.STATUS_SCHEMA_MISMATCH));
                    } else if (firstConnection.get() != client) {
                        client.sendBinary(QwpWireTestUtils.buildAck(sequence));
                    }
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.builder()
                    .fromConfig("ws::addr=localhost:" + server.getPort()
                            + ";auto_flush_rows=1;auto_flush_bytes=off;transaction=on;close_flush_timeout_millis=0;")
                    .senderPoolSize(1).queryPoolMin(0).queryPoolMax(1)
                    .schemaMismatchPolicy(SenderError.Policy.REJECT_AND_CONTINUE).dlqEnabled(false).build()) {
                Sender failed = db.borrowSender();
                failed.table("bad").longColumn("value", 1).atNow();
                Assert.assertTrue(firstReceived.await(5, TimeUnit.SECONDS));
                failed.table("bad").longColumn("value", 2).atNow();
                rejectNow.countDown();
                try {
                    failed.awaitAckedFsn(1, 10_000);
                    Assert.fail("transaction owner must fail");
                } catch (LineSenderServerException expected) {
                    Assert.assertEquals(0, expected.getServerError().getFromFsn());
                    Assert.assertEquals(1, expected.getServerError().getToFsn());
                }
                failed.close();
                try (Sender healthy = db.borrowSender()) {
                    healthy.table("good").longColumn("value", 3).atNow();
                }
            } finally {
                rejectNow.countDown();
            }
        }
    }

    @Test
    public void testMalformedSchemaSequenceFailsClosed() throws Exception {
        CountDownLatch rejected = new CountDownLatch(1);
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                try {
                    client.sendBinary(QwpWireTestUtils.buildNack(99, WebSocketResponse.STATUS_SCHEMA_MISMATCH));
                    rejected.countDown();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            Sender sender = Sender.builder("ws::addr=localhost:" + server.getPort()
                            + ";close_flush_timeout_millis=0;")
                    .schemaMismatchPolicy(SenderError.Policy.REJECT_AND_CONTINUE)
                    .dlqEnabled(false)
                    .build();
            try {
                sender.table("bad").longColumn("value", 1).atNow();
                long target = sender.flushAndGetSequence();
                Assert.assertTrue(rejected.await(5, TimeUnit.SECONDS));
                try {
                    sender.awaitAckedFsn(target, 10_000);
                    Assert.fail("out-of-range NACK must fail closed");
                } catch (LineSenderServerException expected) {
                    Assert.assertEquals(SenderError.Policy.TERMINAL,
                            expected.getServerError().getAppliedPolicy());
                }
            } finally {
                try {
                    sender.close();
                } catch (LineSenderServerException ignored) {
                }
            }
        }
    }

    @Test
    public void testRejectionAfterReturnDoesNotFailNextBorrow() throws Exception {
        CountDownLatch firstReceived = new CountDownLatch(1);
        CountDownLatch rejectNow = new CountDownLatch(1);
        CountDownLatch reported = new CountDownLatch(1);
        AtomicReference<SenderError> rejection = new AtomicReference<>();
        AtomicReference<TestWebSocketServer.ClientHandler> rejectedConnection = new AtomicReference<>();
        Map<TestWebSocketServer.ClientHandler, Long> sequences = new ConcurrentHashMap<>();
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                long sequence = sequences.merge(client, 1L, Long::sum) - 1;
                try {
                    if (rejectedConnection.compareAndSet(null, client)) {
                        firstReceived.countDown();
                        if (!rejectNow.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("test did not release NACK");
                        }
                        client.sendBinary(QwpWireTestUtils.buildNack(sequence, WebSocketResponse.STATUS_SCHEMA_MISMATCH));
                    } else if (rejectedConnection.get() != client) {
                        client.sendBinary(QwpWireTestUtils.buildAck(sequence));
                    }
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.builder()
                    .fromConfig("ws::addr=localhost:" + server.getPort() + ";close_flush_timeout_millis=0;")
                    .senderPoolSize(1).queryPoolMin(0).queryPoolMax(1)
                    .schemaMismatchPolicy(SenderError.Policy.REJECT_AND_CONTINUE).dlqEnabled(false)
                    .errorHandler(error -> { rejection.set(error); reported.countDown(); }).build()) {
                try (Sender a = db.borrowSender()) {
                    a.table("bad").stringColumn("value", "wrong").atNow();
                    a.flush();
                    Assert.assertTrue(firstReceived.await(5, TimeUnit.SECONDS));
                }
                try (Sender b = db.borrowSender()) {
                    b.table("good").longColumn("value", 42).atNow();
                    long target = b.flushAndGetSequence();
                    rejectNow.countDown();
                    Assert.assertTrue("later borrow must drain past old rejection", b.awaitAckedFsn(target, 10_000));
                    Assert.assertTrue(reported.await(5, TimeUnit.SECONDS));
                    Assert.assertEquals(SenderError.Policy.REJECT_AND_CONTINUE, rejection.get().getAppliedPolicy());
                    Assert.assertEquals(0, rejection.get().getRejectedFsn());
                }
            } finally {
                rejectNow.countDown();
            }
        }
    }

    private static QuestDB newPool(TestWebSocketServer server) {
        return QuestDB.builder()
                .fromConfig("ws::addr=localhost:" + server.getPort() + ";close_flush_timeout_millis=0;")
                .senderPoolSize(1).queryPoolMin(0).queryPoolMax(1)
                .schemaMismatchPolicy(SenderError.Policy.REJECT_AND_CONTINUE).dlqEnabled(false)
                .build();
    }
}
