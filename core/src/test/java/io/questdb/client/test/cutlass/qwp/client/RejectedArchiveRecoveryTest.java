/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.RejectedMiniSlotArchive;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotEpoch;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RejectedArchiveRecoveryTest {
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testDamagedDrainedArchiveDoesNotBlockRepeatedBuilds() throws Exception {
        assertRepeatedBuilds(RejectedMiniSlotArchive.METADATA_FILE_NAME, false);
    }

    @Test(timeout = 30_000)
    public void testDamagedOverlappingArchiveQuarantinesOnceAndBuildsContinue() throws Exception {
        assertRepeatedBuilds(RejectedMiniSlotArchive.METADATA_FILE_NAME, true);
    }

    @Test(timeout = 30_000)
    public void testIntactOverlappingArchiveReportsAndBuildsContinue() throws Exception {
        assertRepeatedBuilds(null, true);
    }

    @Test(timeout = 30_000)
    public void testDamagedOverlappingArchiveSegmentQuarantinesOnce() throws Exception {
        assertRepeatedBuilds(RejectedMiniSlotArchive.SEGMENT_FILE_NAME, true);
    }

    private void assertRepeatedBuilds(String damagedFile, boolean overlaps) throws Exception {
        boolean damaged = damagedFile != null;
        Path base = temp.newFolder().toPath();
        Path slot = Files.createDirectory(base.resolve("saved"));
        String archive;
        try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
            // Keep fixture construction independent of the manager's ACK-persistence tick.
            engine.getManagerForTesting().close();
            String epoch = SlotEpoch.openOrCreate(FilesFacade.INSTANCE, slot.toString(), engine.freshFsnNamespace());
            append(engine, false);
            archive = preserve(engine, slot, epoch, 0);
            assertTrue(engine.acknowledge(0));
            append(engine, true); // Uncommitted orphan tail at FSN 1 forces the startup archive scan.
            if (overlaps) archive = preserve(engine, slot, epoch, 1);
        }
        // FSN 0 is the drained prefix of this fixture. acknowledge() only
        // advances the live ring; a partially drained close need not persist
        // that watermark. Write it explicitly so orphan validation happens
        // during build(), rather than after replay ACKs on the I/O thread.
        try (AckWatermark watermark = AckWatermark.open(slot.toString())) {
            assertNotNull(watermark);
            watermark.write(0);
            watermark.sync();
        }
        Path archiveFile = Paths.get(archive, damaged ? damagedFile : RejectedMiniSlotArchive.METADATA_FILE_NAME);
        byte[] archiveBytes = Files.readAllBytes(archiveFile);
        if (damaged) {
            archiveBytes[0] ^= 1; // Corrupt metadata CRC or segment magic, retaining the remaining bytes.
            Files.write(archiveFile, archiveBytes);
        }
        AtomicInteger quarantines = new AtomicInteger();
        CountDownLatch schemaReported = new CountDownLatch(1);
        Map<TestWebSocketServer.ClientHandler, Long> sequences = new ConcurrentHashMap<>();
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                long sequence = sequences.merge(client, 1L, Long::sum) - 1;
                try {
                    client.sendBinary(QwpWireTestUtils.buildAck(sequence));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        })) {
            server.start();
            assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            List<RuntimeException> failures = new ArrayList<>();
            for (int attempt = 0; attempt < 3; attempt++) {
                try (Sender sender = Sender.builder("ws::addr=localhost:" + server.getPort()
                                + ";sf_dir=" + base + ";close_flush_timeout_millis=0;")
                        .senderId("saved").errorHandler(error -> {
                            if (error.getCategory() == SenderError.Category.DATA_LOSS) quarantines.incrementAndGet();
                            if (error.getCategory() == SenderError.Category.SCHEMA_MISMATCH) schemaReported.countDown();
                        }).build()) {
                    assertEquals("quarantine must complete during build", damaged && overlaps ? 1 : 0,
                            quarantines.get());
                    sender.table("healthy").longColumn("value", attempt).atNow();
                    long target = sender.flushAndGetSequence();
                    assertTrue("new rows must drain after recovery", sender.awaitAckedFsn(target, 5_000));
                    if (attempt == 0 && !damaged && overlaps) {
                        assertTrue(schemaReported.await(5, TimeUnit.SECONDS));
                    }
                } catch (RuntimeException e) {
                    failures.add(e);
                }
            }
            assertTrue("all three builds must succeed: " + failures, failures.isEmpty());
        }
        Path quarantined = base.resolve("saved.unreplayable-0");
        if (damaged && overlaps) {
            assertEquals(1, quarantines.get());
            assertTrue(Files.exists(quarantined.resolve(".failed")));
            assertArrayEquals(archiveBytes, Files.readAllBytes(quarantined.resolve(slot.relativize(archiveFile))));
            assertFalse(Files.exists(base.resolve("saved.unreplayable-1")));
        } else {
            assertEquals(0, quarantines.get());
            assertFalse(Files.exists(quarantined));
            assertArrayEquals(archiveBytes, Files.readAllBytes(archiveFile));
        }
    }

    private static String preserve(CursorSendEngine engine, Path slot, String epoch, long fsn) {
        SenderError error = new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.REJECT_AND_CONTINUE, 3, "schema rejected", fsn, fsn, fsn, null, 1);
        return RejectedMiniSlotArchive.preserve(FilesFacade.INSTANCE, engine, null,
                slot.toString(), "saved", epoch, error).path;
    }

    private static void append(CursorSendEngine engine, boolean deferred) {
        long frame = Unsafe.malloc(QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(frame, QwpConstants.HEADER_SIZE, (byte) 0);
            Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS,
                    (byte) (deferred ? QwpConstants.FLAG_DEFER_COMMIT : 0));
            engine.appendBlocking(frame, QwpConstants.HEADER_SIZE);
        } finally {
            Unsafe.free(frame, QwpConstants.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
