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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class RejectedArchiveRecoveryTest {
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testDamagedDrainedArchiveDoesNotBlockRepeatedBuilds() throws Exception {
        assertRepeatedBuilds(RejectedMiniSlotArchive.METADATA_FILE_NAME, false);
    }

    @Test(timeout = 30_000)
    public void testDamagedOverlappingArchiveDoesNotAffectQueue() throws Exception {
        assertRepeatedBuilds(RejectedMiniSlotArchive.METADATA_FILE_NAME, true);
    }

    @Test(timeout = 30_000)
    public void testPublishedArchiveReconstructsCallbackBeforeOrphanRetirement() throws Exception {
        assertRepeatedBuilds(null, true);
    }

    @Test(timeout = 30_000)
    public void testDamagedOverlappingArchiveSegmentDoesNotAffectQueue() throws Exception {
        assertRepeatedBuilds(RejectedMiniSlotArchive.SEGMENT_FILE_NAME, true);
    }

    @Test(timeout = 30_000)
    public void testMismatchedArchiveBoundaryDoesNotAffectQueue() throws Exception {
        assertRepeatedBuilds(AckWatermark.FILE_NAME, true);
    }

    private void assertRepeatedBuilds(String damagedFile, boolean overlaps) throws Exception {
        boolean damaged = damagedFile != null;
        Path base = temp.newFolder().toPath();
        Path slot = Files.createDirectory(base.resolve("saved"));
        // Residue from the original PR and a crashed writer is output only.
        Path oldEpoch = slot.resolve(".slot-epoch");
        Files.write(oldEpoch, new byte[]{0});
        Path staging = Files.createDirectories(slot.resolve("rejected/.tmp-legacy-writer"));
        Path oldMetadata = staging.resolve("rejection-meta.bin");
        Files.write(oldMetadata, new byte[]{1});
        String archive;
        try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
            // Keep fixture construction independent of the manager's ACK-persistence tick.
            engine.getManagerForTesting().close();
            append(engine, false);
            archive = preserve(engine, slot, 0);
            assertTrue(engine.acknowledge(0));
            append(engine, true); // Uncommitted orphan tail at FSN 1.
            if (overlaps) archive = preserve(engine, slot, 1);
        }
        // FSN 0 is the drained prefix of this fixture. acknowledge() only
        // advances the live ring; a partially drained close need not persist
        // that watermark. Write it explicitly so the orphan can retire during
        // build(), rather than after replay ACKs on the I/O thread.
        try (AckWatermark watermark = AckWatermark.open(slot.toString())) {
            assertNotNull(watermark);
            watermark.write(0);
            watermark.sync();
        }
        Path archiveFile = Paths.get(archive, damaged ? damagedFile : RejectedMiniSlotArchive.METADATA_FILE_NAME);
        if (damaged) {
            if (RejectedMiniSlotArchive.SEGMENT_FILE_NAME.equals(damagedFile)) {
                // Keep the valid file size while corrupting the segment header.
                byte[] archiveBytes = Files.readAllBytes(archiveFile);
                archiveBytes[0] ^= 1;
                Files.write(archiveFile, archiveBytes);
            } else if (AckWatermark.FILE_NAME.equals(damagedFile)) {
                try (AckWatermark archiveWatermark = AckWatermark.open(archive)) {
                    assertNotNull(archiveWatermark);
                    archiveWatermark.write(1); // Structurally valid, but expected boundary is 0.
                    archiveWatermark.sync();
                }
            } else {
                Files.write(archiveFile, new byte[]{0});
            }
        }
        byte[] archiveBytes = Files.readAllBytes(archiveFile);
        AtomicInteger quarantines = new AtomicInteger();
        AtomicInteger schemaReports = new AtomicInteger();
        CountDownLatch schemaReported = new CountDownLatch(1);
        AtomicReference<SenderError> recoveredReport = new AtomicReference<>();
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
                            if (error.getCategory() == SenderError.Category.SCHEMA_MISMATCH) {
                                recoveredReport.set(error);
                                schemaReports.incrementAndGet();
                                schemaReported.countDown();
                            }
                        }).build()) {
                    assertEquals("archives must never quarantine a live queue", 0,
                            quarantines.get());
                    sender.table("healthy").longColumn("value", attempt).atNow();
                    long target = sender.flushAndGetSequence();
                    assertTrue("new rows must drain after recovery", sender.awaitAckedFsn(target, 5_000));

                } catch (RuntimeException e) {
                    failures.add(e);
                }
            }
            assertTrue("all three builds must succeed: " + failures, failures.isEmpty());
        }
        Path quarantined = base.resolve("saved.unreplayable-0");
        assertEquals(0, quarantines.get());
        if (overlaps && !damaged) {
            assertTrue("published archive report must precede orphan retirement",
                    schemaReported.await(5, TimeUnit.SECONDS));
            assertEquals(1, schemaReports.get());
            SenderError report = recoveredReport.get();
            assertNotNull(report);
            assertEquals(1, report.getFromFsn());
            assertEquals(1, report.getToFsn());
            assertEquals(1, report.getRejectedFsn());
            assertEquals(archive, report.getRejectedPath());
        } else {
            assertEquals(1, schemaReported.getCount());
            assertEquals(0, schemaReports.get());
        }
        assertFalse(Files.exists(quarantined));
        assertArrayEquals(archiveBytes, Files.readAllBytes(archiveFile));
        assertArrayEquals(new byte[]{0}, Files.readAllBytes(oldEpoch));
        assertArrayEquals(new byte[]{1}, Files.readAllBytes(oldMetadata));
    }

    private static String preserve(CursorSendEngine engine, Path slot, long fsn) {
        SenderError error = new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.REJECT_AND_CONTINUE, 3, "schema rejected", fsn, fsn, fsn, null, 1);
        return new RejectedMiniSlotArchive(FilesFacade.INSTANCE, slot.toString()).preserve(engine, error, null, 0).path;
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
