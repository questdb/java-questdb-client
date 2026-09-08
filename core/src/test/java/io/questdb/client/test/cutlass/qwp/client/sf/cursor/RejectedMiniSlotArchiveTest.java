/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SchemaPreserver;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.cutlass.qwp.client.sf.cursor.RejectedMiniSlotArchive;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotEpoch;
import io.questdb.client.cutlass.qwp.client.sf.cursor.UnreplayableSlotException;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class RejectedMiniSlotArchiveTest {
    private Path root;

    @Before
    public void setUp() throws Exception {
        root = Files.createTempDirectory("qdb-rejected-mini-slot-");
    }

    @After
    public void tearDown() throws Exception {
        if (root != null) {
            try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) { }
                });
            }
        }
    }

    @Test
    public void testEpochSurvivesReopenAndRejectsCorruption() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String slot = Files.createDirectory(root.resolve("slot")).toString();
        String first = SlotEpoch.openOrCreate(ff, slot);
        assertEquals(first, SlotEpoch.openOrCreate(ff, slot));
        assertEquals(first, SlotEpoch.read(ff, slot + '/' + SlotEpoch.FILE_NAME));
        String reset = SlotEpoch.openOrCreate(ff, slot, true);
        assertFalse(first.equals(reset));
        assertEquals(reset, SlotEpoch.openOrCreate(ff, slot, false));
    }

    @Test
    public void testCleanEngineCloseEndsEpochLifecycle() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String slot = Files.createDirectory(root.resolve("clean-close-slot")).toString();
        try (CursorSendEngine engine = new CursorSendEngine(slot, 4096)) {
            SlotEpoch.openOrCreate(ff, slot, engine.freshFsnNamespace());
            assertTrue(ff.exists(slot + '/' + SlotEpoch.FILE_NAME));
        }
        assertFalse(ff.exists(slot + '/' + SlotEpoch.FILE_NAME));
    }

    @Test
    public void testRecoveryFindsOnlyCurrentEpochAndScopedTempCleanup() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String source = Files.createDirectory(root.resolve("recovery-source")).toString();
        String epoch = SlotEpoch.openOrCreate(ff, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, true, "zero");
            SenderError error = rejection(0).withRejectionSpan(0, 0);
            RejectedMiniSlotArchive.Result result = RejectedMiniSlotArchive.preserve(
                    ff, engine, null, source, "slot-recovery", epoch, error);
            SenderError recovered = RejectedMiniSlotArchive.findOverlapping(
                    ff, source, "slot-recovery", epoch, 0, 0);
            assertNotNull(recovered);
            assertEquals(result.path, recovered.getRejectedPath());
            assertEquals(0, recovered.getRejectedFsn());
            assertEquals(null, RejectedMiniSlotArchive.findOverlapping(
                    ff, source, "slot-recovery", java.util.UUID.randomUUID().toString(), 0, 0));

            Path rejected = Paths.get(source, "rejected");
            Path ours = Files.createDirectory(rejected.resolve(
                    ".tmp-slot-recovery-" + epoch + "-fsn-0-0-dead"));
            Files.createFile(ours.resolve(RejectedMiniSlotArchive.SEGMENT_FILE_NAME));
            Path other = Files.createDirectory(rejected.resolve(
                    ".tmp-other-" + epoch + "-fsn-0-0-live"));
            RejectedMiniSlotArchive.cleanupTemporaryDirectories(
                    ff, source, "slot-recovery", epoch);
            assertFalse(Files.exists(ours));
            assertTrue(Files.exists(other));
        }
    }

    @Test
    public void testRecoveryDoesNotReadDamagedArchivesOutsideRequestedRange() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String source = Files.createDirectory(root.resolve("unrelated-archives")).toString();
        String epoch = SlotEpoch.openOrCreate(ff, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            for (int i = 0; i < 3; i++) appendDeltaFrame(engine, i, true, "symbol" + i);
            for (long fsn : new long[]{0, 2}) {
                String archive = RejectedMiniSlotArchive.preserve(
                        ff, engine, null, source, "slot", epoch, rejection(fsn)).path;
                Files.write(Paths.get(archive, RejectedMiniSlotArchive.METADATA_FILE_NAME), new byte[]{0});
            }
            assertNull(RejectedMiniSlotArchive.findOverlapping(ff, source, "slot", epoch, 1, 1));
            // Corruption still fails closed when its range is actually needed.
            try {
                RejectedMiniSlotArchive.findOverlapping(ff, source, "slot", epoch, 0, 0);
                fail("overlapping damaged metadata must not be ignored");
            } catch (UnreplayableSlotException expected) {
                assertTrue(expected.getMessage().contains("invalid rejection metadata size"));
            }
        }
    }

    @Test
    public void testRecoveryRequiresMetadataToMatchDirectoryRange() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String source = Files.createDirectory(root.resolve("mismatched-archive")).toString();
        String epoch = SlotEpoch.openOrCreate(ff, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, true, "zero");
            String archive = RejectedMiniSlotArchive.preserve(
                    ff, engine, null, source, "slot", epoch, rejection(0)).path;
            Files.move(Paths.get(archive), Paths.get(source, "rejected", "slot-" + epoch + "-fsn-0-1"));
            try {
                RejectedMiniSlotArchive.findOverlapping(ff, source, "slot", epoch, 1, 1);
                fail("overlapping directory with contradictory metadata must fail closed");
            } catch (UnreplayableSlotException expected) {
                assertTrue(expected.getMessage().contains("directory identity mismatch"));
            }
        }
    }

    @Test
    public void testRecoveryIgnoresNoncanonicalDirectoryNames() throws Exception {
        String source = Files.createDirectory(root.resolve("noncanonical-archives")).toString();
        String epoch = java.util.UUID.randomUUID().toString();
        Path rejected = Files.createDirectory(Paths.get(source, "rejected"));
        String prefix = "slot-" + epoch + "-fsn-";
        for (String suffix : new String[]{"", "0", "0-0-extra", "-1-0", "2-1", "00-1", "+0-1",
                "0-9223372036854775808"}) {
            Files.createDirectory(rejected.resolve(prefix + suffix));
        }
        assertNull(RejectedMiniSlotArchive.findOverlapping(
                FilesFacade.INSTANCE, source, "slot", epoch, 0, Long.MAX_VALUE));
    }

    @Test
    public void testArchiveReadFailureIsNotReclassifiedAsCorruption() throws Exception {
        String source = Files.createDirectory(root.resolve("archive-read-failure")).toString();
        String epoch = SlotEpoch.openOrCreate(FilesFacade.INSTANCE, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, true, "zero");
            String archive = RejectedMiniSlotArchive.preserve(
                    FilesFacade.INSTANCE, engine, null, source, "slot", epoch, rejection(0)).path;
            MmapSegmentException failure = new MmapSegmentException("injected operational read failure");
            FilesFacade ff = new DelegatingFilesFacade() {
                @Override
                public int openRW(String path) {
                    if (path.equals(archive + '/' + RejectedMiniSlotArchive.SEGMENT_FILE_NAME)) throw failure;
                    return super.openRW(path);
                }
            };
            try {
                RejectedMiniSlotArchive.findOverlapping(ff, source, "slot", epoch, 0, 0);
                fail("operational read failure must propagate");
            } catch (MmapSegmentException expected) {
                assertSame(failure, expected);
            }
            assertTrue(Files.isDirectory(Paths.get(archive)));
        }
    }

    @Test
    public void testPreservedSubsetReopensWithDictionarySupersetAndWorkingCopyKeepsArchive() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String source = Files.createDirectory(root.resolve("source")).toString();
        String dictDir = Files.createDirectory(root.resolve("dict")).toString();
        String epoch = SlotEpoch.openOrCreate(ff, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096);
             PersistedSymbolDict dictionary = PersistedSymbolDict.openClean(dictDir)) {
            dictionary.appendSymbol("zero");
            dictionary.appendSymbol("one");
            dictionary.appendSymbol("unused-superset-entry");
            appendDeltaFrame(engine, 0, true, "zero");
            appendDeltaFrame(engine, 1, true, "one");
            String serverMessage = TestUtils.repeat("column mismatch ", 2048);
            SenderError error = new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                    SenderError.Policy.REJECT_AND_CONTINUE, 3, serverMessage, 1,
                    0, 1, "tab", 42).withRejectionSpan(0, 1);
            RejectedMiniSlotArchive.Result result = RejectedMiniSlotArchive.preserve(
                    ff, engine, dictionary, source, "slot-0", epoch, error);
            assertFalse(result.reused);
            assertTrue(result.bytesWritten > 0);

            RejectedMiniSlotArchive.Metadata metadata = RejectedMiniSlotArchive.readMetadata(ff, result.path);
            assertEquals(0, metadata.fromFsn);
            assertEquals(1, metadata.toFsn);
            assertEquals(serverMessage, metadata.message);

            try (MmapSegment segment = MmapSegment.openExisting(result.path + '/'
                    + RejectedMiniSlotArchive.SEGMENT_FILE_NAME)) {
                long second = MmapSegment.HEADER_SIZE;
                second += MmapSegment.FRAME_HEADER_SIZE
                        + Unsafe.getUnsafe().getInt(segment.address() + second + 4);
                long payload = segment.address() + second + MmapSegment.FRAME_HEADER_SIZE;
                assertEquals(0, Unsafe.getUnsafe().getByte(payload + QwpConstants.HEADER_OFFSET_FLAGS)
                        & QwpConstants.FLAG_DEFER_COMMIT);
            }

            RejectedMiniSlotArchive.Result reused = RejectedMiniSlotArchive.preserve(
                    ff, engine, dictionary, source, "slot-0", epoch, error);
            assertTrue(reused.reused);

            String working = root.resolve("working").toString();
            RejectedMiniSlotArchive.copyToWorkingDirectory(ff, result.path, working);
            assertTrue(ff.exists(result.path + '/' + RejectedMiniSlotArchive.SEGMENT_FILE_NAME));
            try (CursorSendEngine replay = new CursorSendEngine(working, 4096)) {
                assertEquals(1, replay.publishedFsn());
                assertEquals(-1, replay.ackedFsn());
            }
            assertTrue(ff.exists(result.path + '/' + RejectedMiniSlotArchive.SEGMENT_FILE_NAME));
        }
    }

    @Test
    public void testSynchronousPreserverReturnsCompleteCopy() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String source = Files.createDirectory(root.resolve("sync-source")).toString();
        String epoch = SlotEpoch.openOrCreate(ff, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, true, "zero");
            SchemaPreserver preserver = new SchemaPreserver(ff, source, "slot-sync", epoch);
            RejectedMiniSlotArchive.Result result = preserver.preserve(engine,
                    rejection(0), new byte[]{4, 'z', 'e', 'r', 'o'}, 1);
            assertNotNull(RejectedMiniSlotArchive.readMetadata(ff, result.path));
            try (PersistedSymbolDict dictionary = PersistedSymbolDict.open(ff, result.path)) {
                assertNotNull(dictionary);
                assertEquals(1, dictionary.size());
            }
        }
    }

    @Test
    public void testSynchronousPreserverPropagatesFailure() throws Exception {
        FilesFacade ff = FilesFacade.INSTANCE;
        String source = Files.createDirectory(root.resolve("sync-failure")).toString();
        String epoch = SlotEpoch.openOrCreate(ff, source);
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            SchemaPreserver preserver = new SchemaPreserver(ff, source, "slot-failure", epoch);
            try {
                preserver.preserve(engine, rejection(0), null, 0);
                fail("missing source frame must fail preservation");
            } catch (io.questdb.client.cutlass.qwp.client.sf.cursor.SfOperationalException expected) {
                assertEquals(-1, engine.ackedFsn());
            }
        }
    }

    private static SenderError rejection(long fsn) {
        return new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.REJECT_AND_CONTINUE, 3, "column mismatch", fsn,
                fsn, fsn, "tab", 42);
    }

    private static void appendDeltaFrame(CursorSendEngine engine, int deltaStart,
                                         boolean deferred, String symbol) {
        byte[] utf8 = symbol.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int size = QwpConstants.HEADER_SIZE + 2 + 1 + utf8.length;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            Unsafe.getUnsafe().putInt(buf, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS,
                    (byte) (QwpConstants.FLAG_DELTA_SYMBOL_DICT
                            | (deferred ? QwpConstants.FLAG_DEFER_COMMIT : 0)));
            long p = buf + QwpConstants.HEADER_SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) deltaStart);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 1);
            Unsafe.getUnsafe().putByte(p + 2, (byte) utf8.length);
            Unsafe.getUnsafe().copyMemory(utf8, Unsafe.BYTE_OFFSET, null, p + 3, utf8.length);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
