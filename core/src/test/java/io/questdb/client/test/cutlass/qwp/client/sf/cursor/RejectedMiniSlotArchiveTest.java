/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentCorruptionException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.RejectedMiniSlotArchive;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfOperationalException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfRecoveryException;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RejectedMiniSlotArchiveTest {
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testPreservedSubsetReplaysWithDictionaryAndKeepsArchive() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        byte[] dict = {4, 'z', 'e', 'r', 'o', 3, 'o', 'n', 'e', 6, 'u', 'n', 'u', 's', 'e', 'd'};
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, true, "zero");
            appendDeltaFrame(engine, 1, true, "one");
            String message = TestUtils.repeat("mismatch: \u00e9\n\\=", 8192);
            SenderError error = new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                    SenderError.Policy.REJECT_AND_CONTINUE, 3, message, 1, 0, 1, "tab", 42);
            RejectedMiniSlotArchive writer = new RejectedMiniSlotArchive(FilesFacade.INSTANCE, source);
            RejectedMiniSlotArchive.Result result = writer.preserve(engine, error, dict, 3);
            assertTrue(result.bytesWritten > 0);
            Properties metadata = new Properties();
            try (java.io.InputStream input = Files.newInputStream(Paths.get(result.path,
                    RejectedMiniSlotArchive.METADATA_FILE_NAME))) {
                metadata.load(input);
            }
            assertEquals("0", metadata.getProperty("fromFsn"));
            assertEquals("1", metadata.getProperty("toFsn"));
            assertEquals(message, metadata.getProperty("message"));
            try (MmapSegment segment = MmapSegment.openExisting(result.path + '/'
                    + RejectedMiniSlotArchive.SEGMENT_FILE_NAME)) {
                long second = MmapSegment.HEADER_SIZE;
                second += MmapSegment.FRAME_HEADER_SIZE
                        + Unsafe.getUnsafe().getInt(segment.address() + second + 4);
                long payload = segment.address() + second + MmapSegment.FRAME_HEADER_SIZE;
                assertEquals(0, Unsafe.getUnsafe().getByte(payload + QwpConstants.HEADER_OFFSET_FLAGS)
                        & QwpConstants.FLAG_DEFER_COMMIT);
            }
            String working = Paths.get(source, "working").toString();
            copyArchive(result.path, working);
            try (CursorSendEngine replay = new CursorSendEngine(working, 4096)) {
                assertEquals(1, replay.publishedFsn());
                assertEquals(-1, replay.ackedFsn());
                assertEquals(3, replay.getPersistedSymbolDict().size());
                replay.acknowledge(1);
            }
            assertTrue(Files.exists(Paths.get(result.path, RejectedMiniSlotArchive.SEGMENT_FILE_NAME)));
            assertEquals(-1, engine.ackedFsn());
        }
    }

    @Test
    public void testSameRangeIsReusedAcrossWriterRestart() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        String namespace = RejectedMiniSlotArchive.namespaceForSource(source);
        RejectedMiniSlotArchive.Result first;
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "zero");
            first = new RejectedMiniSlotArchive(
                    FilesFacade.INSTANCE, source, namespace).preserve(engine, rejection(0), null, 0);
            assertFalse(first.reused);
        }
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            RejectedMiniSlotArchive.Result second = new RejectedMiniSlotArchive(
                    FilesFacade.INSTANCE, source, namespace).preserve(engine, rejection(0), null, 0);
            assertTrue(second.reused);
            assertEquals(first.path, second.path);
            assertEquals(0, second.bytesWritten);
            try (java.util.stream.Stream<Path> archives = Files.list(Paths.get(source, "rejected"))) {
                assertEquals(1, archives.count());
            }
        }
    }

    @Test
    public void testFreshFsnNamespaceDoesNotReuseOldRange() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        String namespace = RejectedMiniSlotArchive.namespaceForSource(source);
        String first;
        long firstGeneration;
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "first");
            firstGeneration = engine.findSegmentContaining(0).generationToken();
            first = new RejectedMiniSlotArchive(FilesFacade.INSTANCE, source, namespace)
                    .preserve(engine, rejection(0), null, 0).path;
            engine.acknowledge(0);
        }
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "second");
            assertNotEquals(firstGeneration, engine.findSegmentContaining(0).generationToken());
            RejectedMiniSlotArchive writer = new RejectedMiniSlotArchive(
                    FilesFacade.INSTANCE, source, namespace);
            assertNull(writer.findRecoveredOrphanReport(engine, 0, 0));
            RejectedMiniSlotArchive.Result second = writer.preserve(engine, rejection(0), null, 0);
            assertFalse(second.reused);
            assertNotEquals(first, second.path);
        }
    }

    @Test
    public void testRecoveredLookupCleansOnlyExactStagingDirectory() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "zero");
            RejectedMiniSlotArchive writer = new RejectedMiniSlotArchive(FilesFacade.INSTANCE, source);
            Path completed = Paths.get(writer.preserve(engine, rejection(0), null, 0).path);
            Path otherWriter = Paths.get(new RejectedMiniSlotArchive(FilesFacade.INSTANCE, source,
                    RejectedMiniSlotArchive.namespaceForSource(null))
                    .preserve(engine, rejection(0), null, 0).path);
            Path staging = completed.resolveSibling(".tmp-" + completed.getFileName());
            Files.move(completed, staging);
            Path unrelated = Files.createDirectories(Paths.get(source, "rejected", ".tmp-unrelated"));
            Files.write(unrelated.resolve("keep"), new byte[]{1});

            assertNull(new RejectedMiniSlotArchive(FilesFacade.INSTANCE, source)
                    .findRecoveredOrphanReport(engine, 0, 0));
            assertFalse(Files.exists(staging));
            assertTrue(Files.isDirectory(otherWriter));
            assertArrayEquals(new byte[]{1}, Files.readAllBytes(unrelated.resolve("keep")));
        }
        assertNotEquals(RejectedMiniSlotArchive.namespaceForSource(null),
                RejectedMiniSlotArchive.namespaceForSource(null));
    }

    @Test
    public void testParentSyncRetryDoesNotCopyAgain() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        AtomicInteger publications = new AtomicInteger();
        AtomicInteger rootSyncs = new AtomicInteger();
        FilesFacade ff = new DelegatingFilesFacade() {
            @Override
            public int rename(String from, String to) {
                int result = super.rename(from, to);
                if (result == 0) publications.incrementAndGet();
                return result;
            }
            @Override
            public int fsyncDir(String dir) {
                if (dir.equals(source + "/rejected") && rootSyncs.incrementAndGet() <= 2) return -1;
                return super.fsyncDir(dir);
            }
        };
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "zero");
            RejectedMiniSlotArchive writer = new RejectedMiniSlotArchive(ff, source);
            for (int attempt = 0; attempt < 2; attempt++) {
                try {
                    writer.preserve(engine, rejection(0), null, 0);
                    fail("directory barrier must gate successful preservation");
                } catch (SfOperationalException expected) {
                    assertEquals(-1, engine.ackedFsn());
                }
            }
            RejectedMiniSlotArchive.Result result = writer.preserve(engine, rejection(0), null, 0);
            assertEquals(1, publications.get());
            assertTrue(Files.exists(Paths.get(result.path, RejectedMiniSlotArchive.METADATA_FILE_NAME)));
        }
    }

    @Test
    public void testFailedCopyCleansOnlyItsOwnStagingDirectory() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        Path rejected = Files.createDirectory(Paths.get(source, "rejected"));
        Path other = Files.createDirectory(rejected.resolve(".tmp-another-writer"));
        Files.write(other.resolve("keep"), new byte[]{1});
        AtomicInteger attempts = new AtomicInteger();
        FilesFacade ff = new DelegatingFilesFacade() {
            @Override
            public int openRWExclusive(String path) {
                if (path.endsWith(RejectedMiniSlotArchive.METADATA_FILE_NAME) && attempts.getAndIncrement() == 0) return -1;
                return super.openRWExclusive(path);
            }
        };
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "zero");
            RejectedMiniSlotArchive writer = new RejectedMiniSlotArchive(ff, source);
            try {
                writer.preserve(engine, rejection(0), null, 0);
                fail("injected metadata write failure");
            } catch (SfOperationalException expected) {
                assertEquals(-1, engine.ackedFsn());
            }
            try (java.util.stream.Stream<Path> children = Files.list(rejected)) {
                assertEquals(1, children.count());
            }
            writer.preserve(engine, rejection(0), null, 0);
            assertArrayEquals(new byte[]{1}, Files.readAllBytes(other.resolve("keep")));
        }
    }

    @Test
    public void testExistingRecoveryReaderRejectsDamagedArchiveCopy() throws Exception {
        String source = temp.newFolder().getAbsolutePath();
        try (CursorSendEngine engine = new CursorSendEngine(source, 4096)) {
            appendDeltaFrame(engine, 0, false, "zero");
            String archive = new RejectedMiniSlotArchive(FilesFacade.INSTANCE, source)
                    .preserve(engine, rejection(0), null, 0).path;
            Path segment = Paths.get(archive, RejectedMiniSlotArchive.SEGMENT_FILE_NAME);
            byte[] bytes = Files.readAllBytes(segment);
            bytes[0] ^= 1;
            Files.write(segment, bytes);
            String working = source + "/working";
            copyArchive(archive, working);
            try (CursorSendEngine ignored = new CursorSendEngine(working, 4096)) {
                fail("damaged archive must not replay");
            } catch (MmapSegmentCorruptionException | SfRecoveryException expected) {
                assertArrayEquals(bytes, Files.readAllBytes(segment));
            }
        }
    }

    private static void copyArchive(String archive, String working) throws Exception {
        Path target = Files.createDirectory(Paths.get(working));
        try (java.util.stream.Stream<Path> files = Files.list(Paths.get(archive))) {
            for (Path file : (Iterable<Path>) files::iterator) {
                Files.copy(file, target.resolve(file.getFileName()));
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
