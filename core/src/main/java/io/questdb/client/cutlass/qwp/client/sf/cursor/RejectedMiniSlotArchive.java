/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;

/** Writes immutable rejected ranges in the existing SFA replay format. */
public final class RejectedMiniSlotArchive {
    public static final String METADATA_FILE_NAME = "rejection.properties";
    public static final String SEGMENT_FILE_NAME = "rejected.sfa";
    private static final int MODE_OWNER_ONLY = 448; // 0700

    private final String directory;
    private final FilesFacade ff;
    private final String namespace;
    // A published directory awaiting its parent fsync. Retry only that barrier.
    private Result pending;

    public RejectedMiniSlotArchive(FilesFacade ff, String directory) {
        this(ff, directory, namespaceForSource(directory));
    }

    public RejectedMiniSlotArchive(FilesFacade ff, String directory, String namespace) {
        this.ff = ff;
        this.directory = directory;
        this.namespace = UUID.fromString(namespace).toString();
    }

    /** Stable for a disk queue path; unique for each memory-only queue. */
    public static String namespaceForSource(String source) {
        if (source == null) return UUID.randomUUID().toString();
        String path = Paths.get(source).toAbsolutePath().normalize().toString();
        return UUID.nameUUIDFromBytes(path.getBytes(StandardCharsets.UTF_8)).toString();
    }

    /** Returns a structurally valid report for exactly this live range, if one was published. */
    public SenderError findRecoveredOrphanReport(CursorSendEngine engine, long fromFsn, long toFsn) {
        if (fromFsn < 0 || toFsn < fromFsn) return null;
        try {
            PathsForRange paths = paths(engine, fromFsn, toFsn);
            removeKnownDirectory(paths.temp);
            return readReport(Paths.get(paths.completed), fromFsn, toFsn);
        } catch (IOException | RuntimeException ignored) {
            // Archive output must never prevent live-queue recovery.
            return null;
        }
    }

    /** I/O-thread only. Source frames remain live until this returns successfully. */
    public Result preserve(CursorSendEngine engine, SenderError error,
                           byte[] dictionaryEntries, int dictionaryCount) {
        if (pending == null) pending = write(engine, error, dictionaryEntries, dictionaryCount);
        if (ff.fsyncDir(directory + "/rejected") != 0) {
            throw new SfOperationalException("could not sync rejection archive parent " + directory);
        }
        Result result = pending;
        pending = null;
        return result;
    }

    private Result write(CursorSendEngine engine, SenderError error,
                         byte[] dictionaryEntries, int dictionaryCount) {
        if ((dictionaryEntries == null) != (dictionaryCount == 0)) {
            throw new IllegalArgumentException("dictionary snapshot bytes/count mismatch");
        }
        long from = error.getFromFsn();
        long to = error.getToFsn();
        if (from < 0 || to < from || error.getRejectedFsn() < from || error.getRejectedFsn() > to) {
            throw new IllegalArgumentException("invalid rejection span");
        }
        String rejectedRoot = directory + "/rejected";
        ensureDirectory(rejectedRoot);
        if (ff.fsyncDir(directory) != 0) {
            throw new SfOperationalException("could not sync archive destination " + directory);
        }
        PathsForRange paths = paths(engine, from, to);
        try {
            if (readReport(Paths.get(paths.completed), from, to) != null) {
                return new Result(paths.completed, 0, true);
            }
        } catch (IOException | RuntimeException ignored) {
            // Replace only this exact range identity while its source is still live.
        }
        removeKnownDirectory(paths.completed);
        removeKnownDirectory(paths.temp);
        if (ff.exists(paths.completed) || ff.exists(paths.temp)
                || ff.mkdir(paths.temp, MODE_OWNER_ONLY) != 0) {
            throw new SfOperationalException("could not create archive staging directory " + paths.temp);
        }

        boolean published = false;
        try {
            int maxPayload = 0;
            long totalSize = MmapSegment.HEADER_SIZE;
            for (long fsn = from; fsn <= to; fsn++) {
                int len = engine.liveFramePayloadLength(fsn);
                if (len < QwpConstants.HEADER_SIZE) {
                    throw new SfOperationalException("rejection frame is no longer live [fsn=" + fsn + ']');
                }
                totalSize = Math.addExact(totalSize, MmapSegment.FRAME_HEADER_SIZE + (long) len);
                maxPayload = Math.max(maxPayload, len);
                if (fsn == Long.MAX_VALUE) break;
            }
            copyFrames(engine, paths.temp, from, to, totalSize, maxPayload);
            try (SfManifest ignored = SfManifest.create(ff, paths.temp, from, from)) {
                // create() durably writes the sole boundary record.
            }
            try (AckWatermark watermark = AckWatermark.open(ff, paths.temp)) {
                if (watermark == null) throw new SfOperationalException("could not create rejection ack watermark");
                watermark.write(from - 1L);
                watermark.sync();
            }
            if (dictionaryEntries != null) writeDictionary(paths.temp, dictionaryEntries, dictionaryCount);
            writeMetadata(paths.temp, error, dictionaryEntries != null);
            Result result = new Result(paths.completed,
                    occupiedBytes(paths.temp, dictionaryEntries != null), false);
            if (ff.fsyncDir(paths.temp) != 0 || ff.rename(paths.temp, paths.completed) != 0) {
                throw new SfOperationalException("could not publish rejected mini-slot " + paths.completed);
            }
            published = true;
            return result;
        } finally {
            if (!published) removeKnownDirectory(paths.temp);
        }
    }

    private void copyFrames(CursorSendEngine engine, String target, long from, long to,
                            long totalSize, int maxPayload) {
        long scratch = Unsafe.malloc(maxPayload, MemoryTag.NATIVE_DEFAULT);
        try (MmapSegment segment = MmapSegment.create(
                ff, target + '/' + SEGMENT_FILE_NAME, from, totalSize, true)) {
            for (long fsn = from; fsn <= to; fsn++) {
                int len = engine.liveFramePayloadLength(fsn);
                if (len < 0 || len > maxPayload || !engine.copyLiveFrame(fsn, scratch, maxPayload)) {
                    throw new SfOperationalException("rejection frame disappeared during copy [fsn=" + fsn + ']');
                }
                if (fsn == to) {
                    long flags = scratch + QwpConstants.HEADER_OFFSET_FLAGS;
                    Unsafe.getUnsafe().putByte(flags, (byte) (Unsafe.getUnsafe().getByte(flags)
                            & ~QwpConstants.FLAG_DEFER_COMMIT));
                }
                if (segment.tryAppend(scratch, len) < 0) {
                    throw new SfOperationalException("rejection segment sizing changed during copy");
                }
                if (fsn == Long.MAX_VALUE) break;
            }
            segment.syncPublished();
        } finally {
            Unsafe.free(scratch, maxPayload, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void writeDictionary(String target, byte[] entries, int count) {
        long address = Unsafe.malloc(entries.length, MemoryTag.NATIVE_DEFAULT);
        try (PersistedSymbolDict dictionary = PersistedSymbolDict.openClean(ff, target)) {
            if (dictionary == null) throw new SfOperationalException("could not create rejected mini-slot dictionary");
            Unsafe.getUnsafe().copyMemory(entries, Unsafe.BYTE_OFFSET, null, address, entries.length);
            dictionary.appendRawEntries(address, entries.length, count);
        } finally {
            Unsafe.free(address, entries.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private SenderError readReport(Path archive, long fromFsn, long toFsn) throws IOException {
        LinkOption[] noFollow = {LinkOption.NOFOLLOW_LINKS};
        Path metadataFile = archive.resolve(METADATA_FILE_NAME);
        if (!java.nio.file.Files.isDirectory(archive, noFollow)
                || !java.nio.file.Files.isRegularFile(metadataFile, noFollow)
                || java.nio.file.Files.size(metadataFile) > 64 * 1024L) return null;
        Properties metadata = new Properties();
        try (InputStream input = java.nio.file.Files.newInputStream(metadataFile)) {
            metadata.load(input);
        }
        if (!"1".equals(metadata.getProperty("version"))
                || Long.parseLong(metadata.getProperty("fromFsn")) != fromFsn
                || Long.parseLong(metadata.getProperty("toFsn")) != toFsn
                || !SenderError.Category.SCHEMA_MISMATCH.name().equals(metadata.getProperty("category"))
                || !SenderError.Policy.REJECT_AND_CONTINUE.name().equals(metadata.getProperty("policy"))) return null;
        long rejectedFsn = Long.parseLong(metadata.getProperty("rejectedFsn"));
        if (rejectedFsn < fromFsn || rejectedFsn > toFsn) return null;
        String dictionary = metadata.getProperty("dictionary");
        if (!("true".equals(dictionary) || "false".equals(dictionary))) return null;
        String dir = archive.toString();
        if (Boolean.parseBoolean(dictionary) != ff.exists(dir + '/' + PersistedSymbolDict.FILE_NAME)) return null;
        try (MmapSegment segment = MmapSegment.openExisting(ff, dir + '/' + SEGMENT_FILE_NAME);
             SfManifest manifest = SfManifest.open(ff, dir);
             AckWatermark watermark = AckWatermark.open(ff, dir)) {
            if (segment.baseSeq() != fromFsn || segment.frameCount() != toFsn - fromFsn + 1L
                    || manifest == null || manifest.headBase() != fromFsn || manifest.activeBase() != fromFsn
                    || watermark == null || watermark.read() != fromFsn - 1L) return null;
        }
        if (Boolean.parseBoolean(dictionary)) {
            try (PersistedSymbolDict persisted = PersistedSymbolDict.open(ff, dir)) {
                if (persisted == null) return null;
            }
        }
        String detected = metadata.getProperty("detectedAtNanos");
        return new SenderError(SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.REJECT_AND_CONTINUE,
                Integer.parseInt(metadata.getProperty("status")), metadata.getProperty("message"),
                rejectedFsn, rejectedFsn, rejectedFsn, metadata.getProperty("table"),
                detected == null ? 0L : Long.parseLong(detected))
                .withRejectionSpan(fromFsn, toFsn).withRejectedPath(dir);
    }

    private void writeMetadata(String dir, SenderError error, boolean dictionary) {
        Properties metadata = new Properties();
        metadata.setProperty("version", "1");
        metadata.setProperty("fromFsn", Long.toString(error.getFromFsn()));
        metadata.setProperty("toFsn", Long.toString(error.getToFsn()));
        metadata.setProperty("rejectedFsn", Long.toString(error.getRejectedFsn()));
        metadata.setProperty("status", Integer.toString(error.getServerStatusByte()));
        metadata.setProperty("detectedAtNanos", Long.toString(error.getDetectedAtNanos()));
        metadata.setProperty("category", error.getCategory().name());
        metadata.setProperty("policy", error.getAppliedPolicy().name());
        metadata.setProperty("dictionary", Boolean.toString(dictionary));
        if (error.getTableName() != null) metadata.setProperty("table", error.getTableName());
        if (error.getServerMessage() != null) metadata.setProperty("message", error.getServerMessage());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            metadata.store(output, "Preserved schema rejection; replay a working copy after fixing the schema");
        } catch (IOException e) {
            throw new SfOperationalException("could not encode rejection metadata", e);
        }
        byte[] bytes = output.toByteArray();
        long address = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        int fd = -1;
        try {
            Unsafe.getUnsafe().copyMemory(bytes, Unsafe.BYTE_OFFSET, null, address, bytes.length);
            fd = ff.openRWExclusive(dir + '/' + METADATA_FILE_NAME);
            if (fd < 0 || !ff.allocate(fd, bytes.length)
                    || ff.write(fd, address, bytes.length, 0) != bytes.length || ff.fsync(fd) != 0) {
                throw new SfOperationalException("could not write rejection metadata " + dir);
            }
        } finally {
            if (fd >= 0) ff.close(fd);
            Unsafe.free(address, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private PathsForRange paths(CursorSendEngine engine, long fromFsn, long toFsn) {
        MmapSegment source = engine.findSegmentContaining(fromFsn);
        if (source == null) throw new SfOperationalException("rejection range is no longer live");
        String name = namespace + "-seg-" + source.generationToken() + "-fsn-" + fromFsn + '-' + toFsn;
        String root = directory + "/rejected/";
        return new PathsForRange(root + name, root + ".tmp-" + name);
    }

    private void removeKnownDirectory(String dir) {
        Path path = Paths.get(dir);
        if (!java.nio.file.Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) return;
        String[] names = {SEGMENT_FILE_NAME, SfManifest.FILE_NAME, AckWatermark.FILE_NAME,
                PersistedSymbolDict.FILE_NAME, METADATA_FILE_NAME};
        for (String name : names) ff.remove(dir + '/' + name);
        // Unknown contents keep the directory in place rather than broadening deletion scope.
        ff.remove(dir);
    }

    private void ensureDirectory(String dir) {
        if (!ff.exists(dir) && ff.mkdir(dir, MODE_OWNER_ONLY) != 0) {
            throw new SfOperationalException("could not create directory " + dir);
        }
    }

    private long occupiedBytes(String dir, boolean dictionary) {
        long n = ff.length(dir + '/' + SEGMENT_FILE_NAME) + ff.length(dir + '/' + SfManifest.FILE_NAME)
                + ff.length(dir + '/' + AckWatermark.FILE_NAME) + ff.length(dir + '/' + METADATA_FILE_NAME);
        return dictionary ? n + ff.length(dir + '/' + PersistedSymbolDict.FILE_NAME) : n;
    }

    public static void probeDestination(FilesFacade ff, String slotDir) {
        probeDirectory(ff, slotDir + "/rejected");
    }

    public static void probeDirectory(FilesFacade ff, String directory) {
        if (!ff.exists(directory) && ff.mkdir(directory, MODE_OWNER_ONLY) != 0) {
            throw new SfOperationalException("could not create schema preservation destination " + directory);
        }
        String probe = directory + "/.probe-" + UUID.randomUUID();
        int fd = ff.openRWExclusive(probe);
        try {
            if (fd < 0 || ff.fsync(fd) != 0) {
                throw new SfOperationalException("schema preservation destination is not writable " + directory);
            }
        } finally {
            if (fd >= 0) ff.close(fd);
            ff.remove(probe);
        }
        if (ff.fsyncDir(directory) != 0) {
            throw new SfOperationalException("could not sync schema preservation destination " + directory);
        }
    }

    public static final class Result {
        public final long bytesWritten;
        public final String path;
        public final boolean reused;

        Result(String path, long bytesWritten, boolean reused) {
            this.path = path;
            this.bytesWritten = bytesWritten;
            this.reused = reused;
        }
    }

    private static final class PathsForRange {
        final String completed;
        final String temp;

        PathsForRange(String completed, String temp) {
            this.completed = completed;
            this.temp = temp;
        }
    }
}
