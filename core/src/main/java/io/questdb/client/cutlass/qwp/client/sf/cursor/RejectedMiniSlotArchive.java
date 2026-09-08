/*******************************************************************************
 * Copyright (c) 2014-2026 QuestDB
 * Licensed under the Apache License, Version 2.0.
 ******************************************************************************/

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Builds and validates immutable, replay-format copies of rejected SF spans. */
public final class RejectedMiniSlotArchive {
    public static final String METADATA_FILE_NAME = "rejection-meta.bin";
    public static final String SEGMENT_FILE_NAME = "rejected.sfa";
    private static final int METADATA_MAGIC = 0x314a4552; // REJ1 little-endian
    private static final int METADATA_VERSION = 1;
    private static final int MODE_OWNER_ONLY = 448; // 0700

    private RejectedMiniSlotArchive() {
    }

    public static Result preserve(
            FilesFacade ff,
            CursorSendEngine engine,
            PersistedSymbolDict dictionary,
            String slotDir,
            String slotId,
            String epoch,
            SenderError error
    ) {
        return preserve0(ff, engine, dictionary, null, 0, slotDir, slotId, epoch, error);
    }

    public static Result preserveSnapshot(
            FilesFacade ff,
            CursorSendEngine engine,
            byte[] dictionaryEntries,
            int dictionaryCount,
            String slotDir,
            String slotId,
            String epoch,
            SenderError error
    ) {
        if ((dictionaryEntries == null) != (dictionaryCount == 0)) {
            throw new IllegalArgumentException("dictionary snapshot bytes/count mismatch");
        }
        return preserve0(ff, engine, null, dictionaryEntries, dictionaryCount,
                slotDir, slotId, epoch, error);
    }

    private static Result preserve0(
            FilesFacade ff,
            CursorSendEngine engine,
            PersistedSymbolDict dictionary,
            byte[] dictionaryEntries,
            int dictionaryCount,
            String slotDir,
            String slotId,
            String epoch,
            SenderError error
    ) {
        long from = error.getFromFsn();
        long to = error.getToFsn();
        if (from < 0 || to < from || error.getRejectedFsn() < from || error.getRejectedFsn() > to) {
            throw new IllegalArgumentException("invalid rejection span");
        }
        requirePathComponent(slotId, "slot id");
        UUID.fromString(epoch);
        String rejectedRoot = slotDir + "/rejected";
        ensureDirectory(ff, rejectedRoot);
        String identity = slotId + '-' + epoch + "-fsn-" + from + '-' + to;
        String finalDir = rejectedRoot + '/' + identity;
        Metadata expected = Metadata.from(slotId, epoch, error,
                dictionary != null || dictionaryEntries != null);
        if (ff.exists(finalDir)) {
            validate(ff, finalDir, expected);
            // Completes a previous publication whose rename succeeded but
            // whose parent-directory barrier failed transiently.
            if (ff.fsyncDir(rejectedRoot) != 0) {
                throw new SfOperationalException("could not sync rejected mini-slot parent " + rejectedRoot);
            }
            return new Result(finalDir, occupiedBytes(ff, finalDir, expected.hasDictionary), true);
        }

        String tempDir = rejectedRoot + "/.tmp-" + identity + '-' + UUID.randomUUID();
        ensureDirectory(ff, tempDir);
        long totalSize = MmapSegment.HEADER_SIZE;
        int maxPayload = 0;
        for (long fsn = from; fsn <= to; fsn++) {
            int len = engine.liveFramePayloadLength(fsn);
            if (len < QwpConstants.HEADER_SIZE) {
                throw new SfOperationalException("rejection frame is no longer live [fsn=" + fsn + ']');
            }
            totalSize = Math.addExact(totalSize, MmapSegment.FRAME_HEADER_SIZE + (long) len);
            maxPayload = Math.max(maxPayload, len);
            if (fsn == Long.MAX_VALUE) break;
        }

        long scratch = Unsafe.malloc(maxPayload, MemoryTag.NATIVE_DEFAULT);
        try (MmapSegment segment = MmapSegment.create(
                ff, tempDir + '/' + SEGMENT_FILE_NAME, from, totalSize, true)) {
            for (long fsn = from; fsn <= to; fsn++) {
                int len = engine.liveFramePayloadLength(fsn);
                if (len < 0 || len > maxPayload || !engine.copyLiveFrame(fsn, scratch, maxPayload)) {
                    throw new SfOperationalException("rejection frame disappeared during copy [fsn=" + fsn + ']');
                }
                if (fsn == to) {
                    long flagsAddr = scratch + QwpConstants.HEADER_OFFSET_FLAGS;
                    byte flags = Unsafe.getUnsafe().getByte(flagsAddr);
                    Unsafe.getUnsafe().putByte(flagsAddr, (byte) (flags & ~QwpConstants.FLAG_DEFER_COMMIT));
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

        try (SfManifest ignored = SfManifest.create(ff, tempDir, from, from)) {
            // create() durably writes the sole boundary record.
        }
        try (AckWatermark watermark = AckWatermark.open(ff, tempDir)) {
            if (watermark == null) {
                throw new SfOperationalException("could not create rejection ack watermark");
            }
            watermark.write(from - 1L);
            watermark.sync();
        }
        if (dictionary != null) {
            dictionary.snapshotTo(tempDir);
        } else if (dictionaryEntries != null) {
            long entriesAddr = Unsafe.malloc(dictionaryEntries.length, MemoryTag.NATIVE_DEFAULT);
            try (PersistedSymbolDict snapshot = PersistedSymbolDict.openClean(ff, tempDir)) {
                if (snapshot == null) {
                    throw new SfOperationalException("could not create rejected mini-slot dictionary");
                }
                Unsafe.getUnsafe().copyMemory(dictionaryEntries, Unsafe.BYTE_OFFSET, null,
                        entriesAddr, dictionaryEntries.length);
                snapshot.appendRawEntries(entriesAddr, dictionaryEntries.length, dictionaryCount);
            } finally {
                Unsafe.free(entriesAddr, dictionaryEntries.length, MemoryTag.NATIVE_DEFAULT);
            }
        }
        writeMetadata(ff, tempDir, expected);
        if (ff.fsyncDir(tempDir) != 0 || ff.rename(tempDir, finalDir) != 0
                || ff.fsyncDir(rejectedRoot) != 0) {
            throw new SfOperationalException("could not publish rejected mini-slot " + finalDir);
        }
        validate(ff, finalDir, expected);
        return new Result(finalDir, occupiedBytes(ff, finalDir, expected.hasDictionary), false);
    }

    /**
     * Copies a validated archive into a new working directory. Replay may
     * consume that directory; the immutable archive is never adopted or moved.
     */
    public static void copyToWorkingDirectory(FilesFacade ff, String archiveDir, String workingDir) {
        Metadata metadata = readMetadata(ff, archiveDir);
        validate(ff, archiveDir, metadata);
        if (ff.exists(workingDir)) {
            throw new IllegalArgumentException("working directory already exists: " + workingDir);
        }
        ensureDirectory(ff, workingDir);
        copyFile(ff, archiveDir, workingDir, SEGMENT_FILE_NAME);
        copyFile(ff, archiveDir, workingDir, SfManifest.FILE_NAME);
        copyFile(ff, archiveDir, workingDir, AckWatermark.FILE_NAME);
        copyFile(ff, archiveDir, workingDir, METADATA_FILE_NAME);
        if (metadata.hasDictionary) {
            copyFile(ff, archiveDir, workingDir, PersistedSymbolDict.FILE_NAME);
        }
        if (ff.fsyncDir(workingDir) != 0) {
            throw new SfOperationalException("could not sync replay working directory " + workingDir);
        }
    }

    public static Metadata readMetadata(FilesFacade ff, String dir) {
        String path = dir + '/' + METADATA_FILE_NAME;
        long len = ff.length(path);
        if (len < 76 || len > Integer.MAX_VALUE) {
            throw new UnreplayableSlotException("invalid rejection metadata size " + path);
        }
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        int fd = ff.openRW(path);
        try {
            if (fd < 0 || ff.read(fd, mem, len, 0) != len) {
                throw new SfOperationalException("could not read rejection metadata " + path);
            }
            int storedCrc = Unsafe.getUnsafe().getInt(mem + len - 4);
            if (storedCrc != Crc32c.update(Crc32c.INIT, mem, len - 4)) {
                throw new UnreplayableSlotException("rejection metadata CRC mismatch " + path);
            }
            long p = mem;
            if (Unsafe.getUnsafe().getInt(p) != METADATA_MAGIC
                    || Unsafe.getUnsafe().getInt(p + 4) != METADATA_VERSION) {
                throw new UnreplayableSlotException("unsupported rejection metadata " + path);
            }
            p += 8;
            long rejected = Unsafe.getUnsafe().getLong(p); p += 8;
            long from = Unsafe.getUnsafe().getLong(p); p += 8;
            long to = Unsafe.getUnsafe().getLong(p); p += 8;
            long detected = Unsafe.getUnsafe().getLong(p); p += 8;
            int status = Unsafe.getUnsafe().getInt(p); p += 4;
            boolean hasDictionary = Unsafe.getUnsafe().getInt(p) != 0; p += 4;
            String slotId = readString(mem, len - 4, p); p += 4 + utf8LengthAt(mem, p);
            String epoch = readString(mem, len - 4, p); p += 4 + utf8LengthAt(mem, p);
            String category = readString(mem, len - 4, p); p += 4 + utf8LengthAt(mem, p);
            String policy = readString(mem, len - 4, p); p += 4 + utf8LengthAt(mem, p);
            String table = readString(mem, len - 4, p); p += 4 + utf8LengthAt(mem, p);
            String message = readString(mem, len - 4, p); p += 4 + utf8LengthAt(mem, p);
            if (p != mem + len - 4) {
                throw new UnreplayableSlotException("trailing rejection metadata bytes " + path);
            }
            return new Metadata(slotId, epoch, rejected, from, to, detected, status,
                    category, policy, table, message, hasDictionary);
        } finally {
            if (fd >= 0) ff.close(fd);
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Finds the preserved report for a recovered orphan tail. Only completed
     * directories belonging to this slot epoch are considered; temporary
     * directories are never evidence.
     */
    public static SenderError findOverlapping(
            FilesFacade ff, String slotDir, String slotId, String epoch, long fromFsn, long toFsn
    ) {
        requirePathComponent(slotId, "slot id");
        UUID.fromString(epoch);
        String rejectedRoot = slotDir + "/rejected";
        long find = ff.findFirst(rejectedRoot);
        if (find <= 0) {
            if (find > 0) ff.findClose(find);
            return null;
        }
        String prefix = slotId + '-' + epoch + "-fsn-";
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(ff.findName(find));
                int type = ff.findType(find);
                rc = ff.findNext(find);
                if (type != Files.DT_DIR || name == null || !name.startsWith(prefix)) continue;
                int separator = name.indexOf('-', prefix.length());
                if (separator < 0) continue;
                long archiveFrom;
                long archiveTo;
                try {
                    archiveFrom = Long.parseLong(name.substring(prefix.length(), separator));
                    archiveTo = Long.parseLong(name.substring(separator + 1));
                } catch (NumberFormatException e) {
                    continue;
                }
                // Completed archives have canonical range names. Filter before
                // opening metadata: a damaged, already-drained archive is not
                // evidence about this recovered tail and must not block startup.
                if (archiveFrom < 0 || archiveTo < archiveFrom
                        || !name.equals(prefix + archiveFrom + '-' + archiveTo)
                        || archiveTo < fromFsn || archiveFrom > toFsn) {
                    continue;
                }
                String path = rejectedRoot + '/' + name;
                Metadata metadata = readMetadata(ff, path);
                if (!slotId.equals(metadata.slotId) || !epoch.equals(metadata.epoch)
                        || metadata.fromFsn != archiveFrom || metadata.toFsn != archiveTo) {
                    throw new UnreplayableSlotException("rejected mini-slot directory identity mismatch " + path);
                }
                validate(ff, path, metadata);
                SenderError error = new SenderError(
                        SenderError.Category.valueOf(metadata.category),
                        SenderError.Policy.valueOf(metadata.policy), metadata.status,
                        metadata.message, metadata.rejectedFsn, metadata.rejectedFsn,
                        metadata.rejectedFsn, metadata.table.isEmpty() ? null : metadata.table,
                        metadata.detectedAtNanos);
                return error.withRejectionSpan(metadata.fromFsn, metadata.toFsn)
                        .withRejectedPath(path);
            }
        } finally {
            ff.findClose(find);
        }
        return null;
    }

    /**
     * Removes incomplete publications for exactly one slot epoch. The caller
     * must hold that queue's exclusive lifecycle lock; the identity prefix is
     * what keeps shared memory-sender destinations from touching one another.
     */
    public static void cleanupTemporaryDirectories(
            FilesFacade ff, String slotDir, String slotId, String epoch
    ) {
        requirePathComponent(slotId, "slot id");
        UUID.fromString(epoch);
        String rejectedRoot = slotDir + "/rejected";
        long find = ff.findFirst(rejectedRoot);
        if (find <= 0) return;
        String prefix = ".tmp-" + slotId + '-' + epoch + "-fsn-";
        java.util.ArrayList<String> candidates = new java.util.ArrayList<>();
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(ff.findName(find));
                int type = ff.findType(find);
                rc = ff.findNext(find);
                if (type == Files.DT_DIR && name != null && name.startsWith(prefix)) {
                    candidates.add(rejectedRoot + '/' + name);
                }
            }
        } finally {
            ff.findClose(find);
        }
        for (String candidate : candidates) {
            removeKnownTemporaryContents(ff, candidate);
        }
        if (!candidates.isEmpty() && ff.fsyncDir(rejectedRoot) != 0) {
            throw new SfOperationalException("could not sync rejected temporary cleanup " + rejectedRoot);
        }
    }

    private static void removeKnownTemporaryContents(FilesFacade ff, String dir) {
        String[] names = {SEGMENT_FILE_NAME, SfManifest.FILE_NAME, AckWatermark.FILE_NAME,
                PersistedSymbolDict.FILE_NAME, METADATA_FILE_NAME};
        for (String name : names) ff.remove(dir + '/' + name);
        // remove() maps to unlink/rmdir. It deliberately fails if an unknown
        // file appeared, preserving rather than broadening deletion scope.
        ff.remove(dir);
    }

    private static void validate(FilesFacade ff, String dir, Metadata expected) {
        Metadata actual = readMetadata(ff, dir);
        if (!expected.sameIdentity(actual)) {
            throw new UnreplayableSlotException("rejected mini-slot identity mismatch " + dir);
        }
        try (MmapSegment segment = MmapSegment.openExisting(ff, dir + '/' + SEGMENT_FILE_NAME);
             SfManifest manifest = SfManifest.open(ff, dir);
             AckWatermark watermark = AckWatermark.open(ff, dir)) {
            if (segment.baseSeq() != actual.fromFsn
                    || segment.frameCount() != actual.toFsn - actual.fromFsn + 1
                    || manifest == null || manifest.headBase() != actual.fromFsn
                    || manifest.activeBase() != actual.fromFsn
                    || watermark == null || watermark.read() != actual.fromFsn - 1L) {
                throw new UnreplayableSlotException("invalid rejected mini-slot boundaries " + dir);
            }
        } catch (MmapSegmentCorruptionException e) {
            // Positively identified archive corruption is a terminal recovery
            // verdict too. Operational read/mmap failures retain their type.
            UnreplayableSlotException failure = new UnreplayableSlotException(
                    "corrupt rejected mini-slot " + dir + ": " + e.getMessage());
            failure.initCause(e);
            throw failure;
        }
        if (actual.hasDictionary) {
            try (PersistedSymbolDict ignored = PersistedSymbolDict.open(ff, dir)) {
                if (ignored == null) {
                    throw new UnreplayableSlotException("missing rejected mini-slot dictionary " + dir);
                }
            }
        }
    }

    private static void writeMetadata(FilesFacade ff, String dir, Metadata metadata) {
        byte[] slot = metadata.slotId.getBytes(StandardCharsets.UTF_8);
        byte[] epoch = metadata.epoch.getBytes(StandardCharsets.UTF_8);
        byte[] category = metadata.category.getBytes(StandardCharsets.UTF_8);
        byte[] policy = metadata.policy.getBytes(StandardCharsets.UTF_8);
        byte[] table = bytes(metadata.table);
        byte[] message = bytes(metadata.message);
        int len = 48 + 4 + slot.length + 4 + epoch.length + 4 + category.length
                + 4 + policy.length + 4 + table.length + 4 + message.length + 4;
        long mem = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        int fd = -1;
        String path = dir + '/' + METADATA_FILE_NAME;
        try {
            Unsafe.getUnsafe().setMemory(mem, len, (byte) 0);
            long p = mem;
            Unsafe.getUnsafe().putInt(p, METADATA_MAGIC); Unsafe.getUnsafe().putInt(p + 4, METADATA_VERSION); p += 8;
            Unsafe.getUnsafe().putLong(p, metadata.rejectedFsn); p += 8;
            Unsafe.getUnsafe().putLong(p, metadata.fromFsn); p += 8;
            Unsafe.getUnsafe().putLong(p, metadata.toFsn); p += 8;
            Unsafe.getUnsafe().putLong(p, metadata.detectedAtNanos); p += 8;
            Unsafe.getUnsafe().putInt(p, metadata.status); p += 4;
            Unsafe.getUnsafe().putInt(p, metadata.hasDictionary ? 1 : 0); p += 4;
            p = writeString(p, slot); p = writeString(p, epoch); p = writeString(p, category);
            p = writeString(p, policy); p = writeString(p, table); p = writeString(p, message);
            Unsafe.getUnsafe().putInt(p, Crc32c.update(Crc32c.INIT, mem, len - 4));
            fd = ff.openRWExclusive(path);
            if (fd < 0 || !ff.allocate(fd, len) || ff.write(fd, mem, len, 0) != len || ff.fsync(fd) != 0) {
                throw new SfOperationalException("could not write rejection metadata " + path);
            }
        } finally {
            if (fd >= 0) ff.close(fd);
            Unsafe.free(mem, len, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void copyFile(FilesFacade ff, String fromDir, String toDir, String name) {
        String source = fromDir + '/' + name;
        String target = toDir + '/' + name;
        long len = ff.length(source);
        if (len < 0) throw new SfOperationalException("missing archive file " + source);
        int in = ff.openRW(source);
        int out = ff.openRWExclusive(target);
        long mem = Unsafe.malloc(Math.min(Math.max(len, 1), 64 * 1024), MemoryTag.NATIVE_DEFAULT);
        try {
            if (in < 0 || out < 0 || !ff.allocate(out, len)) throw new SfOperationalException("could not copy " + source);
            for (long off = 0; off < len; ) {
                long chunk = Math.min(64 * 1024L, len - off);
                if (ff.read(in, mem, chunk, off) != chunk || ff.write(out, mem, chunk, off) != chunk) {
                    throw new SfOperationalException("short copy of " + source);
                }
                off += chunk;
            }
            if (ff.fsync(out) != 0) throw new SfOperationalException("could not sync " + target);
        } finally {
            if (in >= 0) ff.close(in);
            if (out >= 0) ff.close(out);
            Unsafe.free(mem, Math.min(Math.max(len, 1), 64 * 1024), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long occupiedBytes(FilesFacade ff, String dir, boolean dict) {
        long n = ff.length(dir + '/' + SEGMENT_FILE_NAME) + ff.length(dir + '/' + SfManifest.FILE_NAME)
                + ff.length(dir + '/' + AckWatermark.FILE_NAME) + ff.length(dir + '/' + METADATA_FILE_NAME);
        return dict ? n + ff.length(dir + '/' + PersistedSymbolDict.FILE_NAME) : n;
    }

    private static void ensureDirectory(FilesFacade ff, String dir) {
        if (!ff.exists(dir) && ff.mkdir(dir, MODE_OWNER_ONLY) != 0) {
            throw new SfOperationalException("could not create directory " + dir);
        }
    }

    private static void requirePathComponent(String value, String label) {
        if (value == null || value.isEmpty() || value.indexOf('/') >= 0 || value.indexOf('\\') >= 0 || value.equals(".") || value.equals("..")) {
            throw new IllegalArgumentException("invalid " + label);
        }
    }

    private static byte[] bytes(String value) { return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8); }
    private static long writeString(long p, byte[] value) {
        Unsafe.getUnsafe().putInt(p, value.length);
        if (value.length > 0) Unsafe.getUnsafe().copyMemory(value, Unsafe.BYTE_OFFSET, null, p + 4, value.length);
        return p + 4 + value.length;
    }
    private static int utf8LengthAt(long base, long p) { return Unsafe.getUnsafe().getInt(p); }
    private static String readString(long base, long limitOffset, long p) {
        int len = Unsafe.getUnsafe().getInt(p);
        if (len < 0 || p + 4L + len > base + limitOffset) throw new UnreplayableSlotException("invalid rejection metadata string");
        byte[] bytes = new byte[len];
        if (len > 0) Unsafe.getUnsafe().copyMemory(null, p + 4, bytes, Unsafe.BYTE_OFFSET, len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static final class Result {
        public final long bytesWritten;
        public final String path;
        public final boolean reused;
        Result(String path, long bytesWritten, boolean reused) { this.path = path; this.bytesWritten = bytesWritten; this.reused = reused; }
    }

    /** Stable fields used by startup scanning to reconstruct a notification. */
    public static final class Metadata {
        public final int status;
        public final long detectedAtNanos, fromFsn, rejectedFsn, toFsn;
        public final String category, epoch, message, policy, slotId, table;
        public final boolean hasDictionary;
        Metadata(String slotId, String epoch, long rejectedFsn, long fromFsn, long toFsn,
                 long detectedAtNanos, int status, String category, String policy,
                 String table, String message, boolean hasDictionary) {
            this.slotId = slotId; this.epoch = epoch; this.rejectedFsn = rejectedFsn; this.fromFsn = fromFsn;
            this.toFsn = toFsn; this.detectedAtNanos = detectedAtNanos; this.status = status;
            this.category = category; this.policy = policy; this.table = table;
            this.message = message; this.hasDictionary = hasDictionary;
        }
        static Metadata from(String slotId, String epoch, SenderError e, boolean hasDictionary) {
            return new Metadata(slotId, epoch, e.getRejectedFsn(), e.getFromFsn(), e.getToFsn(),
                    e.getDetectedAtNanos(), e.getServerStatusByte(), e.getCategory().name(),
                    e.getAppliedPolicy().name(), emptyIfNull(e.getTableName()),
                    emptyIfNull(e.getServerMessage()), hasDictionary);
        }
        @Override public boolean equals(Object o) {
            if (!(o instanceof Metadata)) return false;
            Metadata m = (Metadata) o;
            return rejectedFsn == m.rejectedFsn && fromFsn == m.fromFsn && toFsn == m.toFsn
                    && detectedAtNanos == m.detectedAtNanos && status == m.status
                    && hasDictionary == m.hasDictionary && eq(slotId, m.slotId) && eq(epoch, m.epoch)
                    && eq(category, m.category) && eq(policy, m.policy)
                    && eq(table, m.table) && eq(message, m.message);
        }
        @Override public int hashCode() { return slotId.hashCode(); }
        private boolean sameIdentity(Metadata m) {
            return rejectedFsn == m.rejectedFsn && fromFsn == m.fromFsn && toFsn == m.toFsn
                    && category.equals(m.category) && slotId.equals(m.slotId) && epoch.equals(m.epoch)
                    && hasDictionary == m.hasDictionary;
        }
        private static boolean eq(Object a, Object b) { return a == null ? b == null : a.equals(b); }
        private static String emptyIfNull(String value) { return value == null ? "" : value; }
    }
}
