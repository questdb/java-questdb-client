/*******************************************************************************
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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.std.Crc32c;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Crash-safe boundary record for an SF segment chain. Two fixed-size,
 * independently CRC-protected records at offsets 0 and 4096 alternate on
 * update. Recovery selects the valid record with the greatest generation.
 * The separate 4 KiB slots prevent one aligned 512-byte or 4 KiB sector tear
 * from erasing both the update and the previous committed boundary.
 */
final class SfManifest implements QuietCloseable {
    static final String FILE_NAME = "sf-manifest.bin";
    private static final Logger LOG = LoggerFactory.getLogger(SfManifest.class);
    private static final int CRC_OFFSET = 60;
    private static final long FILE_SIZE = 8 * 1024;
    private static final int MAGIC = 0x314d4653; // SFM1 little-endian
    private static final int RECORD_SIZE = 64;
    private static final int RECORD_SLOT_SIZE = 4 * 1024;
    private static final int VERSION = 1;
    private final int fd;
    private final FilesFacade filesFacade;
    private final String path;
    // Preallocated record scratch for update(): the trim path calls update()
    // once per batch and must not malloc/free per call. Guarded by this
    // object's monitor (update() and close() are both synchronized).
    private final long writeScratch;
    private long activeBase;
    private boolean closed;
    private long generation;
    private long headBase;

    private SfManifest(FilesFacade filesFacade, String path, int fd,
                       long generation, long headBase, long activeBase) {
        this.filesFacade = filesFacade;
        this.path = path;
        this.fd = fd;
        this.generation = generation;
        this.headBase = headBase;
        this.activeBase = activeBase;
        this.writeScratch = Unsafe.malloc(RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    static SfManifest create(FilesFacade filesFacade, String dir, long headBase, long activeBase) {
        String path = dir + "/" + FILE_NAME;
        int fd = filesFacade.openRWExclusive(path);
        if (fd < 0) {
            throw new MmapSegmentException("exclusive create failed for SF manifest " + path);
        }
        boolean success = false;
        SfManifest manifest = null;
        try {
            if (!filesFacade.allocate(fd, FILE_SIZE)) {
                throw new MmapSegmentException("could not allocate SF manifest " + path);
            }
            manifest = new SfManifest(filesFacade, path, fd, 0, -1, -1);
            manifest.update(headBase, activeBase);
            if (filesFacade.fsyncDir(dir) != 0) {
                throw new MmapSegmentException("could not sync SF manifest directory " + dir);
            }
            success = true;
            return manifest;
        } finally {
            if (!success) {
                if (manifest != null) {
                    // close() frees the constructor-owned scratch buffer as
                    // well as the fd; closing only the raw fd would leak it.
                    manifest.close();
                } else {
                    filesFacade.close(fd);
                }
                filesFacade.remove(path);
            }
        }
    }

    static SfManifest open(FilesFacade filesFacade, String dir) {
        String path = dir + "/" + FILE_NAME;
        if (!filesFacade.exists(path)) {
            return null;
        }
        if (filesFacade.length(path) != FILE_SIZE) {
            // A wrong-sized manifest is creation debris: create() reaches the
            // full FILE_SIZE via allocate() before writing the first record,
            // so a mis-sized file proves the crash happened before any
            // boundary was ever committed — nothing can depend on it yet
            // (segment flags are stamped only after create() returns). Treat
            // as absent so startup self-heals; genuine post-creation loss is
            // still caught by the manifest-required flag check.
            quarantineDebris(filesFacade, path, "wrong size " + filesFacade.length(path));
            return null;
        }
        int fd = filesFacade.openRW(path);
        if (fd < 0) {
            throw new MmapSegmentException("could not open SF manifest " + path);
        }
        long buffer = Unsafe.malloc(RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Record first = readRecord(filesFacade, fd, buffer, 0);
            Record second = readRecord(filesFacade, fd, buffer, RECORD_SLOT_SIZE);
            Record selected;
            if (first == null) {
                selected = second;
            } else if (second == null || first.generation > second.generation) {
                selected = first;
            } else {
                selected = second;
            }
            if (selected == null) {
                // No valid record in either slot. create() makes the first
                // record durable (write + fsync) before returning, and every
                // later update() rewrites only ONE slot — so a torn update
                // leaves the sibling record intact. Zero valid records
                // therefore proves a creation crash, not boundary loss.
                // Self-heal by treating the file as absent. If durable state
                // DID depend on a manifest (flags stamped, i.e. double-slot
                // bit rot), recovery still fails closed on the
                // manifest-required flag check.
                filesFacade.close(fd);
                // Ownership released: quarantineDebris may throw (when both
                // rename and remove fail) and the catch below must not close
                // this fd again -- the OS may already have handed the number
                // to another thread, and a double-close would silently kill
                // an unrelated descriptor.
                fd = -1;
                quarantineDebris(filesFacade, path, "no valid CRC-protected record");
                return null;
            }
            return new SfManifest(filesFacade, path, fd, selected.generation,
                    selected.headBase, selected.activeBase);
        } catch (Throwable t) {
            if (fd != -1) {
                filesFacade.close(fd);
            }
            throw t;
        } finally {
            Unsafe.free(buffer, RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    long activeBase() {
        return activeBase;
    }

    @Override
    public synchronized void close() {
        // Synchronized against update() so the scratch buffer can never be
        // freed under a concurrent writer; update() checks `closed` inside
        // the same monitor.
        if (!closed) {
            closed = true;
            Unsafe.free(writeScratch, RECORD_SIZE, MemoryTag.NATIVE_DEFAULT);
            filesFacade.close(fd);
        }
    }

    long headBase() {
        return headBase;
    }

    /**
     * Unlinks {@code dir}'s manifest file. Used when a slot is being reset to
     * the "nothing durable" state (fresh-start cleanup, close-time drain, or
     * recovery accepting a segment-less slot as empty). Returns {@code true}
     * when the file is confirmed gone (removed, or never existed).
     */
    static boolean removeFile(FilesFacade filesFacade, String dir) {
        String path = dir + "/" + FILE_NAME;
        return filesFacade.remove(path) || !filesFacade.exists(path);
    }

    synchronized void update(long newHeadBase, long newActiveBase) {
        if (closed) {
            throw new IllegalStateException("SF manifest is closed");
        }
        // Committed boundaries only ever move forward: head advances on trim,
        // active advances on rotation. Clamp instead of throwing because the
        // two writers (producer rotation, manager trim) are serialized on the
        // ring monitor but may each compute their argument from a snapshot
        // the other has already moved past — e.g. rotation reads the sealed
        // list while a trimmed-but-not-yet-removed head segment still sits in
        // it. Regressing a durable boundary would let a later crash-recovery
        // demand a segment file the trim path already unlinked (startup would
        // fail on "missing head segment") or, worse, re-expose stale files
        // below an already-committed head.
        if (generation > 0) {
            if (newHeadBase < headBase) {
                newHeadBase = headBase;
            }
            if (newActiveBase < activeBase) {
                newActiveBase = activeBase;
            }
        }
        if (newHeadBase < 0 || newActiveBase < newHeadBase) {
            throw new IllegalArgumentException("invalid SF manifest boundaries");
        }
        if (generation > 0 && headBase == newHeadBase && activeBase == newActiveBase) {
            return;
        }
        long nextGeneration = generation + 1;
        Unsafe.getUnsafe().setMemory(writeScratch, RECORD_SIZE, (byte) 0);
        Unsafe.getUnsafe().putInt(writeScratch, MAGIC);
        Unsafe.getUnsafe().putInt(writeScratch + 4, VERSION);
        Unsafe.getUnsafe().putLong(writeScratch + 8, nextGeneration);
        Unsafe.getUnsafe().putLong(writeScratch + 16, newHeadBase);
        Unsafe.getUnsafe().putLong(writeScratch + 24, newActiveBase);
        int crc = Crc32c.update(Crc32c.INIT, writeScratch, CRC_OFFSET);
        Unsafe.getUnsafe().putInt(writeScratch + CRC_OFFSET, crc);
        long offset = (nextGeneration & 1L) * RECORD_SLOT_SIZE;
        if (filesFacade.write(fd, writeScratch, RECORD_SIZE, offset) != RECORD_SIZE) {
            throw new MmapSegmentException("short write updating SF manifest " + path);
        }
        if (filesFacade.fsync(fd) != 0) {
            throw new MmapSegmentException("could not sync SF manifest " + path);
        }
        generation = nextGeneration;
        headBase = newHeadBase;
        activeBase = newActiveBase;
    }

    /**
     * Moves creation-crash debris aside so a subsequent exclusive create can
     * succeed. Prefers rename (keeps the bytes for postmortem); falls back to
     * remove; throws when neither works — leaving the debris in place would
     * wedge every subsequent {@link #create}.
     */
    private static void quarantineDebris(FilesFacade filesFacade, String path, String reason) {
        LOG.warn("SF manifest {} is creation-crash debris ({}); quarantining and starting "
                + "from the segment files", path, reason);
        if (filesFacade.rename(path, path + ".corrupt") == 0) {
            return;
        }
        if (filesFacade.remove(path)) {
            return;
        }
        throw new MmapSegmentException("could not quarantine invalid SF manifest " + path);
    }

    private static Record readRecord(FilesFacade filesFacade, int fd, long buffer, long offset) {
        Unsafe.getUnsafe().setMemory(buffer, RECORD_SIZE, (byte) 0);
        if (filesFacade.read(fd, buffer, RECORD_SIZE, offset) != RECORD_SIZE) {
            return null;
        }
        if (Unsafe.getUnsafe().getInt(buffer) != MAGIC
                || Unsafe.getUnsafe().getInt(buffer + 4) != VERSION) {
            return null;
        }
        int expected = Unsafe.getUnsafe().getInt(buffer + CRC_OFFSET);
        int actual = Crc32c.update(Crc32c.INIT, buffer, CRC_OFFSET);
        if (expected != actual) {
            return null;
        }
        long generation = Unsafe.getUnsafe().getLong(buffer + 8);
        long headBase = Unsafe.getUnsafe().getLong(buffer + 16);
        long activeBase = Unsafe.getUnsafe().getLong(buffer + 24);
        if (generation <= 0 || headBase < 0 || activeBase < headBase) {
            return null;
        }
        return new Record(generation, headBase, activeBase);
    }

    private static final class Record {
        private final long activeBase;
        private final long generation;
        private final long headBase;

        private Record(long generation, long headBase, long activeBase) {
            this.generation = generation;
            this.headBase = headBase;
            this.activeBase = activeBase;
        }
    }
}
