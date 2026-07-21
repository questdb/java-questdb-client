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
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persisted high-water mark for the durably-acknowledged FSN. Lives at
 * {@code <slot>/.ack-watermark} alongside the segment files and the slot
 * lock. Read at engine startup to seed {@code ackedFsn}, eliminating the
 * segment-granular re-replay of partially-acked sealed segments across
 * process restarts.
 * <p>
 * Durable acks are cumulative ({@code STATUS_DURABLE_ACK fsn=N} means
 * "everything &lt;= N is durable"), so a single monotonic watermark suffices;
 * no per-frame bitmap is needed.
 * <p>
 * <b>Layout</b> (128 bytes, little-endian, mmap'd for the engine's lifetime):
 * two independently CRC-protected 64-byte records. Each record contains:
 * <pre>
 *   offset 0:   u32 magic = 'AKW1'
 *   offset 4:   u32 version = 1
 *   offset 8:   i64 generation
 *   offset 16:  i64 fsn
 *   offset 24:  reserved (zero-filled through offset 59)
 *   offset 60:  u32 CRC32C of bytes [0, 60)
 * </pre>
 * {@link #write(long)} rewrites the record not selected by the current
 * generation and stores its CRC last. Recovery selects the valid record with
 * the greatest generation. A torn update therefore falls back to the older
 * valid record; if neither record validates, recovery conservatively uses the
 * segment-derived seed.
 * <p>
 * <b>Zero-alloc, store-only ACK writes:</b> {@link #open(String)} maps both
 * records once. Ordinary ACK-only manager updates mutate the inactive record
 * in the mapping and require no malloc/free, read/write syscalls, or rename.
 * <p>
 * <b>fsync cadence:</b> ordinary ACK-only manager updates call
 * {@link #write(long)} and stay syscall-free. Each non-empty background disk-trim
 * quantum calls {@link #sync()} once (one mmap msync and one fd fsync), fsyncs
 * the slot directory before unlinking, and fsyncs it again after the batch. A
 * fully drained close uses the same covering order so the durable watermark
 * guards any acknowledged segment that a host crash restores.
 * <p>
 * <b>Lifecycle:</b> single-writer (the {@link SegmentManager} worker thread)
 * after construction. Read once at engine startup (any thread, before the
 * manager observes the entry). Close releases the mapping and fd. Not
 * thread-safe for concurrent writers.
 */
public final class AckWatermark implements QuietCloseable {

    /**
     * Filename of the watermark within the slot directory. Dot-prefixed so
     * directory enumerators that filter by extension skip it automatically.
     */
    public static final String FILE_NAME = ".ack-watermark";
    public static final int FILE_SIZE = 128;
    /**
     * Sentinel returned by {@link #read()} when neither watermark record is
     * valid.
     */
    public static final long INVALID = Long.MIN_VALUE;
    static final int FILE_MAGIC = 0x31574B41; // 'AKW1' little-endian
    private static final int CRC_OFFSET = 60;
    private static final int FSN_OFFSET = 16;
    private static final Logger LOG = LoggerFactory.getLogger(AckWatermark.class);
    private static final int MAGIC_OFFSET = 0;
    private static final int RECORD_SIZE = 64;
    private static final int VERSION = 1;
    private final int fd;
    private final FilesFacade filesFacade;
    private final long mmapAddress;
    private boolean closed;
    private long fsn;
    private long generation;
    private boolean isStorageReleased;

    private AckWatermark(FilesFacade filesFacade, int fd, long mmapAddress,
                         long generation, long fsn) {
        this.fd = fd;
        this.filesFacade = filesFacade;
        this.fsn = fsn;
        this.generation = generation;
        this.mmapAddress = mmapAddress;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        releaseStorage();
    }

    /**
     * Opens (creating if absent) the watermark file in {@code slotDir} and
     * maps it for the engine's lifetime. Returns {@code null} on any setup
     * failure, leaving the caller to choose whether its durability contract
     * permits operation without one.
     * <p>
     * Wrong-sized files, including the legacy 16-byte non-CRC format, are
     * reset. Trusting a legacy FSN would retain the torn-write ambiguity this
     * format removes; resetting it causes conservative replay from the
     * segment-derived seed instead.
     */
    public static AckWatermark open(String slotDir) {
        return open(FilesFacade.INSTANCE, slotDir);
    }

    /**
     * Facade-aware variant of {@link #open(String)}. Every filesystem call,
     * including the lifetime mapping, goes through {@code filesFacade} so
     * tests can observe or fault-inject the watermark mmap.
     */
    public static AckWatermark open(FilesFacade filesFacade, String slotDir) {
        String filePath = slotDir + "/" + FILE_NAME;
        long existing = filesFacade.exists(filePath) ? filesFacade.length(filePath) : -1L;
        int fd;
        if (existing == FILE_SIZE) {
            fd = filesFacade.openRW(filePath);
        } else {
            fd = filesFacade.openCleanRW(filePath);
            if (fd >= 0 && !filesFacade.allocate(fd, FILE_SIZE)) {
                // FilesFacade.allocate contract on a false return: close the
                // fd and unlink the partial file.
                filesFacade.close(fd);
                filesFacade.remove(filePath);
                fd = -1;
            }
        }
        if (fd < 0) {
            LOG.warn("ack watermark {} could not be opened (rc={})", filePath, fd);
            return null;
        }
        long addr = filesFacade.mmap(fd, FILE_SIZE, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
        if (addr == Files.FAILED_MMAP_ADDRESS) {
            LOG.warn("ack watermark {} could not be mmapped", filePath);
            filesFacade.close(fd);
            return null;
        }
        Record selected = selectRecord(addr);
        return selected == null
                ? new AckWatermark(filesFacade, fd, addr, 0L, INVALID)
                : new AckWatermark(filesFacade, fd, addr, selected.generation, selected.fsn);
    }

    /**
     * Releases the native storage while deliberately leaving the logical
     * closed flag clear. Test-only: recreates a stale racing writer without
     * reflective access to descriptor and mapping internals.
     */
    @TestOnly
    public boolean releaseStorageButKeepWritableForTesting() {
        return releaseStorage();
    }

    /**
     * Best-effort removal of a stale watermark file. Used by the engine
     * startup path when no segments are recovered.
     */
    public static void removeOrphan(String slotDir) {
        removeOrphan(FilesFacade.INSTANCE, slotDir);
    }

    static boolean removeOrphan(FilesFacade filesFacade, String slotDir) {
        return filesFacade.remove(slotDir + "/" + FILE_NAME);
    }

    /**
     * Returns the FSN from the greatest-generation valid record, or
     * {@link #INVALID} when neither record validates.
     */
    public long read() {
        if (closed) return INVALID;
        Record selected = selectRecord(mmapAddress);
        if (selected == null) {
            fsn = INVALID;
            generation = 0L;
        } else {
            fsn = selected.fsn;
            generation = selected.generation;
        }
        return fsn;
    }

    /**
     * Flushes the mapped bytes and then the backing fd. The caller must sync
     * the slot directory after this succeeds so a newly-created watermark's
     * directory entry is durable before segment deletion begins.
     */
    public void sync() {
        if (closed) {
            throw new IllegalStateException("ack watermark is closed");
        }
        if (filesFacade.msync(mmapAddress, FILE_SIZE, false) != 0) {
            throw new IllegalStateException("could not msync ack watermark");
        }
        if (filesFacade.fsync(fd) != 0) {
            throw new IllegalStateException("could not fsync ack watermark");
        }
    }

    /**
     * Updates the inactive record and selects it in memory only after its CRC
     * has been stored. The next {@link #sync()} makes the complete update
     * durable before any covered segment is deleted.
     * <p>
     * Caller responsibility: monotonic ordering. The manager's tick loop only
     * calls this when {@code fsn} has advanced past the last write.
     */
    public void write(long fsn) {
        if (closed) return;
        long nextGeneration = generation + 1L;
        long recordAddress = mmapAddress + (nextGeneration & 1L) * RECORD_SIZE;
        Unsafe.getUnsafe().setMemory(recordAddress, RECORD_SIZE, (byte) 0);
        Unsafe.getUnsafe().putInt(recordAddress + MAGIC_OFFSET, FILE_MAGIC);
        Unsafe.getUnsafe().putInt(recordAddress + 4, VERSION);
        Unsafe.getUnsafe().putLong(recordAddress + 8, nextGeneration);
        Unsafe.getUnsafe().putLong(recordAddress + FSN_OFFSET, fsn);
        int crc = Crc32c.update(Crc32c.INIT, recordAddress, CRC_OFFSET);
        Unsafe.getUnsafe().putInt(recordAddress + CRC_OFFSET, crc);
        generation = nextGeneration;
        this.fsn = fsn;
    }

    private boolean releaseStorage() {
        if (isStorageReleased) {
            return false;
        }
        isStorageReleased = true;
        if (mmapAddress != 0L && mmapAddress != Files.FAILED_MMAP_ADDRESS) {
            filesFacade.munmap(mmapAddress, FILE_SIZE, MemoryTag.MMAP_DEFAULT);
        }
        if (fd >= 0) {
            filesFacade.close(fd);
        }
        return true;
    }

    private static Record readRecord(long address) {
        if (Unsafe.getUnsafe().getInt(address + MAGIC_OFFSET) != FILE_MAGIC
                || Unsafe.getUnsafe().getInt(address + 4) != VERSION) {
            return null;
        }
        int expectedCrc = Unsafe.getUnsafe().getInt(address + CRC_OFFSET);
        int actualCrc = Crc32c.update(Crc32c.INIT, address, CRC_OFFSET);
        if (expectedCrc != actualCrc) {
            return null;
        }
        long generation = Unsafe.getUnsafe().getLong(address + 8);
        long fsn = Unsafe.getUnsafe().getLong(address + FSN_OFFSET);
        if (generation <= 0 || fsn < -1L) {
            return null;
        }
        return new Record(generation, fsn);
    }

    private static Record selectRecord(long address) {
        Record first = readRecord(address);
        Record second = readRecord(address + RECORD_SIZE);
        if (first == null) {
            return second;
        }
        if (second == null || first.generation > second.generation) {
            return first;
        }
        return second;
    }

    private static final class Record {
        private final long fsn;
        private final long generation;

        private Record(long generation, long fsn) {
            this.fsn = fsn;
            this.generation = generation;
        }
    }
}
