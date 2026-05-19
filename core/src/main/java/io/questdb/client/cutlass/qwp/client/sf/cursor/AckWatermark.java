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

import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
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
 * <b>Layout</b> (16 bytes, little-endian, mmap'd for the engine's lifetime):
 * <pre>
 *   offset 0:  u32 magic = 'AKW1' (set once on first write)
 *   offset 4:  u32 reserved (zero)
 *   offset 8:  i64 fsn
 * </pre>
 * <p>
 * <b>Zero-alloc, zero-syscall writes:</b> {@link #open(String)} opens the
 * file and maps the 16 bytes once. {@link #write(long)} is a single
 * 8-byte aligned {@code Unsafe.putLong} into the mapped region. No
 * malloc/free, no read/write syscalls, no rename, on the manager's hot
 * tick path. An 8-byte aligned store is hardware-atomic on x86_64 and
 * arm64, and disk-atomic within one sector (the file is 16 bytes —
 * trivially within one sector), so a torn FSN across a crash boundary
 * is not a concern.
 * <p>
 * <b>Why no CRC:</b> the watermark is an optimization. If the on-disk
 * value is somehow corrupted (filesystem bug, hardware fault), the
 * recovery path's {@code max(lowestBase - 1, watermark)} clamp absorbs
 * the inconsistency: a stale-low watermark just means more re-replay, a
 * stale-high watermark is impossible because no write produces an FSN
 * higher than the segments on disk could account for. CRC adds
 * complexity (multi-store with fence + read-side validate) for
 * marginal additional safety.
 * <p>
 * <b>fsync cadence:</b> intentionally NOT performed. Host crash falls
 * back to recovery's {@code lowestBase - 1} seed (same as before this
 * feature, no regression), at the cost of a bounded re-replay window
 * for whatever durable-acks landed since the last successful page
 * cache flush.
 * <p>
 * <b>Lifecycle:</b> single-writer (the {@link SegmentManager} worker
 * thread) after construction. Read once at engine startup (any thread,
 * before the manager observes the entry). Close releases the mapping
 * and fd. Not thread-safe for concurrent writers.
 */
public final class AckWatermark implements QuietCloseable {

    /**
     * Filename of the watermark within the slot directory.
     * Dot-prefixed so directory enumerators that filter by extension
     * (segment recovery, OrphanScanner) skip it automatically.
     */
    public static final String FILE_NAME = ".ack-watermark";
    public static final int FILE_SIZE = 16;
    /**
     * Sentinel returned by {@link #read()} when the watermark file is
     * present but has not yet been written to (the magic field is zero
     * because the OS zero-filled the freshly created file).
     */
    public static final long INVALID = Long.MIN_VALUE;
    static final int FILE_MAGIC = 0x31574B41; // 'AKW1' little-endian
    private static final int FSN_OFFSET = 8;
    private static final Logger LOG = LoggerFactory.getLogger(AckWatermark.class);
    private static final int MAGIC_OFFSET = 0;
    private final int fd;
    private final long mmapAddress;
    private boolean closed;
    // Stamped once per process either at open() (if the file already
    // has the magic from a prior session) or on the first write() that
    // observes it unset. After the flag flips, write() degenerates to
    // a single 8-byte putLong against the mapped FSN slot -- no memory
    // load of magic on the hot path. Manager-thread-only after
    // construction; no synchronisation needed.
    private boolean magicWritten;

    private AckWatermark(int fd, long mmapAddress, boolean magicAlreadyWritten) {
        this.fd = fd;
        this.mmapAddress = mmapAddress;
        this.magicWritten = magicAlreadyWritten;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (mmapAddress != 0L && mmapAddress != Files.FAILED_MMAP_ADDRESS) {
            Files.munmap(mmapAddress, FILE_SIZE, MemoryTag.MMAP_DEFAULT);
        }
        if (fd >= 0) {
            Files.close(fd);
        }
    }

    /**
     * Opens (creating if absent) the watermark file in {@code slotDir}
     * and maps it for the engine's lifetime. Returns {@code null} on
     * any setup failure (open fail, mmap fail, unexpected size) — the
     * caller falls back to the no-watermark behaviour, no exception
     * escapes. Idempotent at the engine layer: a stale file from a
     * prior session is reused as-is; the first {@link #write(long)}
     * stamps the magic and the new FSN atomically.
     */
    public static AckWatermark open(String slotDir) {
        String filePath = slotDir + "/" + FILE_NAME;
        // Decide by size: existing-and-correct -> openRW preserves the
        // previous session's watermark (defeating which is the whole
        // point of NOT calling openCleanRW unconditionally); missing or
        // wrong-sized -> openCleanRW + allocate creates a fresh
        // FILE_SIZE-byte file (zero magic, read() reports INVALID until
        // the first write).
        long existing = Files.exists(filePath) ? Files.length(filePath) : -1L;
        int fd;
        if (existing == FILE_SIZE) {
            fd = Files.openRW(filePath);
        } else {
            fd = Files.openCleanRW(filePath);
            if (fd >= 0 && !Files.allocate(fd, FILE_SIZE)) {
                // FilesFacade.allocate contract on a false return:
                // close the fd AND unlink the partial file.
                Files.close(fd);
                Files.remove(filePath);
                fd = -1;
            }
        }
        if (fd < 0) {
            LOG.warn("ack watermark {} could not be opened (rc={}); proceeding without it",
                    filePath, fd);
            return null;
        }
        long addr = Files.mmap(fd, FILE_SIZE, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
        if (addr == Files.FAILED_MMAP_ADDRESS) {
            LOG.warn("ack watermark {} could not be mmapped; proceeding without it", filePath);
            Files.close(fd);
            return null;
        }
        // Inspect the existing magic once at open time. If it's already
        // set (cross-session reopen, e.g. recovery after a clean
        // shutdown), the first write() can skip the magic store
        // entirely and degenerate to a single 8-byte FSN put.
        int magic = Unsafe.getUnsafe().getInt(addr + MAGIC_OFFSET);
        return new AckWatermark(fd, addr, magic == FILE_MAGIC);
    }

    /**
     * Best-effort removal of a stale watermark file. Used by the
     * engine startup path when no segments are recovered — a stale
     * watermark file with no segments behind it is meaningless and
     * would only confuse the next session's seed.
     */
    public static void removeOrphan(String slotDir) {
        Files.remove(slotDir + "/" + FILE_NAME);
    }

    /**
     * Single-load read of the current FSN. Returns {@link #INVALID} if
     * the file has never been written (magic field is zero, i.e. the
     * file was freshly created by {@link #open(String)} and no
     * {@link #write(long)} has run yet against this slot).
     */
    public long read() {
        if (closed) return INVALID;
        int magic = Unsafe.getUnsafe().getInt(mmapAddress + MAGIC_OFFSET);
        if (magic != FILE_MAGIC) {
            // Either freshly created (all zeros) or some kind of corruption.
            // Either way, fall back to the segment-derived seed.
            return INVALID;
        }
        return Unsafe.getUnsafe().getLong(mmapAddress + FSN_OFFSET);
    }

    /**
     * Atomically updates the persisted FSN. Single 8-byte aligned
     * store to the mapped region. The first write also stamps the
     * magic so the next session's {@link #read()} can distinguish
     * "valid watermark" from "freshly created file".
     * <p>
     * Caller responsibility: monotonic ordering. The manager's tick
     * loop should only call this when {@code fsn} has advanced past
     * the last write.
     */
    public void write(long fsn) {
        if (closed) return;
        // Steady-state hot path: a single 8-byte aligned putLong, no
        // memory load of the magic. Order matters only on the very
        // first write of a fresh file: FSN first so the next reader
        // that observes the magic (set immediately below) also
        // observes a valid FSN -- no fence needed because the same
        // thread reads both in program order, and crash recovery
        // resumes a fresh process that sees whatever disk-level state
        // the kernel flushed.
        Unsafe.getUnsafe().putLong(mmapAddress + FSN_OFFSET, fsn);
        if (!magicWritten) {
            Unsafe.getUnsafe().putInt(mmapAddress + MAGIC_OFFSET, FILE_MAGIC);
            magicWritten = true;
        }
    }
}
