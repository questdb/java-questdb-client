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
import io.questdb.client.std.Os;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One mmap-backed SF segment file. The user thread (the single producer)
 * appends frames into the mapping; the I/O thread (the single consumer) reads
 * up to {@link #publishedOffset()} for wire send. No locks; the cursor pair
 * {@code appendCursor} / {@code publishedCursor} is the only cross-thread
 * coordination, and {@code publishedCursor} is the publish barrier — the
 * I/O thread MUST NOT read any byte at offset {@code >= publishedOffset()}.
 * <p>
 * On-disk layout — header and frame format:
 * <pre>
 *   [u32 magic 'SF01'] [u8 ver=1] [u8 flags=0] [u16 reserved=0]
 *   [u64 baseSeq]       [u64 createdMicros]                       24-byte header
 *   frame, frame, ...                                              each frame:
 *                                                                  [u32 crc32c]
 *                                                                  [u32 payloadLen]
 *                                                                  [payloadLen bytes]
 *   crc32c covers (payloadLen, payload).
 * </pre>
 * The mapping is sized at construction and never grows. When
 * {@link #tryAppend} returns -1 the caller must rotate to a fresh segment.
 * Closing the segment unmaps and closes the fd; data already written is
 * durable under the page cache (and recoverable across JVM restarts) — call
 * {@link #msync} for OS-crash durability.
 */
public final class MmapSegment implements QuietCloseable {

    public static final int FILE_MAGIC = 0x31304653; // 'SF01' little-endian
    public static final int FRAME_HEADER_SIZE = 8;   // u32 crc + u32 payloadLen
    public static final int HEADER_SIZE = 24;
    public static final byte VERSION = 1;
    private static final Logger LOG = LoggerFactory.getLogger(MmapSegment.class);
    private static final int RECOVERY_BUFFER_SIZE = 1024 * 1024;

    private final FilesFacade filesFacade;
    private final String path;
    private final long sizeBytes;
    // fileBytes: logical size of the backing file — the segment's on-disk
    // footprint for sf_max_total_bytes accounting. Equals sizeBytes for
    // created and memory-backed segments; for recovered segments it can
    // exceed sizeBytes because recovery maps only the validated prefix.
    private final long fileBytes;
    // memoryBacked: true when the segment buffer lives in malloc'd native
    // memory rather than an mmap'd file. The "non-SF async" path uses
    // memory-backed segments — same cursor architecture, no disk involvement.
    // close() and msync() branch on this flag.
    private final boolean memoryBacked;
    // appendCursor: written only by the producer thread, never read by anyone else
    // — it's the reservation cursor. Plain field is fine.
    private long appendCursor;
    // baseSeq: provisional at create time, finalized by rebaseSeq() at rotation
    // time. Mutable to support the cursor engine's hot-spare design — the
    // segment manager pre-creates spares before the producer knows the exact
    // baseSeq the new active will need.
    private long baseSeq;
    private int fd;
    // frameCount: number of frames successfully appended. Single writer (the
    // producer thread in tryAppend); read cross-thread by the I/O thread via
    // SegmentRing.findSegmentContaining and SegmentRing.appendOrFsn-time
    // computations on the active segment. The ring's synchronized accessors
    // give one-sided fencing only — the writer is NOT synchronized on the
    // ring monitor. volatile is the cheapest correct fix.
    private volatile long frameCount;
    private long mmapAddress;
    // publishedCursor: written by producer, read by consumer (I/O thread). Volatile
    // because the consumer must see writes in publication order — once the
    // producer bumps publishedCursor, every byte before it is fully written.
    private volatile long publishedCursor;
    // Bytes between the last valid frame and the file end that look like an
    // attempted-but-invalid frame write (non-zero bytes at the bail-out
    // position). Zero for fresh segments and for cleanly partially-filled
    // segments (uninitialised tail). Set only by openExisting; visible to
    // recovery callers for diagnostics. Final after construction.
    private final long tornTailBytes;

    private MmapSegment(String path, int fd, long mmapAddress, long sizeBytes,
                        long fileBytes, long baseSeq, long initialCursor, long frameCount,
                        boolean memoryBacked, long tornTailBytes, FilesFacade filesFacade) {
        this.path = path;
        this.filesFacade = filesFacade;
        this.fd = fd;
        this.mmapAddress = mmapAddress;
        this.sizeBytes = sizeBytes;
        this.fileBytes = fileBytes;
        this.baseSeq = baseSeq;
        this.appendCursor = initialCursor;
        this.publishedCursor = initialCursor;
        this.frameCount = frameCount;
        this.memoryBacked = memoryBacked;
        this.tornTailBytes = tornTailBytes;
    }

    /**
     * Convenience overload of {@link #create(FilesFacade, String, long, long)}
     * that uses {@link FilesFacade#INSTANCE} (production default). Tests that
     * need to fault-inject filesystem failures (ENOSPC at openCleanRW or
     * allocate) should call the facade-aware overload directly.
     */
    public static MmapSegment create(String path, long baseSeq, long sizeBytes) {
        return create(FilesFacade.INSTANCE, path, baseSeq, sizeBytes);
    }

    /**
     * Creates a fresh segment file at {@code path}, pre-allocating exactly
     * {@code sizeBytes} bytes of real disk blocks and mmapping the whole
     * region RW. Writes the 24-byte header and positions the cursor
     * immediately after it. Throws {@link MmapSegmentException} on any I/O
     * failure (file already exists, openCleanRW failed, ENOSPC during
     * pre-allocation, mmap rejected).
     * <p>
     * Pre-allocation uses {@link FilesFacade#allocate(int, long)} so that
     * ENOSPC surfaces as a clean failure at create time, before the producer
     * starts appending. Without it, a logically-sized-but-sparse file would
     * defer ENOSPC to mmap-store time, where it manifests as a SIGBUS that
     * tears down the JVM. On filesystems where the underlying
     * {@code posix_fallocate} / {@code F_PREALLOCATE} is not supported, the
     * native fallback to {@code ftruncate} reintroduces the SIGBUS risk for
     * that filesystem only.
     */
    public static MmapSegment create(FilesFacade ff, String path, long baseSeq, long sizeBytes) {
        long pathPtr = ff.allocNativePath(path);
        try {
            return create(ff, pathPtr, path, baseSeq, sizeBytes);
        } finally {
            ff.freeNativePath(pathPtr);
        }
    }

    /**
     * Variant of {@link #create(FilesFacade, String, long, long)} that takes a
     * pre-encoded native UTF-8 path pointer plus a parallel String for use in
     * exception messages and {@link #path()}. The pointer must be a
     * null-terminated UTF-8 path, typically built into a reused
     * {@code DirectUtf8Sink} by the rotation hot path so it does not incur a
     * per-call {@code byte[]} + native-malloc the way the String overload does.
     */
    public static MmapSegment create(FilesFacade ff, long pathPtr, String displayPath, long baseSeq, long sizeBytes) {
        if (sizeBytes < HEADER_SIZE + FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException(
                    "sizeBytes too small for header + one minimal frame: " + sizeBytes);
        }
        int fd = ff.openCleanRW(pathPtr);
        if (fd < 0) {
            throw new MmapSegmentException("openCleanRW failed for " + displayPath);
        }
        // Reserve real disk blocks and advance EOF to sizeBytes in one
        // call. ENOSPC surfaces here, before the producer thread starts
        // writing frames into the mapping — a clean false return
        // instead of a SIGBUS-on-mmap-store later (which would abort
        // the JVM).
        if (!ff.allocate(fd, sizeBytes)) {
            ff.close(fd);
            // Unlink the partially-created file so a sf_max_bytes-sized
            // empty file does not survive the failure. Under sustained
            // disk-full pressure with the manager polling, hundreds would
            // otherwise accumulate.
            ff.remove(pathPtr);
            throw new MmapSegmentException("pre-allocation failed for " + displayPath);
        }
        long addr = Files.FAILED_MMAP_ADDRESS;
        try {
            addr = Files.mmap(fd, sizeBytes, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + displayPath);
            }
            // Header goes straight into the mapping — no separate write syscall.
            Unsafe.getUnsafe().putInt(addr, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(addr + 4, VERSION);
            Unsafe.getUnsafe().putByte(addr + 5, (byte) 0); // flags
            Unsafe.getUnsafe().putShort(addr + 6, (short) 0); // reserved
            Unsafe.getUnsafe().putLong(addr + 8, baseSeq);
            Unsafe.getUnsafe().putLong(addr + 16, Os.currentTimeMicros());
            return new MmapSegment(
                    displayPath, fd, addr, sizeBytes, sizeBytes, baseSeq, HEADER_SIZE, 0, false, 0L, ff);
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                Files.munmap(addr, sizeBytes, MemoryTag.MMAP_DEFAULT);
            }
            ff.close(fd);
            // mmap (or header writes) failed after a successful allocate —
            // best-effort unlink to keep the directory from accumulating
            // full-size empty segments under repeated failures.
            ff.remove(pathPtr);
            throw t;
        }
    }

    /**
     * Creates a memory-backed segment with the same on-the-wire layout as
     * {@link #create(String, long, long)} but without any file. Used by the
     * non-SF async ingest path: cursor's lock-free append architecture is
     * still the right answer, but durability is "in JVM memory" — no disk
     * involvement. The segment is freed via {@link #close()} (Unsafe.free).
     */
    public static MmapSegment createInMemory(long baseSeq, long sizeBytes) {
        if (sizeBytes < HEADER_SIZE + FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException(
                    "sizeBytes too small for header + one minimal frame: " + sizeBytes);
        }
        long addr = Unsafe.malloc(sizeBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            // Write the same header so a hex dump of either backing looks
            // identical and any future tool can scan a memory-backed
            // segment without special casing.
            Unsafe.getUnsafe().putInt(addr, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(addr + 4, VERSION);
            Unsafe.getUnsafe().putByte(addr + 5, (byte) 0);
            Unsafe.getUnsafe().putShort(addr + 6, (short) 0);
            Unsafe.getUnsafe().putLong(addr + 8, baseSeq);
            Unsafe.getUnsafe().putLong(addr + 16, Os.currentTimeMicros());
            return new MmapSegment(
                    null, -1, addr, sizeBytes, sizeBytes, baseSeq, HEADER_SIZE, 0, true, 0L, null);
        } catch (Throwable t) {
            Unsafe.free(addr, sizeBytes, MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
    }

    /**
     * Opens an existing segment file for recovery. Validates the header magic
     * / version, then scans frames forward verifying each CRC. The first bad
     * CRC (or a frame whose declared length runs past the file end) is treated
     * as a torn tail; both cursors are positioned at the start of that frame.
     * Throws {@link MmapSegmentException} on header validation failure.
     * <p>
     * The returned segment maps ONLY the validated prefix
     * {@code [0, lastGoodOffset)} and is therefore born full:
     * {@link #tryAppend} returns -1 and the caller rotates onto a freshly
     * created segment before the next append. The tail past the last good
     * frame is never mapped — it may be sparse/unbacked ({@code create()}'s
     * block reservation does not survive external sparsification, and
     * {@link FilesFacade#allocate} cannot retroactively fill holes below EOF),
     * and a store into an unbacked page under ENOSPC raises SIGBUS, aborting
     * the JVM.
     * <p>
     * If recovery observes a torn tail (the bytes at the bail-out position
     * are non-zero, indicating an attempted-but-failed frame write rather
     * than clean unwritten space), a {@code WARN} is emitted with the byte
     * count and the bytes are exposed via {@link #tornTailBytes()} so
     * operators can detect silent truncation from corruption or partial
     * writes. Clean partial fills (writer never attempted to write past the
     * last valid frame) do not log and report {@code 0}.
     */
    public static MmapSegment openExisting(String path) {
        return openExisting(FilesFacade.INSTANCE, path);
    }

    /**
     * Facade-aware recovery variant. Recovery validates the file with
     * positional reads before mapping it. HotSpot can deliver an mmap
     * {@link InternalError} after the lexical catch around an Unsafe load on
     * older JDKs; pread reports EOF and I/O errors synchronously instead.
     */
    public static MmapSegment openExisting(FilesFacade ff, String path) {
        int fd = ff.openRW(path);
        if (fd < 0) {
            throw new MmapSegmentException("openRW failed for " + path);
        }
        long addr = Files.FAILED_MMAP_ADDRESS;
        long mapSize = 0L;
        long scanBuffer = 0L;
        try {
            scanBuffer = Unsafe.malloc(RECOVERY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            long fileSize = ff.length(fd);
            if (fileSize < HEADER_SIZE) {
                throw new MmapSegmentException("file shorter than header: " + path + " size=" + fileSize);
            }
            RecoveryResult recovered = scanFile(ff, fd, path, fileSize, scanBuffer);

            // A concurrent shrink between scan and mmap must not expose a page
            // beyond EOF. Slot locking excludes the normal case, but the
            // second fd-size read also makes fault-injection and hostile file
            // changes deterministic.
            long finalFileSize = ff.length(fd);
            if (finalFileSize < recovered.lastGoodOffset) {
                throw new MmapSegmentException(
                        "file shrank below recovered data: " + path
                                + " size=" + finalFileSize
                                + " recovered=" + recovered.lastGoodOffset);
            }
            if (recovered.hasUnexpectedEof && finalFileSize >= fileSize) {
                throw new MmapSegmentException(
                        "short read before stable file EOF during recovery: " + path
                                + " size=" + fileSize);
            }
            long logicalSize = Math.min(fileSize, finalFileSize);
            if (logicalSize < HEADER_SIZE) {
                throw new MmapSegmentException("file shorter than header after recovery scan: "
                        + path + " size=" + logicalSize);
            }
            // Map ONLY the validated prefix [0, lastGoodOffset). The tail past
            // the last good frame may be sparse/unbacked, and a store into an
            // unbacked page under ENOSPC raises SIGBUS and aborts the JVM.
            // FilesFacade.allocate cannot make that tail safe at recovery time
            // (it reserves [currentSize, target) only — pre-existing holes
            // below EOF are not retroactively filled). Mapping through
            // lastGoodOffset makes the recovered segment born full
            // (capacityRemaining() == 0), so the producer rotates onto a
            // freshly created — and genuinely block-reserved — segment before
            // the next append. Consumers are unaffected: they only read below
            // publishedOffset(), which equals lastGoodOffset.
            mapSize = recovered.lastGoodOffset;
            addr = Files.mmap(fd, mapSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + path);
            }
            MmapSegment segment = new MmapSegment(
                    path,
                    fd,
                    addr,
                    mapSize,
                    logicalSize,
                    recovered.baseSeq,
                    recovered.lastGoodOffset,
                    recovered.frameCount,
                    false,
                    Math.min(recovered.tornTailBytes, logicalSize - recovered.lastGoodOffset),
                    ff
            );
            if (segment.tornTailBytes() > 0) {
                LOG.warn("SF segment {}: torn tail of {} bytes at offset {} "
                                + "(file size {}, frames recovered {}). "
                                + "The torn region is excluded from the recovered mapping and "
                                + "frames past the tear (if any) are discarded. "
                                + "Investigate disk health or unexpected writer crash.",
                        path,
                        segment.tornTailBytes(),
                        segment.publishedOffset(),
                        logicalSize,
                        segment.frameCount());
            }
            // Ownership of fd and addr transfers to the returned segment.
            addr = Files.FAILED_MMAP_ADDRESS;
            return segment;
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                Files.munmap(addr, mapSize, MemoryTag.MMAP_DEFAULT);
            }
            ff.close(fd);
            throw t;
        } finally {
            if (scanBuffer != 0L) {
                Unsafe.free(scanBuffer, RECOVERY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    public long address() {
        return mmapAddress;
    }

    public long baseSeq() {
        return baseSeq;
    }

    /**
     * Bytes available for further appends, accounting for the per-frame
     * 8-byte envelope a future {@link #tryAppend} would also write. This is
     * payload bytes the caller can still fit, NOT raw remaining-mapping bytes.
     */
    public long capacityRemaining() {
        long left = sizeBytes - appendCursor - FRAME_HEADER_SIZE;
        return left < 0 ? 0 : left;
    }

    @Override
    public void close() {
        if (mmapAddress != 0) {
            if (memoryBacked) {
                Unsafe.free(mmapAddress, sizeBytes, MemoryTag.NATIVE_DEFAULT);
            } else {
                Files.munmap(mmapAddress, sizeBytes, MemoryTag.MMAP_DEFAULT);
            }
            mmapAddress = 0;
        }
        if (fd >= 0) {
            if (filesFacade != null) {
                filesFacade.close(fd);
            } else {
                Files.close(fd);
            }
            fd = -1;
        }
    }

    public boolean isFull() {
        return capacityRemaining() <= 0;
    }

    /**
     * Synchronously flushes dirty pages of {@code [HEADER_SIZE, publishedOffset())}
     * to disk via {@code msync(MS_SYNC)}. Off the hot path — call only when
     * the user has opted into OS-crash durability (e.g. {@code sf_msync_on_flush=on}).
     */
    public void msync() {
        if (memoryBacked) return; // no on-disk pages to flush
        long pub = publishedCursor;
        if (pub > HEADER_SIZE) {
            Files.msync(mmapAddress, pub, false);
        }
    }

    /**
     * Bytes safely written and visible to the consumer. Reading any byte at
     * offset {@code >= publishedOffset()} from the mapping is undefined —
     * the producer may be mid-write.
     */
    public long publishedOffset() {
        return publishedCursor;
    }

    /** The on-disk file path this segment was created from / opened against. */
    public String path() {
        return path;
    }

    /**
     * Re-stamps the segment's baseSeq, both in memory and in the on-disk
     * header at offset 8. Used by {@code SegmentRing} at rotation time to
     * pin the segment's identity once the active's frame count is final
     * (the segment manager pre-creates spares with a provisional baseSeq
     * that may be stale by rotation time). Throws {@link IllegalStateException}
     * if any frames have already been appended — a rebase after first
     * append would corrupt the FSN sequence.
     */
    public void rebaseSeq(long newBaseSeq) {
        if (frameCount > 0) {
            throw new IllegalStateException(
                    "cannot rebase: segment has " + frameCount + " frame(s) already appended");
        }
        this.baseSeq = newBaseSeq;
        Unsafe.getUnsafe().putLong(mmapAddress + 8, newBaseSeq);
    }

    public long sizeBytes() {
        return sizeBytes;
    }

    /**
     * Logical size of the backing file in bytes — the segment's on-disk
     * footprint for {@code sf_max_total_bytes} cap accounting. Equals
     * {@link #sizeBytes()} for created and memory-backed segments; for
     * recovered segments it can exceed {@link #sizeBytes()} because recovery
     * maps only the validated prefix (SIGBUS hardening in
     * {@link #openExisting}) while the file keeps its full logical length
     * until trim unlinks it.
     */
    public long fileBytes() {
        return fileBytes;
    }

    /**
     * Appends one frame: writes {@code [crc32c | u32 payloadLen | payload]}
     * starting at the current append cursor, then advances both cursors
     * (publishedCursor last, so the consumer never sees a partial frame).
     * Returns the offset of the appended frame on success, or -1 if the
     * remaining capacity cannot fit {@code FRAME_HEADER_SIZE + payloadLen}.
     * <p>
     * This is the producer thread's hot path. No syscall, no allocation;
     * just a CRC pass and a memcpy into the mapped region.
     */
    public long tryAppend(long payloadAddr, int payloadLen) {
        if (payloadLen < 0) {
            throw new IllegalArgumentException("negative payloadLen: " + payloadLen);
        }
        long total = (long) FRAME_HEADER_SIZE + payloadLen;
        long offset = appendCursor;
        if (offset + total > sizeBytes) {
            return -1L;
        }
        // CRC32C over the (payloadLen, payload) pair. Recovery scans validate
        // each frame by recomputing this CRC over the on-disk bytes.
        long lenAddr = mmapAddress + offset + 4;
        Unsafe.getUnsafe().putInt(lenAddr, payloadLen);
        if (payloadLen > 0) {
            Unsafe.getUnsafe().copyMemory(payloadAddr, mmapAddress + offset + FRAME_HEADER_SIZE, payloadLen);
        }
        int crc = Crc32c.update(Crc32c.INIT, lenAddr, 4);
        if (payloadLen > 0) {
            crc = Crc32c.update(crc, mmapAddress + offset + FRAME_HEADER_SIZE, payloadLen);
        }
        Unsafe.getUnsafe().putInt(mmapAddress + offset, crc);
        appendCursor = offset + total;
        // Plain read + write of the volatile field. `frameCount++` would
        // trip the "non-atomic increment of volatile" inspection, but
        // single-writer invariant (only the producer thread mutates) makes
        // the RMW race-free by design.
        frameCount = frameCount + 1;
        // Publish last. Until this volatile write retires, the consumer
        // cannot see any of the bytes we just wrote.
        publishedCursor = appendCursor;
        return offset;
    }

    /**
     * Walks every published frame in this segment and returns the FSN of the
     * LAST frame whose payload does NOT carry the given flag bit, or {@code -1}
     * when every frame carries it (or the segment is empty).
     * <p>
     * A frame counts as carrying the flag ONLY when it positively parses as a
     * message of the expected protocol: payload at least {@code minPayloadLen}
     * bytes AND the little-endian u32 at payload offset 0 equals
     * {@code headerMagic} AND the byte at {@code flagsOffset} has
     * {@code flagMask} set. Anything else -- short frames, foreign payloads,
     * magic mismatches -- counts as NOT carrying the flag. This direction is
     * deliberate: the caller retires (trims) frames ABOVE the returned FSN,
     * so a frame we cannot positively identify must act as a retirement
     * barrier, never as trimmable. Misclassifying an unknown frame as
     * deferred would silently discard data that should replay.
     * <p>
     * Producer-thread only, and only meaningful before new appends race the
     * walk (recovery time). Used to locate the last commit-bearing QWP frame
     * below a potentially orphaned FLAG_DEFER_COMMIT tail: frames above the
     * returned FSN all carry the flag, i.e. they belong to a transaction whose
     * commit frame was never published.
     */
    public long findLastFrameFsnWithoutPayloadFlag(int flagsOffset, int flagMask, int headerMagic, int minPayloadLen) {
        long best = -1L;
        long off = HEADER_SIZE;
        long frames = frameCount;
        for (long i = 0; i < frames; i++) {
            int payloadLen = Unsafe.getUnsafe().getInt(mmapAddress + off + 4);
            long payload = mmapAddress + off + FRAME_HEADER_SIZE;
            boolean flagSet = payloadLen >= minPayloadLen
                    && payloadLen > flagsOffset
                    && Unsafe.getUnsafe().getInt(payload) == headerMagic
                    && (Unsafe.getUnsafe().getByte(payload + flagsOffset) & flagMask) != 0;
            if (!flagSet) {
                best = baseSeq + i;
            }
            off += FRAME_HEADER_SIZE + payloadLen;
        }
        return best;
    }

    /**
     * Number of frames written since {@link #create} (or recovered by
     * {@link #openExisting}). Used by {@code SegmentRing} to compute
     * {@code lastSeq = baseSeq + frameCount - 1} for ACK / trim decisions.
     * Single-writer; no lock needed.
     */
    public long frameCount() {
        return frameCount;
    }

    /**
     * Bytes between the last valid frame and the file end that look like an
     * attempted-but-invalid frame write — set by {@link #openExisting} when
     * recovery observes non-zero bytes past the bail-out point. {@code 0} for
     * fresh segments, memory-backed segments, and cleanly partially-filled
     * recovered segments. Operators / tests can read this to tell silent
     * truncation (corruption) from a normal partial fill (no incident).
     * A positional read that reaches EOF before the initially reported file
     * size yields {@code 0}: no bytes exist at the bail-out point to identify
     * an attempted write. Hard read errors reject the segment instead of
     * guessing whether its tail is clean.
     */
    public long tornTailBytes() {
        return tornTailBytes;
    }

    private static RecoveryResult scanFile(
            FilesFacade ff,
            int fd,
            String path,
            long fileSize,
            long scanBuffer
    ) {
        RecoveryReader reader = new RecoveryReader(ff, fd, path, fileSize, scanBuffer);
        long headerAddress = reader.addressAt(0L, HEADER_SIZE);
        if (headerAddress == 0L) {
            throw new MmapSegmentException("short read of segment header: " + path);
        }
        int magic = Unsafe.getUnsafe().getInt(headerAddress);
        if (magic != FILE_MAGIC) {
            throw new MmapSegmentException(
                    "bad magic in " + path + ": 0x" + Integer.toHexString(magic));
        }
        byte version = Unsafe.getUnsafe().getByte(headerAddress + 4);
        if (version != VERSION) {
            throw new MmapSegmentException("unsupported version in " + path + ": " + version);
        }
        long baseSeq = Unsafe.getUnsafe().getLong(headerAddress + 8);
        // FSNs are non-negative by construction (see SegmentRing). Reject a
        // corrupt value before unsigned contiguity checks can poison recovery.
        if (baseSeq < 0L) {
            throw new MmapSegmentException("bad baseSeq in " + path + ": " + baseSeq);
        }

        long count = 0L;
        long pos = HEADER_SIZE;
        while (pos + FRAME_HEADER_SIZE <= fileSize) {
            long frameHeader = reader.addressAt(pos, FRAME_HEADER_SIZE);
            if (frameHeader == 0L) {
                break;
            }
            int crcRead = Unsafe.getUnsafe().getInt(frameHeader);
            long payloadLen = Unsafe.getUnsafe().getInt(frameHeader + 4) & 0xFFFF_FFFFL;
            if (payloadLen > fileSize - pos - FRAME_HEADER_SIZE) {
                break;
            }

            int crc = Crc32c.update(Crc32c.INIT, frameHeader + 4, 4L);
            long payloadOffset = pos + FRAME_HEADER_SIZE;
            long remaining = payloadLen;
            boolean hasUnexpectedEof = false;
            while (remaining > 0L) {
                int available = reader.availableAt(payloadOffset);
                if (available == 0) {
                    hasUnexpectedEof = true;
                    break;
                }
                long chunk = Math.min(remaining, available);
                crc = Crc32c.update(crc, reader.addressAt(payloadOffset, 1), chunk);
                payloadOffset += chunk;
                remaining -= chunk;
            }
            if (hasUnexpectedEof || crc != crcRead) {
                break;
            }
            pos += FRAME_HEADER_SIZE + payloadLen;
            count++;
        }
        // Empty hot spares are zero-filled all the way to EOF. Inspect the
        // entire suffix, not just the failed frame header: a valid zero-length
        // frame whose CRC corrupts to zero has an all-zero 8-byte header, while
        // later valid frames still contain data that must be quarantined.
        long tornTailBytes = reader.hasNonZeroAt(pos, fileSize - pos)
                ? fileSize - pos
                : 0L;
        return new RecoveryResult(baseSeq, count, pos, tornTailBytes, reader.hasUnexpectedEof());
    }

    private static final class RecoveryReader {
        private final long address;
        private final int fd;
        private final long fileSize;
        private final FilesFacade filesFacade;
        private final String path;
        private boolean hasUnexpectedEof;
        private int windowLength;
        private long windowOffset = -1L;

        private RecoveryReader(
                FilesFacade filesFacade,
                int fd,
                String path,
                long fileSize,
                long address
        ) {
            this.filesFacade = filesFacade;
            this.fd = fd;
            this.path = path;
            this.fileSize = fileSize;
            this.address = address;
        }

        private long addressAt(long offset, int minBytes) {
            if (minBytes < 1 || minBytes > RECOVERY_BUFFER_SIZE || offset < 0L || offset >= fileSize) {
                return 0L;
            }
            if (windowOffset <= offset
                    && offset - windowOffset <= windowLength
                    && minBytes <= windowLength - (offset - windowOffset)) {
                return address + offset - windowOffset;
            }
            refill(offset, minBytes);
            return windowLength >= minBytes ? address : 0L;
        }

        private int availableAt(long offset) {
            long currentAddress = addressAt(offset, 1);
            if (currentAddress == 0L) {
                return 0;
            }
            return (int) (windowLength - (offset - windowOffset));
        }

        private boolean hasNonZeroAt(long offset, long length) {
            long inspected = 0L;
            while (inspected < length) {
                int available = availableAt(offset + inspected);
                if (available == 0) {
                    return false;
                }
                int chunk = (int) Math.min(length - inspected, available);
                long currentAddress = addressAt(offset + inspected, 1);
                int i = 0;
                while (i < chunk && ((currentAddress + i) & 7L) != 0L) {
                    if (Unsafe.getUnsafe().getByte(currentAddress + i++) != 0) {
                        return true;
                    }
                }
                while (i + 8 <= chunk) {
                    if (Unsafe.getUnsafe().getLong(currentAddress + i) != 0L) {
                        return true;
                    }
                    i += 8;
                }
                while (i < chunk) {
                    if (Unsafe.getUnsafe().getByte(currentAddress + i++) != 0) {
                        return true;
                    }
                }
                inspected += chunk;
            }
            return false;
        }

        private boolean hasUnexpectedEof() {
            return hasUnexpectedEof;
        }

        private void refill(long offset, int minBytes) {
            long maxLength = Math.min(RECOVERY_BUFFER_SIZE, fileSize - offset);
            long total = 0L;
            while (total < minBytes && total < maxLength) {
                long read = filesFacade.read(fd, address + total, maxLength - total, offset + total);
                if (read < 0L) {
                    throw new MmapSegmentException(
                            "read failed during segment recovery: " + path + " offset=" + (offset + total));
                }
                if (read == 0L) {
                    hasUnexpectedEof = offset + total < fileSize;
                    break;
                }
                if (read > maxLength - total) {
                    throw new MmapSegmentException(
                            "invalid read length during segment recovery: " + path + " read=" + read);
                }
                total += read;
            }
            windowOffset = offset;
            windowLength = (int) total;
        }
    }

    private static final class RecoveryResult {
        private final long baseSeq;
        private final long frameCount;
        private final boolean hasUnexpectedEof;
        private final long lastGoodOffset;
        private final long tornTailBytes;

        private RecoveryResult(
                long baseSeq,
                long frameCount,
                long lastGoodOffset,
                long tornTailBytes,
                boolean hasUnexpectedEof
        ) {
            this.baseSeq = baseSeq;
            this.frameCount = frameCount;
            this.lastGoodOffset = lastGoodOffset;
            this.tornTailBytes = tornTailBytes;
            this.hasUnexpectedEof = hasUnexpectedEof;
        }
    }

}
