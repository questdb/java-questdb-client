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
 * Closing the segment unmaps and closes the fd. Dirty mapped pages normally
 * survive a producer-process restart in the OS page cache, but that is not a
 * host-power-loss guarantee. {@link #msync} preserves the legacy mmap-only
 * flush API; use the checked {@link #syncPublished()} mapping-plus-fd barrier
 * for portable power-loss durability.
 */
public final class MmapSegment implements QuietCloseable {

    public static final int FILE_MAGIC = 0x31304653; // 'SF01' little-endian
    public static final int FRAME_HEADER_SIZE = 8;   // u32 crc + u32 payloadLen
    public static final int HEADER_SIZE = 24;
    public static final byte MANIFEST_REQUIRED_FLAG = 1;
    public static final byte VERSION = 1;
    private static final Logger LOG = LoggerFactory.getLogger(MmapSegment.class);
    private static final int RECOVERY_BUFFER_SIZE = 64 * 1024;

    private final FilesFacade filesFacade;
    private final String path;
    private final long sizeBytes;
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
    // Highest published byte offset covered by a successful data barrier.
    // The manager writes this after msync+fsync; the producer reads it before
    // allowing rotation to seal the segment.
    private volatile long durableCursor;
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
    // Monotonic in-memory link to the segment that immediately follows this
    // one. SegmentRing publishes it before promoting the successor to active;
    // close deliberately retains it so a cursor can advance after head trim.
    private volatile MmapSegment successor;
    // Bytes between the last valid frame and the file end that look like an
    // attempted-but-invalid frame write (the suffix contains non-zero bytes).
    // Zero for fresh segments and for cleanly partially-filled
    // segments (uninitialised tail). Set only by openExisting; visible to
    // recovery callers for diagnostics. Final after construction.
    private final long tornTailBytes;

    private MmapSegment(FilesFacade filesFacade, String path, int fd, long mmapAddress, long sizeBytes,
                        long baseSeq, long initialCursor, long frameCount,
                        boolean memoryBacked, long tornTailBytes) {
        this.filesFacade = filesFacade;
        this.path = path;
        this.fd = fd;
        this.mmapAddress = mmapAddress;
        this.sizeBytes = sizeBytes;
        this.baseSeq = baseSeq;
        this.appendCursor = initialCursor;
        this.publishedCursor = initialCursor;
        this.durableCursor = memoryBacked ? initialCursor : HEADER_SIZE;
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
        return create(ff, path, baseSeq, sizeBytes, false);
    }

    static MmapSegment create(FilesFacade ff, String path, long baseSeq, long sizeBytes, boolean manifestRequired) {
        long pathPtr = ff.allocNativePath(path);
        try {
            return create(ff, pathPtr, path, baseSeq, sizeBytes, manifestRequired);
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
        return create(ff, pathPtr, displayPath, baseSeq, sizeBytes, false);
    }

    static MmapSegment create(FilesFacade ff, long pathPtr, String displayPath, long baseSeq, long sizeBytes, boolean manifestRequired) {
        if (sizeBytes < HEADER_SIZE + FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException(
                    "sizeBytes too small for header + one minimal frame: " + sizeBytes);
        }
        int fd = ff.openRWExclusive(pathPtr);
        if (fd < 0) {
            throw new MmapSegmentException("exclusive create failed for " + displayPath);
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
            addr = ff.mmap(fd, sizeBytes, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + displayPath);
            }
            // Header goes straight into the mapping — no separate write syscall.
            Unsafe.getUnsafe().putInt(addr, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(addr + 4, VERSION);
            Unsafe.getUnsafe().putByte(addr + 5, manifestRequired ? MANIFEST_REQUIRED_FLAG : (byte) 0); // flags
            Unsafe.getUnsafe().putShort(addr + 6, (short) 0); // reserved
            Unsafe.getUnsafe().putLong(addr + 8, baseSeq);
            Unsafe.getUnsafe().putLong(addr + 16, Os.currentTimeMicros());
            return new MmapSegment(ff, displayPath, fd, addr, sizeBytes, baseSeq, HEADER_SIZE, 0, false, 0L);
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                ff.munmap(addr, sizeBytes, MemoryTag.MMAP_DEFAULT);
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
            return new MmapSegment(null, null, -1, addr, sizeBytes, baseSeq, HEADER_SIZE, 0, true, 0L);
        } catch (Throwable t) {
            Unsafe.free(addr, sizeBytes, MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
    }

    /**
     * Opens an existing segment file for recovery. Validates the header and
     * scans frames through positioned reads, then mmaps the validated file RW.
     * The first bad CRC (or a frame whose declared length runs past the file
     * end) is treated as a torn tail; both cursors are positioned at the
     * start of that frame. Returns the segment ready for further appends.
     * Throws {@link MmapSegmentException} on header validation failure.
     * <p>
     * If recovery observes a torn tail (the suffix after the last valid frame
     * contains non-zero bytes, indicating an attempted-but-failed frame write
     * rather than clean unwritten space), a {@code WARN} is emitted with the byte
     * count and the bytes are exposed via {@link #tornTailBytes()} so
     * operators can detect silent truncation from corruption or partial
     * writes. Clean partial fills (writer never attempted to write past the
     * last valid frame) do not log and report {@code 0}.
     */
    public static MmapSegment openExisting(String path) {
        return openExisting(FilesFacade.INSTANCE, path);
    }

    /**
     * Facade-aware variant of {@link #openExisting(String)}. Recovery reads the
     * file through {@link FilesFacade#read(int, long, long, long)} before it
     * creates the mapping, so sparse or unbacked pages cannot raise SIGBUS in
     * the JVM. The descriptor length is checked before and after the scan and
     * again after mmap; a short read or size change is an operational failure
     * and aborts recovery.
     * <p>
     * The caller must prevent concurrent writers from modifying the file during
     * recovery and for the lifetime of the returned segment. The length checks
     * detect observed truncate/extend races, but no mmap-based implementation
     * can remain safe against uncoordinated mutation after the final check.
     */
    public static MmapSegment openExisting(FilesFacade ff, String path) {
        int fd = ff.openRW(path);
        if (fd < 0) {
            throw new MmapSegmentException("openRW failed for " + path);
        }
        long addr = Files.FAILED_MMAP_ADDRESS;
        long fileSize = -1L;
        try {
            fileSize = ff.length(fd);
            if (fileSize < 0) {
                throw new MmapSegmentException(
                        "could not stat open segment " + path + " [errno=" + Os.errno() + ']');
            }
            if (fileSize < HEADER_SIZE) {
                // Corruption, not an operational error: the bytes themselves prove
                // this cannot be a whole segment (a create() is never durable at a
                // sub-header size — allocate() reserves the full extent up front).
                throw new MmapSegmentCorruptionException(
                        "file shorter than header: " + path + " size=" + fileSize);
            }

            RecoveryScan scan = scanForRecovery(ff, fd, path, fileSize);
            long finalSize = ff.length(fd);
            if (finalSize < 0) {
                throw new MmapSegmentException(
                        "could not re-stat open segment " + path + " [errno=" + Os.errno() + ']');
            }
            if (finalSize != fileSize) {
                throw new MmapSegmentException(
                        "segment size changed during recovery: " + path
                                + " [before=" + fileSize + ", after=" + finalSize + ']');
            }

            addr = ff.mmap(fd, fileSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + path);
            }
            long mappedSize = ff.length(fd);
            if (mappedSize < 0) {
                throw new MmapSegmentException(
                        "could not stat mapped segment " + path + " [errno=" + Os.errno() + ']');
            }
            if (mappedSize != fileSize) {
                throw new MmapSegmentException(
                        "segment size changed while mapping: " + path
                                + " [before=" + fileSize + ", after=" + mappedSize + ']');
            }
            if (scan.tornTailBytes > 0) {
                LOG.warn("SF segment {}: torn tail of {} bytes at offset {} "
                                + "(file size {}, frames recovered {}). "
                                + "Recovery will overwrite this region on next append; "
                                + "frames past the tear (if any) are discarded. "
                                + "Investigate disk health or unexpected writer crash.",
                        path, scan.tornTailBytes, scan.lastGood, fileSize, scan.frameCount);
            }
            return new MmapSegment(
                    ff,
                    path,
                    fd,
                    addr,
                    fileSize,
                    scan.baseSeq,
                    scan.lastGood,
                    scan.frameCount,
                    false,
                    scan.tornTailBytes
            );
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                ff.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
            }
            ff.close(fd);
            throw t;
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
                filesFacade.munmap(mmapAddress, sizeBytes, MemoryTag.MMAP_DEFAULT);
            }
            mmapAddress = 0;
        }
        if (fd >= 0) {
            filesFacade.close(fd);
            fd = -1;
        }
    }

    boolean manifestRequired() {
        return !memoryBacked && (Unsafe.getUnsafe().getByte(mmapAddress + 5) & MANIFEST_REQUIRED_FLAG) != 0;
    }

    void markManifestRequired() {
        if (memoryBacked || manifestRequired()) {
            return;
        }
        Unsafe.getUnsafe().putByte(mmapAddress + 5, MANIFEST_REQUIRED_FLAG);
        syncHeader();
    }

    void syncHeader() {
        if (memoryBacked) {
            return;
        }
        if (filesFacade.msync(mmapAddress, HEADER_SIZE, false) != 0 || filesFacade.fsync(fd) != 0) {
            throw new MmapSegmentException("could not sync segment header " + path);
        }
        if (durableCursor < HEADER_SIZE) {
            durableCursor = HEADER_SIZE;
        }
    }

    public boolean isFull() {
        return capacityRemaining() <= 0;
    }

    public boolean isPublishedDurable() {
        return durableCursor >= publishedCursor;
    }

    /**
     * Preserves the original explicit mmap-flush behavior for callers that use
     * this low-level API directly. Periodic durability uses
     * {@link #syncPublished()}, which adds checked error handling and an fd
     * barrier.
     */
    public void msync() {
        if (memoryBacked) {
            return;
        }
        long published = publishedCursor;
        if (published > HEADER_SIZE) {
            filesFacade.msync(mmapAddress, published, false);
        }
    }

    /**
     * Synchronously flushes every complete frame published when this method
     * captures {@link #publishedCursor}. A concurrent producer may publish
     * more bytes while the barrier runs; those bytes remain outside the
     * returned durable boundary until a later call.
     *
     * @return the captured byte offset covered by the successful barrier
     */
    public long syncPublished() {
        long published = publishedCursor;
        if (memoryBacked) {
            durableCursor = published;
            return published;
        }
        if (published <= durableCursor) {
            return durableCursor;
        }
        if (filesFacade.msync(mmapAddress, published, false) != 0) {
            throw new MmapSegmentException("could not sync segment data " + path);
        }
        // FlushViewOfFile alone is not power-loss durable on Windows. Keep the
        // fd barrier on every platform so this method has one portable contract.
        if (filesFacade.fsync(fd) != 0) {
            throw new MmapSegmentException("could not sync segment file " + path);
        }
        durableCursor = published;
        return published;
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

    void linkSuccessor(MmapSegment next) {
        if (next == null) {
            throw new IllegalArgumentException("successor must not be null");
        }
        MmapSegment existing = successor;
        if (existing != null && existing != next) {
            throw new IllegalStateException("segment successor already linked");
        }
        successor = next;
    }

    MmapSegment successor() {
        return successor;
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
        // each frame by recomputing this CRC over the on-disk bytes. The
        // payloadLen field (4 bytes at lenAddr) and the payload (at lenAddr+4)
        // are physically contiguous, so a single CRC pass covers both -- one
        // native call per frame, byte-identical to the chained form the
        // recovery scanner recomputes (see scanForRecovery).
        long lenAddr = mmapAddress + offset + 4;
        Unsafe.getUnsafe().putInt(lenAddr, payloadLen);
        if (payloadLen > 0) {
            Unsafe.getUnsafe().copyMemory(payloadAddr, mmapAddress + offset + FRAME_HEADER_SIZE, payloadLen);
        }
        int crc = Crc32c.update(Crc32c.INIT, lenAddr, 4L + payloadLen);
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
     * truncation (corruption) from a normal partial fill (no incident). Sparse
     * holes read back as zeroes; an actual positioned-read failure aborts
     * recovery instead of being classified as a clean tail.
     */
    public long tornTailBytes() {
        return tornTailBytes;
    }

    /**
     * Scans the header and frames through a bounded positioned-read buffer.
     * Sparse file holes are returned by {@code pread}/{@code ReadFile} as zero
     * bytes, while media errors and concurrent truncation surface as ordinary
     * read failures. No recovery byte is dereferenced through mmap.
     */
    private static RecoveryScan scanForRecovery(
            FilesFacade ff,
            int fd,
            String path,
            long fileSize
    ) {
        try (RecoveryReader reader = new RecoveryReader(ff, fd, path, fileSize)) {
            int magic = reader.getInt(0L);
            if (magic != FILE_MAGIC) {
                throw new MmapSegmentCorruptionException(
                        "bad magic in " + path + ": 0x" + Integer.toHexString(magic));
            }
            byte version = reader.getByte(4L);
            if (version != VERSION) {
                // Deliberately NOT the corruption subtype: an unsupported
                // version is a well-formed segment written by a different
                // client build (e.g. after a downgrade). Quarantine-renaming
                // it would strand its frames for the writer that CAN read it;
                // failing recovery keeps the slot intact for that writer.
                throw new MmapSegmentException("unsupported version in " + path + ": " + version);
            }
            long baseSeq = reader.getLong(8L);
            // FSNs are non-negative by construction (see SegmentRing). A
            // negative value on disk is positively identified corruption.
            if (baseSeq < 0L) {
                throw new MmapSegmentCorruptionException(
                        "bad baseSeq in " + path + ": " + baseSeq);
            }

            long frameCount = 0L;
            long pos = HEADER_SIZE;
            while (fileSize - pos >= FRAME_HEADER_SIZE) {
                int crcRead = reader.getInt(pos);
                int payloadLen = reader.getInt(pos + 4L);
                // Avoid addition overflow while rejecting a negative or
                // beyond-EOF frame length as a torn tail.
                if (payloadLen < 0 || payloadLen > fileSize - pos - FRAME_HEADER_SIZE) {
                    break;
                }
                int crcCalc = reader.crc32c(pos + 4L, 4L + payloadLen);
                if (crcCalc != crcRead) {
                    break;
                }
                pos += FRAME_HEADER_SIZE + payloadLen;
                frameCount++;
            }
            long tornTailBytes = detectTornTail(reader, pos, fileSize);
            return new RecoveryScan(baseSeq, frameCount, pos, tornTailBytes);
        }
    }

    /**
     * Distinguishes attempted frame data from clean zero-filled space after the
     * recovery boundary. The entire suffix is inspected: an all-zero corrupt
     * frame header must not hide a non-zero payload or later frames and make a
     * data-bearing segment look like a reusable empty spare. Positioned-read
     * failures are intentionally not swallowed; they are operational failures,
     * not proof of an unwritten tail.
     */
    private static long detectTornTail(RecoveryReader reader, long lastGood, long fileSize) {
        if (lastGood >= fileSize) {
            return 0L;
        }
        return reader.hasNonZero(lastGood, fileSize - lastGood)
                ? fileSize - lastGood
                : 0L;
    }

    private static final class RecoveryReader implements QuietCloseable {
        private final long bufferAddress;
        private final int fd;
        private final FilesFacade filesFacade;
        private final long fileSize;
        private final String path;
        private int bufferLength;
        private long bufferOffset = -1L;

        private RecoveryReader(FilesFacade filesFacade, int fd, String path, long fileSize) {
            this.filesFacade = filesFacade;
            this.fd = fd;
            this.path = path;
            this.fileSize = fileSize;
            this.bufferAddress = Unsafe.malloc(RECOVERY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        @Override
        public void close() {
            Unsafe.free(bufferAddress, RECOVERY_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }

        private int crc32c(long offset, long len) {
            int crc = Crc32c.INIT;
            while (len > 0L) {
                ensure(offset, 1);
                int bufferIndex = (int) (offset - bufferOffset);
                long chunk = Math.min(len, bufferLength - bufferIndex);
                crc = Crc32c.update(crc, bufferAddress + bufferIndex, chunk);
                offset += chunk;
                len -= chunk;
            }
            return crc;
        }

        private void ensure(long offset, int requiredBytes) {
            if (offset >= bufferOffset
                    && offset - bufferOffset <= bufferLength - requiredBytes) {
                return;
            }
            if (offset < 0L || requiredBytes < 0 || offset > fileSize - requiredBytes) {
                throw new MmapSegmentException(
                        "recovery read outside segment " + path
                                + " [offset=" + offset + ", required=" + requiredBytes
                                + ", fileSize=" + fileSize + ']');
            }
            int len = (int) Math.min((long) RECOVERY_BUFFER_SIZE, fileSize - offset);
            readFully(bufferAddress, len, offset);
            bufferOffset = offset;
            bufferLength = len;
        }

        private byte getByte(long offset) {
            ensure(offset, 1);
            return Unsafe.getUnsafe().getByte(bufferAddress + offset - bufferOffset);
        }

        private int getInt(long offset) {
            ensure(offset, Integer.BYTES);
            return Unsafe.getUnsafe().getInt(bufferAddress + offset - bufferOffset);
        }

        private long getLong(long offset) {
            ensure(offset, Long.BYTES);
            return Unsafe.getUnsafe().getLong(bufferAddress + offset - bufferOffset);
        }

        private boolean hasNonZero(long offset, long len) {
            boolean nonZero = false;
            while (len > 0L) {
                ensure(offset, 1);
                int bufferIndex = (int) (offset - bufferOffset);
                int chunk = (int) Math.min(len, bufferLength - bufferIndex);
                long address = bufferAddress + bufferIndex;
                int i = 0;
                while (i < chunk && ((address + i) & (Long.BYTES - 1L)) != 0L) {
                    nonZero |= Unsafe.getUnsafe().getByte(address + i++) != 0;
                }
                while (i <= chunk - Long.BYTES) {
                    nonZero |= Unsafe.getUnsafe().getLong(address + i) != 0L;
                    i += Long.BYTES;
                }
                while (i < chunk) {
                    nonZero |= Unsafe.getUnsafe().getByte(address + i++) != 0;
                }
                offset += chunk;
                len -= chunk;
            }
            return nonZero;
        }

        private void readFully(long address, long len, long offset) {
            long read = 0L;
            while (read < len) {
                long n = filesFacade.read(fd, address + read, len - read, offset + read);
                if (n < 0L) {
                    throw new MmapSegmentException(
                            "could not read SF segment " + path
                                    + " [offset=" + (offset + read)
                                    + ", remaining=" + (len - read)
                                    + ", errno=" + Os.errno() + ']');
                }
                if (n == 0L) {
                    throw new MmapSegmentException(
                            "short read while recovering SF segment " + path
                                    + " [offset=" + (offset + read)
                                    + ", remaining=" + (len - read)
                                    + ", fileSize=" + fileSize + ']');
                }
                if (n > len - read) {
                    throw new MmapSegmentException(
                            "invalid read length while recovering SF segment " + path
                                    + " [offset=" + (offset + read)
                                    + ", requested=" + (len - read)
                                    + ", actual=" + n + ']');
                }
                read += n;
            }
        }
    }

    private static final class RecoveryScan {
        private final long baseSeq;
        private final long frameCount;
        private final long lastGood;
        private final long tornTailBytes;

        private RecoveryScan(long baseSeq, long frameCount, long lastGood, long tornTailBytes) {
            this.baseSeq = baseSeq;
            this.frameCount = frameCount;
            this.lastGood = lastGood;
            this.tornTailBytes = tornTailBytes;
        }
    }
}
