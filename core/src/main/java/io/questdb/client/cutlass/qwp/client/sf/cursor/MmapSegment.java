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
    // Recovery reads the file through a pread window of this size (or the
    // whole file when smaller). Sized so a typical segment scans in a handful
    // of preads; the scan checksums a frame larger than the window in
    // window-sized chunks.
    private static final long RECOVERY_BUF_BYTES = 1L << 20;

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
                        long baseSeq, long initialCursor, long frameCount,
                        boolean memoryBacked, long tornTailBytes) {
        this.path = path;
        this.fd = fd;
        this.mmapAddress = mmapAddress;
        this.sizeBytes = sizeBytes;
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
            return new MmapSegment(displayPath, fd, addr, sizeBytes, baseSeq, HEADER_SIZE, 0, false, 0L);
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
            return new MmapSegment(null, -1, addr, sizeBytes, baseSeq, HEADER_SIZE, 0, true, 0L);
        } catch (Throwable t) {
            Unsafe.free(addr, sizeBytes, MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
    }

    /**
     * Opens an existing segment file for recovery. Validates the header magic
     * / version and scans frames forward verifying each CRC — all through
     * {@code pread} into a private buffer — then mmaps the file RW for further
     * appends. The first bad CRC (or a frame whose declared length runs past
     * the file end, or an unreadable region) is treated as the boundary of
     * recoverable data; both cursors are positioned at the start of that
     * frame. Returns the segment ready for further appends. Throws
     * {@link MmapSegmentException} on header validation failure.
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
     * Facade-aware variant of {@link #openExisting(String)} that takes the file
     * length, open/close, and every recovery read through a {@link FilesFacade}
     * instead of straight off {@link Files}. Production uses
     * {@link FilesFacade#INSTANCE}; the seam exists so recovery's handling of
     * unreadable regions (short reads past a truncation, I/O errors from bad
     * sectors) can be regression-tested on any filesystem.
     * <p>
     * Every recovery-time read goes through {@code pread}
     * ({@link FilesFacade#read(int, long, long, long)}) into a private buffer,
     * never through the mapping. This keeps recovery free of SIGBUS: a sparse
     * hole reads back as zeros (which fails the frame CRC and ends the scan at
     * that boundary), a region past real end-of-file gives a short read, and a
     * media error gives a failed read — all three are ordinary return values
     * with one deterministic outcome per input on every filesystem, JDK, and
     * JIT state. Reading the same regions through the mapping instead would
     * turn them into SIGBUS, which HotSpot converts to an {@code InternalError}
     * whose delivery point under a JIT-compiled caller is imprecise — not
     * reliably catchable by any {@code try/catch} placement. The mapping is
     * created only after the scan completes, for the append path.
     */
    public static MmapSegment openExisting(FilesFacade ff, String path) {
        long fileSize = ff.length(path);
        if (fileSize < HEADER_SIZE) {
            throw new MmapSegmentException("file shorter than header: " + path + " size=" + fileSize);
        }
        int fd = ff.openRW(path);
        if (fd < 0) {
            throw new MmapSegmentException("openRW failed for " + path);
        }
        long addr = Files.FAILED_MMAP_ADDRESS;
        try {
            RecoveryScan scan = scanFile(ff, fd, path, fileSize);
            if (scan.tornTailBytes > 0) {
                LOG.warn("SF segment {}: torn tail of {} bytes at offset {} "
                                + "(file size {}, frames recovered {}). "
                                + "Recovery will overwrite this region on next append; "
                                + "frames past the tear (if any) are discarded. "
                                + "Investigate disk health or unexpected writer crash.",
                        path, scan.tornTailBytes, scan.lastGood, fileSize, scan.frameCount);
            }
            addr = Files.mmap(fd, fileSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + path);
            }
            return new MmapSegment(path, fd, addr, fileSize, scan.baseSeq,
                    scan.lastGood, scan.frameCount, false, scan.tornTailBytes);
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                Files.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
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
                Files.munmap(mmapAddress, sizeBytes, MemoryTag.MMAP_DEFAULT);
            }
            mmapAddress = 0;
        }
        if (fd >= 0) {
            Files.close(fd);
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
     * <p>
     * One case this does NOT count: when the scan stops because the bail-out
     * region is itself unreadable (short read past a truncation, or an I/O
     * error), that region cannot be probed, so this returns {@code 0} even
     * though frames may have been discarded. That outcome is surfaced by the
     * {@code WARN} in {@link #scanFile} instead -- see it for the
     * truncation-vs-media-error caveat.
     */
    public long tornTailBytes() {
        return tornTailBytes;
    }

    /**
     * One-pass recovery scan of the file via {@code pread}: validates the
     * header, walks frames verifying each CRC, and probes the bail-out region
     * for a torn tail. Returns the header fields plus {@code lastGood} (offset
     * just past the last CRC-verified frame), the frame count, and the
     * torn-tail byte count.
     * <p>
     * Reading through {@code pread} instead of the mapping keeps recovery free
     * of SIGBUS by construction: a sparse hole reads back as zeros and fails
     * the frame CRC, a region past real end-of-file gives a short read, and a
     * media error gives a failed read. Each is an ordinary return value with a
     * single deterministic outcome — the boundary of recoverable data — on
     * every filesystem, JDK, and JIT state. The scan is cold (recovery only),
     * so the extra copy over mapped access is immaterial; frame CRCs still go
     * through the native {@link Crc32c}, which is safe here because the buffer
     * is process-private malloc'd memory, never a mapped page.
     */
    private static RecoveryScan scanFile(FilesFacade ff, int fd, String path, long fileSize) {
        long bufSize = Math.min(fileSize, RECOVERY_BUF_BYTES);
        long buf = Unsafe.malloc(bufSize, MemoryTag.NATIVE_DEFAULT);
        try {
            PreadWindow w = new PreadWindow(ff, fd, buf, bufSize, fileSize);
            if (!w.require(0, HEADER_SIZE)) {
                throw new MmapSegmentException(
                        "unreadable header in " + path + " (short read or I/O error at offset 0)");
            }
            int magic = Unsafe.getUnsafe().getInt(w.addrOf(0));
            if (magic != FILE_MAGIC) {
                throw new MmapSegmentException(
                        "bad magic in " + path + ": 0x" + Integer.toHexString(magic));
            }
            byte version = Unsafe.getUnsafe().getByte(w.addrOf(4));
            if (version != VERSION) {
                throw new MmapSegmentException("unsupported version in " + path + ": " + version);
            }
            long baseSeq = Unsafe.getUnsafe().getLong(w.addrOf(8));
            // FSNs are non-negative by construction (see SegmentRing).
            // A negative baseSeq on disk means bit-rot or a malicious file —
            // refuse the segment so SegmentRing.openExisting's narrow catch
            // skips it like any other unreadable .sfa rather than feeding
            // the bad value into Long.compareUnsigned-based contiguity
            // checks (which would place the segment last in baseSeq order
            // and trip the FSN-gap throw, taking the whole recovery down).
            if (baseSeq < 0L) {
                throw new MmapSegmentException(
                        "bad baseSeq in " + path + ": " + baseSeq);
            }

            // Forward scan: lastGood ends up just past the last frame whose
            // CRC verifies. A torn-tail frame (declared length runs past the
            // file end, CRC mismatch, or an unreadable region) leaves both
            // cursors at the start of that frame; the next tryAppend will
            // overwrite it.
            long pos = HEADER_SIZE;
            long count = 0;
            boolean unreadable = false;
            scan:
            while (pos + FRAME_HEADER_SIZE <= fileSize) {
                if (!w.require(pos, FRAME_HEADER_SIZE)) {
                    unreadable = true;
                    break;
                }
                int crcRead = Unsafe.getUnsafe().getInt(w.addrOf(pos));
                int payloadLen = Unsafe.getUnsafe().getInt(w.addrOf(pos + 4));
                // Defensive: a corrupt length field could be enormous or negative,
                // both of which would otherwise overrun the file bounds.
                if (payloadLen < 0 || pos + FRAME_HEADER_SIZE + payloadLen > fileSize) {
                    break;
                }
                // CRC over the (payloadLen, payload) pair, computed
                // incrementally: each Crc32c.update call feeds the next
                // window-sized chunk into the running value, which is
                // bit-identical to one call over the contiguous range
                // (tryAppend writes the CRC the same way).
                long crcPos = pos + 4;
                long remaining = 4L + payloadLen;
                int crc = Crc32c.INIT;
                while (remaining > 0) {
                    int m = (int) Math.min(remaining, bufSize);
                    if (!w.require(crcPos, m)) {
                        unreadable = true;
                        break scan;
                    }
                    crc = Crc32c.update(crc, w.addrOf(crcPos), m);
                    crcPos += m;
                    remaining -= m;
                }
                if (crc != crcRead) {
                    break;
                }
                pos += FRAME_HEADER_SIZE + payloadLen;
                count++;
            }
            if (unreadable) {
                LOG.warn("SF segment recovery: unreadable region at offset {} (file size {}); "
                                + "treating it as the end of recoverable data -- any frames beyond this "
                                + "offset are discarded. The usual causes are a file shorter than its "
                                + "recorded length (truncated under recovery, or size metadata that "
                                + "survived a crash its data blocks did not) or a media error (bad "
                                + "sector); check disk health if this segment was expected to be fully "
                                + "written or if this recurs.",
                        pos, fileSize);
            }

            // Torn-tail probe: distinguishes "writer attempted a write past the
            // last valid frame and failed" (partial write, mid-stream
            // corruption, bit rot) from clean unwritten space. create()
            // truncates the file to size, leaving the tail zero-filled, and the
            // writer only writes non-zero bytes via tryAppend (CRC and length
            // together) — so a non-zero byte at the failed-frame position
            // implies an attempted write, exactly the case operators want
            // flagged. An unreadable probe region was never written, so it is
            // clean unwritten space, not a torn write.
            long torn = 0L;
            if (pos < fileSize && !unreadable) {
                int probe = (int) Math.min(FRAME_HEADER_SIZE, fileSize - pos);
                if (w.require(pos, probe)) {
                    for (int i = 0; i < probe; i++) {
                        if (Unsafe.getUnsafe().getByte(w.addrOf(pos + i)) != 0) {
                            torn = fileSize - pos;
                            break;
                        }
                    }
                }
            }
            return new RecoveryScan(baseSeq, count, pos, torn);
        } finally {
            Unsafe.free(buf, bufSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Sliding {@code pread} window over one file for the recovery scan.
     * {@link #require} repositions the window when the requested span is not
     * already buffered; {@link #addrOf} translates a file offset to the
     * buffered copy's address. A span is unavailable — {@code require} returns
     * {@code false} — when the file ends short of it or a read fails; the
     * caller treats both as the boundary of recoverable data.
     */
    private static final class PreadWindow {
        private final long bufAddr;
        private final long bufSize;
        private final int fd;
        private final FilesFacade ff;
        private final long fileSize;
        private long winLen;
        private long winOff;

        PreadWindow(FilesFacade ff, int fd, long bufAddr, long bufSize, long fileSize) {
            this.ff = ff;
            this.fd = fd;
            this.bufAddr = bufAddr;
            this.bufSize = bufSize;
            this.fileSize = fileSize;
        }

        /** Address of the buffered copy of file offset {@code pos}; call only after {@link #require} returned true for a span covering it. */
        long addrOf(long pos) {
            return bufAddr + (pos - winOff);
        }

        /**
         * Ensures {@code [pos, pos + n)} is buffered, repositioning the window
         * if needed. {@code n} must be {@code <=} the buffer size. Returns
         * false when the span cannot be read fully (short read at EOF, or a
         * read error).
         */
        boolean require(long pos, int n) {
            if (pos >= winOff && pos + n <= winOff + winLen) {
                return true;
            }
            long want = Math.min(bufSize, fileSize - pos);
            long got = 0;
            while (got < want) {
                long r = ff.read(fd, bufAddr + got, want - got, pos + got);
                if (r <= 0) {
                    break;
                }
                got += r;
            }
            winOff = pos;
            winLen = got;
            return n <= got;
        }
    }

    /** Result of {@link #scanFile}: header fields plus scan outcomes. */
    private static final class RecoveryScan {
        final long baseSeq;
        final long frameCount;
        final long lastGood;
        final long tornTailBytes;

        RecoveryScan(long baseSeq, long frameCount, long lastGood, long tornTailBytes) {
            this.baseSeq = baseSeq;
            this.frameCount = frameCount;
            this.lastGood = lastGood;
            this.tornTailBytes = tornTailBytes;
        }
    }
}
