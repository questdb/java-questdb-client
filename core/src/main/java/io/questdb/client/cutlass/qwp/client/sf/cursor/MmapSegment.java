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
    private static final int[] CRC32C_TABLE = buildCrc32cTable();
    private static final Logger LOG = LoggerFactory.getLogger(MmapSegment.class);

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
     * Opens an existing segment file for recovery. mmaps it RW, validates the
     * header magic / version, then scans frames forward verifying each CRC.
     * The first bad CRC (or a frame whose declared length runs past the file
     * end) is treated as a torn tail; both cursors are positioned at the
     * start of that frame. Returns the segment ready for further appends.
     * Throws {@link MmapSegmentException} on header validation failure.
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
     * length (and open/close) through a {@link FilesFacade} instead of straight
     * off {@link Files}. Production uses {@link FilesFacade#INSTANCE}; the seam
     * exists so recovery's mmap-fault guard can be regression-tested on any
     * filesystem.
     * <p>
     * The mapping is sized to {@code ff.length(path)} and every recovery read
     * runs straight out of it, so a facade that reports a length <em>larger</em>
     * than the real file makes the mapping extend past end-of-file. A read of a
     * page beyond real EOF raises SIGBUS on <em>every</em> filesystem — the same
     * fault an unbacked/sparse page raises on ZFS, but reproduced
     * deterministically on ext4/xfs, where a within-EOF hole would instead
     * zero-fill and never exercise the guard. See
     * {@link FilesFacade#length(String)}.
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
            addr = Files.mmap(fd, fileSize, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + path);
            }
            int magic = Unsafe.getUnsafe().getInt(addr);
            if (magic != FILE_MAGIC) {
                throw new MmapSegmentException(
                        "bad magic in " + path + ": 0x" + Integer.toHexString(magic));
            }
            byte version = Unsafe.getUnsafe().getByte(addr + 4);
            if (version != VERSION) {
                throw new MmapSegmentException("unsupported version in " + path + ": " + version);
            }
            long baseSeq = Unsafe.getUnsafe().getLong(addr + 8);
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
            long lastGood = scanFrames(addr, fileSize);
            long count = countFrames(addr, lastGood);
            long tornTail = detectTornTail(addr, lastGood, fileSize);
            if (tornTail > 0) {
                LOG.warn("SF segment {}: torn tail of {} bytes at offset {} "
                                + "(file size {}, frames recovered {}). "
                                + "Recovery will overwrite this region on next append; "
                                + "frames past the tear (if any) are discarded. "
                                + "Investigate disk health or unexpected writer crash.",
                        path, tornTail, lastGood, fileSize, count);
            }
            return new MmapSegment(path, fd, addr, fileSize, baseSeq, lastGood, count, false, tornTail);
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                Files.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
            }
            ff.close(fd);
            // The header reads above (magic/version/baseSeq) run before
            // scanFrames and are otherwise unguarded. An unbacked page 0 --
            // an unflushed header on a truncate-based-allocate filesystem
            // after a crash (create() does not msync), or a file truncated
            // under the mapping -- faults at the Unsafe intrinsic site as a
            // catchable InternalError. Convert it to a MmapSegmentException so
            // SegmentRing's per-file catch skips just this .sfa, instead of
            // letting the raw InternalError escape to SegmentRing's outer catch
            // and abort recovery of every sibling segment. scanFrames and
            // detectTornTail already handle their own in-mapping faults; this
            // covers the header block and any future reader placed ahead of the
            // scan.
            if (isMmapAccessFault(t)) {
                throw new MmapSegmentException(
                        "unreadable mapped header page in " + path
                                + " (unbacked/sparse page 0): " + t.getMessage(), t);
            }
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
     * region is itself an unbacked mapped page (an in-mapping fault, not a
     * backed torn header), that region cannot be probed, so this returns
     * {@code 0} even though frames may have been discarded. That outcome is
     * surfaced by the {@code WARN} in {@link #scanFrames} instead -- see it for
     * the benign-tail-vs-media-error caveat.
     */
    public long tornTailBytes() {
        return tornTailBytes;
    }

    /**
     * True when {@code t} is the JVM's recoverable signal for a fault while
     * accessing a memory-mapped region -- a SIGBUS/SIGSEGV that HotSpot
     * translates into an {@code InternalError} at an {@code Unsafe} intrinsic
     * site instead of aborting the process. It surfaces when a mapped page is
     * not backed by real file blocks: a sparse {@code .sfa} tail on a
     * filesystem whose pre-allocation leaves holes (e.g. ZFS, where a
     * truncate-based {@code allocate} does not materialize blocks), or a file
     * truncated under the mapping. Recovery treats this as an I/O boundary --
     * the same way MappedByteBuffer readers do -- not a fatal VM error.
     * <p>
     * The message is matched on the fragment {@code "unsafe memory access
     * operation"}, which is common to both HotSpot wordings and NOT
     * version-stable as a whole: pre-21 JDKs (including the shipping/CI JDK 8)
     * emit {@code "a fault occurred in a recent unsafe memory access operation
     * in compiled Java code"}, while JDK 21+ shortened it to {@code "a fault
     * occurred in an unsafe memory access operation"}. Matching the shared
     * fragment keeps the guard effective on JDK 8/11/17 as well as 21+, while
     * still being specific enough that a genuine VirtualMachineError (real OOM,
     * StackOverflow) is never swallowed.
     */
    private static boolean isMmapAccessFault(Throwable t) {
        if (!(t instanceof InternalError)) {
            return false;
        }
        String msg = t.getMessage();
        return msg != null && msg.contains("unsafe memory access operation");
    }

    /**
     * Forward scan that returns the offset just past the last frame whose
     * CRC verifies. A torn-tail frame (declared length runs past EOF, or
     * CRC mismatch) leaves both cursors at the start of that frame; the
     * next {@link #tryAppend} will overwrite it. The scan only reads from
     * the mapping — no syscalls.
     */
    private static long scanFrames(long addr, long fileSize) {
        long pos = HEADER_SIZE;
        try {
            while (pos + FRAME_HEADER_SIZE <= fileSize) {
                int crcRead = Unsafe.getUnsafe().getInt(addr + pos);
                int payloadLen = Unsafe.getUnsafe().getInt(addr + pos + 4);
                // Defensive: a corrupt length field could be enormous or negative,
                // both of which would otherwise overrun the mapping.
                if (payloadLen < 0 || pos + FRAME_HEADER_SIZE + payloadLen > fileSize) {
                    return pos;
                }
                // CRC over the contiguous (payloadLen, payload) pair, folded
                // via Unsafe reads rather than the native Crc32c.update.
                // Recovery maps to the file's stat length, but a page inside
                // that range can be unbacked: a sparse pre-allocation tail (a
                // truncate-based allocate that never materialized blocks, as on
                // ZFS), or -- via a torn write, since tryAppend writes the
                // length field before copying the payload -- a real positive
                // payloadLen whose payload spans into an unwritten hole. A raw
                // read of an unbacked page raises SIGBUS; HotSpot translates
                // that into a catchable InternalError ONLY at an Unsafe
                // intrinsic site, NEVER inside JNI native code, so a native
                // Crc32c.update over such a page aborts the whole JVM (and,
                // empirically on pre-21 JDKs, a preceding Unsafe pre-touch does
                // not reliably fault first once an earlier native CRC ran in
                // the same scan). Folding over Unsafe keeps every fault
                // catchable -- handled below as the boundary of recoverable
                // data; a page that instead reads back as zeroes just fails the
                // CRC check and ends the scan. Recovery is cold, so the slower
                // table CRC here is immaterial.
                int crcCalc = crc32cRecovery(addr + pos + 4, 4L + payloadLen);
                if (crcCalc != crcRead) {
                    return pos;
                }
                pos += FRAME_HEADER_SIZE + payloadLen;
            }
        } catch (InternalError e) {
            // The read at `pos` hit a mapped page that is not backed by real
            // file blocks: the JVM translates the underlying SIGBUS into a
            // recoverable InternalError instead of aborting the process. This
            // happens when a prior session left a sparse segment tail (a
            // truncate-based pre-allocation that does not materialize blocks,
            // as on ZFS) or the file was truncated under the mapping. Every
            // frame below `pos` already verified; treat the unreadable region
            // exactly like unwritten space or a torn tail -- the boundary of
            // recoverable data -- rather than letting the error abort recovery
            // of the whole slot. Anything that is not the documented mmap
            // access fault is a genuine VM error, so rethrow it.
            if (!isMmapAccessFault(e)) {
                throw e;
            }
            LOG.warn("SF segment recovery: unreadable mapped page at offset {} (file size {}); "
                            + "treating it as the end of recoverable data -- any frames beyond this "
                            + "offset are discarded. The usual cause is a benign sparse pre-allocation "
                            + "tail (e.g. a truncate-based allocate on ZFS) left by a prior session, "
                            + "but a mid-file media error (bad sector) is indistinguishable here; "
                            + "check disk health if this segment was expected to be fully written or "
                            + "if this recurs.",
                    pos, fileSize);
        }
        return pos;
    }

    /**
     * CRC-32C (Castagnoli) of {@code [addr, addr + len)} read through
     * {@link Unsafe}, seeded like {@code Crc32c.update(Crc32c.INIT, addr, len)}
     * and bit-identical to it (verified) -- but every byte load is an Unsafe
     * intrinsic, so a fault on an unbacked mapped page is a catchable
     * {@link InternalError} instead of the uncatchable JNI SIGBUS the native
     * {@link Crc32c} would raise. Byte-at-a-time via a precomputed table
     * ({@code ~0.5 GiB/s}); used only on the cold recovery scan, never on the
     * append hot path (which stays on the native, hardware-friendly path).
     */
    private static int crc32cRecovery(long addr, long len) {
        int crc = ~Crc32c.INIT;
        for (long i = 0; i < len; i++) {
            crc = (crc >>> 8) ^ CRC32C_TABLE[(crc ^ Unsafe.getUnsafe().getByte(addr + i)) & 0xFF];
        }
        return ~crc;
    }

    /**
     * Standard reflected CRC-32C byte table (polynomial {@code 0x82F63B78}),
     * matching {@code crc32c_table[0]} in the native {@code crc32c.c}. Computed
     * at class init to avoid 256 hand-transcribed literals; drives
     * {@link #crc32cRecovery}.
     */
    private static int[] buildCrc32cTable() {
        int[] table = new int[256];
        for (int n = 0; n < 256; n++) {
            int c = n;
            for (int k = 0; k < 8; k++) {
                c = (c & 1) != 0 ? (0x82F63B78 ^ (c >>> 1)) : (c >>> 1);
            }
            table[n] = c;
        }
        return table;
    }

    /**
     * Distinguishes "torn tail" (writer attempted a write past the last valid
     * frame and failed — partial write, mid-stream corruption, bit rot) from
     * clean unwritten space (manager-allocated segment with zero-filled tail).
     * Returns the byte count from {@code lastGood} to {@code fileSize} when
     * the bytes at the bail-out frame header are non-zero, else {@code 0}.
     * <p>
     * Heuristic but robust for the common cases: {@link #create} truncates the
     * file to size, leaving the tail zero-filled; the writer only writes
     * non-zero bytes via {@link #tryAppend}, which writes the CRC and length
     * fields together. So a non-zero byte at the failed-frame position
     * implies an attempted write — exactly the case operators want flagged.
     */
    private static long detectTornTail(long addr, long lastGood, long fileSize) {
        if (lastGood >= fileSize) {
            return 0L;
        }
        long probe = Math.min(FRAME_HEADER_SIZE, fileSize - lastGood);
        try {
            for (long i = 0; i < probe; i++) {
                if (Unsafe.getUnsafe().getByte(addr + lastGood + i) != 0) {
                    return fileSize - lastGood;
                }
            }
        } catch (InternalError e) {
            // The bail-out region is an unbacked (sparse) mapped page -- see
            // scanFrames for the mechanism. An unbacked hole was never written,
            // so it is clean unwritten space, not a torn write. Rethrow any
            // error that is not the recoverable mmap access fault.
            if (!isMmapAccessFault(e)) {
                throw e;
            }
            return 0L;
        }
        return 0L;
    }

    /**
     * Counts frames in {@code [HEADER_SIZE, lastGood)}. Walks the framing in
     * lockstep with {@link #scanFrames} (which already validated CRCs); so
     * this is just length-driven traversal, no CRC re-check.
     */
    private static long countFrames(long addr, long lastGood) {
        long pos = HEADER_SIZE;
        long count = 0;
        while (pos < lastGood) {
            int payloadLen = Unsafe.getUnsafe().getInt(addr + pos + 4);
            pos += FRAME_HEADER_SIZE + payloadLen;
            count++;
        }
        return count;
    }
}
