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

import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoValidator;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

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
 *   [u32 magic 'SF01'] [u8 ver=2] [u8 flags]   [u16 reserved=0]
 *   [u64 baseSeq]      [u64 createdMicros]                        24-byte header
 *   frame, frame, ...                                              each frame:
 *                                                                  [u32 crc32c]
 *                                                                  [u32 payloadLen]
 *                                                                  [payloadLen bytes]
 *   crc32c covers (payloadLen, payload).
 * </pre>
 * Version 2 adds a second kind of frame, the QWO1 record, told apart by the
 * {@link QwoFormat#MAGIC_RECORD} that starts its payload. The producer reserves
 * an open record {@code [u32 STATE_OPEN][u32 capacity][capacity bytes]} past the
 * published bytes ({@link #reserveRecord}), fills it in place, and seals it into
 * {@code [u32 STATE_SEALED][u32 usedBytes][usedBytes bytes]}
 * ({@link #sealReserved}); the record's own CRCs replace the frame CRC. A sealed
 * record counts as one frame. Recovery opens versions {@link #MIN_VERSION}
 * through {@link #VERSION}. It only reports an open record left by a crash;
 * {@link #sealRecoveredOpenRecord()} seals one that holds committed rows.
 * <p>
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
    public static final byte MIN_VERSION = 1;
    public static final byte VERSION = 2;
    private static final Logger LOG = LoggerFactory.getLogger(MmapSegment.class);
    // Deduplicates the process-wide "mlock refused" warning: the refusal is a
    // soft downgrade (see syncPublished) and must not spam the log once per
    // barrier when RLIMIT_MEMLOCK or the platform says no.
    private static final AtomicBoolean MLOCK_REFUSAL_WARNED = new AtomicBoolean();
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
    // Bumped by the producer after each reserve, seal or abandon dirties bytes
    // above publishedCursor (and by the recovery seal). syncPublished records
    // the value it started from on success, so a later barrier knows whether
    // anything changed since, even when publishedCursor did not move.
    private volatile long dirtyMarks;
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
    // The open record openExisting found at the end of the recovered frames,
    // as QwoValidator.validateOpen judged it. Only observed: the record is
    // still open on disk and counted in tornTailBytes. Written by openExisting
    // and sealRecoveredOpenRecord on the recovery path (single-threaded).
    private boolean isRecoveredOpenRecordValid;
    private long mmapAddress;
    // publishedCursor: written by producer, read by consumer (I/O thread). Volatile
    // because the consumer must see writes in publication order — once the
    // producer bumps publishedCursor, every byte before it is fully written.
    private volatile long publishedCursor;
    private int recoveredOpenRecordCapacity;
    private String recoveredOpenRecordDetail;
    // Envelope offset of the open record, -1 when openExisting found none.
    private long recoveredOpenRecordOffset = -1L;
    private int recoveredOpenRecordRows;
    private int recoveredOpenRecordUsedBytes;
    // Number of failure-arm re-dirty passes performed by syncPublished.
    // Cold-path only; volatile so tests observing from another thread see it.
    private volatile long redirtyPasses;
    // Payload capacity of the record reserved at reservedOffset; 0 when none.
    // Producer thread only.
    private int reservedCapacity;
    // End of the reserved record ([reservedOffset, reservedCursor)) while one is
    // reserved, else at or below appendCursor. Written by the producer, read by
    // the durability barrier (syncPublished), which flushes the open record's
    // committed rows although they sit above publishedCursor.
    private volatile long reservedCursor;
    // Envelope offset of the open record the producer is filling, -1 when none.
    // Written by the producer; volatile because syncPublished reads it to
    // flush a reserved record on every barrier.
    private volatile long reservedOffset = -1L;
    // Monotonic in-memory link to the segment that immediately follows this
    // one. SegmentRing publishes it before promoting the successor to active;
    // close deliberately retains it so a cursor can advance after head trim.
    private volatile MmapSegment successor;
    // End of the furthest range a reserve, seal or abandon has dirtied above
    // publishedCursor (a reservation's full extent). Monotonic; written by the
    // producer before it bumps dirtyMarks, read by syncPublished, so the
    // barrier still covers a seal's zeroed tail or an abandoned reservation
    // once reservedCursor has moved back.
    private volatile long syncHighWater;
    // dirtyMarks as read at the start of the last successful barrier.
    private volatile long syncedDirtyMarks;
    // Bytes between the last valid frame and the file end that look like an
    // attempted-but-invalid frame write (the suffix contains non-zero bytes).
    // Zero for fresh segments and for cleanly partially-filled
    // segments (uninitialised tail). Set only by openExisting; visible to
    // recovery callers for diagnostics. openExisting only OBSERVES the
    // residue -- it never mutates the file: after a mid-file tear the suffix
    // can hold unreachable valid-CRC frames that are the only surviving copy
    // of real payloads. Ring recovery decides after the chain fully validates
    // whether to zero it (sanitizeTornTail on the resumed active or on a
    // proven-dead sealed suffix) or to fail closed leaving the bytes on disk
    // for operator extraction. An open record openExisting found counts as
    // residue; sealRecoveredOpenRecord, the one recovery write, measures the
    // tail again past the record it seals. Otherwise fixed after construction.
    private long tornTailBytes;
    // Set once sanitizeTornTail has durably zeroed the observed residue.
    // Written and read only on the recovery path (single-threaded, before
    // the segment is published to producer/consumer threads).
    private boolean tornTailSanitized;

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
        this.reservedCursor = initialCursor;
        this.syncHighWater = initialCursor;
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

    static MmapSegment create(FilesFacade ff, String path, long baseSeq, long sizeBytes,
                              boolean manifestRequired) {
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
    public static MmapSegment create(FilesFacade ff, long pathPtr, String displayPath, long baseSeq,
                                     long sizeBytes) {
        return create(ff, pathPtr, displayPath, baseSeq, sizeBytes, false);
    }

    static MmapSegment create(FilesFacade ff, long pathPtr, String displayPath, long baseSeq, long sizeBytes,
                              boolean manifestRequired) {
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
            // Unlink the partially-created file so a sf_max_segment_bytes-sized
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
            return new MmapSegment(ff, displayPath, fd, addr, sizeBytes, baseSeq,
                    HEADER_SIZE, 0, false, 0L);
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
            return new MmapSegment(null, null, -1, addr, sizeBytes, baseSeq,
                    HEADER_SIZE, 0, true, 0L);
        } catch (Throwable t) {
            Unsafe.free(addr, sizeBytes, MemoryTag.NATIVE_DEFAULT);
            throw t;
        }
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
     * <p>
     * <b>Delivery is NOT precise before JDK 21, so callers must guard too.</b>
     * Pre-21 HotSpot records the fault and raises the {@code InternalError} at
     * the next return or safepoint check rather than at the faulting
     * instruction, so it can surface in a CALLER's frame, past the handler that
     * wraps the faulting read. JDK-8283699 made delivery precise in 21+.
     * <p>
     * Segment recovery scans frames through positioned reads into a heap-free
     * bounded buffer and only maps the file once the scan has validated it, so
     * a sparse or unbacked page under a frame surfaces as an ordinary read
     * failure rather than a SIGBUS. QWO1 records are the exception: their
     * CRCs are checked on the mapping, and {@link #openExisting} turns a fault
     * this predicate recognises there into an {@link MmapSegmentException}.
     * It stays public for the mappings that ARE still folded over
     * {@code Crc32c.updateUnsafe}: {@code PersistedSymbolDict}'s append and
     * heal paths, whose handlers in {@code QwpWebSocketSender} apply this
     * exemption ahead of their own {@code Error} rethrow so a recognised fault
     * degrades the sender instead of escaping as a raw {@code InternalError}.
     */
    public static boolean isMmapAccessFault(Throwable t) {
        if (!(t instanceof InternalError)) {
            return false;
        }
        String msg = t.getMessage();
        return msg != null && msg.contains("unsafe memory access operation");
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
     * rather than clean unwritten space), a {@code WARN} is emitted with the
     * byte count and the observed size is exposed via {@link #tornTailBytes()}.
     * The residue itself is NOT touched: after a mid-file tear the suffix can
     * still hold unreachable frames with valid CRCs -- the only surviving copy
     * of real payloads -- so whether it may be destroyed is a chain-level
     * decision this method cannot make. {@link SegmentRing} recovery invokes
     * {@link #sanitizeTornTail()} only once the chain has fully validated,
     * and the justification differs by role: sealed suffixes are zeroed on
     * PROOF (frame accounting validated complete, so the residue can hold no
     * replayable frame -- a tear that cost frames fails closed instead, with
     * every byte left on disk for operator extraction), while the resumed
     * active's tail is zeroed by POLICY -- past a mid-file tear it can still
     * hold valid-CRC frames of real unacked payloads, but they are
     * unreachable by replay (the FSN sequence breaks at the tear) and
     * leaving them risks the reseal-brick and stale-frame-resurrection
     * hazards. Clean
     * partial fills (writer never attempted to write past the last valid
     * frame) do not log and report {@code 0}.
     * <p>
     * QWO1 records are validated on the mapping once it exists, without
     * writing to it. A sealed record that fails
     * {@link QwoValidator#validateSealed} ends the recovered frames at its
     * offset, the same as a frame with a bad CRC. An open record at the end of
     * the frames, left by a crash before its seal, is judged by
     * {@link QwoValidator#validateOpen} and reported
     * ({@link #hasRecoverableOpenRecord()}, {@link #hasInvalidOpenRecord()},
     * {@link #recoveredOpenRecordRows()}, {@link #recoveredOpenRecordDetail()}),
     * but stays open on disk and counts as torn tail. Ring recovery calls
     * {@link #sealRecoveredOpenRecord()} on the resumed active only; anywhere
     * else the record is residue like any other.
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
            try {
                scan = validateRecordsAfterMap(path, addr, fileSize, scan);
            } catch (InternalError e) {
                if (!isMmapAccessFault(e)) {
                    throw e;
                }
                throw new MmapSegmentException("mapped-memory fault while validating records in " + path, e);
            }
            // A torn tail that is exactly an open record with committed rows is the
            // designed outcome of a crash with a record open, not an incident: the
            // ring seals it on the resumed active (and logs that) and fails closed on
            // it anywhere else, so it gets no disk-health warning.
            if (scan.tornTailBytes > 0 && !isRecoverableOpenRecordTail(addr, fileSize, scan)) {
                // Observe-only: report the residue, never touch it here. A
                // torn tail may be an interrupted append (dead bytes) OR a
                // mid-file tear with unreachable valid-CRC frames past it --
                // the only surviving copy of real payloads. The
                // destroy/preserve decision belongs to SegmentRing.recover.
                // For sealed members chain validation PROVES the residue
                // dead (complete frame accounting) before sanitizeTornTail
                // runs, and a tear that cost frames fails closed with every
                // byte left on disk. For the segment resumed as active no
                // such proof exists (there is no successor to bound it): its
                // residue is zeroed by policy once the rest of the chain
                // validates, because replay cannot cross the tear and
                // unzeroed residue risks reseal-brick and stale-frame
                // resurrection.
                LOG.warn("SF segment {}: torn tail of {} bytes at offset {} "
                                + "(file size {}, frames recovered {}). "
                                + "The residue is preserved pending chain validation. "
                                + "Investigate disk health or unexpected writer crash.",
                        path, scan.tornTailBytes, scan.lastGood, fileSize, scan.frameCount);
            }
            MmapSegment segment = new MmapSegment(
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
            segment.recoveredOpenRecordOffset = scan.openRecordOffset;
            segment.recoveredOpenRecordCapacity = scan.openRecordCapacity;
            segment.recoveredOpenRecordRows = scan.openRecordRows;
            segment.recoveredOpenRecordUsedBytes = scan.openRecordUsedBytes;
            segment.isRecoveredOpenRecordValid = scan.isOpenRecordValid;
            segment.recoveredOpenRecordDetail = scan.openRecordDetail;
            return segment;
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                ff.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
            }
            ff.close(fd);
            throw t;
        }
    }

    /**
     * Gives back the record reserved at {@code off}: zeroes the whole envelope
     * {@code [off, off + 8 + capacity)} and moves the append cursor back to
     * {@code off}, restoring the zero-fill invariant past the append cursor.
     * Nothing was published, so the consumer never saw the record, and the
     * next {@link #syncPublished()} flushes the zeroed range. Producer thread
     * only.
     * <p>
     * Abandon only a record that holds no committed rows: an earlier barrier
     * may already have made those rows durable, and abandoning them loses
     * them. The sender enforces this; the segment does not check it.
     *
     * @throws IllegalStateException when no record is reserved at {@code off}
     */
    public void abandonReserved(long off) {
        checkReserved(off);
        long end = off + FRAME_HEADER_SIZE + reservedCapacity;
        Unsafe.getUnsafe().setMemory(mmapAddress + off, end - off, (byte) 0);
        appendCursor = off;
        markDirty(end);
        reservedCursor = off;
        reservedOffset = -1L;
        reservedCapacity = 0;
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

    /**
     * True when the last successful {@link #syncPublished()} left nothing for
     * the next one to flush: every published byte is durable, no record is
     * reserved, and no reserve, seal or abandon has changed bytes above the
     * published cursor since. Stricter than {@link #isPublishedDurable()},
     * which ignores those changes; the ring gates rotation and the periodic
     * barrier's proven-durable prefix on this, so an abandoned record's zeroes
     * reach the disk before its segment is sealed. Reads reservedOffset before
     * dirtyMarks, as the barrier does, and syncedDirtyMarks before
     * durableCursor, the reverse of the order the barrier writes them, so a
     * caller that sees a barrier's marks also sees its durable cursor.
     */
    public boolean isDurable() {
        return reservedOffset < 0
                && dirtyMarks == syncedDirtyMarks
                && durableCursor >= publishedCursor;
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
     * <p>
     * The not-yet-durable range is pinned with a best-effort {@code mlock}
     * for the duration of the barrier and, on failure, re-dirtied before the
     * pin is released. Rationale (fsyncgate): after a failed writeback the
     * kernel marks the affected pages clean and reports the error once per
     * open file description, so a naive retry would msync+fsync clean pages
     * and return a vacuous 0 without persisting anything -- and an unpinned
     * clean page can be reclaimed and later re-faulted from stale disk
     * content, silently replacing the only good copy of the frames. The pin
     * guarantees the failure arm re-dirties the genuine in-memory bytes; the
     * re-dirty guarantees the next barrier performs real writeback and
     * reports real errors, so the manager's unlatch-on-success stays honest.
     * A refused mlock (RLIMIT_MEMLOCK, missing capability, stale native
     * library) never affects the barrier outcome; it only widens the
     * microseconds-scale window between the failed syscall and the re-dirty.
     * <p>
     * The barrier also covers bytes above {@code publishedCursor} that the
     * producer changed: a reserved record, whose committed rows live there
     * long before it is sealed and published, and the full extent of a record
     * since sealed (its zeroed tail) or abandoned. A reserved record changes in
     * place, so every barrier flushes all of it again while it stays reserved;
     * otherwise a barrier with nothing published and nothing changed since the
     * last successful one returns at once. The returned offset and
     * {@link #isPublishedDurable()} still describe only the published bytes.
     *
     * @return the captured byte offset covered by the successful barrier
     */
    public long syncPublished() {
        long published = publishedCursor;
        if (memoryBacked) {
            durableCursor = published;
            syncedDirtyMarks = dirtyMarks;
            return published;
        }
        // The producer stores the extent and the marks (markDirty) before
        // reservedCursor and reservedOffset. Reading reservedOffset first, then
        // the marks, then the extents means a barrier that sees a record
        // reserved or released also sees the marks and the extent that left.
        boolean isReserved = reservedOffset >= 0;
        long marks = dirtyMarks;
        if (published <= durableCursor && !isReserved && marks == syncedDirtyMarks) {
            return durableCursor;
        }
        long end = Math.max(published, Math.max(reservedCursor, syncHighWater));
        long lockOffset = durableCursor & -Files.PAGE_SIZE;
        long lockAddr = mmapAddress + lockOffset;
        long lockLen = end - lockOffset;
        boolean locked = filesFacade.mlock(lockAddr, lockLen) == 0;
        if (!locked && MLOCK_REFUSAL_WARNED.compareAndSet(false, true)) {
            LOG.warn("mlock refused for SF barrier range in {} (degrading to re-dirty-only retry protection); "
                    + "raise RLIMIT_MEMLOCK or grant CAP_IPC_LOCK to close the post-failure reclaim window", path);
        }
        boolean durable = false;
        try {
            if (filesFacade.msync(mmapAddress, end, false) != 0) {
                throw new MmapSegmentException("could not sync segment data " + path);
            }
            // FlushViewOfFile alone is not power-loss durable on Windows. Keep the
            // fd barrier on every platform so this method has one portable contract.
            if (filesFacade.fsync(fd) != 0) {
                throw new MmapSegmentException("could not sync segment file " + path);
            }
            durable = true;
            durableCursor = published;
            syncedDirtyMarks = marks;
            return published;
        } finally {
            if (!durable) {
                redirtyRange(lockAddr, mmapAddress + published, mmapAddress + end);
            }
            if (locked) {
                filesFacade.munlock(lockAddr, lockLen);
            }
        }
    }

    private void checkReserved(long off) {
        if (off < 0 || off != reservedOffset) {
            throw new IllegalStateException("no record reserved at offset " + off
                    + " [reservedOffset=" + reservedOffset + ']');
        }
    }

    /**
     * Records that the bytes up to {@code end} changed above publishedCursor:
     * raises {@code syncHighWater}, then bumps {@code dirtyMarks}, so the next
     * barrier covers them. Single writer (the producer, or recovery before the
     * segment is shared).
     */
    private void markDirty(long end) {
        if (end > syncHighWater) {
            syncHighWater = end;
        }
        dirtyMarks = dirtyMarks + 1;
    }

    /**
     * Marks every page overlapping {@code [fromAddr, endAddr)} dirty again by
     * re-storing one word per page; kernel dirty tracking is value-blind, so
     * the store is a real dirtying event that forces the next writeback to
     * re-submit the whole page. A page starting below {@code publishedAddr}
     * starts inside the published prefix (or the immutable header magic after
     * aligning down), so a plain same-value store races with nothing. A page
     * starting at or above it lies in a reserved record the producer may be
     * writing right now; an atomic add of zero dirties it without losing a
     * concurrent store.
     */
    private void redirtyRange(long fromAddr, long publishedAddr, long endAddr) {
        for (long addr = fromAddr; addr < endAddr; addr += Files.PAGE_SIZE) {
            if (addr < publishedAddr) {
                Unsafe.getUnsafe().putByte(addr, Unsafe.getUnsafe().getByte(addr));
            } else {
                Unsafe.getUnsafe().getAndAddInt(null, addr, 0);
            }
        }
        redirtyPasses++;
    }

    /**
     * Seals the open record at {@code off} on the mapping: zeroes
     * {@code [off + 8 + usedBytes, off + 8 + capacity)}, stores the length,
     * then {@code STATE_SEALED}.
     */
    private void sealInPlace(long off, int capacity, int usedBytes) {
        long payload = mmapAddress + off + FRAME_HEADER_SIZE;
        Unsafe.getUnsafe().setMemory(payload + usedBytes, capacity - usedBytes, (byte) 0);
        Unsafe.getUnsafe().putInt(mmapAddress + off + 4, usedBytes);
        Unsafe.getUnsafe().putInt(mmapAddress + off, QwoFormat.STATE_SEALED);
    }

    @TestOnly
    public long redirtyPassesForTest() {
        return redirtyPasses;
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
     * Address of the payload of the frame or record whose envelope starts at
     * {@code envelopeOffset}.
     */
    public long payloadAddress(long envelopeOffset) {
        return mmapAddress + envelopeOffset + FRAME_HEADER_SIZE;
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

    /**
     * Why the open record {@link #openExisting} found failed
     * {@link QwoValidator#validateOpen} (it names the block, table and column
     * where one applies); null when there was none or it was valid.
     */
    public String recoveredOpenRecordDetail() {
        return recoveredOpenRecordDetail;
    }

    /**
     * Envelope offset of the open record {@link #openExisting} found at the end
     * of the recovered frames; -1 when there was none or once
     * {@link #sealRecoveredOpenRecord()} sealed it.
     */
    public long recoveredOpenRecordOffset() {
        return recoveredOpenRecordOffset;
    }

    /**
     * Committed rows in the open record {@link #openExisting} found; 0 when
     * there was none or it failed validation.
     */
    public int recoveredOpenRecordRows() {
        return recoveredOpenRecordRows;
    }

    /**
     * Reserves an open record of {@code payloadCapacity} payload bytes at the
     * append cursor: stores {@code [STATE_OPEN][payloadCapacity]} and moves the
     * append cursor past the reservation. The payload is zero (the invariant
     * past the append cursor) and stays unpublished; the producer fills it at
     * {@link #payloadAddress(long)} and ends it with {@link #sealReserved} or
     * {@link #abandonReserved}. {@link #syncPublished()} flushes the reservation
     * with the published bytes. Producer thread only.
     *
     * @return the record's envelope offset, or -1 with nothing written when the
     * segment cannot fit {@code 8 + payloadCapacity} more bytes
     * @throws IllegalStateException    when a record is already reserved
     * @throws IllegalArgumentException when {@code payloadCapacity} is smaller
     *                                  than the QWO1 record header
     */
    public long reserveRecord(int payloadCapacity) {
        if (reservedOffset >= 0) {
            throw new IllegalStateException("record reserved at offset " + reservedOffset);
        }
        if (payloadCapacity < QwoFormat.HEADER_SIZE) {
            throw new IllegalArgumentException("record capacity below the record header: " + payloadCapacity);
        }
        long off = appendCursor;
        if (off + FRAME_HEADER_SIZE + payloadCapacity > sizeBytes) {
            return -1L;
        }
        Unsafe.getUnsafe().putInt(mmapAddress + off, QwoFormat.STATE_OPEN);
        Unsafe.getUnsafe().putInt(mmapAddress + off + 4, payloadCapacity);
        appendCursor = off + FRAME_HEADER_SIZE + payloadCapacity;
        markDirty(appendCursor);
        reservedCursor = appendCursor;
        reservedCapacity = payloadCapacity;
        reservedOffset = off;
        return off;
    }

    /**
     * Payload capacity of the reserved record; 0 when none is reserved.
     */
    public int reservedCapacity() {
        return reservedCapacity;
    }

    /**
     * Envelope offset of the reserved record; -1 when none is reserved.
     */
    public long reservedOffset() {
        return reservedOffset;
    }

    /**
     * Seals the open record {@link #openExisting} found and judged recoverable
     * ({@link #hasRecoverableOpenRecord()}), and is the one write recovery makes
     * to a QWO1 record. It validates the record again, clears DEFER_COMMIT and
     * fixes blockCount and usedBytes ({@link QwoValidator#finalizeForRecovery}),
     * zeroes the tail past the validator's usedBytes, stores the length, then
     * {@code STATE_SEALED}. The record then counts as a published frame, both
     * cursors move to its end, and the torn tail is measured again past it
     * (normally 0). The next {@link #syncPublished()} flushes it.
     * <p>
     * Only ring recovery calls this, and only on the segment it resumes as the
     * active, after the chain has validated and before
     * {@link #sanitizeTornTail()}. In any other member an open record is
     * residue that fails closed like any torn tail.
     *
     * @return the record's envelope offset
     * @throws IllegalStateException when there is no recoverable open record,
     *                               appends have resumed, or the record changed since {@link #openExisting}
     */
    public long sealRecoveredOpenRecord() {
        if (!hasRecoverableOpenRecord()) {
            throw new IllegalStateException("no recoverable open record in " + path);
        }
        long off = recoveredOpenRecordOffset;
        int capacity = recoveredOpenRecordCapacity;
        if (appendCursor != off || reservedOffset >= 0) {
            throw new IllegalStateException("appends resumed before the open record at offset " + off
                    + " was sealed in " + path + " [appendCursor=" + appendCursor + ']');
        }
        long payload = mmapAddress + off + FRAME_HEADER_SIZE;
        QwoValidator validator = new QwoValidator();
        boolean isValid = validator.validateOpen(payload, capacity);
        int rows = validator.committedRows();
        int used = validator.usedBytes();
        if (!isValid || rows != recoveredOpenRecordRows || used != recoveredOpenRecordUsedBytes) {
            throw new IllegalStateException("open record at offset " + off + " in " + path
                    + " changed since recovery [rows=" + rows + ", usedBytes=" + used
                    + ", detail=" + validator.detail() + ']');
        }
        validator.finalizeForRecovery(payload);
        sealInPlace(off, capacity, used);
        appendCursor = off + FRAME_HEADER_SIZE + used;
        frameCount = frameCount + 1;
        publishedCursor = appendCursor;
        markDirty(off + FRAME_HEADER_SIZE + capacity);
        reservedCursor = appendCursor;
        tornTailBytes = detectTornTail(mmapAddress, appendCursor, sizeBytes);
        recoveredOpenRecordOffset = -1L;
        recoveredOpenRecordCapacity = 0;
        recoveredOpenRecordRows = 0;
        recoveredOpenRecordUsedBytes = 0;
        isRecoveredOpenRecordValid = false;
        recoveredOpenRecordDetail = null;
        LOG.info("SF segment {}: recovered {} committed rows from an open record at offset {} "
                + "and sealed it at {} bytes", path, rows, off, used);
        return off;
    }

    /**
     * Seals the record reserved at {@code off} at {@code usedBytes} payload
     * bytes: zeroes the unused tail {@code [off + 8 + usedBytes, off + 8 + capacity)},
     * then stores the length and, last, {@code STATE_SEALED}, so a crash at any
     * point leaves either an open record recovery can seal or a sealed one. The
     * append cursor moves back to the record's end and the record counts as a
     * frame; like a frame from {@code tryWrite}, it stays unpublished until
     * {@code publishWritten}. The next {@link #syncPublished()} flushes the
     * whole former reservation, the zeroed tail included. Producer thread only.
     *
     * @throws IllegalStateException    when no record is reserved at {@code off}
     * @throws IllegalArgumentException when {@code usedBytes} is outside
     *                                  {@code [QwoFormat.HEADER_SIZE, capacity]}
     */
    public void sealReserved(long off, int usedBytes) {
        checkReserved(off);
        if (usedBytes < QwoFormat.HEADER_SIZE || usedBytes > reservedCapacity) {
            throw new IllegalArgumentException("usedBytes " + usedBytes
                    + " outside [" + QwoFormat.HEADER_SIZE + ", " + reservedCapacity + ']');
        }
        sealInPlace(off, reservedCapacity, usedBytes);
        appendCursor = off + FRAME_HEADER_SIZE + usedBytes;
        // Single writer, as in tryWrite; precedes the publishedCursor store.
        frameCount = frameCount + 1;
        markDirty(off + FRAME_HEADER_SIZE + reservedCapacity);
        reservedCursor = appendCursor;
        reservedOffset = -1L;
        reservedCapacity = 0;
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
        long offset = tryWrite(payloadAddr, payloadLen);
        if (offset != -1L) {
            publishWritten();
        }
        return offset;
    }

    /**
     * Format version in the segment header: {@link #VERSION} for a segment this
     * client created, down to {@link #MIN_VERSION} for one an older client left
     * and recovery resumed. Only a segment of {@link #VERSION} may take a QWO1
     * record: an older client reading the slot would treat one as a torn tail.
     */
    public byte version() {
        return Unsafe.getUnsafe().getByte(mmapAddress + 4);
    }

    /**
     * Makes every frame written by {@link #tryWrite} visible to the consumer: the
     * I/O thread may read, and send, any frame below {@link #publishedOffset()}.
     * Producer thread only.
     */
    void publishWritten() {
        // Until this volatile write retires, the consumer cannot see any of the
        // bytes tryWrite wrote, and once it does, every one of them is complete.
        publishedCursor = appendCursor;
    }

    /**
     * First half of {@link #tryAppend}: writes the frame and advances the append
     * cursor and the frame count, but leaves {@link #publishedOffset()} where it
     * was, so the consumer cannot see the frame until {@link #publishWritten}.
     * {@link SegmentRing} publishes the frame's FSN in between, so the FSN is
     * never behind a frame the I/O thread can send. Returns the frame's offset,
     * or -1 with nothing written if it does not fit. Producer thread only.
     */
    long tryWrite(long payloadAddr, int payloadLen) {
        if (reservedOffset >= 0) {
            throw new IllegalStateException("record reserved at offset " + reservedOffset);
        }
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
        // the RMW race-free by design. It precedes the publishedCursor store
        // in publishWritten, so a consumer that sees the frame's bytes also
        // sees the frame count that makes its FSN discoverable.
        frameCount = frameCount + 1;
        return offset;
    }

    /**
     * Feeds every recovered frame in this segment to the engine's single-pass
     * recovery fold. Segments are visited oldest-first by {@link SegmentRing}.
     */
    void scanRecovery(RecoveredFrameAnalysis analysis) {
        long off = HEADER_SIZE;
        long frames = frameCount;
        for (long i = 0; i < frames; i++) {
            int payloadLen = Unsafe.getUnsafe().getInt(mmapAddress + off + 4);
            analysis.accept(baseSeq + i, mmapAddress + off + FRAME_HEADER_SIZE, payloadLen);
            off += FRAME_HEADER_SIZE + payloadLen;
        }
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
     * truncation (corruption) from a normal partial fill (no incident). An
     * open record left by a crash counts as residue. This is the open-time
     * observation; only {@link #sealRecoveredOpenRecord()} measures it again,
     * past the record it seals. A later
     * {@link #sanitizeTornTail()} zeroes the on-disk residue (after which a
     * re-open reports {@code 0}) but leaves this diagnostic intact. Sparse
     * holes read back as zeroes; an actual positioned-read failure aborts
     * recovery instead of being classified as a clean tail.
     */
    public long tornTailBytes() {
        return tornTailBytes;
    }

    /**
     * Durably zeroes the torn-tail residue this segment was opened with:
     * {@code [sizeBytes - tornTailBytes, sizeBytes)} is set to zero and made
     * durable with an msync+fsync barrier. Restores the invariant every fresh
     * segment already has -- all bytes past the append cursor are zero -- so
     * residue that resumed appends do not fully overwrite cannot survive into
     * a reseal (where sealed-suffix recovery would treat it as fatal
     * corruption on every subsequent startup) and a byte-aligned stale frame
     * with a valid CRC cannot be resurrected at a recycled FSN by a later
     * scan (the frame envelope binds neither position nor FSN).
     * <p>
     * DESTRUCTIVE by design and therefore never called by
     * {@link #openExisting}: after a mid-file tear the residue can hold
     * unreachable valid-CRC frames that are the only surviving copy of
     * unacked payloads. The caller ({@link SegmentRing} recovery) must first
     * establish the residue is not load-bearing, and the strength of that
     * claim differs by role: a sealed suffix is PROVEN dead (frame
     * accounting validated complete against the chain), while the resumed
     * active's tail past a mid-file tear is discarded by POLICY, not proof
     * -- it can hold valid-CRC frames of real unacked payloads that are
     * unreachable by replay (the FSN sequence breaks at the tear), and
     * leaving them risks the two hazards above. Must run before any append
     * resumes; appending first would put live frames inside the zeroed range,
     * so that ordering is rejected. Idempotent; no-op when no residue was
     * observed. The fsync is load-bearing: in MEMORY durability mode rotation
     * does not sync the sealed predecessor's data pages, so an unflushed
     * zero-write plus a crash would regenerate the exact poisoned state this
     * sanitization removes. A failed barrier throws and the caller fails
     * closed; the zeroes may still reach disk via the page cache, which is
     * safe: a retry either observes a clean tail or re-zeroes.
     * <p>
     * An open record {@link #openExisting} found is part of the residue. On
     * the resumed active, seal a recoverable one with
     * {@link #sealRecoveredOpenRecord()} first; otherwise this zeroes its
     * committed rows with the rest.
     */
    public void sanitizeTornTail() {
        if (tornTailBytes == 0 || tornTailSanitized) {
            return;
        }
        long residueStart = sizeBytes - tornTailBytes;
        if (appendCursor != residueStart) {
            throw new MmapSegmentException(
                    "torn tail must be sanitized before appends resume in " + path
                            + " [appendCursor=" + appendCursor
                            + ", residueStart=" + residueStart + ']');
        }
        Unsafe.getUnsafe().setMemory(mmapAddress + residueStart, tornTailBytes, (byte) 0);
        if (filesFacade.msync(mmapAddress, sizeBytes, false) != 0 || filesFacade.fsync(fd) != 0) {
            throw new MmapSegmentException(
                    "could not sync zeroed torn tail in " + path
                            + " [errno=" + Os.errno() + ']');
        }
        tornTailSanitized = true;
        LOG.warn("SF segment {}: zeroed {} bytes of torn-tail residue at offset {}; "
                        + "frames past the tear (if any) are discarded",
                path, tornTailBytes, residueStart);
    }

    /**
     * True when recovery observed torn-tail residue that
     * {@link #sanitizeTornTail()} has not yet durably zeroed -- i.e. the
     * on-disk suffix still carries the observed bytes.
     */
    public boolean hasUnsanitizedTornTail() {
        return tornTailBytes > 0 && !tornTailSanitized;
    }

    /**
     * True when {@link #openExisting} found an open record at the end of the
     * recovered frames and it failed {@link QwoValidator#validateOpen}; see
     * {@link #recoveredOpenRecordDetail()}. The record is left on disk as torn
     * tail. Stays true after {@link #sanitizeTornTail()} zeroes it, so the
     * loss can still be reported.
     */
    public boolean hasInvalidOpenRecord() {
        return recoveredOpenRecordOffset >= 0 && !isRecoveredOpenRecordValid;
    }

    /**
     * True when {@link #openExisting} found a valid open record with committed
     * rows at the end of the recovered frames and it is still on disk:
     * {@link #sealRecoveredOpenRecord()} has not sealed it and
     * {@link #sanitizeTornTail()} has not zeroed it.
     */
    public boolean hasRecoverableOpenRecord() {
        return recoveredOpenRecordOffset >= 0
                && isRecoveredOpenRecordValid
                && recoveredOpenRecordRows > 0
                && !tornTailSanitized;
    }

    /**
     * Scans the header and frames through a bounded positioned-read buffer.
     * Sparse file holes are returned by {@code pread}/{@code ReadFile} as zero
     * bytes, while media errors and concurrent truncation surface as ordinary
     * read failures. No recovery byte is dereferenced through mmap.
     * <p>
     * A frame whose length is at least {@link QwoFormat#HEADER_SIZE} and whose
     * payload starts with {@link QwoFormat#MAGIC_RECORD} is a QWO1 record, and
     * its first word is a state, not a CRC: {@code STATE_SEALED} counts it as a
     * frame (its CRCs are checked after mmap, see
     * {@link #validateRecordsAfterMap}); {@code STATE_OPEN} records its offset
     * and ends the scan; any other word ends the scan like a bad CRC. An open
     * record's length is its reservation capacity, so the same bounds check
     * keeps the whole reservation inside the file.
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
            if (version < MIN_VERSION || version > VERSION) {
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
            long openRecordOffset = -1L;
            boolean sealedRecordSeen = false;
            long pos = HEADER_SIZE;
            while (fileSize - pos >= FRAME_HEADER_SIZE) {
                int word0 = reader.getInt(pos);
                int payloadLen = reader.getInt(pos + 4L);
                // Avoid addition overflow while rejecting a negative or
                // beyond-EOF frame length as a torn tail.
                if (payloadLen < 0 || payloadLen > fileSize - pos - FRAME_HEADER_SIZE) {
                    break;
                }
                if (payloadLen >= QwoFormat.HEADER_SIZE
                        && reader.getInt(pos + FRAME_HEADER_SIZE) == QwoFormat.MAGIC_RECORD) {
                    if (word0 == QwoFormat.STATE_SEALED) {
                        sealedRecordSeen = true;
                        pos += FRAME_HEADER_SIZE + payloadLen;
                        frameCount++;
                        continue;
                    }
                    if (word0 == QwoFormat.STATE_OPEN) {
                        openRecordOffset = pos;
                    }
                    break;
                }
                int crcCalc = reader.crc32c(pos + 4L, 4L + payloadLen);
                if (crcCalc != word0) {
                    break;
                }
                pos += FRAME_HEADER_SIZE + payloadLen;
                frameCount++;
            }
            long tornTailBytes = detectTornTail(reader, pos, fileSize);
            return new RecoveryScan(baseSeq, frameCount, pos, tornTailBytes, openRecordOffset, sealedRecordSeen);
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

    /**
     * {@link #detectTornTail(RecoveryReader, long, long)} over the mapping at
     * {@code addr}, for a boundary that moved after the scan.
     */
    private static long detectTornTail(long addr, long lastGood, long fileSize) {
        if (lastGood >= fileSize) {
            return 0L;
        }
        return hasNonZero(addr + lastGood, fileSize - lastGood)
                ? fileSize - lastGood
                : 0L;
    }

    private static boolean hasNonZero(long address, long len) {
        boolean nonZero = false;
        long i = 0;
        while (i < len && ((address + i) & (Long.BYTES - 1L)) != 0L) {
            nonZero |= Unsafe.getUnsafe().getByte(address + i++) != 0;
        }
        while (i <= len - Long.BYTES) {
            nonZero |= Unsafe.getUnsafe().getLong(address + i) != 0L;
            i += Long.BYTES;
        }
        while (i < len) {
            nonZero |= Unsafe.getUnsafe().getByte(address + i++) != 0;
        }
        return nonZero;
    }

    /**
     * Whether the scan's torn tail is exactly the open record it judged
     * recoverable: valid with committed rows, at the recovery boundary, with
     * nothing but zeroes past its reservation.
     */
    private static boolean isRecoverableOpenRecordTail(long addr, long fileSize, RecoveryScan scan) {
        if (scan.openRecordOffset < 0 || scan.openRecordOffset != scan.lastGood
                || !scan.isOpenRecordValid || scan.openRecordRows <= 0) {
            return false;
        }
        long reservationEnd = scan.openRecordOffset + FRAME_HEADER_SIZE + scan.openRecordCapacity;
        return !hasNonZero(addr + reservationEnd, fileSize - reservationEnd);
    }

    /**
     * Validates the QWO1 records of a scanned segment on its mapping at
     * {@code addr}, writing nothing:
     * <ul>
     *     <li>a sealed record that fails {@link QwoValidator#validateSealed}
     *     ends the recovered frames at its offset, leaving it and everything
     *     after it as a torn tail;</li>
     *     <li>an open record still at the end of the frames is judged by
     *     {@link QwoValidator#validateOpen}, and the outcome is recorded in the
     *     returned scan; the record stays outside the frames, so it counts as
     *     torn tail. An open record behind an invalid sealed record is plain
     *     torn tail.</li>
     * </ul>
     * Returns the scan with the frame count and the boundary adjusted; the torn
     * tail is measured again on the mapping when the boundary moved.
     */
    private static RecoveryScan validateRecordsAfterMap(String path, long addr, long fileSize, RecoveryScan scan) {
        if (!scan.sealedRecordSeen && scan.openRecordOffset < 0) {
            return scan;
        }
        QwoValidator validator = new QwoValidator();
        long frameCount = scan.frameCount;
        long lastGood = scan.lastGood;
        if (scan.sealedRecordSeen) {
            long pos = HEADER_SIZE;
            for (long i = 0; i < scan.frameCount; i++) {
                int word0 = Unsafe.getUnsafe().getInt(addr + pos);
                int len = Unsafe.getUnsafe().getInt(addr + pos + 4);
                long payload = addr + pos + FRAME_HEADER_SIZE;
                if (word0 == QwoFormat.STATE_SEALED
                        && len >= QwoFormat.HEADER_SIZE
                        && Unsafe.getUnsafe().getInt(payload) == QwoFormat.MAGIC_RECORD
                        && !validator.validateSealed(payload, len)) {
                    String detail = validator.detail();
                    LOG.error("SF segment {}: sealed record at offset {} (fsn {}) is invalid, "
                                    + "recovery stops there and keeps {} frames [{}]",
                            path, pos, scan.baseSeq + i, i, detail);
                    frameCount = i;
                    lastGood = pos;
                    break;
                }
                pos += FRAME_HEADER_SIZE + len;
            }
        }
        long tornTailBytes = lastGood == scan.lastGood
                ? scan.tornTailBytes
                : detectTornTail(addr, lastGood, fileSize);
        RecoveryScan result = new RecoveryScan(scan.baseSeq, frameCount, lastGood, tornTailBytes,
                scan.openRecordOffset == lastGood ? lastGood : -1L, scan.sealedRecordSeen);
        long off = result.openRecordOffset;
        if (off >= 0) {
            int capacity = Unsafe.getUnsafe().getInt(addr + off + 4);
            boolean isValid = validator.validateOpen(addr + off + FRAME_HEADER_SIZE, capacity);
            result.openRecordCapacity = capacity;
            result.isOpenRecordValid = isValid;
            result.openRecordRows = validator.committedRows();
            result.openRecordUsedBytes = validator.usedBytes();
            result.openRecordDetail = validator.detail();
            if (!isValid) {
                String detail = result.openRecordDetail;
                LOG.error("SF segment {}: open record at offset {} (capacity {}) is invalid; "
                        + "it is left on disk as torn tail [{}]", path, off, capacity, detail);
            }
        }
        return result;
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
                nonZero |= MmapSegment.hasNonZero(bufferAddress + bufferIndex, chunk);
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
        // Envelope offset of the open record the scan stopped at, -1 when none.
        private final long openRecordOffset;
        private final boolean sealedRecordSeen;
        private final long tornTailBytes;
        // validateOpen's judgement of the open record, set after mmap.
        private boolean isOpenRecordValid;
        private int openRecordCapacity;
        private String openRecordDetail;
        private int openRecordRows;
        private int openRecordUsedBytes;

        private RecoveryScan(long baseSeq, long frameCount, long lastGood, long tornTailBytes,
                             long openRecordOffset, boolean sealedRecordSeen) {
            this.baseSeq = baseSeq;
            this.frameCount = frameCount;
            this.lastGood = lastGood;
            this.tornTailBytes = tornTailBytes;
            this.openRecordOffset = openRecordOffset;
            this.sealedRecordSeen = sealedRecordSeen;
        }
    }
}
