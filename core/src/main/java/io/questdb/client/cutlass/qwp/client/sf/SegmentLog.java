/*+*****************************************************************************
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

package io.questdb.client.cutlass.qwp.client.sf;

import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segmented append-only log of opaque byte frames keyed by a monotonic 64-bit sequence number.
 * <p>
 * On disk:
 * <pre>
 *   &lt;baseSeq:016x&gt;.sfa                          active segment
 *   &lt;baseSeq:016x&gt;-&lt;lastSeq:016x&gt;.sfs           sealed segment
 *   {@value #LOCK_FILE_NAME}                                              flock-held single-writer marker
 * </pre>
 * Each segment file holds:
 * <pre>
 *   [u32 magic 'SF01'] [u8 ver=1] [u8 flags=0] [u16 reserved=0]
 *   [u64 baseSeq] [u64 createdMicros]                          24-byte header
 *   frame, frame, ...                                          each frame:
 *                                                              [u32 crc32c]
 *                                                              [u32 payloadLen]
 *                                                              [payloadLen bytes]
 *   crc32c covers (payloadLen, payload) — torn tails and silent bit-rot are
 *   detected on scan and the active segment is truncated to the last good frame.
 * </pre>
 * Sealed-segment file names encode {@code lastSeq}, so trim and recovery don't
 * need to scan a sealed segment to know its sequence range — only the active
 * segment is scanned (to find a torn tail).
 * <p>
 * This class is single-threaded — one owner thread does all reads/writes/trims.
 */
public final class SegmentLog implements QuietCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentLog.class);


    public static final long DEFAULT_MAX_BYTES_PER_SEGMENT = 64L * 1024 * 1024;
    public static final long DEFAULT_MAX_TOTAL_BYTES = Long.MAX_VALUE;
    public static final long FIRST_SEQ = 0L;

    static final String LOCK_FILE_NAME = ".sf.lock";
    static final String ACTIVE_SUFFIX = ".sfa";
    static final String SEALED_SUFFIX = ".sfs";

    public static final int FILE_MAGIC = 0x31304653; // 'SF01' little-endian
    public static final int HEADER_SIZE = 24;
    public static final int FRAME_HEADER_SIZE = 8; // u32 crc + u32 len

    private static final int MIN_BUF_BYTES = 64;

    private final String dir;
    private final FilesFacade ff;
    private final long maxBytesPerSegment;
    private final long maxTotalBytes;
    // When true, every successful append() forces fsync of the active segment.
    // Trades throughput for the strongest "data on disk after append returns"
    // guarantee. Default off — fsync runs on rotation and on explicit flush().
    private final boolean fsyncEachAppend;

    private final ObjList<Segment> segments = new ObjList<>();
    private Segment active;
    private long nextSeq;
    // Running sum of all segments' writePos. Maintained incrementally on
    // append/rotate/trim/createActive so bytesOnDisk() is O(1) and zero-alloc
    // on the I/O hot path. Re-derivable from segments at any time via the
    // sum of writePos over each segment.
    private long bytesOnDiskCache;

    private int lockFd = -1;

    /** 8-byte scratch for writing frame headers. */
    private long envBuf;
    /** Growable read buffer for replay (frame payloads). */
    private long readBuf;
    private long readBufCap;

    private boolean closed;

    private SegmentLog(String dir, FilesFacade ff, long maxBytesPerSegment, long maxTotalBytes, boolean fsyncEachAppend) {
        this.dir = dir;
        this.ff = ff;
        this.maxBytesPerSegment = maxBytesPerSegment;
        this.maxTotalBytes = maxTotalBytes;
        this.fsyncEachAppend = fsyncEachAppend;
    }

    /**
     * Open or recover a segment log at the given directory. Acquires an exclusive
     * file lock on the directory; only one process may open a given log at a time.
     * Total disk usage is unbounded; use {@link #open(String, long, long)} to cap it.
     */
    public static SegmentLog open(String dir, long maxBytesPerSegment) {
        return open(dir, FilesFacade.INSTANCE, maxBytesPerSegment, DEFAULT_MAX_TOTAL_BYTES, false);
    }

    /**
     * Open or recover a segment log at the given directory with a total disk-usage
     * cap. When {@code maxTotalBytes} is reached, {@link #append} throws
     * {@link SfDiskFullException}; the caller must wait for {@link #trim} to free
     * space (typically driven by server ACKs).
     */
    public static SegmentLog open(String dir, long maxBytesPerSegment, long maxTotalBytes) {
        return open(dir, FilesFacade.INSTANCE, maxBytesPerSegment, maxTotalBytes, false);
    }

    /**
     * Open with full configuration. {@code fsyncEachAppend} forces the OS page
     * cache to flush after every successful {@link #append} — slow but gives the
     * strongest "data on disk before append returns" guarantee, surviving even
     * an OS-level crash.
     */
    public static SegmentLog open(String dir, long maxBytesPerSegment, long maxTotalBytes, boolean fsyncEachAppend) {
        return open(dir, FilesFacade.INSTANCE, maxBytesPerSegment, maxTotalBytes, fsyncEachAppend);
    }

    /**
     * Open with an explicit {@link FilesFacade}. Used by tests to inject fault
     * behavior at the file-I/O boundary; production callers should use the
     * overloads above.
     */
    public static SegmentLog open(String dir, FilesFacade ff, long maxBytesPerSegment, long maxTotalBytes, boolean fsyncEachAppend) {
        if (maxBytesPerSegment < HEADER_SIZE + FRAME_HEADER_SIZE + 16) {
            throw new SfException("maxBytesPerSegment too small: " + maxBytesPerSegment);
        }
        if (maxTotalBytes < maxBytesPerSegment) {
            throw new SfException("maxTotalBytes (" + maxTotalBytes
                    + ") must be >= maxBytesPerSegment (" + maxBytesPerSegment + ")");
        }
        SegmentLog log = new SegmentLog(dir, ff, maxBytesPerSegment, maxTotalBytes, fsyncEachAppend);
        try {
            log.openInternal();
            return log;
        } catch (Throwable t) {
            log.close();
            if (t instanceof SfException) {
                throw t;
            }
            throw new SfException("failed to open SegmentLog at " + dir, t);
        }
    }

    /**
     * Append a frame and return its assigned sequence number. The payload bytes
     * at {@code payloadAddr} are written verbatim, prefixed with an 8-byte SF
     * envelope (CRC32C + length). Rotates to a new active segment if the current
     * one is at or above {@link #maxBytesPerSegment} after the write.
     */
    public long append(long payloadAddr, int payloadLen) {
        ensureOpen();
        // Guard against the partial-rotate failure state (bug C1). When
        // rotate() fails between rename and createActive (e.g. allocNativePath
        // OOMs at the second alloc, or createActive's openCleanRW/fsync fails
        // for the new segment), `active` is left pointing at the now-sealed
        // segment with sealed=true and fd=-1. Without this guard, a small
        // subsequent append that fits under the cap would bypass the rotate
        // trigger below and fall through to ff.write(fd=-1) — which returns
        // -1 and is wrapped as SfDiskFullException (a recoverable backpressure
        // signal) by the short-write branch. The I/O thread would then retry
        // forever and the user thread would deadlock in flush(). Surface a
        // fatal SfException instead so the connection terminates cleanly.
        if (active.sealed || active.fd < 0) {
            throw new SfException("SegmentLog is unusable after a prior rotate failure: "
                    + active.path + " (sealed=" + active.sealed + ", fd=" + active.fd + ")");
        }
        if (payloadLen <= 0) {
            throw new SfException("payloadLen must be > 0, got " + payloadLen);
        }
        long total = (long) FRAME_HEADER_SIZE + payloadLen;
        if (HEADER_SIZE + total > maxBytesPerSegment) {
            // single frame larger than a segment is a misuse
            throw new SfException("frame larger than maxBytesPerSegment: " + payloadLen);
        }
        // Configured total-disk cap: if accepting this frame would push us over,
        // throw disk-full so the caller can back-pressure. The bytes the new frame
        // would add are `total` (frames in existing segments are already counted
        // in bytesOnDisk()). Rotation also costs HEADER_SIZE for the new segment;
        // include that in the projection when we'd rotate.
        long projected = bytesOnDisk() + total;
        if (active.writePos + total > maxBytesPerSegment) {
            projected += HEADER_SIZE;
        }
        if (projected > maxTotalBytes) {
            throw new SfDiskFullException("SF total bytes cap reached: "
                    + bytesOnDisk() + " + " + total + " > " + maxTotalBytes);
        }
        if (active.writePos + total > maxBytesPerSegment) {
            rotate();
        }

        long seq = nextSeq;

        // CRC over [u32 payloadLen | payload]
        Unsafe.getUnsafe().putInt(envBuf + 4, payloadLen);
        int crc = Crc32c.update(Crc32c.INIT, envBuf + 4, 4);
        crc = Crc32c.update(crc, payloadAddr, payloadLen);
        Unsafe.getUnsafe().putInt(envBuf, crc);

        long pos = active.writePos;
        long w = ff.write(active.fd, envBuf, FRAME_HEADER_SIZE, pos);
        if (w != FRAME_HEADER_SIZE) {
            // Most likely ENOSPC. Truncate any partial write back so a retry
            // (after disk space frees up) starts at the same position cleanly.
            ff.truncate(active.fd, pos);
            throw new SfDiskFullException("short write of frame header at pos=" + pos
                    + " (got " + w + " of " + FRAME_HEADER_SIZE + ")");
        }
        long w2 = ff.write(active.fd, payloadAddr, payloadLen, pos + FRAME_HEADER_SIZE);
        if (w2 != payloadLen) {
            // Header landed but payload didn't fit. Truncate back to before the
            // header so the file is in a clean state for retry.
            ff.truncate(active.fd, pos);
            throw new SfDiskFullException("short write of payload at pos=" + (pos + FRAME_HEADER_SIZE)
                    + " (got " + w2 + " of " + payloadLen + ")");
        }
        active.writePos = pos + total;
        active.frameCount++;
        bytesOnDiskCache += total;
        nextSeq = seq + 1;
        if (fsyncEachAppend && ff.fsync(active.fd) != 0) {
            throw new SfException("fsync after append failed for " + active.path);
        }
        return seq;
    }

    /** Force durability of the active segment to disk. */
    public void fsync() {
        ensureOpen();
        if (ff.fsync(active.fd) != 0) {
            throw new SfException("fsync failed for " + active.path);
        }
    }

    /**
     * Visit every frame currently on disk in seq order. The visitor is called
     * with the frame's payload at an off-heap address valid only for the duration
     * of the call. Returning false from the visitor stops iteration.
     */
    public void replay(FrameVisitor visitor) {
        ensureOpen();
        for (int i = 0, n = segments.size(); i < n; i++) {
            Segment s = segments.getQuick(i);
            // Skip segments whose disk file we tried (and failed) to remove
            // on a previous trim. Their frames were acked by the server —
            // re-shipping them on the new connection would produce silent
            // duplicate writes.
            if (s.removePending) {
                continue;
            }
            if (!replaySegment(s, visitor)) {
                return;
            }
        }
    }

    /**
     * Reclaim disk space for every frame whose seq is &lt;= ackedSeq.
     * <p>
     * Sealed segments whose {@code lastSeq <= ackedSeq} are deleted. If the
     * current active segment also has all of its frames acked (i.e. its
     * highest assigned seq &lt;= ackedSeq), it is force-rotated and the
     * just-sealed file is immediately removed. {@code nextSeq} is preserved
     * across the auto-rotate so future appends keep monotonic FSNs.
     * <p>
     * The force-rotate is what makes "trimmed when the server acknowledges
     * it" honest in the public API: a quiet sender whose batches all
     * acknowledge keeps disk at exactly one empty active segment, and on
     * restart no acked frames are replayed.
     */
    public void trim(long ackedSeq) {
        ensureOpen();
        trimSealedSegments(ackedSeq);

        // Force-rotate the active segment when every frame in it has been
        // acked. The just-sealed segment is then removed by a second pass
        // of trimSealedSegments. Cost is one extra rotation per fully-acked
        // burst (typically once per server cumulative ACK), which on a
        // low-rate sender is amortised by the natural-rotation cost it
        // displaces — the active will rotate anyway eventually.
        //
        // The {@code !active.sealed} guard handles the rotate-OOM recovery
        // state from the M2 fix: after an OOM mid-rename, {@code active}
        // points at the now-sealed segment with fd=-1 and pathPtrNative=0;
        // attempting to rotate it again would fail in fsync. The sealed
        // pass above already trimmed the file, so we just skip here.
        //
        // If rotate fails (e.g. fsync EIO), the SfException propagates to
        // the caller. ResponseHandler.onBinaryMessage runs trim() inline
        // with ACK processing, so a thrown SfException there will surface
        // as a connection-level error and the sender goes terminal — the
        // correct response to a broken disk.
        if (active != null && !active.sealed && active.frameCount > 0
                && active.baseSeq + active.frameCount - 1 <= ackedSeq) {
            rotate();
            trimSealedSegments(ackedSeq);
        }
    }

    private void trimSealedSegments(long ackedSeq) {
        int writeIdx = 0;
        for (int i = 0, n = segments.size(); i < n; i++) {
            Segment s = segments.getQuick(i);
            if (!s.sealed) {
                segments.setQuick(writeIdx++, s);
                continue;
            }
            if (s.lastSeq() <= ackedSeq) {
                // Close the fd up front: even if remove fails and the segment
                // stays in the list, we won't read from it again — replay()
                // skips removePending segments and append() never targets a
                // sealed one. Holding the fd would just leak a descriptor.
                if (s.fd != -1) {
                    ff.close(s.fd);
                    s.fd = -1;
                }
                boolean removed;
                if (s.pathPtrNative != 0) {
                    removed = ff.remove(s.pathPtrNative);
                } else {
                    // Recovery case: rotate's allocNativePath OOMed and left
                    // pathPtrNative=0. Fall back to the String form, which
                    // does its own one-shot encode/free internally.
                    removed = ff.remove(s.path);
                }
                if (!removed) {
                    // remove() failed (Windows sharing-violation under AV,
                    // transient NFS error, ESTALE, etc.). DO NOT decrement
                    // bytesOnDiskCache or free pathPtrNative — the file is
                    // still on disk. Keep the segment in the in-memory list
                    // so:
                    //   (a) bytesOnDisk() keeps reporting the truth and the
                    //       sf_max_total_bytes cap stays enforceable;
                    //   (b) the next trim() retries the remove (the
                    //       lastSeq() <= ackedSeq predicate still holds for
                    //       cumulative ACKs);
                    //   (c) replay() skips it via the removePending flag so
                    //       already-acked frames don't re-ship to the new
                    //       server on reconnect.
                    if (!s.removePending) {
                        LOG.warn("trim: remove() failed for sealed segment, "
                                + "will retry on next trim [path={}, baseSeq={}, "
                                + "lastSeq={}, writePos={}]",
                                s.path, s.baseSeq, s.lastSeq(), s.writePos);
                    }
                    s.removePending = true;
                    segments.setQuick(writeIdx++, s);
                    continue;
                }
                if (s.removePending) {
                    LOG.info("trim: retry succeeded for previously-failed "
                            + "remove [path={}, baseSeq={}]", s.path, s.baseSeq);
                }
                if (s.pathPtrNative != 0) {
                    ff.freeNativePath(s.pathPtrNative);
                    s.pathPtrNative = 0;
                }
                bytesOnDiskCache -= s.writePos;
            } else {
                segments.setQuick(writeIdx++, s);
            }
        }
        while (segments.size() > writeIdx) {
            segments.remove(segments.size() - 1);
        }
    }

    /**
     * Lowest seq currently on disk that {@link #replay} will visit, or -1 if
     * none. Must skip {@code removePending} segments — replay() does the same
     * (line 277), and {@code WebSocketSendQueue.doReconnectCycle} pins
     * {@code fsnAtZero} to this value before invoking replay. A mismatch here
     * trips the "SF replay FSN drift" guard inside the replay visitor and
     * aborts every reconnect attempt, turning a transient remove() failure
     * into a permanent reconnect loop.
     */
    public long oldestSeq() {
        ensureOpen();
        for (int i = 0, n = segments.size(); i < n; i++) {
            Segment s = segments.getQuick(i);
            if (s.removePending) {
                continue;
            }
            if (s.frameCount == 0) {
                // Empty segment can only be the tail active (sealed segments
                // always carry frames — rotate drops empty ones). Nothing
                // after this is replay-visible.
                return -1;
            }
            return s.baseSeq;
        }
        return -1;
    }

    /** Sequence number that will be assigned to the next {@link #append}. */
    public long nextSeq() {
        ensureOpen();
        return nextSeq;
    }

    /** Total bytes used by all segments on disk (header + frames). */
    public long bytesOnDisk() {
        ensureOpen();
        return bytesOnDiskCache;
    }

    public int segmentCount() {
        ensureOpen();
        return segments.size();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (int i = 0, n = segments.size(); i < n; i++) {
            Segment s = segments.getQuick(i);
            if (s.fd != -1) {
                ff.close(s.fd);
                s.fd = -1;
            }
            // Last-chance retry for segments whose mid-session remove() failed
            // (e.g. Windows sharing-violation that has since cleared, transient
            // NFS error that has resolved). Without this, an orphan .sfs file
            // would persist on disk and the next process start would
            // rediscover it via scanDirectory and replay its already-acked
            // frames to the new server — silent duplicate writes.
            if (s.removePending) {
                boolean removed = s.pathPtrNative != 0
                        ? ff.remove(s.pathPtrNative)
                        : ff.remove(s.path);
                if (removed) {
                    s.removePending = false;
                } else {
                    LOG.warn("close: remove() still failing for orphaned segment "
                            + "[path={}, baseSeq={}] — file will be rediscovered "
                            + "on next start and re-replay its already-acked "
                            + "frames to the new server", s.path, s.baseSeq);
                }
            }
            if (s.pathPtrNative != 0) {
                ff.freeNativePath(s.pathPtrNative);
                s.pathPtrNative = 0;
            }
        }
        segments.clear();
        active = null;
        if (lockFd != -1) {
            ff.close(lockFd);
            lockFd = -1;
        }
        if (envBuf != 0) {
            Unsafe.free(envBuf, FRAME_HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
            envBuf = 0;
        }
        if (readBuf != 0) {
            Unsafe.free(readBuf, readBufCap, MemoryTag.NATIVE_ILP_RSS);
            readBuf = 0;
            readBufCap = 0;
        }
    }

    // ---- internals ----

    private void openInternal() {
        if (!ff.exists(dir)) {
            int rc = ff.mkdir(dir, 0755);
            if (rc != 0 && !ff.exists(dir)) {
                throw new SfException("cannot create directory: " + dir);
            }
        }

        envBuf = Unsafe.malloc(FRAME_HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        readBufCap = MIN_BUF_BYTES;
        readBuf = Unsafe.malloc(readBufCap, MemoryTag.NATIVE_ILP_RSS);

        // single-writer lock
        String lockPath = dir + "/" + LOCK_FILE_NAME;
        lockFd = ff.openRW(lockPath);
        if (lockFd < 0) {
            throw new SfException("cannot open lock file: " + lockPath);
        }
        if (ff.lock(lockFd) != 0) {
            throw new SfException("SegmentLog at " + dir + " is locked by another process");
        }

        scanDirectory();
        if (active == null) {
            // Mid-rotate crash recovery: rotate() has a window between
            // ff.rename(.sfa → .sfs) and createActive(lastSeq + 1) where the
            // process can die (or createActive can throw, leaving the .sfa
            // removed by its own catch block) with sealed segments on disk
            // and no active. Resuming at FIRST_SEQ here would let the next
            // session's appends produce frames whose FSNs collide with FSNs
            // already on disk in the sealed segments, breaking ACK
            // translation, trim, and replay. Pick up past the highest sealed
            // lastSeqOnDisk instead. scanDirectory sorts segments by baseSeq
            // and sealed segments cover non-overlapping FSN ranges, so the
            // last entry holds the largest lastSeqOnDisk.
            long resumeFrom = FIRST_SEQ;
            int n = segments.size();
            if (n > 0) {
                resumeFrom = segments.getQuick(n - 1).lastSeqOnDisk + 1;
            }
            createActive(resumeFrom);
        }
        nextSeq = active.baseSeq + active.frameCount;
    }

    private void scanDirectory() {
        long find = ff.findFirst(dir);
        if (find == 0) {
            // findFirst returns 0 for either "directory could not be opened"
            // (errno set — transient EACCES/EMFILE/ESTALE/ENOMEM) or
            // "directory is empty." By the time we get here, openInternal has
            // created the directory if missing AND opened+locked the lock
            // file inside it, so an empty listing is impossible — find==0
            // here can only mean opendir failed. Treating it as "nothing to
            // scan" would let openInternal proceed to createActive(...) on
            // top of any unscanned on-disk segments, silently aliasing or
            // overwriting still-existing data. A durability layer must
            // refuse to proceed from an unknown view of its own log.
            throw new SfException("findFirst failed for SF directory: " + dir);
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(ff.findName(find));
                int type = ff.findType(find);
                if (name != null && type != Files.DT_DIR && !LOCK_FILE_NAME.equals(name)) {
                    Segment s = parseFilename(name);
                    if (s != null) {
                        segments.add(s);
                    }
                }
                rc = ff.findNext(find);
            }
            if (rc < 0) {
                // findNext == -1 is a readdir read error (EIO/ESTALE on NFS,
                // etc.). The in-memory `segments` list is now a partial view
                // of what's on disk. Same hazard as findFirst==0: subsequent
                // createActive(...) or appends would alias unscanned on-disk
                // segments. Refuse rather than recover from an unknown
                // partial state.
                throw new SfException("findNext failed mid-scan of SF directory: " + dir);
            }
        } finally {
            ff.findClose(find);
        }

        // Open-time sort by baseSeq. Worst case is `sf_max_total_bytes /
        // sf_max_bytes` segments — at the documented limit (1 TiB / 64 MiB)
        // that is ~16K entries, where the previous insertion sort spent
        // multiple seconds in O(N²) compares + array shifts. In-place
        // quicksort with median-of-three pivot keeps the no-allocation
        // discipline of the surrounding code.
        sortSegmentsByBaseSeq(0, segments.size());

        // Validate: at most one active segment, and only as the last entry.
        for (int i = 0, n = segments.size(); i < n; i++) {
            Segment s = segments.getQuick(i);
            if (!s.sealed && i != n - 1) {
                throw new SfException("multiple active segments found, second one: " + s.path);
            }
        }

        for (int i = 0, n = segments.size(); i < n; i++) {
            Segment s = segments.getQuick(i);
            openSegment(s);
            if (s.sealed) {
                // trust filename's lastSeq, but verify file size is consistent
                long want = HEADER_SIZE; // body checked lazily on replay
                long len = ff.length(s.fd);
                if (len < want) {
                    throw new SfException("sealed segment shorter than header: " + s.path);
                }
                s.writePos = len;
                s.frameCount = (s.lastSeqOnDisk - s.baseSeq) + 1;
            } else {
                long count = scanActive(s);
                s.frameCount = count;
                active = s;
            }
            bytesOnDiskCache += s.writePos;
        }
    }

    /** Returns frame count after truncating any torn tail. Updates s.writePos. */
    private long scanActive(Segment s) {
        long fileLen = ff.length(s.fd);
        if (fileLen < 0) {
            throw new SfException("fstat failed (length=" + fileLen + ") for " + s.path);
        }
        long pos = HEADER_SIZE;
        long count = 0;
        while (pos < fileLen) {
            if (pos + FRAME_HEADER_SIZE > fileLen) {
                break;
            }
            long r = ff.read(s.fd, envBuf, FRAME_HEADER_SIZE, pos);
            if (r != FRAME_HEADER_SIZE) {
                break;
            }
            int crc = Unsafe.getUnsafe().getInt(envBuf);
            int payloadLen = Unsafe.getUnsafe().getInt(envBuf + 4);
            if (payloadLen <= 0 || pos + FRAME_HEADER_SIZE + payloadLen > fileLen) {
                break;
            }
            ensureReadBuf(payloadLen);
            long r2 = ff.read(s.fd, readBuf, payloadLen, pos + FRAME_HEADER_SIZE);
            if (r2 != payloadLen) {
                break;
            }
            int computed = Crc32c.update(Crc32c.INIT, envBuf + 4, 4);
            computed = Crc32c.update(computed, readBuf, payloadLen);
            if (computed != crc) {
                // A CRC mismatch only counts as a torn tail when the failing
                // frame is the LAST one in the file. If the entire frame plus
                // any subsequent bytes are still on disk, this is mid-stream
                // bit-rot — silently truncating would drop every valid frame
                // that follows. Surface the corruption loudly instead.
                long fullFrameEnd = pos + FRAME_HEADER_SIZE + payloadLen;
                if (fullFrameEnd < fileLen) {
                    throw new SfException("CRC mismatch in " + s.path + " at " + pos
                            + " (mid-stream — corrupted frame followed by "
                            + (fileLen - fullFrameEnd) + " more bytes)");
                }
                break;
            }
            pos += FRAME_HEADER_SIZE + payloadLen;
            count++;
        }
        if (pos < fileLen) {
            // torn tail or trailing garbage from a partial pre-allocation: truncate.
            if (!ff.truncate(s.fd, pos)) {
                throw new SfException("failed to truncate torn tail of " + s.path);
            }
        }
        s.writePos = pos;
        return count;
    }

    private boolean replaySegment(Segment s, FrameVisitor visitor) {
        if (s.fd == -1) {
            openSegment(s);
        }
        long fileLen = ff.length(s.fd);
        if (fileLen < 0) {
            throw new SfException("fstat failed (length=" + fileLen + ") for " + s.path);
        }
        long pos = HEADER_SIZE;
        long seq = s.baseSeq;
        while (pos < fileLen) {
            if (pos + FRAME_HEADER_SIZE > fileLen) {
                break;
            }
            long r = ff.read(s.fd, envBuf, FRAME_HEADER_SIZE, pos);
            if (r != FRAME_HEADER_SIZE) {
                throw new SfException("short read of frame header in " + s.path + " at " + pos);
            }
            int crc = Unsafe.getUnsafe().getInt(envBuf);
            int payloadLen = Unsafe.getUnsafe().getInt(envBuf + 4);
            if (payloadLen <= 0 || pos + FRAME_HEADER_SIZE + payloadLen > fileLen) {
                throw new SfException("invalid frame length " + payloadLen + " in " + s.path
                        + " at " + pos);
            }
            ensureReadBuf(payloadLen);
            long r2 = ff.read(s.fd, readBuf, payloadLen, pos + FRAME_HEADER_SIZE);
            if (r2 != payloadLen) {
                throw new SfException("short read of payload in " + s.path + " at " + pos);
            }
            int computed = Crc32c.update(Crc32c.INIT, envBuf + 4, 4);
            computed = Crc32c.update(computed, readBuf, payloadLen);
            if (computed != crc) {
                throw new SfException("CRC mismatch in " + s.path + " at " + pos);
            }
            if (!visitor.visit(seq, readBuf, payloadLen)) {
                return false;
            }
            pos += FRAME_HEADER_SIZE + payloadLen;
            seq++;
        }
        return true;
    }

    private void rotate() {
        Segment old = active;
        if (ff.fsync(old.fd) != 0) {
            throw new SfException("fsync failed during rotate of " + old.path);
        }
        ff.close(old.fd);
        old.fd = -1;
        long lastSeq = old.baseSeq + old.frameCount - 1;
        if (old.frameCount == 0) {
            // empty segment shouldn't happen via rotate, but be defensive: drop it
            ff.remove(old.pathPtrNative);
            ff.freeNativePath(old.pathPtrNative);
            old.pathPtrNative = 0;
            bytesOnDiskCache -= old.writePos;
            segments.remove(segments.size() - 1);
            createActive(old.baseSeq);
            return;
        }
        String sealedPath = sealedPathFor(old.baseSeq, lastSeq);
        if (ff.rename(old.path, sealedPath) != 0) {
            throw new SfException("failed to seal segment by rename " + old.path + " -> " + sealedPath);
        }
        // Filesystem is now in the sealed state. Update bookkeeping to match
        // BEFORE re-encoding the path pointer; if allocNativePath OOMs:
        //   - the stale freed pointer must not be left in the field, or
        //     close() walks segments and calls freeNativePath on it again
        //     → native double-free.
        //   - sealed/lastSeqOnDisk must already be set, or trim never sees
        //     this segment (the !s.sealed guard skips it) → permanent
        //     on-disk leak that survives until the next process restart.
        // trim() handles pathPtrNative==0 by falling back to ff.remove(path).
        ff.freeNativePath(old.pathPtrNative);
        old.pathPtrNative = 0;
        old.path = sealedPath;
        old.sealed = true;
        old.lastSeqOnDisk = lastSeq;
        old.pathPtrNative = ff.allocNativePath(sealedPath);
        createActive(lastSeq + 1);
    }

    private void createActive(long baseSeq) {
        String path = activePathFor(baseSeq);
        int fd = ff.openCleanRW(path, 0);
        if (fd < 0) {
            throw new SfException("cannot create active segment: " + path);
        }
        // The fd and pathPtrNative are owned locally until segments.add(s)
        // below; close()'s cleanup loop only walks the segments list, so
        // anything that throws between the openCleanRW above and segments.add
        // must release them here or they leak. Note ff.allocNativePath can
        // throw CairoException on OOM — keep it inside the try.
        Segment s = new Segment();
        s.baseSeq = baseSeq;
        s.path = path;
        s.fd = fd;
        s.sealed = false;
        s.frameCount = 0;
        try {
            s.pathPtrNative = ff.allocNativePath(path);
            writeHeader(s);
            s.writePos = HEADER_SIZE;
            if (ff.fsync(fd) != 0) {
                throw new SfException("fsync failed for new active segment " + path);
            }
        } catch (Throwable t) {
            ff.close(fd);
            s.fd = -1;
            if (s.pathPtrNative != 0) {
                ff.freeNativePath(s.pathPtrNative);
                s.pathPtrNative = 0;
            }
            // Best-effort cleanup of the orphan .sfa file. If this also
            // throws (e.g. another OOM during path encoding), let it
            // propagate — the original failure is already on the way out.
            try {
                ff.remove(path);
            } catch (Throwable ignored) {
                // best-effort
            }
            throw t;
        }
        segments.add(s);
        bytesOnDiskCache += HEADER_SIZE;
        active = s;
    }

    private void writeHeader(Segment s) {
        long buf = Unsafe.malloc(HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        try {
            Unsafe.getUnsafe().putInt(buf, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(buf + 4, (byte) 1);   // version
            Unsafe.getUnsafe().putByte(buf + 5, (byte) 0);   // flags
            Unsafe.getUnsafe().putShort(buf + 6, (short) 0); // reserved
            Unsafe.getUnsafe().putLong(buf + 8, s.baseSeq);
            Unsafe.getUnsafe().putLong(buf + 16, io.questdb.client.std.Os.currentTimeMicros());
            long w = ff.write(s.fd, buf, HEADER_SIZE, 0);
            if (w != HEADER_SIZE) {
                throw new SfException("short write of header to " + s.path);
            }
        } finally {
            Unsafe.free(buf, HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        }
    }

    private void openSegment(Segment s) {
        s.fd = ff.openRW(s.path);
        if (s.fd < 0) {
            throw new SfException("cannot open segment: " + s.path);
        }
        long len = ff.length(s.fd);
        if (len < HEADER_SIZE) {
            throw new SfException("segment shorter than header: " + s.path);
        }
        long buf = Unsafe.malloc(HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        try {
            long r = ff.read(s.fd, buf, HEADER_SIZE, 0);
            if (r != HEADER_SIZE) {
                throw new SfException("short read of header in " + s.path);
            }
            int magic = Unsafe.getUnsafe().getInt(buf);
            if (magic != FILE_MAGIC) {
                throw new SfException("bad magic in " + s.path + ": 0x" + Integer.toHexString(magic));
            }
            byte version = Unsafe.getUnsafe().getByte(buf + 4);
            if (version != 1) {
                throw new SfException("unsupported version " + version + " in " + s.path);
            }
            long base = Unsafe.getUnsafe().getLong(buf + 8);
            if (base != s.baseSeq) {
                throw new SfException("baseSeq mismatch (filename " + s.baseSeq
                        + ", header " + base + ") in " + s.path);
            }
        } finally {
            Unsafe.free(buf, HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        }
    }

    private void ensureReadBuf(int needed) {
        if (needed > readBufCap) {
            long newCap = Math.max(readBufCap * 2, needed);
            readBuf = Unsafe.realloc(readBuf, readBufCap, newCap, MemoryTag.NATIVE_ILP_RSS);
            readBufCap = newCap;
        }
    }

    private String activePathFor(long baseSeq) {
        return dir + "/" + hex16(baseSeq) + ACTIVE_SUFFIX;
    }

    private String sealedPathFor(long baseSeq, long lastSeq) {
        return dir + "/" + hex16(baseSeq) + "-" + hex16(lastSeq) + SEALED_SUFFIX;
    }

    private static String hex16(long v) {
        return String.format("%016x", v);
    }

    private void ensureOpen() {
        if (closed) {
            throw new SfException("SegmentLog is closed");
        }
    }

    /**
     * In-place quicksort over {@code segments[lo, hi)} keyed by unsigned
     * {@code baseSeq}. Median-of-three pivot selection avoids the
     * pathological O(N²) on already-sorted input that {@code readdir} on
     * many filesystems produces. Recursion depth is bounded by ~2 log₂(N);
     * for the documented 16K-segment ceiling that is well under the JVM
     * default stack.
     */
    private void sortSegmentsByBaseSeq(int lo, int hi) {
        while (hi - lo > 1) {
            int mid = (lo + hi) >>> 1;
            long a = segments.getQuick(lo).baseSeq;
            long b = segments.getQuick(mid).baseSeq;
            long c = segments.getQuick(hi - 1).baseSeq;
            // Median of {a, b, c} → pivot index.
            int pivotIdx;
            if (Long.compareUnsigned(a, b) < 0) {
                if (Long.compareUnsigned(b, c) < 0) pivotIdx = mid;
                else if (Long.compareUnsigned(a, c) < 0) pivotIdx = hi - 1;
                else pivotIdx = lo;
            } else {
                if (Long.compareUnsigned(a, c) < 0) pivotIdx = lo;
                else if (Long.compareUnsigned(b, c) < 0) pivotIdx = hi - 1;
                else pivotIdx = mid;
            }
            long pivot = segments.getQuick(pivotIdx).baseSeq;
            swapSegments(pivotIdx, hi - 1);
            int store = lo;
            for (int i = lo; i < hi - 1; i++) {
                if (Long.compareUnsigned(segments.getQuick(i).baseSeq, pivot) < 0) {
                    swapSegments(i, store++);
                }
            }
            swapSegments(store, hi - 1);
            // Recurse on the smaller partition; loop on the larger to keep
            // recursion depth bounded by log₂(N).
            if (store - lo < hi - store - 1) {
                sortSegmentsByBaseSeq(lo, store);
                lo = store + 1;
            } else {
                sortSegmentsByBaseSeq(store + 1, hi);
                hi = store;
            }
        }
    }

    private void swapSegments(int i, int j) {
        if (i == j) return;
        Segment tmp = segments.getQuick(i);
        segments.setQuick(i, segments.getQuick(j));
        segments.setQuick(j, tmp);
    }

    /** Parse `<baseSeq>.sfa` or `<baseSeq>-<lastSeq>.sfs`. Returns null for unrecognized names. */
    private Segment parseFilename(String name) {
        try {
            if (name.endsWith(ACTIVE_SUFFIX)) {
                String body = name.substring(0, name.length() - ACTIVE_SUFFIX.length());
                if (body.length() != 16) {
                    return null;
                }
                Segment s = new Segment();
                s.baseSeq = Long.parseUnsignedLong(body, 16);
                s.path = dir + "/" + name;
                s.pathPtrNative = ff.allocNativePath(s.path);
                s.sealed = false;
                return s;
            }
            if (name.endsWith(SEALED_SUFFIX)) {
                String body = name.substring(0, name.length() - SEALED_SUFFIX.length());
                int dash = body.indexOf('-');
                if (dash != 16 || body.length() != 33) {
                    return null;
                }
                Segment s = new Segment();
                s.baseSeq = Long.parseUnsignedLong(body.substring(0, 16), 16);
                s.lastSeqOnDisk = Long.parseUnsignedLong(body.substring(17), 16);
                s.path = dir + "/" + name;
                s.pathPtrNative = ff.allocNativePath(s.path);
                s.sealed = true;
                return s;
            }
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public interface FrameVisitor {
        boolean visit(long seq, long payloadAddr, int payloadLen);
    }

    static final class Segment {
        long baseSeq;
        long lastSeqOnDisk;  // sealed: filename-derived; active: 0 (use baseSeq + frameCount - 1)
        long frameCount;
        long writePos;
        String path;
        // Native UTF-8 path pointer cached so repeated remove() calls don't
        // re-encode the path on every ACK-driven trim. Owned by the Segment;
        // freed by SegmentLog on trim/rotate (after rename)/close.
        long pathPtrNative;
        int fd = -1;
        boolean sealed;
        // Trim attempted to delete this segment but ff.remove returned false
        // (e.g. Windows sharing-violation, transient NFS error). The .sfs
        // file is still on disk; the next trim() will retry the remove.
        // While true, the segment stays in the in-memory list so:
        //   (a) bytesOnDisk() keeps counting it (sf_max_total_bytes stays
        //       enforceable),
        //   (b) replay() skips it (its frames were already acked — must not
        //       re-ship to the new server).
        boolean removePending;

        long lastSeq() {
            return sealed ? lastSeqOnDisk : (baseSeq + frameCount - 1);
        }
    }
}
