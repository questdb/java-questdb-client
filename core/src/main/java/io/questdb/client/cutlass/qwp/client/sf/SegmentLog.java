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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
    private final long maxBytesPerSegment;
    private final long maxTotalBytes;
    // When true, every successful append() forces fsync of the active segment.
    // Trades throughput for the strongest "data on disk after append returns"
    // guarantee. Default off — fsync runs on rotation and on explicit flush().
    private final boolean fsyncEachAppend;

    private final List<Segment> segments = new ArrayList<>();
    private Segment active;
    private long nextSeq;

    private int lockFd = -1;

    /** 8-byte scratch for writing frame headers. */
    private long envBuf;
    /** Growable read buffer for replay (frame payloads). */
    private long readBuf;
    private long readBufCap;

    private boolean closed;

    private SegmentLog(String dir, long maxBytesPerSegment, long maxTotalBytes, boolean fsyncEachAppend) {
        this.dir = dir;
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
        return open(dir, maxBytesPerSegment, DEFAULT_MAX_TOTAL_BYTES, false);
    }

    /**
     * Open or recover a segment log at the given directory with a total disk-usage
     * cap. When {@code maxTotalBytes} is reached, {@link #append} throws
     * {@link SfDiskFullException}; the caller must wait for {@link #trim} to free
     * space (typically driven by server ACKs).
     */
    public static SegmentLog open(String dir, long maxBytesPerSegment, long maxTotalBytes) {
        return open(dir, maxBytesPerSegment, maxTotalBytes, false);
    }

    /**
     * Open with full configuration. {@code fsyncEachAppend} forces the OS page
     * cache to flush after every successful {@link #append} — slow but gives the
     * strongest "data on disk before append returns" guarantee, surviving even
     * an OS-level crash.
     */
    public static SegmentLog open(String dir, long maxBytesPerSegment, long maxTotalBytes, boolean fsyncEachAppend) {
        if (maxBytesPerSegment < HEADER_SIZE + FRAME_HEADER_SIZE + 16) {
            throw new SfException("maxBytesPerSegment too small: " + maxBytesPerSegment);
        }
        if (maxTotalBytes < maxBytesPerSegment) {
            throw new SfException("maxTotalBytes (" + maxTotalBytes
                    + ") must be >= maxBytesPerSegment (" + maxBytesPerSegment + ")");
        }
        SegmentLog log = new SegmentLog(dir, maxBytesPerSegment, maxTotalBytes, fsyncEachAppend);
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
        long w = Files.write(active.fd, envBuf, FRAME_HEADER_SIZE, pos);
        if (w != FRAME_HEADER_SIZE) {
            // Most likely ENOSPC. Truncate any partial write back so a retry
            // (after disk space frees up) starts at the same position cleanly.
            Files.truncate(active.fd, pos);
            throw new SfDiskFullException("short write of frame header at pos=" + pos
                    + " (got " + w + " of " + FRAME_HEADER_SIZE + ")");
        }
        long w2 = Files.write(active.fd, payloadAddr, payloadLen, pos + FRAME_HEADER_SIZE);
        if (w2 != payloadLen) {
            // Header landed but payload didn't fit. Truncate back to before the
            // header so the file is in a clean state for retry.
            Files.truncate(active.fd, pos);
            throw new SfDiskFullException("short write of payload at pos=" + (pos + FRAME_HEADER_SIZE)
                    + " (got " + w2 + " of " + payloadLen + ")");
        }
        active.writePos = pos + total;
        active.frameCount++;
        nextSeq = seq + 1;
        if (fsyncEachAppend && Files.fsync(active.fd) != 0) {
            throw new SfException("fsync after append failed for " + active.path);
        }
        return seq;
    }

    /** Force durability of the active segment to disk. */
    public void fsync() {
        ensureOpen();
        if (Files.fsync(active.fd) != 0) {
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
        for (Segment seg : segments) {
            if (!replaySegment(seg, visitor)) {
                return;
            }
        }
    }

    /**
     * Delete every sealed segment whose lastSeq is &lt;= ackedSeq. The active
     * segment is never trimmed, even if all of its frames are acked — it is only
     * deleted when sealed by a rotation.
     */
    public void trim(long ackedSeq) {
        ensureOpen();
        int writeIdx = 0;
        for (int i = 0; i < segments.size(); i++) {
            Segment s = segments.get(i);
            if (!s.sealed) {
                segments.set(writeIdx++, s);
                continue;
            }
            if (s.lastSeq() <= ackedSeq) {
                if (s.fd != -1) {
                    Files.close(s.fd);
                    s.fd = -1;
                }
                Files.remove(s.path);
            } else {
                segments.set(writeIdx++, s);
            }
        }
        while (segments.size() > writeIdx) {
            segments.remove(segments.size() - 1);
        }
    }

    /** Lowest seq currently on disk, or -1 if log is empty. */
    public long oldestSeq() {
        ensureOpen();
        if (segments.isEmpty()) {
            return -1;
        }
        Segment first = segments.get(0);
        if (first.frameCount == 0) {
            return -1;
        }
        return first.baseSeq;
    }

    /** Sequence number that will be assigned to the next {@link #append}. */
    public long nextSeq() {
        ensureOpen();
        return nextSeq;
    }

    /** Total bytes used by all segments on disk (header + frames). */
    public long bytesOnDisk() {
        ensureOpen();
        long total = 0;
        for (Segment s : segments) {
            total += s.writePos;
        }
        return total;
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
        for (Segment s : segments) {
            if (s.fd != -1) {
                Files.close(s.fd);
                s.fd = -1;
            }
        }
        segments.clear();
        active = null;
        if (lockFd != -1) {
            Files.close(lockFd);
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
        if (!Files.exists(dir)) {
            int rc = Files.mkdir(dir, 0755);
            if (rc != 0 && !Files.exists(dir)) {
                throw new SfException("cannot create directory: " + dir);
            }
        }

        envBuf = Unsafe.malloc(FRAME_HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        readBufCap = MIN_BUF_BYTES;
        readBuf = Unsafe.malloc(readBufCap, MemoryTag.NATIVE_ILP_RSS);

        // single-writer lock
        String lockPath = dir + "/" + LOCK_FILE_NAME;
        lockFd = Files.openRW(lockPath);
        if (lockFd < 0) {
            throw new SfException("cannot open lock file: " + lockPath);
        }
        if (Files.lock(lockFd) != 0) {
            throw new SfException("SegmentLog at " + dir + " is locked by another process");
        }

        scanDirectory();
        if (active == null) {
            createActive(FIRST_SEQ);
        }
        nextSeq = active.baseSeq + active.frameCount;
    }

    private void scanDirectory() {
        long find = Files.findFirst(dir);
        if (find == 0) {
            return;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                int type = Files.findType(find);
                if (name != null && type != Files.DT_DIR && !LOCK_FILE_NAME.equals(name)) {
                    Segment s = parseFilename(name);
                    if (s != null) {
                        segments.add(s);
                    }
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }

        segments.sort(Comparator.comparingLong(s -> s.baseSeq));

        // Validate: at most one active segment, and only as the last entry.
        for (int i = 0; i < segments.size(); i++) {
            Segment s = segments.get(i);
            if (!s.sealed && i != segments.size() - 1) {
                throw new SfException("multiple active segments found, second one: " + s.path);
            }
        }

        for (Segment s : segments) {
            openSegment(s);
            if (s.sealed) {
                // trust filename's lastSeq, but verify file size is consistent
                long want = HEADER_SIZE; // body checked lazily on replay
                if (Files.length(s.fd) < want) {
                    throw new SfException("sealed segment shorter than header: " + s.path);
                }
                s.writePos = Files.length(s.fd);
                s.frameCount = (s.lastSeqOnDisk - s.baseSeq) + 1;
            } else {
                long count = scanActive(s);
                s.frameCount = count;
                active = s;
            }
        }
    }

    /** Returns frame count after truncating any torn tail. Updates s.writePos. */
    private long scanActive(Segment s) {
        long fileLen = Files.length(s.fd);
        long pos = HEADER_SIZE;
        long count = 0;
        while (pos < fileLen) {
            if (pos + FRAME_HEADER_SIZE > fileLen) {
                break;
            }
            long r = Files.read(s.fd, envBuf, FRAME_HEADER_SIZE, pos);
            if (r != FRAME_HEADER_SIZE) {
                break;
            }
            int crc = Unsafe.getUnsafe().getInt(envBuf);
            int payloadLen = Unsafe.getUnsafe().getInt(envBuf + 4);
            if (payloadLen <= 0 || pos + FRAME_HEADER_SIZE + payloadLen > fileLen) {
                break;
            }
            ensureReadBuf(payloadLen);
            long r2 = Files.read(s.fd, readBuf, payloadLen, pos + FRAME_HEADER_SIZE);
            if (r2 != payloadLen) {
                break;
            }
            int computed = Crc32c.update(Crc32c.INIT, envBuf + 4, 4);
            computed = Crc32c.update(computed, readBuf, payloadLen);
            if (computed != crc) {
                break;
            }
            pos += FRAME_HEADER_SIZE + payloadLen;
            count++;
        }
        if (pos < fileLen) {
            // torn tail or trailing garbage from a partial pre-allocation: truncate.
            if (!Files.truncate(s.fd, pos)) {
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
        long fileLen = Files.length(s.fd);
        long pos = HEADER_SIZE;
        long seq = s.baseSeq;
        while (pos < fileLen) {
            if (pos + FRAME_HEADER_SIZE > fileLen) {
                break;
            }
            long r = Files.read(s.fd, envBuf, FRAME_HEADER_SIZE, pos);
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
            long r2 = Files.read(s.fd, readBuf, payloadLen, pos + FRAME_HEADER_SIZE);
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
        if (Files.fsync(old.fd) != 0) {
            throw new SfException("fsync failed during rotate of " + old.path);
        }
        Files.close(old.fd);
        old.fd = -1;
        long lastSeq = old.baseSeq + old.frameCount - 1;
        if (old.frameCount == 0) {
            // empty segment shouldn't happen via rotate, but be defensive: drop it
            Files.remove(old.path);
            segments.remove(segments.size() - 1);
            createActive(old.baseSeq);
            return;
        }
        String sealedPath = sealedPathFor(old.baseSeq, lastSeq);
        if (Files.rename(old.path, sealedPath) != 0) {
            throw new SfException("failed to seal segment by rename " + old.path + " -> " + sealedPath);
        }
        old.path = sealedPath;
        old.sealed = true;
        old.lastSeqOnDisk = lastSeq;
        createActive(lastSeq + 1);
    }

    private void createActive(long baseSeq) {
        String path = activePathFor(baseSeq);
        int fd = Files.openCleanRW(path, 0);
        if (fd < 0) {
            throw new SfException("cannot create active segment: " + path);
        }
        Segment s = new Segment();
        s.baseSeq = baseSeq;
        s.path = path;
        s.fd = fd;
        s.sealed = false;
        s.frameCount = 0;
        writeHeader(s);
        s.writePos = HEADER_SIZE;
        if (Files.fsync(fd) != 0) {
            throw new SfException("fsync failed for new active segment " + path);
        }
        segments.add(s);
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
            long w = Files.write(s.fd, buf, HEADER_SIZE, 0);
            if (w != HEADER_SIZE) {
                throw new SfException("short write of header to " + s.path);
            }
        } finally {
            Unsafe.free(buf, HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        }
    }

    private void openSegment(Segment s) {
        s.fd = Files.openRW(s.path);
        if (s.fd < 0) {
            throw new SfException("cannot open segment: " + s.path);
        }
        long len = Files.length(s.fd);
        if (len < HEADER_SIZE) {
            throw new SfException("segment shorter than header: " + s.path);
        }
        long buf = Unsafe.malloc(HEADER_SIZE, MemoryTag.NATIVE_ILP_RSS);
        try {
            long r = Files.read(s.fd, buf, HEADER_SIZE, 0);
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
        int fd = -1;
        boolean sealed;

        long lastSeq() {
            return sealed ? lastSeqOnDisk : (baseSeq + frameCount - 1);
        }
    }
}
