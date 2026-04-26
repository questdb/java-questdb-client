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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

/**
 * One mmap-backed SF segment file. The user thread (the single producer)
 * appends frames into the mapping; the I/O thread (the single consumer) reads
 * up to {@link #publishedOffset()} for wire send. No locks; the cursor pair
 * {@code appendCursor} / {@code publishedCursor} is the only cross-thread
 * coordination, and {@code publishedCursor} is the publish barrier — the
 * I/O thread MUST NOT read any byte at offset {@code >= publishedOffset()}.
 * <p>
 * On-disk layout matches {@link io.questdb.client.cutlass.qwp.client.sf.SegmentLog}
 * so a segment written by the cursor engine can be replayed by the legacy
 * code path on next start (and vice versa). Header and frame format:
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
    // frameCount: number of frames successfully appended (single writer = the
    // producer; SegmentRing reads it after sealing the segment, by which point
    // no further writes will land). Lifecycle gives us happens-before; no
    // volatile needed.
    private long frameCount;
    private long mmapAddress;
    // publishedCursor: written by producer, read by consumer (I/O thread). Volatile
    // because the consumer must see writes in publication order — once the
    // producer bumps publishedCursor, every byte before it is fully written.
    private volatile long publishedCursor;

    private MmapSegment(String path, int fd, long mmapAddress, long sizeBytes,
                        long baseSeq, long initialCursor, long frameCount,
                        boolean memoryBacked) {
        this.path = path;
        this.fd = fd;
        this.mmapAddress = mmapAddress;
        this.sizeBytes = sizeBytes;
        this.baseSeq = baseSeq;
        this.appendCursor = initialCursor;
        this.publishedCursor = initialCursor;
        this.frameCount = frameCount;
        this.memoryBacked = memoryBacked;
    }

    /**
     * Creates a fresh segment file at {@code path}, pre-allocating exactly
     * {@code sizeBytes} bytes and mmapping the whole region RW. Writes the
     * 24-byte header and positions the cursor immediately after it. Throws
     * {@link MmapSegmentException} on any I/O failure (file already exists,
     * disk full, mmap rejected).
     */
    public static MmapSegment create(String path, long baseSeq, long sizeBytes) {
        if (sizeBytes < HEADER_SIZE + FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException(
                    "sizeBytes too small for header + one minimal frame: " + sizeBytes);
        }
        int fd = Files.openCleanRW(path, sizeBytes);
        if (fd < 0) {
            throw new MmapSegmentException("openCleanRW failed for " + path);
        }
        long addr = Files.FAILED_MMAP_ADDRESS;
        try {
            addr = Files.mmap(fd, sizeBytes, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            if (addr == Files.FAILED_MMAP_ADDRESS) {
                throw new MmapSegmentException("mmap failed for " + path);
            }
            // Header goes straight into the mapping — no separate write syscall.
            Unsafe.getUnsafe().putInt(addr, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(addr + 4, VERSION);
            Unsafe.getUnsafe().putByte(addr + 5, (byte) 0); // flags
            Unsafe.getUnsafe().putShort(addr + 6, (short) 0); // reserved
            Unsafe.getUnsafe().putLong(addr + 8, baseSeq);
            Unsafe.getUnsafe().putLong(addr + 16, Os.currentTimeMicros());
            return new MmapSegment(path, fd, addr, sizeBytes, baseSeq, HEADER_SIZE, 0, false);
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                Files.munmap(addr, sizeBytes, MemoryTag.MMAP_DEFAULT);
            }
            Files.close(fd);
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
            return new MmapSegment(null, -1, addr, sizeBytes, baseSeq, HEADER_SIZE, 0, true);
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
     */
    public static MmapSegment openExisting(String path) {
        long fileSize = Files.length(path);
        if (fileSize < HEADER_SIZE) {
            throw new MmapSegmentException("file shorter than header: " + path + " size=" + fileSize);
        }
        int fd = Files.openRW(path);
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
            long lastGood = scanFrames(addr, fileSize);
            long count = countFrames(addr, lastGood);
            return new MmapSegment(path, fd, addr, fileSize, baseSeq, lastGood, count, false);
        } catch (Throwable t) {
            if (addr != Files.FAILED_MMAP_ADDRESS) {
                Files.munmap(addr, fileSize, MemoryTag.MMAP_DEFAULT);
            }
            Files.close(fd);
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
        // CRC over the (payloadLen, payload) pair — same window the legacy
        // SegmentLog uses, so a recovery scan validates either format identically.
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
        frameCount++;
        // Publish last. Until this volatile write retires, the consumer
        // cannot see any of the bytes we just wrote.
        publishedCursor = appendCursor;
        return offset;
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
     * Forward scan that returns the offset just past the last frame whose
     * CRC verifies. A torn-tail frame (declared length runs past EOF, or
     * CRC mismatch) leaves both cursors at the start of that frame; the
     * next {@link #tryAppend} will overwrite it. The scan only reads from
     * the mapping — no syscalls.
     */
    private static long scanFrames(long addr, long fileSize) {
        long pos = HEADER_SIZE;
        while (pos + FRAME_HEADER_SIZE <= fileSize) {
            int crcRead = Unsafe.getUnsafe().getInt(addr + pos);
            int payloadLen = Unsafe.getUnsafe().getInt(addr + pos + 4);
            // Defensive: a corrupt length field could be enormous or negative,
            // both of which would otherwise overrun the mapping.
            if (payloadLen < 0 || pos + FRAME_HEADER_SIZE + payloadLen > fileSize) {
                return pos;
            }
            int crcCalc = Crc32c.update(Crc32c.INIT, addr + pos + 4, 4);
            if (payloadLen > 0) {
                crcCalc = Crc32c.update(crcCalc, addr + pos + FRAME_HEADER_SIZE, payloadLen);
            }
            if (crcCalc != crcRead) {
                return pos;
            }
            pos += FRAME_HEADER_SIZE + payloadLen;
        }
        return pos;
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
