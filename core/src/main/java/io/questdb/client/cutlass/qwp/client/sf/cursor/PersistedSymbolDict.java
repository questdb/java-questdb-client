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

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Append-only, per-slot persistence of the global symbol dictionary that a
 * store-and-forward sender ships to the server with delta encoding. Lives at
 * {@code <slot>/.symbol-dict} alongside the segment files, the slot lock and
 * {@code .ack-watermark}.
 * <p>
 * Delta-encoded SF frames are NOT self-sufficient: a frame carries only the
 * symbols it introduces, so recovering (process restart) or draining (orphan
 * adoption) a slot requires re-registering the whole dictionary on the fresh
 * server before those frames replay. This file is that dictionary. Unlike
 * {@link AckWatermark} -- a discardable optimization protected by a
 * {@code max()} clamp -- this file is <b>load-bearing</b>: a surviving frame
 * that references an id missing from it is unrecoverable. It is therefore held
 * to a stronger durability contract.
 * <p>
 * <b>Layout</b> (little-endian):
 * <pre>
 *   offset 0: u32 magic = 'SYD1'
 *   offset 4: u8  version = 1
 *   offset 5: 3 bytes reserved (zero)
 *   offset 8: entries, each [len: varint][utf8 bytes], in ascending global-id order
 * </pre>
 * Symbol id {@code i} is the {@code i}-th entry (ids are dense and assigned
 * sequentially from 0), so no id needs to be stored.
 * <p>
 * <b>Durability / write-ahead ordering:</b> the producer appends the symbols a
 * frame introduces BEFORE that frame is published to the ring, but does NOT
 * fsync -- matching the rest of store-and-forward, which is page-cache (not
 * disk) durable. This ordering is sufficient for a <b>process/JVM crash</b>: the
 * page cache survives, so both the dictionary and the frames survive and the
 * dictionary is a superset of every recoverable frame's references. It is NOT
 * sufficient for a <b>host/power crash</b>, where unflushed pages can be lost out
 * of order and the dictionary may end up torn relative to the frames it serves --
 * exactly as the segment frames themselves may be lost on a host crash. The send
 * loop's replay guard catches the COMMON host-crash outcome -- a truncated tail,
 * where a frame's delta start id exceeds the recovered dictionary size -- and
 * fails loudly (the unreplayable data must be resent) rather than sending a
 * gapped frame. It does NOT catch every host-crash tear: an interior page lost
 * out of order reads back as zeroes that parse as empty-string entries, and a
 * failed best-effort truncate (see {@link #open}) can leave a stale trailing
 * entry -- either SHIFTS the dense id->symbol mapping without changing the entry
 * count the guard checks, so a replay silently misattributes symbols. That
 * residual exposure sits within the "not host-crash durable" boundary above; a
 * per-entry or running CRC would be needed to close it (the 3 reserved header
 * bytes leave room for a future integrity field).
 * <p>
 * A torn trailing entry from a crash mid-append is self-healing: {@link #open}
 * stops parsing at the first incomplete entry and the next append overwrites it.
 * <p>
 * <b>Lifecycle:</b> single-writer (the producer / user thread). Read once at
 * {@link #open} to seed in-memory state on recovery or orphan-drain. Owner
 * (the engine) closes it. Not thread-safe for concurrent writers.
 */
public final class PersistedSymbolDict implements QuietCloseable {

    /**
     * Filename within the slot directory. Dot-prefixed so directory
     * enumerators that filter by the {@code .sfa} suffix (segment recovery,
     * OrphanScanner, trim) skip it automatically.
     */
    public static final String FILE_NAME = ".symbol-dict";
    static final int FILE_MAGIC = 0x31445953; // 'SYD1' little-endian
    static final int HEADER_SIZE = 8;
    static final byte VERSION = 1;
    private static final Logger LOG = LoggerFactory.getLogger(PersistedSymbolDict.class);
    private final int fd;
    private long appendOffset;
    private boolean closed;
    // In-memory copy of the entry region [len][utf8]... exactly as on disk,
    // populated only when open() recovered existing entries (recovery /
    // orphan-drain). Zero/empty for a freshly created file. READ (not consumed) to
    // seed the producer's id map (readLoadedSymbols) and to seed the send loop's
    // catch-up mirror, which COPIES it. This file retains ownership for the engine's
    // lifetime -- the orphan drainer builds a fresh send loop per wire session
    // against the same engine, and each must re-seed its mirror -- and frees this
    // buffer in close().
    private long loadedEntriesAddr;
    private int loadedEntriesLen;
    private long scratchAddr;
    private int scratchCap;
    private int size;

    private PersistedSymbolDict(int fd, long appendOffset, int size, long loadedEntriesAddr, int loadedEntriesLen) {
        this.fd = fd;
        this.appendOffset = appendOffset;
        this.size = size;
        this.loadedEntriesAddr = loadedEntriesAddr;
        this.loadedEntriesLen = loadedEntriesLen;
    }

    /**
     * Opens (creating if absent) the dictionary file in {@code slotDir}. An
     * existing file is parsed and its complete entries are loaded into memory
     * (see {@link #loadedEntriesAddr()}); a missing or invalid file is (re)created
     * with a fresh header. Returns {@code null} on any I/O failure -- the caller
     * then falls back to full-dictionary (self-sufficient) frames for this slot,
     * so a broken side-file degrades gracefully rather than aborting the sender.
     */
    public static PersistedSymbolDict open(String slotDir) {
        String filePath = slotDir + "/" + FILE_NAME;
        long existing = Files.exists(filePath) ? Files.length(filePath) : -1L;
        if (existing >= HEADER_SIZE) {
            PersistedSymbolDict d = openExisting(filePath, existing);
            if (d != null) {
                return d;
            }
            // Fall through to a clean re-create: a header/parse failure on an
            // existing file means it cannot be trusted for delta replay.
        }
        return openFresh(filePath);
    }

    /**
     * Best-effort removal of a stale dictionary file. Used at fresh-start (a
     * stale dict with no segments behind it is meaningless) and at fully-drained
     * close (the slot is empty, nothing references the dictionary any more),
     * mirroring {@link AckWatermark#removeOrphan}.
     */
    public static void removeOrphan(String slotDir) {
        Files.remove(slotDir + "/" + FILE_NAME);
    }

    /**
     * Appends {@code count} already-encoded entries -- {@code [len varint][utf8]...},
     * the exact layout {@link #appendSymbols} produces -- verbatim from {@code addr}
     * in a single write. The producer uses this when it already holds those bytes
     * (the symbol-dict delta section the frame encoder just wrote), so it does not
     * re-encode the same symbols. Writes {@code len} bytes and advances {@code size}
     * by {@code count}. Same durability/idempotency contract as {@link #appendSymbols}:
     * no fsync, and a short write throws WITHOUT advancing {@code size}/{@code
     * appendOffset}, so a retry keyed off {@link #size()} re-persists the same range
     * at the same offset. No-op when the range is empty or the dictionary is closed.
     */
    public void appendRawEntries(long addr, int len, int count) {
        if (closed || count <= 0 || len <= 0) {
            return;
        }
        long written = Files.write(fd, addr, len, appendOffset);
        if (written != len) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + len + ", actual=" + written + ']');
        }
        appendOffset += len;
        size += count;
    }

    /**
     * Appends one symbol, extending the on-disk dictionary. The caller appends a
     * frame's new symbols BEFORE publishing that frame, so the write ordering
     * (dictionary entry before referencing frame) holds; no fsync is performed
     * (see the class-level durability note). Assigns the next dense id implicitly
     * (the entry's position).
     */
    public void appendSymbol(CharSequence symbol) {
        if (closed) {
            return;
        }
        int utf8Len = Utf8s.utf8Bytes(symbol);
        int varLen = varintSize(utf8Len);
        int recLen = varLen + utf8Len;
        ensureScratch(recLen);
        long p = writeVarint(scratchAddr, utf8Len);
        if (utf8Len > 0) {
            Utf8s.strCpyUtf8(symbol, p, utf8Len);
        }
        long written = Files.write(fd, scratchAddr, recLen, appendOffset);
        if (written != recLen) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + recLen + ", actual=" + written + ']');
        }
        appendOffset += recLen;
        size++;
    }

    /**
     * Appends the dense id range {@code [from .. to]} in a SINGLE write. Encodes
     * the whole {@code [len varint][utf8]...} region into scratch first, then
     * issues one positioned write -- versus one {@code pwrite(2)} per symbol via
     * {@link #appendSymbol}. That per-symbol syscall count is the hot-path cost
     * on a high-cardinality batch (one new symbol per row), which is exactly the
     * store-and-forward workload delta encoding targets. Callers pass the
     * dictionary and the range so the ids resolve to their symbol strings.
     * <p>
     * Same durability and idempotency contract as {@link #appendSymbol}: no
     * fsync, and a short write throws WITHOUT advancing {@code size}/{@code
     * appendOffset}, so a retry keyed off {@link #size()} re-encodes the same
     * range and overwrites at the same offset. No-op when the range is empty or
     * the dictionary is closed.
     */
    public void appendSymbols(GlobalSymbolDictionary dict, int from, int to) {
        if (closed || to < from) {
            return;
        }
        int len = 0;
        for (int id = from; id <= to; id++) {
            CharSequence symbol = dict.getSymbol(id);
            int utf8Len = Utf8s.utf8Bytes(symbol);
            int recLen = varintSize(utf8Len) + utf8Len;
            ensureScratch(len + recLen);
            long p = writeVarint(scratchAddr + len, utf8Len);
            if (utf8Len > 0) {
                Utf8s.strCpyUtf8(symbol, p, utf8Len);
            }
            len += recLen;
        }
        long written = Files.write(fd, scratchAddr, len, appendOffset);
        if (written != len) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + len + ", actual=" + written + ']');
        }
        appendOffset += len;
        size += to - from + 1;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (loadedEntriesAddr != 0L) {
            Unsafe.free(loadedEntriesAddr, loadedEntriesLen, MemoryTag.NATIVE_DEFAULT);
        }
        if (scratchAddr != 0L) {
            Unsafe.free(scratchAddr, scratchCap, MemoryTag.NATIVE_DEFAULT);
            scratchAddr = 0L;
            scratchCap = 0;
        }
        if (fd >= 0) {
            Files.close(fd);
        }
    }

    /**
     * Base address of the loaded entry region -- the concatenated
     * {@code [len][utf8]} bytes of every recovered symbol in id order, exactly
     * as a delta section carries them. Zero when nothing was recovered.
     */
    public long loadedEntriesAddr() {
        return loadedEntriesAddr;
    }

    /**
     * Byte length of {@link #loadedEntriesAddr()}.
     */
    public int loadedEntriesLen() {
        return loadedEntriesLen;
    }

    /**
     * Materialises the loaded entries as symbol strings in ascending-id order
     * (entry {@code i} is symbol id {@code i}). Used once on recovery to
     * repopulate the producer's global dictionary. Empty when nothing was
     * recovered.
     */
    public ObjList<String> readLoadedSymbols() {
        ObjList<String> out = new ObjList<>(Math.max(size, 1));
        long p = loadedEntriesAddr;
        long limit = p + loadedEntriesLen;
        for (int i = 0; i < size && p < limit; i++) {
            long len = 0;
            int shift = 0;
            while (p < limit) {
                byte b = Unsafe.getUnsafe().getByte(p++);
                len |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            if (p + len > limit) {
                break; // defensive: torn tail (should not happen past parse in open)
            }
            out.add(Utf8s.stringFromUtf8Bytes(p, p + len));
            p += len;
        }
        return out;
    }

    /**
     * Number of symbols the dictionary holds (highest id + 1).
     */
    public int size() {
        return size;
    }

    private static PersistedSymbolDict openExisting(String filePath, long fileLen) {
        int fd = Files.openRW(filePath);
        if (fd < 0) {
            LOG.warn("symbol dict {} could not be opened (rc={}); recreating", filePath, fd);
            return null;
        }
        long buf = 0L;
        try {
            int len = (int) fileLen;
            buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long read = Files.read(fd, buf, len, 0);
            if (read != len || Unsafe.getUnsafe().getInt(buf) != FILE_MAGIC) {
                LOG.warn("symbol dict {} unreadable or bad magic; recreating", filePath);
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                Files.close(fd);
                return null;
            }
            // Parse complete entries starting after the header; stop at the
            // first torn/incomplete trailing entry.
            int pos = HEADER_SIZE;
            int count = 0;
            while (pos < len) {
                long[] vr = decodeVarint(buf, pos, len);
                if (vr == null) {
                    break; // torn length varint
                }
                long entryLen = vr[0];
                int next = (int) vr[1];
                // The file length is the sole sanity bound: a length that runs past
                // the file is a torn/incomplete trailing entry and stops the parse
                // (self-healing tail). There is deliberately NO fixed per-entry
                // ceiling -- the write path (appendSymbol/appendSymbols) applies none
                // either, so a legitimately persisted large symbol must recover here,
                // not be truncated and then trip the send loop's "resend required"
                // guard on a normal process-crash recovery. entryLen stays a long so a
                // corrupt multi-gigabyte length cannot wrap an int back under the check.
                if ((long) next + entryLen > len) {
                    break; // torn/incomplete trailing entry -- self-healing tail
                }
                pos = next + (int) entryLen;
                count++;
            }
            int entriesLen = pos - HEADER_SIZE;
            long entriesAddr = 0L;
            if (entriesLen > 0) {
                entriesAddr = Unsafe.malloc(entriesLen, MemoryTag.NATIVE_DEFAULT);
                Unsafe.getUnsafe().copyMemory(buf + HEADER_SIZE, entriesAddr, entriesLen);
            }
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            buf = 0L;
            // Physically drop any torn/stale trailing bytes (a crash mid-append)
            // so a LATER, shorter append cannot leave residue past its own end
            // that a future recovery mis-parses as a ghost symbol -- which would
            // shift every subsequent dense id. Without this, appendOffset already
            // lands just past the last complete entry, so the next append
            // overwrites FROM there, but only truncation guarantees nothing
            // survives BEYOND it. Best-effort: a failed truncate (rare -- an I/O
            // error) leaves a latent silent-corruption path, not merely a lost
            // optimization: if a later, shorter append then leaves stale bytes past
            // its end, a subsequent recovery mis-parses them as a ghost symbol and
            // shifts the dense ids, and the count-based replay guard does not catch
            // it (see the class-level durability note).
            long validLen = HEADER_SIZE + entriesLen;
            if (validLen < len) {
                Files.truncate(fd, validLen);
            }
            return new PersistedSymbolDict(fd, HEADER_SIZE + entriesLen, count, entriesAddr, entriesLen);
        } catch (Throwable t) {
            if (buf != 0L) {
                Unsafe.free(buf, (int) fileLen, MemoryTag.NATIVE_DEFAULT);
            }
            Files.close(fd);
            LOG.warn("symbol dict {} recovery failed ({}); recreating", filePath, String.valueOf(t));
            return null;
        }
    }

    private static PersistedSymbolDict openFresh(String filePath) {
        int fd = Files.openCleanRW(filePath);
        if (fd < 0) {
            LOG.warn("symbol dict {} could not be created (rc={}); proceeding without it", filePath, fd);
            return null;
        }
        long hdr = Unsafe.malloc(HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(hdr, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(hdr + 4, VERSION);
            Unsafe.getUnsafe().putByte(hdr + 5, (byte) 0);
            Unsafe.getUnsafe().putByte(hdr + 6, (byte) 0);
            Unsafe.getUnsafe().putByte(hdr + 7, (byte) 0);
            long written = Files.write(fd, hdr, HEADER_SIZE, 0);
            if (written != HEADER_SIZE) {
                Files.close(fd);
                Files.remove(filePath);
                LOG.warn("symbol dict {} header write failed; proceeding without it", filePath);
                return null;
            }
        } finally {
            Unsafe.free(hdr, HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        return new PersistedSymbolDict(fd, HEADER_SIZE, 0, 0L, 0);
    }

    /**
     * Decodes an unsigned LEB128 varint from {@code buf[pos..limit)}. Returns
     * {@code [value, newPos]} or {@code null} if the varint is truncated
     * (torn tail).
     */
    private static long[] decodeVarint(long buf, int pos, int limit) {
        long value = 0;
        int shift = 0;
        int cur = pos;
        while (cur < limit) {
            byte b = Unsafe.getUnsafe().getByte(buf + cur);
            cur++;
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return new long[]{value, cur};
            }
            shift += 7;
            if (shift > 35) {
                return null; // implausible for an entry length; treat as torn
            }
        }
        return null;
    }

    private static int varintSize(long value) {
        int n = 1;
        while (value > 0x7F) {
            value >>>= 7;
            n++;
        }
        return n;
    }

    private static long writeVarint(long addr, long value) {
        while (value > 0x7F) {
            Unsafe.getUnsafe().putByte(addr++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(addr++, (byte) value);
        return addr;
    }

    private void ensureScratch(int required) {
        if (scratchCap >= required) {
            return;
        }
        int newCap = Math.max(required, Math.max(256, scratchCap * 2));
        scratchAddr = Unsafe.realloc(scratchAddr, scratchCap, newCap, MemoryTag.NATIVE_DEFAULT);
        scratchCap = newCap;
    }
}
