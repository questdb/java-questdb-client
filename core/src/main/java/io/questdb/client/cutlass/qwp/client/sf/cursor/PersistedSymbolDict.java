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
import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;
import org.jetbrains.annotations.TestOnly;
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
 *   offset 4: u8  version = 2
 *   offset 5: 3 bytes reserved (zero)
 *   offset 8: entries, each [len: varint][utf8 bytes][crc32c: u32], in ascending global-id order
 * </pre>
 * Symbol id {@code i} is the {@code i}-th entry (ids are dense and assigned
 * sequentially from 0), so no id needs to be stored. Each entry carries a
 * CRC-32C over its {@code [len][utf8]} bytes (the same checksum the SF segment
 * frames use), so a torn or stale entry is detected on recovery instead of
 * being silently mis-parsed.
 * <p>
 * <b>Durability / write-ahead ordering:</b> the producer appends the symbols a
 * frame introduces BEFORE that frame is published to the ring, but does NOT
 * fsync -- matching the rest of store-and-forward, which is page-cache (not
 * disk) durable. This ordering is sufficient for a <b>process/JVM crash</b>: the
 * page cache survives, so both the dictionary and the frames survive and the
 * dictionary is a superset of every recoverable frame's references. It is NOT
 * sufficient for a <b>host/power crash</b>, where unflushed pages can be lost out
 * of order and the dictionary may end up torn relative to the frames it serves --
 * exactly as the segment frames themselves may be lost on a host crash. Two
 * layers keep a host-crash tear from silently corrupting data:
 * <ul>
 *   <li>The per-entry CRC-32C: {@link #open} verifies every entry and stops at
 *       the first one whose checksum fails, so an interior page lost out of
 *       order (reading back as zeroes) or a stale entry left past the end by a
 *       failed truncate is DETECTED and the trusted region ends before it --
 *       recovery never mis-parses a corrupt entry as a real symbol nor shifts
 *       the dense id->symbol map. The truncate that drops a torn/stale tail is
 *       now failure-checked (see {@link #open}): a file that cannot be trimmed
 *       is untrusted and recreated empty rather than left exposing stale bytes.</li>
 *   <li>The send loop's replay guard: once recovery trusts only the intact
 *       prefix, a surviving frame whose delta start id exceeds that prefix
 *       fails loudly (the unreplayable data must be resent) rather than sending
 *       a gapped frame.</li>
 * </ul>
 * Together these turn every detectable host-crash tear into a fail-clean
 * "resend required" instead of a silent symbol misattribution -- the same
 * CRC-32C protection the segment frames carry. A tear that happened to leave a
 * byte run whose CRC still matches is not distinguished, but that is a 1-in-2^32
 * collision per corrupted entry, no weaker than the frames' own checksum.
 * <p>
 * A torn trailing entry from a crash mid-append is self-healing: {@link #open}
 * stops parsing at the first incomplete entry and the next append overwrites it.
 * <p>
 * <b>Lifecycle:</b> single-writer (the producer / user thread) for appends. Read
 * once at {@link #open} to seed in-memory state on recovery or orphan-drain. The
 * owner (the engine) closes it, and {@code close()} is callable from any thread
 * (a shutdown hook, test cleanup). {@code close()} and the append methods are
 * therefore {@code synchronized}: without that, a close racing an in-flight append
 * could free the scratch buffer or close the fd mid-write and let the write land
 * on a descriptor the OS has reused for another file (silent cross-file
 * corruption). Not thread-safe for concurrent writers.
 */
public final class PersistedSymbolDict implements QuietCloseable {

    /**
     * Filename within the slot directory. Dot-prefixed so directory
     * enumerators that filter by the {@code .sfa} suffix (segment recovery,
     * OrphanScanner, trim) skip it automatically.
     */
    public static final String FILE_NAME = ".symbol-dict";
    static final int CRC_SIZE = 4; // u32 CRC-32C trailing every entry
    static final int FILE_MAGIC = 0x31445953; // 'SYD1' little-endian
    static final int HEADER_SIZE = 8;
    static final byte VERSION = 2; // v2 appended the per-entry CRC-32C
    private static final Logger LOG = LoggerFactory.getLogger(PersistedSymbolDict.class);
    private final int fd;
    // Filesystem seam. Production is FilesFacade.INSTANCE (straight to Files);
    // tests inject a fault facade to exercise recovery I/O failures (a truncate
    // that cannot drop a torn tail, a short write) without a real broken disk.
    private final FilesFacade ff;
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

    private PersistedSymbolDict(FilesFacade ff, int fd, long appendOffset, int size, long loadedEntriesAddr, int loadedEntriesLen) {
        this.ff = ff;
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
        return open(FilesFacade.INSTANCE, slotDir);
    }

    /**
     * Facade-aware variant of {@link #open(String)}. Production passes
     * {@link FilesFacade#INSTANCE}; tests inject a fault facade to drive recovery
     * I/O failures (e.g. a truncate that cannot drop a torn tail).
     */
    public static PersistedSymbolDict open(FilesFacade ff, String slotDir) {
        String filePath = slotDir + "/" + FILE_NAME;
        long existing = ff.exists(filePath) ? ff.length(filePath) : -1L;
        // A dictionary that grew to or past Integer.MAX_VALUE cannot be reopened:
        // openExisting reads it into ONE int-sized native buffer. PAST 2GiB the
        // (int) cast is either negative (malloc rejects it), exactly zero (getInt
        // then reads 4 bytes past a zero-size allocation), or a small positive prefix
        // (whose validLen < len branch would then DESTRUCTIVELY truncate the multi-GB
        // file); AT exactly Integer.MAX_VALUE the cast is exact but the ~2GB malloc is
        // doomed to OutOfMemoryError. The >= guard short-circuits every case to a
        // clean re-create instead of the doomed allocation -- fail-clean, exactly like
        // every other unreadable-file case here, so the sender falls back to full
        // self-sufficient frames. Reaching this needs ~100M+ distinct symbols on one
        // slot (far past realistic symbol cardinality); the guard keeps the read/write
        // size boundary safe anyway.
        if (existing >= Integer.MAX_VALUE) {
            LOG.warn("symbol dict {} too large ({} bytes) to reopen; recreating empty", filePath, existing);
            return openFresh(ff, filePath);
        }
        if (existing >= HEADER_SIZE) {
            PersistedSymbolDict d = openExisting(ff, filePath, existing);
            if (d != null) {
                return d;
            }
            // Fall through to a clean re-create: a header/parse failure on an
            // existing file means it cannot be trusted for delta replay.
        }
        return openFresh(ff, filePath);
    }

    /**
     * Opens the dictionary in {@code slotDir} as a FRESH, EMPTY file, discarding
     * any surviving content. This is the fresh-start counterpart to {@link #open}:
     * a slot with no recovered segments must start with an empty dictionary, so a
     * dictionary left by a prior lifecycle -- a fully-drained slot whose
     * best-effort delete failed, or a crash in the close window -- must NOT be
     * inherited. Unlike {@link #open}, which parses and TRUSTS an existing file for
     * recovery/orphan-drain replay, this truncates it: the fresh-start producer is
     * not seeded from the dictionary, so trusting a survivor would leave the
     * producer's ids diverged from the dictionary the send loop replays and
     * silently misattribute symbols on the next reconnect. Truncating (rather than
     * relying on an unlink succeeding first) closes the gap even when the delete is
     * refused -- e.g. a Windows share lock. Returns {@code null} on I/O failure, so
     * the caller falls back to full self-sufficient frames exactly as {@link #open}
     * does.
     */
    public static PersistedSymbolDict openClean(String slotDir) {
        return openClean(FilesFacade.INSTANCE, slotDir);
    }

    /**
     * Facade-aware variant of {@link #openClean(String)}.
     */
    public static PersistedSymbolDict openClean(FilesFacade ff, String slotDir) {
        return openFresh(ff, slotDir + "/" + FILE_NAME);
    }

    /**
     * Best-effort removal of a stale dictionary file. Used at fully-drained close
     * (the slot is empty, nothing references the dictionary any more), mirroring
     * {@link AckWatermark#removeOrphan}. The fresh-start path deliberately does NOT
     * use this -- it opens a clean dictionary via {@link #openClean} instead, so a
     * failed delete cannot leave a stale dictionary a new session would trust.
     */
    public static void removeOrphan(String slotDir) {
        removeOrphan(FilesFacade.INSTANCE, slotDir);
    }

    /**
     * Facade-aware variant of {@link #removeOrphan(String)}.
     */
    public static void removeOrphan(FilesFacade ff, String slotDir) {
        ff.remove(slotDir + "/" + FILE_NAME);
    }

    /**
     * Appends {@code count} wire entries -- {@code [len varint][utf8]...}, the
     * symbol-dict delta section the frame encoder just wrote -- to the on-disk
     * dictionary, computing and appending a per-entry CRC-32C as it copies so the
     * producer does not re-encode the symbols. The on-disk layout is
     * {@code [len varint][utf8][crc32c]} per entry (see the class layout note); the
     * {@code addr}/{@code len} bytes carry no CRC, so this walks the {@code count}
     * entries to insert one. Advances {@code size} by {@code count}. Same
     * durability/idempotency contract as {@link #appendSymbols}: no fsync, and a
     * short write throws WITHOUT advancing {@code size}/{@code appendOffset}, so a
     * retry keyed off {@link #size()} re-persists the same range at the same
     * offset. No-op when the range is empty or the dictionary is closed.
     */
    public synchronized void appendRawEntries(long addr, int len, int count) {
        if (closed || count <= 0 || len <= 0) {
            return;
        }
        int outLen = len + count * CRC_SIZE;
        ensureScratch(outLen);
        long src = addr;
        long srcLimit = addr + len;
        long dst = scratchAddr;
        for (int i = 0; i < count; i++) {
            long entryStart = src;
            long symLen = 0;
            int shift = 0;
            while (src < srcLimit) {
                byte b = Unsafe.getUnsafe().getByte(src++);
                symLen |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
                if (shift > 35) {
                    // A canonical entry-length varint is <= 5 bytes; a longer
                    // continuation run is corrupt. The downstream entryEnd > srcLimit
                    // check then rejects it. Matches decodeVarint / readVarintAt.
                    break;
                }
            }
            long entryEnd = src + symLen; // src is just past the len varint
            if (entryEnd > srcLimit) {
                throw new IllegalStateException("malformed raw symbol-dict entries to " + FILE_NAME
                        + " [entry=" + i + ", count=" + count + ']');
            }
            int wireSpan = (int) (entryEnd - entryStart); // [len][utf8]
            Unsafe.getUnsafe().copyMemory(entryStart, dst, wireSpan);
            Unsafe.getUnsafe().putInt(dst + wireSpan, Crc32c.update(Crc32c.INIT, entryStart, wireSpan));
            dst += wireSpan + CRC_SIZE;
            src = entryEnd;
        }
        if (src != srcLimit) {
            // The count entries did not consume exactly len bytes -- a caller passed an
            // inconsistent (addr, len, count) triple. Writing outLen would flush an
            // uninitialised scratch tail and mis-advance size, so fail loudly. The sole
            // caller derives count and len from one beginMessage, so this cannot fire
            // today.
            throw new IllegalStateException("raw symbol-dict entries under-filled the buffer to "
                    + FILE_NAME + " [count=" + count + ", len=" + len
                    + ", consumed=" + (int) (src - addr) + ']');
        }
        long written = ff.write(fd, scratchAddr, outLen, appendOffset);
        if (written != outLen) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + outLen + ", actual=" + written + ']');
        }
        appendOffset += outLen;
        size += count;
    }

    /**
     * Appends one symbol, extending the on-disk dictionary. The caller appends a
     * frame's new symbols BEFORE publishing that frame, so the write ordering
     * (dictionary entry before referencing frame) holds; no fsync is performed
     * (see the class-level durability note). Assigns the next dense id implicitly
     * (the entry's position). Writes {@code [len varint][utf8][crc32c]}, the CRC
     * covering the {@code [len][utf8]} bytes so a torn/stale entry is detected on
     * recovery.
     * <p>
     * Test-only: production persists a frame's whole new-symbol range in one write
     * via {@link #appendSymbols} / {@link #appendRawEntries}. Tests use this to
     * build a dictionary one entry at a time.
     */
    @TestOnly
    public synchronized void appendSymbol(CharSequence symbol) {
        if (closed) {
            return;
        }
        int utf8Len = Utf8s.utf8Bytes(symbol);
        int varLen = NativeBufferWriter.varintSize(utf8Len);
        int wireLen = varLen + utf8Len;  // [len][utf8]
        int recLen = wireLen + CRC_SIZE; // + trailing crc
        ensureScratch(recLen);
        long p = NativeBufferWriter.writeVarint(scratchAddr, utf8Len);
        if (utf8Len > 0) {
            Utf8s.strCpyUtf8(symbol, p, utf8Len);
        }
        Unsafe.getUnsafe().putInt(scratchAddr + wireLen, Crc32c.update(Crc32c.INIT, scratchAddr, wireLen));
        long written = ff.write(fd, scratchAddr, recLen, appendOffset);
        if (written != recLen) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + recLen + ", actual=" + written + ']');
        }
        appendOffset += recLen;
        size++;
    }

    /**
     * Appends the dense id range {@code [from .. to]} in a SINGLE write. Encodes
     * the whole {@code [len varint][utf8][crc32c]...} region into scratch first,
     * then issues one positioned write -- versus one {@code pwrite(2)} per symbol
     * via {@link #appendSymbol}. That per-symbol syscall count is the hot-path
     * cost on a high-cardinality batch (one new symbol per row), which is exactly
     * the store-and-forward workload delta encoding targets. Each entry carries a
     * trailing CRC-32C over its {@code [len][utf8]} bytes. Callers pass the
     * dictionary and the range so the ids resolve to their symbol strings.
     * <p>
     * Same durability and idempotency contract as {@link #appendSymbol}: no
     * fsync, and a short write throws WITHOUT advancing {@code size}/{@code
     * appendOffset}, so a retry keyed off {@link #size()} re-encodes the same
     * range and overwrites at the same offset. No-op when the range is empty or
     * the dictionary is closed.
     */
    public synchronized void appendSymbols(GlobalSymbolDictionary dict, int from, int to) {
        if (closed || to < from) {
            return;
        }
        int len = 0;
        for (int id = from; id <= to; id++) {
            CharSequence symbol = dict.getSymbol(id);
            int utf8Len = Utf8s.utf8Bytes(symbol);
            int wireLen = NativeBufferWriter.varintSize(utf8Len) + utf8Len; // [len][utf8]
            ensureScratch(len + wireLen + CRC_SIZE);
            long entryStart = scratchAddr + len;
            long p = NativeBufferWriter.writeVarint(entryStart, utf8Len);
            if (utf8Len > 0) {
                Utf8s.strCpyUtf8(symbol, p, utf8Len);
            }
            Unsafe.getUnsafe().putInt(entryStart + wireLen, Crc32c.update(Crc32c.INIT, entryStart, wireLen));
            len += wireLen + CRC_SIZE;
        }
        long written = ff.write(fd, scratchAddr, len, appendOffset);
        if (written != len) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + len + ", actual=" + written + ']');
        }
        appendOffset += len;
        size += to - from + 1;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (loadedEntriesAddr != 0L) {
            Unsafe.free(loadedEntriesAddr, loadedEntriesLen, MemoryTag.NATIVE_DEFAULT);
            // Null after freeing (like scratchAddr below) so a future accessor that
            // reads loadedEntriesAddr()/loadedEntriesLen() post-close cannot
            // dereference freed native memory; the getters are not closed-guarded.
            loadedEntriesAddr = 0L;
            loadedEntriesLen = 0;
        }
        if (scratchAddr != 0L) {
            Unsafe.free(scratchAddr, scratchCap, MemoryTag.NATIVE_DEFAULT);
            scratchAddr = 0L;
            scratchCap = 0;
        }
        if (fd >= 0) {
            ff.close(fd);
        }
    }

    /**
     * Base address of the loaded entry region -- the concatenated
     * {@code [len][utf8]} bytes of every recovered symbol in id order, exactly
     * as a delta section carries them. Zero when nothing was recovered.
     * <p>
     * <b>Construction-phase only.</b> This hands out a raw pointer into native
     * memory that {@link #close()} frees and nulls, with no closed-guard and no
     * synchronization. It is safe to read only BEFORE the slot's I/O thread and
     * any producer append start -- i.e. while the send loop is being constructed
     * or an orphan-drain is seeding its mirror, both of which happen-before those
     * threads. A caller that reads it from a running thread races {@code close()}
     * and can dereference freed memory (use-after-free).
     */
    public long loadedEntriesAddr() {
        return loadedEntriesAddr;
    }

    /**
     * Byte length of {@link #loadedEntriesAddr()}. Construction-phase only, for
     * the same reason -- see {@link #loadedEntriesAddr()}.
     */
    public int loadedEntriesLen() {
        return loadedEntriesLen;
    }

    /**
     * Materialises the loaded entries as symbol strings in ascending-id order
     * (entry {@code i} is symbol id {@code i}). Used once on recovery to
     * repopulate the producer's global dictionary. Empty when nothing was
     * recovered.
     * <p>
     * <b>Construction-phase only</b> -- like {@link #loadedEntriesAddr()}, this
     * walks the native entry region {@link #close()} frees, with no closed-guard,
     * so it must run before the I/O thread and any producer append start.
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
                if (shift > 35) {
                    // Bound the varint like decodeVarint / appendRawEntries /
                    // readVarintAt: a canonical length is <= 5 bytes. open() already
                    // CRC-validated these bytes, so this is defensive only; the
                    // p + len > limit check below then rejects the over-long run.
                    break;
                }
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

    private static PersistedSymbolDict openExisting(FilesFacade ff, String filePath, long fileLen) {
        int fd = ff.openRW(filePath);
        if (fd < 0) {
            LOG.warn("symbol dict {} could not be opened (rc={}); recreating", filePath, fd);
            return null;
        }
        long buf = 0L;
        long entriesAddr = 0L;
        int entriesLen = 0;
        try {
            int len = (int) fileLen;
            buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long read = ff.read(fd, buf, len, 0);
            if (read != len
                    || Unsafe.getUnsafe().getInt(buf) != FILE_MAGIC
                    || Unsafe.getUnsafe().getByte(buf + 4) != VERSION) {
                LOG.warn("symbol dict {} unreadable, bad magic or unknown version; recreating", filePath);
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                buf = 0L; // null after free so the catch below cannot double-free if ff.close throws
                int fdToClose = fd;
                fd = -1; // relinquish before close so the catch cannot double-close if close throws
                ff.close(fdToClose);
                return null;
            }
            // Parse complete, CRC-valid entries after the header; stop at the first
            // torn/incomplete OR crc-mismatched entry. The CRC turns an interior
            // tear (a lost page reading back as zeroes) or a stale entry left past
            // the end by a failed truncate into a clean stop point, so recovery
            // trusts only the intact prefix instead of silently mis-parsing a
            // corrupt entry and shifting the dense id->symbol map.
            int diskPos = HEADER_SIZE; // walks the on-disk [len][utf8][crc] entries
            int count = 0;
            int wireLen = 0;           // running size of the crc-stripped copy
            while (diskPos < len) {
                long[] vr = decodeVarint(buf, diskPos, len);
                if (vr == null) {
                    break; // torn length varint
                }
                long symLen = vr[0];
                int afterVar = (int) vr[1];
                // [len varint][utf8] then a u32 CRC. symLen stays a long so a
                // corrupt multi-gigabyte length cannot wrap an int back under the
                // bound check. No fixed per-entry ceiling -- the write path applies
                // none, so a legitimately large symbol must recover here.
                long wireEnd = (long) afterVar + symLen; // end of [len][utf8]
                if (wireEnd + CRC_SIZE > len) {
                    break; // torn/incomplete trailing entry (its CRC doesn't fit)
                }
                int wireEndI = (int) wireEnd;
                int wireSpan = wireEndI - diskPos;
                int crcStored = Unsafe.getUnsafe().getInt(buf + wireEndI);
                int crcCalc = Crc32c.update(Crc32c.INIT, buf + diskPos, wireSpan);
                if (crcCalc != crcStored) {
                    break; // corrupt/stale entry -- stop before it (fail-clean)
                }
                diskPos = wireEndI + CRC_SIZE;
                wireLen += wireSpan;
                count++;
            }
            int diskConsumed = diskPos - HEADER_SIZE; // valid entries incl. their CRCs
            // Materialise the trusted entries as WIRE bytes ([len][utf8]..., no
            // CRC) so loadedEntries*/readLoadedSymbols and the send-loop catch-up
            // mirror stay wire-shaped -- the on-disk CRC is stripped here, once, at
            // open. A second no-alloc walk over the already-validated region.
            if (wireLen > 0) {
                entriesAddr = Unsafe.malloc(wireLen, MemoryTag.NATIVE_DEFAULT);
                // Record the length alongside the malloc so the catch below frees the
                // right size (not 0) if this copy walk ever throws.
                entriesLen = wireLen;
                long dst = entriesAddr;
                int p = HEADER_SIZE;
                for (int i = 0; i < count; i++) {
                    int vp = p;
                    long symLen = 0;
                    int shift = 0;
                    while (true) {
                        byte b = Unsafe.getUnsafe().getByte(buf + vp++);
                        symLen |= (long) (b & 0x7F) << shift;
                        if ((b & 0x80) == 0) {
                            break;
                        }
                        shift += 7;
                        if (shift > 35) {
                            break; // corrupt run; these entries were CRC-validated above
                        }
                    }
                    int wireSpan = (vp - p) + (int) symLen; // [len][utf8], no CRC
                    Unsafe.getUnsafe().copyMemory(buf + p, dst, wireSpan);
                    dst += wireSpan;
                    p += wireSpan + CRC_SIZE; // skip the entry's CRC
                }
            }
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            buf = 0L;
            // Drop any torn/stale trailing bytes so a LATER, shorter append cannot
            // leave residue past its own end. Unlike before, the truncate result IS
            // checked: a file we cannot trim could still expose stale post-end bytes
            // whose (self-consistent) per-entry CRC the parse would accept at a
            // shifted position, so a failed truncate makes the file untrusted --
            // open() then recreates it empty (fail-clean) rather than risk a silent
            // misattribution.
            long validLen = HEADER_SIZE + diskConsumed;
            if (validLen < len && !ff.truncate(fd, validLen)) {
                LOG.warn("symbol dict {} could not drop its torn/stale tail (truncate failed); recreating", filePath);
                if (entriesAddr != 0L) {
                    Unsafe.free(entriesAddr, entriesLen, MemoryTag.NATIVE_DEFAULT);
                    // Null after freeing (like buf above) so the catch below cannot
                    // double-free entriesAddr if the following ff.close throws.
                    entriesAddr = 0L;
                    entriesLen = 0;
                }
                int fdToClose = fd;
                fd = -1; // relinquish before close so the catch cannot double-close if close throws
                ff.close(fdToClose);
                return null;
            }
            return new PersistedSymbolDict(ff, fd, validLen, count, entriesAddr, entriesLen);
        } catch (Throwable t) {
            if (buf != 0L) {
                Unsafe.free(buf, (int) fileLen, MemoryTag.NATIVE_DEFAULT);
            }
            // Free entriesAddr if it was allocated and not yet handed off. The success
            // path transfers it to the returned dict, and every path that frees it
            // earlier (the truncate-failure branch above) also nulls it, so this cannot
            // double-free. Keeps the error path leak-free on any throw between its
            // malloc and the return.
            if (entriesAddr != 0L) {
                Unsafe.free(entriesAddr, entriesLen, MemoryTag.NATIVE_DEFAULT);
            }
            if (fd >= 0) { // a branch that already closed fd relinquished it to -1
                ff.close(fd);
            }
            LOG.warn("symbol dict {} recovery failed ({}); recreating", filePath, String.valueOf(t));
            return null;
        }
    }

    private static PersistedSymbolDict openFresh(FilesFacade ff, String filePath) {
        int fd = ff.openCleanRW(filePath);
        if (fd < 0) {
            LOG.warn("symbol dict {} could not be created (rc={}); proceeding without it", filePath, fd);
            return null;
        }
        long hdr = 0L;
        try {
            hdr = Unsafe.malloc(HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            Unsafe.getUnsafe().putInt(hdr, FILE_MAGIC);
            Unsafe.getUnsafe().putByte(hdr + 4, VERSION);
            Unsafe.getUnsafe().putByte(hdr + 5, (byte) 0);
            Unsafe.getUnsafe().putByte(hdr + 6, (byte) 0);
            Unsafe.getUnsafe().putByte(hdr + 7, (byte) 0);
            long written = ff.write(fd, hdr, HEADER_SIZE, 0);
            if (written != HEADER_SIZE) {
                int fdToClose = fd;
                fd = -1; // relinquish before close so the catch cannot double-close if close throws
                ff.close(fdToClose);
                ff.remove(filePath);
                LOG.warn("symbol dict {} header write failed; proceeding without it", filePath);
                return null;
            }
        } catch (Throwable t) {
            // Unreachable today (Files.write is native and returns -1 rather than
            // throwing; the Unsafe puts target a valid 8-byte buffer and an 8-byte
            // malloc cannot realistically OOM), but close the fd against a future
            // edit so it cannot leak -- mirroring openExisting's error handling.
            if (fd >= 0) { // the header-write branch relinquished fd to -1 before closing
                ff.close(fd);
            }
            LOG.warn("symbol dict {} creation failed ({}); proceeding without it", filePath, String.valueOf(t));
            return null;
        } finally {
            if (hdr != 0L) {
                Unsafe.free(hdr, HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
        return new PersistedSymbolDict(ff, fd, HEADER_SIZE, 0, 0L, 0);
    }

    private void ensureScratch(int required) {
        if (scratchCap >= required) {
            return;
        }
        // Double in long: scratchCap * 2 as an int overflows negative past ~1 GB and
        // would make the realloc size negative. required is bounded by one frame's
        // entries (the server batch cap), so this never actually caps -- it mirrors the
        // long-math growth in CursorWebSocketSendLoop.ensureSentDictCapacity.
        long newCap = Math.max(required, Math.max(256L, (long) scratchCap * 2));
        if (newCap > Integer.MAX_VALUE - 8) {
            newCap = Integer.MAX_VALUE - 8;
        }
        scratchAddr = Unsafe.realloc(scratchAddr, scratchCap, (int) newCap, MemoryTag.NATIVE_DEFAULT);
        scratchCap = (int) newCap;
    }
}
