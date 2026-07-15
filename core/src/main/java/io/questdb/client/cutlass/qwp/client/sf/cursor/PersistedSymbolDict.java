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
import io.questdb.client.std.Files;
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
 * to a stronger durability contract, and {@link #open} never destroys it (see
 * "Never recreate over an existing file" below).
 * <p>
 * <b>Layout</b> (little-endian):
 * <pre>
 *   offset 0: u32 magic = 'SYD1'
 *   offset 4: u8  version = 3
 *   offset 5: 3 bytes reserved (zero)
 *   offset 8: chunks, each
 *             [entryCount: varint][entryBytes: varint][entries][crc32c: u32]
 *             where entries = [len: varint][utf8] repeated entryCount times,
 *             occupying exactly entryBytes bytes, and the CRC-32C covers the
 *             two header varints AND the entry region.
 * </pre>
 * A <b>chunk</b> is one append -- i.e. exactly the set of symbols one frame
 * introduces, since the producer persists a frame's new symbols in a single call
 * before publishing it. Symbol id {@code i} is the {@code i}-th entry across all
 * chunks (ids are dense and assigned sequentially from 0), so no id is stored.
 * <p>
 * <b>Why the checksum is per chunk, not per entry.</b> The only consumer of the
 * recovered prefix is the send loop's replay guard, which compares a surviving
 * frame's {@code deltaStart} against the recovered dictionary size -- and every
 * {@code deltaStart} is a chunk boundary, because chunks and frame deltas are
 * written one-for-one. A tear inside a chunk therefore invalidates exactly the
 * frames a per-entry checksum would have invalidated anyway: per-entry
 * granularity buys no extra recoverable prefix. It costs a great deal, though.
 * {@link Crc32c#update} is a native call, so checksumming per entry put one JNI
 * transition -- plus one sub-cache-line copy and one redundant varint decode --
 * on the producer thread for every new symbol. On the high-cardinality batch this
 * feature exists to serve (one new symbol per row), that is a thousand native
 * calls per flush where the chunk needs one.
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
 *   <li>The per-chunk CRC-32C: {@link #open} verifies every chunk and stops at
 *       the first one whose checksum fails, so an interior page lost out of
 *       order (reading back as zeroes) or a stale chunk left past the end by a
 *       failed truncate is DETECTED and the trusted region ends before it --
 *       recovery never mis-parses a corrupt chunk as real symbols nor shifts the
 *       dense id-&gt;symbol map.</li>
 *   <li>The send loop's replay guard: once recovery trusts only the intact
 *       prefix, a surviving frame whose delta start id exceeds that prefix
 *       fails loudly (the unreplayable data must be resent) rather than sending
 *       a gapped frame.</li>
 * </ul>
 * Together these turn every detectable host-crash tear into a fail-clean
 * "resend required" instead of a silent symbol misattribution -- the same
 * CRC-32C protection the segment frames carry. A tear that happened to leave a
 * byte run whose CRC still matches is not distinguished, but that is a 1-in-2^32
 * collision per corrupted chunk, no weaker than the frames' own checksum.
 * <p>
 * A torn trailing chunk from a crash mid-append is self-healing: {@link #open}
 * stops parsing at the first incomplete chunk and the next append overwrites it.
 * <p>
 * <b>Never recreate over an existing file.</b> {@link #open} -- the RECOVERY
 * entry point -- returns {@code null} when an existing file cannot be read or
 * parsed, and NEVER falls back to recreating it empty. Recreating would mean
 * {@code O_TRUNC} over the only copy of load-bearing state, so a single transient
 * read error (an EIO on a flaky disk, a short read) would permanently destroy the
 * dictionary the surviving delta frames reference -- turning a recoverable outage
 * into unrecoverable data. A {@code null} instead degrades the sender to full
 * self-sufficient frames and leaves every byte on disk, so a later attempt, once
 * the transient clears, can still recover the slot in full. Only
 * {@link #openClean} -- the FRESH-slot path, where discarding is the whole point
 * -- truncates.
 * <p>
 * <b>Lifecycle:</b> single-writer (the producer / user thread) for appends. Read
 * once at {@link #open} to seed in-memory state on recovery or orphan-drain. The
 * owner (the engine) closes it, and {@code close()} is callable from any thread
 * (a shutdown hook, test cleanup). {@code close()} and the append methods are
 * therefore {@code synchronized}: without that, a close racing an in-flight append
 * could unmap the production append region (or free the fault-test scratch buffer),
 * close the fd, and let an in-flight write corrupt memory or land on a descriptor
 * the OS has reused for another file. Not thread-safe for concurrent writers.
 */
public final class PersistedSymbolDict implements QuietCloseable {

    /**
     * Filename within the slot directory. Dot-prefixed so directory
     * enumerators that filter by the {@code .sfa} suffix (segment recovery,
     * OrphanScanner, trim) skip it automatically.
     */
    public static final String FILE_NAME = ".symbol-dict";
    static final int CRC_SIZE = 4; // u32 CRC-32C trailing every chunk
    static final int FILE_MAGIC = 0x31445953; // 'SYD1' little-endian
    static final int HEADER_SIZE = 8;
    static final int INITIAL_APPEND_MAP_CAPACITY = 64 * 1024;
    /**
     * Upper bound on a chunk's two header varints ({@code entryCount} and
     * {@code entryBytes}): each is at most 5 bytes for a 32-bit value. The
     * encoders reserve this much in front of the entry region so the header can
     * be back-filled once the region's exact size is known, keeping header,
     * entries and CRC one contiguous run.
     */
    static final int MAX_CHUNK_HEADER_SIZE = 10;
    /**
     * Ceiling for the append scratch buffer, mirroring
     * {@code CursorWebSocketSendLoop.MAX_SENT_DICT_BYTES}: the capacity math is
     * int-typed, so a larger buffer cannot be addressed. Exceeding it throws --
     * {@link #ensureScratch} never silently under-allocates.
     */
    static final int MAX_SCRATCH_BYTES = Integer.MAX_VALUE - 8;
    static final byte VERSION = 3; // v3 moved the CRC-32C from per-entry to per-chunk
    private static final Logger LOG = LoggerFactory.getLogger(PersistedSymbolDict.class);
    private final int fd;
    // Filesystem seam. Production is FilesFacade.INSTANCE (straight to Files);
    // tests inject a fault facade to exercise recovery I/O failures (a truncate
    // that cannot drop a torn tail, a short write) without a real broken disk.
    private final FilesFacade ff;
    // Production writes directly into segmented append mappings. Custom facades retain the
    // positioned-write path so fault tests can inject short writes through ff.write.
    private final boolean mappedAppend;
    private long appendMapAddr;
    private long appendMapCapacity;
    private long appendMapOffset;
    private int appendMapGrowthCount;
    private long appendOffset;
    private long appendWriteCount;
    private boolean closed;
    // In-memory copy of the WIRE entry region [len][utf8]... -- chunk headers and
    // CRCs stripped -- populated only when open() recovered existing chunks
    // (recovery / orphan-drain). Zero/empty for a freshly created file. READ (not
    // consumed) to seed the producer's id map (readLoadedSymbols) and to seed the
    // send loop's catch-up mirror, which COPIES it. This file retains ownership for
    // the engine's lifetime -- the orphan drainer builds a fresh send loop per wire
    // session against the same engine, and each must re-seed its mirror -- and frees
    // this buffer in close().
    private long loadedEntriesAddr;
    private int loadedEntriesLen;
    private long scratchAddr;
    private int scratchCap;
    private int size;

    private PersistedSymbolDict(FilesFacade ff, int fd, long appendOffset, int size, long loadedEntriesAddr, int loadedEntriesLen) {
        this.ff = ff;
        this.fd = fd;
        this.mappedAppend = ff == FilesFacade.INSTANCE;
        this.appendOffset = appendOffset;
        this.size = size;
        this.loadedEntriesAddr = loadedEntriesAddr;
        this.loadedEntriesLen = loadedEntriesLen;
    }

    /**
     * Opens the dictionary file in {@code slotDir} for RECOVERY, creating it only
     * when it does not already exist. An existing file is parsed and its complete,
     * CRC-valid chunks are loaded into memory (see {@link #loadedEntriesAddr()}).
     * <p>
     * Returns {@code null} on any I/O or parse failure -- including an existing file
     * that cannot be read, carries an unknown version, or fails its checksums. The
     * caller then falls back to full-dictionary (self-sufficient) frames for this
     * slot, so a broken side-file degrades gracefully rather than aborting the
     * sender. Crucially, a {@code null} return NEVER destroys the file: see the
     * class-level "Never recreate over an existing file" note.
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
        boolean exists = ff.exists(filePath);
        long existing = exists ? ff.length(filePath) : -1L;
        if (exists && existing < 0) {
            // The file is present but its length could not be stat'd (a transient EIO
            // on a flaky disk). Do NOT fall through to openFresh below, which O_TRUNCs:
            // truncating the only copy of load-bearing state on a TRANSIENT fault is the
            // exact destruction the class-level "Never recreate over an existing file"
            // note forbids -- and unlike the openExisting read path, this routing check
            // otherwise has no guard. A genuine sub-header stub reports a length in
            // [0, HEADER_SIZE); only a stat error reports < 0, so the two are
            // distinguishable. Degrade to full self-sufficient frames and leave every
            // byte on disk for a later attempt, once the transient clears.
            LOG.warn("symbol dict {} exists but its length could not be read; "
                    + "falling back to full-dictionary frames (file left intact)", filePath);
            return null;
        }
        if (existing >= HEADER_SIZE) {
            // A dictionary at or past Integer.MAX_VALUE cannot be read back:
            // openExisting reads it into ONE int-sized native buffer, and the (int)
            // cast is either negative (malloc rejects it) or a small positive prefix
            // (whose parse would then trim the real multi-GB file). Degrade to full
            // self-sufficient frames and leave the file alone. Reaching this needs
            // ~100M+ distinct symbols on one slot, far past realistic cardinality;
            // the guard keeps the read/write size boundary safe anyway.
            if (existing >= Integer.MAX_VALUE) {
                LOG.warn("symbol dict {} too large ({} bytes) to reopen; "
                        + "falling back to full-dictionary frames (file left intact)", filePath, existing);
                return null;
            }
            // NEVER recreate over an existing file on the recovery path: openFresh
            // truncates, and these bytes are the only copy of state the surviving
            // delta frames reference. A null degrades this slot to full
            // self-sufficient frames and preserves the file for a later attempt.
            return openExisting(ff, filePath, existing);
        }
        // Absent, or a sub-header stub left by a crash inside openFresh: no
        // load-bearing content to lose, so create it.
        return openFresh(ff, filePath);
    }

    /**
     * Opens the dictionary in {@code slotDir} as a FRESH, EMPTY file, discarding
     * any surviving content. This is the fresh-start counterpart to {@link #open}:
     * a slot with no recovered segments must start with an empty dictionary, so a
     * dictionary left by a prior lifecycle -- a fully-drained slot whose
     * best-effort delete failed, or a crash in the close window -- must NOT be
     * inherited. Unlike {@link #open}, which parses and TRUSTS an existing file for
     * recovery/orphan-drain replay and never destroys it, this truncates: the
     * fresh-start producer is not seeded from the dictionary, so trusting a survivor
     * would leave the producer's ids diverged from the dictionary the send loop
     * replays and silently misattribute symbols on the next reconnect. Truncating
     * (rather than relying on an unlink succeeding first) closes the gap even when
     * the delete is refused -- e.g. a Windows share lock. Returns {@code null} on
     * I/O failure, so the caller falls back to full self-sufficient frames exactly
     * as {@link #open} does.
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
     * symbol-dict delta section the frame encoder just wrote -- as ONE chunk.
     * <p>
     * The consistency walk below decodes each entry's length varint, but the bytes
     * themselves are copied in a SINGLE {@code copyMemory} and checksummed by a
     * SINGLE {@link Crc32c#update} covering the whole chunk. A per-entry checksum
     * would put one JNI transition, one sub-cache-line copy and one redundant varint
     * decode on the producer thread per new symbol; the chunk needs one of each.
     * <p>
     * Advances {@code size} by {@code count}. Same durability/idempotency contract
     * as {@link #appendSymbols}: no fsync, and a short write throws WITHOUT
     * advancing {@code size}/{@code appendOffset}, so a retry keyed off
     * {@link #size()} re-persists the same range at the same offset. No-op when the
     * range is empty or the dictionary is closed.
     */
    public synchronized void appendRawEntries(long addr, int len, int count) {
        if (closed || count <= 0 || len <= 0) {
            return;
        }
        // Validate the (addr, len, count) triple BEFORE writing anything: an
        // inconsistent triple would record a chunk whose stored entryCount disagreed
        // with the entries it holds, shifting the dense id->symbol map on recovery.
        // The sole caller derives count and len from one beginMessage, so this cannot
        // fire today -- but the file this guards is the one the "resend required"
        // contract rests on, so keep the structural guard. Gated behind assert: it
        // re-walks every entry's length prefix on the common flush path, and the client
        // library runs without -ea in production (embedded in user apps), so this holds
        // the guard in the client's own -ea test suite without the per-flush cost in
        // production.
        assert validateRawEntries(addr, len, count);
        int hdrLen = NativeBufferWriter.varintSize(count) + NativeBufferWriter.varintSize(len);
        if (mappedAppend) {
            long recLen = (long) hdrLen + len + CRC_SIZE;
            ensureAppendMap(checkedRequiredOffset(recLen));
            long recStart = appendMapAddr + appendOffset - appendMapOffset;
            long p = NativeBufferWriter.writeVarint(recStart, count);
            NativeBufferWriter.writeVarint(p, len);
            Unsafe.getUnsafe().copyMemory(addr, recStart + hdrLen, len);
            commitMappedChunk(recStart, hdrLen, len, count);
            return;
        }
        ensureScratch((long) hdrLen + len + CRC_SIZE);
        long p = NativeBufferWriter.writeVarint(scratchAddr, count);
        NativeBufferWriter.writeVarint(p, len);
        Unsafe.getUnsafe().copyMemory(addr, scratchAddr + hdrLen, len);
        flushChunk(scratchAddr, hdrLen, len, count);
    }

    /**
     * Appends one symbol as a single-entry chunk, extending the on-disk dictionary.
     * The caller appends a frame's new symbols BEFORE publishing that frame, so the
     * write ordering (dictionary entry before referencing frame) holds; no fsync is
     * performed (see the class-level durability note). Assigns the next dense id
     * implicitly (the entry's position).
     * <p>
     * Test-only: production persists a frame's whole new-symbol range in one chunk
     * via {@link #appendSymbols} / {@link #appendRawEntries}. Tests use this to
     * build a dictionary one entry at a time.
     */
    @TestOnly
    public synchronized void appendSymbol(CharSequence symbol) {
        if (closed) {
            return;
        }
        int utf8Len = Utf8s.utf8Bytes(symbol);
        int wireLen = NativeBufferWriter.varintSize(utf8Len) + utf8Len; // [len][utf8]
        if (mappedAppend) {
            int hdrLen = NativeBufferWriter.varintSize(1) + NativeBufferWriter.varintSize(wireLen);
            long recLen = (long) hdrLen + wireLen + CRC_SIZE;
            ensureAppendMap(checkedRequiredOffset(recLen));
            long recStart = appendMapAddr + appendOffset - appendMapOffset;
            long p = NativeBufferWriter.writeVarint(recStart, 1);
            p = NativeBufferWriter.writeVarint(p, wireLen);
            p = NativeBufferWriter.writeVarint(p, utf8Len);
            if (utf8Len > 0) {
                Utf8s.strCpyUtf8(symbol, p, utf8Len);
            }
            commitMappedChunk(recStart, hdrLen, wireLen, 1);
            return;
        }
        ensureScratch((long) MAX_CHUNK_HEADER_SIZE + wireLen + CRC_SIZE);
        long entryStart = scratchAddr + MAX_CHUNK_HEADER_SIZE;
        long p = NativeBufferWriter.writeVarint(entryStart, utf8Len);
        if (utf8Len > 0) {
            Utf8s.strCpyUtf8(symbol, p, utf8Len);
        }
        writeChunkFromScratch(wireLen, 1);
    }

    /**
     * Appends the dense id range {@code [from .. to]} as one chunk with one
     * checksum. Production encodes directly into a segmented append mapping, avoiding
     * both the staging copy and a positioned-write syscall on every flush. The
     * scratch/positioned-write fallback exists only behind an injected filesystem
     * facade so short-write recovery tests retain their fault seam. Callers pass the
     * dictionary and the range so the ids resolve to their symbol strings.
     * <p>
     * Same durability and idempotency contract as {@link #appendSymbol}: no fsync,
     * and a short write throws WITHOUT advancing {@code size}/{@code appendOffset},
     * so a retry keyed off {@link #size()} re-encodes the same range and overwrites
     * at the same offset. No-op when the range is empty or the dictionary is closed.
     */
    public synchronized void appendSymbols(GlobalSymbolDictionary dict, int from, int to) {
        if (closed || to < from) {
            return;
        }
        int count = to - from + 1;
        if (mappedAppend) {
            long entriesLenLong = 0L;
            for (int id = from; id <= to; id++) {
                int utf8Len = Utf8s.utf8Bytes(dict.getSymbol(id));
                entriesLenLong += NativeBufferWriter.varintSize(utf8Len) + (long) utf8Len;
                if (entriesLenLong > MAX_SCRATCH_BYTES) {
                    throw new IllegalStateException("symbol dict chunk exceeds the maximum size to "
                            + FILE_NAME + " [required=" + entriesLenLong + ", max="
                            + MAX_SCRATCH_BYTES + ']');
                }
            }
            int entriesLen = (int) entriesLenLong;
            int hdrLen = NativeBufferWriter.varintSize(count)
                    + NativeBufferWriter.varintSize(entriesLen);
            long recLen = (long) hdrLen + entriesLen + CRC_SIZE;
            ensureAppendMap(checkedRequiredOffset(recLen));
            long recStart = appendMapAddr + appendOffset - appendMapOffset;
            long p = NativeBufferWriter.writeVarint(recStart, count);
            p = NativeBufferWriter.writeVarint(p, entriesLen);
            for (int id = from; id <= to; id++) {
                CharSequence symbol = dict.getSymbol(id);
                int utf8Len = Utf8s.utf8Bytes(symbol);
                p = NativeBufferWriter.writeVarint(p, utf8Len);
                if (utf8Len > 0) {
                    Utf8s.strCpyUtf8(symbol, p, utf8Len);
                    p += utf8Len;
                }
            }
            commitMappedChunk(recStart, hdrLen, entriesLen, count);
            return;
        }
        int entriesLen = 0;
        for (int id = from; id <= to; id++) {
            CharSequence symbol = dict.getSymbol(id);
            int utf8Len = Utf8s.utf8Bytes(symbol);
            int wireLen = NativeBufferWriter.varintSize(utf8Len) + utf8Len; // [len][utf8]
            ensureScratch((long) MAX_CHUNK_HEADER_SIZE + entriesLen + wireLen + CRC_SIZE);
            long entryStart = scratchAddr + MAX_CHUNK_HEADER_SIZE + entriesLen;
            long p = NativeBufferWriter.writeVarint(entryStart, utf8Len);
            if (utf8Len > 0) {
                Utf8s.strCpyUtf8(symbol, p, utf8Len);
            }
            entriesLen += wireLen;
        }
        writeChunkFromScratch(entriesLen, count);
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
        if (appendMapAddr != 0L) {
            Files.munmap(appendMapAddr, appendMapCapacity, MemoryTag.MMAP_DEFAULT);
            appendMapAddr = 0L;
            appendMapCapacity = 0L;
            appendMapOffset = 0L;
            // The active window reserves space past the logical end. Return that tail on
            // orderly close; after a crash open() treats the zero-filled reserve as
            // a torn trailing chunk and truncates it to the same appendOffset.
            if (!ff.truncate(fd, appendOffset)) {
                LOG.warn("symbol dict {} could not trim mmap reserve to {}; recovery will "
                                + "discard the zero-filled tail on the next open",
                        FILE_NAME, appendOffset);
            }
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

    @TestOnly
    public synchronized int appendMapGrowthCount() {
        return appendMapGrowthCount;
    }

    @TestOnly
    public synchronized long appendWriteCount() {
        return appendWriteCount;
    }

    /**
     * Base address of the loaded entry region -- the concatenated
     * {@code [len][utf8]} bytes of every recovered symbol in id order, exactly as a
     * delta section carries them (chunk headers and CRCs stripped). Zero when
     * nothing was recovered.
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
                    // Bound the varint like the other decoders: a canonical length is
                    // <= 5 bytes. open() already CRC-validated these bytes, so this is
                    // defensive only; the p + len > limit check below rejects the
                    // over-long run.
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

    private static PersistedSymbolDict openExisting(FilesFacade ff, String filePath, long fileLen) {
        int fd = ff.openRW(filePath);
        if (fd < 0) {
            LOG.warn("symbol dict {} could not be opened (rc={}); "
                    + "falling back to full-dictionary frames (file left intact)", filePath, fd);
            return null;
        }
        long buf = 0L;
        long entriesAddr = 0L;
        int entriesCap = 0;
        try {
            int len = (int) fileLen; // open() bounds fileLen to [HEADER_SIZE, Integer.MAX_VALUE)
            buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            long read = ff.read(fd, buf, len, 0);
            if (read != len
                    || Unsafe.getUnsafe().getInt(buf) != FILE_MAGIC
                    || Unsafe.getUnsafe().getByte(buf + 4) != VERSION) {
                LOG.warn("symbol dict {} unreadable, bad magic or unknown version; "
                        + "falling back to full-dictionary frames (file left intact)", filePath);
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                buf = 0L; // null after free so the catch below cannot double-free if ff.close throws
                int fdToClose = fd;
                fd = -1; // relinquish before close so the catch cannot double-close if close throws
                ff.close(fdToClose);
                return null;
            }
            // Parse the chunks after the header, copying each chunk's entry region
            // into entriesAddr AS WE VALIDATE IT -- one pass over the file, not two.
            // Every chunk sheds its two header varints and its CRC, so the entry
            // region is strictly smaller than the file and len - HEADER_SIZE is a safe
            // upper bound to allocate up front; we shrink to the exact size below.
            //
            // Stop at the first torn/incomplete OR crc-mismatched chunk. The CRC turns
            // an interior tear (a lost page reading back as zeroes) or a stale chunk
            // left past the end by a failed truncate into a clean stop point, so
            // recovery trusts only the intact prefix instead of silently mis-parsing a
            // corrupt chunk and shifting the dense id->symbol map.
            entriesCap = len - HEADER_SIZE;
            long dst = 0L;
            if (entriesCap > 0) {
                entriesAddr = Unsafe.malloc(entriesCap, MemoryTag.NATIVE_DEFAULT);
                dst = entriesAddr;
            }
            Varint v = new Varint();
            int diskPos = HEADER_SIZE;
            int count = 0;
            while (diskPos < len) {
                if (!v.decode(buf, diskPos, len)) {
                    break; // torn entryCount varint
                }
                long entryCount = v.value;
                if (!v.decode(buf, v.end, len)) {
                    break; // torn entryBytes varint
                }
                long entryBytes = v.value;
                int entriesStart = v.end;
                // entryCount/entryBytes stay long so a corrupt multi-gigabyte value
                // cannot wrap an int back under the bound checks.
                long chunkEnd = (long) entriesStart + entryBytes; // end of the entry region
                if (chunkEnd + CRC_SIZE > len) {
                    break; // torn/incomplete trailing chunk (its CRC doesn't fit)
                }
                int chunkEndI = (int) chunkEnd;
                int crcStored = Unsafe.getUnsafe().getInt(buf + chunkEndI);
                int crcCalc = Crc32c.update(Crc32c.INIT, buf + diskPos, chunkEndI - diskPos);
                if (crcCalc != crcStored) {
                    break; // corrupt/stale chunk -- stop before it (fail-clean)
                }
                // A chunk carries at least one entry, and the ids are int-dense. A
                // CRC-valid chunk cannot violate either at realistic cardinality, but
                // keep the int narrowing honest rather than wrapping the id space.
                if (entryCount <= 0 || (long) count + entryCount > Integer.MAX_VALUE) {
                    break;
                }
                Unsafe.getUnsafe().copyMemory(buf + entriesStart, dst, entryBytes);
                dst += entryBytes;
                diskPos = chunkEndI + CRC_SIZE;
                count += (int) entryCount;
            }
            int entriesLen = entriesAddr != 0L ? (int) (dst - entriesAddr) : 0;
            int diskConsumed = diskPos - HEADER_SIZE; // valid chunks incl. headers and CRCs
            if (entriesAddr != 0L && entriesLen == 0) {
                Unsafe.free(entriesAddr, entriesCap, MemoryTag.NATIVE_DEFAULT);
                entriesAddr = 0L;
                entriesCap = 0;
            } else if (entriesAddr != 0L && entriesLen < entriesCap) {
                // Shrink the upper-bound allocation to what the trusted prefix used.
                entriesAddr = Unsafe.realloc(entriesAddr, entriesCap, entriesLen, MemoryTag.NATIVE_DEFAULT);
                entriesCap = entriesLen;
            }
            Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
            buf = 0L;
            // Drop any torn/stale trailing bytes so a LATER, shorter append cannot
            // leave residue past its own end. The truncate result IS checked: a file
            // we cannot trim could still expose stale post-end bytes whose
            // (self-consistent) chunk CRC the parse would accept at a shifted
            // position, so a failed truncate makes the file untrusted -- return null
            // (the sender falls back to full self-sufficient frames) and, per the
            // never-destroy contract, leave every byte on disk.
            long validLen = HEADER_SIZE + diskConsumed;
            if (validLen < len && !ff.truncate(fd, validLen)) {
                LOG.warn("symbol dict {} could not drop its torn/stale tail (truncate failed); "
                        + "falling back to full-dictionary frames (file left intact)", filePath);
                if (entriesAddr != 0L) {
                    Unsafe.free(entriesAddr, entriesCap, MemoryTag.NATIVE_DEFAULT);
                    // Null after freeing (like buf above) so the catch below cannot
                    // double-free entriesAddr if the following ff.close throws.
                    entriesAddr = 0L;
                    entriesCap = 0;
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
                Unsafe.free(entriesAddr, entriesCap, MemoryTag.NATIVE_DEFAULT);
            }
            if (fd >= 0) { // a branch that already closed fd relinquished it to -1
                ff.close(fd);
            }
            // Pass the throwable as a trailing argument with no matching placeholder so
            // slf4j prints the stack trace: this WARN is the only forensic record of why
            // a load-bearing dictionary was abandoned.
            LOG.warn("symbol dict {} recovery failed; falling back to full-dictionary frames "
                    + "(file left intact)", filePath, t);
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
                ff.remove(filePath); // drop the headerless stub rather than litter
                LOG.warn("symbol dict {} header write failed; proceeding without it", filePath);
                return null;
            }
        } catch (Throwable t) {
            // Unreachable with FilesFacade.INSTANCE (Files.write is native and returns
            // -1 rather than throwing; the Unsafe puts target a valid 8-byte buffer and
            // an 8-byte malloc cannot realistically OOM), but the ff seam exists so
            // tests CAN inject a throwing facade -- close the fd and drop the stub so
            // neither leaks.
            if (fd >= 0) { // the header-write branch relinquished fd to -1 before closing
                int fdToClose = fd;
                fd = -1;
                ff.close(fdToClose);
                ff.remove(filePath);
            }
            LOG.warn("symbol dict {} creation failed; proceeding without it", filePath, t);
            return null;
        } finally {
            if (hdr != 0L) {
                Unsafe.free(hdr, HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }
        }
        return new PersistedSymbolDict(ff, fd, HEADER_SIZE, 0, 0L, 0);
    }

    /**
     * Verifies that {@code count} wire entries ({@code [len varint][utf8]}) occupy
     * exactly {@code len} bytes from {@code addr}. Returns {@code true} when the triple
     * is consistent; throws {@link IllegalStateException} (naming the offending entry /
     * count / consumed bytes) otherwise. Called only from an {@code assert} in
     * {@link #appendRawEntries}: it guards an internal invariant the sole caller cannot
     * violate today, so it runs under the test suite's {@code -ea} but is elided from
     * the per-flush production path (client apps run without {@code -ea}).
     */
    private static boolean validateRawEntries(long addr, int len, int count) {
        long src = addr;
        long srcLimit = addr + len;
        for (int i = 0; i < count; i++) {
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
                    // continuation run is corrupt. The bound check below rejects it.
                    break;
                }
            }
            src += symLen; // src was just past the len varint
            if (src > srcLimit) {
                throw new IllegalStateException("malformed raw symbol-dict entries to " + FILE_NAME
                        + " [entry=" + i + ", count=" + count + ']');
            }
        }
        if (src != srcLimit) {
            throw new IllegalStateException("raw symbol-dict entries under-filled the buffer to "
                    + FILE_NAME + " [count=" + count + ", len=" + len
                    + ", consumed=" + (int) (src - addr) + ']');
        }
        return true;
    }

    private long checkedRequiredOffset(long recordLen) {
        long required = appendOffset + recordLen;
        if (recordLen < 0L || required < appendOffset) {
            throw new IllegalStateException("symbol dict append offset overflow to "
                    + FILE_NAME + " [offset=" + appendOffset + ", recordLen=" + recordLen + ']');
        }
        return required;
    }

    /**
     * Commits a chunk already assembled directly in the append mmap. The logical
     * offset and symbol count advance last, after the CRC and a store fence, so an
     * interrupted process leaves either a complete chunk or a tail that open()
     * rejects and truncates.
     */
    private void commitMappedChunk(long recStart, int hdrLen, int entriesLen, int count) {
        long bodyLen = (long) hdrLen + entriesLen;
        long recLen = bodyLen + CRC_SIZE;
        Unsafe.getUnsafe().putInt(
                recStart + bodyLen,
                Crc32c.update(Crc32c.INIT, recStart, bodyLen));
        Unsafe.getUnsafe().storeFence();
        appendOffset += recLen;
        size += count;
    }

    /**
     * Ensures the production append mmap covers the absolute file offset
     * {@code required}. The log is mapped in 64 KiB-aligned windows: small flushes
     * share a window, while a large existing dictionary does not force the process
     * to reserve and remap its whole prefix (or geometrically over-allocate it).
     */
    private void ensureAppendMap(long required) {
        if (appendMapAddr != 0L
                && required >= appendMapOffset
                && required <= appendMapOffset + appendMapCapacity) {
            return;
        }
        long newOffset = appendOffset - appendOffset % INITIAL_APPEND_MAP_CAPACITY;
        long needed = required - newOffset;
        long newCapacity = Math.max(INITIAL_APPEND_MAP_CAPACITY, needed);
        long remainder = newCapacity % INITIAL_APPEND_MAP_CAPACITY;
        if (remainder != 0L) {
            long padding = INITIAL_APPEND_MAP_CAPACITY - remainder;
            if (newCapacity > Long.MAX_VALUE - padding) {
                throw new IllegalStateException("symbol dict mmap capacity overflow to "
                        + FILE_NAME + " [required=" + required + ']');
            }
            newCapacity += padding;
        }
        if (newOffset > Long.MAX_VALUE - newCapacity) {
            throw new IllegalStateException("symbol dict mmap file offset overflow to "
                    + FILE_NAME + " [offset=" + newOffset + ", capacity=" + newCapacity + ']');
        }
        long newFileSize = newOffset + newCapacity;
        if (!ff.allocate(fd, newFileSize)) {
            throw new IllegalStateException("could not grow mmap append region for "
                    + FILE_NAME + " [required=" + required + ", fileSize=" + newFileSize + ']');
        }
        if (appendMapAddr != 0L) {
            long oldAddr = appendMapAddr;
            long oldCapacity = appendMapCapacity;
            appendMapAddr = 0L;
            appendMapCapacity = 0L;
            appendMapOffset = 0L;
            Files.munmap(oldAddr, oldCapacity, MemoryTag.MMAP_DEFAULT);
        }
        long newAddr = Files.mmap(
                fd, newCapacity, newOffset, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
        if (newAddr == Files.FAILED_MMAP_ADDRESS) {
            throw new IllegalStateException("could not mmap append region for "
                    + FILE_NAME + " [offset=" + newOffset + ", capacity=" + newCapacity + ']');
        }
        appendMapAddr = newAddr;
        appendMapCapacity = newCapacity;
        appendMapOffset = newOffset;
        appendMapGrowthCount++;
    }

    /**
     * Grows the append scratch buffer to hold at least {@code required} bytes.
     * <p>
     * Throws when {@code required} exceeds {@link #MAX_SCRATCH_BYTES} rather than
     * clamping to it: a clamp would hand back a buffer SMALLER than the caller asked
     * for and return normally, and every caller then writes {@code required} bytes
     * into it -- turning a clean out-of-memory into a silent native-heap overflow, on
     * the very write path the dictionary's integrity rests on. This is the same
     * loud-failure shape {@code CursorWebSocketSendLoop.ensureSentDictCapacity} uses
     * on the same bound. Unreachable at any realistic cardinality (it needs a ~2 GiB
     * dictionary section in a single frame, itself bounded by the server's batch
     * cap), but a size guard on a data-integrity write path must never
     * under-allocate.
     */
    private void ensureScratch(long required) {
        if (scratchCap >= required) {
            return;
        }
        if (required > MAX_SCRATCH_BYTES) {
            throw new IllegalStateException("symbol dict scratch buffer exceeds the maximum size to "
                    + FILE_NAME + " [required=" + required + ", max=" + MAX_SCRATCH_BYTES + ']');
        }
        // Double in long: scratchCap * 2 as an int overflows negative past ~1 GB and
        // would make the realloc size negative.
        long newCap = Math.max(required, Math.max(256L, (long) scratchCap * 2));
        if (newCap > MAX_SCRATCH_BYTES) {
            newCap = MAX_SCRATCH_BYTES;
        }
        scratchAddr = Unsafe.realloc(scratchAddr, scratchCap, (int) newCap, MemoryTag.NATIVE_DEFAULT);
        scratchCap = (int) newCap;
    }

    /**
     * Checksums {@code [recStart, recStart + hdrLen + entriesLen)} in ONE native
     * call, appends the CRC, and issues ONE positioned write. Advances
     * {@code size}/{@code appendOffset} only on a complete write, so a short write
     * throws and a retry keyed off {@link #size()} re-persists the same range at the
     * same offset.
     */
    private void flushChunk(long recStart, int hdrLen, int entriesLen, int count) {
        int bodyLen = hdrLen + entriesLen;
        int recLen = bodyLen + CRC_SIZE;
        Unsafe.getUnsafe().putInt(recStart + bodyLen, Crc32c.update(Crc32c.INIT, recStart, bodyLen));
        appendWriteCount++;
        long written = ff.write(fd, recStart, recLen, appendOffset);
        if (written != recLen) {
            throw new IllegalStateException("short write to " + FILE_NAME
                    + " [expected=" + recLen + ", actual=" + written + ']');
        }
        appendOffset += recLen;
        size += count;
    }

    /**
     * Writes one chunk whose entry region is ALREADY encoded in scratch at offset
     * {@link #MAX_CHUNK_HEADER_SIZE}. Back-fills the header immediately in front of
     * the entries -- the header is at most {@code MAX_CHUNK_HEADER_SIZE} bytes, so it
     * always fits the reserve -- leaving header, entries and CRC one contiguous run
     * for a single checksum and a single write.
     */
    private void writeChunkFromScratch(int entriesLen, int count) {
        int hdrLen = NativeBufferWriter.varintSize(count) + NativeBufferWriter.varintSize(entriesLen);
        long recStart = scratchAddr + MAX_CHUNK_HEADER_SIZE - hdrLen;
        long p = NativeBufferWriter.writeVarint(recStart, count);
        NativeBufferWriter.writeVarint(p, entriesLen);
        flushChunk(recStart, hdrLen, entriesLen, count);
    }

    /**
     * Zero-allocation LEB128 decoder, one instance per {@link #openExisting} call --
     * not one per chunk. The previous {@code long[]}-returning decoder allocated once
     * per ENTRY, so a million-symbol recovery churned a million throwaway arrays in a
     * class that is otherwise strictly allocation-free.
     */
    private static final class Varint {
        int end;
        long value;

        /**
         * Decodes the varint at {@code buf[pos..limit)}. Returns false -- leaving
         * {@code value}/{@code end} undefined -- when the varint is truncated (a torn
         * tail) or runs longer than a canonical 5-byte length.
         */
        boolean decode(long buf, int pos, int limit) {
            long v = 0;
            int shift = 0;
            int cur = pos;
            while (cur < limit) {
                byte b = Unsafe.getUnsafe().getByte(buf + cur);
                cur++;
                v |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    value = v;
                    end = cur;
                    return true;
                }
                shift += 7;
                if (shift > 35) {
                    return false; // implausible for a chunk header; treat as torn
                }
            }
            return false;
        }
    }
}
