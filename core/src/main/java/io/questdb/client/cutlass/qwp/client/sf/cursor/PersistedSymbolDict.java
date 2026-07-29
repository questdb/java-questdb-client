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
 *   offset 4: u8  version = 1
 *   offset 5: 3 bytes reserved (zero)
 *   offset 8: u64 lineageId
 *   offset 16: chunks, each
 *             [entryCount: varint][entryBytes: varint][entries][crc32c: u32]
 *             where entries = [len: varint][utf8] repeated entryCount times,
 *             occupying exactly entryBytes bytes, and the CRC-32C covers the
 *             two header varints AND the entry region.
 * </pre>
 * <b>{@code lineageId}</b> ties this dictionary to the producer lineage that
 * wrote it -- the same value {@link MmapSegment} stamps into every segment of
 * that lineage. The only check a recovered dictionary previously got was a
 * size heuristic: nothing compared its persisted strings against the strings
 * the surviving frames actually carry at overlapping ids, so a dictionary left
 * by an earlier generation could register its symbols under ids a later
 * generation's rows reference, with no gap, a valid CRC and both bounds checks
 * satisfied. {@link #open} now refuses (returns {@code null}) a dictionary
 * whose generation disagrees with the caller's expectation -- falling back to
 * full self-sufficient frames, which is always safe -- instead of trusting a
 * survivor that merely happens to be the right size.
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
    static final int HEADER_SIZE = 16;
    // One bounded, segment-sized append window avoids the allocate/unmap/mmap
    // cycle every 64 KiB without geometrically reserving up to 2x a large
    // dictionary. close() truncates the unused tail back to appendOffset.
    static final int APPEND_MAP_CAPACITY = 4 * 1024 * 1024;
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
    static final byte VERSION = 1;
    private static final Logger LOG = LoggerFactory.getLogger(PersistedSymbolDict.class);
    private final int fd;
    // Filesystem seam. Production is FilesFacade.INSTANCE (straight to Files);
    // tests inject a fault facade to exercise recovery I/O failures (a truncate
    // that cannot drop a torn tail, a short write) without a real broken disk.
    private final FilesFacade ff;
    // Slot-qualified path, retained purely so diagnostics can name WHICH dictionary they
    // are about: one JVM routinely holds many (a foreground sender plus N orphan
    // drainers), so a warning carrying only the bare filename is unattributable exactly
    // when it fires.
    private final String filePath;
    // Production writes directly into segmented append mappings. Wrapping facades retain the
    // positioned-write path by default so fault tests can inject short writes through ff.write;
    // mmap-specific fault facades opt in through FilesFacade.isMmapAllowed().
    private final boolean mappedAppend;
    // True only when recovery parsed the file through a temporary read-only
    // mmap instead of allocating a second native buffer as large as the file.
    // Test-visible so the peak-memory regression has an observable contract.
    private final boolean mappedRecoveryInput;
    // Entry count that corresponds EXACTLY to loadedEntriesAddr/loadedEntriesLen,
    // fixed at open. Distinct from the live `size`, which appends advance -- including
    // the recovery-time heal in QwpWebSocketSender.healPersistedDictionary, which runs
    // BEFORE the send loop is constructed. The loop seeds its mirror from the loaded
    // BYTES, so it must take its count from here; pairing those bytes with the live
    // size would let sentDictCount claim symbols the mirror does not hold.
    private final int recoveredSize;
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
    // consumed) to seed the producer's id map and to seed the send loop's catch-up
    // mirror. Foreground construction transfers ownership to its sole loop after
    // producer seeding; orphan-drainer loops borrow it because one engine may create
    // several wire sessions. If ownership was not transferred, close() frees it.
    private long loadedEntriesAddr;
    private int loadedEntriesLen;
    private long scratchAddr;
    private int scratchCap;
    private int size;

    private PersistedSymbolDict(
            FilesFacade ff,
            String filePath,
            int fd,
            long appendOffset,
            int size,
            long loadedEntriesAddr,
            int loadedEntriesLen,
            boolean mappedRecoveryInput
    ) {
        this.ff = ff;
        this.filePath = filePath;
        this.fd = fd;
        this.mappedAppend = ff.isMmapAllowed();
        this.mappedRecoveryInput = mappedRecoveryInput;
        this.appendOffset = appendOffset;
        this.size = size;
        this.recoveredSize = size;
        this.loadedEntriesAddr = loadedEntriesAddr;
        this.loadedEntriesLen = loadedEntriesLen;
    }

    /**
     * Opens the dictionary file in {@code slotDir} for RECOVERY, creating it only
     * when it does not already exist. An existing file is parsed and its complete,
     * CRC-valid chunks are loaded into memory (see {@link #loadedEntriesAddr()}).
     * <p>
     * Returns {@code null} on any I/O or parse failure -- including an existing file
     * that cannot be read, carries an unknown version or a generation that disagrees
     * with {@code expectedLineageId}, or fails its checksums. The caller then falls
     * back to full-dictionary (self-sufficient) frames for this slot, so a broken
     * side-file degrades gracefully rather than aborting the sender. Crucially, a
     * {@code null} return NEVER destroys the file: see the class-level "Never
     * recreate over an existing file" note.
     *
     * @param expectedLineageId the producer lineage the caller expects this dictionary
     *                           to belong to -- the same value {@link MmapSegment} carries
     *                           in the segments this slot recovered. A recovered dictionary
     *                           whose stamped generation disagrees is a survivor from a
     *                           DIFFERENT lineage: trusting it would register its symbols
     *                           under ids this generation's rows reference, so it is
     *                           discarded (degrading to full self-sufficient frames) rather
     *                           than trusted on a size heuristic alone.
     */
    public static PersistedSymbolDict open(String slotDir, long expectedLineageId) {
        return open(FilesFacade.INSTANCE, slotDir, expectedLineageId);
    }

    /**
     * Facade-aware variant of {@link #open(String, long)}. Production passes
     * {@link FilesFacade#INSTANCE}; tests inject a fault facade to drive recovery
     * I/O failures (e.g. a truncate that cannot drop a torn tail).
     */
    public static PersistedSymbolDict open(FilesFacade ff, String slotDir, long expectedLineageId) {
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
            // Chunk lengths and the retained contiguous entry region are int-sized,
            // so a dictionary at or past Integer.MAX_VALUE cannot be represented
            // safely even though production recovery maps rather than reads it.
            if (existing >= Integer.MAX_VALUE) {
                LOG.warn("symbol dict {} too large ({} bytes) to reopen; "
                        + "falling back to full-dictionary frames (file left intact)", filePath, existing);
                return null;
            }
            // NEVER recreate over an existing file on the recovery path: openFresh
            // truncates, and these bytes are the only copy of state the surviving
            // delta frames reference. A null degrades this slot to full
            // self-sufficient frames and preserves the file for a later attempt.
            //
            // The holder catches what openExisting's own catch structurally cannot.
            // Recovery reads the file through a mapping (Unsafe loads in
            // scanAndCopyRecoveredChunks), and pre-JDK-21 HotSpot delivers an
            // unsafe-access fault ASYNCHRONOUSLY -- at the next return or safepoint,
            // which can be openExisting's own return, in THIS frame. The instance is
            // fully built by then and owns an fd plus a buffer as large as the file,
            // but the assignment never happens, so nothing else can release them.
            // MmapSegment.openExisting takes an inFlight[] for exactly this shape; the
            // dictionary is at least as exposed, because ensureAppendMap grows the file
            // with ff.allocate and the reserve truncate is skipped after a crash, so a
            // sparse tail is routine. Without this, the fault also escapes open()
            // entirely -- past CursorSendEngine's constructor and out of Sender.build()
            // -- turning the documented "degrade to full-dictionary frames" into a
            // sender that cannot be constructed at all.
            PersistedSymbolDict[] inFlight = new PersistedSymbolDict[1];
            try {
                return openExisting(ff, filePath, existing, expectedLineageId, inFlight);
            } catch (Throwable t) {
                if (inFlight[0] != null) {
                    inFlight[0].close();
                }
                LOG.warn("symbol dict {} recovery faulted late; falling back to "
                        + "full-dictionary frames (file left intact)", filePath, t);
                return null;
            }
        }
        // Absent, or a sub-header stub left by a crash inside openFresh: no
        // load-bearing content to lose, so create it. mustTruncate=false: this
        // is the RECOVERY entry point, and there is nothing here to protect --
        // an absent/stub file has no id space to preserve, so a create failure
        // still just degrades to null (full self-sufficient frames), same as
        // every other recovery I/O failure above.
        return openFresh(ff, filePath, false, expectedLineageId);
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
     * the delete is refused -- e.g. a Windows share lock. Returns {@code null} when
     * the slot has no dictionary at all and a fresh one cannot be created either,
     * so the caller falls back to full self-sufficient frames exactly as
     * {@link #open} does. But when a file DOES survive and cannot be truncated,
     * returning null instead of refusing would proceed anyway: this session would
     * run full-dict from id 0 while the survivor -- describing a prior
     * generation's id space -- stays on disk, and the next recovery would trust
     * it and misattribute symbols with no detectable gap. So that case throws
     * {@link UnreplayableSlotException} instead of degrading -- see {@link #openFresh}.
     *
     * @param lineageId the producer lineage this freshly-started slot belongs to --
     *                   the same value the caller stamps into its fresh initial
     *                   {@link MmapSegment}, so the dictionary and the frames that
     *                   will reference its ids agree from the outset.
     */
    public static PersistedSymbolDict openClean(String slotDir, long lineageId) {
        return openClean(FilesFacade.INSTANCE, slotDir, lineageId);
    }

    /**
     * Facade-aware variant of {@link #openClean(String, long)}.
     */
    public static PersistedSymbolDict openClean(FilesFacade ff, String slotDir, long lineageId) {
        return openFresh(ff, slotDir + "/" + FILE_NAME, true, lineageId);
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
     * checksum. This is the RE-ENCODE path: the steady-state persist ships a frame's
     * pre-encoded delta bytes through {@link #appendRawEntries}, and only a retry
     * after a failed publish rebuilds a range straight from the dictionary. The
     * mapped path stages the entry region in the scratch buffer with a single UTF-8
     * walk per symbol, then bulk-copies it into the append window after the header,
     * and still commits WITHOUT a positioned-write syscall. A direct encode into the
     * window would walk each symbol's UTF-8 length twice -- the exact entriesLen sizes
     * both the header varint and the mmap reserve -- and the back-to-back chunk format
     * leaves no room to reserve-and-back-fill the header in place. The
     * positioned-write fallback runs only behind an injected filesystem facade so
     * short-write recovery tests retain their fault seam. Callers pass the dictionary
     * and the range so the ids resolve to their symbol strings.
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
            // Stage the entry region in scratch with ONE UTF-8 walk per symbol, then
            // bulk-copy it into the append window after the header (see the method
            // javadoc for why a direct in-window encode would have to walk each symbol
            // twice). ensureScratch enforces the same MAX_SCRATCH_BYTES ceiling the old
            // sizing pass did, throwing before size/appendOffset advance.
            int entriesLen = 0;
            for (int id = from; id <= to; id++) {
                CharSequence symbol = dict.getSymbol(id);
                int utf8Len = Utf8s.utf8Bytes(symbol);
                int wireLen = NativeBufferWriter.varintSize(utf8Len) + utf8Len; // [len][utf8]
                ensureScratch((long) entriesLen + wireLen);
                long q = NativeBufferWriter.writeVarint(scratchAddr + entriesLen, utf8Len);
                if (utf8Len > 0) {
                    Utf8s.strCpyUtf8(symbol, q, utf8Len);
                }
                entriesLen += wireLen;
            }
            int hdrLen = NativeBufferWriter.varintSize(count)
                    + NativeBufferWriter.varintSize(entriesLen);
            long recLen = (long) hdrLen + entriesLen + CRC_SIZE;
            ensureAppendMap(checkedRequiredOffset(recLen));
            long recStart = appendMapAddr + appendOffset - appendMapOffset;
            long p = NativeBufferWriter.writeVarint(recStart, count);
            NativeBufferWriter.writeVarint(p, entriesLen);
            if (entriesLen > 0) {
                Unsafe.getUnsafe().copyMemory(scratchAddr, recStart + hdrLen, entriesLen);
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
        // Each step in its own try/catch, as CursorSendEngine.close() already does.
        // `closed` is set first, so a throw from any step short-circuits every retry --
        // stranding the scratch buffer and the fd for the process's lifetime. Unreachable
        // with FilesFacade.INSTANCE (munmap and truncate are native calls returning
        // int/boolean), but the ff seam exists precisely so a test CAN inject a throw,
        // and a close path must not depend on its own steps never failing.
        if (loadedEntriesAddr != 0L) {
            try {
                Unsafe.free(loadedEntriesAddr, loadedEntriesLen, MemoryTag.NATIVE_DEFAULT);
            } catch (Throwable ignored) {
            }
            // Null after freeing (like scratchAddr below) so a CONSTRUCTION-PHASE reader
            // that runs after close observes 0 rather than a dangling pointer. This is
            // NOT a cross-thread guard, and the getters' own javadoc is the accurate
            // statement: they are unsynchronised reads of non-volatile fields, so a
            // caller on another thread has no guarantee of seeing this write and can
            // still dereference freed memory. Ordering, not nulling, is what makes the
            // borrow safe -- every owner closes its loop before the engine.
            loadedEntriesAddr = 0L;
            loadedEntriesLen = 0;
        }
        if (appendMapAddr != 0L) {
            long mapAddr = appendMapAddr;
            long mapCapacity = appendMapCapacity;
            appendMapAddr = 0L;
            appendMapCapacity = 0L;
            appendMapOffset = 0L;
            try {
                ff.munmap(mapAddr, mapCapacity, MemoryTag.MMAP_DEFAULT);
            } catch (Throwable ignored) {
            }
            try {
                // The active window reserves space past the logical end. Return that tail on
                // orderly close; after a crash open() treats the zero-filled reserve as
                // a torn trailing chunk and truncates it to the same appendOffset.
                if (!ff.truncate(fd, appendOffset)) {
                    LOG.warn("symbol dict {} could not trim mmap reserve to {}; recovery will "
                                    + "discard the zero-filled tail on the next open",
                            filePath, appendOffset);
                }
            } catch (Throwable ignored) {
            }
        }
        if (scratchAddr != 0L) {
            try {
                Unsafe.free(scratchAddr, scratchCap, MemoryTag.NATIVE_DEFAULT);
            } catch (Throwable ignored) {
            }
            scratchAddr = 0L;
            scratchCap = 0;
        }
        if (fd >= 0) {
            try {
                ff.close(fd);
            } catch (Throwable ignored) {
            }
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
     * Number of symbols {@link #open} recovered from disk -- the exact entry count of
     * {@link #loadedEntriesAddr()} / {@link #loadedEntriesLen()}. Unlike {@link #size()}
     * this never advances, so a caller seeding from the loaded bytes stays in lockstep
     * with them even after the producer has appended (the recovery heal does exactly
     * that, before the send loop is built).
     */
    public int recoveredSize() {
        return recoveredSize;
    }

    @TestOnly
    public boolean usedMappedRecoveryInput() {
        return mappedRecoveryInput;
    }

    /**
     * Decodes the recovered entries directly into {@code target} in ascending-id
     * order. This avoids materialising a cardinality-sized temporary list that
     * the producer would immediately copy into the global dictionary.
     * Construction-phase only; see {@link #loadedEntriesAddr()}.
     */
    public void addLoadedSymbolsTo(GlobalSymbolDictionary target) {
        decodeLoadedSymbols(target, null);
    }

    /**
     * Materialises the loaded entries as symbol strings in ascending-id order.
     * Retained for recovery-format tests; production decodes directly through
     * {@link #addLoadedSymbolsTo(GlobalSymbolDictionary)}.
     */
    @TestOnly
    public ObjList<String> readLoadedSymbols() {
        ObjList<String> out = new ObjList<>(Math.max(recoveredSize, 1));
        decodeLoadedSymbols(null, out);
        return out;
    }

    private void decodeLoadedSymbols(GlobalSymbolDictionary target, ObjList<String> out) {
        long p = loadedEntriesAddr;
        long limit = p + loadedEntriesLen;
        // open() CRC-validated these bytes and copyRecoveredEntries laid down exactly
        // `size` entries, so this decode runs on trusted, self-consistent input. Any
        // mismatch means an internal invariant broke: fail loud like the rest of this
        // file (copyRecoveredEntries throws too) instead of silently under-populating
        // the dictionary, which would shift every id above the short point.
        // recoveredSize, not the live size: this decodes the LOADED region, whose entry
        // count is fixed at open. Appends (including the recovery heal) advance size
        // without extending that region, so keying off size would over-read.
        for (int i = 0; i < recoveredSize; i++) {
            long len = 0;
            int shift = 0;
            boolean terminated = false;
            while (p < limit) {
                byte b = Unsafe.getUnsafe().getByte(p++);
                len |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    terminated = true;
                    break;
                }
                shift += 7;
                if (shift > 35) {
                    break; // over-long run -- a canonical length varint is <= 5 bytes
                }
            }
            if (!terminated || p + len > limit) {
                // UnreplayableSlotException so Sender.build() can set the slot aside
                // instead of rethrowing forever -- see CursorSendEngine's twin throw.
                throw new UnreplayableSlotException("truncated loaded symbol dictionary entry to "
                        + FILE_NAME + " [entry=" + i + ", size=" + recoveredSize + ']');
            }
            String symbol = Utf8s.stringFromUtf8Bytes(p, p + len);
            if (target != null) {
                target.addRecoveredSymbol(symbol);
            } else {
                out.add(symbol);
            }
            p += len;
        }
        if (p != limit) {
            throw new UnreplayableSlotException("loaded symbol dictionary has trailing bytes to "
                    + FILE_NAME + " [consumed=" + (p - loadedEntriesAddr) + ", length=" + loadedEntriesLen + ']');
        }
    }

    /**
     * Number of symbols the dictionary holds (highest id + 1).
     */
    public int size() {
        return size;
    }

    /**
     * @param inFlight single-slot holder the caller must close when this method throws.
     *                 Set immediately before the return, so a late-delivered mmap access
     *                 fault -- which pre-JDK-21 HotSpot may raise at this method's RETURN,
     *                 in the caller's frame, past the catch below -- still leaves the fully
     *                 built instance (fd plus loaded-entry buffer) reachable by someone.
     */
    private static PersistedSymbolDict openExisting(FilesFacade ff, String filePath, long fileLen,
                                                     long expectedLineageId, PersistedSymbolDict[] inFlight) {
        int fd = ff.openRW(filePath);
        if (fd < 0) {
            LOG.warn("symbol dict {} could not be opened (rc={}); "
                    + "falling back to full-dictionary frames (file left intact)", filePath, fd);
            return null;
        }
        int len = (int) fileLen; // open() bounds fileLen to [HEADER_SIZE, Integer.MAX_VALUE)
        boolean mappedInput = ff.isMmapAllowed();
        long inputAddr = 0L;
        long entriesAddr = 0L;
        int entriesCapacity = 0;
        int entriesLen = 0;
        try {
            if (mappedInput) {
                inputAddr = ff.mmap(fd, fileLen, 0L, Files.MAP_RO, MemoryTag.MMAP_DEFAULT);
                if (inputAddr == Files.FAILED_MMAP_ADDRESS) {
                    inputAddr = 0L;
                    throw new IllegalStateException("could not mmap symbol dictionary for recovery");
                }
            } else {
                // Fault-injection facades retain the positioned-read path so tests
                // can still force short reads without mapping around the seam.
                inputAddr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                long read = ff.read(fd, inputAddr, len, 0);
                if (read != len) {
                    throw new IllegalStateException("short read while recovering symbol dictionary "
                            + "[expected=" + len + ", actual=" + read + ']');
                }
            }
            if (Unsafe.getUnsafe().getInt(inputAddr) != FILE_MAGIC
                    || Unsafe.getUnsafe().getByte(inputAddr + 4) != VERSION) {
                throw new IllegalStateException("bad magic or unknown symbol dictionary version");
            }
            // The only check a recovered dictionary previously got was a size heuristic;
            // nothing compared its persisted strings against what the surviving frames
            // actually reference at overlapping ids. A dictionary stamped with a DIFFERENT
            // generation is a survivor from a prior producer lineage -- trusting it would
            // register its symbols under ids this generation's rows reference, with no gap,
            // a valid CRC and both bounds checks satisfied. Discarding (falling back to full
            // self-sufficient frames) is always safe, so that is the disposition for every
            // ambiguity here -- return null directly rather than throw, since this is an
            // ordinary refusal, not an I/O or parse failure.
            long fileLineageId = Unsafe.getUnsafe().getLong(inputAddr + 8);
            if (fileLineageId != expectedLineageId) {
                LOG.warn("symbol dict {} belongs to lineage {} but this slot is {}; "
                                + "discarding it and falling back to full-dictionary frames",
                        filePath, fileLineageId, expectedLineageId);
                if (mappedInput) {
                    ff.munmap(inputAddr, fileLen, MemoryTag.MMAP_DEFAULT);
                } else {
                    Unsafe.free(inputAddr, len, MemoryTag.NATIVE_DEFAULT);
                }
                ff.close(fd);
                return null;
            }
            // ONE pass: validate each chunk's CRC and, once proven good, copy its
            // entries straight out. The entry region is by construction a subset of
            // the file, so `len` is a safe upper bound to allocate against -- it
            // overshoots only by the chunk headers and CRCs -- and the exact size is
            // reclaimed by the shrink below. The previous shape walked the file
            // TWICE, re-decoding both header varints of every chunk on the second
            // pass purely to size one allocation it could have bounded for free.
            entriesCapacity = len;
            entriesAddr = Unsafe.malloc(entriesCapacity, MemoryTag.NATIVE_DEFAULT);
            RecoveryScan scan = scanAndCopyRecoveredChunks(inputAddr, len, entriesAddr);
            entriesLen = scan.entriesLen;
            if (entriesLen == 0) {
                Unsafe.free(entriesAddr, entriesCapacity, MemoryTag.NATIVE_DEFAULT);
                entriesAddr = 0L;
                entriesCapacity = 0;
            } else if (entriesLen < entriesCapacity) {
                entriesAddr = Unsafe.realloc(
                        entriesAddr, entriesCapacity, entriesLen, MemoryTag.NATIVE_DEFAULT);
                entriesCapacity = entriesLen;
            }
            if (mappedInput) {
                ff.munmap(inputAddr, fileLen, MemoryTag.MMAP_DEFAULT);
            } else {
                Unsafe.free(inputAddr, len, MemoryTag.NATIVE_DEFAULT);
            }
            inputAddr = 0L;
            // Drop any torn/stale trailing bytes so a LATER, shorter append cannot
            // leave residue past its own end. The truncate result IS checked: a file
            // we cannot trim could still expose stale post-end bytes whose
            // (self-consistent) chunk CRC the parse would accept at a shifted
            // position, so a failed truncate makes the file untrusted -- return null
            // (the sender falls back to full self-sufficient frames) and, per the
            // never-destroy contract, leave every byte on disk.
            if (scan.validLen < len && !ff.truncate(fd, scan.validLen)) {
                throw new IllegalStateException("could not drop torn/stale symbol dictionary tail");
            }
            PersistedSymbolDict dict = new PersistedSymbolDict(
                    ff, filePath, fd, scan.validLen, scan.count, entriesAddr, entriesLen, mappedInput);
            // Publish to the holder BEFORE returning: from here on the caller owns the
            // cleanup, including when the return itself faults. See the parameter doc.
            inFlight[0] = dict;
            return dict;
        } catch (Throwable t) {
            if (inputAddr != 0L) {
                if (mappedInput) {
                    ff.munmap(inputAddr, fileLen, MemoryTag.MMAP_DEFAULT);
                } else {
                    Unsafe.free(inputAddr, len, MemoryTag.NATIVE_DEFAULT);
                }
            }
            if (entriesAddr != 0L) {
                // capacity, not entriesLen: the shrink may not have happened yet.
                Unsafe.free(entriesAddr, entriesCapacity, MemoryTag.NATIVE_DEFAULT);
            }
            if (fd >= 0) {
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

    /**
     * Validates every chunk and copies the entries of each proven-good one into
     * {@code dstAddr}, in a single pass. {@code dstAddr} must have room for {@code len}
     * bytes -- the entry region is a subset of the file, so that always suffices.
     * Stops at the first chunk that is torn, fails its CRC, or is internally
     * inconsistent, exactly as the two-pass version did, so the trusted prefix is
     * unchanged.
     */
    private static RecoveryScan scanAndCopyRecoveredChunks(long inputAddr, int len, long dstAddr) {
        Varint v = new Varint();
        int count = 0;
        int diskPos = HEADER_SIZE;
        long entriesLen = 0L;
        while (diskPos < len) {
            if (!v.decode(inputAddr, diskPos, len)) {
                break;
            }
            long entryCount = v.value;
            if (!v.decode(inputAddr, v.end, len)) {
                break;
            }
            long entryBytes = v.value;
            int entriesStart = v.end;
            long chunkEnd = (long) entriesStart + entryBytes;
            if (chunkEnd + CRC_SIZE > len) {
                break;
            }
            int chunkEndI = (int) chunkEnd;
            int crcStored = Unsafe.getUnsafe().getInt(inputAddr + chunkEndI);
            int crcCalc = Crc32c.updateUnsafe(
                    Crc32c.INIT,
                    inputAddr + diskPos,
                    chunkEndI - diskPos);
            if (crcCalc != crcStored
                    || entryCount <= 0
                    // Every entry costs at least its own length varint, so a positive
                    // entryCount inside a zero-byte region is self-contradictory. The CRC
                    // proves the bytes are what was WRITTEN, never that the triple is
                    // consistent, and decodeLoadedSymbols would only discover it later --
                    // as a throw two layers up rather than a trimmed trusted prefix.
                    || entryBytes <= 0
                    || (long) count + entryCount > Integer.MAX_VALUE
                    || entriesLen + entryBytes > Integer.MAX_VALUE) {
                break;
            }
            if (!isConsistentEntryRegion(inputAddr, entriesStart, entryBytes, entryCount)) {
                break;
            }
            Unsafe.getUnsafe().copyMemory(inputAddr + entriesStart, dstAddr + entriesLen, entryBytes);
            entriesLen += entryBytes;
            diskPos = chunkEndI + CRC_SIZE;
            count += (int) entryCount;
        }
        return new RecoveryScan(count, (int) entriesLen, diskPos);
    }

    /**
     * Whether {@code [start, start + bytes)} holds exactly {@code count} well-formed
     * {@code [len varint][utf8]} entries, consuming the region exactly.
     * <p>
     * The chunk CRC proves the bytes are what was WRITTEN; it says nothing about whether
     * the header triple is self-consistent. The only write-side guard,
     * {@code validateRawEntries}, sits behind an {@code assert} -- and this library ships
     * embedded in user applications, which run without {@code -ea}. So a producer bug or
     * a torn write that happens to re-checksum could record a chunk whose stored
     * entryCount disagrees with its entries, shifting the dense id-&gt;symbol map for
     * everything above it.
     * <p>
     * Checking here ends the trusted prefix at that chunk -- the same treatment a CRC
     * failure gets -- instead of letting decodeLoadedSymbols discover it later and throw
     * two layers up, which quarantines the whole slot rather than salvaging its intact
     * prefix. It costs one varint decode per entry on a cold path that already walks
     * every entry immediately afterwards.
     */
    private static boolean isConsistentEntryRegion(long addr, int start, long bytes, long count) {
        long p = start;
        long limit = start + bytes;
        for (long i = 0; i < count; i++) {
            long len = 0;
            int shift = 0;
            boolean terminated = false;
            while (p < limit) {
                byte b = Unsafe.getUnsafe().getByte(addr + p++);
                len |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    terminated = true;
                    break;
                }
                shift += 7;
                if (shift > 35) {
                    return false; // over-long run: a canonical length varint is <= 5 bytes
                }
            }
            if (!terminated || len < 0 || p + len > limit) {
                return false;
            }
            p += len;
        }
        return p == limit;
    }

    /**
     * @param mustTruncate {@code true} for the fresh-slot path ({@link #openClean}),
     *                      where an existing file MUST be cleared -- a survivor that
     *                      cannot be truncated describes a prior generation's id
     *                      space, and proceeding anyway would leave it on disk while
     *                      this session writes rows against a fresh id space from 0.
     *                      The next recovery cannot tell the two apart: ids overlap,
     *                      the CRC is valid, and the catch-up would register the
     *                      wrong strings under ids this generation's rows reference.
     *                      So this case throws {@link UnreplayableSlotException} rather
     *                      than degrading to null. {@code false} for the recovery
     *                      path ({@link #open}), which only ever reaches this method
     *                      when the file is absent or a headerless stub -- nothing
     *                      load-bearing to lose -- so a create failure there still
     *                      just degrades to null like every other recovery I/O
     *                      failure.
     * @param lineageId the producer lineage to stamp into the freshly-created header --
     *                    the caller's expected/derived generation in both the
     *                    {@code mustTruncate} and non-{@code mustTruncate} cases, so a
     *                    dictionary created here always already agrees with its caller.
     */
    private static PersistedSymbolDict openFresh(FilesFacade ff, String filePath, boolean mustTruncate,
                                                 long lineageId) {
        int fd = ff.openCleanRW(filePath);
        if (fd < 0) {
            if (mustTruncate && ff.exists(filePath)) {
                // A fresh slot MUST start with an empty dictionary. Proceeding without one
                // leaves the previous generation's entries on disk while this session
                // writes rows against a fresh id space from 0, and the next recovery
                // cannot distinguish the two: the ids overlap, the CRC is valid, and the
                // catch-up registers the wrong strings under ids this generation's rows
                // reference. Refuse the slot; the bytes stay on disk for an operator.
                // Typed as UnreplayableSlotException (rather than a plain
                // LineSenderException) so Sender.build()'s constructor-time catch
                // quarantines this fresh, dataless slot instead of bricking every
                // restart under a stable senderId.
                throw new UnreplayableSlotException("symbol dict ").put(filePath)
                        .put(" already exists and cannot be truncated (rc=").put(fd)
                        .put("); refusing to start on a slot whose dictionary describes a")
                        .put(" different id space -- move or remove it by hand");
            }
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
            Unsafe.getUnsafe().putLong(hdr + 8, lineageId);
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
            // -1 rather than throwing; the Unsafe puts target a valid 16-byte buffer and
            // a 16-byte malloc cannot realistically OOM), but the ff seam exists so
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
        return new PersistedSymbolDict(ff, filePath, fd, HEADER_SIZE, 0, 0L, 0, false);
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
        // updateUnsafe, NOT the native update: this checksums bytes inside the append
        // MAPPING. ensureAppendMap grows the file with ff.allocate, whose native fallback
        // is ftruncate, so the window can cover blocks the filesystem has not committed
        // (ENOSPC, a quota, a sparse tail left by a crash) -- and touching one raises
        // SIGBUS. Inside JNI that ABORTS THE JVM with no recovery; at an Unsafe intrinsic
        // site HotSpot converts it to a catchable InternalError. MmapSegment.scanFrames
        // already uses updateUnsafe over the same class of mapping for exactly this
        // reason. Costs nothing measurable and drops a JNI transition per flush.
        Unsafe.getUnsafe().putInt(
                recStart + bodyLen,
                Crc32c.updateUnsafe(Crc32c.INIT, recStart, bodyLen));
        Unsafe.getUnsafe().storeFence();
        appendOffset += recLen;
        size += count;
    }

    /**
     * Ensures the production append mmap covers the absolute file offset
     * {@code required}. The log is mapped in 4 MiB-aligned windows: small flushes
     * share a segment-sized window, while a large existing dictionary does not force
     * the process to reserve and remap its whole prefix or geometrically over-allocate it.
     */
    private void ensureAppendMap(long required) {
        if (appendMapAddr != 0L
                && required >= appendMapOffset
                && required <= appendMapOffset + appendMapCapacity) {
            return;
        }
        // Page-align, not APPEND_MAP_CAPACITY-align: mmap only requires a page-aligned
        // file offset. Rounding the START down to a 4 MiB boundary while sizing `needed`
        // to the record's END meant a chunk straddling that boundary produced a window
        // spanning BOTH -- 8 MiB mapped and re-allocated, whose lower half covers bytes
        // already written and never touched again. Steady state then advanced 4 MiB per
        // remap while always mapping 8.
        long pageMask = Files.PAGE_SIZE - 1;
        long newOffset = appendOffset & ~pageMask;
        long needed = required - newOffset;
        long newCapacity = Math.max(APPEND_MAP_CAPACITY, needed);
        long remainder = newCapacity % APPEND_MAP_CAPACITY;
        if (remainder != 0L) {
            long padding = APPEND_MAP_CAPACITY - remainder;
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
            ff.munmap(oldAddr, oldCapacity, MemoryTag.MMAP_DEFAULT);
        }
        long newAddr = ff.mmap(
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
        // long math, matching commitMappedChunk: hdrLen + entriesLen can reach
        // Integer.MAX_VALUE (entriesLen is bounded only by MAX_SCRATCH_BYTES), and an
        // int sum would wrap negative and hand a bogus length to the CRC and the write.
        long bodyLen = (long) hdrLen + entriesLen;
        long recLen = bodyLen + CRC_SIZE;
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

    private static final class RecoveryScan {
        private final int count;
        private final int entriesLen;
        private final int validLen;

        private RecoveryScan(int count, int entriesLen, int validLen) {
            this.count = count;
            this.entriesLen = entriesLen;
            this.validLen = validLen;
        }
    }

    /**
     * Zero-allocation LEB128 decoder, one instance per recovery pass -- not one
     * per chunk. The previous {@code long[]}-returning decoder allocated once per
     * entry, so a million-symbol recovery churned a million throwaway arrays.
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
