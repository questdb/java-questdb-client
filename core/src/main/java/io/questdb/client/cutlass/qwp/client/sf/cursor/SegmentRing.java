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

import io.questdb.client.std.Files;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chain of {@link MmapSegment}s presented to the user thread as one logical
 * append-only log keyed by frame sequence number (FSN). Owns segment
 * lifecycle: rotation when the active segment fills, ACK-driven trim of the
 * oldest sealed segments. Built for the cursor engine's split-brain threading:
 * <ul>
 *   <li><b>Producer thread</b> (single user thread): {@link #appendOrFsn},
 *       {@link #installHotSpare}, {@link #publishedFsn}.</li>
 *   <li><b>I/O thread</b>: {@link #publishedFsn} (read-only), {@link #acknowledge}
 *       (single writer), {@link #drainTrimmable} (single reader).</li>
 *   <li><b>Segment manager</b>: polls {@link #needsHotSpare}, hands new
 *       segments via {@link #installHotSpare}, drains trim-eligible segments
 *       via {@link #drainTrimmable} on its own cadence.</li>
 * </ul>
 * No locks; the only cross-thread state is {@link #publishedFsn} (volatile,
 * single-writer) and {@link #ackedFsn} (volatile, single-writer). Hot-spare
 * handoff uses {@code volatile} as well -- the segment manager publishes a
 * spare; the producer thread consumes it on the next rotation.
 * <p>
 * <b>Backpressure model:</b> {@link #appendOrFsn} returns
 * {@link #BACKPRESSURE_NO_SPARE} when the active is full and no spare is
 * available. The caller (engine) is expected to spin-park until the segment
 * manager catches up, OR until {@link #acknowledge} advances {@link #ackedFsn}
 * far enough that the segment manager can recycle a sealed segment.
 */
public final class SegmentRing implements QuietCloseable {

    /** Sentinel: append failed because no hot spare was available to rotate into. */
    public static final long BACKPRESSURE_NO_SPARE = -1L;
    /**
     * {@link #maxRecoveredFileGeneration()} value for rings that were not
     * produced by a directory-scanning recovery (constructor-built rings:
     * fresh start, memory mode, tests). {@code Long.MIN_VALUE} cannot collide
     * with a real scan result, whose minimum is {@code -1} (scanned, but no
     * generation-named files found).
     */
    public static final long FILE_GENERATION_UNKNOWN = Long.MIN_VALUE;
    /** Sentinel: append failed because the payload doesn't fit in a fresh segment. */
    public static final long PAYLOAD_TOO_LARGE = -2L;
    private static final Logger LOG = LoggerFactory.getLogger(SegmentRing.class);
    // Tally of baseSeq comparisons performed by sortByBaseSeq across every
    // openExisting() recovery on this JVM. Used by SegmentRingTest to
    // assert the sort stays O(N log N) without relying on wall-clock time
    // (CI runner variance makes elapsed-millisecond bounds flaky). Cheap
    // in production: one volatile-free add per partition pass, dwarfed by
    // the mmap I/O the recovery does on every segment.
    private static long sortComparisons;
    private final long maxBytesPerSegment;
    // Sealed segments in baseSeq order, oldest first. Active is held separately.
    // Single-writer (producer thread, on rotation); single-reader at trim time
    // (the segment manager). For now, both sides synchronize via the single-
    // writer guarantee plus the volatile ackedFsn -- the segment manager only
    // looks at sealedSegments after observing a higher ackedFsn, by which
    // point the producer thread's add to sealedSegments has retired.
    private final ObjList<MmapSegment> sealedSegments = new ObjList<>();
    // High-water byte offset within the active segment at which we proactively
    // ask the segment manager to provision a spare (if one isn't already
    // installed). Computed once as 3/4 of segment capacity -- leaves the manager
    // a quarter-of-a-segment of producer runway to do its open+mmap before the
    // producer would otherwise hit BACKPRESSURE_NO_SPARE.
    private final long signalAtBytes;
    private volatile long ackedFsn = -1L;
    // active: written by producer (constructor + appendOrFsn rotation),
    // read by I/O thread via getActive(). Volatile so the I/O thread sees
    // rotations promptly and never observes a torn reference.
    private volatile MmapSegment active;
    // Set to true by close(); checked by installHotSpare under the ring's
    // monitor to reject spares that arrive after the ring has been torn
    // down. Without this, a manager's serviceRing tick that snapshotted
    // the ring before deregister could create a fresh MmapSegment, then
    // call installHotSpare on a closed ring (whose hotSpare was just
    // zeroed by close()) -- the spare's mmap + fd would never be reclaimed.
    private boolean closed;
    // hotSpare: written by segment manager (installHotSpare), read+cleared by
    // producer thread on rotation. Volatile so the producer sees fresh installs.
    private volatile MmapSegment hotSpare;
    // Optional callback the segment manager registers via setManagerWakeup
    // so the producer can wake the manager out of its poll-park the moment
    // a spare is needed (rotation just consumed one, or active crossed the
    // high-water mark while no spare is installed). Without this, the
    // manager only notices on its next polling tick -- fine on average,
    // but the worst-case wait is the full poll interval. Producer-thread-only.
    private Runnable managerWakeup;
    // Highest sf-<gen:016x>.sfa file generation observed by the openExisting
    // directory scan, or FILE_GENERATION_UNKNOWN for constructor-built rings.
    // Written once by openExisting before the ring is published to the
    // caller; read-only afterwards (see maxRecoveredFileGeneration()).
    private long maxRecoveredFileGeneration = FILE_GENERATION_UNKNOWN;
    private long nextSeq;
    private volatile long publishedFsn;
    // Plain (producer-thread-only) flag; set to true the first time we ask
    // the manager for a spare for the current active segment, cleared on
    // every rotation. Coalesces multiple high-water-mark crossings into a
    // single unpark per active.
    private boolean wakeupRequestedForActive;

    /**
     * Creates a ring with the given segment cap and an already-prepared
     * initial active segment. The initial segment must be empty (just headers,
     * frameCount == 0); typically supplied by the segment manager at startup.
     */
    public SegmentRing(MmapSegment initialActive, long maxBytesPerSegment) {
        if (initialActive == null) {
            throw new IllegalArgumentException("initialActive must not be null");
        }
        this.active = initialActive;
        this.maxBytesPerSegment = maxBytesPerSegment;
        // 3/4 of capacity gives the manager a full quarter-segment of producer
        // runway before backpressure kicks in. Long math, no float, no alloc.
        this.signalAtBytes = (maxBytesPerSegment >> 2) * 3;
        this.nextSeq = initialActive.baseSeq() + initialActive.frameCount();
        this.publishedFsn = nextSeq - 1;
    }

    /**
     * Recovers a ring from segments already on disk in {@code sfDir}. Used at
     * sender startup when the user's previous session left durable but
     * not-yet-acked frames behind. Walks every {@code *.sfa} file in the
     * directory, opens each via {@link MmapSegment#openExisting}, and
     * arranges them by baseSeq:
     * <ul>
     *   <li>Highest-baseSeq segment becomes the active. Recovered segments
     *       are mapped only through their validated prefix (SIGBUS hardening
     *       in {@link MmapSegment#openExisting}) and are therefore born full:
     *       the first append backpressures until the manager installs a hot
     *       spare, then rotates onto it.</li>
     *   <li>All others become sealed segments awaiting ACK and trim.</li>
     * </ul>
     * Returns {@code null} only when there is genuinely no data to recover:
     * the directory does not exist, is empty, or contains only empty
     * hot-spare leftovers (which are unlinked). The caller may then safely
     * construct a fresh ring with {@link #SegmentRing(MmapSegment, long)} and
     * a freshly created initial segment.
     * <p>
     * Recovery fails closed: any {@code .sfa} file that cannot be opened --
     * bad magic, bad header, unsupported version, short file, mmap rejection
     * -- aborts recovery with {@link MmapSegmentException}, as does a failure
     * to enumerate the directory. Skipping an unreadable file is never safe
     * here: the interior FSN-contiguity check below cannot expose a skipped
     * segment at the lowest, highest, or sole position (the survivors stay
     * mutually contiguous), so recovery would silently retire the skipped
     * segment's unacked frames, mint duplicate FSNs past a skipped tail, or
     * -- for a sole skipped file -- route the caller onto the truncating
     * fresh-start path that destroys the only surviving segment. An operator
     * who wants to accept the loss moves the file out of the SF directory
     * explicitly.
     */
    public static SegmentRing openExisting(String sfDir, long maxBytesPerSegment) {
        if (!Files.exists(sfDir)) {
            return null;
        }
        ObjList<MmapSegment> opened = new ObjList<>();
        // Highest sf-<gen:016x>.sfa generation seen during the scan; -1 when
        // none matched. Carried on the returned ring so SegmentManager's
        // register() can seed its spare file-generation counter without
        // re-enumerating the directory this scan just walked.
        long maxFileGeneration = -1L;
        long find = Files.findFirst(sfDir);
        if (find < 0) {
            // Fail closed: an enumeration failure (permissions, transient
            // I/O error) is NOT an empty store. Returning null here would
            // route the caller onto the fresh-start path, whose truncating
            // openCleanRW of sf-initial.sfa destroys any surviving data the
            // JVM merely could not list.
            throw new MmapSegmentException(
                    "could not enumerate SF directory " + sfDir
                            + " -- refusing to treat an unreadable directory as an empty"
                            + " store (a fresh start would truncate sf-initial.sfa over"
                            + " any surviving data); check directory permissions");
        }
        if (find == 0) {
            return null;
        }
        // Outer try-catch: anything escaping the recovery body -- IOOBE from
        // ObjList growth, OOM from native mmap during MmapSegment.openExisting,
        // unforeseen RuntimeException from the contiguity check, etc. -- must
        // not leave fds + mmaps owned by `opened` orphaned. Close every
        // recovered segment and rethrow so the engine surfaces the failure.
        try {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    // Track the highest file generation over EVERY entry the
                    // scan visits, before the unlink/quarantine below can
                    // remove it. Including removed files keeps the bound
                    // conservative -- a generation is never reused, so a new
                    // spare can never collide with a quarantined twin's
                    // <name>.sfa.corrupt on a future quarantine rename.
                    long gen = parseFileGeneration(name);
                    if (gen > maxFileGeneration) {
                        maxFileGeneration = gen;
                    }
                    if (name != null && name.endsWith(".sfa")) {
                        String path = sfDir + "/" + name;
                        MmapSegment seg = null;
                        try {
                            try {
                                seg = MmapSegment.openExisting(path);
                            } catch (MmapSegmentException t) {
                                // Fail closed: an unreadable .sfa in the slot is a
                                // recovery failure, not a skippable curiosity. The
                                // interior contiguity check below cannot expose a
                                // skipped segment at the lowest, highest, or sole
                                // position -- the survivors stay mutually
                                // contiguous -- so skipping would silently retire
                                // the segment's unacked frames (lowest/sole: the
                                // engine seeds ackedFsn past them), mint duplicate
                                // FSNs (highest: nextSeq resumes inside the skipped
                                // range), or -- sole -- route the caller onto the
                                // truncating fresh-start path that destroys the
                                // only surviving file. The outer catch closes every
                                // already-recovered segment before this propagates.
                                throw new MmapSegmentException(
                                        "recovery failed: unreadable segment file " + path
                                                + " -- refusing to start with partial data (a skipped"
                                                + " segment silently loses or duplicates frames). Restore"
                                                + " the file from a copy, or move it out of the SF"
                                                + " directory to explicitly accept the data loss",
                                        t);
                            }
                            // Filter out empty leftovers -- typically hot-spare
                            // segments the manager pre-allocated for a prior
                            // session that never got rotated into active. They
                            // carry the provisional baseSeq=0 and frameCount=0,
                            // which would otherwise collide with the real
                            // baseSeq=0 segment and trip the contiguity check
                            // below. No data to recover; close and unlink.
                            // Without the unlink the file persists across crash
                            // cycles and the disk leak compounds with every
                            // unclean shutdown.
                            //
                            // CAUTION: only unlink when the file is genuinely
                            // empty past the header. If frame[0] failed CRC
                            // (bit-rot, partial-page-write at crash, etc.) but
                            // valid frames followed, the recovery scan returns
                            // lastGood=HEADER_SIZE and frameCount=0 -- yet
                            // tornTailBytes is non-zero. Treating that as
                            // "empty hot-spare" would silently destroy every
                            // surviving frame. Quarantine to <path>.corrupt
                            // instead so a postmortem can recover what's left.
                            if (seg.frameCount() == 0) {
                                long torn = seg.tornTailBytes();
                                seg.close();
                                seg = null;
                                if (torn > 0) {
                                    int renameResult = Files.rename(path, path + ".corrupt");
                                    if (renameResult != 0) {
                                        // Fail closed: with the quarantine rename
                                        // failed, the corrupt file is still at its
                                        // original path -- if it is the sole
                                        // segment, the caller's fresh start would
                                        // truncate it (sf-initial.sfa) or restart
                                        // FSNs alongside it. Surface the error
                                        // instead of proceeding over live data.
                                        throw new MmapSegmentException(
                                                "could not quarantine corrupt segment " + path
                                                        + " to " + path + ".corrupt [rc=" + renameResult
                                                        + "] -- refusing to continue recovery while the"
                                                        + " corrupt file occupies its original path");
                                    }
                                } else {
                                    if (!Files.remove(path)) {
                                        // Same fail-closed reasoning: an empty
                                        // leftover that cannot be unlinked would
                                        // be truncated by the caller's fresh
                                        // start (harmless for an empty file) --
                                        // but unlink failing in a directory we
                                        // just created files in means something
                                        // is wrong (permissions changed, dir
                                        // vanished); don't build on that ground.
                                        throw new MmapSegmentException(
                                                "could not unlink empty leftover segment " + path
                                                        + " -- refusing to continue recovery; check"
                                                        + " directory permissions");
                                    }
                                }
                            } else {
                                opened.add(seg);
                                seg = null;
                            }
                        } finally {
                            // Close any seg whose ownership wasn't transferred
                            // (either to opened, or via the empty-branch close
                            // above). Fires on a propagating throw between
                            // open and transfer -- most importantly an OOM
                            // from ObjList.add growing its backing array
                            // after the mmap+fd were already acquired.
                            if (seg != null) {
                                try {
                                    seg.close();
                                } catch (Throwable closeErr) {
                                    LOG.warn("openExisting: error closing in-flight segment {}",
                                            path, closeErr);
                                }
                            }
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
            if (opened.size() == 0) {
                return null;
            }
            // Sort by baseSeq ascending. Worst-case segment count is
            // sf_max_total_bytes / sf_max_bytes -- at the documented ceiling
            // (1 TiB / 64 MiB) that is ~16K entries, where an O(N²) sort spends
            // multiple seconds in compares + shifts before the I/O thread can
            // start. In-place quicksort with median-of-three pivot keeps the
            // no-allocation discipline of the surrounding code; median-of-three
            // is required because readdir on many filesystems returns entries
            // in lexicographic (== baseSeq-hex) order and a naive first-element
            // pivot would degrade back to O(N²) on exactly that common case.
            sortByBaseSeq(opened, 0, opened.size());
            // Sanity: the recovered segments must form a contiguous FSN range.
            // Detect gaps so they don't silently produce duplicate or missing
            // FSNs after recovery. A gap means a segment went missing (a
            // manual deletion) or a sealed segment under-recovered -- its tail
            // was cut short by a sparse/unbacked page or a mid-file media error
            // (bad sector), the same class of fault the recovery scan tolerates on the
            // active segment but which corrupts the range on a sealed one.
            for (int i = 1, n = opened.size(); i < n; i++) {
                MmapSegment prev = opened.get(i - 1);
                MmapSegment curr = opened.get(i);
                long expected = prev.baseSeq() + prev.frameCount();
                if (curr.baseSeq() != expected) {
                    throw new MmapSegmentException(
                            "FSN gap in recovered segments: prev baseSeq=" + prev.baseSeq()
                                    + " frameCount=" + prev.frameCount()
                                    + " expected next baseSeq=" + expected
                                    + " but got " + curr.baseSeq()
                                    + " -- a segment was deleted, or a sealed segment's tail was"
                                    + " truncated (sparse/unbacked page or disk media error);"
                                    + " check disk health");
                }
            }
            // The newest segment becomes the active. Recovered segments are
            // born full (mapped only through the validated prefix -- see the
            // SIGBUS hardening in MmapSegment.openExisting), so the next
            // appendOrFsn returns BACKPRESSURE_NO_SPARE, the manager installs
            // a hot spare, the producer rotates. Same fast path as a mid-life
            // ring whose active just filled.
            int last = opened.size() - 1;
            MmapSegment active = opened.get(last);
            opened.remove(last);
            SegmentRing ring = new SegmentRing(active, maxBytesPerSegment);
            // Older segments become sealed in baseSeq order.
            for (int i = 0, n = opened.size(); i < n; i++) {
                ring.sealedSegments.add(opened.get(i));
            }
            ring.maxRecoveredFileGeneration = maxFileGeneration;
            return ring;
        } catch (Throwable t) {
            // Close every recovered MmapSegment that's still in `opened`.
            // After the success path, `opened` no longer contains the active
            // segment (removed above), but the sealed segments transferred to
            // ring.sealedSegments are still owned by the ring once it's
            // returned -- so this catch only fires before the return statement.
            for (int i = 0, n = opened.size(); i < n; i++) {
                try {
                    opened.get(i).close();
                } catch (Throwable closeErr) {
                    LOG.warn("openExisting: error closing recovered segment during cleanup",
                            closeErr);
                }
            }
            throw t;
        }
    }

    /**
     * Parses the spare-file generation out of a {@code sf-<gen:016x>.sfa}
     * file name; returns {@code -1} for anything else ({@code sf-initial.sfa},
     * non-hex, wrong hex length, {@code null}). Single source of truth shared
     * by the recovery scan above and the segment manager's fallback directory
     * scan (scanMaxGeneration) so the two can never disagree on which file
     * names carry a generation. Note: startsWith + endsWith together imply a
     * minimum length of 7 (the prefix and suffix cannot overlap), so the
     * substring below cannot go out of bounds.
     */
    static long parseFileGeneration(String name) {
        if (name == null || !name.startsWith("sf-") || !name.endsWith(".sfa")) {
            return -1L;
        }
        String hex = name.substring(3, name.length() - 4);
        if (hex.length() != 16) {
            return -1L;
        }
        try {
            return Long.parseUnsignedLong(hex, 16);
        } catch (NumberFormatException e) {
            return -1L;
        }
    }

    /**
     * Highest FSN that the server has ACK'd. Read by the segment manager to
     * decide which sealed segments are safe to munmap + unlink.
     */
    public long ackedFsn() {
        return ackedFsn;
    }

    /**
     * I/O thread (or anyone tracking ACK) advances the ACK cursor. {@code seq}
     * is cumulative -- the server has confirmed every FSN up to and including
     * this value. Idempotent: a second call with the same or smaller value is
     * a no-op.
     * <p>
     * Defense-in-depth: clamp at {@link #publishedFsn} so a malformed/poisoned
     * server NACK with a bogus wireSeq cannot move {@code ackedFsn} past what
     * the producer has actually written. If we didn't clamp, the segment
     * manager could trim segments the I/O thread is still iterating and SEGV
     * the JVM on the next {@code Unsafe.getInt} of an unmapped region.
     *
     * @return {@code true} if the watermark advanced, {@code false} on
     *         no-op (idempotent re-ack or clamped). Callers wishing to fire
     *         a one-shot side effect on advance only -- e.g. dispatching to a
     *         {@code SenderProgressHandler} -- gate on the return value to
     *         avoid emitting stale values.
     */
    public boolean acknowledge(long seq) {
        long pub = publishedFsn;
        if (seq > pub) {
            seq = pub;
        }
        if (seq > ackedFsn) {
            ackedFsn = seq;
            return true;
        }
        return false;
    }

    /**
     * Single-producer append path. Reserves an FSN, writes the frame into
     * the active segment, advances {@link #publishedFsn}. Returns the assigned
     * FSN on success, or one of the {@code BACKPRESSURE_*} / {@code PAYLOAD_*}
     * sentinels on failure.
     * <p>
     * Rotation is automatic: when the active segment is full, the hot spare
     * (if installed) is promoted, the previous active joins the sealed list,
     * and the segment manager is signaled (implicitly -- it polls
     * {@link #needsHotSpare}) to prepare the next spare.
     */
    public long appendOrFsn(long payloadAddr, int payloadLen) {
        long offset = active.tryAppend(payloadAddr, payloadLen);
        if (offset == -1L) {
            // Active is full. Try to rotate.
            MmapSegment spare = hotSpare;
            if (spare == null) {
                return BACKPRESSURE_NO_SPARE;
            }
            // Pin the spare's baseSeq to whatever the active's nextSeq actually
            // is right now. This is the right moment because (a) the active is
            // full, so its frameCount is stable, and (b) the spare hasn't been
            // appended to yet (rebaseSeq enforces that). The segment manager's
            // earlier guess at baseSeq is irrelevant.
            long actualBase = active.baseSeq() + active.frameCount();
            spare.rebaseSeq(actualBase);
            // Mutate sealedSegments under the same monitor used by
            // snapshotSealedSegments -- the I/O thread reads through that
            // path and must not see a half-resized ObjList.
            synchronized (this) {
                sealedSegments.add(active);
            }
            active = spare;
            hotSpare = null;
            // Fresh active just consumed the spare → ask the manager to start
            // making the next one immediately, before this segment fills.
            // The flag is per-active and tracks whether the backup-signal
            // branch has fired for the *current* active. Rotation installs a
            // new active, so the flag resets here to re-arm the backup branch.
            // Plain field reset is safe (producer-only state).
            wakeupRequestedForActive = false;
            Runnable wakeup = managerWakeup;
            if (wakeup != null) {
                wakeup.run();
            }
            offset = active.tryAppend(payloadAddr, payloadLen);
            if (offset == -1L) {
                // Doesn't fit even in a fresh segment -- payload is genuinely too big.
                return PAYLOAD_TOO_LARGE;
            }
        } else if (!wakeupRequestedForActive
                && hotSpare == null
                && managerWakeup != null
                && active.publishedOffset() >= signalAtBytes) {
            // Backup signal: we're past the high-water mark and still don't
            // have a spare (manager hasn't caught up yet, or this is the very
            // first active and rotation hasn't fired the on-rotation wakeup).
            // Fire once per active segment.
            wakeupRequestedForActive = true;
            managerWakeup.run();
        }
        long fsn = nextSeq++;
        // publishedFsn last so the I/O thread never observes a half-written frame.
        publishedFsn = fsn;
        return fsn;
    }

    @Override
    public synchronized void close() {
        // Marking closed BEFORE freeing fields ensures any concurrent
        // installHotSpare (waiting on this monitor) will observe closed
        // when it acquires the lock and reject the spare cleanly. The
        // monitor also serializes against drainTrimmable / nextSealedAfter
        // / firstSealed / findSegmentContaining, so they don't iterate
        // half-freed state.
        closed = true;
        if (active != null) {
            active.close();
            active = null;
        }
        if (hotSpare != null) {
            hotSpare.close();
            hotSpare = null;
        }
        for (int i = 0, n = sealedSegments.size(); i < n; i++) {
            MmapSegment s = sealedSegments.get(i);
            if (s != null) {
                s.close();
            }
        }
        sealedSegments.clear();
    }

    /**
     * Removes and returns sealed segments whose every frame has been ACK'd
     * (i.e. {@code baseSeq + frameCount - 1 <= ackedFsn}). Caller takes
     * ownership and is responsible for {@code close()} + unlinking the file.
     * Called by the segment manager off the hot path. Returns {@code null}
     * when nothing is eligible (avoids ObjList allocation in the steady
     * state where most polls are no-ops).
     */
    public synchronized ObjList<MmapSegment> drainTrimmable() {
        long acked = ackedFsn;
        ObjList<MmapSegment> out = null;
        // Sealed segments are in baseSeq order, oldest first; once we hit one
        // that isn't fully acked, none of the later ones can be either.
        // Synchronized so the I/O thread's snapshotSealedSegments() can't
        // race against the range removal shuffling slots underneath it.
        // Collect the eligible prefix first, then remove it with a single
        // range removal -- per-element remove(0) would shift the suffix on
        // every iteration, making a bulk trim of S segments O(S^2).
        int eligible = 0;
        for (int i = 0, n = sealedSegments.size(); i < n; i++) {
            MmapSegment s = sealedSegments.get(i);
            long lastSeq = s.baseSeq() + s.frameCount() - 1;
            if (lastSeq > acked) {
                break;
            }
            if (out == null) {
                out = new ObjList<>();
            }
            out.add(s);
            eligible++;
        }
        if (eligible > 0) {
            sealedSegments.remove(0, eligible - 1);
        }
        return out;
    }

    /**
     * Returns the segment whose published frame range covers {@code fsn}, or
     * {@code null} if no segment currently holds it (e.g. the FSN is past
     * {@code publishedFsn} or has been trimmed). Used by the reconnect path
     * to position the I/O thread's cursor at the first unacked frame for
     * replay.
     * <p>
     * Walks sealed first (oldest → newest) then the active. The sealed list
     * is small enough -- and reconnects are rare enough -- that the linear
     * scan cost doesn't matter.
     */
    public synchronized MmapSegment findSegmentContaining(long fsn) {
        for (int i = 0, n = sealedSegments.size(); i < n; i++) {
            MmapSegment s = sealedSegments.get(i);
            long base = s.baseSeq();
            if (fsn >= base && fsn < base + s.frameCount()) {
                return s;
            }
        }
        MmapSegment a = active;
        if (a != null) {
            long base = a.baseSeq();
            if (fsn >= base && fsn < base + a.frameCount()) {
                return a;
            }
        }
        return null;
    }

    /**
     * Oldest sealed segment, or {@code null} if the sealed list is empty.
     * Used by the I/O loop's "current was trimmed out from under us"
     * fallback -- see {@link #nextSealedAfter(MmapSegment)}.
     */
    public synchronized MmapSegment firstSealed() {
        return sealedSegments.size() > 0 ? sealedSegments.get(0) : null;
    }

    /** Active segment -- exposed for the I/O thread's "send next batch" path. */
    /**
     * Walks every published frame in the ring (sealed segments plus the active
     * segment) and returns the FSN of the LAST frame whose payload does NOT
     * carry the given flag bit, or {@code -1} when every published frame
     * carries it (or the ring is empty). All frames above the returned FSN
     * carry the flag.
     * <p>
     * Recovery-time helper: locates the last commit-bearing QWP frame below a
     * potentially orphaned FLAG_DEFER_COMMIT tail left behind by a producer
     * that crashed (or closed) mid-transaction. Call before the I/O loop and
     * producer start appending; the walk is not synchronized against appends
     * into the active segment. See
     * {@link MmapSegment#findLastFrameFsnWithoutPayloadFlag} for the
     * positive-identification contract: frames that do not parse as protocol
     * messages count as commit-bearing (retirement barriers), never as
     * trimmable.
     */
    public synchronized long findLastFsnWithoutPayloadFlag(int flagsOffset, int flagMask, int headerMagic, int minPayloadLen) {
        long best = -1L;
        for (int i = 0, n = sealedSegments.size(); i < n; i++) {
            long fsn = sealedSegments.get(i).findLastFrameFsnWithoutPayloadFlag(flagsOffset, flagMask, headerMagic, minPayloadLen);
            if (fsn > best) {
                best = fsn;
            }
        }
        long fsn = active.findLastFrameFsnWithoutPayloadFlag(flagsOffset, flagMask, headerMagic, minPayloadLen);
        return Math.max(best, fsn);
    }

    public MmapSegment getActive() {
        return active;
    }

    /**
     * Direct view of sealed segments (oldest first). NOT thread-safe -- use
     * only from the producer thread, or alongside a lock that excludes
     * concurrent rotation. Cross-thread readers (typically the I/O loop)
     * should use {@link #snapshotSealedSegments(MmapSegment[])} instead.
     */
    public ObjList<MmapSegment> getSealedSegments() {
        return sealedSegments;
    }

    /**
     * Segment manager pre-creates the next segment and parks it here. The
     * producer consumes the spare on its next rotation. Throws if a spare
     * is already installed (the manager should have polled {@link #needsHotSpare}
     * first; double-install is a programming error), or if the ring has
     * been closed since the manager started provisioning the spare. The
     * latter is a benign race -- the manager's catch block already closes
     * the unused spare and unlinks its file.
     */
    public synchronized void installHotSpare(MmapSegment spare) {
        if (closed) {
            throw new IllegalStateException("ring closed");
        }
        if (hotSpare != null) {
            throw new IllegalStateException("hot spare already installed");
        }
        if (spare == null) {
            throw new IllegalArgumentException("spare must not be null");
        }
        hotSpare = spare;
    }

    public long maxBytesPerSegment() {
        return maxBytesPerSegment;
    }

    /**
     * Highest {@code sf-<gen:016x>.sfa} file generation observed during the
     * {@link #openExisting} directory scan: {@code -1} when the scan matched
     * no generation-named files, or {@link #FILE_GENERATION_UNKNOWN} when
     * this ring was not produced by recovery at all (constructor-built:
     * fresh start, memory mode, tests). The maximum is taken over every
     * entry the scan visited -- including empty leftovers it unlinked and
     * corrupt files it quarantined -- so it is a safe upper bound for the
     * segment manager's spare file-generation counter even though those
     * files are gone by the time the ring is registered.
     * <p>
     * Valid only for seeding the ring's FIRST registration: it is a
     * construction-time snapshot, so once any manager has minted spares
     * into the slot it is stale. Re-registering with the SAME manager stays
     * safe regardless -- its generation counter only ratchets upward -- but
     * a hypothetical hand-off to a different manager instance must rescan
     * the directory instead of trusting this value.
     */
    public long maxRecoveredFileGeneration() {
        return maxRecoveredFileGeneration;
    }

    /** True when the segment manager should prepare and install a fresh spare. */
    public boolean needsHotSpare() {
        return hotSpare == null;
    }

    /**
     * Returns the sealed segment whose {@code baseSeq} immediately follows
     * {@code current.baseSeq()}, or {@code null} if no such segment exists
     * (caller should fall through to {@link #getActive()}). Used by the I/O
     * loop to walk forward through the sealed list one segment at a time
     * without snapshotting the whole list -- important when the producer
     * outpaces the I/O thread and sealed segments accumulate well beyond
     * any reasonable snapshot-array size.
     * <p>
     * Identity match is intentionally avoided: we compare {@code baseSeq}
     * so the loop is robust against the case where {@code current} was
     * trimmed out from under us (already ACK'd before the I/O thread
     * advanced) -- we still return the next segment in baseSeq order rather
     * than failing. Synchronized against rotation.
     */
    public synchronized MmapSegment nextSealedAfter(MmapSegment current) {
        long currentBase = current.baseSeq();
        for (int i = 0, n = sealedSegments.size(); i < n; i++) {
            MmapSegment s = sealedSegments.get(i);
            if (s.baseSeq() > currentBase) {
                return s;
            }
        }
        return null;
    }

    /**
     * The next FSN that {@link #appendOrFsn} will assign. Useful for the
     * segment manager to know what {@code baseSeq} the next spare should use.
     */
    public long nextSeqHint() {
        return nextSeq;
    }

    /**
     * Highest FSN whose frame is fully written and visible to consumers (the
     * I/O thread). Returns -1 when nothing has been appended yet. Volatile
     * read; safe to call from any thread.
     */
    public long publishedFsn() {
        return publishedFsn;
    }

    /**
     * Registers a wakeup callback that the producer thread will invoke when
     * a hot spare is needed -- either right after a rotation has consumed the
     * previous spare, or when the active segment crosses the 75% high-water
     * mark while no spare is installed. The callback is expected to be cheap
     * (e.g. {@code LockSupport.unpark} of the segment manager's worker).
     * <p>
     * Set once, before the producer starts appending. Idempotent re-set is
     * allowed but not thread-safe.
     */
    public void setManagerWakeup(Runnable wakeup) {
        this.managerWakeup = wakeup;
    }

    /**
     * Thread-safe snapshot of the current sealed-segment list. Copies
     * references into the caller-supplied {@code target} array (oldest
     * first, packed left). Returns the number of references copied. If
     * {@code target} is too small, copies the first {@code target.length}
     * references and returns {@code -1} as a signal that the caller needs
     * to grow the buffer and retry.
     * <p>
     * Synchronized against rotation (producer's
     * {@link #appendOrFsn} mutates {@code sealedSegments}). Cost is one
     * monitor acquire/release per call, paid by the I/O loop at most once
     * per tick -- far below the cost of the actual {@code sendBinary} that
     * the I/O loop is about to do.
     */
    public synchronized int snapshotSealedSegments(MmapSegment[] target) {
        int n = sealedSegments.size();
        if (n > target.length) {
            for (int i = 0; i < target.length; i++) {
                target[i] = sealedSegments.get(i);
            }
            return -1;
        }
        for (int i = 0; i < n; i++) {
            target[i] = sealedSegments.get(i);
        }
        return n;
    }

    /**
     * Total on-disk bytes the ring currently owns: active + hot spare (if
     * installed) + every sealed segment. Used by {@code SegmentManager}
     * to seed its {@code totalBytes} accounting at register time and to
     * reverse the contribution at deregister time. Uses
     * {@link MmapSegment#fileBytes()} — the file's logical length — rather
     * than the mapped size, because recovered segments map only their
     * validated prefix while the file keeps its full length until trim
     * unlinks it. Synchronized against rotation so we never read a
     * half-resized sealed list.
     */
    public synchronized long totalSegmentBytes() {
        long total = 0L;
        MmapSegment a = active;
        if (a != null) total += a.fileBytes();
        MmapSegment hs = hotSpare;
        if (hs != null) total += hs.fileBytes();
        for (int i = 0, n = sealedSegments.size(); i < n; i++) {
            MmapSegment s = sealedSegments.get(i);
            if (s != null) total += s.fileBytes();
        }
        return total;
    }

    /**
     * Returns the cumulative count of baseSeq comparisons performed by
     * {@link #sortByBaseSeq} since the last {@link #resetSortComparisons()}
     * (or process start). The count is incremented once per partition pass
     * for the median-of-three pivot pick plus once per element compared
     * against the pivot, so a clean run on N segments adds roughly
     * {@code 3 + (hi - lo - 1)} per recursive frame, summing to O(N log N).
     * Exposed for {@code SegmentRingTest} to detect O(N²) regressions
     * deterministically.
     */
    @TestOnly
    public static long getSortComparisons() {
        return sortComparisons;
    }

    /** Zeroes the counter exposed via {@link #getSortComparisons()}. */
    @TestOnly
    public static void resetSortComparisons() {
        sortComparisons = 0;
    }

    /**
     * In-place quicksort over {@code list[lo, hi)} keyed by ascending
     * {@code baseSeq}. Median-of-three pivot avoids the pathological O(N²)
     * on already-sorted input that lexicographic readdir produces (our
     * filenames are zero-padded hex of {@code baseSeq}). Recursion depth is
     * bounded by ~2 log₂(N) -- for the documented 16K-segment ceiling, well
     * under the JVM default stack.
     */
    private static void sortByBaseSeq(ObjList<MmapSegment> list, int lo, int hi) {
        while (hi - lo > 1) {
            int mid = (lo + hi) >>> 1;
            long a = list.get(lo).baseSeq();
            long b = list.get(mid).baseSeq();
            long c = list.get(hi - 1).baseSeq();
            // Median of {a, b, c} → pivot index. Three compareUnsigned calls
            // worst case; bumping by a constant 3 keeps the counter cheap and
            // still strictly upper-bounds the true work (some short-circuit
            // out after 1-2 compares).
            sortComparisons += 3L + (hi - lo - 1);
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
            long pivot = list.get(pivotIdx).baseSeq();
            swap(list, pivotIdx, hi - 1);
            int store = lo;
            for (int i = lo; i < hi - 1; i++) {
                if (Long.compareUnsigned(list.get(i).baseSeq(), pivot) < 0) {
                    swap(list, i, store++);
                }
            }
            swap(list, store, hi - 1);
            // Recurse on the smaller partition; loop on the larger to keep
            // recursion depth bounded by log₂(N).
            if (store - lo < hi - store - 1) {
                sortByBaseSeq(list, lo, store);
                lo = store + 1;
            } else {
                sortByBaseSeq(list, store + 1, hi);
                hi = store;
            }
        }
    }

    private static void swap(ObjList<MmapSegment> list, int i, int j) {
        if (i == j) return;
        MmapSegment tmp = list.get(i);
        list.setQuick(i, list.get(j));
        list.setQuick(j, tmp);
    }
}
