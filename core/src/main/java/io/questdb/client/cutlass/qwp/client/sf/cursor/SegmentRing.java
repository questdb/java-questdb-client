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
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.IdentityHashMap;

/**
 * Chain of {@link MmapSegment}s presented to the user thread as one logical
 * append-only log keyed by frame sequence number (FSN). Owns segment
 * lifecycle: rotation when the active segment fills, ACK-driven trim of the
 * oldest sealed segments. Built for the cursor engine's split-brain threading:
 * <ul>
 *   <li><b>Producer thread</b> (single user thread): {@link #appendOrFsn},
 *       {@link #installHotSpare}, {@link #publishedFsn}.</li>
 *   <li><b>I/O thread</b>: {@link #publishedFsn} (read-only), {@link #acknowledge}
 *       (single writer), and one pinned segment cursor for native reads.</li>
 *   <li><b>Segment manager</b>: polls {@link #needsHotSpare}, hands new
 *       segments via {@link #installHotSpare}, and stages trim-eligible
 *       segments into hidden cleanup ownership on its own cadence.</li>
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
    /** Sentinel: append failed because the payload doesn't fit in a fresh segment. */
    public static final long PAYLOAD_TOO_LARGE = -2L;
    private static final RetainedSegmentMembershipMode DEFAULT_MEMBERSHIP_MODE =
            RetainedSegmentMembershipMode.IDENTITY;
    private static final Logger LOG = LoggerFactory.getLogger(SegmentRing.class);
    private static final int MAX_PENDING_TRIMS = 64;
    // Tally of sealed-list entries inspected by nextSealedAfter(). Test-only
    // operation count for deterministic traversal-complexity assertions.
    private static long nextSealedComparisons;
    // Tally of baseSeq comparisons performed by sortByBaseSeq across every
    // openExisting() recovery on this JVM. Used by SegmentRingTest to
    // assert the sort stays O(N log N) without relying on wall-clock time
    // (CI runner variance makes elapsed-millisecond bounds flaky). Cheap
    // in production: one volatile-free add per partition pass (plus two per
    // sift level in the rare heapsort fallback), dwarfed by the mmap I/O
    // the recovery does on every segment.
    private static long sortComparisons;
    // References copied while compacting the logical sealed-segment head.
    // Test-only operation count for deterministic trim-complexity assertions.
    private static long trimMovedReferences;
    private final long maxBytesPerSegment;
    private final SfManifest manifest;
    // ACKed segments leave live traversal under the ring monitor before the
    // manager unmaps them. They remain owned here until close + unlink + the
    // directory barrier succeed, so failures stay retryable and accounted.
    // At most one bounded manager batch is pending at a time.
    private final ObjList<MmapSegment> pendingTrims = new ObjList<>(MAX_PENDING_TRIMS);
    // Sealed segments in baseSeq order, oldest first. Active is held separately.
    // Single-writer (producer thread, on rotation); single-reader at trim time
    // (the segment manager). For now, both sides synchronize via the single-
    // writer guarantee plus the volatile ackedFsn -- the segment manager only
    // looks at sealedSegments after observing a higher ackedFsn, by which
    // point the producer thread's add to sealedSegments has retired.
    private final ObjList<MmapSegment> sealedSegments = new ObjList<>();
    // Logical head of sealedSegments. Head removal nulls one entry and advances
    // this index; occasional compaction bounds unused prefix slots.
    private int sealedHead;
    // Frontier index into sealedSegments: every sealed segment in
    // [sealedHead, firstNonDurableSealed) has been proven durable by an earlier
    // periodic pass and never needs re-scanning -- publishedCursor is frozen at
    // seal and durableCursor only advances, so durability never regresses. The
    // periodic sync (copyPendingSyncSegments) skips this proven-durable prefix,
    // keeping the steady-state copy-under-monitor O(1) instead of O(live-sealed)
    // work that would otherwise grow with a producer-outpaces-drain backlog.
    // Rotation seals only already-durable predecessors (the
    // requestSyncBeforeRotation gate), so the sole source of non-durable sealed
    // segments is a crash-recovery resume; those are covered because the
    // frontier starts at 0. Maintained entirely under this monitor, in the same
    // coordinate space as sealedHead (shifted by compaction, reset on clear).
    private int firstNonDurableSealed;
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
    // Latest periodic data-barrier failure. The manager latches it and the
    // producer observes it before its next append; the manager clears it
    // again once a subsequent periodic sync pass succeeds, so a transient
    // disk fault does not permanently brick the producer.
    private volatile MmapSegmentException durabilityFailure;
    // hotSpare: written by segment manager (installHotSpare), read+cleared by
    // producer thread on rotation. Volatile so the producer sees fresh installs.
    private volatile MmapSegment hotSpare;
    // Segment whose native mapping the single I/O consumer may dereference.
    // Guarded by this monitor: cursor lookup/switch and manager trim staging
    // are atomic, so a pinned segment cannot be hidden or unmapped.
    private MmapSegment ioPinnedSegment;
    // Optional callback the segment manager registers via setManagerWakeup
    // so the producer can wake the manager out of its poll-park the moment
    // a spare is needed, and the I/O thread can wake it after releasing a
    // segment that may now be trimmable. Without this, the manager only
    // notices on its next polling tick.
    private Runnable managerWakeup;
    private long nextSeq;
    private boolean periodicSyncEnabled;
    private volatile long publishedFsn;
    private volatile boolean syncRequested;
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
        this(initialActive, maxBytesPerSegment, null);
    }

    SegmentRing(MmapSegment initialActive, long maxBytesPerSegment, SfManifest manifest) {
        if (initialActive == null) {
            throw new IllegalArgumentException("initialActive must not be null");
        }
        this.active = initialActive;
        this.manifest = manifest;
        this.maxBytesPerSegment = maxBytesPerSegment;
        // 3/4 of capacity gives the manager a full quarter-segment of producer
        // runway before backpressure kicks in. Long math, no float, no alloc.
        this.signalAtBytes = (maxBytesPerSegment >> 2) * 3;
        this.nextSeq = initialActive.baseSeq() + initialActive.frameCount();
        this.publishedFsn = nextSeq - 1;
    }

    /**
     * Compatibility wrapper using the production facade. New startup code uses
     * {@link #recover(FilesFacade, String, long)} so EMPTY is explicit.
     */
    public static SegmentRing openExisting(String sfDir, long maxBytesPerSegment) {
        return openExisting(FilesFacade.INSTANCE, sfDir, maxBytesPerSegment);
    }

    /**
     * Facade-aware variant of {@link #openExisting(String, long)}: every
     * filesystem touch (enumeration, open, mmap, quarantine rename, manifest
     * I/O) goes through {@code filesFacade} so recovery's fail-closed behavior
     * can be fault-injected in tests. Returns {@code null} when the slot holds
     * nothing recoverable; throws {@link MmapSegmentException} when the slot's
     * state cannot be proven safe.
     */
    public static SegmentRing openExisting(FilesFacade filesFacade, String sfDir, long maxBytesPerSegment) {
        Recovery recovery = recover(filesFacade, sfDir, maxBytesPerSegment);
        return recovery.status == RecoveryStatus.EMPTY ? null : recovery.ring;
    }

    /**
     * Exhaustively discovers and validates an SF slot without mutating frame
     * data. Only after enumeration, opens, CRC scans, contiguity and manifest
     * boundaries all succeed does it migrate a legacy chain or discard
     * validated spares. Torn-tail residue is handled destroy-last:
     * {@link MmapSegment#openExisting} only observes it, and recovery zeroes
     * it exclusively where validation has proven it non-load-bearing -- the
     * resumed active's tail (about to be reclaimed by appends) and sealed
     * suffixes whose frame accounting validated complete against the chain.
     * A tear that cost frames (e.g. a mid-file tear in a sealed member)
     * fails closed BEFORE any of that, leaving every byte on disk for
     * operator extraction.
     */
    static Recovery recover(FilesFacade filesFacade, String sfDir, long maxBytesPerSegment) {
        return recover(filesFacade, sfDir, maxBytesPerSegment, null);
    }

    static Recovery recover(
            FilesFacade filesFacade,
            String sfDir,
            long maxBytesPerSegment,
            MembershipObserver membershipObserver
    ) {
        if (!filesFacade.exists(sfDir)) {
            return Recovery.empty();
        }
        ObjList<String> names = new ObjList<>();
        long find = filesFacade.findFirst(sfDir);
        if (find < 0) {
            throw new MmapSegmentException("could not enumerate SF directory " + sfDir);
        }
        if (find > 0) {
            int rc = 1;
            try {
                while (rc > 0) {
                    String name = Files.utf8ToString(filesFacade.findName(find));
                    if (name != null && name.endsWith(".sfa")) {
                        names.add(name);
                    }
                    rc = filesFacade.findNext(find);
                }
                if (rc < 0) {
                    throw new MmapSegmentException("could not fully enumerate SF directory " + sfDir);
                }
            } finally {
                filesFacade.findClose(find);
            }
        }

        ObjList<MmapSegment> all = new ObjList<>();
        // Files whose own bytes prove corruption (bad magic, sub-header size,
        // negative baseSeq). They are excluded from the chain and quarantined
        // to <name>.corrupt — but only AFTER the surviving chain validates (or
        // resolves to EMPTY). The precise invariant: a failed recovery never
        // mutates COMMITTED CHAIN BYTES. It is not "never mutates the slot"
        // -- several windows durably mutate before a later step can still
        // fail: proven-dead sealed residue is zeroed just before the
        // fail-closed first-sight throw in sanitizeSealedResidue; the
        // legacy-migration sanitize zeroes proven-dead residue before
        // SfManifest.create (or any later step) can fail; and validated-
        // extra cleanup plus corrupt-file quarantine run before the
        // active-sanitize barrier or ring construction can fail. Every such
        // window is confined to bytes the already-validated chain proves no
        // replay can ever need, or to preserve-by-rename quarantines that
        // keep the bytes on disk.
        // Whether a quarantined file was load-bearing is decided by the
        // manifest-boundary / contiguity checks below, not by the skip itself.
        // Operational open/stat/read/mmap errors, observed size instability,
        // and unsupported versions are NOT in this bucket: they throw the
        // plain MmapSegmentException type and abort recovery, because the
        // underlying file may be perfectly intact and silently dropping it
        // could lose durable frames.
        ObjList<String> corruptPaths = null;
        SfManifest manifest = null;
        try {
            for (int i = 0, n = names.size(); i < n; i++) {
                String path = sfDir + "/" + names.get(i);
                try {
                    all.add(MmapSegment.openExisting(filesFacade, path));
                } catch (MmapSegmentCorruptionException e) {
                    LOG.warn("recovery: {} is not a readable SF segment; excluding it and "
                            + "deferring quarantine until the surviving chain validates -- {}",
                            path, e.toString());
                    if (corruptPaths == null) {
                        corruptPaths = new ObjList<>();
                    }
                    corruptPaths.add(path);
                } catch (MmapSegmentException e) {
                    throw new MmapSegmentException("recovery failed for recognized segment " + path, e);
                }
            }
            manifest = SfManifest.open(filesFacade, sfDir);
            if (all.size() == 0) {
                if (corruptPaths != null) {
                    // Nothing valid survived. With a manifest this is still a
                    // hole we can prove (boundaries reference segments that are
                    // now unreadable) -- fail without mutating. Without one,
                    // legacy semantics apply: quarantine and start fresh.
                    if (manifest != null) {
                        throw new SfRecoveryException("every SF segment in " + sfDir
                                + " is corrupt but " + SfManifest.FILE_NAME
                                + " references durable data");
                    }
                    quarantineCorrupt(filesFacade, corruptPaths);
                    return Recovery.empty();
                }
                if (manifest != null) {
                    // No .sfa files at all. Only two protocols legitimately
                    // produce this, and both leave head == active: the
                    // close-time drain durably collapses the boundaries
                    // BEFORE its first unlink, and a fresh-start crash can
                    // leave a boundary-less (0,0) manifest behind. Uncollapsed
                    // boundaries therefore prove durable, never-declared-acked
                    // frames existed in [headBase, activeBase] whose files
                    // vanished outside the protocol (manual wipe, partial
                    // restore) -- fail closed and keep the manifest as
                    // evidence instead of silently starting fresh.
                    long manifestHeadBase = manifest.headBase();
                    long manifestActiveBase = manifest.activeBase();
                    if (manifestHeadBase != manifestActiveBase) {
                        throw new SfRecoveryException(SfManifest.FILE_NAME + " in " + sfDir
                                + " references durable data (headBase=" + manifestHeadBase
                                + ", activeBase=" + manifestActiveBase
                                + ") but no segment files exist");
                    }
                    // Collapsed boundaries: nothing recoverable exists, so
                    // accept EMPTY -- but shout, because a manual wipe of a
                    // fully-acked slot looks identical and the operator
                    // should know.
                    LOG.warn("SF manifest exists in {} with collapsed boundaries ({}) and "
                            + "no segment files (clean-drain or fresh-start crash window, or "
                            + "manual segment removal); discarding it and starting fresh",
                            sfDir, manifestActiveBase);
                    manifest.close();
                    manifest = null;
                    if (!SfManifest.removeFile(filesFacade, sfDir)) {
                        throw new MmapSegmentException(
                                "could not remove stale SF manifest in " + sfDir);
                    }
                }
                return Recovery.empty();
            }

            ObjList<MmapSegment> data = new ObjList<>();
            boolean requiresManifest = false;
            for (int i = 0, n = all.size(); i < n; i++) {
                MmapSegment segment = all.get(i);
                requiresManifest |= segment.manifestRequired();
                if (segment.frameCount() > 0) {
                    data.add(segment);
                }
            }
            sortByBaseSeq(data, 0, data.size());
            if (manifest == null && requiresManifest) {
                throw new SfRecoveryException("new-format SF segment exists but "
                        + SfManifest.FILE_NAME + " is missing");
            }

            MmapSegment active;
            ObjList<MmapSegment> chain = new ObjList<>();
            long headBase;
            long activeBase;
            if (manifest == null) {
                if (data.size() > 0) {
                    // Legacy migration is the one branch with no recorded boundary
                    // to check a set-aside file against: validateContiguous only
                    // compares SURVIVORS, so a file that sat BELOW data.get(0)
                    // leaves a contiguous chain, and the manifest this branch is
                    // about to create would record headBase as if that file's
                    // frames had been acked. They never were -- the ack seed
                    // derives from the head.
                    //
                    // A chain based at 0 needs no check: nothing can precede it,
                    // so a set-aside file of any kind was a stray or a spare. And
                    // a chain based above 0 is the NORMAL steady state for a
                    // long-lived legacy slot whose head was ack-trimmed, so the
                    // refusal has to key off positive evidence that a file was
                    // set aside during THIS recovery, never off the base alone.
                    long legacyChainStart = data.get(0).baseSeq();
                    if (legacyChainStart != 0L) {
                        if (corruptPaths != null) {
                            // A corrupt file's own baseSeq is unreadable (it is
                            // inside the bytes that failed), so nothing can place
                            // it above the chain head. Fail closed.
                            throw new SfRecoveryException("cannot migrate the legacy SF chain in "
                                    + sfDir + " based at " + legacyChainStart + ": a corrupt segment"
                                    + " of unknown identity could be its head, and no manifest"
                                    + " boundary exists to prove otherwise");
                        }
                        for (int i = 0, n = all.size(); i < n; i++) {
                            MmapSegment segment = all.get(i);
                            // A frameless segment carrying torn-tail residue is a
                            // segment whose frame[0] failed: bytes were written to
                            // it, so it is not an untouched spare. Its header IS
                            // readable, so its position is provable -- refuse only
                            // when it really sat below the chain head.
                            if (segment.frameCount() == 0
                                    && segment.tornTailBytes() > 0
                                    && segment.baseSeq() < legacyChainStart) {
                                throw new SfRecoveryException("cannot migrate the legacy SF chain in "
                                        + sfDir + " based at " + legacyChainStart + ": the segment at"
                                        + " base " + segment.baseSeq() + " lost its frames to a torn"
                                        + " write and sits below that head, so its range cannot be"
                                        + " shown already-acked");
                            }
                        }
                    }
                    validateContiguous(data);
                    for (int i = 0, n = data.size(); i < n; i++) {
                        chain.add(data.get(i));
                    }
                    active = chain.get(chain.size() - 1);
                    headBase = chain.get(0).baseSeq();
                    activeBase = active.baseSeq();
                    // Legacy sealed members carrying pre-manifest torn
                    // residue: contiguity just proved their frame accounting
                    // complete, so the residue is dead bytes. Zero it now
                    // (silently -- legacy slots predate the fail-closed
                    // sealed-suffix contract) so the migrated chain presents
                    // the all-zero sealed suffixes the manifest-era
                    // invariant requires.
                    sanitizeSealedResidue(chain, false);
                } else {
                    active = chooseEmptyInitial(all, sfDir);
                    if (active == null) {
                        // Legacy slot holding only empty leftovers, every one
                        // of them torn (a clean empty would have been chosen).
                        // Nothing recoverable: quarantine the torn evidence,
                        // drop the clean debris, start fresh -- exactly the
                        // pre-manifest behavior.
                        for (int i = 0, n = all.size(); i < n; i++) {
                            MmapSegment segment = all.get(i);
                            String path = segment.path();
                            long torn = segment.tornTailBytes();
                            segment.close();
                            if (torn > 0) {
                                quarantineFile(filesFacade, path);
                            } else if (!filesFacade.remove(path)) {
                                LOG.warn("could not remove empty SF leftover {}", path);
                            }
                        }
                        all.clear();
                        quarantineCorrupt(filesFacade, corruptPaths);
                        return Recovery.empty();
                    }
                    chain.add(active);
                    headBase = active.baseSeq();
                    activeBase = active.baseSeq();
                }
                manifest = SfManifest.create(filesFacade, sfDir, headBase, activeBase);
                for (int i = 0, n = chain.size(); i < n; i++) {
                    chain.get(i).markManifestRequired();
                }
            } else {
                headBase = manifest.headBase();
                activeBase = manifest.activeBase();
                for (int i = 0, n = data.size(); i < n; i++) {
                    MmapSegment segment = data.get(i);
                    long end = segment.baseSeq() + segment.frameCount();
                    if (segment.baseSeq() < headBase) {
                        if (end > headBase) {
                            throw new SfRecoveryException("segment overlaps committed SF head boundary");
                        }
                        continue; // acknowledged stale file after manifest-before-unlink crash
                    }
                    if (segment.baseSeq() > activeBase) {
                        throw new SfRecoveryException("segment exists beyond committed SF active boundary");
                    }
                    chain.add(segment);
                }
                if (chain.size() > 0) {
                    validateContiguous(chain);
                    if (chain.get(0).baseSeq() != headBase) {
                        throw new SfRecoveryException("missing expected SF head segment at base " + headBase);
                    }
                }
                active = findActive(all, activeBase);
                if (active == null) {
                    if (chain.size() == 0 && headBase == activeBase && corruptPaths == null) {
                        // Clean-drain crash window: the close-time drain first
                        // durably collapses the boundaries to head == active
                        // (declaring every frame acked), then unlinks segments
                        // in ascending order -- so dying between the active's
                        // unlink and the spare's/manifest's leaves exactly
                        // this state: no data frame anywhere, no file at the
                        // committed active base, only empty spares and/or
                        // acked stale files. Nothing recoverable exists;
                        // accept EMPTY and clear the debris. Guarded on
                        // corruptPaths because an unreadable .sfa of unknown
                        // identity could be the real active -- in that case
                        // fail closed instead of guessing.
                        LOG.warn("SF manifest in {} has collapsed boundaries ({}) with no "
                                + "segment at the active base and no recovered frames; "
                                + "accepting the clean-drain crash window as empty",
                                sfDir, activeBase);
                        for (int i = 0, n = all.size(); i < n; i++) {
                            MmapSegment segment = all.get(i);
                            String path = segment.path();
                            long torn = segment.tornTailBytes();
                            segment.close();
                            if (torn > 0) {
                                // Preserve, not destroy: on success the bytes move to
                                // .corrupt; if even the rename fails, they stay under
                                // their original name instead (quarantineFile only
                                // logs a warning on a failed rename, so a torn,
                                // data-bearing leftover really can remain a plain
                                // .sfa file here). Either way the file survives for
                                // the NEXT recovery to re-examine -- and it is THAT
                                // recovery's FSN boundary checks, not the name, that
                                // keep it from being mistaken for this generation's:
                                // a base above activeBase throws ("segment exists
                                // beyond committed SF active boundary"), a base
                                // colliding with the new chain fails
                                // validateContiguous's expected-next-base check, and
                                // a base wholly below headBase is excluded from the
                                // chain and quarantined again as a non-retained extra.
                                quarantineFile(filesFacade, path);
                            } else if (!filesFacade.remove(path)) {
                                // Returning EMPTY here makes the caller start fresh at
                                // baseSeq 0 in a directory that still holds a prior
                                // generation's file. Their FSN ranges then overlap while
                                // their symbol ids describe different strings, and no
                                // downstream guard can see it: contiguity and the
                                // manifest boundaries both reason about FSNs, and both
                                // generations number theirs from the same origin. Refuse
                                // the slot so the state stays representable-by-refusal
                                // rather than silently mixed.
                                //
                                // Operational, not terminal: an unlink can fail for a
                                // transient reason (a share lock, an antivirus or backup
                                // handle), exactly as the manifest unlink below can. The
                                // plain type aborts startup for a retry instead of
                                // quarantining a slot whose bytes may be perfectly intact.
                                throw new MmapSegmentException("could not remove drained SF leftover "
                                        + path + "; refusing to start fresh in a slot that still"
                                        + " holds a prior generation's segment");
                            }
                        }
                        all.clear();
                        manifest.close();
                        manifest = null;
                        if (!SfManifest.removeFile(filesFacade, sfDir)) {
                            throw new MmapSegmentException(
                                    "could not remove stale SF manifest in " + sfDir);
                        }
                        return Recovery.empty();
                    }
                    throw new SfRecoveryException("missing expected SF active segment at base " + activeBase);
                }
                if (chain.size() == 0) {
                    if (headBase != activeBase || active.frameCount() != 0 || corruptPaths != null) {
                        // corruptPaths guard: with an unreadable .sfa in the
                        // slot, the innocent-looking empty at the active base
                        // could be a leftover spare coincidentally carrying
                        // the same provisional baseSeq as a corrupted real
                        // active -- accepting it would quarantine unacked
                        // frames and re-issue their FSNs. Fail closed.
                        throw new SfRecoveryException(
                                "missing SF chain between committed boundaries"
                                        + (corruptPaths != null
                                        ? " (a corrupt segment prevents proving the empty state)" : ""));
                    }
                    chain.add(active);
                } else if (chain.get(chain.size() - 1) != active) {
                    MmapSegment tail = chain.get(chain.size() - 1);
                    long chainEnd = tail.baseSeq() + tail.frameCount();
                    if (corruptPaths == null && active.frameCount() == 0 && active.baseSeq() == chainEnd) {
                        // Rotation committed (manifest fsync'd, promoted spare's
                        // header synced) but the process/OS died before a single
                        // frame of the new active reached disk: the sealed chain
                        // ends exactly where the empty active begins. Legal
                        // crash state -- resume appending into it. Refused when
                        // corrupt segments exist (same stand-in hazard as the
                        // empty-chain acceptance above).
                        chain.add(active);
                    } else {
                        throw new SfRecoveryException(
                                "missing expected SF active/tail segment at base " + activeBase);
                    }
                }
                // Sealed members must present an all-zero suffix: fresh
                // segments are zero-allocated and this recovery sanitizes
                // the resumed active before it takes appends, so a reseal
                // can never carry residue forward. Reaching this point
                // proves every sealed member's frame accounting is complete
                // (validateContiguous plus the head/active boundary matching
                // above), so any suffix residue here is dead bytes -- legacy
                // pre-sanitization poison or an out-of-protocol scribble
                // past the last frame, never lost frames. Zero it durably,
                // then still fail closed on first sight so the incident is
                // surfaced; the restart then proves the chain clean. A tear
                // that DID cost frames never gets here: the contiguity or
                // boundary checks above throw first, with every byte left
                // on disk for operator extraction.
                sanitizeSealedResidue(chain, true);
                for (int i = 0, n = chain.size(); i < n; i++) {
                    chain.get(i).markManifestRequired();
                }
            }

            // Build membership and clean validated extras while every mmap and
            // the manifest still have one local owner. In particular, an OOM
            // while allocating the identity map falls through to the outer
            // failure cleanup instead of stranding extras after transfer.
            RetainedSegmentMembership retained = newDefaultMembership(chain, membershipObserver);
            for (int i = 0, n = all.size(); i < n; i++) {
                MmapSegment segment = all.get(i);
                if (!retained.contains(segment)) {
                    String path = null;
                    long torn = 0;
                    Throwable inspectionError = null;
                    try {
                        path = segment.path();
                        torn = segment.tornTailBytes();
                    } catch (Throwable error) {
                        inspectionError = error;
                    }
                    // A close failure is not best-effort: keep local ownership
                    // and fail through the outer cleanup rather than transfer a
                    // ring while an extra may still own an mmap or fd.
                    segment.close();
                    all.setQuick(i, null);
                    if (inspectionError != null) {
                        warnPostRecoveryCleanupFailure(inspectionError);
                        continue;
                    }
                    try {
                        cleanupClosedExtra(filesFacade, path, torn);
                    } catch (Throwable cleanupError) {
                        warnPostRecoveryCleanupFailure(cleanupError);
                    }
                }
            }
            quarantineCorrupt(filesFacade, corruptPaths);

            // The resumed active is the only segment that takes appends and
            // the only one a later rotation reseals, so recovery zeroes ITS
            // residue exactly here -- every chain/manifest check has passed
            // and the ring is about to be exposed. Resumed appends restart
            // at lastGood but stop wherever the last payload fits, so
            // unzeroed residue would survive a seal-via-rotation (bricking
            // the next startup's sealed-suffix check) and a byte-aligned
            // stale frame with a valid CRC could be resurrected at a
            // recycled FSN by a later scan. The durable barrier inside
            // sanitizeTornTail is load-bearing in MEMORY durability mode,
            // where rotation does not sync the sealed predecessor's data
            // pages. A failed barrier aborts recovery (fail closed); the
            // retry re-observes the same residue because openExisting never
            // mutates. Unlike sealed suffixes, the active's residue is NOT
            // proven dead -- past a mid-file tear it can hold valid-CRC
            // frames of real unacked payloads. Zeroing is policy: replay
            // cannot cross the tear, and preserving the bytes would trade
            // the two hazards above for data no recovery path can use (see
            // sanitizeTornTail).
            active.sanitizeTornTail();
            for (int i = 1, n = chain.size(); i < n; i++) {
                chain.get(i - 1).linkSuccessor(chain.get(i));
            }
            SegmentRing ring = new SegmentRing(active, maxBytesPerSegment, manifest);
            for (int i = 0, n = chain.size() - 1; i < n; i++) {
                ring.sealedSegments.add(chain.get(i));
            }
            // Allocate the return wrapper before transfer. Until this succeeds,
            // the outer catch remains the sole owner and can close all segments
            // and the manifest if construction or sealed-list growth fails.
            Recovery recovery = Recovery.recovered(ring);
            manifest = null;
            all.clear();
            return recovery;
        } catch (Throwable t) {
            for (int i = 0, n = all.size(); i < n; i++) {
                MmapSegment segment = all.get(i);
                if (segment != null) {
                    try {
                        segment.close();
                    } catch (Throwable closeError) {
                        warnRecoveryCloseFailure(closeError);
                    }
                }
            }
            if (manifest != null) {
                try {
                    manifest.close();
                } catch (Throwable closeError) {
                    warnRecoveryCloseFailure(closeError);
                }
            }
            throw t;
        }
    }

    /**
     * Durably advances the manifest head past {@code trimming} (the LAST
     * sealed segment of the bounded batch the manager is about to unlink).
     * One durable commit covers every earlier batch member: head values are
     * segment boundaries and the batch is a contiguous prefix of the sealed
     * chain, so recovery discards each member as "stale below head"
     * regardless of how far the unlink loop got. The successor and the
     * current active are both read under the ring monitor, so a concurrent
     * rotation (which also mutates the manifest under this monitor) can never
     * make the head leapfrog a still-live sealed segment: if rotation sealed
     * the old active after the caller's snapshot, {@code trimming.successor()}
     * now points at that sealed segment, not at the new active.
     */
    private synchronized void advanceManifestHeadPast(MmapSegment trimming) {
        if (manifest == null) {
            return;
        }
        MmapSegment successor = trimming.successor();
        long newHeadBase = (successor == null || successor == active)
                ? active.baseSeq()
                : successor.baseSeq();
        manifest.update(newHeadBase, active.baseSeq());
    }

    /**
     * Picks the clean (untorn) empty segment to reuse as a legacy slot's
     * initial active, preferring {@code sf-initial.sfa}. Returns {@code null}
     * when no clean empty exists; torn empties are never reused here because
     * their bytes are quarantine evidence, not blank space.
     */
    private static MmapSegment chooseEmptyInitial(ObjList<MmapSegment> all, String sfDir) {
        String initialPath = sfDir + "/sf-initial.sfa";
        MmapSegment selected = null;
        for (int i = 0, n = all.size(); i < n; i++) {
            MmapSegment segment = all.get(i);
            if (segment.frameCount() != 0 || segment.tornTailBytes() > 0) {
                continue;
            }
            if (selected == null || initialPath.equals(segment.path())) {
                selected = segment;
            }
        }
        return selected;
    }

    private static void cleanupClosedExtra(FilesFacade filesFacade, String path, long torn) {
        if (torn > 0) {
            quarantineFile(filesFacade, path);
        } else if (!filesFacade.remove(path)) {
            LOG.warn("could not remove validated stale/empty SF segment {}", path);
        }
    }

    private static RetainedSegmentMembership newDefaultMembership(
            final ObjList<MmapSegment> chain,
            final MembershipObserver observer
    ) {
        if (observer != null) {
            observer.beforeMembershipAllocation();
        }
        if (DEFAULT_MEMBERSHIP_MODE == RetainedSegmentMembershipMode.LINEAR) {
            if (observer == null) {
                return segment -> {
                    for (int i = 0, n = chain.size(); i < n; i++) {
                        if (chain.get(i) == segment) {
                            return true;
                        }
                    }
                    return false;
                };
            }
            return segment -> {
                for (int i = 0, n = chain.size(); i < n; i++) {
                    observer.onMembershipOperation();
                    if (chain.get(i) == segment) {
                        return true;
                    }
                }
                return false;
            };
        }

        final IdentityHashMap<MmapSegment, Boolean> retained = new IdentityHashMap<>(chain.size());
        for (int i = 0, n = chain.size(); i < n; i++) {
            retained.put(chain.get(i), Boolean.TRUE);
        }
        if (observer == null) {
            return retained::containsKey;
        }
        return segment -> {
            observer.onMembershipOperation();
            return retained.containsKey(segment);
        };
    }

    /** Renames every collected corrupt path to {@code <path>.corrupt}, best-effort. */
    private static void quarantineCorrupt(FilesFacade filesFacade, ObjList<String> corruptPaths) {
        if (corruptPaths == null) {
            return;
        }
        for (int i = 0, n = corruptPaths.size(); i < n; i++) {
            try {
                quarantineFile(filesFacade, corruptPaths.get(i));
            } catch (Throwable cleanupError) {
                warnPostRecoveryCleanupFailure(cleanupError);
            }
        }
    }

    private static void quarantineFile(FilesFacade filesFacade, String path) {
        if (filesFacade.rename(path, path + ".corrupt") != 0) {
            LOG.warn("could not quarantine {} to {}.corrupt; it will be re-examined "
                    + "on the next recovery", path, path);
        }
    }

    /**
     * Locates the segment the manifest's {@code activeBase} refers to.
     * Preference order among same-base candidates:
     * <ol>
     *   <li>a segment with recovered frames (the durable chain tail);</li>
     *   <li>an empty segment with a torn tail (the promoted active whose
     *       first frame write was cut short — an attempted write marks it as
     *       the one rotation actually exposed);</li>
     *   <li>a clean empty segment.</li>
     * </ol>
     * Multiple equivalent empties at the same base are NOT an error: a fresh
     * start or a rotation crash routinely leaves both the initial/promoted
     * segment and a provisioned hot spare carrying the same provisional
     * baseSeq. They are interchangeable blanks — pick one deterministically
     * and let the extras cleanup discard the rest. Bricking startup on this
     * state would turn every "kill -9 shortly after start" into a manual
     * repair.
     */
    private static MmapSegment findActive(ObjList<MmapSegment> all, long activeBase) {
        MmapSegment tornEmpty = null;
        MmapSegment cleanEmpty = null;
        for (int i = 0, n = all.size(); i < n; i++) {
            MmapSegment segment = all.get(i);
            if (segment.baseSeq() != activeBase) {
                continue;
            }
            if (segment.frameCount() > 0) {
                return segment;
            }
            if (segment.tornTailBytes() > 0) {
                if (tornEmpty == null) {
                    tornEmpty = segment;
                }
            } else if (cleanEmpty == null) {
                cleanEmpty = segment;
            }
        }
        return tornEmpty != null ? tornEmpty : cleanEmpty;
    }

    private static void validateContiguous(ObjList<MmapSegment> segments) {
        for (int i = 1, n = segments.size(); i < n; i++) {
            MmapSegment previous = segments.get(i - 1);
            MmapSegment current = segments.get(i);
            long expected = previous.baseSeq() + previous.frameCount();
            if (current.baseSeq() != expected) {
                throw new SfRecoveryException("FSN gap in recovered segments: expected "
                        + expected + " but got " + current.baseSeq());
            }
        }
    }

    /**
     * Durably zeroes torn-tail residue on the chain's sealed members (every
     * element but the last, which is the active). Callers must have already
     * proven each sealed member's frame accounting complete -- contiguity
     * plus head/active boundary matching -- which is exactly what makes the
     * residue provably dead: a tear that cost frames breaks those checks and
     * fails recovery before any mutation, preserving the bytes (potentially
     * the only copy of unreachable valid-CRC frames) for operator
     * extraction. With {@code failClosedOnSight} the incident is still
     * surfaced as a first-sight {@link SfSanitizedResidueException} after
     * sanitizing: the residue is already durably zeroed when it propagates,
     * so a retry proves the chain clean (attended callers get that via
     * restart; unattended callers key off the distinct type to retry
     * instead of quarantining a just-healed slot). Without the flag the
     * chain proceeds immediately (legacy migration, which predates the
     * sealed-suffix contract).
     */
    private static void sanitizeSealedResidue(ObjList<MmapSegment> chain, boolean failClosedOnSight) {
        String firstTornPath = null;
        for (int i = 0, n = chain.size() - 1; i < n; i++) {
            MmapSegment sealed = chain.get(i);
            if (sealed.tornTailBytes() > 0) {
                sealed.sanitizeTornTail();
                if (firstTornPath == null) {
                    firstTornPath = sealed.path();
                }
            }
        }
        if (failClosedOnSight && firstTornPath != null) {
            throw new SfSanitizedResidueException("corrupt torn tail in sealed SF segment " + firstTornPath);
        }
    }

    private static void warnPostRecoveryCleanupFailure(Throwable cleanupError) {
        try {
            LOG.warn("post-recovery cleanup failed; leftover files will be "
                    + "re-examined on the next startup", cleanupError);
        } catch (Throwable ignored) {
            // Cleanup diagnostics must not invalidate a recovered ring.
        }
    }

    private static void warnRecoveryCloseFailure(Throwable closeError) {
        try {
            LOG.warn("error closing SF resource after recovery failure", closeError);
        } catch (Throwable ignored) {
            // Preserve the original recovery failure and continue closing.
        }
    }

    interface MembershipObserver {
        void beforeMembershipAllocation();

        void onMembershipOperation();
    }

    static final class Recovery {
        private final SegmentRing ring;
        private final RecoveryStatus status;

        private Recovery(RecoveryStatus status, SegmentRing ring) {
            this.status = status;
            this.ring = ring;
        }

        static Recovery empty() {
            return new Recovery(RecoveryStatus.EMPTY, null);
        }

        SegmentRing ring() {
            return ring;
        }

        static Recovery recovered(SegmentRing ring) {
            return new Recovery(RecoveryStatus.RECOVERED, ring);
        }

        RecoveryStatus status() {
            return status;
        }
    }

    enum RecoveryStatus {
        EMPTY,
        RECOVERED
    }

    interface RetainedSegmentMembership {
        boolean contains(MmapSegment segment);
    }

    private enum RetainedSegmentMembershipMode {
        IDENTITY,
        LINEAR
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
        checkDurability();
        long offset = active.tryAppend(payloadAddr, payloadLen);
        if (offset == -1L) {
            // Active is full. Try to rotate.
            MmapSegment spare = hotSpare;
            if (spare == null) {
                return BACKPRESSURE_NO_SPARE;
            }
            // Periodic mode must make the predecessor's complete published
            // range durable before the manifest can name its successor. The
            // manager performs the barrier; the producer uses the existing
            // backpressure path while it waits.
            MmapSegment previous = active;
            if (requestSyncBeforeRotation(previous)) {
                wakeManager();
                return BACKPRESSURE_NO_SPARE;
            }
            // Pin the spare's baseSeq to whatever the active's nextSeq actually
            // is right now. This is the right moment because (a) the active is
            // full, so its frameCount is stable, and (b) the spare hasn't been
            // appended to yet (rebaseSeq enforces that). The segment manager's
            // earlier guess at baseSeq is irrelevant.
            long actualBase = previous.baseSeq() + previous.frameCount();
            spare.rebaseSeq(actualBase);
            if (manifest != null) {
                // Make the spare's rebased identity durable BEFORE the manifest
                // references it. Without this barrier an OS crash could leave a
                // durable manifest pointing at baseSeq=actualBase while the
                // spare's on-disk header still carries the manager's
                // provisional guess -- recovery would then find no segment at
                // the committed active boundary and fail a startup that lost
                // nothing. One msync per rotation, amortized over a whole
                // segment of appends; runs outside the monitor because the
                // spare is not yet visible to any other thread.
                //
                // MEMORY mode deliberately does not sync the predecessor's
                // data pages. PERIODIC mode reached this point only after the
                // manager covered the predecessor's complete published range.
                spare.syncHeader();
            }
            // Publish the successor before the volatile active promotion. The
            // same monitor protects the sealed list and nextSealedAfter's trim
            // fallback, while the volatile link also remains readable from a
            // current segment after the manager removes and closes it.
            synchronized (this) {
                if (manifest != null) {
                    // Inside the monitor: serialized with the trim path's
                    // advanceManifestHeadPast so neither writer publishes a
                    // boundary computed from a state the other has already
                    // moved past (SfManifest additionally clamps monotonic).
                    // BEFORE any ring mutation: if the manifest fsync throws,
                    // the rotation never happened -- previous stays active,
                    // the spare stays installed, and the producer's retry
                    // re-runs this block from a consistent state.
                    long headBase = sealedHead < sealedSegments.size()
                            ? sealedSegments.get(sealedHead).baseSeq()
                            : previous.baseSeq();
                    manifest.update(headBase, actualBase);
                }
                previous.linkSuccessor(spare);
                sealedSegments.add(previous);
                active = spare;
            }
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

    public void checkDurability() {
        MmapSegmentException failure = durabilityFailure;
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Clears a latched periodic data-barrier failure. Called by the segment
     * manager after a subsequent periodic sync pass over every live segment
     * succeeds: the published range the failed barrier was meant to cover is
     * durable now, so the producer may resume. The monitor serializes the
     * clear with {@link #recordDurabilityFailure(Throwable)}; the volatile
     * write publishes it to producer threads calling {@link #checkDurability()}.
     */
    void clearDurabilityFailure() {
        if (durabilityFailure != null) {
            synchronized (this) {
                durabilityFailure = null;
            }
        }
    }

    synchronized void clearSyncRequestIfActiveDurable() {
        if (active != null && active.isPublishedDurable()) {
            syncRequested = false;
        }
    }

    synchronized void copyLiveSegmentsForSync(ObjList<MmapSegment> target) {
        target.clear();
        for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
            target.add(sealedSegments.get(i));
        }
        if (active != null) {
            target.add(active);
        }
    }

    /**
     * Copies the live segments that may still need a durability barrier: every
     * sealed segment from the {@link #firstNonDurableSealed} frontier onward,
     * plus the active segment. First advances the frontier past any sealed
     * segments an earlier pass (or rotation's pre-seal barrier) has since made
     * durable. Used by the periodic sync path in place of
     * {@link #copyLiveSegmentsForSync}: the proven-durable prefix would
     * otherwise be re-copied under this monitor and re-scanned every tick as
     * no-op {@link MmapSegment#syncPublished()} early-returns -- O(live-sealed)
     * work that grows without bound under a producer-outpaces-drain backlog.
     * The frontier is a conservative lower bound (it only ever advances past
     * segments observed durable, and durability never regresses), so this can
     * never skip a segment that still needs a barrier.
     */
    synchronized void copyPendingSyncSegments(ObjList<MmapSegment> target) {
        target.clear();
        // Invariant maintained by every mutation site (rotation append, trim's
        // removeSealedHead, compaction shift, close). A frontier that drifted
        // above size would silently skip un-fsynced segments, so guard it in
        // tests; the clamp below keeps production safe if it is ever violated.
        assert firstNonDurableSealed >= sealedHead && firstNonDurableSealed <= sealedSegments.size()
                : "durability frontier out of range: firstNonDurableSealed=" + firstNonDurableSealed
                + " sealedHead=" + sealedHead + " size=" + sealedSegments.size();
        int i = firstNonDurableSealed;
        if (i < sealedHead) {
            i = sealedHead;
        }
        int n = sealedSegments.size();
        while (i < n && sealedSegments.get(i).isPublishedDurable()) {
            i++;
        }
        firstNonDurableSealed = i;
        for (; i < n; i++) {
            target.add(sealedSegments.get(i));
        }
        if (active != null) {
            target.add(active);
        }
    }

    void enablePeriodicSync() {
        periodicSyncEnabled = true;
        syncRequested = true;
    }

    boolean isSyncRequested() {
        return syncRequested;
    }

    void recordDurabilityFailure(Throwable failure) {
        if (durabilityFailure == null) {
            MmapSegmentException wrapped = failure instanceof MmapSegmentException
                    ? (MmapSegmentException) failure
                    : new MmapSegmentException("periodic SF data sync failed", failure);
            synchronized (this) {
                if (durabilityFailure == null) {
                    durabilityFailure = wrapped;
                }
            }
        }
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
        ioPinnedSegment = null;
        if (active != null) {
            active.close();
            active = null;
        }
        if (hotSpare != null) {
            hotSpare.close();
            hotSpare = null;
        }
        for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
            sealedSegments.get(i).close();
        }
        sealedSegments.clear();
        sealedHead = 0;
        firstNonDurableSealed = 0;
        for (int i = 0, n = pendingTrims.size(); i < n; i++) {
            pendingTrims.getQuick(i).close();
        }
        pendingTrims.clear();
        if (manifest != null) {
            manifest.close();
        }
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
        while (sealedHead < sealedSegments.size()) {
            MmapSegment s = sealedSegments.get(sealedHead);
            if (s == ioPinnedSegment) {
                break;
            }
            long lastSeq = s.baseSeq() + s.frameCount() - 1;
            if (lastSeq > acked) {
                break;
            }
            if (out == null) {
                out = new ObjList<>();
            }
            out.add(s);
            removeSealedHead();
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
     * Binary-searches the sealed list, then falls back to the active
     * segment: O(log live-sealed) per call, so repositioning stays cheap
     * even when a producer-outpaces-drain backlog has grown the sealed
     * list without bound.
     */
    public synchronized MmapSegment findSegmentContaining(long fsn) {
        return findSegmentContaining0(fsn);
    }

    /**
     * Atomically finds and pins the segment containing {@code fsn}. The pin
     * remains until the I/O cursor switches or releases it, preventing trim
     * staging from hiding or unmapping the returned segment meanwhile.
     */
    synchronized MmapSegment pinSegmentContaining(long fsn) {
        MmapSegment segment = findSegmentContaining0(fsn);
        if (segment != null) {
            switchIoPin(segment);
        }
        return segment;
    }

    /**
     * Oldest sealed segment, or {@code null} if the sealed list is empty.
     * Used by the I/O loop's "current was trimmed out from under us"
     * fallback -- see {@link #nextSealedAfter(MmapSegment)}.
     */
    public synchronized MmapSegment firstSealed() {
        return sealedHead < sealedSegments.size() ? sealedSegments.get(sealedHead) : null;
    }

    /**
     * Returns the oldest fully acknowledged, unpinned live segment without
     * removing it. Staging later transfers it to hidden ring-owned cleanup
     * state, where unlink/barrier failures remain retryable without exposing
     * an unmapped segment to traversal.
     */
    public synchronized MmapSegment firstTrimmable() {
        if (sealedHead == sealedSegments.size()) {
            return null;
        }
        MmapSegment segment = sealedSegments.get(sealedHead);
        if (segment == ioPinnedSegment) {
            return null;
        }
        long lastSeq = segment.baseSeq() + segment.frameCount() - 1;
        return lastSeq <= ackedFsn ? segment : null;
    }

    /**
     * Performs the one ordered recovery fold across sealed segments and the
     * active segment. The returned native suffix remains owned by the caller.
     * <p>
     * Replaces the former {@code findLastFsnWithoutPayloadFlag} walk: the
     * deferred-commit tail scan is one of several verdicts the single fold now
     * produces, so the ring is walked once rather than once per question. Call
     * before the I/O loop and producer start appending; the walk is not
     * synchronized against appends into the active segment.
     */
    synchronized RecoveredFrameAnalysis analyzeRecovery(int symbolBaseline) {
        RecoveredFrameAnalysis analysis = new RecoveredFrameAnalysis(symbolBaseline, ackedFsn);
        try {
            for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
                sealedSegments.get(i).scanRecovery(analysis);
            }
            active.scanRecovery(analysis);
            analysis.finish();
            return analysis;
        } catch (Throwable t) {
            analysis.close();
            throw t;
        }
    }

    public MmapSegment getActive() {
        return active;
    }

    /** Atomically pins and returns the current active segment for I/O. */
    synchronized MmapSegment pinActiveSegment() {
        MmapSegment segment = active;
        switchIoPin(segment);
        return segment;
    }

    /**
     * Direct view of sealed segments (oldest first). NOT thread-safe -- use
     * only from the producer thread, or alongside a lock that excludes
     * concurrent rotation. Cross-thread readers (typically the I/O loop)
     * should use {@link #snapshotSealedSegments(MmapSegment[])} instead.
     */
    public synchronized ObjList<MmapSegment> getSealedSegments() {
        compactSealedSegments();
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

    /** True when the segment manager should prepare and install a fresh spare. */
    public boolean needsHotSpare() {
        return hotSpare == null;
    }

    /**
     * Atomically advances the I/O pin from {@code current} to the next live
     * sealed segment, or to the active segment when no sealed successor
     * remains. An active cursor stays pinned in place until rotation seals it.
     */
    synchronized MmapSegment advancePinnedSegment(MmapSegment current) {
        assert ioPinnedSegment == current;
        MmapSegment liveActive = active;
        if (current == liveActive) {
            return current;
        }
        MmapSegment next = nextSealedAfter(current);
        if (next == null) {
            MmapSegment first = sealedHead < sealedSegments.size()
                    ? sealedSegments.get(sealedHead)
                    : null;
            next = first != null && first.baseSeq() > current.baseSeq()
                    ? first
                    : liveActive;
        }
        switchIoPin(next);
        return next;
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
     * Each segment publishes its successor once, before rotation exposes that
     * successor as active. A constant-time head check detects when trimming
     * removed the immediate successor and falls forward to the oldest live
     * sealed segment. Synchronized against rotation and head removal.
     */
    public synchronized MmapSegment nextSealedAfter(MmapSegment current) {
        nextSealedComparisons++;
        MmapSegment successor = current.successor();
        if (successor == null) {
            return null;
        }
        if (successor == active) {
            return null;
        }
        MmapSegment first = sealedHead < sealedSegments.size() ? sealedSegments.get(sealedHead) : null;
        if (first != null && successor.baseSeq() >= first.baseSeq()) {
            return successor;
        }
        // Head trimming may have removed the immediate successor while the
        // I/O cursor still held an older segment. Trims only remove a prefix,
        // so the current head is the first live segment after that prefix.
        return first;
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
     * Drops a directory-durable prefix from hidden trim ownership and returns
     * its exact byte total for manager accounting.
     */
    synchronized long commitPendingTrims(MmapSegment[] expected, int count) {
        if (count < 0 || count > pendingTrims.size()) {
            throw new IllegalArgumentException("invalid pending trim count: " + count);
        }
        long bytes = 0;
        for (int i = 0; i < count; i++) {
            MmapSegment segment = pendingTrims.getQuick(i);
            if (segment != expected[i]) {
                throw new IllegalStateException("pending trim prefix changed");
            }
            bytes += segment.sizeBytes();
        }
        if (count == pendingTrims.size()) {
            pendingTrims.clear();
        } else if (count > 0) {
            pendingTrims.remove(0, count - 1);
        }
        return bytes;
    }

    /** Copies the hidden retry batch into manager-thread scratch storage. */
    synchronized int copyPendingTrims(MmapSegment[] target) {
        int count = pendingTrims.size();
        if (target.length < count) {
            throw new IllegalArgumentException("pending trim target is too small");
        }
        for (int i = 0; i < count; i++) {
            target[i] = pendingTrims.getQuick(i);
        }
        return count;
    }

    /** Number of hidden trim entries retained for retry. */
    synchronized int pendingTrimCount() {
        return pendingTrims.size();
    }

    @TestOnly
    public synchronized MmapSegment getHotSpareForTesting() {
        return hotSpare;
    }

    @TestOnly
    public synchronized int getPendingTrimCount() {
        return pendingTrims.size();
    }

    /**
     * Number of live segments the periodic path would barrier this tick,
     * advancing the durability frontier exactly as a real tick does. In the
     * steady state (every sealed segment proven durable) this collapses to 1
     * -- the active segment -- proving the proven-durable sealed prefix is no
     * longer copied/scanned under the monitor.
     */
    @TestOnly
    public synchronized int pendingSyncSegmentCountForTest() {
        ObjList<MmapSegment> scratch = new ObjList<>();
        copyPendingSyncSegments(scratch);
        return scratch.size();
    }

    @TestOnly
    public synchronized MmapSegment pinSegmentContainingForTest(long fsn) {
        return pinSegmentContaining(fsn);
    }

    @TestOnly
    public void recordDurabilityFailureForTesting(Throwable failure) {
        recordDurabilityFailure(failure);
    }

    @TestOnly
    public synchronized void releasePinnedSegmentForTest(MmapSegment expected) {
        releasePinnedSegment(expected);
    }

    private synchronized boolean requestSyncBeforeRotation(MmapSegment previous) {
        if (periodicSyncEnabled && !previous.isPublishedDurable()) {
            syncRequested = true;
            return true;
        }
        return false;
    }

    /** Releases the I/O cursor pin and wakes trim if it still names {@code expected}. */
    synchronized void releasePinnedSegment(MmapSegment expected) {
        if (ioPinnedSegment == expected) {
            ioPinnedSegment = null;
            wakeManager();
        }
    }

    /**
     * Commits removal of the segment returned by {@link #firstTrimmable()}.
     * Returns false if concurrent lifecycle activity changed the head.
     */
    public synchronized boolean removeTrimmable(MmapSegment segment) {
        if (sealedHead == sealedSegments.size()
                || sealedSegments.get(sealedHead) != segment
                || segment == ioPinnedSegment) {
            return false;
        }
        long lastSeq = segment.baseSeq() + segment.frameCount() - 1;
        if (lastSeq > ackedFsn) {
            return false;
        }
        removeSealedHead();
        return true;
    }

    /**
     * Durably advances the manifest past, then hides, one bounded ACKed and
     * unpinned sealed prefix. Once this returns, I/O lookup cannot discover
     * any staged segment; the manager owns only the physical cleanup attempt.
     */
    synchronized int stagePendingTrims(MmapSegment[] target, int maxCount, long coveredAck) {
        if (pendingTrims.size() != 0) {
            return 0;
        }
        if (maxCount < 0 || maxCount > MAX_PENDING_TRIMS || target.length < maxCount) {
            throw new IllegalArgumentException("invalid trim batch size: " + maxCount);
        }
        int count = 0;
        for (int i = sealedHead, n = sealedSegments.size(); i < n && count < maxCount; i++) {
            MmapSegment segment = sealedSegments.get(i);
            long lastSeq = segment.baseSeq() + segment.frameCount() - 1L;
            if (segment == ioPinnedSegment || lastSeq > coveredAck) {
                break;
            }
            target[count++] = segment;
        }
        if (count == 0) {
            return 0;
        }
        try {
            advanceManifestHeadPast(target[count - 1]);
        } catch (Throwable t) {
            for (int i = 0; i < count; i++) {
                target[i] = null;
            }
            throw t;
        }
        for (int i = 0; i < count; i++) {
            MmapSegment segment = target[i];
            assert sealedSegments.get(sealedHead) == segment;
            pendingTrims.add(segment);
            removeSealedHead();
        }
        return count;
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
        int n = sealedSegments.size() - sealedHead;
        int copyCount = Math.min(n, target.length);
        for (int i = 0; i < copyCount; i++) {
            target[i] = sealedSegments.get(sealedHead + i);
        }
        return n > target.length ? -1 : n;
    }

    public void syncAllLiveSegments() {
        ObjList<MmapSegment> segments = new ObjList<>();
        copyLiveSegmentsForSync(segments);
        for (int i = 0, n = segments.size(); i < n; i++) {
            segments.getQuick(i).syncPublished();
        }
        clearSyncRequestIfActiveDurable();
    }

    /**
     * Total mmap'd bytes the ring currently owns: active + hot spare (if
     * installed) + every sealed segment. Used by {@code SegmentManager}
     * to seed its {@code totalBytes} accounting at register time and to
     * reverse the contribution at deregister time. Synchronized against
     * rotation so we never read a half-resized sealed list.
     */
    public synchronized long totalSegmentBytes() {
        long total = 0L;
        MmapSegment a = active;
        if (a != null) total += a.sizeBytes();
        MmapSegment hs = hotSpare;
        if (hs != null) total += hs.sizeBytes();
        for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
            total += sealedSegments.get(i).sizeBytes();
        }
        for (int i = 0, n = pendingTrims.size(); i < n; i++) {
            total += pendingTrims.getQuick(i).sizeBytes();
        }
        return total;
    }

    private MmapSegment findSegmentContaining0(long fsn) {
        // The sealed list is strictly ascending and contiguous in baseSeq:
        // rotation seals the predecessor exactly where the promoted spare's
        // rebased baseSeq starts, and recovery sorts the chain then rejects
        // any gap via validateContiguous. The only sealed segment that can
        // cover fsn is therefore the rightmost one whose baseSeq is at or
        // below fsn -- binary-search for it (unsigned, matching
        // sortByBaseSeq's key order) instead of walking a list that grows
        // without bound while the producer outpaces the drain.
        int lo = sealedHead;
        int hi = sealedSegments.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Long.compareUnsigned(sealedSegments.get(mid).baseSeq(), fsn) <= 0) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        if (hi >= sealedHead) {
            MmapSegment segment = sealedSegments.get(hi);
            long base = segment.baseSeq();
            if (fsn >= base && fsn < base + segment.frameCount()) {
                return segment;
            }
        }
        MmapSegment liveActive = active;
        if (liveActive != null) {
            long base = liveActive.baseSeq();
            if (fsn >= base && fsn < base + liveActive.frameCount()) {
                return liveActive;
            }
        }
        return null;
    }

    private void compactSealedSegments() {
        if (sealedHead > 0) {
            int liveCount = sealedSegments.size() - sealedHead;
            trimMovedReferences += liveCount;
            sealedSegments.remove(0, sealedHead - 1);
            // The durable-prefix frontier lives in the same index space as the
            // entries we just shifted down by sealedHead; move it with them.
            // The invariant firstNonDurableSealed >= sealedHead keeps this >= 0.
            firstNonDurableSealed -= sealedHead;
            if (firstNonDurableSealed < 0) {
                firstNonDurableSealed = 0;
            }
            sealedHead = 0;
        }
    }

    private void removeSealedHead() {
        sealedSegments.setQuick(sealedHead++, null);
        // If the removed head WAS the frontier (a non-durable but already-ACKed
        // recovery-resumed segment can be trimmed before its first barrier),
        // keep the frontier at or ahead of the live head.
        if (firstNonDurableSealed < sealedHead) {
            firstNonDurableSealed = sealedHead;
        }
        int size = sealedSegments.size();
        if (sealedHead == size) {
            sealedSegments.clear();
            sealedHead = 0;
            firstNonDurableSealed = 0;
        } else if (sealedHead >= 64 && sealedHead >= size - sealedHead) {
            compactSealedSegments();
        }
    }

    private void switchIoPin(MmapSegment segment) {
        MmapSegment previous = ioPinnedSegment;
        ioPinnedSegment = segment;
        if (previous != null && previous != segment) {
            wakeManager();
        }
    }

    private void wakeManager() {
        Runnable wakeup = managerWakeup;
        if (wakeup != null) {
            wakeup.run();
        }
    }

    /** Returns the sealed-list operation count used by traversal tests. */
    @TestOnly
    public static long getNextSealedComparisons() {
        return nextSealedComparisons;
    }

    /**
     * Returns the cumulative count of baseSeq comparisons performed by
     * {@link #sortByBaseSeq} since the last {@link #resetSortComparisons()}
     * (or process start). The count is incremented once per partition pass
     * for the median-of-three pivot pick plus once per element compared
     * against the pivot ({@code 3 + (hi - lo - 1)} per pass), and by two per
     * sift-down level when a range falls back to heapsort, so it strictly
     * upper-bounds the true compare count and sums to O(N log N) on every
     * input. Exposed for {@code SegmentRingTest} to detect O(N²) regressions
     * deterministically.
     */
    @TestOnly
    public static long getSortComparisons() {
        return sortComparisons;
    }

    /** Returns the references moved by sealed-list compaction. */
    @TestOnly
    public static long getTrimMovedReferences() {
        return trimMovedReferences;
    }

    /** Zeroes the counter exposed via {@link #getNextSealedComparisons()}. */
    @TestOnly
    public static void resetNextSealedComparisons() {
        nextSealedComparisons = 0;
    }

    /** Zeroes the counter exposed via {@link #getSortComparisons()}. */
    @TestOnly
    public static void resetSortComparisons() {
        sortComparisons = 0;
    }

    /** Zeroes the counter exposed via {@link #getTrimMovedReferences()}. */
    @TestOnly
    public static void resetTrimMovedReferences() {
        trimMovedReferences = 0;
    }

    /**
     * Drives the recovery-time baseSeq sort over the whole list. Exposed so
     * {@code SegmentRingTest} can feed adversarial orders (organ-pipe, mass
     * duplicates, median-of-three killer, unsigned-boundary keys) straight
     * into the sort and assert comparison bounds without staging thousands
     * of segment files on disk.
     */
    @TestOnly
    public static void sortByBaseSeqForTest(ObjList<MmapSegment> list) {
        sortByBaseSeq(list, 0, list.size());
    }

    /**
     * In-place introsort over {@code list[lo, hi)} keyed by ascending
     * unsigned {@code baseSeq}. Median-of-three quicksort handles the readdir
     * orders a healthy slot produces (lexicographic enumeration of the
     * generation-numbered filenames yields already-sorted baseSeqs; hashed
     * directory order is effectively random), and a partition-pass budget of
     * 2·⌊log₂(N)⌋ demotes any range that keeps splitting badly to in-place
     * heapsort. Without that budget, Lomuto with a median-of-three pivot is
     * O(N²) on organ-pipe, duplicate-heavy and median-of-three-killer orders
     * -- reachable only through corrupted-yet-parseable or operator-copied
     * headers, but at the documented 16K-segment ceiling that is 10⁷..10⁸
     * comparisons of startup stall before recovery validation gets to
     * reject the slot, so the fallback makes O(N log N) unconditional.
     * Recursion depth stays under log₂(N) (recurse on the smaller side,
     * loop on the larger), well within the JVM default stack.
     */
    private static void sortByBaseSeq(ObjList<MmapSegment> list, int lo, int hi) {
        int n = hi - lo;
        if (n > 1) {
            sortByBaseSeq(list, lo, hi, 2 * (31 - Integer.numberOfLeadingZeros(n)));
        }
    }

    private static void sortByBaseSeq(ObjList<MmapSegment> list, int lo, int hi, int budget) {
        while (hi - lo > 1) {
            if (budget-- == 0) {
                heapSortByBaseSeq(list, lo, hi);
                return;
            }
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
            // recursion depth bounded by log₂(N). Children inherit the
            // remaining pass budget: it counts passes along a root-to-leaf
            // path, so a chain of bad splits exhausts it after ~2 log₂(N)
            // levels no matter how the work is divided.
            if (store - lo < hi - store - 1) {
                sortByBaseSeq(list, lo, store, budget);
                lo = store + 1;
            } else {
                sortByBaseSeq(list, store + 1, hi, budget);
                hi = store;
            }
        }
    }

    /**
     * In-place heapsort over {@code list[lo, hi)} keyed by ascending unsigned
     * {@code baseSeq}: the introsort fallback for ranges whose partition-pass
     * budget ran out. Guaranteed O(N log N) for any key distribution and any
     * initial order; no allocation.
     */
    private static void heapSortByBaseSeq(ObjList<MmapSegment> list, int lo, int hi) {
        int n = hi - lo;
        for (int root = (n >>> 1) - 1; root >= 0; root--) {
            siftDownByBaseSeq(list, lo, root, n);
        }
        for (int end = n - 1; end > 0; end--) {
            swap(list, lo, lo + end);
            siftDownByBaseSeq(list, lo, 0, end);
        }
    }

    private static void siftDownByBaseSeq(ObjList<MmapSegment> list, int lo, int root, int heapSize) {
        while (true) {
            int child = (root << 1) + 1;
            if (child >= heapSize) {
                return;
            }
            // At most two unsigned compares per level (sibling pick + parent
            // test); bump the counter by the constant 2 up front -- same
            // cheap-upper-bound convention as the partition pass.
            sortComparisons += 2;
            if (child + 1 < heapSize
                    && Long.compareUnsigned(list.get(lo + child).baseSeq(),
                    list.get(lo + child + 1).baseSeq()) < 0) {
                child++;
            }
            if (Long.compareUnsigned(list.get(lo + root).baseSeq(),
                    list.get(lo + child).baseSeq()) >= 0) {
                return;
            }
            swap(list, lo + root, lo + child);
            root = child;
        }
    }

    private static void swap(ObjList<MmapSegment> list, int i, int j) {
        if (i == j) return;
        MmapSegment tmp = list.get(i);
        list.setQuick(i, list.get(j));
        list.setQuick(j, tmp);
    }
}
