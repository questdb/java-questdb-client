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
    /** Sentinel: append failed because the payload doesn't fit in a fresh segment. */
    public static final long PAYLOAD_TOO_LARGE = -2L;
    private static final Logger LOG = LoggerFactory.getLogger(SegmentRing.class);
    // Tally of sealed-list entries inspected by nextSealedAfter(). Test-only
    // operation count for deterministic traversal-complexity assertions.
    private static long nextSealedComparisons;
    // Tally of baseSeq comparisons performed by sortByBaseSeq across every
    // openExisting() recovery on this JVM. Used by SegmentRingTest to
    // assert the sort stays O(N log N) without relying on wall-clock time
    // (CI runner variance makes elapsed-millisecond bounds flaky). Cheap
    // in production: one volatile-free add per partition pass, dwarfed by
    // the mmap I/O the recovery does on every segment.
    private static long sortComparisons;
    // References copied while compacting the logical sealed-segment head.
    // Test-only operation count for deterministic trim-complexity assertions.
    private static long trimMovedReferences;
    private final long maxBytesPerSegment;
    private final SfManifest manifest;
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
     * Exhaustively discovers and validates an SF slot without mutation. Only
     * after enumeration, opens, CRC scans, contiguity and manifest boundaries
     * all succeed does it migrate a legacy chain or discard validated spares.
     */
    static Recovery recover(FilesFacade filesFacade, String sfDir, long maxBytesPerSegment) {
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
        // negative baseSeq, unreadable header page). They are excluded from
        // the chain and quarantined to <name>.corrupt — but only AFTER the
        // surviving chain validates (or resolves to EMPTY), so a failed
        // recovery never mutates the slot. Whether a quarantined file was
        // load-bearing is decided by the manifest-boundary / contiguity
        // checks below, not by the skip itself. Operational open errors
        // (EMFILE, EACCES, mmap rejection, unsupported version) are NOT in
        // this bucket: they throw the plain MmapSegmentException type and
        // abort recovery, because the underlying file may be perfectly
        // intact and silently dropping it could lose durable frames.
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
                        throw new MmapSegmentException("every SF segment in " + sfDir
                                + " is corrupt but " + SfManifest.FILE_NAME
                                + " references durable data");
                    }
                    quarantineCorrupt(filesFacade, corruptPaths);
                    return Recovery.empty();
                }
                if (manifest != null) {
                    // No .sfa files at all. Two legitimate protocols produce
                    // this: the close-time drain unlinks the last segment
                    // before it removes the manifest, and a fresh-start crash
                    // can leave a boundary-less (0,0) manifest behind. In
                    // both cases nothing recoverable exists, so accept EMPTY
                    // -- but shout, because a manual wipe of segment files
                    // looks identical and the operator should know.
                    LOG.warn("SF manifest exists in {} with no segment files "
                            + "(clean-drain or fresh-start crash window, or manual "
                            + "segment removal); discarding it and starting fresh", sfDir);
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
                throw new MmapSegmentException("new-format SF segment exists but "
                        + SfManifest.FILE_NAME + " is missing");
            }

            MmapSegment active;
            ObjList<MmapSegment> chain = new ObjList<>();
            long headBase;
            long activeBase;
            if (manifest == null) {
                if (data.size() > 0) {
                    validateContiguous(data);
                    for (int i = 0, n = data.size(); i < n; i++) {
                        chain.add(data.get(i));
                    }
                    active = chain.get(chain.size() - 1);
                    headBase = chain.get(0).baseSeq();
                    activeBase = active.baseSeq();
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
                            throw new MmapSegmentException("segment overlaps committed SF head boundary");
                        }
                        continue; // acknowledged stale file after manifest-before-unlink crash
                    }
                    if (segment.baseSeq() > activeBase) {
                        throw new MmapSegmentException("segment exists beyond committed SF active boundary");
                    }
                    chain.add(segment);
                }
                if (chain.size() > 0) {
                    validateContiguous(chain);
                    if (chain.get(0).baseSeq() != headBase) {
                        throw new MmapSegmentException("missing expected SF head segment at base " + headBase);
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
                                quarantineFile(filesFacade, path);
                            } else if (!filesFacade.remove(path)) {
                                LOG.warn("could not remove drained SF leftover {}", path);
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
                    throw new MmapSegmentException("missing expected SF active segment at base " + activeBase);
                }
                if (chain.size() == 0) {
                    if (headBase != activeBase || active.frameCount() != 0 || corruptPaths != null) {
                        // corruptPaths guard: with an unreadable .sfa in the
                        // slot, the innocent-looking empty at the active base
                        // could be a leftover spare coincidentally carrying
                        // the same provisional baseSeq as a corrupted real
                        // active -- accepting it would quarantine unacked
                        // frames and re-issue their FSNs. Fail closed.
                        throw new MmapSegmentException(
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
                        throw new MmapSegmentException(
                                "missing expected SF active/tail segment at base " + activeBase);
                    }
                }
                for (int i = 0, n = chain.size() - 1; i < n; i++) {
                    if (chain.get(i).tornTailBytes() > 0) {
                        throw new MmapSegmentException("corrupt torn tail in sealed SF segment " + chain.get(i).path());
                    }
                }
                for (int i = 0, n = chain.size(); i < n; i++) {
                    chain.get(i).markManifestRequired();
                }
            }

            for (int i = 1, n = chain.size(); i < n; i++) {
                chain.get(i - 1).linkSuccessor(chain.get(i));
            }
            SegmentRing ring = new SegmentRing(active, maxBytesPerSegment, manifest);
            manifest = null;
            for (int i = 0, n = chain.size() - 1; i < n; i++) {
                ring.sealedSegments.add(chain.get(i));
            }
            // Ownership of the chain transferred. Clean up only validated
            // extras; recovery is already successful, so cleanup failure
            // must never turn startup into a partially-mutating error or
            // orphan the constructed ring -- swallow and let the next
            // startup re-examine the leftovers. Extras with a torn tail
            // carry evidence of attempted writes -- keep the bytes under a
            // .corrupt name instead of unlinking them.
            try {
                for (int i = 0, n = all.size(); i < n; i++) {
                    MmapSegment segment = all.get(i);
                    if (!containsIdentity(chain, segment)) {
                        String path = segment.path();
                        long torn = segment.tornTailBytes();
                        segment.close();
                        if (torn > 0) {
                            quarantineFile(filesFacade, path);
                        } else if (!filesFacade.remove(path)) {
                            LOG.warn("could not remove validated stale/empty SF segment {}", path);
                        }
                    }
                }
                all.clear();
                quarantineCorrupt(filesFacade, corruptPaths);
            } catch (Throwable cleanupError) {
                LOG.warn("post-recovery cleanup failed; leftover files will be "
                        + "re-examined on the next startup", cleanupError);
            }
            return Recovery.recovered(ring);
        } catch (Throwable t) {
            for (int i = 0, n = all.size(); i < n; i++) {
                try {
                    all.get(i).close();
                } catch (Throwable closeError) {
                    LOG.warn("error closing SF segment after recovery failure", closeError);
                }
            }
            if (manifest != null) {
                manifest.close();
            }
            throw t;
        }
    }

    /**
     * Durably advances the manifest head past {@code trimming} (the sealed
     * segment the manager is about to unlink). The successor and the current
     * active are both read under the ring monitor, so a concurrent rotation
     * (which also mutates the manifest under this monitor) can never make the
     * head leapfrog a still-live sealed segment: if rotation sealed the old
     * active after the caller's snapshot, {@code trimming.successor()} now
     * points at that sealed segment, not at the new active.
     */
    synchronized void advanceManifestHeadPast(MmapSegment trimming) {
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

    /** Renames every collected corrupt path to {@code <path>.corrupt}, best-effort. */
    private static void quarantineCorrupt(FilesFacade filesFacade, ObjList<String> corruptPaths) {
        if (corruptPaths == null) {
            return;
        }
        for (int i = 0, n = corruptPaths.size(); i < n; i++) {
            quarantineFile(filesFacade, corruptPaths.get(i));
        }
    }

    private static void quarantineFile(FilesFacade filesFacade, String path) {
        if (filesFacade.rename(path, path + ".corrupt") != 0) {
            LOG.warn("could not quarantine {} to {}.corrupt; it will be re-examined "
                    + "on the next recovery", path, path);
        }
    }

    private static boolean containsIdentity(ObjList<MmapSegment> list, MmapSegment value) {
        for (int i = 0, n = list.size(); i < n; i++) {
            if (list.get(i) == value) return true;
        }
        return false;
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
                throw new MmapSegmentException("FSN gap in recovered segments: expected "
                        + expected + " but got " + current.baseSeq());
            }
        }
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
            MmapSegment previous = active;
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
                // Deliberately NOT msync'd here: the sealed predecessor's
                // data pages. A power loss can therefore tear the sealed
                // tail after the boundary is committed, and recovery will
                // fail closed on chainEnd != activeBase. That is the
                // intended semantics -- page-level durability of frame data
                // follows the sender's opt-in msync cadence, and recovery
                // must refuse to guess when the two disagree.
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
        for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
            sealedSegments.get(i).close();
        }
        sealedSegments.clear();
        sealedHead = 0;
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
     * Walks sealed first (oldest → newest) then the active. The sealed list
     * is small enough -- and reconnects are rare enough -- that the linear
     * scan cost doesn't matter.
     */
    public synchronized MmapSegment findSegmentContaining(long fsn) {
        for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
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
        return sealedHead < sealedSegments.size() ? sealedSegments.get(sealedHead) : null;
    }

    /**
     * Returns the oldest fully acknowledged sealed segment without removing
     * it. The segment manager keeps it owned by the ring until close + unlink
     * succeeds, so a failed unlink cannot make the path disappear from live
     * bookkeeping or allow its identifier to be reused.
     */
    public synchronized MmapSegment firstTrimmable() {
        if (sealedHead == sealedSegments.size()) {
            return null;
        }
        MmapSegment segment = sealedSegments.get(sealedHead);
        long lastSeq = segment.baseSeq() + segment.frameCount() - 1;
        return lastSeq <= ackedFsn ? segment : null;
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
        for (int i = sealedHead, n = sealedSegments.size(); i < n; i++) {
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
     * Commits removal of the segment returned by {@link #firstTrimmable()}.
     * Returns false if concurrent lifecycle activity changed the head.
     */
    public synchronized boolean removeTrimmable(MmapSegment segment) {
        if (sealedHead == sealedSegments.size() || sealedSegments.get(sealedHead) != segment) {
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
        return total;
    }

    private void compactSealedSegments() {
        if (sealedHead > 0) {
            int liveCount = sealedSegments.size() - sealedHead;
            trimMovedReferences += liveCount;
            sealedSegments.remove(0, sealedHead - 1);
            sealedHead = 0;
        }
    }

    private void removeSealedHead() {
        sealedSegments.setQuick(sealedHead++, null);
        int size = sealedSegments.size();
        if (sealedHead == size) {
            sealedSegments.clear();
            sealedHead = 0;
        } else if (sealedHead >= 64 && sealedHead >= size - sealedHead) {
            compactSealedSegments();
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
     * against the pivot, so a clean run on N segments adds roughly
     * {@code 3 + (hi - lo - 1)} per recursive frame, summing to O(N log N).
     * Exposed for {@code SegmentRingTest} to detect O(N²) regressions
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
