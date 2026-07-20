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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.std.Compat;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.LockSupport;

/**
 * Facade that bundles a {@link SegmentRing} with a {@link SegmentManager} and
 * exposes the user-facing API the wire-send loop calls into. Keeps SF append
 * work on the user thread (where it belongs) and segment lifecycle work on
 * the manager thread (where it belongs).
 * <p>
 * <b>Responsibilities:</b>
 * <ul>
 *   <li>Owning the ring + manager lifecycle (open / close / startup recovery).</li>
 *   <li>Providing a user-thread append path that handles backpressure
 *       (spin briefly, then return — caller decides whether to retry).</li>
 *   <li>Exposing read accessors for the I/O thread: {@link #publishedFsn},
 *       {@link #activeSegment}, {@link #sealedSegments}.</li>
 *   <li>Routing server ACKs to the ring for trim.</li>
 * </ul>
 * <b>Not in scope:</b>
 * <ul>
 *   <li>Multi-producer support. Single producer (one user thread) only.</li>
 * </ul>
 */
public final class CursorSendEngine implements QuietCloseable {

    /**
     * Throttle the "producer is backpressured" WARN log to at most once per this interval.
     */
    public static final long BACKPRESSURE_LOG_THROTTLE_NANOS = 5_000_000_000L; // 5 s
    /**
     * Default deadline for {@link #appendBlocking}: 30 seconds.
     */
    public static final long DEFAULT_APPEND_DEADLINE_NANOS = 30_000_000_000L;
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(CursorSendEngine.class);
    private final long appendDeadlineNanos;
    // Number of times appendBlocking observed BACKPRESSURE_NO_SPARE on its first
    // ring.appendOrFsn attempt. One increment per blocking-call that had to wait
    // for the manager (or for ACKs) — not one per spin-park. Producer-thread
    // writer; volatile because the user may sample it from any thread.
    private final java.util.concurrent.atomic.AtomicLong backpressureStallCount =
            new java.util.concurrent.atomic.AtomicLong();
    private final SegmentManager manager;
    // We own the manager iff the user constructed us with no manager — in that
    // case close() also stops the manager. When the manager is shared across
    // many engines (one per Sender), the caller owns and closes it.
    private final boolean ownsManager;
    private final SegmentRing ring;
    private final long segmentSizeBytes;
    private final String sfDir;
    // Held for the engine's lifetime in disk mode. {@code null} in memory
    // mode (no slot, no lock). Released by {@link #close()}; the kernel
    // also drops it on hard process exit.
    private final SlotLock slotLock;
    // True when the constructor recovered an existing on-disk slot rather
    // than starting fresh. Diagnostic accessor for tests and observability;
    // every frame carries its full inline schema, so producer-side schema reset
    // on recovery is not required (the symbol dictionary, which delta frames do
    // NOT carry in full, is re-registered by an I/O-thread catch-up instead).
    private final boolean wasRecoveredFromDisk;
    // FSN of the last commit-bearing (non-FLAG_DEFER_COMMIT) frame found in a
    // ring recovered from disk, or -1 for fresh/memory rings and recovered
    // rings whose every frame is deferred. Frames above this FSN in the
    // recovered ring belong to a transaction whose commit frame was never
    // published; the server will never ack them until some later commit
    // covers them. Read by the sender's close-time drain to avoid waiting on
    // acks that cannot arrive.
    private long recoveredCommitBoundaryFsn = -1L;
    // Highest symbol id any recovered delta frame references, or -1 for
    // fresh/memory rings (and recovered rings with no symbol-bearing frame). A
    // resuming producer seeds its dictionary baseline from the persisted
    // .symbol-dict; if that dictionary was torn below this id by a host crash
    // (the side-file is not fsync'd), the producer would re-use ids the surviving
    // frames already define. seedGlobalDictionaryFromPersisted compares this
    // against the recovered dictionary size to fail clean instead. Computed once
    // in the constructor's recovery branch; -1 elsewhere.
    private long recoveredMaxSymbolId = -1L;
    // Highest deltaStart across the recovered COMMITTED frames; 0 when none carries a symbol
    // dictionary. ZERO means every surviving frame is SELF-SUFFICIENT -- it re-registers its
    // dictionary from id 0 -- so the slot replays with no dictionary at all and the send loop
    // needs no catch-up. ABOVE zero means at least one frame is a true delta whose ids depend
    // on registrations it does not itself carry, so the loop must seed its mirror (and ship a
    // catch-up) before replaying. Both the full-dict-fallback discard below and the send
    // loop's mirror seeding key off this.
    private long recoveredMaxSymbolDeltaStart;
    // FSN of the last frame of a recovered orphaned deferred tail, or -1 when
    // the recovered ring has no such tail. When >= 0, frames
    // [recoveredCommitBoundaryFsn + 1 .. recoveredOrphanTipFsn] all carry
    // FLAG_DEFER_COMMIT with no covering commit frame -- an aborted
    // transaction. The send loop must never transmit them; it retires the
    // range with a cumulative self-acknowledge once everything below is
    // server-acked (CursorWebSocketSendLoop.tryRetireOrphanTail).
    private long recoveredOrphanTipFsn = -1L;
    // Engine-owned mmap'd watermark file. {@code null} in memory mode and
    // in disk mode if open() failed (we proceed without it; recovery just
    // falls back to lowestBase - 1). Lifetime tied to the engine: opened
    // in the constructor, closed by {@link #close()}. The segment manager
    // writes through this on every tick where ackedFsn has advanced.
    private final AckWatermark watermark;
    // Engine-owned per-slot symbol dictionary file (disk mode only; {@code null}
    // in memory mode and if open() failed). Enables delta-encoded SF frames:
    // recovery / orphan-drain load it to re-register the dictionary on the fresh
    // server before replaying non-self-sufficient frames. Opened in the
    // constructor, closed by {@link #close()}. When null in disk mode the engine
    // reports delta encoding as unavailable and the sender keeps full-dict frames.
    private final PersistedSymbolDict persistedSymbolDict;
    // Engine-owned output of the single ordered recovery walk. It is retained
    // because both producer seeding and every recycled send loop need the same
    // frame-rebuilt symbol suffix. Null for fresh and memory-only engines.
    private final RecoveredFrameAnalysis recoveredFrameAnalysis;
    // close() is publicly callable from any thread (Sender.close from a user
    // thread, JVM shutdown hooks, test cleanup). volatile + synchronized
    // close() makes the check-and-set atomic and gives readers a fence.
    private volatile boolean closed;
    // Producer-thread-only: timestamp of the last "we're backpressured" log
    // line, used to throttle. Plain long is fine.
    private long lastBackpressureLogNs;

    /**
     * Creates an engine with a private, non-shared {@link SegmentManager},
     * unbounded total bytes (use only for tests / single-segment scenarios),
     * and the default append deadline.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes) {
        this(sfDir, segmentSizeBytes, SegmentManager.UNLIMITED_TOTAL_BYTES,
                DEFAULT_APPEND_DEADLINE_NANOS);
    }

    /**
     * Creates an engine with a private, non-shared {@link SegmentManager}
     * capped at {@code maxTotalBytes} of cursor-allocated memory/disk
     * (active + spare + sealed). Producer's {@link #appendBlocking} blocks
     * up to {@code appendDeadlineNanos} when the cap is full and ACKs
     * haven't drained sealed segments; on deadline expiry it throws.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes,
                            long maxTotalBytes, long appendDeadlineNanos) {
        this(sfDir, segmentSizeBytes, maxTotalBytes, appendDeadlineNanos, FilesFacade.INSTANCE);
    }

    /**
     * As {@link #CursorSendEngine(String, long, long, long)}, but with an explicit
     * {@link FilesFacade} for the persisted symbol dictionary.
     * <p>
     * The seam exists so a test can drive a dictionary I/O fault -- a short write from
     * a full disk or an exhausted quota -- through the REAL producer path
     * ({@code flush()} -> the write-ahead persist), and assert it surfaces as a
     * {@code LineSenderException} like every other flush-path failure rather than as a
     * raw {@code IllegalStateException} that would sail past every caller's
     * {@code catch (LineSenderException)}. Nothing else could reach that translation:
     * {@code PersistedSymbolDict} has facade-aware overloads, but the engine used to
     * call only the {@code FilesFacade.INSTANCE} ones, so no fault could be injected
     * from outside.
     */
    @TestOnly
    public CursorSendEngine(String sfDir, long segmentSizeBytes,
                            long maxTotalBytes, long appendDeadlineNanos, FilesFacade dictFf) {
        this(sfDir, segmentSizeBytes,
                new SegmentManager(segmentSizeBytes, SegmentManager.DEFAULT_POLL_NANOS, maxTotalBytes),
                true, appendDeadlineNanos, dictFf);
    }

    /**
     * Creates an engine that shares the given {@link SegmentManager} (which
     * must already be {@link SegmentManager#start()}'d). The caller retains
     * ownership of the manager. Uses the default append deadline.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager) {
        this(sfDir, segmentSizeBytes, manager, false, DEFAULT_APPEND_DEADLINE_NANOS,
                FilesFacade.INSTANCE);
    }

    private CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager,
                             boolean ownsManager, long appendDeadlineNanos, FilesFacade dictFf) {
        // sfDir == null  → memory-only mode (non-SF async ingest). Same
        //                  cursor architecture, no disk involvement; segments
        //                  live in malloc'd native memory.
        // sfDir != null  → store-and-forward mode. Segments are mmap'd files
        //                  under sfDir, recoverable across sender restarts.
        boolean memoryMode = sfDir == null;
        SlotLock acquiredLock = null;
        if (!memoryMode) {
            try {
                if (sfDir.isEmpty()) {
                    throw new IllegalArgumentException("sfDir must not be empty");
                }
                // Acquire the slot lock BEFORE we touch any *.sfa files. Two
                // engines pointed at the same slot would otherwise race on
                // recovery and create overlapping FSN ranges. SlotLock.acquire
                // also creates the slot dir if it doesn't exist yet — no
                // separate mkdir step needed here.
                acquiredLock = SlotLock.acquire(sfDir);
            } catch (Throwable t) {
                // The delegating constructors evaluate `new SegmentManager(...)`
                // BEFORE this body runs, so on a pre-try throw (e.g. slot lock
                // collision) an owned manager is already alive and would leak
                // its native path-scratch sink -- 256 bytes per failed
                // construction attempt. Close it before propagating.
                if (ownsManager) {
                    try {
                        manager.close();
                    } catch (Throwable ignored) {
                    }
                }
                throw t;
            }
        }
        this.slotLock = acquiredLock;
        this.sfDir = sfDir;
        this.segmentSizeBytes = segmentSizeBytes;
        this.manager = manager;
        this.ownsManager = ownsManager;
        this.appendDeadlineNanos = appendDeadlineNanos;

        // Track the ring locally until every step succeeds — only commit it
        // to this.ring at the very end. If anything between ring allocation
        // and manager.register throws, the catch block closes the local
        // reference instead of orphaning the mmap'd segments + fds.
        SegmentRing ringInProgress = null;
        AckWatermark watermarkInProgress = null;
        PersistedSymbolDict persistedDictInProgress = null;
        RecoveredFrameAnalysis recoveredFrameAnalysisInProgress = null;
        try {
            // v2 segment payloads may depend on a persisted symbol-dictionary
            // prefix. Install the rollback barrier before recovery or any new
            // append so a v1-only client can never skip the v2 files, treat the
            // slot as empty, and silently restart at FSN 0. Current recovery
            // recognizes and skips the reserved guard filenames.
            if (!memoryMode) {
                SegmentRing.installLegacyReaderBarrier(sfDir);
            }
            // Disk mode: try to recover any *.sfa files left behind by a prior
            // session before deciding to start fresh. Without this the engine
            // would create a new sf-initial.sfa at baseSeq=0, overlapping FSNs
            // already on disk and corrupting ACK translation, trim, and replay.
            SegmentRing recovered = memoryMode ? null
                    : SegmentRing.openExisting(sfDir, segmentSizeBytes);
            this.wasRecoveredFromDisk = recovered != null;
            if (recovered != null) {
                ringInProgress = recovered;
                // Seed ackedFsn to one below the lowest segment's baseSeq.
                // We don't know what was actually acked before the prior
                // session crashed, but anything trimmed off the ring's
                // bottom must have been acked (trim is ack-driven). Without
                // this seed, ackedFsn stays at -1 and the I/O loop's
                // start-time positioning would walk to FSN 0 — which may
                // not exist on disk if earlier segments have been trimmed,
                // causing it to fall through to the active segment's tip
                // and skip the unacked sealed segments entirely.
                //
                // Then check the persisted ack watermark. If present and
                // larger than the segment-derived seed, prefer it: it
                // captures durable-acks that landed inside the lowest
                // surviving sealed segment before the previous sender
                // crashed. Without this, those already-acked frames would
                // be re-replayed on reconnect, producing row-level
                // duplicates unless the target table dedupes them.
                // max(lowestBase - 1, watermark) absorbs both write
                // orderings of the manager's "persist then trim" tick:
                //   - persist crashed before trim: segments still on disk
                //     are >= lowest, watermark is correct; max picks
                //     watermark.
                //   - trim ran before persist: segments are gone (so
                //     lowestBase is higher than watermark), watermark is
                //     stale; max picks lowestBase - 1.
                MmapSegment first = recovered.firstSealed();
                long lowestBase = first != null
                        ? first.baseSeq()
                        : recovered.getActive().baseSeq();
                // Open the watermark and use it (if present) to refine
                // the seed. The watermark may carry durable-acks the
                // previous sender received for frames inside the lowest
                // surviving sealed segment -- without it, those frames
                // get re-replayed across process restart, producing
                // row-level duplicates unless the target table dedupes
                // them. max(lowestBase - 1, watermark) absorbs both
                // write orderings of the manager's "persist then trim"
                // tick:
                //   - persist crashed before trim: segments still on
                //     disk are >= lowest, watermark is correct; max
                //     picks watermark.
                //   - trim ran before persist: segments are gone (so
                //     lowestBase is higher than watermark), watermark
                //     is stale; max picks lowestBase - 1.
                // open() returns null on any setup failure so a missing
                // mmap doesn't take down the engine -- we just fall
                // back to the bare lowestBase - 1 seed.
                watermarkInProgress = AckWatermark.open(sfDir);
                // Load the persisted symbol dictionary so delta-encoded frames
                // in this recovered slot can be re-registered on the fresh
                // server before replay. Null on open failure -> delta disabled.
                persistedDictInProgress = PersistedSymbolDict.open(dictFf, sfDir);
                long baseSeed = lowestBase - 1;
                long watermarkFsn = watermarkInProgress != null
                        ? watermarkInProgress.read()
                        : AckWatermark.INVALID;
                // Reject watermarks past publishedFsn: a correctly
                // operating prior session cannot have produced one, so
                // a value above the on-disk frame ceiling is corruption
                // (torn write on a non-atomic filesystem, hardware
                // fault, manual edit). Trusting it would seed ackedFsn
                // = publishedFsn after ring.acknowledge clamps it, and
                // the cursor would position past every un-acked frame
                // -- silent data loss. Fall back to the segment-derived
                // seed so the un-acked tail still replays.
                long publishedFsn = recovered.publishedFsn();
                long candidate = Math.max(watermarkFsn, baseSeed);
                long seed = candidate > publishedFsn ? baseSeed : candidate;
                if (seed >= 0) {
                    recovered.acknowledge(seed);
                }
                // Fold the whole recovered ring once. The result checkpoints all
                // running metadata and raw symbol bytes at each commit-bearing
                // frame, so its final snapshot excludes an orphan deferred tail
                // without requiring a second bounded scan.
                recoveredFrameAnalysisInProgress = recovered.analyzeRecovery(
                        persistedDictInProgress == null ? 0 : persistedDictInProgress.size());
                // Locate the last commit-bearing frame below a potentially
                // orphaned FLAG_DEFER_COMMIT tail. A producer that crashed (or
                // closed) mid-transaction leaves deferred frames with no
                // covering commit frame at the top of the ring. The server
                // never acks uncommitted deferred frames, so (a) close-time
                // drains must not wait for them (see the sender's
                // drainOnClose), and (b) replaying them into a NEW session's
                // commit would resurrect half a transaction -- see the WARN
                // below. Computed before the I/O loop or producer append.
                this.recoveredCommitBoundaryFsn = recoveredFrameAnalysisInProgress.commitBoundaryFsn();
                if (publishedFsn >= 0 && recoveredCommitBoundaryFsn < publishedFsn) {
                    this.recoveredOrphanTipFsn = publishedFsn;
                    LOG.warn("recovered SF log ends with {} deferred frame(s) whose transaction was never "
                                    + "committed [commitBoundaryFsn={}, publishedFsn={}]. The tail belongs to an "
                                    + "aborted transaction: it will never be transmitted and its slots are "
                                    + "retired (trimmed) once every frame below it is server-acked.",
                            publishedFsn - Math.max(recoveredCommitBoundaryFsn, -1L),
                            recoveredCommitBoundaryFsn, publishedFsn);
                }
                // Highest symbol id the surviving COMMITTED frames reference. A
                // resuming producer compares this against its recovered dictionary
                // size (seedGlobalDictionaryFromPersisted) to detect a host-crash
                // tear: if a committed frame references an id the (unsynced, torn)
                // .symbol-dict no longer holds, resuming would re-use it. The walk is
                // bounded to recoveredCommitBoundaryFsn so the aborted orphan-deferred
                // tail -- retired without ever being transmitted -- does not inflate
                // this and over-reject an otherwise-recoverable slot. maxDeltaEnd()
                // returns 0 when no such frame carries a symbol, yielding -1 here.
                // Computed before the I/O loop or producer append; single-threaded.
                this.recoveredMaxSymbolId = recoveredFrameAnalysisInProgress.maxDeltaEnd() - 1L;
                // Full-dict-fallback recovery. When the persisted .symbol-dict is a
                // SUBSET of the ids the surviving frames reference
                // (recoveredMaxSymbolId >= its size) YET every such frame is
                // self-sufficient (maxDeltaStart() == 0 -- a full-dict frame that
                // re-registers its dictionary from id 0), the slot was written in
                // full-dict fallback: the dictionary never opened when writing, so no
                // side-file exists and this recovery opened a FRESH EMPTY one. Those
                // frames replay with no dictionary, so discard the empty side-file and
                // recover in full-dict mode -- isDeltaDictEnabled() then reports false
                // and the producer + send loop both run full-dict, exactly as the slot
                // was written. Without this the sender's seed-time guard would treat the
                // empty dictionary as a host-crash tear and brick build(), even though
                // the orphan drainer drains the same frames fine. A genuine torn DELTA
                // dictionary keeps a frame with deltaStart > 0 (maxDeltaStart() > 0)
                // and is NOT discarded here: it still fails clean at seed time, since
                // the ids its delta frames reference cannot be rebuilt without the lost
                // dictionary. The recoveredMaxSymbolId >= size guard means this never
                // fires for a slot whose dictionary is intact, nor for an empty slot
                // (recoveredMaxSymbolId == -1). Single-threaded; before the I/O loop.
                this.recoveredMaxSymbolDeltaStart = recoveredFrameAnalysisInProgress.maxDeltaStart();
                if (persistedDictInProgress != null
                        && recoveredMaxSymbolId >= persistedDictInProgress.size()
                        && recoveredMaxSymbolDeltaStart == 0L) {
                    persistedDictInProgress.close();
                    persistedDictInProgress = null;
                    // Re-fold at baseline 0. The analysis above was keyed to the
                    // dictionary's size, and every consumer of it presents the baseline it
                    // derived the same way -- seedGlobalDictionaryFromPersisted computes
                    // baseline 0 once pd is gone, and checkedRecoveryAnalysis rejects a
                    // baseline that disagrees with the fold. Discarding a dictionary that
                    // held entries (size > 0) therefore desynchronised the two and threw
                    // IllegalStateException("recovery symbol baseline mismatch") out of
                    // build(). That is NOT an UnreplayableSlotException, so build()'s
                    // quarantine handler could not catch it and set the slot aside: with a
                    // stable senderId every restart re-recovered the same slot and threw
                    // again, so the application could never construct a Sender -- it could
                    // not even BUFFER new rows. Exactly the outage quarantineTornSlot
                    // exists to prevent, on a slot that is fully recoverable (its frames
                    // carry their whole dictionary inline).
                    //
                    // Reachable on one transient plus one crash: a session whose
                    // .symbol-dict fails to open (EIO, fd exhaustion, a Windows share
                    // lock) falls back to full-dict frames and, per the never-recreate
                    // contract, leaves the previous session's populated side-file intact;
                    // if that session then crashes, this recovery opens a dictionary with
                    // size > 0 next to self-sufficient frames that out-reach it.
                    //
                    // Re-folding rather than keeping the dictionary preserves the discard's
                    // whole point -- the slot recovers in full-dict mode, exactly as it was
                    // written, with producer, mirror and replay guard all anchored at 0.
                    recoveredFrameAnalysisInProgress.close();
                    recoveredFrameAnalysisInProgress = recovered.analyzeRecovery(0);
                    this.recoveredCommitBoundaryFsn = recoveredFrameAnalysisInProgress.commitBoundaryFsn();
                    this.recoveredMaxSymbolId = recoveredFrameAnalysisInProgress.maxDeltaEnd() - 1L;
                    this.recoveredMaxSymbolDeltaStart = recoveredFrameAnalysisInProgress.maxDeltaStart();
                }
            } else {
                // Fresh start with no recovered segments. Any stale
                // watermark from a prior fully-drained session refers
                // to a lifecycle now gone -- unlink it before opening
                // so the new session's first read() correctly reports
                // INVALID (magic=0 on a freshly zero-filled file).
                if (!memoryMode) {
                    AckWatermark.removeOrphan(sfDir);
                    watermarkInProgress = AckWatermark.open(sfDir);
                    // A fresh slot MUST start with an EMPTY symbol dictionary.
                    // Unlike the ack watermark above -- a discardable optimization a
                    // max() clamp protects -- the dictionary is load-bearing: a
                    // delta frame referencing an id missing from it is unrecoverable,
                    // and a STALE dictionary inherited here (the segments are gone, so
                    // the producer is NOT seeded from it) shifts the dense id->symbol
                    // mapping and silently misattributes symbols on the next
                    // reconnect. openClean() truncates any survivor to empty rather
                    // than trusting a best-effort delete that may have failed (e.g. a
                    // Windows share lock); if the clean open itself fails,
                    // persistedSymbolDict stays null and the sender falls back to full
                    // self-sufficient frames, which is also safe.
                    persistedDictInProgress = PersistedSymbolDict.openClean(dictFf, sfDir);
                }
                MmapSegment initial;
                String initialPath = null;
                if (memoryMode) {
                    initial = MmapSegment.createInMemory(0L, segmentSizeBytes);
                } else {
                    initialPath = sfDir + "/sf-initial.sfa";
                    initial = MmapSegment.create(initialPath, 0L, segmentSizeBytes);
                }
                try {
                    ringInProgress = new SegmentRing(initial, segmentSizeBytes);
                } catch (Throwable t) {
                    initial.close();
                    if (initialPath != null) {
                        Files.remove(initialPath);
                    }
                    throw t;
                }
            }

            if (ownsManager) {
                manager.start();
            }
            manager.register(ringInProgress, sfDir, watermarkInProgress);
            // All construction succeeded — commit the ring, watermark and
            // symbol-dictionary references.
            this.ring = ringInProgress;
            this.watermark = watermarkInProgress;
            this.persistedSymbolDict = persistedDictInProgress;
            this.recoveredFrameAnalysis = recoveredFrameAnalysisInProgress;
        } catch (Throwable t) {
            // Stop an owned manager before freeing the ring and watermark it may
            // touch, then release the slot lock. Each cleanup is in its own
            // try/catch so a single failure doesn't strand later cleanups.
            // Closing an owned-but-never-started manager is safe (no worker to
            // join) and required: skipping it leaked the manager's native
            // path-scratch sink whenever construction failed before start().
            if (ownsManager) {
                try {
                    manager.close();
                } catch (Throwable ignored) {
                }
            }
            if (ringInProgress != null) {
                try {
                    ringInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (watermarkInProgress != null) {
                try {
                    watermarkInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (persistedDictInProgress != null) {
                try {
                    persistedDictInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (recoveredFrameAnalysisInProgress != null) {
                try {
                    recoveredFrameAnalysisInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (acquiredLock != null) {
                try {
                    acquiredLock.close();
                } catch (Throwable ignored) {
                }
            }
            throw t;
        }
    }

    /**
     * I/O thread accessor: highest FSN safe to send.
     */
    public long ackedFsn() {
        return ring.ackedFsn();
    }

    /**
     * Records a server ACK for cumulative FSN {@code seq}. Triggers
     * background trim of any sealed segments whose every frame is now
     * acknowledged. Idempotent and monotonic.
     *
     * @return {@code true} if the ack watermark actually advanced;
     * {@code false} on a no-op (idempotent re-ack or value clamped
     * at publishedFsn).
     */
    public boolean acknowledge(long seq) {
        return ring.acknowledge(seq);
    }

    /**
     * I/O thread accessor: the current active mmap'd segment.
     */
    public MmapSegment activeSegment() {
        return ring.getActive();
    }

    /**
     * Append the payload, blocking up to {@link #appendDeadlineNanos} when
     * the cursor ring is at its memory/disk cap and waiting for ACK-driven
     * trim to free space. Returns the assigned FSN on success.
     * <p>
     * Backpressure is surfaced two ways:
     * <ul>
     *   <li>{@link #getTotalBackpressureStalls()} counter — incremented once
     *       per blocking-call that had to wait for the manager.</li>
     *   <li>WARN log throttled to one line per
     *       {@link #BACKPRESSURE_LOG_THROTTLE_NANOS} of sustained
     *       backpressure, so ops can correlate slow flushes to the cap.</li>
     * </ul>
     * Throws {@link io.questdb.client.cutlass.line.LineSenderException} when
     * the deadline expires — silent unbounded blocking would mask "wire path
     * is wedged" failures (server down, slow disk, etc.) from the user.
     */
    public long appendBlocking(long payloadAddr, int payloadLen) {
        long fsn = ring.appendOrFsn(payloadAddr, payloadLen);
        if (fsn >= 0) return fsn;
        if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
            throw new MmapSegmentException("payload too large for segment");
        }
        // First miss → record one stall (not one per spin) and start the
        // deadline clock.
        backpressureStallCount.incrementAndGet();
        long deadlineNs = System.nanoTime() + appendDeadlineNanos;
        while (true) {
            long now = System.nanoTime();
            if (now >= deadlineNs) {
                throw new io.questdb.client.cutlass.line.LineSenderException(
                        "cursor ring backpressured for ").put(appendDeadlineNanos / 1_000_000L)
                        .put(" ms — wire path is not draining (server slow / disconnected, or sf_max_total_bytes too small)");
            }
            if (now - lastBackpressureLogNs >= BACKPRESSURE_LOG_THROTTLE_NANOS) {
                lastBackpressureLogNs = now;
                LOG.warn("cursor producer backpressured ({} stalls so far); waiting for I/O drain — will throw after {} ms",
                        backpressureStallCount.get(), appendDeadlineNanos / 1_000_000L);
            }
            LockSupport.parkNanos(50_000L); // 50 µs
            fsn = ring.appendOrFsn(payloadAddr, payloadLen);
            if (fsn >= 0) return fsn;
            if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                throw new MmapSegmentException("payload too large for segment");
            }
        }
    }

    /**
     * User-thread append path. Spins briefly while waiting for the segment
     * manager to provision a hot spare; if backpressure persists past
     * {@code spinDeadlineNanos}, returns {@link SegmentRing#BACKPRESSURE_NO_SPARE}
     * so the caller can decide whether to {@code parkNanos} or surface the
     * pressure to the user.
     * <p>
     * Returns the assigned FSN on success, or one of the
     * {@code SegmentRing.BACKPRESSURE_*} / {@code PAYLOAD_*} sentinels.
     */
    public long appendOrFsn(long payloadAddr, int payloadLen, long spinDeadlineNanos) {
        long fsn = ring.appendOrFsn(payloadAddr, payloadLen);
        if (fsn >= 0) {
            return fsn;
        }
        if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
            return fsn;
        }
        // Backpressure: spin briefly, then return so the caller decides.
        // The spin tightens the gap between manager-installs-spare and
        // producer-consumes-spare — usually a few µs on an idle manager thread.
        while (System.nanoTime() < spinDeadlineNanos) {
            Compat.onSpinWait();
            fsn = ring.appendOrFsn(payloadAddr, payloadLen);
            if (fsn >= 0 || fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                return fsn;
            }
        }
        return SegmentRing.BACKPRESSURE_NO_SPARE;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        // Capture drain state BEFORE closing the ring — once the ring is
        // closed, its accessors aren't safe to read. The active segment is
        // never trimmed by drainTrimmable (only sealed segments are), so
        // when everything published has been acked we have to unlink the
        // residual .sfa files here. Without this, the next sender (or a
        // drainer adopting this slot) would replay already-acked data
        // against potentially-fresh server state — duplicate writes when
        // the server has no dedup state for those messageSequences.
        // Memory mode has no files to unlink.
        // The whole close sequence runs under try/finally so the slot lock
        // is ALWAYS released, even if manager/ring close or unlink throws —
        // otherwise a kernel-held flock outlives the engine and the next
        // sender for the same slot collides on a lock the dead engine
        // never released.
        try {
            // "Fully drained" includes BOTH the obvious case (every published
            // FSN has been acked) AND the never-published case (publishedFsn
            // < 0). The latter matters because a drainer adopting an empty
            // orphan slot — segments filtered as empty by recovery, engine
            // recreates a fresh sf-initial.sfa — would otherwise leave that
            // fresh empty file behind, the next scanner finds it, adopts the
            // slot again, and the cycle repeats forever (M6).
            boolean fullyDrained = sfDir != null
                    && (ring.publishedFsn() < 0
                    || ring.ackedFsn() >= ring.publishedFsn());
            // Each cleanup step in its own try/catch so a single failure
            // doesn't strand later cleanups — mirrors the constructor's
            // catch block. Without this, a throw from manager.deregister
            // or manager.close() would leave the ring mmap'd and any
            // residual .sfa files on disk, where the next sender can
            // adopt them and replay already-acked data.
            try {
                manager.deregister(ring);
            } catch (Throwable ignored) {
            }
            if (ownsManager) {
                try {
                    manager.close();
                } catch (Throwable ignored) {
                }
            }
            try {
                ring.close();
            } catch (Throwable ignored) {
            }
            // Close the watermark mmap/fd after the manager (which
            // writes through it) is gone but before the slot lock is
            // released. On fully-drained close, also unlink the file
            // -- a stale watermark with no segments behind it would
            // confuse a future recovery cycle if (it wouldn't actually
            // confuse current recovery, which only reads the watermark
            // when segments are present, but unlinking keeps the slot
            // dir clean and matches the "remove orphan" intent above).
            if (watermark != null) {
                try {
                    watermark.close();
                } catch (Throwable ignored) {
                }
            }
            if (persistedSymbolDict != null) {
                try {
                    persistedSymbolDict.close();
                } catch (Throwable ignored) {
                }
            }
            if (recoveredFrameAnalysis != null) {
                try {
                    recoveredFrameAnalysis.close();
                } catch (Throwable ignored) {
                }
            }
            if (fullyDrained) {
                try {
                    unlinkAllSegmentFiles(sfDir);
                } catch (Throwable ignored) {
                }
                try {
                    AckWatermark.removeOrphan(sfDir);
                } catch (Throwable ignored) {
                }
                try {
                    // Slot fully drained: the dictionary has no frames behind it.
                    PersistedSymbolDict.removeOrphan(sfDir);
                } catch (Throwable ignored) {
                }
                try {
                    // The logical slot lock lives OUTSIDE the slot dir (in the
                    // shared .slot-locks dir) so it survives a slot rename; the
                    // fully-drained retirement that removes this slot's other
                    // side-files must remove it too, or .slot-locks accumulates a
                    // dead lock+pid pair per distinct slot name for the lifetime of
                    // sf_dir. This engine still holds the directory-local lock, so
                    // the best-effort unlink is safe (see SlotLock.removeOrphanLogical).
                    SlotLock.removeOrphanLogical(sfDir);
                } catch (Throwable ignored) {
                }
            }
        } finally {
            if (slotLock != null) {
                try {
                    slotLock.close();
                } catch (Throwable ignored) {
                    // best-effort; flock is also released by kernel on process exit
                }
            }
        }
    }

    /**
     * Decodes the cached recovery suffix directly into the producer's global
     * dictionary. Recovery always builds the analysis with the persisted
     * prefix size as its baseline, so no intermediate cardinality-sized list is
     * needed on the production path.
     */
    public long addRecoveredSymbolsTo(int baseline, GlobalSymbolDictionary target) {
        if (recoveredFrameAnalysis == null) {
            return baseline;
        }
        RecoveredFrameAnalysis analysis = checkedRecoveryAnalysis(baseline);
        long coverage = analysis.coverage();
        if (coverage >= 0L) {
            analysis.addDecodedSymbolsTo(target);
        }
        return coverage;
    }

    long recoveredSymbolCoverage(int baseline) {
        return checkedRecoveryAnalysis(baseline).coverage();
    }

    int recoveredSymbolSuffixCount(int baseline) {
        return checkedRecoveryAnalysis(baseline).rawCount();
    }

    int recoveredSymbolSuffixLen(int baseline) {
        return checkedRecoveryAnalysis(baseline).rawLen();
    }

    void copyRecoveredSymbolSuffix(int baseline, long target) {
        RecoveredFrameAnalysis analysis = checkedRecoveryAnalysis(baseline);
        int len = analysis.rawLen();
        if (len > 0) {
            io.questdb.client.std.Unsafe.getUnsafe().copyMemory(analysis.rawAddr(), target, len);
        }
    }

    void releaseRecoveredSymbolStorage() {
        if (recoveredFrameAnalysis != null) {
            recoveredFrameAnalysis.releaseRawStorage();
        }
    }

    @TestOnly
    public long recoveryFramesVisited() {
        return recoveredFrameAnalysis == null ? 0L : recoveredFrameAnalysis.framesVisited();
    }

    @TestOnly
    public long recoverySymbolEntriesVisited() {
        return recoveredFrameAnalysis == null ? 0L : recoveredFrameAnalysis.symbolEntriesVisited();
    }

    @TestOnly
    public int recoverySymbolNativeCapacity() {
        return recoveredFrameAnalysis == null ? 0 : recoveredFrameAnalysis.rawCapacity();
    }

    private RecoveredFrameAnalysis checkedRecoveryAnalysis(int baseline) {
        if (recoveredFrameAnalysis == null || recoveredFrameAnalysis.baseline() != baseline) {
            throw new IllegalStateException("recovery symbol baseline mismatch [expected="
                    + (recoveredFrameAnalysis == null ? "none" : recoveredFrameAnalysis.baseline())
                    + ", actual=" + baseline + ']');
        }
        return recoveredFrameAnalysis;
    }

    /**
     * Pass-through to {@link SegmentRing#findSegmentContaining(long)}.
     */
    public MmapSegment findSegmentContaining(long fsn) {
        return ring.findSegmentContaining(fsn);
    }

    /**
     * Pass-through to {@link SegmentRing#firstSealed()}.
     */
    public MmapSegment firstSealed() {
        return ring.firstSealed();
    }

    /**
     * The engine's persisted symbol dictionary, or {@code null} in memory mode
     * (and in disk mode if it failed to open). The producer appends new symbols
     * to it; recovery / orphan-drain read its loaded entries to seed catch-up.
     */
    public PersistedSymbolDict getPersistedSymbolDict() {
        return persistedSymbolDict;
    }

    /**
     * Number of times {@link #appendBlocking} hit
     * {@link SegmentRing#BACKPRESSURE_NO_SPARE} on its first attempt and
     * had to wait for the segment manager (or for ACKs) to free space.
     * One increment per blocking-call, not per spin-park. Cumulative.
     */
    public long getTotalBackpressureStalls() {
        return backpressureStallCount.get();
    }

    /**
     * Whether the sender may delta-encode symbol dictionaries on this engine.
     * Always true in memory mode (the send loop keeps an in-process catch-up
     * mirror). In disk mode it requires the persisted dictionary to have opened,
     * since delta frames are not self-sufficient and recovery / orphan-drain must
     * be able to rebuild the dictionary from disk. When false in disk mode the
     * sender falls back to full self-sufficient frames.
     */
    public boolean isDeltaDictEnabled() {
        return sfDir == null || persistedSymbolDict != null;
    }

    /**
     * Pass-through to {@link SegmentRing#nextSealedAfter(MmapSegment)}.
     */
    public MmapSegment nextSealedAfter(MmapSegment current) {
        return ring.nextSealedAfter(current);
    }

    /**
     * I/O thread accessor: highest FSN whose frame is fully written.
     */
    public long publishedFsn() {
        return ring.publishedFsn();
    }

    /**
     * I/O thread accessor: sealed segments waiting to drain. Direct view —
     * NOT thread-safe under producer-thread rotation. The I/O loop should
     * use {@link #sealedSegmentsSnapshot(MmapSegment[])} instead.
     */
    public ObjList<MmapSegment> sealedSegments() {
        return ring.getSealedSegments();
    }

    /**
     * Thread-safe snapshot pass-through to
     * {@link SegmentRing#snapshotSealedSegments(MmapSegment[])}. Returns
     * the count copied, or -1 if the buffer is too small.
     */
    public int sealedSegmentsSnapshot(MmapSegment[] target) {
        return ring.snapshotSealedSegments(target);
    }

    /**
     * Configured per-segment size in bytes.
     */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    public String sfDir() {
        return sfDir;
    }

    /**
     * True when this engine opened against a pre-existing on-disk slot
     * (i.e. {@code SegmentRing.openExisting} returned a non-null ring at
     * construction). Memory-mode engines and fresh-disk engines return
     * false. Used by the sender to decide whether to mark schema state as
     * needing a reset before the first send.
     */
    public boolean wasRecoveredFromDisk() {
        return wasRecoveredFromDisk;
    }

    /**
     * FSN of the last commit-bearing frame in a disk-recovered ring, or
     * {@code -1} for fresh/memory rings. Frames above it are an orphaned
     * deferred tail (transaction never committed) that the server will not
     * ack until a later commit-bearing frame covers them.
     */
    public long recoveredCommitBoundaryFsn() {
        return recoveredCommitBoundaryFsn;
    }

    /**
     * Highest symbol id any recovered delta frame references, or {@code -1} for
     * fresh/memory rings (and recovered rings with no symbol-bearing frame). A
     * resuming producer compares this against its recovered dictionary size to
     * detect a host-crash tear of the persisted {@code .symbol-dict}.
     */
    public long recoveredMaxSymbolId() {
        return recoveredMaxSymbolId;
    }

    /**
     * Highest {@code deltaStart} across the recovered committed frames; {@code 0} when every
     * surviving frame is self-sufficient (or none carries a dictionary at all).
     * <p>
     * The send loop uses this to decide whether it needs a catch-up: at zero, every frame
     * re-registers its dictionary from id 0 as it replays, so seeding the mirror -- and
     * shipping a catch-up frame off it -- would be pure redundancy. Above zero, at least one
     * frame's delta starts above ids it does not itself carry, so the mirror must hold those
     * ids before the replay begins or the server null-pads the hole.
     */
    public long recoveredMaxSymbolDeltaStart() {
        return recoveredMaxSymbolDeltaStart;
    }

    /**
     * FSN of the last frame of a recovered orphaned deferred tail, or
     * {@code -1} when none. See {@link #recoveredCommitBoundaryFsn()}: the
     * orphan range is {@code [recoveredCommitBoundaryFsn() + 1 .. this]}.
     */
    public long recoveredOrphanTipFsn() {
        return recoveredOrphanTipFsn;
    }

    /**
     * Retires a recovered deferred tail once every frame below it is ACKed.
     * The operation is local and idempotent: no wire sequence ever referred
     * to these aborted-transaction frames.
     *
     * @return true if no orphan tail remains, false if lower frames still need
     *         server ACKs
     */
    public boolean retireRecoveredOrphanTailIfReady() {
        long orphanTip = recoveredOrphanTipFsn;
        if (orphanTip < 0L) {
            return true;
        }
        long orphanStart = recoveredCommitBoundaryFsn + 1L;
        if (ackedFsn() < orphanStart - 1L) {
            return false;
        }
        LOG.warn("retiring orphaned deferred tail: {} frame(s) [fsn {}..{}] belong to a transaction "
                        + "whose commit was never published; aborting them (never transmitted, slots trimmed)",
                orphanTip - orphanStart + 1L, orphanStart, orphanTip);
        acknowledge(orphanTip);
        recoveredOrphanTipFsn = -1L;
        return true;
    }

    /**
     * Unlinks every {@code .sfa} file under {@code dir}. Called only on
     * clean shutdown when the ring confirms every published FSN has been
     * acked — at that moment the slot has no recoverable work and the
     * files are pure noise that would mislead the next sender's recovery.
     * Best-effort: logs and continues on failures, since we're already on
     * the close path.
     */
    private static void unlinkAllSegmentFiles(String dir) {
        if (!io.questdb.client.std.Files.exists(dir)) return;
        long find = io.questdb.client.std.Files.findFirst(dir);
        if (find < 0) {
            LOG.warn("close-time unlink could not enumerate {}; "
                    + "any residual sf-*.sfa files will be picked up by the next recovery", dir);
            return;
        }
        if (find == 0) return;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = io.questdb.client.std.Files.utf8ToString(
                        io.questdb.client.std.Files.findName(find));
                rc = io.questdb.client.std.Files.findNext(find);
                if (name == null || !name.endsWith(".sfa")) continue;
                String path = dir + "/" + name;
                if (!io.questdb.client.std.Files.remove(path)) {
                    LOG.warn("Failed to unlink fully-acked segment {} on close", path);
                }
            }
        } finally {
            io.questdb.client.std.Files.findClose(find);
        }
    }
}
