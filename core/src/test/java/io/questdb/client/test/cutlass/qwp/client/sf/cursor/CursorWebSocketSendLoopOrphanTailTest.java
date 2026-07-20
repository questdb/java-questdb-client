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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Orphaned deferred tail containment (the skip-and-self-ack replay policy).
 * <p>
 * A producer that crashes (or closes) mid-transaction leaves FLAG_DEFER_COMMIT
 * frames with no covering commit frame at the top of its disk-recovered SF
 * log. Those frames belong to a transaction that was never committed -- it is
 * aborted by definition, and replaying them to the server would let a
 * commit-bearing frame from the NEW session commit them, resurrecting a
 * partial transaction and violating {@code transaction=on} atomicity.
 * <p>
 * Contract under test:
 * <ul>
 *   <li>Recovery detects the tail: {@code recoveredCommitBoundaryFsn} is the
 *       last commit-bearing frame, {@code recoveredOrphanTipFsn} the tail's
 *       top (or -1 when the log ends with a commit frame).</li>
 *   <li>Fast path: when everything below the tail is already acked
 *       (trivially so when the whole log is the tail), the loop retires the
 *       tail via cumulative self-acknowledge before any frame is sent --
 *       zero wire cost, no reconnect.</li>
 *   <li>Slow path: committed-covered frames below the tail replay first; the
 *       cursor never enters the tail; once the server acks the boundary the
 *       tail retires and the connection recycles exactly once so the linear
 *       wireSeq&lt;-&gt;FSN mapping re-anchors past the gap. Frames appended
 *       by the new session then flow with correct ack attribution.</li>
 * </ul>
 */
public class CursorWebSocketSendLoopOrphanTailTest {

    private static final int FLAG_DEFER_COMMIT = 0x01;
    private static final int FLAG_DELTA_SYMBOL_DICT = 0x08;
    private static final int HEADER_OFFSET_FLAGS = 5;
    private static final int HEADER_SIZE = 12;
    private static final int MAGIC_MESSAGE = 0x31505751; // "QWP1" little-endian

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-orphan-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testRecoveryDetectsOrphanedDeferredTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, false); // fsn 0: commit-bearing
                appendFrame(engine, true);  // fsn 1: deferred
                appendFrame(engine, true);  // fsn 2: deferred -- orphan tail [1..2]
            }
            try (CursorSendEngine engine = newEngine()) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertEquals("last commit-bearing frame", 0L, engine.recoveredCommitBoundaryFsn());
                assertEquals("orphan tail tip", 2L, engine.recoveredOrphanTipFsn());
            }
        });
    }

    @Test
    public void testRecoveryReportsNoOrphansWhenLogEndsWithCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, true);  // fsn 0: deferred
                appendFrame(engine, true);  // fsn 1: deferred
                appendFrame(engine, false); // fsn 2: the group-closing commit frame
            }
            try (CursorSendEngine engine = newEngine()) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertEquals("commit frame at the top: whole log is commit-covered",
                        2L, engine.recoveredCommitBoundaryFsn());
                assertEquals("no orphan tail", -1L, engine.recoveredOrphanTipFsn());
            }
        });
    }

    @Test
    public void testFastPathRetiresWholeDeferredLogBeforeAnySend() throws Exception {
        // The whole recovered log is one uncommitted deferred group
        // (boundary = -1): nothing below the tail needs acks, so the tail
        // retires at start() before any connection exists. No frame may ever
        // reach the wire and no reconnect may occur.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, true);
                appendFrame(engine, true);
                appendFrame(engine, true);
            }
            try (CursorSendEngine engine = newEngine()) {
                assertEquals(-1L, engine.recoveredCommitBoundaryFsn());
                assertEquals(2L, engine.recoveredOrphanTipFsn());

                List<AckingClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, clients);
                try {
                    loop.start();
                    awaitAckedFsn(engine, 2L);
                    assertEquals("orphaned deferred frames must never be transmitted",
                            0, totalSent(clients));
                    // The retirement happens in start() on the user thread; the
                    // I/O thread's initial connect may or may not have run yet.
                    // Either way there must never be a RE-connect.
                    assertTrue("fast path must not need more than the initial connection",
                            clientCount(clients) <= 1);
                } finally {
                    loop.close();
                }
            }
        });
    }

    /**
     * A whole-deferred slot that ALSO ships a dictionary catch-up must still retire its
     * tail at start(), before any connection work, and must not reconnect.
     * <p>
     * testFastPathRetiresWholeDeferredLogBeforeAnySend covers the same shape without a
     * dictionary, so nothing pinned that adding a catch-up -- which consumes wire
     * sequences before any data frame -- leaves the start()-time retirement intact.
     * <p>
     * This deliberately does NOT pin trySendOne's in-place re-anchor arm: both connection
     * setup sites call tryRetireOrphanTail first, so that arm is only reached when frames
     * below the tail still needed acks -- which means they were sent on this connection.
     * Verified by reverting its guard: this test still passes.
     */
    @Test
    public void testCatchUpBearingWholeDeferredSlotRetiresAtStartWithoutReconnecting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, true);
                appendFrame(engine, true);
                appendFrame(engine, true);
            }
            // A populated dictionary is what makes the loop ship a catch-up. The frames
            // carry no symbols, so recoveredMaxSymbolId stays -1 and the full-dict
            // discard cannot fire on it.
            try (PersistedSymbolDict pd = PersistedSymbolDict.openClean(tmpDir)) {
                assertNotNull(pd);
                pd.appendSymbol("a");
                pd.appendSymbol("b");
            }

            try (CursorSendEngine engine = newEngine()) {
                assertEquals(-1L, engine.recoveredCommitBoundaryFsn());
                assertEquals(2L, engine.recoveredOrphanTipFsn());

                List<AckingClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, clients);
                try {
                    loop.start();
                    assertEquals("scaffolding: the mirror must be seeded, so a catch-up ships",
                            2, loop.sentDictCount());
                    awaitAckedFsn(engine, 2L);
                    assertTrue("retiring after only a catch-up must re-anchor in place, "
                                    + "not reconnect",
                            clientCount(clients) <= 1);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRecoveryReleasesAbortedTailSymbolStorage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeltaSymbolFrame(engine, 0, 'a');
                appendLargeDeferredDeltaSymbolFrame(engine, 1, 8_000);
            }
            try (CursorSendEngine engine = newEngine()) {
                assertEquals("native recovery storage must be trimmed to the committed prefix",
                        2, engine.recoverySymbolNativeCapacity());
                assertEquals("large deferred frame is the aborted tail",
                        1L, engine.recoveredOrphanTipFsn());
            }
        });
    }

    @Test
    public void testSlowPathReplaysBelowTailThenRetiresAndRecyclesOnce() throws Exception {
        // fsn 0 is commit-covered and unacked: it must replay. fsns 1..2 are
        // the orphan tail: never transmitted. Once the server acks fsn 0 the
        // tail retires and the connection recycles exactly once (wire-seq
        // realignment). Frames appended by the new session then flow with
        // correct cumulative-ack attribution across the FSN gap.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, false); // fsn 0
                appendFrame(engine, true);  // fsn 1
                appendFrame(engine, true);  // fsn 2
            }
            try (CursorSendEngine engine = newEngine()) {
                assertEquals(0L, engine.recoveredCommitBoundaryFsn());
                assertEquals(2L, engine.recoveredOrphanTipFsn());
                assertEquals("recovery must not pre-ack anything", -1L, engine.ackedFsn());

                List<AckingClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, clients);
                try {
                    loop.start();
                    // Replay fsn 0, server acks it, tail retires.
                    awaitAckedFsn(engine, 2L);
                    assertEquals("only the commit-covered frame below the tail may be sent",
                            1, totalSent(clients));
                    // Wire-seq realignment: exactly one recycle beyond the
                    // initial connection.
                    awaitClientCount(clients, 2);
                    assertEquals(2, clientCount(clients));

                    // New-session traffic flows across the FSN gap with
                    // correct ack attribution: fsn 3 is wireSeq 0 on the new
                    // connection, and its cumulative ack lands on fsn 3.
                    appendFrame(engine, false); // fsn 3
                    awaitAckedFsn(engine, 3L);
                    assertEquals("fsn 0 before the recycle + fsn 3 after it",
                            2, totalSent(clients));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRecoveredMaxSymbolIdSpansSealedSegments() throws Exception {
        // The torn-dict detector must walk the SEALED segments too, not just the active
        // one. Every other test in the suite fits its whole slot in a single active
        // segment, so the sealed walk in SegmentRing.maxSymbolDeltaEnd was dead code as
        // far as the tests were concerned: deleting it left them all green while
        // recoveredMaxSymbolId silently collapsed to -1.
        //
        // -1 is not a benign wrong answer. seedGlobalDictionaryFromPersisted's guard is
        // `recoveredMaxSymbolId >= pd.size()`, so a value that is too LOW never fires: a
        // torn dictionary is trusted, the producer resumes seeded from the short
        // dictionary, and it hands ids the surviving frames already define to different
        // symbols -- the exact silent misattribution the whole feature exists to prevent.
        //
        // The shape that needs the sealed walk is the crash the guard was built for. In a
        // plain multi-segment slot the ACTIVE segment holds the newest frames and thus the
        // highest ids, so an active-only walk happens to get the right answer. It stops
        // being right when the active segment contributes NOTHING to the committed range
        // -- here, a producer that died mid-transaction, leaving an aborted deferred tail
        // long enough to overflow into a fresh segment. maxSymbolDeltaEnd skips those
        // frames (fsn > recoveredCommitBoundaryFsn), so the highest COMMITTED id is left
        // sitting in a SEALED segment, reachable only through the sealed walk.
        TestUtils.assertMemoryLeak(() -> {
            // Small segments so the tail really does roll one. Each frame is
            // FRAME_HEADER_SIZE + (QWP HEADER_SIZE + 2) = 22 bytes, against 256 - 24
            // usable, so a segment holds ~10.
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 256)) {
                // Committed delta frames registering ids 0,1,2 -- the highest COMMITTED
                // ids in the slot.
                for (int i = 0; i < 3; i++) {
                    appendDeltaFrame(engine, false, i, 1);
                }
                assertNull("the committed frames must still be in the ACTIVE segment here",
                        engine.firstSealed());
                // The aborted transaction: deferred frames with no covering commit. Keep
                // appending until they overflow the segment, then a few more, so the
                // committed frames end up SEALED and the active segment holds nothing but
                // the tail. Ids stay small so the test's one-byte varint encoding holds.
                int deferredId = 3;
                for (int i = 0; i < 64 && engine.firstSealed() == null; i++) {
                    appendDeltaFrame(engine, true, deferredId++, 1);
                }
                assertNotNull("the deferred tail must have rolled a segment", engine.firstSealed());
                for (int i = 0; i < 3; i++) {
                    appendDeltaFrame(engine, true, deferredId++, 1);
                }
            }
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 256)) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertNotNull("the recovered slot must have a sealed segment", engine.firstSealed());
                assertEquals("the last commit-bearing frame is fsn 2",
                        2L, engine.recoveredCommitBoundaryFsn());
                // ids 0,1,2 were introduced by the committed frames, all of which now live
                // in a SEALED segment. The active segment holds only the deferred tail,
                // which the walk skips -- so an active-only walk returns 0 and this comes
                // back -1.
                assertEquals("the highest COMMITTED id lives in a SEALED segment; "
                                + "maxSymbolDeltaEnd must walk the sealed segments to see it",
                        2L, engine.recoveredMaxSymbolId());
            }
        });
    }

    @Test
    public void testRecoveredMaxSymbolIdExcludesOrphanTailFrames() throws Exception {
        // recoveredMaxSymbolId must reflect only COMMITTED (transmitted) frames, not
        // the aborted orphan-tail frames trySendOne retires without ever sending. A
        // host crash that tears the persisted dictionary down to the committed ids
        // while an orphan-tail frame introduced a HIGHER id must NOT over-reject the
        // resume: the producer never reuses an orphan id on the wire (the tail retires
        // first), so counting it would inflate recoveredMaxSymbolId and make
        // seedGlobalDictionaryFromPersisted fail a fully-recoverable slot. The
        // maxSymbolDeltaEnd walk is therefore bounded to recoveredCommitBoundaryFsn.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                // fsn 0: commit-bearing delta frame registering ids 0,1.
                appendDeltaFrame(engine, false, 0, 2);
                // fsn 1: DEFERRED delta frame registering id 2 -- the orphan tail.
                appendDeltaFrame(engine, true, 2, 1);
            }
            try (CursorSendEngine engine = newEngine()) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertEquals("last commit-bearing frame", 0L, engine.recoveredCommitBoundaryFsn());
                assertEquals("orphan tail tip", 1L, engine.recoveredOrphanTipFsn());
                // Only the committed frame's ids (0,1) count -> highest id 1. The
                // orphan-tail frame's id 2 is excluded, so a resume whose recovered
                // dictionary holds ids 0,1 (size 2) is NOT over-rejected.
                assertEquals("orphan-tail id 2 must be excluded from recoveredMaxSymbolId",
                        1L, engine.recoveredMaxSymbolId());
            }
        });
    }

    @Test
    public void testZeroCountDeltaFrameAnchorsRecoveredMaxSymbolIdAtItsBaseline() throws Exception {
        // A committed delta frame that introduces NO new symbol (deltaCount == 0 -- a
        // commit frame, or one whose rows only re-use existing ids) still carries
        // deltaStart == the producer's baseline at encode time, because beginMessage
        // ALWAYS sets FLAG_DELTA_SYMBOL_DICT. maxSymbolDeltaEnd counts it as
        // deltaStart + deltaCount == deltaStart (NOT 0), so recoveredMaxSymbolId ==
        // deltaStart - 1 even though the frame introduces nothing.
        //
        // This is the mechanism behind the torn-dict guard's deliberate CONSERVATIVE
        // over-strand (see seedGlobalDictionaryFromPersisted): if a host crash tears
        // the persisted dictionary below such a frame's baseline while its
        // symbol-introducing predecessors were already acked and trimmed, both the
        // seed-time guard (recoveredMaxSymbolId >= pd.size()) and the drainer's replay
        // guard (deltaStart > sentDictCount) fire and quarantine the slot -- fail-clean
        // "resend required" -- even though the frame's rows may reference only ids the
        // truncated dictionary still holds.
        //
        // Counting the zero-count frame's baseline is load-bearing SAFETY: a "fix" that
        // skipped zero-count frames (returning 0 for them) would UNDER-strand and let a
        // genuinely torn dictionary through, silently shifting the dense id map. This
        // pins that a zero-count delta frame is anchored at its baseline, not skipped.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                // fsn 0: commit-bearing delta frame that genuinely registers ids 0..4.
                appendDeltaFrame(engine, false, 0, 5);
                // fsn 1: commit-bearing delta frame with deltaStart 10, deltaCount 0 --
                // introduces NOTHING, but its baseline (10) sits ABOVE every id any
                // surviving frame actually introduces (max 4). Models a commit /
                // symbol-reusing frame emitted after ids 5..9 were registered by
                // predecessor frames that have since been acked and trimmed away.
                appendDeltaFrame(engine, false, 10, 0);
            }
            try (CursorSendEngine engine = newEngine()) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertEquals("both frames are commit-bearing", 1L, engine.recoveredCommitBoundaryFsn());
                // The zero-count frame drives recoveredMaxSymbolId to 9 (its baseline
                // 10, minus 1), NOT to 4 (the highest id any surviving frame actually
                // introduces) and NOT to 0 (which skipping it would yield). This
                // inflation is exactly what makes seedGlobalDictionaryFromPersisted
                // over-reject a dictionary holding ids 0..4 (size 5).
                assertEquals("a zero-count delta frame anchors recoveredMaxSymbolId at its baseline-1",
                        9L, engine.recoveredMaxSymbolId());
            }
        });
    }

    @Test
    public void testRecoveryScansFramesOnceAndReusesCachedSymbolSuffix() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeltaSymbolFrame(engine, 0, 'a');
                appendDeltaSymbolFrame(engine, 1, 'b');
                appendDeltaSymbolFrame(engine, 2, 'c');
            }

            try (CursorSendEngine engine = newEngine()) {
                assertEquals("one recovery visit per frame", 3L, engine.recoveryFramesVisited());

                GlobalSymbolDictionary first = new GlobalSymbolDictionary();
                GlobalSymbolDictionary second = new GlobalSymbolDictionary();
                assertEquals(3L, engine.addRecoveredSymbolsTo(0, first));
                assertEquals(3L, engine.addRecoveredSymbolsTo(0, second));
                assertEquals("a", first.getSymbol(0));
                assertEquals("c", second.getSymbol(2));
                assertEquals("producer seed reads must reuse the recovery result",
                        3L, engine.recoveryFramesVisited());

                CursorWebSocketSendLoop loop = newLoop(engine, new ArrayList<>());
                try {
                    assertEquals("send-loop construction must copy the cached native suffix",
                            3L, engine.recoveryFramesVisited());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRecoverySkipsEntriesAlreadyCoveredByPersistedPrefix() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                PersistedSymbolDict pd = engine.getPersistedSymbolDict();
                assertNotNull(pd);
                pd.appendSymbol("a");
                pd.appendSymbol("b");
                pd.appendSymbol("c");
                appendDeltaSymbolFrame(engine, 0, 'a');
                appendDeltaSymbolFrame(engine, 1, 'b');
                appendDeltaSymbolFrame(engine, 2, 'c');
            }

            try (CursorSendEngine engine = newEngine()) {
                assertEquals("covered delta payloads must not be parsed entry-by-entry",
                        0L, engine.recoverySymbolEntriesVisited());
                GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
                PersistedSymbolDict pd = engine.getPersistedSymbolDict();
                assertNotNull(pd);
                pd.addLoadedSymbolsTo(recovered);
                assertEquals(3L, engine.addRecoveredSymbolsTo(recovered.size(), recovered));
                assertEquals("direct decode must not duplicate the covered frame entries",
                        3, recovered.size());
                assertEquals("c", recovered.getSymbol(2));
            }
        });
    }

    /**
     * A gap-reset inside an UNCOMMITTED deferred tail must not publish the committed
     * snapshot's counts over the tail's bytes.
     * <p>
     * accept() checkpoints committedRawLen/Count only at a commit-bearing frame, while
     * foldDelta's gap-reset rewinds runningRawLen/Count to 0 without touching them. The
     * next appendRaw therefore overwrites [0, committedRawLen) IN PLACE. That is harmless
     * when a commit-bearing frame follows -- it re-checkpoints -- but a reset in the
     * deferred tail never gets that refresh, and finish() then hands out the OLD counts
     * over the TAIL's bytes.
     * <p>
     * Here fsn 0 commits symbol 'a' (committedRawCount=1 over 2 bytes), fsn 1 is a DEFERRED
     * gap that is durably acked (so the reset is permitted), and fsn 2 is a DEFERRED
     * self-sufficient frame carrying 'b' -- which rewinds and overwrites 'a' in place. No
     * commit-bearing frame follows. Without the guard, recovery reports coverage 1 and
     * decodes "b" as id 0, while the frame that actually replays registered "a": silent
     * symbol misattribution. With it, recovery fails clean.
     */
    @Test
    public void testGapResetInsideTheDeferredTailFailsCleanInsteadOfMisattributing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeltaSymbolFrame(engine, 0, 'a');            // fsn 0, commit-bearing
                appendDeltaFrame(engine, true, 5, 0);              // fsn 1, deferred gap
                appendDeferredDeltaSymbolFrame(engine, 0, 'b');    // fsn 2, deferred reset
            }
            // Ack through the gap so the reset at fsn 2 is permitted (an UNACKED gap is
            // latched instead -- testSelfSufficientFrameCannotHideUnackedRecoveryGap).
            try (AckWatermark watermark = AckWatermark.open(tmpDir)) {
                assertNotNull(watermark);
                watermark.write(1L);
            }

            try (CursorSendEngine engine = newEngine()) {
                assertEquals(1L, engine.ackedFsn());
                GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
                assertEquals("a rewind with no following commit must fail clean, not publish "
                                + "the committed counts over the tail's bytes",
                        -1L, engine.addRecoveredSymbolsTo(0, recovered));
                assertEquals("nothing may be recovered from an overwritten snapshot",
                        0, recovered.size());
            }
        });
    }

    /**
     * A torn DELTA slot must KEEP its dictionary: the full-dict-fallback discard is
     * gated on {@code recoveredMaxSymbolDeltaStart == 0L} and that conjunct is
     * load-bearing in the negative direction.
     * <p>
     * Here the side-file holds [a,b] and the surviving frames register c@2 and d@3 --
     * so the frames out-reach the dictionary (recoveredMaxSymbolId 3 >= size 2, the
     * other conjunct) but are NOT self-sufficient. Drop the deltaStart conjunct and
     * recovery discards the only source of ids 0..1, re-folds at baseline 0, finds
     * deltaStart 2 above a coverage of 0, marks a gap and quarantines a slot that is
     * perfectly recoverable.
     * <p>
     * Every existing candidate passes either way: the torn-dict tests have frames
     * covering from id 0, and testRecoverySkipsEntriesAlreadyCoveredByPersistedPrefix
     * fails the size conjunct first, so neither one is load-bearing there.
     */
    @Test
    public void testTornDeltaSlotKeepsItsDictionaryInsteadOfBeingDiscarded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeltaSymbolFrame(engine, 2, 'c');
                appendDeltaSymbolFrame(engine, 3, 'd');
            }
            try (PersistedSymbolDict pd = PersistedSymbolDict.openClean(tmpDir)) {
                assertNotNull(pd);
                pd.appendSymbol("a");
                pd.appendSymbol("b");
            }

            try (CursorSendEngine engine = newEngine()) {
                assertNotNull("a torn DELTA slot must keep its dictionary -- its frames "
                                + "cannot rebuild the ids below their own delta start",
                        engine.getPersistedSymbolDict());
                assertEquals("scaffolding: the frames must out-reach the dictionary, so the "
                                + "OTHER discard conjunct is satisfied and this test really "
                                + "pins the deltaStart one",
                        3L, engine.recoveredMaxSymbolId());
                assertEquals(2, engine.getPersistedSymbolDict().size());

                GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
                assertEquals("recovery must cover ids 0..3, not fail clean",
                        4L, engine.addRecoveredSymbolsTo(2, recovered));
                assertEquals("only the suffix above the persisted prefix is added",
                        2, recovered.size());
                assertEquals("c", recovered.getSymbol(0));
                assertEquals("d", recovered.getSymbol(1));
            }
        });
    }

    @Test
    public void testSelfSufficientFrameRepairsAckedRecoveryGap() throws Exception {
        // fsn 0 models the tail of an old delta epoch whose registering frames
        // have already been trimmed: with an empty persisted dictionary its
        // deltaStart=1 is a gap. It is durably ACKed, so it will not replay.
        // fsn 1 starts a new, self-sufficient epoch from id 0 and is the first
        // frame that WILL replay. Recovery must use that full frame as the new
        // source of truth instead of permanently latching the earlier gap.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeltaFrame(engine, false, 1, 0);
                appendDeltaSymbolFrame(engine, 0, 'a');
            }
            try (AckWatermark watermark = AckWatermark.open(tmpDir)) {
                assertNotNull(watermark);
                watermark.write(0L);
            }

            try (CursorSendEngine engine = newEngine()) {
                assertEquals(0L, engine.ackedFsn());
                GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
                assertEquals("the full frame must re-anchor replay coverage",
                        1L, engine.addRecoveredSymbolsTo(0, recovered));
                assertEquals(1, recovered.size());
                assertEquals("a", recovered.getSymbol(0));
            }
        });
    }

    @Test
    public void testSelfSufficientFrameCannotHideUnackedRecoveryGap() throws Exception {
        // Safety twin: when the gapped frame itself is unacked, it reaches the
        // wire before the later full frame. Recovery must keep the gap latched
        // and quarantine rather than pretending the later reset repairs the
        // invalid replay order.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeltaFrame(engine, false, 1, 0);
                appendDeltaSymbolFrame(engine, 0, 'a');
            }
            try (CursorSendEngine engine = newEngine()) {
                GlobalSymbolDictionary recovered = new GlobalSymbolDictionary();
                assertEquals("an unacked gap remains unreplayable",
                        -1L, engine.addRecoveredSymbolsTo(0, recovered));
                assertEquals(0, recovered.size());
            }
        });
    }

    // ---------------------------------------------------------------------
    // harness
    // ---------------------------------------------------------------------

    /**
     * In-memory transport emulating a healthy server: counts sends and
     * answers every received frame with a cumulative empty-table STATUS_OK
     * for the highest wire seq seen. sendBinary and tryReceiveFrame both run
     * on the I/O thread; sentCount is volatile for test-thread assertions.
     */
    private static final class AckingClient extends WebSocketClient {
        private int ackedUpTo;
        private volatile int sentCount;

        AckingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sentCount++;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            int sent = sentCount;
            if (sent <= ackedUpTo) {
                return false;
            }
            ackedUpTo = sent;
            // STATUS_OK frame: status(1) + sequence(8) + tableCount(2)
            int size = 11;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
                Unsafe.getUnsafe().putLong(ptr + 1, sent - 1); // cumulative wire seq
                Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);
                handler.onBinaryMessage(ptr, size);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
            return true;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }

    private static void appendFrame(CursorSendEngine engine, boolean defer) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 'x');
            }
            // The recovery walk only classifies frames that positively parse
            // as QWP messages, so write the real magic + flags byte.
            Unsafe.getUnsafe().putInt(buf, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + HEADER_OFFSET_FLAGS,
                    (byte) (defer ? FLAG_DEFER_COMMIT : 0));
            engine.appendBlocking(buf, 16);
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // Appends a QWP frame carrying a symbol-dict delta section ([deltaStart varint]
    // [deltaCount varint]) so the recovery walk's maxSymbolDeltaEnd counts it.
    // deltaStart/deltaCount stay < 128 so each encodes in a single LEB128 byte.
    private static void appendDeltaFrame(CursorSendEngine engine, boolean defer, int deltaStart, int deltaCount) {
        int size = HEADER_SIZE + 2;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
            }
            Unsafe.getUnsafe().putInt(buf, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + HEADER_OFFSET_FLAGS,
                    (byte) (FLAG_DELTA_SYMBOL_DICT | (defer ? FLAG_DEFER_COMMIT : 0)));
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE, (byte) deltaStart);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 1, (byte) deltaCount);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void appendDeferredDeltaSymbolFrame(
            CursorSendEngine engine, int deltaStart, char symbol) {
        int size = HEADER_SIZE + 4;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
            }
            Unsafe.getUnsafe().putInt(buf, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + HEADER_OFFSET_FLAGS,
                    (byte) (FLAG_DELTA_SYMBOL_DICT | FLAG_DEFER_COMMIT));
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE, (byte) deltaStart);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 1, (byte) 1);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 2, (byte) 1);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 3, (byte) symbol);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void appendDeltaSymbolFrame(CursorSendEngine engine, int deltaStart, char symbol) {
        int size = HEADER_SIZE + 4; // start, count=1, symbolLen=1, symbol byte
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
            }
            Unsafe.getUnsafe().putInt(buf, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + HEADER_OFFSET_FLAGS, (byte) FLAG_DELTA_SYMBOL_DICT);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE, (byte) deltaStart);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 1, (byte) 1);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 2, (byte) 1);
            Unsafe.getUnsafe().putByte(buf + HEADER_SIZE + 3, (byte) symbol);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void appendLargeDeferredDeltaSymbolFrame(
            CursorSendEngine engine,
            int deltaStart,
            int symbolLen
    ) {
        int size = HEADER_SIZE
                + NativeBufferWriter.varintSize(deltaStart)
                + NativeBufferWriter.varintSize(1)
                + NativeBufferWriter.varintSize(symbolLen)
                + symbolLen;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
            }
            Unsafe.getUnsafe().putInt(buf, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + HEADER_OFFSET_FLAGS,
                    (byte) (FLAG_DEFER_COMMIT | FLAG_DELTA_SYMBOL_DICT));
            long p = NativeBufferWriter.writeVarint(buf + HEADER_SIZE, deltaStart);
            p = NativeBufferWriter.writeVarint(p, 1);
            p = NativeBufferWriter.writeVarint(p, symbolLen);
            Unsafe.getUnsafe().setMemory(p, symbolLen, (byte) 'z');
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void awaitAckedFsn(CursorSendEngine engine, long target) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (engine.ackedFsn() < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for ackedFsn", target, engine.ackedFsn());
            }
            Thread.sleep(1);
        }
    }

    private static void awaitClientCount(List<AckingClient> clients, int target) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (clientCount(clients) < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for reconnect", target, clientCount(clients));
            }
            Thread.sleep(1);
        }
    }

    private static int clientCount(List<AckingClient> clients) {
        synchronized (clients) {
            return clients.size();
        }
    }

    private static int totalSent(List<AckingClient> clients) {
        synchronized (clients) {
            int total = 0;
            for (AckingClient c : clients) {
                total += c.sentCount;
            }
            return total;
        }
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    private CursorWebSocketSendLoop newLoop(CursorSendEngine engine, List<AckingClient> clients) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    AckingClient c = new AckingClient();
                    synchronized (clients) {
                        clients.add(c);
                    }
                    return c;
                },
                1L, 5L);
    }
}
