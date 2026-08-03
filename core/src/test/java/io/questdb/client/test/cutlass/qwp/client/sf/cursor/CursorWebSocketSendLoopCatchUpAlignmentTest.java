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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Guards the reconnect/failover symbol-dictionary catch-up ACK alignment in
 * {@link CursorWebSocketSendLoop#setWireBaselineWithCatchUp}.
 * <p>
 * On a fresh connection the loop re-registers the whole dictionary with a
 * catch-up frame BEFORE replaying data frames. Each catch-up frame consumes a
 * wire sequence, so the loop anchors {@code fsnAtZero = replayStart - catchUpFrames}
 * to keep every catch-up frame mapped to an already-acked FSN. Dropping the
 * {@code - catchUpFrames} term is silent data loss: a server ACK for a catch-up
 * frame then translates through {@code engine.acknowledge(fsnAtZero + wireSeq)}
 * to an FSN at or above {@code replayStart}, trimming a not-yet-delivered data
 * frame from the store-and-forward log.
 * <p>
 * The loop is constructed but never {@link CursorWebSocketSendLoop#start started};
 * the catch-up runs against a stub {@link WebSocketClient} that counts frames, and
 * the OK is delivered straight into the inner {@code ResponseHandler} -- the same
 * white-box idiom {@code CursorWebSocketSendLoopDurableAckTest} uses, because
 * {@code setWireBaselineWithCatchUp} and the wire ports have no public entry point.
 * {@link CursorSendEngine#ackedFsn()} is the authoritative trim watermark asserted
 * against.
 */
public class CursorWebSocketSendLoopCatchUpAlignmentTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-cursor-catchup-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test
    public void testCatchUpFrameAckDoesNotAdvanceTrimWatermark() throws Exception {
        // Single catch-up frame (server advertises no cap). Two frames were
        // acked before the reconnect (ackedFsn=1), FSN 2 is unacked. The catch-up
        // frame's OK must NOT advance the watermark past 1 -- it carries no data,
        // only the dictionary the fresh server needs before replay.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0); // 0 => no cap => one frame
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 3);            // FSN 0,1,2 published
                engine.acknowledge(1);              // ackedFsn=1 => replayStart=2, FSN 2 still unacked
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "s0", "s1");   // sentDictCount=2 => catch-up fires
                    long replayStart = engine.ackedFsn() + 1L; // = 2

                    invokeSetWireBaselineWithCatchUp(loop, replayStart);

                    assertEquals("whole dictionary fits one frame under no cap",
                            1, client.framesSent);

                    // Behavioural (the harm): the catch-up frame (wire seq 0) is
                    // OK'd by the fresh server. It carries no data, so it must
                    // resolve to an already-acked FSN and leave the trim watermark
                    // untouched -- advancing it would trim the undelivered FSN 2.
                    deliverOk(loop, 0);
                    assertEquals("catch-up frame ACK must not advance the trim watermark "
                                    + "(would trim an undelivered data frame -> silent data loss)",
                            1L, engine.ackedFsn());
                    // Mechanism: the catch-up frames are anchored below replayStart.
                    assertEquals("fsnAtZero must be anchored catchUpFrames below replayStart",
                            replayStart - client.framesSent, loop.fsnAtZero());
                } finally {
                    loop.close(); // frees the seeded mirror + the stub client's buffers
                }
            }
        });
    }

    @Test
    public void testSplitCatchUpFramesAcksDoNotAdvanceTrimWatermark() throws Exception {
        // A small advertised cap splits the dictionary across several catch-up
        // frames, so the fsnAtZero offset must subtract the full frame count. Ack
        // the LAST catch-up wire sequence: it still maps below replayStart. With
        // the offset dropped it would translate to replayStart+1 and over-trim.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(40); // budget 12 => one 11-byte symbol per frame
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 5);            // FSN 0..4 published
                engine.acknowledge(2);              // ackedFsn=2 => replayStart=3, FSN 3,4 unacked
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "symbol0000", "symbol0001"); // 11 bytes each -> two frames
                    long replayStart = engine.ackedFsn() + 1L; // = 3

                    invokeSetWireBaselineWithCatchUp(loop, replayStart);

                    assertEquals("cap must split the two symbols across two frames",
                            2, client.framesSent);

                    // ACK the highest catch-up wire sequence (the last catch-up
                    // frame). It too must map below replayStart -- with the offset
                    // dropped it translates to replayStart+1 and over-trims.
                    deliverOk(loop, client.framesSent - 1);
                    assertEquals("no catch-up frame ACK may advance the trim watermark",
                            2L, engine.ackedFsn());
                    assertEquals("fsnAtZero must subtract the full split frame count",
                            replayStart - client.framesSent, loop.fsnAtZero());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testSplitCatchUpStagesOnlyPrefixAcrossReconnects() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(3_100);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    String symX = TestUtils.repeat("x", 3_000);
                    String symY = TestUtils.repeat("y", 3_000);
                    seedMirror(loop, symX, symY);

                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the small cap must split the dictionary", 2, client.framesSent);
                    assertEquals("catch-up must send the symbol bytes as a second payload slice",
                            2, client.multipartFramesSent);
                    assertEquals("the split chunks need one small prefix buffer",
                            1, loop.catchUpFrameGrowthCount());
                    // Splitting is only correct if the chunks reassemble gap-free.
                    assertCatchUpReassembles(client, symX, symY);

                    client.cap = 7_000;
                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the larger cap must combine the dictionary", 3, client.framesSent);
                    assertEquals("combining symbols must not grow the prefix-only buffer",
                            1, loop.catchUpFrameGrowthCount());
                    assertCatchUpReassembles(client, symX, symY);

                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the next reconnect sends one combined frame", 4, client.framesSent);
                    assertEquals("the prefix buffer must be reused across reconnects",
                            1, loop.catchUpFrameGrowthCount());
                    assertCatchUpReassembles(client, symX, symY);
                } finally {
                    // assertMemoryLeak verifies that close releases the retained buffer.
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testSplitCatchUpChunksTileTheDictionaryExactly() throws Exception {
        // Three chunks rather than two: a start-id error that happens to cancel
        // out across a single boundary cannot survive two of them, and only a
        // multi-chunk walk exercises the accumulating "chunkStartId +=
        // chunkSymbols" drift. Six 100-byte symbols under a cap that fits two
        // per frame; the reassembly must return all six, in order, with no null.
        TestUtils.assertMemoryLeak(() -> {
            String[] symbols = new String[6];
            for (int i = 0; i < symbols.length; i++) {
                symbols[i] = TestUtils.repeat(String.valueOf((char) ('a' + i)), 100);
            }
            CatchUpCapturingClient client = new CatchUpCapturingClient(250);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, symbols);
                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the cap must force a three-way split", 3, client.framesSent);
                    assertCatchUpReassembles(client, symbols);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testEmptyDictionaryReconnectSendsNoCatchUpFrame() throws Exception {
        // The sentDictCount > 0 half of setWireBaselineWithCatchUp's gate. Every
        // other test here seeds at least one symbol, so the empty-dictionary
        // branch -- a delta-enabled connection that has not registered anything
        // yet, i.e. every reconnect before the first symbol -- was never
        // exercised. Emitting a zero-entry catch-up frame there would burn a wire
        // sequence and, via fsnAtZero = replayStart - catchUpFrames, shift the
        // baseline so the first real frame no longer lands on replayStart.
        //
        // framesSent and fsnAtZero alone do NOT discriminate the sentDictCount > 0
        // conjunct: sendDictCatchUp has no unsplit fast path, so with sentDictCount == 0
        // its walk is a no-op and both values come out identical whether or not the
        // guard skips the call. capReads is what does discriminate it: sendDictCatchUp's
        // first statement reads client.getServerMaxBatchSize(), so removing the
        // sentDictCount > 0 conjunct (this test's engine has hasReplayDictionaryDependency
        // == true and a real client, so the other two conjuncts hold) would call
        // sendDictCatchUp anyway and register that read even though it ships nothing.
        TestUtils.assertMemoryLeak(() -> {
            // cap 0 ("server advertises no cap"); the value is otherwise incidental here.
            AtomicInteger capReads = new AtomicInteger();
            CatchUpCapturingClient client = new CatchUpCapturingClient(0, false, capReads::incrementAndGet);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    // Deliberately no seedMirror: sentDictCount stays 0.
                    invokeSetWireBaselineWithCatchUp(loop, 7L);
                    assertEquals("an empty dictionary must not ship a catch-up frame",
                            0, client.framesSent);
                    assertEquals("the baseline must stay at replayStart when nothing is re-registered",
                            7L, loop.fsnAtZero());
                    assertEquals("an empty dictionary must skip sendDictCatchUp entirely -- "
                                    + "not call it and discover there is nothing to send",
                            0, capReads.get());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testDegradedDeltaPrefixSlotWithLostDictStillEmitsReconnectCatchUp() throws Exception {
        // Isolates the second disjunct of hasReplayDictionaryDependency
        // (CursorWebSocketSendLoop constructor): isDeltaDictEnabled() ||
        // recoveredMaxSymbolDeltaStart() > 0. It is load-bearing only when the first
        // disjunct is false. A sender that degraded to full-dict (disableDeltaDict) and
        // whose .symbol-dict then did not survive -- a host-crash tear on top of the
        // degrade -- reports isDeltaDictEnabled() == false, yet its recovered ring still
        // carries a DELTA PREFIX (deltaStart > 0) and the full-dict suffix it shipped after
        // degrading. The loop rebuilds the mirror from the frames' own deltas (they carry
        // their symbols inline) and MUST still re-register the whole dictionary with a
        // catch-up: dropping the recoveredMaxSymbolDeltaStart > 0 disjunct makes
        // hasReplayDictionaryDependency false, skips the catch-up, and replays the
        // deltaStart > 0 prefix against a server that never registered ids 0..n -- a gap.
        // Not covered elsewhere: CursorWebSocketSendLoopTornDictGuardTest exercises the
        // GAPPED torn slot (frames out-reach what any frame defines -> refuse, no catch-up),
        // and the seedMirror catch-up tests inject the mirror instead of recovering a slot.
        TestUtils.assertMemoryLeak(() -> {
            // Delta prefix (deltaStart 0, 1) then the full-dict suffix the degraded producer
            // ships (deltaStart 0, whole dict).
            try (CursorSendEngine writer = newEngine()) {
                appendDeltaDictFrame(writer, 0, 'a');
                appendDeltaDictFrame(writer, 1, 'b');
                appendDeltaDictFrame(writer, 0, 'a', 'b', 'c');
            }
            // Tear the dictionary away, so recovery finds none: PersistedSymbolDict.open()
            // returns null for an absent file and the recovery path never fabricates one,
            // leaving isDeltaDictEnabled() false while the delta prefix keeps maxDeltaStart > 0.
            Files.remove(tmpDir + "/" + PersistedSymbolDict.FILE_NAME);
            CatchUpCapturingClient client = new CatchUpCapturingClient(0); // no cap => one frame
            try (CursorSendEngine engine = newEngine()) { // re-open recovers the slot
                assertFalse("a torn/absent side-file leaves the delta dict disabled",
                        engine.isDeltaDictEnabled());
                assertTrue("the delta prefix survives the fold, so maxDeltaStart > 0",
                        engine.recoveredMaxSymbolDeltaStart() > 0L);
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                    assertTrue("a degraded delta-prefix slot must STILL emit a reconnect catch-up "
                                    + "via the recoveredMaxSymbolDeltaStart > 0 disjunct, even with "
                                    + "the delta dict disabled", client.framesSent >= 1);
                    // The catch-up re-registers the whole rebuilt dictionary [a, b, c] -- the
                    // ids the delta prefix and full-dict suffix reference -- so the replayed
                    // deltaStart=0 suffix redefines ids the server already holds.
                    assertCatchUpReassembles(client, "a", "b", "c");
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCatchUpChunksBelowTheDefaultReceiveBufferWhenNoCapIsAdvertised() throws Exception {
        // getServerMaxBatchSize() == 0 means "not advertised", not "unbounded". The
        // transport still closes anything larger than the server's receive buffer -- the
        // default is 131072 -- with 1009, which is correctly non-terminal for a
        // catch-up-only connection, so the reconnect ships the identical frame forever.
        TestUtils.assertMemoryLeak(() -> {
            List<byte[]> frames = captureCatchUpFrames(/*advertisedCap*/ 0, /*symbols*/ 20_000);
            assertTrue("a large dictionary must still chunk without an advertised cap",
                    frames.size() > 1);
            for (byte[] frame : frames) {
                assertTrue("catch-up frame of " + frame.length
                                + " bytes would be refused by a default receive buffer",
                        frame.length <= CursorWebSocketSendLoop.uncappedCatchUpPackingLimit());
            }
            assertCatchUpReassembles(frames, 20_000);
        });
    }

    @Test
    public void testUncappedServerDoesNotInventACapGapForALargeSymbol() throws Exception {
        // The solo-frame limit must NOT be tightened with the packing limit. An entry
        // that already shipped inside a data frame was bounded by effectiveAutoFlushBytes,
        // so declaring it a cap gap here would create a new terminal for data the
        // producer sent successfully.
        TestUtils.assertMemoryLeak(() -> {
            List<byte[]> frames = captureCatchUpFramesWithOneLargeSymbol(
                    /*advertisedCap*/ 0, /*largeSymbolBytes*/ 256 * 1024);
            assertFalse("a large symbol must not be reported as a cap gap", frames.isEmpty());
        });
    }

    @Test
    public void testCatchUpSplitsVariableWidthEntriesWithoutDrift() throws Exception {
        // Every other split test here uses uniformly-sized symbols, so the chunk
        // walk always advances by the same stride and a span miscalculation could
        // cancel out. These entries are 14, 7, 11 and 22 bytes, so each hop is a
        // different width and the walk has to resume mid-dictionary at an
        // irregular boundary; the reassembly then pins the result per id.
        //
        // The symbols are multi-byte UTF-8 because that is a convenient source of
        // widths that differ from their char counts, and it exercises the wire
        // framing end to end -- NOT because sendDictCatchUp could confuse bytes
        // for chars: that method holds no String, char or length() at all, only
        // pointer arithmetic over [len varint][utf8]. The byte-vs-char hazard
        // lives on the persist path and is covered by
        // PersistedSymbolDictTest.testMultiByteUtf8SymbolsRoundTripAcrossReopen.
        TestUtils.assertMemoryLeak(() -> {
            String[] symbols = {
                    // entry width below = 1-byte length varint + the UTF-8 bytes
                    "températures",           // 12 chars, 13 bytes -> 14
                    "東京",                    //  2 chars,  6 bytes ->  7
                    "sensor🔥",              //  7 chars, 10 bytes -> 11
                    "ascii_after_multibyte"   // 21 chars, 21 bytes -> 22; a drift shows up here
            };
            // Self-check: prove the literals really are multi-byte at runtime, so
            // the widths stay irregular even if the source file ever loses its
            // encoding and they collapse to single-byte '?'.
            for (String s : symbols) {
                if (!"ascii_after_multibyte".equals(s)) {
                    assertTrue("expected a multi-byte UTF-8 symbol, got pure ASCII: " + s,
                            s.getBytes(StandardCharsets.UTF_8).length > s.length());
                }
            }
            CatchUpCapturingClient client = new CatchUpCapturingClient(80);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, symbols);
                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertTrue("the cap must split the dictionary", client.framesSent > 1);
                    assertCatchUpReassembles(client, symbols);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testTransientCatchUpSendFailureIsRetriableNotTerminal() throws Exception {
        // A transient wire failure WHILE shipping the catch-up (the fresh
        // connection drops mid-handshake) must surface as a retriable
        // CatchUpSendException for the reconnect loop to handle -- it must NOT
        // call fail(). From inside the catch-up fail() re-enters connectLoop
        // (corrupting the fsnAtZero/nextWireSeq mapping, or overflowing the stack
        // on a flapping connection) or, with no reconnect attempt reachable,
        // latches a terminal -- turning a transient outage into a hard failure and
        // breaking store-and-forward. Only the oversized-entry (non-retriable)
        // terminal was covered; this pins the retriable path.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0, true); // sendBinary throws
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                engine.acknowledge(0); // ackedFsn=0 => a real unacked frame exists behind the catch-up
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "s0", "s1"); // non-empty dict => catch-up fires and hits the failing send
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("a transient catch-up send failure must raise a retriable "
                                + "CatchUpSendException, not be swallowed into fail()/a terminal");
                    } catch (RuntimeException e) {
                        assertEquals("transient catch-up send failure must surface as CatchUpSendException",
                                "CatchUpSendException", e.getClass().getSimpleName());
                    }
                    // Retriable, not terminal: the producer-facing error latch stays clear.
                    loop.checkError();
                } finally {
                    loop.close();
                }
            }
        });
    }

    /**
     * A delta section that declares more entries than it carries must bail, not advance
     * the mirror's count past bytes it does not hold.
     * <p>
     * readVarintAt used to return 0 with its end position AT the limit when the payload
     * was already exhausted, so a caller computing {@code p = end + len} got exactly
     * {@code p == limit} and its {@code p > limit} bail-out could not fire. The walk then
     * ran the remaining pseudo-entries for free and {@code sentDictCount += newCount}
     * claimed symbols the mirror has no bytes for -- after which the reconnect catch-up
     * ships a chunk whose deltaCount exceeds its payload. Returning -1 for a truncated
     * varint makes the boundary detectable and every bail-out fire.
     * <p>
     * The frame here is well-formed except for its count: two entries declared, one
     * supplied, ending exactly on the boundary that used to slip through.
     */
    @Test
    public void testDeltaDeclaringMoreEntriesThanItCarriesDoesNotAdvanceTheMirror() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    // [deltaStart=0][deltaCount=2][len=1]['a'] -- the second entry is absent
                    // and the payload ends exactly where its length varint would start.
                    int payloadLen = QwpConstants.HEADER_SIZE + 4;
                    long frame = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.getUnsafe().setMemory(frame, payloadLen, (byte) 0);
                        Unsafe.getUnsafe().putInt(frame, QwpConstants.MAGIC_MESSAGE);
                        Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS,
                                QwpConstants.FLAG_DELTA_SYMBOL_DICT);
                        long p = frame + QwpConstants.HEADER_SIZE;
                        Unsafe.getUnsafe().putByte(p, (byte) 0);       // deltaStart
                        Unsafe.getUnsafe().putByte(p + 1, (byte) 2);   // deltaCount: a lie
                        Unsafe.getUnsafe().putByte(p + 2, (byte) 1);   // entry 0 length
                        Unsafe.getUnsafe().putByte(p + 3, (byte) 'a'); // entry 0 payload
                        loop.accumulateSentDictForTest(frame, payloadLen, 0);
                    } finally {
                        Unsafe.free(frame, payloadLen, MemoryTag.NATIVE_DEFAULT);
                    }
                    assertEquals("a truncated delta must not advance the mirror at all",
                            0, loop.sentDictCount());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testAccumulateSentDictPartialOverlapExtendsMirror() throws Exception {
        // M3: accumulateSentDict must handle a delta that STRADDLES the mirror tip
        // (deltaStart < sentDictCount < deltaStart+deltaCount) by copying only the
        // new tail, not dropping the whole frame. The monotonic producer never emits
        // a straddling delta in steady state (so the pre-fix drop-whole-frame guard
        // passed every test), but a torn-dict replay can seed the mirror smaller than
        // a frame's coverage. Seed the mirror with 1 symbol, feed a [0..2] delta, and
        // assert the mirror extends to all 3 -- pre-fix it stayed at 1, leaving the
        // reconnect catch-up incomplete and shifting server-side ids.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "aa"); // sentDictCount = 1, mirror holds "aa"
                    int[] frameLen = new int[1];
                    long frame = buildDeltaFrame(0, new String[]{"aa", "bb", "cc"}, frameLen);
                    try {
                        loop.accumulateSentDictForTest(frame, frameLen[0], 0);
                    } finally {
                        Unsafe.free(frame, frameLen[0], MemoryTag.NATIVE_DEFAULT);
                    }
                    assertEquals("straddling delta must extend the mirror to all 3 ids",
                            3, loop.sentDictCount());
                    assertEquals("mirror must hold the two new tail symbols after the "
                                    + "already-held prefix, gap-free",
                            Arrays.asList("aa", "bb", "cc"), readMirrorSymbols(loop));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCatchUpChunkFrameSizeOverflowFailsLoud() throws Exception {
        // M3: sendDictCatchUp caps each chunk under the budget, so the single-frame
        // catch-up path cannot overflow its int frameLen at any real cardinality. The
        // guard must still be LOCAL -- a future caller must not be able to feed a
        // wrapped-negative frameLen to Unsafe.malloc. An oversized symbolsLen must
        // fail loud (CatchUpSendException) BEFORE the malloc; the guard fires before
        // symbolsAddr is read, so a dummy address is fine.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    // symbolsLen past the mirror ceiling: HEADER + varints + symbolsLen
                    // overflows an int, so the guard must reject it before malloc.
                    loop.sendCatchUpChunkForTest(0, 1, 0L, Integer.MAX_VALUE - 4);
                    fail("an overflowing catch-up frame size must fail loud, not malloc negative");
                } catch (RuntimeException e) {
                    assertEquals("overflow must surface as CatchUpSendException",
                            "CatchUpSendException", e.getClass().getSimpleName());
                    assertTrue("message must name the frame-size guard: " + e.getMessage(),
                            e.getMessage().contains("catch-up frame exceeds the maximum size"));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCorruptCatchUpMirrorLatchesTerminalNotLivelock() throws Exception {
        // A mirror whose [len varint][utf8] framing disagrees with sentDictCount can
        // only arise from memory corruption -- it is built from CRC-validated frames.
        // The split catch-up's running-pointer walk must latch a terminal (recordFatal,
        // surfaced by checkError) rather than let a bare throw unwind into connectLoop
        // and reconnect-livelock. A non-zero cap forces the packing loop that walks it.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(64);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    // Entry 0 = [len=1]['a'] (2 bytes); entry 1 = [len=100] with no bytes
                    // following (a truncated tail), so the second entry's end runs past the
                    // buffer while sentDictCount = 2 claims both. loop.close() frees addr.
                    long addr = Unsafe.malloc(3, MemoryTag.NATIVE_DEFAULT);
                    long p = writeVarint(addr, 1);
                    Unsafe.getUnsafe().putByte(p, (byte) 'a');
                    writeVarint(addr + 2, 100);
                    loop.seedSentDictMirrorForTest(addr, 3, 2);

                    try {
                        invokeSetWireBaselineWithCatchUp(loop, 0L);
                        fail("a corrupt mirror must raise CatchUpSendException");
                    } catch (RuntimeException e) {
                        assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                    }
                    try {
                        loop.checkError();
                        fail("a corrupt catch-up mirror must latch a terminal, not livelock");
                    } catch (LineSenderException terminal) {
                        assertTrue("terminal must name the corrupt mirror: " + terminal.getMessage(),
                                terminal.getMessage().contains("invalid symbol dictionary mirror during catch-up"));
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testForegroundCatchUpCapGapRetriesPastOrphanBudget() throws Exception {
        // The foreground policy must never accrue or exhaust the orphan drainer's
        // quarantine budget. Drive more cap gaps than that entire budget and assert every
        // failure remains retriable to the I/O loop and invisible to the producer.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newForegroundLoop(engine, client);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    for (int i = 1; i <= maxAttempts + 4; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                        loop.checkError();
                    }
                    assertEquals("foreground retries must not burn the orphan attempt budget",
                            0, loop.catchUpCapGapAttempts());
                    assertEquals("foreground retries must not anchor an orphan cap-gap episode",
                            -1L, loop.catchUpCapGapFirstNanos());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCatchUpCapGapStrikesAloneDoNotLatchWithinTheEscalationWindow() throws Exception {
        // The strike count alone must NOT latch a terminal: escalation also requires the
        // cap-gap episode to have persisted for catchUpCapGapMinEscalationWindowMillis.
        //
        // This keeps a routine rolling restart from quarantining a drainable orphan slot.
        // MAX_CATCHUP_CAP_GAP_ATTEMPTS strikes accrue in ~2 minutes at the capped
        // reconnect backoff -- less than the time the larger-cap node is away -- so a
        // count-only budget would quarantine the slot on the very transient the budget
        // exists to ride out. Here we drive far MORE than the budget's strikes inside a
        // deliberately huge window and assert the orphan loop stays retriable.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                // A one-hour dwell the test cannot possibly elapse.
                CursorWebSocketSendLoop loop = newLoop(engine, client, 3_600_000L);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    for (int i = 1; i <= maxAttempts + 4; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                        // The producer-facing latch must stay clear on EVERY attempt,
                        // including the ones past the strike budget.
                        loop.checkError();
                    }
                    assertTrue("the strikes really did exceed the budget",
                            loop.catchUpCapGapAttempts() > maxAttempts);

                    // Backdate the episode anchor past the window: the very next cap gap
                    // now satisfies BOTH conditions and latches. This pins the AND -- if
                    // escalation ignored the wall clock the loop would already have
                    // latched above; if it ignored the strike count it could never latch.
                    loop.setCatchUpCapGapFirstNanosForTest(System.nanoTime() - TimeUnit.HOURS.toNanos(2));
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the escalating cap gap must still raise CatchUpSendException");
                    } catch (RuntimeException e) {
                        assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                    }
                    try {
                        loop.checkError();
                        fail("a cap gap that outlives the escalation window must latch a terminal");
                    } catch (LineSenderException terminal) {
                        assertTrue("terminal must name the exhausted catch-up cap gap: " + terminal.getMessage(),
                                terminal.getMessage().contains("during catch-up")
                                        && terminal.getMessage().contains("must be resent"));
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testDefaultCapGapEscalationWindowKeepsStrikesAloneRetriable() throws Exception {
        // testCatchUpCapGapStrikesAloneDoNotLatchWithinTheEscalationWindow pins the count+dwell
        // AND, but with an INJECTED one-hour dwell -- so it stays green even if the DEFAULT dwell
        // every orphan drainer inherits (BackgroundDrainer forwards
        // DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS) were set to 0. A zero default
        // makes escalation count-only, quarantining a drainable orphan slot on the very routine
        // rolling restart the dwell exists to ride out. Construct the loop with the DEFAULT dwell
        // (as the drainer does), drive well past the strike budget in far under the window, and
        // assert the loop stays retriable: setting the constant to 0 reddens this.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client,
                        CursorWebSocketSendLoop.DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    for (int i = 1; i <= maxAttempts + 4; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                        // Under a non-zero default the episode is far too young to escalate, so
                        // the producer-facing latch must stay clear on every attempt -- including
                        // those past the strike budget. A zero default latches here instead.
                        loop.checkError();
                    }
                    assertTrue("the strikes must exceed the budget, so only the dwell keeps it retriable",
                            loop.catchUpCapGapAttempts() > maxAttempts);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCatchUpCapGapDwellConversionSaturatesInsteadOfOverflowing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long maxExactMillis = Long.MAX_VALUE / 1_000_000L;
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop exactLoop = newLoop(
                        engine, new CatchUpCapturingClient(0), maxExactMillis);
                try {
                    assertEquals(maxExactMillis * 1_000_000L,
                            exactLoop.catchUpCapGapMinEscalationWindowNanos());
                } finally {
                    exactLoop.close();
                }

                CursorWebSocketSendLoop saturatedLoop = newLoop(
                        engine, new CatchUpCapturingClient(0), maxExactMillis + 1L);
                try {
                    assertEquals("an oversized dwell must become effectively infinite, not negative",
                            Long.MAX_VALUE,
                            saturatedLoop.catchUpCapGapMinEscalationWindowNanos());
                } finally {
                    saturatedLoop.close();
                }
            }
        });
    }

    @Test
    public void testCapGapEpisodeWithANegativeAnchorStillEscalates() throws Exception {
        // A cap-gap episode anchored at a NEGATIVE nanoTime instant must escalate like any
        // other. A System.nanoTime() value is only meaningful as a difference -- its origin
        // is arbitrary and the spec permits negative values -- so no state may ride on the
        // anchor's sign. sendDictCatchUp once tested catchUpCapGapFirstNanos < 0 to mean "no
        // episode open": that read a negative anchor as unset, re-anchored it to now on every
        // strike and pinned episodeNanos at ~0, so the dwell was never satisfied and the
        // terminal could never latch, however long the cap gap truly persisted.
        //
        // That is what reddened CI. The sibling test above backdates the anchor two hours,
        // and on Linux nanoTime() is nanos-since-boot: on a CI agent up ten minutes it is
        // ~6e11, so "two hours ago" comes out ~ -6.6e12 -- negative. The defect therefore
        // only surfaced where uptime is under that backdate: every fresh CI agent, and never
        // a long-lived dev box (which is why it passed locally). Planting the negative anchor
        // directly pins the sentinel on ANY machine, whatever its uptime.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                // A one-hour dwell, against an anchor two hours back: satisfied on elapsed,
                // but only if the negative anchor survives to the subtraction.
                CursorWebSocketSendLoop loop = newLoop(engine, client, 3_600_000L);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    // Satisfy the strike half of the AND, one short of the budget.
                    for (int i = 1; i < maxAttempts; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                        loop.checkError(); // dwell unmet => retriable, whatever the count
                    }
                    // The episode began two hours ago on a machine booted minutes ago.
                    loop.setCatchUpCapGapFirstNanosForTest(-TimeUnit.HOURS.toNanos(2));

                    // Both halves now hold, so this strike must latch the terminal.
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the escalating cap gap must still raise CatchUpSendException");
                    } catch (RuntimeException e) {
                        assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                    }
                    try {
                        loop.checkError();
                        fail("a cap-gap episode anchored at a negative nanoTime instant must still "
                                + "escalate -- the anchor's sign carries no meaning");
                    } catch (LineSenderException terminal) {
                        assertTrue("terminal must name the exhausted catch-up cap gap: " + terminal.getMessage(),
                                terminal.getMessage().contains("during catch-up")
                                        && terminal.getMessage().contains("must be resent"));
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testTransientCatchUpFailureDoesNotBurnTheCapGapBudget() throws Exception {
        // A TRANSIENT catch-up failure (the wire drops mid-catch-up -- a flapping LB, a
        // reset) must never increment the cap-gap terminal budget. The budget exists to
        // prove a PERSISTENT cluster capability gap; letting a transient feed it means
        // enough wire flaps hard-fail a live store-and-forward producer, which is the
        // exact failure store-and-forward promises cannot happen.
        //
        // The production code is correct, but nothing pinned it: the counter is never
        // read by the existing transient test, and one transient can never reach a
        // 16-strike budget anyway. So drive MORE transients than the whole budget and
        // assert the counter never moves and no terminal ever latches.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            // A cap that FITS (no cap gap), but whose sendBinary always throws: every
            // failure here is transport-transient, never a capability gap.
            CatchUpCapturingClient client = new CatchUpCapturingClient(0, true);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client, 0L);
                try {
                    seedMirror(loop, "alpha");
                    for (int i = 1; i <= maxAttempts + 4; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("a transient send failure must raise CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                        loop.checkError(); // a transient is retriable, forever
                        assertEquals("a transient must NOT burn the cap-gap terminal budget",
                                0, loop.catchUpCapGapAttempts());
                        assertEquals("a transient must NOT anchor a cap-gap episode",
                                -1L, loop.catchUpCapGapFirstNanos());
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testTransportOutageRestartsCapGapEpisode() throws Exception {
        assertUnrelatedReconnectStateRestartsCapGapEpisode(false);
    }

    @Test
    public void testRoleRejectRestartsCapGapEpisode() throws Exception {
        assertUnrelatedReconnectStateRestartsCapGapEpisode(true);
    }

    @Test
    public void testCatchUpCapGapRetriesUntilBudgetThenLatches() throws Exception {
        // M1: an entry too large for the fresh server's cap during catch-up (a
        // heterogeneous / rolling-cap failover to a smaller-cap node) must NOT latch
        // on first sight. sendDictCatchUp throws a RETRIABLE CatchUpSendException so
        // the reconnect loop rides it out -- a larger-cap node may return -- and only
        // after MAX_CATCHUP_CAP_GAP_ATTEMPTS consecutive cap gaps does it recordFatal.
        // Pre-fix the first cap gap latched a terminal, so one transient failover to a
        // smaller-cap node quarantined the orphan slot. (A successful catch-up resets the budget;
        // the other catch-up tests, which use a fitting cap, never trip it.)
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            // Pin the budget against a LITERAL before deriving anything from it. The
            // retriable loop below is bounded by maxAttempts, so keying this test purely
            // off the constant under test makes it TAUTOLOGICAL: a regression of
            // MAX_CATCHUP_CAP_GAP_ATTEMPTS to 1 -- which is precisely the pre-fix bug this
            // test names, a single cap gap quarantining the slot -- would run the loop ZERO
            // times, the "exhausting" attempt would become the FIRST attempt, and the test
            // would still pass green. Requiring > 1 makes that regression fail here, and it
            // also guarantees the loop runs at least once, so the first cap gap is genuinely
            // asserted retriable rather than vacuously skipped.
            assertTrue("the cap-gap settle budget must tolerate MORE THAN ONE gap, else a single "
                            + "transient failover to a smaller-cap node quarantines the slot "
                            + "[MAX_CATCHUP_CAP_GAP_ATTEMPTS=" + maxAttempts + ']',
                    maxAttempts > 1);
            // cap 160 => catch-up budget is below a ~216-byte solo frame for a 200-char symbol.
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    // Attempts 1 .. max-1 are retriable: no terminal is latched.
                    for (int i = 1; i < maxAttempts; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                            assertTrue("attempt " + i + " must name the catch-up cap gap: "
                                            + e.getMessage(),
                                    e.getMessage().contains("during catch-up"));
                        }
                        loop.checkError(); // under budget => retriable => no terminal
                    }
                    // The exhausting attempt still throws, and now latches the terminal.
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the exhausting cap gap must still raise CatchUpSendException");
                    } catch (RuntimeException e) {
                        assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                    }
                    try {
                        loop.checkError();
                        fail("exhausting the cap-gap settle budget must latch a terminal");
                    } catch (LineSenderException terminal) {
                        assertTrue("terminal must name the exhausted catch-up cap gap: " + terminal.getMessage(),
                                terminal.getMessage().contains("during catch-up")
                                        && terminal.getMessage().contains("must be resent"));
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testSuccessfulCatchUpResetsCapGapBudget() throws Exception {
        // The cap-gap settle budget (catchUpCapGapAttempts) counts CONSECUTIVE cap
        // gaps across reconnects; a successful catch-up ends the episode and MUST reset
        // it to 0 (sendDictCatchUp's final line). Otherwise cap gaps interspersed with
        // successful catch-ups -- a rolling-cap cluster where a larger-cap node comes
        // and goes -- would accumulate to a spurious terminal over a long-lived orphan drainer.
        // testCatchUpCapGapRetriesUntilBudgetThenLatches only accrues gaps under one
        // fixed cap with no success interleaved, so it cannot pin the reset.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            // Same anti-tautology pin as testCatchUpCapGapRetriesUntilBudgetThenLatches.
            // With maxAttempts == 1 the accrual loop below would run ZERO times and the
            // "budget accrued to max-1" precondition would degenerate to 0 == 0, so the
            // reset-to-0 assertion that is the whole point of this test would prove nothing.
            assertTrue("the cap-gap settle budget must tolerate MORE THAN ONE gap "
                            + "[MAX_CATCHUP_CAP_GAP_ATTEMPTS=" + maxAttempts + ']',
                    maxAttempts > 1);
            CatchUpCapturingClient client = new CatchUpCapturingClient(160); // too small for a 200-char symbol
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    // Accrue max-1 consecutive cap gaps (each retriable, no terminal).
                    for (int i = 1; i < maxAttempts; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                    }
                    assertEquals("precondition: budget accrued to max-1",
                            maxAttempts - 1, loop.catchUpCapGapAttempts());

                    // A larger-cap node returns: the whole dictionary re-registers with
                    // no cap gap, so the settle budget must reset to 0.
                    client.cap = 0; // no cap => the 200-char symbol fits one frame
                    invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                    assertEquals("a successful catch-up must reset the cap-gap settle budget",
                            0, loop.catchUpCapGapAttempts());

                    // Behavioural proof the budget is genuinely fresh: max-1 more cap
                    // gaps still latch NO terminal (they would if the counter had stayed
                    // at max-1 -- one more gap would have quarantined the slot).
                    client.cap = 160;
                    for (int i = 1; i < maxAttempts; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("post-reset cap gap must be retriable (attempt " + i + ')');
                        } catch (RuntimeException e) {
                            assertEquals("CatchUpSendException", e.getClass().getSimpleName());
                        }
                        loop.checkError(); // fresh budget => still under max => no terminal
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testMirrorOverflowFailsLoud() throws Exception {
        // ensureSentDictCapacity must latch a terminal -- not silently overflow the
        // int capacity math into a heap-corrupting copyMemory -- when the sent-dict
        // mirror would exceed MAX_SENT_DICT_BYTES. Unreachable at real cardinality
        // (~200M+ symbols on one connection), so drive the guard directly with an
        // oversized required, mirroring testCatchUpChunkFrameSizeOverflowFailsLoud.
        TestUtils.assertMemoryLeak(() -> {
            long overCeiling = (long) CursorWebSocketSendLoop.maxSentDictBytes() + 1L;
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    try {
                        loop.ensureSentDictCapacityForTest(overCeiling);
                        fail("a mirror capacity past MAX_SENT_DICT_BYTES must fail loud, not overflow");
                    } catch (LineSenderException e) {
                        assertEquals("overflow must surface as LineSenderException",
                                "LineSenderException", e.getClass().getSimpleName());
                        assertTrue("message must name the mirror ceiling: " + e.getMessage(),
                                e.getMessage().contains("mirror exceeds the maximum size"));
                    }
                    // recordFatal (not a bare throw) latched the terminal, so the loop
                    // winds down instead of reconnecting into the same overflow.
                    try {
                        loop.checkError();
                        fail("mirror overflow must latch a terminal");
                    } catch (LineSenderException terminal) {
                        assertTrue(terminal.getMessage().contains("mirror exceeds the maximum size"));
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    private static void appendFrames(CursorSendEngine engine, int count) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, payload[i]);
            }
            for (int i = 0; i < count; i++) {
                engine.appendBlocking(buf, 16);
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Appends a delta-symbol-dictionary frame starting at {@code deltaStart}, one 1-byte
     * symbol per char. deltaStart and deltaCount are small enough to be single-byte
     * varints. deltaStart 0 is the self-sufficient full-dict frame a degraded producer
     * ships; deltaStart > 0 is a delta frame that depends on the ids below it.
     */
    private static void appendDeltaDictFrame(CursorSendEngine engine, int deltaStart, char... symbols) {
        int size = QwpConstants.HEADER_SIZE + 2 + symbols.length * 2;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < size; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 0);
            }
            Unsafe.getUnsafe().putInt(buf, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS,
                    QwpConstants.FLAG_DELTA_SYMBOL_DICT);
            long p = buf + QwpConstants.HEADER_SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) deltaStart);         // deltaStart
            Unsafe.getUnsafe().putByte(p + 1, (byte) symbols.length); // deltaCount
            long q = p + 2;
            for (char c : symbols) {
                Unsafe.getUnsafe().putByte(q, (byte) 1);
                Unsafe.getUnsafe().putByte(q + 1, (byte) c);
                q += 2;
            }
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertUnrelatedReconnectStateRestartsCapGapEpisode(boolean roleReject) throws Exception {
        // Accrue an orphan drainer's cap-gap strikes to one short of terminal, then
        // simulate a long unrelated outage before another small-cap node appears. The
        // outage must end the old episode: its wall-clock duration says nothing about
        // whether the cluster's batch cap remained incompatible while no node answered.
        TestUtils.assertMemoryLeak(() -> {
            int maxAttempts = CursorWebSocketSendLoop.maxCatchUpCapGapAttempts();
            assertTrue("the cap-gap settle budget must have a retriable interval", maxAttempts > 1);

            int[] reconnectCalls = {0};
            long[] staleAnchor = {Long.MIN_VALUE};
            CursorWebSocketSendLoop[] loopRef = new CursorWebSocketSendLoop[1];
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        () -> {
                            int call = ++reconnectCalls[0];
                            if (call < maxAttempts) {
                                return new CatchUpCapturingClient(160);
                            }
                            if (call == maxAttempts) {
                                assertEquals("precondition: consecutive cap gaps survive reconnect",
                                        maxAttempts - 1,
                                        loopRef[0].catchUpCapGapAttempts());
                                // Model the elapsed outage without sleeping. With the defect,
                                // this old anchor survives the unrelated failure and the next
                                // cap gap immediately satisfies both terminal conditions.
                                staleAnchor[0] = System.nanoTime() - TimeUnit.HOURS.toNanos(2);
                                loopRef[0].setCatchUpCapGapFirstNanosForTest(staleAnchor[0]);
                                if (roleReject) {
                                    throw new QwpRoleMismatchException(
                                            "PRIMARY", null, "all endpoints role-rejected");
                                }
                                throw new LineSenderException("transport unavailable");
                            }
                            if (call == maxAttempts + 1) {
                                // Stop after getServerMaxBatchSize() has driven the final cap
                                // gap, leaving its fresh episode state observable below.
                                return new CatchUpCapturingClient(160, false,
                                        () -> loopRef[0].setRunningForTest(false));
                            }
                            throw new AssertionError("unexpected reconnect call " + call);
                        },
                        0L, 0L, false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L, TimeUnit.HOURS.toMillis(1),
                        CursorWebSocketSendLoop.ReconnectPolicy.ORPHAN);
                loopRef[0] = loop;
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    loop.setRunningForTest(true);
                    invokeConnectLoop(loop);

                    loop.checkError();
                    assertEquals("pre-outage cap gaps must not carry into the new episode",
                            1, loop.catchUpCapGapAttempts());
                    assertTrue("the post-outage cap gap must get a fresh dwell anchor",
                            loop.catchUpCapGapFirstNanos() > staleAnchor[0]);
                    assertEquals("test must observe gaps, the unrelated state, and a new gap",
                            maxAttempts + 1, reconnectCalls[0]);
                } finally {
                    loop.close();
                }
            }
        });
    }

    // Builds a QWP delta frame [12-byte header][deltaStart varint][deltaCount
    // varint][ [len varint][utf8] ... ] for the given symbols. accumulateSentDict
    // skips the header, so its content is irrelevant; the caller frees the frame.
    private static long buildDeltaFrame(int deltaStart, String[] symbols, int[] outLen) {
        int deltaCount = symbols.length;
        int size = 12 + varintSize(deltaStart) + varintSize(deltaCount);
        for (String s : symbols) {
            size += varintSize(s.getBytes(StandardCharsets.UTF_8).length)
                    + s.getBytes(StandardCharsets.UTF_8).length;
        }
        long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        long p = writeVarint(addr + 12, deltaStart);
        p = writeVarint(p, deltaCount);
        for (String s : symbols) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            p = writeVarint(p, b.length);
            for (byte x : b) {
                Unsafe.getUnsafe().putByte(p++, x);
            }
        }
        outLen[0] = size;
        return addr;
    }

    // Parses the loop's native sent-dictionary mirror ([len varint][utf8]...) back
    // into the symbol strings a reconnect catch-up would re-register.
    private static List<String> readMirrorSymbols(CursorWebSocketSendLoop loop) throws Exception {
        long addr = loop.sentDictBytesAddr();
        int len = loop.sentDictBytesLen();
        List<String> out = new ArrayList<>();
        long p = addr;
        long limit = addr + len;
        while (p < limit) {
            long l = 0;
            int shift = 0;
            while (p < limit) {
                byte b = Unsafe.getUnsafe().getByte(p++);
                l |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            byte[] bytes = new byte[(int) l];
            for (int i = 0; i < l; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(p++);
            }
            out.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return out;
    }

    // Delivers a 0-table STATUS_OK for {@code wireSeq} into the loop's response
    // handler, mimicking the server acking a catch-up frame (which carries no tables).
    private static void deliverOk(CursorWebSocketSendLoop loop, long wireSeq) throws Exception {
        int size = 11; // status(1) + sequence(8) + tableCount(2)
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
            Unsafe.getUnsafe().putLong(ptr + 1, wireSeq);
            Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);
            loop.deliverResponseForTest(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void invokeSetWireBaselineWithCatchUp(CursorWebSocketSendLoop loop, long replayStart) {
        loop.setWireBaselineWithCatchUpForTest(replayStart);
    }

    private static void invokeConnectLoop(CursorWebSocketSendLoop loop) {
        loop.connectLoopForTest(new LineSenderException("test reconnect"), "reconnect", 0L);
    }

    /**
     * start()'s catch of CatchUpSendException must absorb a transient catch-up send
     * failure, not let it out of Sender construction.
     * <p>
     * A recovered sender re-registers its dictionary with a catch-up on the very first
     * connect, and in SYNC/OFF startup that runs on the CALLER's thread inside start().
     * Without the catch, a server that drops during the catch-up turns a transient outage
     * into a build() failure -- an Invariant B violation, and the one thing
     * store-and-forward exists to prevent. Delete the block and everything else stays
     * green: no other test combines a seeded mirror with a failing client through start().
     * <p>
     * The failure must also stay non-terminal: the client is dropped so the I/O thread
     * reconnects and re-sends the catch-up off the caller's thread.
     */
    @Test
    public void testStartAbsorbsATransientCatchUpSendFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                CatchUpCapturingClient client = new CatchUpCapturingClient(0, true);
                CursorWebSocketSendLoop loop = newForegroundLoop(engine, client);
                try {
                    // One mirror entry is enough to make setWireBaselineWithCatchUp ship a
                    // catch-up, which the client then fails. loop.close() frees addr.
                    long addr = Unsafe.malloc(2, MemoryTag.NATIVE_DEFAULT);
                    long p = writeVarint(addr, 1);
                    Unsafe.getUnsafe().putByte(p, (byte) 'a');
                    loop.seedSentDictMirrorForTest(addr, 2, 1);

                    loop.start(); // must NOT throw: build() would fail on a transient
                    loop.checkError(); // and must not have latched a terminal
                } finally {
                    loop.close();
                }
            }
        });
    }

    private CursorWebSocketSendLoop newLoop(CursorSendEngine engine, WebSocketClient client) {
        return newLoop(engine, client, 0L);
    }

    /**
     * As {@link #newLoop(CursorSendEngine, WebSocketClient)} but with an explicit
     * cap-gap escalation dwell. These white-box tests model an orphan drainer, where
     * {@code 0} means count-only quarantine; foreground loops retry indefinitely.
     */
    private CursorWebSocketSendLoop newLoop(
            CursorSendEngine engine, WebSocketClient client, long capGapWindowMillis
    ) {
        return new CursorWebSocketSendLoop(
                client, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new UnsupportedOperationException("test loop is never started");
                },
                100L, 5_000L, false,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                0L, capGapWindowMillis,
                CursorWebSocketSendLoop.ReconnectPolicy.ORPHAN);
    }

    private CursorWebSocketSendLoop newForegroundLoop(
            CursorSendEngine engine, WebSocketClient client
    ) {
        // Deliberately use the compatibility overload: its safe default must remain the
        // foreground RETRY_FOREVER policy for external callers.
        return new CursorWebSocketSendLoop(
                client, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new UnsupportedOperationException("test loop is never started");
                },
                100L, 5_000L, false,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                0L, 0L);
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16_384);
    }

    /**
     * Seeds a fresh dictionary of {@code symbolCount} distinct ordinary symbols, runs a
     * catch-up against a client that advertises {@code advertisedCap}, and returns the
     * captured frames. Self-contained -- the engine and loop it builds are opened and
     * closed entirely within this call, so the caller only sees the heap-copied frame
     * bytes {@link CatchUpCapturingClient#capture} produces.
     */
    private List<byte[]> captureCatchUpFrames(int advertisedCap, int symbolCount) throws Exception {
        String[] symbols = new String[symbolCount];
        for (int i = 0; i < symbolCount; i++) {
            symbols[i] = "sym" + i;
        }
        CatchUpCapturingClient client = new CatchUpCapturingClient(advertisedCap);
        try (CursorSendEngine engine = newEngine()) {
            CursorWebSocketSendLoop loop = newLoop(engine, client);
            try {
                seedMirror(loop, symbols);
                invokeSetWireBaselineWithCatchUp(loop, 0L);
            } finally {
                loop.close();
            }
        }
        return client.capturedFrames;
    }

    /**
     * As {@link #captureCatchUpFrames}, but the dictionary holds a single entry of
     * {@code largeSymbolBytes} bytes -- large enough to exceed the packing limit on its
     * own, so the walk ships it as its own oversized solo chunk (the accepted residual;
     * Task 13 files the halve-and-retry follow-up) instead of ever reaching the
     * cap-gap terminal.
     */
    private List<byte[]> captureCatchUpFramesWithOneLargeSymbol(
            int advertisedCap, int largeSymbolBytes
    ) throws Exception {
        String largeSymbol = TestUtils.repeat("x", largeSymbolBytes);
        CatchUpCapturingClient client = new CatchUpCapturingClient(advertisedCap);
        try (CursorSendEngine engine = newEngine()) {
            CursorWebSocketSendLoop loop = newLoop(engine, client);
            try {
                seedMirror(loop, largeSymbol);
                invokeSetWireBaselineWithCatchUp(loop, 0L);
            } finally {
                loop.close();
            }
        }
        return client.capturedFrames;
    }

    /**
     * Reassembles the frames captured since the last call through the same
     * {@link QwpWireTestUtils#accumulateDeltaDictionary} the end-to-end tests'
     * handler uses -- with {@code allowGap=true}, so a hole surfaces as a null
     * entry here instead of raising {@code DictionaryGapException} the way a
     * real server now would -- and asserts the result is the seeded dictionary,
     * dense and in order.
     * <p>
     * This is what frame counting cannot do. A catch-up split ships its chunks as
     * {@code [deltaStart, deltaStart+count)} ranges that must tile {@code [0, n)}
     * exactly; an off-by-one in the walk's start id keeps the frame COUNT intact
     * while overlapping a range (an id silently takes its neighbour's symbol) or
     * skipping one (surfaced here as a null entry; against a real server that id
     * would instead be REJECTED as a dictionary gap). Comparing the reassembled
     * dictionary catches all three shapes -- overlap, gap and shift -- because it
     * compares content per id, not just the ranges.
     */
    /**
     * As {@link #assertCatchUpReassembles(CatchUpCapturingClient, String...)}, but for
     * {@link #captureCatchUpFrames} / {@link #captureCatchUpFramesWithOneLargeSymbol},
     * which return the captured frames directly instead of a client -- and where the
     * caller only knows the dictionary's SIZE, not each generated symbol's literal text.
     * Proves the frames tile {@code [0, expectedCount)} exactly, gap-free.
     */
    private static void assertCatchUpReassembles(List<byte[]> frames, int expectedCount) {
        List<String> rebuilt = new ArrayList<>();
        for (byte[] frame : frames) {
            // allowGap=true: this assertion exists to PROVE the chunks tile [0, n)
            // with no hole, so a gap must be observable here rather than thrown.
            QwpWireTestUtils.accumulateDeltaDictionary(frame, rebuilt, true);
        }
        assertEquals("reassembled dictionary size", expectedCount, rebuilt.size());
        for (int i = 0; i < expectedCount; i++) {
            assertTrue("symbol at id " + i + " must not be a gap (a null here is a gap a "
                    + "real server would REJECT)", rebuilt.get(i) != null);
        }
    }

    private static void assertCatchUpReassembles(CatchUpCapturingClient client, String... expected) {
        List<String> rebuilt = new ArrayList<>();
        for (byte[] frame : client.capturedFrames) {
            // Tiling correctly is necessary but not sufficient: the split exists to
            // keep every frame under the server's advertised batch cap, and a
            // perfectly contiguous split that ignores the cap would otherwise only
            // be caught indirectly, by a frame-count assertion.
            if (client.cap > 0) {
                assertTrue("catch-up frame of " + frame.length
                        + " bytes exceeds the advertised cap " + client.cap,
                        frame.length <= client.cap);
            }
            // allowGap=true: this assertion exists to PROVE the chunks tile [0, n)
            // with no hole, so a gap must be observable here rather than thrown.
            QwpWireTestUtils.accumulateDeltaDictionary(frame, rebuilt, true);
        }
        client.capturedFrames.clear();
        assertEquals("reassembled dictionary size", expected.length, rebuilt.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals("symbol at id " + i + " (a null here is a gap a real server would"
                    + " REJECT; allowGap surfaces it instead of throwing so this assertion"
                    + " can prove there is none)", expected[i], rebuilt.get(i));
        }
    }

    // Populates the loop's native sent-dictionary mirror with {@code symbols} in
    // the on-wire [len varint][utf8] layout, so setWireBaselineWithCatchUp sees a
    // non-empty dictionary to re-register. loop.close() frees it.
    private static void seedMirror(CursorWebSocketSendLoop loop, String... symbols) throws Exception {
        int total = 0;
        for (String s : symbols) {
            int len = s.getBytes(StandardCharsets.UTF_8).length;
            total += varintSize(len) + len;
        }
        long addr = Unsafe.malloc(total, MemoryTag.NATIVE_DEFAULT);
        long p = addr;
        for (String s : symbols) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            p = writeVarint(p, bytes.length);
            for (byte b : bytes) {
                Unsafe.getUnsafe().putByte(p++, b);
            }
        }
        loop.seedSentDictMirrorForTest(addr, total, symbols.length);
        // The mirror carries its own entry lengths ([len varint][utf8]); the catch-up
        // walks them with a running pointer on demand, so nothing else to seed here.
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

    // Stub transport: completes no real I/O. getServerMaxBatchSize drives the
    // catch-up split; sendBinary counts the frames the catch-up emitted, or --
    // when throwOnSend is set -- raises a transient wire error to model the fresh
    // connection dropping mid-catch-up.
    private static final class CatchUpCapturingClient extends WebSocketClient {
        // Mutable so a test can model a rolling-cap cluster: raise it for a node that
        // accepts the dictionary, lower it for a smaller-cap node that cap-gaps.
        private int cap;
        private final List<byte[]> capturedFrames = new ArrayList<>();
        private final Runnable onCapRead;
        private final boolean throwOnSend;
        private int framesSent;
        private int multipartFramesSent;

        CatchUpCapturingClient(int cap) {
            this(cap, false);
        }

        CatchUpCapturingClient(int cap, boolean throwOnSend) {
            this(cap, throwOnSend, null);
        }

        CatchUpCapturingClient(int cap, boolean throwOnSend, Runnable onCapRead) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.cap = cap;
            this.throwOnSend = throwOnSend;
            this.onCapRead = onCapRead;
        }

        @Override
        public int getServerMaxBatchSize() {
            if (onCapRead != null) {
                onCapRead.run();
            }
            return cap;
        }

        @Override
        public int getServerQwpVersion() {
            return 1;
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            recordSend();
            capture(dataPtr, length, 0L, 0);
        }

        @Override
        public void sendBinary(
                long firstPtr,
                int firstLength,
                long secondPtr,
                int secondLength
        ) {
            multipartFramesSent++;
            recordSend();
            capture(firstPtr, firstLength, secondPtr, secondLength);
        }

        /**
         * Keeps the bytes of every frame sent, joining the two slices of a
         * multipart send back into the single frame the server would see.
         * Counting frames alone cannot detect the failure that matters here: an
         * off-by-one in the chunk walk's start id ships the same NUMBER of
         * frames while overlapping or skipping ids, and the server null-pads a
         * skipped id into a silent NULL symbol.
         */
        private void capture(long firstPtr, int firstLength, long secondPtr, int secondLength) {
            byte[] frame = new byte[firstLength + secondLength];
            for (int i = 0; i < firstLength; i++) {
                frame[i] = Unsafe.getUnsafe().getByte(firstPtr + i);
            }
            for (int i = 0; i < secondLength; i++) {
                frame[firstLength + i] = Unsafe.getUnsafe().getByte(secondPtr + i);
            }
            capturedFrames.add(frame);
        }

        private void recordSend() {
            if (throwOnSend) {
                throw new RuntimeException("transient wire failure during catch-up");
            }
            framesSent++;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
