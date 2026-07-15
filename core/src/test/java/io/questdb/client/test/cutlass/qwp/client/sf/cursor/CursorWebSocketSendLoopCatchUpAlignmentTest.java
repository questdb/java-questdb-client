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
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
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
                            replayStart - client.framesSent, readLong(loop, "fsnAtZero"));
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
                            replayStart - client.framesSent, readLong(loop, "fsnAtZero"));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testSplitCatchUpReusesOneFrameBufferAcrossReconnects() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(3_100);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 3_000), TestUtils.repeat("y", 3_000));

                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the small cap must split the dictionary", 2, client.framesSent);
                    assertEquals("the split chunks fit the initial native buffer",
                            1, loop.catchUpFrameGrowthCount());

                    client.cap = 7_000;
                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the larger cap must combine the dictionary", 3, client.framesSent);
                    assertEquals("the combined frame must grow the retained native buffer once",
                            2, loop.catchUpFrameGrowthCount());

                    invokeSetWireBaselineWithCatchUp(loop, 0L);
                    assertEquals("the next reconnect sends one combined frame", 4, client.framesSent);
                    assertEquals("the grown native frame buffer must be reused across reconnects",
                            2, loop.catchUpFrameGrowthCount());
                } finally {
                    // assertMemoryLeak verifies that close releases the retained buffer.
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
                    } catch (InvocationTargetException e) {
                        assertEquals("transient catch-up send failure must surface as CatchUpSendException",
                                "CatchUpSendException", e.getCause().getClass().getSimpleName());
                    }
                    // Retriable, not terminal: the producer-facing error latch stays clear.
                    loop.checkError();
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
                        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                                "accumulateSentDict", long.class, int.class, int.class);
                        m.setAccessible(true);
                        m.invoke(loop, frame, frameLen[0], 0);
                    } finally {
                        Unsafe.free(frame, frameLen[0], MemoryTag.NATIVE_DEFAULT);
                    }
                    assertEquals("straddling delta must extend the mirror to all 3 ids",
                            3, readInt(loop, "sentDictCount"));
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
                    Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                            "sendCatchUpChunk", int.class, int.class, long.class, int.class);
                    m.setAccessible(true);
                    // symbolsLen past the mirror ceiling: HEADER + varints + symbolsLen
                    // overflows an int, so the guard must reject it before malloc.
                    m.invoke(loop, 0, 1, 0L, Integer.MAX_VALUE - 4);
                    fail("an overflowing catch-up frame size must fail loud, not malloc negative");
                } catch (InvocationTargetException e) {
                    assertEquals("overflow must surface as CatchUpSendException",
                            "CatchUpSendException", e.getCause().getClass().getSimpleName());
                    assertTrue("message must name the frame-size guard: " + e.getCause().getMessage(),
                            e.getCause().getMessage().contains("catch-up frame exceeds the maximum size"));
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newForegroundLoop(engine, client);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    for (int i = 1; i <= maxAttempts + 4; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                        }
                        loop.checkError();
                    }
                    assertEquals("foreground retries must not burn the orphan attempt budget",
                            0, readInt(loop, "catchUpCapGapAttempts"));
                    assertEquals("foreground retries must not anchor an orphan cap-gap episode",
                            -1L, readLong(loop, "catchUpCapGapFirstNanos"));
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
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
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                        }
                        // The producer-facing latch must stay clear on EVERY attempt,
                        // including the ones past the strike budget.
                        loop.checkError();
                    }
                    assertTrue("the strikes really did exceed the budget",
                            readInt(loop, "catchUpCapGapAttempts") > maxAttempts);

                    // Backdate the episode anchor past the window: the very next cap gap
                    // now satisfies BOTH conditions and latches. This pins the AND -- if
                    // escalation ignored the wall clock the loop would already have
                    // latched above; if it ignored the strike count it could never latch.
                    Field anchor = CursorWebSocketSendLoop.class.getDeclaredField("catchUpCapGapFirstNanos");
                    anchor.setAccessible(true);
                    anchor.setLong(loop, System.nanoTime() - TimeUnit.HOURS.toNanos(2));
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the escalating cap gap must still raise CatchUpSendException");
                    } catch (InvocationTargetException e) {
                        assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
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
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                        }
                        loop.checkError(); // dwell unmet => retriable, whatever the count
                    }
                    // The episode began two hours ago on a machine booted minutes ago.
                    Field anchor = CursorWebSocketSendLoop.class.getDeclaredField("catchUpCapGapFirstNanos");
                    anchor.setAccessible(true);
                    anchor.setLong(loop, -TimeUnit.HOURS.toNanos(2));

                    // Both halves now hold, so this strike must latch the terminal.
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the escalating cap gap must still raise CatchUpSendException");
                    } catch (InvocationTargetException e) {
                        assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
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
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                        }
                        loop.checkError(); // a transient is retriable, forever
                        assertEquals("a transient must NOT burn the cap-gap terminal budget",
                                0, readInt(loop, "catchUpCapGapAttempts"));
                        assertEquals("a transient must NOT anchor a cap-gap episode",
                                -1L, readLong(loop, "catchUpCapGapFirstNanos"));
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
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
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                            assertTrue("attempt " + i + " must name the catch-up cap gap: "
                                            + e.getCause().getMessage(),
                                    e.getCause().getMessage().contains("during catch-up"));
                        }
                        loop.checkError(); // under budget => retriable => no terminal
                    }
                    // The exhausting attempt still throws, and now latches the terminal.
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the exhausting cap gap must still raise CatchUpSendException");
                    } catch (InvocationTargetException e) {
                        assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
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
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                        }
                    }
                    assertEquals("precondition: budget accrued to max-1",
                            maxAttempts - 1, readInt(loop, "catchUpCapGapAttempts"));

                    // A larger-cap node returns: the whole dictionary re-registers with
                    // no cap gap, so the settle budget must reset to 0.
                    client.cap = 0; // no cap => the 200-char symbol fits one frame
                    invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                    assertEquals("a successful catch-up must reset the cap-gap settle budget",
                            0, readInt(loop, "catchUpCapGapAttempts"));

                    // Behavioural proof the budget is genuinely fresh: max-1 more cap
                    // gaps still latch NO terminal (they would if the counter had stayed
                    // at max-1 -- one more gap would have quarantined the slot).
                    client.cap = 160;
                    for (int i = 1; i < maxAttempts; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("post-reset cap gap must be retriable (attempt " + i + ')');
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
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
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_SENT_DICT_BYTES");
            maxField.setAccessible(true);
            long overCeiling = (long) maxField.getInt(null) + 1L;
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    Method m = CursorWebSocketSendLoop.class.getDeclaredMethod("ensureSentDictCapacity", long.class);
                    m.setAccessible(true);
                    try {
                        m.invoke(loop, overCeiling);
                        fail("a mirror capacity past MAX_SENT_DICT_BYTES must fail loud, not overflow");
                    } catch (InvocationTargetException e) {
                        assertEquals("overflow must surface as LineSenderException",
                                "LineSenderException", e.getCause().getClass().getSimpleName());
                        assertTrue("message must name the mirror ceiling: " + e.getCause().getMessage(),
                                e.getCause().getMessage().contains("mirror exceeds the maximum size"));
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

    private void assertUnrelatedReconnectStateRestartsCapGapEpisode(boolean roleReject) throws Exception {
        // Accrue an orphan drainer's cap-gap strikes to one short of terminal, then
        // simulate a long unrelated outage before another small-cap node appears. The
        // outage must end the old episode: its wall-clock duration says nothing about
        // whether the cluster's batch cap remained incompatible while no node answered.
        TestUtils.assertMemoryLeak(() -> {
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
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
                                        readInt(loopRef[0], "catchUpCapGapAttempts"));
                                // Model the elapsed outage without sleeping. With the defect,
                                // this old anchor survives the unrelated failure and the next
                                // cap gap immediately satisfies both terminal conditions.
                                staleAnchor[0] = System.nanoTime() - TimeUnit.HOURS.toNanos(2);
                                setField(loopRef[0], "catchUpCapGapFirstNanos", staleAnchor[0]);
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
                                        () -> setBooleanFieldUnchecked(loopRef[0], "running", false));
                            }
                            throw new AssertionError("unexpected reconnect call " + call);
                        },
                        5_000L, 0L, 0L, false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L, TimeUnit.HOURS.toMillis(1),
                        CursorWebSocketSendLoop.CatchUpCapGapPolicy.TERMINAL_AFTER_SETTLE_BUDGET);
                loopRef[0] = loop;
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    setBooleanField(loop, "running", true);
                    invokeConnectLoop(loop);

                    loop.checkError();
                    assertEquals("pre-outage cap gaps must not carry into the new episode",
                            1, readInt(loop, "catchUpCapGapAttempts"));
                    assertTrue("the post-outage cap gap must get a fresh dwell anchor",
                            readLong(loop, "catchUpCapGapFirstNanos") > staleAnchor[0]);
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

    private static int readInt(CursorWebSocketSendLoop loop, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(loop);
    }

    // Parses the loop's native sent-dictionary mirror ([len varint][utf8]...) back
    // into the symbol strings a reconnect catch-up would re-register.
    private static List<String> readMirrorSymbols(CursorWebSocketSendLoop loop) throws Exception {
        long addr = readLong(loop, "sentDictBytesAddr");
        int len = readInt(loop, "sentDictBytesLen");
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
            Field f = CursorWebSocketSendLoop.class.getDeclaredField("responseHandler");
            f.setAccessible(true);
            Object handler = f.get(loop);
            Method m = handler.getClass().getDeclaredMethod("onBinaryMessage", long.class, int.class);
            m.setAccessible(true);
            m.invoke(handler, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void invokeSetWireBaselineWithCatchUp(CursorWebSocketSendLoop loop, long replayStart) throws Exception {
        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod("setWireBaselineWithCatchUp", long.class);
        m.setAccessible(true);
        m.invoke(loop, replayStart);
    }

    private static void invokeConnectLoop(CursorWebSocketSendLoop loop) throws Exception {
        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                "connectLoop", Throwable.class, String.class, long.class);
        m.setAccessible(true);
        m.invoke(loop, new LineSenderException("test reconnect"), "reconnect", 0L);
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
                5_000L, 100L, 5_000L, false,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                0L, capGapWindowMillis,
                CursorWebSocketSendLoop.CatchUpCapGapPolicy.TERMINAL_AFTER_SETTLE_BUDGET);
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
                5_000L, 100L, 5_000L, false,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                0L, 0L);
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16_384);
    }

    private static long readLong(CursorWebSocketSendLoop loop, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(loop);
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
        setField(loop, "sentDictBytesAddr", addr);
        setIntField(loop, "sentDictBytesCapacity", total);
        setIntField(loop, "sentDictBytesLen", total);
        setIntField(loop, "sentDictCount", symbols.length);
    }

    private static void setField(Object target, String name, long value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static void setIntField(Object target, String name, int value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    private static void setBooleanField(Object target, String name, boolean value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setBoolean(target, value);
    }

    private static void setBooleanFieldUnchecked(Object target, String name, boolean value) {
        try {
            setBooleanField(target, name, value);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
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
        private final Runnable onCapRead;
        private final boolean throwOnSend;
        private int framesSent;

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
