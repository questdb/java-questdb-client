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
import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Poison-frame detector and NACK-recycle pacing under durable-ack mode.
 * <p>
 * The detector's contract (design/qwp-nack-policy-v2.md): a frame the server
 * deterministically rejects — {@code maxHeadFrameRejections} consecutive
 * server-active rejections of the same frame with no acceptance progress in
 * between — escalates to a typed {@code PROTOCOL_VIOLATION} terminal instead
 * of recycling the connection forever. "Acceptance progress" must be measured
 * at the OK level (highest {@code STATUS_OK}-acknowledged FSN), NOT at the
 * engine's trim watermark: in durable-ack mode {@code ackedFsn} advances only
 * on {@code STATUS_DURABLE_ACK} coverage, so every post-NACK recycle replays
 * from the durable watermark and re-OKs frames BEHIND the poisoned one.
 * Keying/resetting strikes on the trim watermark lets those re-OKs launder the
 * strike count each cycle — a deterministically-NACKing frame then recycles
 * the connection indefinitely (each cycle a full connect + window replay).
 * <p>
 * Related pacing promise from the same doc: RETRIABLE recycles go through
 * "capped backoff + jitter". A NACK against a healthy, reachable server must
 * therefore not spin the recycle loop at server NACK rate.
 */
public class CursorWebSocketSendLoopPoisonFrameTest {

    private static final int MAX_REJECTIONS = 3;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-poison-" + System.nanoTime()).toString();
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
    public void testDurableModeDetectorFiresDespiteReplayReOks() throws Exception {
        // Durable-ack mode, two frames in the SF log. Frame 0 is OK'd but its
        // durable ack never arrives (upload lag), so the trim watermark stays
        // at -1. Frame 1 is deterministically NACK'd (WRITE_ERROR, RETRIABLE).
        // Each recycle replays from the durable watermark: the server re-OKs
        // frame 0, then re-NACKs frame 1. After MAX_REJECTIONS such cycles the
        // detector MUST escalate frame 1 to a PROTOCOL_VIOLATION terminal --
        // the rejection is deterministic and replay cannot succeed. The re-OKs
        // of frame 0 are progress BEHIND the poisoned frame and must not
        // reset the strike count.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);

                for (int cycle = 1; cycle <= MAX_REJECTIONS; cycle++) {
                    // One connection generation: both frames (re)sent...
                    setSentCount(loop, 2);
                    // ...server re-OKs the already-applied frame 0 (fsn 0)...
                    deliverOk(loop, 0, names("trades"), txns(7L));
                    // Durable trim never advances: the OK is queued awaiting
                    // its STATUS_DURABLE_ACK, which never arrives.
                    assertEquals(-1L, engine.ackedFsn());
                    // ...then the server deterministically NACKs frame 1
                    // (fsn 1). The RETRIABLE policy recycles the wire inline
                    // through the real connectLoop/swapClient machinery:
                    // durable-ack tracking is dropped, wire sequencing
                    // restarts, replay resumes from ackedFsn+1 = fsn 0.
                    deliverRetriableNack(loop, 1, "disk full");
                }

                try {
                    loop.checkError();
                    fail("after " + MAX_REJECTIONS + " consecutive rejections of the same frame "
                            + "with no OK-level progress at or beyond it, the poison-frame "
                            + "detector must latch a PROTOCOL_VIOLATION terminal -- otherwise a "
                            + "deterministically-NACKing frame recycles the connection forever");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION,
                            e.getServerError().getCategory());
                    assertEquals(SenderError.Policy.TERMINAL,
                            e.getServerError().getAppliedPolicy());
                }
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testPoisonTerminalNamesTheRejectedFsn() throws Exception {
        // The escalated terminal must name the frame the server rejected, not
        // "durable trim watermark + 1". Frame 0 is OK'd (awaiting its durable
        // ack, so ackedFsn stays -1); frame 1 draws MAX_REJECTIONS consecutive
        // NACKs. The poisoned frame is fsn 1 -- reporting fsn 0 points the
        // operator at bytes the server ACCEPTED.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                setSentCount(loop, 2);

                deliverOk(loop, 0, names("trades"), txns(7L));
                for (int i = 0; i < MAX_REJECTIONS; i++) {
                    // Each RETRIABLE NACK recycles the wire (real swapClient,
                    // wire seq reset) -- restore the "both frames replayed"
                    // state before the next rejection so the NACK is
                    // attributable to a sent frame.
                    setSentCount(loop, 2);
                    deliverRetriableNack(loop, 1, "disk full");
                }

                try {
                    loop.checkError();
                    fail("poison-frame detector must have escalated by now");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION,
                            e.getServerError().getCategory());
                    assertEquals("the poisoned-frame terminal must name the FSN the server "
                                    + "rejected (fsn 1), not the durable trim watermark + 1 "
                                    + "(fsn 0 -- a frame the server accepted)",
                            1L, e.getServerError().getFromFsn());
                    // A NACK names the exact frame, so the poison span is that
                    // single frame -- not [fsn, publishedFsn], which would
                    // sweep in frames that are merely head-of-line blocked.
                    assertEquals(1L, e.getServerError().getToFsn());
                }
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNonOrderlyClosePoisonKeysOnOkLevelHeadOfLine() throws Exception {
        // The close-strike variant of the FSN-attribution rule: a non-orderly
        // close cannot name a frame, so the detector charges the OK-level
        // head-of-line frame -- the first frame the server has NOT accepted
        // at the OK level. Frame 0 is OK'd (awaiting its durable ack, trim
        // still -1); the connection then dies non-orderly after each replay.
        // The poisoned suspect is fsn 1, not "durable trim + 1" = fsn 0.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                setSentCount(loop, 2);

                deliverOk(loop, 0, names("trades"), txns(7L));
                for (int i = 0; i < MAX_REJECTIONS; i++) {
                    // Non-orderly close after at least one send on this
                    // connection counts a strike; each close recycles the
                    // wire (real swapClient), so restore the sent count
                    // before the next close event.
                    setSentCount(loop, 2);
                    deliverNonOrderlyClose(loop);
                }

                try {
                    loop.checkError();
                    fail("poison-frame detector must have escalated by now");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION,
                            e.getServerError().getCategory());
                    assertEquals("a close-attributed poison must charge the OK-level "
                                    + "head-of-line frame (fsn 1), not the durable trim "
                                    + "watermark + 1 (fsn 0 -- accepted at the OK level)",
                            1L, e.getServerError().getFromFsn());
                }
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNonOrderlyCloseAfterOnlyCatchUpDoesNotStrike() throws Exception {
        // C2 regression: the dictionary catch-up advances nextWireSeq WITHOUT
        // sending a data frame. A non-orderly close in that window -- a flapping
        // LB/middlebox that completes the upgrade, accepts the catch-up, then drops
        // before the first replay frame -- must be strike-EXEMPT. Keying the
        // poison-strike gate off nextWireSeq > 0 (rather than
        // dataFrameSentThisConnection) charges a strike on a frame that was never
        // sent; MAX_REJECTIONS such closes then escalate a TRANSIENT outage to a
        // PROTOCOL_VIOLATION terminal, hard-failing the producer and quarantining an
        // orphan drainer -- exactly what store-and-forward's retry-forever contract
        // forbids. Mirror image of testNonOrderlyClosePoisonKeysOnOkLevelHeadOfLine,
        // which sends a real data frame (setSentCount) and DOES escalate.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                for (int i = 0; i < MAX_REJECTIONS + 2; i++) {
                    // Model the catch-up: nextWireSeq advanced, but NO data frame
                    // sent. swapClient resets both on every recycle, so re-apply
                    // before each close. Pre-fix, each of these lands a strike on the
                    // never-sent head frame and the loop terminals by now.
                    setCatchUpWireSeqOnly(loop, 2);
                    deliverNonOrderlyClose(loop);
                }
                // No strike was ever charged, so nothing escalated: the loop stays
                // retriable and the producer-facing error latch is clear.
                assertEquals("a catch-up-only non-orderly close must charge no poison strike",
                        0, loop.poisonStrikes());
                loop.checkError();
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNonOrderlyRejectionAfterOnlyCatchUpDoesNotStrike() throws Exception {
        // C2 regression: the server-NACK twin of
        // testNonOrderlyCloseAfterOnlyCatchUpDoesNotStrike. handleServerRejection's
        // pre-send gate keys off dataFrameSentThisConnection, NOT nextWireSeq -- the
        // dictionary catch-up advances nextWireSeq WITHOUT sending a data frame. A
        // server NACK of a catch-up frame (nextWireSeq>0, no data frame sent) must
        // take the pre-send path (surface + recycle, no strike), not the post-send
        // poison-strike path. Pre-fix (gate on highestSent >= 0, i.e. nextWireSeq>0)
        // each catch-up NACK strikes the never-sent head frame; MAX_REJECTIONS such
        // strikes escalate a TRANSIENT outage to a producer-fatal PROTOCOL_VIOLATION
        // terminal -- exactly what store-and-forward's retry-forever contract forbids.
        // WRITE_ERROR (RETRIABLE) is the discriminating category: it DOES accrue
        // strikes on the post-send path (see testNackRecycleIsPacedAgainstHealthyServer),
        // so a regressed gate escalates here and checkError() throws.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                for (int i = 0; i < MAX_REJECTIONS + 2; i++) {
                    // Model the catch-up: nextWireSeq advanced, but NO data frame
                    // sent. The pre-send recycle (swapClient) resets nextWireSeq, so
                    // re-apply before each NACK (mirrors the onClose twin).
                    setCatchUpWireSeqOnly(loop, 2);
                    deliverRetriableNack(loop, 1, "disk full");
                }
                // No strike was ever charged, so nothing escalated: the loop stays
                // retriable and the producer-facing error latch is clear.
                assertEquals("a catch-up-only NACK must charge no poison strike",
                        0, loop.poisonStrikes());
                loop.checkError();
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNackRecycleIsPacedAgainstHealthyServer() throws Exception {
        // A reachable, healthy server that NACKs the head frame (RETRIABLE)
        // must not drive the recycle loop at server NACK rate: connectLoop's
        // backoff only engages after a FAILED connect attempt, and every
        // connect here succeeds instantly. design/qwp-nack-policy-v2.md
        // promises capped backoff + jitter for RETRIABLE recycles, so
        // NACK-initiated recycles must be paced by at least the initial
        // backoff. Red behavior: hundreds of full recycles per second.
        final long initialBackoffMillis = 200L;
        final long runMillis = 1_200L;

        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                final List<Long> reconnectNanos = new ArrayList<>();
                CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                    synchronized (reconnectNanos) {
                        reconnectNanos.add(System.nanoTime());
                    }
                    return new NackingClient();
                };
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        new NackingClient(), engine, 0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        initialBackoffMillis, 1_000L,
                        false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        // Keep the detector out of the way: this test measures
                        // pacing, not escalation.
                        1_000_000);
                try {
                    loop.start();
                    Thread.sleep(runMillis);
                } finally {
                    loop.close();
                }
                Long[] stamps;
                synchronized (reconnectNanos) {
                    stamps = reconnectNanos.toArray(new Long[0]); // insertion order == reconnect order
                }
                assertTrue("sanity: the NACK must actually recycle the connection at least once",
                        stamps.length >= 1);
                // Pacing is per-recycle: each recycle parks >= the reconnect backoff before
                // the next connect. Assert consecutive-reconnect gaps, not a count in a fixed
                // window, so a starved/oversleeping test thread only yields MORE paced
                // samples, never a false failure. Floor = 3/4 of the configured backoff:
                // below the >=200ms real park, far above the unpaced bug's sub-ms gaps.
                final long minGapNanos = initialBackoffMillis * 1_000_000L * 3 / 4;
                for (int i = 1; i < stamps.length; i++) {
                    long gapNanos = stamps[i] - stamps[i - 1];
                    assertTrue("NACK-triggered recycles against a healthy server must be paced "
                                    + "by the initial reconnect backoff (" + initialBackoffMillis
                                    + "ms): recycle " + i + " came " + (gapNanos / 1_000_000L)
                                    + "ms after the previous one -- the recycle loop is unpaced",
                            gapNanos >= minGapNanos);
                }
            }
        });
    }

    @Test
    public void testNonOrderlyCloseRecycleIsPacedAgainstAcceptingMiddlebox() throws Exception {
        // Close-path analogue of testNackRecycleIsPacedAgainstHealthyServer and
        // the regression guard for the poison-frame false-positive: a
        // middlebox/LB that completes the WS upgrade, accepts the head frame,
        // then non-orderly-closes (1006) while its backend is down succeeds at
        // CONNECT every cycle, so connectLoop's failed-connect backoff never
        // engages. Before the fix, onClose recycled through the UNPACED fail():
        // the strike counter climbed at connect+send+close RTT rate and latched
        // a PROTOCOL_VIOLATION terminal in well under a second on a TRANSIENT
        // outage. The fix routes the below-threshold close-strike through
        // failPaced (matching the NACK path), so the recycle parks before the
        // first reconnect. With the detector held out of the way (huge
        // maxHeadFrameRejections), the recycle rate must be bounded by the
        // reconnect backoff. Red behavior: hundreds of full recycles per second.
        final long initialBackoffMillis = 200L;
        final long runMillis = 1_200L;
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                final List<Long> reconnectNanos = new ArrayList<>();
                CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                    synchronized (reconnectNanos) {
                        reconnectNanos.add(System.nanoTime());
                    }
                    return new ClosingMiddleboxClient();
                };
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        new ClosingMiddleboxClient(), engine, 0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        initialBackoffMillis, 1_000L,
                        false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        // Keep the detector out of the way: this test measures
                        // close-path pacing, not escalation -- escalation is
                        // covered by testNonOrderlyClosePoisonKeysOnOkLevelHeadOfLine.
                        1_000_000);
                try {
                    loop.start();
                    Thread.sleep(runMillis);
                } finally {
                    loop.close();
                }
                Long[] stamps;
                synchronized (reconnectNanos) {
                    stamps = reconnectNanos.toArray(new Long[0]); // insertion order == reconnect order
                }
                assertTrue("sanity: the non-orderly close must actually recycle the connection at least once",
                        stamps.length >= 1);
                // Pacing is per-recycle: each recycle parks >= the reconnect backoff before
                // the next connect. Assert consecutive-reconnect gaps, not a count in a fixed
                // window, so a starved/oversleeping test thread only yields MORE paced
                // samples, never a false failure. Floor = 3/4 of the configured backoff:
                // below the >=200ms real park, far above the unpaced bug's sub-ms gaps.
                final long minGapNanos = initialBackoffMillis * 1_000_000L * 3 / 4;
                for (int i = 1; i < stamps.length; i++) {
                    long gapNanos = stamps[i] - stamps[i - 1];
                    assertTrue("non-orderly-close recycles against an accepting middlebox must be "
                                    + "paced by the reconnect backoff (" + initialBackoffMillis
                                    + "ms): recycle " + i + " came " + (gapNanos / 1_000_000L)
                                    + "ms after the previous one -- the close path is recycling unpaced",
                            gapNanos >= minGapNanos);
                }
            }
        });
    }

    @Test
    public void testOrderlyCloseChurnIsPacedAfterFirstRecycle() throws Exception {
        // Orderly closes (GOING_AWAY / NORMAL_CLOSURE) are deliberately
        // strike-exempt -- the server is asking us to go elsewhere, not
        // judging the bytes -- and connect always SUCCEEDS against a draining
        // LB, so neither failPaced's strike-keyed dose nor connectLoop's
        // failed-connect backoff ever engages, and no poison terminal bounds
        // the loop (correct: Invariant B). Before the fix these recycles ran
        // through the unpaced fail(): a drain window churned full reconnects
        // (fresh WebSocketClient + SSLContext + trust-store read) at
        // handshake RTT rate, forever. The zero-progress pacer must keep the
        // FIRST recycle immediate (failover latency) and pace consecutive
        // zero-progress repeats with the escalating reconnect backoff.
        // Red behavior: hundreds of full recycles per second.
        final long initialBackoffMillis = 200L;
        final long runMillis = 1_200L;
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                final List<Long> reconnectNanos = new ArrayList<>();
                CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                    synchronized (reconnectNanos) {
                        reconnectNanos.add(System.nanoTime());
                    }
                    return new OrderlyClosingClient();
                };
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        new OrderlyClosingClient(), engine, 0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        initialBackoffMillis, 1_000L,
                        false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        // Detector out of the way -- orderly closes must not
                        // strike anyway; this test measures pacing.
                        1_000_000);
                try {
                    loop.start();
                    Thread.sleep(runMillis);
                    // Orderly closes are exempt from the poison detector:
                    // pacing must not have turned into a terminal.
                    loop.checkError();
                } finally {
                    loop.close();
                }
                Long[] stamps;
                synchronized (reconnectNanos) {
                    stamps = reconnectNanos.toArray(new Long[0]); // insertion order == reconnect order
                }
                assertTrue("sanity: the orderly close must actually recycle the connection at least once",
                        stamps.length >= 1);
                // Pacing is per-recycle: the FIRST recycle is immediate (failover latency),
                // then each consecutive zero-progress recycle parks >= the reconnect backoff.
                // Assert consecutive-reconnect gaps, not a count in a fixed window, so a
                // starved/oversleeping test thread only yields MORE paced samples, never a
                // false failure. The one immediate first recycle is reconnect[0], never a
                // measured gap. Floor = 3/4 of the configured backoff.
                final long minGapNanos = initialBackoffMillis * 1_000_000L * 3 / 4;
                for (int i = 1; i < stamps.length; i++) {
                    long gapNanos = stamps[i] - stamps[i - 1];
                    assertTrue("orderly-close (GOING_AWAY) recycles against a draining peer must "
                                    + "be paced by the reconnect backoff after the first recycle ("
                                    + initialBackoffMillis + "ms): recycle " + i + " came "
                                    + (gapNanos / 1_000_000L) + "ms after the previous one -- the "
                                    + "orderly-close path is recycling unpaced",
                            gapNanos >= minGapNanos);
                }
            }
        });
    }

    @Test
    public void testPreSendCloseChurnIsPaced() throws Exception {
        // A peer that completes the WS upgrade then drops the connection
        // BEFORE the client sends anything (health-checked frontend with a
        // dead backend, idle-timeout proxy) hits the close-before-any-send
        // branch: strike-exempt (nextWireSeq resets to 0 on every
        // swapClient, so EVERY cycle re-arms the exemption) and, before the
        // fix, unpaced -- an idle sender churned full reconnects at handshake
        // RTT rate forever without ever accruing a strike. Zero-progress
        // recycles must be paced; the exemption from the poison DETECTOR
        // must survive (pacing, not a terminal -- Invariant B).
        final long initialBackoffMillis = 200L;
        final long runMillis = 1_200L;

        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                // Deliberately NO frames: the engine is idle, so nothing is
                // ever sent and every close arrives with nextWireSeq == 0.
                final List<Long> reconnectNanos = new ArrayList<>();
                CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                    synchronized (reconnectNanos) {
                        reconnectNanos.add(System.nanoTime());
                    }
                    return new PreSendClosingClient();
                };
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        new PreSendClosingClient(), engine, 0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        initialBackoffMillis, 1_000L,
                        false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        1_000_000);
                try {
                    loop.start();
                    Thread.sleep(runMillis);
                    // Close-before-any-send is strike-exempt: the pacer must
                    // not have escalated to a poison terminal.
                    loop.checkError();
                } finally {
                    loop.close();
                }
                Long[] stamps;
                synchronized (reconnectNanos) {
                    stamps = reconnectNanos.toArray(new Long[0]); // insertion order == reconnect order
                }
                assertTrue("sanity: the pre-send close must actually recycle the connection at least once",
                        stamps.length >= 1);
                // Pacing is per-recycle: the FIRST recycle is immediate (failover latency),
                // then each consecutive zero-progress recycle parks >= the reconnect backoff.
                // Assert consecutive-reconnect gaps, not a count in a fixed window, so a
                // starved/oversleeping test thread only yields MORE paced samples, never a
                // false failure. The one immediate first recycle is reconnect[0], never a
                // measured gap. Floor = 3/4 of the configured backoff.
                final long minGapNanos = initialBackoffMillis * 1_000_000L * 3 / 4;
                for (int i = 1; i < stamps.length; i++) {
                    long gapNanos = stamps[i] - stamps[i - 1];
                    assertTrue("pre-send close recycles (close before any send, nextWireSeq == 0) "
                                    + "must be paced by the reconnect backoff (" + initialBackoffMillis
                                    + "ms): recycle " + i + " came " + (gapNanos / 1_000_000L)
                                    + "ms after the previous one -- the strike-exempt close path "
                                    + "is recycling unpaced",
                            gapNanos >= minGapNanos);
                }
            }
        });
    }

    @Test
    public void testExemptRecyclePacerFirstImmediateThenEscalatesAndResetsOnProgress() throws Exception {
        // The zero-progress pacer's contract, pinned at the recycle level:
        // (1) the FIRST exempt recycle is immediate -- a one-off GOING_AWAY
        // handoff must rotate endpoints without delay (failover latency);
        // (2) a CONSECUTIVE exempt recycle with no acceptance progress in
        // between parks for at least the initial reconnect backoff;
        // (3) OK-level progress resets the pacer, so the next exempt recycle
        // is treated as a fresh failover, not churn.
        final long initialBackoffMillis = 500L;
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newExemptPacerLoop(engine, clients, initialBackoffMillis);

                long t0 = System.nanoTime();
                deliverOrderlyClose(loop);
                long firstNanos = System.nanoTime() - t0;
                assertEquals("the FIRST exempt recycle must take the immediate (level-0) branch",
                        1, loop.zeroProgressRecycles());

                t0 = System.nanoTime();
                deliverOrderlyClose(loop);
                long secondNanos = System.nanoTime() - t0;
                assertEquals("a CONSECUTIVE exempt recycle must take the paced (level-1) branch",
                        2, loop.zeroProgressRecycles());
                assertTrue("a CONSECUTIVE zero-progress exempt recycle must park for at least the "
                                + "initial reconnect backoff (" + initialBackoffMillis + "ms): took "
                                + (secondNanos / 1_000_000L) + "ms",
                        secondNanos >= initialBackoffMillis * 1_000_000L);
                assertTrue("the FIRST exempt recycle must stay immediate (failover latency), far "
                                + "shorter than the paced second: first=" + (firstNanos / 1_000_000L)
                                + "ms second=" + (secondNanos / 1_000_000L) + "ms",
                        firstNanos * 2 <= secondNanos);

                // OK-level acceptance progress (fsn 0 accepted) resets the
                // pacer: the next exempt recycle is a fresh failover.
                setSentCount(loop, 1);
                deliverOk(loop, 0, names("trades"), txns(7L));
                t0 = System.nanoTime();
                deliverOrderlyClose(loop);
                long thirdNanos = System.nanoTime() - t0;
                assertEquals("OK-level progress must reset the zero-progress pacer -- the next exempt "
                                + "recycle is a fresh failover (level 0), not churn",
                        1, loop.zeroProgressRecycles());
                assertTrue("the post-progress exempt recycle must stay immediate: third="
                                + (thirdNanos / 1_000_000L) + "ms second=" + (secondNanos / 1_000_000L) + "ms",
                        thirdNanos * 2 <= secondNanos);
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testPoisonDwellHoldsEscalationUntilWallClockWindowElapses() throws Exception {
        // P2 guard for the minimum-escalation-window: even once the strike
        // threshold (here 2) is reached, the detector must NOT escalate until
        // the suspect has stayed poisoned for at least the configured wall-clock
        // window. Strikes here accrue synchronously in milliseconds, so a burst
        // past the threshold must leave the loop ALIVE while the window is open;
        // only after it elapses may a further same-frame strike escalate. The
        // window=0 (legacy immediate) path is covered by
        // testPoisonTerminalNamesTheRejectedFsn.
        TestUtils.assertMemoryLeak(() -> {
            // Claim A: strike threshold reached, dwell window still open -> NO escalation.
            // Huge window so a synchronous burst can't cross it; no sleep -> no added time.
            final long heldWindowMillis = 60_000L;
            List<WebSocketClient> clientsHeld = new ArrayList<>();
            try (CursorSendEngine engineHeld = newEngine()) {
                appendFrames(engineHeld, 2);
                CursorWebSocketSendLoop loopHeld = newDurableLoopWithWindow(engineHeld, clientsHeld, 2, heldWindowMillis);
                setSentCount(loopHeld, 2);
                deliverOk(loopHeld, 0, names("trades"), txns(7L));
                for (int i = 0; i < 3; i++) {
                    setSentCount(loopHeld, 2);
                    deliverRetriableNack(loopHeld, 1, "disk full");
                }
                assertEquals("burst must accrue strikes past the threshold (non-vacuous)",
                        3, loopHeld.poisonStrikes());
                loopHeld.checkError(); // window open -> must not have escalated
            } finally {
                closeAll(clientsHeld);
            }

            // Claim B: once the window elapses, the next same-frame strike escalates.
            final long escWindowMillis = 200L;
            List<WebSocketClient> clientsEsc = new ArrayList<>();
            try (CursorSendEngine engineEsc = newEngine()) {
                appendFrames(engineEsc, 2);
                CursorWebSocketSendLoop loopEsc = newDurableLoopWithWindow(engineEsc, clientsEsc, 2, escWindowMillis);
                setSentCount(loopEsc, 2);
                deliverOk(loopEsc, 0, names("trades"), txns(7L));
                setSentCount(loopEsc, 2);
                deliverRetriableNack(loopEsc, 1, "disk full"); // strike 1 anchors the window
                loopEsc.checkError();                           // below threshold -> no escalation
                Thread.sleep(escWindowMillis + 150);            // cross the window (sleep only lengthens under starvation)
                setSentCount(loopEsc, 2);
                deliverRetriableNack(loopEsc, 1, "disk full"); // threshold reached AND window elapsed
                try {
                    loopEsc.checkError();
                    fail("after the dwell window elapses, the next strike must escalate");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION, e.getServerError().getCategory());
                    assertEquals("escalated terminal must still name the poisoned frame (fsn 1)",
                            1L, e.getServerError().getFromFsn());
                }
            } finally {
                closeAll(clientsEsc);
            }
        });
    }

    @Test
    public void testPostSendCatchUpNackDoesNotStrikeOrLaunderPoisonState() throws Exception {
        // Regression: after a reconnect the loop ships the head DATA frame
        // (dataFrameSentThisConnection=true) BEFORE tryReceiveAcks reads the server's
        // NACK of a dictionary CATCH-UP frame in the same loop iteration. That NACK
        // names a wire seq below the replay head, so its fsn = fsnAtZero+cappedSeq is
        // at or below ackedFsn -- negative when replayStart < catchUpFrames. It must
        // NOT charge a poison strike: recordHeadRejectionStrike(fsn) would set
        // poisonFsn to that value (the common shape yields -1, the "no suspect"
        // sentinel), laundering any genuine in-progress poison run and reporting a
        // bogus fsn. The fix routes an fsn <= ackedFsn rejection to the pre-send path
        // (surface + recycle, no strike), symmetric with the success path's
        // engine.acknowledge() no-op below ackedFsn.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                // Model: 2 catch-up frames (wire seq 0,1) + 1 data frame (wire seq 2)
                // sent, nothing acked (ackedFsn = -1), so fsnAtZero = replayStart(0) -
                // catchUpFrames(2) = -2. A NACK of catch-up wire seq 0 maps to fsn -2,
                // strictly below the -1 sentinel so the assertion discriminates.
                setPostCatchUpDataFrameBaseline(loop, -2L, 3L);
                assertEquals("precondition: no poison suspect yet",
                        -1L, loop.poisonFsn());

                deliverRetriableNack(loop, 0, "disk full");

                // The catch-up NACK was surfaced + recycled but charged NO strike, so
                // the sentinel is intact. Pre-fix it was set to -2, laundering the
                // poison detector's state.
                assertEquals("catch-up NACK must not set/launder the poison sentinel",
                        -1L, loop.poisonFsn());
                loop.checkError();
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testPostSendCatchUpNackAtAckedFsnBoundaryDoesNotStrike() throws Exception {
        // Boundary companion to testPostSendCatchUpNackDoesNotStrikeOrLaunderPoisonState:
        // the COMMON single-frame catch-up maps its only catch-up frame to
        // fsn == ackedFsn EXACTLY (the last catch-up frame lands on replayStart-1 ==
        // ackedFsn). The pre-send routing guard is "fsn <= ackedFsn", so this equality
        // case must also skip the poison strike. Without pinning it a "<=" -> "<"
        // mutation would slip the single/last catch-up frame's NACK onto the post-send
        // poison-strike path and launder the detector -- exactly the hazard the guard
        // closes -- yet the strictly-below test above would still pass.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                // 1 catch-up frame (wire seq 0) + 1 data frame (wire seq 1), nothing
                // acked (ackedFsn = -1), so fsnAtZero = replayStart(0) - catchUpFrames(1)
                // = -1. A NACK of catch-up wire seq 0 maps to fsn -1 == ackedFsn exactly.
                setPostCatchUpDataFrameBaseline(loop, -1L, 2L);
                assertEquals("precondition: no strikes yet",
                        0L, loop.poisonStrikes());

                deliverRetriableNack(loop, 0, "disk full");

                // poisonStrikes -- NOT poisonFsn -- is the discriminator at this exact
                // boundary: a wrongly-charged strike calls recordHeadRejectionStrike(-1),
                // which sets poisonFsn to the rejected fsn -1 -- the "no suspect"
                // sentinel -- so a poisonFsn check cannot see the laundering here. It
                // still bumps poisonStrikes 0 -> 1, so the correct pre-send routing (no
                // strike) shows as poisonStrikes staying 0. A "<=" -> "<" mutation of the
                // fsn-vs-ackedFsn guard charges the strike and fails this.
                assertEquals("catch-up NACK at fsn == ackedFsn must not charge a poison strike",
                        0L, loop.poisonStrikes());
                assertEquals("... and must leave the poison sentinel intact",
                        -1L, loop.poisonFsn());
                loop.checkError();
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testPostSendNotWritableNackNeverEscalatesToPoisonTerminal() throws Exception {
        // RETRIABLE_OTHER (NOT_WRITABLE) is a node-state verdict, not a frame
        // verdict: an all-replica window answers it on every rotated endpoint
        // for the SAME head frame, with no OK progress in between -- transient
        // by Invariant B (a replica gets promoted). It must therefore never
        // count a poison strike. Before the fix, handleServerRejection struck
        // unconditionally: the deliveries below crossed the threshold and
        // latched a producer-fatal PROTOCOL_VIOLATION terminal for a transient
        // cluster condition. The loop must instead stay alive and recycle the
        // wire on every rejection (endpoint rotation).
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                setSentCount(loop, 2);
                deliverOk(loop, 0, names("trades"), txns(7L));

                int recyclesBefore = clients.size();
                for (int i = 0; i < MAX_REJECTIONS + 2; i++) {
                    setSentCount(loop, 2);
                    deliverNotWritableNack(loop, 1, "read-only replica");
                }
                // Well past the strike threshold (dwell window is 0 here, so a
                // single wrongly-counted run would have escalated already):
                // must NOT have latched a terminal.
                loop.checkError();
                // ...and every rejection must still have recycled the wire
                // (rotate endpoints), not stalled.
                assertTrue("each NOT_WRITABLE rejection must recycle the connection: expected >= "
                                + (MAX_REJECTIONS + 2) + " reconnects, observed "
                                + (clients.size() - recyclesBefore),
                        clients.size() - recyclesBefore >= MAX_REJECTIONS + 2);
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNotWritableRecycleFirstImmediateThenPaced() throws Exception {
        // Routing pin for the post-send RETRIABLE_OTHER recycle: it must go
        // through the zero-progress pacer (failExemptPaced) -- not raw fail(),
        // which leaves every recycle immediate so an all-replica window churns
        // full TLS reconnects at handshake RTT rate for its whole duration;
        // and not failPaced, which parks even the FIRST recycle while a
        // genuine failover must rotate endpoints without delay.
        final long initialBackoffMillis = 500L;
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newExemptPacerLoop(engine, clients, initialBackoffMillis);

                setSentCount(loop, 2);
                long t0 = System.nanoTime();
                deliverNotWritableNack(loop, 1, "read-only replica");
                long firstNanos = System.nanoTime() - t0;
                assertEquals("the FIRST NOT_WRITABLE recycle must take the immediate (level-0) branch",
                        1, loop.zeroProgressRecycles());

                setSentCount(loop, 2);
                t0 = System.nanoTime();
                deliverNotWritableNack(loop, 1, "read-only replica");
                long secondNanos = System.nanoTime() - t0;
                assertEquals("a CONSECUTIVE NOT_WRITABLE recycle must take the paced (level-1) branch",
                        2, loop.zeroProgressRecycles());
                assertTrue("a CONSECUTIVE zero-progress NOT_WRITABLE recycle must park for at least the "
                                + "initial reconnect backoff (" + initialBackoffMillis + "ms): took "
                                + (secondNanos / 1_000_000L) + "ms",
                        secondNanos >= initialBackoffMillis * 1_000_000L);
                assertTrue("the FIRST NOT_WRITABLE recycle must stay immediate: first="
                                + (firstNanos / 1_000_000L) + "ms second=" + (secondNanos / 1_000_000L) + "ms",
                        firstNanos * 2 <= secondNanos);

                // Pacing only, never a terminal: NOT_WRITABLE says nothing
                // about the bytes.
                loop.checkError();
            } finally {
                closeAll(clients);
            }
        });
    }

    // ---------------------------------------------------------------------
    // harness
    // ---------------------------------------------------------------------

    /**
     * In-memory transport emulating a healthy server that deterministically
     * NACKs the head frame: accepts the connection, waits for one send on
     * this connection, then delivers exactly one WRITE_ERROR (RETRIABLE)
     * rejection for wireSeq 0.
     */
    private static final class NackingClient extends WebSocketClient {
        private boolean nackDelivered;
        private volatile int sentCount;

        NackingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sentCount++;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (nackDelivered || sentCount == 0) {
                return false;
            }
            nackDelivered = true;
            long packed = buildErrorPayload(0L, WebSocketResponse.STATUS_WRITE_ERROR, "disk full");
            long ptr = packed & 0xFFFFFFFFFFFFL;
            int size = (int) (packed >>> 48);
            try {
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

    /**
     * In-memory transport emulating a middlebox/LB that completes the WS
     * upgrade and accepts the head frame, then non-orderly-closes (1006)
     * because its backend is down: accepts the connection, waits for one
     * send on this connection, then delivers exactly one ABNORMAL_CLOSURE
     * close. No OK ever arrives, so the poison suspect FSN never advances --
     * the accepting-middlebox outage scenario.
     */
    private static final class ClosingMiddleboxClient extends WebSocketClient {
        private boolean closeDelivered;
        private volatile int sentCount;

        ClosingMiddleboxClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sentCount++;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (closeDelivered || sentCount == 0) {
                return false;
            }
            closeDelivered = true;
            // 1006 ABNORMAL_CLOSURE: non-orderly, after >=1 send -> counts a strike.
            handler.onClose(1006, "backend down");
            return true;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }

    /**
     * In-memory transport emulating a load balancer draining connections: the
     * WS upgrade completes, then the peer sends an orderly GOING_AWAY close.
     * Orderly closes are strike-exempt by design, so before the zero-progress
     * pacer the recycle loop churned at connect+close RTT rate.
     */
    private static final class OrderlyClosingClient extends WebSocketClient {
        private boolean closeDelivered;

        OrderlyClosingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (closeDelivered) {
                return false;
            }
            closeDelivered = true;
            // 1001 GOING_AWAY: orderly, never a strike.
            handler.onClose(1001, "server draining");
            return true;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }

    /**
     * In-memory transport emulating a health-checked frontend with a dead
     * backend: the WS upgrade completes, then the connection dies non-orderly
     * (1006) before the client sends anything. With nothing sent on the
     * connection (nextWireSeq == 0) the close is strike-exempt -- the
     * exemption re-arms every cycle because swapClient resets nextWireSeq.
     */
    private static final class PreSendClosingClient extends WebSocketClient {
        private boolean closeDelivered;

        PreSendClosingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (closeDelivered) {
                return false;
            }
            closeDelivered = true;
            // 1006 ABNORMAL_CLOSURE before any send on this connection.
            handler.onClose(1006, "backend down");
            return true;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
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

    private static long buildErrorPayload(long wireSeq, byte status, String message) {
        // Error frame: status(1) + sequence(8) + msgLen(2) + bytes
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        int size = 11 + msg.length;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putByte(ptr, status);
        Unsafe.getUnsafe().putLong(ptr + 1, wireSeq);
        Unsafe.getUnsafe().putShort(ptr + 9, (short) msg.length);
        for (int i = 0; i < msg.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + 11 + i, msg[i]);
        }
        return ptr | (((long) size) << 48);
    }

    private static long buildOkPayload(long wireSeq, String[] tableNames, long[] seqTxns) {
        // STATUS_OK frame: status(1) + sequence(8) + tableCount(2) + entries
        int size = 11;
        for (String t : tableNames) size += 2 + t.getBytes(StandardCharsets.UTF_8).length + 8;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        int offset = 0;
        Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_OK);
        offset += 1;
        Unsafe.getUnsafe().putLong(ptr + offset, wireSeq);
        offset += 8;
        Unsafe.getUnsafe().putShort(ptr + offset, (short) tableNames.length);
        offset += 2;
        for (int i = 0; i < tableNames.length; i++) {
            byte[] name = tableNames[i].getBytes(StandardCharsets.UTF_8);
            Unsafe.getUnsafe().putShort(ptr + offset, (short) name.length);
            offset += 2;
            for (int j = 0; j < name.length; j++) {
                Unsafe.getUnsafe().putByte(ptr + offset + j, name[j]);
            }
            offset += name.length;
            Unsafe.getUnsafe().putLong(ptr + offset, seqTxns[i]);
            offset += 8;
        }
        return ptr | (((long) size) << 48);
    }

    private static void deliverOk(CursorWebSocketSendLoop loop, long wireSeq,
                                  String[] tableNames, long[] seqTxns) throws Exception {
        long packed = buildOkPayload(wireSeq, tableNames, seqTxns);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void deliverOrderlyClose(CursorWebSocketSendLoop loop) throws Exception {
        loop.deliverCloseForTest(1001, "server draining"); // GOING_AWAY: orderly, strike-exempt
    }

    private static void deliverNonOrderlyClose(CursorWebSocketSendLoop loop) throws Exception {
        loop.deliverCloseForTest(1006, "connection reset"); // ABNORMAL_CLOSURE
    }

    private static void deliverNotWritableNack(CursorWebSocketSendLoop loop, long wireSeq,
                                               String msg) throws Exception {
        long packed = buildErrorPayload(wireSeq, WebSocketResponse.STATUS_NOT_WRITABLE, msg);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void deliverRetriableNack(CursorWebSocketSendLoop loop, long wireSeq,
                                             String msg) throws Exception {
        long packed = buildErrorPayload(wireSeq, WebSocketResponse.STATUS_WRITE_ERROR, msg);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void closeAll(List<WebSocketClient> clients) {
        // swapClient already closed all but the most recently installed
        // client; close() is idempotent, so sweep them all.
        for (WebSocketClient c : clients) {
            try {
                c.close();
            } catch (Throwable ignored) {
                // best-effort
            }
        }
    }

    private static void invokeOnBinaryMessage(CursorWebSocketSendLoop loop, long ptr, int size) throws Exception {
        loop.deliverResponseForTest(ptr, size);
    }

    private static String[] names(String... v) {
        return v;
    }

    private static void setSentCount(CursorWebSocketSendLoop loop, long count) throws Exception {
        loop.setNextWireSeqForTest(count);
        // A non-zero sent count models DATA frames sent on this connection. The
        // poison-strike / pre-send gates key off dataFrameSentThisConnection, not
        // nextWireSeq (the dictionary catch-up advances nextWireSeq without sending
        // a data frame), so keep the two in sync for these white-box scenarios.
        loop.setDataFrameSentThisConnectionForTest(count > 0);
    }

    // Sets ONLY nextWireSeq -- deliberately NOT dataFrameSentThisConnection -- to
    // model the dictionary catch-up having advanced the wire sequence with no data
    // frame sent yet. Contrast setSentCount, which sets both.
    private static void setCatchUpWireSeqOnly(CursorWebSocketSendLoop loop, long count) throws Exception {
        loop.setNextWireSeqForTest(count);
    }

    // Models a fresh connection that has shipped the dictionary catch-up (advancing
    // fsnAtZero below the replay head) AND the head data frame: sets fsnAtZero,
    // nextWireSeq, and dataFrameSentThisConnection=true together.
    private static void setPostCatchUpDataFrameBaseline(CursorWebSocketSendLoop loop,
                                                        long fsnAtZero, long nextWireSeq) throws Exception {
        loop.setFsnAtZeroForTest(fsnAtZero);
        loop.setNextWireSeqForTest(nextWireSeq);
        loop.setDataFrameSentThisConnectionForTest(true);
    }

    private static long[] txns(long... v) {
        return v;
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    /**
     * A durable-ack loop wired the way production wires it -- a live
     * reconnect factory and {@code running == true} -- but with the I/O
     * thread never spun: frames are delivered by reflection, and a RETRIABLE
     * NACK's inline recycle takes the REAL connectLoop/swapClient path
     * (factory connect, durable tracking drop, wire-seq reset, cursor
     * repositioning). Backoffs are shrunk so paced recycles don't slow the
     * test down.
     */
    private CursorWebSocketSendLoop newDurableLoop(CursorSendEngine engine,
                                                   List<WebSocketClient> clients) throws Exception {
        CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    NackingClient c = new NackingClient();
                    clients.add(c);
                    return c;
                },
                5L, 10L, true,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                MAX_REJECTIONS);
        // The loop is driven directly by the test, not by its own I/O thread,
        // but the recycle machinery must see a live loop or connectLoop's guard
        // treats the first retriable NACK as fatal.
        loop.setRunningForTest(true);
        return loop;
    }

    /**
     * Same wiring as {@link #newDurableLoop} but with caller-chosen reconnect
     * backoffs large enough to measure: the zero-progress pacer's park is the
     * observable under test, so the doses must dominate scheduling noise.
     * The detector threshold is huge -- these tests exercise pacing on
     * strike-EXEMPT paths, never escalation.
     */
    private CursorWebSocketSendLoop newExemptPacerLoop(CursorSendEngine engine,
                                                       List<WebSocketClient> clients,
                                                       long initialBackoffMillis) throws Exception {
        CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    NackingClient c = new NackingClient();
                    clients.add(c);
                    return c;
                },
                initialBackoffMillis, 5 * initialBackoffMillis, true,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                1_000_000);
        loop.setRunningForTest(true);
        return loop;
    }

    /**
     * Same as {@link #newDurableLoop} but with an explicit strike threshold and
     * a non-zero poison-escalation dwell window (the 12-arg master constructor),
     * so a test can exercise the wall-clock gate that holds escalation until the
     * suspect has stayed poisoned long enough.
     */
    private CursorWebSocketSendLoop newDurableLoopWithWindow(CursorSendEngine engine,
                                                             List<WebSocketClient> clients,
                                                             int maxRejections,
                                                             long windowMillis) throws Exception {
        CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    NackingClient c = new NackingClient();
                    clients.add(c);
                    return c;
                },
                5L, 10L, true,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                maxRejections, windowMillis);
        loop.setRunningForTest(true);
        return loop;
    }
}
