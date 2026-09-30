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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.HttpTokenProvider;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Interleavings between the symbol-dictionary recycle swap
 * ({@code QwpWebSocketSender.recycleForDictReset()}) and two things outside
 * the producer's own control: a real connection outage on its own stream, and
 * a sibling {@code BackgroundDrainer} running against a co-located orphan
 * slot.
 * <p>
 * (a) proves the barrier refuses to swap while the I/O thread is itself
 * mid-reconnect (not idle, not yet given up): step 2's loop close can cancel
 * a live socket but not a reconnect blocked in a hostname resolve or a
 * credential pull, so the recycle stays armed, the producer keeps buffering
 * into the old epoch without observing the outage, and the swap runs at the
 * first drained barrier after the reconnect.
 * <p>
 * (b) proves the swap only ever tears down the producer's OWN cursor
 * engine/I/O loop: an orphan drainer's engine and loop are entirely separate
 * objects owned by {@code BackgroundDrainerPool}, so a recycle firing while a
 * drain is in flight must leave the drain untouched and able to complete
 * afterward.
 * <p>
 * (c) proves a drop that starts right behind the ack draining the ring --
 * after the barrier's link check -- cannot hold the producer either: step 2
 * stops the loop only if its I/O thread has not begun the reconnect.
 */
public class SymbolDictRecycleOutageTest {

    private static final String ORPHAN_MARKER_SYMBOL = "orphan-marker-1";

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * Kills the server out from under an armed, fully-drained sender, waits
     * for the I/O thread to actually enter its own reconnect loop (not just
     * assumed via a fixed sleep), then hits the barrier on the calling
     * thread. The barrier must not swap against a loop that is between
     * connections and must return at once: {@code reconnect_max_duration_millis}
     * bounds only the initial connect, and the loop-close join budget is
     * never entered. The main thread then revives a server on the same port,
     * mirroring {@code ReconnectTest}'s down-then-up realism, and the swap
     * runs at the first drained barrier after the reconnect.
     */
    @Test
    public void testSyncModeRecycleDoesNotBlockProducerDuringOutage() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("outage-recycle").toString();
            AckAllHandler firstHandler = new AckAllHandler();
            int port;
            try (TestWebSocketServer server = new TestWebSocketServer(firstHandler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2"
                        + ";reconnect_initial_backoff_millis=20"
                        + ";reconnect_max_backoff_millis=80"
                        + ";reconnect_max_duration_millis=6000;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: the arming batch must be acked before the outage",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Kill the connection AND the listener -- a real outage, not
                    // just a dropped socket the same server would re-accept
                    // instantly.
                    server.close();

                    // Confirm the I/O thread actually entered its own reconnect
                    // loop against the now-refused port before we hit the
                    // barrier -- so the barrier provably sees a loop that is
                    // between connections, not one that simply hasn't noticed
                    // the drop yet.
                    long attemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (ws.getTotalReconnectAttempts() == 0 && System.nanoTime() < attemptDeadline) {
                        Thread.sleep(5);
                    }
                    Assert.assertTrue("the I/O thread must have entered reconnect before "
                                    + "the triggering table() call",
                            ws.getTotalReconnectAttempts() > 0);

                    // The barrier must return at once and must not swap: step
                    // 2's loop close can cancel a live socket, but a reconnect
                    // blocked in a hostname resolve or a credential pull ignores
                    // the cancel and would hold this call for the join budget.
                    long startNanos = System.nanoTime();
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    Assert.assertTrue("no swap while the loop is between connections", ws.isResetArmed());
                    Assert.assertEquals("no swap while the loop is between connections",
                            0, ws.getSymbolDictEpoch());
                    Assert.assertTrue("the barrier must not block the producer on the reconnect "
                                    + "or the loop-close budget [elapsedMillis=" + elapsedMillis + ']',
                            elapsedMillis < 3_000);

                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(fsn2 > fsn1);
                    OutageRecycleHandler revivedHandler = new OutageRecycleHandler();
                    try (TestWebSocketServer revived =
                                 new TestWebSocketServer(revivedHandler, false, null, port)) {
                        revived.start();
                        Assert.assertTrue(revived.awaitStart(5, TimeUnit.SECONDS));
                        Assert.assertTrue("the outage-window row must land once reconnected",
                                sender.awaitAckedFsn(fsn2, 10_000));
                        Assert.assertEquals("the reconnect alone must not swap", 0, ws.getSymbolDictEpoch());

                        // Link up and ring drained: this barrier swaps, and "d"
                        // registers into the fresh dictionary.
                        sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                        Assert.assertFalse("the deferred swap must disarm", ws.isResetArmed());
                        Assert.assertEquals("the deferred swap must commit once", 1, ws.getSymbolDictEpoch());
                        long fsn3 = sender.flushAndGetSequence();
                        Assert.assertTrue(sender.awaitAckedFsn(fsn3, 10_000));
                        Assert.assertEquals("the post-swap connection's first frame must carry a "
                                        + "fresh dictionary, not a, b, c",
                                0, revivedHandler.firstFrameDeltaStart);
                        Assert.assertEquals(Collections.singletonList("d"), revivedHandler.dict());
                    }
                }
            }
        });
    }

    /**
     * Default configuration: no {@code reconnect_*} knob and no
     * {@code initial_connect_retry}, so the builder resolves
     * {@code initialConnectMode} to OFF. Under the store-and-forward
     * contract the barrier never swaps while the endpoint is down: the
     * triggering {@code table()} returns normally, the recycle stays armed,
     * the flush publishes into the old epoch's SF slot, and once the endpoint
     * returns on the same port the I/O loop's own reconnect replays every row
     * sent during the outage with zero loss. The first drained barrier after
     * the reconnect then commits exactly one epoch, and the fresh
     * connection's first frame carries the fresh dictionary.
     */
    @Test
    public void testDefaultConfigRecycleBuffersThroughOutage() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("default-config-outage").toString();
            AckAllHandler firstHandler = new AckAllHandler();
            int port;
            try (TestWebSocketServer server = new TestWebSocketServer(firstHandler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertTrue("the recycle must be on under a default configuration",
                            ws.isSymbolDictResetEnabled());

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: the arming batch must be acked before the outage",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Kill the listener AND the live connection. The ring is
                    // drained and the sender-level connected flag stays true,
                    // but the loop is between connections, so the next table()
                    // must NOT swap.
                    server.close();
                    long attemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (ws.getTotalReconnectAttempts() == 0 && System.nanoTime() < attemptDeadline) {
                        Thread.sleep(5);
                    }
                    Assert.assertTrue("the I/O thread must have entered reconnect before the barrier",
                            ws.getTotalReconnectAttempts() > 0);

                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertTrue("no swap while the loop is between connections", ws.isResetArmed());
                    Assert.assertEquals("no swap while the loop is between connections",
                            0, ws.getSymbolDictEpoch());
                    Assert.assertTrue(ws.wasEverConnected());

                    // Producer keeps working against the dead endpoint: the
                    // flush publishes into the old epoch's SF slot.
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-outage FSN must exceed pre-outage FSN", fsn2 > fsn1);

                    // Endpoint back on the SAME port: the I/O loop's own
                    // reconnect must land the buffered rows -- zero loss --
                    // and only then does the barrier swap.
                    OutageRecycleHandler revivedHandler = new OutageRecycleHandler();
                    try (TestWebSocketServer revived =
                                 new TestWebSocketServer(revivedHandler, false, null, port)) {
                        revived.start();
                        Assert.assertTrue(revived.awaitStart(5, TimeUnit.SECONDS));
                        Assert.assertTrue("rows sent during the outage must replay once "
                                        + "the endpoint returns",
                                sender.awaitAckedFsn(fsn2, 10_000));
                        Assert.assertEquals("the reconnect alone must not swap", 0, ws.getSymbolDictEpoch());
                        Assert.assertTrue("still armed after the reconnect", ws.isResetArmed());

                        // Link up and ring drained: this barrier swaps, and "d"
                        // registers into the fresh dictionary.
                        sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                        Assert.assertEquals("the deferred swap must commit exactly one epoch",
                                1, ws.getSymbolDictEpoch());
                        Assert.assertFalse("a committed swap disarms", ws.isResetArmed());
                        Assert.assertTrue("wasEverConnected() must stay sticky across the swap's "
                                        + "rebuilt loop", ws.wasEverConnected());
                        long fsn3 = sender.flushAndGetSequence();
                        Assert.assertTrue(sender.awaitAckedFsn(fsn3, 10_000));
                        Assert.assertEquals("the post-swap connection's first frame must carry a "
                                        + "fresh (empty) dictionary, not a, b, c",
                                0, revivedHandler.firstFrameDeltaStart);
                        Assert.assertEquals(Collections.singletonList("d"), revivedHandler.dict());

                        // And the epoch keeps extending normally from there.
                        sender.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                        long fsn4 = sender.flushAndGetSequence();
                        Assert.assertTrue(sender.awaitAckedFsn(fsn4, 5_000));
                        Assert.assertEquals("no second swap", 1, ws.getSymbolDictEpoch());
                        Assert.assertEquals("later batches must extend the same fresh dictionary",
                                Arrays.asList("d", "e"), revivedHandler.dict());
                    }
                }
            }
        });
    }

    /**
     * An orphan drainer's engine and I/O loop are objects entirely separate
     * from the foreground sender's own {@code cursorEngine}/{@code
     * cursorSendLoop} -- {@code BackgroundDrainerPool} owns them. Seeds a
     * sibling orphan slot (mirrors {@code OrphanScanIntegrationTest}'s ghost
     * recipe), lets the drainer adopt it and get its replay frame gated on
     * the wire, then arms and fires a recycle on the foreground stream while
     * the drain is provably still in flight. The recycle must leave the
     * drain untouched: releasing the gate afterward still lets it complete,
     * and every one of the three streams (pre-recycle foreground,
     * post-recycle foreground, drained orphan) lands with the right symbol.
     */
    @Test
    public void testOrphanDrainerSurvivesRecycleMidDrain() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("outage-orphan-drain").toString();

            // Phase 1: seed a sibling orphan slot. The ghost writes one row
            // carrying a uniquely-marked symbol and dies without ever being
            // acked -- same recipe as OrphanScanIntegrationTest.
            SilentHandler ghostSilent = new SilentHandler();
            try (TestWebSocketServer ghostServer = new TestWebSocketServer(ghostSilent)) {
                ghostServer.start();
                Assert.assertTrue(ghostServer.awaitStart(5, TimeUnit.SECONDS));
                String ghostCfg = "ws::addr=localhost:" + ghostServer.getPort()
                        + ";sf_dir=" + sfDir + ";sender_id=ghost;close_flush_timeout_millis=0;";
                try (Sender ghost = Sender.fromConfig(ghostCfg)) {
                    ghost.table("orphaned").symbol("s", ORPHAN_MARKER_SYMBOL).longColumn("v", 99L).atNow();
                    ghost.flush();
                    Assert.assertTrue("ghost frame must reach the wire before close",
                            ghostSilent.awaitFrame(5, TimeUnit.SECONDS));
                }
            }
            Assert.assertEquals("ghost slot must be a candidate orphan",
                    1, OrphanScanner.scan(sfDir, "primary").size());

            // Phase 2: one server serves both the primary sender and the
            // orphan drainer it spawns. Gating is CONTENT-based (whichever
            // connection ships the ghost's marker symbol), not
            // connection-order-based -- the drainer's connect can race the
            // primary's own first flush, and content-based gating stays
            // correct regardless of which one wins that race.
            PrimaryAndOrphanHandler handler = new PrimaryAndOrphanHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String primaryCfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";sender_id=primary;drain_orphans=on;symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(primaryCfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    // Let the drainer discover + adopt the ghost slot and get
                    // its replay frame gated on the wire before touching the
                    // foreground stream at all -- proves the two run
                    // concurrently, not sequentially.
                    Assert.assertTrue("orphan drainer must ship its replay frame",
                            handler.awaitOrphanFrame(10, TimeUnit.SECONDS));

                    // Arm + fire the recycle on the foreground stream. These
                    // frames carry none of the orphan marker, so they get
                    // acked immediately regardless of the drain's state.
                    sender.table("t").symbol("s", "pre-a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "pre-b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Recycle fires synchronously here, tearing down + rebuilding
                    // ONLY the foreground's own cursor engine/I/O loop.
                    sender.table("t").symbol("s", "post-c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());

                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle row must land on the fresh connection",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);

                    // The drain must still be exactly where it was -- gated,
                    // not failed, not restarted -- proving the recycle never
                    // reached into the drainer's separate stack.
                    Assert.assertFalse("the drainer's connection must not have been touched by "
                                    + "the foreground's recycle", handler.orphanAcked());

                    // Now release the drainer's gate: a drain that survived the
                    // recycle untouched must still be able to complete.
                    handler.releaseOrphan();
                    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (OrphanScanner.scan(sfDir, "primary").size() > 0
                            && System.nanoTime() < deadlineNanos) {
                        Thread.sleep(10);
                    }
                    Assert.assertEquals("orphan drainer must complete the drain after the recycle",
                            0, OrphanScanner.scan(sfDir, "primary").size());
                }

                // Per-row symbol correctness for all three streams.
                Assert.assertEquals("pre-recycle foreground stream",
                        Arrays.asList("pre-a", "pre-b"), handler.dictContaining("pre-a"));
                Assert.assertEquals("post-recycle foreground stream",
                        Collections.singletonList("post-c"), handler.dictContaining("post-c"));
                Assert.assertEquals("drained orphan stream",
                        Collections.singletonList(ORPHAN_MARKER_SYMBOL),
                        handler.dictContaining(ORPHAN_MARKER_SYMBOL));
            }
        });
    }

    /**
     * Invariant B's seed: a 401 handshake rejection AFTER a recycle is
     * transient only because the swap seeds the fresh loop with
     * markEverConnected() -- without it, the fresh loop would classify the
     * same 401 as a pre-first-connect endpoint-policy failure and latch a
     * terminal, turning a transient auth blip into data loss. With the
     * default (foreground) initial connect ensureConnected() records the
     * first connect itself; with an ASYNC initial connect only the I/O
     * thread ever sees it, so the seed exists only if the recycle's loop
     * close carries the loop's own sticky across the swap (step 2), and the
     * CLOSE_LOOP resume's re-close must carry it the same way.
     */
    @Test(timeout = 60_000L)
    public void testPostRecycleEndpointPolicyRejectionIsTransient() throws Exception {
        assertPostRecycle401IsTransient("", false);
    }

    @Test(timeout = 60_000L)
    public void testPostRecycleEndpointPolicyRejectionIsTransientWithAsyncInitialConnect() throws Exception {
        assertPostRecycle401IsTransient("initial_connect_retry=async;", false);
    }

    @Test(timeout = 60_000L)
    public void testPostCloseLoopResumeEndpointPolicyRejectionIsTransientWithAsyncInitialConnect() throws Exception {
        assertPostRecycle401IsTransient("initial_connect_retry=async;", true);
    }

    private void assertPostRecycle401IsTransient(String connectModeCfg, boolean viaCloseLoopResume) throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath()
                    .resolve("post-recycle-401-" + (connectModeCfg.isEmpty() ? "eager" : "async")
                            + (viaCloseLoopResume ? "-resume" : "")).toString();
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;" + connectModeCfg;
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: arm batch must be acked", sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());
                    Assert.assertTrue("setup: the first connect has happened", ws.wasEverConnected());

                    // Every handshake from here on is met with 401 -- including
                    // the fresh loop's very first connect.
                    server.setRejectWithStatus(401, "Unauthorized");

                    if (viaCloseLoopResume) {
                        // The state a step-2 close failure leaves behind: old
                        // loop dead, no swap, resume pending. The next row's
                        // table() finishes the close, and its send builds the
                        // fresh loop.
                        ws.forceCloseLoopAbandonForTesting();
                    }
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow(); // recycle, or resume, runs here
                    Assert.assertEquals(viaCloseLoopResume
                                    ? "the resume must not swap"
                                    : "the swap itself needs no connection",
                            viaCloseLoopResume ? 0 : 1, ws.getSymbolDictEpoch());
                    Assert.assertTrue(viaCloseLoopResume
                                    ? "the seed must survive the resume's re-close"
                                    : "the seed must survive the swap",
                            ws.wasEverConnected());

                    // Producing keeps working: the rejection is transient under
                    // Invariant B, so rows buffer and nothing latches.
                    long fsn2 = sender.flushAndGetSequence();

                    // The fresh loop's connect is deferred to its own I/O thread
                    // and races this thread, so wait for the server to actually
                    // observe (and reject) at least one handshake before checking
                    // anything below -- otherwise this thread could relent before
                    // the fresh loop's first attempt ever reaches the wire, and
                    // the assertions that follow would pass without exercising
                    // the 401 path at all.
                    long rejectDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (server.statusRejectCount() == 0 && System.nanoTime() < rejectDeadline) {
                        Thread.sleep(2);
                    }
                    Assert.assertTrue("the fresh loop must actually hit the 401 "
                                    + "before this test can exercise Invariant B's seed",
                            server.statusRejectCount() > 0);
                    Assert.assertNull("must not latch a terminal on a post-recycle 401",
                            ws.getLastTerminalError());

                    // Clear BEFORE close() -- drainOnClose would otherwise burn its
                    // whole flush budget against the rejecting server.
                    server.setRejectWithStatus(0, null);
                    Assert.assertTrue("buffered rows must land once the endpoint relents",
                            sender.awaitAckedFsn(fsn2, 10_000));
                    Assert.assertTrue(ws.wasEverConnected());
                }
            }
        });
    }

    /**
     * (c) A server that answers the last outstanding frame with its ack and
     * then closes -- a restart or drain sending GOING_AWAY -- while table()
     * sits in the starvation wait. The ack ends the wait with the link still
     * up, but the I/O thread starts its reconnect at once, typically while the
     * recycle is still taking the slot lock, and that reconnect's credential
     * pull ignores interrupts here, as a blocking java.net read or the bundled
     * OidcDeviceAuth's native HTTP round trip does. A step-2 close() landing
     * after the reconnect began used to wait out the whole shutdown budget,
     * throw, and leave a CLOSE_LOOP resume that waited it out again on every
     * send, so no row was accepted until the pull returned. Step 2 now stops
     * the loop only if the reconnect has not begun: every call returns
     * promptly without throwing, rows keep buffering while the pull is stuck,
     * and the recycle either ran at once or stays armed and runs at the first
     * drained barrier after the reconnect. The race is real, so a single
     * iteration may not lose it even without the fix (roughly a third of them
     * do): the loop repeats it, and the loop-close budget is shrunk so a
     * regression fails in half a second instead of thirty. An iteration whose
     * ack arrives only after the wait's deadline (a stalled CI box) never
     * reaches the swap; it must still pass, but most iterations must race.
     */
    @Test(timeout = 120_000L)
    public void testServerCloseBehindDrainingAckDoesNotStallProducer() throws Exception {
        assertMemoryLeak(() -> {
            int iterations = 15;
            int raced = 0;
            for (int i = 0; i < iterations; i++) {
                if (assertServerCloseBehindDrainingAckDoesNotStall(i)) {
                    raced++;
                }
            }
            Assert.assertTrue("too few iterations reached the swap to exercise the race [raced="
                    + raced + '/' + iterations + ']', raced >= 10);
        });
    }

    private static CursorWebSocketSendLoop cursorSendLoop(QwpWebSocketSender ws) throws Exception {
        Field f = QwpWebSocketSender.class.getDeclaredField("cursorSendLoop");
        f.setAccessible(true);
        return (CursorWebSocketSendLoop) f.get(ws);
    }

    /**
     * Returns whether the ack ended the starvation wait, i.e. whether this
     * iteration reached the swap while the reconnect was starting.
     */
    private boolean assertServerCloseBehindDrainingAckDoesNotStall(int iteration) throws Exception {
        final long maxWaitMillis = 300;
        final String at = "iteration " + iteration + ": ";
        String sfDir = temporaryFolder.getRoot().toPath().resolve("close-behind-ack-" + iteration).toString();
        HoldThenGoAwayHandler handler = new HoldThenGoAwayHandler();
        BlockingTokenProvider tokens = new BlockingTokenProvider();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("localhost:" + server.getPort())
                    .storeAndForwardDir(sfDir)
                    .symbolDictResetThreshold(2)
                    .symbolDictResetMaxWaitMillis(maxWaitMillis)
                    .httpTokenProvider(tokens)
                    .build()) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                try {
                    // A regression fails in half a second, not after the 30 s default.
                    cursorSendLoop(ws).setShutdownAwaitTimeoutMillis(500);

                    // Arm while the server withholds the arming batch's ack.
                    handler.hold = true;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertTrue(at + "the arming batch must reach the server",
                            handler.awaitHeld(5, TimeUnit.SECONDS));
                    Assert.assertTrue(at + "must be armed", ws.isResetArmed());
                    // Let the armed window elapse so the next table() takes the starvation wait.
                    Thread.sleep(maxWaitMillis + 50);
                    handler.hold = false;
                    tokens.block(); // the reconnect's credential pull will not return

                    Thread releaser = new Thread(() -> {
                        try {
                            Thread.sleep(50);
                            handler.ackHeldThenGoAway();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, "ack-then-go-away");
                    releaser.start();
                    long elapsedMillis;
                    try {
                        long t0 = System.nanoTime();
                        sender.table("t"); // parks in the starvation wait until the ack lands
                        elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    } catch (LineSenderException e) {
                        throw new AssertionError(at + "a close behind the draining ack must not fail the "
                                + "producer: " + e.getMessage(), e);
                    } finally {
                        releaser.join();
                    }
                    Assert.assertTrue(at + "table() must not wait on the reconnect [millis="
                            + elapsedMillis + ']', elapsedMillis < 2_000);
                    final boolean raced = ws.getSymbolDictResetStarvationTimeouts() == 0;
                    if (ws.getSymbolDictEpoch() == 0) {
                        Assert.assertTrue(at + "a recycle that lost the race stays armed", ws.isResetArmed());
                    }

                    // Either the old loop's reconnect or the fresh loop's first
                    // connect is now stuck in its pull; rows must keep buffering.
                    Assert.assertTrue(at + "the credential pull never blocked",
                            tokens.awaitBlocked(5, TimeUnit.SECONDS));
                    long fsn = -1L;
                    for (int r = 0; r < 5; r++) {
                        long t0 = System.nanoTime();
                        sender.table("t").symbol("s", "x").longColumn("v", r).atNow();
                        long next = sender.flushAndGetSequence();
                        long callMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                        Assert.assertTrue(at + "a row must not wait on the stuck pull [millis="
                                + callMillis + ']', callMillis < 2_000);
                        Assert.assertTrue(at + "rows must keep buffering while the pull is stuck", next > fsn);
                        fsn = next;
                    }

                    tokens.release();
                    Assert.assertTrue(at + "the buffered rows must land once the pull returns",
                            sender.awaitAckedFsn(fsn, 10_000));
                    sender.table("t"); // drained barrier with the link back up
                    Assert.assertEquals(at + "the recycle must commit exactly once", 1L, ws.getSymbolDictEpoch());
                    Assert.assertFalse(at + "a committed recycle disarms", ws.isResetArmed());
                    sender.symbol("s", "after").longColumn("v", 9L).atNow();
                    long after = sender.flushAndGetSequence();
                    Assert.assertTrue(at + "the post-recycle row must land", sender.awaitAckedFsn(after, 10_000));
                    return raced;
                } finally {
                    tokens.release();
                }
            }
        }
    }

    /** ACKs every frame it receives immediately; does not otherwise inspect the wire. */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Hands out a token at once until {@link #block()}; from then on each pull
     * parks until {@link #release()} and ignores interrupts, as a blocking
     * java.net read or the bundled OidcDeviceAuth's native HTTP round trip
     * does, so the loop's close() cannot cancel it.
     */
    private static class BlockingTokenProvider implements HttpTokenProvider {
        private final CountDownLatch pullBlocked = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);
        private volatile boolean blocking;

        @Override
        public CharSequence getToken() {
            if (blocking) {
                pullBlocked.countDown();
                boolean interrupted = false;
                while (released.getCount() != 0L) {
                    try {
                        released.await();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return "token";
        }

        boolean awaitBlocked(long timeout, TimeUnit unit) throws InterruptedException {
            return pullBlocked.await(timeout, unit);
        }

        void block() {
            blocking = true;
        }

        void release() {
            blocking = false;
            released.countDown();
        }
    }

    /**
     * Acks per connection (each connection's wire sequence restarts at 0)
     * unless {@link #hold} is set, in which case it withholds the ack. {@link
     * #ackHeldThenGoAway()} then answers the withheld frame and immediately
     * closes with GOING_AWAY: a restart or drain acking its backlog on the way
     * out.
     */
    private static class HoldThenGoAwayHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch held = new CountDownLatch(1);
        private final Map<TestWebSocketServer.ClientHandler, Long> nextSeq = new IdentityHashMap<>();
        volatile boolean hold;
        private TestWebSocketServer.ClientHandler heldClient;
        private long heldSeq = -1L;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long seq;
            synchronized (this) {
                Long n = nextSeq.get(client);
                seq = n == null ? 0L : n;
                nextSeq.put(client, seq + 1L);
                if (hold) {
                    heldClient = client;
                    heldSeq = seq;
                    held.countDown();
                    return;
                }
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(seq));
            } catch (IOException e) {
                // connection gone: the sender replays on its next one
            }
        }

        void ackHeldThenGoAway() throws IOException {
            TestWebSocketServer.ClientHandler client;
            long seq;
            synchronized (this) {
                client = heldClient;
                seq = heldSeq;
            }
            // The ack is cumulative: acking the last withheld frame drains the
            // ring. One write, so the close lands right behind the ack.
            client.sendBinaryThenClose(QwpWireTestUtils.buildAck(seq), WebSocketCloseCode.GOING_AWAY, "restart");
        }

        boolean awaitHeld(long timeout, TimeUnit unit) throws InterruptedException {
            return held.await(timeout, unit);
        }
    }

    /**
     * Tracks the most recent connection -- the old loop's reconnect first,
     * then the swap's fresh connection -- and records the delta-start id of
     * that connection's first data frame, so after the swap the fields
     * describe the fresh connection. Tracks by connection identity like
     * {@code SymbolDictRecycleTest.RecycleHandler} so a partially-established
     * retry that never sends data cannot corrupt the state of the connection
     * that actually does.
     */
    private static class OutageRecycleHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final List<String> dict = new ArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler currentClient;
        private boolean seenFirstDataFrame;
        volatile int firstFrameDeltaStart = -1;

        synchronized List<String> dict() {
            return new ArrayList<>(dict);
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                dict.clear();
                nextSeq.set(0);
                seenFirstDataFrame = false;
                firstFrameDeltaStart = -1;
            }
            QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            if (!seenFirstDataFrame && QwpWireTestUtils.tableCount(data) > 0) {
                seenFirstDataFrame = true;
                if (QwpWireTestUtils.hasDelta(data)) {
                    int[] pos = {HEADER_SIZE};
                    firstFrameDeltaStart = QwpWireTestUtils.readVarint(data, pos);
                }
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Receives binary frames but never acks. Causes the sender to leave
     * unacked data on disk on close -- mirrors {@code
     * OrphanScanIntegrationTest.SilentHandler}.
     */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch frameReceived = new CountDownLatch(1);

        boolean awaitFrame(long timeout, TimeUnit unit) throws InterruptedException {
            return frameReceived.await(timeout, unit);
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frameReceived.countDown();
        }
    }

    /**
     * Serves both the primary sender's own stream and the orphan drainer it
     * spawns from a single {@code TestWebSocketServer}. Acks every
     * connection's frames immediately EXCEPT whichever one ships {@link
     * #ORPHAN_MARKER_SYMBOL} -- that connection is identified by its wire
     * content, not by arrival order (the drainer's connect can race the
     * primary's own first flush), and is withheld until {@link
     * #releaseOrphan()}. Per-connection wire sequence counters mirror {@code
     * OrphanScanIntegrationTest.AckHandler}: each WebSocket connection numbers
     * its own frames from 0.
     */
    private static class PrimaryAndOrphanHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final ConcurrentHashMap<TestWebSocketServer.ClientHandler, ConnState> byClient =
                new ConcurrentHashMap<>();
        private final CountDownLatch orphanFrameSeen = new CountDownLatch(1);
        private final CountDownLatch orphanGate = new CountDownLatch(1);
        private volatile boolean orphanAcked;

        boolean awaitOrphanFrame(long timeout, TimeUnit unit) throws InterruptedException {
            return orphanFrameSeen.await(timeout, unit);
        }

        /** A copy of whichever connection's dictionary contains {@code marker}, or empty. */
        List<String> dictContaining(String marker) {
            for (ConnState state : byClient.values()) {
                synchronized (state.dict) {
                    if (state.dict.contains(marker)) {
                        return new ArrayList<>(state.dict);
                    }
                }
            }
            return Collections.emptyList();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            ConnState state = byClient.computeIfAbsent(client, c -> new ConnState());
            boolean isOrphanFrame;
            synchronized (state.dict) {
                QwpWireTestUtils.accumulateDeltaDictionary(data, state.dict);
                isOrphanFrame = state.dict.contains(ORPHAN_MARKER_SYMBOL);
            }
            if (isOrphanFrame) {
                orphanFrameSeen.countDown();
                try {
                    if (!orphanGate.await(20, TimeUnit.SECONDS)) {
                        throw new AssertionError("orphan ack gate never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                orphanAcked = true;
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(state.seq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** True once the gated orphan frame has actually been acked (gate released). */
        boolean orphanAcked() {
            return orphanAcked;
        }

        void releaseOrphan() {
            orphanGate.countDown();
        }

        private static class ConnState {
            final List<String> dict = new ArrayList<>();
            final AtomicLong seq = new AtomicLong(0);
        }
    }
}
