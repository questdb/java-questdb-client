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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
 * (a) proves the recycle's step 2 ({@code cursorSendLoop.close()}) correctly
 * joins an I/O thread that is itself mid-reconnect (not idle, not yet given
 * up), and that step 7 no longer recovers the connection on the calling
 * thread -- it defers to the I/O loop, so the swap returns promptly and the
 * producer never observes the outage -- exercising
 * {@code CursorWebSocketSendLoop.close()}'s "handles both states" contract
 * under a real outage rather than a synthetic one.
 * <p>
 * (b) proves the swap only ever tears down the producer's OWN cursor
 * engine/I/O loop: an orphan drainer's engine and loop are entirely separate
 * objects owned by {@code BackgroundDrainerPool}, so a recycle firing while a
 * drain is in flight must leave the drain untouched and able to complete
 * afterward.
 */
public class SymbolDictRecycleOutageTest {

    private static final String ORPHAN_MARKER_SYMBOL = "orphan-marker-1";

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * Kills the server out from under an armed, fully-drained sender, waits
     * for the pre-recycle I/O thread to actually enter its own reconnect
     * loop (not just assumed via a fixed sleep) -- so the recycle's step 2
     * ({@code cursorSendLoop.close()}) provably joins a MID-reconnect
     * thread -- then triggers the recycle inline, on the calling thread.
     * {@code reconnect_max_duration_millis} bounds only the sender's initial
     * connect; under the store-and-forward contract step 7 no longer
     * re-enters {@code connectWithRetry} on the producer thread, so the
     * triggering {@code table()} call must return well within that budget
     * even though the endpoint is still down when it fires. The main thread
     * revives a fresh server on the same port after asserting the bound,
     * mirroring {@code ReconnectTest}'s down-then-up realism.
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

                    // Confirm the pre-recycle I/O thread actually entered its
                    // own reconnect loop against the now-refused port before we
                    // trigger the recycle -- so step 2's close() below is
                    // provably joining a MID-reconnect thread, not one that
                    // simply hasn't noticed the drop yet.
                    long attemptDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (ws.getTotalReconnectAttempts() == 0 && System.nanoTime() < attemptDeadline) {
                        Thread.sleep(5);
                    }
                    Assert.assertTrue("pre-recycle I/O thread must have entered reconnect before "
                                    + "the triggering table() call",
                            ws.getTotalReconnectAttempts() > 0);

                    // The recycle must return promptly: reconnect_max_duration_millis
                    // governs only the initial connect, and step 7 defers to the
                    // I/O loop instead of re-entering connectWithRetry on the
                    // producer thread.
                    long startNanos = System.nanoTime();
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals("recycle must complete despite the outage",
                            1, ws.getSymbolDictEpoch());
                    Assert.assertTrue("the swap must not block the producer on the reconnect "
                                    + "budget [elapsedMillis=" + elapsedMillis + ']',
                            elapsedMillis < 3_000);

                    long fsn2 = sender.flushAndGetSequence();
                    OutageRecycleHandler revivedHandler = new OutageRecycleHandler();
                    try (TestWebSocketServer revived =
                                 new TestWebSocketServer(revivedHandler, false, null, port)) {
                        revived.start();
                        Assert.assertTrue(revived.awaitStart(5, TimeUnit.SECONDS));
                        Assert.assertTrue("the outage-window row must land once reconnected",
                                sender.awaitAckedFsn(fsn2, 10_000));
                        Assert.assertTrue(fsn2 > fsn1);
                        Assert.assertEquals(0, revivedHandler.firstFrameDeltaStart);
                        Assert.assertEquals(Collections.singletonList("c"), revivedHandler.dict());
                    }
                }
            }
        });
    }

    /**
     * Default configuration: no {@code reconnect_*} knob and no
     * {@code initial_connect_retry}, so the builder resolves
     * {@code initialConnectMode} to OFF. Under the store-and-forward
     * contract, step 7 no longer opens a connection on the calling thread
     * at all -- it defers to the I/O loop, so the triggering {@code table()}
     * call must return normally even while the endpoint refuses
     * connections.
     * <p>
     * Proves the swap commits exactly one epoch and disarms without the
     * caller ever observing a transport failure, that the flush right after
     * publishes into the fresh epoch's SF slot, and that once the endpoint
     * returns on the same port the I/O loop's own reconnect replays every
     * row sent during the outage with zero loss -- reconnecting only, never
     * re-running a teardown step and never swapping a second time.
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
                    // drained, so the sender-level connected flag is still true
                    // and the next table() call fires the recycle into a wire
                    // that is already down.
                    server.close();

                    // The ring is drained, so the next table() fires the recycle
                    // into a wire that is already down. The swap must complete AND
                    // return normally -- the reconnect is the I/O loop's job, so
                    // no transport failure may reach the producer. "c" registers
                    // into the fresh dictionary after the swap's
                    // resetSymbolDictStateForNewConnection but before the wire is
                    // up, which keeps pinning the drained-guard: a deferred
                    // connect that cleared the batch watermark would ship a row
                    // pointing at an id the server never received.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertEquals("the swap must commit exactly one epoch",
                            1, ws.getSymbolDictEpoch());
                    Assert.assertEquals(1, ws.getSymbolDictResetsPerformed());
                    Assert.assertFalse("a committed swap disarms", ws.isResetArmed());

                    // Producer keeps working against the dead endpoint: the
                    // flush publishes into the fresh epoch's SF slot.
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN",
                            fsn2 > fsn1);

                    // Endpoint back on the SAME port: the I/O loop's own
                    // reconnect must land the buffered rows -- zero loss.
                    OutageRecycleHandler revivedHandler = new OutageRecycleHandler();
                    try (TestWebSocketServer revived =
                                 new TestWebSocketServer(revivedHandler, false, null, port)) {
                        revived.start();
                        Assert.assertTrue(revived.awaitStart(5, TimeUnit.SECONDS));

                        Assert.assertTrue("rows sent during the outage must replay once "
                                        + "the endpoint returns",
                                sender.awaitAckedFsn(fsn2, 10_000));
                        Assert.assertEquals("the recovery reconnects only -- no second swap",
                                1, ws.getSymbolDictEpoch());
                        Assert.assertEquals(1, ws.getSymbolDictResetsPerformed());
                        Assert.assertEquals("the fresh connection's first frame must carry a "
                                        + "fresh (empty) dictionary, not a, b",
                                0, revivedHandler.firstFrameDeltaStart);
                        Assert.assertEquals(Collections.singletonList("c"), revivedHandler.dict());

                        // And the epoch keeps extending normally from there.
                        sender.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                        long fsn3 = sender.flushAndGetSequence();
                        Assert.assertTrue(sender.awaitAckedFsn(fsn3, 5_000));
                        Assert.assertEquals("later batches must extend the same fresh dictionary",
                                Arrays.asList("c", "e"), revivedHandler.dict());
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
     * Reconstructs the single connection it expects (the recycle's
     * post-outage reconnect) and records the delta-start id of its first
     * data frame. Tracks by connection identity like
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
