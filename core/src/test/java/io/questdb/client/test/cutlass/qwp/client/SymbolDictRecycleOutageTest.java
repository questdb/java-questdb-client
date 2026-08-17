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
import java.util.concurrent.atomic.AtomicReference;

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
 * up), and that step 7's fresh {@code ensureConnected()} recovers once the
 * endpoint accepts connections again -- exercising
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
     * loop (not just assumed via a fixed sleep), then triggers the recycle
     * from a background thread -- because step 7's fresh
     * {@code ensureConnected()} blocks the calling {@code table()} call (sync
     * initial-connect mode) until the endpoint accepts again or
     * {@code reconnect_max_duration_millis} elapses. The main thread revives
     * a fresh server on the same port shortly after, well inside that
     * budget, mirroring {@code ReconnectTest}'s down-then-up realism.
     */
    @Test
    public void testRecycleDuringOutageReconnectsAfresh() throws Exception {
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
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());

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

                    // Trigger the recycle off-thread: step 7's fresh
                    // ensureConnected() blocks the caller (sync initial-connect
                    // mode) until the endpoint accepts again.
                    AtomicReference<Throwable> triggerFailure = new AtomicReference<>();
                    Thread trigger = new Thread(() -> {
                        try {
                            sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                        } catch (Throwable t) {
                            triggerFailure.set(t);
                        }
                    }, "recycle-trigger");
                    trigger.start();

                    // Resolve the outage shortly after -- well inside
                    // reconnect_max_duration_millis=6000 -- on the SAME port.
                    Thread.sleep(150);
                    OutageRecycleHandler revivedHandler = new OutageRecycleHandler();
                    try (TestWebSocketServer revived =
                                 new TestWebSocketServer(revivedHandler, false, null, port)) {
                        revived.start();
                        Assert.assertTrue(revived.awaitStart(5, TimeUnit.SECONDS));

                        trigger.join(10_000);
                        Assert.assertFalse("recycle-trigger thread must have finished once the "
                                        + "endpoint accepts again", trigger.isAlive());
                        Assert.assertNull("triggering table() must not throw once the outage "
                                        + "resolves within budget: " + triggerFailure.get(),
                                triggerFailure.get());

                        Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                        Assert.assertEquals("recycle must complete despite the outage",
                                1, ws.getSymbolDictEpochForTest());
                        Assert.assertTrue("revived server must observe a fresh handshake",
                                revived.handshakeCount() >= 1);

                        // The "c" row was built (atNow()) inside the triggering
                        // call but not yet flushed -- flush now and prove it
                        // lands on the fresh connection.
                        long fsn2 = sender.flushAndGetSequence();
                        Assert.assertTrue("post-recycle row must land once reconnected",
                                sender.awaitAckedFsn(fsn2, 5_000));
                        Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN",
                                fsn2 > fsn1);

                        Assert.assertEquals("the fresh connection's first frame must carry a "
                                        + "fresh (empty) dictionary, not a, b",
                                0, revivedHandler.firstFrameDeltaStart);
                        Assert.assertEquals("post-recycle dictionary must hold only the new "
                                        + "epoch's symbol, nothing lost or duplicated from "
                                        + "before the outage",
                                Collections.singletonList("c"), revivedHandler.dict());
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
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());

                    // Recycle fires synchronously here, tearing down + rebuilding
                    // ONLY the foreground's own cursor engine/I/O loop.
                    sender.table("t").symbol("s", "post-c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());

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
