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
import io.questdb.client.std.Compat;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Pins that the symbol-dictionary recycle's fresh connection ({@code
 * QwpWebSocketSender.recycleForDictReset()}'s step 7 reconnect) never pays
 * for a delta-dictionary catch-up frame, and that the state-reset it relies
 * on to get there is not accidentally general-purpose.
 * <p>
 * The catch-up mechanism itself is {@link DeltaDictCatchUpTest}'s territory
 * ({@code CursorWebSocketSendLoop.setWireBaselineWithCatchUp}'s gate:
 * {@code client != null && sentDictCount > 0 && hasReplayDictionaryDependency}).
 * This suite does not re-implement or re-verify that mechanism -- it only
 * observes the ONE fact specific to the recycle: {@code sentDictCount} on the
 * fresh loop starts at 0 because {@code recycleForDictReset()}'s step 6
 * rebuilds the engine on a freshly-emptied slot, whose {@code
 * PersistedSymbolDict.recoveredSize()} is 0 -- so the loop constructor's
 * {@code pd.recoveredSize() > 0} seed never fires, and the gate stays false
 * for the whole first post-recycle connection. A PLAIN (non-recycle)
 * reconnect on that same connection, by contrast, reuses the SAME loop
 * instance whose mirror has since grown from the frames it sent -- so it DOES
 * trip the gate. Observing both back to back in one test is the only way to
 * prove the zero count above is the recycle's fresh-mirror property and not a
 * blind spot in how this suite's handler counts frames.
 * <p>
 * No production change is expected to make these pass. A failure here means
 * either the fresh-mirror seeding regressed (a post-recycle connection
 * started paying for catch-up again) or the recycle's {@code
 * sentMaxSymbolId} reset ({@code recycleForDictReset()}'s step 5) leaked
 * onto the ordinary reconnect path, which today never touches that
 * baseline.
 */
public class SymbolDictRecycleCatchUpSkipTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * The core scenario, SF-disk mode. {@code symbol_dict_reset_threshold=3}
     * is deliberately higher than the 2 symbols this test registers in the
     * new epoch before forcing the unplanned drop: epoch 0 crosses the
     * threshold on its own (a, b, x -- 3 distinct symbols), so the recycle
     * fires exactly once, synchronously, on the "c" call. Epoch 1 then
     * registers only c, d (2 symbols, below the threshold) before the drop,
     * and only e (a 3rd) after it -- staying unarmed for the whole test so no
     * SECOND recycle can sneak in and confound the "does a plain reconnect
     * still catch up / preserve the baseline" assertions below. (A lower
     * threshold that let epoch 1 re-arm on c, d would turn the later {@code
     * table("e")} call into an unwanted second recycle, landing e on a 4th
     * connection instead of a plain reconnect's 3rd -- exactly the
     * confounder this threshold choice avoids.)
     */
    @Test
    public void testRecycleSkipsCatchUpThenUnplannedReconnectBoundsCatchUpToNewEpoch() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("catchup-skip").toString();
            SkipCatchUpHandler handler = new SkipCatchUpHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=3;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    // Epoch 0 (connection 1): 3 distinct symbols cross threshold=3 and arm.
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "x").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: the arming batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=3", ws.isResetArmed());
                    Assert.assertEquals(1, handler.connectionsAccepted.get());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Ring drained: this table() call recycles synchronously onto a fresh
                    // connection (2), a fresh (empty) engine/dictionary/epoch, and "c" is
                    // then the new epoch's own first symbol.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    Assert.assertEquals("recycle must open a fresh connection",
                            2, server.handshakeCount());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("epoch-1 batch must be acked before the unplanned drop",
                            sender.awaitAckedFsn(fsn2, 5_000));

                    // --- Pin 1 + 2: zero catch-up frames, dictionary tiles from 0. ---
                    // Connection 2 is the FIRST connection after the recycle: its loop's
                    // sentDictCount mirror was seeded from the fresh engine's
                    // PersistedSymbolDict.recoveredSize() == 0 (nothing survived the
                    // recycle's slot wipe), so setWireBaselineWithCatchUp's
                    // `sentDictCount > 0` gate stays false for this whole connection.
                    Assert.assertEquals("first post-recycle connection must send zero catch-up "
                                    + "(zero-table) frames",
                            0, handler.zeroTableFramesFor(2));
                    Assert.assertEquals("connection 2's dictionary must tile ids from 0 with "
                                    + "exactly the new epoch's symbols, none of epoch 0's a, b, x",
                            Arrays.asList("c", "d"), handler.dictFor(2));

                    // --- Positive control + pin 3: an UNPLANNED reconnect (server-side
                    // drop, no recycle involved) on this SAME connection DOES produce a
                    // catch-up frame, and that catch-up is bounded to exactly what this
                    // epoch has sent so far (c, d) -- proving both that the zero count
                    // above is a real property (not a handler blind spot) and that the
                    // recycle's fresh mirror does not somehow retain epoch 0's symbols.
                    handler.dropConnection(2);
                    waitFor(() -> handler.connectionsAccepted.get() >= 3, 5_000);
                    waitFor(() -> handler.dictFor(3).size() >= 2, 5_000);

                    Assert.assertTrue("an unplanned reconnect mid-epoch must still produce a "
                                    + "catch-up frame",
                            handler.zeroTableFramesFor(3) >= 1);
                    Assert.assertEquals("the catch-up must bound itself to exactly this epoch's "
                                    + "own symbols (c, d), never replaying epoch 0's a, b, x",
                            Arrays.asList("c", "d"), handler.dictFor(3));

                    // --- Pin 4: the plain reconnect preserved sentMaxSymbolId. A NEW
                    // symbol registered after it must ship with a delta start ABOVE 0.
                    // Nothing on this I/O-thread reconnect path touches sentMaxSymbolId
                    // (resetSymbolDictStateForNewConnection runs only on the foreground
                    // initial-connect path, guarded by the connected flag, and never
                    // fires here), so the producer's baseline (c, d already at ids 0, 1)
                    // survives the wire boundary and e resumes at id 2. Only
                    // recycleForDictReset()'s step 5 ever zeroes that baseline; a
                    // regression that folded the reset into a path this reconnect DOES
                    // run would re-ship the whole dictionary from deltaStart 0.
                    sender.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                    long fsn3 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-reconnect row must still get acked",
                            sender.awaitAckedFsn(fsn3, 5_000));
                    Assert.assertTrue("connection 3's post-reconnect data frame carrying the new "
                                    + "symbol e must ship a delta start ABOVE the surviving "
                                    + "baseline (>= 1), not 0",
                            handler.sawDeltaAboveBaselineOn(3));
                }

                Assert.assertEquals("exactly 3 connections total (epoch 0, epoch 1's first "
                                + "connection, epoch 1's unplanned reconnect)",
                        3, handler.connectionsAccepted.get());
            }
        });
    }

    /**
     * Pin 5: the recycle's step 7 reconnect funnels through {@code
     * ensureConnected()}'s {@code ASYNC} arm exactly like any other initial
     * connect, which ends up at the same {@code swapClient} catch-up gate as
     * the SYNC-mode scenario above. Mirrors {@code
     * SymbolDictRecycleMemoryModeTest#testRecycleUnderAsyncInitialConnect},
     * but in SF-disk mode (this suite's mode throughout) rather than memory
     * mode, and asserts the zero-catch-up property instead of just the
     * delta-start/dictionary-content pair that test already covers.
     */
    @Test
    public void testRecycleUnderAsyncInitialConnectSendsZeroCatchUpFrames() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("catchup-skip-async").toString();
            SkipCatchUpHandler handler = new SkipCatchUpHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";initial_connect_retry=async;symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    // Let the I/O thread complete the deferred initial connect before
                    // driving any traffic through it (see
                    // SymbolDictRecycleMemoryModeTest.awaitWasEverConnected).
                    awaitWasEverConnected(ws);

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(1, handler.connectionsAccepted.get());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Recycles synchronously on the producer thread for steps 1-6; step
                    // 7's reconnect just re-arms the ASYNC path -- the actual handshake
                    // happens on the I/O thread and must be awaited via the ack below.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm immediately (producer-side state)",
                            ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get acked once the async "
                                    + "I/O thread completes the fresh handshake",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertEquals(2, handler.connectionsAccepted.get());
                }

                Assert.assertEquals("exactly 2 connections total", 2, handler.connectionsAccepted.get());
                Assert.assertEquals("the ASYNC path funnels through the same swapClient catch-up "
                                + "gate -- the first post-recycle connection must still send zero "
                                + "catch-up frames",
                        0, handler.zeroTableFramesFor(2));
                Assert.assertEquals("connection 2's dictionary must hold only the post-recycle "
                                + "symbols, not a, b",
                        Arrays.asList("c", "d"), handler.dictFor(2));
            }
        });
    }

    /**
     * Spins until the I/O thread has completed the deferred ASYNC initial
     * connect (mirrors {@code SymbolDictRecycleMemoryModeTest}'s helper of
     * the same name).
     */
    private static void awaitWasEverConnected(QwpWebSocketSender ws) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ws.wasEverConnected()) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("I/O thread did not complete the async initial "
                        + "connect within 5s");
            }
            Compat.onSpinWait();
        }
    }

    private static void waitFor(BoolCondition cond, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted");
            }
        }
        Assert.fail("waitFor timed out");
    }

    @FunctionalInterface
    private interface BoolCondition {
        boolean test();
    }

    /**
     * Reconstructs each connection's per-connection delta dictionary (mirrors
     * {@code DeltaDictCatchUpTest.CatchUpHandler} / {@code
     * SymbolDictRecycleTest.RecycleHandler}), counts zero-table (catch-up)
     * frames per connection, tracks whether any data frame on a connection
     * carried a delta start above 0, and -- unlike the sibling handlers --
     * exposes {@link #dropConnection(int)} so the TEST THREAD can force an
     * unplanned drop asynchronously, independent of the ack-driven close a
     * handler normally does from inside {@code onBinaryMessage}.
     */
    private static class SkipCatchUpHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private final List<AtomicInteger> zeroTableFramesByConn = new CopyOnWriteArrayList<>();
        private final List<AtomicBoolean> deltaAboveBaselineByConn = new CopyOnWriteArrayList<>();
        private final List<TestWebSocketServer.ClientHandler> clientsByConn = new CopyOnWriteArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

        synchronized List<String> dictFor(int connNumber) {
            return connNumber <= dictsByConn.size()
                    // Copy under the lock: the caller iterates it unlocked while the
                    // server thread may still be appending to the live inner list.
                    ? new ArrayList<>(dictsByConn.get(connNumber - 1))
                    : new ArrayList<>();
        }

        /** Closes connection N's socket from the caller's thread, forcing an unplanned reconnect. */
        void dropConnection(int connNumber) {
            TestWebSocketServer.ClientHandler client;
            synchronized (this) {
                client = connNumber <= clientsByConn.size() ? clientsByConn.get(connNumber - 1) : null;
            }
            Assert.assertNotNull("no such connection to drop: " + connNumber, client);
            client.close();
        }

        boolean sawDeltaAboveBaselineOn(int connNumber) {
            return connNumber <= deltaAboveBaselineByConn.size()
                    && deltaAboveBaselineByConn.get(connNumber - 1).get();
        }

        int zeroTableFramesFor(int connNumber) {
            return connNumber <= zeroTableFramesByConn.size()
                    ? zeroTableFramesByConn.get(connNumber - 1).get()
                    : 0;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            boolean newConnection = currentClient != client;
            if (newConnection) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                dictsByConn.add(new ArrayList<>()); // fresh dictionary per connection
                zeroTableFramesByConn.add(new AtomicInteger());
                deltaAboveBaselineByConn.add(new AtomicBoolean());
                clientsByConn.add(client);
                nextSeq.set(0);
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            if (QwpWireTestUtils.tableCount(data) == 0) {
                zeroTableFramesByConn.get(connNumber - 1).incrementAndGet();
            } else if (QwpWireTestUtils.hasDelta(data)) {
                // A DATA frame (tableCount > 0) carrying a delta section. A start id
                // >= 1 means the producer resumed the delta ABOVE the surviving
                // baseline; a reset baseline would instead re-ship from 0.
                int[] pos = {HEADER_SIZE};
                if (QwpWireTestUtils.readVarint(data, pos) >= 1) {
                    deltaAboveBaselineByConn.get(connNumber - 1).set(true);
                }
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
