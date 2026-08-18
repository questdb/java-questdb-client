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
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Memory-mode ({@code sf_dir} omitted) counterpart of {@link SymbolDictRecycleTest}.
 * <p>
 * The recycle swap's eight steps ({@code QwpWebSocketSender.recycleForDictReset()})
 * were written against the store-and-forward slot lifecycle, but the factory's
 * {@code slotPath == null} arm, {@code CursorSendEngine}'s file-less close, and the
 * barrier itself are all mode-agnostic by construction -- nothing in
 * {@code maybeRecycleForDictReset()} or the swap checks whether the sender is
 * SF-backed. This suite pins that: every scenario {@code SymbolDictRecycleTest}
 * proves for a disk-backed sender must hold identically for a {@code Sender.fromConfig}
 * sender built with no {@code sf_dir} at all. No production change is expected to
 * make these pass; a failure here means Task 5's swap accidentally gated something
 * on store-and-forward being present.
 */
public class SymbolDictRecycleMemoryModeTest {

    @Test
    public void testRecycleAtEmptyBacklog() throws Exception {
        assertMemoryLeak(() -> {
            RecycleHandler handler = new RecycleHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                // No sf_dir: memory mode. Everything else mirrors
                // SymbolDictRecycleTest#testRecycleAtEmptyBacklog exactly.
                String cfg = "ws::addr=localhost:" + port + ";symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(1, handler.connectionsAccepted.get());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Ring drained, no row in progress: this table() call must
                    // recycle synchronously, exactly as in SF mode.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals("recycle must open a fresh connection",
                            2, server.handshakeCount());
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get acked",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN "
                                    + "[fsn1=" + fsn1 + ", fsn2=" + fsn2 + ']',
                            fsn2 > fsn1);
                }

                Assert.assertEquals("exactly 2 connections total", 2, handler.connectionsAccepted.get());
                Assert.assertEquals("connection 2's first data frame must carry deltaStart == 0 "
                                + "(a fresh, empty dictionary)",
                        0, handler.conn2FirstFrameDeltaStart);
                Assert.assertEquals("connection 2's dictionary must hold only the post-recycle "
                                + "symbols, not a, b",
                        Arrays.asList("c", "d"), handler.dictFor(2));
            }
        });
    }

    /**
     * Strengthens {@link #testRecycleAtEmptyBacklog} into a content oracle: every
     * row before and after the recycle carries a distinct symbol value, and this
     * asserts the server observed the FULL, exact, gap-free, duplicate-free
     * sequence across both connections -- not just a spot check of the boundary
     * frame. Proves the epoch swap loses (and doesn't duplicate) nothing that was
     * ever acked, in memory mode exactly as {@code testPostRecycleSlotContents}
     * proves the persisted-dictionary shape in SF mode.
     */
    @Test
    public void testRecycleLosesNothingAcked() throws Exception {
        assertMemoryLeak(() -> {
            RecycleHandler handler = new RecycleHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";symbol_dict_reset_threshold=2;";

                List<String> preRecycleSymbols = Arrays.asList("p0", "p1", "p2", "p3");
                List<String> postRecycleSymbols = Arrays.asList("q0", "q1", "q2", "q3");

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    for (String symbol : preRecycleSymbols) {
                        sender.table("t").symbol("s", symbol).longColumn("v", 1L).atNow();
                    }
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: pre-recycle batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("threshold=2 crossed well before the 4th symbol",
                            ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Ring drained: the FIRST post-recycle table() call recycles
                    // synchronously, then the row it is building lands on the
                    // fresh connection alongside the rest of postRecycleSymbols.
                    boolean first = true;
                    for (String symbol : postRecycleSymbols) {
                        sender.table("t").symbol("s", symbol).longColumn("v", 2L).atNow();
                        if (first) {
                            Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                            Assert.assertEquals(1, ws.getSymbolDictEpoch());
                            first = false;
                        }
                    }
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get acked",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);
                }

                Assert.assertEquals("exactly 2 connections total", 2, handler.connectionsAccepted.get());
                Assert.assertEquals("connection 1 must have received exactly the pre-recycle symbols, "
                                + "in order, nothing lost or duplicated",
                        preRecycleSymbols, handler.dictFor(1));
                Assert.assertEquals("connection 2 must have received exactly the post-recycle symbols, "
                                + "in order, nothing lost or duplicated",
                        postRecycleSymbols, handler.dictFor(2));

                List<String> observedAcrossBothEpochs = new ArrayList<>(handler.dictFor(1));
                observedAcrossBothEpochs.addAll(handler.dictFor(2));
                List<String> expectedAcrossBothEpochs = new ArrayList<>(preRecycleSymbols);
                expectedAcrossBothEpochs.addAll(postRecycleSymbols);
                Assert.assertEquals("the epoch boundary must lose (and not duplicate) nothing that "
                                + "was ever acked",
                        expectedAcrossBothEpochs, observedAcrossBothEpochs);
            }
        });
    }

    /**
     * {@code initial_connect_retry=async} defers even the FIRST connect to the
     * I/O thread ({@code ensureConnected()}'s {@code ASYNC} arm leaves
     * {@code client == null} and lets {@code CursorWebSocketSendLoop} dial in the
     * background) -- and {@code recycleForDictReset()}'s step 7 reconnect reuses
     * the exact same {@code initialConnectMode} switch, so the post-recycle
     * connection is ALSO dialled asynchronously rather than inline on the
     * producer thread that called {@code table()}. Only the producer-side halves
     * of the swap (steps 4-6: FSN epoch roll, dictionary swap, engine rebuild)
     * are guaranteed synchronous by the time {@code table()} returns; the fresh
     * handshake itself must be awaited separately here, unlike the SYNC-mode
     * tests above where {@code server.handshakeCount()} is already correct the
     * instant {@code table()} returns.
     */
    @Test
    public void testRecycleUnderAsyncInitialConnect() throws Exception {
        assertMemoryLeak(() -> {
            RecycleHandler handler = new RecycleHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port
                        + ";initial_connect_retry=async;symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    // Let the I/O thread complete the deferred initial connect
                    // before driving any traffic through it. Spins on the
                    // CLIENT-side sticky flag, not server.handshakeCount(): the
                    // server counts a handshake the moment IT finishes writing
                    // the upgrade response, which can observably precede the
                    // client processing that response and flipping
                    // wasEverConnected() -- polling the server-side counter as
                    // a proxy for client-side connectedness raced exactly that
                    // window when this test was first written.
                    awaitWasEverConnected(ws);

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(1, handler.connectionsAccepted.get());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // Ring drained: this table() call recycles synchronously on
                    // the producer thread for steps 1-6, but step 7's reconnect
                    // just re-arms the ASYNC path -- the actual handshake still
                    // happens on the I/O thread, so it must be awaited.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm immediately (producer-side state, "
                                    + "not gated on the wire)",
                            ws.isResetArmed());
                    Assert.assertEquals("recycle must advance the epoch immediately (producer-side "
                                    + "state, not gated on the wire)",
                            1, ws.getSymbolDictEpoch());

                    // Don't gate on a connection counter here -- rows queue on
                    // the (memory-mode) cursor ring regardless of wire state in
                    // ASYNC mode, and awaitAckedFsn below is itself the
                    // deterministic wait for the fresh handshake to land.
                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get acked once the "
                                    + "async I/O thread completes the fresh handshake",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);
                    // The ack above only arrives over a completed second
                    // handshake, so this is a safe post-condition, not a race.
                    Assert.assertEquals(2, handler.connectionsAccepted.get());
                }

                Assert.assertEquals("exactly 2 connections total", 2, handler.connectionsAccepted.get());
                Assert.assertEquals("connection 2's first data frame must carry deltaStart == 0 "
                                + "(a fresh, empty dictionary)",
                        0, handler.conn2FirstFrameDeltaStart);
                Assert.assertEquals("connection 2's dictionary must hold only the post-recycle "
                                + "symbols, not a, b",
                        Arrays.asList("c", "d"), handler.dictFor(2));
            }
        });
    }

    /**
     * Spins until the I/O thread has completed the deferred ASYNC initial
     * connect. Needed only for the async test above: SYNC/OFF-mode recycle
     * blocks {@code table()} until the fresh handshake completes, so those
     * tests observe connectedness synchronously, but ASYNC mode hands the
     * connect off to the I/O thread and returns control to the caller before
     * it necessarily lands.
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

    /**
     * Reconstructs each connection's per-connection delta dictionary (mirrors
     * {@code SymbolDictRecycleTest.RecycleHandler}) and records the delta-start
     * id of connection 2's first non-empty data frame.
     */
    private static class RecycleHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        volatile int conn2FirstFrameDeltaStart = -1;
        private boolean conn2SeenFirstDataFrame;
        private TestWebSocketServer.ClientHandler currentClient;
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

        synchronized List<String> dictFor(int connNumber) {
            return connNumber <= dictsByConn.size()
                    ? new ArrayList<>(dictsByConn.get(connNumber - 1))
                    : new ArrayList<>();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            boolean newConnection = currentClient != client;
            if (newConnection) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                dictsByConn.add(new ArrayList<>());
                nextSeq.set(0);
                conn2SeenFirstDataFrame = false;
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            if (connNumber == 2 && !conn2SeenFirstDataFrame && QwpWireTestUtils.tableCount(data) > 0) {
                conn2SeenFirstDataFrame = true;
                if (QwpWireTestUtils.hasDelta(data)) {
                    int[] pos = {HEADER_SIZE};
                    conn2FirstFrameDeltaStart = QwpWireTestUtils.readVarint(data, pos);
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
