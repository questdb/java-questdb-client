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
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies the delta symbol-dictionary catch-up on reconnect (memory-mode).
 * <p>
 * When a memory-mode sender reconnects, the server it lands on has an empty
 * dictionary (the server discards it on every disconnect). Because the producer
 * ships monotonic deltas -- each symbol id once -- a naive replay would leave the
 * fresh server with a dictionary gap. The I/O thread prevents this by sending a
 * full-dictionary catch-up frame before any post-reconnect traffic. This test
 * reconstructs the server's per-connection dictionary from the captured wire
 * bytes and asserts it stays complete and gap-free across the reconnect.
 */
public class DeltaDictCatchUpTest {

    @Test
    public void testReconnectCatchUpRebuildsDictionary() throws Exception {
        // Connection 1: send "alpha" (id 0), ACK it, then drop the socket so the
        // sender reconnects. Connection 2 (fresh, empty dict): send "beta" (id 1).
        // Without catch-up, connection 2's first data frame would carry
        // deltaStart=1 and the fresh server would never learn id 0.
        assertMemoryLeak(() -> {
            CatchUpHandler handler = new CatchUpHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    sender.table("t").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.dictFor(1).size() >= 1, 5_000);

                    // Wait until the server has actually closed connection 1 before
                    // sending batch 2, so batch 2 cannot race into connection 1 and
                    // must drive the reconnect + catch-up.
                    waitFor(() -> handler.conn1Closed, 5_000);

                    sender.table("t").symbol("s", "beta").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.connectionsAccepted.get() >= 2
                            && handler.dictFor(2).size() >= 2, 5_000);
                }

                // The fresh (2nd) connection's dictionary, rebuilt purely from the
                // frames it received, must hold both symbols contiguously with no
                // null gap -- exactly what the catch-up frame guarantees.
                List<String> conn2 = handler.dictFor(2);
                Assert.assertTrue("2nd connection saw a catch-up frame with 0 tables",
                        handler.sawZeroTableFrameOnConn2);
                Assert.assertTrue("the catch-up frame carries no rows, so it must defer its "
                                + "(empty) commit -- FLAG_DEFER_COMMIT set",
                        handler.catchUpDeferredOnConn2);
                Assert.assertEquals("2nd connection dictionary size", 2, conn2.size());
                Assert.assertEquals("alpha", conn2.get(0));
                Assert.assertEquals("beta", conn2.get(1));
                assertAckSequencesStartAtZero(handler.ackSequenceStarts());
            }
        });
    }

    @Test
    public void testReconnectPreservesMonotonicDeltaBaseline() throws Exception {
        // Regression: the producer's sent-symbol watermark (sentMaxSymbolId) must
        // SURVIVE a reconnect. resetSymbolDictStateForNewConnection deliberately
        // leaves it untouched -- the I/O thread re-registers the whole dictionary via
        // a catch-up frame before replay, so the producer keeps shipping deltas ABOVE
        // the baseline across the wire boundary. If a regression reset it on
        // reconnect, the first post-reconnect data frame would re-ship the whole
        // dictionary inline (deltaStart=0), pure wasted bandwidth. The sibling
        // testReconnectCatchUpRebuildsDictionary asserts only that the final
        // dictionary is complete -- which a reset-then-redefine ALSO satisfies (the
        // server tolerates the redefinition) -- so it does NOT catch that regression.
        // This pins the baseline survival directly: connection 2's data frame must
        // carry a delta starting at id 1 (above alpha), not 0.
        assertMemoryLeak(() -> {
            CatchUpHandler handler = new CatchUpHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    // Connection 1 registers alpha (id 0); the server ACKs and drops it.
                    sender.table("t").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.dictFor(1).size() >= 1, 5_000);
                    waitFor(() -> handler.conn1Closed, 5_000);

                    // Connection 2 (fresh) registers beta (id 1). With the baseline
                    // preserved, beta ships as a delta ABOVE id 0 (deltaStart=1).
                    sender.table("t").symbol("s", "beta").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.connectionsAccepted.get() >= 2
                            && handler.dictFor(2).size() >= 2, 5_000);
                }

                Assert.assertTrue("connection 2 must re-register the dictionary via a catch-up first",
                        handler.sawZeroTableFrameOnConn2);
                Assert.assertTrue("post-reconnect data frame must ship a delta ABOVE the surviving "
                                + "baseline (deltaStart >= 1); a reset baseline re-ships the whole "
                                + "dictionary from deltaStart 0",
                        handler.conn2SawDeltaAboveBaseline);
                assertAckSequencesStartAtZero(handler.ackSequenceStarts());
            }
        });
    }

    @Test
    public void testFixedCapNearBoundarySymbolCatchesUpWithoutTerminal() throws Exception {
        // Regression (homogeneous single cap): a symbol whose length sits just below
        // the advertised cap is ACCEPTED into a data frame (messageSize <= cap) and
        // enters the sent-dictionary mirror. On reconnect the catch-up must
        // re-register it under the SAME cap -- the bare catch-up frame (header + two
        // varints + the entry) is smaller than the data frame that already shipped
        // it (which also carried the table schema + a row), so it fits.
        //
        // Pre-fix, the single-entry terminal used the conservative PACKING budget
        // (cap - HEADER_SIZE - 16), which is stricter than the producer's publish
        // gate (messageSize <= cap) by more than the minimal data-frame overhead. So
        // a symbol accepted onto the wire under cap C could exceed that budget and
        // trip a spurious "during catch-up" terminal, permanently hard-failing a
        // running producer on its first transient reconnect. Concretely at cap=200:
        // table("t").symbol("s", <173 chars>).atNow() encodes to 198 bytes (<=200,
        // accepted), its dict entry is 2+173=175 bytes (> old budget 172 -> old
        // terminal), while the real solo catch-up frame is 12+1+1+175=189 (<=200 ->
        // fits). Unlike testCatchUpCapGapRetriesUntilBudgetThenLatches (a genuinely
        // oversized entry on a shrunk cap, which MUST still terminate), this entry
        // is legally shippable and must NOT terminate.
        final int cap = 200;
        final String nearCapSymbol = TestUtils.repeat("x", 173);
        assertMemoryLeak(() -> {
            CatchUpHandler handler = new CatchUpHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(cap); // same cap on every handshake (homogeneous)
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    // Symbol-only row so the near-cap symbol drives the frame size.
                    sender.table("t").symbol("s", nearCapSymbol).atNow();
                    sender.flush();
                    waitFor(() -> handler.dictFor(1).size() >= 1, 5_000);
                    waitFor(() -> handler.conn1Closed, 5_000);

                    // The reconnect runs the catch-up under the SAME cap. Pre-fix
                    // this latched a terminal (surfacing on this flush); post-fix the
                    // catch-up ships the near-cap symbol and the flush goes through.
                    sender.table("t").symbol("s", "beta").atNow();
                    sender.flush();
                    waitFor(() -> handler.connectionsAccepted.get() >= 2
                            && handler.dictFor(2).size() >= 2, 5_000);
                }

                // Connection 2's dictionary, rebuilt purely from the frames it
                // received, must hold the near-cap symbol (re-registered by the
                // catch-up) and beta, gap-free -- proving the catch-up SHIPPED rather
                // than terminated.
                List<String> conn2 = handler.dictFor(2);
                Assert.assertTrue("2nd connection saw a catch-up frame with 0 tables",
                        handler.sawZeroTableFrameOnConn2);
                Assert.assertEquals("2nd connection dictionary size", 2, conn2.size());
                Assert.assertEquals(nearCapSymbol, conn2.get(0));
                Assert.assertEquals("beta", conn2.get(1));
                assertAckSequencesStartAtZero(handler.ackSequenceStarts());
            }
        });
    }

    @Test
    public void testForegroundCatchUpCapGapRetriesPastOrphanBudgetAndRecovers() throws Exception {
        // A dictionary entry that exceeds the reconnect server's per-chunk budget
        // (cap - HEADER_SIZE - 16) cannot be shipped as a catch-up chunk. A live
        // foreground sender must keep retrying without surfacing that cluster state to
        // the producer, even after the orphan drainer's 16-attempt quarantine budget.
        //
        // Connection 1 advertises no cap, so the ~202-byte symbol registers and
        // enters the sent-dictionary mirror. The handler then shrinks the
        // advertised cap to 160 (catch-up budget 132) and drops the socket, so the
        // reconnect's catch-up cannot re-ship the symbol. (One fixed cap can't do
        // this: the client refuses to SEND a single-table frame over the cap, and
        // that data frame is always larger than the bare catch-up entry.)
        assertMemoryLeak(() -> {
            CapShrinkHandler handler = new CapShrinkHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                handler.setServer(server);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String bigSymbol = TestUtils.repeat("x", 200); // ~202-byte dict entry
                // Zeroing the dwell makes the old faulty foreground policy terminal at
                // its 16th cap gap. Reaching 20 handshakes therefore proves the live loop
                // is no longer governed by the orphan budget.
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50"
                        + ";catch_up_cap_gap_min_escalation_window_millis=0;")) {
                    sender.table("t").symbol("s", bigSymbol).longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> server.handshakeCount() >= 20, 10_000);

                    // Producer calls remain usable while the wire is stuck in the cap
                    // gap. Then restore the larger-cap node and prove the buffered row is
                    // actually acknowledged, not merely accepted into the local SF ring.
                    sender.table("t").symbol("s", "y").longColumn("v", 2L).atNow();
                    sender.flush();
                    server.setAdvertisedMaxBatchSize(0);
                    sender.table("t").symbol("s", "z").longColumn("v", 3L).atNow();
                    long targetFsn = sender.flushAndGetSequence();
                    Assert.assertTrue("foreground sender must recover and drain after the cap is restored",
                            sender.awaitAckedFsn(targetFsn, 5_000));
                }
                assertAckSequencesStartAtZero(handler.ackSequenceStarts());
            }
        });
    }

    @Test
    public void testReconnectCatchUpSplitsLargeDictionaryAcrossFrames() throws Exception {
        // Connection 1 registers 40 ten-character symbols (~440 dictionary bytes),
        // then drops once the server has learned them all. On reconnect the fresh
        // server has an empty dictionary, so the I/O thread must replay all 40 as a
        // catch-up -- but the server advertises a 160-byte batch cap, so the whole
        // dictionary cannot fit in a single frame. The catch-up therefore splits
        // into several contiguous zero-table frames that the fresh server stitches
        // back into a complete, gap-free dictionary.
        final int symbolCount = 40;
        assertMemoryLeak(() -> {
            SplitCatchUpHandler handler = new SplitCatchUpHandler(symbolCount);
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(160); // small cap forces the catch-up to split
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    // One row per flush so each frame stays under the 160-byte cap; the
                    // sent dictionary still accumulates all 40 symbols on connection 1.
                    for (int i = 0; i < symbolCount; i++) {
                        sender.table("t").symbol("s", symbolName(i)).longColumn("v", i).atNow();
                        sender.flush();
                    }
                    // Wait until connection 1 has learned every symbol, so the sender's
                    // sent-dictionary mirror (the catch-up source) holds all of them.
                    waitFor(() -> handler.dictFor(1).size() >= symbolCount, 10_000);

                    // Wait until the server has actually closed connection 1 before
                    // sending batch 2, so batch 2 drives the reconnect + split catch-up.
                    waitFor(() -> handler.conn1Closed, 5_000);

                    sender.table("t").symbol("s", symbolName(symbolCount)).longColumn("v", symbolCount).atNow();
                    sender.flush();
                    waitFor(() -> handler.connectionsAccepted.get() >= 2
                            && handler.dictFor(2).size() >= symbolCount + 1, 10_000);
                }

                // Connection 2's dictionary, rebuilt purely from the frames it received,
                // must hold every symbol contiguously with no null gap -- the split
                // catch-up frames reassemble exactly.
                List<String> conn2 = handler.dictFor(2);
                Assert.assertEquals("2nd connection dictionary size", symbolCount + 1, conn2.size());
                for (int i = 0; i <= symbolCount; i++) {
                    Assert.assertEquals("symbol at id " + i, symbolName(i), conn2.get(i));
                }
                // The catch-up had to span more than one zero-table frame to stay under
                // the advertised cap -- that split is the behaviour under test.
                Assert.assertTrue("catch-up split into multiple frames (saw "
                                + handler.zeroTableFramesOnConn2 + ")",
                        handler.zeroTableFramesOnConn2 >= 2);
                assertAckSequencesStartAtZero(handler.ackSequenceStarts());
            }
        });
    }

    private static void assertAckSequencesStartAtZero(List<Long> ackSequenceStarts) {
        Assert.assertFalse("the fixture must ACK at least one connection", ackSequenceStarts.isEmpty());
        for (int i = 0; i < ackSequenceStarts.size(); i++) {
            Assert.assertEquals("connection " + (i + 1) + " must begin ACK sequencing at zero",
                    0L, ackSequenceStarts.get(i).longValue());
        }
    }

    private static String symbolName(int i) {
        // 10-char symbols so 40 of them clearly exceed the advertised 160-byte cap.
        return "symbol" + (1000 + i);
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
     * Mirrors the server's per-connection delta-dictionary accumulation: the
     * dictionary resets on every new connection, and each frame's delta section
     * ({@code setQuick(deltaStart + i)}, null-padding to reach deltaStart) extends
     * or overwrites it. A missing catch-up would show up here as a null gap.
     */
    private static class CatchUpHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        // Set once the server has closed connection 1. A test waits on this
        // (rather than a fixed sleep) before sending batch 2, so batch 2 cannot
        // race into connection 1's pre-close window and must land on the reconnect.
        volatile boolean conn1Closed;
        // Set from the flags byte of the zero-table catch-up frame on connection 2:
        // the catch-up carries no rows and must defer its (empty) commit.
        volatile boolean catchUpDeferredOnConn2;
        // Set when connection 2 receives a DATA frame (tableCount > 0) whose delta
        // starts ABOVE id 0 (deltaStart >= 1). This can only happen if the producer's
        // monotonic baseline SURVIVED the reconnect: a reset would re-ship the whole
        // dictionary from deltaStart 0. Robust to replay -- a replayed pre-reconnect
        // frame carries its original deltaStart 0, so only a genuinely-above-baseline
        // post-reconnect frame trips it.
        volatile boolean conn2SawDeltaAboveBaseline;
        volatile boolean sawZeroTableFrameOnConn2;
        private final List<Long> ackSequenceStarts = new CopyOnWriteArrayList<>();
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private boolean hasUnresolvedSequence;
        private final AtomicLong nextSeq = new AtomicLong(0);

        List<Long> ackSequenceStarts() {
            return new ArrayList<>(ackSequenceStarts);
        }

        synchronized List<String> dictFor(int connNumber) {
            return connNumber <= dictsByConn.size()
                    // Copy under the lock: the caller iterates it unlocked while the
                    // server thread may still be appending to the live inner list.
                    ? new ArrayList<>(dictsByConn.get(connNumber - 1))
                    : new ArrayList<>();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            boolean newConnection = currentClient != client;
            if (newConnection) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                dictsByConn.add(new ArrayList<>()); // fresh dictionary per connection
                nextSeq.set(0);
                hasUnresolvedSequence = false;
            }
            if (hasUnresolvedSequence) {
                // A real server marks the connection's pipeline broken on the first
                // rejected frame and answers every later frame with silence -- no ACK,
                // no NACK -- until the connection resets (QwpIngressUpgradeProcessor's
                // hasUnresolvedSequence gate). Responding here would let the client
                // believe frames were processed that a real server dropped, and a
                // cumulative ACK could then leapfrog the rejected sequence.
                return;
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            try {
                QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            } catch (QwpWireTestUtils.DictionaryGapException gap) {
                // A real server answers a gap with STATUS_DICTIONARY_GAP and does NOT
                // apply the frame. ACKing here is what let a client sequence a real
                // server rejects pass green.
                hasUnresolvedSequence = true;
                try {
                    long nackSequence = nextSeq.getAndIncrement();
                    if (newConnection) {
                        ackSequenceStarts.add(nackSequence);
                    }
                    client.sendBinary(QwpWireTestUtils.buildNack(nackSequence, WebSocketResponse.STATUS_DICTIONARY_GAP));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return;
            }
            if (connNumber == 2) {
                if (QwpWireTestUtils.tableCount(data) == 0) {
                    sawZeroTableFrameOnConn2 = true;
                    // FLAG_DEFER_COMMIT is bit 0x01 of the flags byte (offset 5).
                    catchUpDeferredOnConn2 = (data[5] & 0x01) != 0;
                } else if (data.length >= 12 && (data[5] & 0x08) != 0) {
                    // A post-reconnect DATA frame carrying a delta section
                    // (FLAG_DELTA_SYMBOL_DICT = 0x08). A deltaStart >= 1 means the
                    // producer resumed the delta ABOVE the surviving baseline; a reset
                    // baseline would re-ship from deltaStart 0. Checking any frame (not
                    // just the first) keeps this robust to a replayed pre-reconnect
                    // frame arriving ahead of the new one -- that replay carries its
                    // original deltaStart 0 and does not trip the flag.
                    int[] pos = {12};
                    if (QwpWireTestUtils.readVarint(data, pos) >= 1) {
                        conn2SawDeltaAboveBaseline = true;
                    }
                }
            }
            try {
                long ackSequence = nextSeq.getAndIncrement();
                if (newConnection) {
                    ackSequenceStarts.add(ackSequence);
                }
                client.sendBinary(QwpWireTestUtils.buildAck(ackSequence));
                // Drop the first connection right after ACKing its only frame,
                // forcing the sender to reconnect onto a fresh dictionary.
                if (connNumber == 1) {
                    Thread.sleep(50);
                    client.close();
                    conn1Closed = true;
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * ACKs connection 1's frames with no advertised cap (so an oversized symbol
     * registers), then -- once connection 1 has sent something -- shrinks the
     * advertised batch cap and drops the socket. The reconnect (connection 2)
     * therefore advertises a cap whose catch-up budget is too small for the
     * symbol, exercising the foreground retry path in sendDictCatchUp.
     */
    private static class CapShrinkHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        private final List<Long> ackSequenceStarts = new CopyOnWriteArrayList<>();
        private final List<String> dict = new ArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler currentClient;
        private boolean hasUnresolvedSequence;
        private volatile TestWebSocketServer server;

        List<Long> ackSequenceStarts() {
            return new ArrayList<>(ackSequenceStarts);
        }

        void setServer(TestWebSocketServer server) {
            this.server = server;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            boolean newConnection = currentClient != client;
            if (newConnection) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                dict.clear(); // fresh server dictionary per connection
                nextSeq.set(0);
                hasUnresolvedSequence = false;
            }
            if (hasUnresolvedSequence) {
                // A real server marks the connection's pipeline broken on the first
                // rejected frame and answers every later frame with silence -- no ACK,
                // no NACK -- until the connection resets (QwpIngressUpgradeProcessor's
                // hasUnresolvedSequence gate). Responding here would let the client
                // believe frames were processed that a real server dropped, and a
                // cumulative ACK could then leapfrog the rejected sequence.
                return;
            }
            try {
                QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            } catch (QwpWireTestUtils.DictionaryGapException gap) {
                // A real server answers a gap with STATUS_DICTIONARY_GAP and does NOT
                // apply the frame. ACKing here is what let a client sequence a real
                // server rejects pass green.
                hasUnresolvedSequence = true;
                try {
                    long nackSequence = nextSeq.getAndIncrement();
                    if (newConnection) {
                        ackSequenceStarts.add(nackSequence);
                    }
                    client.sendBinary(QwpWireTestUtils.buildNack(nackSequence, WebSocketResponse.STATUS_DICTIONARY_GAP));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return;
            }
            try {
                long ackSequence = nextSeq.getAndIncrement();
                if (newConnection) {
                    ackSequenceStarts.add(ackSequence);
                }
                client.sendBinary(QwpWireTestUtils.buildAck(ackSequence));
                if (connectionsAccepted.get() == 1) {
                    // Connection 1 registered the big symbol. Shrink the cap so the
                    // reconnect's catch-up budget can't fit it, then drop to force
                    // the reconnect. Setting the cap before the close (not from the
                    // test thread after it) removes the set-vs-reconnect race.
                    server.setAdvertisedMaxBatchSize(160); // catch-up budget = 132
                    Thread.sleep(50);
                    client.close();
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Like {@link CatchUpHandler}, but drops connection 1 only after it has
     * learned the whole batch, and counts the zero-table catch-up frames on
     * connection 2 so a test can assert the dictionary replay split across
     * several frames to respect the advertised batch cap.
     */
    private static class SplitCatchUpHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        // Set once the server has closed connection 1 (see CatchUpHandler.conn1Closed).
        volatile boolean conn1Closed;
        volatile int zeroTableFramesOnConn2;
        private final List<Long> ackSequenceStarts = new CopyOnWriteArrayList<>();
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private final int dropConn1AtDictSize;
        private final AtomicLong nextSeq = new AtomicLong(0);
        private boolean conn1Dropped;
        private TestWebSocketServer.ClientHandler currentClient;
        private boolean hasUnresolvedSequence;

        SplitCatchUpHandler(int dropConn1AtDictSize) {
            this.dropConn1AtDictSize = dropConn1AtDictSize;
        }

        List<Long> ackSequenceStarts() {
            return new ArrayList<>(ackSequenceStarts);
        }

        synchronized List<String> dictFor(int connNumber) {
            return connNumber <= dictsByConn.size()
                    // Copy under the lock: the caller iterates it unlocked while the
                    // server thread may still be appending to the live inner list.
                    ? new ArrayList<>(dictsByConn.get(connNumber - 1))
                    : new ArrayList<>();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            boolean newConnection = currentClient != client;
            if (newConnection) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                dictsByConn.add(new ArrayList<>()); // fresh dictionary per connection
                nextSeq.set(0);
                hasUnresolvedSequence = false;
            }
            if (hasUnresolvedSequence) {
                // A real server marks the connection's pipeline broken on the first
                // rejected frame and answers every later frame with silence -- no ACK,
                // no NACK -- until the connection resets (QwpIngressUpgradeProcessor's
                // hasUnresolvedSequence gate). Responding here would let the client
                // believe frames were processed that a real server dropped, and a
                // cumulative ACK could then leapfrog the rejected sequence.
                return;
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            try {
                QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            } catch (QwpWireTestUtils.DictionaryGapException gap) {
                // A real server answers a gap with STATUS_DICTIONARY_GAP and does NOT
                // apply the frame. ACKing here is what let a client sequence a real
                // server rejects pass green.
                hasUnresolvedSequence = true;
                try {
                    long nackSequence = nextSeq.getAndIncrement();
                    if (newConnection) {
                        ackSequenceStarts.add(nackSequence);
                    }
                    client.sendBinary(QwpWireTestUtils.buildNack(nackSequence, WebSocketResponse.STATUS_DICTIONARY_GAP));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return;
            }
            if (connNumber == 2 && QwpWireTestUtils.tableCount(data) == 0) {
                zeroTableFramesOnConn2++;
            }
            try {
                long ackSequence = nextSeq.getAndIncrement();
                if (newConnection) {
                    ackSequenceStarts.add(ackSequence);
                }
                client.sendBinary(QwpWireTestUtils.buildAck(ackSequence));
                // Drop connection 1 only once it has learned the entire batch, so
                // the sender's sent-dictionary mirror is complete and the reconnect
                // catch-up must replay a dictionary larger than the batch cap.
                if (connNumber == 1 && !conn1Dropped && dict.size() >= dropConn1AtDictSize) {
                    conn1Dropped = true;
                    Thread.sleep(50);
                    client.close();
                    conn1Closed = true;
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }
}
