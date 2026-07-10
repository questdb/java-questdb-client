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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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
                Assert.assertEquals("2nd connection dictionary size", 2, conn2.size());
                Assert.assertEquals("alpha", conn2.get(0));
                Assert.assertEquals("beta", conn2.get(1));
            }
        });
    }

    @Test
    public void testCatchUpEntryTooLargeForCapFailsTerminally() throws Exception {
        // A dictionary entry that exceeds the reconnect server's per-chunk budget
        // (cap - HEADER_SIZE - 16) cannot be shipped as a catch-up chunk.
        // sendDictCatchUp must latch a clean terminal ("... during catch-up")
        // rather than call fail(): pre-fix the oversized entry drove an endless
        // reconnect loop (the entry never shrinks and the same cluster
        // re-advertises the same cap) and re-entered connectLoop from the catch-up.
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
                LineSenderException terminal = null;
                Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";");
                try {
                    sender.table("t").symbol("s", bigSymbol).longColumn("v", 1L).atNow();
                    sender.flush();
                    // The terminal latches on the I/O thread once the reconnect's
                    // catch-up hits the oversized entry; it surfaces to the producer
                    // on a subsequent flush. Poll a bounded time for it. The polling
                    // rows use a small symbol that fits the shrunk cap, so the
                    // producer-side cap check never fires and flush() surfaces the
                    // I/O thread's catch-up terminal via checkError.
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline && terminal == null) {
                        try {
                            sender.table("t").symbol("s", "y").longColumn("v", 2L).atNow();
                            sender.flush();
                            Thread.sleep(20);
                        } catch (LineSenderException e) {
                            terminal = e;
                        }
                    }
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException e) {
                        if (terminal == null) {
                            terminal = e;
                        }
                    }
                }
                Assert.assertNotNull("an oversized catch-up entry must surface a terminal", terminal);
                Assert.assertTrue("terminal must come from the catch-up path, got: " + terminal.getMessage(),
                        terminal.getMessage().contains("during catch-up"));
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
            }
        });
    }

    private static String symbolName(int i) {
        // 10-char symbols so 40 of them clearly exceed the advertised 160-byte cap.
        return "symbol" + (1000 + i);
    }

    private static int readVarint(byte[] buf, int[] pos) {
        int result = 0;
        int shift = 0;
        while (pos[0] < buf.length) {
            int b = buf[pos[0]++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 28) throw new IllegalStateException("varint too long");
        }
        throw new IllegalStateException("varint truncated");
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
        volatile boolean sawZeroTableFrameOnConn2;
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

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
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            accumulate(data, dict);
            if (connNumber == 2 && tableCount(data) == 0) {
                sawZeroTableFrameOnConn2 = true;
            }
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
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

        private static void accumulate(byte[] frame, List<String> dict) {
            final byte FLAG_DELTA_SYMBOL_DICT = 0x08;
            if (frame.length < 12 || (frame[5] & FLAG_DELTA_SYMBOL_DICT) == 0) {
                return;
            }
            int[] pos = {12}; // just past the 12-byte QWP header
            int deltaStart = readVarint(frame, pos);
            int deltaCount = readVarint(frame, pos);
            while (dict.size() < deltaStart) {
                dict.add(null); // null-pad, mirroring the server
            }
            for (int i = 0; i < deltaCount; i++) {
                int len = readVarint(frame, pos);
                String symbol = new String(frame, pos[0], len, StandardCharsets.UTF_8);
                pos[0] += len;
                int idx = deltaStart + i;
                while (dict.size() <= idx) {
                    dict.add(null);
                }
                dict.set(idx, symbol);
            }
        }

        private static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }

        private static int tableCount(byte[] frame) {
            return (frame[6] & 0xFF) | ((frame[7] & 0xFF) << 8);
        }
    }

    /**
     * ACKs connection 1's frames with no advertised cap (so an oversized symbol
     * registers), then -- once connection 1 has sent something -- shrinks the
     * advertised batch cap and drops the socket. The reconnect (connection 2)
     * therefore advertises a cap whose catch-up budget is too small for the
     * symbol, exercising the oversized-entry terminal in sendDictCatchUp.
     */
    private static class CapShrinkHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler currentClient;
        private volatile TestWebSocketServer server;

        void setServer(TestWebSocketServer server) {
            this.server = server;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
            }
            try {
                client.sendBinary(CatchUpHandler.buildAck(nextSeq.getAndIncrement()));
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
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private final int dropConn1AtDictSize;
        private final AtomicLong nextSeq = new AtomicLong(0);
        private boolean conn1Dropped;
        private TestWebSocketServer.ClientHandler currentClient;

        SplitCatchUpHandler(int dropConn1AtDictSize) {
            this.dropConn1AtDictSize = dropConn1AtDictSize;
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
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            CatchUpHandler.accumulate(data, dict);
            if (connNumber == 2 && CatchUpHandler.tableCount(data) == 0) {
                zeroTableFramesOnConn2++;
            }
            try {
                client.sendBinary(CatchUpHandler.buildAck(nextSeq.getAndIncrement()));
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
