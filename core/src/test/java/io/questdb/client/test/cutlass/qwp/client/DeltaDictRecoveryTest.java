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
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end recovery for the delta symbol dictionary in store-and-forward mode.
 * <p>
 * A file-mode sender writes delta-encoded SYMBOL frames (each frame carries only
 * the ids it introduces) to a slot but never drains it -- simulating a crash. A
 * fresh sender then recovers the slot and replays those non-self-sufficient
 * frames to a brand-new server whose dictionary starts empty. Correctness hinges
 * on the persisted {@code .symbol-dict}: the recovering sender loads it, the I/O
 * thread re-registers the whole dictionary via a catch-up frame, and only then do
 * the delta frames replay. This test reconstructs the server-side dictionary from
 * the wire and asserts it comes out complete and gap-free.
 */
public class DeltaDictRecoveryTest {

    private static final int DISTINCT_SYMBOLS = 8;
    private static final int ROWS = 40;
    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-delta-recov-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) {
            rmDirRec(sfDir);
        }
    }

    @Test
    public void testRecoveredSlotReplaysDeltaFramesAgainstFreshServer() throws Exception {
        // Phase 1: silent server (no acks). Sender 1 writes symbol rows and
        // close-fast (no drain), leaving unacked delta frames + a persisted
        // dictionary in the slot.
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));

            String pad = TestUtils.repeat("x", 64);
            String cfg = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sf_max_bytes=4096"
                    + ";close_flush_timeout_millis=0;";
            try (Sender s1 = Sender.fromConfig(cfg)) {
                for (int i = 0; i < ROWS; i++) {
                    s1.table("m")
                            .symbol("s", "sym-" + (i % DISTINCT_SYMBOLS))
                            .stringColumn("p", pad)
                            .longColumn("v", i)
                            .atNow();
                    s1.flush();
                }
            }
        }

        // Ack a prefix so recovery does NOT replay from the self-sufficient head.
        // Rows 0..DISTINCT_SYMBOLS-1 register all the symbols, so stamping the
        // watermark at FSN DISTINCT_SYMBOLS-1 makes recovery replay from FSN
        // DISTINCT_SYMBOLS onward -- frames whose delta starts at
        // DISTINCT_SYMBOLS and carries NO new symbols (rows past the first cycle
        // reuse existing ids). The early ids those frames reference then exist
        // ONLY in the persisted dictionary, so the reconstructed dictionary below
        // is complete solely because the catch-up frame re-registered them. That
        // pins the content assertions to the catch-up: without it (or with a
        // broken one) the fresh server would null-pad ids 0..DISTINCT_SYMBOLS-1
        // and the per-id checks would fail.
        java.nio.file.Path slot = Paths.get(sfDir, "default");
        writeAckWatermark(slot.resolve(".ack-watermark"), DISTINCT_SYMBOLS - 1);

        // Phase 2: fresh server that reconstructs its per-connection dictionary
        // from the delta sections. Sender 2 recovers the slot and replays.
        DictReconstructingHandler handler = new DictReconstructingHandler();
        try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
            int port = good.getPort();
            good.start();
            Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
            try (Sender ignored = Sender.fromConfig(cfg)) {
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && handler.maxDictSize() < DISTINCT_SYMBOLS) {
                    Thread.sleep(20);
                }
            }

            // The recovering sender must have re-registered the dictionary via a
            // catch-up (0-table) frame before replaying delta frames.
            Assert.assertTrue("recovery sent a full-dictionary catch-up frame",
                    handler.sawCatchUpFrame);
            // The reconstructed dictionary must be complete and gap-free: exactly
            // the DISTINCT_SYMBOLS symbols, no null padding left by a missing id.
            List<String> dict = handler.dictSnapshot();
            Assert.assertEquals("reconstructed dictionary size", DISTINCT_SYMBOLS, dict.size());
            for (int i = 0; i < DISTINCT_SYMBOLS; i++) {
                Assert.assertEquals("dictionary id " + i, "sym-" + i, dict.get(i));
            }
        }
    }

    @Test
    public void testTornDictionaryFailsCleanlyInsteadOfCorrupting() throws Exception {
        // Phase 1: each row introduces a new symbol, so frame i carries deltaStart=i.
        // Silent server + close-fast leaves all frames unacked in the slot.
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";close_flush_timeout_millis=0;";
            try (Sender s1 = Sender.fromConfig(cfg)) {
                for (int i = 0; i < 6; i++) {
                    s1.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                    s1.flush();
                }
            }
        }

        // Simulate a host/power crash: the segment frames survive but the persisted
        // dictionary is lost, and the ack watermark was left mid-stream. Truncate
        // .symbol-dict to its 8-byte header (0 symbols) and stamp the watermark at
        // FSN 2, so recovery replays from FSN 3 -- a frame with deltaStart=3.
        java.nio.file.Path slot = Paths.get(sfDir, "default");
        java.nio.file.Path dict = slot.resolve(".symbol-dict");
        byte[] header = Arrays.copyOf(java.nio.file.Files.readAllBytes(dict), 8);
        java.nio.file.Files.write(dict, header);
        writeAckWatermark(slot.resolve(".ack-watermark"), 2);

        // Phase 2: recover against a fresh counting server. The replay guard must
        // fire (frame deltaStart 3 > recovered dictionary size 0) and fail terminally
        // rather than send a gapped frame that would corrupt the table.
        CountingHandler handler = new CountingHandler();
        try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
            int port = good.getPort();
            good.start();
            Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
            LineSenderException terminal = null;
            Sender s2 = Sender.fromConfig(cfg);
            try {
                Thread.sleep(1_000); // let the I/O loop attempt replay and hit the guard
            } finally {
                try {
                    s2.close();
                } catch (LineSenderException e) {
                    terminal = e;
                }
            }
            Assert.assertEquals("no frame may be replayed to a fresh server with a torn dictionary",
                    0, handler.frames.get());
            Assert.assertNotNull("a torn dictionary must surface a terminal error", terminal);
            Assert.assertTrue(terminal.getMessage(),
                    terminal.getMessage().contains("symbol dictionary is incomplete"));
        }
    }

    private static void writeAckWatermark(java.nio.file.Path path, long fsn) throws IOException {
        byte[] buf = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(0x31574B41); // 'AKW1'
        bb.putInt(0);          // reserved
        bb.putLong(fsn);
        java.nio.file.Files.write(path, buf);
    }

    private static int readVarint(byte[] buf, int[] pos) {
        int result = 0;
        int shift = 0;
        while (pos[0] < buf.length) {
            int b = buf[pos[0]++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalStateException("varint too long");
            }
        }
        throw new IllegalStateException("varint truncated");
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            rmDirRec(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /**
     * Reconstructs the per-connection symbol dictionary from delta sections,
     * mirroring the server's {@code setQuick(deltaStart + i)} + null-padding.
     */
    private static class DictReconstructingHandler implements TestWebSocketServer.WebSocketServerHandler {
        volatile boolean sawCatchUpFrame;
        private final List<String> dict = new ArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler currentClient;

        synchronized List<String> dictSnapshot() {
            return new ArrayList<>(dict);
        }

        synchronized int maxDictSize() {
            return dict.size();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                dict.clear(); // fresh server dictionary per connection
            }
            accumulate(data);
            if (tableCount(data) == 0 && hasDelta(data)) {
                sawCatchUpFrame = true;
            }
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
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

        private static boolean hasDelta(byte[] frame) {
            return frame.length >= 12 && (frame[5] & 0x08) != 0;
        }

        private static int tableCount(byte[] frame) {
            return (frame[6] & 0xFF) | ((frame[7] & 0xFF) << 8);
        }

        private void accumulate(byte[] frame) {
            if (!hasDelta(frame)) {
                return;
            }
            int[] pos = {12};
            int deltaStart = readVarint(frame, pos);
            int deltaCount = readVarint(frame, pos);
            while (dict.size() < deltaStart) {
                dict.add(null);
            }
            for (int i = 0; i < deltaCount; i++) {
                int len = readVarint(frame, pos);
                String sym = new String(frame, pos[0], len, StandardCharsets.UTF_8);
                pos[0] += len;
                int idx = deltaStart + i;
                while (dict.size() <= idx) {
                    dict.add(null);
                }
                dict.set(idx, sym);
            }
        }
    }

    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks -- sender leaves everything unacked in the slot
        }
    }

    /** Counts every binary frame it receives and acks it. */
    private static class CountingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger frames = new AtomicInteger();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
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
    }
}
