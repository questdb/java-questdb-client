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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pin-down for recovery replay across sender restarts.
 * <p>
 * Previously {@code CursorWebSocketSendLoop.start()} began at the active
 * segment, skipping every sealed segment on disk. After a crash + restart
 * with multiple segments holding unacked data, the foreground sender
 * would orphan everything in sealed and only ship the active's tail.
 * <p>
 * Today {@code start()} positions at {@code engine.ackedFsn() + 1} —
 * walking sealed segments oldest-first — and the engine constructor
 * seeds {@code ackedFsn} to {@code lowestBaseSeq - 1} on recovery so the
 * positioning lands on the right segment even if earlier ones were
 * trimmed before the crash.
 */
public class RecoveryReplayTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-recov-replay-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test
    public void testRestartReplaysSealedSegmentsAgainstFreshServer() throws Exception {
        // Phase 1: silent server, sender 1 writes enough to rotate at
        // least once, closes fast (no drain). Slot ends up with sealed +
        // active segments holding unacked data.
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port1 = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));

            // Use a tight segment cap and pad each row with a sizable
            // payload so 50 batches genuinely span multiple segments.
            // Without rotation there'd be no sealed segments and the
            // start-position bug couldn't manifest — defeating the test.
            String pad = repeat("x", 64);
            String cfg1 = "ws::addr=localhost:" + port1
                    + ";sf_dir=" + sfDir
                    + ";sf_max_bytes=4096"
                    + ";close_flush_timeout_millis=0;";
            try (Sender s1 = Sender.fromConfig(cfg1)) {
                for (int i = 0; i < 50; i++) {
                    s1.table("foo").stringColumn("p", pad).longColumn("v", i).atNow();
                    s1.flush();
                }
            }
        }

        // Sanity: the slot must hold at least one sealed segment (one
        // that's been rotated out of active and closed). We verify by
        // checking publishedFsn jumps across the active segment's base
        // seq when re-opened — i.e. there's data in a segment older than
        // the active.
        int populatedCount = countPopulatedSegmentFiles(sfDir + "/default");
        Assert.assertTrue("expected multi-segment slot with data, got "
                        + populatedCount + " populated .sfa files",
                populatedCount >= 2);

        // Phase 2: fresh server that ACKs every binary frame. Sender 2
        // opens the same slot. The bug-fix expectation: every frame
        // sender 1 wrote (50 of them) reaches the new server. Without
        // the fix, the sender would only ship the active segment's data
        // (≪ 50) and orphan the sealed segments forever.
        AckHandler ack = new AckHandler();
        try (TestWebSocketServer good = new TestWebSocketServer(ack)) {
            int port2 = good.getPort();
            good.start();
            Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

            String cfg2 = "ws::addr=localhost:" + port2
                    + ";sf_dir=" + sfDir + ";";
            try (Sender ignored = Sender.fromConfig(cfg2)) {
                // No new appends — purely replay.
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline
                        && ack.distinctPayloadHashes.size() < 50) {
                    Thread.sleep(20);
                }
            }
            // Each row carries a unique long, so every frame's bytes are
            // distinct. With the start-position fix we expect all 50 of
            // sender 1's rows to reach server 2; without the fix the cursor
            // would skip straight to the active segment and orphan
            // everything in sealed.
            Assert.assertEquals(
                    "every distinct row written by sender 1 must replay through to server 2",
                    50, ack.distinctPayloadHashes.size());
        }
    }

    /**
     * Counts only segment files that actually carry frames — opens each
     * .sfa via the cursor's MmapSegment recovery path and excludes the
     * empty hot-spares the segment manager pre-allocates. Without this
     * filter, the multi-segment sanity check could pass for the wrong
     * reason on a deployment that's only used a single segment.
     */
    private static int countPopulatedSegmentFiles(String dir) {
        if (!Files.exists(dir)) return 0;
        long find = Files.findFirst(dir);
        if (find <= 0) return 0;
        int n = 0;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && name.endsWith(".sfa")) {
                    try {
                        try (MmapSegment seg = MmapSegment.openExisting(dir + "/" + name)) {
                            if (seg.frameCount() > 0) n++;
                        }
                    } catch (Throwable ignored) {
                        // best-effort
                    }
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
        return n;
    }

    private static String repeat(String c, int n) {
        return io.questdb.client.test.tools.TestUtils.repeat(String.valueOf(c), Math.max(0, n));
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) rmDirRec(child);
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
     * Acks every binary frame and tracks distinct payloads.
     */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        // Distinct *payload bytes* — each row carries a unique long value
        // so every frame's bytes differ. Counts unique frames received,
        // independent of any amplification (re-sends, fragmentation).
        final java.util.Set<String> distinctPayloadHashes =
                java.util.Collections.synchronizedSet(new java.util.HashSet<>());
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            distinctPayloadHashes.add(java.util.Arrays.toString(data));
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }

    /**
     * Receives binary frames but never acks. Sender drops them on close.
     */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally empty
        }
    }
}
