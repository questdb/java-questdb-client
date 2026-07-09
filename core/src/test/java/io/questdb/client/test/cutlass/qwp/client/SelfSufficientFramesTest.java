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
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pins down how the symbol dictionary is framed on the wire.
 * <p>
 * Both engine modes ship <b>monotonic</b> deltas -- each symbol id travels once,
 * not the whole dictionary per message -- which is the bandwidth win this feature
 * adds. The I/O thread re-registers the dictionary with a catch-up frame whenever
 * it (re)connects, so a fresh server can resolve the non-self-sufficient delta
 * frames that follow.
 * <p>
 * The modes differ only in where the catch-up's dictionary comes from: memory
 * mode keeps it in an in-process mirror; file-backed store-and-forward keeps it in
 * a per-slot {@code .symbol-dict} file so a recovered or orphan-drained slot (a
 * fresh process with no in-memory mirror) can rebuild it. This test asserts the
 * monotonic wire framing in both modes and the presence of that dictionary file.
 */
public class SelfSufficientFramesTest {

    /** First byte of the symbol-dict delta payload after the 12-byte QWP header. */
    private static final int DELTA_START_OFFSET = 12;

    @Test
    public void testFileModeShipsMonotonicDeltaAndPersistsDict() throws Exception {
        // File-backed SF also ships monotonic deltas now: batch 2 carries only
        // "beta" (deltaStart=1). The dictionary is durably kept in .symbol-dict
        // so a recovered/orphan-drained slot can rebuild it.
        Path sfDir = Files.createTempDirectory("qwp-sf-selfsufficient");
        CapturingHandler handler = new CapturingHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
            // The engine places slot files under sf_dir/<sender_id> (default "default").
            Path dictFile = sfDir.resolve("default").resolve(".symbol-dict");
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> handler.batches.size() >= 1, 5_000);

                sender.table("foo").symbol("s", "beta").longColumn("v", 2L).atNow();
                sender.flush();
                waitFor(() -> handler.batches.size() >= 2, 5_000);

                // Check the persisted dictionary while the sender is live: a
                // fully-drained close intentionally unlinks it (slot cleanup).
                Assert.assertTrue("persisted dictionary file exists", Files.exists(dictFile));
                byte[] dict = Files.readAllBytes(dictFile);
                Assert.assertTrue("dictionary retains alpha", containsUtf8(dict, "alpha"));
                Assert.assertTrue("dictionary retains beta", containsUtf8(dict, "beta"));
            }

            Assert.assertEquals("expected 2 captured batches", 2, handler.batches.size());
            byte[] b1 = handler.batches.get(0);
            byte[] b2 = handler.batches.get(1);

            Assert.assertEquals("batch 1 deltaStart must be 0",
                    0, readVarint(b1, DELTA_START_OFFSET));
            Assert.assertEquals("batch 1 deltaCount must be 1", 1, readVarint(b1, DELTA_START_OFFSET + 1));
            // batch 2 ships ONLY beta as a delta from id 1.
            Assert.assertEquals("batch 2 deltaStart must be 1 (monotonic)",
                    1, readVarint(b2, DELTA_START_OFFSET));
            Assert.assertEquals("batch 2 deltaCount must be 1 (only the new symbol)",
                    1, readVarint(b2, DELTA_START_OFFSET + 1));
        } finally {
            rmDir(sfDir);
        }
    }

    @Test
    public void testDiskModeFallsBackToFullDictWhenPersistedDictUnopenable() throws Exception {
        // When the per-slot .symbol-dict cannot be opened in disk mode,
        // isDeltaDictEnabled() is false and the sender must fall back to
        // self-sufficient frames: every batch re-ships the WHOLE dictionary from
        // id 0. A recovered / orphan-drained slot then has no dictionary to
        // rebuild deltas from, so a monotonic delta would dangle ids on the fresh
        // server -- the full-dict frame is the safe degradation. Force the open
        // failure by planting a DIRECTORY where the dictionary file belongs:
        // openRW / openCleanRW on a directory fails, so open() returns null.
        Path sfDir = Files.createTempDirectory("qwp-sf-fallback");
        Path dictPath = sfDir.resolve("default").resolve(".symbol-dict");
        Files.createDirectories(dictPath);             // a directory, not a file
        Files.createFile(dictPath.resolve("blocker")); // non-empty: cannot be unlinked/rmdir'd
        CapturingHandler handler = new CapturingHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
            try (Sender sender = Sender.fromConfig(config)) {
                sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> handler.batches.size() >= 1, 5_000);

                sender.table("foo").symbol("s", "beta").longColumn("v", 2L).atNow();
                sender.flush();
                waitFor(() -> handler.batches.size() >= 2, 5_000);
            }

            // The planted directory is untouched -- the dictionary never opened,
            // so delta encoding stayed disabled.
            Assert.assertTrue("planted .symbol-dict directory must remain (open failed)",
                    Files.isDirectory(dictPath));

            Assert.assertEquals("expected 2 captured batches", 2, handler.batches.size());
            byte[] b1 = handler.batches.get(0);
            byte[] b2 = handler.batches.get(1);

            // Full-dict fallback: BOTH batches start at id 0, and batch 2 re-ships
            // the WHOLE dictionary (alpha + beta), NOT a monotonic delta (which
            // would be deltaStart=1, deltaCount=1 as in the test above).
            Assert.assertEquals("batch 1 deltaStart must be 0",
                    0, readVarint(b1, DELTA_START_OFFSET));
            Assert.assertEquals("batch 1 deltaCount must be 1",
                    1, readVarint(b1, DELTA_START_OFFSET + 1));
            Assert.assertEquals("batch 2 deltaStart must be 0 (full-dict fallback, not monotonic)",
                    0, readVarint(b2, DELTA_START_OFFSET));
            Assert.assertEquals("batch 2 deltaCount must be 2 (whole dictionary re-shipped)",
                    2, readVarint(b2, DELTA_START_OFFSET + 1));
        } finally {
            rmDir(sfDir);
        }
    }

    private static boolean containsUtf8(byte[] haystack, String needle) {
        byte[] n = needle.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        outer:
        for (int i = 0; i + n.length <= haystack.length; i++) {
            for (int j = 0; j < n.length; j++) {
                if (haystack[i + j] != n[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    @Test
    public void testMemoryModeShipsMonotonicDelta() throws Exception {
        // Memory-mode (no sf_dir): each symbol id ships once. Batch 2 carries
        // only "beta" as a delta starting at id 1, not the whole dictionary.
        CapturingHandler handler = new CapturingHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> handler.batches.size() >= 1, 5_000);

                sender.table("foo").symbol("s", "beta").longColumn("v", 2L).atNow();
                sender.flush();
                waitFor(() -> handler.batches.size() >= 2, 5_000);
            }

            Assert.assertEquals("expected 2 captured batches", 2, handler.batches.size());
            byte[] b1 = handler.batches.get(0);
            byte[] b2 = handler.batches.get(1);

            // Batch 1 introduces alpha at id 0.
            Assert.assertEquals("batch 1 deltaStart must be 0",
                    0, readVarint(b1, DELTA_START_OFFSET));
            Assert.assertEquals("batch 1 deltaCount must be 1",
                    1, readVarint(b1, DELTA_START_OFFSET + 1));

            // Batch 2 ships ONLY beta as a delta from id 1.
            Assert.assertEquals("batch 2 deltaStart must be 1 (monotonic)",
                    1, readVarint(b2, DELTA_START_OFFSET));
            Assert.assertEquals("batch 2 deltaCount must be 1 (only the new symbol)",
                    1, readVarint(b2, DELTA_START_OFFSET + 1));
        }
    }

    private static int readVarint(byte[] buf, int offset) {
        // Simple unsigned varint decode — sufficient for small values.
        int result = 0;
        int shift = 0;
        while (offset < buf.length) {
            int b = buf[offset++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 28) throw new IllegalStateException("varint too long");
        }
        throw new IllegalStateException("varint truncated");
    }

    private static void rmDir(Path dir) {
        try {
            if (dir == null || !Files.exists(dir)) {
                return;
            }
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {
                            // best-effort
                        }
                    });
        } catch (IOException ignored) {
            // best-effort
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

    /** Captures every binary frame for later inspection AND ACKs it. */
    private static class CapturingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final java.util.List<byte[]> batches =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            batches.add(data.clone());
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
