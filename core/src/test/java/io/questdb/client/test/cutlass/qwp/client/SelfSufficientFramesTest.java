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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pins down the "every frame on disk is self-sufficient" rule for the symbol
 * dictionary.
 * <p>
 * The cursor SF path used to elide previously-sent symbols on subsequent
 * batches over the same connection, emitting a delta-dict that carried only
 * the new entries. That's wrong for SF: the bytes survive process restarts and
 * replay against fresh server connections (post-reconnect, or via a background
 * drainer adopting an orphan slot). A delta that references symbol ids the new
 * server has never seen is unrecoverable.
 * <p>
 * Today every frame must carry a complete symbol-dict delta starting at id 0
 * (column schemas travel inline on the first batch too). This test asserts the
 * symbol-dict invariant on the wire.
 */
public class SelfSufficientFramesTest {

    /** First byte of the symbol-dict delta payload after the 12-byte QWP header. */
    private static final int DELTA_START_OFFSET = 12;

    @Test
    public void testEverySymbolBatchIncludesFullDeltaFromZero() throws Exception {
        // Send two batches against the same connection, each with a
        // distinct symbol value. With the old schema-ref/delta encoding,
        // batch 2 would emit deltaStart=1, deltaCount=1 — only the new
        // symbol. With self-sufficient frames, batch 2 must emit
        // deltaStart=0 covering BOTH symbols.
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

            // The deltaStart varint sits right after the 12-byte header.
            // For self-sufficient frames it must be 0 (single byte 0x00)
            // in BOTH batches — regardless of how many symbols the prior
            // batch already shipped.
            int deltaStart1 = readVarint(b1, DELTA_START_OFFSET);
            int deltaStart2 = readVarint(b2, DELTA_START_OFFSET);
            Assert.assertEquals("batch 1 deltaStart must be 0", 0, deltaStart1);
            Assert.assertEquals("batch 2 deltaStart must be 0 (self-sufficient)",
                    0, deltaStart2);

            // batch 2 must include >= 2 symbols in its delta dict (alpha
            // from the prior batch + beta from this one). The varint at
            // DELTA_START_OFFSET+1 is deltaCount.
            int deltaCount2 = readVarint(b2, DELTA_START_OFFSET + 1);
            Assert.assertTrue("batch 2 must redefine at least 2 symbols, got " + deltaCount2,
                    deltaCount2 >= 2);

            // Sanity: batch 2 should NOT be much smaller than batch 1 —
            // with schema-ref/delta encoding it would have been; with
            // self-sufficient frames the size is in the same ballpark.
            Assert.assertTrue("batch 2 (" + b2.length + " bytes) must not be drastically smaller than batch 1 ("
                            + b1.length + ")",
                    b2.length >= b1.length / 2);
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
