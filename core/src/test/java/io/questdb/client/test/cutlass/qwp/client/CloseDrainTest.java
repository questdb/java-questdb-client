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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression tests for the close() drain semantics.
 * <p>
 * Without {@code close_flush_timeout_millis}, close() returned as soon as
 * the cursor I/O loop's {@code running} flag flipped — meaning frames
 * still queued in the engine could be dropped when the JVM exited
 * immediately after close(). The drain timeout makes close() wait for
 * the server to ACK everything published before shutting the loop down.
 */
public class CloseDrainTest {

    @Test
    public void testCloseBlocksUntilAckArrives() throws Exception {
        // Server delays every ACK by 800ms. With the default
        // close_flush_timeout_millis=60000, close() must wait for that
        // ACK before returning. Pre-fix close() returned within milliseconds.
        long ackDelayMs = 800;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port + ";";  // memory mode
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                sender.close();
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            Assert.assertTrue(
                    "close() took only " + elapsedMs + "ms — did not wait for ACK; "
                            + "drain timeout is broken or never enabled",
                    elapsedMs >= ackDelayMs / 2);
        }
    }

    @Test
    public void testCloseFastWhenTimeoutIsZero() throws Exception {
        // Same delayed-ACK server, but with close_flush_timeout_millis=0
        // (fast close). close() must return immediately, well before the
        // ACK delay would have elapsed.
        long ackDelayMs = 1500;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=0;";
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                sender.close();
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            Assert.assertTrue(
                    "close() with timeout=0 took " + elapsedMs + "ms — fast close is broken",
                    elapsedMs < ackDelayMs / 2);
        }
    }

    @Test
    public void testCloseFastWhenTimeoutIsMinusOne() throws Exception {
        // Documented contract: close_flush_timeout_millis=-1 opts out of the
        // drain (fast close), same as 0. See LineSenderBuilder#closeFlushTimeoutMillis
        // Javadoc — "Set to 0 or -1 to opt out — close() will not wait at all".
        //
        // Currently fails because -1 collides with the PARAMETER_NOT_SET_EXPLICITLY
        // sentinel in LineSenderBuilder, so the build path silently substitutes
        // DEFAULT_CLOSE_FLUSH_TIMEOUT_MILLIS (60s) and close() blocks for the
        // full ACK delay instead of returning fast.
        long ackDelayMs = 1500;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=-1;";
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                sender.close();
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            Assert.assertTrue(
                    "close() with timeout=-1 took " + elapsedMs + "ms — "
                            + "the documented -1 opt-out is being silently overridden by the default",
                    elapsedMs < ackDelayMs / 2);
        }
    }

    @Test
    public void testCloseDrainTimesOutWhenAcksNeverArrive() throws Exception {
        // Server that buffers frames silently and never ACKs. close() must
        // throw a drain-timeout LineSenderException after roughly the
        // configured timeout — not hang forever and not return immediately.
        long timeoutMs = 500;
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=" + timeoutMs + ";";
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                try {
                    sender.close();
                    Assert.fail("close() should have thrown a drain-timeout error");
                } catch (LineSenderException e) {
                    Assert.assertTrue("expected drain-timeout message, got: " + e.getMessage(),
                            e.getMessage().contains("drain timed out"));
                }
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            // idempotent — closed flag is set on first call
            Assert.assertTrue("close() returned too early: " + elapsedMs + "ms",
                    elapsedMs >= timeoutMs);
            Assert.assertTrue("close() exceeded the bounded timeout by too much: " + elapsedMs + "ms",
                    elapsedMs < timeoutMs * 4);
        }
    }

    @Test
    public void testCloseSkipsDrainForUncommittedDeferredTail() throws Exception {
        // Regression test for the close()-hang on abandoned deferred
        // transactions: the server withholds acks for FLAG_DEFER_COMMIT
        // frames until their group-closing commit lands, so a close()-time
        // drain that targets publishedFsn (instead of the last commit
        // boundary) can only ever time out -- 300s hangs in the e2e suite
        // (testDeferredCommitConnectionDropRollsBack).
        //
        // Same producer sequence as that e2e test, against a server that
        // never acks (which is exactly what the real server does to an
        // uncommitted deferred tail): defer-commit mode, publish rows, no
        // commit, close(). Fixed close() drains to
        // min(publishedFsn, commitBoundary) = -1, abandons the tail with a
        // WARN, and returns immediately. Broken close() targets the deferred
        // frame and throws "drain timed out" after the full timeout.
        long timeoutMs = 2000;
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=" + timeoutMs + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                ((QwpWebSocketSender) sender).setDeferCommit(true);
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush(); // publishes a deferred frame; commit never sent

                long t0 = System.nanoTime();
                try {
                    sender.close();
                } catch (LineSenderException e) {
                    Assert.fail("close() must not wait for acks of an uncommitted deferred "
                            + "tail (the server withholds them by design), but threw: "
                            + e.getMessage());
                }
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                Assert.assertTrue("close() took " + elapsedMs + "ms -- it drained toward the "
                                + "uncommitted deferred tail instead of stopping at the commit "
                                + "boundary",
                        elapsedMs < timeoutMs / 2);
            }
        }
    }

    @Test
    public void testCloseDrainsToCommitBoundaryAndAbandonsDeferredTail() throws Exception {
        // Mixed case: one committed frame followed by an uncommitted deferred
        // tail. close() must still wait for the committed frame's ack (the
        // commit-boundary drain is not an opt-out of draining altogether) but
        // must not wait for the deferred tail above it.
        //
        // The handler acks only the first data frame (the committed one) and
        // stays silent above it -- mirroring the real server, which acks
        // commit-bearing frames and withholds acks for deferred ones. Broken
        // close() targets publishedFsn (the deferred tail) and throws "drain
        // timed out"; fixed close() returns once the boundary frame is acked.
        long timeoutMs = 2000;
        AckFirstFrameOnlyHandler handler = new AckFirstFrameOnlyHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=" + timeoutMs + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush(); // commit-bearing frame FSN 0 -- boundary = 0, acked

                ((QwpWebSocketSender) sender).setDeferCommit(true);
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush(); // deferred frame FSN 1 -- never acked, never committed

                long t0 = System.nanoTime();
                try {
                    sender.close();
                } catch (LineSenderException e) {
                    Assert.fail("close() must drain to the commit boundary (FSN 0) and abandon "
                            + "the uncommitted deferred tail above it, but threw: "
                            + e.getMessage());
                }
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                Assert.assertTrue("close() took " + elapsedMs + "ms -- it drained toward the "
                                + "uncommitted deferred tail instead of stopping at the acked "
                                + "commit boundary",
                        elapsedMs < timeoutMs / 2);
            }
        }
    }

    @Test
    public void testDrainBlocksUntilAckArrivesAndReturnsTrue() throws Exception {
        // Public drain(timeoutMillis): explicit pre-close drain that the
        // caller controls per call-site. Same delayed-ACK server as
        // testCloseBlocksUntilAckArrives, but the wait happens inside the
        // explicit drain() call. The subsequent close() should be a near-
        // instant no-op because everything is already acked.
        long ackDelayMs = 600;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                long t0 = System.nanoTime();
                boolean drained = sender.drain(5_000);
                long drainElapsedMs = (System.nanoTime() - t0) / 1_000_000;
                Assert.assertTrue("drain(5000) must return true when the ACK arrives within budget",
                        drained);
                Assert.assertTrue("drain returned too fast (no actual wait): " + drainElapsedMs + "ms",
                        drainElapsedMs >= ackDelayMs / 2);

                long c0 = System.nanoTime();
                sender.close();
                long closeElapsedMs = (System.nanoTime() - c0) / 1_000_000;
                Assert.assertTrue("close() after drained sender should be near-instant, was "
                        + closeElapsedMs + "ms",
                        closeElapsedMs < ackDelayMs);
            }
        }
    }

    /**
     * Regression test for #7142: drain() after a prior flush() with unacked
     * frames must block for those frames, even though the inner
     * flushAndGetSequence() publishes nothing and returns -1.
     * <p>
     * On buggy code (no drain() override): drain() calls the default
     * Sender.drain() → flushAndGetSequence() returns -1 → awaitAckedFsn(-1, ...)
     * returns true immediately (ackedFsn >= -1 is always true) at elapsed≈0ms.
     * The elapsed >= 300 assertion fails deterministically.
     * <p>
     * On fixed code: drain() uses the watermark override → waits for the
     * delayed ACK (~600ms) → passes.
     */
    @Test
    public void testDrainAfterFlushWaitsForPriorUnackedFrames() throws Exception {
        long ackDelayMs = 600;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();                         // publish FSN 0; ACK delayed ~600ms

                long t0 = System.nanoTime();
                boolean drained = sender.drain(5_000);  // empty flush → -1 on buggy code
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

                Assert.assertTrue("drain() must return true when ACK arrives within budget",
                        drained);
                Assert.assertTrue(
                        "drain() must wait for prior unacked frame, but returned in only "
                                + elapsedMs + "ms (expected >= " + (ackDelayMs / 2) + "ms)",
                        elapsedMs >= ackDelayMs / 2);
            }
        }
    }

    @Test
    public void testDrainReturnsFalseOnTimeoutAndSenderStillUsable() throws Exception {
        // Server never ACKs. drain() with a small timeout must return false
        // rather than throw (unlike close()'s implicit drain, which
        // converts a timeout into a LineSenderException). The sender stays
        // usable for further row writes after a false return; the
        // outstanding frames remain pending and close()'s own drain still
        // runs.
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port + ";close_flush_timeout_millis=0;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                long t0 = System.nanoTime();
                boolean drained = sender.drain(200);
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                Assert.assertFalse("drain must return false when the server never acks", drained);
                Assert.assertTrue("drain returned far past the timeout: " + elapsedMs + "ms",
                        elapsedMs >= 150 && elapsedMs < 2_000);
                // Sender must still be usable: write another row and flush
                // without observing the latched error from the silent peer.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
            }
        }
    }

    @Test
    public void testDrainNonZeroTimeoutOnFastServerReturnsImmediately() throws Exception {
        // Fast server: every frame is acked promptly. drain(longTimeout)
        // must return true quickly -- no spurious wait when there is
        // nothing to wait for.
        DelayingAckHandler handler = new DelayingAckHandler(0);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                long t0 = System.nanoTime();
                Assert.assertTrue(sender.drain(5_000));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                Assert.assertTrue("drain on a fast server must return promptly, took " + elapsedMs + "ms",
                        elapsedMs < 2_000);
            }
        }
    }

    @Test
    public void testAsyncCloseDrainSucceedsWhenServerStartsDuringDrain() throws Exception {
        DelayingAckHandler handler = new DelayingAckHandler(0);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + sfDirOpt()
                    + ";initial_connect_retry=async"
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=20"
                    + ";reconnect_max_backoff_millis=100"
                    + ";close_flush_timeout_millis=5000;";

            Sender sender = Sender.fromConfig(cfg);
            sender.table("foo").longColumn("v", 1L).atNow();
            sender.flush();

            Thread starter = new Thread(() -> {
                try {
                    Thread.sleep(150);
                    server.start();
                } catch (Exception ignored) {
                }
            }, "delayed-server-start");
            starter.start();

            long t0 = System.nanoTime();
            sender.close();
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

            starter.join(5000);
            Assert.assertTrue(server.awaitStart(2, TimeUnit.SECONDS));
            Assert.assertTrue("close() took " + elapsedMs + "ms",
                    elapsedMs < 4500);
        }
    }

    @Test
    public void testAsyncCloseDrainSucceedsWhenServerWasUpAllAlong() throws Exception {
        DelayingAckHandler handler = new DelayingAckHandler(0);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            for (int i = 0; i < 20; i++) {
                String cfg = "ws::addr=localhost:" + port
                        + sfDirOpt()
                        + ";initial_connect_retry=async"
                        + ";reconnect_max_duration_millis=10000"
                        + ";reconnect_initial_backoff_millis=20"
                        + ";reconnect_max_backoff_millis=100"
                        + ";close_flush_timeout_millis=3000;";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    sender.table("foo").longColumn("v", i).atNow();
                    sender.flush();
                    // Time only close(): the 2500ms budget covers close()'s
                    // drain latency. Building the first store-and-forward
                    // sender carries a one-time cold-start cost (class
                    // loading, JIT, sf buffer mmap) that belongs to
                    // construction, not to close().
                    long t0 = System.nanoTime();
                    sender.close();
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                    Assert.assertTrue("iteration " + i + " close() took " + elapsedMs + "ms",
                            elapsedMs < 2500);
                }
            }
        }
    }

    private static String sfDirOpt() {
        String dir = Paths.get(
                System.getProperty("java.io.tmpdir"),
                "qdb-close-drain-" + System.nanoTime()).toString();
        return ";sf_dir=" + dir;
    }

    /** Acks every binary frame after a fixed delay, so we can observe close() blocking. */
    private static class DelayingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong(0);

        DelayingAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                Thread.sleep(delayMs);
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /** Receives but never ACKs — used to verify close() honors its timeout cap. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally drop the frame on the floor
        }
    }

    /**
     * Acks only the first data frame, then goes silent — models a server that
     * acks the commit-bearing frame and withholds acks for the uncommitted
     * deferred tail above it (the FLAG_DEFER_COMMIT ack contract).
     */
    private static class AckFirstFrameOnlyHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong received = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (received.getAndIncrement() == 0) {
                try {
                    client.sendBinary(buildAck(0));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            // frames past the first are deferred: withhold their acks
        }
    }

    // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
    static byte[] buildAck(long seq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(seq);
        bb.putShort((short) 0);
        return buf;
    }
}
