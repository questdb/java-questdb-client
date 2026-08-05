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
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test(timeout = 30_000L)
    public void testCloseBlocksAcrossAllReplicaWindowUntilPromotion() throws Exception {
        DelayingAckHandler handler = new DelayingAckHandler(0);
        try (TestWebSocketServer server = new TestWebSocketServer(handler, false, "PRIMARY")) {
            server.setRejectWithRole("REPLICA");
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + server.getPort()
                    + ";initial_connect_retry=async"
                    + ";reconnect_initial_backoff_millis=20"
                    + ";reconnect_max_backoff_millis=100"
                    + ";close_flush_timeout_millis=10000;";
            QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg);
            CountDownLatch closeDrainWaiting = new CountDownLatch(1);
            CountDownLatch releaseCloseDrain = new CountDownLatch(1);
            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            AtomicReference<Throwable> hookFailure = new AtomicReference<>();
            sender.setCloseDrainWaitingHook(() -> {
                closeDrainWaiting.countDown();
                try {
                    if (!releaseCloseDrain.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("promotion did not release the close-drain witness");
                    }
                } catch (Throwable t) {
                    hookFailure.set(t);
                }
            });
            sender.table("foo").longColumn("v", 1L).atNow();
            sender.flush();

            Thread closer = new Thread(() -> {
                try {
                    sender.close();
                } catch (Throwable t) {
                    closeFailure.set(t);
                }
            }, "all-replica-close-drain");
            try {
                closer.start();
                Assert.assertTrue("server never produced the all-replica role rejection",
                        server.awaitRoleReject(5, TimeUnit.SECONDS));
                Assert.assertTrue("close never observed its real unacknowledged drain target",
                        closeDrainWaiting.await(5, TimeUnit.SECONDS));

                Assert.assertTrue("pre-promotion witness must include a role rejection",
                        server.roleRejectCount() >= 1);
                Assert.assertEquals("pre-promotion data must remain unacknowledged",
                        -1L, sender.getAckedFsn());
                Assert.assertTrue("close must remain pending for the whole all-replica window",
                        closer.isAlive());
                Assert.assertEquals("promotion must not have delivered or replayed the frame yet",
                        0L, handler.nextSeq.get());

                // The close-drain barrier has held continuously since it observed
                // targetFsn > ackedFsn. Promote first, then release the barrier:
                // completion therefore cannot precede the deterministic recovery event.
                server.setAdvertisedRole("PRIMARY");
                server.setRejectWithRole(null);
                releaseCloseDrain.countDown();
                closer.join(10_000L);

                Assert.assertFalse("close did not complete after promotion", closer.isAlive());
                Assert.assertNull("close-drain witness failed", hookFailure.get());
                Assert.assertNull("close failed after promotion", closeFailure.get());
                Assert.assertEquals("the unacknowledged frame must be delivered exactly once",
                        1L, handler.nextSeq.get());
            } finally {
                server.setAdvertisedRole("PRIMARY");
                server.setRejectWithRole(null);
                releaseCloseDrain.countDown();
                closer.join(10_000L);
                sender.close();
            }
        }
    }

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
    public void testCloseStartedHookRunsAfterClosedStateTransition() throws Exception {
        QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 1);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> closerFailure = new AtomicReference<>();
        AtomicReference<Throwable> hookFailure = new AtomicReference<>();
        sender.setCloseStartedHook(() -> {
            try {
                try {
                    sender.table("must_reject_after_close_started");
                    Assert.fail("close-started hook ran before closed=true was published");
                } catch (LineSenderException expected) {
                    // Required lifecycle boundary: public operations already reject.
                }
                entered.countDown();
                if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("close-started hook release timed out");
                }
            } catch (Throwable t) {
                hookFailure.set(t);
                entered.countDown();
            }
        });

        Assert.assertEquals("installing the hook must not emit a pre-invocation witness",
                1L, entered.getCount());
        Thread closer = new Thread(() -> {
            try {
                sender.close();
            } catch (Throwable t) {
                closerFailure.set(t);
            }
        }, "close-started-hook-test");
        try {
            closer.start();
            Assert.assertTrue("close() never reached its internal lifecycle witness",
                    entered.await(10, TimeUnit.SECONDS));
            Assert.assertTrue("close() completed while its internal witness was held",
                    closer.isAlive());
        } finally {
            release.countDown();
            closer.join(10_000L);
            sender.close();
        }
        Assert.assertFalse("close thread did not finish", closer.isAlive());
        Assert.assertNull("close-started hook failed", hookFailure.get());
        Assert.assertNull("close() failed", closerFailure.get());
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
                    String msg = e.getMessage();
                    Assert.assertTrue("expected drain-timeout message, got: " + msg,
                            msg.contains("drain timed out"));
                    Assert.assertFalse("no outage may be named when the wire never dropped, got: " + msg,
                            msg.contains("the wire is not draining:"));
                    Assert.assertTrue("expected the generic guidance tail, got: " + msg,
                            msg.contains("data may be lost (use larger closeFlushTimeoutMillis or smaller batches)"));
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
    public void testCloseDrainTimeoutNamesTheReconnectOutage() throws Exception {
        // The drain-timeout message exists to name the outage the I/O thread is
        // riding out: a revoked token must read as an auth failure, not as a
        // timeout-tuning problem. The server accepts the first connection, drops
        // it on the first frame without acking, and 401s every reconnect.
        long timeoutMs = 1500;
        try (TestWebSocketServer server = new TestWebSocketServer(new ReceiveThenDropHandler())) {
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port
                    + ";initial_connect_retry=sync"
                    + ";reconnect_initial_backoff_millis=20"
                    + ";reconnect_max_backoff_millis=100"
                    + ";close_flush_timeout_millis=" + timeoutMs + ";";
            QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg);
            boolean closeAttempted = false;
            try {
                // The sender is already connected; from here every NEW handshake
                // is rejected with 401.
                server.setRejectWithStatus(401, "Unauthorized");
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                // The handler drops the connection on that frame. Wait until the
                // reconnect loop has been through at least one full failed attempt
                // so lastReconnectError holds the 401 before close() gives up.
                long deadline = System.currentTimeMillis() + 5_000;
                while (sender.getTotalReconnectAttempts() < 2
                        && System.currentTimeMillis() < deadline) {
                    Thread.sleep(20);
                }
                Assert.assertTrue("the reconnect loop must have retried against the 401",
                        sender.getTotalReconnectAttempts() >= 2);
                try {
                    closeAttempted = true;
                    sender.close();
                    Assert.fail("close() should have thrown a drain-timeout error");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue("expected drain-timeout prefix, got: " + msg,
                            msg.contains("drain timed out"));
                    Assert.assertTrue("expected the outage to be named, got: " + msg,
                            msg.contains("the wire is not draining:"));
                    Assert.assertTrue("expected the 401 to be named, got: " + msg,
                            msg.contains("WebSocket upgrade rejected with HTTP 401"));
                }
            } finally {
                if (!closeAttempted) {
                    sender.close();
                }
            }
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

    /**
     * A batch the server cap can never fit is RETAINED for retry, so on close()
     * {@code flushPendingRows} throws {@code BatchTooLargeForCapException}. close() must
     * discard that batch and keep going -- commit, seal and DRAIN what an earlier
     * successful flush already published -- before rethrowTerminal surfaces the retained
     * batch's error. Letting the throw escape instead skips all three and abandons the
     * earlier rows.
     * <p>
     * The e2e sibling {@code QwpSenderOversizeRowInBatchTest} asserts a real server's row
     * count, which pins the COMMIT half: let the exception escape and those rows never
     * reach a transaction at all. It cannot pin the DRAIN half. Over localhost the earlier
     * rows are normally acked before close() is even entered, so
     * {@code drainOnClose} returns at its {@code ackedFsn >= target} early-out and removing
     * the drain changes nothing there -- verified by mutation: skipping only
     * {@code drainOnClose} leaves that test green.
     * <p>
     * Here the handler withholds every ack until the close-drain witness releases it, so
     * the earlier row is PROVABLY unacknowledged when close() reaches the drain. The
     * witness runs only past that early-out, so observing it fire is proof the drain had
     * real work to do -- and releasing the acks from inside it is what lets close()
     * finish, making the assertion impossible to satisfy without the drain.
     */
    @Test(timeout = 30_000L)
    public void testCloseDrainsEarlierRowsBeforeSurfacingRetainedBatchError() throws Exception {
        GatedAckHandler handler = new GatedAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler, false, "PRIMARY")) {
            // Small enough that a handful of modest rows cannot fit, while every
            // individual row stays far below it (so the per-row guard cannot fire).
            server.setAdvertisedMaxBatchSize(4096);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + server.getPort()
                    + ";auto_flush_rows=10000"
                    + ";auto_flush_bytes=off"
                    + ";auto_flush_interval=60000"
                    + ";close_flush_timeout_millis=10000;";
            QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg);

            CountDownLatch drainWaiting = new CountDownLatch(1);
            sender.setCloseDrainWaitingHook(() -> {
                drainWaiting.countDown();
                handler.releaseAcks();
            });

            // The earlier row: flushed successfully, then held unacked by the handler.
            sender.table("earlier").longColumn("v", 1L).atNow();
            sender.flush();

            // 8 x ~1 KB into ONE table: no split can fit this under the 4 KB cap, and no
            // single row comes close to it.
            char[] chunkChars = new char[1024];
            Arrays.fill(chunkChars, 'x');
            String chunk = new String(chunkChars);
            LineSenderException flushThrown = null;
            try {
                for (int i = 0; i < 8; i++) {
                    sender.table("oversize").stringColumn("payload", chunk).atNow();
                }
                sender.flush();
            } catch (LineSenderException e) {
                flushThrown = e;
            }
            Assert.assertNotNull("flush() must refuse a batch no split can fit under the cap",
                    flushThrown);
            Assert.assertTrue("expected the batch-cap rejection, got: " + flushThrown.getMessage(),
                    flushThrown.getMessage().contains("batch too large for server batch cap"));

            Assert.assertEquals("the earlier row must still be unacknowledged when close() starts,"
                            + " or the drain has nothing to wait for and this test proves nothing",
                    -1L, sender.getAckedFsn());

            LineSenderException closeThrown = null;
            try {
                sender.close();
            } catch (LineSenderException e) {
                closeThrown = e;
            }

            Assert.assertEquals("close() must reach the bounded drain with the earlier row still"
                            + " unacknowledged; the witness fires only past drainOnClose's"
                            + " ackedFsn >= target early-out, so a skipped drain leaves it unfired",
                    0L, drainWaiting.getCount());
            Assert.assertNotNull("close() must still surface the retained batch's error after"
                    + " committing and draining the earlier row", closeThrown);
            Assert.assertTrue("expected the batch-cap rejection from close(), got: "
                            + closeThrown.getMessage(),
                    closeThrown.getMessage().contains("batch too large for server batch cap"));
            // close() surfaced the batch-cap error rather than a "drain timed out" one, so
            // the drain ran to completion; the handler's counter is the server-side witness
            // that the earlier row's frame really was carried across and acked.
            Assert.assertTrue("the drain must have carried the earlier row's frame to the server"
                            + " and taken its ack [acksSent=" + handler.nextSeq.get() + ']',
                    handler.nextSeq.get() >= 1L);
        }
    }

    private static String sfDirOpt() {
        String dir = Paths.get(
                System.getProperty("java.io.tmpdir"),
                "qdb-close-drain-" + System.nanoTime()).toString();
        return ";sf_dir=" + dir;
    }

    /**
     * Receives frames but withholds every ack until {@link #releaseAcks()} is called, so a
     * close-time drain provably has an unacknowledged target to wait on.
     */
    private static class GatedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                if (!released.await(20, TimeUnit.SECONDS)) {
                    throw new AssertionError("close-drain witness never released the ack gate");
                }
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void releaseAcks() {
            released.countDown();
        }
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

    /**
     * Receives the first frame, closes the connection without acking it, and
     * ignores everything after -- the drop forces the sender into its reconnect
     * loop with data still undrained.
     */
    private static class ReceiveThenDropHandler implements TestWebSocketServer.WebSocketServerHandler {
        private boolean dropped;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (!dropped) {
                dropped = true;
                client.close();
            }
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
