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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.DurableAckTiers;
import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The I/O loop must not poll while idle: with nothing to send and nothing on
 * the wire it blocks in the transport's doorbell-aware wait, and a producer
 * publish (or close()) is what brings it back. The fake transport here blocks
 * for real, so a loop that still polled would either spin the receive counter
 * or hang close().
 */
public class CursorWebSocketSendLoopIdleWaitTest {
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-idle-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testIdleLoopBlocksAndPublishWakesIt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                BlockingDoorbellClient client = new BlockingDoorbellClient();
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        client, engine, 0L,
                        BlockingDoorbellClient::new,
                        100L, 1_000L,
                        DurableAckTiers.NONE,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS);
                try {
                    loop.start();

                    // Idle: a polling loop would rack up thousands of receive
                    // attempts in 500 ms; a blocking one makes a handful at most.
                    Thread.sleep(500);
                    int idleReceives = client.receiveAttempts.get();
                    assertTrue("idle loop polled the socket " + idleReceives + " times", idleReceives <= 5);
                    assertTrue("idle loop never blocked in the transport wait", client.waits.get() >= 1);

                    // Publish: the engine rings the doorbell and the frame goes
                    // out promptly, without any timer involved (the wait has no
                    // timeout -- keepalives are off).
                    long t0 = System.nanoTime();
                    appendFrame(engine);
                    long deadline = t0 + TimeUnit.SECONDS.toNanos(5);
                    while (client.sentFrames.get() < 1 && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    assertEquals("frame was not sent after publish", 1, client.sentFrames.get());
                    long latencyMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertTrue("publish-to-send took " + latencyMillis + " ms", latencyMillis < 500);

                    // Idle again: still no polling.
                    int before = client.receiveAttempts.get();
                    Thread.sleep(300);
                    assertTrue("loop resumed polling after a send", client.receiveAttempts.get() - before <= 5);
                } finally {
                    // close() must ring the doorbell: the thread is blocked
                    // without a timeout, so without the wake this would hang.
                    long t0 = System.nanoTime();
                    loop.close();
                    long closeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertTrue("close() took " + closeMillis + " ms", closeMillis < 2_000);
                }
            }
        });
    }

    /**
     * Regression: a reconnect rewinds the send cursor from inside the I/O
     * thread. That creates sendable work that no producer publish and no
     * inbound byte will ever announce, so the loop must not block on it. Here
     * the first transport's send throws, so the iteration did no work at all;
     * the replay must reach the replacement transport with the producer idle.
     */
    @Test
    public void testReplayAfterReconnectIsNotStrandedByIdleWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                BlockingDoorbellClient first = new BlockingDoorbellClient();
                first.failFirstSend = true;
                BlockingDoorbellClient replacement = new BlockingDoorbellClient();
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        first, engine, 0L,
                        () -> replacement,
                        1L, 10L,
                        DurableAckTiers.NONE,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS);
                try {
                    loop.start();
                    appendFrame(engine);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (replacement.sentFrames.get() < 1 && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    assertEquals("frame must be replayed on the replacement transport", 1, replacement.sentFrames.get());
                    assertEquals("original transport never completed a send", 0, first.sentFrames.get());
                } finally {
                    loop.close();
                }
            }
        });
    }

    /**
     * Same stranding through the PING path: in durable-ack mode the loop PINGs
     * while OKs await durable confirmation (steps 3/4), and a PING that throws
     * reconnects and rewinds the cursor AFTER the iteration consumed
     * recycledThisIteration. Step 5 must not block on that rewound cursor.
     */
    @Test
    public void testReplayAfterPingFailureIsNotStrandedByIdleWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                BlockingDoorbellClient first = new BlockingDoorbellClient();
                first.deliverOkWithPendingTable = true;
                first.failPing = true;
                BlockingDoorbellClient replacement = new BlockingDoorbellClient();
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        first, engine, 0L,
                        () -> replacement,
                        1L, 10L,
                        DurableAckTiers.REPLICATED,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS);
                try {
                    loop.start();
                    appendFrame(engine);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (first.pingAttempts.get() == 0 && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    assertTrue("durable-ack PING never attempted", first.pingAttempts.get() > 0);
                    while (replacement.sentFrames.get() < 1 && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    assertEquals("frame must be replayed on the replacement transport after the PING failure",
                            1, replacement.sentFrames.get());
                } finally {
                    loop.close();
                }
            }
        });
    }

    /**
     * The publish hook rings the doorbell only while the I/O thread is parked
     * (or committing to park). A loop that is busy -- here, inside the
     * transport's receive -- picks the frames up on its next iteration, so a
     * burst of publishes must not cost one wake syscall each.
     */
    @Test
    public void testBusyLoopIsNotWokenPerPublish() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                BlockingDoorbellClient client = new BlockingDoorbellClient();
                // Hold the very first receive: the loop is busy from its first
                // iteration and never reaches the idle wait before the burst.
                CountDownLatch gate = new CountDownLatch(1);
                client.receiveGate = gate;
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        client, engine, 0L,
                        BlockingDoorbellClient::new,
                        100L, 1_000L,
                        DurableAckTiers.NONE,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS);
                try {
                    loop.start();
                    assertTrue("loop never entered the held receive", client.receiveHeld.await(5, TimeUnit.SECONDS));
                    int wakesBefore = client.wakes.get();
                    final int frames = 200;
                    for (int i = 0; i < frames; i++) {
                        appendFrame(engine);
                    }
                    int wakes = client.wakes.get() - wakesBefore;
                    assertEquals("busy loop was rung " + wakes + " times for " + frames + " publishes", 0, wakes);
                    gate.countDown();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (client.sentFrames.get() < frames && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    assertEquals("frames published to a busy loop were not all sent", frames, client.sentFrames.get());
                } finally {
                    gate.countDown();
                    loop.close();
                }
            }
        });
    }

    /**
     * The idle decision re-checks the engine's publish stamp after committing
     * to park: a publish that lands after trySendOne found nothing but before
     * ioParked is set rings no doorbell, so without the re-check the loop
     * would block on an untimed wait with the frame stranded behind it. The
     * transport's receive, which runs inside that window, is gated so the
     * publish lands exactly there.
     */
    @Test
    public void testPublishInsideIdleDecisionWindowIsNotStranded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                BlockingDoorbellClient client = new BlockingDoorbellClient();
                CountDownLatch gate = new CountDownLatch(1);
                client.receiveGate = gate;
                client.gateInIdleWindow = true;
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        client, engine, 0L,
                        BlockingDoorbellClient::new,
                        100L, 1_000L,
                        DurableAckTiers.NONE,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS);
                try {
                    loop.start();
                    appendFrame(engine);
                    assertTrue("loop never reached the idle-decision window",
                            client.receiveHeld.await(5, TimeUnit.SECONDS));
                    assertEquals(1, client.sentFrames.get());
                    // The loop is between trySendOne (found nothing) and the
                    // park decision: this publish rings no doorbell.
                    int wakesBefore = client.wakes.get();
                    appendFrame(engine);
                    assertEquals("publish to a non-parked loop must not ring", 0, client.wakes.get() - wakesBefore);
                    gate.countDown();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (client.sentFrames.get() < 2 && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    assertEquals("frame published inside the idle-decision window was stranded",
                            2, client.sentFrames.get());
                } finally {
                    gate.countDown();
                    loop.close();
                }
            }
        });
    }

    private static void appendFrame(CursorSendEngine engine) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, payload[i]);
            }
            engine.appendBlocking(buf, 16);
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * A transport whose idle wait really blocks (no bounded park), like the
     * real one, with a stateful doorbell.
     */
    private static final class BlockingDoorbellClient extends WebSocketClient {
        final AtomicInteger receiveAttempts = new AtomicInteger();
        final AtomicInteger sentFrames = new AtomicInteger();
        final AtomicInteger waits = new AtomicInteger();
        final AtomicInteger pingAttempts = new AtomicInteger();
        final AtomicInteger wakes = new AtomicInteger();
        // Counted down once the receive below is held at receiveGate.
        final CountDownLatch receiveHeld = new CountDownLatch(1);
        volatile boolean failFirstSend;
        volatile boolean failPing;
        // Answer the first sent frame with a STATUS_OK naming one table, so a
        // durable-ack loop has a pending entry and starts PINGing.
        volatile boolean deliverOkWithPendingTable;
        // One-shot: the receive that takes the gate blocks on it (5 s cap).
        // Plain: the very first receive. gateInIdleWindow: the second
        // receive after the first send, i.e. the one that runs after
        // trySendOne found nothing and before the park decision.
        volatile CountDownLatch receiveGate;
        volatile boolean gateInIdleWindow;
        private boolean okDelivered;
        private int receivesSinceSend;
        private final Semaphore doorbell = new Semaphore(0);

        BlockingDoorbellClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public boolean awaitReadableOrWake(int timeoutMillis) {
            waits.incrementAndGet();
            try {
                if (timeoutMillis < 0) {
                    doorbell.acquire();
                    doorbell.drainPermits();
                    return true;
                }
                boolean woken = doorbell.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
                doorbell.drainPermits();
                return woken;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public void wake() {
            wakes.incrementAndGet();
            doorbell.release();
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            if (failFirstSend) {
                failFirstSend = false;
                throw new HttpClientException("peer disconnect [errno=104]");
            }
            sentFrames.incrementAndGet();
            receivesSinceSend = 0;
        }

        @Override
        public void sendPing() {
            pingAttempts.incrementAndGet();
            if (failPing) {
                throw new HttpClientException("timed out [errno=11]");
            }
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            receiveAttempts.incrementAndGet();
            receivesSinceSend++;
            CountDownLatch gate = receiveGate;
            if (gate != null && (!gateInIdleWindow || (sentFrames.get() == 1 && receivesSinceSend == 2))) {
                receiveGate = null;
                receiveHeld.countDown();
                try {
                    gate.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (deliverOkWithPendingTable && !okDelivered && sentFrames.get() > 0) {
                okDelivered = true;
                // STATUS_OK | sequence 0 | tableCount 1 | nameLen 1 't' seqTxn 1
                int len = 1 + 8 + 2 + 2 + 1 + 8;
                long p = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putByte(p, (byte) 0);
                    Unsafe.getUnsafe().putLong(p + 1, 0L);
                    Unsafe.getUnsafe().putShort(p + 9, (short) 1);
                    Unsafe.getUnsafe().putShort(p + 11, (short) 1);
                    Unsafe.getUnsafe().putByte(p + 13, (byte) 't');
                    Unsafe.getUnsafe().putLong(p + 14, 1L);
                    handler.onBinaryMessage(p, len);
                } finally {
                    Unsafe.free(p, len, MemoryTag.NATIVE_DEFAULT);
                }
                return true;
            }
            return false;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
