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

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.client.TestPorts;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Down-then-up transport-outage recovery for {@link BackgroundDrainer},
 * end-to-end over a real wire (M8 conjunction gap).
 * <p>
 * Invariant B's two halves were previously pinned only in isolation:
 * "a transport outage longer than the settle budget never quarantines"
 * (ScriptedFactory unit level, ends with {@code requestStop()}) and
 * "the drainer recovers once errors clear" (scripted throws, no real
 * outage). This test conjoins them on ONE endpoint: the server is DOWN at
 * drainer start (every connect is a genuine ECONNREFUSED through the real
 * {@link WebSocketClient} connect/upgrade path), stays down for several
 * multiples of {@code reconnect_max_duration_millis} while the drainer
 * sweeps, then comes back UP on the SAME port — and the drainer must
 * complete the drain, having never dropped a {@code .failed} sentinel or
 * fired a persistent-failure escalation during the outage.
 */
public class BackgroundDrainerTransportOutageRecoveryTest {

    private static final long FAST_BACKOFF_MAX_MILLIS = 4L;
    private static final long FAST_BACKOFF_MILLIS = 1L;
    /** Deliberately tiny: the outage below outlives it several times over. */
    private static final long RECONNECT_MAX_DURATION_MILLIS = 200L;
    private static final int SEEDED_FRAMES = 5;
    private static final long SEGMENT_SIZE_BYTES = 16384L;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 20;

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-outage-recovery-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        rmDirRec(slotPath);
    }

    @Test
    public void testDrainerSurvivesOutageLongerThanBudgetThenDrainsWhenServerReturns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long targetFsn = seedSlot(SEEDED_FRAMES);
            int port = TestPorts.findUnusedPort();
            WireFactory factory = new WireFactory(port);
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_SIZE_BYTES,
                    SF_MAX_TOTAL_BYTES,
                    factory,
                    RECONNECT_MAX_DURATION_MILLIS,
                    FAST_BACKOFF_MILLIS,
                    FAST_BACKOFF_MAX_MILLIS,
                    /* requestDurableAck */ true,
                    /* durableAckKeepaliveIntervalMillis */ 200L);
            CountingListener listener = new CountingListener();
            drainer.setListener(listener);

            Thread t = new Thread(drainer, "outage-recovery-drainer");
            t.setDaemon(true);
            t.start();
            try {
                // OUTAGE PHASE: nothing listens on the port, so every sweep is a
                // real refused connect. Hold the outage for 3x the wall-clock
                // budget AND at least a handful of sweeps, whichever is later --
                // under an Invariant B breach (transport errors charged to the
                // budget / attempt cap) the drainer escalates well within this
                // window and the thread dies.
                long outageUntilNanos = System.nanoTime()
                        + 3 * RECONNECT_MAX_DURATION_MILLIS * 1_000_000L;
                while ((System.nanoTime() < outageUntilNanos || factory.attempts() < 8)
                        && t.isAlive()) {
                    Thread.sleep(10);
                }
                assertTrue("drainer gave up during a transport outage (attempts="
                                + factory.attempts() + ", outcome=" + drainer.outcome()
                                + "): Invariant B says a down server is transient -- the "
                                + "drainer must still be retrying 3x past the settle budget",
                        t.isAlive());
                assertEquals("outage must not escalate past PENDING",
                        BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
                assertEquals("outage must not fire a persistent-failure escalation",
                        0, listener.persistentFailures.get());
                assertFalse("outage must not quarantine (.failed sentinel) the slot",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

                // RECOVERY PHASE: the server comes back on the SAME port. The
                // drainer's next sweep connects for real and ships the slot.
                try (TestWebSocketServer server = new TestWebSocketServer(
                        new AckAllHandler(), true, null, port)) {
                    server.start();
                    assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                    t.join(20_000);
                    if (t.isAlive()) {
                        drainer.requestStop();
                        t.join(5_000);
                        fail("drainer did not drain within 20s of the server returning "
                                + "(outcome=" + drainer.outcome()
                                + ", attempts=" + factory.attempts()
                                + ", lastError=" + drainer.getLastErrorMessage() + ")");
                    }
                }
            } finally {
                drainer.requestStop();
                t.join(5_000);
            }

            assertEquals("server recovery must complete the drain",
                    BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
            assertEquals("every seeded frame must be durably acked",
                    targetFsn, drainer.getAckedFsn());
            assertEquals(0, listener.persistentFailures.get());
            assertFalse("no .failed sentinel after a successful drain",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    private static void rmDirRec(String dir) {
        if (dir == null || !Files.exists(dir)) return;
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

    /** Seeds {@code frames} frames and returns the slot's published fsn --
     * the drain target the drainer must ack up to. */
    private long seedSlot(int frames) {
        try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_SIZE_BYTES)) {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, payload[i]);
                }
                for (int i = 0; i < frames; i++) {
                    engine.appendBlocking(buf, 16);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
            return engine.publishedFsn();
        }
    }

    /**
     * Acks every frame (OK + durable-ack, per-connection wire sequence) so a
     * reconnected drainer drains to completion. Trimmed-down clone of the
     * mid-drain test's healthy-server behaviour.
     */
    private static final class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE = "trades";
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long[] counter = wireSeqByConn.get(client);
            if (counter == null) {
                counter = new long[1];
                wireSeqByConn.put(client, counter);
            }
            long seq = counter[0]++;
            try {
                client.sendBinary(okFrame(seq, seq));
                client.sendBinary(durableAckFrame(seq));
            } catch (IOException ignored) {
                // Best-effort ack: the connection died under us; the client
                // replays on its next connection.
            }
        }

        private static byte[] durableAckFrame(long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x02); // STATUS_DURABLE_ACK
            bb.putShort((short) 1); // tableCount
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seqTxn);
            return bb.array();
        }

        private static byte[] okFrame(long wireSeq, long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(wireSeq);
            bb.putShort((short) 1); // tableCount
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seqTxn);
            return bb.array();
        }
    }

    /** Records persistent-failure escalations; the outage must produce none. */
    private static final class CountingListener implements BackgroundDrainerListener {
        final AtomicInteger persistentFailures = new AtomicInteger();

        @Override
        public void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
            persistentFailures.incrementAndGet();
        }

        @Override
        public void onDurableAckUnavailable(String slotPath, int attemptNumber) {
            // transport errors never fire this; nothing to record
        }
    }

    /**
     * Real-wire connect factory: every call performs a genuine TCP connect +
     * WebSocket upgrade against the fixed loopback port -- refused while the
     * server is down, a live upgraded client once it is up. Exactly the client
     * the production connect walk would hand back.
     */
    private static final class WireFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger calls = new AtomicInteger();
        private final int port;

        WireFactory(int port) {
            this.port = port;
        }

        int attempts() {
            return calls.get();
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            calls.incrementAndGet();
            WebSocketClient c = WebSocketClientFactory.newPlainTextInstance();
            try {
                c.setQwpMaxVersion(1);
                c.setQwpRequestDurableAck(true);
                c.setConnectTimeout(5_000);
                c.connect("localhost", port);
                c.upgrade("/write/v4", 5_000, null);
            } catch (Throwable t) {
                c.close();
                throw t;
            }
            return c;
        }
    }
}
