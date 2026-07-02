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
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Mid-drain durable-ack capability-gap coverage for {@link BackgroundDrainer}.
 * <p>
 * The initial-connect path ({@code connectWithDurableAckRetry}) gives a
 * cluster-wide durable-ack capability gap a bounded settle budget (16
 * consecutive sweeps / wall clock) before quarantining the slot — the budget
 * exists precisely for rolling-upgrade transients. The same condition hit
 * <i>mid-drain</i> (wire drops, the loop's reconnect sweep lands on a node
 * that upgrades but does not advertise durable ack) must get the same budget:
 * the drainer re-enters the budgeted connect instead of dropping a
 * {@code .failed} sentinel on the first sweep. Genuine terminals (auth,
 * non-421 upgrade reject) still quarantine immediately — the sanctioned
 * terminal set is unchanged.
 * <p>
 * Wire realism: a real {@link TestWebSocketServer} acks over a live socket;
 * the scripted {@link CursorWebSocketSendLoop.ReconnectFactory} decides,
 * per connect attempt, whether the sweep sees a healthy node or the
 * capability gap. The mid-drain drop is deterministic — the server closes
 * the first connection after durably acking exactly one frame.
 */
public class BackgroundDrainerMidDrainCapabilityGapTest {

    private static final long FAST_BACKOFF_MAX_MILLIS = 4L;
    private static final long FAST_BACKOFF_MILLIS = 1L;
    private static final long RECONNECT_MAX_DURATION_MILLIS = 60_000L;
    private static final int SEEDED_FRAMES = 5;
    private static final long SEGMENT_SIZE_BYTES = 16384L;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 20;

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mid-drain-gap-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        rmDirRec(slotPath);
    }

    @Test
    public void testMidDrainCapabilityGapGetsSettleBudgetNotQuarantine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot(SEEDED_FRAMES);
            GapScenarioHandler handler = new GapScenarioHandler(/* dropFirstConnection */ true);
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Call 1: healthy connect (drain starts). The server durably
                // acks one frame, then drops the wire. Calls 2-4: the
                // reconnect sweep finds only the capability-gap node. Call 5+:
                // the rolling upgrade settled; a capable node is back.
                ScriptedWireFactory factory =
                        new ScriptedWireFactory(server.getPort(), 2, 4);
                BackgroundDrainer drainer = newDrainer(factory);
                CountingListener listener = new CountingListener();
                drainer.setListener(listener);

                runToCompletion(drainer);

                assertEquals("a transient capability gap inside the settle budget "
                                + "must not quarantine the slot",
                        BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                assertFalse("no .failed sentinel after a successful drain",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                // 1 healthy + 3 gap sweeps + 1 healthy at minimum. Stopping at
                // 2 means the drainer latched terminal on the first gap sweep.
                assertTrue("expected the drainer to retry through the gap, attempts="
                        + factory.attempts(), factory.attempts() >= 5);
                // The loop's own failed sweep (call 2) latches the loop; budget
                // attempts 1 and 2 (calls 3, 4) fire the observability callback.
                assertEquals(Arrays.asList(1, 2), listener.unavailableAttempts);
                assertEquals(0, listener.persistentFailures.get());
            }
        });
    }

    @Test
    public void testMidDrainPersistentCapabilityGapExhaustsBudgetThenQuarantines() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot(SEEDED_FRAMES);
            GapScenarioHandler handler = new GapScenarioHandler(/* dropFirstConnection */ true);
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Gap never clears: every sweep after the drop throws.
                ScriptedWireFactory factory =
                        new ScriptedWireFactory(server.getPort(), 2, Integer.MAX_VALUE);
                BackgroundDrainer drainer = newDrainer(factory);
                CountingListener listener = new CountingListener();
                drainer.setListener(listener);

                runToCompletion(drainer);

                int budget = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
                assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
                assertTrue("persistent gap must quarantine after the budget",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                // Escalation goes through the settle budget, not the generic
                // wire-error path: the persistent-failure callback fires once
                // with the full budget consumed.
                assertEquals(1, listener.persistentFailures.get());
                assertEquals(budget, listener.lastPersistentTotalAttempts.get());
                // 1 healthy connect + 1 loop reconnect sweep (latches the loop)
                // + the full budget of re-entered sweeps.
                assertEquals(2 + budget, factory.attempts());
            }
        });
    }

    @Test
    public void testMidDrainTerminalUpgradeErrorStillQuarantinesImmediately() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot(SEEDED_FRAMES);
            GapScenarioHandler handler = new GapScenarioHandler(/* dropFirstConnection */ true);
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Non-421 upgrade reject mid-drain: sanctioned terminal, no
                // settle budget — the drainer must quarantine on the first
                // sweep exactly as before.
                ScriptedWireFactory factory = new ScriptedWireFactory(
                        server.getPort(), 2, Integer.MAX_VALUE,
                        () -> new WebSocketUpgradeException(500, null, "server error during upgrade"));
                BackgroundDrainer drainer = newDrainer(factory);
                CountingListener listener = new CountingListener();
                drainer.setListener(listener);

                runToCompletion(drainer);

                assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
                assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertEquals("terminal upgrade error must not consume gap sweeps",
                        2, factory.attempts());
                assertEquals(0, listener.unavailableAttempts.size());
                assertEquals(0, listener.persistentFailures.get());
            }
        });
    }

    private BackgroundDrainer newDrainer(ScriptedWireFactory factory) {
        return new BackgroundDrainer(
                slotPath,
                SEGMENT_SIZE_BYTES,
                SF_MAX_TOTAL_BYTES,
                factory,
                RECONNECT_MAX_DURATION_MILLIS,
                FAST_BACKOFF_MILLIS,
                FAST_BACKOFF_MAX_MILLIS,
                /* requestDurableAck */ true,
                /* durableAckKeepaliveIntervalMillis */ 200L);
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

    private static void runToCompletion(BackgroundDrainer drainer) throws InterruptedException {
        Thread t = new Thread(drainer, "test-mid-drain-drainer");
        t.setDaemon(true);
        t.start();
        t.join(20_000);
        if (t.isAlive()) {
            drainer.requestStop();
            t.join(5_000);
            fail("drainer did not finish within 20s (outcome=" + drainer.outcome() + ")");
        }
    }

    private void seedSlot(int frames) {
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
        }
    }

    /**
     * Records listener invocations for exact-count assertions.
     */
    private static final class CountingListener implements BackgroundDrainerListener {
        final AtomicInteger lastPersistentTotalAttempts = new AtomicInteger(-1);
        final AtomicInteger persistentFailures = new AtomicInteger();
        final List<Integer> unavailableAttempts = new ArrayList<>();

        @Override
        public synchronized void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
            persistentFailures.incrementAndGet();
            lastPersistentTotalAttempts.set(totalAttempts);
        }

        @Override
        public synchronized void onDurableAckUnavailable(String slotPath, int attemptNumber) {
            unavailableAttempts.add(attemptNumber);
        }
    }

    /**
     * Server-side script. Connection #1 durably acks exactly one frame, then
     * closes the socket — a deterministic mid-drain wire drop. Every later
     * connection acks all traffic (OK + durable-ack per frame, per-connection
     * wire sequence), so a reconnected loop drains to completion.
     * <p>
     * State is keyed per {@code ClientHandler} identity. A dead connection's
     * reader can still deliver late buffered frames AFTER a newer connection
     * started (the server reads ahead of the socket close), so any
     * "latest-connection" flip-flop bookkeeping desyncs the per-connection
     * wire sequence and produces phantom connections. Acks are best-effort:
     * a late frame from a dead connection must neither ack with a stale
     * counter nor kill the reader thread of a live one.
     */
    private static final class GapScenarioHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE = "trades";
        private final boolean dropFirstConnection;
        private final List<TestWebSocketServer.ClientHandler> arrivalOrder = new ArrayList<>();
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

        GapScenarioHandler(boolean dropFirstConnection) {
            this.dropFirstConnection = dropFirstConnection;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long[] counter = wireSeqByConn.get(client);
            if (counter == null) {
                counter = new long[1];
                wireSeqByConn.put(client, counter);
                arrivalOrder.add(client);
            }
            int connectionIndex = arrivalOrder.indexOf(client) + 1;
            long seq = counter[0]++;
            try {
                if (dropFirstConnection && connectionIndex == 1) {
                    if (seq == 0) {
                        client.sendBinary(okFrame(seq, seq));
                        client.sendBinary(durableAckFrame(seq));
                    } else if (seq == 1) {
                        client.close(); // mid-drain wire drop
                    }
                    // seq > 1: late buffered frames from the condemned
                    // connection; ignore.
                } else {
                    client.sendBinary(okFrame(seq, seq));
                    client.sendBinary(durableAckFrame(seq));
                }
            } catch (IOException ignored) {
                // Best-effort ack: the connection died under us (e.g. racing
                // its own close). The client replays on its next connection.
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

    /**
     * Per-call-index scripted factory over a real wire. Call indexes inside
     * {@code [throwFrom, throwTo]} (1-based, inclusive) throw the scripted
     * exception; every other call returns a live upgraded client against the
     * test server, with durable ack requested — exactly the client the
     * production connect walk would hand back.
     */
    private static final class ScriptedWireFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger calls = new AtomicInteger();
        private final int port;
        private final ThrowableSupplier throwSupplier;
        private final int throwFrom;
        private final int throwTo;

        ScriptedWireFactory(int port, int throwFrom, int throwTo) {
            this(port, throwFrom, throwTo,
                    () -> new QwpDurableAckMismatchException("localhost", port, "primary"));
        }

        ScriptedWireFactory(int port, int throwFrom, int throwTo, ThrowableSupplier throwSupplier) {
            this.port = port;
            this.throwFrom = throwFrom;
            this.throwTo = throwTo;
            this.throwSupplier = throwSupplier;
        }

        int attempts() {
            return calls.get();
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            int n = calls.incrementAndGet();
            if (n >= throwFrom && n <= throwTo) {
                Throwable t = throwSupplier.get();
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                if (t instanceof Exception) throw (Exception) t;
                throw new RuntimeException(t);
            }
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

    @FunctionalInterface
    private interface ThrowableSupplier {
        Throwable get();
    }
}
