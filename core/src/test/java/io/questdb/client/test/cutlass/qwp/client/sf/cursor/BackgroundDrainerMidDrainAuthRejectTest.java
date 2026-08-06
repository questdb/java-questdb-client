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

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Mid-drain rotating-credential 401/403 coverage for {@link BackgroundDrainer}.
 * <p>
 * {@code connectWithDurableAckRetry} gives an ORPHAN drainer whose credential
 * rotates ({@code hasDynamicCredential()}) a bounded ride-out requiring both
 * an attempt threshold ({@link BackgroundDrainer#DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS})
 * and a wall-clock dwell floor before quarantining on a 401 — the header is
 * re-derived from the token provider every attempt, so a rejection can be a self-healing window (a
 * revocation landing mid-flight, the IdP rotating signing keys, clock skew) a
 * freshly pulled token clears. The same rejection hit <i>mid-drain</i> (the
 * wire drops, the loop's reconnect sweep is refused) must get the same ride-out
 * rather than dropping a {@code .failed} sentinel on the first sweep — otherwise
 * a token rotation during an in-progress drain permanently abandons replayable
 * data on a fault that heals in seconds. The initial-connect ride-out
 * ({@link BackgroundDrainerDurableAckRetryTest}) never exercised the mid-drain
 * reconnect, which the ORPHAN {@link CursorWebSocketSendLoop} handles.
 * <p>
 * A CONSTANT credential still quarantines on the first mid-drain 401: it is
 * uniformly rejected across the cluster and will not heal. The sanctioned
 * terminal set is otherwise unchanged.
 * <p>
 * Wire realism: a real {@link TestWebSocketServer} durably acks over a live
 * socket; the scripted {@link CursorWebSocketSendLoop.ReconnectFactory} decides,
 * per connect attempt, whether the sweep reaches a healthy node or is refused
 * with a 401. The mid-drain drop is deterministic — the server closes the first
 * connection after durably acking exactly one frame.
 */
public class BackgroundDrainerMidDrainAuthRejectTest {

    private static final long FAST_BACKOFF_MAX_MILLIS = 4L;
    private static final long FAST_BACKOFF_MILLIS = 1L;
    private static final long RECONNECT_MAX_DURATION_MILLIS = 25L;
    private static final int SEEDED_FRAMES = 5;
    private static final long SEGMENT_SIZE_BYTES = 16384L;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 20;

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mid-drain-auth-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        rmDirRec(slotPath);
    }

    @Test
    public void testMidDrainConstantCredential401QuarantinesImmediately() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot(SEEDED_FRAMES);
            DropFirstHandler handler = new DropFirstHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Same mid-drain 401, but the credential is CONSTANT: no ride-out,
                // it must quarantine on the first sweep exactly as before the fix.
                ScriptedWireFactory factory = new ScriptedWireFactory(
                        server.getPort(), 2, Integer.MAX_VALUE, /* dynamicCredential */ false);
                BackgroundDrainer drainer = newDrainer(factory);
                List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
                drainer.setErrorSink(captured::add);

                runToCompletion(drainer);

                assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
                assertTrue("a constant-credential 401 must quarantine the slot",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertEquals("a constant credential must not consume the rotating-401 ride-out",
                        2, factory.attempts());
                assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
                assertEquals(SenderError.Category.DATA_LOSS, captured.get(0).getCategory());
            }
        });
    }

    @Test
    public void testMidDrainPersistentRotating401ExhaustsRideOutThenQuarantines() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot(SEEDED_FRAMES);
            DropFirstHandler handler = new DropFirstHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // The rotation never heals: every sweep after the drop is a 401.
                ScriptedWireFactory factory = new ScriptedWireFactory(
                        server.getPort(), 2, Integer.MAX_VALUE, /* dynamicCredential */ true);
                BackgroundDrainer drainer = newDrainer(factory);
                List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
                drainer.setErrorSink(captured::add);

                runToCompletion(drainer);

                assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
                assertTrue("a persistent rotating 401 must quarantine after the ride-out",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                // 1 healthy connect + 1 loop reconnect sweep (latches the loop's authTerminal) + enough
                // re-entered sweeps to satisfy both the attempt threshold and the wall-clock dwell floor.
                assertTrue("the drainer must reach the rotating-auth attempt threshold",
                        factory.attempts() >= 2 + BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS);
                assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
                assertEquals(SenderError.Category.DATA_LOSS, captured.get(0).getCategory());
            }
        });
    }

    @Test
    public void testMidDrainRotating401RidesOutThenDrains() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot(SEEDED_FRAMES);
            DropFirstHandler handler = new DropFirstHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Call 1: healthy connect (drain starts; server durably acks one
                // frame, then drops the wire). Calls 2-4: the reconnect sweep is
                // refused with a 401. Call 5+: the rotation healed; the freshly
                // pulled token is accepted and the drain completes.
                ScriptedWireFactory factory = new ScriptedWireFactory(
                        server.getPort(), 2, 4, /* dynamicCredential */ true);
                BackgroundDrainer drainer = newDrainer(factory);
                List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
                drainer.setErrorSink(captured::add);

                runToCompletion(drainer);

                // Without the mid-drain ride-out the first 401 (call 2) latches a
                // fatal terminal and the drainer quarantines: outcome FAILED,
                // attempts == 2, a .failed sentinel. The fix routes it into the
                // ride-out instead, so the drain survives the rotation.
                assertEquals("a rotating 401 that heals within the ride-out must not quarantine",
                        BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                assertFalse("no .failed sentinel after a successful drain",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertTrue("expected the drainer to ride out the 401s, attempts=" + factory.attempts(),
                        factory.attempts() >= 5);
                assertTrue("a healed rotation must report no data loss: " + captured, captured.isEmpty());
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
        Thread t = new Thread(drainer, "test-mid-drain-auth-drainer");
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
     * Server-side script. Connection #1 durably acks exactly one frame, then
     * closes the socket — a deterministic mid-drain wire drop. Every later
     * connection acks all traffic, so a reconnected loop drains to completion.
     * Keyed per {@code ClientHandler} identity; a dead connection's late
     * buffered frames are ignored rather than acked with a stale counter.
     */
    private static final class DropFirstHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE = "trades";
        private final List<TestWebSocketServer.ClientHandler> arrivalOrder = new ArrayList<>();
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

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
                if (connectionIndex == 1) {
                    if (seq == 0) {
                        client.sendBinary(okFrame(seq, seq));
                        client.sendBinary(durableAckFrame(seq));
                    } else if (seq == 1) {
                        client.close(); // mid-drain wire drop
                    }
                    // seq > 1: late buffered frames from the condemned connection; ignore.
                } else {
                    client.sendBinary(okFrame(seq, seq));
                    client.sendBinary(durableAckFrame(seq));
                }
            } catch (IOException ignored) {
                // Best-effort ack: the connection died under us. The client replays
                // on its next connection.
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
     * {@code [throwFrom, throwTo]} (1-based, inclusive) throw a
     * {@link QwpAuthFailedException} (401); every other call returns a live
     * upgraded client against the test server. {@link #hasDynamicCredential()}
     * reports whether the credential rotates — the signal the orphan drainer's
     * terminal policy reads.
     */
    private static final class ScriptedWireFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger calls = new AtomicInteger();
        private final boolean dynamicCredential;
        private final int port;
        private final int throwFrom;
        private final int throwTo;

        ScriptedWireFactory(int port, int throwFrom, int throwTo, boolean dynamicCredential) {
            this.port = port;
            this.throwFrom = throwFrom;
            this.throwTo = throwTo;
            this.dynamicCredential = dynamicCredential;
        }

        int attempts() {
            return calls.get();
        }

        @Override
        public boolean hasDynamicCredential() {
            return dynamicCredential;
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            int n = calls.incrementAndGet();
            if (n >= throwFrom && n <= throwTo) {
                throw new QwpAuthFailedException(401, "localhost", port);
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
}
