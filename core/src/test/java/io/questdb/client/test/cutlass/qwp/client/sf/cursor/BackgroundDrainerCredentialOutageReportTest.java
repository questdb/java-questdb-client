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
import io.questdb.client.cutlass.qwp.client.QwpCredentialUnavailableException;
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
 * Credential-outage observability for an orphan {@link BackgroundDrainer}.
 * <p>
 * A credential the client cannot ACQUIRE — the configured token provider throws
 * instead of returning one, after a revocation, an IdP outage, or a sign-in the
 * user has not finished — is retried indefinitely under Invariant B, exactly like
 * a transport outage: the un-acked rows stay safe in store-and-forward and no
 * {@code .failed} sentinel is dropped. Retrying forever is correct; retrying
 * SILENTLY is not. A revoked refresh token does not heal on its own, so with no
 * report the outage is invisible until SF fills and resurfaces as ring
 * backpressure, which points the operator at disk sizing instead of at their
 * credentials.
 * <p>
 * The foreground sender already reports it (see
 * {@code WebSocketTokenProviderTest#testPersistentCredentialOutageIsReportedToTheErrorHandler}).
 * An orphan drainer rides out the very same fault and had neither half:
 * <ul>
 *   <li>its drain loop's {@code SenderError} dispatcher was never wired, so the
 *       loop's own {@code credential-unavailable} report was dropped into a null;</li>
 *   <li>at initial connect the exception matched none of the typed arms and landed
 *       in the generic transport arm, whose WARN says "cluster unreachable" — the
 *       wrong condition, sending the operator after a network fault that does not
 *       exist.</li>
 * </ul>
 * Both halves are pinned here.
 * <p>
 * Wire realism matches {@link BackgroundDrainerMidDrainAuthRejectTest}: a real
 * {@link TestWebSocketServer} durably acks over a live socket while the scripted
 * {@link CursorWebSocketSendLoop.ReconnectFactory} decides, per connect attempt,
 * whether the sweep produces a client or fails to obtain a credential.
 */
public class BackgroundDrainerCredentialOutageReportTest {

    private static final long FAST_BACKOFF_MAX_MILLIS = 4L;
    private static final long FAST_BACKOFF_MILLIS = 1L;
    private static final String PROVIDER_FAILURE_MESSAGE = "refresh token revoked by the IdP";
    private static final long RECONNECT_MAX_DURATION_MILLIS = 25L;
    private static final int SEEDED_FRAMES = 5;
    private static final long SEGMENT_SIZE_BYTES = 16384L;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 20;
    private static final String TABLE = "trades";

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-drainer-credential-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        rmDirRec(slotPath);
    }

    @Test
    public void testInitialConnectCredentialOutageIsNamedNotMislabelledUnreachable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The drain loop does not exist yet at initial connect, so the sink cannot
            // carry this one -- the log is the only diagnostic, which makes naming the
            // condition the whole of the fix. "cluster unreachable" is actively
            // misleading here: nothing was attempted on the wire at all.
            seedSlot(SEEDED_FRAMES);
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Calls 1-2: the token provider throws. Call 3+: it hands over a token.
                ScriptedWireFactory factory = new ScriptedWireFactory(server.getPort(), 1, 2);
                BackgroundDrainer drainer = newDrainer(factory);

                ch.qos.logback.classic.Logger drainerLogger = (ch.qos.logback.classic.Logger)
                        org.slf4j.LoggerFactory.getLogger(BackgroundDrainer.class);
                ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                        new ch.qos.logback.core.read.ListAppender<>();
                appender.start();
                ch.qos.logback.classic.Level savedLevel = drainerLogger.getLevel();
                drainerLogger.setLevel(ch.qos.logback.classic.Level.ALL);
                drainerLogger.addAppender(appender);
                try {
                    runToCompletion(drainer);
                } finally {
                    drainerLogger.detachAppender(appender);
                    drainerLogger.setLevel(savedLevel);
                    appender.stop();
                }

                // Invariant B: a credential outage is transient, so it is ridden out --
                // never quarantined, and the drain completes once a token appears.
                assertEquals(BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                assertFalse("a credential outage must never quarantine the slot",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertTrue("the drainer must have ridden out both outage sweeps, attempts="
                        + factory.attempts(), factory.attempts() >= 3);

                boolean named = false;
                boolean mislabelled = false;
                for (ch.qos.logback.classic.spi.ILoggingEvent e : appender.list) {
                    String msg = e.getFormattedMessage();
                    if (msg.contains("token provider failed to supply a credential")
                            && msg.contains(PROVIDER_FAILURE_MESSAGE)) {
                        named = true;
                    }
                    if (msg.contains("cluster unreachable")) {
                        mislabelled = true;
                    }
                }
                assertTrue("a credential outage at initial connect must name itself in the log -- "
                        + "it is the only diagnostic that path produces. Saw: " + appender.list, named);
                assertFalse("a credential outage must not be reported as a network fault. Saw: "
                        + appender.list, mislabelled);
            }
        });
    }

    @Test
    public void testMidDrainCredentialOutageReachesTheErrorSink() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The wire drops after one durable ack; the loop's own reconnect sweeps then
            // fail to obtain a credential. The loop rides that out itself (it never
            // reaches the drainer's connect path), so its dispatcher is the ONLY route
            // to the sink -- and it was never wired on an orphan drainer.
            seedSlot(SEEDED_FRAMES);
            DropFirstHandler handler = new DropFirstHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // Call 1: healthy connect, drain starts. Calls 2-4: the mid-drain
                // reconnect cannot obtain a credential. Call 5+: a token is available
                // again and the drain finishes.
                ScriptedWireFactory factory = new ScriptedWireFactory(server.getPort(), 2, 4);
                BackgroundDrainer drainer = newDrainer(factory);
                List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
                drainer.setErrorSink(captured::add);

                runToCompletion(drainer);

                assertEquals("a credential outage must be ridden out, not quarantined",
                        BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                assertFalse("no .failed sentinel after a drain that recovered",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

                SenderError credentialError = null;
                for (SenderError e : captured) {
                    if (e.getServerMessage() != null
                            && e.getServerMessage().contains("credential-unavailable")) {
                        credentialError = e;
                        break;
                    }
                }
                assertTrue("the credential outage must reach the drainer's error sink -- without it "
                                + "the only signal is a throttled slf4j WARN, a NOP in an app with no "
                                + "binding configured. Saw: " + captured,
                        credentialError != null);
                assertEquals(SenderError.Category.SECURITY_ERROR, credentialError.getCategory());
                // RETRIABLE, not TERMINAL: the rows are safe in SF and the drain recovers.
                assertEquals(SenderError.Policy.RETRIABLE, credentialError.getAppliedPolicy());
                assertTrue("the provider's own failure must be carried through: "
                        + credentialError.getServerMessage(),
                        credentialError.getServerMessage().contains(PROVIDER_FAILURE_MESSAGE));

                for (SenderError e : captured) {
                    assertFalse("a drain that recovered must report no data loss: " + captured,
                            e.getCategory() == SenderError.Category.DATA_LOSS);
                    // An ORPHAN loop latches TERMINAL only to hand the slot back to this
                    // drainer, which then decides. Forwarding it would announce a dead
                    // producer for a fault the very next sweep clears.
                    assertFalse("the loop's hand-back terminal must not reach the sink: " + captured,
                            e.getAppliedPolicy() == SenderError.Policy.TERMINAL);
                }
            }
        });
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
        Thread t = new Thread(drainer, "test-credential-outage-drainer");
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
     * Durably acks everything on every connection — the wire is never the fault
     * under test here, only the credential the client cannot obtain to open it.
     */
    private static final class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long[] counter = wireSeqByConn.computeIfAbsent(client, k -> new long[1]);
            long seq = counter[0]++;
            try {
                client.sendBinary(okFrame(seq, seq));
                client.sendBinary(durableAckFrame(seq));
            } catch (IOException ignored) {
                // Best-effort ack: the connection died under us; the client replays.
            }
        }
    }

    /**
     * Server-side script. Connection #1 durably acks exactly one frame, then closes
     * the socket — a deterministic mid-drain wire drop that forces the loop's own
     * reconnect sweep. Every later connection acks all traffic, so a reconnected
     * loop drains to completion.
     */
    private static final class DropFirstHandler implements TestWebSocketServer.WebSocketServerHandler {
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
                // Best-effort ack: the connection died under us; the client replays.
            }
        }
    }

    /**
     * Per-call-index scripted factory over a real wire. Call indexes inside
     * {@code [throwFrom, throwTo]} (1-based, inclusive) fail to obtain a credential —
     * a {@link QwpCredentialUnavailableException} wrapping the provider's own
     * exception, exactly as {@code QwpWebSocketSender} wraps a throwing
     * {@code httpTokenProvider}. Every other call returns a live upgraded client.
     */
    private static final class ScriptedWireFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger calls = new AtomicInteger();
        private final int port;
        private final int throwFrom;
        private final int throwTo;

        ScriptedWireFactory(int port, int throwFrom, int throwTo) {
            this.port = port;
            this.throwFrom = throwFrom;
            this.throwTo = throwTo;
        }

        int attempts() {
            return calls.get();
        }

        @Override
        public boolean hasDynamicCredential() {
            // A token provider is by definition a rotating credential.
            return true;
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            int n = calls.incrementAndGet();
            if (n >= throwFrom && n <= throwTo) {
                throw new QwpCredentialUnavailableException(
                        new RuntimeException(PROVIDER_FAILURE_MESSAGE));
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
