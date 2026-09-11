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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CursorWebSocketSendLoopForegroundReconnectPolicyTest {
    private static final long SEGMENT_SIZE_BYTES = 16_384L;

    @Rule
    public final TemporaryFolder sfDir = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testAsyncInitialAuthFailureSurfacesTerminalToTheCaller() throws Exception {
        assertAsyncInitialForegroundSurfacesTerminal(false,
                () -> new QwpAuthFailedException(401, "localhost", 1),
                "ws-upgrade-failed");
    }

    @Test
    public void testAsyncInitialDurableAckMismatchSurfacesTerminalToTheCaller() throws Exception {
        assertAsyncInitialForegroundSurfacesTerminal(true,
                () -> new QwpDurableAckMismatchException("localhost", 1, "primary"),
                "durable-ack-mismatch");
    }

    @Test
    public void testFirstConnectCatchUpFailureKeepsStartupTerminalArmed() throws Exception {
        // swapClient must not latch hasEverConnected before the dictionary
        // catch-up succeeds. An ASYNC first connect that completes the upgrade
        // and then dies inside the catch-up has never fully established a
        // connection, so a subsequent bad-credential rejection is still a
        // STARTUP failure and must latch the terminal -- not retry silently
        // while the sender buffers into store-and-forward forever.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(
                    sfDir.newFolder().getAbsolutePath(), SEGMENT_SIZE_BYTES)) {
                CatchUpThenAuthFactory factory = new CatchUpThenAuthFactory();
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null,
                        engine,
                        0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        1L,
                        4L,
                        false,
                        0L,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L,
                        0L,
                        CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND,
                        0L);
                try {
                    seedMirror(loop, "sym0"); // non-empty mirror => swapClient runs the catch-up
                    appendFrame(engine, (byte) 1);
                    loop.start();

                    await(() -> loop.getTerminalError() != null,
                            "an endpoint-policy failure after a failed first catch-up must latch the startup terminal");
                    Assert.assertTrue("the terminal must name the endpoint-policy failure, got: "
                                    + loop.getTerminalError().getMessage(),
                            loop.getTerminalError().getMessage().contains("ws-upgrade-failed"));
                    Assert.assertFalse("a connection whose catch-up failed was never fully established",
                            loop.hasEverConnected());
                    Assert.assertEquals("the auth rejection must latch on first sight",
                            2, factory.attempts());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testPostStartAuthFailureRetriesUntilCredentialsRecover() throws Exception {
        assertForegroundRecovers(false,
                () -> new QwpAuthFailedException(401, "localhost", 1));
    }

    @Test
    public void testPostStartDurableAckMismatchRetriesUntilCapabilityRecovers() throws Exception {
        assertForegroundRecovers(true,
                () -> new QwpDurableAckMismatchException("localhost", 1, "primary"));
    }

    /**
     * An endpoint-policy failure before the sender has EVER reached the server is a
     * startup problem, so it must reach the caller rather than retry silently: an
     * operator with wrong credentials has to learn that, not watch a mute sender
     * buffer forever. ASYNC startup has no caller left to throw at, so the terminal
     * is latched for {@code SenderErrorHandler} delivery and the {@code close()}
     * rethrow. Post-start the opposite holds -- see {@link #assertForegroundRecovers}.
     */
    private void assertAsyncInitialForegroundSurfacesTerminal(
            boolean durableAck,
            FailureSupplier failureSupplier,
            String expectedDetail
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            DropFirstConnectionHandler handler = new DropFirstConnectionHandler(durableAck, false);
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true);
                 CursorSendEngine engine = new CursorSendEngine(
                         sfDir.newFolder().getAbsolutePath(), SEGMENT_SIZE_BYTES)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                GatedFactory factory = new GatedFactory(
                        server.getPort(), durableAck, failureSupplier);
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null,
                        engine,
                        0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        1L,
                        4L,
                        durableAck,
                        durableAck ? 10L : 0L,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L,
                        0L,
                        CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND,
                        0L);
                try {
                    appendFrame(engine, (byte) 1);
                    loop.start();

                    await(() -> loop.getTerminalError() != null,
                            "an async initial endpoint-policy failure must latch a terminal");
                    Assert.assertTrue("the terminal must name the endpoint-policy failure, got: "
                                    + loop.getTerminalError().getMessage(),
                            loop.getTerminalError().getMessage().contains(expectedDetail));
                    // Never reached the server, so the terminal is a startup verdict.
                    Assert.assertFalse(loop.hasEverConnected());
                    // Latched on the FIRST rejection: retrying an endpoint-policy
                    // failure at startup is exactly the silent-buffering regression
                    // this pins. Attempts cannot climb once running flips false.
                    Assert.assertEquals("a startup endpoint-policy failure must not be retried",
                            1, factory.attempts());
                } finally {
                    factory.allowConnect();
                    loop.close();
                }
            }
        });
    }

    /**
     * The post-start twin of {@link #assertAsyncInitialForegroundSurfacesTerminal}:
     * handing the loop a live client seeds {@code hasEverConnected}, so the sender is
     * past initialization and store-and-forward owns the buffered data. Every
     * endpoint-policy failure is then a transient to ride out, never a terminal
     * (Invariant B).
     */
    private void assertForegroundRecovers(boolean durableAck, FailureSupplier failureSupplier) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            DropFirstConnectionHandler handler = new DropFirstConnectionHandler(durableAck, true);
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true);
                 CursorSendEngine engine = new CursorSendEngine(
                         sfDir.newFolder().getAbsolutePath(), SEGMENT_SIZE_BYTES)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                WebSocketClient initialClient = connect(server.getPort(), durableAck);
                ScriptedFactory factory = new ScriptedFactory(
                        server.getPort(), durableAck, failureSupplier);
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        initialClient,
                        engine,
                        0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        1L,
                        4L,
                        durableAck,
                        durableAck ? 10L : 0L,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L,
                        0L,
                        CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND,
                        0L);
                // Wire an error sink. Retrying is what the store-and-forward contract
                // demands, but until dispatchRetriedEndpointPolicyFailure existed the retry
                // was programmatically INVISIBLE: dispatchError ran only in the terminal
                // branches, so a revoked token produced nothing but a throttled slf4j WARN
                // -- in a library that ships embedded in user apps, often with no binding
                // configured -- and the failure eventually surfaced as "sf_max_total_bytes
                // too small", blaming disk sizing for an auth problem.
                List<SenderError> dispatched = Collections.synchronizedList(new ArrayList<>());
                SenderErrorDispatcher dispatcher =
                        new SenderErrorDispatcher(dispatched::add, 16);
                loop.setErrorDispatcher(dispatcher);
                try {
                    appendFrame(engine, (byte) 1);
                    loop.start();

                    await(() -> loop.getTotalReconnects() >= 1L
                                    || loop.getTerminalError() != null,
                            "foreground reconnect did not complete");
                    Assert.assertNull("post-start endpoint-policy failures must not stop the producer",
                            loop.getTerminalError());
                    await(() -> !dispatched.isEmpty(),
                            "a retried endpoint-policy failure must reach the error handler");
                    SenderError observed = dispatched.get(0);
                    Assert.assertEquals("the retry must be reported as retriable, not terminal",
                            SenderError.Policy.RETRIABLE, observed.getAppliedPolicy());
                    Assert.assertTrue("the reconnect loop must retry both scripted failures",
                            factory.attempts() >= 3);

                    long target = appendFrame(engine, (byte) 2);
                    await(() -> engine.ackedFsn() >= target || loop.getTerminalError() != null,
                            "foreground sender did not deliver after the policy failure cleared");
                    Assert.assertNull(loop.getTerminalError());
                    Assert.assertEquals(target, engine.ackedFsn());
                } finally {
                    loop.close();
                    dispatcher.close();
                    initialClient.close();
                }
            }
        });
    }

    private static long appendFrame(CursorSendEngine engine, byte marker) {
        long buffer = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buffer + i, marker);
            }
            return engine.appendBlocking(buffer, 16);
        } finally {
            Unsafe.free(buffer, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void await(Condition condition, String failureMessage) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.isTrue()) {
            if (System.nanoTime() >= deadline) {
                Assert.fail(failureMessage);
            }
            Thread.sleep(1L);
        }
    }

    private static WebSocketClient connect(int port, boolean durableAck) throws Exception {
        WebSocketClient client = WebSocketClientFactory.newPlainTextInstance();
        try {
            client.setQwpMaxVersion(1);
            client.setQwpRequestDurableAck(durableAck);
            client.setConnectTimeout(5_000);
            client.connect("localhost", port);
            client.upgrade("/write/v4", 5_000, null);
            return client;
        } catch (Throwable e) {
            client.close();
            throw e;
        }
    }

    private static void seedMirror(CursorWebSocketSendLoop loop, String symbol) {
        // Mirror entry layout: [len varint][utf8]. One symbol under 128 bytes
        // keeps the varint single-byte. The loop takes ownership of the buffer
        // and frees it with the mirror on close (the same contract
        // CursorWebSocketSendLoopCatchUpAlignmentTest.seedMirror relies on).
        byte[] bytes = symbol.getBytes(StandardCharsets.UTF_8);
        int total = 1 + bytes.length;
        long addr = Unsafe.malloc(total, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putByte(addr, (byte) bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(addr + 1 + i, bytes[i]);
        }
        loop.seedSentDictMirrorForTest(addr, total, 1);
    }

    @FunctionalInterface
    private interface Condition {
        boolean isTrue() throws Exception;
    }

    @FunctionalInterface
    private interface FailureSupplier {
        Exception get();
    }

    private static final class ScriptedFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger attempts = new AtomicInteger();
        private final boolean durableAck;
        private final FailureSupplier failureSupplier;
        private final int port;

        private ScriptedFactory(int port, boolean durableAck, FailureSupplier failureSupplier) {
            this.port = port;
            this.durableAck = durableAck;
            this.failureSupplier = failureSupplier;
        }

        int attempts() {
            return attempts.get();
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            if (attempts.incrementAndGet() <= 2) {
                throw failureSupplier.get();
            }
            return connect(port, durableAck);
        }
    }

    private static final class GatedFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger attempts = new AtomicInteger();
        private final boolean durableAck;
        private final FailureSupplier failureSupplier;
        private final int port;
        private volatile boolean connectAllowed;

        private GatedFactory(int port, boolean durableAck, FailureSupplier failureSupplier) {
            this.port = port;
            this.durableAck = durableAck;
            this.failureSupplier = failureSupplier;
        }

        void allowConnect() {
            connectAllowed = true;
        }

        int attempts() {
            return attempts.get();
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            attempts.incrementAndGet();
            if (!connectAllowed) {
                throw failureSupplier.get();
            }
            return connect(port, durableAck);
        }
    }

    private static final class DropFirstConnectionHandler
            implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE = "trades";
        private final boolean durableAck;
        private final boolean dropFirstConnection;
        private final Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConnection =
                new IdentityHashMap<>();
        private TestWebSocketServer.ClientHandler firstConnection;

        private DropFirstConnectionHandler(boolean durableAck, boolean dropFirstConnection) {
            this.durableAck = durableAck;
            this.dropFirstConnection = dropFirstConnection;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long[] sequence = wireSeqByConnection.get(client);
            if (sequence == null) {
                sequence = new long[1];
                wireSeqByConnection.put(client, sequence);
                if (firstConnection == null) {
                    firstConnection = client;
                }
            }
            long wireSeq = sequence[0]++;
            try {
                client.sendBinary(okFrame(wireSeq));
                if (durableAck) {
                    client.sendBinary(durableAckFrame(wireSeq));
                }
                if (dropFirstConnection && client == firstConnection) {
                    client.close();
                }
            } catch (IOException ignored) {
                // The deliberate close may race the acknowledgement write.
            }
        }

        private static byte[] durableAckFrame(long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x02);
            bb.putShort((short) 1);
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seqTxn);
            return bb.array();
        }

        private static byte[] okFrame(long wireSeq) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(wireSeq);
            bb.putShort((short) 1);
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(wireSeq);
            return bb.array();
        }
    }

    private static final class CatchUpThenAuthFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger attempts = new AtomicInteger();

        int attempts() {
            return attempts.get();
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            if (attempts.incrementAndGet() == 1) {
                // The upgrade "succeeds" (the loop receives a live client), then
                // every send fails: swapClient's dictionary catch-up dies
                // mid-flight on the first connection.
                return new FailingSendClient();
            }
            throw new QwpAuthFailedException(401, "localhost", 1);
        }
    }

    private static final class FailingSendClient extends WebSocketClient {
        private FailingSendClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public int getServerMaxBatchSize() {
            return 0; // no advertised cap: the catch-up ships one unchunked frame
        }

        @Override
        public int getServerQwpVersion() {
            return 1;
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            throw new RuntimeException("transient wire failure during catch-up");
        }

        @Override
        public void sendBinary(long firstPtr, int firstLength, long secondPtr, int secondLength) {
            throw new RuntimeException("transient wire failure during catch-up");
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
