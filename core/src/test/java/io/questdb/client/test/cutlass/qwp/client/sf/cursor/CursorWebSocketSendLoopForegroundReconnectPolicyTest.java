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
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
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
    public void testPostStartAuthFailureRetriesUntilCredentialsRecover() throws Exception {
        assertForegroundRecovers(false,
                () -> new QwpAuthFailedException(401, "localhost", 1));
    }

    @Test
    public void testPostStartDurableAckMismatchRetriesUntilCapabilityRecovers() throws Exception {
        assertForegroundRecovers(true,
                () -> new QwpDurableAckMismatchException("localhost", 1, "primary"));
    }

    private void assertForegroundRecovers(boolean durableAck, FailureSupplier failureSupplier) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            DropFirstConnectionHandler handler = new DropFirstConnectionHandler(durableAck);
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
                        100L,
                        1L,
                        4L,
                        durableAck,
                        durableAck ? 10L : 0L,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L,
                        0L,
                        CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND);
                try {
                    appendFrame(engine, (byte) 1);
                    loop.start();

                    await(() -> loop.getTotalReconnects() >= 1L
                                    || loop.getTerminalError() != null,
                            "foreground reconnect did not complete");
                    Assert.assertNull("post-start endpoint-policy failures must not stop the producer",
                            loop.getTerminalError());
                    Assert.assertTrue("the reconnect loop must retry both scripted failures",
                            factory.attempts() >= 3);

                    long target = appendFrame(engine, (byte) 2);
                    await(() -> engine.ackedFsn() >= target || loop.getTerminalError() != null,
                            "foreground sender did not deliver after the policy failure cleared");
                    Assert.assertNull(loop.getTerminalError());
                    Assert.assertEquals(target, engine.ackedFsn());
                } finally {
                    loop.close();
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

    private static final class DropFirstConnectionHandler
            implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE = "trades";
        private final boolean durableAck;
        private final Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConnection =
                new IdentityHashMap<>();
        private TestWebSocketServer.ClientHandler firstConnection;

        private DropFirstConnectionHandler(boolean durableAck) {
            this.durableAck = durableAck;
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
                if (client == firstConnection) {
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
}
