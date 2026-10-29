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
import io.questdb.client.cutlass.qwp.client.DurableAckTiers;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Orphan-slot drain with the LOCAL durable-ack tier ({@code request_durable_ack=local}).
 * <p>
 * The {@link BackgroundDrainer} hands its configured tier set to every {@link CursorWebSocketSendLoop} it
 * builds. With LOCAL alone, {@code STATUS_LOCAL_DURABLE_ACK} is the trim trigger: STATUS_OK must not trim
 * the slot, and each local ack must advance the drainer's watermark until the slot is fully drained. A
 * drainer that passed a different tier set would either trim on STATUS_OK (no tier) or treat local acks
 * as progress only and never finish (replicated tier).
 * <p>
 * Wire realism: a real {@link TestWebSocketServer} grants the requested {@code local} tier on upgrade,
 * OKs every frame at once and sends the local acks only when the test releases them.
 */
public class BackgroundDrainerLocalDurableAckTest {

    private static final long FAST_BACKOFF_MAX_MILLIS = 4L;
    private static final long FAST_BACKOFF_MILLIS = 1L;
    private static final long RECONNECT_MAX_DURATION_MILLIS = 60_000L;
    private static final int SEEDED_FRAMES = 5;
    private static final long SEGMENT_SIZE_BYTES = 16384L;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 20;
    private static final String TABLE = "trades";

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-drainer-local-ack-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        rmDirRec(slotPath);
    }

    @Test
    public void testOrphanSlotWithLocalTierTrimsOnLocalDurableAcks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedSlot();
            LocalAckHoldingHandler handler = new LocalAckHoldingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                BackgroundDrainer drainer = new BackgroundDrainer(
                        slotPath,
                        SEGMENT_SIZE_BYTES,
                        SF_MAX_TOTAL_BYTES,
                        new LocalTierWireFactory(server.getPort()),
                        RECONNECT_MAX_DURATION_MILLIS,
                        FAST_BACKOFF_MILLIS,
                        FAST_BACKOFF_MAX_MILLIS,
                        DurableAckTiers.LOCAL,
                        /* durableAckKeepaliveIntervalMillis */ 200L);
                Thread runner = new Thread(drainer, "test-local-ack-drainer");
                runner.setDaemon(true);
                runner.start();
                try {
                    assertEquals("the server must see the local tier requested on upgrade",
                            "local", server.pollDurableAckRequest(5, TimeUnit.SECONDS));
                    assertTrue("server did not receive and OK every orphan frame",
                            handler.allFramesOked.await(5, TimeUnit.SECONDS));
                    // Give the drainer several 50ms polls to observe any OK-driven trim.
                    Thread.sleep(250);
                    assertEquals("STATUS_OK alone must not trim a LOCAL-tier orphan slot",
                            -1L, drainer.getAckedFsn());
                    assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());

                    handler.sendLocalDurableAck(1);
                    awaitAckedFsn(drainer, 1);
                    assertEquals("a local ack covering frames 0..1 must trim exactly that far",
                            1L, drainer.getAckedFsn());
                    assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());

                    handler.sendLocalDurableAck(SEEDED_FRAMES - 1);
                    runner.join(10_000L);
                } finally {
                    if (runner.isAlive()) {
                        drainer.requestStop();
                        runner.join(5_000L);
                    }
                }

                assertEquals("local acks must drain the orphan slot to completion",
                        BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
                assertEquals(SEEDED_FRAMES - 1L, drainer.getAckedFsn());
                assertFalse("no .failed sentinel after a successful drain",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertEquals("the drain must not have needed a reconnect", 1, server.handshakeCount());
            }
        });
    }

    private static void awaitAckedFsn(BackgroundDrainer drainer, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (drainer.getAckedFsn() < expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("STATUS_LOCAL_DURABLE_ACK did not trim the LOCAL-tier orphan slot [expected="
                        + expected + ", ackedFsn=" + drainer.getAckedFsn() + "]",
                drainer.getAckedFsn() >= expected);
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

    private void seedSlot() {
        try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_SIZE_BYTES)) {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, payload[i]);
                }
                for (int i = 0; i < SEEDED_FRAMES; i++) {
                    engine.appendBlocking(buf, 16);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    /**
     * OKs every frame as it arrives (wire seq {@code n} carries table seqTxn {@code n}) and sends
     * {@code STATUS_LOCAL_DURABLE_ACK} only when the test calls {@link #sendLocalDurableAck}.
     */
    private static final class LocalAckHoldingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final CountDownLatch allFramesOked = new CountDownLatch(1);
        private TestWebSocketServer.ClientHandler client;
        private long nextSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            this.client = client;
            long seq = nextSeq++;
            try {
                client.sendBinary(okFrame(seq));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (nextSeq == SEEDED_FRAMES) {
                allFramesOked.countDown();
            }
        }

        synchronized void sendLocalDurableAck(long seqTxn) throws IOException {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put(WebSocketResponse.STATUS_LOCAL_DURABLE_ACK);
            bb.putShort((short) 1); // tableCount
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seqTxn);
            client.sendBinary(bb.array());
        }

        private static byte[] okFrame(long seq) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put(WebSocketResponse.STATUS_OK);
            bb.putLong(seq);
            bb.putShort((short) 1); // tableCount
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seq);
            return bb.array();
        }
    }

    /**
     * Connects a real client requesting the LOCAL tier and, like the production connect walk, refuses a
     * server that did not grant it.
     */
    private static final class LocalTierWireFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final int port;

        LocalTierWireFactory(int port) {
            this.port = port;
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            WebSocketClient c = WebSocketClientFactory.newPlainTextInstance();
            try {
                c.setQwpMaxVersion(1);
                c.setQwpDurableAckTiers(DurableAckTiers.LOCAL);
                c.setConnectTimeout(5_000);
                c.connect("localhost", port);
                c.upgrade("/write/v4", 5_000, null);
                if (!c.isServerDurableAckEnabled()) {
                    throw new QwpDurableAckMismatchException(
                            "localhost", port, DurableAckTiers.LOCAL, c.getServerDurableAckEcho());
                }
            } catch (Throwable t) {
                c.close();
                throw t;
            }
            return c;
        }
    }
}
