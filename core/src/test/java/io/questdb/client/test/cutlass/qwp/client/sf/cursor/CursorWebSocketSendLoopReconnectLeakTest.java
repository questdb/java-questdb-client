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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression: when the cursor I/O loop reconnects via {@code swapClient},
 * the new {@link WebSocketClient} is installed in the loop's private
 * {@code client} field but the owner ({@code QwpWebSocketSender} or
 * {@code BackgroundDrainer}) keeps the stale pre-reconnect reference.
 * Pre-fix, {@code loop.close()} did not close its own client either —
 * so on shutdown the live post-reconnect socket leaked because the
 * owner was closing a stale (already-closed) reference and nobody was
 * closing the live one.
 * <p>
 * The fix is to make {@code loop.close()} close its current
 * {@code client} after stopping the I/O thread; owners' duplicate close
 * calls remain safe because {@code WebSocketClient.close()} is idempotent.
 */
public class CursorWebSocketSendLoopReconnectLeakTest {

    @Test
    public void testCloseClosesLivePostReconnectClient() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            DisconnectAfterFirstAckHandler handler = new DisconnectAfterFirstAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";";
                Sender sender = Sender.fromConfig(cfg);
                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                // Hand the sender to the handler so it can wait for the
                // client's ACK counter to advance before closing the
                // connection — a deterministic alternative to a fixed
                // server-side sleep.
                handler.bindSender(wss);
                WebSocketClient liveClient;
                try {
                    // Batch 1: server ACKs and immediately disconnects. The
                    // I/O loop sees the wire failure, runs through reconnect,
                    // calls swapClient(newClient). After this the loop's
                    // private client field points at the new socket; the
                    // sender's client field still points at the (closed) old one.
                    sender.table("foo").longColumn("v", 1L).atNow();
                    sender.flush();

                    // Wait for the loop to register a successful reconnect.
                    // The handler can't count a "connection" until it sees a
                    // binary frame, and the I/O loop has nothing to replay
                    // post-ACK — so use the loop's own counter instead. Spin
                    // (no sleep) because the reconnect normally lands in a
                    // few milliseconds; the deadline only protects against
                    // pathological CI stalls.
                    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (wss.getTotalReconnectsSucceeded() < 1L) {
                        if (System.nanoTime() > deadlineNanos) {
                            throw new AssertionError(
                                    "precondition: reconnect must happen within 5s — saw "
                                            + wss.getTotalReconnectsSucceeded()
                                            + " successful reconnects");
                        }
                        io.questdb.client.std.Compat.onSpinWait();
                    }

                    // Reach into the loop to capture the live client BEFORE we
                    // call sender.close() — that's the reference we want to
                    // verify gets closed.
                    CursorWebSocketSendLoop loop = readField(
                            sender, "cursorSendLoop", CursorWebSocketSendLoop.class);
                    Assert.assertNotNull("loop should be wired up", loop);
                    liveClient = readField(loop, "client", WebSocketClient.class);
                    Assert.assertNotNull(
                            "live client should still be installed in the loop",
                            liveClient);
                    // Sanity: the live client should be in a connected state
                    // before close. (If it isn't, the test setup is wrong.)
                    Assert.assertTrue(
                            "precondition: live post-reconnect client should be "
                                    + "connected before sender.close()",
                            liveClient.isConnected());
                } finally {
                    sender.close();
                }

                // Post-fix: loop.close closed the current client. Pre-fix:
                // sender.close only closed its STALE reference (the original
                // pre-reconnect client), the live one was orphaned.
                Assert.assertFalse(
                        "live post-reconnect client must be closed by loop.close() "
                                + "— otherwise its native socket / fds leak past "
                                + "sender.close()",
                        liveClient.isConnected());
            }
        });
    }

    private static <T> T readField(Object target, String name, Class<T> type) throws Exception {
        Class<?> cls = target.getClass();
        while (cls != null) {
            try {
                Field f = cls.getDeclaredField(name);
                f.setAccessible(true);
                Object v = f.get(target);
                return type.cast(v);
            } catch (NoSuchFieldException e) {
                cls = cls.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    /** ACKs the first frame, then closes the connection so the sender reconnects. */
    private static class DisconnectAfterFirstAckHandler
            implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong();
        private TestWebSocketServer.ClientHandler firstClient;
        // Bound by the test so the handler can wait for the client to
        // confirm receipt of the ACK before closing the wire, replacing the
        // fixed 50ms server-side sleep that was racing the ACK delivery on
        // loaded CI machines.
        private volatile QwpWebSocketSender sender;

        void bindSender(QwpWebSocketSender sender) {
            this.sender = sender;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (firstClient == null || firstClient != client) {
                connectionsAccepted.incrementAndGet();
                if (firstClient == null) firstClient = client;
            }
            long observedReceived = totalBinaryReceived.incrementAndGet();
            try {
                long baselineAcks = sender == null ? 0L : sender.getTotalAcks();
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
                if (observedReceived == 1L) {
                    // Wait until the client confirms it processed the ACK we
                    // just sent — getTotalAcks is the I/O thread's own
                    // counter, so seeing it advance past the pre-send
                    // baseline guarantees the ACK was applied before we
                    // close the wire underneath the loop.
                    awaitAckProcessed(baselineAcks);
                    client.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void awaitAckProcessed(long baseline) {
            QwpWebSocketSender s = sender;
            if (s == null) return;
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (s.getTotalAcks() <= baseline) {
                if (System.nanoTime() > deadlineNanos) {
                    throw new AssertionError(
                            "client never reported processing the ACK within 5s "
                                    + "(baseline=" + baseline + ", current=" + s.getTotalAcks() + ")");
                }
                io.questdb.client.std.Compat.onSpinWait();
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
