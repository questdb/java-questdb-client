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
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Behavior of {@code initial_connect_retry}: when the server is briefly
 * unavailable at startup, the sender should keep trying through the
 * configured cap (instead of failing immediately).
 */
public class InitialConnectRetryTest {

    private static final int TEST_PORT = 19_700 + (int) (System.nanoTime() % 100);

    @Test
    public void testWithoutRetryFailsImmediately() {
        // No server on this port. With initial_connect_retry off (default),
        // fromConfig must throw without sitting around for the cap.
        int port = TEST_PORT + 1;
        long t0 = System.nanoTime();
        try (Sender ignored = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
            Assert.fail("expected immediate connect failure");
        } catch (Exception expected) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            Assert.assertTrue("must fail fast (took " + elapsedMs + " ms)",
                    elapsedMs < 2_000L);
        }
    }

    @Test
    public void testWithRetrySucceedsWhenServerComesUpInTime() throws Exception {
        // initial_connect_retry=true; we open the sender BEFORE starting
        // the server, then start the server in a background thread after
        // a short delay. The retry loop should see the server come up and
        // proceed cleanly.
        int port = TEST_PORT + 2;
        AckHandler handler = new AckHandler();
        TestWebSocketServer server = new TestWebSocketServer(port, handler);
        Thread starter = new Thread(() -> {
            try {
                Thread.sleep(300);
                server.start();
            } catch (Exception e) {
                // best-effort
            }
        }, "delayed-server-start");
        starter.setDaemon(true);
        starter.start();
        try {
            String cfg = "ws::addr=localhost:" + port
                    + ";initial_connect_retry=true"
                    + ";reconnect_max_duration_millis=5000"
                    + ";reconnect_initial_backoff_millis=50"
                    + ";reconnect_max_backoff_millis=200"
                    + ";close_flush_timeout_millis=0;";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
            }
        } finally {
            try {
                server.close();
            } catch (Exception ignored) {
                // already closed
            }
        }
    }

    @Test
    public void testWithRetryGivesUpAfterCap() {
        // No server. With retry on but a tight cap, fromConfig should
        // throw within the cap window (with some slack).
        int port = TEST_PORT + 3;
        long t0 = System.nanoTime();
        String cfg = "ws::addr=localhost:" + port
                + ";initial_connect_retry=true"
                + ";reconnect_max_duration_millis=400"
                + ";reconnect_initial_backoff_millis=10"
                + ";reconnect_max_backoff_millis=50;";
        try (Sender ignored = Sender.fromConfig(cfg)) {
            Assert.fail("expected give-up after cap");
        } catch (Exception expected) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            Assert.assertTrue("must give up around the cap (took " + elapsedMs + " ms)",
                    elapsedMs >= 300L && elapsedMs < 3_000L);
            String msg = expected.getMessage();
            Assert.assertTrue("error must mention startup retry: " + msg,
                    msg != null && (msg.contains("initial connect")
                            || msg.contains("Failed to connect")));
        }
    }

    /** Acks every binary frame so the sender's flush completes. */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
