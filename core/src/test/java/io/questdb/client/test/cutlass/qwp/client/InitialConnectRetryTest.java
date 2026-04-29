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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Behavior of {@code initial_connect_retry}: when the server is briefly
 * unavailable at startup, the sender should keep trying through the
 * configured cap (instead of failing immediately).
 */
public class InitialConnectRetryTest {

    private static final int TEST_PORT = 19_700 + (int) (System.nanoTime() % 100);

    @Test
    public void testWithRetryGivesUpAfterCap() {
        // No server. With retry on, fromConfig must run the retry loop and
        // ultimately throw with the connectWithRetry-shaped message that
        // names the elapsed budget and attempt count. The actual budget
        // honoring is observable through that message — we don't need a
        // wall-clock check.
        int port = TEST_PORT + 3;
        String cfg = "ws::addr=127.0.0.1:" + port
                + ";initial_connect_retry=true"
                + ";reconnect_max_duration_millis=400"
                + ";reconnect_initial_backoff_millis=10"
                + ";reconnect_max_backoff_millis=50;";
        try (Sender ignored = Sender.fromConfig(cfg)) {
            Assert.fail("expected give-up after cap");
        } catch (Exception expected) {
            String msg = expected.getMessage();
            Assert.assertNotNull("error must have a message", msg);
            Assert.assertTrue("error must come from the retry loop: " + msg,
                    msg.contains("initial connect") && msg.contains("attempts"));
        }
    }

    @Test
    public void testWithRetrySucceedsWhenServerComesUpInTime() {
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
            String cfg = "ws::addr=127.0.0.1:" + port
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
    public void testWithoutRetryFailsImmediately() {
        // No server on this port. With initial_connect_retry off (default),
        // fromConfig must throw on the first connect failure rather than enter
        // the retry loop. We assert the structural shape of the error: the
        // raw "Failed to connect" message from buildAndConnect, NOT the
        // "initial connect ... attempts" message connectWithRetry produces.
        int port = TEST_PORT + 1;
        // Use the IPv4 literal so the test doesn't pay first-call
        // getaddrinfo("localhost") cost on Windows (1-2 s cold lookup).
        try (Sender ignored = Sender.fromConfig("ws::addr=127.0.0.1:" + port + ";")) {
            Assert.fail("expected immediate connect failure");
        } catch (Exception expected) {
            String msg = expected.getMessage();
            Assert.assertNotNull("error must have a message", msg);
            Assert.assertTrue("error must be the raw connect-refused: " + msg,
                    msg.contains("Failed to connect"));
            Assert.assertFalse("error must NOT mention the retry loop: " + msg,
                    msg.contains("attempts"));
        }
    }

    /**
     * Acks every binary frame so the sender's flush completes.
     */
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
