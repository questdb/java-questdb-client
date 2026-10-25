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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KeepaliveTest {

    @Test
    public void testIdleConnectionSendsPings() throws Exception {
        final long intervalMillis = 100;
        AtomicInteger pings = new AtomicInteger();
        long startNanos = System.nanoTime();
        try (TestWebSocketServer server = startServer(pings);
             Sender ignored = Sender.fromConfig(config(server, intervalMillis))) {
            // generous: exits as soon as 3 pings land, and Windows CI can stall for seconds
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (pings.get() < 3 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            // a PING must not cost a reconnect
            Assert.assertEquals(1, server.handshakeCount());
        }
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        int seen = pings.get();
        Assert.assertTrue("expected periodic pings on an idle connection, saw " + seen, seen >= 3);
        // the gate spaces PINGs at least one interval apart, so this holds however slow the box is
        Assert.assertTrue("ping flood: " + seen + " in " + elapsedMillis + " ms", seen <= elapsedMillis / intervalMillis + 1);
    }

    @Test
    public void testNoPingsWhileFramesFlow() throws Exception {
        // every send restarts the idle clock, so a sender flushing well inside the
        // interval never pings; without that it pinged once per interval regardless
        final long intervalMillis = 300;
        AtomicInteger pings = new AtomicInteger();
        try (TestWebSocketServer server = startServer(pings);
             Sender sender = Sender.fromConfig(config(server, intervalMillis))) {
            long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(5 * intervalMillis);
            while (System.nanoTime() < end) {
                sender.table("t").longColumn("x", 1).atNow();
                sender.flush();
                Thread.sleep(10);
            }
        }
        // one stall past the interval on a slow box may earn a PING; ~5 means none were suppressed
        Assert.assertTrue("pinged while frames were flowing: " + pings.get(), pings.get() <= 1);
    }

    @Test
    public void testNoPingsByDefault() throws Exception {
        AtomicInteger pings = new AtomicInteger();
        try (TestWebSocketServer server = startServer(pings);
             Sender ignored = Sender.fromConfig("ws::addr=localhost:" + server.getPort() + ";")) {
            // A broken disable (interval 0 treated as "always due") pings on every idle pass
            Thread.sleep(500);
        }
        Assert.assertEquals(0, pings.get());
    }

    private static String config(TestWebSocketServer server, long keepaliveMillis) {
        return "ws::addr=localhost:" + server.getPort() + ";keepalive_interval_millis=" + keepaliveMillis + ";";
    }

    private static TestWebSocketServer startServer(AtomicInteger pings) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            private long nextSeq;

            @Override
            public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                try {
                    client.sendBinary(QwpWireTestUtils.buildAck(nextSeq++));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onPing(TestWebSocketServer.ClientHandler client) {
                pings.incrementAndGet();
            }
        });
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }
}
