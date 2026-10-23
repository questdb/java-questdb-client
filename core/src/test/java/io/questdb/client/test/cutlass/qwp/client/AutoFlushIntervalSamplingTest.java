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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The auto_flush_interval check samples the clock at a rate-adaptive cadence
 * instead of on every row. A batch filling at a steady rate must still be
 * flushed by the row that crosses the interval; after a burst, a trickle may
 * overshoot by at most 7 rows.
 */
public class AutoFlushIntervalSamplingTest {

    @Test
    public void testSteadyTrickleFlushesOnTheRowThatCrossesTheInterval() throws Exception {
        AtomicInteger messages = new AtomicInteger();
        try (TestWebSocketServer server = startServer(messages);
             Sender s = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                     + ";auto_flush_rows=100000;auto_flush_interval=200;")) {
            long t0 = System.nanoTime();
            for (int i = 0; i < 3; i++) {
                s.table("t").longColumn("v", i).atNow();
                Thread.sleep(40);
            }
            Assert.assertEquals("no row has crossed the 200 ms interval yet", 0, messages.get());
            while (System.nanoTime() - t0 < TimeUnit.MILLISECONDS.toNanos(210)) {
                Thread.sleep(5);
            }
            s.table("t").longColumn("v", 3).atNow(); // the row past the interval flushes
            awaitMessages(messages, 1);
            Assert.assertEquals(1, messages.get());
        }
    }

    @Test
    public void testBurstThenTrickleOvershootsByAtMostSevenRows() throws Exception {
        AtomicInteger messages = new AtomicInteger();
        try (TestWebSocketServer server = startServer(messages);
             Sender s = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                     + ";auto_flush_rows=100000;auto_flush_interval=50;")) {
            for (int i = 0; i < 1000; i++) {
                s.table("t").longColumn("v", i).atNow();
            }
            Assert.assertEquals("a 1000-row burst stays under both thresholds", 0, messages.get());
            Thread.sleep(100);
            // Rows arriving after the interval: the sampled clock may let up to
            // seven of them through before the batch is flushed, never more.
            int rows = 0;
            while (messages.get() == 0 && rows < 8) {
                s.table("t").longColumn("v", 1000 + rows++).atNow();
                Thread.sleep(2);
            }
            awaitMessages(messages, 1);
            Assert.assertTrue("flushed only after " + rows + " trickle rows", rows <= 8);
        }
    }

    private static void awaitMessages(AtomicInteger messages, int n) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (messages.get() < n && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Assert.assertTrue("expected " + n + " message(s), saw " + messages.get(), messages.get() >= n);
    }

    private static TestWebSocketServer startServer(AtomicInteger messages) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            private long seq;

            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                messages.incrementAndGet();
                // STATUS_OK ack: status, cumulative wire sequence, table count.
                byte[] ack = new byte[11];
                ByteBuffer.wrap(ack).order(ByteOrder.LITTLE_ENDIAN).put((byte) 0).putLong(seq++).putShort((short) 0);
                try {
                    client.sendBinary(ack);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }
}
