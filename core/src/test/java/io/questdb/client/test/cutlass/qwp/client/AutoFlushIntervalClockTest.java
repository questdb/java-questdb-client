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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Pins how often the WebSocket sender reads its clock on the row path, via the
 * injectable auto-flush clock, and that interval-based auto-flush still fires.
 */
public class AutoFlushIntervalClockTest {
    private static final int N = 1000;

    @Test
    public void testDefaultIntervalWhenKeyAbsentAndZeroWhenOff() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startAckingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    Assert.assertEquals("ws:: without auto_flush_interval keeps the 100 ms default",
                            TimeUnit.MILLISECONDS.toNanos(100),
                            ((QwpWebSocketSender) sender).getAutoFlushIntervalNanos());
                }
                try (Sender sender = Sender.fromConfig(cfg(server) + "auto_flush_interval=off;")) {
                    Assert.assertEquals(0L, ((QwpWebSocketSender) sender).getAutoFlushIntervalNanos());
                }
            }
        });
    }

    @Test
    public void testIntervalFlushesOnSecondRowOfAgedBatch() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startAckingServer();
                 Sender sender = Sender.fromConfig(cfg(server) + "auto_flush_rows=1000000;auto_flush_interval=100;")) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                AtomicLong now = new AtomicLong();
                ws.setNanoClockForTesting(now::get);
                long fsn0 = ws.getCursorEngineForTesting().publishedFsn();

                sender.table("t").longColumn("v", 1L).atNow();
                Assert.assertEquals("first row must stay buffered",
                        fsn0, ws.getCursorEngineForTesting().publishedFsn());

                now.addAndGet(TimeUnit.MILLISECONDS.toNanos(200));
                sender.table("t").longColumn("v", 2L).atNow();
                Assert.assertTrue("batch older than the interval must auto-flush on its second row",
                        ws.getCursorEngineForTesting().publishedFsn() > fsn0);
            }
        });
    }

    @Test
    public void testIntervalOffReadsNoClock() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startAckingServer();
                 Sender sender = Sender.fromConfig(cfg(server) + "auto_flush_interval=off;")) {
                AtomicLong reads = new AtomicLong();
                ((QwpWebSocketSender) sender).setNanoClockForTesting(() -> reads.incrementAndGet() * 1_000L);
                for (int i = 0; i < N; i++) {
                    sender.table("t").longColumn("v", i).atNow();
                    sender.flush();
                }
                Assert.assertEquals("auto_flush_interval=off must not read the clock on the row path",
                        0L, reads.get());
            }
        });
    }

    @Test
    public void testIntervalOffKeepsRowCapAndExplicitFlush() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startAckingServer();
                 Sender sender = Sender.fromConfig(cfg(server) + "auto_flush_interval=off;auto_flush_rows=10;")) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                long fsn0 = ws.getCursorEngineForTesting().publishedFsn();
                for (int i = 0; i < 9; i++) {
                    sender.table("t").longColumn("v", i).atNow();
                }
                Assert.assertEquals("9 rows stay below the row cap",
                        fsn0, ws.getCursorEngineForTesting().publishedFsn());
                sender.table("t").longColumn("v", 9L).atNow();
                long fsn1 = ws.getCursorEngineForTesting().publishedFsn();
                Assert.assertTrue("the 10th row must trigger the row-cap flush", fsn1 > fsn0);

                for (int i = 0; i < 3; i++) {
                    sender.table("t").longColumn("v", i).atNow();
                }
                Assert.assertEquals(fsn1, ws.getCursorEngineForTesting().publishedFsn());
                sender.flush();
                Assert.assertTrue("explicit flush() must send the partial batch",
                        ws.getCursorEngineForTesting().publishedFsn() > fsn1);
            }
        });
    }

    @Test
    public void testOneClockReadPerSingleRowFlush() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startAckingServer();
                 Sender sender = Sender.fromConfig(cfg(server) + "auto_flush_rows=1000000;auto_flush_interval=100;")) {
                AtomicLong reads = new AtomicLong();
                ((QwpWebSocketSender) sender).setNanoClockForTesting(() -> reads.incrementAndGet() * 1_000L);
                for (int i = 0; i < N; i++) {
                    sender.table("t").longColumn("v", i).atNow();
                    sender.flush();
                }
                Assert.assertEquals("one clock read (the batch timestamp) per single-row flush",
                        N, reads.get());
            }
        });
    }

    private static String cfg(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";";
    }

    private static TestWebSocketServer startAckingServer() throws Exception {
        AtomicLong seq = new AtomicLong();
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                try {
                    client.sendBinary(CloseDrainTest.buildAck(seq.getAndIncrement()));
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
