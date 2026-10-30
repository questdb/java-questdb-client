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
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.ProgressNotifier;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The producer-side ack waits park on the sender's ProgressNotifier until the
 * I/O thread reports progress, instead of polling every 50 us.
 */
public class AwaitAckedFsnWakeTest {

    @Test
    public void testAwaitAckedFsnWakesOnTheAckWithoutPolling() throws Exception {
        CloseDrainTest.DelayingAckHandler handler = new CloseDrainTest.DelayingAckHandler(300L);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + server.getPort() + ";close_flush_timeout_millis=10000;";
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg)) {
                sender.table("t").longColumn("v", 1).atNow();
                long fsn = sender.flushAndGetSequence();
                ProgressNotifier notifier = sender.progressNotifierForTest();
                long calls0 = notifier.awaitCount();
                long t0 = System.nanoTime();
                Assert.assertTrue(sender.awaitAckedFsn(fsn, 10_000L));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                Assert.assertTrue("waited for the delayed ack: " + elapsedMs, elapsedMs >= 150L);
                long calls = notifier.awaitCount() - calls0;
                // 300 ms / 10 ms cap = 30; a 50 us poll would make ~6000
                Assert.assertTrue("polled instead of parking: " + calls, calls <= 40L);
            }
        }
    }

    @Test
    public void testCloseDrainWakesOnTheAckWithoutPolling() throws Exception {
        CloseDrainTest.DelayingAckHandler handler = new CloseDrainTest.DelayingAckHandler(500L);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + server.getPort() + ";close_flush_timeout_millis=10000;";
            QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg);
            try {
                sender.table("t").longColumn("v", 1).atNow();
                sender.flush();
                ProgressNotifier notifier = sender.progressNotifierForTest();
                long calls0 = notifier.awaitCount();
                sender.close();
                Assert.assertTrue(handler.nextSeq.get() >= 1L);
                long calls = notifier.awaitCount() - calls0;
                Assert.assertTrue("polled instead of parking: " + calls, calls <= 60L);
            } finally {
                sender.close();
            }
        }
    }

    @Test
    public void testUnrelatedAcksDoNotExtendTheTimeout() throws Exception {
        FirstAckOnlyHandler handler = new FirstAckOnlyHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + server.getPort() + ";close_flush_timeout_millis=0;";
            try (QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg)) {
                sender.table("t").longColumn("v", 1).atNow();
                long fsn0 = sender.flushAndGetSequence();
                Assert.assertTrue(sender.awaitAckedFsn(fsn0, 5_000L));
                sender.table("t").longColumn("v", 2).atNow();
                long fsn1 = sender.flushAndGetSequence();
                long t0 = System.nanoTime();
                Assert.assertFalse(sender.awaitAckedFsn(fsn1, 200L));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                Assert.assertTrue("returned early: " + elapsedMs, elapsedMs >= 200L);
                Assert.assertTrue("overshot: " + elapsedMs, elapsedMs < 2_000L);
            }
        }
    }

    /**
     * Acks the first frame at once and never acks a later one.
     */
    private static class FirstAckOnlyHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (nextSeq.getAndIncrement() == 0) {
                try {
                    client.sendBinary(CloseDrainTest.buildAck(0));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
