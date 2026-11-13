/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * {@code Long.MAX_VALUE} as a wait budget means "no limit". It once overflowed an
 * absolute nanoTime deadline, so the waits below returned at once instead.
 */
public class QwpWebSocketSenderUnboundedWaitTest {

    @Test(timeout = 20_000)
    public void testCloseFlushWaitsWithoutLimit() throws Exception {
        assertMemoryLeak(() -> {
            HeldAckHandler handler = new HeldAckHandler();
            try (TestWebSocketServer server = start(handler)) {
                Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";schema_mode=off;close_flush_timeout_millis=" + Long.MAX_VALUE + ";");
                sender.table("t").longColumn("v", 1).atNow();
                sender.flush();
                Assert.assertTrue(handler.received.await(5, TimeUnit.SECONDS));
                AtomicReference<Throwable> error = new AtomicReference<>();
                Thread closer = new Thread(() -> {
                    try {
                        sender.close();
                    } catch (Throwable t) {
                        error.set(t);
                    }
                }, "unbounded-close");
                closer.start();
                try {
                    closer.join(200);
                    Assert.assertTrue("close() must wait for the ACK", closer.isAlive());
                } finally {
                    handler.release.countDown();
                    closer.join(10_000);
                }
                Assert.assertFalse(closer.isAlive());
                Assert.assertNull(error.get());
                Assert.assertTrue(handler.acked.get());
            }
        });
    }

    @Test(timeout = 20_000)
    public void testDrainWaitsWithoutLimit() throws Exception {
        assertMemoryLeak(() -> {
            HeldAckHandler handler = new HeldAckHandler();
            try (TestWebSocketServer server = start(handler);
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";schema_mode=off;close_flush_timeout_millis=0;")) {
                sender.table("t").longColumn("v", 1).atNow();
                AtomicReference<Boolean> drained = new AtomicReference<>();
                AtomicReference<Throwable> error = new AtomicReference<>();
                Thread drainer = new Thread(() -> {
                    try {
                        drained.set(sender.drain(Long.MAX_VALUE));
                    } catch (Throwable t) {
                        error.set(t);
                    }
                }, "unbounded-drain");
                drainer.start();
                try {
                    Assert.assertTrue(handler.received.await(5, TimeUnit.SECONDS));
                    drainer.join(200);
                    Assert.assertTrue("drain() must wait for the ACK", drainer.isAlive());
                } finally {
                    handler.release.countDown();
                    drainer.join(10_000);
                }
                Assert.assertFalse(drainer.isAlive());
                Assert.assertNull(error.get());
                Assert.assertEquals(Boolean.TRUE, drained.get());
            }
        });
    }

    private static TestWebSocketServer start(HeldAckHandler handler) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        try {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            return server;
        } catch (Throwable t) {
            server.close();
            throw t;
        }
    }

    // Holds the ACK for the first data frame until the test releases it.
    private static final class HeldAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicBoolean acked = new AtomicBoolean();
        private final CountDownLatch received = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private long sequence;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            received.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    return;
                }
                client.sendBinary(QwpWireTestUtils.buildAck(sequence++));
                acked.set(true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
    }
}
