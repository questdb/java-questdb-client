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

package io.questdb.client.test;

import io.questdb.client.QuestDB;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.test.cutlass.qwp.client.TestPorts;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proves the ingest-side async callbacks exposed on the {@link QuestDB} facade
 * ({@link io.questdb.client.QuestDBBuilder#errorHandler}/{@code connectionListener})
 * actually reach the pooled {@link io.questdb.client.Sender}s -- not merely the
 * lower-level {@code Sender.builder()}.
 * <p>
 * Each test eagerly prewarms one ingest sender ({@code sender_pool_min=1})
 * pointed at a dead port in {@code initial_connect_retry=async} mode with a
 * tight reconnect budget: the pool's I/O thread exhausts the budget in the
 * background and surfaces the failure through whichever facade-wired callback is
 * under test. No server is required.
 */
public class QuestDBFacadeCallbacksTest {

    private static final TestWebSocketServer.WebSocketServerHandler NOOP_HANDLER =
            new TestWebSocketServer.WebSocketServerHandler() {
                @Override
                public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                }
            };

    @Test
    public void testFacadeConnectionListenerReceivesEvents() throws Exception {
        int port = TestPorts.findUnusedPort();
        CountDownLatch sawEvent = new CountDownLatch(1);
        SenderConnectionListener listener = new SenderConnectionListener() {
            @Override
            public void onEvent(@NotNull SenderConnectionEvent event) {
                sawEvent.countDown();
            }
        };
        try (QuestDB ignored = QuestDB.builder()
                .fromConfig(config(port))
                .connectionListener(listener)
                .build()) {
            Assert.assertTrue(
                    "facade-wired connectionListener must observe at least one connection event",
                    sawEvent.await(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testFacadeErrorHandlerReceivesAsyncIngestError() throws Exception {
        // A 401 server produces a genuine auth terminal that surfaces even in
        // async mode; the facade-wired errorHandler must receive it. (Under
        // Invariant B a mere connection error would retry forever and never
        // surface -- only a genuine terminal like auth does.)
        try (TestWebSocketServer server = new TestWebSocketServer(NOOP_HANDLER)) {
            server.setRejectWithStatus(401, "Unauthorized");
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            ErrorInbox inbox = new ErrorInbox();
            try (QuestDB ignored = QuestDB.builder()
                    .fromConfig(config(server.getPort()))
                    .errorHandler(inbox)
                    .build()) {
                Assert.assertTrue(
                        "facade-wired errorHandler must receive the async auth-terminal SenderError",
                        inbox.await(5, TimeUnit.SECONDS));
                Assert.assertNotNull("a SenderError must be delivered", inbox.get());
            }
        }
    }

    // One cluster config drives both pools. Eagerly prewarm one sender
    // (sender_pool_min=1) so build() exercises the production
    // buildManagedSlotSender path that applies the facade callbacks; async + a
    // tight budget -> the I/O thread fails fast against the dead port.
    // query_pool_min=0 -> the query pool never connects, so the test is isolated
    // to the ingest callbacks.
    private static String config(int port) {
        return "ws::addr=localhost:" + port + ";sender_pool_min=1;sender_pool_max=1"
                + ";query_pool_min=0;query_pool_max=1"
                + ";initial_connect_retry=async;reconnect_max_duration_millis=400"
                + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50"
                + ";close_flush_timeout_millis=0;";
    }

    private static final class ErrorInbox implements SenderErrorHandler {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<SenderError> first = new AtomicReference<>();

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        SenderError get() {
            return first.get();
        }

        @Override
        public void onError(@NotNull SenderError error) {
            first.compareAndSet(null, error);
            latch.countDown();
        }
    }
}
