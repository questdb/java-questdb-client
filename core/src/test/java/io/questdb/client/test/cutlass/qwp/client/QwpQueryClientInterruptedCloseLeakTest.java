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

import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * {@link QwpQueryClient#close()} must free the I/O thread's native buffer pool and its WebSocket even
 * when the calling thread arrives carrying an interrupt.
 * <p>
 * {@code Thread.join(long)} throws {@code InterruptedException} the instant the caller's flag is set,
 * without ever checking whether the joined thread exited. Before the fix that turned close()'s I/O-thread
 * join into an immediate throw, taking the "could not join" return and skipping {@code closePool()} and
 * {@code webSocketClient.close()} - a leak with no second chance, because {@code closedFlag} is CAS'd on
 * entry and a pooled worker has already been removed from {@code QueryClientPool.all} by the reap that
 * called it.
 * <p>
 * The path is real rather than theoretical: {@code PoolHousekeeper.stop()} interrupts the housekeeper
 * thread to break a recovery build's credential pull, and that same thread runs {@code
 * queryPool.reapIdle()} immediately afterwards with the flag still set.
 * <p>
 * {@code assertMemoryLeak} is the assertion - it compares native memory per tag around the body, so a
 * skipped {@code closePool()} fails the test. The interrupt-preserved check guards the other half of the
 * contract: taking the flag out of the way must not swallow the caller's cancellation.
 */
public class QwpQueryClientInterruptedCloseLeakTest {

    @Test(timeout = 30_000)
    public void testCloseFreesNativeResourcesWhenTheCallerCarriesAnInterrupt() throws Exception {
        try {
            assertMemoryLeak(() -> {
                TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
                });
                server.setSendServerInfo(true);
                try {
                    server.start();
                    Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                    QwpQueryClient client = QwpQueryClient.fromConfig(
                            "ws::addr=localhost:" + server.getPort() + ";auth_timeout_ms=2000;");
                    try {
                        client.connect();
                        Assert.assertTrue("the client must bind the endpoint, or there is no I/O thread "
                                + "and no buffer pool for this test to observe", client.isConnected());

                        // Arrive at close() already interrupted, exactly as the housekeeper does after
                        // stop() escalates.
                        Thread.currentThread().interrupt();
                    } finally {
                        client.close();
                    }

                    Assert.assertTrue("close() must hand the caller's cancellation back, not swallow it",
                            Thread.currentThread().isInterrupted());
                } finally {
                    server.close();
                }
            });
        } finally {
            // Never let the flag escape into the next test on this thread.
            Thread.interrupted();
        }
    }
}
