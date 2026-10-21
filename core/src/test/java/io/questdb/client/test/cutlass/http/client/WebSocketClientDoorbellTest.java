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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The platform transport's idle wait against a real socket: it blocks for the
 * requested time, a cross-thread {@link WebSocketClient#wake()} ends it at
 * once, a wake that lands before the wait is not lost, a drained wake does not
 * linger, and inbound bytes end it too. Exercises whichever doorbell the build
 * platform uses (eventfd, kqueue EVFILT_USER or a Windows Event).
 */
public class WebSocketClientDoorbellTest {

    @Test
    public void testDoorbell() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            })) {
                server.start();
                assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
                    client.connect("127.0.0.1", server.getPort());
                    client.upgrade("/write/v4", null);

                    // 1. Nothing happens: the wait runs to its timeout.
                    long t0 = System.nanoTime();
                    assertFalse(client.awaitReadableOrWake(150));
                    long waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertTrue("wait returned early after " + waited + " ms", waited >= 120);

                    // 2. A wake from another thread ends a long wait promptly.
                    Thread ringer = new Thread(() -> {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ignored) {
                        }
                        client.wake();
                    }, "ringer");
                    ringer.start();
                    t0 = System.nanoTime();
                    assertTrue(client.awaitReadableOrWake(5_000));
                    waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    ringer.join();
                    assertTrue("wake took " + waited + " ms", waited >= 80 && waited < 2_000);

                    // 3. A wake before the wait is remembered (stateful doorbell).
                    client.wake();
                    t0 = System.nanoTime();
                    assertTrue(client.awaitReadableOrWake(5_000));
                    waited = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                    assertTrue("pre-rung wait took " + waited + " ms", waited < 500);

                    // 4. ...and once consumed it does not linger.
                    assertFalse(client.awaitReadableOrWake(100));

                    // 5. Inbound bytes end the wait: the server answers a PING with a PONG.
                    client.sendPing();
                    assertTrue("socket readability did not end the wait", client.awaitReadableOrWake(5_000));

                    // 6. wake() on a closed client is a safe no-op.
                    client.close();
                    client.wake();
                }
            }
        });
    }
}
