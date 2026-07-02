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

package io.questdb.client.test.impl;

import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryClientPoolLeakTest {

    // QwpBindValues holds a NativeBufferWriter whose default buffer is
    // 8192 bytes tagged NATIVE_DEFAULT. It is allocated at QwpQueryClient
    // field-init time, which means QwpQueryClient.fromConfig() commits this
    // allocation before connect() is ever called.
    //
    // Both of these tests construct a scenario where connect() reliably throws:
    // a FakeStatusServer returns HTTP 421 with X-QuestDB-Role: REPLICA, and the
    // client requests target=primary. The walk rejects the only endpoint and
    // connect() throws HttpClientException.
    //
    // If QueryClientPool.createUnlocked() does not close the half-built client
    // when connect() throws, the NATIVE_DEFAULT counter ends higher than it
    // started. The assertion compares before/after directly: no leak means
    // delta == 0.

    @Test(timeout = 10_000)
    public void acquireDoesNotLeakNativeScratchOnConnectFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (FakeStatusServer rejecter = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA")) {
                rejecter.start();
                String cfg = "ws::addr=127.0.0.1:" + rejecter.port()
                        + ";target=primary;failover=off;auth_timeout_ms=1000;";

                QueryClientPool pool = new QueryClientPool(
                        cfg, 0, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
                try {
                    long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                    try {
                        pool.acquire();
                        Assert.fail("expected acquire() to throw on connect rejection");
                    } catch (RuntimeException expected) {
                        // QueryException wrapping the underlying connect failure.
                    }
                    long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                    Assert.assertEquals(
                            "acquire() leaked NATIVE_DEFAULT bytes on connect failure",
                            baseline, after);
                } finally {
                    pool.close();
                }
            }
        });
    }

    @Test(timeout = 10_000)
    public void preWarmDoesNotLeakNativeScratchOnConnectFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (FakeStatusServer rejecter = new FakeStatusServer(421, "X-QuestDB-Role: REPLICA")) {
                rejecter.start();
                String cfg = "ws::addr=127.0.0.1:" + rejecter.port()
                        + ";target=primary;failover=off;auth_timeout_ms=1000;";

                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                try {
                    new QueryClientPool(cfg, 1, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
                    Assert.fail("expected QueryClientPool ctor to throw on connect rejection");
                } catch (RuntimeException expected) {
                    // target=primary against role=REPLICA yields a connect failure
                    // out of createUnlocked().
                }
                long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                Assert.assertEquals(
                        "pool ctor leaked NATIVE_DEFAULT bytes on connect failure",
                        baseline, after);
            }
        });
    }

    private static final class FakeStatusServer implements AutoCloseable {
        final AtomicInteger connections = new AtomicInteger();
        private final String roleHeader;
        private final ServerSocket socket;
        private final int statusCode;
        private volatile boolean running = true;

        FakeStatusServer(int statusCode, String roleHeader) throws IOException {
            this.statusCode = statusCode;
            this.roleHeader = roleHeader;
            this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        }

        @Override
        public void close() throws IOException {
            running = false;
            socket.close();
        }

        int port() {
            return socket.getLocalPort();
        }

        void start() {
            Thread t = new Thread(this::loop, "fake-status-" + statusCode);
            t.setDaemon(true);
            t.start();
        }

        private void handle(Socket s) {
            try (Socket sock = s) {
                connections.incrementAndGet();
                byte[] discard = new byte[8192];
                int n = sock.getInputStream().read(discard);
                if (n < 0) return;
                StringBuilder resp = new StringBuilder();
                resp.append("HTTP/1.1 ").append(statusCode).append(' ').append(reason(statusCode)).append("\r\n");
                if (roleHeader != null) {
                    resp.append(roleHeader).append("\r\n");
                }
                resp.append("Content-Length: 0\r\nConnection: close\r\n\r\n");
                OutputStream out = sock.getOutputStream();
                out.write(resp.toString().getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (Exception ignored) {
            }
        }

        private void loop() {
            while (running) {
                try {
                    Socket s = socket.accept();
                    Thread h = new Thread(() -> handle(s), "fake-status-handler-" + statusCode);
                    h.setDaemon(true);
                    h.start();
                } catch (IOException e) {
                    if (!running) return;
                }
            }
        }

        private static String reason(int code) {
            switch (code) {
                case 401:
                    return "Unauthorized";
                case 421:
                    return "Misdirected Request";
                default:
                    return "Status";
            }
        }
    }
}
