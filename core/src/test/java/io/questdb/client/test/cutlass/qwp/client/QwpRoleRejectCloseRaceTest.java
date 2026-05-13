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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class QwpRoleRejectCloseRaceTest {

    @Test(timeout = 15_000)
    public void testCloseDuringRoleRejectBackoffReturnsPromptly() throws Exception {
        try (RoleRejectServer server = new RoleRejectServer()) {
            server.start();

            String cfg = "ws::addr=127.0.0.1:" + server.port()
                    + ";reconnect_initial_backoff_millis=4000"
                    + ";reconnect_max_backoff_millis=4000"
                    + ";auth_timeout_ms=2000"
                    + ";auto_flush_rows=1"
                    + ";close_flush_timeout_millis=0"
                    + ";initial_connect_retry=async;";

            Sender sender = Sender.fromConfig(cfg);
            long elapsed;
            try {
                // Push a row so the I/O thread starts attempting connect; the
                // first attempt will hit the role reject and enter the parkNanos
                // backoff branch.
                sender.table("t").longColumn("v", 1L).atNow();
                waitFor(() -> server.upgrades.get() >= 1, 5_000);
                Thread.sleep(100);
            } finally {
                // Bracket the close() so the timing assertion is meaningful;
                // the race this test is named for lives entirely inside close().
                long start = System.currentTimeMillis();
                sender.close();
                elapsed = System.currentTimeMillis() - start;
            }
            Assert.assertTrue(
                    "close() during role-reject backoff must return promptly (got " + elapsed + "ms)",
                    elapsed < 2_000);
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(20);
        }
    }

    private static final class RoleRejectServer implements AutoCloseable {
        final AtomicInteger upgrades = new AtomicInteger();
        private final ServerSocket socket;
        private volatile boolean running = true;

        RoleRejectServer() throws IOException {
            this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        }

        int port() {
            return socket.getLocalPort();
        }

        void start() {
            Thread t = new Thread(this::loop, "role-reject-server");
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void close() throws IOException {
            running = false;
            socket.close();
        }

        private void loop() {
            while (running) {
                try {
                    Socket s = socket.accept();
                    Thread h = new Thread(() -> handle(s), "role-reject-handler");
                    h.setDaemon(true);
                    h.start();
                } catch (IOException e) {
                    if (!running) return;
                }
            }
        }

        private void handle(Socket s) {
            try (Socket sock = s) {
                byte[] discard = new byte[8192];
                int n = sock.getInputStream().read(discard);
                if (n < 0) return;
                upgrades.incrementAndGet();
                String resp = "HTTP/1.1 421 Misdirected Request\r\n"
                        + "X-QuestDB-Role: PRIMARY_CATCHUP\r\n"
                        + "Content-Length: 0\r\nConnection: close\r\n\r\n";
                OutputStream out = sock.getOutputStream();
                out.write(resp.getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (Exception ignored) {
            }
        }
    }
}
