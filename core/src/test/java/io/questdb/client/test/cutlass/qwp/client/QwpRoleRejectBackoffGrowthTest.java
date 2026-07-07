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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression guard for the foreground role-reject retry storm.
 *
 * <p>When every reachable endpoint role-rejects the {@code /write/v4} upgrade
 * (a genuine all-replica failover window, or a misconfigured address list that
 * points at replicas only), the cursor I/O loop MUST retry with the same
 * capped exponential backoff-with-jitter every other reconnect branch uses --
 * NOT pin at {@code reconnect_initial_backoff_millis} forever. Pinning turned
 * this into a fixed ~10/s storm of fresh TLS handshakes (new
 * {@code WebSocketClient} + new {@code SSLContext} + trust-store re-read) per
 * endpoint, in breach of the documented capped-exponential-backoff contract and
 * asymmetric with the orphan drainer, which already grows to
 * {@code reconnect_max_backoff_millis}.
 *
 * <p>The server here is plaintext loopback, so a role-reject upgrade completes
 * in well under a millisecond and the wall-clock gap between successive attempts
 * is dominated by the backoff park. Under the old fixed-interval bug every gap
 * stayed {@code ~= reconnect_initial_backoff_millis}; under capped exponential
 * backoff a later gap climbs many multiples past it.
 */
public class QwpRoleRejectBackoffGrowthTest {

    @Test(timeout = 30_000)
    public void testRoleRejectRetryUsesCappedExponentialBackoff() throws Exception {
        try (RoleRejectServer server = new RoleRejectServer()) {
            server.start();

            final long initialBackoffMillis = 50;
            String cfg = "ws::addr=127.0.0.1:" + server.port()
                    + ";reconnect_initial_backoff_millis=" + initialBackoffMillis
                    + ";reconnect_max_backoff_millis=4000"
                    + ";auth_timeout_ms=2000"
                    + ";auto_flush_rows=1"
                    + ";close_flush_timeout_millis=0"
                    + ";initial_connect_retry=async;";

            try (Sender sender = Sender.fromConfig(cfg)) {
                // Kick the I/O thread into the connect/role-reject loop.
                sender.table("t").longColumn("v", 1L).atNow();
                // Wait for enough attempts to observe several backoff doublings:
                // the parked gaps run ~50, ~100, ~200, ~400, ~800, ~1600 ms
                // (+jitter). Seven attempts give six gaps up to the ~1600 ms step.
                waitFor(() -> server.attemptNanos.size() >= 7, 25_000);
            }

            Long[] ts = server.attemptNanos.toArray(new Long[0]);
            Assert.assertTrue("expected at least 7 upgrade attempts, got " + ts.length, ts.length >= 7);

            long firstGapMs = (ts[1] - ts[0]) / 1_000_000L;
            long maxGapMs = 0;
            StringBuilder gaps = new StringBuilder();
            for (int i = 1; i < ts.length; i++) {
                long gapMs = (ts[i] - ts[i - 1]) / 1_000_000L;
                gaps.append(gapMs).append(i < ts.length - 1 ? "," : "");
                if (gapMs > maxGapMs) {
                    maxGapMs = gapMs;
                }
            }

            // Under the fixed-interval bug every gap stayed ~= 50 ms (no jitter,
            // no growth) over a sub-millisecond plaintext handshake, so maxGap
            // could never climb past ~60 ms. Capped exponential backoff drives a
            // later gap to 400 ms+ by the fourth doubling. Require maxGap to reach
            // at least 4x the initial interval: unreachable under the old
            // behaviour, comfortably cleared under the new one.
            Assert.assertTrue(
                    "role-reject backoff did not grow (fixed-interval storm): gaps=[" + gaps
                            + "]ms maxGap=" + maxGapMs + "ms firstGap=" + firstGapMs
                            + "ms initial=" + initialBackoffMillis + "ms",
                    maxGapMs >= initialBackoffMillis * 4);
            // And a later gap must dwarf the first, proving genuine growth rather
            // than a single anomalous park.
            Assert.assertTrue(
                    "role-reject gaps are flat, not exponential: gaps=[" + gaps
                            + "]ms maxGap=" + maxGapMs + "ms firstGap=" + firstGapMs + "ms",
                    maxGapMs >= firstGapMs * 3);
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
    }

    private static final class RoleRejectServer implements AutoCloseable {
        final CopyOnWriteArrayList<Long> attemptNanos = new CopyOnWriteArrayList<>();
        private final ServerSocket socket;
        private final AtomicBoolean running = new AtomicBoolean(true);

        RoleRejectServer() throws IOException {
            this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        }

        int port() {
            return socket.getLocalPort();
        }

        void start() {
            Thread t = new Thread(this::loop, "role-reject-backoff-server");
            t.setDaemon(true);
            t.start();
        }

        @Override
        public void close() throws IOException {
            running.set(false);
            socket.close();
        }

        private void loop() {
            while (running.get()) {
                try {
                    Socket s = socket.accept();
                    Thread h = new Thread(() -> handle(s), "role-reject-backoff-handler");
                    h.setDaemon(true);
                    h.start();
                } catch (IOException e) {
                    if (!running.get()) {
                        return;
                    }
                }
            }
        }

        private void handle(Socket s) {
            try (Socket sock = s) {
                byte[] discard = new byte[8192];
                int n = sock.getInputStream().read(discard);
                if (n < 0) {
                    return;
                }
                // Record the attempt only once we have actually read the upgrade
                // request, so the timestamp reflects a real handshake attempt.
                attemptNanos.add(System.nanoTime());
                String resp = "HTTP/1.1 421 Misdirected Request\r\n"
                        + "X-QuestDB-Role: REPLICA\r\n"
                        + "Content-Length: 0\r\nConnection: close\r\n\r\n";
                OutputStream out = sock.getOutputStream();
                out.write(resp.getBytes(StandardCharsets.US_ASCII));
                out.flush();
            } catch (Exception ignored) {
            }
        }
    }
}
