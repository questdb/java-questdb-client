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

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class QwpQueryClientAuthTimeoutTest {

    @Test(timeout = 10_000)
    public void testAuthTimeoutBoundsConnectAttemptToBlackholeHost() throws Exception {
        ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        int port = listener.getLocalPort();
        List<Socket> held = new ArrayList<>();
        Thread acceptor = new Thread(() -> {
            try {
                while (!listener.isClosed()) {
                    Socket s = listener.accept();
                    synchronized (held) {
                        held.add(s);
                    }
                }
            } catch (Exception ignored) {
            }
        }, "blackhole-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();

        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=127.0.0.1:" + port
                        + ";auth_timeout_ms=300;failover=off;target=any;")) {
            long start = System.currentTimeMillis();
            try {
                client.connect();
                Assert.fail("expected connect to fail with auth_timeout");
            } catch (HttpClientException ex) {
                long elapsed = System.currentTimeMillis() - start;
                Assert.assertTrue(
                        "expected message to mention auth_timeout, got: " + ex.getMessage(),
                        ex.getMessage().contains("auth_timeout"));
                Assert.assertTrue(
                        "auth_timeout=300ms should bail in <5s, took " + elapsed + "ms",
                        elapsed < 5_000);
            }
        } finally {
            listener.close();
            synchronized (held) {
                for (Socket s : held) {
                    try {
                        s.close();
                    } catch (Exception ignored) {
                    }
                }
            }
            acceptor.join(500);
        }
    }
}
