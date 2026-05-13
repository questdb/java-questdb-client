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
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class QwpQueryClientUpgradeStatusTest {

    @Test(timeout = 10_000)
    public void test401_classifiedAsAuthFailed_terminal() throws Exception {
        runOneShotStatus(401, true);
    }

    @Test(timeout = 10_000)
    public void test403_classifiedAsAuthFailed_terminal() throws Exception {
        runOneShotStatus(403, true);
    }

    @Test(timeout = 10_000)
    public void test404_NOT_classifiedAsAuthFailed() throws Exception {
        // 404 indicates path mismatch (a single mid-deploy node may 404 while
        // peers are healthy). It must NOT short-circuit failover.
        runOneShotStatus(404, false);
    }

    private static void runOneShotStatus(int statusCode, boolean expectAuthFailed) throws Exception {
        ServerSocket listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        int port = listener.getLocalPort();
        AtomicBoolean accepted = new AtomicBoolean();
        String response = "HTTP/1.1 " + statusCode + ' ' + reason(statusCode) + "\r\n"
                + "Content-Length: 0\r\n\r\n";
        byte[] respBytes = response.getBytes(StandardCharsets.US_ASCII);
        Thread serverThread = new Thread(() -> {
            while (!listener.isClosed()) {
                try {
                    Socket s = listener.accept();
                    accepted.set(true);
                    Thread t = new Thread(() -> {
                        try (Socket sock = s) {
                            byte[] buf = new byte[8192];
                            int n = sock.getInputStream().read(buf);
                            if (n < 0) return;
                            OutputStream os = sock.getOutputStream();
                            os.write(respBytes);
                            os.flush();
                        } catch (Exception ignored) {
                        }
                    }, "fake-status-handler-" + statusCode);
                    t.setDaemon(true);
                    t.start();
                } catch (Exception ignored) {
                    return;
                }
            }
        }, "fake-status-" + statusCode);
        serverThread.setDaemon(true);
        serverThread.start();

        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=127.0.0.1:" + port + ";failover=off;target=any;")) {
            try {
                client.connect();
                Assert.fail("expected connect to fail with HTTP " + statusCode);
            } catch (QwpAuthFailedException ae) {
                if (!expectAuthFailed) {
                    Assert.fail("status " + statusCode + " should NOT be auth-failed; got: " + ae.getMessage());
                }
                Assert.assertEquals(statusCode, ae.getStatusCode());
            } catch (HttpClientException ex) {
                if (expectAuthFailed) {
                    Assert.fail("status " + statusCode + " should be auth-failed; got generic: " + ex.getMessage());
                }
            }
            Assert.assertTrue("server never accepted a connection", accepted.get());
        } finally {
            listener.close();
            serverThread.join(500);
        }
    }

    private static String reason(int code) {
        switch (code) {
            case 401:
                return "Unauthorized";
            case 403:
                return "Forbidden";
            case 404:
                return "Not Found";
            default:
                return "Status";
        }
    }
}
