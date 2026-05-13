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

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class WebSocketClient421RoleHeaderTest {

    @Test(timeout = 10_000)
    public void testReplicaReject() throws Exception {
        assertCapturedRole("REPLICA", "X-QuestDB-Role: REPLICA");
    }

    @Test(timeout = 10_000)
    public void testPrimaryCatchupReject() throws Exception {
        assertCapturedRole("PRIMARY_CATCHUP", "X-QuestDB-Role: PRIMARY_CATCHUP");
    }

    @Test(timeout = 10_000)
    public void testRoleHeaderAbsentOn421_NullCaptured() throws Exception {
        assertCapturedRole(null, null);
    }

    @Test(timeout = 10_000)
    public void testCaseInsensitiveHeaderName() throws Exception {
        assertCapturedRole("REPLICA", "x-questdb-role: REPLICA");
    }

    @Test(timeout = 10_000)
    public void testRoleValuePreservesCase() throws Exception {
        // Parser must NOT uppercase the value -- predicates compare case-insensitive.
        assertCapturedRole("replica", "X-QuestDB-Role: replica");
    }

    @Test(timeout = 10_000)
    public void testRoleValueTrailingWhitespaceTrimmed() throws Exception {
        assertCapturedRole("REPLICA", "X-QuestDB-Role:   REPLICA   ");
    }

    @Test(timeout = 10_000)
    public void testRoleValueEmpty_NullCaptured() throws Exception {
        assertCapturedRole(null, "X-QuestDB-Role: ");
    }

    @Test(timeout = 10_000)
    public void testHeaderMatchAnchoredAtLineStart() throws Exception {
        // A response body or another header value containing the literal
        // "X-QuestDB-Role:" must NOT be picked up by the parser. The custom
        // header line below appears as a non-header value -- only headers at
        // CRLF boundaries should match.
        ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        int port = listener.getLocalPort();
        Thread serverThread = new Thread(() -> {
            try (Socket s = listener.accept()) {
                byte[] discardBuf = new byte[8192];
                int n = s.getInputStream().read(discardBuf);
                if (n < 0) return;
                String resp = "HTTP/1.1 421 Misdirected Request\r\n"
                        + "X-Echoed-Header: X-QuestDB-Role: REPLICA\r\n"
                        + "Content-Length: 0\r\n\r\n";
                OutputStream os = s.getOutputStream();
                os.write(resp.getBytes(StandardCharsets.US_ASCII));
                os.flush();
            } catch (Exception ignored) {
            }
        }, "fake-421-smuggle");
        serverThread.setDaemon(true);
        serverThread.start();
        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
            client.setQwpMaxVersion(1);
            client.connect("127.0.0.1", port);
            try {
                client.upgrade("/write/v4", null);
                Assert.fail("expected upgrade to fail");
            } catch (HttpClientException ex) {
                Assert.assertNull(client.getUpgradeRejectRole());
            }
        } finally {
            listener.close();
            serverThread.join(500);
        }
    }

    private static void assertCapturedRole(String expected, String roleHeaderLine) throws Exception {
        ServerSocket listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        int port = listener.getLocalPort();
        Thread serverThread = new Thread(() -> {
            try (Socket s = listener.accept()) {
                byte[] discardBuf = new byte[8192];
                int n = s.getInputStream().read(discardBuf);
                if (n < 0) return;
                StringBuilder resp = new StringBuilder();
                resp.append("HTTP/1.1 421 Misdirected Request\r\n");
                if (roleHeaderLine != null) resp.append(roleHeaderLine).append("\r\n");
                resp.append("Content-Length: 0\r\n\r\n");
                OutputStream os = s.getOutputStream();
                os.write(resp.toString().getBytes(StandardCharsets.US_ASCII));
                os.flush();
            } catch (Exception ignored) {
            }
        }, "fake-421");
        serverThread.setDaemon(true);
        serverThread.start();

        try (WebSocketClient client = WebSocketClientFactory.newPlainTextInstance()) {
            client.setQwpMaxVersion(1);
            client.connect("127.0.0.1", port);
            try {
                client.upgrade("/write/v4", null);
                Assert.fail("expected upgrade to fail with 421");
            } catch (HttpClientException ex) {
                Assert.assertTrue(
                        "expected 'WebSocket upgrade failed:' message, got: " + ex.getMessage(),
                        ex.getMessage().contains("WebSocket upgrade failed:"));
                Assert.assertEquals(expected, client.getUpgradeRejectRole());
            }
        } finally {
            listener.close();
            serverThread.join(500);
        }
    }
}
