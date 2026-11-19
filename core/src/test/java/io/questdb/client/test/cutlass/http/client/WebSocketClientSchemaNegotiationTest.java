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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketReadinessWaiter;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

public class WebSocketClientSchemaNegotiationTest {
    @Test
    public void testDefaultDoesNotRequestAndIgnoresUnsolicitedConfirmation() throws Exception {
        ScriptedSocket socket = new ScriptedSocket("X-QWP-Schema: enabled\r\n");
        try (TestClient client = new TestClient(socket)) {
            client.connect("localhost", 9000);
            client.upgrade("/write/v4", 1000, null);
            Assert.assertFalse(socket.request().contains("X-QWP-Request-Schema:"));
            Assert.assertFalse(client.isQwpSchemaEnabled());
        }
    }

    @Test
    public void testRequestedConfirmationAndAbsentLegacyResponse() throws Exception {
        ScriptedSocket socket = new ScriptedSocket("X-QWP-Schema: EnAbLeD\r\n", "");
        try (TestClient client = new TestClient(socket)) {
            client.requestQwpSchema();
            client.connect("localhost", 9000);
            client.upgrade("/write/v4", 1000, null);
            Assert.assertTrue(socket.request().contains("X-QWP-Request-Schema: true\r\n"));
            Assert.assertTrue(client.isQwpSchemaEnabled());

            client.disconnect();
            Assert.assertFalse("disconnect must clear per-connection confirmation", client.isQwpSchemaEnabled());
            socket.nextResponse();
            client.connect("localhost", 9000);
            client.upgrade("/write/v4", 1000, null);
            Assert.assertTrue("the one-way request survives reuse", socket.request().contains("X-QWP-Request-Schema: true\r\n"));
            Assert.assertFalse("an absent response must not retain stale state", client.isQwpSchemaEnabled());
        }
    }

    @Test
    public void testRejectsInvalidEmptyAndDuplicateConfirmations() throws Exception {
        assertRejected("X-QWP-Schema: disabled\r\n", "Invalid X-QWP-Schema");
        assertRejected("X-QWP-Schema: \r\n", "Invalid X-QWP-Schema");
        assertRejected("X-QWP-Schema: enabled\r\nX-QWP-Schema: enabled\r\n", "Duplicate X-QWP-Schema");
    }

    @Test
    public void testDoesNotAcceptPrefixedOrValueSpoofHeaders() throws Exception {
        String[] headers = {
                "Not-X-QWP-Schema: enabled\r\n",
                "X-Comment: X-QWP-Schema: enabled\r\n"
        };
        for (String header : headers) {
            ScriptedSocket socket = new ScriptedSocket(header);
            try (TestClient client = new TestClient(socket)) {
                client.requestQwpSchema();
                client.connect("localhost", 9000);
                client.upgrade("/write/v4", 1000, null);
                Assert.assertFalse(client.isQwpSchemaEnabled());
            }
        }
    }

    private static void assertRejected(String headers, String message) throws Exception {
        ScriptedSocket socket = new ScriptedSocket(headers);
        try (TestClient client = new TestClient(socket)) {
            client.requestQwpSchema();
            client.connect("localhost", 9000);
            try {
                client.upgrade("/write/v4", 1000, null);
                Assert.fail("expected invalid schema confirmation");
            } catch (HttpClientException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains(message));
            }
            Assert.assertFalse(client.isQwpSchemaEnabled());
        }
    }

    private static final class ScriptedSocket implements Socket {
        private final String[] extraHeaders;
        private int responseIndex;
        private byte[] response;
        private int responsePosition;
        private String request;

        private ScriptedSocket(String... extraHeaders) {
            this.extraHeaders = extraHeaders;
        }

        @Override
        public void close() {
        }

        @Override
        public int getFd() {
            return 1;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        void nextResponse() {
            responseIndex++;
            response = null;
            responsePosition = 0;
        }

        @Override
        public void of(int fd) {
        }

        String request() {
            return request;
        }

        @Override
        public int recv(long bufferPtr, int bufferLen) {
            if (response == null || responsePosition == response.length) {
                return 0;
            }
            int count = Math.min(bufferLen, response.length - responsePosition);
            for (int i = 0; i < count; i++) {
                Unsafe.getUnsafe().putByte(bufferPtr + i, response[responsePosition++]);
            }
            return count;
        }

        @Override
        public int send(long bufferPtr, int bufferLen) {
            byte[] bytes = new byte[bufferLen];
            for (int i = 0; i < bufferLen; i++) bytes[i] = Unsafe.getUnsafe().getByte(bufferPtr + i);
            request = new String(bytes, StandardCharsets.US_ASCII);
            String key = header(request, "Sec-WebSocket-Key:");
            response = response(key, extraHeaders[responseIndex]);
            responsePosition = 0;
            return bufferLen;
        }

        @Override
        public void startTlsSession(CharSequence peerName, SocketReadinessWaiter waiter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean supportsTls() {
            return false;
        }

        @Override
        public int tlsIO(int readinessFlags) {
            return 0;
        }

        @Override
        public boolean wantsTlsWrite() {
            return false;
        }

        private static String header(String request, String name) {
            int start = request.indexOf(name) + name.length();
            int end = request.indexOf("\r\n", start);
            return request.substring(start, end).trim();
        }

        private static byte[] response(String key, String extraHeaders) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                String accept = Base64.getEncoder().encodeToString(sha1.digest(
                        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").getBytes(StandardCharsets.US_ASCII)));
                return ("HTTP/1.1 101 Switching Protocols\r\n"
                        + "Upgrade: websocket\r\n"
                        + "Connection: Upgrade\r\n"
                        + "Sec-WebSocket-Accept: " + accept + "\r\n"
                        + extraHeaders
                        + "\r\n").getBytes(StandardCharsets.US_ASCII);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }

    private static final class TestClient extends WebSocketClient {
        private TestClient(ScriptedSocket socket) {
            super(DefaultHttpClientConfiguration.INSTANCE, (nf, log) -> socket);
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
