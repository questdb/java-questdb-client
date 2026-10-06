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

package io.questdb.client.azure.test;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A local stand-in for the Azure instance metadata service (IMDS), the managed-identity endpoint. Azure Identity
 * is pointed at it with {@code AZURE_POD_IDENTITY_AUTHORITY_HOST}. Until {@link #start()} its port refuses
 * connections, like an unreachable endpoint. A raw socket server: the JDK's HTTP server rejects the
 * {@code //metadata/...} request target MSAL sends.
 */
final class ImdsStub implements Closeable {
    private static final InetAddress LOOPBACK = loopback();
    private final Answer answer;
    private final int port;
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private boolean closed; // guarded by this
    private ServerSocket server; // guarded by this

    ImdsStub(Answer answer) throws IOException {
        this.answer = answer;
        try (ServerSocket probe = new ServerSocket(0, 50, LOOPBACK)) {
            this.port = probe.getLocalPort();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        if (server != null) {
            server.close();
        }
    }

    /**
     * The endpoint for {@code AZURE_POD_IDENTITY_AUTHORITY_HOST}.
     */
    String endpoint() {
        return "http://127.0.0.1:" + port;
    }

    /**
     * Starts answering. Idempotent; does nothing once closed.
     */
    synchronized void start() throws IOException {
        if (closed || server != null) {
            return;
        }
        final ServerSocket s = new ServerSocket();
        s.setReuseAddress(true);
        s.bind(new InetSocketAddress(LOOPBACK, port), 50);
        server = s;
        final Thread acceptor = new Thread(() -> accept(s), "imds-stub");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    /**
     * The token requests answered so far; a connection that asked nothing is not one.
     */
    int tokenRequests() {
        return tokenRequests.get();
    }

    private static InetAddress loopback() {
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String readHead(InputStream in) throws IOException {
        final ByteArrayOutputStream head = new ByteArrayOutputStream();
        final byte[] buf = new byte[4096];
        try {
            int n;
            while ((n = in.read(buf)) > 0) {
                head.write(buf, 0, n);
                if (new String(head.toByteArray(), StandardCharsets.ISO_8859_1).contains("\r\n\r\n")) {
                    break;
                }
            }
        } catch (SocketTimeoutException ignore) {
            // a connection that asks nothing
        }
        return new String(head.toByteArray(), StandardCharsets.ISO_8859_1);
    }

    // The resource parameter of the request line, echoed in the token response.
    private static String resourceOf(String requestLine) throws UnsupportedEncodingException {
        final int at = requestLine.indexOf("resource=");
        if (at < 0) {
            return "unknown";
        }
        String value = requestLine.substring(at + "resource=".length());
        for (char end : new char[]{'&', ' '}) {
            final int i = value.indexOf(end);
            if (i >= 0) {
                value = value.substring(0, i);
            }
        }
        return URLDecoder.decode(value, "UTF-8");
    }

    private void accept(ServerSocket s) {
        while (!s.isClosed()) {
            final Socket socket;
            try {
                socket = s.accept();
            } catch (IOException e) {
                return;
            }
            final Thread handler = new Thread(() -> answer(socket), "imds-stub-connection");
            handler.setDaemon(true);
            handler.start();
        }
    }

    private void answer(Socket socket) {
        try (Socket sock = socket) {
            sock.setSoTimeout(5_000);
            final String head = readHead(sock.getInputStream());
            if (head.isEmpty()) {
                return;
            }
            tokenRequests.incrementAndGet();
            final int eol = head.indexOf('\r');
            final String requestLine = eol < 0 ? head : head.substring(0, eol);
            final int status;
            final String body;
            if (answer == Answer.TOKEN) {
                final long now = System.currentTimeMillis() / 1000;
                status = 200;
                body = "{\"access_token\":\"stub-imds-token\",\"client_id\":\"00000000-0000-0000-0000-000000000001\","
                        + "\"expires_in\":\"3599\",\"expires_on\":\"" + (now + 3599) + "\",\"ext_expires_in\":\"3599\","
                        + "\"not_before\":\"" + now + "\",\"resource\":\"" + resourceOf(requestLine) + "\","
                        + "\"token_type\":\"Bearer\"}";
            } else {
                status = 400;
                body = "{\"error\":\"invalid_request\",\"error_description\":\"Identity not found\"}";
            }
            final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            final OutputStream out = sock.getOutputStream();
            out.write(("HTTP/1.1 " + status + (status == 200 ? " OK" : " Bad Request")
                    + "\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: " + bytes.length
                    + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
            out.write(bytes);
            out.flush();
        } catch (IOException ignore) {
            // the client went away
        }
    }

    enum Answer {
        /**
         * A token for the requested resource.
         */
        TOKEN,
        /**
         * HTTP 400 "Identity not found": the identity is not assigned to this host.
         */
        IDENTITY_NOT_FOUND
    }
}
