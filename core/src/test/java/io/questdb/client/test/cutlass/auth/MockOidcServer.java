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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.std.str.StringSink;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A minimal HTTP/1.1 server for tests that impersonates an OIDC identity provider (and,
 * when needed, the QuestDB {@code /settings} endpoint). It speaks just enough HTTP to drive
 * {@link io.questdb.client.cutlass.auth.OidcDeviceAuth}: it reads a request, hands the path
 * and body to a {@link Handler}, and writes back a {@code Content-Length}-framed response on
 * a keep-alive connection.
 */
public class MockOidcServer implements Closeable {
    private final Thread acceptThread;
    private final List<Socket> connSockets = Collections.synchronizedList(new ArrayList<>());
    private final List<Thread> connThreads = Collections.synchronizedList(new ArrayList<>());
    private final Handler handler;
    private final List<String> requestAuthHeaders = Collections.synchronizedList(new ArrayList<>());
    private final ServerSocket serverSocket;

    public MockOidcServer(Handler handler) throws IOException {
        this.handler = handler;
        this.serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.acceptThread = new Thread(this::acceptLoop, "mock-oidc-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    public static MockResponse chunkedJson(int status, String body) {
        return new MockResponse(status, body, true);
    }

    public static MockResponse dropConnection() {
        // close the connection without responding, so the client sees a transport failure (connection
        // reset / EOF) on this request - used to simulate an unreachable endpoint that is co-located with
        // a working one on the same mock origin
        MockResponse response = new MockResponse(0, "", false);
        response.dropConnection = true;
        return response;
    }

    public static MockResponse json(int status, String body) {
        return new MockResponse(status, body, false);
    }

    public static MockResponse oversizedJson(long bodyBytes) {
        // stream a chunked body larger than the client's response-size cap (MAX_RESPONSE_BODY_BYTES), so the
        // bounded read aborts on the cap instead of letting a hostile or MITM'd server stream an endless body
        // and wedge the thread. The payload is all whitespace, which the JSON lexer skips, so the byte cap is
        // what trips - not a parse error, and not the lexer's per-value length limit
        MockResponse response = new MockResponse(200, "", true);
        response.oversizedBodyBytes = bodyBytes;
        return response;
    }

    public static MockResponse raw(String rawResponse) {
        // write the supplied bytes verbatim as the whole HTTP response, so a test can craft a malformed
        // status line (for example a status code carrying control bytes) that the int-typed status factories
        // cannot express
        MockResponse response = new MockResponse(0, "", false);
        response.rawResponse = rawResponse;
        return response;
    }

    public static MockResponse stall() {
        MockResponse response = new MockResponse(200, "", true);
        response.stall = true;
        return response;
    }

    @Override
    public void close() throws IOException {
        // tear the server down deterministically so a test's threads are gone before its assertions (and
        // assertMemoryLeak's native-memory check) run, instead of lingering as daemon threads that can
        // perturb a later test: stop accepting, drop every connection (which unblocks a handler reading a
        // socket), then interrupt and join the accept and connection threads (interrupt wakes a stalled
        // handler that is sleeping on the response body)
        serverSocket.close();
        synchronized (connSockets) {
            for (Socket s : connSockets) {
                try {
                    s.close();
                } catch (IOException ignore) {
                    // already closed
                }
            }
        }
        interruptAndJoin(acceptThread);
        synchronized (connThreads) {
            for (Thread t : connThreads) {
                interruptAndJoin(t);
            }
        }
    }

    public String httpUrl(String path) {
        return "http://127.0.0.1:" + port() + path;
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    public List<String> requestAuthHeaders() {
        return requestAuthHeaders;
    }

    private static void interruptAndJoin(Thread t) {
        t.interrupt();
        try {
            t.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringSink sb = new StringSink();
        boolean any = false;
        int c;
        while ((c = in.read()) != -1) {
            any = true;
            if (c == '\r') {
                continue;
            }
            if (c == '\n') {
                return sb.toString();
            }
            sb.put((char) c);
        }
        return any ? sb.toString() : null;
    }

    private static Request readRequest(InputStream in) throws IOException {
        String requestLine = readLine(in);
        if (requestLine == null || requestLine.isEmpty()) {
            return null;
        }
        String[] parts = requestLine.split(" ");
        String method = parts[0];
        String path = parts.length > 1 ? parts[1] : "";
        int contentLength = 0;
        String authorization = null;
        String line;
        while ((line = readLine(in)) != null && !line.isEmpty()) {
            int idx = line.indexOf(':');
            if (idx > 0) {
                String name = line.substring(0, idx).trim();
                if ("content-length".equalsIgnoreCase(name)) {
                    contentLength = Integer.parseInt(line.substring(idx + 1).trim());
                } else if ("authorization".equalsIgnoreCase(name)) {
                    authorization = line.substring(idx + 1).trim();
                }
            }
        }
        String body = "";
        if (contentLength > 0) {
            byte[] buf = new byte[contentLength];
            int read = 0;
            while (read < contentLength) {
                int n = in.read(buf, read, contentLength - read);
                if (n < 0) {
                    break;
                }
                read += n;
            }
            body = new String(buf, 0, read, StandardCharsets.UTF_8);
        }
        return new Request(method, path, body, authorization);
    }

    private static String reason(int status) {
        switch (status) {
            case 200:
                return "OK";
            case 400:
                return "Bad Request";
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

    private static void writeChunked(OutputStream out, byte[] body) throws IOException {
        // split into small chunks so a multi-KB value spans several, exercising the chunked decoder
        final int chunkSize = 64;
        for (int off = 0; off < body.length; off += chunkSize) {
            int len = Math.min(chunkSize, body.length - off);
            out.write((Integer.toHexString(len) + "\r\n").getBytes(StandardCharsets.US_ASCII));
            out.write(body, off, len);
            out.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)); // terminal chunk
    }

    private static void writeOversized(OutputStream out, long bodyBytes) throws IOException {
        // chunked body of the requested size, all whitespace after the opening brace so the JSON lexer keeps
        // consuming (no per-value limit) until the client trips its response-size cap. The client aborts and
        // closes the connection mid-stream once the cap is crossed, so tolerate the write failing under us
        out.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        final int chunkLen = 64 * 1024;
        final byte[] chunk = new byte[chunkLen];
        Arrays.fill(chunk, (byte) ' ');
        chunk[0] = '{'; // open an object once; the rest is whitespace, an unterminated body the cap cuts short
        final byte[] crlf = "\r\n".getBytes(StandardCharsets.US_ASCII);
        try {
            long remaining = bodyBytes;
            while (remaining > 0) {
                final int len = (int) Math.min(chunkLen, remaining);
                out.write((Integer.toHexString(len) + "\r\n").getBytes(StandardCharsets.US_ASCII));
                out.write(chunk, 0, len);
                out.write(crlf);
                chunk[0] = ' '; // only the first chunk opens the object; the rest is pure whitespace
                remaining -= len;
            }
            out.write("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
        } catch (IOException ignore) {
            // expected: the client aborts on its response-size cap mid-stream and closes the connection
        }
    }

    private static void writeResponse(OutputStream out, MockResponse response) throws IOException {
        if (response.rawResponse != null) {
            out.write(response.rawResponse.getBytes(StandardCharsets.US_ASCII));
            out.flush();
            return;
        }
        if (response.oversizedBodyBytes > 0) {
            writeOversized(out, response.oversizedBodyBytes);
            return;
        }
        if (response.stall) {
            // send chunked headers then block without sending the body, so the client must abort on its
            // own configured deadline rather than wedging on the HttpClient default timeout
            out.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            out.flush();
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException ignore) {
            }
            return;
        }
        byte[] bodyBytes = response.body.getBytes(StandardCharsets.UTF_8);
        StringSink head = new StringSink();
        head.put("HTTP/1.1 ").put(response.status).put(' ').put(reason(response.status)).put("\r\n");
        head.put("Content-Type: application/json\r\n");
        if (response.chunked) {
            head.put("Transfer-Encoding: chunked\r\n");
            head.put("\r\n");
            out.write(head.toString().getBytes(StandardCharsets.US_ASCII));
            writeChunked(out, bodyBytes);
        } else {
            head.put("Content-Length: ").put(bodyBytes.length).put("\r\n");
            head.put("\r\n");
            out.write(head.toString().getBytes(StandardCharsets.US_ASCII));
            out.write(bodyBytes);
        }
        out.flush();
    }

    private void acceptLoop() {
        while (!serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                connSockets.add(socket);
                Thread connThread = new Thread(() -> handleConnection(socket), "mock-oidc-conn");
                connThread.setDaemon(true);
                connThreads.add(connThread);
                connThread.start();
            } catch (IOException e) {
                // server socket closed, stop accepting
                return;
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
            Request request;
            while ((request = readRequest(in)) != null) {
                requestAuthHeaders.add(request.authorization);
                MockResponse response = handler.handle(request.method, request.path, request.body);
                if (response.dropConnection) {
                    // returning closes the socket (try-with-resources on its streams), so the client's
                    // in-flight read fails with a transport error
                    return;
                }
                writeResponse(out, response);
            }
        } catch (SocketException e) {
            // client closed the connection, expected
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    public interface Handler {
        MockResponse handle(String method, String path, String body);
    }

    public static class MockResponse {
        final String body;
        final boolean chunked;
        final int status;
        boolean dropConnection;
        long oversizedBodyBytes;
        String rawResponse;
        boolean stall;

        MockResponse(int status, String body, boolean chunked) {
            this.status = status;
            this.body = body;
            this.chunked = chunked;
        }
    }

    public static class Request {
        final String authorization;
        final String body;
        final String method;
        final String path;

        Request(String method, String path, String body, String authorization) {
            this.method = method;
            this.path = path;
            this.body = body;
            this.authorization = authorization;
        }
    }
}
