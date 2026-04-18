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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.std.QuietCloseable;

/**
 * QWP egress (query results) client.
 * <p>
 * Connection shape: one WebSocket to {@code /read/v1}, one dedicated I/O thread
 * that owns the socket and the decoder. The user thread submits a query and
 * drains result batches via the supplied {@link QwpColumnBatchHandler}; the
 * I/O thread reads and decodes ahead so that decoding of batch {@code N+1}
 * overlaps with the user's processing of batch {@code N}.
 * <p>
 * Thread safety: not thread-safe for concurrent queries on the same client.
 * One {@link #execute} at a time. Opening one client per query-issuing thread
 * is the recommended pattern.
 */
public class QwpQueryClient implements QuietCloseable {

    public static final String DEFAULT_ENDPOINT_PATH = "/read/v1";
    public static final int QWP_MAX_VERSION = 1;
    private static final int DEFAULT_IO_BUFFER_POOL_SIZE = 4;
    private final CharSequence host;
    private final int port;
    private String authorizationHeader;
    private int bufferPoolSize = DEFAULT_IO_BUFFER_POOL_SIZE;
    private boolean connected;
    private String endpointPath = DEFAULT_ENDPOINT_PATH;
    private QwpEgressIoThread ioThread;
    private Thread ioThreadHandle;
    private int negotiatedQwpVersion;
    private long nextRequestId = 1;
    private WebSocketClient webSocketClient;

    private QwpQueryClient(CharSequence host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Creates a plain-text (non-TLS) QWP query client.
     */
    public static QwpQueryClient newPlainText(CharSequence host, int port) {
        return new QwpQueryClient(host, port);
    }

    @Override
    public void close() {
        connected = false;
        if (ioThread != null) {
            ioThread.shutdown();
            if (ioThreadHandle != null) {
                try {
                    ioThreadHandle.join(5_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            ioThread.closePool();
            ioThread = null;
            ioThreadHandle = null;
        }
        if (webSocketClient != null) {
            webSocketClient.close();
            webSocketClient = null;
        }
    }

    /**
     * Opens the TCP connection, performs the WebSocket upgrade, and spawns the I/O thread.
     * Must be called before any query is submitted.
     */
    public void connect() {
        if (connected) {
            return;
        }
        webSocketClient = WebSocketClientFactory.newPlainTextInstance();
        webSocketClient.setQwpMaxVersion(QWP_MAX_VERSION);
        webSocketClient.setQwpClientId(defaultClientId());
        webSocketClient.connect(host, port);
        webSocketClient.upgrade(endpointPath, authorizationHeader);
        negotiatedQwpVersion = webSocketClient.getServerQwpVersion();

        ioThread = new QwpEgressIoThread(webSocketClient, bufferPoolSize);
        ioThreadHandle = new Thread(ioThread, "qwp-egress-io");
        ioThreadHandle.setDaemon(true);
        ioThreadHandle.start();
        connected = true;
    }

    /**
     * Executes {@code sql} and drives the supplied handler through the result stream.
     * <p>
     * Blocks the calling thread until the server sends {@code RESULT_END} or
     * {@code QUERY_ERROR}. While the user thread is inside {@code handler.onBatch},
     * the I/O thread keeps reading and decoding ahead up to the configured buffer-pool depth.
     */
    public void execute(String sql, QwpColumnBatchHandler handler) {
        if (!connected) {
            throw new IllegalStateException("QwpQueryClient not connected; call connect() first");
        }
        long requestId = nextRequestId++;
        try {
            ioThread.submitQuery(sql, requestId);
            while (true) {
                QueryEvent ev = ioThread.takeEvent();
                switch (ev.kind) {
                    case QueryEvent.KIND_BATCH:
                        try {
                            handler.onBatch(ev.buffer.batch);
                        } finally {
                            ioThread.releaseBuffer(ev.buffer);
                        }
                        break;
                    case QueryEvent.KIND_END:
                        handler.onEnd(ev.totalRows);
                        return;
                    case QueryEvent.KIND_ERROR:
                        handler.onError(ev.errorStatus, ev.errorMessage);
                        return;
                    default:
                        handler.onError((byte) 0, "unknown event kind " + ev.kind);
                        return;
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            handler.onError((byte) 0, "interrupted while waiting for server response");
        }
    }

    public int getNegotiatedQwpVersion() {
        return negotiatedQwpVersion;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * Overrides the default I/O buffer pool depth (4). Larger pools let the
     * I/O thread decode further ahead of the consumer at the cost of memory;
     * smaller pools reduce memory but may stall the I/O thread on slow consumers.
     * Must be called before {@link #connect()}.
     */
    public QwpQueryClient withBufferPoolSize(int size) {
        if (size < 1) throw new IllegalArgumentException("bufferPoolSize must be >= 1");
        this.bufferPoolSize = size;
        return this;
    }

    public QwpQueryClient withAuthorization(String authorizationHeader) {
        this.authorizationHeader = authorizationHeader;
        return this;
    }

    public QwpQueryClient withEndpointPath(String endpointPath) {
        this.endpointPath = endpointPath;
        return this;
    }

    private static String defaultClientId() {
        return "questdb-java-egress/1.0.0";
    }
}
