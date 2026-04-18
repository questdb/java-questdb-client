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
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Misc;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * QWP egress (query results) client. Phase-1 skeleton: connects to /read/v1,
 * negotiates the QWP protocol version, and closes cleanly.
 * <p>
 * Thread safety: not thread-safe. A single instance should be used from one thread.
 * <p>
 * Query execution wiring (QUERY_REQUEST encoding, RESULT_BATCH decoding, column-batch
 * handler dispatch) is added in subsequent commits; this skeleton exists so the
 * WebSocket upgrade to /read/v1 can be exercised end-to-end against the server.
 */
public class QwpQueryClient implements QuietCloseable {

    /**
     * Default endpoint path for QWP egress on the QuestDB HTTP server.
     */
    public static final String DEFAULT_ENDPOINT_PATH = "/read/v1";

    /**
     * Default QWP protocol version requested by this client.
     */
    public static final int QWP_MAX_VERSION = 1;

    private final QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
    private final QwpFrameRouter frameRouter = new QwpFrameRouter();
    private final CharSequence host;
    private final int port;
    private final NativeBufferWriter sendScratch = new NativeBufferWriter();
    private String authorizationHeader;
    private boolean connected;
    private int defaultTimeoutMillis = 30_000;
    private String endpointPath = DEFAULT_ENDPOINT_PATH;
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
        Misc.free(sendScratch);
        if (webSocketClient != null) {
            webSocketClient.close();
            webSocketClient = null;
        }
    }

    /**
     * Executes {@code sql} against the server and delivers result batches to the handler.
     * <p>
     * Blocks until the server sends either {@code RESULT_END} or {@code QUERY_ERROR}.
     * The handler's {@code onBatch}, {@code onEnd}, {@code onError} callbacks run on
     * the calling thread during {@code receiveFrame} processing.
     * <p>
     * Phase 1: no bind parameters, no CREDIT (server streams unbounded).
     */
    public void execute(String sql, QwpColumnBatchHandler handler) {
        if (!connected) {
            throw new IllegalStateException("QwpQueryClient not connected; call connect() first");
        }
        long requestId = nextRequestId++;
        writeQueryRequest(sql, requestId);
        webSocketClient.sendBinary(sendScratch.getBufferPtr(), sendScratch.getPosition());
        sendScratch.reset();

        frameRouter.of(handler, decoder, requestId);
        while (!frameRouter.isDone()) {
            boolean got = webSocketClient.receiveFrame(frameRouter, defaultTimeoutMillis);
            if (!got) {
                handler.onError((byte) 0, "timeout waiting for server response");
                break;
            }
        }
    }

    /**
     * Opens the TCP connection and performs the WebSocket upgrade handshake.
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
        connected = true;
    }

    public int getNegotiatedQwpVersion() {
        return negotiatedQwpVersion;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * Sets the HTTP Authorization header used during the upgrade handshake.
     * Must be called before {@link #connect()}.
     */
    public QwpQueryClient withAuthorization(String authorizationHeader) {
        this.authorizationHeader = authorizationHeader;
        return this;
    }

    /**
     * Overrides the default egress endpoint path ({@value #DEFAULT_ENDPOINT_PATH}).
     * Must be called before {@link #connect()}.
     */
    public QwpQueryClient withEndpointPath(String endpointPath) {
        this.endpointPath = endpointPath;
        return this;
    }

    private static String defaultClientId() {
        return "questdb-java-egress/1.0.0";
    }

    /**
     * Encodes a {@code QUERY_REQUEST} into {@link #sendScratch} at position 0.
     * Layout: msg_kind + request_id + sql_len (varint) + sql (UTF-8) +
     * initial_credit (varint) + bind_count (varint).
     */
    private void writeQueryRequest(String sql, long requestId) {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        sendScratch.reset();
        sendScratch.putByte(QwpEgressMsgKind.QUERY_REQUEST);
        sendScratch.putLong(requestId);
        sendScratch.putVarint(sqlBytes.length);
        for (byte b : sqlBytes) {
            sendScratch.putByte(b);
        }
        sendScratch.putVarint(0); // initial_credit = 0 (unbounded)
        sendScratch.putVarint(0); // bind_count = 0
    }

    /**
     * WebSocket frame handler that decodes QWP egress responses and dispatches to the
     * user-supplied {@link QwpColumnBatchHandler}. Reused across {@code execute()} calls.
     */
    private static final class QwpFrameRouter implements WebSocketFrameHandler {
        private QwpResultBatchDecoder decoder;
        private boolean done;
        private QwpColumnBatchHandler handler;
        private long requestId;

        public boolean isDone() {
            return done;
        }

        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            if (payloadLen < QwpConstants.HEADER_SIZE + 1) {
                handler.onError((byte) 0, "server sent short frame (" + payloadLen + " bytes)");
                done = true;
                return;
            }
            byte msgKind = Unsafe.getUnsafe().getByte(payloadPtr + QwpConstants.HEADER_SIZE);
            if (msgKind == QwpEgressMsgKind.RESULT_BATCH) {
                try {
                    decoder.decode(payloadPtr, payloadLen);
                    handler.onBatch(decoder.getBatch());
                } catch (QwpDecodeException e) {
                    handler.onError((byte) 0, e.getMessage());
                    done = true;
                }
            } else if (msgKind == QwpEgressMsgKind.RESULT_END) {
                long totalRows = decodeResultEnd(payloadPtr, payloadLen);
                handler.onEnd(totalRows);
                done = true;
            } else if (msgKind == QwpEgressMsgKind.QUERY_ERROR) {
                decodeQueryError(payloadPtr, payloadLen);
                done = true;
            } else {
                handler.onError((byte) 0, "unknown msg_kind 0x" + Integer.toHexString(msgKind & 0xFF));
                done = true;
            }
        }

        @Override
        public void onClose(int code, String reason) {
            if (!done) {
                handler.onError((byte) 0, "server closed connection: code=" + code + " reason=" + reason);
                done = true;
            }
        }

        public void of(QwpColumnBatchHandler handler, QwpResultBatchDecoder decoder, long requestId) {
            this.handler = handler;
            this.decoder = decoder;
            this.requestId = requestId;
            this.done = false;
        }

        private void decodeQueryError(long payload, int payloadLen) {
            // Body: msg_kind(1) + requestId(8) + status(1) + msgLen(u16) + msgBytes
            long p = payload + QwpConstants.HEADER_SIZE + 1 /* kind */ + 8 /* reqId */;
            byte status = Unsafe.getUnsafe().getByte(p);
            p += 1;
            int msgLen = Unsafe.getUnsafe().getShort(p) & 0xFFFF;
            p += 2;
            byte[] bytes = new byte[msgLen];
            for (int i = 0; i < msgLen; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(p + i);
            }
            handler.onError(status, new String(bytes, StandardCharsets.UTF_8));
        }

        /**
         * RESULT_END body: msg_kind(1) + requestId(8) + final_seq(varint) + total_rows(varint).
         * We only need total_rows, so walk past the first two varints.
         */
        private long decodeResultEnd(long payload, int payloadLen) {
            long p = payload + QwpConstants.HEADER_SIZE + 1 /* kind */ + 8 /* reqId */;
            long limit = payload + payloadLen;
            // Skip final_seq varint.
            while (p < limit && (Unsafe.getUnsafe().getByte(p++) & 0x80) != 0) {
                // continuation
            }
            // Decode total_rows varint.
            long total = 0;
            int shift = 0;
            while (p < limit) {
                byte b = Unsafe.getUnsafe().getByte(p++);
                total |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            return total;
        }
    }
}
