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

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Misc;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Client for QuestDB Enterprise QWP table commit-tail subscriptions. Opens a
 * WebSocket against the QWP egress endpoint, subscribes to one or more
 * tables, and delivers row batches via a {@link QwpSubscriptionHandler}
 * callback.
 * <p>
 * Threading: not thread-safe. One instance per logical client; all calls
 * (subscribe, cancel, poll, close) must execute on a single thread.
 * <p>
 * Subscriptions are an Enterprise-only feature on the server side. Against
 * an OSS server, the SUBSCRIBE_REQUEST frame is rejected and the connection
 * disconnects.
 *
 * <pre>
 * try (QwpSubscribeClient client = QwpSubscribeClient.newPlainText("127.0.0.1", 9000)
 *         .withBasicAuth("admin", "quest")) {
 *     client.connect();
 *     QwpSubscription sub = client.subscribe("trades", new QwpSubscriptionHandler() {
 *         public void onAck(long startTxn, int schemaId) { ... }
 *         public void onBatch(long txn, QwpColumnBatch batch) {
 *             for (int row = 0; row &lt; batch.getRowCount(); row++) {
 *                 String sym = batch.getSymbol(0, row);
 *                 double price = batch.getDoubleValue(1, row);
 *                 long ts = batch.getLongValue(2, row);
 *                 // ... process the row ...
 *             }
 *         }
 *         public void onEnd(byte reason, long lastTxn, String message) { ... }
 *     });
 *     while (sub.isActive()) {
 *         client.poll(1000);
 *     }
 * }
 * </pre>
 */
public class QwpSubscribeClient implements QuietCloseable {

    /**
     * Default endpoint path on the server. Same as the query egress path; the
     * server distinguishes subscribe traffic by msg_kind, not by URL.
     */
    public static final String DEFAULT_ENDPOINT_PATH = "/read/v1";
    /**
     * Default WebSocket port on the server (HTTP port).
     */
    public static final int DEFAULT_WS_PORT = 9000;
    private static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 10_000;
    private static final int SCRATCH_CAP = 8_192;
    /**
     * Reusable decoded-batch view shared across every subscription on this
     * client. Single-threaded usage means we can pool one buffer and let
     * {@link #handleBatch} hand the same instance to whichever sub's
     * handler is firing - the {@link QwpColumnBatch} contract is "valid
     * until the callback returns" so no two callbacks see overlapping
     * lifetimes.
     */
    private final QwpBatchBuffer batchBuffer = new QwpBatchBuffer(SCRATCH_CAP);
    /**
     * Connection-scoped row decoder. Holds the symbol-dictionary heap and
     * the schema registry that subsequent SUBSCRIBE_BATCH frames reference;
     * a CACHE_RESET wipes both via {@link QwpResultBatchDecoder#applyCacheReset(byte)}.
     */
    private final QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
    private final FrameDispatcher dispatcher = new FrameDispatcher();
    private final CharSequence host;
    private final int port;
    private final long scratchAddr;
    private final Map<Long, QwpSubscriptionImpl> subscriptions = new HashMap<>();
    private final WebSocketClient ws;
    private String authorization;
    private String clientId;
    private boolean closed;
    private boolean connected;
    private String endpointPath = DEFAULT_ENDPOINT_PATH;
    private int handshakeTimeoutMs = DEFAULT_HANDSHAKE_TIMEOUT_MS;
    private long initialCredit = -1;
    private int maxBatchRows = -1;
    private int maxVersion = QwpConstants.MAX_SUPPORTED_VERSION;

    private QwpSubscribeClient(WebSocketClient ws, CharSequence host, int port) {
        this.ws = ws;
        this.host = host;
        this.port = port;
        this.scratchAddr = Unsafe.malloc(SCRATCH_CAP, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * Creates a plaintext client (no TLS) targeting the given host/port.
     * The TCP connection and WebSocket upgrade do not happen until
     * {@link #connect()} is called.
     */
    public static QwpSubscribeClient newPlainText(CharSequence host, int port) {
        return new QwpSubscribeClient(WebSocketClientFactory.newPlainTextInstance(), host, port);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        // Cancel every active subscription on the wire so the server frees its
        // hub-side state quickly. The SUBSCRIPTION_END frames may not arrive
        // before we disconnect; that is fine, the server gc's on
        // onConnectionClosed.
        for (QwpSubscriptionImpl sub : subscriptions.values()) {
            if (sub.active) {
                try {
                    sendSubCancel(sub.subscriptionId);
                } catch (Throwable ignored) {
                }
            }
        }
        try {
            if (ws.isConnected()) {
                ws.disconnect();
            }
        } catch (Throwable ignored) {
        }
        try {
            ws.close();
        } catch (Throwable ignored) {
        }
        Unsafe.free(scratchAddr, SCRATCH_CAP, MemoryTag.NATIVE_DEFAULT);
        Misc.free(decoder);
        Misc.free(batchBuffer);
    }

    /**
     * Establishes the TCP connection and WebSocket upgrade. Idempotent: a
     * second call on an already-connected client is a no-op.
     */
    public void connect() {
        if (closed) {
            throw new HttpClientException("client is closed");
        }
        if (connected) return;
        if (clientId != null) {
            ws.setQwpClientId(clientId);
        }
        if (maxVersion > 0) {
            ws.setQwpMaxVersion(maxVersion);
        }
        if (maxBatchRows > 0) {
            ws.setQwpMaxBatchRows(maxBatchRows);
        }
        ws.connect(host, port);
        ws.upgrade(endpointPath, handshakeTimeoutMs, authorization);
        // Drain the unsolicited SERVER_INFO frame so subsequent receives see
        // SUBSCRIBE_* frames directly.
        ServerInfoDrain drain = new ServerInfoDrain();
        long deadline = System.nanoTime() + (long) handshakeTimeoutMs * 1_000_000L;
        while (!drain.received) {
            int waitMs = remainingMs(deadline);
            if (waitMs <= 0) {
                throw new HttpClientException("timed out waiting for SERVER_INFO");
            }
            ws.receiveFrame(drain, waitMs);
        }
        connected = true;
    }

    /**
     * Returns the server's negotiated QWP version, or {@code -1} if the
     * connection has not been upgraded.
     */
    public int getNegotiatedQwpVersion() {
        return connected ? ws.getServerQwpVersion() : -1;
    }

    public boolean isConnected() {
        return connected && !closed && ws.isConnected();
    }

    /**
     * Drives the receive path for {@code timeoutMs}, dispatching any inbound
     * frames to their subscriptions' handlers. Returns when at least one
     * frame has been processed or the timeout has elapsed (whichever comes
     * first).
     */
    public void poll(int timeoutMs) {
        if (!isConnected()) {
            throw new HttpClientException("not connected");
        }
        ws.receiveFrame(dispatcher, timeoutMs);
    }

    /**
     * Subscribes to {@code tableName} starting from the next-committed txn.
     * Blocks until the server's SUBSCRIBE_ACK arrives (or the call times out
     * via {@code handshakeTimeoutMs}). Once this method returns, the handler
     * has already received {@link QwpSubscriptionHandler#onAck}.
     */
    public QwpSubscription subscribe(String tableName, QwpSubscriptionHandler handler) {
        return subscribe(tableName, 0L, handler);
    }

    /**
     * Subscribes to {@code tableName} starting from {@code startTxn}.
     * {@code startTxn=0} means tail-from-now; non-zero values request
     * resume-from-txn (server may reject with STALE if the underlying WAL
     * segment has been purged).
     */
    public QwpSubscription subscribe(String tableName, long startTxn, QwpSubscriptionHandler handler) {
        if (!isConnected()) {
            throw new HttpClientException("not connected");
        }
        if (tableName == null) {
            throw new IllegalArgumentException("tableName must not be null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        long subId = nextSubscriptionId();
        QwpSubscriptionImpl sub = new QwpSubscriptionImpl(subId, handler);
        subscriptions.put(subId, sub);
        sendSubscribeRequest(subId, tableName, startTxn,
                initialCredit < 0 ? Long.MAX_VALUE : initialCredit,
                maxBatchRows < 0 ? 0 : maxBatchRows);
        // Block until ack or end arrives for this subscription.
        long deadline = System.nanoTime() + (long) handshakeTimeoutMs * 1_000_000L;
        while (!sub.acked && sub.active) {
            int waitMs = remainingMs(deadline);
            if (waitMs <= 0) {
                subscriptions.remove(subId);
                throw new HttpClientException("timed out waiting for SUBSCRIBE_ACK");
            }
            ws.receiveFrame(dispatcher, waitMs);
        }
        if (!sub.acked) {
            // onEnd already fired before ack -- subscribe failed.
            throw new HttpClientException("subscribe failed [reason=")
                    .put(QwpSubscribeMsgKind.reasonName(sub.endReason)).put(']');
        }
        return sub;
    }

    /**
     * Sets the {@code Authorization} header sent on the WebSocket upgrade.
     * Must be called before {@link #connect()}.
     */
    public void withAuthorization(String authorizationHeader) {
        checkNotConnected();
        this.authorization = authorizationHeader;
    }

    /**
     * Convenience wrapper for HTTP basic authentication. Equivalent to
     * {@code withAuthorization("Basic " + base64(user + ":" + pwd))}.
     */
    public QwpSubscribeClient withBasicAuth(String user, String pwd) {
        if (user == null || pwd == null) {
            throw new IllegalArgumentException("user and pwd must not be null");
        }
        String token = Base64.getEncoder().encodeToString((user + ":" + pwd).getBytes(StandardCharsets.UTF_8));
        withAuthorization("Basic " + token);
        return this;
    }

    /**
     * Convenience wrapper for bearer token authentication.
     */
    public QwpSubscribeClient withBearerToken(String token) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        withAuthorization("Bearer " + token);
        return this;
    }

    public QwpSubscribeClient withClientId(String clientId) {
        checkNotConnected();
        this.clientId = clientId;
        return this;
    }

    public QwpSubscribeClient withEndpointPath(String path) {
        checkNotConnected();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("endpoint path must not be empty");
        }
        this.endpointPath = path;
        return this;
    }

    public QwpSubscribeClient withHandshakeTimeoutMs(int timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be positive");
        }
        this.handshakeTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * Initial byte-credit budget per subscription. {@code -1} (the default)
     * sends {@code Long.MAX_VALUE}, effectively unbounded.
     */
    public QwpSubscribeClient withInitialCredit(long bytes) {
        if (bytes < -1) {
            throw new IllegalArgumentException("credit must be >= -1");
        }
        this.initialCredit = bytes;
        return this;
    }

    public QwpSubscribeClient withMaxBatchRows(int rows) {
        if (rows < 0) {
            throw new IllegalArgumentException("rows must be >= 0");
        }
        this.maxBatchRows = rows;
        return this;
    }

    public QwpSubscribeClient withMaxVersion(int maxVersion) {
        if (maxVersion < 1) {
            throw new IllegalArgumentException("maxVersion must be positive");
        }
        this.maxVersion = maxVersion;
        return this;
    }

    private void checkNotConnected() {
        if (connected) {
            throw new HttpClientException("client already connected");
        }
    }

    private long nextSubscriptionId() {
        long id;
        do {
            id = (long) (Math.random() * Long.MAX_VALUE);
        } while (id == 0L || subscriptions.containsKey(id));
        return id;
    }

    private static int remainingMs(long deadlineNs) {
        long remaining = (deadlineNs - System.nanoTime()) / 1_000_000L;
        if (remaining <= 0) return 0;
        if (remaining > Integer.MAX_VALUE) return Integer.MAX_VALUE;
        return (int) remaining;
    }

    private void sendSubCancel(long subId) {
        long p = scratchAddr;
        Unsafe.getUnsafe().putByte(p++, QwpSubscribeMsgKind.SUB_CANCEL);
        Unsafe.getUnsafe().putLong(p, subId);
        ws.sendBinary(scratchAddr, 9);
    }

    private void sendSubCredit(long subId, long bytes) {
        long p = scratchAddr;
        Unsafe.getUnsafe().putByte(p++, QwpSubscribeMsgKind.SUB_CREDIT);
        Unsafe.getUnsafe().putLong(p, subId);
        p += 8;
        p = encodeVarint(p, bytes);
        ws.sendBinary(scratchAddr, (int) (p - scratchAddr));
    }

    private void sendSubscribeRequest(long subId, String tableName, long startTxn, long credit, int batchMaxRows) {
        byte[] tn = tableName.getBytes(StandardCharsets.UTF_8);
        if (tn.length > 0xFFFF) {
            throw new IllegalArgumentException("tableName too long");
        }
        long p = scratchAddr;
        Unsafe.getUnsafe().putByte(p++, QwpSubscribeMsgKind.SUBSCRIBE_REQUEST);
        Unsafe.getUnsafe().putLong(p, subId);
        p += 8;
        Unsafe.getUnsafe().putShort(p, (short) tn.length);
        p += 2;
        for (byte b : tn) {
            Unsafe.getUnsafe().putByte(p++, b);
        }
        p = encodeVarint(p, startTxn);
        p = encodeVarint(p, credit);
        Unsafe.getUnsafe().putInt(p, batchMaxRows);
        Unsafe.getUnsafe().putInt(p + 4, 0); // flags
        p += 8;
        ws.sendBinary(scratchAddr, (int) (p - scratchAddr));
    }

    private static long encodeVarint(long addr, long v) {
        while ((v & ~0x7FL) != 0) {
            Unsafe.getUnsafe().putByte(addr++, (byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        Unsafe.getUnsafe().putByte(addr++, (byte) v);
        return addr;
    }

    private static long readVarint(long addr, long limit, long[] outValueAndBytes) {
        long value = 0;
        int shift = 0;
        long p = addr;
        while (p < limit) {
            byte b = Unsafe.getUnsafe().getByte(p++);
            value |= ((long) (b & 0x7F)) << shift;
            if ((b & 0x80) == 0) {
                outValueAndBytes[0] = value;
                outValueAndBytes[1] = p - addr;
                return p;
            }
            shift += 7;
            if (shift >= 64) {
                throw new HttpClientException("varint overflow");
            }
        }
        throw new HttpClientException("truncated varint");
    }

    private static int readU16(long addr) {
        return Unsafe.getUnsafe().getShort(addr) & 0xFFFF;
    }

    private final class FrameDispatcher implements WebSocketFrameHandler {

        private final long[] varintScratch = new long[2];

        @Override
        public void onBinaryMessage(long payloadAddr, int payloadLen) {
            if (payloadLen < QwpConstants.HEADER_SIZE + 1) {
                return;
            }
            byte flags = Unsafe.getUnsafe().getByte(payloadAddr + QwpConstants.HEADER_OFFSET_FLAGS);
            byte msgKind = Unsafe.getUnsafe().getByte(payloadAddr + QwpConstants.HEADER_SIZE);
            long bodyAddr = payloadAddr + QwpConstants.HEADER_SIZE + 1;
            int bodyLen = payloadLen - QwpConstants.HEADER_SIZE - 1;
            switch (msgKind) {
                case QwpSubscribeMsgKind.SUBSCRIBE_ACK:
                    handleAck(bodyAddr, bodyLen);
                    return;
                case QwpSubscribeMsgKind.SUBSCRIBE_BATCH:
                    handleBatch(payloadAddr, payloadLen, flags, bodyAddr, bodyLen);
                    return;
                case QwpSubscribeMsgKind.SUBSCRIPTION_END:
                    handleEnd(bodyAddr, bodyLen);
                    return;
                case QwpEgressMsgKind.CACHE_RESET:
                    handleCacheReset(bodyAddr, bodyLen);
                    return;
                default:
                    // Other server-to-client frames (RESULT_BATCH, SERVER_INFO)
                    // are not relevant to the subscribe client; the WAL-tail
                    // use case never asks for them.
            }
        }

        @Override
        public void onClose(int code, String reason) {
            // All active subscriptions are now dead. Synthesize END callbacks so
            // applications get a clean lifecycle signal.
            for (QwpSubscriptionImpl sub : subscriptions.values()) {
                if (sub.active) {
                    sub.active = false;
                    sub.handler.onEnd(QwpSubscribeMsgKind.SUB_END_ERROR, sub.lastTxn,
                            reason == null ? "connection closed" : reason);
                }
            }
        }

        private void handleAck(long bodyAddr, int bodyLen) {
            // sub_id(8) + start_txn(8) + schema_id(4) + schema_full
            if (bodyLen < 20) return;
            long subId = Unsafe.getUnsafe().getLong(bodyAddr);
            long startTxn = Unsafe.getUnsafe().getLong(bodyAddr + 8);
            int schemaId = Unsafe.getUnsafe().getInt(bodyAddr + 16);
            QwpSubscriptionImpl sub = subscriptions.get(subId);
            if (sub == null) return;
            // Seed the decoder's connection-scoped schema registry with the
            // table's columns BEFORE the first SUBSCRIBE_BATCH lands. Subscribe
            // batches reference the schema by id (mode 0x01) on every frame -
            // the decoder rejects an unknown id, so we have to register it
            // here from the ACK body's full schema section. registerSchemaFull
            // expects the bytes to end at the last column's wire-type byte
            // exactly, which matches QwpEgressSchemaWriter.writeFull's output.
            try {
                decoder.registerSchemaFull(bodyAddr + 20, bodyAddr + bodyLen);
            } catch (QwpDecodeException e) {
                sub.active = false;
                sub.endReason = QwpSubscribeMsgKind.SUB_END_ERROR;
                sub.handler.onEnd(QwpSubscribeMsgKind.SUB_END_ERROR, 0L,
                        "schema decode failure: " + e.getMessage());
                throw new HttpClientException("subscribe-ack schema decode failed: " + e.getMessage());
            }
            sub.startTxn = startTxn;
            sub.schemaId = schemaId;
            sub.acked = true;
            sub.handler.onAck(startTxn, schemaId);
        }

        private void handleBatch(long payloadAddr, int payloadLen, byte flags, long bodyAddr, int bodyLen) {
            // Per-batch prelude: sub_id(8) + batch_seq(varint) + txn(u64).
            // Anything past txn is the (optionally zstd-wrapped) delta_section
            // + table_block, which the OSS QwpResultBatchDecoder knows how to
            // decode via decodeAfterPrelude.
            if (bodyLen < 16) return;
            long subId = Unsafe.getUnsafe().getLong(bodyAddr);
            QwpSubscriptionImpl sub = subscriptions.get(subId);
            if (sub == null) return;
            long bodyLimit = bodyAddr + bodyLen;
            long after = readVarint(bodyAddr + 8, bodyLimit, varintScratch);
            long batchSeq = varintScratch[0];
            if (after + 8 > bodyLimit) return;
            long txn = Unsafe.getUnsafe().getLong(after);
            long rowsStart = after + 8;
            sub.lastTxn = txn;
            try {
                decoder.decodeAfterPrelude(
                        batchBuffer,
                        rowsStart,
                        bodyLimit,
                        flags,
                        subId,
                        batchSeq,
                        payloadAddr,
                        payloadLen
                );
            } catch (QwpDecodeException e) {
                // The decoder is now out of step with the server's byte
                // stream; further frames cannot be trusted. Synthesise an
                // END for this sub and let the connection close.
                sub.active = false;
                sub.endReason = QwpSubscribeMsgKind.SUB_END_ERROR;
                sub.handler.onEnd(QwpSubscribeMsgKind.SUB_END_ERROR, sub.lastTxn,
                        "decode failure: " + e.getMessage());
                throw new HttpClientException("subscribe batch decode failed: " + e.getMessage());
            }
            sub.handler.onBatch(txn, batchBuffer.batch);
        }

        private void handleCacheReset(long bodyAddr, int bodyLen) {
            // CACHE_RESET body: reset_mask(u8). Bit 0 = SYMBOL dict, bit 1 =
            // schema cache. We delegate to the decoder since it owns both.
            if (bodyLen < 1) return;
            byte mask = Unsafe.getUnsafe().getByte(bodyAddr);
            decoder.applyCacheReset(mask);
        }

        private void handleEnd(long bodyAddr, int bodyLen) {
            // sub_id(8) + reason(1) + last_txn(8) + msg_len(2) + msg_bytes
            if (bodyLen < 19) return;
            long subId = Unsafe.getUnsafe().getLong(bodyAddr);
            byte reason = Unsafe.getUnsafe().getByte(bodyAddr + 8);
            long lastTxn = Unsafe.getUnsafe().getLong(bodyAddr + 9);
            int msgLen = readU16(bodyAddr + 17);
            String message = "";
            if (msgLen > 0 && bodyLen >= 19 + msgLen) {
                byte[] bytes = new byte[msgLen];
                for (int i = 0; i < msgLen; i++) {
                    bytes[i] = Unsafe.getUnsafe().getByte(bodyAddr + 19 + i);
                }
                message = new String(bytes, StandardCharsets.UTF_8);
            }
            QwpSubscriptionImpl sub = subscriptions.get(subId);
            if (sub == null) return;
            sub.endReason = reason;
            sub.lastTxn = lastTxn;
            sub.active = false;
            sub.handler.onEnd(reason, lastTxn, message);
        }
    }

    private final class QwpSubscriptionImpl implements QwpSubscription {

        private final QwpSubscriptionHandler handler;
        private final long subscriptionId;
        private boolean acked;
        private boolean active = true;
        private byte endReason;
        private long lastTxn;
        private int schemaId;
        private long startTxn;

        QwpSubscriptionImpl(long subscriptionId, QwpSubscriptionHandler handler) {
            this.subscriptionId = subscriptionId;
            this.handler = handler;
        }

        @Override
        public void cancel() {
            if (!active) return;
            sendSubCancel(subscriptionId);
        }

        @Override
        public int getSchemaId() {
            return schemaId;
        }

        @Override
        public long getStartTxn() {
            return startTxn;
        }

        @Override
        public long getSubscriptionId() {
            return subscriptionId;
        }

        @Override
        public void grantCredit(long additionalBytes) {
            if (additionalBytes <= 0) return;
            sendSubCredit(subscriptionId, additionalBytes);
        }

        @Override
        public boolean isActive() {
            return active;
        }
    }

    private static final class ServerInfoDrain implements WebSocketFrameHandler {

        boolean received;

        @Override
        public void onBinaryMessage(long payloadAddr, int payloadLen) {
            if (payloadLen >= QwpConstants.HEADER_SIZE + 1) {
                byte kind = Unsafe.getUnsafe().getByte(payloadAddr + QwpConstants.HEADER_SIZE);
                if (kind == QwpEgressMsgKind.SERVER_INFO) {
                    received = true;
                }
            }
        }

        @Override
        public void onClose(int code, String reason) {
            received = true; // unblock the loop; connect() will surface the close
        }
    }
}
