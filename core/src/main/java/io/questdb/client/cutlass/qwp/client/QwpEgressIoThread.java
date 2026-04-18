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
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Misc;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Dedicated I/O thread that owns the client's {@link WebSocketClient} and drives
 * receive + decode off the user thread. The user thread submits a query via
 * {@link #submitQuery} and drains events via {@link #takeEvent} / {@link #releaseBuffer};
 * meanwhile the I/O thread is free to read and decode the next batch in parallel.
 * <p>
 * A small pool of {@link QwpBatchBuffer} instances (default: 4) holds decoded
 * batches in flight. When the pool is exhausted the I/O thread blocks on
 * {@link #freeBuffers} until the user releases a buffer. This gives natural
 * back-pressure — if the consumer is slow, the I/O thread stops reading and
 * the kernel's TCP window closes on the server side.
 */
public class QwpEgressIoThread implements Runnable, WebSocketFrameHandler {

    private static final int DEFAULT_BUFFER_CAPACITY = 64 * 1024;
    private static final long POLL_TIMEOUT_MS = 100;
    // Pool of pre-allocated buffers. I/O thread takes, user thread releases.
    private final BlockingQueue<QwpBatchBuffer> freeBuffers;
    private final QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
    // Events delivered from I/O thread to user thread (RESULT_BATCH / RESULT_END / QUERY_ERROR).
    private final BlockingQueue<QueryEvent> events;
    // Single-slot request queue (Phase-1 allows one in-flight query).
    private final BlockingQueue<QueryRequest> requests = new ArrayBlockingQueue<>(1);
    private final NativeBufferWriter sendScratch = new NativeBufferWriter();
    private final WebSocketClient wsClient;
    // Per-query state; accessed only from the I/O thread.
    private long currentRequestId;
    private boolean currentQueryDone;
    private volatile Throwable fatalError;
    private volatile boolean shutdown;

    public QwpEgressIoThread(WebSocketClient wsClient, int bufferPoolSize) {
        this.wsClient = wsClient;
        this.freeBuffers = new ArrayBlockingQueue<>(bufferPoolSize);
        this.events = new ArrayBlockingQueue<>(bufferPoolSize + 2);
        for (int i = 0; i < bufferPoolSize; i++) {
            freeBuffers.offer(new QwpBatchBuffer(DEFAULT_BUFFER_CAPACITY));
        }
    }

    /**
     * Releases a buffer back to the I/O thread pool. Call after the user
     * handler finishes processing a {@code KIND_BATCH} event.
     */
    public void releaseBuffer(QwpBatchBuffer buffer) {
        freeBuffers.offer(buffer);
    }

    @Override
    public void onBinaryMessage(long payloadPtr, int payloadLen) {
        if (payloadLen < QwpConstants.HEADER_SIZE + 1) {
            emitError((byte) 0, "server sent truncated frame");
            return;
        }
        byte msgKind = Unsafe.getUnsafe().getByte(payloadPtr + QwpConstants.HEADER_SIZE);
        if (msgKind == QwpEgressMsgKind.RESULT_BATCH) {
            handleResultBatch(payloadPtr, payloadLen);
        } else if (msgKind == QwpEgressMsgKind.RESULT_END) {
            long total = decodeResultEnd(payloadPtr, payloadLen);
            events.offer(new QueryEvent().asEnd(total));
            currentQueryDone = true;
        } else if (msgKind == QwpEgressMsgKind.QUERY_ERROR) {
            decodeAndEmitError(payloadPtr, payloadLen);
            currentQueryDone = true;
        } else {
            emitError((byte) 0, "unknown msg_kind 0x" + Integer.toHexString(msgKind & 0xFF));
            currentQueryDone = true;
        }
    }

    @Override
    public void onClose(int code, String reason) {
        if (!currentQueryDone) {
            emitError((byte) 0, "server closed connection: code=" + code + " reason=" + reason);
            currentQueryDone = true;
        }
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                QueryRequest req;
                try {
                    req = requests.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    return;
                }
                if (req == null) continue;

                currentRequestId = req.requestId;
                currentQueryDone = false;
                sendQueryRequest(req);

                while (!currentQueryDone && !shutdown) {
                    // onBinaryMessage (on this same thread) sets currentQueryDone.
                    wsClient.receiveFrame(this, (int) POLL_TIMEOUT_MS);
                }
            }
        } catch (Throwable t) {
            fatalError = t;
            emitError((byte) 0, "I/O thread failure: " + t.getMessage());
        }
    }

    /**
     * Blocking pop of the next event. Called by the user thread during {@code execute()}.
     */
    public QueryEvent takeEvent() throws InterruptedException {
        return events.take();
    }

    /**
     * Signals shutdown. Does not join the thread — caller handles that.
     */
    public void shutdown() {
        shutdown = true;
    }

    /**
     * Blocking submission of a query. Called by the user thread.
     */
    public void submitQuery(String sql, long requestId) throws InterruptedException {
        requests.put(new QueryRequest(sql, requestId));
    }

    /**
     * Frees native scratch owned by the pool. Call after the thread has terminated.
     */
    void closePool() {
        Misc.free(sendScratch);
        for (QwpBatchBuffer b : freeBuffers) {
            b.close();
        }
        freeBuffers.clear();
    }

    private void decodeAndEmitError(long payload, int payloadLen) {
        // Body: msg_kind(1) + requestId(8) + status(1) + msgLen(u16) + msgBytes
        long p = payload + QwpConstants.HEADER_SIZE + 1 + 8;
        byte status = Unsafe.getUnsafe().getByte(p);
        p += 1;
        int msgLen = Unsafe.getUnsafe().getShort(p) & 0xFFFF;
        p += 2;
        byte[] bytes = new byte[msgLen];
        for (int i = 0; i < msgLen; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(p + i);
        }
        events.offer(new QueryEvent().asError(status, new String(bytes, StandardCharsets.UTF_8)));
    }

    /**
     * RESULT_END body: msg_kind(1) + requestId(8) + final_seq(varint) + total_rows(varint).
     * We only need total_rows.
     */
    private long decodeResultEnd(long payload, int payloadLen) {
        long p = payload + QwpConstants.HEADER_SIZE + 1 + 8;
        long limit = payload + payloadLen;
        while (p < limit && (Unsafe.getUnsafe().getByte(p++) & 0x80) != 0) {
            // skip final_seq continuation bytes
        }
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

    private void emitError(byte status, String message) {
        events.offer(new QueryEvent().asError(status, message));
    }

    private void handleResultBatch(long payloadPtr, int payloadLen) {
        QwpBatchBuffer buf;
        try {
            buf = freeBuffers.take();
        } catch (InterruptedException ie) {
            return;
        }
        buf.copyFromPayload(payloadPtr, payloadLen);
        try {
            decoder.decode(buf);
        } catch (QwpDecodeException e) {
            freeBuffers.offer(buf);
            emitError((byte) 0, "decode failure: " + e.getMessage());
            currentQueryDone = true;
            return;
        }
        events.offer(new QueryEvent().asBatch(buf));
    }

    /**
     * Builds and transmits a QUERY_REQUEST frame on the WebSocket.
     */
    private void sendQueryRequest(QueryRequest req) {
        byte[] sqlBytes = req.sql.getBytes(StandardCharsets.UTF_8);
        sendScratch.reset();
        sendScratch.putByte(QwpEgressMsgKind.QUERY_REQUEST);
        sendScratch.putLong(req.requestId);
        sendScratch.putVarint(sqlBytes.length);
        for (byte b : sqlBytes) {
            sendScratch.putByte(b);
        }
        sendScratch.putVarint(0); // initial_credit = 0 (unbounded)
        sendScratch.putVarint(0); // bind_count = 0
        wsClient.sendBinary(sendScratch.getBufferPtr(), sendScratch.getPosition());
        sendScratch.reset();
    }

    private static final class QueryRequest {
        final long requestId;
        final String sql;

        QueryRequest(String sql, long requestId) {
            this.sql = sql;
            this.requestId = requestId;
        }
    }
}
