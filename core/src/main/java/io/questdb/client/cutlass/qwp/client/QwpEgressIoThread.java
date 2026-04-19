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
 * back-pressure -- if the consumer is slow, the I/O thread stops reading and
 * the kernel's TCP window closes on the server side.
 */
public class QwpEgressIoThread implements Runnable, WebSocketFrameHandler {

    private static final int DEFAULT_BUFFER_CAPACITY = 64 * 1024;
    private static final long POLL_TIMEOUT_MS = 100;
    private final QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
    // Events delivered from I/O thread to user thread (RESULT_BATCH / RESULT_END / QUERY_ERROR).
    private final BlockingQueue<QueryEvent> events;
    // Pool of pre-allocated buffers. I/O thread takes, user thread releases.
    private final BlockingQueue<QwpBatchBuffer> freeBuffers;
    // Single-slot request queue (Phase-1 allows one in-flight query).
    private final BlockingQueue<QueryRequest> requests = new ArrayBlockingQueue<>(1);
    private final NativeBufferWriter sendScratch = new NativeBufferWriter();
    private final WebSocketClient wsClient;
    private boolean currentQueryDone;
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
     * Decodes a QUERY_ERROR payload into a {@link QueryEvent}. Visible for testing.
     * Bound-checks msgLen against the actual payload: a hostile or buggy server
     * can encode msgLen=0xFFFF with a tiny payload, which would otherwise read
     * up to 65 KiB of native memory beyond the frame and surface it to the user
     * callback as a String.
     */
    public static QueryEvent decodeError(long payload, int payloadLen) {
        long payloadEnd = payload + payloadLen;
        long p = payload + QwpConstants.HEADER_SIZE + 1 + 8;
        if (p + 1 + 2 > payloadEnd) {
            return new QueryEvent().asError((byte) 0, "QUERY_ERROR frame truncated before msg_len");
        }
        byte status = Unsafe.getUnsafe().getByte(p);
        p += 1;
        int msgLen = Unsafe.getUnsafe().getShort(p) & 0xFFFF;
        p += 2;
        if (p + msgLen > payloadEnd) {
            return new QueryEvent().asError((byte) 0,
                    "QUERY_ERROR msg_len " + msgLen + " exceeds frame remainder " + (payloadEnd - p));
        }
        byte[] bytes = new byte[msgLen];
        for (int i = 0; i < msgLen; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(p + i);
        }
        return new QueryEvent().asError(status, new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public void onBinaryMessage(long payloadPtr, int payloadLen) {
        if (payloadLen < QwpConstants.HEADER_SIZE + 1) {
            emitError((byte) 0, "server sent truncated frame");
            // Stop the receive loop; the framing is broken and any further bytes
            // would be misinterpreted relative to the expected message boundary.
            currentQueryDone = true;
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

    /**
     * Releases a buffer back to the I/O thread pool. Call after the user
     * handler finishes processing a {@code KIND_BATCH} event.
     */
    public void releaseBuffer(QwpBatchBuffer buffer) {
        freeBuffers.offer(buffer);
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                QueryRequest req;
                try {
                    req = requests.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    break;
                }
                if (req == null) continue;

                // Per-query state; accessed only from the I/O thread.
                currentQueryDone = false;
                sendQueryRequest(req);

                while (!currentQueryDone && !shutdown) {
                    // onBinaryMessage (on this same thread) sets currentQueryDone.
                    wsClient.receiveFrame(this, (int) POLL_TIMEOUT_MS);
                }
            }
        } catch (Throwable t) {
            emitErrorBlocking((byte) 0, "I/O thread failure: " + t.getMessage());
        } finally {
            // Wake any user thread blocked on events.take(). Without this, a close()
            // (or any abnormal exit) while a user thread is mid-execute() would let
            // takeEvent() block forever -- once the I/O thread is gone, no further
            // events arrive on the queue.
            if (!currentQueryDone) {
                emitErrorBlocking((byte) 0, shutdown
                        ? "I/O thread shut down with query in flight"
                        : "I/O thread terminated with query in flight");
                currentQueryDone = true;
            }
        }
    }

    /**
     * Signals shutdown. Does not join the thread -- caller handles that.
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
     * Blocking pop of the next event. Called by the user thread during {@code execute()}.
     */
    public QueryEvent takeEvent() throws InterruptedException {
        return events.take();
    }

    private void decodeAndEmitError(long payload, int payloadLen) {
        QueryEvent ev = decodeError(payload, payloadLen);
        events.offer(ev);
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

    /**
     * Like {@link #emitError} but blocks until the event is enqueued. Used on shutdown
     * and fatal-error paths where dropping the event would leave the user thread
     * blocked on {@link #takeEvent} indefinitely.
     */
    private void emitErrorBlocking(byte status, String message) {
        try {
            events.put(new QueryEvent().asError(status, message));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
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

    /**
     * Frees native scratch owned by the pool. Call after the thread has terminated.
     * <p>
     * Drains any unconsumed batch events still in the events queue and closes
     * their buffers. Without this, a close() that races with an in-flight query
     * would leak the {@link QwpBatchBuffer} native scratches that were enqueued
     * but never consumed.
     * <p>
     * Pushes a final sentinel error onto the events queue so any user thread
     * blocked on {@link #takeEvent} (or that returns from a handler after the
     * pool has been drained) wakes up with a clear error rather than blocking
     * forever on an empty queue.
     */
    void closePool() {
        Misc.free(sendScratch);
        QueryEvent ev;
        while ((ev = events.poll()) != null) {
            if (ev.kind == QueryEvent.KIND_BATCH && ev.buffer != null) {
                ev.buffer.close();
            }
        }
        for (QwpBatchBuffer b : freeBuffers) {
            b.close();
        }
        freeBuffers.clear();
        // The events queue capacity is bufferPoolSize + 2 with no consumer competing
        // for slots after the I/O thread has joined, so offer is guaranteed to succeed.
        events.offer(new QueryEvent().asError((byte) 0, "QwpQueryClient closed"));
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
