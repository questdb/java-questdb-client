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
import java.util.concurrent.atomic.AtomicLong;

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
    private static final Object RELEASE_TOKEN = new Object();
    private final QwpResultBatchDecoder decoder = new QwpResultBatchDecoder();
    // Events delivered from I/O thread (producer) to user thread (consumer):
    // RESULT_BATCH / RESULT_END / EXEC_DONE / QUERY_ERROR. Hot path of every
    // query. Purpose-built SPSC with spin-then-park avoids the ~2 microseconds
    // per offer/take that j.u.c.ArrayBlockingQueue burns on its ReentrantLock +
    // Condition pair for an empty queue hand-off.
    private final QwpSpscQueue<QueryEvent> events;
    // Pool of pre-allocated buffers. I/O thread takes, user thread releases.
    // Kept on ArrayBlockingQueue: the take uses a timeout to poll pending
    // cancels while waiting, a pattern the SPSC queue doesn't offer.
    private final BlockingQueue<QwpBatchBuffer> freeBuffers;
    // Pending CANCEL requestId set by the user thread via {@link #requestCancel}.
    // The I/O thread polls this between {@code receiveFrame} iterations; when
    // non-negative, it emits a CANCEL frame for that requestId and resets to -1.
    // Using AtomicLong (not volatile long) to guarantee 64-bit atomicity on all
    // supported JVMs.
    private final AtomicLong pendingCancelRequestId = new AtomicLong(-1L);
    // One-slot release latch: user thread offers a token from releaseBuffer
    // (producer), I/O thread drains it before returning from onBinaryMessage
    // (consumer). Holds the payload bytes in the WebSocket recv buffer steady
    // for the duration of the user handler, since in-place decode makes batch
    // pointers alias those bytes. Same SPSC reasoning as {@link #events} --
    // the I/O thread typically parks on this for the duration of the user's
    // onBatch callback, which is microseconds for a no-op consumer.
    private final QwpSpscQueue<Object> pendingRelease = new QwpSpscQueue<>(1);
    // Single-slot request queue (Phase-1 allows one in-flight query).
    private final BlockingQueue<QueryRequest> requests = new ArrayBlockingQueue<>(1);
    private final NativeBufferWriter sendScratch = new NativeBufferWriter();
    private final WebSocketClient wsClient;
    // Per-query credit state (accessed only from the I/O thread).
    // creditEnabled == (initialCredit > 0); controls whether we emit CREDIT
    // replenish frames after each batch release.
    // Set true by closePool() before it drains freeBuffers / events, so any
    // user thread still inside releaseBuffer (or one that returns from onBatch
    // after close has run) sees closed == true and frees the buffer in place
    // instead of stranding it in a queue nobody will iterate again.
    // Volatile because the writer (close-caller thread) and reader (user
    // thread inside releaseBuffer) are different threads.
    private volatile boolean closed;
    private boolean creditEnabled;
    private boolean currentQueryDone;
    private long currentRequestId = -1L;
    private volatile boolean shutdown;

    public QwpEgressIoThread(WebSocketClient wsClient, int bufferPoolSize) {
        this.wsClient = wsClient;
        this.freeBuffers = new ArrayBlockingQueue<>(bufferPoolSize);
        // +2 reserves slots for a trailing RESULT_END and a synthetic error
        // frame the I/O thread may emit on shutdown, so a full buffer pool
        // never stalls the producer.
        this.events = new QwpSpscQueue<>(bufferPoolSize + 2);
        // Track allocations as we go so a partial-pool failure (e.g. native
        // OOM at iteration K) frees the K-1 buffers already in freeBuffers.
        // Without this, the half-built QwpEgressIoThread instance escapes the
        // failed constructor and is unreachable from QwpQueryClient.close(),
        // leaking those buffers' native scratches for the JVM's lifetime.
        try {
            for (int i = 0; i < bufferPoolSize; i++) {
                freeBuffers.offer(new QwpBatchBuffer(DEFAULT_BUFFER_CAPACITY));
            }
        } catch (Throwable t) {
            for (QwpBatchBuffer b : freeBuffers) {
                b.close();
            }
            freeBuffers.clear();
            throw t;
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
        } else if (msgKind == QwpEgressMsgKind.EXEC_DONE) {
            decodeAndEmitExecDone(payloadPtr, payloadLen);
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
     * <p>
     * Also signals the release latch so the I/O thread, parked at the end of
     * {@code handleResultBatch}, can resume and let the WebSocket recv buffer
     * compact past the consumed frame.
     */
    public void releaseBuffer(QwpBatchBuffer buffer) {
        if (closed) {
            // closePool already drained and cleared freeBuffers; offering this
            // buffer back into the pool would strand it (no consumer left to
            // close it). Free its native scratch in place. The pendingRelease
            // queue is also abandoned -- the I/O thread that would have read
            // the token is gone.
            buffer.close();
            return;
        }
        freeBuffers.offer(buffer);
        pendingRelease.offer(RELEASE_TOKEN);
    }

    /**
     * Queues a CANCEL frame for {@code requestId} to be sent by the I/O thread
     * between the next two {@code receiveFrame} iterations (typically within
     * {@link #POLL_TIMEOUT_MS}). Safe to call from any thread. If a CANCEL for
     * the same (or another) requestId is already pending, the newer id wins --
     * multiple concurrent cancels coalesce into one send.
     */
    public void requestCancel(long requestId) {
        pendingCancelRequestId.set(requestId);
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
                currentRequestId = req.requestId;
                creditEnabled = req.initialCredit > 0L;
                sendQueryRequest(req);

                while (!currentQueryDone && !shutdown) {
                    // onBinaryMessage (on this same thread) sets currentQueryDone.
                    wsClient.receiveFrame(this, (int) POLL_TIMEOUT_MS);
                    drainPendingCancel();
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
     * Blocking submission of a query. Called by the user thread. {@code initialCredit}
     * is the client-advertised send-ahead budget in bytes -- 0 means "unbounded"
     * (server streams without flow control, Phase-1 default); any positive value
     * asks the server to suspend once it has emitted that many bytes and wait
     * for a CREDIT frame.
     */
    public void submitQuery(String sql, long requestId, long initialCredit) throws InterruptedException {
        requests.put(new QueryRequest(sql, requestId, initialCredit));
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
     * EXEC_DONE body: msg_kind(1) + requestId(8) + op_type(1) + rows_affected(varint).
     * Parses all fields, surfaces as a {@link QueryEvent#KIND_EXEC_DONE} event.
     */
    private void decodeAndEmitExecDone(long payload, int payloadLen) {
        long p = payload + QwpConstants.HEADER_SIZE + 1 + 8;
        long limit = payload + payloadLen;
        if (p + 1 > limit) {
            emitError((byte) 0, "EXEC_DONE frame truncated before op_type");
            return;
        }
        byte opType = Unsafe.getUnsafe().getByte(p++);
        long rowsAffected = 0;
        int shift = 0;
        while (p < limit) {
            byte b = Unsafe.getUnsafe().getByte(p++);
            rowsAffected |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) {
                emitError((byte) 0, "EXEC_DONE rows_affected varint overflow");
                return;
            }
        }
        events.offer(new QueryEvent().asExecDone(opType, rowsAffected));
    }

    /**
     * RESULT_END body: msg_kind(1) + requestId(8) + final_seq(varint) + total_rows(varint).
     * We only need total_rows. Both varint loops cap at 10 bytes so a hostile
     * server cannot drive {@code shift} past 63 (where Java's {@code <<} masks
     * the count to 6 bits and silently wraps high bytes back into the low bits,
     * producing a wildly wrong total).
     */
    private long decodeResultEnd(long payload, int payloadLen) {
        long p = payload + QwpConstants.HEADER_SIZE + 1 + 8;
        long limit = payload + payloadLen;
        int seqBytes = 0;
        while (p < limit && (Unsafe.getUnsafe().getByte(p++) & 0x80) != 0) {
            if (++seqBytes > 9) {
                // Continuation bit set on byte 10 of the final_seq varint --
                // malformed. Surface a 0 total rather than read into the
                // total_rows section with a desynced cursor.
                return 0L;
            }
        }
        long total = 0;
        int shift = 0;
        while (p < limit) {
            byte b = Unsafe.getUnsafe().getByte(p++);
            total |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) {
                // Same overflow guard decodeAndEmitExecDone uses; without it,
                // byte 10 of total_rows could land in the sign bit and bytes
                // 11+ would silently wrap.
                return 0L;
            }
        }
        return total;
    }

    /**
     * Flushes the pending cancel (if any) to the wire. Called from the I/O
     * thread at every loop boundary so a cancel set by a user thread reaches
     * the server regardless of whether the I/O thread was waiting on a frame
     * or on a free buffer.
     */
    private void drainPendingCancel() {
        long id = pendingCancelRequestId.getAndSet(-1L);
        if (id >= 0L) {
            sendCancel(id);
        }
    }

    private void emitError(byte status, String message) {
        events.offer(new QueryEvent().asError(status, message));
    }

    /**
     * Like {@link #emitError} but retries until the event is enqueued. Used on
     * shutdown and fatal-error paths where dropping the event would leave the
     * user thread blocked on {@link #takeEvent} indefinitely.
     */
    private void emitErrorBlocking(byte status, String message) {
        QueryEvent ev = new QueryEvent().asError(status, message);
        while (!events.offer(ev)) {
            // Events queue is bounded; the consumer side (user thread) drains
            // steadily under a cooperative shutdown. Yield between retries so
            // we don't busy-spin while the consumer is scheduled.
            Thread.yield();
            if (Thread.currentThread().isInterrupted()) {
                // Restore the interrupt and abandon the error publish. Caller
                // paths that rely on the error reaching the user may see a
                // silent drop -- acceptable under explicit interrupt.
                return;
            }
        }
    }

    private void handleResultBatch(long payloadPtr, int payloadLen) {
        QwpBatchBuffer buf;
        try {
            // Poll rather than take so a pending cancel set by the user thread
            // still gets flushed while the I/O thread is waiting for the user
            // to release a buffer. Without this, an app that never releases
            // (or sleeps in its handler) would wedge the cancel path -- the
            // server never sees the CANCEL and keeps on streaming.
            buf = freeBuffers.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            while (buf == null && !shutdown) {
                drainPendingCancel();
                buf = freeBuffers.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            if (buf == null) return;
        } catch (InterruptedException ie) {
            return;
        }
        // Decode in place: column layouts reference payloadPtr (the WebSocket recv
        // buffer) directly, skipping the previous per-batch memcpy into buf.scratchAddr.
        try {
            decoder.decode(buf, payloadPtr, payloadLen);
        } catch (QwpDecodeException e) {
            freeBuffers.offer(buf);
            emitError((byte) 0, "decode failure: " + e.getMessage());
            currentQueryDone = true;
            return;
        }
        events.offer(new QueryEvent().asBatch(buf));
        // Park on the release latch. Returning sooner would let receiveFrame
        // compact the WebSocket recv buffer, overwriting the bytes that the
        // user-visible column pointers still reference. User thread's
        // releaseBuffer offers the token that unblocks this take.
        try {
            pendingRelease.take();
        } catch (InterruptedException ie) {
            // Shutdown path: leave the batch to the user thread; they'll see
            // either the in-progress batch or a subsequent close event.
            return;
        }
        // Credit replenish: the user is done with the batch, so the recv-buffer
        // bytes are free. Tell the server it can stream {@code payloadLen} more
        // bytes. No-op when the query wasn't started under flow control.
        if (creditEnabled) {
            sendCredit(currentRequestId, payloadLen);
        }
    }

    /**
     * Builds and transmits a CANCEL frame on the WebSocket. Wire format:
     * {@code msg_kind(0x14) + request_id(u64)}.
     */
    private void sendCancel(long requestId) {
        sendScratch.reset();
        sendScratch.putByte(QwpEgressMsgKind.CANCEL);
        sendScratch.putLong(requestId);
        wsClient.sendBinary(sendScratch.getBufferPtr(), sendScratch.getPosition());
        sendScratch.reset();
    }

    /**
     * Builds and transmits a CREDIT frame on the WebSocket. Wire format:
     * {@code msg_kind(0x15) + request_id(u64) + additional_bytes(varint)}.
     */
    private void sendCredit(long requestId, long additionalBytes) {
        sendScratch.reset();
        sendScratch.putByte(QwpEgressMsgKind.CREDIT);
        sendScratch.putLong(requestId);
        sendScratch.putVarint(additionalBytes);
        wsClient.sendBinary(sendScratch.getBufferPtr(), sendScratch.getPosition());
        sendScratch.reset();
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
        sendScratch.putVarint(req.initialCredit); // 0 = unbounded (Phase-1 default)
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
        // Set closed BEFORE draining so a user thread that returns from
        // onBatch concurrently and calls releaseBuffer sees closed=true and
        // frees the buffer in place rather than offering it into a queue
        // we're about to clear. Volatile write pairs with the volatile read
        // in releaseBuffer.
        closed = true;
        Misc.free(sendScratch);
        Misc.free(decoder);
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
        final long initialCredit;
        final long requestId;
        final String sql;

        QueryRequest(String sql, long requestId, long initialCredit) {
            this.sql = sql;
            this.requestId = requestId;
            this.initialCredit = initialCredit;
        }
    }
}
