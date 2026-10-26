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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.http.client.WebSocketSendBuffer;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;

import java.util.ArrayDeque;
import java.util.List;

/**
 * In-process {@link WebSocketClient} that hands every frame the send loop writes
 * to a {@link QwpServerModel} and plays the model's responses back, so loop tests
 * see real server commit semantics without a socket.
 * <p>
 * Each instance is one model connection, opened by the constructor. Each
 * {@code sendBinary} call and each {@link #sendBinaryBatch} call is one socket
 * write: the client records it with {@link QwpServerModel#recordSocketWrite},
 * counting the bytes a socket would carry, masked WebSocket headers included,
 * then passes each frame it carries to {@link QwpServerModel#receive}, in order.
 * A batch is built in the real send buffer by the base class's
 * {@code appendBinaryToBatch}, and {@link #sendBinaryBatch} decodes it frame by
 * frame. Batching is on by default; {@link #setBatchSendSupported} turns it off,
 * the way a client without it behaves.
 * <p>
 * The responses queue on this instance, and each {@link #tryReceiveFrame} call
 * delivers one of them from native memory. {@link #holdResponses} keeps them
 * queued, so a test can let the loop send more frames before it sees a
 * response, and {@link #dropConnection()} models a dead link.
 * <p>
 * The send loop calls the transport methods on its I/O thread; any thread may
 * call {@link #holdResponses} and {@link #dropConnection()}.
 */
public final class ServerModelClient extends WebSocketClient {
    private static final int QWP_VERSION = 1;
    private final int connection;
    private final QwpServerModel model;
    private final ArrayDeque<byte[]> responses = new ArrayDeque<>();
    private boolean hasReportedDrop;
    private volatile boolean isBatchSendSupported = true;
    private volatile boolean isDropped;
    private volatile boolean isHoldingResponses;

    public ServerModelClient(QwpServerModel model) {
        super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        this.model = model;
        this.connection = model.openConnection();
    }

    /**
     * Starts a batch in the base class's send buffer. Unlike the base class it
     * skips the connected check: this client has no upgraded socket.
     */
    @Override
    public void beginBinaryBatch() {
        getSendBuffer().reset();
    }

    /**
     * The model connection id of this client.
     */
    public int connection() {
        return connection;
    }

    /**
     * Drops the link: the next {@link #tryReceiveFrame} reports
     * {@code onClose(1001, "drop")} once, queued responses are discarded, and
     * frames sent from now on never reach the model.
     */
    public void dropConnection() {
        isDropped = true;
    }

    @Override
    public int getServerQwpVersion() {
        return QWP_VERSION;
    }

    /**
     * While held, responses queue on this client and {@link #tryReceiveFrame}
     * delivers none; once released, they are delivered in order. A drop is
     * reported even while held.
     */
    public void holdResponses(boolean hold) {
        isHoldingResponses = hold;
    }

    @Override
    public boolean isBatchSendSupported() {
        return isBatchSendSupported;
    }

    @Override
    public void sendBinary(long dataPtr, int length) {
        if (isDropped) {
            return;
        }
        byte[] frame = new byte[length];
        copyToArray(dataPtr, frame, 0, length);
        model.recordSocketWrite(WebSocketFrameWriter.headerSize(length, true) + length);
        deliver(frame);
    }

    @Override
    public void sendBinary(long firstPtr, int firstLength, long secondPtr, int secondLength) {
        if (isDropped) {
            return;
        }
        int length = firstLength + secondLength;
        byte[] frame = new byte[length];
        copyToArray(firstPtr, frame, 0, firstLength);
        copyToArray(secondPtr, frame, firstLength, secondLength);
        model.recordSocketWrite(WebSocketFrameWriter.headerSize(length, true) + length);
        deliver(frame);
    }

    /**
     * One socket write of the batch in the send buffer: records its byte count,
     * then unmasks each WebSocket frame in it and passes the frames to the model
     * in order.
     */
    @Override
    public void sendBinaryBatch() {
        WebSocketSendBuffer buffer = getSendBuffer();
        try {
            if (isDropped) {
                return;
            }
            long ptr = buffer.getBufferPtr();
            int length = buffer.getWritePos();
            model.recordSocketWrite(length);
            int pos = 0;
            while (pos < length) {
                pos = deliverMaskedFrame(ptr, pos, length);
            }
        } finally {
            buffer.reset();
        }
    }

    /**
     * Whether this client sends a batch with one socket write; true by default.
     * Set it before the loop gets the client.
     */
    public void setBatchSendSupported(boolean isSupported) {
        isBatchSendSupported = isSupported;
    }

    @Override
    public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
        if (isDropped) {
            if (hasReportedDrop) {
                return false;
            }
            hasReportedDrop = true;
            synchronized (responses) {
                responses.clear();
            }
            handler.onClose(1001, "drop");
            return true;
        }
        if (isHoldingResponses) {
            return false;
        }
        byte[] response;
        synchronized (responses) {
            response = responses.pollFirst();
        }
        if (response == null) {
            return false;
        }
        long ptr = Unsafe.malloc(response.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < response.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + i, response[i]);
            }
            handler.onBinaryMessage(ptr, response.length);
        } finally {
            Unsafe.free(ptr, response.length, MemoryTag.NATIVE_DEFAULT);
        }
        return true;
    }

    @Override
    protected void ioWait(int timeout, int op) {
    }

    @Override
    protected void setupIoWait() {
    }

    private static void copyToArray(long src, byte[] dst, int offset, int length) {
        for (int i = 0; i < length; i++) {
            dst[offset + i] = Unsafe.getUnsafe().getByte(src + i);
        }
    }

    private void deliver(byte[] frame) {
        List<byte[]> out = model.receive(connection, frame);
        synchronized (responses) {
            responses.addAll(out);
        }
    }

    // Decodes the masked binary WebSocket frame at ptr + pos, delivers its
    // payload and returns the position just past it.
    private int deliverMaskedFrame(long ptr, int pos, int limit) {
        int byte0 = Unsafe.getUnsafe().getByte(ptr + pos) & 0xFF;
        int byte1 = Unsafe.getUnsafe().getByte(ptr + pos + 1) & 0xFF;
        if (byte0 != 0x82 || (byte1 & 0x80) == 0) {
            throw new IllegalStateException("not a masked final binary frame at " + pos
                    + ": " + Integer.toHexString(byte0) + ' ' + Integer.toHexString(byte1));
        }
        int lengthCode = byte1 & 0x7F;
        int headerLength;
        long payloadLength;
        if (lengthCode == 126) {
            payloadLength = ((Unsafe.getUnsafe().getByte(ptr + pos + 2) & 0xFF) << 8)
                    | (Unsafe.getUnsafe().getByte(ptr + pos + 3) & 0xFF);
            headerLength = 4;
        } else if (lengthCode == 127) {
            payloadLength = Long.reverseBytes(Unsafe.getUnsafe().getLong(ptr + pos + 2));
            headerLength = 10;
        } else {
            payloadLength = lengthCode;
            headerLength = 2;
        }
        long maskAddr = ptr + pos + headerLength;
        long payloadAddr = maskAddr + 4;
        if (payloadLength > Integer.MAX_VALUE || payloadAddr + payloadLength > ptr + limit) {
            throw new IllegalStateException("frame at " + pos + " runs past the batch: " + payloadLength);
        }
        byte[] frame = new byte[(int) payloadLength];
        for (int i = 0; i < frame.length; i++) {
            byte mask = Unsafe.getUnsafe().getByte(maskAddr + (i & 3));
            frame[i] = (byte) (Unsafe.getUnsafe().getByte(payloadAddr + i) ^ mask);
        }
        deliver(frame);
        return (int) (payloadAddr + payloadLength - ptr);
    }

}
