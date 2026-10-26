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
 * {@code sendBinary} call is one socket write: the client records it with
 * {@link QwpServerModel#recordSocketWrite}, assembles the slices into one frame
 * and passes it to {@link QwpServerModel#receive}; the responses queue on this
 * instance, and each {@link #tryReceiveFrame} call delivers one of them from
 * native memory. {@link #holdResponses} keeps them queued, so a test can let
 * the loop send more frames before it sees a response, and
 * {@link #dropConnection()} models a dead link.
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
    private volatile boolean isDropped;
    private volatile boolean isHoldingResponses;

    public ServerModelClient(QwpServerModel model) {
        super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        this.model = model;
        this.connection = model.openConnection();
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
    public void sendBinary(long dataPtr, int length) {
        if (isDropped) {
            return;
        }
        byte[] frame = new byte[length];
        copyToArray(dataPtr, frame, 0, length);
        deliver(frame);
    }

    @Override
    public void sendBinary(long firstPtr, int firstLength, long secondPtr, int secondLength) {
        if (isDropped) {
            return;
        }
        byte[] frame = new byte[firstLength + secondLength];
        copyToArray(firstPtr, frame, 0, firstLength);
        copyToArray(secondPtr, frame, firstLength, secondLength);
        deliver(frame);
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
        model.recordSocketWrite(frame.length);
        List<byte[]> out = model.receive(connection, frame);
        synchronized (responses) {
            responses.addAll(out);
        }
    }

}
