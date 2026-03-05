/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.QuietCloseable;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Encodes ILP v4 messages for WebSocket transport.
 * <p>
 * This encoder delegates column encoding to {@link QwpColumnWriter} and wraps
 * the encoded payload with a 12-byte ILP4 header.
 */
public class QwpWebSocketEncoder implements QuietCloseable {

    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private NativeBufferWriter buffer;
    private byte flags;

    public QwpWebSocketEncoder() {
        this.buffer = new NativeBufferWriter();
        this.flags = 0;
    }

    public QwpWebSocketEncoder(int bufferSize) {
        this.buffer = new NativeBufferWriter(bufferSize);
        this.flags = 0;
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer.close();
            buffer = null;
        }
    }

    public int encode(QwpTableBuffer tableBuffer, boolean useSchemaRef) {
        buffer.reset();
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();
        columnWriter.setBuffer(buffer);
        columnWriter.encodeTable(tableBuffer, useSchemaRef, false, isGorillaEnabled());
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
    }

    public int encodeWithDeltaDict(
            QwpTableBuffer tableBuffer,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId,
            boolean useSchemaRef
    ) {
        buffer.reset();
        int deltaStart = confirmedMaxId + 1;
        int deltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        byte savedFlags = flags;
        flags |= FLAG_DELTA_SYMBOL_DICT;
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();
        buffer.putVarint(deltaStart);
        buffer.putVarint(deltaCount);
        for (int id = deltaStart; id < deltaStart + deltaCount; id++) {
            String symbol = globalDict.getSymbol(id);
            buffer.putString(symbol);
        }
        columnWriter.setBuffer(buffer);
        columnWriter.encodeTable(tableBuffer, useSchemaRef, true, isGorillaEnabled());
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        flags = savedFlags;
        return buffer.getPosition();
    }

    public QwpBufferWriter getBuffer() {
        return buffer;
    }

    public boolean isGorillaEnabled() {
        return (flags & FLAG_GORILLA) != 0;
    }

    public void setGorillaEnabled(boolean enabled) {
        if (enabled) {
            flags |= FLAG_GORILLA;
        } else {
            flags &= ~FLAG_GORILLA;
        }
    }

    public void writeHeader(int tableCount, int payloadLength) {
        buffer.putByte((byte) 'Q');
        buffer.putByte((byte) 'W');
        buffer.putByte((byte) 'P');
        buffer.putByte((byte) '1');
        buffer.putByte(VERSION_1);
        buffer.putByte(flags);
        buffer.putShort((short) tableCount);
        buffer.putInt(payloadLength);
    }
}
