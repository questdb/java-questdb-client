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

import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.QuietCloseable;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Encodes QWP v1 messages for WebSocket transport.
 * <p>
 * This encoder delegates column encoding to {@link QwpColumnWriter} and wraps
 * the encoded payload with a 12-byte QWP v1 header.
 */
public class QwpWebSocketEncoder implements QuietCloseable {

    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private NativeBufferWriter buffer;
    // QWP ingress always advertises Gorilla timestamp encoding. The column
    // writer still emits a per-column encoding byte and falls back to raw
    // values when delta-of-delta overflows int32.
    private byte flags = FLAG_GORILLA;
    private int payloadStart;
    private byte version = VERSION;

    public QwpWebSocketEncoder() {
        this.buffer = new NativeBufferWriter();
    }

    public QwpWebSocketEncoder(int bufferSize) {
        this.buffer = new NativeBufferWriter(bufferSize);
    }

    public void addTable(QwpTableBuffer tableBuffer) {
        columnWriter.encodeTable(tableBuffer, true, true);
    }

    public void beginMessage(
            int tableCount,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId
    ) {
        buffer.reset();
        int deltaStart = confirmedMaxId + 1;
        int deltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        byte headerFlags = (byte) (flags | FLAG_DELTA_SYMBOL_DICT);
        byte origFlags = flags;
        flags = headerFlags;
        writeHeader(tableCount, 0);
        flags = origFlags;
        payloadStart = buffer.getPosition();
        buffer.putVarint(deltaStart);
        buffer.putVarint(deltaCount);
        for (int id = deltaStart; id < deltaStart + deltaCount; id++) {
            String symbol = globalDict.getSymbol(id);
            buffer.putString(symbol);
        }
        columnWriter.setBuffer(buffer);
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer.close();
            buffer = null;
        }
    }

    public int encode(QwpTableBuffer tableBuffer) {
        buffer.reset();
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();
        columnWriter.setBuffer(buffer);
        columnWriter.encodeTable(tableBuffer, false, true);
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
    }

    public int encodeWithDeltaDict(
            QwpTableBuffer tableBuffer,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId
    ) {
        beginMessage(1, globalDict, confirmedMaxId, batchMaxId);
        addTable(tableBuffer);
        return finishMessage();
    }

    public int finishMessage() {
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
    }

    public QwpBufferWriter getBuffer() {
        return buffer;
    }

    public void setDeferCommit(boolean defer) {
        if (defer) {
            flags |= FLAG_DEFER_COMMIT;
        } else {
            flags &= ~FLAG_DEFER_COMMIT;
        }
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public void writeHeader(int tableCount, int payloadLength) {
        buffer.putByte((byte) 'Q');
        buffer.putByte((byte) 'W');
        buffer.putByte((byte) 'P');
        buffer.putByte((byte) '1');
        buffer.putByte(version);
        buffer.putByte(flags);
        buffer.putShort((short) tableCount);
        buffer.putInt(payloadLength);
    }
}
