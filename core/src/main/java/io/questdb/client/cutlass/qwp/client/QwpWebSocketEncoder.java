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
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;

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
    // Byte offsets, within the buffer, of the symbol-dict delta ENTRY region
    // ([len][utf8]... only, without the two section varints) that beginMessage
    // last wrote. Let the producer persist those bytes straight to the slot's
    // .symbol-dict instead of re-encoding the same symbols (see
    // QwpWebSocketSender.persistNewSymbolsBeforePublish). Valid until the next
    // beginMessage; stored as offsets so they survive a buffer realloc.
    private int deltaCount;
    private int deltaEntriesEnd;
    private int deltaEntriesStart;
    private int deltaStart;
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
        deltaStart = confirmedMaxId + 1;
        deltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        byte headerFlags = (byte) (flags | FLAG_DELTA_SYMBOL_DICT);
        byte origFlags = flags;
        flags = headerFlags;
        writeHeader(tableCount, 0);
        flags = origFlags;
        payloadStart = buffer.getPosition();
        buffer.putVarint(deltaStart);
        buffer.putVarint(deltaCount);
        deltaEntriesStart = buffer.getPosition();
        for (int id = deltaStart; id < deltaStart + deltaCount; id++) {
            String symbol = globalDict.getSymbol(id);
            buffer.putString(symbol);
        }
        deltaEntriesEnd = buffer.getPosition();
        columnWriter.setBuffer(buffer);
    }

    @Override
    public void close() {
        if (buffer != null) {
            buffer.close();
            buffer = null;
        }
    }

    /**
     * Copies one single-table split message from the combined message currently
     * staged in this encoder. The table body is copied byte-for-byte from its
     * recorded offset; columns and rows are not encoded again.
     */
    public int copySplitMessage(
            MicrobatchBuffer target,
            int tableBodyOffset,
            int tableBodyLength,
            boolean deferCommit,
            int confirmedMaxId,
            int batchMaxId
    ) {
        if (target.getBufferPos() != 0) {
            throw new IllegalStateException("split message target is not empty");
        }
        if (tableBodyOffset < deltaEntriesEnd
                || tableBodyLength < 0
                || (long) tableBodyOffset + tableBodyLength > buffer.getPosition()) {
            throw new IllegalArgumentException("table body slice is outside the staged message");
        }

        int splitDeltaStart = confirmedMaxId + 1;
        int splitDeltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        int deltaEntriesLength = splitDeltaEntriesLength(splitDeltaStart, splitDeltaCount);
        int messageSize = splitMessageSize(
                tableBodyLength, splitDeltaStart, splitDeltaCount, deltaEntriesLength);
        target.ensureCapacity(messageSize);

        long source = buffer.getBufferPtr();
        long destination = target.getBufferPtr();
        Vect.memcpy(destination, source, HEADER_SIZE);

        byte splitFlags = Unsafe.getUnsafe().getByte(source + HEADER_OFFSET_FLAGS);
        if (deferCommit) {
            splitFlags |= FLAG_DEFER_COMMIT;
        } else {
            splitFlags &= ~FLAG_DEFER_COMMIT;
        }
        Unsafe.getUnsafe().putByte(destination + HEADER_OFFSET_FLAGS, splitFlags);
        Unsafe.getUnsafe().putShort(destination + 6, (short) 1);
        Unsafe.getUnsafe().putInt(destination + 8, messageSize - HEADER_SIZE);

        long writeAddress = destination + HEADER_SIZE;
        writeAddress = NativeBufferWriter.writeVarint(writeAddress, splitDeltaStart);
        writeAddress = NativeBufferWriter.writeVarint(writeAddress, splitDeltaCount);
        if (deltaEntriesLength > 0) {
            Vect.memcpy(writeAddress, source + deltaEntriesStart, deltaEntriesLength);
            writeAddress += deltaEntriesLength;
        }
        Vect.memcpy(writeAddress, source + tableBodyOffset, tableBodyLength);
        writeAddress += tableBodyLength;
        assert writeAddress == destination + messageSize;

        target.setBufferPos(messageSize);
        return messageSize;
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

    /**
     * Byte length of the symbol-dict delta ENTRY region ({@code [len][utf8]...},
     * excluding the two section varints) that {@link #beginMessage} last wrote.
     */
    public int getDeltaEntriesLen() {
        return deltaEntriesEnd - deltaEntriesStart;
    }

    /**
     * Byte offset, within {@link #getBuffer()}, of the symbol-dict delta ENTRY
     * region {@link #beginMessage} last wrote.
     */
    public int getDeltaEntriesStart() {
        return deltaEntriesStart;
    }

    public int getSplitMessageSize(int tableBodyLength, int confirmedMaxId, int batchMaxId) {
        if (tableBodyLength < 0) {
            throw new IllegalArgumentException("tableBodyLength must be non-negative");
        }
        int splitDeltaStart = confirmedMaxId + 1;
        int splitDeltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        int deltaEntriesLength = splitDeltaEntriesLength(splitDeltaStart, splitDeltaCount);
        return splitMessageSize(
                tableBodyLength, splitDeltaStart, splitDeltaCount, deltaEntriesLength);
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

    private int splitDeltaEntriesLength(int splitDeltaStart, int splitDeltaCount) {
        if (splitDeltaCount == 0) {
            return 0;
        }
        if (splitDeltaStart != deltaStart || splitDeltaCount != deltaCount) {
            throw new IllegalStateException("split delta does not match the staged message"
                    + " [stagedStart=" + deltaStart
                    + ", stagedCount=" + deltaCount
                    + ", splitStart=" + splitDeltaStart
                    + ", splitCount=" + splitDeltaCount + ']');
        }
        return deltaEntriesEnd - deltaEntriesStart;
    }

    private int splitMessageSize(
            int tableBodyLength,
            int splitDeltaStart,
            int splitDeltaCount,
            int deltaEntriesLength
    ) {
        long messageSize = (long) HEADER_SIZE
                + NativeBufferWriter.varintSize(splitDeltaStart)
                + NativeBufferWriter.varintSize(splitDeltaCount)
                + deltaEntriesLength
                + tableBodyLength;
        if (messageSize > Integer.MAX_VALUE) {
            throw new OutOfMemoryError("split QWP message size overflow: " + messageSize);
        }
        return (int) messageSize;
    }
}
