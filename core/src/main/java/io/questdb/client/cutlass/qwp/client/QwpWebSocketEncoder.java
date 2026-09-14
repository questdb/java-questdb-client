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

import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
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
    private boolean schemaMessage;
    private byte version = VERSION;

    public QwpWebSocketEncoder() {
        this.buffer = new NativeBufferWriter();
    }

    public QwpWebSocketEncoder(int bufferSize) {
        this.buffer = new NativeBufferWriter(bufferSize);
    }

    public void addTable(QwpTableBuffer tableBuffer) {
        rejectBoundLegacyTable(tableBuffer);
        if (schemaMessage) {
            throw new IllegalStateException("legacy table cannot be added to a schema message");
        }
        columnWriter.encodeTable(tableBuffer, true, true);
    }

    public void addSchemaTable(QwpTableBuffer tableBuffer, int tableId, long metadataVersion) {
        validateBoundSchemaTable(tableBuffer, tableId, metadataVersion, true);
        if (!schemaMessage) {
            throw new IllegalStateException("schema table requires a schema message");
        }
        validateIdentity(tableId, metadataVersion);
        columnWriter.encodeSchemaTable(tableBuffer, tableId, metadataVersion, true, true);
    }

    public void beginMessage(
            int tableCount,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId
    ) {
        schemaMessage = false;
        beginMessage0(tableCount, globalDict, confirmedMaxId, batchMaxId);
    }

    public void beginSchemaMessage(
            int tableCount,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId
    ) {
        if (tableCount <= 0) {
            throw new IllegalArgumentException("schema message must contain at least one table");
        }
        rejectControlFlag();
        schemaMessage = true;
        beginMessage0(tableCount, globalDict, confirmedMaxId, batchMaxId);
    }

    private void beginMessage0(
            int tableCount,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId
    ) {
        buffer.reset();
        deltaStart = confirmedMaxId + 1;
        deltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        byte headerFlags = (byte) (flags | FLAG_DELTA_SYMBOL_DICT | (schemaMessage ? FLAG_SCHEMA : 0));
        writeHeader(tableCount, 0, headerFlags);
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
        rejectBoundLegacyTable(tableBuffer);
        schemaMessage = false;
        buffer.reset();
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();
        columnWriter.setBuffer(buffer);
        columnWriter.encodeTable(tableBuffer, false, true);
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
    }

    public int encodeSchema(QwpTableBuffer tableBuffer, int tableId, long metadataVersion) {
        validateIdentity(tableId, metadataVersion);
        validateBoundSchemaTable(tableBuffer, tableId, metadataVersion, false);
        if (tableBuffer.getSchemaBinding() != null && tableBuffer.getRowCount() == 0) {
            return 0;
        }
        rejectControlFlag();
        schemaMessage = true;
        buffer.reset();
        writeHeader(1, 0, (byte) (flags | FLAG_SCHEMA));
        int payloadStart = buffer.getPosition();
        columnWriter.setBuffer(buffer);
        columnWriter.encodeSchemaTable(tableBuffer, tableId, metadataVersion, false, true);
        buffer.patchInt(8, buffer.getPosition() - payloadStart);
        return buffer.getPosition();
    }

    public int encodeSchema(QwpTableBuffer tableBuffer) {
        QwpSchemaBinding binding = tableBuffer.getSchemaBinding();
        if (binding == null) {
            throw new IllegalStateException("schema encoding requires an attached schema binding");
        }
        return encodeSchema(tableBuffer, binding.getTableId(), binding.getMetadataVersion());
    }

    public int encodeWithDeltaDict(
            QwpTableBuffer tableBuffer,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId
    ) {
        rejectBoundLegacyTable(tableBuffer);
        beginMessage(1, globalDict, confirmedMaxId, batchMaxId);
        addTable(tableBuffer);
        return finishMessage();
    }

    public int finishMessage() {
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
    }

    private static void validateIdentity(int tableId, long metadataVersion) {
        if (!((tableId == -1 && metadataVersion == -1) || (tableId >= 0 && metadataVersion >= 0))) {
            throw new IllegalArgumentException("schema identity must be both known or both unknown");
        }
    }

    private static void rejectBoundLegacyTable(QwpTableBuffer tableBuffer) {
        if (tableBuffer.getSchemaBinding() != null) {
            throw new IllegalStateException("schema-bound table cannot use legacy framing");
        }
    }

    private static void validateBoundSchemaTable(
            QwpTableBuffer tableBuffer,
            int tableId,
            long metadataVersion,
            boolean additive
    ) {
        QwpSchemaBinding binding = tableBuffer.getSchemaBinding();
        if (binding == null) {
            return;
        }
        if (binding.getTableId() != tableId || binding.getMetadataVersion() != metadataVersion) {
            throw new IllegalArgumentException("explicit schema identity does not match the attached binding");
        }
        if (tableBuffer.hasInProgressRow()) {
            throw new IllegalStateException("cannot encode a schema-bound table with an incomplete row");
        }
        if (additive && tableBuffer.getRowCount() == 0) {
            throw new IllegalStateException("cannot add an empty schema-bound table");
        }
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
        writeHeader(tableCount, payloadLength, flags);
    }

    private void writeHeader(int tableCount, int payloadLength, byte headerFlags) {
        if (tableCount < 0 || tableCount > 0xffff) {
            throw new IllegalArgumentException("QWP table count is outside unsigned-short range: " + tableCount);
        }
        buffer.putByte((byte) 'Q');
        buffer.putByte((byte) 'W');
        buffer.putByte((byte) 'P');
        buffer.putByte((byte) '1');
        buffer.putByte(version);
        buffer.putByte(headerFlags);
        buffer.putShort((short) tableCount);
        buffer.putInt(payloadLength);
    }

    private void rejectControlFlag() {
        if ((flags & io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol.FLAG_CONTROL) != 0) {
            throw new IllegalStateException("schema data messages cannot use the control flag");
        }
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
