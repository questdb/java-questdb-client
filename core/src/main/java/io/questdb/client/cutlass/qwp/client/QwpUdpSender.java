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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.ArrayBufferAppender;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.line.udp.UdpLineChannel;
import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.network.NetworkFacade;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Fire-and-forget ILP v4 sender over UDP.
 * <p>
 * Each {@link #flush()} encodes all buffered table data into self-contained
 * datagrams (one per table) and sends them via UDP. Datagrams use local
 * symbol dictionaries (no global/delta dict) and full schema (no schema refs).
 * <p>
 * When {@code maxDatagramSize > 0}, the sender automatically flushes before
 * a datagram exceeds the size limit. The in-progress row stays staged in sender
 * state until commit, so committed table data can be flushed without replaying
 * the row back into column storage.
 */
public class QwpUdpSender implements Sender {
    private static final byte ENTRY_AT_MICROS = 1;
    private static final byte ENTRY_AT_NANOS = 2;
    private static final byte ENTRY_BOOL = 3;
    private static final byte ENTRY_DECIMAL128 = 4;
    private static final byte ENTRY_DECIMAL256 = 5;
    private static final byte ENTRY_DECIMAL64 = 6;
    private static final byte ENTRY_DOUBLE = 7;
    private static final byte ENTRY_DOUBLE_ARRAY = 8;
    private static final byte ENTRY_LONG = 9;
    private static final byte ENTRY_LONG_ARRAY = 10;
    private static final byte ENTRY_STRING = 11;
    private static final byte ENTRY_SYMBOL = 12;
    private static final byte ENTRY_TIMESTAMP_COL_MICROS = 13;
    private static final byte ENTRY_TIMESTAMP_COL_NANOS = 14;
    private static final int VARINT_INT_UPPER_BOUND = 5;
    private static final int SAFETY_MARGIN_BYTES = 8;
    private static final Logger LOG = LoggerFactory.getLogger(QwpUdpSender.class);

    private final ArraySizeCounter arraySizeCounter = new ArraySizeCounter();
    private final UdpLineChannel channel;
    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private final NativeBufferWriter headerBuffer = new NativeBufferWriter();
    private final int maxDatagramSize;
    private final SegmentedNativeBufferWriter payloadBuffer = new SegmentedNativeBufferWriter();
    private final boolean trackDatagramEstimate;
    private final ObjList<ColumnEntry> rowJournal = new ObjList<>();
    private final NativeSegmentList sendSegments = new NativeSegmentList();
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    private QwpTableBuffer.ColumnBuffer[] touchedColumns = new QwpTableBuffer.ColumnBuffer[8];

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private long committedEstimate;
    private int currentRowColumnCount;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;
    private QwpTableBuffer.ColumnBuffer[] missingColumns = new QwpTableBuffer.ColumnBuffer[8];
    private int missingColumnCount;
    private int rowJournalSize;
    private int touchedColumnCount;

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl) {
        this(nf, interfaceIPv4, sendToAddress, port, ttl, 0);
    }

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl, int maxDatagramSize) {
        this.channel = new UdpLineChannel(nf, interfaceIPv4, sendToAddress, port, ttl);
        this.tableBuffers = new CharSequenceObjHashMap<>();
        this.maxDatagramSize = maxDatagramSize;
        this.trackDatagramEstimate = maxDatagramSize > 0;
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        if (unit == ChronoUnit.NANOS) {
            atNanos(timestamp);
        } else {
            long micros = toMicros(timestamp, unit);
            atMicros(micros);
        }
    }

    @Override
    public void at(Instant timestamp) {
        checkNotClosed();
        checkTableSelected();
        long micros = timestamp.getEpochSecond() * 1_000_000L + timestamp.getNano() / 1000L;
        atMicros(micros);
    }

    @Override
    public void atNow() {
        checkNotClosed();
        checkTableSelected();
        commitCurrentRow();
    }

    @Override
    public Sender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        appendBooleanColumn(columnName, value, true);
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        throw new LineSenderException("bufferView() is not supported for UDP sender");
    }

    @Override
    public void cancelRow() {
        checkNotClosed();
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (hasInProgressRow()) {
                    currentTableBuffer.cancelCurrentRow();
                    currentTableBuffer.rollbackUncommittedColumns();
                    cachedTimestampColumn = null;
                    cachedTimestampNanosColumn = null;
                    clearStagedRow();
                }
                flushInternal();
            } catch (Exception e) {
                LOG.error("Error during close flush: {}", String.valueOf(e));
            }
            closed = true;
            ObjList<CharSequence> keys = tableBuffers.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                CharSequence key = keys.getQuick(i);
                if (key != null) {
                    QwpTableBuffer tb = tableBuffers.get(key);
                    if (tb != null) {
                        tb.close();
                    }
                }
            }
            tableBuffers.clear();
            channel.close();
            payloadBuffer.close();
            sendSegments.close();
            headerBuffer.close();
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDecimal64Column(name, value, true);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDecimal128Column(name, value, true);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDecimal256Column(name, value, true);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDoubleArrayColumn(name, values, true);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDoubleArrayColumn(name, values, true);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDoubleArrayColumn(name, values, true);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendDoubleArrayColumn(name, array, true);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        appendDoubleColumn(columnName, value, true);
        return this;
    }

    @Override
    public void flush() {
        checkNotClosed();
        ensureNoInProgressRow("flush buffer");
        flushInternal();
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendLongArrayColumn(name, values, true);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendLongArrayColumn(name, values, true);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendLongArrayColumn(name, values, true);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        appendLongArrayColumn(name, array, true);
        return this;
    }

    @Override
    public Sender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        appendLongColumn(columnName, value, true);
        return this;
    }

    @Override
    public void reset() {
        checkNotClosed();
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            QwpTableBuffer buf = tableBuffers.get(keys.getQuick(i));
            if (buf != null) {
                buf.rollbackUncommittedColumns();
                buf.reset();
            }
        }
        currentTableBuffer = null;
        currentTableName = null;
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
        resetCommittedEstimate();
    }

    @Override
    public Sender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        appendStringColumn(columnName, value, true);
        return this;
    }

    @Override
    public Sender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        appendSymbolColumn(columnName, value, true);
        return this;
    }

    @Override
    public Sender table(CharSequence tableName) {
        checkNotClosed();
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        ensureNoInProgressRow("switch tables");
        if (trackDatagramEstimate && currentTableBuffer != null && currentTableBuffer.getRowCount() > 0) {
            flushSingleTable(currentTableName, currentTableBuffer);
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
        resetCommittedEstimate();

        currentTableName = tableName.toString();
        currentTableBuffer = tableBuffers.get(currentTableName);
        if (currentTableBuffer == null) {
            currentTableBuffer = new QwpTableBuffer(currentTableName);
            tableBuffers.put(currentTableName, currentTableBuffer);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        if (unit == ChronoUnit.NANOS) {
            appendTimestampColumn(columnName, TYPE_TIMESTAMP_NANOS, value, ENTRY_TIMESTAMP_COL_NANOS, true);
        } else {
            long micros = toMicros(value, unit);
            appendTimestampColumn(columnName, TYPE_TIMESTAMP, micros, ENTRY_TIMESTAMP_COL_MICROS, true);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        appendTimestampColumn(columnName, TYPE_TIMESTAMP, micros, ENTRY_TIMESTAMP_COL_MICROS, true);
        return this;
    }

    private QwpTableBuffer.ColumnBuffer acquireColumn(CharSequence name, byte type, boolean nullable) {
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getExistingColumn(name, type);
        if (col == null && currentTableBuffer.getRowCount() > 0) {
            flushCommittedRowsOfCurrentTable();
            col = currentTableBuffer.getExistingColumn(name, type);
        }
        if (col == null) {
            col = currentTableBuffer.getOrCreateColumn(name, type, nullable);
        }
        return col;
    }

    private void appendBooleanColumn(CharSequence name, boolean value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_BOOLEAN, false);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_BOOL;
            e.column = col;
            e.boolValue = value;
        }
    }

    private void appendDecimal128Column(CharSequence name, Decimal128 value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL128, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL128;
            e.column = col;
            e.objectValue = value;
        }
    }

    private void appendDecimal256Column(CharSequence name, Decimal256 value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL256, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL256;
            e.column = col;
            e.objectValue = value;
        }
    }

    private void appendDecimal64Column(CharSequence name, Decimal64 value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL64, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL64;
            e.column = col;
            e.objectValue = value;
        }
    }

    private void appendDesignatedTimestamp(long value, boolean nanos, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col;
        if (nanos) {
            if (cachedTimestampNanosColumn == null) {
                cachedTimestampNanosColumn = acquireColumn("", TYPE_TIMESTAMP_NANOS, true);
            }
            col = cachedTimestampNanosColumn;
        } else {
            if (cachedTimestampColumn == null) {
                cachedTimestampColumn = acquireColumn("", TYPE_TIMESTAMP, true);
            }
            col = cachedTimestampColumn;
        }
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = nanos ? ENTRY_AT_NANOS : ENTRY_AT_MICROS;
            e.column = col;
            e.longValue = value;
        }
    }

    private void appendDoubleArrayColumn(CharSequence name, Object value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE_ARRAY, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE_ARRAY;
            e.column = col;
            e.objectValue = value;
        }
    }

    private void appendDoubleColumn(CharSequence name, double value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE, false);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE;
            e.column = col;
            e.doubleValue = value;
        }
    }

    private void appendLongArrayColumn(CharSequence name, Object value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG_ARRAY, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG_ARRAY;
            e.column = col;
            e.objectValue = value;
        }
    }

    private void appendLongColumn(CharSequence name, long value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG, false);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG;
            e.column = col;
            e.longValue = value;
        }
    }

    private void appendStringColumn(CharSequence name, CharSequence value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_STRING, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_STRING;
            e.column = col;
            e.stringValue = Chars.toString(value);
        }
    }

    private void appendSymbolColumn(CharSequence name, CharSequence value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_SYMBOL, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_SYMBOL;
            e.column = col;
            e.stringValue = Chars.toString(value);
        }
    }

    private void appendTimestampColumn(CharSequence name, byte type, long value, byte journalKind, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, type, true);
        currentRowColumnCount++;
        addTouchedColumn(col);

        if (addJournal) {
            ColumnEntry e = nextJournalEntry();
            e.kind = journalKind;
            e.column = col;
            e.longValue = value;
        }
    }

    private void atMicros(long timestampMicros) {
        if (currentRowColumnCount == 0) {
            throw new LineSenderException("no columns were provided");
        }
        appendDesignatedTimestamp(timestampMicros, false, true);
        commitCurrentRow();
    }

    private void atNanos(long timestampNanos) {
        if (currentRowColumnCount == 0) {
            throw new LineSenderException("no columns were provided");
        }
        appendDesignatedTimestamp(timestampNanos, true, true);
        commitCurrentRow();
    }

    private void checkNotClosed() {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
    }

    private void checkTableSelected() {
        if (currentTableBuffer == null) {
            throw new LineSenderException("table() must be called before adding columns");
        }
    }

    private void clearStagedRow() {
        for (int i = 0; i < rowJournalSize; i++) {
            ColumnEntry entry = rowJournal.getQuick(i);
            entry.column = null;
            entry.objectValue = null;
            entry.stringValue = null;
        }
        rowJournalSize = 0;
        currentRowColumnCount = 0;
        touchedColumnCount = 0;
        missingColumnCount = 0;
    }

    private void collectMissingColumns(int targetRows) {
        missingColumnCount = 0;
        for (int i = 0, n = currentTableBuffer.getColumnCount(); i < n; i++) {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getColumn(i);
            if (isTouchedColumn(col)) {
                continue;
            }
            if (col.getSize() >= targetRows) {
                continue;
            }
            ensureMissingColumnCapacity(missingColumnCount + 1);
            missingColumns[missingColumnCount++] = col;
        }
    }

    private void commitCurrentRow() {
        if (currentRowColumnCount == 0) {
            throw new LineSenderException("no columns were provided");
        }

        long estimate = 0;
        int targetRows = currentTableBuffer.getRowCount() + 1;
        collectMissingColumns(targetRows);
        if (trackDatagramEstimate) {
            estimate = estimateCurrentDatagramSizeWithStagedRow(targetRows);
            if (estimate > maxDatagramSize) {
                if (currentTableBuffer.getRowCount() == 0) {
                    throw singleRowTooLarge(estimate);
                }
                flushCommittedRowsOfCurrentTable();
                targetRows = currentTableBuffer.getRowCount() + 1;
                collectMissingColumns(targetRows);
                estimate = estimateCurrentDatagramSizeWithStagedRow(targetRows);
                if (estimate > maxDatagramSize) {
                    throw singleRowTooLarge(estimate);
                }
            }
        }

        materializeCurrentRow();
        if (trackDatagramEstimate) {
            committedEstimate = estimate;
        }
        clearStagedRow();
    }

    private void ensureNoInProgressRow(String operation) {
        if (hasInProgressRow()) {
            throw new LineSenderException(
                    "Cannot " + operation + " while row is in progress. "
                            + "Use sender.at(), sender.atNow(), or sender.cancelRow() first."
            );
        }
    }

    private int encodePayloadForUdp(QwpTableBuffer tableBuffer) {
        payloadBuffer.reset();
        columnWriter.setBuffer(payloadBuffer);
        columnWriter.encodeTable(tableBuffer, false, false, false);
        payloadBuffer.finish();
        return payloadBuffer.getPosition();
    }

    private long estimateArrayValueSize(int nDims, long elementCount) {
        return 1L + (long) nDims * 4 + elementCount * 8;
    }

    private long estimateArrayValueSize(DoubleArray array) {
        arraySizeCounter.reset();
        array.appendToBufPtr(arraySizeCounter);
        return arraySizeCounter.size;
    }

    private long estimateArrayValueSize(LongArray array) {
        arraySizeCounter.reset();
        array.appendToBufPtr(arraySizeCounter);
        return arraySizeCounter.size;
    }

    private long estimateBaseForCurrentSchema() {
        long estimate = HEADER_SIZE;
        int tableNameUtf8 = NativeBufferWriter.utf8Length(currentTableName);
        estimate += NativeBufferWriter.varintSize(tableNameUtf8) + tableNameUtf8;
        estimate += VARINT_INT_UPPER_BOUND;
        estimate += VARINT_INT_UPPER_BOUND;
        estimate += 1;

        QwpColumnDef[] defs = currentTableBuffer.getColumnDefs();
        for (QwpColumnDef def : defs) {
            int nameUtf8 = NativeBufferWriter.utf8Length(def.getName());
            estimate += NativeBufferWriter.varintSize(nameUtf8) + nameUtf8;
            estimate += 1;

            byte type = def.getTypeCode();
            if (type == TYPE_STRING || type == TYPE_VARCHAR) {
                estimate += 4;
            } else if (type == TYPE_SYMBOL) {
                estimate += 1;
            } else if (type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128 || type == TYPE_DECIMAL256) {
                estimate += 1;
            }
        }
        estimate += SAFETY_MARGIN_BYTES;
        return estimate;
    }

    private long estimateCurrentDatagramSizeWithStagedRow(int targetRows) {
        long estimate = currentTableBuffer.getRowCount() > 0 ? committedEstimate : estimateBaseForCurrentSchema();
        for (int i = 0; i < rowJournalSize; i++) {
            ColumnEntry entry = rowJournal.getQuick(i);
            QwpTableBuffer.ColumnBuffer col = entry.column;
            estimate += estimateEntryPayload(entry, col);
            if (col.isNullable()) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(col.getSize());
            }
        }
        for (int i = 0; i < missingColumnCount; i++) {
            QwpTableBuffer.ColumnBuffer col = missingColumns[i];
            int missing = targetRows - col.getSize();
            if (col.isNullable()) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(col.getSize());
            } else {
                estimate += nonNullablePaddingCost(col.getType(), col.getValueCount(), missing);
            }
        }
        return estimate;
    }

    private long estimateEntryPayload(ColumnEntry entry, QwpTableBuffer.ColumnBuffer col) {
        int valueCountBefore = col.getValueCount();
        return switch (entry.kind) {
            case ENTRY_AT_MICROS, ENTRY_AT_NANOS, ENTRY_DOUBLE, ENTRY_LONG,
                    ENTRY_TIMESTAMP_COL_MICROS, ENTRY_TIMESTAMP_COL_NANOS -> 8;
            case ENTRY_BOOL -> packedBytes(valueCountBefore + 1) - packedBytes(valueCountBefore);
            case ENTRY_DECIMAL64 -> 8;
            case ENTRY_DECIMAL128 -> 16;
            case ENTRY_DECIMAL256 -> 32;
            case ENTRY_DOUBLE_ARRAY -> estimateDoubleArrayPayload(entry.objectValue);
            case ENTRY_LONG_ARRAY -> estimateLongArrayPayload(entry.objectValue);
            case ENTRY_STRING -> entry.stringValue == null ? 0 : 4L + utf8Length(entry.stringValue);
            case ENTRY_SYMBOL -> estimateSymbolPayloadDelta(col, valueCountBefore, entry.stringValue);
            default -> throw new LineSenderException("unknown staged row entry type: " + entry.kind);
        };
    }

    private long estimateDoubleArrayPayload(Object value) {
        if (value instanceof double[] values) {
            return estimateArrayValueSize(1, values.length);
        }
        if (value instanceof double[][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            return estimateArrayValueSize(2, (long) dim0 * dim1);
        }
        if (value instanceof double[][][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            return estimateArrayValueSize(3, (long) dim0 * dim1 * dim2);
        }
        if (value instanceof DoubleArray values) {
            return estimateArrayValueSize(values);
        }
        throw new LineSenderException("unsupported double array type");
    }

    private long estimateLongArrayPayload(Object value) {
        if (value instanceof long[] values) {
            return estimateArrayValueSize(1, values.length);
        }
        if (value instanceof long[][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            return estimateArrayValueSize(2, (long) dim0 * dim1);
        }
        if (value instanceof long[][][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            return estimateArrayValueSize(3, (long) dim0 * dim1 * dim2);
        }
        if (value instanceof LongArray values) {
            return estimateArrayValueSize(values);
        }
        throw new LineSenderException("unsupported long array type");
    }

    private void ensureMissingColumnCapacity(int required) {
        if (required <= missingColumns.length) {
            return;
        }

        int newCapacity = missingColumns.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        QwpTableBuffer.ColumnBuffer[] newArr = new QwpTableBuffer.ColumnBuffer[newCapacity];
        System.arraycopy(missingColumns, 0, newArr, 0, missingColumnCount);
        missingColumns = newArr;
    }

    private void ensureTouchedColumnCapacity(int required) {
        if (required <= touchedColumns.length) {
            return;
        }

        int newCapacity = touchedColumns.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        QwpTableBuffer.ColumnBuffer[] newArr = new QwpTableBuffer.ColumnBuffer[newCapacity];
        System.arraycopy(touchedColumns, 0, newArr, 0, touchedColumnCount);
        touchedColumns = newArr;
    }

    private long estimateSymbolPayloadDelta(QwpTableBuffer.ColumnBuffer col, int valueCountBefore, CharSequence value) {
        if (value == null) {
            return 0;
        }

        int dictSizeBefore = col.getSymbolDictionarySize();
        if (col.hasSymbol(value)) {
            int maxIndex = Math.max(0, dictSizeBefore - 1);
            return NativeBufferWriter.varintSize(maxIndex);
        }

        int dictSizeAfter = dictSizeBefore + 1;
        long delta = 0;
        int utf8Len = utf8Length(value);
        delta += NativeBufferWriter.varintSize(utf8Len) + utf8Len;
        delta += NativeBufferWriter.varintSize(dictSizeAfter) - NativeBufferWriter.varintSize(dictSizeBefore);

        if (dictSizeBefore > 0 && valueCountBefore > 0) {
            int oldMax = dictSizeBefore - 1;
            int newMax = dictSizeAfter - 1;
            delta += (long) valueCountBefore * (
                    NativeBufferWriter.varintSize(newMax) - NativeBufferWriter.varintSize(oldMax)
            );
        }

        delta += NativeBufferWriter.varintSize(dictSizeAfter - 1);
        return delta;
    }

    private void flushCommittedRowsOfCurrentTable() {
        if (currentTableBuffer == null || currentTableBuffer.getRowCount() == 0) {
            return;
        }
        sendTableBuffer(currentTableName, currentTableBuffer);
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        resetCommittedEstimate();
    }

    private void flushInternal() {
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }
            sendTableBuffer(tableName, tableBuffer);
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
        resetCommittedEstimate();
    }

    private void flushSingleTable(String tableName, QwpTableBuffer tableBuffer) {
        sendTableBuffer(tableName, tableBuffer);
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
        resetCommittedEstimate();
    }

    private boolean hasInProgressRow() {
        return rowJournalSize > 0;
    }

    private boolean isTouchedColumn(QwpTableBuffer.ColumnBuffer col) {
        for (int i = 0; i < touchedColumnCount; i++) {
            if (touchedColumns[i] == col) {
                return true;
            }
        }
        return false;
    }

    private void addTouchedColumn(QwpTableBuffer.ColumnBuffer col) {
        if (isTouchedColumn(col)) {
            return;
        }
        ensureTouchedColumnCapacity(touchedColumnCount + 1);
        touchedColumns[touchedColumnCount++] = col;
    }

    private void materializeCurrentRow() {
        for (int i = 0; i < rowJournalSize; i++) {
            ColumnEntry entry = rowJournal.getQuick(i);
            QwpTableBuffer.ColumnBuffer col = entry.column;
            switch (entry.kind) {
                case ENTRY_AT_MICROS, ENTRY_AT_NANOS, ENTRY_LONG, ENTRY_TIMESTAMP_COL_MICROS, ENTRY_TIMESTAMP_COL_NANOS ->
                        col.addLong(entry.longValue);
                case ENTRY_BOOL -> col.addBoolean(entry.boolValue);
                case ENTRY_DECIMAL64 -> col.addDecimal64((Decimal64) entry.objectValue);
                case ENTRY_DECIMAL128 -> col.addDecimal128((Decimal128) entry.objectValue);
                case ENTRY_DECIMAL256 -> col.addDecimal256((Decimal256) entry.objectValue);
                case ENTRY_DOUBLE -> col.addDouble(entry.doubleValue);
                case ENTRY_DOUBLE_ARRAY -> appendDoubleArrayValue(col, entry.objectValue);
                case ENTRY_LONG_ARRAY -> appendLongArrayValue(col, entry.objectValue);
                case ENTRY_STRING -> col.addString(entry.stringValue);
                case ENTRY_SYMBOL -> col.addSymbol(entry.stringValue);
                default -> throw new LineSenderException("unknown staged row entry type: " + entry.kind);
            }
        }
        currentTableBuffer.nextRow(missingColumns, missingColumnCount);
    }

    private void appendDoubleArrayValue(QwpTableBuffer.ColumnBuffer col, Object value) {
        if (value instanceof double[] values) {
            col.addDoubleArray(values);
        } else if (value instanceof double[][] values) {
            col.addDoubleArray(values);
        } else if (value instanceof double[][][] values) {
            col.addDoubleArray(values);
        } else if (value instanceof DoubleArray values) {
            col.addDoubleArray(values);
        } else {
            throw new LineSenderException("unsupported double array type");
        }
    }

    private void appendLongArrayValue(QwpTableBuffer.ColumnBuffer col, Object value) {
        if (value instanceof long[] values) {
            col.addLongArray(values);
        } else if (value instanceof long[][] values) {
            col.addLongArray(values);
        } else if (value instanceof long[][][] values) {
            col.addLongArray(values);
        } else if (value instanceof LongArray values) {
            col.addLongArray(values);
        } else {
            throw new LineSenderException("unsupported long array type");
        }
    }

    private static long nonNullablePaddingCost(byte type, int valuesBefore, int missing) {
        return switch (type) {
            case TYPE_BOOLEAN -> packedBytes(valuesBefore + missing) - packedBytes(valuesBefore);
            case TYPE_BYTE -> missing;
            case TYPE_SHORT, TYPE_CHAR -> (long) missing * 2;
            case TYPE_INT, TYPE_FLOAT -> (long) missing * 4;
            case TYPE_LONG, TYPE_DOUBLE, TYPE_DATE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS -> (long) missing * 8;
            case TYPE_UUID -> (long) missing * 16;
            case TYPE_LONG256 -> (long) missing * 32;
            case TYPE_DECIMAL64 -> (long) missing * 8;
            case TYPE_DECIMAL128 -> (long) missing * 16;
            case TYPE_DECIMAL256 -> (long) missing * 32;
            case TYPE_STRING, TYPE_VARCHAR -> (long) missing * 4;
            case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY -> (long) missing * 5;
            case TYPE_SYMBOL -> throw new IllegalStateException("symbol columns must be nullable");
            default -> 0;
        };
    }

    private static int packedBytes(int valueCount) {
        return (valueCount + 7) / 8;
    }

    private ColumnEntry nextJournalEntry() {
        if (rowJournalSize < rowJournal.size()) {
            ColumnEntry entry = rowJournal.getQuick(rowJournalSize);
            entry.column = null;
            entry.objectValue = null;
            entry.stringValue = null;
            rowJournalSize++;
            return entry;
        }
        ColumnEntry entry = new ColumnEntry();
        rowJournal.add(entry);
        rowJournalSize++;
        return entry;
    }

    private void resetCommittedEstimate() {
        committedEstimate = 0;
    }

    private void sendTableBuffer(CharSequence tableName, QwpTableBuffer tableBuffer) {
        int payloadLength = encodePayloadForUdp(tableBuffer);
        headerBuffer.reset();
        headerBuffer.putByte((byte) 'Q');
        headerBuffer.putByte((byte) 'W');
        headerBuffer.putByte((byte) 'P');
        headerBuffer.putByte((byte) '1');
        headerBuffer.putByte(VERSION_1);
        headerBuffer.putByte((byte) 0);
        headerBuffer.putShort((short) 1);
        headerBuffer.putInt(payloadLength);

        sendSegments.reset();
        sendSegments.add(headerBuffer.getBufferPtr(), headerBuffer.getPosition());
        sendSegments.appendFrom(payloadBuffer.getSegments());

        try {
            channel.sendSegments(
                    sendSegments.getAddress(),
                    sendSegments.getSegmentCount(),
                    (int) sendSegments.getTotalLength()
            );
        } catch (LineSenderException e) {
            LOG.warn("UDP send failed [table={}, errno={}]: {}", tableName, channel.errno(), String.valueOf(e));
        }
        tableBuffer.reset();
    }

    private LineSenderException singleRowTooLarge(long estimate) {
        return new LineSenderException(
                "single row exceeds maximum datagram size (" + maxDatagramSize
                        + " bytes), estimated " + estimate + " bytes"
        );
    }

    private long toMicros(long value, ChronoUnit unit) {
        return switch (unit) {
            case NANOS -> value / 1000L;
            case MICROS -> value;
            case MILLIS -> value * 1000L;
            case SECONDS -> value * 1_000_000L;
            case MINUTES -> value * 60_000_000L;
            case HOURS -> value * 3_600_000_000L;
            case DAYS -> value * 86_400_000_000L;
            default -> throw new LineSenderException("Unsupported time unit: " + unit);
        };
    }

    private static int bitmapBytes(int size) {
        return (size + 7) / 8;
    }

    private static int utf8Length(CharSequence s) {
        if (s == null) {
            return 0;
        }
        int len = 0;
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c < 0x80) {
                len++;
            } else if (c < 0x800) {
                len += 2;
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                i++;
                len += 4;
            } else if (Character.isSurrogate(c)) {
                len++;
            } else {
                len += 3;
            }
        }
        return len;
    }

    private static final class ArraySizeCounter implements ArrayBufferAppender {
        private long size;

        @Override
        public void putBlockOfBytes(long from, long len) {
            size += len;
        }

        @Override
        public void putByte(byte b) {
            size++;
        }

        @Override
        public void putDouble(double value) {
            size += 8;
        }

        @Override
        public void putInt(int value) {
            size += 4;
        }

        @Override
        public void putLong(long value) {
            size += 8;
        }

        private void reset() {
            size = 0;
        }
    }

    private static class ColumnEntry {
        boolean boolValue;
        QwpTableBuffer.ColumnBuffer column;
        double doubleValue;
        byte kind;
        long longValue;
        Object objectValue;
        String stringValue;
    }
}
