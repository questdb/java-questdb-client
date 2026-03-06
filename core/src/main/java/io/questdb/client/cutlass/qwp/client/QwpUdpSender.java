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
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.line.udp.UdpLineChannel;
import io.questdb.client.cutlass.qwp.protocol.OffHeapAppendMemory;
import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.network.NetworkFacade;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
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

    private final UdpLineChannel channel;
    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private final NativeBufferWriter headerBuffer = new NativeBufferWriter();
    private final int maxDatagramSize;
    private final SegmentedNativeBufferWriter payloadWriter = new SegmentedNativeBufferWriter();
    private final NativeRowStaging stagedRow = new NativeRowStaging();
    private final boolean trackDatagramEstimate;
    private final NativeSegmentList datagramSegments = new NativeSegmentList();
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    private InProgressColumnState[] inProgressColumns = new InProgressColumnState[8];

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private long committedDatagramEstimate;
    private int inProgressRowValueCount;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;
    private QwpTableBuffer.ColumnBuffer[] rowFillColumns = new QwpTableBuffer.ColumnBuffer[8];
    private int rowFillColumnCount;
    private int inProgressColumnCount;

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
        try {
            commitCurrentRow();
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
    }

    @Override
    public Sender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        try {
            stageBooleanColumnValue(columnName, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        throw new LineSenderException("bufferView() is not supported for UDP sender");
    }

    @Override
    public void cancelRow() {
        checkNotClosed();
        rollbackCurrentRowToCommittedState();
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (hasInProgressRow()) {
                    rollbackCurrentRowToCommittedState();
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
            payloadWriter.close();
            datagramSegments.close();
            headerBuffer.close();
            stagedRow.close();
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDecimal64ColumnValue(name, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDecimal128ColumnValue(name, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDecimal256ColumnValue(name, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDoubleArrayColumnValue(name, values);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDoubleArrayColumnValue(name, values);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDoubleArrayColumnValue(name, values);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageDoubleArrayColumnValue(name, array);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        try {
            stageDoubleColumnValue(columnName, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
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
        try {
            stageLongArrayColumnValue(name, values);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageLongArrayColumnValue(name, values);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageLongArrayColumnValue(name, values);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        try {
            stageLongArrayColumnValue(name, array);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        try {
            stageLongColumnValue(columnName, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
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
        clearTransientRowState();
        resetCommittedDatagramEstimate();
    }

    @Override
    public Sender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        try {
            stageStringColumnValue(columnName, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        try {
            stageSymbolColumnValue(columnName, value);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
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
        } else {
            clearTransientRowState();
            resetCommittedDatagramEstimate();
        }

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
        try {
            if (unit == ChronoUnit.NANOS) {
                stageTimestampColumnValue(columnName, TYPE_TIMESTAMP_NANOS, value);
            } else {
                long micros = toMicros(value, unit);
                stageTimestampColumnValue(columnName, TYPE_TIMESTAMP, micros);
            }
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        try {
            long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
            stageTimestampColumnValue(columnName, TYPE_TIMESTAMP, micros);
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
        return this;
    }

    private QwpTableBuffer.ColumnBuffer acquireColumn(CharSequence name, byte type, boolean nullable) {
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getExistingColumn(name, type);
        if (col == null && currentTableBuffer.getRowCount() > 0) {
            // schema change while having some rows accumulated -> we flush committed rows of the current table
            // and start from scratch with the new schema

            if (hasInProgressRow()) {
                // the slowest case: we are adding a new column while the in-progress row already has some columns.
                // we need to store the in-progress row in a temporary buffer, flush the committed rows, and then replay the staged row into the new schema
                // assumption: this case is rare

                snapshotCurrentRowIntoReplayBuffer();
                rollbackCurrentRowToCommittedState(false);
                flushCommittedRowsOfCurrentTable();
                replaySnapshotAsInProgressRow();
                stagedRow.clear();
            } else {
                flushCommittedRowsOfCurrentTable();
            }
            col = currentTableBuffer.getExistingColumn(name, type);
        }

        if (col == null) {
            col = currentTableBuffer.getOrCreateColumn(name, type, nullable);
        }
        return col;
    }

    private void beginColumnWrite(QwpTableBuffer.ColumnBuffer column, CharSequence columnName) {
        InProgressColumnState existing = findInProgressColumnState(column);
        if (existing != null) {
            if (columnName != null && columnName.isEmpty()) {
                throw new LineSenderException("designated timestamp already set for current row");
            }
            throw new LineSenderException("column '" + columnName + "' already set for current row");
        }
        appendInProgressColumnState(column);
    }

    private void appendInProgressColumnState(QwpTableBuffer.ColumnBuffer column) {
        ensureInProgressColumnCapacity(inProgressColumnCount + 1);
        InProgressColumnState state = inProgressColumns[inProgressColumnCount];
        if (state == null) {
            state = new InProgressColumnState();
            inProgressColumns[inProgressColumnCount] = state;
        }
        state.of(column);
        inProgressColumnCount++;
    }

    private InProgressColumnState findInProgressColumnState(QwpTableBuffer.ColumnBuffer column) {
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            if (state.column == column) {
                return state;
            }
        }
        return null;
    }

    private void stageBooleanColumnValue(CharSequence name, boolean value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_BOOLEAN, false);
        beginColumnWrite(col, name);
        col.addBoolean(value);
        inProgressRowValueCount++;
    }

    private void stageDecimal128ColumnValue(CharSequence name, Decimal128 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL128, true);
        beginColumnWrite(col, name);
        col.addDecimal128(value);
        inProgressRowValueCount++;
    }

    private void stageDecimal256ColumnValue(CharSequence name, Decimal256 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL256, true);
        beginColumnWrite(col, name);
        col.addDecimal256(value);
        inProgressRowValueCount++;
    }

    private void stageDecimal64ColumnValue(CharSequence name, Decimal64 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL64, true);
        beginColumnWrite(col, name);
        col.addDecimal64(value);
        inProgressRowValueCount++;
    }

    private void stageDesignatedTimestampValue(long value, boolean nanos) {
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
        beginColumnWrite(col, "");
        col.addLong(value);
    }

    private void stageDoubleArrayColumnValue(CharSequence name, Object value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE_ARRAY, true);
        beginColumnWrite(col, name);
        appendDoubleArrayValue(col, value);
        inProgressRowValueCount++;
    }

    private void stageDoubleColumnValue(CharSequence name, double value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE, false);
        beginColumnWrite(col, name);
        col.addDouble(value);
        inProgressRowValueCount++;
    }

    private void stageLongArrayColumnValue(CharSequence name, Object value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG_ARRAY, true);
        beginColumnWrite(col, name);
        appendLongArrayValue(col, value);
        inProgressRowValueCount++;
    }

    private void stageLongColumnValue(CharSequence name, long value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG, false);
        beginColumnWrite(col, name);
        col.addLong(value);
        inProgressRowValueCount++;
    }

    private void stageStringColumnValue(CharSequence name, CharSequence value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_STRING, true);
        beginColumnWrite(col, name);
        col.addString(value);
        inProgressRowValueCount++;
    }

    private void stageSymbolColumnValue(CharSequence name, CharSequence value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_SYMBOL, true);
        beginColumnWrite(col, name);
        col.addSymbol(value);
        inProgressRowValueCount++;
    }

    private void stageTimestampColumnValue(CharSequence name, byte type, long value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, type, true);
        beginColumnWrite(col, name);
        col.addLong(value);
        inProgressRowValueCount++;
    }

    private void appendDoubleArrayValue(QwpTableBuffer.ColumnBuffer column, Object value) {
        if (value instanceof double[] values) {
            column.addDoubleArray(values);
            return;
        }
        if (value instanceof double[][] values) {
            column.addDoubleArray(values);
            return;
        }
        if (value instanceof double[][][] values) {
            column.addDoubleArray(values);
            return;
        }
        if (value instanceof DoubleArray values) {
            column.addDoubleArray(values);
            return;
        }
        throw new LineSenderException("unsupported double array type");
    }

    private void appendLongArrayValue(QwpTableBuffer.ColumnBuffer column, Object value) {
        if (value instanceof long[] values) {
            column.addLongArray(values);
            return;
        }
        if (value instanceof long[][] values) {
            column.addLongArray(values);
            return;
        }
        if (value instanceof long[][][] values) {
            column.addLongArray(values);
            return;
        }
        if (value instanceof LongArray values) {
            column.addLongArray(values);
            return;
        }
        throw new LineSenderException("unsupported long array type");
    }

    private void stageNullArrayColumnValue(CharSequence name, byte type) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, type, true);
        beginColumnWrite(col, name);
        col.addNull();
        inProgressRowValueCount++;
    }

    private void atMicros(long timestampMicros) {
        if (inProgressRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }
        try {
            stageDesignatedTimestampValue(timestampMicros, false);
            commitCurrentRow();
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
    }

    private void atNanos(long timestampNanos) {
        if (inProgressRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }
        try {
            stageDesignatedTimestampValue(timestampNanos, true);
            commitCurrentRow();
        } catch (RuntimeException | Error e) {
            rollbackCurrentRowToCommittedState();
            throw e;
        }
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

    private void clearInProgressRow() {
        inProgressRowValueCount = 0;
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            if (state != null) {
                state.clear();
            }
        }
        inProgressColumnCount = 0;
        rowFillColumnCount = 0;
    }

    private void rollbackCurrentRowToCommittedState() {
        rollbackCurrentRowToCommittedState(true);
    }

    private void rollbackCurrentRowToCommittedState(boolean clearReplayBuffer) {
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
        }
        clearCachedTimestampColumns();
        clearInProgressRow();
        if (clearReplayBuffer) {
            clearReplayBuffer();
        }
    }

    private void collectRowFillColumns(int targetRows) {
        rowFillColumnCount = 0;
        for (int i = 0, n = currentTableBuffer.getColumnCount(); i < n; i++) {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getColumn(i);
            if (isStagedColumn(col)) {
                continue;
            }
            if (col.getSize() >= targetRows) {
                continue;
            }
            ensureRowFillColumnCapacity(rowFillColumnCount + 1);
            rowFillColumns[rowFillColumnCount++] = col;
        }
    }

    private void commitCurrentRow() {
        if (inProgressRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }

        long estimate = 0;
        int targetRows = currentTableBuffer.getRowCount() + 1;
        collectRowFillColumns(targetRows);
        if (trackDatagramEstimate) {
            estimate = estimateCurrentDatagramSizeWithInProgressRow(targetRows);
            if (estimate > maxDatagramSize) {
                if (currentTableBuffer.getRowCount() == 0) {
                    throw singleRowTooLarge(estimate);
                }
                snapshotCurrentRowIntoReplayBuffer();
                rollbackCurrentRowToCommittedState(false);
                flushCommittedRowsOfCurrentTable();
                replaySnapshotAsInProgressRow();
                stagedRow.clear();
                targetRows = currentTableBuffer.getRowCount() + 1;
                collectRowFillColumns(targetRows);
                estimate = estimateCurrentDatagramSizeWithInProgressRow(targetRows);
                if (estimate > maxDatagramSize) {
                    throw singleRowTooLarge(estimate);
                }
            }
        }

        currentTableBuffer.nextRow(rowFillColumns, rowFillColumnCount);
        if (trackDatagramEstimate) {
            committedDatagramEstimate = estimate;
        }
        clearInProgressRow();
    }

    private void ensureNoInProgressRow(String operation) {
        if (hasInProgressRow()) {
            throw new LineSenderException(
                    "Cannot " + operation + " while row is in progress. "
                            + "Use sender.at(), sender.atNow(), or sender.cancelRow() first."
            );
        }
    }

    private int encodeTablePayloadForUdp(QwpTableBuffer tableBuffer) {
        payloadWriter.reset();
        columnWriter.setBuffer(payloadWriter);
        columnWriter.encodeTable(tableBuffer, false, false, false);
        payloadWriter.finish();
        return payloadWriter.getPosition();
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

    private long estimateCurrentDatagramSizeWithInProgressRow(int targetRows) {
        long estimate = currentTableBuffer.getRowCount() > 0 ? committedDatagramEstimate : estimateBaseForCurrentSchema();
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            QwpTableBuffer.ColumnBuffer col = state.column;
            estimate += estimateInProgressColumnPayload(state);
            if (col.isNullable()) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(state.sizeBefore);
            }
        }
        for (int i = 0; i < rowFillColumnCount; i++) {
            QwpTableBuffer.ColumnBuffer col = rowFillColumns[i];
            int missing = targetRows - col.getSize();
            if (col.isNullable()) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(col.getSize());
            } else {
                estimate += nonNullablePaddingCost(col.getType(), col.getValueCount(), missing);
            }
        }
        return estimate;
    }

    private long estimateInProgressColumnPayload(InProgressColumnState state) {
        QwpTableBuffer.ColumnBuffer col = state.column;
        int valueCountBefore = state.valueCountBefore;
        int valueCountAfter = col.getValueCount();
        if (valueCountAfter == valueCountBefore) {
            return 0;
        }

        return switch (col.getType()) {
            case TYPE_BOOLEAN -> packedBytes(valueCountAfter) - packedBytes(valueCountBefore);
            case TYPE_DECIMAL64 -> 8;
            case TYPE_DECIMAL128 -> 16;
            case TYPE_DECIMAL256 -> 32;
            case TYPE_DOUBLE, TYPE_LONG, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS -> 8;
            case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY -> estimateArrayPayloadBytes(col, state);
            case TYPE_STRING, TYPE_VARCHAR -> 4L + (col.getStringDataSize() - state.stringDataSizeBefore);
            case TYPE_SYMBOL -> estimateSymbolPayloadDelta(col, state);
            default -> throw new LineSenderException("unsupported in-progress column type: " + col.getType());
        };
    }

    private void ensureRowFillColumnCapacity(int required) {
        if (required <= rowFillColumns.length) {
            return;
        }

        int newCapacity = rowFillColumns.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        QwpTableBuffer.ColumnBuffer[] newArr = new QwpTableBuffer.ColumnBuffer[newCapacity];
        System.arraycopy(rowFillColumns, 0, newArr, 0, rowFillColumnCount);
        rowFillColumns = newArr;
    }

    private void ensureInProgressColumnCapacity(int required) {
        if (required <= inProgressColumns.length) {
            return;
        }

        int newCapacity = inProgressColumns.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        InProgressColumnState[] newArr = new InProgressColumnState[newCapacity];
        System.arraycopy(inProgressColumns, 0, newArr, 0, inProgressColumnCount);
        inProgressColumns = newArr;
    }

    private long estimateArrayPayloadBytes(QwpTableBuffer.ColumnBuffer col, InProgressColumnState state) {
        int shapeCount = col.getArrayShapeOffset() - state.arrayShapeOffsetBefore;
        int dataCount = col.getArrayDataOffset() - state.arrayDataOffsetBefore;
        int elementSize = col.getType() == TYPE_LONG_ARRAY ? Long.BYTES : Double.BYTES;
        return 1L + (long) shapeCount * Integer.BYTES + (long) dataCount * elementSize;
    }

    private long estimateSymbolPayloadDelta(QwpTableBuffer.ColumnBuffer col, InProgressColumnState state) {
        int valueCountBefore = state.valueCountBefore;
        int valueCountAfter = col.getValueCount();
        if (valueCountAfter == valueCountBefore) {
            return 0;
        }

        int dictSizeBefore = state.symbolDictionarySizeBefore;
        long dataAddress = col.getDataAddress();
        int idx = Unsafe.getUnsafe().getInt(dataAddress + (long) valueCountBefore * Integer.BYTES);
        int dictSizeAfter = col.getSymbolDictionarySize();

        if (dictSizeAfter == dictSizeBefore) {
            return NativeBufferWriter.varintSize(idx);
        }

        long delta = 0;
        CharSequence value = col.getSymbolValue(idx);
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
        clearCachedTimestampColumns();
        resetCommittedDatagramEstimate();
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
        clearTransientRowState();
        resetCommittedDatagramEstimate();
    }

    private void flushSingleTable(String tableName, QwpTableBuffer tableBuffer) {
        sendTableBuffer(tableName, tableBuffer);
        clearTransientRowState();
        resetCommittedDatagramEstimate();
    }

    private boolean hasInProgressRow() {
        return inProgressColumnCount > 0;
    }

    private boolean isStagedColumn(QwpTableBuffer.ColumnBuffer col) {
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            if (state != null && state.column == col) {
                return true;
            }
        }
        return false;
    }

    private void snapshotCurrentRowIntoReplayBuffer() {
        stagedRow.clear();
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            int replayKind;
            if (state.column == cachedTimestampColumn) {
                replayKind = ENTRY_AT_MICROS;
            } else if (state.column == cachedTimestampNanosColumn) {
                replayKind = ENTRY_AT_NANOS;
            } else {
                replayKind = 0;
            }
            stagedRow.snapshotInProgressColumn(state, replayKind);
        }
    }

    private void replaySnapshotAsInProgressRow() {
        clearInProgressRow();
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        for (int i = 0, n = stagedRow.size(); i < n; i++) {
            QwpTableBuffer.ColumnBuffer column = currentTableBuffer.getOrCreateColumn(
                    stagedRow.getColumnName(i),
                    stagedRow.getColumnType(i),
                    stagedRow.isColumnNullable(i)
            );
            appendInProgressColumnState(column);
            stagedRow.appendEntryIntoColumn(i, column);

            int kind = stagedRow.getKind(i);
            if (kind == ENTRY_AT_MICROS) {
                cachedTimestampColumn = column;
            } else if (kind == ENTRY_AT_NANOS) {
                cachedTimestampNanosColumn = column;
            } else {
                inProgressRowValueCount++;
            }
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

    private void resetCommittedDatagramEstimate() {
        committedDatagramEstimate = 0;
    }

    private void clearCachedTimestampColumns() {
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
    }

    private void clearReplayBuffer() {
        stagedRow.clear();
    }

    private void clearTransientRowState() {
        clearCachedTimestampColumns();
        clearInProgressRow();
        clearReplayBuffer();
    }

    // Public test hooks because module boundaries prevent tests from sharing this package.
    @TestOnly
    public void stageNullDoubleArrayForTest(CharSequence name) {
        stageNullArrayColumnValue(name, TYPE_DOUBLE_ARRAY);
    }

    @TestOnly
    public void stageNullLongArrayForTest(CharSequence name) {
        stageNullArrayColumnValue(name, TYPE_LONG_ARRAY);
    }

    @TestOnly
    public QwpTableBuffer currentTableBufferForTest() {
        return currentTableBuffer;
    }

    private void sendTableBuffer(CharSequence tableName, QwpTableBuffer tableBuffer) {
        int payloadLength = encodeTablePayloadForUdp(tableBuffer);
        headerBuffer.reset();
        headerBuffer.putByte((byte) 'Q');
        headerBuffer.putByte((byte) 'W');
        headerBuffer.putByte((byte) 'P');
        headerBuffer.putByte((byte) '1');
        headerBuffer.putByte(VERSION_1);
        headerBuffer.putByte((byte) 0);
        headerBuffer.putShort((short) 1);
        headerBuffer.putInt(payloadLength);

        datagramSegments.reset();
        datagramSegments.add(headerBuffer.getBufferPtr(), headerBuffer.getPosition());
        datagramSegments.appendFrom(payloadWriter.getSegments());

        try {
            channel.sendSegments(
                    datagramSegments.getAddress(),
                    datagramSegments.getSegmentCount(),
                    (int) datagramSegments.getTotalLength()
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

    private static final class InProgressColumnState {
        private int arrayDataOffsetBefore;
        private int arrayShapeOffsetBefore;
        private QwpTableBuffer.ColumnBuffer column;
        private int sizeBefore;
        private long stringDataSizeBefore;
        private int symbolDictionarySizeBefore;
        private int valueCountBefore;

        void clear() {
            column = null;
        }

        void of(QwpTableBuffer.ColumnBuffer column) {
            this.column = column;
            this.sizeBefore = column.getSize();
            this.valueCountBefore = column.getValueCount();
            this.stringDataSizeBefore = column.getStringDataSize();
            this.arrayShapeOffsetBefore = column.getArrayShapeOffset();
            this.arrayDataOffsetBefore = column.getArrayDataOffset();
            this.symbolDictionarySizeBefore = column.getSymbolDictionarySize();
        }
    }

    private static final class NativeRowStaging {
        private static final int AUX_INT_OFFSET = 4;
        private static final int ENTRY_SIZE = 40;
        private static final int LONG0_OFFSET = 8;
        private static final int LONG1_OFFSET = 16;
        private static final int LONG2_OFFSET = 24;
        private static final int LONG3_OFFSET = 32;

        private final Decimal128 decimal128Sink = new Decimal128();
        private final Decimal256 decimal256Sink = new Decimal256();
        private final Decimal64 decimal64Sink = new Decimal64();
        private final OffHeapAppendMemory entries = new OffHeapAppendMemory(ENTRY_SIZE * 8L);
        private final OffHeapAppendMemory varData = new OffHeapAppendMemory(128);
        private String[] columnNames = new String[8];
        private byte[] columnTypes = new byte[8];
        private boolean[] columnNullables = new boolean[8];
        private int size;

        void appendEntryIntoColumn(int index, QwpTableBuffer.ColumnBuffer column) {
            long entryAddress = entryAddress(index);
            int kind = Unsafe.getUnsafe().getInt(entryAddress);
            int auxInt = Unsafe.getUnsafe().getInt(entryAddress + AUX_INT_OFFSET);
            long long0 = Unsafe.getUnsafe().getLong(entryAddress + LONG0_OFFSET);
            long long1 = Unsafe.getUnsafe().getLong(entryAddress + LONG1_OFFSET);
            switch (kind) {
                case ENTRY_AT_MICROS, ENTRY_AT_NANOS, ENTRY_LONG, ENTRY_TIMESTAMP_COL_MICROS, ENTRY_TIMESTAMP_COL_NANOS ->
                        column.addLong(long0);
                case ENTRY_BOOL -> column.addBoolean(long0 != 0);
                case ENTRY_DECIMAL64 -> {
                    decimal64Sink.ofRaw(long0);
                    decimal64Sink.setScale(auxInt);
                    column.addDecimal64(decimal64Sink);
                }
                case ENTRY_DECIMAL128 -> {
                    decimal128Sink.ofRaw(long0, long1);
                    decimal128Sink.setScale(auxInt);
                    column.addDecimal128(decimal128Sink);
                }
                case ENTRY_DECIMAL256 -> {
                    decimal256Sink.ofRaw(
                            long0,
                            long1,
                            Unsafe.getUnsafe().getLong(entryAddress + LONG2_OFFSET),
                            Unsafe.getUnsafe().getLong(entryAddress + LONG3_OFFSET)
                    );
                    decimal256Sink.setScale(auxInt);
                    column.addDecimal256(decimal256Sink);
                }
                case ENTRY_DOUBLE -> column.addDouble(Double.longBitsToDouble(long0));
                case ENTRY_DOUBLE_ARRAY -> column.addDoubleArrayPayload(varData.addressOf(long1), long0);
                case ENTRY_LONG_ARRAY -> column.addLongArrayPayload(varData.addressOf(long1), long0);
                case ENTRY_STRING -> column.addStringUtf8(varData.addressOf(long0), auxInt);
                case ENTRY_SYMBOL -> {
                    if (auxInt < 0) {
                        column.addSymbol(null);
                    } else {
                        column.addSymbolUtf8(varData.addressOf(long0), auxInt);
                    }
                }
                default -> throw new LineSenderException("unknown staged row entry type: " + kind);
            }
        }

        void clear() {
            for (int i = 0; i < size; i++) {
                columnNames[i] = null;
            }
            size = 0;
            entries.truncate();
            varData.truncate();
        }

        void close() {
            entries.close();
            varData.close();
        }

        byte getColumnType(int index) {
            return columnTypes[index];
        }

        String getColumnName(int index) {
            return columnNames[index];
        }

        int getKind(int index) {
            return Unsafe.getUnsafe().getInt(entryAddress(index));
        }

        boolean isColumnNullable(int index) {
            return columnNullables[index];
        }

        void snapshotInProgressColumn(InProgressColumnState state, int replayKind) {
            QwpTableBuffer.ColumnBuffer column = state.column;
            int kind = replayKind != 0 ? replayKind : defaultKind(column.getType());
            int valueIndex = state.valueCountBefore;
            long dataAddress = column.getDataAddress();

            switch (kind) {
                case ENTRY_AT_MICROS, ENTRY_AT_NANOS, ENTRY_LONG, ENTRY_TIMESTAMP_COL_MICROS, ENTRY_TIMESTAMP_COL_NANOS -> {
                    long value = Unsafe.getUnsafe().getLong(dataAddress + (long) valueIndex * Long.BYTES);
                    stageEntry(column, kind, 0, value, 0, 0, 0);
                }
                case ENTRY_BOOL -> {
                    boolean value = Unsafe.getUnsafe().getByte(dataAddress + valueIndex) != 0;
                    stageEntry(column, ENTRY_BOOL, 0, value ? 1 : 0, 0, 0, 0);
                }
                case ENTRY_DECIMAL64 -> {
                    long value = Unsafe.getUnsafe().getLong(dataAddress + (long) valueIndex * Long.BYTES);
                    stageEntry(column, ENTRY_DECIMAL64, column.getDecimalScale(), value, 0, 0, 0);
                }
                case ENTRY_DECIMAL128 -> {
                    long offset = (long) valueIndex * 16;
                    stageEntry(
                            column,
                            ENTRY_DECIMAL128,
                            column.getDecimalScale(),
                            Unsafe.getUnsafe().getLong(dataAddress + offset),
                            Unsafe.getUnsafe().getLong(dataAddress + offset + Long.BYTES),
                            0,
                            0
                    );
                }
                case ENTRY_DECIMAL256 -> {
                    long offset = (long) valueIndex * 32;
                    stageEntry(
                            column,
                            ENTRY_DECIMAL256,
                            column.getDecimalScale(),
                            Unsafe.getUnsafe().getLong(dataAddress + offset),
                            Unsafe.getUnsafe().getLong(dataAddress + offset + 8),
                            Unsafe.getUnsafe().getLong(dataAddress + offset + 16),
                            Unsafe.getUnsafe().getLong(dataAddress + offset + 24)
                    );
                }
                case ENTRY_DOUBLE -> {
                    long value = Unsafe.getUnsafe().getLong(dataAddress + (long) valueIndex * Double.BYTES);
                    stageEntry(column, ENTRY_DOUBLE, 0, value, 0, 0, 0);
                }
                case ENTRY_DOUBLE_ARRAY -> snapshotDoubleArray(column, state);
                case ENTRY_LONG_ARRAY -> snapshotLongArray(column, state);
                case ENTRY_STRING -> snapshotString(column, state);
                case ENTRY_SYMBOL -> snapshotSymbol(column, state);
                default -> throw new LineSenderException("unsupported replay row entry type: " + kind);
            }
        }

        int size() {
            return size;
        }

        private int defaultKind(byte columnType) {
            return switch (columnType) {
                case TYPE_BOOLEAN -> ENTRY_BOOL;
                case TYPE_DECIMAL128 -> ENTRY_DECIMAL128;
                case TYPE_DECIMAL256 -> ENTRY_DECIMAL256;
                case TYPE_DECIMAL64 -> ENTRY_DECIMAL64;
                case TYPE_DOUBLE -> ENTRY_DOUBLE;
                case TYPE_DOUBLE_ARRAY -> ENTRY_DOUBLE_ARRAY;
                case TYPE_LONG -> ENTRY_LONG;
                case TYPE_LONG_ARRAY -> ENTRY_LONG_ARRAY;
                case TYPE_STRING, TYPE_VARCHAR -> ENTRY_STRING;
                case TYPE_SYMBOL -> ENTRY_SYMBOL;
                case TYPE_TIMESTAMP -> ENTRY_TIMESTAMP_COL_MICROS;
                case TYPE_TIMESTAMP_NANOS -> ENTRY_TIMESTAMP_COL_NANOS;
                default -> throw new LineSenderException("unsupported replay row column type: " + columnType);
            };
        }

        private long entryAddress(int index) {
            return entries.addressOf((long) index * ENTRY_SIZE);
        }

        private void ensureCapacity(int required) {
            if (required <= columnNames.length) {
                return;
            }

            int newCapacity = columnNames.length;
            while (newCapacity < required) {
                newCapacity *= 2;
            }

            String[] newColumnNames = new String[newCapacity];
            System.arraycopy(columnNames, 0, newColumnNames, 0, size);
            columnNames = newColumnNames;

            byte[] newColumnTypes = new byte[newCapacity];
            System.arraycopy(columnTypes, 0, newColumnTypes, 0, size);
            columnTypes = newColumnTypes;

            boolean[] newColumnNullables = new boolean[newCapacity];
            System.arraycopy(columnNullables, 0, newColumnNullables, 0, size);
            columnNullables = newColumnNullables;
        }

        private void snapshotDoubleArray(QwpTableBuffer.ColumnBuffer column, InProgressColumnState state) {
            if (column.getValueCount() == state.valueCountBefore) {
                stageEntry(column, ENTRY_DOUBLE_ARRAY, 0, -1, 0, 0, 0);
                return;
            }

            long offset = varData.getAppendOffset();
            try {
                int shapeStart = state.arrayShapeOffsetBefore;
                int shapeEnd = column.getArrayShapeOffset();
                int dataStart = state.arrayDataOffsetBefore;
                int dataEnd = column.getArrayDataOffset();
                int[] shapes = column.getArrayShapes();
                double[] data = column.getDoubleArrayData();

                varData.putByte((byte) (shapeEnd - shapeStart));
                for (int i = shapeStart; i < shapeEnd; i++) {
                    varData.putInt(shapes[i]);
                }
                for (int i = dataStart; i < dataEnd; i++) {
                    varData.putDouble(data[i]);
                }

                long payloadLength = varData.getAppendOffset() - offset;
                stageEntry(column, ENTRY_DOUBLE_ARRAY, 0, payloadLength, offset, 0, 0);
            } catch (Throwable t) {
                varData.jumpTo(offset);
                throw t;
            }
        }

        private void snapshotLongArray(QwpTableBuffer.ColumnBuffer column, InProgressColumnState state) {
            if (column.getValueCount() == state.valueCountBefore) {
                stageEntry(column, ENTRY_LONG_ARRAY, 0, -1, 0, 0, 0);
                return;
            }

            long offset = varData.getAppendOffset();
            try {
                int shapeStart = state.arrayShapeOffsetBefore;
                int shapeEnd = column.getArrayShapeOffset();
                int dataStart = state.arrayDataOffsetBefore;
                int dataEnd = column.getArrayDataOffset();
                int[] shapes = column.getArrayShapes();
                long[] data = column.getLongArrayData();

                varData.putByte((byte) (shapeEnd - shapeStart));
                for (int i = shapeStart; i < shapeEnd; i++) {
                    varData.putInt(shapes[i]);
                }
                for (int i = dataStart; i < dataEnd; i++) {
                    varData.putLong(data[i]);
                }

                long payloadLength = varData.getAppendOffset() - offset;
                stageEntry(column, ENTRY_LONG_ARRAY, 0, payloadLength, offset, 0, 0);
            } catch (Throwable t) {
                varData.jumpTo(offset);
                throw t;
            }
        }

        private void snapshotString(QwpTableBuffer.ColumnBuffer column, InProgressColumnState state) {
            if (column.getValueCount() == state.valueCountBefore) {
                stageEntry(column, ENTRY_STRING, -1, 0, 0, 0, 0);
                return;
            }

            long offsetsAddress = column.getStringOffsetsAddress();
            int start = Unsafe.getUnsafe().getInt(offsetsAddress + (long) state.valueCountBefore * Integer.BYTES);
            int end = Unsafe.getUnsafe().getInt(offsetsAddress + (long) (state.valueCountBefore + 1) * Integer.BYTES);
            int len = end - start;

            long offset = varData.getAppendOffset();
            try {
                if (len > 0) {
                    varData.putBlockOfBytes(column.getStringDataAddress() + start, len);
                }
                stageEntry(column, ENTRY_STRING, len, offset, 0, 0, 0);
            } catch (Throwable t) {
                varData.jumpTo(offset);
                throw t;
            }
        }

        private void snapshotSymbol(QwpTableBuffer.ColumnBuffer column, InProgressColumnState state) {
            if (column.getValueCount() == state.valueCountBefore) {
                stageEntry(column, ENTRY_SYMBOL, -1, 0, 0, 0, 0);
                return;
            }

            int valueIndex = state.valueCountBefore;
            int localIndex = Unsafe.getUnsafe().getInt(column.getDataAddress() + (long) valueIndex * Integer.BYTES);
            CharSequence value = column.getSymbolValue(localIndex);
            stageUtf8(column, ENTRY_SYMBOL, value);
        }

        private void stageEntry(
                QwpTableBuffer.ColumnBuffer column,
                int kind,
                int auxInt,
                long long0,
                long long1,
                long long2,
                long long3
        ) {
            ensureCapacity(size + 1);
            columnNames[size] = column.getName();
            columnTypes[size] = column.getType();
            columnNullables[size] = column.isNullable();
            entries.putInt(kind);
            entries.putInt(auxInt);
            entries.putLong(long0);
            entries.putLong(long1);
            entries.putLong(long2);
            entries.putLong(long3);
            size++;
        }

        private void stageUtf8(QwpTableBuffer.ColumnBuffer column, int kind, CharSequence value) {
            int len = -1;
            long offset = 0;
            if (value != null) {
                offset = varData.getAppendOffset();
                varData.putUtf8(value);
                len = (int) (varData.getAppendOffset() - offset);
            }
            stageEntry(column, kind, len, offset, 0, 0, 0);
        }
    }
}
