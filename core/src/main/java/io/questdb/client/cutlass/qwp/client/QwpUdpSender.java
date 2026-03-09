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
import java.util.Arrays;

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
    private static final int ADAPTIVE_HEADROOM_EWMA_SHIFT = 2;
    private static final int VARINT_INT_UPPER_BOUND = 5;
    private static final Logger LOG = LoggerFactory.getLogger(QwpUdpSender.class);

    private final UdpLineChannel channel;
    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private final NativeBufferWriter headerBuffer = new NativeBufferWriter();
    private final int maxDatagramSize;
    private final SegmentedNativeBufferWriter payloadWriter = new SegmentedNativeBufferWriter();
    private final boolean trackDatagramEstimate;
    private final NativeSegmentList datagramSegments = new NativeSegmentList();
    private final CharSequenceObjHashMap<TableHeadroomState> tableHeadroomStates;
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    private InProgressColumnState[] inProgressColumns = new InProgressColumnState[8];

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private long committedDatagramEstimate;
    private int inProgressRowValueCount;
    private TableHeadroomState currentTableHeadroomState;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;

    // prefix* arrays: per-column snapshots captured before the in-progress row,
    // used to encode and flush only the committed prefix when a row is still being built.
    // Indexed by column index. -1 means the column has no in-progress data.
    private int[] prefixArrayDataOffsetBefore = new int[8];
    private int[] prefixArrayShapeOffsetBefore = new int[8];
    private long[] prefixStringDataSizeBefore = new long[8];
    private int[] prefixSymbolDictionarySizeBefore = new int[8];
    private int[] prefixSizeBefore = new int[8];
    private int[] prefixValueCountBefore = new int[8];

    // per-column marks to detect duplicate writes within a single row; compared against currentRowMark
    private int[] stagedColumnMarks = new int[8];
    // monotonically increasing mark; incremented per row to invalidate stagedColumnMarks without clearing
    private int currentRowMark = 1;
    private int inProgressColumnCount;

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl) {
        this(nf, interfaceIPv4, sendToAddress, port, ttl, 0);
    }

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl, int maxDatagramSize) {
        this.channel = new UdpLineChannel(nf, interfaceIPv4, sendToAddress, port, ttl);
        this.tableHeadroomStates = new CharSequenceObjHashMap<>();
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
            tableHeadroomStates.clear();
            currentTableHeadroomState = null;
            channel.close();
            payloadWriter.close();
            datagramSegments.close();
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
        currentTableHeadroomState = null;
        currentTableName = null;
        clearTransientRowState();
        resetCommittedDatagramEstimate();
        tableHeadroomStates.clear();
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
        currentTableHeadroomState = tableHeadroomStates.get(currentTableName);
        if (currentTableHeadroomState == null) {
            currentTableHeadroomState = new TableHeadroomState();
            tableHeadroomStates.put(currentTableName, currentTableHeadroomState);
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
                flushCommittedPrefixPreservingCurrentRow();
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
        int columnIndex = column.getIndex();
        ensureStagedColumnMarkCapacity(columnIndex + 1);
        if (stagedColumnMarks[columnIndex] == currentRowMark) {
            if (columnName != null && columnName.isEmpty()) {
                throw new LineSenderException("designated timestamp already set for current row");
            }
            throw new LineSenderException("column '" + columnName + "' already set for current row");
        }
        stagedColumnMarks[columnIndex] = currentRowMark;
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

    private void completeColumnWrite() {
        inProgressColumns[inProgressColumnCount - 1].captureAfterWrite();
    }

    private void stageBooleanColumnValue(CharSequence name, boolean value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_BOOLEAN, false);
        beginColumnWrite(col, name);
        col.addBoolean(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageDecimal128ColumnValue(CharSequence name, Decimal128 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL128, true);
        beginColumnWrite(col, name);
        col.addDecimal128(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageDecimal256ColumnValue(CharSequence name, Decimal256 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL256, true);
        beginColumnWrite(col, name);
        col.addDecimal256(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageDecimal64ColumnValue(CharSequence name, Decimal64 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL64, true);
        beginColumnWrite(col, name);
        col.addDecimal64(value);
        completeColumnWrite();
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
        completeColumnWrite();
    }

    private void stageDoubleArrayColumnValue(CharSequence name, Object value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE_ARRAY, true);
        beginColumnWrite(col, name);
        appendDoubleArrayValue(col, value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageDoubleColumnValue(CharSequence name, double value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE, false);
        beginColumnWrite(col, name);
        col.addDouble(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageLongArrayColumnValue(CharSequence name, Object value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG_ARRAY, true);
        beginColumnWrite(col, name);
        appendLongArrayValue(col, value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageLongColumnValue(CharSequence name, long value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG, false);
        beginColumnWrite(col, name);
        col.addLong(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageStringColumnValue(CharSequence name, CharSequence value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_STRING, true);
        beginColumnWrite(col, name);
        col.addString(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageSymbolColumnValue(CharSequence name, CharSequence value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_SYMBOL, true);
        beginColumnWrite(col, name);
        col.addSymbol(value);
        completeColumnWrite();
        inProgressRowValueCount++;
    }

    private void stageTimestampColumnValue(CharSequence name, byte type, long value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, type, true);
        beginColumnWrite(col, name);
        col.addLong(value);
        completeColumnWrite();
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
        completeColumnWrite();
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
        if (inProgressColumnCount > 0) {
            currentRowMark++;
            if (currentRowMark == 0) {
                Arrays.fill(stagedColumnMarks, 0);
                currentRowMark = 1;
            }
        }
        inProgressColumnCount = 0;
    }

    private void rollbackCurrentRowToCommittedState() {
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
        }
        clearCachedTimestampColumns();
        clearInProgressRow();
    }

    private void commitCurrentRow() {
        if (inProgressRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }

        long estimate = 0;
        long committedEstimateBeforeRow = 0;
        int targetRows = currentTableBuffer.getRowCount() + 1;
        if (trackDatagramEstimate) {
            committedEstimateBeforeRow = currentTableBuffer.getRowCount() > 0
                    ? committedDatagramEstimate
                    : estimateBaseForCurrentSchema();
            estimate = estimateCurrentDatagramSizeWithInProgressRow(targetRows);
            if (estimate > maxDatagramSize) {
                if (currentTableBuffer.getRowCount() == 0) {
                    throw singleRowTooLarge(estimate);
                }
                flushCommittedPrefixPreservingCurrentRow();
                targetRows = currentTableBuffer.getRowCount() + 1;
                committedEstimateBeforeRow = estimateBaseForCurrentSchema();
                estimate = estimateCurrentDatagramSizeWithInProgressRow(targetRows);
                if (estimate > maxDatagramSize) {
                    throw singleRowTooLarge(estimate);
                }
            }
        }

        currentTableBuffer.nextRow();
        if (trackDatagramEstimate) {
            committedDatagramEstimate = estimate;
        }
        clearInProgressRow();
        if (trackDatagramEstimate) {
            long rowDatagramGrowth = estimate - committedEstimateBeforeRow;
            recordCommittedRowAndMaybeFlush(rowDatagramGrowth);
        }
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

    private int encodeCommittedPrefixPayloadForUdp(QwpTableBuffer tableBuffer) {
        payloadWriter.reset();
        columnWriter.setBuffer(payloadWriter);
        columnWriter.encodeTable(
                tableBuffer,
                tableBuffer.getRowCount(),
                prefixValueCountBefore,
                prefixStringDataSizeBefore,
                prefixSymbolDictionarySizeBefore,
                false,
                false,
                false
        );
        payloadWriter.finish();
        return payloadWriter.getPosition();
    }

    private long estimateBaseForCurrentSchema() {
        if (currentTableHeadroomState != null) {
            long cachedEstimate = currentTableHeadroomState.getCachedBaseEstimate(currentTableBuffer.getColumnCount());
            if (cachedEstimate > -1) {
                return cachedEstimate;
            }
        }

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
        if (currentTableHeadroomState != null) {
            currentTableHeadroomState.cacheBaseEstimate(currentTableBuffer.getColumnCount(), estimate);
        }
        return estimate;
    }

    private long estimateCurrentDatagramSizeWithInProgressRow(int targetRows) {
        long estimate = currentTableBuffer.getRowCount() > 0 ? committedDatagramEstimate : estimateBaseForCurrentSchema();
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            estimate += state.payloadEstimateDelta;
            if (state.nullable) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(state.sizeBefore);
            }
        }
        ensureStagedColumnMarkCapacity(currentTableBuffer.getColumnCount());
        for (int i = 0, columnCount = currentTableBuffer.getColumnCount(); i < columnCount; i++) {
            if (stagedColumnMarks[i] == currentRowMark) {
                continue;
            }
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getColumn(i);
            int missing = targetRows - col.getSize();
            if (col.isNullable()) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(col.getSize());
            } else {
                estimate += nonNullablePaddingCost(col.getType(), col.getValueCount(), missing);
            }
        }
        return estimate;
    }

    private static long estimateInProgressColumnPayload(InProgressColumnState state) {
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

    private void ensureStagedColumnMarkCapacity(int required) {
        if (required <= stagedColumnMarks.length) {
            return;
        }

        int newCapacity = stagedColumnMarks.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        int[] newArr = new int[newCapacity];
        System.arraycopy(stagedColumnMarks, 0, newArr, 0, stagedColumnMarks.length);
        stagedColumnMarks = newArr;
    }

    private static long estimateArrayPayloadBytes(QwpTableBuffer.ColumnBuffer col, InProgressColumnState state) {
        int shapeCount = col.getArrayShapeOffset() - state.arrayShapeOffsetBefore;
        int dataCount = col.getArrayDataOffset() - state.arrayDataOffsetBefore;
        int elementSize = col.getType() == TYPE_LONG_ARRAY ? Long.BYTES : Double.BYTES;
        return 1L + (long) shapeCount * Integer.BYTES + (long) dataCount * elementSize;
    }

    private static long estimateSymbolPayloadDelta(QwpTableBuffer.ColumnBuffer col, InProgressColumnState state) {
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
        sendWholeTableBuffer(currentTableName, currentTableBuffer);
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
            sendWholeTableBuffer(tableName, tableBuffer);
        }
        clearTransientRowState();
        resetCommittedDatagramEstimate();
    }

    private void flushSingleTable(String tableName, QwpTableBuffer tableBuffer) {
        sendWholeTableBuffer(tableName, tableBuffer);
        clearTransientRowState();
        resetCommittedDatagramEstimate();
    }

    private boolean hasInProgressRow() {
        return inProgressColumnCount > 0;
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

    private void recordCommittedRowAndMaybeFlush(long rowDatagramGrowth) {
        if (currentTableBuffer == null || currentTableBuffer.getRowCount() == 0 || currentTableHeadroomState == null) {
            return;
        }

        currentTableHeadroomState.recordCommittedRow(currentTableBuffer.getColumnCount(), rowDatagramGrowth);
        if (shouldFlushCommittedRowsAfterCommit()) {
            flushCommittedRowsOfCurrentTable();
        }
    }

    private void resetCommittedDatagramEstimate() {
        committedDatagramEstimate = 0;
    }

    private void clearCachedTimestampColumns() {
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
    }

    private void clearTransientRowState() {
        clearCachedTimestampColumns();
        clearInProgressRow();
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

    @TestOnly
    public long committedDatagramEstimateForTest() {
        return committedDatagramEstimate;
    }

    private void captureInProgressColumnPrefixState() {
        int columnCount = currentTableBuffer.getColumnCount();
        ensurePrefixColumnCapacity(columnCount);
        for (int i = 0; i < columnCount; i++) {
            prefixSizeBefore[i] = -1;
            prefixValueCountBefore[i] = -1;
        }
        for (int i = 0; i < inProgressColumnCount; i++) {
            InProgressColumnState state = inProgressColumns[i];
            int columnIndex = state.column.getIndex();
            prefixSizeBefore[columnIndex] = state.sizeBefore;
            prefixValueCountBefore[columnIndex] = state.valueCountBefore;
            prefixStringDataSizeBefore[columnIndex] = state.stringDataSizeBefore;
            prefixArrayShapeOffsetBefore[columnIndex] = state.arrayShapeOffsetBefore;
            prefixArrayDataOffsetBefore[columnIndex] = state.arrayDataOffsetBefore;
            prefixSymbolDictionarySizeBefore[columnIndex] = state.symbolDictionarySizeBefore;
        }
    }

    private void ensurePrefixColumnCapacity(int required) {
        if (required <= prefixSizeBefore.length) {
            return;
        }

        int newCapacity = prefixSizeBefore.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        prefixSizeBefore = new int[newCapacity];
        prefixValueCountBefore = new int[newCapacity];
        prefixStringDataSizeBefore = new long[newCapacity];
        prefixArrayShapeOffsetBefore = new int[newCapacity];
        prefixArrayDataOffsetBefore = new int[newCapacity];
        prefixSymbolDictionarySizeBefore = new int[newCapacity];
    }

    private void flushCommittedPrefixPreservingCurrentRow() {
        if (currentTableBuffer == null || currentTableBuffer.getRowCount() == 0) {
            return;
        }

        captureInProgressColumnPrefixState();
        sendCommittedPrefix(currentTableName, currentTableBuffer);
        currentTableBuffer.retainInProgressRow(
                prefixSizeBefore,
                prefixValueCountBefore,
                prefixStringDataSizeBefore,
                prefixArrayShapeOffsetBefore,
                prefixArrayDataOffsetBefore
        );
        resetCommittedDatagramEstimate();
        for (int i = 0; i < inProgressColumnCount; i++) {
            inProgressColumns[i].rebaseToEmptyTable();
        }
    }

    private void sendCommittedPrefix(CharSequence tableName, QwpTableBuffer tableBuffer) {
        int payloadLength = encodeCommittedPrefixPayloadForUdp(tableBuffer);
        sendEncodedPayload(tableName, payloadLength);
    }

    private void sendEncodedPayload(CharSequence tableName, int payloadLength) {
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
    }

    private void sendWholeTableBuffer(CharSequence tableName, QwpTableBuffer tableBuffer) {
        int payloadLength = encodeTablePayloadForUdp(tableBuffer);
        sendEncodedPayload(tableName, payloadLength);
        tableBuffer.reset();
    }

    private boolean shouldFlushCommittedRowsAfterCommit() {
        if (committedDatagramEstimate >= maxDatagramSize) {
            return true;
        }

        long predictedNextRowGrowth = currentTableHeadroomState.predictNextRowGrowth();
        if (predictedNextRowGrowth <= 0) {
            return false;
        }
        return committedDatagramEstimate > maxDatagramSize - predictedNextRowGrowth;
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

    /**
     * Captures the state of a column buffer at the moment the in-progress row starts
     * writing to it. The snapshot allows the sender to compute incremental datagram
     * size estimates and to roll back the column to its pre-row state on error or cancel.
     */
    private static final class InProgressColumnState {
        private int arrayDataOffsetBefore;
        private int arrayShapeOffsetBefore;
        private QwpTableBuffer.ColumnBuffer column;
        private boolean nullable;
        private long payloadEstimateDelta;
        private int sizeBefore;
        private long stringDataSizeBefore;
        private int symbolDictionarySizeBefore;
        private int valueCountBefore;

        void clear() {
            column = null;
            nullable = false;
            payloadEstimateDelta = 0;
        }

        void of(QwpTableBuffer.ColumnBuffer column) {
            this.column = column;
            this.nullable = column.isNullable();
            this.payloadEstimateDelta = 0;
            this.sizeBefore = column.getSize();
            this.valueCountBefore = column.getValueCount();
            this.stringDataSizeBefore = column.getStringDataSize();
            this.arrayShapeOffsetBefore = column.getArrayShapeOffset();
            this.arrayDataOffsetBefore = column.getArrayDataOffset();
            this.symbolDictionarySizeBefore = column.getSymbolDictionarySize();
        }

        void captureAfterWrite() {
            this.payloadEstimateDelta = estimateInProgressColumnPayload(this);
        }

        void rebaseToEmptyTable() {
            this.sizeBefore = 0;
            this.valueCountBefore = 0;
            this.stringDataSizeBefore = 0;
            this.arrayShapeOffsetBefore = 0;
            this.arrayDataOffsetBefore = 0;
            this.symbolDictionarySizeBefore = 0;
            captureAfterWrite();
        }
    }

    private static final class TableHeadroomState {
        private long cachedBaseEstimate = -1;
        private int cachedBaseEstimateColumnCount = -1;
        private int committedSampleCount;
        private int schemaColumnCount = -1;
        private long ewmaRowDatagramGrowth;
        private long lastRowDatagramGrowth;

        void cacheBaseEstimate(int currentSchemaColumnCount, long estimate) {
            cachedBaseEstimateColumnCount = currentSchemaColumnCount;
            cachedBaseEstimate = estimate;
        }

        long getCachedBaseEstimate(int currentSchemaColumnCount) {
            return cachedBaseEstimateColumnCount == currentSchemaColumnCount ? cachedBaseEstimate : -1;
        }

        long predictNextRowGrowth() {
            if (committedSampleCount < 2) {
                return 0;
            }
            return Math.max(lastRowDatagramGrowth, ewmaRowDatagramGrowth);
        }

        void recordCommittedRow(int currentSchemaColumnCount, long rowDatagramGrowth) {
            if (schemaColumnCount != currentSchemaColumnCount) {
                committedSampleCount = 0;
                ewmaRowDatagramGrowth = 0;
                lastRowDatagramGrowth = 0;
                schemaColumnCount = currentSchemaColumnCount;
            }

            lastRowDatagramGrowth = rowDatagramGrowth;
            if (committedSampleCount == 0) {
                ewmaRowDatagramGrowth = rowDatagramGrowth;
            } else {
                ewmaRowDatagramGrowth += (rowDatagramGrowth - ewmaRowDatagramGrowth) >> ADAPTIVE_HEADROOM_EWMA_SHIFT;
            }
            if (committedSampleCount < Integer.MAX_VALUE) {
                committedSampleCount++;
            }
        }
    }
}
