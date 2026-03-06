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
    private final SegmentedNativeBufferWriter payloadWriter = new SegmentedNativeBufferWriter();
    private final NativeRowStaging stagedRow = new NativeRowStaging();
    private final boolean trackDatagramEstimate;
    private final NativeSegmentList datagramSegments = new NativeSegmentList();
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    private QwpTableBuffer.ColumnBuffer[] stagedColumns = new QwpTableBuffer.ColumnBuffer[8];

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private long committedDatagramEstimate;
    private int stagedRowValueCount;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;
    private QwpTableBuffer.ColumnBuffer[] rowFillColumns = new QwpTableBuffer.ColumnBuffer[8];
    private int rowFillColumnCount;
    private int stagedColumnCount;

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
        stageBooleanColumnValue(columnName, value);
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
        stageDecimal64ColumnValue(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageDecimal128ColumnValue(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageDecimal256ColumnValue(name, value);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageDoubleArrayColumnValue(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageDoubleArrayColumnValue(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageDoubleArrayColumnValue(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageDoubleArrayColumnValue(name, array);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        stageDoubleColumnValue(columnName, value);
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
        stageLongArrayColumnValue(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageLongArrayColumnValue(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageLongArrayColumnValue(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) {
            return this;
        }
        checkNotClosed();
        checkTableSelected();
        stageLongArrayColumnValue(name, array);
        return this;
    }

    @Override
    public Sender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        stageLongColumnValue(columnName, value);
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
        resetCommittedDatagramEstimate();
    }

    @Override
    public Sender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        stageStringColumnValue(columnName, value);
        return this;
    }

    @Override
    public Sender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        stageSymbolColumnValue(columnName, value);
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
        resetCommittedDatagramEstimate();

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
            stageTimestampColumnValue(columnName, TYPE_TIMESTAMP_NANOS, value, ENTRY_TIMESTAMP_COL_NANOS);
        } else {
            long micros = toMicros(value, unit);
            stageTimestampColumnValue(columnName, TYPE_TIMESTAMP, micros, ENTRY_TIMESTAMP_COL_MICROS);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        stageTimestampColumnValue(columnName, TYPE_TIMESTAMP, micros, ENTRY_TIMESTAMP_COL_MICROS);
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

    private void stageBooleanColumnValue(CharSequence name, boolean value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_BOOLEAN, false);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageBoolean(col, value);
    }

    private void stageDecimal128ColumnValue(CharSequence name, Decimal128 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL128, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageDecimal128(col, value);
    }

    private void stageDecimal256ColumnValue(CharSequence name, Decimal256 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL256, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageDecimal256(col, value);
    }

    private void stageDecimal64ColumnValue(CharSequence name, Decimal64 value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL64, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageDecimal64(col, value);
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
        addStagedColumn(col);
        stagedRow.stageLong(col, nanos ? ENTRY_AT_NANOS : ENTRY_AT_MICROS, value);
    }

    private void stageDoubleArrayColumnValue(CharSequence name, Object value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE_ARRAY, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageDoubleArray(col, value, estimateDoubleArrayPayload(value));
    }

    private void stageDoubleColumnValue(CharSequence name, double value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE, false);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageDouble(col, value);
    }

    private void stageLongArrayColumnValue(CharSequence name, Object value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG_ARRAY, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageLongArray(col, value, estimateLongArrayPayload(value));
    }

    private void stageLongColumnValue(CharSequence name, long value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG, false);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageLong(col, ENTRY_LONG, value);
    }

    private void stageStringColumnValue(CharSequence name, CharSequence value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_STRING, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageUtf8(col, ENTRY_STRING, value, 0);
    }

    private void stageSymbolColumnValue(CharSequence name, CharSequence value) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_SYMBOL, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageUtf8(col, ENTRY_SYMBOL, value, estimateSymbolPayloadDelta(col, col.getValueCount(), value));
    }

    private void stageTimestampColumnValue(CharSequence name, byte type, long value, byte entryKind) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, type, true);
        stagedRowValueCount++;
        addStagedColumn(col);
        stagedRow.stageLong(col, entryKind, value);
    }

    private void atMicros(long timestampMicros) {
        if (stagedRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }
        stageDesignatedTimestampValue(timestampMicros, false);
        commitCurrentRow();
    }

    private void atNanos(long timestampNanos) {
        if (stagedRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }
        stageDesignatedTimestampValue(timestampNanos, true);
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
        stagedRow.clear();
        stagedRowValueCount = 0;
        stagedColumnCount = 0;
        rowFillColumnCount = 0;
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
        if (stagedRowValueCount == 0) {
            throw new LineSenderException("no columns were provided");
        }

        long estimate = 0;
        int targetRows = currentTableBuffer.getRowCount() + 1;
        collectRowFillColumns(targetRows);
        if (trackDatagramEstimate) {
            estimate = estimateCurrentDatagramSizeWithStagedRow(targetRows);
            if (estimate > maxDatagramSize) {
                if (currentTableBuffer.getRowCount() == 0) {
                    throw singleRowTooLarge(estimate);
                }
                flushCommittedRowsOfCurrentTable();
                targetRows = currentTableBuffer.getRowCount() + 1;
                collectRowFillColumns(targetRows);
                estimate = estimateCurrentDatagramSizeWithStagedRow(targetRows);
                if (estimate > maxDatagramSize) {
                    throw singleRowTooLarge(estimate);
                }
            }
        }

        commitStagedRow();
        if (trackDatagramEstimate) {
            committedDatagramEstimate = estimate;
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

    private int encodeTablePayloadForUdp(QwpTableBuffer tableBuffer) {
        payloadWriter.reset();
        columnWriter.setBuffer(payloadWriter);
        columnWriter.encodeTable(tableBuffer, false, false, false);
        payloadWriter.finish();
        return payloadWriter.getPosition();
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
        long estimate = currentTableBuffer.getRowCount() > 0 ? committedDatagramEstimate : estimateBaseForCurrentSchema();
        for (int i = 0, n = stagedRow.size(); i < n; i++) {
            QwpTableBuffer.ColumnBuffer col = stagedRow.getColumn(i);
            estimate += estimateStagedEntryPayload(i, col);
            if (col.isNullable()) {
                estimate += bitmapBytes(targetRows) - bitmapBytes(col.getSize());
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

    private long estimateStagedEntryPayload(int entryIndex, QwpTableBuffer.ColumnBuffer col) {
        int valueCountBefore = col.getValueCount();
        return switch (stagedRow.getKind(entryIndex)) {
            case ENTRY_AT_MICROS, ENTRY_AT_NANOS, ENTRY_DOUBLE, ENTRY_LONG,
                    ENTRY_TIMESTAMP_COL_MICROS, ENTRY_TIMESTAMP_COL_NANOS -> 8;
            case ENTRY_BOOL -> packedBytes(valueCountBefore + 1) - packedBytes(valueCountBefore);
            case ENTRY_DECIMAL64 -> 8;
            case ENTRY_DECIMAL128 -> 16;
            case ENTRY_DECIMAL256 -> 32;
            case ENTRY_DOUBLE_ARRAY, ENTRY_LONG_ARRAY -> stagedRow.getLong0(entryIndex);
            case ENTRY_STRING -> stagedRow.getAuxInt(entryIndex) < 0 ? 0 : 4L + stagedRow.getAuxInt(entryIndex);
            case ENTRY_SYMBOL -> stagedRow.getLong1(entryIndex);
            default -> throw new LineSenderException("unknown staged row entry type: " + stagedRow.getKind(entryIndex));
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

    private void ensureStagedColumnCapacity(int required) {
        if (required <= stagedColumns.length) {
            return;
        }

        int newCapacity = stagedColumns.length;
        while (newCapacity < required) {
            newCapacity *= 2;
        }

        QwpTableBuffer.ColumnBuffer[] newArr = new QwpTableBuffer.ColumnBuffer[newCapacity];
        System.arraycopy(stagedColumns, 0, newArr, 0, stagedColumnCount);
        stagedColumns = newArr;
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
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
        resetCommittedDatagramEstimate();
    }

    private void flushSingleTable(String tableName, QwpTableBuffer tableBuffer) {
        sendTableBuffer(tableName, tableBuffer);
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        clearStagedRow();
        resetCommittedDatagramEstimate();
    }

    private boolean hasInProgressRow() {
        return stagedRow.size() > 0;
    }

    private boolean isStagedColumn(QwpTableBuffer.ColumnBuffer col) {
        for (int i = 0; i < stagedColumnCount; i++) {
            if (stagedColumns[i] == col) {
                return true;
            }
        }
        return false;
    }

    private void addStagedColumn(QwpTableBuffer.ColumnBuffer col) {
        if (isStagedColumn(col)) {
            return;
        }
        ensureStagedColumnCapacity(stagedColumnCount + 1);
        stagedColumns[stagedColumnCount++] = col;
    }

    private void commitStagedRow() {
        stagedRow.commitIntoTableBuffer(currentTableBuffer, rowFillColumns, rowFillColumnCount);
    }

    private static void commitDoubleArrayValue(QwpTableBuffer.ColumnBuffer col, Object value) {
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

    private static void commitLongArrayValue(QwpTableBuffer.ColumnBuffer col, Object value) {
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

    private void resetCommittedDatagramEstimate() {
        committedDatagramEstimate = 0;
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
        private QwpTableBuffer.ColumnBuffer[] columns = new QwpTableBuffer.ColumnBuffer[8];
        private int size;
        private Object[] sidecarObjects = new Object[8];

        void stageBoolean(QwpTableBuffer.ColumnBuffer column, boolean value) {
            stageEntry(column, null, ENTRY_BOOL, 0, value ? 1 : 0, 0, 0, 0);
        }

        void stageDecimal128(QwpTableBuffer.ColumnBuffer column, Decimal128 value) {
            stageEntry(column, null, ENTRY_DECIMAL128, value.getScale(), value.getHigh(), value.getLow(), 0, 0);
        }

        void stageDecimal256(QwpTableBuffer.ColumnBuffer column, Decimal256 value) {
            stageEntry(
                    column,
                    null,
                    ENTRY_DECIMAL256,
                    value.getScale(),
                    value.getHh(),
                    value.getHl(),
                    value.getLh(),
                    value.getLl()
            );
        }

        void stageDecimal64(QwpTableBuffer.ColumnBuffer column, Decimal64 value) {
            stageEntry(column, null, ENTRY_DECIMAL64, value.getScale(), value.getValue(), 0, 0, 0);
        }

        void stageDouble(QwpTableBuffer.ColumnBuffer column, double value) {
            stageEntry(column, null, ENTRY_DOUBLE, 0, Double.doubleToRawLongBits(value), 0, 0, 0);
        }

        void stageDoubleArray(QwpTableBuffer.ColumnBuffer column, Object value, long estimatePayload) {
            stageEntry(column, value, ENTRY_DOUBLE_ARRAY, 0, estimatePayload, 0, 0, 0);
        }

        void stageLong(QwpTableBuffer.ColumnBuffer column, int kind, long value) {
            stageEntry(column, null, kind, 0, value, 0, 0, 0);
        }

        void stageLongArray(QwpTableBuffer.ColumnBuffer column, Object value, long estimatePayload) {
            stageEntry(column, value, ENTRY_LONG_ARRAY, 0, estimatePayload, 0, 0, 0);
        }

        void stageUtf8(QwpTableBuffer.ColumnBuffer column, int kind, CharSequence value, long long1) {
            int len = -1;
            long offset = 0;
            if (value != null) {
                offset = varData.getAppendOffset();
                varData.putUtf8(value);
                len = (int) (varData.getAppendOffset() - offset);
            }
            stageEntry(column, null, kind, len, offset, long1, 0, 0);
        }

        void clear() {
            for (int i = 0; i < size; i++) {
                columns[i] = null;
                sidecarObjects[i] = null;
            }
            size = 0;
            entries.truncate();
            varData.truncate();
        }

        void close() {
            entries.close();
            varData.close();
        }

        int getAuxInt(int index) {
            return Unsafe.getUnsafe().getInt(entryAddress(index) + AUX_INT_OFFSET);
        }

        QwpTableBuffer.ColumnBuffer getColumn(int index) {
            return columns[index];
        }

        int getKind(int index) {
            return Unsafe.getUnsafe().getInt(entryAddress(index));
        }

        long getLong0(int index) {
            return Unsafe.getUnsafe().getLong(entryAddress(index) + LONG0_OFFSET);
        }

        long getLong1(int index) {
            return Unsafe.getUnsafe().getLong(entryAddress(index) + LONG1_OFFSET);
        }

        void commitIntoTableBuffer(QwpTableBuffer tableBuffer, QwpTableBuffer.ColumnBuffer[] rowFillColumns, int rowFillColumnCount) {
            for (int i = 0; i < size; i++) {
                QwpTableBuffer.ColumnBuffer column = columns[i];
                long entryAddress = entryAddress(i);
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
                    case ENTRY_DOUBLE_ARRAY -> commitDoubleArrayValue(column, sidecarObjects[i]);
                    case ENTRY_LONG_ARRAY -> commitLongArrayValue(column, sidecarObjects[i]);
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
            tableBuffer.nextRow(rowFillColumns, rowFillColumnCount);
        }

        int size() {
            return size;
        }

        private void stageEntry(
                QwpTableBuffer.ColumnBuffer column,
                Object sidecarObject,
                int kind,
                int auxInt,
                long long0,
                long long1,
                long long2,
                long long3
        ) {
            ensureCapacity(size + 1);
            columns[size] = column;
            sidecarObjects[size] = sidecarObject;
            entries.putInt(kind);
            entries.putInt(auxInt);
            entries.putLong(long0);
            entries.putLong(long1);
            entries.putLong(long2);
            entries.putLong(long3);
            size++;
        }

        private long entryAddress(int index) {
            return entries.addressOf((long) index * ENTRY_SIZE);
        }

        private void ensureCapacity(int required) {
            if (required <= columns.length) {
                return;
            }

            int newCapacity = columns.length;
            while (newCapacity < required) {
                newCapacity *= 2;
            }

            QwpTableBuffer.ColumnBuffer[] newColumns = new QwpTableBuffer.ColumnBuffer[newCapacity];
            System.arraycopy(columns, 0, newColumns, 0, size);
            columns = newColumns;

            Object[] newSidecarObjects = new Object[newCapacity];
            System.arraycopy(sidecarObjects, 0, newSidecarObjects, 0, size);
            sidecarObjects = newSidecarObjects;
        }
    }
}
