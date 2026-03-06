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
 * a datagram exceeds the size limit. The current in-progress row is cancelled,
 * committed rows are flushed, and the in-progress row is replayed from a journal.
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
    private final NativeBufferWriter buffer = new NativeBufferWriter();
    private final UdpLineChannel channel;
    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private final int maxDatagramSize;
    private final ObjList<ColumnEntry> rowJournal = new ObjList<>();
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private long committedEstimate;
    private int committedEstimateColumnCount;
    private int currentRowColumnCount;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;
    private int estimateColumnCount;
    private int rowJournalSize;
    private long runningEstimate;

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl) {
        this(nf, interfaceIPv4, sendToAddress, port, ttl, 0);
    }

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl, int maxDatagramSize) {
        this.channel = new UdpLineChannel(nf, interfaceIPv4, sendToAddress, port, ttl);
        this.tableBuffers = new CharSequenceObjHashMap<>();
        this.maxDatagramSize = maxDatagramSize;
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
            rollbackEstimateToCommitted();
        }
        rowJournalSize = 0;
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (hasInProgressRow()) {
                    currentTableBuffer.cancelCurrentRow();
                    rollbackEstimateToCommitted();
                    rowJournalSize = 0;
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
            buffer.close();
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
                buf.reset();
            }
        }
        currentTableBuffer = null;
        currentTableName = null;
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        rowJournalSize = 0;
        resetEstimateState();
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
        if (maxDatagramSize > 0 && currentTableBuffer != null && currentTableBuffer.getRowCount() > 0) {
            flushSingleTable(currentTableName, currentTableBuffer);
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        rowJournalSize = 0;
        resetEstimateState();

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
            if (currentRowColumnCount > 0) {
                throw new LineSenderException("schema change in middle of row is not supported");
            }
            flushSingleTable(currentTableName, currentTableBuffer);
            col = currentTableBuffer.getOrCreateColumn(name, type, nullable);
        }

        if (col == null) {
            col = currentTableBuffer.getOrCreateColumn(name, type, nullable);
        }
        syncSchemaEstimate();
        return col;
    }

    private void appendBooleanColumn(CharSequence name, boolean value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_BOOLEAN, false);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addBoolean(value);

        long payloadDelta = packedBytes(col.getValueCount()) - packedBytes(valueCountBefore);
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_BOOL;
            e.name = col.getName();
            e.boolValue = value;
        }
    }

    private void appendDecimal128Column(CharSequence name, Decimal128 value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL128, true);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addDecimal128(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 16;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL128;
            e.name = col.getName();
            e.objectValue = value;
        }
    }

    private void appendDecimal256Column(CharSequence name, Decimal256 value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL256, true);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addDecimal256(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 32;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL256;
            e.name = col.getName();
            e.objectValue = value;
        }
    }

    private void appendDecimal64Column(CharSequence name, Decimal64 value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DECIMAL64, true);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addDecimal64(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 8;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL64;
            e.name = col.getName();
            e.objectValue = value;
        }
    }

    private void appendDesignatedTimestamp(long value, boolean nanos, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col;
        if (nanos) {
            if (cachedTimestampNanosColumn == null) {
                cachedTimestampNanosColumn = acquireColumn("", TYPE_TIMESTAMP_NANOS, true);
            } else {
                syncSchemaEstimate();
            }
            col = cachedTimestampNanosColumn;
        } else {
            if (cachedTimestampColumn == null) {
                cachedTimestampColumn = acquireColumn("", TYPE_TIMESTAMP, true);
            } else {
                syncSchemaEstimate();
            }
            col = cachedTimestampColumn;
        }

        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addLong(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 8;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = nanos ? ENTRY_AT_NANOS : ENTRY_AT_MICROS;
            e.longValue = value;
        }
    }

    private void appendDoubleArrayColumn(CharSequence name, Object value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE_ARRAY, true);
        int sizeBefore = col.getSize();

        long payloadDelta;
        if (value instanceof double[] values) {
            payloadDelta = estimateArrayValueSize(1, values.length);
            col.addDoubleArray(values);
        } else if (value instanceof double[][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            payloadDelta = estimateArrayValueSize(2, (long) dim0 * dim1);
            col.addDoubleArray(values);
        } else if (value instanceof double[][][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            payloadDelta = estimateArrayValueSize(3, (long) dim0 * dim1 * dim2);
            col.addDoubleArray(values);
        } else if (value instanceof DoubleArray values) {
            payloadDelta = estimateArrayValueSize(values);
            col.addDoubleArray(values);
        } else {
            throw new LineSenderException("unsupported double array type");
        }

        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE_ARRAY;
            e.name = col.getName();
            e.objectValue = value;
        }
    }

    private void appendDoubleColumn(CharSequence name, double value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_DOUBLE, false);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addDouble(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 8;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE;
            e.name = col.getName();
            e.doubleValue = value;
        }
    }

    private void appendLongArrayColumn(CharSequence name, Object value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG_ARRAY, true);
        int sizeBefore = col.getSize();

        long payloadDelta;
        if (value instanceof long[] values) {
            payloadDelta = estimateArrayValueSize(1, values.length);
            col.addLongArray(values);
        } else if (value instanceof long[][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            payloadDelta = estimateArrayValueSize(2, (long) dim0 * dim1);
            col.addLongArray(values);
        } else if (value instanceof long[][][] values) {
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            payloadDelta = estimateArrayValueSize(3, (long) dim0 * dim1 * dim2);
            col.addLongArray(values);
        } else if (value instanceof LongArray values) {
            payloadDelta = estimateArrayValueSize(values);
            col.addLongArray(values);
        } else {
            throw new LineSenderException("unsupported long array type");
        }

        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG_ARRAY;
            e.name = col.getName();
            e.objectValue = value;
        }
    }

    private void appendLongColumn(CharSequence name, long value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_LONG, false);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addLong(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 8;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG;
            e.name = col.getName();
            e.longValue = value;
        }
    }

    private void appendStringColumn(CharSequence name, CharSequence value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_STRING, true);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        long stringBytesBefore = col.getStringDataSize();
        col.addString(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 4
                + (col.getStringDataSize() - stringBytesBefore);
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_STRING;
            e.name = col.getName();
            e.stringValue = Chars.toString(value);
        }
    }

    private void appendSymbolColumn(CharSequence name, CharSequence value, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, TYPE_SYMBOL, true);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        int dictSizeBefore = col.getSymbolDictionarySize();
        col.addSymbol(value);

        long payloadDelta = estimateSymbolPayloadDelta(col, valueCountBefore, dictSizeBefore, value);
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_SYMBOL;
            e.name = col.getName();
            e.stringValue = Chars.toString(value);
        }
    }

    private void appendTimestampColumn(CharSequence name, byte type, long value, byte journalKind, boolean addJournal) {
        QwpTableBuffer.ColumnBuffer col = acquireColumn(name, type, true);
        int sizeBefore = col.getSize();
        int valueCountBefore = col.getValueCount();
        col.addLong(value);

        long payloadDelta = (long) (col.getValueCount() - valueCountBefore) * 8;
        applyValueEstimate(col, sizeBefore, col.getSize(), payloadDelta);
        currentRowColumnCount++;

        if (addJournal && maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = journalKind;
            e.name = col.getName();
            e.longValue = value;
        }
    }

    private void applyRowPaddingEstimate(int targetRows) {
        for (int i = 0, n = currentTableBuffer.getColumnCount(); i < n; i++) {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getColumn(i);
            int sizeBefore = col.getSize();
            int missing = targetRows - sizeBefore;
            if (missing <= 0) {
                continue;
            }

            if (col.isNullable()) {
                runningEstimate += bitmapBytes(sizeBefore + missing) - bitmapBytes(sizeBefore);
                continue;
            }

            int valuesBefore = col.getValueCount();
            runningEstimate += nonNullablePaddingCost(col.getType(), valuesBefore, missing);
        }
    }

    private void applyValueEstimate(QwpTableBuffer.ColumnBuffer col, int sizeBefore, int sizeAfter, long payloadDelta) {
        runningEstimate += payloadDelta;
        if (col.isNullable()) {
            runningEstimate += bitmapBytes(sizeAfter) - bitmapBytes(sizeBefore);
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

    private void commitCurrentRow() {
        if (currentRowColumnCount == 0) {
            throw new LineSenderException("no columns were provided");
        }

        int targetRows = currentTableBuffer.getRowCount() + 1;
        applyRowPaddingEstimate(targetRows);

        if (maxDatagramSize > 0) {
            maybeAutoFlush();
        }

        currentTableBuffer.nextRow();
        committedEstimate = runningEstimate;
        committedEstimateColumnCount = estimateColumnCount;
        currentRowColumnCount = 0;
        rowJournalSize = 0;
    }

    private void ensureNoInProgressRow(String operation) {
        if (hasInProgressRow()) {
            throw new LineSenderException(
                    "Cannot " + operation + " while row is in progress. "
                            + "Use sender.at(), sender.atNow(), or sender.cancelRow() first."
            );
        }
    }

    private int encodeForUdp(QwpTableBuffer tableBuffer) {
        buffer.reset();
        // Write 12-byte QWP1 header: magic, version, flags=0, tableCount=1, payloadLength=0 (patched later)
        buffer.putByte((byte) 'Q');
        buffer.putByte((byte) 'W');
        buffer.putByte((byte) 'P');
        buffer.putByte((byte) '1');
        buffer.putByte(VERSION_1);
        buffer.putByte((byte) 0); // flags
        buffer.putShort((short) 1); // tableCount
        buffer.putInt(0); // payloadLength placeholder
        int payloadStart = buffer.getPosition();
        columnWriter.setBuffer(buffer);
        columnWriter.encodeTable(tableBuffer, false, false, false);
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
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

    private long estimateSymbolPayloadDelta(
            QwpTableBuffer.ColumnBuffer col,
            int valueCountBefore,
            int dictSizeBefore,
            CharSequence value
    ) {
        int valueCountAfter = col.getValueCount();
        if (valueCountAfter == valueCountBefore) {
            return 0;
        }

        int dictSizeAfter = col.getSymbolDictionarySize();
        if (dictSizeAfter == dictSizeBefore) {
            int maxIndex = Math.max(0, dictSizeAfter - 1);
            return NativeBufferWriter.varintSize(maxIndex);
        }

        long delta = 0;
        int utf8Len = utf8Length(value);
        delta += NativeBufferWriter.varintSize(utf8Len) + utf8Len;
        delta += NativeBufferWriter.varintSize(dictSizeAfter)
                - NativeBufferWriter.varintSize(dictSizeBefore);

        if (dictSizeBefore > 0 && valueCountBefore > 0) {
            int oldMax = dictSizeBefore - 1;
            int newMax = dictSizeAfter - 1;
            delta += (long) valueCountBefore * (
                    NativeBufferWriter.varintSize(newMax)
                            - NativeBufferWriter.varintSize(oldMax)
            );
        }

        int newMax = dictSizeAfter - 1;
        delta += NativeBufferWriter.varintSize(newMax);
        return delta;
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

            int len = encodeForUdp(tableBuffer);
            try {
                channel.send(buffer.getBufferPtr(), len);
            } catch (LineSenderException e) {
                LOG.warn("UDP send failed [table={}, errno={}]: {}", tableName, channel.errno(), String.valueOf(e));
            }
            tableBuffer.reset();
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        rowJournalSize = 0;
        resetEstimateState();
    }

    private void flushSingleTable(String tableName, QwpTableBuffer tableBuffer) {
        int len = encodeForUdp(tableBuffer);
        try {
            channel.send(buffer.getBufferPtr(), len);
        } catch (LineSenderException e) {
            LOG.warn("UDP send failed [table={}, errno={}]: {}", tableName, channel.errno(), String.valueOf(e));
        }
        tableBuffer.reset();
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        resetEstimateState();
    }

    private void maybeAutoFlush() {
        if (runningEstimate <= maxDatagramSize) {
            return;
        }

        if (currentTableBuffer.getRowCount() == 0) {
            throw singleRowTooLarge(runningEstimate);
        }

        currentTableBuffer.cancelCurrentRow();
        rollbackEstimateToCommitted();

        flushSingleTable(currentTableName, currentTableBuffer);
        replayRowJournal();
        applyRowPaddingEstimate(currentTableBuffer.getRowCount() + 1);

        if (runningEstimate > maxDatagramSize) {
            throw singleRowTooLarge(runningEstimate);
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

    private void replayRowJournal() {
        for (int i = 0; i < rowJournalSize; i++) {
            ColumnEntry entry = rowJournal.getQuick(i);
            switch (entry.kind) {
                case ENTRY_AT_MICROS -> appendDesignatedTimestamp(entry.longValue, false, false);
                case ENTRY_AT_NANOS -> appendDesignatedTimestamp(entry.longValue, true, false);
                case ENTRY_BOOL -> appendBooleanColumn(entry.name, entry.boolValue, false);
                case ENTRY_DECIMAL128 -> appendDecimal128Column(entry.name, (Decimal128) entry.objectValue, false);
                case ENTRY_DECIMAL256 -> appendDecimal256Column(entry.name, (Decimal256) entry.objectValue, false);
                case ENTRY_DECIMAL64 -> appendDecimal64Column(entry.name, (Decimal64) entry.objectValue, false);
                case ENTRY_DOUBLE -> appendDoubleColumn(entry.name, entry.doubleValue, false);
                case ENTRY_DOUBLE_ARRAY -> appendDoubleArrayColumn(entry.name, entry.objectValue, false);
                case ENTRY_LONG -> appendLongColumn(entry.name, entry.longValue, false);
                case ENTRY_LONG_ARRAY -> appendLongArrayColumn(entry.name, entry.objectValue, false);
                case ENTRY_STRING -> appendStringColumn(entry.name, entry.stringValue, false);
                case ENTRY_SYMBOL -> appendSymbolColumn(entry.name, entry.stringValue, false);
                case ENTRY_TIMESTAMP_COL_MICROS ->
                        appendTimestampColumn(entry.name, TYPE_TIMESTAMP, entry.longValue, ENTRY_TIMESTAMP_COL_MICROS, false);
                case ENTRY_TIMESTAMP_COL_NANOS ->
                        appendTimestampColumn(entry.name, TYPE_TIMESTAMP_NANOS, entry.longValue, ENTRY_TIMESTAMP_COL_NANOS, false);
                default -> throw new LineSenderException("unknown row journal entry type: " + entry.kind);
            }
        }
    }

    private void resetEstimateState() {
        runningEstimate = 0;
        committedEstimate = 0;
        estimateColumnCount = 0;
        committedEstimateColumnCount = 0;
        currentRowColumnCount = 0;
    }

    private boolean hasInProgressRow() {
        return currentTableBuffer != null && currentTableBuffer.hasInProgressRow();
    }

    private void rollbackEstimateToCommitted() {
        runningEstimate = committedEstimate;
        estimateColumnCount = committedEstimateColumnCount;
        currentRowColumnCount = 0;
    }

    private LineSenderException singleRowTooLarge(long estimate) {
        return new LineSenderException(
                "single row exceeds maximum datagram size (" + maxDatagramSize
                        + " bytes), estimated " + estimate + " bytes"
        );
    }

    private void syncSchemaEstimate() {
        int newColumnCount = currentTableBuffer.getColumnCount();
        if (newColumnCount == estimateColumnCount) {
            return;
        }

        if (estimateColumnCount == 0) {
            long base = HEADER_SIZE;
            int tableNameUtf8 = NativeBufferWriter.utf8Length(currentTableName);
            base += NativeBufferWriter.varintSize(tableNameUtf8) + tableNameUtf8;
            base += VARINT_INT_UPPER_BOUND; // row count varint upper bound
            base += VARINT_INT_UPPER_BOUND; // column count varint upper bound
            base += 1; // schema mode byte
            base += SAFETY_MARGIN_BYTES;
            runningEstimate += base;
        }

        QwpColumnDef[] defs = currentTableBuffer.getColumnDefs();
        for (int i = estimateColumnCount; i < newColumnCount; i++) {
            QwpColumnDef def = defs[i];
            int nameUtf8 = NativeBufferWriter.utf8Length(def.getName());
            runningEstimate += NativeBufferWriter.varintSize(nameUtf8) + nameUtf8;
            runningEstimate += 1; // wire type code

            byte type = def.getTypeCode();
            if (type == TYPE_STRING || type == TYPE_VARCHAR) {
                runningEstimate += 4; // offset[0]
            } else if (type == TYPE_SYMBOL) {
                runningEstimate += 1; // varintSize(0) for empty dictionary length
            } else if (type == TYPE_DECIMAL64 || type == TYPE_DECIMAL128 || type == TYPE_DECIMAL256) {
                runningEstimate += 1; // scale byte
            }
        }
        estimateColumnCount = newColumnCount;
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
        double doubleValue;
        byte kind;
        long longValue;
        String name;
        Object objectValue;
        String stringValue;
    }
}
