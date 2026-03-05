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
    private static final Logger LOG = LoggerFactory.getLogger(QwpUdpSender.class);

    private final NativeBufferWriter buffer = new NativeBufferWriter();
    private final UdpLineChannel channel;
    private final QwpColumnWriter columnWriter = new QwpColumnWriter();
    private final int maxDatagramSize;
    private final ObjList<ColumnEntry> rowJournal = new ObjList<>();
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;
    private int rowJournalSize;

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
        if (maxDatagramSize > 0) {
            maybeAutoFlush();
        }
        currentTableBuffer.nextRow();
        rowJournalSize = 0;
    }

    @Override
    public Sender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        String name = columnName.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_BOOLEAN, false);
        col.addBoolean(value);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_BOOL;
            e.name = name;
            e.boolValue = value;
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
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
        }
        rowJournalSize = 0;
    }

    @Override
    public void close() {
        if (!closed) {
            try {
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
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DECIMAL64, true);
        col.addDecimal64(value);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL64;
            e.name = colName;
            e.objectValue = value;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DECIMAL128, true);
        col.addDecimal128(value);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL128;
            e.name = colName;
            e.objectValue = value;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DECIMAL256, true);
        col.addDecimal256(value);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DECIMAL256;
            e.name = colName;
            e.objectValue = value;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE_ARRAY;
            e.name = colName;
            e.objectValue = values;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE_ARRAY;
            e.name = colName;
            e.objectValue = values;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE_ARRAY;
            e.name = colName;
            e.objectValue = values;
        }
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(array);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE_ARRAY;
            e.name = colName;
            e.objectValue = array;
        }
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        String name = columnName.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_DOUBLE, false);
        col.addDouble(value);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_DOUBLE;
            e.name = name;
            e.doubleValue = value;
        }
        return this;
    }

    @Override
    public void flush() {
        checkNotClosed();
        flushInternal();
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG_ARRAY;
            e.name = colName;
            e.objectValue = values;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG_ARRAY;
            e.name = colName;
            e.objectValue = values;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG_ARRAY;
            e.name = colName;
            e.objectValue = values;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        String colName = name.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(colName, TYPE_LONG_ARRAY, true);
        col.addLongArray(array);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG_ARRAY;
            e.name = colName;
            e.objectValue = array;
        }
        return this;
    }

    @Override
    public Sender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        String name = columnName.toString();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_LONG, false);
        col.addLong(value);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_LONG;
            e.name = name;
            e.longValue = value;
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
                buf.reset();
            }
        }
        currentTableBuffer = null;
        currentTableName = null;
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        rowJournalSize = 0;
    }

    @Override
    public Sender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        String name = columnName.toString();
        String strValue = value != null ? value.toString() : null;
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_STRING, true);
        col.addString(strValue);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_STRING;
            e.name = name;
            e.stringValue = strValue;
        }
        return this;
    }

    @Override
    public Sender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        String name = columnName.toString();
        String strValue = value != null ? value.toString() : null;
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_SYMBOL, true);
        col.addSymbol(strValue);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_SYMBOL;
            e.name = name;
            e.stringValue = strValue;
        }
        return this;
    }

    @Override
    public Sender table(CharSequence tableName) {
        checkNotClosed();
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        // Flush current table on switch if auto-flush is enabled and there are committed rows
        if (maxDatagramSize > 0 && currentTableBuffer != null && currentTableBuffer.getRowCount() > 0) {
            flushSingleTable(currentTableName, currentTableBuffer);
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        rowJournalSize = 0;
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
        String name = columnName.toString();
        if (unit == ChronoUnit.NANOS) {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_TIMESTAMP_NANOS, true);
            col.addLong(value);
            if (maxDatagramSize > 0) {
                ColumnEntry e = nextJournalEntry();
                e.kind = ENTRY_TIMESTAMP_COL_NANOS;
                e.name = name;
                e.longValue = value;
            }
        } else {
            long micros = toMicros(value, unit);
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_TIMESTAMP, true);
            col.addLong(micros);
            if (maxDatagramSize > 0) {
                ColumnEntry e = nextJournalEntry();
                e.kind = ENTRY_TIMESTAMP_COL_MICROS;
                e.name = name;
                e.longValue = micros;
            }
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        String name = columnName.toString();
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, TYPE_TIMESTAMP, true);
        col.addLong(micros);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_TIMESTAMP_COL_MICROS;
            e.name = name;
            e.longValue = micros;
        }
        return this;
    }

    private void atMicros(long timestampMicros) {
        if (cachedTimestampColumn == null) {
            cachedTimestampColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
        }
        cachedTimestampColumn.addLong(timestampMicros);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_AT_MICROS;
            e.longValue = timestampMicros;
            maybeAutoFlush();
        }
        currentTableBuffer.nextRow();
        rowJournalSize = 0;
    }

    private void atNanos(long timestampNanos) {
        if (cachedTimestampNanosColumn == null) {
            cachedTimestampNanosColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP_NANOS, true);
        }
        cachedTimestampNanosColumn.addLong(timestampNanos);
        if (maxDatagramSize > 0) {
            ColumnEntry e = nextJournalEntry();
            e.kind = ENTRY_AT_NANOS;
            e.longValue = timestampNanos;
            maybeAutoFlush();
        }
        currentTableBuffer.nextRow();
        rowJournalSize = 0;
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

    private int encodeForUdp(QwpTableBuffer tableBuffer) {
        buffer.reset();
        // Write 12-byte ILP4 header: magic, version, flags=0, tableCount=1, payloadLength=0 (patched later)
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

    private void flushInternal() {
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) continue;
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) continue;

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
    }

    private void maybeAutoFlush() {
        int tentativeRowCount = currentTableBuffer.getRowCount() + 1;
        long estimate = QwpDatagramSizeEstimator.estimate(currentTableBuffer, tentativeRowCount);
        if (estimate > maxDatagramSize) {
            if (currentTableBuffer.getRowCount() == 0) {
                throw new LineSenderException(
                        "single row exceeds maximum datagram size (" + maxDatagramSize
                                + " bytes), estimated " + estimate + " bytes"
                );
            }
            currentTableBuffer.cancelCurrentRow();
            flushSingleTable(currentTableName, currentTableBuffer);
            replayRowJournal();
            // Post-replay check: the replayed row alone may still exceed the limit.
            tentativeRowCount = currentTableBuffer.getRowCount() + 1;
            estimate = QwpDatagramSizeEstimator.estimate(currentTableBuffer, tentativeRowCount);
            if (estimate > maxDatagramSize) {
                throw new LineSenderException(
                        "single row exceeds maximum datagram size (" + maxDatagramSize
                                + " bytes), estimated " + estimate + " bytes"
                );
            }
        }
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

    private void replayDoubleArray(ColumnEntry entry) {
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(entry.name, TYPE_DOUBLE_ARRAY, true);
        if (entry.objectValue instanceof double[] a) {
            col.addDoubleArray(a);
        } else if (entry.objectValue instanceof double[][] a) {
            col.addDoubleArray(a);
        } else if (entry.objectValue instanceof double[][][] a) {
            col.addDoubleArray(a);
        } else if (entry.objectValue instanceof DoubleArray a) {
            col.addDoubleArray(a);
        }
    }

    private void replayLongArray(ColumnEntry entry) {
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(entry.name, TYPE_LONG_ARRAY, true);
        if (entry.objectValue instanceof long[] a) {
            col.addLongArray(a);
        } else if (entry.objectValue instanceof long[][] a) {
            col.addLongArray(a);
        } else if (entry.objectValue instanceof long[][][] a) {
            col.addLongArray(a);
        } else if (entry.objectValue instanceof LongArray a) {
            col.addLongArray(a);
        }
    }

    private void replayRowJournal() {
        for (int i = 0; i < rowJournalSize; i++) {
            ColumnEntry entry = rowJournal.getQuick(i);
            switch (entry.kind) {
                case ENTRY_AT_MICROS -> {
                    cachedTimestampColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
                    cachedTimestampColumn.addLong(entry.longValue);
                }
                case ENTRY_AT_NANOS -> {
                    cachedTimestampNanosColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP_NANOS, true);
                    cachedTimestampNanosColumn.addLong(entry.longValue);
                }
                case ENTRY_BOOL ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_BOOLEAN, false).addBoolean(entry.boolValue);
                case ENTRY_DECIMAL128 ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_DECIMAL128, true)
                                .addDecimal128((Decimal128) entry.objectValue);
                case ENTRY_DECIMAL256 ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_DECIMAL256, true)
                                .addDecimal256((Decimal256) entry.objectValue);
                case ENTRY_DECIMAL64 ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_DECIMAL64, true)
                                .addDecimal64((Decimal64) entry.objectValue);
                case ENTRY_DOUBLE ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_DOUBLE, false).addDouble(entry.doubleValue);
                case ENTRY_DOUBLE_ARRAY -> replayDoubleArray(entry);
                case ENTRY_LONG ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_LONG, false).addLong(entry.longValue);
                case ENTRY_LONG_ARRAY -> replayLongArray(entry);
                case ENTRY_STRING ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_STRING, true).addString(entry.stringValue);
                case ENTRY_SYMBOL ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_SYMBOL, true).addSymbol(entry.stringValue);
                case ENTRY_TIMESTAMP_COL_MICROS ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_TIMESTAMP, true).addLong(entry.longValue);
                case ENTRY_TIMESTAMP_COL_NANOS ->
                        currentTableBuffer.getOrCreateColumn(entry.name, TYPE_TIMESTAMP_NANOS, true).addLong(entry.longValue);
            }
        }
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
