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
 */
public class QwpUdpSender implements Sender {
    private static final Logger LOG = LoggerFactory.getLogger(QwpUdpSender.class);

    private final UdpLineChannel channel;
    private final QwpWebSocketEncoder encoder;
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;

    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    private boolean closed;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;

    public QwpUdpSender(NetworkFacade nf, int interfaceIPv4, int sendToAddress, int port, int ttl) {
        this.encoder = new QwpWebSocketEncoder();
        this.encoder.setGorillaEnabled(false);
        this.channel = new UdpLineChannel(nf, interfaceIPv4, sendToAddress, port, ttl);
        this.tableBuffers = new CharSequenceObjHashMap<>();
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
        currentTableBuffer.nextRow();
    }

    @Override
    public Sender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_BOOLEAN, false);
        col.addBoolean(value);
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
            encoder.close();
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL64, true);
        col.addDecimal64(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL128, true);
        col.addDecimal128(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DECIMAL256, true);
        col.addDecimal256(value);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(array);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_DOUBLE, false);
        col.addDouble(value);
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
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name.toString(), TYPE_LONG_ARRAY, true);
        col.addLongArray(array);
        return this;
    }

    @Override
    public Sender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_LONG, false);
        col.addLong(value);
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
    }

    @Override
    public Sender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_STRING, true);
        col.addString(value != null ? value.toString() : null);
        return this;
    }

    @Override
    public Sender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_SYMBOL, true);
        if (value != null) {
            col.addSymbol(value.toString());
        } else {
            col.addSymbol(null);
        }
        return this;
    }

    @Override
    public Sender table(CharSequence tableName) {
        checkNotClosed();
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
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
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP_NANOS, true);
            col.addLong(value);
        } else {
            long micros = toMicros(value, unit);
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
            col.addLong(micros);
        }
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName.toString(), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    private void atMicros(long timestampMicros) {
        if (cachedTimestampColumn == null) {
            cachedTimestampColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
        }
        cachedTimestampColumn.addLong(timestampMicros);
        currentTableBuffer.nextRow();
    }

    private void atNanos(long timestampNanos) {
        if (cachedTimestampNanosColumn == null) {
            cachedTimestampNanosColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP_NANOS, true);
        }
        cachedTimestampNanosColumn.addLong(timestampNanos);
        currentTableBuffer.nextRow();
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

    private void flushInternal() {
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) continue;
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) continue;

            int len = encoder.encode(tableBuffer, false);
            try {
                channel.send(encoder.getBuffer().getBufferPtr(), len);
            } catch (LineSenderException e) {
                LOG.warn("UDP send failed [table={}, errno={}]: {}", tableName, channel.errno(), String.valueOf(e));
            }
            tableBuffer.reset();
        }
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
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
}
