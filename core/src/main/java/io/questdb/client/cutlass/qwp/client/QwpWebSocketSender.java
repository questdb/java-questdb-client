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
import io.questdb.client.cairo.TableUtils;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.LongHashSet;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;


/**
 * QWP v1 WebSocket client sender for streaming data to QuestDB.
 * <p>
 * This sender uses a double-buffering scheme with asynchronous I/O for high throughput:
 * <ul>
 *   <li>User thread writes rows to the active microbatch buffer</li>
 *   <li>When buffer is full (row count, byte size, or age), it's sealed and enqueued</li>
 *   <li>A dedicated I/O thread sends batches asynchronously</li>
 *   <li>Double-buffering ensures one buffer is always available for writing</li>
 * </ul>
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code autoFlushRows} - Maximum rows per batch (default: 500)</li>
 *   <li>{@code autoFlushBytes} - Maximum bytes per batch (default: 1MB)</li>
 *   <li>{@code autoFlushIntervalNanos} - Maximum age before auto-flush (default: 100ms)</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 * try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", 9000)) {
 *     for (int i = 0; i &lt; 100_000; i++) {
 *         sender.table("metrics")
 *               .symbol("host", "server-" + (i % 10))
 *               .doubleColumn("cpu", Math.random() * 100)
 *               .atNow();
 *         // Rows are batched and sent asynchronously!
 *     }
 *     // flush() waits for all pending batches to be sent
 *     sender.flush();
 * }
 * </pre>
 * <p>
 * <b>Fast-path API for high-throughput generators</b>
 * <p>
 * For maximum throughput, bypass the fluent API to avoid per-row overhead
 * (no column-name hashmap lookups, no {@code checkNotClosed()}/{@code checkTableSelected()}
 * per column, direct access to column buffers). Use {@link #getTableBuffer(String)},
 * {@link #getOrAddGlobalSymbol(String)}, and {@link #incrementPendingRowCount()}:
 * <pre>
 * // Setup (once)
 * QwpTableBuffer tableBuffer = sender.getTableBuffer("q");
 * QwpTableBuffer.ColumnBuffer colSymbol = tableBuffer.getOrCreateColumn("s", TYPE_SYMBOL, true);
 * QwpTableBuffer.ColumnBuffer colBid = tableBuffer.getOrCreateColumn("b", TYPE_DOUBLE, false);
 *
 * // Hot path (per row)
 * colSymbol.addSymbolWithGlobalId(symbol, sender.getOrAddGlobalSymbol(symbol));
 * colBid.addDouble(bid);
 * tableBuffer.nextRow();
 * sender.incrementPendingRowCount();
 * </pre>
 */
public class QwpWebSocketSender implements Sender {

    public static final int DEFAULT_AUTO_FLUSH_BYTES = 1024 * 1024; // 1MB
    public static final long DEFAULT_AUTO_FLUSH_INTERVAL_NANOS = 100_000_000L; // 100ms
    public static final int DEFAULT_AUTO_FLUSH_ROWS = 500;
    public static final int DEFAULT_IN_FLIGHT_WINDOW_SIZE = InFlightWindow.DEFAULT_WINDOW_SIZE; // 8
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int DEFAULT_MAX_NAME_LENGTH = 127;
    private static final int DEFAULT_MICROBATCH_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final Logger LOG = LoggerFactory.getLogger(QwpWebSocketSender.class);
    private static final String WRITE_PATH = "/write/v4";
    private final AckFrameHandler ackHandler = new AckFrameHandler(this);
    private final WebSocketResponse ackResponse = new WebSocketResponse();
    private final String authorizationHeader;
    private final int autoFlushBytes;
    private final long autoFlushIntervalNanos;
    // Auto-flush configuration
    private final int autoFlushRows;
    private final Decimal256 currentDecimal256 = new Decimal256();
    // Encoder for QWP v1 messages
    private final QwpWebSocketEncoder encoder;
    // Global symbol dictionary for delta encoding
    private final GlobalSymbolDictionary globalSymbolDictionary;
    private final String host;
    // Flow control configuration
    private final int inFlightWindowSize;
    private final int port;
    // Track schema hashes that have been sent to the server (for schema reference mode)
    // First time we send a schema: full schema. Subsequent times: 8-byte hash reference.
    // Combined key = schemaHash XOR (tableNameHash << 32) to include table name in lookup.
    private final LongHashSet sentSchemaHashes = new LongHashSet();
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    private final boolean tlsEnabled;
    private MicrobatchBuffer activeBuffer;
    // Double-buffering for async I/O
    private MicrobatchBuffer buffer0;
    private MicrobatchBuffer buffer1;
    // Cached column references to avoid repeated hashmap lookups
    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    // WebSocket client (zero-GC native implementation)
    private WebSocketClient client;
    private boolean closed;
    private boolean connected;
    // Track max global symbol ID used in current batch (for delta calculation)
    private int currentBatchMaxSymbolId = -1;
    private QwpTableBuffer currentTableBuffer;
    private String currentTableName;
    private long firstPendingRowTimeNanos;
    // Configuration
    private boolean gorillaEnabled = true;
    // Flow control
    private InFlightWindow inFlightWindow;
    // Track highest symbol ID sent to server (for delta encoding)
    // Once sent over TCP, server is guaranteed to receive it (or connection dies)
    private volatile int maxSentSymbolId = -1;
    // Batch sequence counter (must match server's messageSequence)
    private long nextBatchSequence = 0;
    // Async mode: pending row tracking
    private int pendingRowCount;
    private boolean sawBinaryAck;
    private WebSocketSendQueue sendQueue;

    private QwpWebSocketSender(
            String host,
            int port,
            boolean tlsEnabled,
            int bufferSize,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            String authorizationHeader
    ) {
        this.authorizationHeader = authorizationHeader;
        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.encoder = new QwpWebSocketEncoder(bufferSize);
        this.tableBuffers = new CharSequenceObjHashMap<>();
        this.currentTableBuffer = null;
        this.currentTableName = null;
        this.connected = false;
        this.closed = false;
        this.autoFlushRows = autoFlushRows;
        this.autoFlushBytes = autoFlushBytes;
        this.autoFlushIntervalNanos = autoFlushIntervalNanos;
        this.inFlightWindowSize = inFlightWindowSize;

        // Initialize global symbol dictionary for delta encoding
        this.globalSymbolDictionary = new GlobalSymbolDictionary();

        // Initialize double-buffering if async mode (window > 1)
        if (inFlightWindowSize > 1) {
            int microbatchBufferSize = Math.max(DEFAULT_MICROBATCH_BUFFER_SIZE, autoFlushBytes * 2);
            this.buffer0 = new MicrobatchBuffer(microbatchBufferSize, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos);
            this.buffer1 = new MicrobatchBuffer(microbatchBufferSize, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos);
            this.activeBuffer = buffer0;
        }
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     * Uses synchronous mode for backward compatibility.
     *
     * @param host server host
     * @param port server HTTP port (WebSocket upgrade happens on same port)
     * @return connected sender
     */
    public static QwpWebSocketSender connect(String host, int port) {
        return connect(host, port, false);
    }

    /**
     * Creates a new sender with TLS and connects to the specified host and port.
     * Uses synchronous mode with default auto-flush settings.
     *
     * @param host       server host
     * @param port       server HTTP port
     * @param tlsEnabled whether to use TLS
     * @return connected sender
     */
    public static QwpWebSocketSender connect(String host, int port, boolean tlsEnabled) {
        return connect(
                host, port, tlsEnabled, DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS
        );
    }

    /**
     * Creates a new sender with TLS and connects to the specified host and port.
     * Uses synchronous mode with custom auto-flush settings.
     *
     * @param host                   server host
     * @param port                   server HTTP port
     * @param tlsEnabled             whether to use TLS
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @return connected sender
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            boolean tlsEnabled,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos
    ) {
        return connect(host, port, tlsEnabled, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos, null);
    }

    public static QwpWebSocketSender connect(
            String host,
            int port,
            boolean tlsEnabled,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader
    ) {
        QwpWebSocketSender sender = new QwpWebSocketSender(
                host, port, tlsEnabled, DEFAULT_BUFFER_SIZE, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                1,   // window=1 for sync behavior
                authorizationHeader
        );
        sender.ensureConnected();
        return sender;
    }

    /**
     * Creates a new sender with async mode and custom configuration.
     *
     * @param host                   server host
     * @param port                   server HTTP port
     * @param tlsEnabled             whether to use TLS
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @return connected sender
     */
    public static QwpWebSocketSender connectAsync(
            String host,
            int port,
            boolean tlsEnabled,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos
    ) {
        return connectAsync(
                host, port, tlsEnabled, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos, DEFAULT_IN_FLIGHT_WINDOW_SIZE
        );
    }

    /**
     * Creates a new sender with async mode and full configuration including flow control.
     *
     * @param host                   server host
     * @param port                   server HTTP port
     * @param tlsEnabled             whether to use TLS
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @param inFlightWindowSize     max batches awaiting server ACK (default: 8)
     * @return connected sender
     */
    public static QwpWebSocketSender connectAsync(
            String host,
            int port,
            boolean tlsEnabled,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize
    ) {
        return connectAsync(
                host, port, tlsEnabled, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos, inFlightWindowSize, null
        );
    }

    public static QwpWebSocketSender connectAsync(
            String host,
            int port,
            boolean tlsEnabled,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            String authorizationHeader
    ) {
        QwpWebSocketSender sender = new QwpWebSocketSender(
                host, port, tlsEnabled, DEFAULT_BUFFER_SIZE, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos, inFlightWindowSize, authorizationHeader
        );
        sender.ensureConnected();
        return sender;
    }

    /**
     * Creates a new sender with async mode and default configuration.
     *
     * @param host       server host
     * @param port       server HTTP port
     * @param tlsEnabled whether to use TLS
     * @return connected sender
     */
    public static QwpWebSocketSender connectAsync(String host, int port, boolean tlsEnabled) {
        return connectAsync(
                host, port, tlsEnabled, DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS
        );
    }

    /**
     * Creates a sender without connecting. For testing only.
     * <p>
     * This allows unit tests to test sender logic without requiring a real server.
     * Uses default auto-flush settings.
     *
     * @param host               server host (not connected)
     * @param port               server port (not connected)
     * @param inFlightWindowSize window size: 1 for sync behavior, >1 for async
     * @return unconnected sender
     */
    public static QwpWebSocketSender createForTesting(String host, int port, int inFlightWindowSize) {
        return new QwpWebSocketSender(
                host, port, false, DEFAULT_BUFFER_SIZE, DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS, inFlightWindowSize, null
        );
        // Note: does NOT call ensureConnected()
    }

    /**
     * Creates a sender with custom flow control settings without connecting. For testing only.
     *
     * @param host                   server host (not connected)
     * @param port                   server port (not connected)
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @param inFlightWindowSize     window size: 1 for sync behavior, >1 for async
     * @return unconnected sender
     */
    public static QwpWebSocketSender createForTesting(
            String host,
            int port,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize
    ) {
        return new QwpWebSocketSender(
                host, port, false, DEFAULT_BUFFER_SIZE, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos, inFlightWindowSize, null
        );
        // Note: does NOT call ensureConnected()
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
        // Server-assigned timestamp - just send the row without designated timestamp
        sendRow();
    }

    @Override
    public QwpWebSocketSender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_BOOLEAN, false);
        col.addBoolean(value);
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        throw new LineSenderException("bufferView() is not supported for WebSocket sender");
    }

    /**
     * Adds a BYTE column value to the current row.
     *
     * @param columnName the column name
     * @param value      the byte value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender byteColumn(CharSequence columnName, byte value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_BYTE, false);
        col.addByte(value);
        return this;
    }

    @Override
    public void cancelRow() {
        checkNotClosed();
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
        }
    }

    /**
     * Adds a CHAR column value to the current row.
     * <p>
     * CHAR is stored as a 2-byte UTF-16 code unit in QuestDB.
     *
     * @param columnName the column name
     * @param value      the character value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender charColumn(CharSequence columnName, char value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_CHAR, false);
        col.addShort((short) value);
        return this;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;

            // Flush any remaining data
            try {
                if (inFlightWindowSize > 1) {
                    // Async mode (window > 1): flush accumulated rows in table buffers first
                    flushPendingRows();

                    if (activeBuffer != null && activeBuffer.hasData()) {
                        sealAndSwapBuffer();
                    }
                    // Wait for all batches to be sent and acknowledged before closing
                    if (sendQueue != null) {
                        sendQueue.flush();
                    }
                    if (inFlightWindow != null) {
                        inFlightWindow.awaitEmpty();
                    }
                } else {
                    // Sync mode (window=1): flush pending rows synchronously
                    if (pendingRowCount > 0 && client != null && client.isConnected()) {
                        flushSync();
                    }
                }
            } catch (Exception e) {
                LOG.error("Error during close: {}", String.valueOf(e));
            }

            // Shut down the I/O thread before closing the socket or buffers
            // it may be using. This must run even if the flush above failed.
            if (sendQueue != null) {
                try {
                    sendQueue.close();
                } catch (Exception e) {
                    LOG.error("Error closing send queue: {}", String.valueOf(e));
                }
            }

            // Close buffers (async mode only, window > 1)
            if (buffer0 != null) {
                buffer0.close();
            }
            if (buffer1 != null) {
                buffer1.close();
            }

            if (client != null) {
                client.close();
                client = null;
            }
            encoder.close();
            // Close all table buffers to free off-heap column memory
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

            LOG.info("QwpWebSocketSender closed");
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DECIMAL64, true);
        col.addDecimal64(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DECIMAL128, true);
        col.addDecimal128(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DECIMAL256, true);
        col.addDecimal256(value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        if (value == null || value.isEmpty()) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            currentDecimal256.ofString(value);
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DECIMAL256, true);
            col.addDecimal256(currentDecimal256);
        } catch (Exception e) {
            throw new LineSenderException("Failed to parse decimal value: " + value, e);
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_DOUBLE_ARRAY, true);
        col.addDoubleArray(array);
        return this;
    }

    @Override
    public QwpWebSocketSender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_DOUBLE, false);
        col.addDouble(value);
        return this;
    }

    /**
     * Adds a FLOAT column value to the current row.
     *
     * @param columnName the column name
     * @param value      the float value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender floatColumn(CharSequence columnName, float value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_FLOAT, false);
        col.addFloat(value);
        return this;
    }

    @Override
    public void flush() {
        checkNotClosed();
        ensureConnected();

        if (inFlightWindowSize > 1) {
            // Async mode (window > 1): flush pending rows and wait for ACKs
            flushPendingRows();

            // Flush any remaining data in the active microbatch buffer
            if (activeBuffer.hasData()) {
                sealAndSwapBuffer();
            }

            // Wait for all pending batches to be sent to the server
            sendQueue.flush();

            // Wait for all in-flight batches to be acknowledged by the server
            inFlightWindow.awaitEmpty();

            LOG.debug("Flush complete [totalBatches={}, totalBytes={}, totalAcked={}]", sendQueue.getTotalBatchesSent(), sendQueue.getTotalBytesSent(), inFlightWindow.getTotalAcked());
        } else {
            // Sync mode (window=1): flush pending rows and wait for ACKs synchronously
            flushSync();
        }
    }

    /**
     * Returns the auto-flush byte threshold.
     */
    public int getAutoFlushBytes() {
        return autoFlushBytes;
    }

    /**
     * Returns the auto-flush interval in nanoseconds.
     */
    public long getAutoFlushIntervalNanos() {
        return autoFlushIntervalNanos;
    }

    /**
     * Returns the auto-flush row threshold.
     */
    public int getAutoFlushRows() {
        return autoFlushRows;
    }

    /**
     * Returns the max symbol ID sent to the server.
     * Once sent over TCP, server is guaranteed to receive it (or connection dies).
     */
    public int getMaxSentSymbolId() {
        return maxSentSymbolId;
    }

    /**
     * Returns the number of pending rows not yet flushed.
     * For testing.
     */
    public int getPendingRowCount() {
        return pendingRowCount;
    }

    /**
     * Gets or creates a table buffer for direct access.
     * For high-throughput generators that want to bypass fluent API overhead.
     */
    public QwpTableBuffer getTableBuffer(String tableName) {
        QwpTableBuffer buffer = tableBuffers.get(tableName);
        if (buffer == null) {
            buffer = new QwpTableBuffer(tableName);
            tableBuffers.put(tableName, buffer);
        }
        currentTableBuffer = buffer;
        currentTableName = tableName;
        return buffer;
    }


    /**
     * Adds an INT column value to the current row.
     *
     * @param columnName the column name
     * @param value      the int value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender intColumn(CharSequence columnName, int value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_INT, false);
        col.addInt(value);
        return this;
    }

    /**
     * Returns whether Gorilla encoding is enabled.
     */
    public boolean isGorillaEnabled() {
        return gorillaEnabled;
    }

    /**
     * Adds a LONG256 column value to the current row.
     *
     * @param columnName the column name
     * @param l0         the least significant 64 bits
     * @param l1         the second 64 bits
     * @param l2         the third 64 bits
     * @param l3         the most significant 64 bits
     * @return this sender for method chaining
     */
    public QwpWebSocketSender long256Column(CharSequence columnName, long l0, long l1, long l2, long l3) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_LONG256, true);
        col.addLong256(l0, l1, l2, l3);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_LONG_ARRAY, true);
        col.addLongArray(values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(name), TYPE_LONG_ARRAY, true);
        col.addLongArray(array);
        return this;
    }

    @Override
    public QwpWebSocketSender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_LONG, false);
        col.addLong(value);
        return this;
    }

    @Override
    public void reset() {
        checkNotClosed();
        // Reset ALL table buffers, not just the current one
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            QwpTableBuffer buf = tableBuffers.get(keys.getQuick(i));
            if (buf != null) {
                buf.reset();
            }
        }
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;
        currentTableBuffer = null;
        currentTableName = null;
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
    }

    /**
     * Sets whether to use Gorilla timestamp encoding.
     */
    public void setGorillaEnabled(boolean enabled) {
        this.gorillaEnabled = enabled;
        this.encoder.setGorillaEnabled(enabled);
    }

    /**
     * Adds a SHORT column value to the current row.
     *
     * @param columnName the column name
     * @param value      the short value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender shortColumn(CharSequence columnName, short value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_SHORT, false);
        col.addShort(value);
        return this;
    }

    @Override
    public QwpWebSocketSender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_STRING, true);
        col.addString(value != null ? value.toString() : null);
        return this;
    }

    @Override
    public QwpWebSocketSender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_SYMBOL, true);

        if (value != null) {
            // Register symbol in global dictionary and track max ID for delta calculation
            String symbolValue = value.toString();
            int globalId = globalSymbolDictionary.getOrAddSymbol(symbolValue);
            if (globalId > currentBatchMaxSymbolId) {
                currentBatchMaxSymbolId = globalId;
            }
            // Store global ID in the column buffer
            col.addSymbolWithGlobalId(symbolValue, globalId);
        } else {
            col.addSymbol(null);
        }
        return this;
    }

    @Override
    public QwpWebSocketSender table(CharSequence tableName) {
        checkNotClosed();
        validateTableName(tableName);
        // Fast path: if table name matches current, skip hashmap lookup
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        // Table changed - invalidate cached column references
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        currentTableName = tableName.toString();
        currentTableBuffer = tableBuffers.get(currentTableName);
        if (currentTableBuffer == null) {
            currentTableBuffer = new QwpTableBuffer(currentTableName);
            tableBuffers.put(currentTableName, currentTableBuffer);
        }
        // Both modes accumulate rows until flush
        return this;
    }

    @Override
    public QwpWebSocketSender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        if (unit == ChronoUnit.NANOS) {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_TIMESTAMP_NANOS, true);
            col.addLong(value);
        } else {
            long micros = toMicros(value, unit);
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_TIMESTAMP, true);
            col.addLong(micros);
        }
        return this;
    }

    @Override
    public QwpWebSocketSender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_TIMESTAMP, true);
        col.addLong(micros);
        return this;
    }

    /**
     * Adds a UUID column value to the current row.
     *
     * @param columnName the column name
     * @param lo         the low 64 bits of the UUID
     * @param hi         the high 64 bits of the UUID
     * @return this sender for method chaining
     */
    public QwpWebSocketSender uuidColumn(CharSequence columnName, long lo, long hi) {
        checkNotClosed();
        checkTableSelected();
        QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(checkedColumnName(columnName), TYPE_UUID, true);
        col.addUuid(hi, lo);
        return this;
    }

    private void atMicros(long timestampMicros) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampColumn == null) {
            cachedTimestampColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP, true);
        }
        cachedTimestampColumn.addLong(timestampMicros);
        sendRow();
    }

    private void atNanos(long timestampNanos) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampNanosColumn == null) {
            cachedTimestampNanosColumn = currentTableBuffer.getOrCreateColumn("", TYPE_TIMESTAMP_NANOS, true);
        }
        cachedTimestampNanosColumn.addLong(timestampNanos);
        sendRow();
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

    private String checkedColumnName(CharSequence name) {
        if (name == null || !TableUtils.isValidColumnName(name, DEFAULT_MAX_NAME_LENGTH)) {
            if (name == null || name.length() == 0) {
                throw new LineSenderException("column name cannot be empty");
            }
            if (name.length() > DEFAULT_MAX_NAME_LENGTH) {
                throw new LineSenderException("column name too long [maxLength=" + DEFAULT_MAX_NAME_LENGTH + "]");
            }
            throw new LineSenderException("column name contains illegal characters: " + name);
        }
        return name.toString();
    }

    /**
     * Ensures the active buffer is ready for writing (in FILLING state).
     * If the buffer is in RECYCLED state, resets it. If it's in use, waits for it.
     */
    private void ensureActiveBufferReady() {
        if (activeBuffer.isFilling()) {
            return; // Already ready
        }

        if (activeBuffer.isRecycled()) {
            // Buffer was recycled but not reset - reset it now
            activeBuffer.reset();
            return;
        }

        // Buffer is in use (SEALED or SENDING) - wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            LOG.debug("Waiting for active buffer [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for active buffer to be recycled");
            }
        }

        // Buffer should now be RECYCLED - reset it
        if (activeBuffer.isRecycled()) {
            activeBuffer.reset();
        }
    }

    private void ensureConnected() {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
        if (!connected) {
            // Create WebSocket client using factory (zero-GC native implementation)
            if (tlsEnabled) {
                client = WebSocketClientFactory.newInsecureTlsInstance();
            } else {
                client = WebSocketClientFactory.newPlainTextInstance();
            }

            // Connect and upgrade to WebSocket
            try {
                client.connect(host, port);
                client.upgrade(WRITE_PATH, authorizationHeader);
            } catch (Exception e) {
                client.close();
                client = null;
                throw new LineSenderException("Failed to connect to " + host + ":" + port, e);
            }

            // a window for tracking batches awaiting ACK (both modes)
            inFlightWindow = new InFlightWindow(inFlightWindowSize, InFlightWindow.DEFAULT_TIMEOUT_MS);

            // Initialize send queue for async mode (window > 1)
            // The send queue handles both sending AND receiving (single I/O thread)
            if (inFlightWindowSize > 1) {
                sendQueue = new WebSocketSendQueue(client, inFlightWindow,
                        WebSocketSendQueue.DEFAULT_ENQUEUE_TIMEOUT_MS,
                        WebSocketSendQueue.DEFAULT_SHUTDOWN_TIMEOUT_MS);
            }
            // Sync mode (window=1): no send queue - we send and read ACKs synchronously

            // Clear sent schema hashes - server starts fresh on each connection
            sentSchemaHashes.clear();

            connected = true;
            LOG.info("Connected to WebSocket [host={}, port={}, windowSize={}]", host, port, inFlightWindowSize);
        }
    }

    private void failExpectedIfNeeded(long expectedSequence, LineSenderException error) {
        if (inFlightWindow != null && inFlightWindow.getLastError() == null) {
            inFlightWindow.fail(expectedSequence, error);
        }
    }

    /**
     * Flushes pending rows by encoding and sending them.
     * Each table's rows are encoded into a separate QWP v1 message and sent as one WebSocket frame.
     */
    private void flushPendingRows() {
        if (pendingRowCount <= 0) {
            return;
        }

        // Invalidate cached column references — table buffers will be reset below
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;

        LOG.debug("Flushing pending rows [count={}, tables={}]", pendingRowCount, tableBuffers.size());

        // Ensure activeBuffer is ready for writing
        // It might be in RECYCLED state if previous batch was sent but we didn't swap yet
        ensureActiveBufferReady();

        // Encode all table buffers that have data
        // Iterate over the keys list directly
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue; // Skip null entries (shouldn't happen but be safe)
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null) {
                continue;
            }
            int rowCount = tableBuffer.getRowCount();
            if (rowCount > 0) {
                // Check if this schema has been sent before (use schema reference mode if so)
                // Combined key includes table name since server caches by (tableName, schemaHash)
                long schemaHash = tableBuffer.getSchemaHash();
                long schemaKey = schemaHash ^ ((long) tableBuffer.getTableName().hashCode() << 32);
                boolean useSchemaRef = sentSchemaHashes.contains(schemaKey);

                LOG.debug("Encoding table [name={}, rows={}, maxSentSymbolId={}, batchMaxId={}, useSchemaRef={}]", tableName, rowCount, maxSentSymbolId, currentBatchMaxSymbolId, useSchemaRef);

                // Encode this table's rows with delta symbol dictionary
                int messageSize = encoder.encodeWithDeltaDict(
                        tableBuffer,
                        globalSymbolDictionary,
                        maxSentSymbolId,
                        currentBatchMaxSymbolId,
                        useSchemaRef
                );

                QwpBufferWriter buffer = encoder.getBuffer();

                // Copy to microbatch buffer and seal immediately
                // Each QWP v1 message must be in its own WebSocket frame
                activeBuffer.ensureCapacity(messageSize);
                activeBuffer.write(buffer.getBufferPtr(), messageSize);
                activeBuffer.incrementRowCount();
                activeBuffer.setMaxSymbolId(currentBatchMaxSymbolId);

                // Seal and enqueue for sending
                sealAndSwapBuffer();

                // Update sent state only after successful enqueue.
                // If sealAndSwapBuffer() threw, these remain unchanged so the
                // next batch's delta dictionary will correctly re-include the
                // symbols and schema that the server never received.
                maxSentSymbolId = currentBatchMaxSymbolId;
                if (!useSchemaRef) {
                    sentSchemaHashes.add(schemaKey);
                }

                // Reset table buffer and batch-level symbol tracking
                tableBuffer.reset();
                currentBatchMaxSymbolId = -1;
            }
        }

        // Reset pending count
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;
    }

    /**
     * Flushes pending rows synchronously, blocking until server ACKs.
     * Used in sync mode for simpler, blocking operation.
     */
    private void flushSync() {
        if (pendingRowCount <= 0) {
            return;
        }

        // Invalidate cached column references — table buffers will be reset below
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;

        LOG.debug("Sync flush [pendingRows={}, tables={}]", pendingRowCount, tableBuffers.size());

        // Encode all table buffers that have data into a single message
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

            // Check if this schema has been sent before (use schema reference mode if so)
            // Combined key includes table name since server caches by (tableName, schemaHash)
            long schemaHash = tableBuffer.getSchemaHash();
            long schemaKey = schemaHash ^ ((long) tableBuffer.getTableName().hashCode() << 32);
            boolean useSchemaRef = sentSchemaHashes.contains(schemaKey);

            // Encode this table's rows with delta symbol dictionary
            int messageSize = encoder.encodeWithDeltaDict(
                    tableBuffer,
                    globalSymbolDictionary,
                    maxSentSymbolId,
                    currentBatchMaxSymbolId,
                    useSchemaRef
            );

            if (messageSize > 0) {
                QwpBufferWriter buffer = encoder.getBuffer();

                // Track batch in InFlightWindow before sending
                long batchSequence = nextBatchSequence++;
                inFlightWindow.addInFlight(batchSequence);

                LOG.debug("Sending sync batch [seq={}, bytes={}, rows={}, maxSentSymbolId={}, useSchemaRef={}]", batchSequence, messageSize, tableBuffer.getRowCount(), currentBatchMaxSymbolId, useSchemaRef);

                // Send over WebSocket
                client.sendBinary(buffer.getBufferPtr(), messageSize);

                // Wait for ACK synchronously
                waitForAck(batchSequence);

                // Update sent state only after successful send + ACK.
                // If sendBinary() or waitForAck() threw, these remain unchanged
                // so the next batch's delta dictionary will correctly re-include
                // the symbols and schema that the server never received.
                maxSentSymbolId = currentBatchMaxSymbolId;
                if (!useSchemaRef) {
                    sentSchemaHashes.add(schemaKey);
                }
            }

            // Reset table buffer after sending
            tableBuffer.reset();

            // Reset batch-level symbol tracking
            currentBatchMaxSymbolId = -1;
        }

        // Reset pending row tracking
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;

        LOG.debug("Sync flush complete [totalAcked={}]", inFlightWindow.getTotalAcked());
    }

    private long getPendingBytes() {
        long bytes = 0;
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence key = keys.getQuick(i);
            if (key != null) {
                QwpTableBuffer tb = tableBuffers.get(key);
                if (tb != null) {
                    bytes += tb.getBufferedBytes();
                }
            }
        }
        return bytes;
    }

    /**
     * Seals the current buffer and swaps to the other buffer.
     * Enqueues the sealed buffer for async sending.
     */
    private void sealAndSwapBuffer() {
        if (!activeBuffer.hasData()) {
            return; // Nothing to send
        }

        MicrobatchBuffer toSend = activeBuffer;
        toSend.seal();

        LOG.debug("Sealing buffer [id={}, rows={}, bytes={}]", toSend.getBatchId(), toSend.getRowCount(), toSend.getBufferPos());

        // Swap to the other buffer
        activeBuffer = (activeBuffer == buffer0) ? buffer1 : buffer0;

        // If the other buffer is still being sent, wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            LOG.debug("Waiting for buffer recycle [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for buffer to be recycled");
            }
            LOG.debug("Buffer recycled [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
        }

        // Reset the new active buffer
        int stateBeforeReset = activeBuffer.getState();
        LOG.debug("Resetting buffer [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(stateBeforeReset));
        activeBuffer.reset();

        // Enqueue the sealed buffer for sending.
        // If enqueue fails, roll back local state so the same batch can be retried.
        try {
            if (!sendQueue.enqueue(toSend)) {
                throw new LineSenderException("Failed to enqueue buffer for sending");
            }
        } catch (LineSenderException e) {
            activeBuffer = toSend;
            if (toSend.isSealed()) {
                toSend.rollbackSealForRetry();
            }
            throw e;
        }
    }

    /**
     * Accumulates the current row.
     * Both sync and async modes buffer rows until flush (explicit or auto-flush).
     * The difference is that sync mode flush() blocks until server ACKs.
     */
    private void sendRow() {
        ensureConnected();
        currentTableBuffer.nextRow();

        // Both modes: accumulate rows, don't encode yet
        if (pendingRowCount == 0) {
            firstPendingRowTimeNanos = System.nanoTime();
        }
        pendingRowCount++;

        // Check if any flush threshold is exceeded
        if (shouldAutoFlush()) {
            if (inFlightWindowSize > 1) {
                flushPendingRows();
            } else {
                // Sync mode (window=1): flush directly with ACK wait
                flushSync();
            }
        }
    }

    /**
     * Checks if any auto-flush threshold is exceeded.
     */
    private boolean shouldAutoFlush() {
        if (pendingRowCount <= 0) {
            return false;
        }
        if (autoFlushRows > 0 && pendingRowCount >= autoFlushRows) {
            return true;
        }
        if (autoFlushBytes > 0 && getPendingBytes() >= autoFlushBytes) {
            return true;
        }
        if (autoFlushIntervalNanos > 0) {
            long ageNanos = System.nanoTime() - firstPendingRowTimeNanos;
            return ageNanos >= autoFlushIntervalNanos;
        }
        return false;
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

    private void validateTableName(CharSequence name) {
        if (name == null || !TableUtils.isValidTableName(name, DEFAULT_MAX_NAME_LENGTH)) {
            if (name == null || name.length() == 0) {
                throw new LineSenderException("table name cannot be empty");
            }
            if (name.length() > DEFAULT_MAX_NAME_LENGTH) {
                throw new LineSenderException("table name too long [maxLength=" + DEFAULT_MAX_NAME_LENGTH + "]");
            }
            throw new LineSenderException("table name contains illegal characters: " + name);
        }
    }

    /**
     * Waits synchronously for an ACK from the server for the specified batch.
     */
    private void waitForAck(long expectedSequence) {
        long deadline = System.currentTimeMillis() + InFlightWindow.DEFAULT_TIMEOUT_MS;

        while (System.currentTimeMillis() < deadline) {
            try {
                sawBinaryAck = false;
                boolean received = client.receiveFrame(ackHandler, 1000); // 1 second timeout per read attempt

                if (received) {
                    // Non-binary frames (e.g. ping/pong/text) are not ACKs.
                    if (!sawBinaryAck) {
                        continue;
                    }
                    long sequence = ackResponse.getSequence();
                    if (ackResponse.isSuccess()) {
                        // Cumulative ACK - acknowledge all batches up to this sequence
                        inFlightWindow.acknowledgeUpTo(sequence);
                        if (sequence >= expectedSequence) {
                            return; // Our batch was acknowledged (cumulative)
                        }
                        // Got ACK for lower sequence - continue waiting
                    } else {
                        String errorMessage = ackResponse.getErrorMessage();
                        LineSenderException error = new LineSenderException(
                                "Server error for batch " + sequence + ": " +
                                        ackResponse.getStatusName() + " - " + errorMessage);
                        inFlightWindow.fail(sequence, error);
                        if (sequence == expectedSequence) {
                            throw error;
                        }
                    }
                }
            } catch (LineSenderException e) {
                failExpectedIfNeeded(expectedSequence, e);
                throw e;
            } catch (Exception e) {
                LineSenderException wrapped = new LineSenderException("Error waiting for ACK: " + e.getMessage(), e);
                failExpectedIfNeeded(expectedSequence, wrapped);
                throw wrapped;
            }
        }

        LineSenderException timeout = new LineSenderException("Timeout waiting for ACK for batch " + expectedSequence);
        failExpectedIfNeeded(expectedSequence, timeout);
        throw timeout;
    }

    private record AckFrameHandler(
            QwpWebSocketSender sender
    ) implements WebSocketFrameHandler {

        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            sender.sawBinaryAck = true;
            if (!WebSocketResponse.isStructurallyValid(payloadPtr, payloadLen)) {
                throw new LineSenderException(
                        "Invalid ACK response payload [length=" + payloadLen + ']'
                );
            }
            if (!sender.ackResponse.readFrom(payloadPtr, payloadLen)) {
                throw new LineSenderException("Failed to parse ACK response");
            }
        }

        @Override
        public void onClose(int code, String reason) {
            throw new LineSenderException("WebSocket closed while waiting for ACK: " + reason);
        }
    }
}
