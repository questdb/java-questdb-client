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

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.Sender;
import io.questdb.client.cairo.TableUtils;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Misc;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

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
 *   <li>{@code autoFlushRows} - Maximum rows per batch (default: 1000)</li>
 *   <li>{@code autoFlushBytes} - Maximum bytes per batch (default: disabled)</li>
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
 */
public class QwpWebSocketSender implements Sender {

    public static final int DEFAULT_AUTO_FLUSH_BYTES = 0;
    public static final long DEFAULT_AUTO_FLUSH_INTERVAL_NANOS = 100_000_000L; // 100ms
    public static final int DEFAULT_AUTO_FLUSH_ROWS = 1_000;
    public static final int DEFAULT_IN_FLIGHT_WINDOW_SIZE = 128;
    public static final int DEFAULT_MAX_SCHEMAS_PER_CONNECTION = 65_535;
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int DEFAULT_MICROBATCH_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final Logger LOG = LoggerFactory.getLogger(QwpWebSocketSender.class);
    private static final int MAX_TABLE_NAME_LENGTH = 127;
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
    private final int maxSchemasPerConnection;
    private final int port;
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    // null means plain text (no TLS)
    private final ClientTlsConfiguration tlsConfig;
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
    private int maxSentSchemaId = -1;
    // Track the highest symbol ID sent to server (for delta encoding)
    // Once sent over TCP, server is guaranteed to receive it (or connection dies)
    private int maxSentSymbolId = -1;
    // Batch sequence counter (must match server's messageSequence)
    private long nextBatchSequence = 0;
    private int nextSchemaId;
    // Async mode: pending row tracking
    private long pendingBytes;
    private int pendingRowCount;
    // Highest client sequence durably uploaded, tracked when durable-ack opt-in is enabled
    // and the server emits STATUS_DURABLE_ACK frames.
    private long highestDurableSequence = -1;
    // Opt-in: request server-side STATUS_DURABLE_ACK frames after WAL reaches object store.
    // Must be set before the first send; has no effect once the WebSocket upgrade has completed.
    private boolean requestDurableAck;
    private boolean sawBinaryAck;
    private boolean sawPong;
    private WebSocketSendQueue sendQueue;

    private QwpWebSocketSender(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            String authorizationHeader,
            int maxSchemasPerConnection
    ) {
        this.authorizationHeader = authorizationHeader;
        this.host = host;
        this.port = port;
        this.tlsConfig = tlsConfig;
        this.encoder = new QwpWebSocketEncoder(DEFAULT_BUFFER_SIZE);
        this.tableBuffers = new CharSequenceObjHashMap<>();
        this.currentTableBuffer = null;
        this.currentTableName = null;
        this.connected = false;
        this.closed = false;
        this.autoFlushRows = autoFlushRows;
        this.autoFlushBytes = autoFlushBytes;
        this.autoFlushIntervalNanos = autoFlushIntervalNanos;
        this.inFlightWindowSize = inFlightWindowSize;
        this.maxSchemasPerConnection = maxSchemasPerConnection;

        // Initialize global symbol dictionary for delta encoding
        this.globalSymbolDictionary = new GlobalSymbolDictionary();

        // Initialize double-buffering if async mode (window > 1)
        if (inFlightWindowSize > 1) {
            int microbatchBufferSize = Math.max(DEFAULT_MICROBATCH_BUFFER_SIZE, autoFlushBytes * 2);
            try {
                this.buffer0 = new MicrobatchBuffer(microbatchBufferSize);
                this.buffer1 = new MicrobatchBuffer(microbatchBufferSize);
            } catch (Throwable t) {
                if (buffer0 != null) {
                    buffer0.close();
                }
                encoder.close();
                throw t;
            }
            this.activeBuffer = buffer0;
        }
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     * Uses default auto-flush settings and in-flight window size.
     *
     * @param host server host
     * @param port server HTTP port (WebSocket upgrade happens on same port)
     * @return connected sender
     */
    public static QwpWebSocketSender connect(String host, int port) {
        return connect(host, port, null);
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     * Uses default auto-flush settings and in-flight window size.
     *
     * @param host      server host
     * @param port      server HTTP port
     * @param tlsConfig TLS configuration, or null for plain text
     * @return connected sender
     */
    public static QwpWebSocketSender connect(String host, int port, ClientTlsConfiguration tlsConfig) {
        return connect(
                host, port, tlsConfig,
                DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                DEFAULT_IN_FLIGHT_WINDOW_SIZE, null, DEFAULT_MAX_SCHEMAS_PER_CONNECTION
        );
    }

    /**
     * Creates a new sender with full configuration and connects.
     * <p>
     * In-flight window size controls the flow behavior: 1 means synchronous (each batch
     * waits for ACK), greater than 1 enables asynchronous pipelining with a background I/O thread.
     *
     * @param host                   server host
     * @param port                   server HTTP port
     * @param tlsConfig              TLS configuration, or null for plain text
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @param inFlightWindowSize     max batches awaiting server ACK (1 = sync, default: 128)
     * @param authorizationHeader    HTTP Authorization header value, or null
     * @return connected sender
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            String authorizationHeader
    ) {
        return connect(
                host,
                port,
                tlsConfig,
                autoFlushRows,
                autoFlushBytes,
                autoFlushIntervalNanos,
                inFlightWindowSize,
                authorizationHeader,
                DEFAULT_MAX_SCHEMAS_PER_CONNECTION
        );
    }

    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            String authorizationHeader,
            int maxSchemasPerConnection
    ) {
        QwpWebSocketSender sender = new QwpWebSocketSender(
                host, port, tlsConfig,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, authorizationHeader, maxSchemasPerConnection
        );
        try {
            sender.ensureConnected();
        } catch (Throwable t) {
            sender.close();
            throw t;
        }
        return sender;
    }

    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            String authorizationHeader,
            int maxSchemasPerConnection,
            boolean requestDurableAck
    ) {
        QwpWebSocketSender sender = new QwpWebSocketSender(
                host, port, tlsConfig,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, authorizationHeader, maxSchemasPerConnection
        );
        try {
            sender.setRequestDurableAck(requestDurableAck);
            sender.ensureConnected();
        } catch (Throwable t) {
            sender.close();
            throw t;
        }
        return sender;
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
        return createForTesting(host, port, inFlightWindowSize, null);
    }

    public static QwpWebSocketSender createForTesting(String host, int port, int inFlightWindowSize, String authorizationHeader) {
        return new QwpWebSocketSender(
                host, port, null,
                DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                inFlightWindowSize, authorizationHeader, DEFAULT_MAX_SCHEMAS_PER_CONNECTION
        );
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
        return createForTesting(
                host,
                port,
                autoFlushRows,
                autoFlushBytes,
                autoFlushIntervalNanos,
                inFlightWindowSize,
                DEFAULT_MAX_SCHEMAS_PER_CONNECTION
        );
    }

    public static QwpWebSocketSender createForTesting(
            String host,
            int port,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            int inFlightWindowSize,
            int maxSchemasPerConnection
    ) {
        return new QwpWebSocketSender(
                host, port, null,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                inFlightWindowSize, null, maxSchemasPerConnection
        );
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        try {
            if (unit == ChronoUnit.NANOS) {
                atNanos(timestamp);
            } else {
                long micros = toMicros(timestamp, unit);
                atMicros(micros);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
    }

    @Override
    public void at(Instant timestamp) {
        checkNotClosed();
        checkTableSelected();
        try {
            long micros = timestamp.getEpochSecond() * 1_000_000L + timestamp.getNano() / 1000L;
            atMicros(micros);
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
    }

    @Override
    public void atNow() {
        checkNotClosed();
        checkTableSelected();
        try {
            // Server-assigned timestamp - just send the row without designated timestamp
            sendRow();
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
    }

    @Override
    public QwpWebSocketSender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_BOOLEAN, false);
            if (col != null) {
                col.addBoolean(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_BYTE, false);
            if (col != null) {
                col.addByte(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public void cancelRow() {
        checkNotClosed();
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_CHAR, false);
            if (col != null) {
                col.addShort((short) value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            boolean ioThreadStopped = true;

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
                        sendQueue.awaitPendingAcks();
                    } else if (inFlightWindow != null) {
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
                    ioThreadStopped = false;
                    LOG.error("Error closing send queue: {}", String.valueOf(e));
                }
            }

            // Always free resources the I/O thread never touches:
            // encoder and table buffers are user-thread-only.
            encoder.close();
            ObjList<CharSequence> keys = tableBuffers.keys();
            for (int i = 0, n = keys.size(); i < n; i++) {
                CharSequence key = keys.getQuick(i);
                if (key != null) {
                    Misc.free(tableBuffers.get(key));
                }
            }
            tableBuffers.clear();

            if (!ioThreadStopped) {
                // The I/O thread may still be using the socket and microbatch
                // buffers (buffer0/buffer1). Freeing them would risk SIGSEGV.
                LOG.error("I/O thread is still running, leaking WebSocket client and microbatch buffers");
                return;
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

            LOG.info("QwpWebSocketSender closed");
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL64, true);
            if (col != null) {
                col.addDecimal64(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL128, true);
            if (col != null) {
                col.addDecimal128(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        if (value == null || value.isNull()) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL256, true);
            if (col != null) {
                col.addDecimal256(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        if (value == null || value.length() == 0) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            currentDecimal256.ofString(value);
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL256, true);
            if (col != null) {
                col.addDecimal256(currentDecimal256);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(array);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_DOUBLE, true);
            if (col != null) {
                col.addDouble(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_FLOAT, true);
            if (col != null) {
                col.addFloat(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public void flush() {
        checkNotClosed();
        ensureNoInProgressRow();
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
            sendQueue.awaitPendingAcks();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Flush complete [totalBatches={}, totalBytes={}, totalAcked={}]", sendQueue.getTotalBatchesSent(), sendQueue.getTotalBytesSent(), inFlightWindow.getTotalAcked());
            }
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
     * Returns the highest client batch sequence acknowledged by the server
     * (committed to WAL), or -1 if no ACK has been received yet.
     */
    public long getHighestAckedSequence() {
        return inFlightWindow != null ? inFlightWindow.getHighestAckedSequence() : -1;
    }

    /**
     * Returns the highest client batch sequence that the server has reported
     * as durably persisted to the object store via a STATUS_DURABLE_ACK frame,
     * or -1 if no durable ACK has been observed yet on this connection.
     * <p>
     * Only meaningful when the connection was opened with
     * {@link #setRequestDurableAck(boolean)} = true on a server where primary
     * replication is enabled; otherwise remains -1.
     */
    public long getHighestDurableSequence() {
        return sendQueue != null ? sendQueue.getHighestDurableSequence() : highestDurableSequence;
    }

    /**
     * Returns the max symbol ID sent to the server.
     * Once sent over TCP, server is guaranteed to receive it (or connection dies).
     */
    public int getMaxSentSymbolId() {
        return maxSentSymbolId;
    }

    /**
     * Registers a symbol value in the global dictionary and returns its global ID.
     * Called from {@link QwpTableBuffer.ColumnBuffer#addSymbol(CharSequence)}.
     *
     * @param symbol the symbol value to register
     * @return the global symbol ID
     */
    public int getOrAddGlobalSymbol(CharSequence symbol) {
        int globalId = globalSymbolDictionary.getOrAddSymbol(symbol);
        if (globalId > currentBatchMaxSymbolId) {
            currentBatchMaxSymbolId = globalId;
        }
        return globalId;
    }

    /**
     * Returns the number of pending rows not yet flushed.
     * For testing.
     */
    public int getPendingRowCount() {
        return pendingRowCount;
    }

    @TestOnly
    public QwpTableBuffer getTableBuffer(String tableName) {
        QwpTableBuffer buffer = tableBuffers.get(tableName);
        if (buffer == null) {
            buffer = new QwpTableBuffer(tableName, this);
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_INT, true);
            if (col != null) {
                col.addInt(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_LONG256, true);
            if (col != null) {
                col.addLong256(l0, l1, l2, l3);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        if (values == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        if (array == null) return this;
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(array);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_LONG, true);
            if (col != null) {
                col.addLong(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Sends a WebSocket PING and reads frames until the PONG comes back,
     * processing any STATUS_DURABLE_ACK or STATUS_OK frames along the way.
     * After this method returns, {@link #getHighestDurableSequence()} reflects
     * the latest durable watermark reported by the server.
     * <p>
     * In async mode the PING is queued for the I/O thread; this method
     * returns once the I/O thread has sent it and processed the response.
     *
     * @throws LineSenderException if the connection is closed or the ping times out
     */
    public void ping() {
        checkNotClosed();
        ensureConnected();
        if (inFlightWindowSize > 1) {
            sendQueue.pingAndDrain();
        } else {
            syncPing();
        }
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
        pendingBytes = 0;
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
     * Opts the connection in for STATUS_DURABLE_ACK frames. Must be called
     * before any send operation — the flag is consulted once, during WebSocket
     * upgrade. Setting this true on a server without primary replication
     * enabled is a no-op: the server silently ignores the header.
     * <p>
     * Observe durable progress via {@link #getHighestDurableSequence()}.
     *
     * @throws LineSenderException if the connection is already established or closed
     */
    public void setRequestDurableAck(boolean enabled) {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
        if (connected) {
            throw new LineSenderException(
                    "setRequestDurableAck must be called before the first send");
        }
        this.requestDurableAck = enabled;
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_SHORT, false);
            if (col != null) {
                col.addShort(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_VARCHAR, true);
            if (col != null) {
                col.addString(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_SYMBOL, true);
            if (col != null) {
                col.addSymbol(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender table(CharSequence tableName) {
        checkNotClosed();
        // Fast path: if table name matches current, skip hashmap lookup
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        // Prevent switching tables while a row is in progress
        if (currentTableBuffer != null && currentTableBuffer.hasInProgressRow()) {
            throw new LineSenderException("cannot switch tables while row is in progress"
                    + " [currentTable=").put(currentTableName).put(']');
        }
        // Table changed - invalidate cached column references
        validateTableName(tableName);
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        currentTableBuffer = tableBuffers.get(tableName);
        if (currentTableBuffer != null) {
            currentTableName = currentTableBuffer.getTableName();
        } else {
            currentTableName = tableName.toString();
            currentTableBuffer = new QwpTableBuffer(currentTableName, this);
            tableBuffers.put(currentTableName, currentTableBuffer);
        }
        // Both modes accumulate rows until flush
        return this;
    }

    @Override
    public QwpWebSocketSender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        try {
            if (unit == ChronoUnit.NANOS) {
                QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_TIMESTAMP_NANOS, true);
                if (col != null) {
                    col.addLong(value);
                }
            } else {
                long micros = toMicros(value, unit);
                QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_TIMESTAMP, true);
                if (col != null) {
                    col.addLong(micros);
                }
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        try {
            long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_TIMESTAMP, true);
            if (col != null) {
                col.addLong(micros);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
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
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_UUID, true);
            if (col != null) {
                col.addUuid(hi, lo);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    private void atMicros(long timestampMicros) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampColumn == null) {
            cachedTimestampColumn = currentTableBuffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
        }
        cachedTimestampColumn.addLong(timestampMicros);
        sendRow();
    }

    private void atNanos(long timestampNanos) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampNanosColumn == null) {
            cachedTimestampNanosColumn = currentTableBuffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP_NANOS);
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Waiting for active buffer [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            }
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
            if (tlsConfig != null) {
                client = WebSocketClientFactory.newTlsInstance(tlsConfig);
            } else {
                client = WebSocketClientFactory.newPlainTextInstance();
            }

            // Connect and upgrade to WebSocket
            try {
                client.setQwpMaxVersion(QwpConstants.MAX_SUPPORTED_VERSION);
                client.setQwpClientId(QwpConstants.CLIENT_ID);
                client.setQwpRequestDurableAck(requestDurableAck);
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
                try {
                    sendQueue = new WebSocketSendQueue(client, inFlightWindow,
                            WebSocketSendQueue.DEFAULT_ENQUEUE_TIMEOUT_MS,
                            WebSocketSendQueue.DEFAULT_SHUTDOWN_TIMEOUT_MS);
                } catch (Throwable t) {
                    inFlightWindow = null;
                    client.close();
                    client = null;
                    throw new LineSenderException("Failed to start I/O thread for " + host + ":" + port, t);
                }
            }
            // Sync mode (window=1): no send queue - we send and read ACKs synchronously

            // Use the version selected by the server
            encoder.setVersion((byte) client.getServerQwpVersion());

            // Server starts fresh on each connection, so any sender-local schema
            // IDs retained from a prior connection must be discarded as well.
            resetSchemaStateForNewConnection();

            connected = true;
            LOG.info("Connected to WebSocket [host={}, port={}, windowSize={}, qwpVersion={}]",
                    host, port, inFlightWindowSize, client.getServerQwpVersion());
        }
    }

    private void ensureNoInProgressRow() {
        if (currentTableBuffer != null && currentTableBuffer.hasInProgressRow()) {
            throw new LineSenderException(
                    "Cannot flush while row is in progress. "
                            + "Use sender.at(), sender.atNow(), or sender.cancelRow() first."
            );
        }
    }

    private void failExpectedIfNeeded(long expectedSequence, LineSenderException error) {
        if (inFlightWindow != null && inFlightWindow.getLastError() == null) {
            inFlightWindow.fail(expectedSequence, error);
        }
    }

    /**
     * Flushes pending rows by encoding and sending them.
     * All non-empty tables are encoded into a single QWP v1 message and sent as one WebSocket frame.
     */
    private void flushPendingRows() {
        if (pendingRowCount <= 0) {
            return;
        }

        // Invalidate cached column references — table buffers will be reset below
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;

        ObjList<CharSequence> keys = tableBuffers.keys();

        // Count non-empty tables for the message header
        int tableCount = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer != null && tableBuffer.getRowCount() > 0) {
                tableCount++;
            }
        }

        if (tableCount == 0) {
            pendingBytes = 0;
            pendingRowCount = 0;
            firstPendingRowTimeNanos = 0;
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Flushing pending rows [count={}, tables={}]", pendingRowCount, tableCount);
        }

        // Ensure activeBuffer is ready for writing
        // It might be in RECYCLED state if previous batch was sent but we didn't swap yet
        ensureActiveBufferReady();

        // Encode all non-empty tables into a single QWP v1 message
        int batchMaxSchemaId = maxSentSchemaId;
        encoder.beginMessage(tableCount, globalSymbolDictionary, maxSentSymbolId, currentBatchMaxSymbolId);
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }

            if (tableBuffer.getSchemaId() < 0) {
                if (nextSchemaId >= maxSchemasPerConnection) {
                    throw new LineSenderException("maximum schemas per connection exceeded")
                            .put("[maxSchemasPerConnection=").put(maxSchemasPerConnection).put(']');
                }
                tableBuffer.setSchemaId(nextSchemaId++);
            }
            batchMaxSchemaId = Math.max(batchMaxSchemaId, tableBuffer.getSchemaId());
            boolean useSchemaRef = tableBuffer.getSchemaId() <= maxSentSchemaId;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Encoding table [name={}, rows={}, maxSentSymbolId={}, batchMaxId={}, useSchemaRef={}]", tableName, tableBuffer.getRowCount(), maxSentSymbolId, currentBatchMaxSymbolId, useSchemaRef);
            }

            encoder.addTable(tableBuffer, useSchemaRef);
        }
        int messageSize = encoder.finishMessage();

        QwpBufferWriter buffer = encoder.getBuffer();

        // Copy the single multi-table message to the microbatch buffer and seal
        activeBuffer.ensureCapacity(messageSize);
        activeBuffer.write(buffer.getBufferPtr(), messageSize);
        activeBuffer.incrementRowCount();
        sealAndSwapBuffer();

        // Update sent state only after successful enqueue.
        // If sealAndSwapBuffer() threw, these remain unchanged so the
        // next batch's delta dictionary will correctly re-include the
        // symbols and schema that the server never received.
        maxSentSymbolId = currentBatchMaxSymbolId;
        maxSentSchemaId = batchMaxSchemaId;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }
            tableBuffer.reset();
        }
        currentBatchMaxSymbolId = -1;

        // Reset pending count
        pendingBytes = 0;
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

        ObjList<CharSequence> keys = tableBuffers.keys();

        // Count non-empty tables for the message header
        int tableCount = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer != null && tableBuffer.getRowCount() > 0) {
                tableCount++;
            }
        }

        if (tableCount == 0) {
            pendingBytes = 0;
            pendingRowCount = 0;
            firstPendingRowTimeNanos = 0;
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sync flush [pendingRows={}, tables={}]", pendingRowCount, tableCount);
        }

        // Encode all non-empty tables into a single QWP v1 message
        int batchMaxSchemaId = maxSentSchemaId;
        encoder.beginMessage(tableCount, globalSymbolDictionary, maxSentSymbolId, currentBatchMaxSymbolId);
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }

            if (tableBuffer.getSchemaId() < 0) {
                if (nextSchemaId >= maxSchemasPerConnection) {
                    throw new LineSenderException("maximum schemas per connection exceeded")
                            .put("[maxSchemasPerConnection=").put(maxSchemasPerConnection).put(']');
                }
                tableBuffer.setSchemaId(nextSchemaId++);
            }
            batchMaxSchemaId = Math.max(batchMaxSchemaId, tableBuffer.getSchemaId());
            boolean useSchemaRef = tableBuffer.getSchemaId() <= maxSentSchemaId;

            if (LOG.isDebugEnabled()) {
                LOG.debug("Encoding table [name={}, rows={}, maxSentSymbolId={}, batchMaxId={}, useSchemaRef={}]", tableName, tableBuffer.getRowCount(), maxSentSymbolId, currentBatchMaxSymbolId, useSchemaRef);
            }

            encoder.addTable(tableBuffer, useSchemaRef);
        }
        int messageSize = encoder.finishMessage();

        QwpBufferWriter buffer = encoder.getBuffer();

        // Track batch in InFlightWindow before sending
        long batchSequence = nextBatchSequence++;
        inFlightWindow.addInFlight(batchSequence);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending sync batch [seq={}, bytes={}, tables={}, maxSentSymbolId={}]", batchSequence, messageSize, tableCount, currentBatchMaxSymbolId);
        }

        // Send over WebSocket and fail the in-flight entry if send throws,
        // so close() does not hang waiting for an ACK that will never arrive.
        try {
            client.sendBinary(buffer.getBufferPtr(), messageSize);
        } catch (LineSenderException e) {
            failExpectedIfNeeded(batchSequence, e);
            throw e;
        } catch (Throwable t) {
            LineSenderException error = new LineSenderException("Failed to send batch " + batchSequence, t);
            failExpectedIfNeeded(batchSequence, error);
            throw error;
        }

        // Wait for ACK synchronously
        waitForAck(batchSequence);

        // Update sent state only after successful send + ACK.
        // If sendBinary() or waitForAck() threw, these remain unchanged
        // so the next batch's delta dictionary will correctly re-include
        // the symbols and schema that the server never received.
        maxSentSymbolId = currentBatchMaxSymbolId;
        maxSentSchemaId = batchMaxSchemaId;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }
            tableBuffer.reset();
        }
        currentBatchMaxSymbolId = -1;

        // Reset pending row tracking
        pendingBytes = 0;
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sync flush complete [totalAcked={}]", inFlightWindow.getTotalAcked());
        }
    }

    private long getPendingBytes() {
        return pendingBytes;
    }

    private void resetSchemaStateForNewConnection() {
        maxSentSchemaId = -1;
        nextSchemaId = 0;

        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }

            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer != null) {
                tableBuffer.setSchemaId(-1);
            }
        }
    }

    private void rollbackRow() {
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
        }
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sealing buffer [id={}, rows={}, bytes={}]", toSend.getBatchId(), toSend.getRowCount(), toSend.getBufferPos());
        }

        // Swap to the other buffer
        activeBuffer = (activeBuffer == buffer0) ? buffer1 : buffer0;

        // If the other buffer is still being sent, wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Waiting for buffer recycle [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            }
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for buffer to be recycled");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Buffer recycled [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            }
        }

        // Reset the new active buffer
        int stateBeforeReset = activeBuffer.getState();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resetting buffer [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(stateBeforeReset));
        }
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
        if (autoFlushBytes > 0) {
            long bytesBefore = currentTableBuffer.getBufferedBytes();
            currentTableBuffer.nextRow();
            pendingBytes += currentTableBuffer.getBufferedBytes() - bytesBefore;
        } else {
            currentTableBuffer.nextRow();
        }

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

    private void syncPing() {
        client.sendPing(1000);
        long deadline = System.currentTimeMillis() + InFlightWindow.DEFAULT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            sawPong = false;
            sawBinaryAck = false;
            boolean received = client.receiveFrame(ackHandler, 1000);
            if (received) {
                if (sawBinaryAck) {
                    long sequence = ackResponse.getSequence();
                    if (ackResponse.isDurableAck()) {
                        if (sequence > highestDurableSequence) {
                            highestDurableSequence = sequence;
                        }
                    } else if (ackResponse.isSuccess()) {
                        inFlightWindow.acknowledgeUpTo(sequence);
                    }
                }
                if (sawPong) {
                    return;
                }
            }
        }
        throw new LineSenderException("Ping timed out");
    }

    private long toMicros(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / 1000L;
            case MICROS:
                return value;
            case MILLIS:
                return value * 1000L;
            case SECONDS:
                return value * 1_000_000L;
            case MINUTES:
                return value * 60_000_000L;
            case HOURS:
                return value * 3_600_000_000L;
            case DAYS:
                return value * 86_400_000_000L;
            default:
                throw new LineSenderException("Unsupported time unit: " + unit);
        }
    }

    private void validateTableName(CharSequence name) {
        if (name == null || !TableUtils.isValidTableName(name, MAX_TABLE_NAME_LENGTH)) {
            if (name == null || name.length() == 0) {
                throw new LineSenderException("table name cannot be empty");
            }
            if (name.length() > MAX_TABLE_NAME_LENGTH) {
                throw new LineSenderException("table name too long [maxLength=" + MAX_TABLE_NAME_LENGTH + "]");
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
                    } else if (ackResponse.isDurableAck()) {
                        // Durable-upload watermark for opted-in connections. Record the highest
                        // value seen; this method does not block for durable acks — callers who
                        // need to wait on durability can poll getHighestDurableSequence().
                        if (sequence > highestDurableSequence) {
                            highestDurableSequence = sequence;
                        }
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

    private static class AckFrameHandler implements WebSocketFrameHandler {
        private final QwpWebSocketSender sender;

        AckFrameHandler(QwpWebSocketSender sender) {
            this.sender = sender;
        }

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

        @Override
        public void onPong(long payloadPtr, int payloadLen) {
            sender.sawPong = true;
        }
    }
}
