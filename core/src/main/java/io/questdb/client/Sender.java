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

package io.questdb.client;

import io.questdb.client.cutlass.auth.AuthUtils;
import io.questdb.client.cutlass.line.AbstractLineTcpSender;
import io.questdb.client.cutlass.line.LineChannel;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV1;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.client.cutlass.line.LineTcpSenderV3;
import io.questdb.client.cutlass.line.http.AbstractLineHttpSender;
import io.questdb.client.cutlass.line.tcp.DelegatingTlsChannel;
import io.questdb.client.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.impl.ConfStringParser;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Files;
import io.questdb.client.std.IntList;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import javax.security.auth.DestroyFailedException;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Influx Line Protocol client to feed data to a remote QuestDB instance.
 * <p>
 * Use {@link #builder(Transport)} or {@link #fromConfig(CharSequence)} method to create a new instance.
 * <br>
 * How to use the Sender:
 * <ol>
 *     <li>Obtain an instance via {@link #builder(Transport)} or {@link #fromConfig(CharSequence)}</li>
 *     <li>Use {@link #table(CharSequence)} to select a table</li>
 *     <li>Use {@link #symbol(CharSequence, CharSequence)} to add all symbols. You must add symbols before adding other columns.</li>
 *     <li>Use {@link #stringColumn(CharSequence, CharSequence)}, {@link #longColumn(CharSequence, long)},
 *     {@link #doubleColumn(CharSequence, double)}, {@link #boolColumn(CharSequence, boolean)},
 *     {@link #timestampColumn(CharSequence, long, ChronoUnit)} to add remaining columns columns</li>
 *     <li>Use {@link #at(long, ChronoUnit)} (long)} to finish a row with an explicit timestamp.Alternatively, you can use
 *     {@link #atNow()} which will add a timestamp on a server.</li>
 *     <li>Optionally: You can use {@link #flush()} to send locally buffered data into a server</li>
 * </ol>
 * <p>
 * Sender implements the <code>java.io.Closeable</code> interface. Thus, you must call the {@link #close()} method
 * when you no longer need it.
 * <br>
 * Thread-safety: Sender is not thread-safe. Each thread-safe needs its own instance, or you have to implement
 * a mechanism for passing Sender instances among thread. An object pool could have this role.
 * <br>
 * This client supports both HTTP and TCP protocols. In most cases you should prefer HTTP protocol as it provides
 * stronger transactional guarantees and better feedback in case of errors.
 * <p>
 * Error handling: Most errors throw an instance of {@link LineSenderException}.
 * <p>
 * When an error occurs while sending data to a server, the Sender does NOT clear its internal buffers.
 * This allows you to retry sending the same data by calling {@link #flush()} again.
 * <br>
 * Recovery strategies:
 * - For transient errors (e.g., temporary network issues): Simply retry by calling {@link #flush()}
 * - For permanent errors (e.g., invalid data format): You have two options:
 *   1. Close the Sender and create a new instance, or
 *   2. Call {@link #reset()} to clear the internal buffers and start building a new row
 * <br>
 * Note: If the underlying error is permanent, retrying {@link #flush()} will fail again.
 * Use {@link #reset()} to discard the problematic data and continue with new data.
 * <br>
 * Note: WebSocket transport uses a terminal sender-level failure model after a
 * connection has been established. After a WebSocket send, ACK, or connection
 * failure, {@link #reset()} does not recover the sender; close it and create a
 * new one.
 *
 */
public interface Sender extends Closeable, ArraySender<Sender> {

    int PROTOCOL_VERSION_NOT_SET_EXPLICIT = -1;
    int PROTOCOL_VERSION_V1 = 1;
    int PROTOCOL_VERSION_V2 = 2;
    int PROTOCOL_VERSION_V3 = 3;

    /**
     * Create a Sender builder instance from a configuration string.
     * <br>
     * This allows using the configuration string as a template for creating a Sender builder instance and then
     * tune options which are not available in the configuration string. Configurations options specified in the
     * configuration string cannot be overridden via the builder methods.
     * <p>
     * <b>Example 1</b><br>
     * This example creates a Sender instance that connects to a QuestDB server over HTTP transport. The created Sender
     * will auto-flush data when number of buffered rows reaches 1000.
     * <code>http::addr=localhost:9000;auto_flush_rows=1000;</code>
     * <br>
     * <b>Example 2</b><br>
     * This example creates a Sender instance that connects to a QuestDB server over TCP transport.
     * <code>tcp::addr=localhost:9009;</code>
     * <p>
     * Refer to <a href="https://questdb.io/docs/reference/clients/overview/">QuestDB documentation</a> for a full list
     * of configuration options.
     *
     * @param configurationString configuration string
     * @return Sender instance
     * @see #fromEnv()
     * @see #builder(CharSequence)
     */
    static LineSenderBuilder builder(CharSequence configurationString) {
        return new LineSenderBuilder().fromConfig(configurationString);
    }

    /**
     * Construct a Builder object to create a new Sender instance with a specific transport.
     * <p>
     * HTTP transport is suitable for most use-cases. It provides stronger transactional guarantees and better feedback
     * in case of errors. The TCP transport is left for compatibility with older versions of QuestDB and for use-cases
     * where HTTP transport is not suitable, when communicating with a QuestDB server over a high-latency network.
     *
     * @param transport transport to use
     * @return Builder object to create a new Sender instance.
     */
    static LineSenderBuilder builder(Transport transport) {
        int protocol;
        switch (transport) {
            case HTTP:
                protocol = LineSenderBuilder.PROTOCOL_HTTP;
                break;
            case TCP:
                protocol = LineSenderBuilder.PROTOCOL_TCP;
                break;
            case UDP:
                protocol = LineSenderBuilder.PROTOCOL_UDP;
                break;
            case WEBSOCKET:
                protocol = LineSenderBuilder.PROTOCOL_WEBSOCKET;
                break;
            default:
                throw new IllegalArgumentException("unknown transport: " + transport);
        }
        return new LineSenderBuilder(protocol);
    }

    /**
     * Create a Sender instance from a configuration string.
     * <br>
     * Configuration string fully describes Sender configuration.
     * <p>
     * <b>Example 1</b><br>
     * This example creates a Sender instance that connects to a QuestDB server over HTTP transport. The created Sender
     * will auto-flush data when number of buffered rows reaches 1000.
     * <code>http::addr=localhost:9000;auto_flush_rows=1000;</code>
     * <br>
     * <b>Example 2</b><br>
     * This example creates a Sender instance that connects to a QuestDB server over TCP transport.
     * <code>tcp::addr=localhost:9009;</code>
     * <p>
     * Refer to <a href="https://questdb.io/docs/reference/clients/overview/">QuestDB documentation</a> for a full list
     * of configuration options.
     *
     * @param configurationString configuration string
     * @return Sender instance
     * @see #fromEnv()
     * @see #builder(CharSequence)
     */
    static Sender fromConfig(CharSequence configurationString) {
        return builder(configurationString).build();
    }

    /**
     * Create a new Sender instance described by a configuration string available as an environment variable.
     * <br>
     * It obtains a string from an environment variable <code>QDB_CLIENT_CONF</code> and then calls
     * {@link #fromConfig(CharSequence)}.
     * <br>
     * This is a convenience method suitable for Cloud environments.
     * <br>
     * <b>Example</b><br>
     * 1. Export a configuration string as an environment variable:
     * <pre>{@code export QDB_CLIENT_CONF="http::addr=localhost:9000;auto_flush_rows=100;"}</pre>
     * 2. Create and use a Sender:
     * <pre>{@code
     * try (Sender sender = Sender.fromEnv()) {
     *  for (int i = 0; i < 1000; i++) {
     *    sender.table("my_table").longColumn("value", i).atNow();
     *  }
     * }
     * }</pre>
     *
     * @return Sender instance
     * @see #fromConfig(CharSequence)
     */
    static Sender fromEnv() {
        String configString = System.getenv("QDB_CLIENT_CONF");
        if (Chars.isBlank(configString)) {
            throw new LineSenderException("QDB_CLIENT_CONF environment variable is not set");
        }
        return fromConfig(configString);
    }

    /**
     * Finalize the current row and assign an explicit timestamp.
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     *
     * @param timestamp timestamp value since epoch
     * @param unit      timestamp unit
     */
    void at(long timestamp, ChronoUnit unit);

    /**
     * Finalize the current row and assign an explicit timestamp.
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     *
     * @param timestamp timestamp value
     */
    void at(Instant timestamp);

    /**
     * Finalize the current row and let QuestDB server assign a timestamp. If you need to set timestamp
     * explicitly then see {@link #at(long, ChronoUnit)}.
     * <br>
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     */
    void atNow();

    /**
     * Block until the server has acknowledged every frame up to {@code targetFsn},
     * or until {@code timeoutMillis} elapses. Pair with {@link #flushAndGetSequence()}
     * to obtain {@code targetFsn} for a specific flush.
     * <br>
     * When {@code request_durable_ack=on} (Enterprise primary replication), {@code targetFsn}
     * advances after durable upload to object storage, not on the ordinary commit ACK.
     * <br>
     * Only the WebSocket QWP transport tracks frame sequence numbers. On other transports
     * (HTTP, TCP, UDP) the call returns immediately: {@code true} when {@code targetFsn < 0}
     * (nothing to wait for), {@code false} otherwise.
     *
     * @param targetFsn     FSN to wait for; typically the return value of {@link #flushAndGetSequence()}
     * @param timeoutMillis upper bound on the wait; {@code <= 0} returns the current state without blocking
     * @return {@code true} if the server has acknowledged up to {@code targetFsn} on return, {@code false} on timeout
     * @throws LineSenderException if the transport has latched a terminal error
     */
    default boolean awaitAckedFsn(long targetFsn, long timeoutMillis) {
        return targetFsn < 0L;
    }

    /**
     * Add a BINARY column value as a byte array. The bytes are written verbatim
     * with no encoding or transformation. To mark the value NULL, do not call
     * this method for the current row (the null bitmap path).
     * <p>
     * A {@code null} array reference is rejected to keep the NULL contract
     * explicit; use the null bitmap instead. An empty array is accepted on the
     * wire, but QuestDB's BINARY storage uses the same NULL sentinel for
     * zero-length and absent values, so an empty payload round-trips as NULL
     * on read.
     *
     * @param name  name of the column
     * @param value the bytes to write; must not be null
     * @return this instance for method chaining
     * @throws LineSenderException if {@code value} is null or the configured
     *                             protocol version does not support BINARY
     */
    default Sender binaryColumn(CharSequence name, byte[] value) {
        throw new LineSenderException("current protocol version does not support binary");
    }

    /**
     * Add a BINARY column value from off-heap memory. The bytes at {@code [ptr,
     * ptr + len)} are copied verbatim into the sender's buffer; the caller's
     * memory only needs to stay valid for the duration of this call.
     * <p>
     * {@code len} must be in {@code [0, Integer.MAX_VALUE]} (BINARY's wire
     * offset entries are uint32). {@code ptr} must be non-zero when
     * {@code len > 0}. To mark the value NULL, omit the column from the row.
     * QuestDB's BINARY storage uses the same sentinel for empty and absent
     * values, so {@code len == 0} round-trips as NULL on read.
     *
     * @param name name of the column
     * @param ptr  native address of the first byte
     * @param len  number of bytes to copy, 0..{@link Integer#MAX_VALUE}
     * @return this instance for method chaining
     * @throws LineSenderException on bad arguments or if the configured
     *                             protocol version does not support BINARY
     */
    default Sender binaryColumn(CharSequence name, long ptr, long len) {
        throw new LineSenderException("current protocol version does not support binary");
    }

    /**
     * Add a BINARY column value from a {@link DirectByteSlice} view over
     * off-heap memory. Equivalent to {@link #binaryColumn(CharSequence, long,
     * long)} with the slice's {@code ptr()} and {@code size()}; the slice is
     * not retained after the call returns.
     *
     * @param name  name of the column
     * @param slice byte slice; must not be null
     * @return this instance for method chaining
     * @throws LineSenderException if {@code slice} is null or the configured
     *                             protocol version does not support BINARY
     */
    default Sender binaryColumn(CharSequence name, DirectByteSlice slice) {
        if (slice == null) {
            throw new LineSenderException(
                    "BINARY slice cannot be null; mark the row null via the null bitmap instead");
        }
        return binaryColumn(name, slice.ptr(), slice.size());
    }

    /**
     * Add a column with a boolean value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender boolColumn(CharSequence name, boolean value);

    /**
     * Returns a direct view of the current sender's internal not flush data.
     * <p>
     * The returned {@link DirectByteSlice} provides borrowed access to the raw byte buffer
     * that hasn't been flush yet.
     * </p>
     *
     * @return a read-only view of the pending transmission data buffer
     */
    DirectByteSlice bufferView();

    /**
     * Add a column with a single-byte signed integer value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support BYTE
     */
    default Sender byteColumn(CharSequence name, byte value) {
        throw new LineSenderException("current protocol version does not support byte");
    }

    /**
     * Cancel the current row. This method is useful when you want to discard a row that you started, but
     * you don't want to send it to a server.
     * <br>
     * After calling this method you can start a new row by calling {@link #table(CharSequence)} again.
     * <br>
     * This is only used when communicating over HTTP transport, and it's illegal to call this method when
     * communicating over TCP transport.
     */
    void cancelRow();

    /**
     * Add a column with a 16-bit Java {@code char} value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support CHAR
     */
    default Sender charColumn(CharSequence name, char value) {
        throw new LineSenderException("current protocol version does not support char");
    }

    /**
     * Close this Sender.
     * <br>
     * This must be called before dereferencing Sender, otherwise resources might leak.
     * Upon returning from this method the Sender is closed and cannot be used anymore.
     * Close method is idempotent, calling this method multiple times has no effect.
     * Calling any other on a closed Sender will throw {@link LineSenderException}
     * <br>
     */
    @Override
    void close();

    /**
     * Add a column with a Decimal256 value serialized using the binary format.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    default Sender decimalColumn(CharSequence name, Decimal256 value) {
        throw new LineSenderException("current protocol version does not support decimal");
    }

    /**
     * Add a column with a Decimal128 value serialized using the binary format.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    default Sender decimalColumn(CharSequence name, Decimal128 value) {
        throw new LineSenderException("current protocol version does not support decimal");
    }

    /**
     * Add a column with a Decimal64 value serialized using the binary format.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    default Sender decimalColumn(CharSequence name, Decimal64 value) {
        throw new LineSenderException("current protocol version does not support decimal");
    }

    /**
     * Add a column with a Decimal value serialized using the text format.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    default Sender decimalColumn(CharSequence name, CharSequence value) {
        throw new LineSenderException("current protocol version does not support decimal");
    }

    /**
     * Add a column with a floating point value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender doubleColumn(CharSequence name, double value);

    /**
     * Convenience: flush every buffered row and block until the server has
     * acknowledged the resulting frame, or until {@code timeoutMillis} elapses.
     * <br>
     * The default implementation is equivalent to
     * {@code awaitAckedFsn(flushAndGetSequence(), timeoutMillis)},
     * which is the same shape as the implicit drain {@link #close()} runs --
     * with the caller controlling the timeout per call-site rather than
     * relying on the builder-time {@code close_flush_timeout_millis}.
     * <br>
     * <b>Note:</b> Implementations that support frame-level acknowledgements
     * (e.g. WebSocket QWP) may override this method with <i>watermark
     * semantics</i>: flushing first, then waiting for the current global
     * published FSN rather than the per-call value from
     * {@code flushAndGetSequence()}. This ensures that {@code drain()} after
     * a previous publish with unacknowledged frames still blocks until those
     * frames are acknowledged, even when the current flush publishes nothing.
     * <br>
     * Returns immediately on transports that do not track frame sequence
     * numbers ({@code HTTP}, {@code TCP}, {@code UDP}): the flush still
     * happens, the wait is a no-op, and the return value is {@code true}.
     *
     * @param timeoutMillis upper bound on the wait; {@code <= 0} returns the
     *                      current state without blocking (the flush still
     *                      happens before the check)
     * @return {@code true} if the server has acknowledged every published
     *         frame on return, {@code false} on timeout
     * @throws LineSenderException if the transport has latched a terminal error
     */
    default boolean drain(long timeoutMillis) {
        return awaitAckedFsn(flushAndGetSequence(), timeoutMillis);
    }

    /**
     * Add a column with a 32-bit floating point value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support FLOAT
     */
    default Sender floatColumn(CharSequence name, float value) {
        throw new LineSenderException("current protocol version does not support float");
    }

    /**
     * Force flushing internal buffers to a server.
     * <br>
     * You should also call this method when you expect a period of quiescence during which no data will be written.
     * Otherwise, previously buffered data would not be sent to a server.
     * <br>
     * This method is also useful when you need a fine control over Sender batching behaviour. Buffer flushing reduces
     * the batching effect. This means it can lower the overall throughput, as each batch has a certain fixed cost
     * component, but it can decrease maximum latency as messages spend less time waiting in buffers and waiting for
     * automatic flush.
     *
     * @see LineSenderBuilder#bufferCapacity(int)
     * @see LineSenderBuilder#maxBufferCapacity(int)
     * @see LineSenderBuilder#autoFlushRows(int)
     */
    void flush();

    /**
     * Same as {@link #flush()} but returns the highest frame sequence number (FSN) the
     * call published. Producer-side correlation handle: log
     * {@code (returnedFsn, domainContext)} alongside the data, then join to the
     * {@link SenderError#getFromFsn()} / {@link SenderError#getToFsn()} span when an
     * async error is delivered, or pass it to {@link #awaitAckedFsn(long, long)} for
     * a bounded blocking wait.
     * <br>
     * Returns {@code -1} when nothing was published by this call, and on transports that
     * do not track frame sequence numbers (HTTP, TCP, UDP).
     *
     * @return highest FSN published by this call, or {@code -1} if no data was published
     *         or the transport does not expose FSNs
     * @throws LineSenderException under the same conditions as {@link #flush()}
     */
    default long flushAndGetSequence() {
        flush();
        return -1L;
    }

    /**
     * Add a GEOHASH column value from pre-packed bits and an explicit bit precision.
     * <p>
     * The {@code bits} long carries the geohash in its low {@code precisionBits} bits;
     * higher bits are masked off and never reach the wire. {@code precisionBits} must
     * be in {@code [1, 60]}, matching {@code GEOHASH(Nb)} on the server.
     * <p>
     * Precision is locked the first time a value is added to the column: subsequent
     * rows must use the same precision or a {@link LineSenderException} is thrown.
     * To mark the value NULL, do not call this method for the current row.
     *
     * @param name          name of the column
     * @param bits          packed geohash; low {@code precisionBits} bits significant
     * @param precisionBits number of significant bits, 1..60
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support
     *                             GEOHASH, {@code precisionBits} is out of range, or
     *                             the precision does not match the column's previously
     *                             locked precision
     */
    default Sender geoHashColumn(CharSequence name, long bits, int precisionBits) {
        throw new LineSenderException("current protocol version does not support geohash");
    }

    /**
     * Add a GEOHASH column value from a base32 geohash string (e.g. "u33d8").
     * <p>
     * The string is decoded as standard geohash base32 (lower- or upper-case;
     * characters in {@code 0-9} and {@code b c d e f g h j k m n p q r s t u v w x
     * y z}; {@code a, i, l, o} are not part of the alphabet). Each character
     * contributes 5 bits, so a 4-character string produces a 20-bit geohash and
     * the longest accepted input is 12 characters (60 bits).
     * <p>
     * The first call locks the column at {@code value.length() * 5} bits; all
     * subsequent rows must supply strings of the same length.
     *
     * @param name  name of the column
     * @param value base32 geohash string, 1..12 characters; must not be null or empty
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support
     *                             GEOHASH, the string is null, empty, longer than 12
     *                             characters, contains non-base32 characters, or its
     *                             derived precision does not match the column's
     *                             previously locked precision
     */
    default Sender geoHashColumn(CharSequence name, CharSequence value) {
        throw new LineSenderException("current protocol version does not support geohash");
    }

    /**
     * Highest frame sequence number (FSN) the server has acknowledged.
     * Returns {@code -1} when no batch has been published yet, and on transports that
     * do not track FSNs (HTTP, TCP, UDP).
     * <br>
     * Snapshot accessor: for a bounded blocking wait, use
     * {@link #awaitAckedFsn(long, long)}.
     *
     * @return highest acknowledged FSN, or {@code -1} if none or unsupported
     */
    default long getAckedFsn() {
        return -1L;
    }

    /**
     * Add a column with a 32-bit signed integer value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support INT
     */
    default Sender intColumn(CharSequence name, int value) {
        throw new LineSenderException("current protocol version does not support int");
    }

    /**
     * Add an IPv4 column value, as a packed 32-bit address in host byte order
     * (e.g. 192.168.1.1 -> 0xC0A80101).
     * <p>
     * Per QuestDB convention, the bit pattern 0 (i.e. 0.0.0.0) is the IPv4
     * NULL sentinel and surfaces as NULL on read.
     *
     * @param name    name of the column
     * @param address packed IPv4 address
     * @return this instance for method chaining
     */
    default Sender ipv4Column(CharSequence name, int address) {
        throw new LineSenderException("current protocol version does not support ipv4");
    }

    /**
     * Add an IPv4 column value from a dotted-quad string (e.g. "192.168.1.1").
     *
     * @param name    name of the column
     * @param address dotted-quad IPv4 address; must not be null
     * @return this instance for method chaining
     * @throws LineSenderException if the address fails to parse, or the
     *                             configured protocol version does not support IPv4
     */
    default Sender ipv4Column(CharSequence name, CharSequence address) {
        throw new LineSenderException("current protocol version does not support ipv4");
    }

    /**
     * Add a LONG256 column value, packed as four 64-bit words, least-significant first
     * (so the 256-bit value is {@code (l3 << 192) | (l2 << 128) | (l1 << 64) | l0}).
     *
     * @param name name of the column
     * @param l0   bits 0..63 (least significant)
     * @param l1   bits 64..127
     * @param l2   bits 128..191
     * @param l3   bits 192..255 (most significant)
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support LONG256
     */
    default Sender long256Column(CharSequence name, long l0, long l1, long l2, long l3) {
        throw new LineSenderException("current protocol version does not support long256");
    }

    /**
     * Add a column with an integer value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender longColumn(CharSequence name, long value);


    /**
     * Clear the internal buffers, discarding any unsent data.
     * <br>
     * This method discards all buffered data that hasn't been sent to the server yet,
     * allowing you to start fresh with new data. The auto-flush timer is reset and will
     * restart based on the configured auto-flush interval when the next row is added.
     * <br>
     * This is useful for error recovery when you encounter a permanent error (e.g., invalid
     * data format) and want to continue sending new data without retrying the problematic data.
     * After calling this method, you can start building a new row by calling {@link #table(CharSequence)}.
     * <br>
     * Note: This method is only available for HTTP transport. TCP transport doesn't support
     * this operation.
     *
     * @see #flush()
     */
    void reset();

    /**
     * Add a column with a 16-bit signed integer value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support SHORT
     */
    default Sender shortColumn(CharSequence name, short value) {
        throw new LineSenderException("current protocol version does not support short");
    }

    /**
     * Add a column with a string value.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender stringColumn(CharSequence name, CharSequence value);

    /**
     * Add a column with a symbol value. You must call add symbols before adding any other column types.
     *
     * @param name  name of the column
     * @param value value to add
     * @return this instance for method chaining
     */
    Sender symbol(CharSequence name, CharSequence value);

    /**
     * Select the table for a new row. This is always the first method to start an error. It's an error to call other
     * methods without calling this method first.
     * <br>
     * After calling this method you can start adding columns to the row and then call {@link #atNow()} or {@link #at(Instant)}
     * to finalize the row. You can then start a new row by calling this method again.
     * <br>
     * If you want to cancel the current row, you can call {@link #cancelRow()}.
     *
     * @param table name of the table
     * @return this instance for method chaining
     */
    Sender table(CharSequence table);

    /**
     * Add a column with a non-designated timestamp value.
     *
     * @param name  name of the column
     * @param value timestamp value since epoch
     * @param unit  timestamp value unit
     * @return this instance for method chaining
     */
    Sender timestampColumn(CharSequence name, long value, ChronoUnit unit);

    /**
     * Add a column with a non-designated timestamp value.
     *
     * @param name  name of the column
     * @param value timestamp value
     * @return this instance for method chaining
     */
    Sender timestampColumn(CharSequence name, Instant value);

    /**
     * Add a UUID column value, packed as two 64-bit halves.
     *
     * @param name name of the column
     * @param lo   low 64 bits of the UUID
     * @param hi   high 64 bits of the UUID
     * @return this instance for method chaining
     * @throws LineSenderException if the configured protocol version does not support UUID
     */
    default Sender uuidColumn(CharSequence name, long lo, long hi) {
        throw new LineSenderException("current protocol version does not support uuid");
    }

    /**
     * Initial-connect behavior for the WebSocket cursor SF transport.
     * <ul>
     *   <li>{@link #OFF} — single attempt on the user thread; a startup
     *       failure throws immediately. Correct for fail-fast deployments
     *       where a misconfigured host should not stall app startup.</li>
     *   <li>{@link #SYNC} — same retry loop the in-flight reconnect path
     *       uses, but it runs on the user thread inside {@code fromConfig}.
     *       Blocks up to {@code reconnect_max_duration_millis}. Auth/upgrade
     *       failures stay terminal. Useful when the server is expected to
     *       come up shortly after the producer and the producer is willing
     *       to wait.</li>
     *   <li>{@link #ASYNC} — {@code fromConfig} returns immediately with an
     *       unconnected sender; the I/O thread runs the same retry loop in
     *       the background. The user thread can call {@code at()} /
     *       {@code flush()} immediately; rows accumulate in the cursor SF
     *       engine until the wire is up. Connect failures are retried
     *       indefinitely in the background; a terminal upgrade failure
     *       (auth reject, capability mismatch) is delivered to the async
     *       error inbox as a {@link io.questdb.client.SenderError} (no
     *       synchronous throw on the user call site). Wire
     *       {@code error_handler=...} to observe these.</li>
     * </ul>
     * <p>
     * Default resolution when the caller does not pick a value:
     * {@link #SYNC} if any {@code reconnect_*} knob was tuned explicitly
     * (so the budget the user wrote actually applies to the first connect),
     * otherwise {@link #OFF}. Pass an explicit value to override -- an
     * explicit {@link #OFF} alongside a tuned {@code reconnect_*} budget
     * still gets {@link #OFF}.
     */
    enum InitialConnectMode {
        OFF,
        SYNC,
        ASYNC
    }

    /**
     * Durability contract for the store-and-forward write path. Selects when
     * the SF segment file is fsynced; trades latency / throughput for
     * crash-survival of unacked frames.
     * <ul>
     *   <li>{@link #MEMORY} — never fsync explicitly. Bytes live in the OS
     *       page cache; survive a JVM crash but not an OS crash. Default
     *       and the lowest-latency setting.</li>
     *   <li>{@link #FLUSH} — fsync the active segment at every
     *       {@code Sender.flush()} (and at the implicit close-flush). One
     *       fsync per user flush, regardless of frame count.</li>
     *   <li>{@link #APPEND} — fsync after every individual frame append.
     *       Strongest guarantee, slowest path; pay a disk fsync per row.</li>
     * </ul>
     */
    enum SfDurability {
        MEMORY,
        FLUSH,
        APPEND
    }

    /**
     * Configure TLS mode.
     * Most users should not need to use anything but the default mode.
     */
    enum TlsValidationMode {

        /**
         * Sender validates a server certificate chain and throws an exception
         * when a certificate is not trusted.
         */
        DEFAULT,

        /**
         * Suitable for testing. In this mode Sender does not validate a server certificate chain.
         * This is inherently insecure and should never be used in a production environment.
         * Useful in test environments with self-signed certificates.
         */
        INSECURE
    }

    /**
     * Transport to use for communication with a QuestDB server.
     */
    enum Transport {
        /**
         * Use HTTP transport to communicate with a QuestDB server.
         * <p>
         * This transport is suitable for most use-cases. It provides stronger transactional guarantees and better
         * feedback in case of errors.
         */
        HTTP,

        /**
         * Use TCP transport to communicate with a QuestDB server.
         * <p>
         * Most users should not need to use this transport. It's left for compatibility with older versions of QuestDB
         * and for use-cases where HTTP transport is not suitable, when communicating with a QuestDB server over a high-latency
         * network
         */
        TCP,

        /**
         * Fire-and-forget binary ingestion over UDP.
         * <p>
         * UDP transport sends datagrams without waiting for acknowledgement. It is suitable for
         * high-throughput scenarios where occasional message loss is acceptable.
         */
        UDP,

        /**
         * Use WebSocket transport to communicate with a QuestDB server.
         * <p>
         * WebSocket transport uses the QWP v1 binary protocol for efficient data ingestion.
         * It supports both synchronous and asynchronous modes with flow control.
         */
        WEBSOCKET
    }

    /**
     * Builder class to construct a new instance of a Sender.
     * <br>
     * Example usage for HTTP transport:
     * <pre>{@code
     * try (Sender sender = Sender.builder(Sender.Transport.HTTP)
     *  .address("localhost:9000")
     *  .build()) {
     *      sender.table(tableName).column("value", 42).atNow();
     *      sender.flush();
     *  }
     * }</pre>
     * <br>
     * Example usage for HTTP transport and TLS:
     * <pre>{@code
     * try (Sender sender = Sender.builder(Sender.Transport.HTTP)
     *  .address("localhost:9000")
     *  .enableTls()
     *  .build()) {
     *    sender.table(tableName).column("value", 42).atNow();
     *    sender.flush();
     *   }
     * }</pre>
     * <br>
     * Example usage for TCP transport and TLS:
     * <pre>{@code
     * try (Sender sender = Sender.builder(Sender.Transport.TCP)
     *  .address("localhost:9000")
     *  .enableTls()
     *  .build()) {
     *    sender.table(tableName).column("value", 42).atNow();
     *    sender.flush();
     *   }
     * }</pre>
     *
     * @see Sender#fromConfig(CharSequence) for creating a Sender directly from a configuration String
     */
    final class LineSenderBuilder {
        private static final int AUTO_FLUSH_DISABLED = 0;
        // close() drain timeout. Default applied at build() time. 0 or -1
        // means "fast close" (skip the drain entirely); any positive value
        // bounds the wait for ackedFsn to catch up to publishedFsn. Uses
        // its own sentinel because -1 is a documented user-facing value
        // and would otherwise collide with PARAMETER_NOT_SET_EXPLICITLY.
        private static final long CLOSE_FLUSH_TIMEOUT_NOT_SET = Long.MIN_VALUE;
        private static final int DEFAULT_AUTO_FLUSH_INTERVAL_MILLIS = 1_000;
        private static final int DEFAULT_AUTO_FLUSH_ROWS = 75_000;
        private static final int DEFAULT_BUFFER_CAPACITY = 64 * 1024;
        // Default close() drain timeout: block up to 60s waiting for the
        // server to ACK everything published into the engine before
        // shutting down the I/O loop. The wide default reflects what real
        // workloads need on the close path -- catch-up replicas, slow
        // consumers, and small server send buffers under chunky payloads
        // all routinely take tens of seconds to acknowledge a backlog,
        // and silently dropping unacked rows in close() is a much worse
        // default than spending the wall-clock to wait. Callers who want
        // a tighter close budget either set close_flush_timeout_millis
        // explicitly or call the new drain(timeoutMillis) before close().
        private static final long DEFAULT_CLOSE_FLUSH_TIMEOUT_MILLIS = 60_000L;
        private static final int DEFAULT_HTTP_PORT = 9000;
        private static final int DEFAULT_HTTP_TIMEOUT = 30_000;
        private static final int DEFAULT_MAXIMUM_BUFFER_CAPACITY = 100 * 1024 * 1024;
        private static final int DEFAULT_MAX_BACKGROUND_DRAINERS = 4;
        private static final int DEFAULT_MAX_BACKOFF_MILLIS = 1_000;
        // Default ceiling on cursor-allocated bytes (active + spare + sealed).
        // RAM is precious; if you're not persisting to disk, you don't get
        // to balloon. Memory mode = 128 MiB (32 segments at default size).
        private static final long DEFAULT_MAX_BYTES_MEMORY = 128L * 1024 * 1024;
        // Disk is cheap and SF's job is to absorb backpressure during wire
        // outages — the cap should be large enough that normal traffic
        // never approaches it. SF mode = 10 GiB (2560 segments at default
        // size). Users can lower this on space-constrained hosts.
        private static final long DEFAULT_MAX_BYTES_SF = 10L * 1024 * 1024 * 1024;
        private static final int DEFAULT_MAX_DATAGRAM_SIZE = 1400;
        private static final int DEFAULT_MAX_NAME_LEN = 127;
        private static final long DEFAULT_MAX_RETRY_NANOS = TimeUnit.SECONDS.toNanos(10); // keep sync with the contract of the configuration method
        private static final long DEFAULT_MIN_REQUEST_THROUGHPUT = 100 * 1024; // 100KB/s, keep in sync with the contract of the configuration method
        // Default per-segment size for the cursor SF/memory-mode ring (4 MiB).
        // Smaller than the legacy 64 MiB default — cursor has no per-rotation
        // syscall cost so smaller segments give finer trim granularity and
        // make the cap arithmetic friendlier (cap / segment >> 2).
        private static final long DEFAULT_SEGMENT_BYTES = 4L * 1024 * 1024;
        // Slot identity within sfDir. Each sender owns <sfDir>/<senderId>/ and
        // takes an advisory exclusive lock there. Default "default" is fine for
        // single-sender deployments; multi-sender setups must set this explicitly
        // or the second sender will fail with "sf slot already in use".
        private static final String DEFAULT_SENDER_ID = "default";
        private static final int DEFAULT_TCP_PORT = 9009;
        private static final int DEFAULT_UDP_PORT = 9007;
        private static final int DEFAULT_WEBSOCKET_PORT = 9000;
        private static final int DEFAULT_WS_AUTO_FLUSH_BYTES = 0;
        private static final long DEFAULT_WS_AUTO_FLUSH_INTERVAL_NANOS = 100_000_000L; // 100ms
        private static final int DEFAULT_WS_AUTO_FLUSH_ROWS = 1_000;
        // Cadence at which the SF cursor I/O loop sends a keepalive PING
        // while waiting on STATUS_DURABLE_ACK frames. Default applied at
        // build() time. 0 or negative is a documented "disable" value, so
        // a Long.MIN_VALUE sentinel keeps it distinguishable from "unset".
        private static final long DURABLE_ACK_KEEPALIVE_NOT_SET = Long.MIN_VALUE;
        private static final int MIN_BUFFER_SIZE = AuthUtils.CHALLENGE_LEN + 1; // challenge size + 1;
        // sf-client.md section 4.4: the inbox capacity must accommodate the
        // distinct error categories in a bursty error stream so that drop-oldest
        // does not erase the trailing distribution of categories.
        private static final int MIN_ERROR_INBOX_CAPACITY = 16;
        // The PARAMETER_NOT_SET_EXPLICITLY constant is used to detect if a parameter was set explicitly in configuration parameters
        // where it matters. This is needed to detect invalid combinations of parameters. Why?
        // We want to fail-fast even when an explicitly configured options happens to be same value as the default value,
        // because this still indicates a user error and silently ignoring it could lead to hard-to-debug issues.
        private static final int PARAMETER_NOT_SET_EXPLICITLY = -1;
        private static final int PORT_NOT_SET = -1;
        private static final int PROTOCOL_HTTP = 1;
        private static final int PROTOCOL_TCP = 0;
        private static final int PROTOCOL_UDP = 3;
        private static final int PROTOCOL_WEBSOCKET = 2;
        private final ObjList<String> hosts = new ObjList<>();
        private final IntList ports = new IntList();
        private long authTimeoutMillis = QwpWebSocketSender.DEFAULT_AUTH_TIMEOUT_MS;
        private int autoFlushBytes = PARAMETER_NOT_SET_EXPLICITLY;
        private int autoFlushIntervalMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private int autoFlushRows = PARAMETER_NOT_SET_EXPLICITLY;
        private int bufferCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        private long closeFlushTimeoutMillis = CLOSE_FLUSH_TIMEOUT_NOT_SET;
        // Upper bound (ms) on the TCP connect. PARAMETER_NOT_SET_EXPLICITLY ->
        // 0 (no application-level connect timeout; OS connect timeout applies).
        private int connectTimeoutMillis = PARAMETER_NOT_SET_EXPLICITLY;
        // Optional user-supplied async connection-event listener. When null,
        // the sender uses DefaultSenderConnectionListener.INSTANCE
        // (loud-not-silent log of every transition).
        private io.questdb.client.SenderConnectionListener connectionListener;
        // Bounded inbox capacity for the async connection-event dispatcher.
        // PARAMETER_NOT_SET_EXPLICITLY → spec default (64).
        private int connectionListenerInboxCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        // Optional user-supplied observer for background orphan-slot drainer
        // events (durable-ack capability-gap retries, all-replica failover
        // windows, persistent-failure escalation). When null, drainers run
        // without a listener. Only meaningful with drainOrphans=true.
        private io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener drainerListener;
        // Orphan adoption: when true, the foreground sender scans
        // <sf_dir>/*/ at startup for sibling slots that hold unacked data
        // and reports them. Default false. Spec calls for spawning
        // background drainers to actually empty those slots; the drainer
        // runtime lands in a follow-up commit. For now we surface the
        // count via logging so users can confirm orphans are being seen.
        private boolean drainOrphans = false;
        // Orphan-scan exclusion for the connection pool. The pool co-manages
        // exactly <orphanDrainBase>-<i> for i in [0, orphanDrainSlotCount) and
        // recovers each of those on (re)creation, so pooled senders must never
        // treat one another's live slots as drainable orphans. Anything else --
        // a different base, a bare un-suffixed id, OR a same-base index at or
        // above the count (a slot left behind by a larger pool before maxSize
        // shrank) -- is still drained, so unacked data is never stranded.
        private String orphanDrainBase;
        private int orphanDrainSlotCount;
        private long durableAckKeepaliveIntervalMillis = DURABLE_ACK_KEEPALIVE_NOT_SET;
        // Optional user-supplied async error handler. When null, the sender
        // uses DefaultSenderErrorHandler.INSTANCE (loud-not-silent log).
        private io.questdb.client.SenderErrorHandler errorHandler;
        // Bounded inbox capacity for the async error dispatcher.
        // PARAMETER_NOT_SET_EXPLICITLY → spec default (256).
        private int errorInboxCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        private int maxFrameRejections = PARAMETER_NOT_SET_EXPLICITLY;
        private long poisonMinEscalationWindowMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private String httpPath;
        private String httpSettingsPath;
        private int httpTimeout = PARAMETER_NOT_SET_EXPLICITLY;
        private String httpToken;
        // Drives the initial-connect strategy. null means "not set
        // explicitly", which build() resolves to SYNC when any reconnect_*
        // knob was tuned by the user, otherwise OFF. SYNC retries on the
        // user thread up to the reconnect cap. ASYNC returns immediately
        // and lets the I/O thread retry in the background, surfacing
        // terminal failures via the error inbox.
        private InitialConnectMode initialConnectMode = null;
        private String keyId;
        private int maxBackgroundDrainers = DEFAULT_MAX_BACKGROUND_DRAINERS;
        private int maxBackoffMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private int maxDatagramSize = PARAMETER_NOT_SET_EXPLICITLY;
        private int maxNameLength = PARAMETER_NOT_SET_EXPLICITLY;
        private int maximumBufferCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        private final HttpClientConfiguration httpClientConfiguration = new DefaultHttpClientConfiguration() {
            @Override
            public int getInitialRequestBufferSize() {
                return bufferCapacity == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_BUFFER_CAPACITY : bufferCapacity;
            }

            @Override
            public int getMaximumRequestBufferSize() {
                return maximumBufferCapacity == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_MAXIMUM_BUFFER_CAPACITY : maximumBufferCapacity;
            }

            @Override
            public String getSettingsPath() {
                return httpSettingsPath == null ? super.getSettingsPath() : httpSettingsPath;
            }

            @Override
            public int getTimeout() {
                return httpTimeout == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_HTTP_TIMEOUT : httpTimeout;
            }

            @Override
            public int getConnectTimeout() {
                return connectTimeoutMillis == PARAMETER_NOT_SET_EXPLICITLY ? 0 : connectTimeoutMillis;
            }
        };
        private long minRequestThroughput = PARAMETER_NOT_SET_EXPLICITLY;
        private int multicastTtl = PARAMETER_NOT_SET_EXPLICITLY;
        private String password;
        private PrivateKey privateKey;
        private int protocol = PARAMETER_NOT_SET_EXPLICITLY;
        private int protocolVersion = PARAMETER_NOT_SET_EXPLICITLY;
        private long reconnectInitialBackoffMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private long reconnectMaxBackoffMillis = PARAMETER_NOT_SET_EXPLICITLY;
        // Reconnect policy. Defaults applied at build() time. Per-outage
        // time cap (default 300_000), initial backoff (default 100), and
        // max backoff (default 5_000) for the cursor I/O loop's exponential
        // retry-with-jitter loop.
        private long reconnectMaxDurationMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean requestDurableAck;
        private int retryTimeoutMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean transactional;
        private String senderId = DEFAULT_SENDER_ID;
        // Per-append deadline for SF appendBlocking spin-then-throw. Used to
        // be a hardcoded 30s constant; expose so tight-SLA users can lower
        // and offline-tolerant users can raise.
        private long sfAppendDeadlineMillis = PARAMETER_NOT_SET_EXPLICITLY;
        // Store-and-forward (WebSocket only). SF is enabled iff sfDir is non-null —
        // there is no separate on/off flag (presence of the directory is the switch).
        // null sfDir → memory-only async ingest (same lock-free architecture, no disk).
        private String sfDir;
        // Durability contract for SF append/flush. Today only MEMORY is
        // implemented; FLUSH and APPEND are deferred follow-ups (cursor needs
        // to learn fsync first).
        private SfDurability sfDurability = SfDurability.MEMORY;
        private long sfMaxBytes = PARAMETER_NOT_SET_EXPLICITLY;
        private long sfMaxTotalBytes = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean shouldDestroyPrivKey;
        private boolean tlsEnabled;
        private TlsValidationMode tlsValidationMode;
        private char[] trustStorePassword;
        private String trustStorePath;
        private String username;

        private LineSenderBuilder() {

        }

        private LineSenderBuilder(int protocol) {
            this.protocol = protocol;
        }

        /**
         * Set address of a QuestDB server. It can be either a domain name or a textual representation of an IP address.
         * Only IPv4 addresses are supported.
         * <br>
         * Optionally, you can also include a port. In this can you separate a port from the address by using a colon.
         * Example: my.example.org:54321.
         * <p>
         * If you include a port then you must not call {@link LineSenderBuilder#port(int)}.
         *
         * @param address address of a QuestDB server
         * @return this instance for method chaining.
         */
        public LineSenderBuilder address(CharSequence address) {
            if (Chars.isBlank(address)) {
                throw new LineSenderException("address cannot be empty nor null");
            }
            int portIndex = Chars.indexOf(address, ':');
            if (portIndex + 1 == address.length()) {
                throw new LineSenderException("invalid address, use IPv4 address or a domain name [address=").put(address).put("]");
            }
            String hostSansPort;
            int parsedPort = -1;
            if (portIndex != -1) {
                try {
                    parsedPort = Numbers.parseInt(address, portIndex + 1, address.length());
                    if (parsedPort < 1 || parsedPort > 65535) {
                        throw new LineSenderException("invalid port [port=").put(parsedPort).put("]");
                    }
                } catch (NumericException e) {
                    throw new LineSenderException("cannot parse a port from the address, use IPv4 address or a domain name")
                            .put(" [address=").put(address).put("]");
                }
                hostSansPort = address.subSequence(0, portIndex).toString();
            } else {
                hostSansPort = address.toString();
            }

            if (parsedPort != -1) {
                for (int i = 0, n = hosts.size(); i < n; i++) {
                    String storedHost = hosts.get(i);
                    if (Chars.equals(storedHost, hostSansPort)) {
                        if (ports.size() > i && ports.getQuick(i) == parsedPort) {
                            throw new LineSenderException("duplicated addresses are not allowed ")
                                    .put("[address=").put(address).put("]");
                        }
                    }
                }
                while (ports.size() < hosts.size()) {
                    ports.add(PORT_NOT_SET);
                }
                this.hosts.add(hostSansPort);
                this.ports.add(parsedPort);
            } else {
                this.hosts.add(hostSansPort);
            }
            return this;
        }

        /**
         * Advanced TLS configuration. Most users should not need to use this.
         *
         * @return instance of {@link AdvancedTlsSettings} to advanced TLS configuration
         */
        public AdvancedTlsSettings advancedTls() {
            if (LineSenderBuilder.this.trustStorePath != null) {
                throw new LineSenderException("custom trust store was already configured ")
                        .put("[path=").put(LineSenderBuilder.this.trustStorePath).put("]");
            }
            if (tlsValidationMode == TlsValidationMode.INSECURE) {
                throw new LineSenderException("TLS validation was already disabled");
            }
            return new AdvancedTlsSettings();
        }

        /**
         * Upper bound, in milliseconds, on establishing the TCP connection to a
         * QuestDB endpoint. When set, a connect that does not complete within
         * this budget is aborted (instead of riding the much longer OS-level
         * connect timeout). Applies to both HTTP/WebSocket transports. Default
         * is unset (0), which falls back to the OS connect timeout.
         *
         * @param millis connect timeout in milliseconds; must be &gt; 0
         * @return this instance for method chaining
         */
        public LineSenderBuilder connectTimeoutMillis(int millis) {
            if (this.connectTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("connect timeout was already configured ")
                        .put("[connect_timeout=").put(this.connectTimeoutMillis).put("]");
            }
            if (millis <= 0) {
                throw new LineSenderException("connect_timeout must be > 0: ").put(millis);
            }
            this.connectTimeoutMillis = millis;
            return this;
        }

        /**
         * Per-endpoint timeout on the WebSocket upgrade response read. Default
         * {@value QwpWebSocketSender#DEFAULT_AUTH_TIMEOUT_MS} ms.
         */
        public LineSenderBuilder authTimeoutMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException(
                        "auth_timeout_ms is only supported for WebSocket transport");
            }
            if (millis <= 0L) {
                throw new LineSenderException("auth_timeout_ms must be > 0: ").put(millis);
            }
            this.authTimeoutMillis = millis;
            return this;
        }

        /**
         * Set the maximum number of bytes per batch before auto-flushing.
         * <br>
         * This is only used when communicating over WebSocket transport.
         * <br>
         * Default value is 0, which disables byte-based auto-flush.
         *
         * @param bytes maximum bytes per batch
         * @return this instance for method chaining
         */
        public LineSenderBuilder autoFlushBytes(int bytes) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("auto flush bytes is only supported for WebSocket transport");
            }
            if (this.autoFlushBytes != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("auto flush bytes was already configured")
                        .put("[bytes=").put(this.autoFlushBytes).put("]");
            }
            if (bytes < 0) {
                throw new LineSenderException("auto flush bytes cannot be negative")
                        .put("[bytes=").put(bytes).put("]");
            }
            this.autoFlushBytes = bytes;
            return this;
        }

        /**
         * Set the interval in milliseconds at which the Sender automatically flushes its buffer.
         * <br>
         * It flushes the buffer even when the number of buffered rows is less than the value set by {@link #autoFlushRows(int)}.
         * This prevents rows from being locally buffered for too long when the rate of incoming data is low.
         * <p>
         * <strong>Important:</strong>This option does not cause the Sender to flush the buffer at regular intervals.
         * Auto-flushing is only triggered when calling {@link #atNow()}, {@link #at(Instant)} or {@link #at(long, ChronoUnit)}.
         * The Sender will not flush the buffer if no new rows are added even if the auto-flush interval has elapsed.
         * <p>
         * This is only used when communicating over HTTP and WebSocket transport, and it's illegal to call this method when
         * communicating over TCP or UDP transport.
         * <br>
         * You cannot set this value when auto-flush is disabled. See {@link #disableAutoFlush()}.
         * <br>
         * Default value is 1000 milliseconds for HTTP and 100 milliseconds for WebSocket.
         *
         * @param autoFlushIntervalMillis interval at which the Sender automatically flushes it's buffer in milliseconds.
         * @return this instance for method chaining
         */
        public LineSenderBuilder autoFlushIntervalMillis(int autoFlushIntervalMillis) {
            if (this.autoFlushIntervalMillis != PARAMETER_NOT_SET_EXPLICITLY && this.autoFlushIntervalMillis != Integer.MAX_VALUE) {
                throw new LineSenderException("auto flush interval was already configured ")
                        .put("[autoFlushIntervalMillis=").put(this.autoFlushIntervalMillis).put("]");
            }
            if (this.autoFlushIntervalMillis == Integer.MAX_VALUE && autoFlushIntervalMillis != Integer.MAX_VALUE) {
                throw new LineSenderException("cannot set auto flush interval when interval based auto-flush is already disabled");
            }
            if (autoFlushIntervalMillis <= 0) {
                throw new LineSenderException("auto flush interval cannot be negative ")
                        .put("[autoFlushIntervalMillis=").put(autoFlushIntervalMillis).put("]");
            }
            this.autoFlushIntervalMillis = autoFlushIntervalMillis;
            return this;
        }

        /**
         * Set the maximum number of rows that are buffered locally before they are automatically sent to a server.
         * <br>
         * This is only used when communicating over HTTP and WebSocket transport, and it's illegal to call this method when
         * communicating over TCP or UDP transport.
         * <br>
         * The Sender automatically flushes it's buffer when the number of accumulated rows reaches the configured value.
         * You must make sure that the buffer has sufficient capacity to accommodate all locally buffered data.
         * Otherwise, the Sender will throw an exception.
         * <br>
         * Setting this to 1 means that the Sender will send each row to a server immediately after it is added. This
         * effectively disables batching and may lead to a significant performance degradation.
         * <br>
         * Setting this to 0 disables row-based auto-flush. Interval-based auto-flush remains enabled.
         * <p>
         * You cannot set this value when auto-flush is disabled. See {@link #disableAutoFlush()}.
         *
         * @param autoFlushRows maximum number of rows that can be buffered locally before they are sent to a server.
         * @return this instance for method chaining
         * @see #flush()
         * @see #disableAutoFlush()
         * @see #maxBufferCapacity(int)
         * @see #autoFlushIntervalMillis(int)
         */
        public LineSenderBuilder autoFlushRows(int autoFlushRows) {
            if (this.autoFlushRows != PARAMETER_NOT_SET_EXPLICITLY && this.autoFlushRows != AUTO_FLUSH_DISABLED) {
                throw new LineSenderException("auto flush rows was already configured ")
                        .put("[autoFlushRows=").put(this.autoFlushRows).put("]");
            } else if (this.autoFlushRows == AUTO_FLUSH_DISABLED && autoFlushRows != AUTO_FLUSH_DISABLED) {
                throw new LineSenderException("cannot set auto flush rows when auto-flush is already disabled");
            }
            if (autoFlushRows < 0) {
                throw new LineSenderException("auto flush rows cannot be negative ")
                        .put("[autoFlushRows=").put(autoFlushRows).put("]");
            }
            this.autoFlushRows = autoFlushRows;
            return this;
        }

        /**
         * Configure capacity of an internal buffer.
         * <p>
         * When communicating over HTTP protocol this buffer size is treated as the initial buffer capacity. Buffer can
         * grow up to {@link #maxBufferCapacity(int)}. You should call {@link #flush()} to send buffered data to
         * a server. Otherwise, data will be sent automatically when number of buffered rows reaches {@link #autoFlushRows(int)}.
         * <br>
         * When communicating over TCP protocol this buffer size is treated as the maximum buffer capacity. The Sender
         * will automatically flush the buffer when it reaches this capacity.
         * <br>
         * WebSocket transport does not support configuring buffer capacity explicitly.
         *
         * @param bufferCapacity buffer capacity in bytes.
         * @return this instance for method chaining
         * @see Sender#flush()
         */
        public LineSenderBuilder bufferCapacity(int bufferCapacity) {
            if (protocol == PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("buffer capacity is not supported for WebSocket transport");
            }
            if (this.bufferCapacity != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("buffer capacity was already configured ")
                        .put("[capacity=").put(this.bufferCapacity).put("]");
            }
            if (bufferCapacity < 0) {
                throw new LineSenderException("buffer capacity cannot be negative ")
                        .put("[capacity=").put(bufferCapacity).put("]");
            }
            this.bufferCapacity = bufferCapacity;
            return this;
        }

        /**
         * Build a Sender instance. This method construct a Sender instance.
         * <br>
         * You are responsible for calling {@link #close()} when you no longer need the Sender instance.
         *
         * @return returns a configured instance of Sender.
         */
        public Sender build() {
            configureDefaults();
            validateParameters();

            NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
            if (protocol == PROTOCOL_HTTP) {
                int actualAutoFlushRows = autoFlushRows == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_AUTO_FLUSH_ROWS : autoFlushRows;
                long actualMaxRetriesNanos = retryTimeoutMillis == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_MAX_RETRY_NANOS : retryTimeoutMillis * 1_000_000L;
                long actualMinRequestThroughput = minRequestThroughput == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_MIN_REQUEST_THROUGHPUT : minRequestThroughput;
                long actualAutoFlushIntervalMillis;
                if (autoFlushIntervalMillis == Integer.MAX_VALUE) {
                    actualAutoFlushIntervalMillis = Long.MAX_VALUE;
                } else {
                    actualAutoFlushIntervalMillis = TimeUnit.MILLISECONDS.toNanos(autoFlushIntervalMillis == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_AUTO_FLUSH_INTERVAL_MILLIS : autoFlushIntervalMillis);
                }
                ClientTlsConfiguration tlsConfig = null;
                if (tlsEnabled) {
                    assert (trustStorePath == null) == (trustStorePassword == null); //either both null or both non-null
                    tlsConfig = new ClientTlsConfiguration(trustStorePath, trustStorePassword, tlsValidationMode == TlsValidationMode.DEFAULT ? ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL : ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE);
                }
                return AbstractLineHttpSender.createLineSender(hosts, ports, httpPath, httpClientConfiguration, tlsConfig, actualAutoFlushRows, httpToken,
                        username, password, maxNameLength, actualMaxRetriesNanos, maxBackoffMillis, actualMinRequestThroughput, actualAutoFlushIntervalMillis, protocolVersion);
            }

            if (protocol == PROTOCOL_WEBSOCKET) {
                if (hosts.size() < 1) {
                    throw new LineSenderException("WebSocket transport requires at least one host:port pair");
                }

                int actualAutoFlushRows = autoFlushRows == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_WS_AUTO_FLUSH_ROWS : autoFlushRows;
                int actualAutoFlushBytes = autoFlushBytes == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_WS_AUTO_FLUSH_BYTES : autoFlushBytes;
                long actualAutoFlushIntervalNanos = autoFlushIntervalMillis == PARAMETER_NOT_SET_EXPLICITLY
                        ? DEFAULT_WS_AUTO_FLUSH_INTERVAL_NANOS
                        : TimeUnit.MILLISECONDS.toNanos(autoFlushIntervalMillis);

                String wsAuthHeader = buildWebSocketAuthHeader();

                ClientTlsConfiguration wsTlsConfig = null;
                if (tlsEnabled) {
                    assert (trustStorePath == null) == (trustStorePassword == null);
                    wsTlsConfig = new ClientTlsConfiguration(
                            trustStorePath,
                            trustStorePassword,
                            tlsValidationMode == TlsValidationMode.DEFAULT
                                    ? ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL
                                    : ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE
                    );
                }

                // Setting sfDir enables store-and-forward (mmap'd, recoverable
                // across sender restarts); omitting it gives memory-only mode
                // (same lock-free architecture, no disk involvement). The
                // sf_durability != memory rejection lives in validateParameters
                // so it is reached by build() and by no-connect validation alike.
                long actualSfMaxBytes = sfMaxBytes == PARAMETER_NOT_SET_EXPLICITLY
                        ? DEFAULT_SEGMENT_BYTES
                        : sfMaxBytes;
                // Default cap depends on backing: RAM (memory mode) is tight
                // by default; disk (SF mode) is cheap so the default is
                // generous enough that normal traffic never hits it.
                long defaultMaxTotal = sfDir == null
                        ? DEFAULT_MAX_BYTES_MEMORY
                        : DEFAULT_MAX_BYTES_SF;
                long actualSfMaxTotalBytes = sfMaxTotalBytes == PARAMETER_NOT_SET_EXPLICITLY
                        ? Math.max(defaultMaxTotal, actualSfMaxBytes * 2)
                        : sfMaxTotalBytes;
                long actualCloseFlushTimeoutMillis = closeFlushTimeoutMillis == CLOSE_FLUSH_TIMEOUT_NOT_SET
                        ? DEFAULT_CLOSE_FLUSH_TIMEOUT_MILLIS
                        : closeFlushTimeoutMillis;
                long actualReconnectMaxDurationMillis =
                        reconnectMaxDurationMillis == PARAMETER_NOT_SET_EXPLICITLY
                                ? CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_DURATION_MILLIS
                                : reconnectMaxDurationMillis;
                long actualReconnectInitialBackoffMillis =
                        reconnectInitialBackoffMillis == PARAMETER_NOT_SET_EXPLICITLY
                                ? CursorWebSocketSendLoop.DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS
                                : reconnectInitialBackoffMillis;
                long actualReconnectMaxBackoffMillis =
                        reconnectMaxBackoffMillis == PARAMETER_NOT_SET_EXPLICITLY
                                ? CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS
                                : reconnectMaxBackoffMillis;
                // Resolve the initial-connect mode. An explicit user choice
                // (via initialConnectMode/initialConnectRetry, or the
                // initial_connect_retry conf key) wins unconditionally --
                // including initial_connect_retry=off paired with a tuned
                // reconnect budget. When the user left it unset and tuned
                // any reconnect_* knob, promote to SYNC so the budget they
                // wrote actually applies to the first connect: the knob
                // name reads as a generic retry budget but the underlying
                // path only governs reconnects from an established
                // connection, and silently ignoring the budget on the
                // initial connect is the canonical footgun this implicit
                // upgrade removes.
                InitialConnectMode actualInitialConnectMode;
                if (initialConnectMode != null) {
                    actualInitialConnectMode = initialConnectMode;
                } else if (reconnectMaxDurationMillis != PARAMETER_NOT_SET_EXPLICITLY
                        || reconnectInitialBackoffMillis != PARAMETER_NOT_SET_EXPLICITLY
                        || reconnectMaxBackoffMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    actualInitialConnectMode = InitialConnectMode.SYNC;
                } else {
                    actualInitialConnectMode = InitialConnectMode.OFF;
                }
                long actualDurableAckKeepaliveIntervalMillis =
                        durableAckKeepaliveIntervalMillis == DURABLE_ACK_KEEPALIVE_NOT_SET
                                ? CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS
                                : durableAckKeepaliveIntervalMillis;
                int actualMaxFrameRejections = maxFrameRejections != PARAMETER_NOT_SET_EXPLICITLY
                        ? maxFrameRejections
                        : CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS;
                long actualPoisonMinEscalationWindowMillis = poisonMinEscalationWindowMillis != PARAMETER_NOT_SET_EXPLICITLY
                        ? poisonMinEscalationWindowMillis
                        : CursorWebSocketSendLoop.DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS;

                // sfDir is the parent (group root); the actual slot lives
                // under sfDir/senderId. This is what the engine sees — the
                // slot lock and segment files all live one level deeper than
                // the user-supplied path. Memory mode skips this composition
                // (slotPath stays null).
                //
                // The slot ctor inside CursorSendEngine creates the slot
                // directory itself, but Files.mkdir is non-recursive — so we
                // must ensure the parent group root exists first.
                String slotPath;
                if (sfDir == null) {
                    slotPath = null;
                } else {
                    if (!Files.exists(sfDir)) {
                        int rc = Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT);
                        // mkdir is non-zero on failure, but "already exists"
                        // is one such failure. Multiple SF senders sharing one
                        // sf_dir can be built concurrently (the pool calls
                        // build() outside its lock), so two threads can both
                        // pass the exists() check and race into mkdir; the
                        // loser gets EEXIST. Treat a benign creation race --
                        // the dir now exists -- as success and only fail when
                        // the directory is genuinely absent afterwards.
                        if (rc != 0 && !Files.exists(sfDir)) {
                            throw new LineSenderException(
                                    "could not create sf_dir: " + sfDir + " rc=" + rc);
                        }
                    }
                    slotPath = sfDir + "/" + senderId;
                }
                long actualSfAppendDeadlineNanos =
                        sfAppendDeadlineMillis == PARAMETER_NOT_SET_EXPLICITLY
                                ? CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS
                                : sfAppendDeadlineMillis * 1_000_000L;
                CursorSendEngine cursorEngine = new CursorSendEngine(
                        slotPath, actualSfMaxBytes,
                        actualSfMaxTotalBytes, actualSfAppendDeadlineNanos);
                int actualErrorInboxCapacity = errorInboxCapacity != PARAMETER_NOT_SET_EXPLICITLY
                        ? errorInboxCapacity
                        : io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher.DEFAULT_CAPACITY;
                int actualConnectionListenerInboxCapacity = connectionListenerInboxCapacity != PARAMETER_NOT_SET_EXPLICITLY
                        ? connectionListenerInboxCapacity
                        : io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher.DEFAULT_CAPACITY;
                List<QwpWebSocketSender.Endpoint> wsEndpoints =
                        new ArrayList<>(hosts.size());
                for (int i = 0, n = hosts.size(); i < n; i++) {
                    wsEndpoints.add(new QwpWebSocketSender.Endpoint(hosts.getQuick(i), ports.getQuick(i)));
                }
                QwpWebSocketSender connected;
                try {
                    connected = QwpWebSocketSender.connect(
                            wsEndpoints,
                            wsTlsConfig,
                            actualAutoFlushRows,
                            actualAutoFlushBytes,
                            actualAutoFlushIntervalNanos,
                            wsAuthHeader,
                            requestDurableAck,
                            cursorEngine,
                            actualCloseFlushTimeoutMillis,
                            actualReconnectMaxDurationMillis,
                            actualReconnectInitialBackoffMillis,
                            actualReconnectMaxBackoffMillis,
                            actualInitialConnectMode,
                            errorHandler,
                            actualErrorInboxCapacity,
                            actualDurableAckKeepaliveIntervalMillis,
                            authTimeoutMillis,
                            connectTimeoutMillis == PARAMETER_NOT_SET_EXPLICITLY ? 0 : connectTimeoutMillis,
                            connectionListener,
                            actualConnectionListenerInboxCapacity,
                            actualMaxFrameRejections,
                            actualPoisonMinEscalationWindowMillis
                    );
                } catch (Throwable t) {
                    // connect() failed before ownership of cursorEngine
                    // transferred — close it ourselves.
                    try {
                        cursorEngine.close();
                    } catch (Throwable ignored) {
                        // best-effort
                    }
                    throw t;
                }
                // connect() succeeded — `connected` now owns cursorEngine
                // via setCursorEngine(engine, true). From here on, ANY
                // failure must close `connected` (which closes the engine
                // through ownsCursorEngine), not cursorEngine directly:
                // closing the engine alone would leak the I/O thread,
                // dispatcher daemon, drainer pool, microbatch buffers and
                // WebSocketClient inside the abandoned `connected`.
                connected.setTransactional(transactional);
                try {
                    // Install the drainer listener BEFORE startOrphanDrainers
                    // below: drainers must see the listener at submit time so
                    // no early drainer event is lost to a late installation.
                    if (drainerListener != null) {
                        connected.setDrainerListener(drainerListener);
                    }
                    // Once the foreground sender is up, dispatch drainers
                    // for any sibling orphan slots. Scan AFTER we acquire
                    // our own slot lock so we never accidentally try to
                    // adopt our own data; the OrphanScanner.scan filter
                    // also excludes our sender_id.
                    if (drainOrphans && sfDir != null) {
                        io.questdb.client.std.ObjList<String> orphans =
                                io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner
                                        .scan(sfDir, senderId, orphanDrainBase, orphanDrainSlotCount);
                        if (orphans.size() > 0) {
                            org.slf4j.LoggerFactory.getLogger(LineSenderBuilder.class)
                                    .info("dispatching drainers for {} orphan slot(s) under {} "
                                                    + "(max_background_drainers={})",
                                            orphans.size(), sfDir, maxBackgroundDrainers);
                            connected.startOrphanDrainers(
                                    orphans,
                                    maxBackgroundDrainers,
                                    actualSfMaxBytes,
                                    actualSfMaxTotalBytes);
                        }
                    }
                    return connected;
                } catch (Throwable t) {
                    try {
                        connected.close();
                    } catch (Throwable ignored) {
                        // best-effort
                    }
                    throw t;
                }
            }

            if (protocol == PROTOCOL_UDP) {
                if (hosts.size() != 1 || ports.size() != 1) {
                    throw new LineSenderException("only a single address (host:port) is supported for UDP transport");
                }
                int sendToAddr = resolveIPv4(hosts.getQuick(0));
                int actualMaxDatagramSize = maxDatagramSize == PARAMETER_NOT_SET_EXPLICITLY
                        ? DEFAULT_MAX_DATAGRAM_SIZE : maxDatagramSize;
                int actualTtl = multicastTtl == PARAMETER_NOT_SET_EXPLICITLY ? 0 : multicastTtl;
                return new QwpUdpSender(nf, 0, sendToAddr, ports.getQuick(0), actualTtl, actualMaxDatagramSize);
            }

            assert protocol == PROTOCOL_TCP;

            if (hosts.size() != 1 || ports.size() != 1) {
                throw new LineSenderException("only a single address (host:port) is supported for TCP transport");
            }

            LineChannel channel = new PlainTcpLineChannel(nf, hosts.getQuick(0), ports.getQuick(0), bufferCapacity * 2);
            AbstractLineTcpSender sender;
            if (tlsEnabled) {
                DelegatingTlsChannel tlsChannel;
                try {
                    tlsChannel = new DelegatingTlsChannel(channel, trustStorePath, trustStorePassword, tlsValidationMode, hosts.getQuick(0));
                } catch (Throwable t) {
                    channel.close();
                    throw rethrow(t);
                }
                channel = tlsChannel;
            }
            try {
                switch (protocolVersion) {
                    case PROTOCOL_VERSION_V1:
                        sender = new LineTcpSenderV1(channel, bufferCapacity, maxNameLength);
                        break;
                    case PROTOCOL_VERSION_V2:
                        sender = new LineTcpSenderV2(channel, bufferCapacity, maxNameLength);
                        break;
                    case PROTOCOL_VERSION_V3:
                        sender = new LineTcpSenderV3(channel, bufferCapacity, maxNameLength);
                        break;
                    default:
                        throw new LineSenderException("unknown protocol version [version=").put(protocolVersion).put("]");
                }
            } catch (Throwable t) {
                channel.close();
                throw rethrow(t);
            }
            if (privateKey != null) {
                try {
                    sender.authenticate(keyId, privateKey);
                } catch (Throwable t) {
                    sender.close();
                    throw rethrow(t);
                } finally {
                    if (shouldDestroyPrivKey) {
                        try {
                            privateKey.destroy();
                        } catch (DestroyFailedException e) {
                            // not much we can do
                        }
                    }
                }
            }
            return sender;
        }

        /**
         * close() drain timeout in milliseconds. The sender's {@code close()}
         * method blocks up to this many millis waiting for the server to ACK
         * every batch already published into the engine before shutting down
         * the I/O loop. Default {@code 60000} (60 s) -- generous enough to
         * survive real-workload backlogs (slow consumers, catch-up replicas,
         * chunky payloads on small server send buffers) without silently
         * dropping unacked rows; callers that need a longer pre-close wait
         * for a specific submission can call
         * {@link Sender#drain(long)} explicitly before close().
         * <p>
         * Set to {@code 0} or {@code -1} to opt out — close() will not wait
         * at all (fast close). Pending data is then lost in memory mode and
         * recovered by the next sender in SF mode.
         * <p>
         * WebSocket transport only.
         */
        public LineSenderBuilder closeFlushTimeoutMillis(long timeoutMillis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("close_flush_timeout_millis is only supported for WebSocket transport");
            }
            this.closeFlushTimeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * Sets the async listener invoked on every connection-state transition:
         * initial connect, primary failover, individual endpoint attempt
         * failures, the full address list being unreachable, and terminal
         * auth/budget rejections. The listener runs on a dedicated daemon
         * dispatcher thread, never on the I/O thread or producer thread; slow
         * listeners cannot stall publishing or reconnect. If the bounded inbox
         * fills up, surplus events are dropped (visible via
         * {@code QwpWebSocketSender.getDroppedConnectionNotifications()}).
         *
         * <p>WebSocket transport only; setting on other transports throws.
         *
         * @param listener the listener; {@code null} resets to the loud-not-silent default
         * @return this instance for method chaining
         */
        public LineSenderBuilder connectionListener(io.questdb.client.SenderConnectionListener listener) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("connection_listener is only supported for WebSocket transport");
            }
            this.connectionListener = listener;
            return this;
        }

        /**
         * Sets the bounded inbox capacity used by the async connection-event
         * dispatcher. When the inbox fills up, additional events are dropped
         * and counted. Default 64.
         *
         * <p>WebSocket transport only; setting on other transports throws.
         *
         * @param capacity must be {@code >= 1}
         * @return this instance for method chaining
         */
        public LineSenderBuilder connectionListenerInboxCapacity(int capacity) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("connection_listener_inbox_capacity is only supported for WebSocket transport");
            }
            if (capacity < 1) {
                throw new LineSenderException("connection_listener_inbox_capacity must be >= 1, was " + capacity);
            }
            this.connectionListenerInboxCapacity = capacity;
            return this;
        }

        /**
         * Disables automatic flushing of buffered data.
         * <p>
         * The Sender buffers data locally before flushing it to a server. This method disables automatic flushing, requiring
         * explicit invocation of {@link #flush()} to send buffered data to the server. It also disables automatic flushing
         * upon closing. This grants fine control over batching behavior.
         * <br>
         * The QuestDB server processes a batch as a single transaction, provided all rows in the batch target the same table.
         * Therefore, you can use this method to explicitly control transaction boundaries and ensure atomic processing of all
         * data in a batch. To maintain atomicity, ensure that all data in a batch is sent to the same table.
         * <p>
         * It is essential to ensure the maximum buffer capacity is sufficient to accommodate all locally buffered data.
         * <p>
         * This method should only be used when communicating via the HTTP protocol. Calling this method is illegal when
         * communicating over the TCP protocol.
         *
         * @return this instance for method chaining
         * @see #autoFlushRows(int)
         * @see #maxBufferCapacity(int)
         */
        public LineSenderBuilder disableAutoFlush() {
            if (this.autoFlushRows != PARAMETER_NOT_SET_EXPLICITLY && this.autoFlushRows != AUTO_FLUSH_DISABLED) {
                throw new LineSenderException("auto flush rows was already configured ")
                        .put("[autoFlushRows=").put(this.autoFlushRows).put("]");
            }
            if (this.autoFlushIntervalMillis != PARAMETER_NOT_SET_EXPLICITLY && this.autoFlushIntervalMillis != Integer.MAX_VALUE) {
                throw new LineSenderException("auto flush interval was already configured ")
                        .put("[autoFlushIntervalMillis=").put(this.autoFlushIntervalMillis).put("]");
            }

            this.autoFlushRows = AUTO_FLUSH_DISABLED;
            this.autoFlushIntervalMillis = Integer.MAX_VALUE;
            return this;
        }

        /**
         * Sets the async listener observing background orphan-slot drainer
         * events: per-attempt durable-ack capability-gap retries
         * ({@code onDurableAckUnavailable}), transient all-replica failover
         * windows ({@code onPrimaryUnavailable}), and the eventual escalation
         * to a {@code .failed} sentinel
         * ({@code onDurableAckPersistentFailure}). The listener runs on the
         * drainers' own threads, so it must be thread-safe and must not block
         * — hand off to a queue or metrics sink and return. Only meaningful
         * when {@link #drainOrphans(boolean)} is enabled.
         *
         * <p>WebSocket transport only; setting on other transports throws.
         *
         * @param listener the listener; {@code null} keeps the default (no listener)
         * @return this instance for method chaining
         */
        public LineSenderBuilder drainerListener(
                io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener listener) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("drainer_listener is only supported for WebSocket transport");
            }
            this.drainerListener = listener;
            return this;
        }

        /**
         * Opt in to adopting sibling slots under {@code <sf_dir>/*} at
         * startup that hold unacked data left behind by a crashed sender or
         * a different sender_id. Default {@code false}. WebSocket only;
         * requires {@code sf_dir} to be set.
         * <p>
         * On startup, after the foreground sender has acquired its own slot
         * lock, the scan walks every sibling slot directory and dispatches a
         * background drainer for each candidate orphan. Each drainer takes
         * the slot's exclusive lock, replays the slot's unacked frames over
         * its own WebSocket connection to the same target, and unlinks the
         * slot once fully drained. Concurrency is capped by
         * {@link #maxBackgroundDrainers(int)} (default {@code 4}).
         * <p>
         * Slots flagged with the {@code .failed} sentinel are skipped
         * (manual reset required), and the foreground sender's own slot is
         * never adopted.
         * <p>
         * Close-latency note: {@code close()} stops adopted drainers. A
         * drainer still connecting (e.g. during an outage) is stop-signaled
         * immediately and exits within ~50ms; a drainer actively replaying
         * frames is given a ~2.5s grace window to finish, plus a 0.5s stop
         * window — so {@code close()} may take up to ~3s while orphan
         * drainers are in flight (and a drainer parked in a blocking native
         * connect is abandoned to exit on its own daemon thread).
         * Un-drained slots stay on disk and are re-adopted by the next
         * sender that enables {@code drain_orphans}.
         */
        public LineSenderBuilder drainOrphans(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("drain_orphans is only supported for WebSocket transport");
            }
            this.drainOrphans = enabled;
            return this;
        }

        /**
         * Cadence (in millis) at which the SF cursor I/O loop sends a
         * keepalive PING while waiting on STATUS_DURABLE_ACK frames. The
         * server only flushes pending durable-ack frames in response to
         * inbound recv events, so an idle opted-in client needs to prod
         * the server periodically. Effective only when
         * {@link #requestDurableAck(boolean) request_durable_ack=on}.
         * <p>
         * Pass {@code 0} or negative to disable keepalive PINGs entirely
         * — the caller then accepts that durable-ack frames may not
         * arrive on idle connections (e.g. they are sending data
         * continuously, or have their own ping driver).
         * <p>
         * Default
         * {@value io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop#DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS}
         * ms. WebSocket transport only.
         */
        public LineSenderBuilder durableAckKeepaliveIntervalMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException(
                        "durable_ack_keepalive_interval_millis is only supported for WebSocket transport");
            }
            this.durableAckKeepaliveIntervalMillis = millis;
            return this;
        }

        /**
         * Configure authentication. This is needed when QuestDB server required clients to authenticate.
         * <br>
         * This is only used when communicating over TCP transport, and it's illegal to call this method when
         * communicating over HTTP transport.
         *
         * @param keyId keyId the client will send to a server.
         * @return an instance of {@link AuthBuilder}. As to finish authentication configuration.
         * @see #httpToken(String)
         * @see #httpUsernamePassword(String, String)
         */
        public LineSenderBuilder.AuthBuilder enableAuth(String keyId) {
            if (this.keyId != null) {
                throw new LineSenderException("authentication keyId was already configured ")
                        .put("[keyId=").put(this.keyId).put("]");
            }
            if (Chars.isBlank(keyId)) {
                throw new LineSenderException("keyId cannot be empty nor null");
            }
            this.keyId = keyId;
            return new LineSenderBuilder.AuthBuilder();
        }

        /**
         * Instruct a client to use TLS when connecting to a QuestDB server
         *
         * @return this instance for method chaining.
         */
        public LineSenderBuilder enableTls() {
            if (tlsEnabled) {
                throw new LineSenderException("tls was already enabled");
            }
            tlsEnabled = true;
            return this;
        }

        /**
         * Sets the async error handler invoked for every server-side rejection.
         * The handler runs on a dedicated daemon dispatcher thread, never on the
         * I/O thread or producer thread. Slow handlers do not stall publishing;
         * if the bounded inbox fills up, surplus notifications are dropped
         * (visible via {@code QwpWebSocketSender.getDroppedErrorNotifications()}).
         *
         * <p>WebSocket transport only; setting on other transports throws.
         *
         * @param handler the handler; {@code null} resets to the loud-not-silent default
         * @return this instance for method chaining
         */
        public LineSenderBuilder errorHandler(io.questdb.client.SenderErrorHandler handler) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("error_handler is only supported for WebSocket transport");
            }
            this.errorHandler = handler;
            return this;
        }

        /**
         * Sets the bounded inbox capacity used by the async error dispatcher.
         * When the inbox fills up, additional notifications are dropped and
         * counted. Default 256.
         *
         * <p>WebSocket transport only; setting on other transports throws.
         *
         * @param capacity must be {@code >= 16} (sf-client.md section 4.4).
         *                 The floor exists because overflow drops the oldest
         *                 entry and watermarks are monotonic, so the inbox
         *                 must be wide enough to keep a useful trailing
         *                 window of categories under bursty errors.
         * @return this instance for method chaining
         */
        public LineSenderBuilder errorInboxCapacity(int capacity) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("error_inbox_capacity is only supported for WebSocket transport");
            }
            if (capacity < MIN_ERROR_INBOX_CAPACITY) {
                throw new LineSenderException("error_inbox_capacity must be >= "
                        + MIN_ERROR_INBOX_CAPACITY + ", was " + capacity);
            }
            this.errorInboxCapacity = capacity;
            return this;
        }

        /**
         * Path component of the HTTP URL.
         * <br>
         * This is only used when communicating over HTTP transport.
         *
         * @param path HTTP path
         * @return this instance for method chaining
         */
        public LineSenderBuilder httpPath(String path) {
            if (protocol == PROTOCOL_TCP) {
                throw new LineSenderException("HTTP path is not supported for TCP protocol");
            }
            if (protocol == PROTOCOL_UDP) {
                throw new LineSenderException("HTTP path is not supported for UDP transport");
            }
            if (protocol == PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("HTTP path is not supported for WebSocket protocol");
            }
            if (this.httpPath != null) {
                throw new LineSenderException("path was already configured");
            }
            if (Chars.isBlank(path)) {
                throw new LineSenderException("path cannot be empty nor null");
            }
            if (!Chars.startsWith(path, '/')) {
                throw new LineSenderException("the path has to start with '/'");
            }
            this.httpPath = path;
            return this;
        }

        /**
         * Sets the HTTP path for auto-detecting the line protocol version when #protocolVersion is not explicitly set.
         * <ul>
         *   <li>only for HTTP transport.</li>
         *   <li>Mandatory when the server uses a <b>non-default</b> {@code http.context.settings} configuration.</li>
         * </ul>
         *
         * <b>Example:</b> If the server configures {@code http.context.settings=/custom/settings},
         * call {@code httpSettingPath("/custom/settings")}.
         * <p>
         * This is only used when communicating over HTTP transport.
         *
         * @param path The HTTP path to query for server protocol settings. Must:
         *             <ul>
         *               <li>Start with '/'</li>
         *               <li>Match the server's {@code http.context.settings} value if non-default</li>
         *             </ul>
         * @return this instance for method chaining
         */
        @SuppressWarnings("unused")
        public LineSenderBuilder httpSettingPath(String path) {
            if (protocol == PROTOCOL_TCP) {
                throw new LineSenderException("HTTP settings path is not supported for TCP protocol");
            }
            if (protocol == PROTOCOL_UDP) {
                throw new LineSenderException("HTTP settings path is not supported for UDP transport");
            }
            if (protocol == PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("HTTP settings path is not supported for WebSocket protocol");
            }
            if (this.httpSettingsPath != null) {
                throw new LineSenderException("the path was already configured");
            }
            if (Chars.isBlank(path)) {
                throw new LineSenderException("the path cannot be empty nor null");
            }
            if (!Chars.startsWith(path, '/')) {
                throw new LineSenderException("the path has to start with '/'");
            }
            this.httpSettingsPath = path;
            return this;
        }

        /**
         * Set timeout is milliseconds for HTTP requests.
         * <br>
         * This is only used when communicating over HTTP transport, and it's illegal to call this method when
         * communicating over TCP transport.
         *
         * @param httpTimeoutMillis timeout is milliseconds for HTTP requests.
         * @return this instance for method chaining
         */
        public LineSenderBuilder httpTimeoutMillis(int httpTimeoutMillis) {
            if (this.httpTimeout != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("HTTP timeout was already configured ")
                        .put("[timeout=").put(this.httpTimeout).put("]");
            }
            if (httpTimeoutMillis < 1) {
                throw new LineSenderException("HTTP timeout must be positive ")
                        .put("[timeout=").put(httpTimeoutMillis).put("]");

            }
            this.httpTimeout = httpTimeoutMillis;
            return this;
        }

        /**
         * Use HTTP Authentication token.
         * <br>
         * This is only used when communicating over HTTP and WebSocket transport, and it's illegal to
         * call this method when communicating over TCP or UDP transport.
         *
         * @param token HTTP authentication token
         * @return this instance for method chaining
         */
        public LineSenderBuilder httpToken(String token) {
            if (this.username != null) {
                throw new LineSenderException("authentication username was already configured ")
                        .put("[username=").put(this.username).put("]");
            }
            if (this.httpToken != null) {
                throw new LineSenderException("token was already configured");
            }
            if (Chars.isBlank(token)) {
                throw new LineSenderException("token cannot be empty nor null");
            }
            this.httpToken = token;
            return this;
        }

        /**
         * Use username and password for authentication when communicating over HTTP or WebSocket protocol.
         * <br>
         * This is only used when communicating over HTTP and WebSocket transport, and it's illegal to call this method when
         * communicating over TCP or UDP transport.
         *
         * @param username username
         * @param password password
         * @return this instance for method chaining
         * @see #httpToken(String)
         */
        public LineSenderBuilder httpUsernamePassword(String username, String password) {
            if (this.username != null) {
                throw new LineSenderException("authentication username was already configured ")
                        .put("[username=").put(this.username).put("]");
            }
            if (Chars.isBlank(username)) {
                throw new LineSenderException("username cannot be empty nor null");
            }
            if (Chars.isBlank(password)) {
                throw new LineSenderException("password cannot be empty nor null");
            }
            if (httpToken != null) {
                throw new LineSenderException("token authentication is already configured");
            }
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Three-way control over initial-connect behavior — see
         * {@link InitialConnectMode} for the value semantics, including
         * the default-resolution rule when this method is not called.
         * Calling this pins the mode and suppresses the implicit upgrade
         * to SYNC that otherwise fires when a {@code reconnect_*} knob is
         * tuned. WebSocket transport only. Replaces
         * {@link #initialConnectRetry(boolean)} for users who want the
         * {@link InitialConnectMode#ASYNC} mode.
         */
        public LineSenderBuilder initialConnectMode(InitialConnectMode mode) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("initial_connect_mode is only supported for WebSocket transport");
            }
            if (mode == null) {
                throw new LineSenderException("initial_connect_mode cannot be null");
            }
            this.initialConnectMode = mode;
            return this;
        }

        /**
         * Opt in to retrying the initial connect with the same backoff /
         * cap / auth-terminal policy as in-flight reconnect. Set true if
         * your deployment expects the server to come up shortly after the
         * sender. Auth failures (HTTP 401/403/non-101) stay terminal in
         * either mode.
         * <p>
         * When this method is not called, the resolution rule documented
         * on {@link InitialConnectMode} applies: SYNC implicitly when any
         * {@code reconnect_*} knob was tuned, otherwise OFF. Calling this
         * with either {@code true} or {@code false} pins the mode and
         * suppresses the implicit upgrade.
         * <p>
         * For non-blocking startup (the producer thread returns immediately
         * and the I/O thread retries in the background), use
         * {@link #initialConnectMode(InitialConnectMode)} with
         * {@link InitialConnectMode#ASYNC}.
         *
         * @deprecated Superseded by {@link #initialConnectMode(InitialConnectMode)},
         * which exposes the third {@link InitialConnectMode#ASYNC} mode this
         * boolean cannot reach. {@code initialConnectRetry(true)} is equivalent to
         * {@code initialConnectMode(InitialConnectMode.SYNC)};
         * {@code initialConnectRetry(false)} is equivalent to
         * {@code initialConnectMode(InitialConnectMode.OFF)}.
         */
        @Deprecated
        public LineSenderBuilder initialConnectRetry(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("initial_connect_retry is only supported for WebSocket transport");
            }
            this.initialConnectMode = enabled ? InitialConnectMode.SYNC : InitialConnectMode.OFF;
            return this;
        }

        /**
         * Cap on concurrent background drainer threads when
         * {@link #drainOrphans(boolean)} is on. Default {@code 4}. Each
         * drainer carries one segment-manager thread + one I/O thread +
         * one socket, so users running many senders per JVM should set
         * this low.
         */
        public LineSenderBuilder maxBackgroundDrainers(int n) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("max_background_drainers is only supported for WebSocket transport");
            }
            if (n < 0) {
                throw new LineSenderException("max_background_drainers must be >= 0: ").put(n);
            }
            this.maxBackgroundDrainers = n;
            return this;
        }

        /**
         * Configures the maximum backoff time between retry attempts when the Sender encounters recoverable errors.
         * <br>
         * This setting is applicable only when communicating over the HTTP transport, and it is illegal to invoke this
         * method when communicating over the TCP transport.
         * <p>
         * The Sender uses exponential backoff with jitter for retry operations. The backoff time starts at a small value
         * and doubles with each retry attempt, up to the maximum value specified here. This helps prevent overwhelming
         * the server during temporary outages while still providing quick recovery when the service becomes available again.
         * <p>
         * This parameter works in conjunction with {@link #retryTimeoutMillis(int)}. While retryTimeoutMillis sets
         * the total time the Sender will spend retrying, maxBackoffMillis controls the maximum delay between individual
         * retry attempts.
         * <p>
         * Setting this value to zero effectively disables the backoff mechanism, causing retries to occur with minimal
         * delay (though some small jitter is still applied).
         * <p>
         * Default value: 1,000 milliseconds (1 second).
         *
         * @param maxBackoffMillis the maximum backoff time between retry attempts in milliseconds.
         * @return this instance, enabling method chaining.
         * @throws LineSenderException if maxBackoffMillis is negative, if this method is called for TCP protocol,
         *                             or if maxBackoffMillis was already configured.
         */
        public LineSenderBuilder maxBackoffMillis(int maxBackoffMillis) {
            if (this.maxBackoffMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("max backoff was already configured ")
                        .put("[maxBackoffMillis=").put(this.maxBackoffMillis).put("]");
            }
            if (maxBackoffMillis < 0) {
                throw new LineSenderException("max backoff cannot be negative ")
                        .put("[maxBackoffMillis=").put(maxBackoffMillis).put("]");
            }
            if (protocol == PROTOCOL_TCP) {
                throw new LineSenderException("max backoff is not supported for TCP protocol");
            }
            this.maxBackoffMillis = maxBackoffMillis;
            return this;
        }

        /**
         * Set the maximum local buffer capacity in bytes.
         * <br>
         * This is a hard limit on the maximum buffer capacity. The buffer cannot grow beyond this limit and Sender
         * will throw an exception if you try to accommodate more data in the buffer. To prevent this from happening
         * you should call {@link #flush()} periodically or set {@link #autoFlushRows(int)} to make sure that
         * Sender will flush the buffer automatically before it reaches the maximum capacity.
         * <br>
         * This is only used when communicating over HTTP transport since TCP transport uses a fixed buffer size.
         * <br>
         * Default value: 100 MB
         *
         * @param maximumBufferCapacity maximum buffer capacity in bytes.
         * @return this instance for method chaining
         */
        public LineSenderBuilder maxBufferCapacity(int maximumBufferCapacity) {
            if (protocol == PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("maximum buffer capacity is not supported for WebSocket transport");
            }
            if (maximumBufferCapacity < DEFAULT_BUFFER_CAPACITY) {
                throw new LineSenderException("maximum buffer capacity cannot be less than initial buffer capacity ")
                        .put("[maximumBufferCapacity=").put(maximumBufferCapacity)
                        .put(", initialBufferCapacity=").put(DEFAULT_BUFFER_CAPACITY)
                        .put("]");
            }
            this.maximumBufferCapacity = maximumBufferCapacity;
            return this;
        }

        /**
         * Set the maximum datagram size in bytes for UDP transport. Only valid for UDP transport.
         * <br>
         * The practical limit depends on the network MTU (typically 1500 bytes for Ethernet).
         * <br>
         * Default value: 1400 bytes
         *
         * @param maxDatagramSize maximum datagram size in bytes
         * @return this instance for method chaining
         */
        public LineSenderBuilder maxDatagramSize(int maxDatagramSize) {
            if (this.maxDatagramSize != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("max datagram size was already configured ")
                        .put("[maxDatagramSize=").put(this.maxDatagramSize).put("]");
            }
            if (maxDatagramSize < 1) {
                throw new LineSenderException("max datagram size must be positive ")
                        .put("[maxDatagramSize=").put(maxDatagramSize).put("]");
            }
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_UDP) {
                throw new LineSenderException("max datagram size is only supported for UDP transport");
            }
            this.maxDatagramSize = maxDatagramSize;
            return this;
        }

        /**
         * Set the maximum length of a table or column name in bytes.
         * Matches the `cairo.max.file.name.length` setting in the server.
         * The default is 127 bytes.
         * If running over HTTP and protocol version 2 is auto-negotiated, this
         * value is picked up from the server.
         */
        public LineSenderBuilder maxNameLength(int maxNameLength) {
            if (this.maxNameLength != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("max name length was already configured ")
                        .put("[max_name_len=").put(this.maxNameLength).put("]");
            }
            if (maxNameLength < 16) {
                throw new LineSenderException("max_name_len must be at least 16 bytes ")
                        .put("[max_name_len=").put(maxNameLength).put("]");
            }
            this.maxNameLength = maxNameLength;
            return this;
        }

        /**
         * Minimum expected throughput in bytes per second for HTTP requests.
         * <br>
         * If the throughput is lower than this value, the connection will time out.
         * The value is expressed as a number of bytes per second. This is used to calculate additional request timeout,
         * on top of {@link #httpTimeoutMillis(int)}
         * <br>
         * This is useful when you are sending large batches of data, and you want to ensure that the connection
         * does not time out while sending the batch. Setting this to 0 disables the throughput calculation and the
         * connection will only time out based on the {@link #httpTimeoutMillis(int)} value.
         * <p>
         * The default is 100 KiB/s.
         * This is only used when communicating over HTTP transport, and it's illegal to call this method when
         * communicating over TCP transport.
         *
         * @param minRequestThroughput minimum expected throughput in bytes per second for HTTP requests.
         * @return this instance for method chaining
         */
        public LineSenderBuilder minRequestThroughput(int minRequestThroughput) {
            if (minRequestThroughput < 1) {
                throw new LineSenderException("minimum request throughput must not be negative ")
                        .put("[minRequestThroughput=").put(minRequestThroughput).put("]");
            }
            this.minRequestThroughput = minRequestThroughput;
            return this;
        }

        /**
         * Set the multicast TTL for UDP transport. Only valid for UDP transport.
         * <br>
         * Valid range: 0-255.
         * <br>
         * Default value: 0 (restricted to same host). Set to 1 for local subnet.
         *
         * @param multicastTtl multicast TTL value
         * @return this instance for method chaining
         */
        public LineSenderBuilder multicastTtl(int multicastTtl) {
            if (this.multicastTtl != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("multicast TTL was already configured ")
                        .put("[multicastTtl=").put(this.multicastTtl).put("]");
            }
            if (multicastTtl < 0) {
                throw new LineSenderException("multicast TTL cannot be negative ")
                        .put("[multicastTtl=").put(multicastTtl).put("]");
            }
            if (multicastTtl > 255) {
                throw new LineSenderException("multicast TTL cannot exceed 255 ")
                        .put("[multicastTtl=").put(multicastTtl).put("]");
            }
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_UDP) {
                throw new LineSenderException("multicast TTL is only supported for UDP transport");
            }
            this.multicastTtl = multicastTtl;
            return this;
        }

        /**
         * Set port where a QuestDB server is listening on.
         *
         * @param port port where a QuestDB server is listening on.
         * @return this instance for method chaining
         */
        public LineSenderBuilder port(int port) {
            if (port < 1 || port > 65535) {
                throw new LineSenderException("invalid port [port=").put(port).put("]");
            }
            this.ports.add(port);
            return this;
        }

        /**
         * Sets the protocol version used by the client to connect to the server.
         * <p>
         * The client currently supports {@link #PROTOCOL_VERSION_V1}, {@link #PROTOCOL_VERSION_V2} and
         * {@link #PROTOCOL_VERSION_V3} (default).
         * <p>
         * In most cases, this method should not be called. Set {@link #PROTOCOL_VERSION_V1} only when connecting to a legacy server.
         * <p>
         *
         * @param protocolVersion The desired protocol version.
         * @return This instance for method chaining.
         */
        public LineSenderBuilder protocolVersion(int protocolVersion) {
            if (this.protocolVersion != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol version was already configured ")
                        .put("[protocolVersion=").put(this.protocolVersion).put("]");
            }
            if (protocolVersion < PROTOCOL_VERSION_V1 || protocolVersion > PROTOCOL_VERSION_V3) {
                throw new LineSenderException("current client only supports protocol version 1(text format for all datatypes), " +
                        "2(binary format for part datatypes), 3(decimal datatype) or explicitly unset");
            }
            this.protocolVersion = protocolVersion;
            return this;
        }

        /**
         * Initial reconnect backoff in millis. Doubled (with jitter) each
         * failed attempt, capped at {@link #reconnectMaxBackoffMillis(long)}.
         * Default {@code 100}. WebSocket only.
         */
        public LineSenderBuilder reconnectInitialBackoffMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("reconnect_initial_backoff_millis is only supported for WebSocket transport");
            }
            if (millis <= 0) {
                throw new LineSenderException("reconnect_initial_backoff_millis must be > 0: ").put(millis);
            }
            this.reconnectInitialBackoffMillis = millis;
            return this;
        }

        /**
         * Poison-frame detector threshold: consecutive server rejections
         * (retriable NACK, or non-orderly close after a send) of the SAME
         * head-of-line frame, with no ack progress in between, before the
         * sender declares the frame poisoned and latches a typed terminal
         * instead of reconnect-replaying forever. Retriable rejections below
         * the threshold recycle the connection and replay from the
         * store-and-forward log — no data is dropped either way.
         * Default {@code 4}. WebSocket only.
         */
        public LineSenderBuilder maxFrameRejections(int rejections) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("max_frame_rejections is only supported for WebSocket transport");
            }
            if (rejections < 1) {
                throw new LineSenderException("max_frame_rejections must be >= 1: ").put(rejections);
            }
            this.maxFrameRejections = rejections;
            return this;
        }

        /**
         * Minimum wall-clock dwell (millis) a suspected frame must stay
         * poisoned before the poison detector escalates to a typed terminal,
         * even once {@link #maxFrameRejections(int)} strikes have accrued.
         * With paced recycles, strikes can accrue in well under a second (e.g.
         * a middlebox/LB that accepts the connection then non-orderly-closes
         * each cycle while its backend is briefly down), so a strike count
         * alone can turn a transient outage into a producer-fatal terminal.
         * This window guarantees a brief outage a chance to clear (an OK
         * at/beyond the suspect resets the detector) first. {@code 0} disables
         * the dwell (legacy immediate escalation at the strike threshold).
         * Default {@code 5_000} (5 s). WebSocket only.
         */
        public LineSenderBuilder poisonMinEscalationWindowMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("poison_min_escalation_window_millis is only supported for WebSocket transport");
            }
            if (millis < 0) {
                throw new LineSenderException("poison_min_escalation_window_millis must be >= 0: ").put(millis);
            }
            this.poisonMinEscalationWindowMillis = millis;
            return this;
        }

        /**
         * Max reconnect backoff in millis. Caps the exponential growth so
         * a long outage doesn't end up sleeping minutes between attempts.
         * Default {@code 5_000} (5 s). WebSocket only.
         */
        public LineSenderBuilder reconnectMaxBackoffMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("reconnect_max_backoff_millis is only supported for WebSocket transport");
            }
            if (millis <= 0) {
                throw new LineSenderException("reconnect_max_backoff_millis must be > 0: ").put(millis);
            }
            this.reconnectMaxBackoffMillis = millis;
            return this;
        }

        /**
         * Cap on the blocking initial-connect retry budget when
         * {@code initial_connect_retry=sync}. {@code fromConfig} retries
         * with exponential backoff until connect succeeds or this many
         * millis elapse, then throws. The background reconnect loop
         * (mid-stream outages and async initial connect) does NOT consult
         * this value: it retries indefinitely and halts only on a terminal
         * auth/upgrade error or {@code close()}.
         * <p>
         * Default {@code 300_000} (5 minutes). Lower for fail-fast startup;
         * higher for tolerating a slow server boot. Must be positive: a zero
         * budget would make the SYNC initial connect throw without a single
         * attempt (even against a healthy server) and would collapse the
         * background drainer's durable-ack settle budget to its first sweep.
         * WebSocket only.
         */
        public LineSenderBuilder reconnectMaxDurationMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("reconnect_max_duration_millis is only supported for WebSocket transport");
            }
            if (millis <= 0) {
                throw new LineSenderException("reconnect_max_duration_millis must be > 0: ").put(millis);
            }
            this.reconnectMaxDurationMillis = millis;
            return this;
        }

        /**
         * Opts the connection in for STATUS_DURABLE_ACK frames. When enabled,
         * servers with primary replication will emit per-table durable-upload
         * watermarks as WAL data reaches the object store.
         * <p>
         * This setting is only supported for WebSocket transport.
         *
         * @param enabled true to request durable ACKs
         * @return this instance for method chaining
         */
        public LineSenderBuilder requestDurableAck(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("request_durable_ack is only supported for WebSocket transport");
            }
            this.requestDurableAck = enabled;
            return this;
        }

        /**
         * Enables transactional mode. Auto-flush sends data to the server
         * with {@code FLAG_DEFER_COMMIT}; only an explicit {@code flush()}
         * triggers the server-side WAL commit. This allows accumulating
         * datasets larger than the server's recv buffer while committing
         * atomically per table.
         *
         * @param enabled true to enable transactional mode
         * @return this instance for method chaining
         */
        public LineSenderBuilder transactional(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("transactional is only supported for WebSocket transport");
            }
            this.transactional = enabled;
            return this;
        }

        /**
         * Configures the maximum time the Sender will spend retrying upon receiving a recoverable error from the server.
         * <br>
         * This setting is applicable only when communicating over the HTTP transport, and it is illegal to invoke this
         * method when communicating over the TCP transport.
         * <p>
         * Recoverable errors are those not caused by the client sending invalid data to the server. For instance,
         * connection issues or server outages are considered recoverable errors, whereas attempts to send a row
         * with an incorrect data type are not.
         * <p>
         * Setting this value to zero disables retries entirely. In such cases, the Sender will throw an exception
         * immediately. It's important to note that the Sender does not retry operations that fail
         * during {@link #close()}. Therefore, it is recommended to explicitly call {@link #flush()} before closing
         * the Sender.
         * <p>
         * <b>Warning:</b> Retrying may lead to data duplication. It is advisable to use
         * <a href="https://questdb.io/docs/concept/deduplication/">QuestDB deduplication</a> to mitigate this risk.
         * <p>
         * Default value: 10,000 milliseconds.
         *
         * @param retryTimeoutMillis the maximum retry duration in milliseconds.
         * @return this instance, enabling method chaining.
         */
        public LineSenderBuilder retryTimeoutMillis(int retryTimeoutMillis) {
            if (this.retryTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("retry timeout was already configured ")
                        .put("[retryTimeoutMillis=").put(this.retryTimeoutMillis).put("]");
            }
            if (retryTimeoutMillis < 0) {
                throw new LineSenderException("retry timeout cannot be negative ")
                        .put("[retryTimeoutMillis=").put(retryTimeoutMillis).put("]");
            }
            if (protocol == PROTOCOL_TCP) {
                throw new LineSenderException("retrying is not supported for TCP protocol");
            }
            this.retryTimeoutMillis = retryTimeoutMillis;
            return this;
        }

        /**
         * Names this sender's slot inside the SF group root (see
         * {@link #storeAndForwardDir(String)}). The actual on-disk slot is
         * {@code <sf_dir>/<sender_id>/}, locked exclusively for the sender's
         * lifetime via {@code flock}. Default is {@code "default"}.
         * <p>
         * Multi-sender deployments writing to the same group root MUST set
         * this to a distinct value per sender; the second sender to start
         * with a colliding id fails fast with "sf slot already in use".
         * <p>
         * Allowed characters: letters, digits, {@code _ -}. No path
         * separators, no {@code .}, no spaces — the id is used verbatim as
         * a directory name.
         */
        public LineSenderBuilder senderId(String id) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("sender_id is only supported for WebSocket transport");
            }
            validateSenderId(id);
            this.senderId = id;
            return this;
        }

        /**
         * The slot id ({@code sender_id}) currently configured on this
         * builder, either parsed from the config string or left at its
         * {@code "default"} default. Introspection hook for the connection
         * pool, which derives a distinct per-slot id from this base so that
         * multiple pooled senders sharing one {@code sf_dir} don't collide
         * on the slot {@code flock}.
         */
        public String getConfiguredSenderId() {
            return senderId;
        }

        /**
         * The store-and-forward group root ({@code sf_dir}) currently
         * configured on this builder, or {@code null} when SF is disabled.
         * Introspection hook for the connection pool, which needs the group
         * root to locate its own managed slot dirs {@code <sf_dir>/<base>-<i>}
         * when recovering unacked data a previous run left behind.
         */
        public String getConfiguredSfDir() {
            return sfDir;
        }

        /**
         * Excludes the connection pool's <em>live</em> slot set from
         * {@link #drainOrphans(boolean)} scanning: a sibling slot under
         * {@code sf_dir} named {@code <base>-<index>} with
         * {@code 0 <= index < slotCount} is never treated as a drainable orphan.
         * <p>
         * Internal introspection hook for the connection pool. The pool gives
         * each pooled SF sender a distinct slot id {@code <base>-<index>} and
         * recovers each slot's unacked data itself when it (re)creates that
         * slot. Without this exclusion, one pooled sender's startup drainer
         * could adopt a sibling pool slot's lock and dir, reintroducing the
         * very "sf slot already in use" collision the per-slot ids were added
         * to prevent.
         * <p>
         * Unlike a blanket {@code <base>-} prefix exclusion, the bound is the
         * pool's {@code maxSize}: a same-base slot whose index is at or above
         * {@code slotCount} (e.g. {@code <base>-3} left behind by a larger pool
         * before {@code maxSize} shrank from 4 to 2) is NOT excluded and is
         * drained like any foreign leftover, so its unacked data is recovered
         * instead of being silently stranded. Foreign leftovers (a different
         * base, or a bare un-suffixed id) are also still drained.
         * <p>
         * Pass a {@code null}/empty base or {@code slotCount <= 0} to disable
         * the exclusion (the default).
         */
        public LineSenderBuilder orphanDrainExcludeManagedSlots(String base, int slotCount) {
            this.orphanDrainBase = base;
            this.orphanDrainSlotCount = slotCount;
            return this;
        }

        /**
         * True iff store-and-forward is enabled (an {@code sf_dir} was set).
         * Introspection hook for the connection pool: SF senders own an
         * exclusive on-disk slot, so each pooled sender needs its own slot
         * id, whereas non-SF (memory-mode / HTTP / TCP) senders share no
         * such resource and need no per-slot identity.
         */
        public boolean isStoreAndForwardEnabled() {
            return sfDir != null;
        }

        /**
         * Per-call deadline for {@code Sender.flush()} spinning on a full
         * cursor segment ring waiting for ACKs to drain space. Default
         * 30 s. Lower for fail-fast services that prefer surfacing
         * backpressure as an error; raise for offline-tolerant pipelines
         * that should ride out long server pauses.
         */
        public LineSenderBuilder sfAppendDeadlineMillis(long millis) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("sf_append_deadline_millis is only supported for WebSocket transport");
            }
            if (millis <= 0) {
                throw new LineSenderException("sf_append_deadline_millis must be > 0: ").put(millis);
            }
            this.sfAppendDeadlineMillis = millis;
            return this;
        }

        /**
         * Enables store-and-forward and sets its directory. Setting the SF
         * directory <i>is</i> the on-switch — there is no separate
         * enable/disable flag. SF is off iff {@code dir} was never set.
         * <p>
         * Every batch is persisted to disk before it leaves the wire and is
         * reclaimed as soon as the server acknowledges it. On restart the
         * sender replays only batches whose acknowledgement had not been
         * received before the previous sender shut down — typically the last
         * in-flight batches at close time. Acknowledged batches are not
         * replayed: their disk space is freed during normal operation by an
         * automatic per-frame trim that force-rotates the active segment
         * once every frame in it has been acknowledged.
         * <p>
         * Note that {@link io.questdb.client.cutlass.qwp.client.QwpWebSocketSender#close()}
         * under SF returns once data is on disk, not on server-ack, so a
         * sender closed immediately after a flush may still have unacked
         * batches in flight; those will be replayed by the next sender
         * against the same directory. WebSocket transport only.
         * <p>
         * The sender takes ownership of the underlying SF storage and closes
         * it when the sender itself is closed.
         *
         * @param dir filesystem directory; created if it doesn't exist
         */
        public LineSenderBuilder storeAndForwardDir(String dir) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            if (dir == null || dir.isEmpty()) {
                throw new LineSenderException("store_and_forward dir cannot be empty");
            }
            this.sfDir = dir;
            return this;
        }

        /**
         * Selects the durability contract for SF appends and flushes. See
         * {@link SfDurability} for the value semantics.
         * <p>
         * Replaces the prior pair of independent {@code sf_fsync} and
         * {@code sf_fsync_on_flush} booleans — they were three states
         * crammed into two flags. WebSocket transport only.
         */
        public LineSenderBuilder storeAndForwardDurability(SfDurability durability) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            if (durability == null) {
                throw new LineSenderException("sf_durability cannot be null");
            }
            this.sfDurability = durability;
            return this;
        }

        /**
         * Maximum bytes per segment file before rotation. Defaults to
         * {@code DEFAULT_SEGMENT_BYTES}
         * (4 MiB). Smaller segments mean faster trim of acked data; larger
         * segments mean fewer rotations.
         */
        public LineSenderBuilder storeAndForwardMaxBytes(long maxBytes) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            if (maxBytes <= 0) {
                throw new LineSenderException("sf_max_bytes must be positive: ").put(maxBytes);
            }
            this.sfMaxBytes = maxBytes;
            return this;
        }

        /**
         * Hard cap on cursor-allocated bytes (active + spare + sealed
         * segments). When the cap is reached, the producer's
         * {@code Sender.flush()} blocks until ACK-driven trim frees space;
         * if the cap is exhausted past the configured deadline (default 30 s),
         * {@code flush()} throws. Default: {@code 128 MiB}, which applies to
         * both memory-mode and SF-mode rings — for SF deployments with
         * cheap disk, raise this knob explicitly. WebSocket transport only.
         */
        public LineSenderBuilder storeAndForwardMaxTotalBytes(long maxTotalBytes) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            if (maxTotalBytes <= 0) {
                throw new LineSenderException("sf_max_total_bytes must be positive: ").put(maxTotalBytes);
            }
            this.sfMaxTotalBytes = maxTotalBytes;
            return this;
        }

        private static boolean charsEqualsRange(CharSequence a, CharSequence b, int bStart, int bEnd) {
            int len = bEnd - bStart;
            if (a.length() != len) {
                return false;
            }
            for (int i = 0; i < len; i++) {
                if (a.charAt(i) != b.charAt(bStart + i)) {
                    return false;
                }
            }
            return true;
        }

        private static int getValue(CharSequence configurationString, int pos, StringSink sink, String name) {
            if ((pos = ConfStringParser.value(configurationString, pos, sink)) < 0) {
                throw new LineSenderException("invalid ").put(name).put(" [error=").put(sink).put("]");
            }
            return pos;
        }

        private static SfDurability parseDurabilityValue(@NotNull StringSink value) {
            if (Chars.equalsIgnoreCase("memory", value)) return SfDurability.MEMORY;
            if (Chars.equalsIgnoreCase("flush", value)) return SfDurability.FLUSH;
            if (Chars.equalsIgnoreCase("append", value)) return SfDurability.APPEND;
            throw new LineSenderException("invalid sf_durability [value=").put(value)
                    .put(", allowed-values=[memory, flush, append]]");
        }

        private static int parseIntValue(@NotNull StringSink value, @NotNull String name) {
            if (Chars.isBlank(value)) {
                throw new LineSenderException(name).put(" cannot be empty");
            }
            try {
                return Numbers.parseInt(value);
            } catch (NumericException e) {
                throw new LineSenderException("invalid ").put(name).put(" [value=").put(value).put("]");
            }
        }

        private static long parseLongValue(@NotNull StringSink value, @NotNull String name) {
            if (Chars.isBlank(value)) {
                throw new LineSenderException(name).put(" cannot be empty");
            }
            try {
                return Numbers.parseLong(value);
            } catch (NumericException e) {
                throw new LineSenderException("invalid ").put(name).put(" [value=").put(value).put("]");
            }
        }

        private static int parseSizeIntValue(@NotNull StringSink value, @NotNull String name) {
            long size = parseSizeValue(value, name);
            if (size > Integer.MAX_VALUE) {
                throw new LineSenderException(name).put(" exceeds maximum [value=").put(value).put(']');
            }
            return (int) size;
        }

        /**
         * Parses a byte-count value with optional unit suffix:
         * <ul>
         *   <li>plain decimal: {@code 67108864}</li>
         *   <li>kibibyte: {@code 64k} or {@code 64kb}</li>
         *   <li>mebibyte: {@code 64m} or {@code 64mb}</li>
         *   <li>gibibyte: {@code 4g} or {@code 4gb}</li>
         * </ul>
         * Suffixes are case-insensitive. Powers of 2 (1024-based), not 1000;
         * matches what most JVM size flags accept (-Xmx, -Xss, etc.).
         */
        private static long parseSizeValue(@NotNull StringSink value, @NotNull String name) {
            if (Chars.isBlank(value)) {
                throw new LineSenderException(name).put(" cannot be empty");
            }
            int len = value.length();
            // Strip a trailing 'b' / 'B' so '64m' and '64mb' both work.
            int end = len;
            if (end > 0) {
                char tail = value.charAt(end - 1);
                if (tail == 'b' || tail == 'B') {
                    end--;
                }
            }
            long multiplier = 1L;
            if (end > 0) {
                char unit = value.charAt(end - 1);
                switch (unit) {
                    case 'k':
                    case 'K':
                        multiplier = 1024L;
                        end--;
                        break;
                    case 'm':
                    case 'M':
                        multiplier = 1024L * 1024;
                        end--;
                        break;
                    case 'g':
                    case 'G':
                        multiplier = 1024L * 1024 * 1024;
                        end--;
                        break;
                    case 't':
                    case 'T':
                        multiplier = 1024L * 1024 * 1024 * 1024;
                        end--;
                        break;
                    default: // no unit suffix — treat as raw bytes
                }
            }
            if (end <= 0) {
                throw new LineSenderException("invalid ").put(name).put(" [value=").put(value).put("]");
            }
            // parseLong only takes a full CharSequence. The suffix-trimming
            // path is parser-time (called once per connect string), so a
            // tiny per-call substring allocation is acceptable.
            CharSequence digits = end == len ? value : value.toString().substring(0, end);
            try {
                long n = Numbers.parseLong(digits);
                // Overflow check on multiply.
                if (multiplier != 1 && n != 0 && n > Long.MAX_VALUE / multiplier) {
                    throw new LineSenderException(name).put(" overflows long [value=").put(value).put(']');
                }
                return n * multiplier;
            } catch (NumericException e) {
                throw new LineSenderException("invalid ").put(name).put(" [value=").put(value).put("]");
            }
        }

        private static int resolveIPv4(String host) {
            try {
                byte[] addr = InetAddress.getByName(host).getAddress();
                if (addr.length != 4) {
                    throw new LineSenderException("IPv6 addresses are not supported [host=").put(host).put("]");
                }
                return ((addr[0] & 0xFF) << 24)
                        | ((addr[1] & 0xFF) << 16)
                        | ((addr[2] & 0xFF) << 8)
                        | (addr[3] & 0xFF);
            } catch (UnknownHostException e) {
                throw new LineSenderException("could not resolve host [host=" + host + "]", e);
            }
        }

        private static RuntimeException rethrow(Throwable t) {
            if (t instanceof LineSenderException) {
                throw (LineSenderException) t;
            }
            throw new LineSenderException(t);
        }

        private static void validateSenderId(String id) {
            if (id == null || id.isEmpty()) {
                throw new LineSenderException("sender_id must not be empty");
            }
            for (int i = 0, n = id.length(); i < n; i++) {
                char c = id.charAt(i);
                boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                        || (c >= '0' && c <= '9') || c == '_' || c == '-';
                if (!ok) {
                    throw new LineSenderException(
                            "sender_id contains invalid character: '" + c
                                    + "' (allowed: letters, digits, _ -)");
                }
            }
        }

        private void addAddressEntry(CharSequence src, int start, int end, int defaultPort) {
            int colon = Chars.indexOf(src, start, end, ':');
            if (colon == end - 1) {
                throw new LineSenderException("invalid address, use IPv4 address or a domain name [address=")
                        .put(src.subSequence(start, end)).put("]");
            }
            int hostEnd = colon < 0 ? end : colon;
            int portStart = colon < 0 ? end : colon + 1;
            while (hostEnd > start && Character.isWhitespace(src.charAt(hostEnd - 1))) hostEnd--;
            while (portStart < end && Character.isWhitespace(src.charAt(portStart))) portStart++;
            int parsedPort = -1;
            if (colon >= 0) {
                try {
                    parsedPort = Numbers.parseInt(src, portStart, end);
                    if (parsedPort < 1 || parsedPort > 65535) {
                        throw new LineSenderException("invalid port [port=").put(parsedPort).put("]");
                    }
                } catch (NumericException e) {
                    throw new LineSenderException("cannot parse a port from the address, use IPv4 address or a domain name")
                            .put(" [address=").put(src.subSequence(start, end)).put("]");
                }
            }
            if (hostEnd == start) {
                throw new LineSenderException("empty host in addr entry [address=")
                        .put(src.subSequence(start, end)).put("]");
            }
            int effectivePort = parsedPort != -1 ? parsedPort : defaultPort;
            for (int i = 0, n = hosts.size(); i < n; i++) {
                String storedHost = hosts.get(i);
                if (charsEqualsRange(storedHost, src, start, hostEnd)) {
                    int storedEffectivePort = ports.size() > i && ports.getQuick(i) != PORT_NOT_SET
                            ? ports.getQuick(i) : defaultPort;
                    if (storedEffectivePort == effectivePort) {
                        throw new LineSenderException("duplicated addresses are not allowed [address=")
                                .put(src.subSequence(start, end)).put("]");
                    }
                }
            }
            while (ports.size() < hosts.size()) {
                ports.add(defaultPort);
            }
            hosts.add(src.subSequence(start, hostEnd).toString());
            ports.add(effectivePort);
        }

        private void appendAddress(String host, int port) {
            hosts.add(host);
            ports.add(port);
        }

        private String buildWebSocketAuthHeader() {
            if (username != null && password != null) {
                String credentials = username + ":" + password;
                return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
            }
            if (httpToken != null) {
                return "Bearer " + httpToken;
            }
            return null;
        }

        private void configureDefaults() {
            if (protocol == PARAMETER_NOT_SET_EXPLICITLY) {
                protocol = PROTOCOL_TCP;
            }
            if (bufferCapacity == PARAMETER_NOT_SET_EXPLICITLY) {
                bufferCapacity = DEFAULT_BUFFER_CAPACITY;
            }
            if (maximumBufferCapacity == PARAMETER_NOT_SET_EXPLICITLY) {
                maximumBufferCapacity = protocol == PROTOCOL_HTTP ? DEFAULT_MAXIMUM_BUFFER_CAPACITY : bufferCapacity;
            }
            int defaultPort;
            if (protocol == PROTOCOL_HTTP) {
                defaultPort = DEFAULT_HTTP_PORT;
            } else if (protocol == PROTOCOL_UDP) {
                defaultPort = DEFAULT_UDP_PORT;
            } else if (protocol == PROTOCOL_WEBSOCKET) {
                defaultPort = DEFAULT_WEBSOCKET_PORT;
            } else {
                defaultPort = DEFAULT_TCP_PORT;
            }
            int hostsCount = Math.max(hosts.size(), 1);
            for (int i = 0, n = ports.size(); i < n; i++) {
                if (ports.getQuick(i) == PORT_NOT_SET) {
                    ports.set(i, defaultPort);
                }
            }
            while (ports.size() < hostsCount) {
                ports.add(defaultPort);
            }
            if (tlsValidationMode == null) {
                tlsValidationMode = TlsValidationMode.DEFAULT;
            }
            if (protocol == PROTOCOL_TCP && protocolVersion == PARAMETER_NOT_SET_EXPLICITLY) {
                // keep protocol_version = 1 as default when use does not set protocol_version explicit for tcp/tcps protocol.
                protocolVersion = PROTOCOL_VERSION_V1;
            }
            if (maxNameLength == PARAMETER_NOT_SET_EXPLICITLY) {
                maxNameLength = DEFAULT_MAX_NAME_LEN;
            }
            if (maxBackoffMillis == PARAMETER_NOT_SET_EXPLICITLY && protocol == PROTOCOL_HTTP) {
                maxBackoffMillis = DEFAULT_MAX_BACKOFF_MILLIS;
            }
        }

        /**
         * Configure SenderBuilder from a configuration string.
         * <br>
         * This allows to use a configuration string as a template and amend it with additional configuration options.
         * <br>
         * It does not allow to override already configured options and throws an exception if you try to do so.
         * <br>
         *
         * @param configurationString configuration string
         * @return this instance for method chaining
         * @see #fromConfig(CharSequence)
         */
        private LineSenderBuilder fromConfig(CharSequence configurationString) {
            if (Chars.isBlank(configurationString)) {
                throw new LineSenderException("configuration string cannot be empty nor null");
            }
            StringSink sink = new StringSink();
            int pos = ConfStringParser.of(configurationString, sink);
            if (pos < 0) {
                throw new LineSenderException("invalid configuration string: ").put(sink);
            }
            if (Chars.equals("http", sink)) {
                if (tlsEnabled) {
                    throw new LineSenderException("cannot use http protocol when TLS is enabled. use https instead");
                }
                http();
            } else if (Chars.equals("tcp", sink)) {
                if (tlsEnabled) {
                    throw new LineSenderException("cannot use tcp protocol when TLS is enabled. use tcps instead");
                }
                tcp();
            } else if (Chars.equals("https", sink)) {
                http();
                tlsEnabled = true;
            } else if (Chars.equals("tcps", sink)) {
                tcp();
                tlsEnabled = true;
            } else if (Chars.equals("ws", sink)) {
                if (tlsEnabled) {
                    throw new LineSenderException("cannot use ws protocol when TLS is enabled. use wss instead");
                }
                websocket();
            } else if (Chars.equals("wss", sink)) {
                websocket();
                tlsEnabled = true;
            } else if (Chars.equals("udp", sink)) {
                udp();
            } else if (Chars.equals("udps", sink)) {
                throw new LineSenderException("TLS is not supported for UDP");
            } else {
                throw new LineSenderException("invalid schema [schema=").put(sink).put(", supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            }

            if (protocol == PROTOCOL_WEBSOCKET) {
                return fromConfigWebSocket(configurationString);
            }

            String tcpToken = null;
            String user = null;
            String password = null;

            // We need the autoFlushBytesSet and initBufSizeSet flags, because auto_flush_bytes and init_buf_size params
            // share the same SenderBuilder field. TCP transport allows both to be set as long as they have the same
            // value. At the same time, we want to fail when the same parameter is set twice.
            boolean initBufSizeSet = false;
            boolean autoFlushBytesSet = false;
            while (ConfStringParser.hasNext(configurationString, pos)) {
                pos = ConfStringParser.nextKey(configurationString, pos, sink);
                if (pos < 0) {
                    throw new LineSenderException("invalid configuration string [error=").put(sink).put(']');
                }
                if (Chars.equals("addr", sink)) {
                    pos = getValue(configurationString, pos, sink, "address");
                    int defaultPort = protocol == PROTOCOL_HTTP ? DEFAULT_HTTP_PORT
                            : protocol == PROTOCOL_UDP ? DEFAULT_UDP_PORT
                              : protocol == PROTOCOL_WEBSOCKET ? DEFAULT_WEBSOCKET_PORT
                                : DEFAULT_TCP_PORT;
                    int valLen = sink.length();
                    int entryStart = 0;
                    for (int i = 0; i <= valLen; i++) {
                        if (i == valLen || sink.charAt(i) == ',') {
                            int s = entryStart;
                            int e = i;
                            while (s < e && Character.isWhitespace(sink.charAt(s))) s++;
                            while (e > s && Character.isWhitespace(sink.charAt(e - 1))) e--;
                            if (s == e) {
                                throw new LineSenderException("empty addr entry");
                            }
                            addAddressEntry(sink, s, e, defaultPort);
                            entryStart = i + 1;
                        }
                    }
                } else if (Chars.equals("user", sink)) {
                    // deprecated key: user, new key: username
                    pos = getValue(configurationString, pos, sink, "user");
                    if (protocol == PROTOCOL_UDP) {
                        throw new LineSenderException("username is not supported for UDP transport");
                    }
                    user = sink.toString();
                } else if (Chars.equals("username", sink)) {
                    pos = getValue(configurationString, pos, sink, "username");
                    if (protocol == PROTOCOL_UDP) {
                        throw new LineSenderException("username is not supported for UDP transport");
                    }
                    user = sink.toString();
                } else if (Chars.equals("pass", sink)) {
                    // deprecated key: pass, new key: password
                    pos = getValue(configurationString, pos, sink, "pass");
                    if (protocol == PROTOCOL_TCP) {
                        throw new LineSenderException("password is not supported for TCP protocol");
                    } else if (protocol == PROTOCOL_UDP) {
                        throw new LineSenderException("password is not supported for UDP transport");
                    }
                    password = sink.toString();
                } else if (Chars.equals("password", sink)) {
                    pos = getValue(configurationString, pos, sink, "password");
                    if (protocol == PROTOCOL_TCP) {
                        throw new LineSenderException("password is not supported for TCP protocol");
                    } else if (protocol == PROTOCOL_UDP) {
                        throw new LineSenderException("password is not supported for UDP transport");
                    }
                    password = sink.toString();
                } else if (Chars.equals("tls_verify", sink)) {
                    pos = getValue(configurationString, pos, sink, "tls_verify");
                    if (tlsValidationMode != null) {
                        throw new LineSenderException("tls_verify was already configured");
                    }
                    if (Chars.equals("on", sink)) {
                        tlsValidationMode = TlsValidationMode.DEFAULT;
                    } else if (Chars.equals("unsafe_off", sink)) {
                        tlsValidationMode = TlsValidationMode.INSECURE;
                    } else {
                        throw new LineSenderException("invalid tls_verify [value=").put(sink).put(", allowed-values=[on, unsafe_off]]");
                    }
                } else if (Chars.equals("tls_roots", sink)) {
                    pos = getValue(configurationString, pos, sink, "tls_roots");
                    if (trustStorePath != null) {
                        throw new LineSenderException("tls_roots was already configured");
                    }
                    trustStorePath = sink.toString();
                } else if (Chars.equals("tls_roots_password", sink)) {
                    pos = getValue(configurationString, pos, sink, "tls_roots_password");
                    if (trustStorePassword != null) {
                        throw new LineSenderException("tls_roots_password was already configured");
                    }
                    trustStorePassword = new char[sink.length()];
                    for (int i = 0, n = sink.length(); i < n; i++) {
                        trustStorePassword[i] = sink.charAt(i);
                    }
                } else if (Chars.equals("token", sink)) {
                    pos = getValue(configurationString, pos, sink, "token");
                    if (protocol == PROTOCOL_TCP) {
                        tcpToken = sink.toString();
                        // will configure later, we need to know a keyId first
                    } else if (protocol == PROTOCOL_UDP) {
                        throw new LineSenderException("token is not supported for UDP transport");
                    } else {
                        httpToken(sink.toString());
                    }
                } else if (Chars.equals("retry_timeout", sink)) {
                    pos = getValue(configurationString, pos, sink, "retry_timeout");
                    int timeout = parseIntValue(sink, "retry_timeout");
                    retryTimeoutMillis(timeout);
                } else if (Chars.equals("max_buf_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "max_buf_size");
                    int maxBufferSize = parseSizeIntValue(sink, "max_buf_size");
                    maxBufferCapacity(maxBufferSize);
                } else if (Chars.equals("max_name_len", sink)) {
                    pos = getValue(configurationString, pos, sink, "max_name_len");
                    int len = parseIntValue(sink, "max_name_len");
                    maxNameLength(len);
                } else if (Chars.equals("init_buf_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "init_buf_size");
                    int initBufSize = parseSizeIntValue(sink, "init_buf_size");
                    if (autoFlushBytesSet) {
                        assert protocol == PROTOCOL_TCP;
                        if (initBufSize != bufferCapacity) {
                            throw new LineSenderException("TCP transport requires init_buf_size and auto_flush_bytes to be set to the same value [init_buf_size=").put(initBufSize).put(", auto_flush_bytes=").put(bufferCapacity).put(']');
                        }
                    } else {
                        bufferCapacity(initBufSize);
                    }
                    initBufSizeSet = true;
                } else if (Chars.equals("auto_flush_rows", sink)) {
                    pos = getValue(configurationString, pos, sink, "auto_flush_rows");
                    int autoFlushRows;
                    if (Chars.equalsIgnoreCase("off", sink)) {
                        autoFlushRows = 0;
                    } else {
                        autoFlushRows = parseIntValue(sink, "auto_flush_rows");
                        if (autoFlushRows < 1) {
                            throw new LineSenderException("invalid auto_flush_rows [value=").put(autoFlushRows).put("]");
                        }
                    }
                    autoFlushRows(autoFlushRows);
                } else if (Chars.equals("auto_flush_interval", sink)) {
                    pos = getValue(configurationString, pos, sink, "auto_flush_interval");
                    int autoFlushInterval;
                    if (Chars.equalsIgnoreCase("off", sink)) {
                        autoFlushInterval = Integer.MAX_VALUE;
                    } else {
                        autoFlushInterval = parseIntValue(sink, "auto_flush_interval");
                        if (autoFlushInterval < 1) {
                            throw new LineSenderException("invalid auto_flush_interval [value=").put(autoFlushInterval).put("]");
                        }
                    }
                    autoFlushIntervalMillis(autoFlushInterval);
                } else if (Chars.equals("auto_flush_bytes", sink)) {
                    if (protocol != PROTOCOL_TCP && protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("auto_flush_bytes is only supported for TCP and WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "auto_flush_bytes");
                    if (protocol == PROTOCOL_TCP) {
                        if (Chars.equalsIgnoreCase("off", sink)) {
                            throw new LineSenderException("TCP transport must have auto_flush_bytes enabled");
                        }
                        int autoFlushBytes = parseIntValue(sink, "auto_flush_bytes");
                        if (initBufSizeSet) {
                            if (autoFlushBytes != bufferCapacity) {
                                throw new LineSenderException("TCP transport requires init_buf_size and auto_flush_bytes to be set to the same value [init_buf_size=").put(bufferCapacity).put(", auto_flush_bytes=").put(autoFlushBytes).put(']');
                            }
                        } else {
                            bufferCapacity(autoFlushBytes);
                        }
                    } else {
                        if (Chars.equalsIgnoreCase("off", sink)) {
                            autoFlushBytes(0);
                        } else {
                            int autoFlushBytes = parseIntValue(sink, "auto_flush_bytes");
                            autoFlushBytes(autoFlushBytes);
                        }
                    }
                    autoFlushBytesSet = true;
                } else if (Chars.equals("auto_flush", sink)) {
                    pos = getValue(configurationString, pos, sink, "auto_flush");
                    if (Chars.equalsIgnoreCase("off", sink)) {
                        disableAutoFlush();
                    } else if (!Chars.equalsIgnoreCase("on", sink)) {
                        throw new LineSenderException("invalid auto_flush [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else if (Chars.equals("request_timeout", sink)) {
                    pos = getValue(configurationString, pos, sink, "request_timeout");
                    int requestTimeout = parseIntValue(sink, "request_timeout");
                    httpTimeoutMillis(requestTimeout);
                } else if (Chars.equals("connect_timeout", sink)) {
                    pos = getValue(configurationString, pos, sink, "connect_timeout");
                    connectTimeoutMillis(parseIntValue(sink, "connect_timeout"));
                } else if (Chars.equals("request_min_throughput", sink)) {
                    pos = getValue(configurationString, pos, sink, "request_min_throughput");
                    int requestMinThroughput = parseIntValue(sink, "request_min_throughput");
                    minRequestThroughput(requestMinThroughput);
                } else if (Chars.equals("protocol_version", sink)) {
                    pos = getValue(configurationString, pos, sink, "protocol_version");
                    if (!Chars.equalsIgnoreCase("auto", sink)) {
                        int protocolVersion = parseIntValue(sink, "protocol_version");
                        protocolVersion(protocolVersion);
                    }
                } else if (Chars.equals("request_durable_ack", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("request_durable_ack is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "request_durable_ack");
                    if (Chars.equalsIgnoreCase("on", sink)) {
                        requestDurableAck(true);
                    } else if (Chars.equalsIgnoreCase("off", sink)) {
                        requestDurableAck(false);
                    } else {
                        throw new LineSenderException("invalid request_durable_ack [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else if (Chars.equals("transaction", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("transaction is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "transaction");
                    if (Chars.equalsIgnoreCase("on", sink)) {
                        transactional(true);
                    } else if (Chars.equalsIgnoreCase("off", sink)) {
                        transactional(false);
                    } else {
                        throw new LineSenderException("invalid transaction [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else if (Chars.equals("sf_dir", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_dir is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_dir");
                    storeAndForwardDir(sink.toString());
                } else if (Chars.equals("sender_id", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sender_id is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sender_id");
                    senderId(sink.toString());
                } else if (Chars.equals("sf_max_bytes", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_max_bytes is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_max_bytes");
                    storeAndForwardMaxBytes(parseSizeValue(sink, "sf_max_bytes"));
                } else if (Chars.equals("sf_max_total_bytes", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_max_total_bytes is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_max_total_bytes");
                    storeAndForwardMaxTotalBytes(parseSizeValue(sink, "sf_max_total_bytes"));
                } else if (Chars.equals("sf_durability", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_durability is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_durability");
                    storeAndForwardDurability(parseDurabilityValue(sink));
                } else if (Chars.equals("close_flush_timeout_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("close_flush_timeout_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "close_flush_timeout_millis");
                    closeFlushTimeoutMillis(parseLongValue(sink, "close_flush_timeout_millis"));
                } else if (Chars.equals("auth_timeout_ms", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("auth_timeout_ms is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "auth_timeout_ms");
                    authTimeoutMillis(parseLongValue(sink, "auth_timeout_ms"));
                } else if (Chars.equals("durable_ack_keepalive_interval_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException(
                                "durable_ack_keepalive_interval_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "durable_ack_keepalive_interval_millis");
                    durableAckKeepaliveIntervalMillis(parseLongValue(sink, "durable_ack_keepalive_interval_millis"));
                } else if (Chars.equals("reconnect_max_duration_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("reconnect_max_duration_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "reconnect_max_duration_millis");
                    reconnectMaxDurationMillis(parseLongValue(sink, "reconnect_max_duration_millis"));
                } else if (Chars.equals("reconnect_initial_backoff_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("reconnect_initial_backoff_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "reconnect_initial_backoff_millis");
                    reconnectInitialBackoffMillis(parseLongValue(sink, "reconnect_initial_backoff_millis"));
                } else if (Chars.equals("max_frame_rejections", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("max_frame_rejections is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "max_frame_rejections");
                    maxFrameRejections(parseIntValue(sink, "max_frame_rejections"));
                } else if (Chars.equals("poison_min_escalation_window_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("poison_min_escalation_window_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "poison_min_escalation_window_millis");
                    poisonMinEscalationWindowMillis(parseLongValue(sink, "poison_min_escalation_window_millis"));
                } else if (Chars.equals("initial_connect_retry", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("initial_connect_retry is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "initial_connect_retry");
                    if (Chars.equalsIgnoreCase("on", sink) || Chars.equalsIgnoreCase("true", sink)
                            || Chars.equalsIgnoreCase("sync", sink)) {
                        initialConnectMode(InitialConnectMode.SYNC);
                    } else if (Chars.equalsIgnoreCase("off", sink) || Chars.equalsIgnoreCase("false", sink)) {
                        initialConnectMode(InitialConnectMode.OFF);
                    } else if (Chars.equalsIgnoreCase("async", sink)) {
                        initialConnectMode(InitialConnectMode.ASYNC);
                    } else {
                        throw new LineSenderException("invalid initial_connect_retry [value=").put(sink).put(", allowed-values=[on, off, true, false, sync, async]]");
                    }
                } else if (Chars.equals("sf_append_deadline_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_append_deadline_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_append_deadline_millis");
                    sfAppendDeadlineMillis(parseLongValue(sink, "sf_append_deadline_millis"));
                } else if (Chars.equals("drain_orphans", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("drain_orphans is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "drain_orphans");
                    if (Chars.equalsIgnoreCase("on", sink) || Chars.equalsIgnoreCase("true", sink)) {
                        drainOrphans(true);
                    } else if (Chars.equalsIgnoreCase("off", sink) || Chars.equalsIgnoreCase("false", sink)) {
                        drainOrphans(false);
                    } else {
                        throw new LineSenderException("invalid drain_orphans [value=").put(sink).put(", allowed-values=[on, off, true, false]]");
                    }
                } else if (Chars.equals("max_background_drainers", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("max_background_drainers is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "max_background_drainers");
                    maxBackgroundDrainers(parseIntValue(sink, "max_background_drainers"));
                } else if (Chars.equals("error_inbox_capacity", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("error_inbox_capacity is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "error_inbox_capacity");
                    errorInboxCapacity(parseIntValue(sink, "error_inbox_capacity"));
                } else if (Chars.equals("connection_listener_inbox_capacity", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("connection_listener_inbox_capacity is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "connection_listener_inbox_capacity");
                    connectionListenerInboxCapacity(parseIntValue(sink, "connection_listener_inbox_capacity"));
                } else if (Chars.equals("reconnect_max_backoff_millis", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("reconnect_max_backoff_millis is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "reconnect_max_backoff_millis");
                    reconnectMaxBackoffMillis(parseLongValue(sink, "reconnect_max_backoff_millis"));
                } else if (Chars.equals("max_datagram_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "max_datagram_size");
                    int mds = parseIntValue(sink, "max_datagram_size");
                    maxDatagramSize(mds);
                } else if (Chars.equals("multicast_ttl", sink)) {
                    pos = getValue(configurationString, pos, sink, "multicast_ttl");
                    int ttl = parseIntValue(sink, "multicast_ttl");
                    multicastTtl(ttl);
                } else if (Chars.equals("zone", sink)) {
                    // failover.md §1.1: zone= is egress-only effective. Ingress is
                    // zone-blind (pinned to v1) and silently accepts the key so
                    // the same connect string works on both sides.
                    pos = getValue(configurationString, pos, sink, "zone");
                } else if (Chars.equals("buffer_pool_size", sink)
                        || Chars.equals("client_id", sink)
                        || Chars.equals("compression", sink)
                        || Chars.equals("compression_level", sink)
                        || Chars.equals("failover", sink)
                        || Chars.equals("failover_backoff_initial_ms", sink)
                        || Chars.equals("failover_backoff_max_ms", sink)
                        || Chars.equals("failover_max_attempts", sink)
                        || Chars.equals("failover_max_duration_ms", sink)
                        || Chars.equals("initial_credit", sink)
                        || Chars.equals("max_batch_rows", sink)
                        || Chars.equals("target", sink)) {
                    // connect-string.md "Query client keys" and "Multi-host failover":
                    // these keys configure the QwpQueryClient (egress) only. The
                    // Sender silently consumes them so the same connect string can be
                    // shared between the Sender and the QwpQueryClient without an
                    // "unknown configuration key" error. Validation is the egress
                    // parser's job; the ingress parser does not interpret the value.
                    // Capture the key name before getValue clears the sink so a
                    // genuine value-parse error names the offending key.
                    String egressKey = Chars.toString(sink);
                    pos = getValue(configurationString, pos, sink, egressKey);
                } else if (Chars.equals("on_internal_error", sink)
                        || Chars.equals("on_parse_error", sink)
                        || Chars.equals("on_schema_error", sink)
                        || Chars.equals("on_security_error", sink)
                        || Chars.equals("on_server_error", sink)
                        || Chars.equals("on_write_error", sink)) {
                    // connect-string.md "Error handling": the on_*_error keys select
                    // the per-category error policy. The spec reserves them and
                    // directs new client implementations to accept them in the
                    // connect string. The Sender does not wire them to a policy yet,
                    // so it consumes them as an accepted no-op rather than rejecting
                    // them. Capture the key name before getValue clears the sink so a
                    // genuine value-parse error names the offending key.
                    String reservedKey = Chars.toString(sink);
                    pos = getValue(configurationString, pos, sink, reservedKey);
                } else {
                    // sf-client.md §4.6: parser must reject unknown keys.
                    // Forward-compat is via the spec, not silent ignore — silent
                    // ignore guarantees divergence across language clients.
                    throw new LineSenderException("unknown configuration key [key=").put(sink).put(']');
                }
            }
            if (hosts.size() == 0) {
                throw new LineSenderException("addr is missing");
            }
            if (trustStorePath != null) {
                if (trustStorePassword == null) {
                    throw new LineSenderException("tls_roots was configured, but tls_roots_password is missing");
                }
            } else if (trustStorePassword != null) {
                throw new LineSenderException("tls_roots_password was configured, but tls_roots is missing");
            }
            if (protocol == PROTOCOL_HTTP || protocol == PROTOCOL_WEBSOCKET) {
                if (user != null) {
                    httpUsernamePassword(user, password);
                } else if (password != null) {
                    throw new LineSenderException("password is configured, but username is missing");
                }
            } else {
                if (user != null) {
                    enableAuth(user).authToken(tcpToken);
                } else if (tcpToken != null) {
                    throw new LineSenderException("TCP token is configured, but user is missing");
                }
            }
            return this;
        }

        /**
         * Configures the WebSocket (QWP) ingress path from a {@code ws}/{@code wss}
         * connect string, driven by {@link ConfigView} over the {@link ConfigSchema} registry.
         * The reject pass surfaces unknown keys (with a relocated-key hint for
         * legacy http/tcp/udp keys); {@link #validateWsConfig} runs the cross-key
         * checks; the rest applies through the existing fluent setters, feeding
         * the {@code PROTOCOL_WEBSOCKET} build path. Duplicate keys resolve
         * last-write-wins. {@link ConfigView}'s {@link IllegalArgumentException}s
         * surface as {@link LineSenderException} to keep the Sender contract.
         */
        private LineSenderBuilder fromConfigWebSocket(CharSequence configurationString) {
            try {
                ConfigString cs = ConfigString.parse(configurationString);
                ConfigView view = new ConfigView(cs);
                validateWsConfig(view, tlsEnabled);

                view.getHostPorts("addr", DEFAULT_WEBSOCKET_PORT, this::appendAddress);

                StringSink v = new StringSink();
                String s;

                String token = view.getStr("token");
                if (token != null) {
                    httpToken(token);
                }
                String user = view.getStr("username");
                if (user != null) {
                    httpUsernamePassword(user, view.getStr("password"));
                }

                s = view.getEnum("tls_verify");
                if (s != null) {
                    tlsValidationMode = "on".equals(s) ? TlsValidationMode.DEFAULT : TlsValidationMode.INSECURE;
                }
                s = view.getStr("tls_roots");
                if (s != null) {
                    trustStorePath = s;
                }
                s = view.getStr("tls_roots_password");
                if (s != null) {
                    trustStorePassword = s.toCharArray();
                }
                if (view.has("auth_timeout_ms")) {
                    authTimeoutMillis(view.getLong("auth_timeout_ms", 0));
                }
                if (view.has("connect_timeout")) {
                    // getInt (not getLong + cast): connectTimeoutMillis takes an
                    // int, and an over-int value must reject, not wrap.
                    connectTimeoutMillis(view.getInt("connect_timeout", 0));
                }

                s = view.getStr("auto_flush_rows");
                if (s != null) {
                    int rows;
                    if (s.equalsIgnoreCase("off")) {
                        rows = 0;
                    } else {
                        v.clear();
                        v.put(s);
                        rows = parseIntValue(v, "auto_flush_rows");
                        if (rows < 1) {
                            throw new LineSenderException("invalid auto_flush_rows [value=").put(rows).put("]");
                        }
                    }
                    autoFlushRows(rows);
                }
                s = view.getStr("auto_flush_interval");
                if (s != null) {
                    int interval;
                    if (s.equalsIgnoreCase("off")) {
                        interval = Integer.MAX_VALUE;
                    } else {
                        v.clear();
                        v.put(s);
                        interval = parseIntValue(v, "auto_flush_interval");
                        if (interval < 1) {
                            throw new LineSenderException("invalid auto_flush_interval [value=").put(interval).put("]");
                        }
                    }
                    autoFlushIntervalMillis(interval);
                }
                s = view.getStr("auto_flush_bytes");
                if (s != null) {
                    if (s.equalsIgnoreCase("off")) {
                        autoFlushBytes(0);
                    } else {
                        v.clear();
                        v.put(s);
                        autoFlushBytes(parseIntValue(v, "auto_flush_bytes"));
                    }
                }
                s = view.getStr("auto_flush");
                if (s != null) {
                    if (s.equalsIgnoreCase("off")) {
                        disableAutoFlush();
                    } else if (!s.equalsIgnoreCase("on")) {
                        throw new LineSenderException("invalid auto_flush [value=").put(s).put(", allowed-values=[on, off]]");
                    }
                }

                if (view.has("max_name_len")) {
                    maxNameLength(wsInt(view, v, "max_name_len"));
                }
                if (view.has("max_background_drainers")) {
                    maxBackgroundDrainers(wsInt(view, v, "max_background_drainers"));
                }
                if (view.has("error_inbox_capacity")) {
                    errorInboxCapacity(wsInt(view, v, "error_inbox_capacity"));
                }
                if (view.has("connection_listener_inbox_capacity")) {
                    connectionListenerInboxCapacity(wsInt(view, v, "connection_listener_inbox_capacity"));
                }
                if (view.has("close_flush_timeout_millis")) {
                    closeFlushTimeoutMillis(wsLong(view, v, "close_flush_timeout_millis"));
                }
                if (view.has("durable_ack_keepalive_interval_millis")) {
                    durableAckKeepaliveIntervalMillis(wsLong(view, v, "durable_ack_keepalive_interval_millis"));
                }
                if (view.has("reconnect_max_duration_millis")) {
                    reconnectMaxDurationMillis(wsLong(view, v, "reconnect_max_duration_millis"));
                }
                if (view.has("reconnect_initial_backoff_millis")) {
                    reconnectInitialBackoffMillis(wsLong(view, v, "reconnect_initial_backoff_millis"));
                }
                if (view.has("reconnect_max_backoff_millis")) {
                    reconnectMaxBackoffMillis(wsLong(view, v, "reconnect_max_backoff_millis"));
                }
                if (view.has("max_frame_rejections")) {
                    maxFrameRejections(wsInt(view, v, "max_frame_rejections"));
                }
                if (view.has("poison_min_escalation_window_millis")) {
                    poisonMinEscalationWindowMillis(wsLong(view, v, "poison_min_escalation_window_millis"));
                }
                if (view.has("sf_append_deadline_millis")) {
                    sfAppendDeadlineMillis(wsLong(view, v, "sf_append_deadline_millis"));
                }
                if (view.has("sf_max_bytes")) {
                    storeAndForwardMaxBytes(wsSize(view, v, "sf_max_bytes"));
                }
                if (view.has("sf_max_total_bytes")) {
                    storeAndForwardMaxTotalBytes(wsSize(view, v, "sf_max_total_bytes"));
                }

                s = view.getStr("sf_dir");
                if (s != null) {
                    storeAndForwardDir(s);
                }
                s = view.getStr("sender_id");
                if (s != null) {
                    senderId(s);
                }
                s = view.getStr("sf_durability");
                if (s != null) {
                    v.clear();
                    v.put(s);
                    storeAndForwardDurability(parseDurabilityValue(v));
                }
                s = view.getStr("transaction");
                if (s != null) {
                    if (s.equalsIgnoreCase("on")) {
                        transactional(true);
                    } else if (s.equalsIgnoreCase("off")) {
                        transactional(false);
                    } else {
                        throw new LineSenderException("invalid transaction [value=").put(s).put(", allowed-values=[on, off]]");
                    }
                }
                s = view.getStr("request_durable_ack");
                if (s != null) {
                    if (s.equalsIgnoreCase("on")) {
                        requestDurableAck(true);
                    } else if (s.equalsIgnoreCase("off")) {
                        requestDurableAck(false);
                    } else {
                        throw new LineSenderException("invalid request_durable_ack [value=").put(s).put(", allowed-values=[on, off]]");
                    }
                }
                s = view.getStr("drain_orphans");
                if (s != null) {
                    if (s.equalsIgnoreCase("on") || s.equalsIgnoreCase("true")) {
                        drainOrphans(true);
                    } else if (s.equalsIgnoreCase("off") || s.equalsIgnoreCase("false")) {
                        drainOrphans(false);
                    } else {
                        throw new LineSenderException("invalid drain_orphans [value=").put(s).put(", allowed-values=[on, off, true, false]]");
                    }
                }
                s = view.getStr("initial_connect_retry");
                if (s != null) {
                    if (s.equalsIgnoreCase("on") || s.equalsIgnoreCase("true") || s.equalsIgnoreCase("sync")) {
                        initialConnectMode(InitialConnectMode.SYNC);
                    } else if (s.equalsIgnoreCase("off") || s.equalsIgnoreCase("false")) {
                        initialConnectMode(InitialConnectMode.OFF);
                    } else if (s.equalsIgnoreCase("async")) {
                        initialConnectMode(InitialConnectMode.ASYNC);
                    } else {
                        throw new LineSenderException("invalid initial_connect_retry [value=").put(s).put(", allowed-values=[on, off, true, false, sync, async]]");
                    }
                }
                return this;
            } catch (IllegalArgumentException e) {
                throw new LineSenderException(e.getMessage());
            }
        }

        /**
         * Validates the cross-key invariants of a WebSocket {@code ws}/{@code wss}
         * config without constructing a Sender. Shared by {@link #fromConfigWebSocket}
         * and the {@code QuestDB} facade's fail-fast build path. {@code tls} is true
         * for the {@code wss} schema. Mirrors the decisions the fluent build path
         * makes, so the ingress and egress sides reject the same config with the
         * same message.
         */
        static void validateWsConfig(ConfigView view, boolean tls) {
            view.getHostPorts("addr", DEFAULT_WEBSOCKET_PORT, (host, port) -> {
            });
            if (!view.has("addr")) {
                throw new IllegalArgumentException("missing required key: addr");
            }
            String user = view.getStr("username");
            String password = view.getStr("password");
            String token = view.getStr("token");
            // Basic auth needs both halves; reject either half alone with the same
            // message the egress QwpQueryClient uses, so a shared ws/wss string
            // fails identically on both sides.
            if ((user == null) != (password == null)) {
                throw new IllegalArgumentException("username and password must be provided together");
            }
            if (token != null && (user != null || password != null)) {
                throw new IllegalArgumentException("cannot use both token and username/password authentication");
            }
            String tlsVerify = view.getStr("tls_verify");
            String tlsRoots = view.getStr("tls_roots");
            String tlsRootsPassword = view.getStr("tls_roots_password");
            if (!tls && (tlsVerify != null || tlsRoots != null || tlsRootsPassword != null)) {
                throw new IllegalArgumentException("tls_verify/tls_roots/tls_roots_password require the wss:: schema");
            }
            if ((tlsRoots == null) != (tlsRootsPassword == null)) {
                throw new IllegalArgumentException("tls_roots and tls_roots_password must be provided together");
            }
        }

        /**
         * Fully validates a {@code ws}/{@code wss} connect string the same way
         * {@link #build()} does, but without connecting: it parses every value
         * through the real fluent setters and then runs {@link #configureDefaults}
         * and {@link #validateParameters}, exactly the prefix {@code build()} runs
         * before opening a socket. The {@code QuestDB} facade calls this so a
         * malformed ingest config fails at its {@code build()} time even when the
         * sender pool min is 0 and nothing connects. Ingress value keys are
         * registry-{@code STRING}, so only this real parse -- not the typed
         * {@link ConfigView} getters -- validates their values. Throws
         * {@link LineSenderException} on any malformed key or value.
         */
        static void validateWsConfigString(CharSequence configurationString) {
            LineSenderBuilder builder = new LineSenderBuilder();
            builder.fromConfig(configurationString);
            builder.configureDefaults();
            builder.validateParameters();
        }

        private static int wsInt(ConfigView view, StringSink v, String key) {
            v.clear();
            v.put(view.getStr(key));
            return parseIntValue(v, key);
        }

        private static long wsLong(ConfigView view, StringSink v, String key) {
            v.clear();
            v.put(view.getStr(key));
            return parseLongValue(v, key);
        }

        private static long wsSize(ConfigView view, StringSink v, String key) {
            v.clear();
            v.put(view.getStr(key));
            return parseSizeValue(v, key);
        }

        /**
         * Snapshot of the WebSocket (QWP) config this builder applied, keyed by
         * connect-string key name. Drives the per-key "honored" guard test --
         * proves each ws/wss key read from a config string reaches the builder.
         */
        @TestOnly
        public java.util.Map<String, Object> wsConfigSnapshotForTest() {
            java.util.Map<String, Object> m = new java.util.HashMap<>();
            m.put("auto_flush_rows", autoFlushRows);
            m.put("auto_flush_bytes", autoFlushBytes);
            m.put("auto_flush_interval", autoFlushIntervalMillis);
            m.put("max_name_len", maxNameLength);
            m.put("transaction", transactional);
            m.put("request_durable_ack", requestDurableAck);
            m.put("sender_id", senderId);
            m.put("sf_dir", sfDir);
            m.put("sf_max_bytes", sfMaxBytes);
            m.put("sf_max_total_bytes", sfMaxTotalBytes);
            m.put("sf_durability", sfDurability == null ? null : sfDurability.name());
            m.put("sf_append_deadline_millis", sfAppendDeadlineMillis);
            m.put("close_flush_timeout_millis", closeFlushTimeoutMillis);
            m.put("durable_ack_keepalive_interval_millis", durableAckKeepaliveIntervalMillis);
            m.put("initial_connect_retry", initialConnectMode == null ? null : initialConnectMode.name());
            m.put("reconnect_max_duration_millis", reconnectMaxDurationMillis);
            m.put("reconnect_initial_backoff_millis", reconnectInitialBackoffMillis);
            m.put("reconnect_max_backoff_millis", reconnectMaxBackoffMillis);
            m.put("drain_orphans", drainOrphans);
            m.put("max_background_drainers", maxBackgroundDrainers);
            m.put("max_frame_rejections", maxFrameRejections);
            m.put("poison_min_escalation_window_millis", poisonMinEscalationWindowMillis);
            m.put("error_inbox_capacity", errorInboxCapacity);
            m.put("connection_listener_inbox_capacity", connectionListenerInboxCapacity);
            m.put("token", httpToken);
            m.put("auth_timeout_ms", authTimeoutMillis);
            m.put("connect_timeout", connectTimeoutMillis == PARAMETER_NOT_SET_EXPLICITLY ? 0 : connectTimeoutMillis);
            m.put("username", username);
            m.put("password", password);
            m.put("tls_verify", tlsValidationMode == null ? null : tlsValidationMode.name());
            m.put("tls_roots", trustStorePath);
            m.put("tls_roots_password", trustStorePassword == null ? null : new String(trustStorePassword));
            return m;
        }

        /**
         * Use HTTP protocol as transport.
         * <br>
         * Configures the Sender to use the HTTP protocol.
         */
        private void http() {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=").put(protocol).put("]");
            }
            protocol = PROTOCOL_HTTP;
        }

        private void tcp() {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=").put(protocol).put("]");
            }
            protocol = PROTOCOL_TCP;
        }

        private void udp() {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=").put(protocol).put("]");
            }
            protocol = PROTOCOL_UDP;
        }

        private void validateParameters() {
            if (hosts.size() == 0) {
                throw new LineSenderException("questdb server address not set");
            }
            if (hosts.size() != ports.size()) {
                throw new LineSenderException("mismatch between number of hosts and number of ports");
            }
            for (int i = 0, n = hosts.size(); i < n; i++) {
                String host = hosts.get(i);
                int port = ports.getQuick(i);
                for (int j = i + 1; j < n; j++) {
                    if (ports.getQuick(j) == port && Chars.equals(host, hosts.get(j))) {
                        throw new LineSenderException("duplicated addresses are not allowed [address=")
                                .put(host).put(':').put(port).put("]");
                    }
                }
            }
            if (!tlsEnabled && trustStorePath != null) {
                throw new LineSenderException("custom trust store configured, but TLS was not enabled ")
                        .put("[path=").put(LineSenderBuilder.this.trustStorePath).put("]");
            }
            if (!tlsEnabled && tlsValidationMode != TlsValidationMode.DEFAULT) {
                throw new LineSenderException("TLS validation disabled, but TLS was not enabled");
            }
            if (keyId != null && bufferCapacity < MIN_BUFFER_SIZE) {
                throw new LineSenderException("Requested buffer too small ")
                        .put("[minimalCapacity=").put(MIN_BUFFER_SIZE)
                        .put(", requestedCapacity=").put(bufferCapacity)
                        .put("]");
            }
            if (requestDurableAck && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("request_durable_ack is only supported for WebSocket transport");
            }
            if (protocol == PROTOCOL_HTTP) {
                if (httpClientConfiguration.getMaximumRequestBufferSize() < httpClientConfiguration.getInitialRequestBufferSize()) {
                    throw new LineSenderException("maximum buffer capacity cannot be less than initial buffer capacity ")
                            .put("[maximumBufferCapacity=").put(httpClientConfiguration.getMaximumRequestBufferSize())
                            .put(", initialBufferCapacity=").put(httpClientConfiguration.getInitialRequestBufferSize())
                            .put("]");
                }
                if (privateKey != null) {
                    throw new LineSenderException("plain old token authentication is not supported for HTTP protocol. Did you mean to use HTTP token authentication?");
                }
            } else if (protocol == PROTOCOL_TCP) {
                if (username != null || password != null) {
                    throw new LineSenderException("username/password authentication is not supported for TCP protocol");
                }
                if (httpPath != null) {
                    throw new LineSenderException("HTTP path is not supported for TCP protocol");
                }
                if (httpSettingsPath != null) {
                    throw new LineSenderException("HTTP settings path is not supported for TCP protocol");
                }
                if (autoFlushRows == AUTO_FLUSH_DISABLED) {
                    throw new LineSenderException("disabling auto-flush is not supported for TCP protocol");
                } else if (autoFlushRows != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("auto flush rows is not supported for TCP protocol");
                }
                if (httpToken != null) {
                    throw new LineSenderException("HTTP token authentication is not supported for TCP protocol");
                }
                if (retryTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("retrying is not supported for TCP protocol");
                }
                if (httpTimeout != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("HTTP timeout is not supported for TCP protocol");
                }
                if (minRequestThroughput != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("minimum request throughput is not supported for TCP protocol");
                }
                if (maximumBufferCapacity != bufferCapacity) {
                    throw new LineSenderException("maximum buffer capacity must be the same as initial buffer capacity for TCP protocol")
                            .put("[maximumBufferCapacity=").put(maximumBufferCapacity)
                            .put(", initialBufferCapacity=").put(bufferCapacity)
                            .put("]");
                }
                if (autoFlushIntervalMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("auto flush interval is not supported for TCP protocol");
                }
            } else if (protocol == PROTOCOL_UDP) {
                if (privateKey != null) {
                    throw new LineSenderException("authentication is not supported for UDP transport");
                }
                if (httpToken != null) {
                    throw new LineSenderException("HTTP token authentication is not supported for UDP transport");
                }
                if (username != null || password != null) {
                    throw new LineSenderException("username/password authentication is not supported for UDP transport");
                }
                if (tlsEnabled) {
                    throw new LineSenderException("TLS is not supported for UDP transport");
                }
                if (retryTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("retry timeout is not supported for UDP transport");
                }
                if (httpTimeout != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("HTTP timeout is not supported for UDP transport");
                }
                if (minRequestThroughput != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("minimum request throughput is not supported for UDP transport");
                }
                if (protocolVersion != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("protocol version is not supported for UDP transport");
                }
                if (httpPath != null) {
                    throw new LineSenderException("HTTP path is not supported for UDP transport");
                }
                if (httpSettingsPath != null) {
                    throw new LineSenderException("HTTP settings path is not supported for UDP transport");
                }
                if (maxBackoffMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("max backoff is not supported for UDP transport");
                }
                if (autoFlushRows != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("auto flush rows is not supported for UDP transport");
                }
                if (autoFlushIntervalMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("auto flush interval is not supported for UDP transport");
                }
                if (autoFlushBytes != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("auto flush bytes is not supported for UDP transport");
                }
            } else if (protocol == PROTOCOL_WEBSOCKET) {
                if (privateKey != null) {
                    throw new LineSenderException("TCP authentication is not supported for WebSocket protocol");
                }
                if (httpToken != null && (username != null || password != null)) {
                    throw new LineSenderException("cannot use both token and username/password authentication");
                }
                if (httpPath != null) {
                    throw new LineSenderException("HTTP path is not supported for WebSocket protocol");
                }
                if (httpSettingsPath != null) {
                    throw new LineSenderException("HTTP settings path is not supported for WebSocket protocol");
                }
                if (httpTimeout != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("HTTP timeout is not supported for WebSocket protocol");
                }
                if (retryTimeoutMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("retry timeout is not supported for WebSocket protocol");
                }
                if (minRequestThroughput != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("minimum request throughput is not supported for WebSocket protocol");
                }
                if (maxBackoffMillis != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("max backoff is not supported for WebSocket protocol");
                }
                if (protocolVersion != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("protocol version is not supported for WebSocket protocol");
                }
                if (autoFlushIntervalMillis == Integer.MAX_VALUE) {
                    throw new LineSenderException("disabling auto-flush is not supported for WebSocket protocol");
                }
                // The cursor send path does not fsync yet, so any sf_durability
                // other than memory is rejected rather than silently downgraded.
                // Validating it here (rather than at connect time) lets a
                // no-connect config check reject it as a full build() does.
                if (sfDurability != SfDurability.MEMORY) {
                    throw new LineSenderException(
                            "sf_durability=" + sfDurability.name().toLowerCase()
                                    + " is not yet supported (deferred follow-up; use sf_durability=memory)");
                }
            } else {
                throw new LineSenderException("unsupported protocol ")
                        .put("[protocol=").put(protocol).put("]");
            }
        }

        private void websocket() {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=").put(protocol).put("]");
            }
            protocol = PROTOCOL_WEBSOCKET;
        }

        public class AdvancedTlsSettings {
            /**
             * Configure a custom truststore. This is only needed when using {@link #enableTls()} when your default
             * truststore does not contain certificate chain used by a server. Most users should not need it.
             * <br>
             * The path can be either a path on a local filesystem. Or you can prefix it with "classpath:" to instruct
             * the Sender to load a trust store from a classpath.
             *
             * @param trustStorePath     a path to a trust store.
             * @param trustStorePassword a password to for the truststore
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder customTrustStore(String trustStorePath, char[] trustStorePassword) {
                if (LineSenderBuilder.this.trustStorePath != null) {
                    throw new LineSenderException("custom trust store was already configured ")
                            .put("[path=").put(LineSenderBuilder.this.trustStorePath).put("]");
                }
                if (Chars.isBlank(trustStorePath)) {
                    throw new LineSenderException("trust store path cannot be empty nor null");
                }
                if (trustStorePassword == null) {
                    throw new LineSenderException("trust store password cannot be null");
                }

                LineSenderBuilder.this.trustStorePath = trustStorePath;
                LineSenderBuilder.this.trustStorePassword = trustStorePassword;
                return LineSenderBuilder.this;
            }

            /**
             * This server certification validation altogether.
             * This is suitable when testing self-signed certificate. It's inherently insecure and should
             * never be used in a production.
             * <br>
             * If you cannot use trusted certificate then you should prefer {@link  #customTrustStore(String, char[])}
             * over disabling validation.
             *
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder disableCertificateValidation() {
                LineSenderBuilder.this.tlsValidationMode = TlsValidationMode.INSECURE;
                return LineSenderBuilder.this;
            }

        }

        /**
         * Auxiliary class to configure client authentication.
         * If you have an instance of {@link PrivateKey} then you can pass it directly.
         * Alternative a private key encoded as a string token can be used too.
         */
        public class AuthBuilder {

            /**
             * Authenticate by using a token.
             *
             * @param token authentication token
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder authToken(String token) {
                if (Chars.isBlank(token)) {
                    throw new LineSenderException("token cannot be empty nor null");
                }
                try {
                    LineSenderBuilder.this.privateKey = AuthUtils.toPrivateKey(token);
                } catch (IllegalArgumentException e) {
                    throw new LineSenderException("could not import token", e);
                }
                LineSenderBuilder.this.shouldDestroyPrivKey = true;
                return LineSenderBuilder.this;
            }

            /**
             * Authenticate by using a {@link PrivateKey} directly.
             *
             * @param privateKey authentication private key
             * @return an instance of LineSenderBuilder for further configuration
             */
            public LineSenderBuilder privateKey(PrivateKey privateKey) {
                LineSenderBuilder.this.privateKey = privateKey;
                return LineSenderBuilder.this;
            }
        }
    }
}
