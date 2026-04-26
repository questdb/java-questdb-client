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
import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.cutlass.qwp.client.sf.SfDiskFullException;
import io.questdb.client.impl.ConfStringParser;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.IntList;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import javax.security.auth.DestroyFailedException;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
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
     * @see #fromConfig(CharSequence)
     * @see LineSenderBuilder#fromConfig(CharSequence)
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
     * Add a column with a Decimal128 value serialized using the binary format.
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
        private static final int DEFAULT_AUTO_FLUSH_INTERVAL_MILLIS = 1_000;
        private static final int DEFAULT_AUTO_FLUSH_ROWS = 75_000;
        private static final int DEFAULT_BUFFER_CAPACITY = 64 * 1024;
        private static final int DEFAULT_HTTP_PORT = 9000;
        private static final int DEFAULT_HTTP_TIMEOUT = 30_000;
        private static final int DEFAULT_IN_FLIGHT_WINDOW_SIZE = 128;
        private static final int DEFAULT_MAXIMUM_BUFFER_CAPACITY = 100 * 1024 * 1024;
        private static final int DEFAULT_MAX_BACKOFF_MILLIS = 1_000;
        private static final int DEFAULT_MAX_DATAGRAM_SIZE = 1400;
        private static final int DEFAULT_MAX_NAME_LEN = 127;
        private static final long DEFAULT_MAX_RETRY_NANOS = TimeUnit.SECONDS.toNanos(10); // keep sync with the contract of the configuration method
        private static final long DEFAULT_MIN_REQUEST_THROUGHPUT = 100 * 1024; // 100KB/s, keep in sync with the contract of the configuration method
        private static final int DEFAULT_TCP_PORT = 9009;
        private static final int DEFAULT_UDP_PORT = 9007;
        private static final int DEFAULT_WEBSOCKET_PORT = 9000;
        private static final int DEFAULT_WS_AUTO_FLUSH_BYTES = 0;
        private static final long DEFAULT_WS_AUTO_FLUSH_INTERVAL_NANOS = 100_000_000L; // 100ms
        private static final int DEFAULT_WS_AUTO_FLUSH_ROWS = 1_000;
        private static final int MIN_BUFFER_SIZE = AuthUtils.CHALLENGE_LEN + 1; // challenge size + 1;
        // The PARAMETER_NOT_SET_EXPLICITLY constant is used to detect if a parameter was set explicitly in configuration parameters
        // where it matters. This is needed to detect invalid combinations of parameters. Why?
        // We want to fail-fast even when an explicitly configured options happens to be same value as the default value,
        // because this still indicates a user error and silently ignoring it could lead to hard-to-debug issues.
        private static final int PARAMETER_NOT_SET_EXPLICITLY = -1;
        private static final int PROTOCOL_HTTP = 1;
        private static final int PROTOCOL_TCP = 0;
        private static final int PROTOCOL_UDP = 3;
        private static final int PROTOCOL_WEBSOCKET = 2;
        private final ObjList<String> hosts = new ObjList<>();
        private final IntList ports = new IntList();
        private int autoFlushBytes = PARAMETER_NOT_SET_EXPLICITLY;
        private int autoFlushIntervalMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private int autoFlushRows = PARAMETER_NOT_SET_EXPLICITLY;
        private int bufferCapacity = PARAMETER_NOT_SET_EXPLICITLY;
        private String httpPath;
        private String httpSettingsPath;
        private int httpTimeout = PARAMETER_NOT_SET_EXPLICITLY;
        private String httpToken;
        private int inFlightWindowSize = PARAMETER_NOT_SET_EXPLICITLY;
        private String keyId;
        private int maxBackoffMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private int maxDatagramSize = PARAMETER_NOT_SET_EXPLICITLY;
        private int maxNameLength = PARAMETER_NOT_SET_EXPLICITLY;
        private int maxSchemasPerConnection = PARAMETER_NOT_SET_EXPLICITLY;
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
        };
        private long minRequestThroughput = PARAMETER_NOT_SET_EXPLICITLY;
        private int multicastTtl = PARAMETER_NOT_SET_EXPLICITLY;
        private String password;
        private PrivateKey privateKey;
        private int protocol = PARAMETER_NOT_SET_EXPLICITLY;
        private int protocolVersion = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean requestDurableAck;
        private int retryTimeoutMillis = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean shouldDestroyPrivKey;
        // Store-and-forward (WebSocket only). storeAndForward must be true AND
        // sfDir must be set for SF to activate.
        private boolean storeAndForward;
        private String sfDir;
        private long sfMaxBytes = PARAMETER_NOT_SET_EXPLICITLY;
        private long sfMaxTotalBytes = PARAMETER_NOT_SET_EXPLICITLY;
        private boolean sfFsync;
        private boolean sfFsyncOnFlush;
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

            // best effort dup detection, we might have incomplete information at this point,
            // for example port or protocol might not be configured yet. so we are conservative
            // and only detect dups when we have full information about the address
            if (parsedPort != -1) {
                // we have a port, so we can do a full dup check
                for (int i = 0, n = hosts.size(); i < n; i++) {
                    String storedHost = hosts.get(i);
                    if (Chars.equals(storedHost, hostSansPort)) {
                        // given host is already configured, let's see if the port is the same
                        if (ports.size() > i) {
                            // ok, the previous address had a port explicitly configured, let's see if it's the same
                            if (ports.getQuick(i) == parsedPort) {
                                throw new LineSenderException("duplicated addresses are not allowed ")
                                        .put("[address=").put(address).put("]");
                            }
                        }
                    }
                }

            }
            this.hosts.add(hostSansPort);
            if (parsedPort != -1) {
                // port was specified in the address, so we use it
                this.ports.add(parsedPort);
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
         * @param enabled ignored
         * @return this instance for method chaining
         * @deprecated Async mode is now derived from {@link #inFlightWindowSize(int)}.
         * Window size 1 implies synchronous mode, greater than 1 implies asynchronous mode.
         * The default window size is 128 (asynchronous). Call {@code inFlightWindowSize(1)}
         * for synchronous behavior.
         * <br>
         * This method is a no-op and will be removed in a future release.
         */
        @Deprecated
        public LineSenderBuilder asyncMode(boolean enabled) {
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
                if (hosts.size() != 1 || ports.size() != 1) {
                    throw new LineSenderException("only a single address (host:port) is supported for WebSocket transport");
                }

                int actualAutoFlushRows = autoFlushRows == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_WS_AUTO_FLUSH_ROWS : autoFlushRows;
                int actualAutoFlushBytes = autoFlushBytes == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_WS_AUTO_FLUSH_BYTES : autoFlushBytes;
                long actualAutoFlushIntervalNanos = autoFlushIntervalMillis == PARAMETER_NOT_SET_EXPLICITLY
                        ? DEFAULT_WS_AUTO_FLUSH_INTERVAL_NANOS
                        : TimeUnit.MILLISECONDS.toNanos(autoFlushIntervalMillis);
                int actualInFlightWindowSize = inFlightWindowSize == PARAMETER_NOT_SET_EXPLICITLY ? DEFAULT_IN_FLIGHT_WINDOW_SIZE : inFlightWindowSize;
                int actualMaxSchemasPerConnection = maxSchemasPerConnection == PARAMETER_NOT_SET_EXPLICITLY
                        ? QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION : maxSchemasPerConnection;

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

                SegmentLog segmentLog = null;
                if (storeAndForward) {
                    if (sfDir == null) {
                        throw new LineSenderException(
                                "store_and_forward=on requires sf_dir to be set");
                    }
                    if (actualInFlightWindowSize <= 1) {
                        throw new LineSenderException(
                                "store_and_forward requires async mode (in_flight_window > 1)");
                    }
                    long actualSfMaxBytes = sfMaxBytes == PARAMETER_NOT_SET_EXPLICITLY
                            ? SegmentLog.DEFAULT_MAX_BYTES_PER_SEGMENT
                            : sfMaxBytes;
                    long actualSfMaxTotalBytes = sfMaxTotalBytes == PARAMETER_NOT_SET_EXPLICITLY
                            ? SegmentLog.DEFAULT_MAX_TOTAL_BYTES
                            : sfMaxTotalBytes;
                    segmentLog = SegmentLog.open(
                            sfDir, actualSfMaxBytes, actualSfMaxTotalBytes, sfFsync);
                } else if (sfDir != null) {
                    throw new LineSenderException(
                            "sf_dir is set but store_and_forward is not enabled");
                }

                try {
                    return QwpWebSocketSender.connect(
                            hosts.getQuick(0),
                            ports.getQuick(0),
                            wsTlsConfig,
                            actualAutoFlushRows,
                            actualAutoFlushBytes,
                            actualAutoFlushIntervalNanos,
                            actualInFlightWindowSize,
                            wsAuthHeader,
                            actualMaxSchemasPerConnection,
                            requestDurableAck,
                            segmentLog,
                            sfFsyncOnFlush
                    );
                } catch (Throwable t) {
                    // If connect failed, the sender's close() ran and would have closed
                    // the log; but if setSegmentLog never ran (e.g. validation threw earlier
                    // in the connect path), we have to clean it up ourselves.
                    if (segmentLog != null) {
                        try {
                            segmentLog.close();
                        } catch (Throwable ignored) {
                            // best-effort
                        }
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
         * Set the maximum number of batches that can be in-flight awaiting server acknowledgment.
         * <br>
         * This is only used when communicating over WebSocket transport.
         * <br>
         * A value of 1 means synchronous mode: each batch waits for an ACK before sending the next one.
         * A value greater than 1 enables asynchronous mode with pipelined sends and a background I/O thread.
         * <br>
         * Default value is 128 (asynchronous).
         *
         * @param size maximum number of in-flight batches
         * @return this instance for method chaining
         */
        public LineSenderBuilder inFlightWindowSize(int size) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("in-flight window size is only supported for WebSocket transport");
            }
            if (this.inFlightWindowSize != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("in-flight window size was already configured")
                        .put("[size=").put(this.inFlightWindowSize).put("]");
            }
            if (size < 1) {
                throw new LineSenderException("in-flight window size must be positive")
                        .put("[size=").put(size).put("]");
            }
            this.inFlightWindowSize = size;
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
         * Sets the maximum number of distinct schemas the WebSocket sender may assign on one connection.
         */
        public LineSenderBuilder maxSchemasPerConnection(int maxSchemasPerConnection) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("max schemas per connection is only supported for WebSocket transport");
            }
            if (this.maxSchemasPerConnection != PARAMETER_NOT_SET_EXPLICITLY) {
                throw new LineSenderException("max schemas per connection was already configured")
                        .put("[maxSchemasPerConnection=").put(this.maxSchemasPerConnection).put("]");
            }
            if (maxSchemasPerConnection < 1) {
                throw new LineSenderException("max schemas per connection must be positive")
                        .put("[maxSchemasPerConnection=").put(maxSchemasPerConnection).put("]");
            }
            this.maxSchemasPerConnection = maxSchemasPerConnection;
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
         * Opts the connection in for STATUS_DURABLE_ACK frames. When enabled,
         * servers with primary replication will emit per-table durable-upload
         * watermarks as WAL data reaches the object store.
         * <p>
         * This setting is only supported for WebSocket transport.
         * <p>
         * Observe durable progress via
         * {@link QwpWebSocketSender#getHighestDurableSeqTxn(CharSequence)}.
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
         * Toggle store-and-forward. Must be paired with
         * {@link #storeAndForwardDir(String)}; activating SF without a dir is a
         * configuration error caught at build() time. WebSocket transport only.
         */
        public LineSenderBuilder storeAndForward(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            this.storeAndForward = enabled;
            return this;
        }

        /**
         * Set the store-and-forward directory. Has effect only when SF is also
         * enabled via {@link #storeAndForward(boolean)} (or {@code store_and_forward=on}
         * in the connect string).
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
         * The sender takes ownership of the underlying SegmentLog and closes it
         * when the sender itself is closed.
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
         * Maximum bytes per segment file before rotation. Defaults to
         * {@link SegmentLog#DEFAULT_MAX_BYTES_PER_SEGMENT}
         * (64 MiB). Smaller segments mean faster trim of acked data; larger
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
         * Hard cap on total bytes consumed by SF on disk. When the cap is reached,
         * subsequent appends throw {@link SfDiskFullException}
         * which propagates as back-pressure: {@code flush()} blocks on the user
         * thread until ACKs trim acknowledged segments and free space. Default is
         * unbounded ({@link Long#MAX_VALUE}).
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

        /**
         * When enabled, every successful SF append calls {@code fsync} on the
         * active segment file before returning. Trades throughput for the
         * strongest durability guarantee — every captured frame survives an OS
         * crash, not just a process crash.
         * <p>
         * Default: off. With {@code sf_fsync=off}, fsync only fires on
         * segment rotation and new-segment header creation; bytes appended to
         * the active segment between rotations live only in the OS page cache
         * and may be lost in an OS crash, kernel panic, or power loss. The
         * JVM going down is survived (the page cache outlives the process).
         * <p>
         * If you flush coarsely (one fsync per flush is acceptable) and want
         * OS-crash survival without paying per-append fsync cost, set
         * {@link #storeAndForwardFsyncOnFlush(boolean)} instead.
         */
        public LineSenderBuilder storeAndForwardFsync(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            this.sfFsync = enabled;
            return this;
        }

        /**
         * When enabled, every successful {@code Sender.flush()} (and the
         * implicit flush during {@code close()}) calls {@code fsync} on the
         * SF active segment file before returning. Trades flush latency
         * (one fsync per flush) for OS-crash survival of every byte that
         * the user explicitly flushed.
         * <p>
         * Off by default. Use this when batches are large or flushes are
         * coarse and you want OS-crash durability without paying the
         * per-append fsync cost of {@link #storeAndForwardFsync(boolean)}.
         * Avoid it when batches are small and flushes are frequent — every
         * flush blocks on a disk fsync, which is typically the slowest
         * operation in the SF write path.
         * <p>
         * Combining {@code sf_fsync=on} and {@code sf_fsync_on_flush=on}
         * is allowed but redundant: per-append fsync already covers every
         * byte before flush returns.
         */
        public LineSenderBuilder storeAndForwardFsyncOnFlush(boolean enabled) {
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY && protocol != PROTOCOL_WEBSOCKET) {
                throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
            }
            this.sfFsyncOnFlush = enabled;
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

        private static int getValue(CharSequence configurationString, int pos, StringSink sink, String name) {
            if ((pos = ConfStringParser.value(configurationString, pos, sink)) < 0) {
                throw new LineSenderException("invalid ").put(name).put(" [error=").put(sink).put("]");
            }
            return pos;
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
            if (ports.size() == 0) {
                if (protocol == PROTOCOL_HTTP) {
                    ports.add(DEFAULT_HTTP_PORT);
                } else if (protocol == PROTOCOL_UDP) {
                    ports.add(DEFAULT_UDP_PORT);
                } else if (protocol == PROTOCOL_WEBSOCKET) {
                    ports.add(DEFAULT_WEBSOCKET_PORT);
                } else {
                    ports.add(DEFAULT_TCP_PORT);
                }
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
            if (protocol != PARAMETER_NOT_SET_EXPLICITLY) {
                String protocolName;
                switch (protocol) {
                    case PROTOCOL_HTTP:
                        protocolName = "http";
                        break;
                    case PROTOCOL_UDP:
                        protocolName = "udp";
                        break;
                    case PROTOCOL_WEBSOCKET:
                        protocolName = "websocket";
                        break;
                    default:
                        protocolName = "tcp";
                        break;
                }
                throw new LineSenderException("protocol was already configured ")
                        .put("[protocol=")
                        .put(protocolName).put("]");
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
                    address(sink);
                    if (ports.size() == hosts.size() - 1) {
                        // not set
                        port(protocol == PROTOCOL_HTTP ? DEFAULT_HTTP_PORT
                                : protocol == PROTOCOL_UDP ? DEFAULT_UDP_PORT
                                : protocol == PROTOCOL_WEBSOCKET ? DEFAULT_WEBSOCKET_PORT
                                : DEFAULT_TCP_PORT);
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
                    int maxBufferSize = parseIntValue(sink, "max_buf_size");
                    maxBufferCapacity(maxBufferSize);
                } else if (Chars.equals("max_name_len", sink)) {
                    pos = getValue(configurationString, pos, sink, "max_name_len");
                    int len = parseIntValue(sink, "max_name_len");
                    maxNameLength(len);
                } else if (Chars.equals("init_buf_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "init_buf_size");
                    int initBufSize = parseIntValue(sink, "init_buf_size");
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
                } else if (Chars.equals("in_flight_window", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("in_flight_window is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "in_flight_window");
                    int windowSize = parseIntValue(sink, "in_flight_window");
                    inFlightWindowSize(windowSize);
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
                } else if (Chars.equals("max_schemas_per_connection", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("max_schemas_per_connection is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "max_schemas_per_connection");
                    int maxSchemas = parseIntValue(sink, "max_schemas_per_connection");
                    maxSchemasPerConnection(maxSchemas);
                } else if (Chars.equals("store_and_forward", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("store_and_forward is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "store_and_forward");
                    if (Chars.equalsIgnoreCase("on", sink)) {
                        storeAndForward(true);
                    } else if (Chars.equalsIgnoreCase("off", sink)) {
                        storeAndForward(false);
                    } else {
                        throw new LineSenderException("invalid store_and_forward [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else if (Chars.equals("sf_dir", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_dir is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_dir");
                    storeAndForwardDir(sink.toString());
                } else if (Chars.equals("sf_max_bytes", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_max_bytes is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_max_bytes");
                    long maxBytes = parseLongValue(sink, "sf_max_bytes");
                    storeAndForwardMaxBytes(maxBytes);
                } else if (Chars.equals("sf_max_total_bytes", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_max_total_bytes is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_max_total_bytes");
                    long maxTotal = parseLongValue(sink, "sf_max_total_bytes");
                    storeAndForwardMaxTotalBytes(maxTotal);
                } else if (Chars.equals("sf_fsync", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_fsync is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_fsync");
                    if (Chars.equalsIgnoreCase("on", sink)) {
                        storeAndForwardFsync(true);
                    } else if (Chars.equalsIgnoreCase("off", sink)) {
                        storeAndForwardFsync(false);
                    } else {
                        throw new LineSenderException("invalid sf_fsync [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else if (Chars.equals("sf_fsync_on_flush", sink)) {
                    if (protocol != PROTOCOL_WEBSOCKET) {
                        throw new LineSenderException("sf_fsync_on_flush is only supported for WebSocket transport");
                    }
                    pos = getValue(configurationString, pos, sink, "sf_fsync_on_flush");
                    if (Chars.equalsIgnoreCase("on", sink)) {
                        storeAndForwardFsyncOnFlush(true);
                    } else if (Chars.equalsIgnoreCase("off", sink)) {
                        storeAndForwardFsyncOnFlush(false);
                    } else {
                        throw new LineSenderException("invalid sf_fsync_on_flush [value=").put(sink).put(", allowed-values=[on, off]]");
                    }
                } else if (Chars.equals("max_datagram_size", sink)) {
                    pos = getValue(configurationString, pos, sink, "max_datagram_size");
                    int mds = parseIntValue(sink, "max_datagram_size");
                    maxDatagramSize(mds);
                } else if (Chars.equals("multicast_ttl", sink)) {
                    pos = getValue(configurationString, pos, sink, "multicast_ttl");
                    int ttl = parseIntValue(sink, "multicast_ttl");
                    multicastTtl(ttl);
                } else {
                    // ignore unknown keys, unless they are malformed
                    if ((pos = ConfStringParser.value(configurationString, pos, sink)) < 0) {
                        throw new LineSenderException("invalid parameter [error=").put(sink).put("]");
                    }
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
                if (inFlightWindowSize != PARAMETER_NOT_SET_EXPLICITLY) {
                    throw new LineSenderException("in-flight window size is not supported for UDP transport");
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
