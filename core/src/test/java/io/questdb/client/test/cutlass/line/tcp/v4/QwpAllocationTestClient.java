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

package io.questdb.client.test.cutlass.line.tcp.v4;

import io.questdb.client.Sender;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Test client for ILP allocation profiling.
 * <p>
 * Supports 4 protocol modes:
 * <ul>
 *   <li>ilp-tcp: Old ILP text protocol over TCP (port 9009)</li>
 *   <li>ilp-http: Old ILP text protocol over HTTP (port 9000)</li>
 *   <li>qwp-websocket: New QWP binary protocol over WebSocket (port 9000)</li>
 *   <li>qwp-udp: New QWP binary protocol over UDP (port 9007)</li>
 * </ul>
 * <p>
 * Sends rows with various column types to exercise all code paths.
 * Run with an allocation profiler (async-profiler, JFR, etc.) to find hotspots.
 * <p>
 * Usage:
 * <pre>
 * java -cp ... QwpAllocationTestClient [options]
 *
 * Options:
 *   --protocol=PROTOCOL       Protocol: ilp-tcp, ilp-http, qwp-websocket, qwp-udp (default: qwp-websocket)
 *   --host=HOST               Server host (default: localhost)
 *   --port=PORT               Server port (default: 9009 for TCP, 9000 for HTTP/WS, 9007 for UDP)
 *   --rows=N                  Total rows to send (default: 10000000)
 *   --batch=N                 Batch/flush size (default: 10000)
 *   --max-datagram-size=N     Max datagram size in bytes (UDP only, default: 1400)
 *   --warmup=N                Warmup rows (default: 100000)
 *   --report=N                Report progress every N rows (default: 1000000)
 *   --target-throughput=N      Target throughput in rows/sec (0 = unlimited, default: 0)
 *   --no-warmup               Skip warmup phase
 *   --help                    Show this help
 *
 * Examples:
 *   QwpAllocationTestClient --protocol=qwp-websocket --rows=1000000 --batch=5000
 *   QwpAllocationTestClient --protocol=qwp-udp --rows=1000000 --max-datagram-size=8192
 *   QwpAllocationTestClient --protocol=ilp-tcp --host=remote-server --port=9009
 * </pre>
 */
public class QwpAllocationTestClient {

    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_FLUSH_BYTES = 0; // 0 = use protocol default
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 0; // 0 = use protocol default
    // Default configuration
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_IN_FLIGHT_WINDOW = 0; // 0 = use protocol default (8)
    private static final int DEFAULT_MAX_DATAGRAM_SIZE = 0; // 0 = use protocol default (1400)
    private static final int DEFAULT_REPORT_INTERVAL = 1_000_000;
    private static final int DEFAULT_ROWS = 80_000_000;
    private static final int DEFAULT_TARGET_THROUGHPUT = 0; // 0 = unlimited
    private static final int DEFAULT_WARMUP_ROWS = 100_000;
    private static final String PROTOCOL_ILP_HTTP = "ilp-http";
    // Protocol modes
    private static final String PROTOCOL_ILP_TCP = "ilp-tcp";
    private static final String PROTOCOL_QWP_UDP = "qwp-udp";
    private static final String PROTOCOL_QWP_WEBSOCKET = "qwp-websocket";
    private static final String[] STRINGS = {
            "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Singapore",
            "Hong Kong", "Dubai", "Mumbai", "Shanghai", "Moscow", "Seoul", "Bangkok",
            "Amsterdam", "Zurich", "Frankfurt", "Milan", "Madrid"
    };
    // Pre-computed test data to avoid allocation during the test
    private static final String[] SYMBOLS = {
            "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "BRK.A", "JPM", "JNJ",
            "V", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "CMCSA"
    };
    private static final String TABLE_NAME = "ilp_alloc_test";

    public static void main(String[] args) {
        // Parse command-line options
        String protocol = PROTOCOL_QWP_WEBSOCKET;
        String host = DEFAULT_HOST;
        int port = -1; // -1 means use default for protocol
        int totalRows = DEFAULT_ROWS;
        int batchSize = DEFAULT_BATCH_SIZE;
        int flushBytes = DEFAULT_FLUSH_BYTES;
        long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        int inFlightWindow = DEFAULT_IN_FLIGHT_WINDOW;
        int maxDatagramSize = DEFAULT_MAX_DATAGRAM_SIZE;
        int warmupRows = DEFAULT_WARMUP_ROWS;
        int reportInterval = DEFAULT_REPORT_INTERVAL;
        int targetThroughput = DEFAULT_TARGET_THROUGHPUT;
        boolean isDropTable = true;

        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            } else if (arg.startsWith("--protocol=")) {
                protocol = arg.substring("--protocol=".length()).toLowerCase();
            } else if (arg.startsWith("--host=")) {
                host = arg.substring("--host=".length());
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--rows=")) {
                totalRows = Integer.parseInt(arg.substring("--rows=".length()));
            } else if (arg.startsWith("--batch=")) {
                batchSize = Integer.parseInt(arg.substring("--batch=".length()));
            } else if (arg.startsWith("--flush-bytes=")) {
                flushBytes = Integer.parseInt(arg.substring("--flush-bytes=".length()));
            } else if (arg.startsWith("--flush-interval-ms=")) {
                flushIntervalMs = Long.parseLong(arg.substring("--flush-interval-ms=".length()));
            } else if (arg.startsWith("--in-flight-window=")) {
                inFlightWindow = Integer.parseInt(arg.substring("--in-flight-window=".length()));
            } else if (arg.startsWith("--max-datagram-size=")) {
                maxDatagramSize = Integer.parseInt(arg.substring("--max-datagram-size=".length()));
            } else if (arg.startsWith("--warmup=")) {
                warmupRows = Integer.parseInt(arg.substring("--warmup=".length()));
            } else if (arg.startsWith("--report=")) {
                reportInterval = Integer.parseInt(arg.substring("--report=".length()));
            } else if (arg.startsWith("--target-throughput=")) {
                targetThroughput = Integer.parseInt(arg.substring("--target-throughput=".length()));
            } else if (arg.equals("--no-warmup")) {
                warmupRows = 0;
            } else if (arg.equals("--no-drop")) {
                isDropTable = false;
            } else if (!arg.startsWith("--")) {
                // Legacy positional args: protocol [host] [port] [rows]
                protocol = arg.toLowerCase();
            } else {
                System.err.println("Unknown option: " + arg);
                printUsage();
                System.exit(1);
            }
        }

        // Use default port if not specified
        if (port == -1) {
            port = getDefaultPort(protocol);
        }

        System.out.println("ILP Allocation Test Client");
        System.out.println("==========================");
        System.out.println("Protocol: " + protocol);
        System.out.println("Host: " + host);
        System.out.println("Port: " + port);
        System.out.println("Total rows: " + String.format("%,d", totalRows));
        System.out.println("Batch size (rows): " + String.format("%,d", batchSize) + (batchSize == 0 ? " (default)" : ""));
        System.out.println("Flush bytes: " + (flushBytes == 0 ? "(default)" : String.format("%,d", flushBytes)));
        System.out.println("Flush interval: " + (flushIntervalMs == 0 ? "(default)" : flushIntervalMs + " ms"));
        System.out.println("In-flight window: " + (inFlightWindow == 0 ? "(default: 8)" : inFlightWindow));
        System.out.println("Max datagram size: " + (maxDatagramSize == 0 ? "(default: 1400)" : maxDatagramSize));
        System.out.println("Warmup rows: " + String.format("%,d", warmupRows));
        System.out.println("Report interval: " + String.format("%,d", reportInterval));
        System.out.println("Target throughput: " + (targetThroughput == 0 ? "(unlimited)" : String.format("%,d", targetThroughput) + " rows/sec"));
        System.out.println();

        try {
            if (isDropTable) {
                recreateTable(host);
            } else {
                System.out.println("Skipping table drop (--no-drop)");
            }
            runTest(protocol, host, port, totalRows, batchSize, flushBytes, flushIntervalMs,
                    maxDatagramSize, warmupRows, reportInterval, targetThroughput);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static Sender createSender(
            String protocol,
            String host,
            int port,
            int batchSize,
            int flushBytes,
            long flushIntervalMs,
            int maxDatagramSize
    ) {
        switch (protocol) {
            case PROTOCOL_ILP_TCP:
                return Sender.builder(Sender.Transport.TCP)
                        .address(host)
                        .port(port)
                        .build();
            case PROTOCOL_ILP_HTTP:
                return Sender.builder(Sender.Transport.HTTP)
                        .address(host)
                        .port(port)
                        .autoFlushRows(batchSize)
                        .build();
            case PROTOCOL_QWP_WEBSOCKET:
                Sender.LineSenderBuilder wsBuilder = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(host)
                        .port(port);
                if (batchSize > 0) wsBuilder.autoFlushRows(batchSize);
                if (flushBytes >= 0) wsBuilder.autoFlushBytes(flushBytes);
                if (flushIntervalMs > 0) wsBuilder.autoFlushIntervalMillis((int) flushIntervalMs);
                return wsBuilder.build();
            case PROTOCOL_QWP_UDP:
                Sender.LineSenderBuilder udpBuilder = Sender.builder(Sender.Transport.UDP)
                        .address(host)
                        .port(port);
                if (maxDatagramSize > 0) udpBuilder.maxDatagramSize(maxDatagramSize);
                return udpBuilder.build();
            default:
                throw new IllegalArgumentException("Unknown protocol: " + protocol +
                        ". Use one of: ilp-tcp, ilp-http, qwp-websocket, qwp-udp");
        }
    }

    /**
     * Estimates the size of a single row in bytes for throughput calculation.
     */
    private static int estimatedRowSize() {
        // Rough estimate (binary protocol):
        // - 2 symbols: ~10 bytes each = 20 bytes
        // - 3 longs: 8 bytes each = 24 bytes
        // - 4 doubles: 8 bytes each = 32 bytes
        // - 1 string: ~10 bytes average
        // - 1 boolean: 1 byte
        // - 2 timestamps: 8 bytes each = 16 bytes
        // - Overhead: ~20 bytes
        // Total: ~123 bytes
        return 123;
    }

    private static int getDefaultPort(String protocol) {
        if (PROTOCOL_ILP_HTTP.equals(protocol) || PROTOCOL_QWP_WEBSOCKET.equals(protocol)) {
            return 9000;
        }
        if (PROTOCOL_QWP_UDP.equals(protocol)) {
            return 9007;
        }
        return 9009;
    }

    private static void printUsage() {
        System.out.println("ILP Allocation Test Client");
        System.out.println();
        System.out.println("Usage: QwpAllocationTestClient [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --protocol=PROTOCOL      Protocol to use (default: qwp-websocket)");
        System.out.println("  --host=HOST              Server host (default: localhost)");
        System.out.println("  --port=PORT              Server port (default: 9009 for TCP, 9000 for HTTP/WS, 9007 for UDP)");
        System.out.println("  --rows=N                 Total rows to send (default: 80000000)");
        System.out.println("  --batch=N                Auto-flush after N rows (default: 10000)");
        System.out.println("  --flush-bytes=N          Auto-flush after N bytes (default: protocol default)");
        System.out.println("  --flush-interval-ms=N    Auto-flush after N ms (default: protocol default)");
        System.out.println("  --in-flight-window=N     Max batches awaiting server ACK (default: 8, WebSocket only)");
        System.out.println("  --send-queue=N           Max batches waiting to send (default: 16, WebSocket only)");
        System.out.println("  --max-datagram-size=N    Max datagram size in bytes (default: 1400, UDP only)");
        System.out.println("  --warmup=N               Warmup rows (default: 100000)");
        System.out.println("  --report=N               Report progress every N rows (default: 1000000)");
        System.out.println("  --target-throughput=N     Target throughput in rows/sec (0 = unlimited, default: 0)");
        System.out.println("  --no-warmup              Skip warmup phase");
        System.out.println("  --no-drop                Don't drop/recreate the table (for parallel clients)");
        System.out.println("  --help                   Show this help");
        System.out.println();
        System.out.println("Protocols:");
        System.out.println("  ilp-tcp          Old ILP text protocol over TCP (default port: 9009)");
        System.out.println("  ilp-http         Old ILP text protocol over HTTP (default port: 9000)");
        System.out.println("  qwp-websocket    New QWP binary protocol over WebSocket (default port: 9000)");
        System.out.println("  qwp-udp          New QWP binary protocol over UDP (default port: 9007)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  QwpAllocationTestClient --protocol=qwp-websocket --rows=1000000 --batch=5000");
        System.out.println("  QwpAllocationTestClient --protocol=qwp-udp --rows=1000000 --max-datagram-size=8192");
        System.out.println("  QwpAllocationTestClient --protocol=ilp-tcp --host=remote-server");
        System.out.println("  QwpAllocationTestClient --protocol=ilp-tcp --rows=100000 --no-warmup");
    }

    private static void recreateTable(String host) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        String url = "jdbc:postgresql://" + host + ":8812/qdb";
        try (Connection conn = DriverManager.getConnection(url, properties);
             Statement st = conn.createStatement()
        ) {
            st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            st.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + " timestamp TIMESTAMP,"
                    + " exchange SYMBOL,"
                    + " currency SYMBOL,"
                    + " trade_id LONG,"
                    + " volume LONG,"
                    + " price DOUBLE,"
                    + " bid DOUBLE,"
                    + " ask DOUBLE,"
                    + " sequence LONG,"
                    + " spread DOUBLE,"
                    + " venue VARCHAR,"
                    + " is_buy BOOLEAN,"
                    + " event_time TIMESTAMP"
                    + ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");
        }
        System.out.println("Recreated table " + TABLE_NAME);
    }

    private static void runTest(String protocol, String host, int port, int totalRows,
                                int batchSize, int flushBytes, long flushIntervalMs,
                                int maxDatagramSize,
                                int warmupRows, int reportInterval,
                                int targetThroughput) throws IOException {
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (Sender sender = createSender(protocol, host, port, batchSize, flushBytes, flushIntervalMs,
                maxDatagramSize)) {
            System.out.println("Connected! Protocol: " + protocol);
            System.out.println();

            // Warm-up phase
            if (warmupRows > 0) {
                System.out.println("Warming up (" + String.format("%,d", warmupRows) + " rows)...");
                long warmupStart = System.nanoTime();
                for (int i = 0; i < warmupRows; i++) {
                    sendRow(sender, i);
                }
                sender.flush();
                long warmupTime = System.nanoTime() - warmupStart;
                System.out.println("Warmup complete in " + TimeUnit.NANOSECONDS.toMillis(warmupTime) + " ms");
                System.out.println();

                // Give GC a chance to clean up warmup allocations
                System.gc();
                Thread.sleep(100);
            }

            // Main test phase
            System.out.println("Starting main test (" + String.format("%,d", totalRows) + " rows)...");
            if (reportInterval > 0 && reportInterval <= totalRows) {
                System.out.println("Progress will be reported every " + String.format("%,d", reportInterval) + " rows");
            }
            System.out.println();

            long startTime = System.nanoTime();
            long lastReportTime = startTime;
            int lastReportRows = 0;
            // Pacing: check every ~0.1ms worth of rows to keep bursts small
            int paceCheckInterval = targetThroughput > 0 ? Math.max(1, targetThroughput / 10_000) : 0;
            double nanosPerRow = targetThroughput > 0 ? 1_000_000_000.0 / targetThroughput : 0;

            for (int i = 0; i < totalRows; i++) {
                sendRow(sender, i);

                // Pacing: busy-spin until we're back on schedule
                if (nanosPerRow > 0 && (i + 1) % paceCheckInterval == 0) {
                    long expectedElapsedNanos = (long) ((i + 1) * nanosPerRow);
                    while (System.nanoTime() - startTime < expectedElapsedNanos) {
                        io.questdb.client.std.Compat.onSpinWait();
                    }
                }

                // Report progress
                if (reportInterval > 0 && (i + 1) % reportInterval == 0) {
                    long now = System.nanoTime();
                    long elapsedSinceReport = now - lastReportTime;
                    int rowsSinceReport = (i + 1) - lastReportRows;
                    int rowsPerSec = (int) (rowsSinceReport / (elapsedSinceReport / 1_000_000_000.0));

                    System.out.printf("Progress: %,d / %,d rows (%.1f%%) - %,d rows/sec%n",
                            i + 1, totalRows,
                            (i + 1) * 100.0 / totalRows,
                            rowsPerSec);

                    lastReportTime = now;
                    lastReportRows = i + 1;
                }
            }

            // Final flush
            sender.flush();

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            double totalSeconds = totalTime / 1_000_000_000.0;
            double rowsPerSecond = totalRows / totalSeconds;

            System.out.println();
            System.out.println("Test Complete!");
            System.out.println("==============");
            System.out.println("Protocol: " + protocol);
            System.out.println("Total rows: " + String.format("%,d", totalRows));
            System.out.println("Batch size: " + String.format("%,d", batchSize));
            System.out.println("Total time: " + String.format("%.2f", totalSeconds) + " seconds");
            System.out.println("Throughput: " + String.format("%,.0f", rowsPerSecond) + " rows/second");
            System.out.println("Data rate (before compression): " + String.format("%.2f", ((long) totalRows * estimatedRowSize()) / (1024.0 * 1024.0 * totalSeconds)) + " MB/s (estimated)");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    private static void sendRow(Sender sender, int rowIndex) {
        // Base timestamp with small variations
        long baseTimestamp = 1704067200000000L; // 2024-01-01 00:00:00 UTC in micros
        long timestamp = baseTimestamp + (rowIndex * 1000L) + (rowIndex % 100);

        sender.table(TABLE_NAME)
                // Symbol columns
                .symbol("exchange", SYMBOLS[rowIndex % SYMBOLS.length])
                .symbol("currency", rowIndex % 2 == 0 ? "USD" : "EUR")

                // Numeric columns
                .longColumn("trade_id", rowIndex)
                .longColumn("volume", 100 + (rowIndex % 10000))
                .doubleColumn("price", 100.0 + (rowIndex % 1000) * 0.01)
                .doubleColumn("bid", 99.5 + (rowIndex % 1000) * 0.01)
                .doubleColumn("ask", 100.5 + (rowIndex % 1000) * 0.01)
                .longColumn("sequence", rowIndex % 1000000)
                .doubleColumn("spread", 0.5 + (rowIndex % 100) * 0.01)

                // String column
                .stringColumn("venue", STRINGS[rowIndex % STRINGS.length])

                // Boolean column
                .boolColumn("is_buy", rowIndex % 2 == 0)

                // Additional timestamp column
                .timestampColumn("event_time", timestamp - 1000, ChronoUnit.MICROS)

                // Designated timestamp
                .at(timestamp, ChronoUnit.MICROS);
    }
}
