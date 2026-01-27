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

package io.questdb.client.test;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf16Sink;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.std.Numbers.hexDigits;
import static io.questdb.client.test.tools.TestUtils.assertEventually;
import static java.time.temporal.ChronoField.*;

public class AbstractQdbTest extends AbstractTest {

    protected static final StringSink sink = new StringSink();
    private static final DateTimeFormatter DATE_TIME_FORMATTER;
    // Configuration defaults
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_HTTP_PORT = 9000;
    private static final String DEFAULT_PG_PASSWORD = "quest";
    private static final int DEFAULT_PG_PORT = 8812;
    private static final String DEFAULT_PG_USER = "admin";
    // Table name counter for uniqueness
    private static final AtomicLong TABLE_NAME_COUNTER = new AtomicLong(System.currentTimeMillis());
    // Shared PostgreSQL connection
    private static Connection pgConnection;

    /**
     * Print the output of a SQL query to TSV format.
     */
    public static long printToSink(StringSink sink, ResultSet rs) throws SQLException {
        // dump metadata
        ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                sink.put('\t');
            }

            sink.put(metaData.getColumnName(i + 1));
        }
        sink.put('\n');

        Timestamp timestamp;
        long rows = 0;
        while (rs.next()) {
            rows++;
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sink.put('\t');
                }
                switch (JDBCType.valueOf(metaData.getColumnType(i))) {
                    case VARCHAR:
                    case NUMERIC:
                        String stringValue = rs.getString(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(stringValue);
                        }
                        break;
                    case INTEGER:
                        int intValue = rs.getInt(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(intValue);
                        }
                        break;
                    case DOUBLE:
                        double doubleValue = rs.getDouble(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(doubleValue);
                        }
                        break;
                    case TIMESTAMP:
                        timestamp = rs.getTimestamp(i);
                        if (timestamp == null) {
                            sink.put("null");
                        } else {
                            sink.put(DATE_TIME_FORMATTER.format(timestamp.toLocalDateTime()));
                        }
                        break;
                    case REAL:
                        float floatValue = rs.getFloat(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(floatValue);
                        }
                        break;
                    case SMALLINT:
                        sink.put(rs.getShort(i));
                        break;
                    case BIGINT:
                        long longValue = rs.getLong(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(longValue);
                        }
                        break;
                    case CHAR:
                        String strValue = rs.getString(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(strValue.charAt(0));
                        }
                        break;
                    case BIT:
                        sink.put(rs.getBoolean(i));
                        break;
                    case TIME:
                    case DATE:
                        timestamp = rs.getTimestamp(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(DATE_TIME_FORMATTER.format(timestamp.toLocalDateTime()));
                        }
                        break;
                    case BINARY:
                        InputStream stream = rs.getBinaryStream(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            toSink(stream, sink);
                        }
                        break;
                    case OTHER:
                        Object object = rs.getObject(i);
                        if (rs.wasNull()) {
                            sink.put("null");
                        } else {
                            sink.put(object.toString());
                        }
                        break;
                    case ARRAY:
                        Array array = rs.getArray(i);
                        if (array == null) {
                            sink.put("null");
                        } else {
                            writeArrayContent(sink, array.getArray());
                        }
                        break;
                    default:
                        assert false;
                }
            }
            sink.put('\n');
        }
        return rows;
    }

    @BeforeClass
    public static void setUpStatic() {
        AbstractTest.setUpStatic();
        System.err.printf("CLEANING UP TEST TABLES%n");
        // Cleanup all test tables before starting tests
        try (Connection conn = getPgConnection();
             Statement readStmt = conn.createStatement();
             Statement stmt = conn.createStatement();
             ResultSet rs = readStmt.executeQuery("SELECT table_name FROM tables() WHERE table_name LIKE 'test_%'")) {
            while (rs.next()) {
                String tableName = rs.getString(1);
                try {
                    stmt.execute(String.format("DROP TABLE IF EXISTS '%s'", tableName));
                    LOG.info("Dropped test table: {}", tableName);
                } catch (SQLException e) {
                    LOG.warn("Failed to drop test table {}: {}", tableName, e.getMessage());
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass
    public static void tearDownStatic() {
        closePgConnection();
    }

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Normalize line endings to LF for comparison.
     */
    private static String normalizeLineEndings(String s) {
        return s == null ? null : s.replace("\r\n", "\n").replace("\r", "\n");
    }

    private static void toSink(InputStream is, Utf16Sink sink) {
        // limit what we print
        byte[] bb = new byte[1];
        int i = 0;
        try {
            while (is.read(bb) > 0) {
                byte b = bb[0];
                if (i > 0) {
                    if ((i % 16) == 0) {
                        sink.put('\n');
                        Numbers.appendHexPadded(sink, i);
                    }
                } else {
                    Numbers.appendHexPadded(sink, i);
                }
                sink.putAscii(' ');

                final int v;
                if (b < 0) {
                    v = 256 + b;
                } else {
                    v = b;
                }

                if (v < 0x10) {
                    sink.putAscii('0');
                    sink.putAscii(hexDigits[b]);
                } else {
                    sink.putAscii(hexDigits[v / 0x10]);
                    sink.putAscii(hexDigits[v % 0x10]);
                }

                i++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeArrayContent(StringSink sink, Object array) {
        if (array == null) {
            sink.put("null");
            return;
        }
        if (!array.getClass().isArray()) {
            if (array instanceof Number) {
                if (array instanceof Double) {
                    double d = ((Number) array).doubleValue();
                    if (Numbers.isNull(d)) {
                        sink.put("null");
                    } else {
                        sink.put(d);
                    }
                }
                if (array instanceof Float) {
                    float f = ((Number) array).floatValue();
                    if (Numbers.isNull(f)) {
                        sink.put("null");
                    } else {
                        sink.put(f);
                    }
                }
                if (array instanceof Long) {
                    long l = ((Number) array).longValue();
                    if (l == Numbers.LONG_NULL) {
                        sink.put("null");
                    } else {
                        sink.put(l);
                    }
                }
            } else if (array instanceof Boolean) {
                sink.put((Boolean) array);
            } else {
                sink.put(array.toString());
            }
            return;
        }

        sink.put('{');
        int length = java.lang.reflect.Array.getLength(array);
        for (int i = 0; i < length; i++) {
            Object element = java.lang.reflect.Array.get(array, i);
            writeArrayContent(sink, element);

            if (i < length - 1) {
                sink.put(',');
            }
        }
        sink.put('}');
    }

    /**
     * Close the shared PostgreSQL connection.
     */
    protected static synchronized void closePgConnection() {
        if (pgConnection != null) {
            try {
                pgConnection.close();
                LOG.info("Closed PostgreSQL connection");
            } catch (SQLException e) {
                LOG.warn("Error closing PostgreSQL connection", e);
            } finally {
                pgConnection = null;
            }
        }
    }

    /**
     * Get configuration value from environment variable or system property.
     * Environment variables take precedence over system properties.
     */
    protected static String getConfig(String envKey, String sysPropKey, String defaultValue) {
        String value = System.getenv(envKey);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return System.getProperty(sysPropKey, defaultValue);
    }

    protected static boolean getConfigBool(String envKey, String sysPropKey, boolean defaultValue) {
        String value = getConfig(envKey, sysPropKey, null);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }

    protected static int getConfigInt(String envKey, String sysPropKey, int defaultValue) {
        String value = getConfig(envKey, sysPropKey, null);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid integer value for {}/{}: {}, using default: {}",
                        envKey, sysPropKey, value, defaultValue);
            }
        }
        return defaultValue;
    }

    /**
     * Get HTTP port.
     */
    protected static int getHttpPort() {
        return getConfigInt("QUESTDB_HTTP_PORT", "questdb.http.port", DEFAULT_HTTP_PORT);
    }

    /**
     * Get or create the shared PostgreSQL connection.
     */
    protected static synchronized Connection getPgConnection() throws SQLException {
        if (pgConnection == null || pgConnection.isClosed()) {
            pgConnection = initPgConnection();
        }
        return pgConnection;
    }

    /**
     * Get PostgreSQL password.
     */
    protected static String getPgPassword() {
        return getConfig("QUESTDB_PG_PASSWORD", "questdb.pg.password", DEFAULT_PG_PASSWORD);
    }

    /**
     * Get PostgreSQL wire protocol port.
     */
    protected static int getPgPort() {
        return getConfigInt("QUESTDB_PG_PORT", "questdb.pg.port", DEFAULT_PG_PORT);
    }

    /**
     * Get PostgreSQL user.
     */
    protected static String getPgUser() {
        return getConfig("QUESTDB_PG_USER", "questdb.pg.user", DEFAULT_PG_USER);
    }

    /**
     * Get whether a QuestDB instance is running locally.
     */
    protected static boolean getQuestDBRunning() {
        return getConfigBool("QUESTDB_RUNNING", "questdb.running", false);
    }

    /**
     * Get QuestDB host address.
     */
    protected static String getQuestDbHost() {
        return getConfig("QUESTDB_HOST", "questdb.host", DEFAULT_HOST);
    }

    /**
     * Initialize a new PostgreSQL connection to QuestDB.
     */
    protected static Connection initPgConnection() throws SQLException {
        String host = getQuestDbHost();
        int port = getPgPort();
        String user = getPgUser();
        String password = getPgPassword();

        String url = String.format("jdbc:postgresql://%s:%d/qdb?sslmode=disable", host, port);
        LOG.info("Connecting to QuestDB via PostgreSQL wire protocol: {}", url);

        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Assert that SQL query results match expected TSV, polling until they do or timeout is reached.
     */
    protected void assertSqlEventually(CharSequence expected, String sql) throws Exception {
        assertEventually(() -> {
            try (Statement statement = getPgConnection().createStatement();
                 ResultSet rs = statement.executeQuery(sql)) {
                sink.clear();
                printToSink(sink, rs);
                TestUtils.assertEquals(expected, sink);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 5);
    }

    /**
     * Assert that table exists, polling until it does or timeout is reached.
     */
    protected void assertTableExistsEventually(CharSequence tableName) throws Exception {
        assertEventually(() -> {
            try (Statement stmt = getPgConnection().createStatement();
                 ResultSet rs = stmt.executeQuery(
                         String.format("SELECT COUNT(*) AS cnt FROM tables() WHERE table_name = '%s'", tableName)
                 )) {
                Assert.assertTrue(rs.next());
                final long actualSize = rs.getLong(1);
                Assert.assertEquals(1, actualSize);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 5);
    }

    /**
     * Assert that table has expected size, polling until it does or timeout is reached.
     */
    protected void assertTableSizeEventually(CharSequence tableName, int expectedSize) throws Exception {
        final String sql = String.format("SELECT COUNT(*) AS cnt FROM \"%s\"", tableName);
        assertEventually(() -> {
            try (
                    Statement stmt = getPgConnection().createStatement();
                    ResultSet rs = stmt.executeQuery(sql)
            ) {
                Assert.assertTrue(rs.next());
                final long actualSize = rs.getLong(1);
                Assert.assertEquals(expectedSize, actualSize);
            } catch (SQLException e) {
                // If the table does not exist yet, we may get an exception
                if (e.getMessage().contains("table does not exist")) {
                    Assert.fail("Table not found: " + tableName);
                }
                throw new RuntimeException(e);
            }
        }, 5);
    }

    /**
     * Drop a table if it exists.
     */
    protected void dropTable(String tableName) {
        try {
            executeSql(String.format("DROP TABLE IF EXISTS '%s'", tableName));
            LOG.info("Dropped table: {}", tableName);
        } catch (SQLException e) {
            LOG.warn("Failed to drop table {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * Drop multiple tables.
     */
    protected void dropTables(List<String> tableNames) {
        for (String tableName : tableNames) {
            dropTable(tableName);
        }
    }

    /**
     * Execute SQL and assert no exceptions are thrown.
     */
    protected void execute(String sql) throws Exception {
        try (Statement statement = getPgConnection().createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * Execute a SELECT query and return results as a list of maps.
     * Each map represents a row with column names as keys.
     */
    protected List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement stmt = getPgConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }
        }

        return results;
    }

    /**
     * Execute a DDL or DML statement (CREATE, DROP, INSERT, etc.).
     */
    protected void executeSql(String sql) throws SQLException {
        try (Statement stmt = getPgConnection().createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * Generate a unique table name with the given prefix.
     * This ensures test isolation when running tests in parallel.
     */
    protected String generateTableName(String prefix) {
        return prefix + "_" + TABLE_NAME_COUNTER.incrementAndGet();
    }

    /**
     * Query table contents with optional ORDER BY clause and return as TSV-formatted string.
     */
    protected String queryTableAsTsv(String tableName, String orderBy) throws SQLException {
        StringBuilder sb = new StringBuilder();

        String sql = String.format("SELECT * FROM '%s'", tableName);
        if (orderBy != null && !orderBy.isEmpty()) {
            sql += " ORDER BY " + orderBy;
        }

        try (Statement stmt = getPgConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Header row
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    sb.append('\t');
                }
                sb.append(metaData.getColumnName(i));
            }
            sb.append('\n');

            // Data rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        sb.append('\t');
                    }
                    Object value = rs.getObject(i);
                    sb.append(value == null ? "" : value.toString());
                }
                sb.append('\n');
            }
        }

        return sb.toString();
    }

    /**
     * Check if table exists (non-blocking, no polling).
     */
    protected boolean tableExists(String tableName) throws SQLException {
        List<Map<String, Object>> result = executeQuery(
                String.format("SELECT table_name FROM tables() WHERE table_name = '%s'", tableName)
        );
        return !result.isEmpty();
    }

    /**
     * Track a table for cleanup after the test.
     * If the table already exists, we block until it is dropped.
     *
     * @param tableName the name of the table to track
     */
    protected void useTable(String tableName) throws Exception {
        TestUtils.assertEventually(() -> {
            try {
                if (!tableExists(tableName)) {
                    return;
                }
                Thread.sleep(100);
                dropTable(tableName);
                Assert.fail("Table " + tableName + " already exists. Dropping it.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for table to be dropped: " + tableName, e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    static {
        DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral('T')
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .appendFraction(NANO_OF_SECOND, 9, 9, true)
                .appendLiteral('Z')
                .toFormatter();
    }
}
