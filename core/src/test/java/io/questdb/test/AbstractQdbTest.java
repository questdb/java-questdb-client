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

package io.questdb.test;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AbstractQdbTest extends AbstractTest {

    // Configuration defaults
    private static final String DEFAULT_HOST = "127.0.0.1";
    private static final int DEFAULT_HTTP_PORT = 9000;
    private static final int DEFAULT_ILP_TCP_PORT = 9009;
    private static final int DEFAULT_ILP_UDP_PORT = 9009;
    private static final String DEFAULT_PG_PASSWORD = "quest";
    private static final int DEFAULT_PG_PORT = 8812;
    private static final String DEFAULT_PG_USER = "admin";
    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    // Polling configuration
    private static final long DEFAULT_POLL_TIMEOUT_MS = 30_000;
    // Table name counter for uniqueness
    private static final AtomicLong TABLE_NAME_COUNTER = new AtomicLong(System.currentTimeMillis());

    // Shared PostgreSQL connection
    private static Connection pgConnection;

    @BeforeClass
    public static void setUpStatic() {
        AbstractTest.setUpStatic();
    }

    @AfterClass
    public static void tearDownStatic() {
        closePgConnection();
    }

    /**
     * Normalize line endings to LF for comparison.
     */
    private static String normalizeLineEndings(String s) {
        return s == null ? null : s.replace("\r\n", "\n").replace("\r", "\n");
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
     * Get ILP TCP port.
     */
    protected static int getIlpTcpPort() {
        return getConfigInt("QUESTDB_ILP_TCP_PORT", "questdb.ilp.tcp.port", DEFAULT_ILP_TCP_PORT);
    }

    /**
     * Get ILP UDP port.
     */
    protected static int getIlpUdpPort() {
        return getConfigInt("QUESTDB_ILP_UDP_PORT", "questdb.ilp.udp.port", DEFAULT_ILP_UDP_PORT);
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

    @Before
    public void setUp() {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Assert that table contents match expected TSV, polling until they do or timeout is reached.
     */
    protected void assertTableContents(String tableName, String expectedTsv) throws Exception {
        assertTableContents(tableName, expectedTsv, null, DEFAULT_POLL_TIMEOUT_MS);
    }

    /**
     * Assert that table contents match expected TSV, polling until they do or timeout is reached.
     */
    protected void assertTableContents(String tableName, String expectedTsv, String orderBy) throws Exception {
        assertTableContents(tableName, expectedTsv, orderBy, DEFAULT_POLL_TIMEOUT_MS);
    }

    /**
     * Assert that table contents match expected TSV, polling until they do or timeout is reached.
     */
    protected void assertTableContents(String tableName, String expectedTsv, String orderBy, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String lastContent = null;

        while (System.currentTimeMillis() < deadline) {
            try {
                lastContent = queryTableAsTsv(tableName, orderBy);
                if (normalizeLineEndings(expectedTsv).equals(normalizeLineEndings(lastContent))) {
                    return; // Success
                }
            } catch (SQLException e) {
                // Ignore and retry
            }
            Thread.sleep(DEFAULT_POLL_INTERVAL_MS);
        }

        assertEquals("Table '" + tableName + "' contents mismatch after " + timeoutMs + "ms",
                normalizeLineEndings(expectedTsv), normalizeLineEndings(lastContent));
    }

    /**
     * Assert that a table exists, polling until it does or timeout is reached.
     */
    protected void assertTableExists(String tableName) throws Exception {
        assertTableExists(tableName, DEFAULT_POLL_TIMEOUT_MS);
    }

    // ==================== Assertion Helpers with Polling ====================

    /**
     * Assert that a table exists, polling until it does or timeout is reached.
     */
    protected void assertTableExists(String tableName, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            try {
                List<Map<String, Object>> result = executeQuery(
                        String.format("SELECT table_name FROM tables() WHERE table_name = '%s'", tableName)
                );
                if (!result.isEmpty()) {
                    return; // Table exists
                }
            } catch (SQLException e) {
                // Ignore and retry
            }
            Thread.sleep(DEFAULT_POLL_INTERVAL_MS);
        }

        fail("Table '" + tableName + "' does not exist after " + timeoutMs + "ms");
    }

    /**
     * Assert that table has expected row count, polling until it does or timeout is reached.
     */
    protected void assertTableRowCount(String tableName, long expectedCount) throws Exception {
        assertTableRowCount(tableName, expectedCount, DEFAULT_POLL_TIMEOUT_MS);
    }

    /**
     * Assert that table has expected row count, polling until it does or timeout is reached.
     */
    protected void assertTableRowCount(String tableName, long expectedCount, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long lastCount = -1;

        while (System.currentTimeMillis() < deadline) {
            try {
                lastCount = getTableRowCount(tableName);
                if (lastCount == expectedCount) {
                    return; // Success
                }
            } catch (SQLException e) {
                // Ignore and retry
            }
            Thread.sleep(DEFAULT_POLL_INTERVAL_MS);
        }

        fail("Table '" + tableName + "' row count: expected " + expectedCount +
                " but was " + lastCount + " after " + timeoutMs + "ms");
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
     * Get the row count for a table.
     *
     * @return row count, or -1 if table doesn't exist
     */
    protected long getTableRowCount(String tableName) throws SQLException {
        try {
            List<Map<String, Object>> result = executeQuery(
                    String.format("SELECT count(*) as cnt FROM '%s'", tableName)
            );
            if (!result.isEmpty()) {
                Object cnt = result.get(0).get("cnt");
                if (cnt instanceof Number) {
                    return ((Number) cnt).longValue();
                }
            }
        } catch (SQLException e) {
            // Table might not exist yet
            if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
                return -1;
            }
            throw e;
        }
        return -1;
    }

    /**
     * Query table contents and return as TSV-formatted string.
     * First line contains column headers, subsequent lines contain data.
     */
    protected String queryTableAsTsv(String tableName) throws SQLException {
        return queryTableAsTsv(tableName, null);
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
}
