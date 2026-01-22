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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.test.AbstractQdbTest;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V1;

/**
 * Base class for TCP sender integration tests.
 * Provides helper methods for creating TCP senders and managing test tables.
 */
public abstract class AbstractLineTcpSenderTest extends AbstractQdbTest {

    // List of tables created during the test for cleanup
    private final List<String> createdTables = new ArrayList<>();

    @After
    public void cleanupTables() {
        dropTables(createdTables);
        createdTables.clear();
    }

    /**
     * Create a TCP sender with V1 protocol (no authentication).
     */
    protected Sender createTcpSender() {
        return createTcpSender(PROTOCOL_VERSION_V1);
    }

    /**
     * Create a TCP sender with specified protocol version.
     *
     * @param protocolVersion the ILP protocol version (V1, V2, or V3)
     */
    protected Sender createTcpSender(int protocolVersion) {
        return Sender.builder(Sender.Transport.TCP)
                .address(getQuestDbHost())
                .port(getIlpTcpPort())
                .protocolVersion(protocolVersion)
                .build();
    }

    /**
     * Create an authenticated TCP sender.
     *
     * @param keyId          the authentication key ID
     * @param token          the authentication token (private key in base64)
     * @param protocolVersion the ILP protocol version
     */
    protected Sender createAuthenticatedTcpSender(String keyId, String token, int protocolVersion) {
        return Sender.builder(Sender.Transport.TCP)
                .address(getQuestDbHost())
                .port(getIlpTcpPort())
                .protocolVersion(protocolVersion)
                .enableAuth(keyId).authToken(token)
                .build();
    }

    /**
     * Create a TCP sender with TLS enabled.
     *
     * @param protocolVersion the ILP protocol version
     */
    protected Sender createTlsTcpSender(int protocolVersion) {
        return Sender.builder(Sender.Transport.TCP)
                .address(getQuestDbHost())
                .port(getIlpTcpPort())
                .protocolVersion(protocolVersion)
                .enableTls()
                .build();
    }

    /**
     * Track a table for cleanup after the test.
     *
     * @param tableName the name of the table to track
     */
    protected void trackTable(String tableName) {
        createdTables.add(tableName);
    }

    /**
     * Generate a unique table name with the given prefix and track it for cleanup.
     *
     * @param prefix the table name prefix
     * @return the generated unique table name
     */
    protected String createTrackedTable(String prefix) {
        String tableName = generateTableName(prefix);
        trackTable(tableName);
        return tableName;
    }

    /**
     * Send data using the sender and assert the expected row count.
     * This method flushes the sender and polls for the expected row count.
     *
     * @param sender           the sender to flush
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void flushAndAssertRowCount(Sender sender, String tableName, long expectedRowCount) throws Exception {
        sender.flush();
        assertTableRowCount(tableName, expectedRowCount);
    }

    /**
     * Send data using the sender, close it, and assert the expected row count.
     * This method closes the sender (which flushes) and polls for the expected row count.
     *
     * @param sender           the sender to close
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void closeAndAssertRowCount(Sender sender, String tableName, long expectedRowCount) throws Exception {
        sender.close();
        assertTableRowCount(tableName, expectedRowCount);
    }
}
