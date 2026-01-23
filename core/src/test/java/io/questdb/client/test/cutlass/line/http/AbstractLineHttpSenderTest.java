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

package io.questdb.client.test.cutlass.line.http;

import io.questdb.client.Sender;
import io.questdb.client.test.AbstractQdbTest;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V1;

/**
 * Base class for HTTP sender integration tests.
 * Provides helper methods for creating HTTP senders and managing test tables.
 * <p>
 * HTTP transport provides stronger transactional guarantees than TCP and
 * better feedback in case of errors.
 */
public abstract class AbstractLineHttpSenderTest extends AbstractQdbTest {

    /**
     * Send data using the sender, close it, and assert the expected row count.
     * This method closes the sender (which flushes) and polls for the expected row count.
     *
     * @param sender           the sender to close
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void closeAndAssertRowCount(Sender sender, String tableName, int expectedRowCount) throws Exception {
        sender.close();
        assertTableSizeEventually(tableName, expectedRowCount);
    }

    /**
     * Create an HTTP sender with V1 protocol (no authentication).
     */
    protected Sender createHttpSender() {
        return createHttpSender(PROTOCOL_VERSION_V1);
    }

    /**
     * Create an HTTP sender with specified protocol version.
     *
     * @param protocolVersion the ILP protocol version (V1, V2, or V3)
     */
    protected Sender createHttpSender(int protocolVersion) {
        return Sender.builder(Sender.Transport.HTTP)
                .address(getQuestDbHost())
                .port(getHttpPort())
                .protocolVersion(protocolVersion)
                .build();
    }

    /**
     * Create an HTTP sender with custom auto-flush settings.
     *
     * @param protocolVersion   the ILP protocol version
     * @param autoFlushRows     auto-flush after this many rows (0 to disable)
     * @param autoFlushInterval auto-flush interval in milliseconds (0 to disable)
     */
    protected Sender createHttpSenderWithAutoFlush(int protocolVersion, int autoFlushRows, int autoFlushInterval) {
        return Sender.builder(Sender.Transport.HTTP)
                .address(getQuestDbHost())
                .port(getHttpPort())
                .protocolVersion(protocolVersion)
                .autoFlushRows(autoFlushRows)
                .autoFlushIntervalMillis(autoFlushInterval)
                .build();
    }

    /**
     * Create an HTTP sender with basic authentication.
     *
     * @param username        the username for basic auth
     * @param password        the password for basic auth
     * @param protocolVersion the ILP protocol version
     */
    protected Sender createHttpSenderWithBasicAuth(String username, String password, int protocolVersion) {
        return Sender.builder(Sender.Transport.HTTP)
                .address(getQuestDbHost())
                .port(getHttpPort())
                .protocolVersion(protocolVersion)
                .httpUsernamePassword(username, password)
                .build();
    }

    /**
     * Create an HTTP sender with TLS enabled.
     *
     * @param protocolVersion the ILP protocol version
     */
    protected Sender createTlsHttpSender(int protocolVersion) {
        return Sender.builder(Sender.Transport.HTTP)
                .address(getQuestDbHost())
                .port(getHttpPort())
                .protocolVersion(protocolVersion)
                .enableTls()
                .build();
    }

    /**
     * Generate a unique table name with the given prefix and track it for cleanup.
     *
     * @param prefix the table name prefix
     * @return the generated unique table name
     */
    protected String createTrackedTable(String prefix) {
        return generateTableName(prefix);
    }

    /**
     * Send data using the sender and assert the expected row count.
     * This method flushes the sender and polls for the expected row count.
     *
     * @param sender           the sender to flush
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void flushAndAssertRowCount(Sender sender, String tableName, int expectedRowCount) throws Exception {
        sender.flush();
        assertTableSizeEventually(tableName, expectedRowCount);
    }
}
