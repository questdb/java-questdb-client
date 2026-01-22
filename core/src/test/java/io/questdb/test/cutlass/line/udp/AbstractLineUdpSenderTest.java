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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cutlass.line.AbstractLineSender;
import io.questdb.cutlass.line.LineUdpSender;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractQdbTest;
import org.junit.After;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for UDP sender integration tests.
 * Provides helper methods for creating UDP senders and managing test tables.
 * <p>
 * Note: UDP is a fire-and-forget protocol, so tests need extra delays
 * to account for network latency and server processing time.
 */
public abstract class AbstractLineUdpSenderTest extends AbstractQdbTest {

    // Default buffer capacity for UDP sender
    protected static final int DEFAULT_BUFFER_CAPACITY = 2048;

    // Default TTL for multicast
    protected static final int DEFAULT_TTL = 1;

    // Extra delay for UDP to account for fire-and-forget nature
    protected static final long UDP_SETTLE_DELAY_MS = 5;

    // List of tables created during the test for cleanup
    private final List<String> createdTables = new ArrayList<>();

    @After
    public void cleanupTables() {
        dropTables(createdTables);
        createdTables.clear();
    }

    /**
     * Get localhost IPv4 address as integer.
     */
    protected static int getLocalhostIPv4() {
        return parseIPv4("127.0.0.1");
    }

    /**
     * Parse IPv4 address to integer representation.
     */
    protected static int parseIPv4(String address) {
        try {
            return Numbers.parseIPv4(address);
        } catch (NumericException e) {
            throw new IllegalArgumentException("Invalid IPv4 address: " + address, e);
        }
    }

    /**
     * Send data using the sender, close it, and assert the expected row count.
     * This method closes the sender (which flushes), waits for UDP to settle, and polls for the expected row count.
     *
     * @param sender           the sender to close
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void closeAndAssertRowCount(AbstractLineSender sender, String tableName, long expectedRowCount) throws Exception {
        sender.close();
        // Extra delay for UDP to ensure data reaches the server
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, expectedRowCount);
    }

    /**
     * Create a UDP sender for multicast.
     *
     * @param interfaceAddress the interface address to bind to
     * @param multicastAddress the multicast group address
     * @param bufferCapacity   the buffer capacity in bytes
     * @param ttl              time-to-live for multicast packets
     */
    protected AbstractLineSender createMulticastUdpSender(
            int interfaceAddress,
            int multicastAddress,
            int bufferCapacity,
            int ttl
    ) {
        return new LineUdpSender(
                interfaceAddress,
                multicastAddress,
                getIlpUdpPort(),
                bufferCapacity,
                ttl
        );
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
     * Create a UDP sender with specified buffer size.
     *
     * @param bufferCapacity the buffer capacity in bytes
     */
    protected AbstractLineSender createUdpSender(int bufferCapacity) {
        return new LineUdpSender(
                getLocalhostIPv4(),  // interface address
                getTargetIPv4(),     // target address
                getIlpUdpPort(),     // target port
                bufferCapacity,
                DEFAULT_TTL
        );
    }

    /**
     * Create a UDP sender with default settings.
     * Uses localhost as both interface and target address.
     */
    protected AbstractLineSender createUdpSender() {
        return createUdpSender(DEFAULT_BUFFER_CAPACITY);
    }

    /**
     * Send data using the sender and assert the expected row count.
     * This method flushes the sender, waits for UDP to settle, and polls for the expected row count.
     * <p>
     * UDP is fire-and-forget, so we need extra delay to ensure the server has processed the data.
     *
     * @param sender           the sender to flush
     * @param tableName        the table to check
     * @param expectedRowCount the expected number of rows
     */
    protected void flushAndAssertRowCount(AbstractLineSender sender, String tableName, long expectedRowCount) throws Exception {
        sender.flush();
        // Extra delay for UDP to ensure data reaches the server
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, expectedRowCount);
    }

    /**
     * Get target IPv4 address for UDP sender.
     */
    protected int getTargetIPv4() {
        return parseIPv4(getQuestDbHost());
    }

    /**
     * Track a table for cleanup after the test.
     *
     * @param tableName the name of the table to track
     */
    protected void trackTable(String tableName) {
        createdTables.add(tableName);
    }
}
