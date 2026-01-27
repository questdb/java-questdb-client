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

package io.questdb.client.test.cutlass.line.udp;

import io.questdb.client.cutlass.line.AbstractLineSender;
import io.questdb.client.cutlass.line.LineUdpSender;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.test.cutlass.line.AbstractLineSenderTest;

/**
 * Base class for UDP sender integration tests.
 * Provides helper methods for creating UDP senders and managing test tables.
 * <p>
 * Note: UDP is a fire-and-forget protocol, so tests need extra delays
 * to account for network latency and server processing time.
 */
public abstract class AbstractLineUdpSenderTest extends AbstractLineSenderTest {

    // Default buffer capacity for UDP sender
    protected static final int DEFAULT_BUFFER_CAPACITY = 2048;

    // Default TTL for multicast
    protected static final int DEFAULT_TTL = 1;

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
     * Get target IPv4 address for UDP sender.
     */
    protected int getTargetIPv4() {
        return parseIPv4(getQuestDbHost());
    }
}
