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

package io.questdb.client.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.AuthUtils;
import io.questdb.client.std.Numbers;
import io.questdb.client.test.cutlass.line.AbstractLineSenderTest;

import java.security.PrivateKey;
import java.util.function.Consumer;

/**
 * Base class for TCP sender integration tests.
 * Provides helper methods for creating TCP senders and managing test tables.
 */
public abstract class AbstractLineTcpSenderTest extends AbstractLineSenderTest {
    protected static final String AUTH_KEY_ID1 = "testUser1";
    protected final static String AUTH_KEY_ID2_INVALID = "invalid";
    protected final static int HOST = Numbers.parseIPv4("127.0.0.1");
    protected static final Consumer<Sender> SET_TABLE_NAME_ACTION = s -> s.table("test_mytable");
    protected final static String TOKEN = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    protected final static PrivateKey AUTH_PRIVATE_KEY1 = AuthUtils.toPrivateKey(TOKEN);

    /**
     * Get whether the ILP TCP protocol is authenticated.
     */
    protected static boolean getIlpTcpAuthEnabled() {
        return getConfigBool("QUESTDB_ILP_TCP_AUTH_ENABLE", "questdb.ilp.tcp.auth.enable", false);
    }

    /**
     * Get whether the ILP TCP protocol is secure (TLS).
     */
    protected static boolean getIlpTcpTlsEnabled() {
        return getConfigBool("QUESTDB_ILP_TCP_TLS_ENABLE", "questdb.ilp.tcp.tls.enable", false);
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
}
