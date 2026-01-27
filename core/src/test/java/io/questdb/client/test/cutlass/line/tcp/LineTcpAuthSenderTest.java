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
import io.questdb.client.cutlass.line.AbstractLineTcpSender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.client.std.datetime.microtime.Micros;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;
import static io.questdb.client.Sender.Transport;
import static io.questdb.client.test.tools.TestUtils.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

/**
 * Tests for LineTcpSender.
 * <p>
 * Unit tests use DummyLineChannel/ByteChannel (no server needed).
 * Integration tests use external QuestDB via AbstractLineTcpSenderTest
 * infrastructure.
 */
public class LineTcpAuthSenderTest extends AbstractLineTcpSenderTest {
    @BeforeClass
    public static void setUpStatic() {
        AbstractLineTcpSenderTest.setUpStatic();
        Assume.assumeTrue(getIlpTcpAuthEnabled());
    }

    @Test
    public void testAuthSuccess() throws Exception {
        useTable("test_auth_success");

        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, getIlpTcpPort(), 256 * 1024)) {
            sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
            sender.metric("test_auth_success").field("my int field", 42).$();
            sender.flush();
        }

        assertTableExistsEventually("test_auth_success");
    }

    @Test
    public void testAuthWrongKey() {
        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, getIlpTcpPort(), 2048)) {
            sender.authenticate(AUTH_KEY_ID2_INVALID, AUTH_PRIVATE_KEY1);
            // 30 seconds should be enough even on a slow CI server
            long deadline = System.nanoTime() + SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                sender.metric("test_auth_wrong_key").field("my int field", 42).$();
                sender.flush();
            }
            fail("Client fail to detected qdb server closed a connection due to wrong credentials");
        } catch (LineSenderException expected) {
            // ignored
        }
    }

    @Test
    public void testBuilderAuthSuccess() throws Exception {
        useTable("test_builder_auth_success");

        try (Sender sender = Sender.builder(Transport.TCP)
                .address("127.0.0.1:" + getIlpTcpPort())
                .enableAuth(AUTH_KEY_ID1).authToken(TOKEN)
                .protocolVersion(PROTOCOL_VERSION_V2)
                .build()) {
            sender.table("test_builder_auth_success").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_auth_success");
    }

    @Test
    public void testBuilderAuthSuccess_confString() throws Exception {
        useTable("test_builder_auth_success_conf_string");

        try (Sender sender = Sender.fromConfig("tcp::addr=127.0.0.1:" + getIlpTcpPort() + ";user=" + AUTH_KEY_ID1
                + ";token=" + TOKEN + ";protocol_version=2;")) {
            sender.table("test_builder_auth_success_conf_string").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_auth_success_conf_string");
    }

    @Test
    public void testConfString() throws Exception {
        useTable("test_conf_string");

        String confString = "tcp::addr=127.0.0.1:" + getIlpTcpPort() + ";user=" + AUTH_KEY_ID1 + ";token=" + TOKEN
                + ";protocol_version=2;";
        try (Sender sender = Sender.fromConfig(confString)) {
            long tsMicros = Micros.floor("2022-02-25");
            sender.table("test_conf_string")
                    .longColumn("int_field", 42)
                    .boolColumn("bool_field", true)
                    .stringColumn("string_field", "foo")
                    .doubleColumn("double_field", 42.0)
                    .timestampColumn("ts_field", tsMicros, ChronoUnit.MICROS)
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_conf_string", 1);
        assertSqlEventually(
                "int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000000Z\t2022-02-25T00:00:00.000000000Z\n",
                "select int_field, bool_field, string_field, double_field, ts_field, timestamp from test_conf_string");
    }

    @Test
    public void testMinBufferSizeWhenAuth() {
        int tinyCapacity = 42;
        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, getIlpTcpPort(), tinyCapacity)) {
            sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
            fail();
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "challenge did not fit into buffer");
        }
    }
}
