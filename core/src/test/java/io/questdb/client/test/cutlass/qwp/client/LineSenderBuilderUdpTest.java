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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpUdpSender;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for UDP transport support in the Sender.builder() API.
 * These tests verify the builder configuration and validation,
 * not actual UDP connectivity (which requires a running server).
 */
public class LineSenderBuilderUdpTest extends AbstractTest {

    @Test
    public void testInvalidSchema_includesUdp() {
        assertBadConfig("invalid::addr=localhost:9000;", "udp");
    }

    @Test
    public void testUdpScheme_buildsQwpUdpSender() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.fromConfig("udp::addr=localhost:9007;")) {
                Assert.assertTrue(sender instanceof QwpUdpSender);
            }
        });
    }

    @Test
    public void testUdpScheme_customMaxDatagramSize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.fromConfig("udp::addr=localhost:9007;max_datagram_size=500;")) {
                Assert.assertTrue(sender instanceof QwpUdpSender);
            }
        });
    }

    @Test
    public void testUdpScheme_customMulticastTtl() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.fromConfig("udp::addr=localhost:9007;multicast_ttl=5;")) {
                Assert.assertTrue(sender instanceof QwpUdpSender);
            }
        });
    }

    @Test
    public void testUdpScheme_noAddr_throws() {
        // sf-client.md §4.6 now rejects unknown keys, so a valid key
        // (multicast_ttl=) is used to drive the parser past key parsing
        // and surface the missing-addr error on its own.
        assertBadConfig("udp::multicast_ttl=1;", "addr is missing");
    }

    @Test
    public void testUdp_authNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .enableAuth("keyId")
                        .authToken("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48"),
                "not supported for UDP");
    }

    @Test
    public void testUdp_autoFlushBytesNotSupported() {
        assertThrowsAny(
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .autoFlushBytes(1000),
                "auto flush bytes is only supported for WebSocket transport");
    }

    @Test
    public void testUdp_autoFlushIntervalNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .autoFlushIntervalMillis(100),
                "not supported for UDP");
    }

    @Test
    public void testUdp_autoFlushRowsNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .autoFlushRows(100),
                "not supported for UDP");
    }

    @Test
    public void testUdp_defaultPort9007() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.fromConfig("udp::addr=localhost;")) {
                Assert.assertTrue(sender instanceof QwpUdpSender);
            }
        });
    }

    @Test
    public void testUdp_httpPathNotSupported() {
        assertThrowsAny(
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .httpPath("/custom/path"),
                "not supported for UDP");
    }

    @Test
    public void testUdp_httpSettingPathNotSupported() {
        assertThrowsAny(
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .httpSettingPath("/custom/settings"),
                "not supported for UDP");
    }

    @Test
    public void testUdp_httpTimeoutNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .httpTimeoutMillis(5000),
                "not supported for UDP");
    }

    @Test
    public void testUdp_httpTokenNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .httpToken("token"),
                "not supported for UDP");
    }

    @Test
    public void testUdp_maxBackoffNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .maxBackoffMillis(5000),
                "not supported for UDP");
    }

    @Test
    public void testUdp_maxDatagramSize() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.UDP)
                .address("localhost")
                .maxDatagramSize(500);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testUdp_maxDatagramSizeDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .maxDatagramSize(500)
                        .maxDatagramSize(600));
    }

    @Test
    public void testUdp_maxDatagramSizeZero_fails() {
        assertThrows("must be positive",
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .maxDatagramSize(0));
    }

    @Test
    public void testUdp_minRequestThroughputNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .minRequestThroughput(100),
                "not supported for UDP");
    }

    @Test
    public void testUdp_multicastTtl() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.UDP)
                .address("localhost")
                .multicastTtl(5);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testUdp_multicastTtlDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .multicastTtl(5)
                        .multicastTtl(10));
    }

    @Test
    public void testUdp_multicastTtlNegative_fails() {
        assertThrows("cannot be negative",
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .multicastTtl(-1));
    }

    @Test
    public void testUdp_protocolVersionNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .protocolVersion(1),
                "not supported for UDP");
    }

    @Test
    public void testUdp_ipv6Address_throws() {
        assertThrowsAny(
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("::1"),
                "cannot parse a port", "use IPv4 address");
    }

    @Test
    public void testUdp_resolveUnknownHost_throws() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("this.host.does.not.exist.questdb.invalid"),
                "could not resolve host");
    }

    @Test
    public void testUdp_retryTimeoutNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .retryTimeoutMillis(100),
                "not supported for UDP");
    }

    @Test
    public void testUdp_tlsEnabled_throws() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .enableTls(),
                "TLS is not supported for UDP");
    }

    @Test
    public void testUdp_tokenNotSupported() {
        assertBadConfig("udp::addr=localhost:9007;token=foo;", "token is not supported for UDP");
    }

    @Test
    public void testUdp_transportEnum() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (Sender sender = Sender.builder(Sender.Transport.UDP)
                    .address("localhost:9007")
                    .build()) {
                Assert.assertTrue(sender instanceof QwpUdpSender);
            }
        });
    }

    @Test
    public void testUdp_usernamePasswordNotSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .httpUsernamePassword("user", "pass"),
                "not supported for UDP");
    }

    @Test
    public void testUdpScheme_password_throws() {
        assertBadConfig("udp::addr=localhost;password=bar;", "password is not supported for UDP");
    }

    @Test
    public void testUdpScheme_username_throws() {
        assertBadConfig("udp::addr=localhost;username=foo;", "username is not supported for UDP");
    }

    @Test
    public void testUdp_maxDatagramSizeNonUdp_fails() {
        assertThrows("only supported for UDP transport",
                () -> Sender.builder(Sender.Transport.HTTP)
                        .address("localhost")
                        .maxDatagramSize(500));
    }

    @Test
    public void testUdp_multicastTtlExceeds255_fails() {
        assertThrows("cannot exceed 255",
                () -> Sender.builder(Sender.Transport.UDP)
                        .address("localhost")
                        .multicastTtl(256));
    }

    @Test
    public void testUdp_multicastTtlNonUdp_fails() {
        assertThrows("only supported for UDP transport",
                () -> Sender.builder(Sender.Transport.HTTP)
                        .address("localhost")
                        .multicastTtl(1));
    }

    @Test
    public void testUdps_throws() {
        assertBadConfig("udps::addr=localhost:9007;", "TLS is not supported for UDP");
    }

    @SuppressWarnings("resource")
    private static void assertBadConfig(String config, String... anyOf) {
        assertThrowsAny(() -> Sender.fromConfig(config), anyOf);
    }

    private static void assertThrows(String expectedSubstring, Runnable action) {
        try {
            action.run();
            Assert.fail("Expected LineSenderException containing '" + expectedSubstring + "'");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), expectedSubstring);
        }
    }

    private static void assertThrowsAny(Sender.LineSenderBuilder builder, String... anyOf) {
        assertThrowsAny(builder::build, anyOf);
    }

    private static void assertThrowsAny(Runnable action, String... anyOf) {
        try {
            action.run();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            for (String s : anyOf) {
                if (msg.contains(s)) {
                    return;
                }
            }
            Assert.fail("Expected message containing one of [" + String.join(", ", anyOf) + "] but got: " + msg);
        }
    }
}
