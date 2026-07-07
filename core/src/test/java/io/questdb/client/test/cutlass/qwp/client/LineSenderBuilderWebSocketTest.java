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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Tests for WebSocket transport support in the Sender.builder() API.
 * These tests verify the builder configuration and validation,
 * not actual WebSocket connectivity (which requires a running server).
 */
public class LineSenderBuilderWebSocketTest extends AbstractTest {

    private static final String LOCALHOST = "localhost";

    @Test
    public void testAddressConfiguration() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST + ":9000");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAddressEmpty_fails() {
        assertThrows("address cannot be empty",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address(""));
    }

    @Test
    public void testAddressEndsWithColon_fails() {
        assertThrows("invalid address",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address("foo:"));
    }

    @Test
    public void testAddressNull_fails() {
        assertThrows("null",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address(null));
    }

    @Test
    public void testAddressWithoutPort_usesDefaultPort9000() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeWithAllOptions() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(500)
                .autoFlushBytes(512 * 1024)
                .autoFlushIntervalMillis(50);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushBytes() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushBytes(1024 * 1024);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushBytesDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushBytes(1024)
                        .autoFlushBytes(2048));
    }

    @Test
    public void testAutoFlushBytesNegative_fails() {
        assertThrows("cannot be negative",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushBytes(-1));
    }

    @Test
    public void testAutoFlushBytesNotSupportedForHttp_fails() {
        assertThrows("only supported for WebSocket transport",
                () -> Sender.builder(Sender.Transport.HTTP)
                        .address("localhost")
                        .autoFlushBytes(1024));
    }

    @Test
    public void testAutoFlushBytesZero() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushBytes(0);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushIntervalMillis() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushIntervalMillis(100);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushIntervalMillisDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushIntervalMillis(100)
                        .autoFlushIntervalMillis(200));
    }

    @Test
    public void testAutoFlushIntervalMillisNegative_fails() {
        assertThrows("cannot be negative",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushIntervalMillis(-1));
    }

    @Test
    public void testAutoFlushIntervalMillisZero_fails() {
        assertThrows("cannot be negative",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushIntervalMillis(0));
    }

    @Test
    public void testAutoFlushRows() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(1000);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAutoFlushRowsDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushRows(100)
                        .autoFlushRows(200));
    }

    @Test
    public void testAutoFlushRowsNegative_fails() {
        assertThrows("cannot be negative",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .autoFlushRows(-1));
    }

    @Test
    public void testAutoFlushRowsZero_disablesRowBasedAutoFlush() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(0);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testBufferCapacityNotSupported_fails() {
        assertThrows("buffer capacity is not supported for WebSocket transport",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .bufferCapacity(128 * 1024));
    }

    @Test
    public void testBuilderWithWebSocketTransport() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET);
        Assert.assertNotNull("Builder should be created for WebSocket transport", builder);
    }

    @Test
    public void testBuilderWithWebSocketTransportCreatesCorrectSenderType() throws Exception {
        assertMemoryLeak(() -> {
            int port;
            try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
                port = s.getLocalPort();
            }
            assertThrowsAny(
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address(LOCALHOST + ":" + port),
                    "connect", "Failed"
            );
        });
    }

    @Test
    public void testConnectionRefused() throws Exception {
        assertMemoryLeak(() -> {
            int port = findUnusedPort();
            assertThrowsAny(
                    Sender.builder(Sender.Transport.WEBSOCKET)
                            .address(LOCALHOST + ":" + port),
                    "connect", "Failed"
            );
        });
    }

    @Test
    public void testCustomTrustStore_butTlsNotEnabled_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .advancedTls().customTrustStore("/some/path", "password".toCharArray())
                        .address(LOCALHOST),
                "TLS was not enabled");
    }

    @Test
    public void testDefaultIsAsync() {
        // Default in-flight window size is 128 (async)
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testDisableAutoFlush_notSupportedForWebSocket() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .disableAutoFlush(),
                "not supported for WebSocket");
    }

    @Test
    public void testDnsResolutionFailure() throws Exception {
        assertMemoryLeak(() -> assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld:9000"),
                "resolve", "connect", "Failed"
        ));
    }

    @Test
    public void testDuplicateAddresses_fails() {
        assertThrows("duplicated addresses",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":9000")
                        .address(LOCALHOST + ":9000"));
    }

    @Test
    @Ignore("TCP authentication is not supported for WebSocket protocol")
    public void testEnableAuth_notSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .enableAuth("keyId")
                        .authToken("token"),
                "not supported for WebSocket");
    }

    @Test
    public void testDurableAckKeepaliveIntervalAcceptedOnWebSocket() {
        // Positive value, zero (disable), and negative (also disable) all
        // build cleanly. The setter is just a setter; semantics are validated
        // at I/O-loop construction time.
        Assert.assertNotNull(Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .durableAckKeepaliveIntervalMillis(50));
        Assert.assertNotNull(Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .durableAckKeepaliveIntervalMillis(0));
        Assert.assertNotNull(Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .durableAckKeepaliveIntervalMillis(-1));
    }

    @Test
    public void testDurableAckKeepaliveIntervalConfigStringParses() {
        // Smoke test: the parser accepts the knob, including the disable-by-zero
        // value, without erroring at config-string construction. The full
        // end-to-end behaviour (server interaction) is covered by
        // ReplicationTest in the parent repo, which exercises the default
        // value. fromConfig() is allowed to throw a LineSenderException at
        // connect time -- we only assert the parser did not reject the key.
        try {
            Sender.fromConfig("ws::addr=127.0.0.1:1;durable_ack_keepalive_interval_millis=50;").close();
        } catch (LineSenderException e) {
            Assert.assertFalse("parser must not reject the key, was: " + e.getMessage(),
                    e.getMessage().contains("durable_ack_keepalive_interval_millis"));
        }
        try {
            Sender.fromConfig("ws::addr=127.0.0.1:1;durable_ack_keepalive_interval_millis=0;").close();
        } catch (LineSenderException e) {
            Assert.assertFalse("parser must accept zero (disable), was: " + e.getMessage(),
                    e.getMessage().contains("durable_ack_keepalive_interval_millis"));
        }
    }

    @Test
    public void testDurableAckKeepaliveIntervalNotSupportedForTcp() {
        // The knob is WebSocket-only; the TCP-protocol path must reject it
        // loudly so a user who copy-pasted the param into the wrong transport
        // sees the mismatch immediately rather than silently losing the
        // intended behaviour.
        assertThrows("durable_ack_keepalive_interval_millis is only supported for WebSocket transport",
                () -> Sender.builder(Sender.Transport.TCP)
                        .address(LOCALHOST)
                        .durableAckKeepaliveIntervalMillis(100));
    }

    @Test
    public void testAuthTimeoutConfig_acceptsPositive() {
        Sender.LineSenderBuilder builder = Sender.builder("ws::addr=localhost:9000;auth_timeout_ms=2500;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAuthTimeoutConfig_zeroRejected() {
        assertBadConfig("ws::addr=localhost:9000;auth_timeout_ms=0;", "auth_timeout_ms must be > 0");
    }

    @Test
    public void testAuthTimeoutConfig_negativeRejected() {
        assertBadConfig("ws::addr=localhost:9000;auth_timeout_ms=-50;", "auth_timeout_ms must be > 0");
    }

    @Test
    public void testAuthTimeoutConfig_notSupportedForHttp() {
        assertBadConfig("http::addr=localhost:9000;auth_timeout_ms=1000;",
                "auth_timeout_ms is only supported for WebSocket transport");
    }

    @Test
    public void testAuthTimeoutBuilder_notSupportedForTcp() {
        assertThrows("auth_timeout_ms is only supported for WebSocket transport",
                () -> Sender.builder(Sender.Transport.TCP)
                        .address(LOCALHOST)
                        .authTimeoutMillis(1000));
    }

    @Test
    public void testWsConfigString_emptyHost_fails() {
        assertBadConfig("ws::addr=:9000;", "empty host in addr entry");
    }

    @Test
    public void testWsConfigString_dupAddr_explicitThenDefaultPort_fails() {
        assertBadConfig("ws::addr=a:9000,a;", "duplicate addr entry");
    }

    @Test
    public void testWsConfigString_dupAddr_defaultThenExplicitPort_fails() {
        assertBadConfig("ws::addr=a,a:9000;", "duplicate addr entry");
    }

    @Test
    public void testWsConfigString_dupAddr_bothDefaultPort_fails() {
        assertBadConfig("ws::addr=a,a;", "duplicate addr entry");
    }

    @Test
    public void testWsConfigString_addrWithTrailingHostWhitespace_trimmed() {
        Sender.LineSenderBuilder builder = Sender.builder("ws::addr=localhost :9000;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testWsConfigString_addrWithLeadingPortWhitespace_trimmed() {
        Sender.LineSenderBuilder builder = Sender.builder("ws::addr=localhost: 9000;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testFullAsyncConfiguration() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(1000)
                .autoFlushBytes(1024 * 1024)
                .autoFlushIntervalMillis(100);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testFullAsyncConfigurationWithTls() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls()
                .advancedTls().disableCertificateValidation()
                .autoFlushRows(1000)
                .autoFlushBytes(1024 * 1024);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testHttpPath_notSupportedForWebSocket() {
        assertThrows("HTTP path is not supported for WebSocket protocol",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpPath("/custom/path"));
    }

    @Test
    public void testHttpSettingPath_notSupportedForWebSocket() {
        assertThrows("HTTP settings path is not supported for WebSocket protocol",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpSettingPath("/custom/settings"));
    }

    @Test
    public void testHttpTimeout_notSupportedForWebSocket() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpTimeoutMillis(5000),
                "not supported for WebSocket");
    }

    @Test
    public void testHttpToken_accepted() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .httpToken("token");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testInvalidPort_fails() {
        assertThrows("invalid port",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address(LOCALHOST + ":99999"));
    }

    @Test
    public void testInvalidSchema_fails() {
        assertBadConfig("invalid::addr=localhost:9000;", "invalid schema [schema=invalid, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
    }

    @Test
    public void testMalformedPortInAddress_fails() {
        assertThrows("cannot parse a port from the address",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address("foo:nonsense12334"));
    }

    @Test
    public void testMaxBackoff_notSupportedForWebSocket() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .maxBackoffMillis(1000),
                "not supported for WebSocket");
    }

    @Test
    public void testMaxBufferCapacityNotSupported_fails() {
        assertThrows("maximum buffer capacity is not supported for WebSocket transport",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .maxBufferCapacity(128 * 1024));
    }

    @Test
    public void testMaxNameLength() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .maxNameLength(256);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testMaxNameLengthDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .maxNameLength(128)
                        .maxNameLength(256));
    }

    @Test
    public void testMaxNameLengthTooSmall_fails() {
        assertThrows("at least 16 bytes",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .maxNameLength(10));
    }

    @Test
    public void testMinRequestThroughput_notSupportedForWebSocket() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .minRequestThroughput(10000),
                "not supported for WebSocket");
    }

    @Test
    public void testMultipleAddressesAccepted() throws Exception {
        // Multi-host failover is supported on the WebSocket transport since
        // the X-QuestDB-Role rotation hook landed. The builder should accept
        // any positive number of addresses without an early reject.
        // build() still fails on connect (no server is listening on the
        // probed ephemeral ports) but the failure mode must NOT be a
        // builder-level rejection of multi-host configuration.
        // Use ephemeral ports captured-and-released so the test is not
        // accidentally skipped by a real local service binding 9000/9001.
        int p1, p2;
        try (java.net.ServerSocket s1 = new java.net.ServerSocket(0);
             java.net.ServerSocket s2 = new java.net.ServerSocket(0)) {
            p1 = s1.getLocalPort();
            p2 = s2.getLocalPort();
        }
        try (Sender ignored = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST + ":" + p1)
                .address(LOCALHOST + ":" + p2)
                .build()) {
            Assert.fail("expected connect failure (no listener), but build() returned a sender");
        } catch (LineSenderException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage();
            Assert.assertFalse(
                    "multi-address must no longer be rejected at builder level: " + msg,
                    msg.contains("single address"));
            // Off-mode single-pass walk wraps the per-host failure in a
            // "failed after walking N host(s)" message.
            Assert.assertTrue("expected a connect failure: " + msg,
                    msg.contains("Failed to connect")
                            || msg.contains("walked"));
        }
    }

    @Test
    public void testMultipleAddresses_buildable() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST + ":9000")
                .address(LOCALHOST + ":9001");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testMultipleAddresses_noPorts_buildPastValidation() {
        // configureDefaults() must pad ports so hosts.size == ports.size for
        // multi-host builders that omit explicit ports. The build() then fails
        // on connection (no server), NOT on the "host:port pair" validation.
        try {
            Sender.builder(Sender.Transport.WEBSOCKET)
                    .address("127.0.0.1")
                    .address("127.0.0.1")
                    .build()
                    .close();
            Assert.fail("expected build to fail with connection error");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertFalse(
                    "build failed on host/port count validation: " + msg,
                    msg.contains("host:port pair") || msg.contains("host/port count mismatch"));
        }
    }

    @Test
    public void testMixedPortAddresses_unevenCounts_rejected() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":9000")
                        .address(LOCALHOST + ":9001")
                        .port(9099),
                "mismatch between number of hosts and number of ports");
    }

    @Test
    public void testWsConfigString_sameHostDifferentPorts_passes() {
        Sender.LineSenderBuilder builder = Sender.builder("ws::addr=a:9000,a:9001;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testNoAddress_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET),
                "address not set");
    }

    @Test
    public void testPortMismatch_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":9000")
                        .port(9001),
                "mismatch");
    }

    @Test
    public void testProtocolVersion_notSupportedForWebSocket() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .protocolVersion(Sender.PROTOCOL_VERSION_V2),
                "not supported for WebSocket");
    }

    @Test
    public void testRetryTimeout_notSupportedForWebSocket() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .retryTimeoutMillis(5000),
                "not supported for WebSocket");
    }

    @Test
    public void testSyncModeAutoFlushDefaults() throws Exception {
        // Regression test: connect() must not hardcode autoFlush to 0.
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0)) {
                Assert.assertEquals(
                        QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                        sender.getAutoFlushRows()
                );
                Assert.assertEquals(
                        QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                        sender.getAutoFlushBytes()
                );
                Assert.assertEquals(
                        QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                        sender.getAutoFlushIntervalNanos()
                );
            }
        });
    }

    @Test
    public void testTcpAuth_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .enableAuth("keyId")
                        .authToken("5UjEMuA0Pj5pjK8a-fa24dyIf-Es5mYny3oE_Wmus48"),
                "not supported for WebSocket");
    }

    @Test
    public void testTlsDoubleSet_fails() {
        assertThrows("already enabled",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .enableTls()
                        .enableTls());
    }

    @Test
    public void testTlsEnabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls();
        Assert.assertNotNull(builder);
    }

    @Test
    public void testTlsValidationDisabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls()
                .advancedTls().disableCertificateValidation();
        Assert.assertNotNull(builder);
    }

    @Test
    public void testTlsValidationDisabled_butTlsNotEnabled_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .advancedTls().disableCertificateValidation()
                        .address(LOCALHOST),
                "TLS was not enabled");
    }

    @Test
    public void testUsernamePassword_accepted() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .httpUsernamePassword("user", "pass");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testWsConfigString() throws Exception {
        assertMemoryLeak(() -> {
            int port = findUnusedPort();
            assertBadConfig("ws::addr=localhost:" + port + ";", "connect", "Failed");
        });
    }

    @Test
    public void testWsConfigString_missingAddr_fails() throws Exception {
        assertMemoryLeak(() -> {
            int port = findUnusedPort();
            assertBadConfig("ws::addr=localhost:" + port + ";", "connect", "Failed");
            // sf-client.md §4.6 now rejects unknown keys, so a valid key
            // (user=) is used to drive the parser past key parsing and
            // surface the missing-addr error on its own.
            assertBadConfig("ws::user=foo;", "missing required key: addr");
        });
    }

    @Test
    public void testWsConfigString_protocolAlreadyConfigured_fails() throws Exception {
        assertMemoryLeak(() -> {
            int port = findUnusedPort();
            assertThrowsAny(
                    Sender.builder("ws::addr=localhost:" + port + ";")
                            .enableTls(),
                    "TLS", "connect", "Failed"
            );
        });
    }

    @Test
    public void testWsConfigString_uppercaseNotSupported() {
        assertBadConfig("WS::addr=localhost:9000;", "invalid schema");
    }

    @Test
    public void testWsConfigString_multipleAddresses() {
        Sender.LineSenderBuilder builder =
                Sender.builder("ws::addr=a:9000,b:9001,c:9002;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testWsConfigString_withAutoFlushBytes() {
        Sender.LineSenderBuilder builder = Sender.builder("ws::addr=localhost:9000;auto_flush_bytes=1024;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testWsConfigString_withAutoFlushBytesDoubleSet_lastWriteWins() {
        // Duplicate keys resolve last-write-wins on the QWP path: a repeated
        // auto_flush_bytes is accepted, not rejected. Sender.builder() parses
        // without connecting, so a successful parse returns a builder.
        Sender.LineSenderBuilder builder =
                Sender.builder("ws::addr=localhost:9000;auto_flush_bytes=1024;auto_flush_bytes=2048;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testWsConfigString_withAutoFlushBytesInvalid_fails() {
        assertBadConfig("ws::addr=localhost:9000;auto_flush_bytes=-1;", "cannot be negative");
    }

    @Test
    public void testWsConfigString_withGorilla_fails() {
        // gorilla has been removed; QWP ingestion always uses Gorilla timestamp
        // encoding. The Sender rejects the key on a ws:: string as an unknown
        // key, matching the QwpQueryClient (egress).
        assertBadConfig("ws::addr=localhost:9000;gorilla=off;", "unknown configuration key: gorilla");
        assertBadConfig("ws::addr=localhost:9000;gorilla=on;", "unknown configuration key: gorilla");
    }

    @Test
    public void testWsConfigString_withInitBufSize_fails() {
        assertBadConfig("ws::addr=localhost:9000;init_buf_size=1024;", "unknown configuration key: init_buf_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWsConfigString_withMaxBufSize_fails() {
        assertBadConfig("ws::addr=localhost:9000;max_buf_size=1000000;", "unknown configuration key: max_buf_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWsConfigString_withMaxSchemasPerConnection_fails() {
        // max_schemas_per_connection was removed with the QWP schema-reference
        // mechanism. The connection string used to accept it; it must now be
        // rejected as an unknown key rather than silently swallowed.
        assertBadConfig("ws::addr=localhost:9000;max_schemas_per_connection=1024;",
                "unknown configuration key: max_schemas_per_connection");
    }

    @Test
    public void testWsConfigString_withPath_fails() {
        // path is an egress endpoint that the QWP read client no longer accepts.
        // The Sender rejects it on a ws:: string as an unknown key, matching the
        // QwpQueryClient (egress).
        assertBadConfig("ws::addr=localhost:9000;path=/read/v1;",
                "unknown configuration key: path");
    }

    @Test
    public void testWsConfigString_withEgressOnlyKeysSilentlyAccepted() {
        // connect-string.md "Query client keys" and "Multi-host failover": these
        // keys configure the QwpQueryClient (egress) only. A ws:: / wss:: connect
        // string is shared by the Sender and the QwpQueryClient, so the Sender
        // must silently consume the egress-only keys rather than reject them as
        // unknown. The Sender does not interpret the value -- validation is the
        // egress parser's job, so even an out-of-range value parses here.
        String[] keys = {
                "buffer_pool_size=8",
                "client_id=batch-job/42",
                "compression=zstd",
                "compression_level=5",
                "failover=on",
                "failover_backoff_initial_ms=50",
                "failover_backoff_max_ms=1000",
                "failover_max_attempts=8",
                "failover_max_duration_ms=30000",
                "initial_credit=1048576",
                "max_batch_rows=512",
                "target=primary",
        };
        StringBuilder all = new StringBuilder("ws::addr=localhost:9000;");
        for (String kv : keys) {
            Assert.assertNotNull(Sender.builder("ws::addr=localhost:9000;" + kv + ";"));
            all.append(kv).append(';');
        }
        // All egress-only keys at once -- a typical shared-config connect string.
        Assert.assertNotNull(Sender.builder(all.toString()));
        // The Sender does not validate egress-only values; an out-of-range one
        // parses without complaint.
        Assert.assertNotNull(Sender.builder("ws::addr=localhost:9000;buffer_pool_size=-1;"));
    }

    @Test
    public void testWsConfigString_withReservedErrorPolicyKeysSilentlyAccepted() {
        // connect-string.md "Error handling": the on_*_error keys are reserved by
        // the spec, which directs new clients to accept them in the connect
        // string. The Sender does not wire them to a policy yet, so it consumes
        // them as an accepted no-op -- it must not reject them as unknown keys.
        // Mirror of the QwpQueryClient (egress) behavior so one connect string
        // carrying these keys configures both clients.
        String[] keys = {
                "on_internal_error=halt",
                "on_parse_error=halt",
                "on_schema_error=drop",
                "on_security_error=halt",
                "on_server_error=auto",
                "on_write_error=drop",
        };
        StringBuilder all = new StringBuilder("ws::addr=localhost:9000;");
        for (String kv : keys) {
            Assert.assertNotNull(Sender.builder("ws::addr=localhost:9000;" + kv + ";"));
            all.append(kv).append(';');
        }
        Assert.assertNotNull(Sender.builder(all.toString()));
    }

    @Test
    public void testWsConfigString_withMaxDatagramSize_fails() {
        // max_datagram_size applies to the UDP transport only; it is absent
        // from the QWP connect-string vocabulary shared with the egress client.
        assertBadConfig("ws::addr=localhost:9000;max_datagram_size=1400;",
                "unknown configuration key: max_datagram_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWsConfigString_withMulticastTtl_fails() {
        // multicast_ttl applies to the UDP transport only; it is absent from
        // the QWP connect-string vocabulary shared with the egress client.
        assertBadConfig("ws::addr=localhost:9000;multicast_ttl=4;",
                "unknown configuration key: multicast_ttl (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWsConfigString_withProtocolVersionAuto_fails() {
        // protocol_version is not part of the QWP connect-string vocabulary at
        // all; even the no-op "auto" value is rejected on ws::, matching the
        // egress QwpQueryClient and the other language clients.
        assertBadConfig("ws::addr=localhost:9000;protocol_version=auto;",
                "unknown configuration key: protocol_version (QWP negotiates the protocol version during the WebSocket upgrade)");
    }

    @Test
    public void testWsConfigString_withProtocolVersion_fails() {
        // protocol_version is a legacy ILP key, not part of the QWP
        // connect-string vocabulary; QWP negotiates its version at handshake.
        assertBadConfig("ws::addr=localhost:9000;protocol_version=2;",
                "unknown configuration key: protocol_version (QWP negotiates the protocol version during the WebSocket upgrade)");
    }

    @Test
    public void testWsConfigString_withRequestMinThroughput_fails() {
        // request_min_throughput is an HTTP-only key, absent from the QWP
        // connect-string vocabulary.
        assertBadConfig("ws::addr=localhost:9000;request_min_throughput=102400;",
                "unknown configuration key: request_min_throughput (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWsConfigString_withRequestTimeout_fails() {
        // request_timeout is an HTTP-only key, absent from the QWP
        // connect-string vocabulary.
        assertBadConfig("ws::addr=localhost:9000;request_timeout=10000;",
                "unknown configuration key: request_timeout (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWsConfigString_withRetryTimeout_fails() {
        // retry_timeout is an HTTP-only key; the QWP analogue is the per-outage
        // reconnect budget (reconnect_max_duration_millis).
        assertBadConfig("ws::addr=localhost:9000;retry_timeout=10000;",
                "unknown configuration key: retry_timeout (use reconnect_max_duration_millis on ws/wss)");
    }

    @Test
    public void testWsConfigString_reconnectMaxDurationZero_fails() {
        // A zero budget must be rejected up front: connectWithRetry's deadline
        // (startNanos + 0) makes the SYNC initial connect throw "after 0ms /
        // 0 attempts" without ever dialing a healthy server, and the drainer's
        // capabilityGapBudgetNanos=0 exhausts the durable-ack settle budget
        // (0 >= 0) on the first gap sweep, quarantining the slot instantly
        // instead of after the documented 16-sweep budget.
        assertBadConfig("ws::addr=localhost:9000;reconnect_max_duration_millis=0;",
                "reconnect_max_duration_millis must be > 0");
    }

    @Test
    public void testWsConfigString_reconnectMaxDurationNegative_fails() {
        assertBadConfig("ws::addr=localhost:9000;reconnect_max_duration_millis=-1;",
                "reconnect_max_duration_millis must be > 0");
    }

    @Test
    public void testReconnectMaxDurationBuilder_zeroRejected() {
        assertThrows("reconnect_max_duration_millis must be > 0",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .reconnectMaxDurationMillis(0));
    }

    @Test
    public void testReconnectMaxDurationBuilder_negativeRejected() {
        assertThrows("reconnect_max_duration_millis must be > 0",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .reconnectMaxDurationMillis(-1));
    }

    @Test
    public void testWsConfigString_usernameWithoutPassword_fails() {
        // The ingress ws path rejects a username with no password up front in
        // validateWsConfig, with the same message the egress QwpQueryClient uses,
        // so a shared ws/wss string fails identically on both sides.
        assertBadConfig("ws::addr=localhost:9000;username=alice;", "username and password must be provided together");
    }

    @Test
    public void testWsConfigString_passwordWithoutUsername_fails() {
        // The reverse half-credential is rejected with the same message.
        assertBadConfig("ws::addr=localhost:9000;password=secret;", "username and password must be provided together");
    }

    @Test
    public void testWsConfigString_tokenWithBasicAuth_fails() {
        // token and username/password are mutually exclusive on the ingress side.
        assertBadConfig("ws::addr=localhost:9000;token=ey.abc;username=alice;password=secret;",
                "cannot use both token and username/password authentication");
    }

    @Test
    public void testWsConfigString_tlsKeysOnNonTlsSchema_fails() {
        // tls_verify/tls_roots/tls_roots_password require the wss schema; on a
        // plain ws string validateWsConfig rejects them.
        assertBadConfig("ws::addr=localhost:9000;tls_verify=on;", "require the wss:: schema");
        assertBadConfig("ws::addr=localhost:9000;tls_roots=/ca.p12;", "require the wss:: schema");
    }

    @Test
    public void testWsConfigString_withToken() throws Exception {
        assertMemoryLeak(() -> {
            Sender.LineSenderBuilder builder = Sender.builder("ws::addr=localhost:9000;token=mytoken;");
            Assert.assertNotNull(builder);
        });
    }

    @Test
    public void testWsConfigString_withUsernamePassword() throws Exception {
        assertMemoryLeak(() -> {
            int port = findUnusedPort();
            assertBadConfig("ws::addr=localhost:" + port + ";username=user;password=pass;", "connect", "Failed");
        });
    }

    @Test
    public void testWssConfigString() throws Exception {
        assertMemoryLeak(() -> assertBadConfig("wss::addr=localhost:9000;tls_verify=unsafe_off;", "connect", "Failed", "SSL"));
    }

    @Test
    public void testWssConfigString_uppercaseNotSupported() {
        assertBadConfig("WSS::addr=localhost:9000;", "invalid schema");
    }

    @Test
    public void testWssConfigString_withAutoFlushBytes() {
        Sender.LineSenderBuilder builder = Sender.builder("wss::addr=localhost:9000;tls_verify=unsafe_off;auto_flush_bytes=1024;");
        Assert.assertNotNull(builder);
    }

    @Test
    public void testWssConfigString_withInitBufSize_fails() {
        assertBadConfig("wss::addr=localhost:9000;tls_verify=unsafe_off;init_buf_size=1024;", "unknown configuration key: init_buf_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWssConfigString_withMaxBufSize_fails() {
        assertBadConfig("wss::addr=localhost:9000;tls_verify=unsafe_off;max_buf_size=1000000;", "unknown configuration key: max_buf_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testWssConfigString_withToken() {
        Sender.LineSenderBuilder builder = Sender.builder("wss::addr=localhost:9000;tls_verify=unsafe_off;token=mytoken;");
        Assert.assertNotNull(builder);
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

    // There is a TOCTOU race between closing the ServerSocket and the caller's
    // connect attempt — another process could bind the port in between. This is
    // acceptable because every caller is a negative test that expects the connection
    // to fail. If the port is stolen, the test connects to a non-QuestDB endpoint,
    // which also fails with the same error.
    private static int findUnusedPort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
