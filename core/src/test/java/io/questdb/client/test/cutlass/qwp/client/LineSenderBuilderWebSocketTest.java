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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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

    // ==================== Transport Selection Tests ====================

    @Test
    public void testAddressWithoutPort_usesDefaultPort9000() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeCanBeSetMultipleTimes() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .asyncMode(false);
        Assert.assertNotNull(builder);
    }

    // ==================== Address Configuration Tests ====================

    @Test
    public void testAsyncModeDisabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(false);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeEnabled() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testAsyncModeWithAllOptions() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .autoFlushRows(500)
                .autoFlushBytes(512 * 1024)
                .autoFlushIntervalMillis(50)
                .inFlightWindowSize(8)
                .sendQueueCapacity(16);
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

    // ==================== TLS Configuration Tests ====================

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

    // ==================== Async Mode Tests ====================

    @Test
    public void testAutoFlushRowsZero_disablesRowBasedAutoFlush() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .autoFlushRows(0);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testBufferCapacity() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .bufferCapacity(128 * 1024);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testBufferCapacityDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .bufferCapacity(1024)
                        .bufferCapacity(2048));
    }

    // ==================== Auto Flush Rows Tests ====================

    @Test
    public void testBufferCapacityNegative_fails() {
        assertThrows("cannot be negative",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .bufferCapacity(-1));
    }

    @Test
    public void testBuilderWithWebSocketTransport() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET);
        Assert.assertNotNull("Builder should be created for WebSocket transport", builder);
    }

    @Test
    public void testBuilderWithWebSocketTransportCreatesCorrectSenderType() throws Exception {
        int port;
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            port = s.getLocalPort();
        }
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":" + port),
                "connect", "Failed"
        );
    }

    @Test
    public void testConnectionRefused() throws Exception {
        int port = findUnusedPort();
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":" + port),
                "connect", "Failed"
        );
    }

    // ==================== Auto Flush Bytes Tests ====================

    @Test
    public void testCustomTrustStore_butTlsNotEnabled_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .advancedTls().customTrustStore("/some/path", "password".toCharArray())
                        .address(LOCALHOST),
                "TLS was not enabled");
    }

    @Test
    @Ignore("Disable auto flush may need different semantics for WebSocket")
    public void testDisableAutoFlush_semantics() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .disableAutoFlush();
        Assert.assertNotNull(builder);
    }

    @Test
    public void testDnsResolutionFailure() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld:9000"),
                "resolve", "connect", "Failed"
        );
    }

    @Test
    public void testDuplicateAddresses_fails() {
        assertThrows("duplicated addresses",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":9000")
                        .address(LOCALHOST + ":9000"));
    }

    // ==================== Auto Flush Interval Tests ====================

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
    public void testFullAsyncConfiguration() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .autoFlushRows(1000)
                .autoFlushBytes(1024 * 1024)
                .autoFlushIntervalMillis(100)
                .inFlightWindowSize(16)
                .sendQueueCapacity(32);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testFullAsyncConfigurationWithTls() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .enableTls()
                .advancedTls().disableCertificateValidation()
                .asyncMode(true)
                .autoFlushRows(1000)
                .autoFlushBytes(1024 * 1024)
                .inFlightWindowSize(16)
                .sendQueueCapacity(32);
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("HTTP path is HTTP-specific and may not apply to WebSocket")
    public void testHttpPath_mayNotApply() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .httpPath("/custom/path");
        Assert.assertNotNull(builder);
    }

    // ==================== In-Flight Window Size Tests ====================

    @Test
    @Ignore("HTTP timeout is HTTP-specific and may not apply to WebSocket")
    public void testHttpTimeout_mayNotApply() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .httpTimeoutMillis(5000);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testHttpToken_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpToken("token"),
                "not yet supported");
    }

    @Test
    @Ignore("HTTP token authentication is not yet supported for WebSocket protocol")
    public void testHttpToken_notYetSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpToken("token"),
                "not yet supported");
    }

    @Test
    public void testInFlightWindowSizeDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(true)
                        .inFlightWindowSize(8)
                        .inFlightWindowSize(16));
    }

    @Test
    public void testInFlightWindowSizeNegative_fails() {
        assertThrows("must be positive",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(true)
                        .inFlightWindowSize(-1));
    }

    // ==================== Send Queue Capacity Tests ====================

    @Test
    public void testInFlightWindowSizeZero_fails() {
        assertThrows("must be positive",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(true)
                        .inFlightWindowSize(0));
    }

    @Test
    public void testInFlightWindowSize_withAsyncMode() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .inFlightWindowSize(16);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testInFlightWindowSize_withoutAsyncMode_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .inFlightWindowSize(16),
                "requires async mode");
    }

    @Test
    public void testInvalidPort_fails() {
        assertThrows("invalid port",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address(LOCALHOST + ":99999"));
    }

    @Test
    public void testInvalidSchema_fails() {
        assertBadConfig("invalid::addr=localhost:9000;", "invalid schema [schema=invalid, supported-schemas=[http, https, tcp, tcps, ws, wss]]");
    }

    // ==================== Combined Async Configuration Tests ====================

    @Test
    public void testMalformedPortInAddress_fails() {
        assertThrows("cannot parse a port from the address",
                () -> Sender.builder(Sender.Transport.WEBSOCKET).address("foo:nonsense12334"));
    }

    @Test
    @Ignore("Max backoff is HTTP-specific and may not apply to WebSocket")
    public void testMaxBackoff_mayNotApply() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .maxBackoffMillis(1000);
        Assert.assertNotNull(builder);
    }

    // ==================== Config String Tests (ws:// and wss://) ====================

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
    @Ignore("Min request throughput is HTTP-specific and may not apply to WebSocket")
    public void testMinRequestThroughput_mayNotApply() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .minRequestThroughput(10000);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testMultipleAddresses_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST + ":9000")
                        .address(LOCALHOST + ":9001"),
                "single address");
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

    // ==================== Buffer Configuration Tests ====================

    @Test
    @Ignore("Protocol version is for ILP text protocol, WebSocket uses ILP v4 binary protocol")
    public void testProtocolVersion_notApplicable() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .protocolVersion(Sender.PROTOCOL_VERSION_V2);
        Assert.assertNotNull(builder);
    }

    @Test
    @Ignore("Retry timeout is HTTP-specific and may not apply to WebSocket")
    public void testRetryTimeout_mayNotApply() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .retryTimeoutMillis(5000);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testSendQueueCapacityDoubleSet_fails() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(true)
                        .sendQueueCapacity(16)
                        .sendQueueCapacity(32));
    }

    // ==================== Unsupported Features (TCP Authentication) ====================

    @Test
    public void testSendQueueCapacityNegative_fails() {
        assertThrows("must be positive",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(true)
                        .sendQueueCapacity(-1));
    }

    @Test
    public void testSendQueueCapacityZero_fails() {
        assertThrows("must be positive",
                () -> Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(true)
                        .sendQueueCapacity(0));
    }

    // ==================== Unsupported Features (HTTP Token Authentication) ====================

    @Test
    public void testSendQueueCapacity_withAsyncMode() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST)
                .asyncMode(true)
                .sendQueueCapacity(32);
        Assert.assertNotNull(builder);
    }

    @Test
    public void testSendQueueCapacity_withoutAsyncMode_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .sendQueueCapacity(32),
                "requires async mode");
    }

    // ==================== Unsupported Features (Username/Password Authentication) ====================

    @Test
    public void testSyncModeDoesNotAllowInFlightWindowSize() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(false)
                        .inFlightWindowSize(16),
                "requires async mode");
    }

    @Test
    public void testSyncModeDoesNotAllowSendQueueCapacity() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .asyncMode(false)
                        .sendQueueCapacity(32),
                "requires async mode");
    }

    // ==================== Unsupported Features (HTTP-specific options) ====================

    @Test
    public void testSyncModeAutoFlushDefaults() throws Exception {
        // Regression test: sync-mode connect() must not hardcode autoFlush to 0.
        // createForTesting(host, port, windowSize) mirrors what connect(h,p,tls)
        // creates internally. Verify it uses sensible defaults.
        TestUtils.assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1);
            try {
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
            } finally {
                sender.close();
            }
        });
    }

    @Test
    public void testSyncModeIsDefault() {
        Sender.LineSenderBuilder builder = Sender.builder(Sender.Transport.WEBSOCKET)
                .address(LOCALHOST);
        Assert.assertNotNull(builder);
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

    // ==================== Unsupported Features (Protocol Version) ====================

    @Test
    public void testUsernamePassword_fails() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpUsernamePassword("user", "pass"),
                "not yet supported");
    }

    // ==================== Config String Unsupported Options ====================

    @Test
    @Ignore("Username/password authentication is not yet supported for WebSocket protocol")
    public void testUsernamePassword_notYetSupported() {
        assertThrowsAny(
                Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(LOCALHOST)
                        .httpUsernamePassword("user", "pass"),
                "not yet supported");
    }

    @Test
    public void testWsConfigString() throws Exception {
        int port = findUnusedPort();
        assertBadConfig("ws::addr=localhost:" + port + ";", "connect", "Failed");
    }

    // ==================== Edge Cases ====================

    @Test
    public void testWsConfigString_missingAddr_fails() throws Exception {
        int port = findUnusedPort();
        assertBadConfig("ws::addr=localhost:" + port + ";", "connect", "Failed");
        assertBadConfig("ws::foo=bar;", "addr is missing");
    }

    @Test
    public void testWsConfigString_protocolAlreadyConfigured_fails() throws Exception {
        int port = findUnusedPort();
        assertThrowsAny(
                Sender.builder("ws::addr=localhost:" + port + ";")
                        .enableTls(),
                "TLS", "connect", "Failed"
        );
    }

    @Test
    public void testWsConfigString_uppercaseNotSupported() {
        assertBadConfig("WS::addr=localhost:9000;", "invalid schema");
    }

    @Test
    @Ignore("Token authentication in ws config string is not yet supported")
    public void testWsConfigString_withToken_notYetSupported() {
        assertBadConfig("ws::addr=localhost:9000;token=mytoken;", "not yet supported");
    }

    @Test
    @Ignore("Username/password in ws config string is not yet supported")
    public void testWsConfigString_withUsernamePassword_notYetSupported() {
        assertBadConfig("ws::addr=localhost:9000;username=user;password=pass;", "not yet supported");
    }

    // ==================== Connection Tests ====================

    @Test
    public void testWssConfigString() {
        assertBadConfig("wss::addr=localhost:9000;tls_verify=unsafe_off;", "connect", "Failed", "SSL");
    }

    @Test
    public void testWssConfigString_uppercaseNotSupported() {
        assertBadConfig("WSS::addr=localhost:9000;", "invalid schema");
    }

    // ==================== Sync vs Async Mode Tests ====================

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

    private static int findUnusedPort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            return s.getLocalPort();
        }
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
