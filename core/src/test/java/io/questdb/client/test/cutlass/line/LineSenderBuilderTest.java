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

package io.questdb.client.test.cutlass.line;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.auth.AuthUtils;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import javax.security.auth.DestroyFailedException;
import java.security.PrivateKey;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests for LineSenderBuilder that don't require a running QuestDB instance.
 * Tests that require an actual QuestDB connection have been moved to integration tests.
 */
public class LineSenderBuilderTest {
    private static final String AUTH_TOKEN_KEY1 = "UvuVb1USHGRRT08gEnwN2zGZrvM4MsLQ5brgF6SVkAw=";
    private static final String LOCALHOST = "localhost";
    private static final char[] TRUSTSTORE_PASSWORD = "questdb".toCharArray();
    private static final String TRUSTSTORE_PATH = "/keystore/server.keystore";

    @Test
    public void testAddressDoubleSet_firstAddressThenAddress() throws Exception {
        assertMemoryLeak(() -> assertThrows("only a single address",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).address("127.0.0.1")));
    }

    @Test
    public void testAddressEmpty() throws Exception {
        assertMemoryLeak(() -> assertThrows("address cannot be empty",
                () -> Sender.builder(Sender.Transport.TCP).address("")));
    }

    @Test
    public void testAddressEndsWithColon() throws Exception {
        assertMemoryLeak(() -> assertThrows("invalid address",
                () -> Sender.builder(Sender.Transport.TCP).address("foo:")));
    }

    @Test
    public void testAddressNull() throws Exception {
        assertMemoryLeak(() -> assertThrows("null",
                () -> Sender.builder(Sender.Transport.TCP).address(null)));
    }

    @Test
    public void testAuthDoubleSet() throws Exception {
        assertMemoryLeak(() -> assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.TCP).enableAuth("foo").authToken(AUTH_TOKEN_KEY1).enableAuth("bar")));
    }

    @Test
    public void testAuthTooSmallBuffer() throws Exception {
        assertMemoryLeak(() -> assertThrows("minimalCapacity",
                Sender.builder(Sender.Transport.TCP)
                        .enableAuth("foo").authToken(AUTH_TOKEN_KEY1).address(LOCALHOST + ":9001")
                        .bufferCapacity(1)));
    }

    @Test
    public void testAuthWithBadToken() throws Exception {
        assertMemoryLeak(() -> assertThrows("could not import token",
                () -> Sender.builder(Sender.Transport.TCP).enableAuth("foo").authToken("bar token")));
    }

    @Test
    public void testAutoFlushIntervalMustBePositive() {
        assertThrows("auto flush interval cannot be negative [autoFlushIntervalMillis=0]",
                () -> Sender.builder(Sender.Transport.HTTP).autoFlushIntervalMillis(0));
        assertThrows("auto flush interval cannot be negative [autoFlushIntervalMillis=-1]",
                () -> Sender.builder(Sender.Transport.HTTP).autoFlushIntervalMillis(-1));
    }

    @Test
    public void testAutoFlushIntervalNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("auto flush interval is not supported for TCP protocol",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).autoFlushIntervalMillis(1)));
    }

    @Test
    public void testAutoFlushInterval_afterAutoFlushDisabled() throws Exception {
        assertMemoryLeak(() -> assertThrows("cannot set auto flush interval when interval based auto-flush is already disabled",
                () -> Sender.builder(Sender.Transport.HTTP).disableAutoFlush().autoFlushIntervalMillis(1)));
    }

    @Test
    public void testAutoFlushInterval_doubleConfiguration() throws Exception {
        assertMemoryLeak(() -> assertThrows("auto flush interval was already configured [autoFlushIntervalMillis=1]",
                () -> Sender.builder(Sender.Transport.HTTP).autoFlushIntervalMillis(1).autoFlushIntervalMillis(1)));
    }

    @Test
    public void testAutoFlushRowsCannotBeNegative() {
        assertThrows("auto flush rows cannot be negative [autoFlushRows=-1]",
                () -> Sender.builder(Sender.Transport.HTTP).autoFlushRows(-1));
    }

    @Test
    public void testAutoFlushRowsNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("auto flush rows is not supported for TCP protocol",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).autoFlushRows(1)));
    }

    @Test
    public void testAutoFlushRows_doubleConfiguration() throws Exception {
        assertMemoryLeak(() -> assertThrows("auto flush rows was already configured [autoFlushRows=1]",
                () -> Sender.builder(Sender.Transport.HTTP).autoFlushRows(1).autoFlushRows(1)));
    }

    @Test
    public void testBufferPoolSizeSilentlyAcceptedOnIngress() throws Exception {
        // connect-string.md "Query client keys": buffer_pool_size is egress-only
        // (it sizes the QwpQueryClient I/O thread's decoded-batch buffer pool).
        // Ingress silently accepts the key so the same connect string can be
        // shared between the Sender and the QwpQueryClient without an "unknown
        // configuration key" error. Exercise every ingress protocol
        // -- HTTP/HTTPS, WebSocket, TCP, UDP -- and confirm the parser does not
        // raise on the key. A connect-time failure is tolerated (these are
        // unreachable test addresses) as long as the exception message does
        // not point at buffer_pool_size= as the offender.
        assertMemoryLeak(() -> {
            // protocol_version is pinned on HTTP/HTTPS so fromConfig does not
            // contact the server for auto-detection.
            assertConfStrOk("http::addr=" + LOCALHOST + ";buffer_pool_size=8;protocol_version=2;");
            assertConfStrOk("https::addr=" + LOCALHOST + ";buffer_pool_size=8;tls_verify=unsafe_off;protocol_version=2;");
            // UDP build is purely local; no I/O.
            assertConfStrOk("udp::addr=" + LOCALHOST + ";buffer_pool_size=8;");
            // WebSocket and TCP attempt synchronous connect at build time.
            // Allow LineSenderException for connect failure but require the
            // parser did not flag buffer_pool_size=.
            assertConfStrAcceptsBufferPoolSizeKey("ws::addr=127.0.0.1:1;buffer_pool_size=8;");
            assertConfStrAcceptsBufferPoolSizeKey("tcp::addr=127.0.0.1:1;buffer_pool_size=8;");
            // Empty value is also accepted: an empty value after `=` is
            // well-formed and the silent-consume branch does not parse it.
            assertConfStrOk("http::addr=" + LOCALHOST + ";buffer_pool_size=;protocol_version=2;");
            // The Sender does not validate the value either -- range checking
            // is the QwpQueryClient parser's responsibility, not the Sender's.
            // A value the egress parser would reject (0, negative, non-numeric)
            // is still silently consumed here.
            assertConfStrOk("http::addr=" + LOCALHOST + ";buffer_pool_size=0;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";buffer_pool_size=-1;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";buffer_pool_size=big;protocol_version=2;");
        });
    }

    @Test
    public void testBufferSizeDoubleSet() throws Exception {
        assertMemoryLeak(() -> assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.TCP).bufferCapacity(1024).bufferCapacity(1024)));
    }

    @Test
    public void testConfStringValidation() throws Exception {
        assertMemoryLeak(() -> {
            assertConfStrError("foo", "invalid schema [schema=foo, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            assertConfStrError("badschema::addr=bar;", "invalid schema [schema=badschema, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            assertConfStrError("http::addr=localhost:-1;", "invalid port [port=-1]");
            assertConfStrError("http::auto_flush=on;", "addr is missing");
            assertConfStrError("http::addr=localhost;tls_roots=/some/path;", "tls_roots was configured, but tls_roots_password is missing");
            assertConfStrError("http::addr=localhost;tls_roots_password=hunter123;", "tls_roots_password was configured, but tls_roots is missing");
            assertConfStrError("tcp::addr=localhost;user=foo;", "token cannot be empty nor null");
            assertConfStrError("tcp::addr=localhost;username=foo;", "token cannot be empty nor null");
            assertConfStrError("tcp::addr=localhost;token=foo;", "TCP token is configured, but user is missing");
            assertConfStrError("http::addr=localhost;user=foo;", "password cannot be empty nor null");
            assertConfStrError("http::addr=localhost;username=foo;", "password cannot be empty nor null");
            assertConfStrError("http::addr=localhost;pass=foo;", "password is configured, but username is missing");
            assertConfStrError("http::addr=localhost;password=foo;", "password is configured, but username is missing");
            assertConfStrError("http::addr=localhost;auth=Bearer xyz;", "unknown configuration key [key=auth]");
            assertConfStrError("http::addr=localhost;path=/read/v1;", "unknown configuration key [key=path]");
            assertConfStrError("tcp::addr=localhost;pass=foo;", "password is not supported for TCP protocol");
            assertConfStrError("tcp::addr=localhost;password=foo;", "password is not supported for TCP protocol");
            assertConfStrError("tcp::addr=localhost;retry_timeout=;", "retry_timeout cannot be empty");
            assertConfStrError("tcp::addr=localhost;max_buf_size=;", "max_buf_size cannot be empty");
            assertConfStrError("tcp::addr=localhost;init_buf_size=;", "init_buf_size cannot be empty");
            assertConfStrError("http::addr=localhost:8080;tls_verify=unsafe_off;", "TLS validation disabled, but TLS was not enabled");
            assertConfStrError("http::addr=localhost:8080;tls_verify=bad;", "invalid tls_verify [value=bad, allowed-values=[on, unsafe_off]]");
            assertConfStrError("tcps::addr=localhost;pass=unsafe_off;", "password is not supported for TCP protocol");
            assertConfStrError("tcps::addr=localhost;password=unsafe_off;", "password is not supported for TCP protocol");
            assertConfStrError("http::addr=localhost:8080;max_buf_size=-32;", "maximum buffer capacity cannot be less than initial buffer capacity [maximumBufferCapacity=-32, initialBufferCapacity=65536]");
            assertConfStrError("http::addr=localhost:8080;max_buf_size=notanumber;", "invalid max_buf_size [value=notanumber]");
            assertConfStrError("http::addr=localhost:8080;init_buf_size=notanumber;", "invalid init_buf_size [value=notanumber]");
            assertConfStrError("http::addr=localhost:8080;init_buf_size=-42;", "buffer capacity cannot be negative [capacity=-42]");
            assertConfStrError("http::addr=localhost:8080;max_buf_size=4g;", "max_buf_size exceeds maximum [value=4g]");
            assertConfStrError("http::addr=localhost:8080;init_buf_size=4g;", "init_buf_size exceeds maximum [value=4g]");
            assertConfStrError("http::addr=localhost:8080;auto_flush_rows=0;", "invalid auto_flush_rows [value=0]");
            assertConfStrError("http::addr=localhost:8080;auto_flush_rows=notanumber;", "invalid auto_flush_rows [value=notanumber]");
            assertConfStrError("http::addr=localhost:8080;auto_flush=invalid;", "invalid auto_flush [value=invalid, allowed-values=[on, off]]");
            assertConfStrError("http::addr=localhost:8080;auto_flush=off;auto_flush_rows=100;", "cannot set auto flush rows when auto-flush is already disabled");
            assertConfStrError("http::addr=localhost:8080;auto_flush_rows=100;auto_flush=off;", "auto flush rows was already configured [autoFlushRows=100]");
            assertConfStrError("HTTP::addr=localhost;", "invalid schema [schema=HTTP, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            assertConfStrError("HTTPS::addr=localhost;", "invalid schema [schema=HTTPS, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            assertConfStrError("TCP::addr=localhost;", "invalid schema [schema=TCP, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            assertConfStrError("TCPS::addr=localhost;", "invalid schema [schema=TCPS, supported-schemas=[http, https, tcp, tcps, ws, wss, udp]]");
            assertConfStrError("http::addr=localhost;auto_flush=off;auto_flush_interval=1;", "cannot set auto flush interval when interval based auto-flush is already disabled");
            assertConfStrError("http::addr=localhost;auto_flush=off;auto_flush_rows=1;", "cannot set auto flush rows when auto-flush is already disabled");
            assertConfStrError("http::addr=localhost;auto_flush_bytes=1024;", "auto_flush_bytes is only supported for TCP and WebSocket transport");
            assertConfStrError("http::addr=localhost;protocol_version=10", "current client only supports protocol version 1(text format for all datatypes), 2(binary format for part datatypes), 3(decimal datatype) or explicitly unset");
            assertConfStrError("http::addr=localhost:48884;max_name_len=10;", "max_name_len must be at least 16 bytes [max_name_len=10]");
            assertConfStrError("ws::addr=localhost;max_buf_size=1000000;", "unknown configuration key: max_buf_size (applies to legacy http/tcp/udp transports only)");
            assertConfStrError("wss::addr=localhost;tls_verify=unsafe_off;max_buf_size=1000000;", "unknown configuration key: max_buf_size (applies to legacy http/tcp/udp transports only)");
            assertConfStrError("ws::addr=localhost;init_buf_size=1024;", "unknown configuration key: init_buf_size (applies to legacy http/tcp/udp transports only)");
            assertConfStrError("wss::addr=localhost;tls_verify=unsafe_off;init_buf_size=1024;", "unknown configuration key: init_buf_size (applies to legacy http/tcp/udp transports only)");

            assertConfStrOk("addr=localhost:8080", "auto_flush_rows=100", "protocol_version=1");
            assertConfStrOk("addr=localhost:8080", "auto_flush=on", "auto_flush_rows=100", "protocol_version=2");
            assertConfStrOk("addr=localhost:8080", "auto_flush_rows=100", "auto_flush=on", "protocol_version=2");
            assertConfStrOk("addr=localhost", "auto_flush=on", "protocol_version=2");
            assertConfStrOk("addr=localhost:8080", "max_name_len=1024", "protocol_version=2");

            assertConfStrError("tcp::addr=localhost;auto_flush_bytes=1024;init_buf_size=2048;", "TCP transport requires init_buf_size and auto_flush_bytes to be set to the same value [init_buf_size=2048, auto_flush_bytes=1024]");
            assertConfStrError("tcp::addr=localhost;init_buf_size=1024;auto_flush_bytes=2048;", "TCP transport requires init_buf_size and auto_flush_bytes to be set to the same value [init_buf_size=1024, auto_flush_bytes=2048]");
            assertConfStrError("tcp::addr=localhost;auto_flush_bytes=off;", "TCP transport must have auto_flush_bytes enabled");

            assertConfStrOk("http::addr=localhost;auto_flush=off;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;auto_flush_interval=off;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;auto_flush_rows=off;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;auto_flush_interval=off;auto_flush_rows=off;protocol_version=1;");
            assertConfStrOk("http::addr=localhost;auto_flush_interval=off;auto_flush_rows=1;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;auto_flush_rows=off;auto_flush_interval=1;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;auto_flush_interval=off;auto_flush_rows=off;auto_flush=off;protocol_version=2;");
            assertConfStrOk("http::addr=localhost;auto_flush=off;auto_flush_interval=off;auto_flush_rows=off;protocol_version=1;");
            assertConfStrOk("http::addr=localhost:8080;protocol_version=2;");
            assertConfStrOk("http::addr=localhost:8080;token=foo;protocol_version=2;");
            assertConfStrOk("http::addr=localhost:8080;token=foo=bar;protocol_version=2;");
            assertConfStrOk("addr=localhost:8080", "token=foo", "retry_timeout=1000", "max_buf_size=1000000", "protocol_version=2");
            assertConfStrOk("addr=localhost:8080", "token=foo", "retry_timeout=1000", "max_buf_size=1000000", "protocol_version=1");
            assertConfStrOk("http::addr=localhost:8080;token=foo;max_buf_size=1000000;retry_timeout=1000;protocol_version=2;");
            assertConfStrOk("http::addr=localhost:8080;max_buf_size=100m;protocol_version=2;");
            assertConfStrOk("http::addr=localhost:8080;max_buf_size=1G;protocol_version=2;");
            assertConfStrOk("http::addr=localhost:8080;init_buf_size=64k;protocol_version=2;");
            assertConfStrOk("http::addr=localhost:8080;init_buf_size=128KB;max_buf_size=2M;protocol_version=2;");
            assertConfStrOk("https::addr=localhost:8080;tls_verify=unsafe_off;auto_flush_rows=100;protocol_version=2;");
            assertConfStrOk("https::addr=localhost:8080;tls_verify=unsafe_off;auto_flush_rows=100;protocol_version=2;max_name_len=256;");
            assertConfStrOk("https::addr=localhost:8080;tls_verify=on;protocol_version=2;");
            assertConfStrOk("https::addr=localhost:8080;tls_verify=on;protocol_version=3;");
            assertConfStrError("https::addr=2001:0db8:85a3:0000:0000:8a2e:0370:7334;tls_verify=on;", "cannot parse a port from the address, use IPv4 address or a domain name [address=2001:0db8:85a3:0000:0000:8a2e:0370:7334]");
            assertConfStrError("https::addr=[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9000;tls_verify=on;", "cannot parse a port from the address, use IPv4 address or a domain name [address=[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9000]");

            // sf-client.md §4.6: unknown keys must be rejected, not silently
            // ignored. Forward-compat is via the spec, so silent ignore would
            // let language clients drift on the same connect string.
            assertConfStrError("http::addr=localhost;not_a_real_key=foo;", "unknown configuration key [key=not_a_real_key]");
            assertConfStrError("tcp::addr=localhost;not_a_real_key=foo;", "unknown configuration key [key=not_a_real_key]");
            assertConfStrError("ws::addr=localhost;not_a_real_key=foo;", "unknown configuration key: not_a_real_key");
            assertConfStrError("udp::addr=localhost;not_a_real_key=foo;", "unknown configuration key [key=not_a_real_key]");
            // The unknown-key error must surface even when the value would
            // itself be malformed -- the key is the reportable defect.
            assertConfStrError("http::addr=localhost;not_a_real_key=", "unknown configuration key [key=not_a_real_key]");

            // in_flight_window has been removed; it is now rejected like any
            // other unknown configuration key.
            assertConfStrError("http::addr=localhost;in_flight_window=10000;protocol_version=2;", "unknown configuration key [key=in_flight_window]");
            assertConfStrError("udp::addr=localhost;in_flight_window=10000;", "unknown configuration key [key=in_flight_window]");
            assertConfStrError("http::addr=localhost;in_flight_window=;protocol_version=2;", "unknown configuration key [key=in_flight_window]");
        });
    }

    @Test
    public void testCustomTruststoreButTlsNotEnabled() throws Exception {
        assertMemoryLeak(() -> assertThrows("TLS was not enabled",
                Sender.builder(Sender.Transport.TCP)
                        .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)
                        .address(LOCALHOST)));
    }

    @Test
    public void testCustomTruststoreDoubleSet() throws Exception {
        assertMemoryLeak(() -> assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.TCP)
                        .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)
                        .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)));
    }

    @Test
    public void testCustomTruststorePasswordCannotBeNull() {
        assertThrows("trust store password cannot be null",
                () -> Sender.builder(Sender.Transport.TCP).advancedTls().customTrustStore(TRUSTSTORE_PATH, null));
    }

    @Test
    public void testCustomTruststorePathCannotBeBlank() {
        assertThrows("trust store path cannot be empty nor null",
                () -> Sender.builder(Sender.Transport.TCP).advancedTls().customTrustStore("", TRUSTSTORE_PASSWORD));
        assertThrows("trust store path cannot be empty nor null",
                () -> Sender.builder(Sender.Transport.TCP).advancedTls().customTrustStore(null, TRUSTSTORE_PASSWORD));
    }

    @Test
    public void testDisableAutoFlushNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("auto-flush is not supported for TCP protocol",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).disableAutoFlush()));
    }

    @Test
    public void testDnsResolutionFail() throws Exception {
        assertMemoryLeak(() -> assertThrows("could not resolve",
                Sender.builder(Sender.Transport.TCP).address("this-domain-does-not-exist-i-hope-better-to-use-a-silly-tld.silly-tld")));
    }

    @Test
    public void testDuplicatedAddresses() throws Exception {
        assertMemoryLeak(() -> {
            assertThrows("duplicated addresses are not allowed [address=localhost:9000]",
                    () -> Sender.builder(Sender.Transport.TCP).address("localhost:9000").address("localhost:9000"));
            assertThrows("duplicated addresses are not allowed [address=localhost:9000]",
                    () -> Sender.builder(Sender.Transport.TCP).address("localhost:9000").address("localhost:9001").address("localhost:9000"));
        });
    }

    @Test
    public void testDuplicatedAddressesWithDifferentPortsAllowed() throws Exception {
        assertMemoryLeak(() -> Sender.builder(Sender.Transport.TCP).address("localhost:9000").address("localhost:9001"));
    }

    @Test
    public void testDuplicatedAddressesWithNoPortsAllowed() throws Exception {
        assertMemoryLeak(() -> {
            Sender.builder(Sender.Transport.TCP).address("localhost:9000").address("localhost");
            Sender.builder(Sender.Transport.TCP).address("localhost").address("localhost:9000");
        });
    }

    @Test
    public void testEgressOnlyQueryClientKeysSilentlyAcceptedOnIngress() throws Exception {
        // connect-string.md "Query client keys" and "Multi-host failover":
        // these keys configure the QwpQueryClient only. The Sender silently
        // consumes them so a single connect string is portable between Sender
        // and QwpQueryClient. Same rationale as buffer_pool_size (covered
        // separately) and zone. The Sender does not interpret the value --
        // range, enum, and type checks are the egress parser's job. A
        // malformed value at the ingress parser is still consumed.
        assertMemoryLeak(() -> {
            // Each egress-only key on its own with a representative happy-path
            // value. Covers query-client knobs and per-Execute failover knobs.
            String[] keys = {
                    "client_id=batch-job/42",
                    "compression=zstd",
                    "compression_level=5",
                    "failover=on",
                    "failover_backoff_initial_ms=50",
                    "failover_backoff_max_ms=1000",
                    "failover_max_attempts=8",
                    "failover_max_duration_ms=30000",
                    "initial_credit=1048576",
                    "max_batch_rows=10000",
                    "target=primary",
            };
            StringBuilder all = new StringBuilder("http::addr=").append(LOCALHOST).append(";protocol_version=2;");
            for (String kv : keys) {
                assertConfStrOk("http::addr=" + LOCALHOST + ";" + kv + ";protocol_version=2;");
                all.append(kv).append(';');
            }
            // All 11 keys at once -- a typical shared-config connect string.
            assertConfStrOk(all.toString());

            // Out-of-range / malformed values are silently consumed too -- the
            // ingress parser does not validate egress-only keys.
            assertConfStrOk("http::addr=" + LOCALHOST + ";compression=bogus;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";compression_level=-1;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";max_batch_rows=0;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";failover=maybe;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";target=somewhere;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";failover_max_attempts=-3;protocol_version=2;");

            // Empty values are well-formed and silently consumed.
            assertConfStrOk("http::addr=" + LOCALHOST + ";compression=;protocol_version=2;");
            assertConfStrOk("http::addr=" + LOCALHOST + ";target=;protocol_version=2;");
        });
    }

    @Test
    public void testFailFastWhenSetCustomTrustStoreTwice() {
        assertThrows("already configured",
                () -> Sender.builder(Sender.Transport.TCP)
                        .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)
                        .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD));
    }

    @Test
    public void testFirstTlsValidationDisabledThenCustomTruststore() throws Exception {
        assertMemoryLeak(() -> assertThrows("TLS validation was already disabled",
                () -> Sender.builder(Sender.Transport.TCP)
                        .advancedTls().disableCertificateValidation()
                        .advancedTls().customTrustStore(TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD)));
    }

    @Test
    public void testHostNorAddressSet() throws Exception {
        assertMemoryLeak(() -> assertThrows("server address not set",
                Sender.builder(Sender.Transport.TCP)));
    }

    @Test
    public void testHttpPathNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("HTTP path is not supported for TCP protocol",
                () -> Sender.builder(Sender.Transport.TCP).address(LOCALHOST).httpPath("/custom/path")));
    }

    @Test
    public void testHttpSettingPathNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("HTTP settings path is not supported for TCP protocol",
                () -> Sender.builder(Sender.Transport.TCP).address(LOCALHOST).httpSettingPath("/custom/settings")));
    }

    @Test
    public void testHttpTokenNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("HTTP token authentication is not supported for TCP protocol",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).httpToken("foo")));
    }

    @Test
    public void testInvalidHttpTimeout() throws Exception {
        assertMemoryLeak(() -> {
            assertThrows("HTTP timeout must be positive [timeout=0]",
                    () -> Sender.builder(Sender.Transport.HTTP).address("someurl").httpTimeoutMillis(0));
            assertThrows("HTTP timeout must be positive [timeout=-1]",
                    () -> Sender.builder(Sender.Transport.HTTP).address("someurl").httpTimeoutMillis(-1));
            assertThrows("HTTP timeout was already configured [timeout=100]",
                    () -> Sender.builder(Sender.Transport.HTTP).address("someurl").httpTimeoutMillis(100).httpTimeoutMillis(200));
            assertThrows("HTTP timeout is not supported for TCP protocol",
                    Sender.builder(Sender.Transport.TCP).address("localhost").httpTimeoutMillis(5000));
        });
    }

    @Test
    public void testInvalidRetryTimeout() {
        assertThrows("retry timeout cannot be negative [retryTimeoutMillis=-1]",
                () -> Sender.builder(Sender.Transport.HTTP).retryTimeoutMillis(-1));
        assertThrows("retry timeout was already configured [retryTimeoutMillis=100]",
                () -> Sender.builder(Sender.Transport.HTTP).retryTimeoutMillis(100).retryTimeoutMillis(200));
    }

    @Test
    public void testMalformedPortInAddress() throws Exception {
        assertMemoryLeak(() -> assertThrows("cannot parse a port from the address",
                () -> Sender.builder(Sender.Transport.TCP).address("foo:nonsense12334")));
    }

    @Test
    public void testMaxRequestBufferSizeCannotBeLessThanDefault() throws Exception {
        assertMemoryLeak(() -> assertThrows("maximum buffer capacity cannot be less than initial buffer capacity [maximumBufferCapacity=65535, initialBufferCapacity=65536]",
                () -> Sender.builder(Sender.Transport.HTTP).address("localhost:1").maxBufferCapacity(65535)));
    }

    @Test
    public void testMaxRequestBufferSizeCannotBeLessThanInitialBufferSize() throws Exception {
        assertMemoryLeak(() -> assertThrows("maximum buffer capacity cannot be less than initial buffer capacity [maximumBufferCapacity=100000, initialBufferCapacity=200000]",
                Sender.builder(Sender.Transport.HTTP).address("localhost:1").maxBufferCapacity(100_000).bufferCapacity(200_000)));
    }

    @Test
    public void testMaxRetriesNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("retrying is not supported for TCP protocol",
                () -> Sender.builder(Sender.Transport.TCP).address(LOCALHOST).retryTimeoutMillis(100)));
    }

    @Test
    public void testMinRequestThroughputCannotBeNegative() throws Exception {
        assertMemoryLeak(() -> assertThrows("minimum request throughput must not be negative [minRequestThroughput=-100]",
                () -> Sender.builder(Sender.Transport.HTTP).address(LOCALHOST).minRequestThroughput(-100)));
    }

    @Test
    public void testMinRequestThroughputNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("minimum request throughput is not supported for TCP protocol",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).minRequestThroughput(1)));
    }

    @Test
    public void testPlainAuth_connectionRefused() throws Exception {
        assertMemoryLeak(() -> assertThrows("could not connect",
                Sender.builder(Sender.Transport.TCP)
                        .enableAuth("foo").authToken(AUTH_TOKEN_KEY1).address(LOCALHOST + ":19003")));
    }

    @Test
    public void testPlainOldTokenNotSupportedForHttpProtocol() throws Exception {
        assertMemoryLeak(() -> assertThrows("old token authentication is not supported for HTTP protocol",
                Sender.builder(Sender.Transport.HTTP).address("localhost:9000").enableAuth("key").authToken(AUTH_TOKEN_KEY1)));
    }

    @Test
    public void testPlainPrivateKey_connectionRefused() throws Exception {
        // privateKey(PrivateKey) is the bring-your-own-key alternative to
        // authToken(String): the caller hands over a PrivateKey already in
        // memory (e.g. fetched from an HSM/KMS) and retains ownership of
        // its lifecycle. Verify the builder accepts it and walks all the
        // way to the connect step (which fails -- nothing listens on
        // 19003), and that the key is NOT destroyed by the builder
        // (shouldDestroyPrivKey stays false on this path).
        assertMemoryLeak(() -> {
            PrivateKey key = AuthUtils.toPrivateKey(AUTH_TOKEN_KEY1);
            try {
                assertThrows("could not connect",
                        Sender.builder(Sender.Transport.TCP)
                                .enableAuth("foo")
                                .privateKey(key)
                                .address(LOCALHOST + ":19003"));
                assertFalse("privateKey() must not destroy a caller-owned key", key.isDestroyed());
            } finally {
                try {
                    key.destroy();
                } catch (DestroyFailedException ignore) {
                    // best-effort; some PrivateKey impls reject destroy()
                }
            }
        });
    }

    @Test
    public void testPlain_connectionRefused() throws Exception {
        assertMemoryLeak(() -> assertThrows("could not connect",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST + ":19003")));
    }

    @Test
    public void testPortDoubleSet_firstAddressThenPort() throws Exception {
        assertMemoryLeak(() -> assertThrows("mismatch",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST + ":9000").port(9000)));
    }

    @Test
    public void testPortDoubleSet_firstPortThenAddress() throws Exception {
        assertMemoryLeak(() -> assertThrows("mismatch",
                Sender.builder(Sender.Transport.TCP).port(9000).address(LOCALHOST + ":9000")));
    }

    @Test
    public void testPortDoubleSet_firstPortThenPort() throws Exception {
        assertMemoryLeak(() -> assertThrows("questdb server address not set",
                Sender.builder(Sender.Transport.TCP).port(9000).port(9000)));
    }

    @Test
    public void testSmallMaxNameLen() throws Exception {
        assertMemoryLeak(() -> assertThrows("max_name_len must be at least 16 bytes [max_name_len=10]",
                () -> Sender.builder(Sender.Transport.TCP).maxNameLength(10)));
    }

    @Test
    public void testTlsDoubleSet() throws Exception {
        assertMemoryLeak(() -> assertThrows("already enabled",
                () -> Sender.builder(Sender.Transport.TCP).enableTls().enableTls()));
    }

    @Test
    public void testTlsValidationDisabledButTlsNotEnabled() throws Exception {
        assertMemoryLeak(() -> assertThrows("TLS was not enabled",
                Sender.builder(Sender.Transport.TCP)
                        .advancedTls().disableCertificateValidation()
                        .address(LOCALHOST)));
    }

    @Test
    public void testTlsValidationDisabledDoubleSet() throws Exception {
        assertMemoryLeak(() -> assertThrows("TLS validation was already disabled",
                () -> Sender.builder(Sender.Transport.TCP)
                        .advancedTls().disableCertificateValidation()
                        .advancedTls().disableCertificateValidation()));
    }

    @Test
    public void testTls_connectionRefused() throws Exception {
        assertMemoryLeak(() -> assertThrows("could not connect",
                Sender.builder(Sender.Transport.TCP).enableTls().address(LOCALHOST + ":19003")));
    }

    @Test
    public void testUsernamePasswordAuthNotSupportedForTcp() throws Exception {
        assertMemoryLeak(() -> assertThrows("username/password authentication is not supported for TCP protocol",
                Sender.builder(Sender.Transport.TCP).address(LOCALHOST).httpUsernamePassword("foo", "bar")));
    }

    @Test
    public void testZoneSilentlyAcceptedOnIngress() throws Exception {
        // failover.md §1.1 / changelog 2026-05-08: zone= is egress-only effective.
        // Ingress is zone-blind (pinned to v1) and silently accepts the key so
        // the same connect string can be shared across ingress and egress
        // clients without a per-startup WARN that would fire spuriously for a
        // setting already working on its egress siblings. Exercise every
        // ingress protocol -- HTTP/HTTPS, WebSocket, TCP, UDP -- and confirm
        // the parser does not raise on the key. A connect-time failure is
        // tolerated (these are unreachable test addresses) as long as the
        // exception message does not point at zone= as the offender.
        assertMemoryLeak(() -> {
            // protocol_version is pinned on HTTP/HTTPS so fromConfig does not
            // contact the server for auto-detection.
            assertConfStrOk("http::addr=" + LOCALHOST + ";zone=eu-west-1a;protocol_version=2;");
            assertConfStrOk("https::addr=" + LOCALHOST + ";zone=eu-west-1a;tls_verify=unsafe_off;protocol_version=2;");
            // UDP build is purely local; no I/O.
            assertConfStrOk("udp::addr=" + LOCALHOST + ";zone=eu-west-1a;");
            // WebSocket and TCP attempt synchronous connect at build time.
            // Allow LineSenderException for connect failure but require the
            // parser did not flag zone=.
            assertConfStrAcceptsZoneKey("ws::addr=127.0.0.1:1;zone=eu-west-1a;");
            assertConfStrAcceptsZoneKey("tcp::addr=127.0.0.1:1;zone=eu-west-1a;");
            // Empty zone value is also accepted: an empty value after `=`
            // is well-formed and the zone branch silently consumes it.
            assertConfStrOk("http::addr=" + LOCALHOST + ";zone=;protocol_version=2;");
            // Mixed-case zone identifier is accepted verbatim by the ingress
            // parser; case-insensitivity is the egress comparator's
            // responsibility, not the parser's.
            assertConfStrOk("http::addr=" + LOCALHOST + ";zone=EU-West-1a;protocol_version=2;");
        });
    }

    private static void assertConfStrAcceptsBufferPoolSizeKey(String conf) {
        try {
            Sender.fromConfig(conf).close();
        } catch (LineSenderException e) {
            if (e.getMessage().contains("buffer_pool_size")) {
                fail("parser must not flag buffer_pool_size=, was: " + e.getMessage());
            }
        }
    }

    private static void assertConfStrAcceptsZoneKey(String conf) {
        try {
            Sender.fromConfig(conf).close();
        } catch (LineSenderException e) {
            if (e.getMessage().contains("zone")) {
                fail("parser must not flag zone=, was: " + e.getMessage());
            }
        }
    }

    private static void assertConfStrError(String conf, String expectedError) {
        try {
            try (Sender ignored = Sender.fromConfig(conf)) {
                fail("should fail with bad conf string");
            }
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), expectedError);
        }
    }

    private static void assertConfStrOk(String... params) {
        StringBuilder sb = new StringBuilder();
        sb.append("http").append("::");
        shuffle(params);
        for (int i = 0; i < params.length; i++) {
            sb.append(params[i]).append(";");
        }
        assertConfStrOk(sb.toString());
    }

    private static void assertConfStrOk(String conf) {
        Sender.fromConfig(conf).close();
    }

    private static void assertThrows(String expectedSubstring, Sender.LineSenderBuilder builder) {
        assertThrows(expectedSubstring, builder::build);
    }

    private static void assertThrows(String expectedSubstring, Runnable action) {
        try {
            action.run();
            fail("Expected LineSenderException containing '" + expectedSubstring + "'");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), expectedSubstring);
        }
    }

    private static void shuffle(String[] input) {
        for (int i = 0; i < input.length; i++) {
            int j = (int) (Math.random() * input.length);
            String tmp = input[i];
            input[i] = input[j];
            input[j] = tmp;
        }
    }
}
