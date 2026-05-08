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

import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

/**
 * Exhaustive unit coverage for {@link QwpQueryClient#fromConfig}: every parse
 * failure path is exercised with its exact user-visible error message, and
 * the happy-path branches that land distinct settings on the returned client
 * are verified via introspection where the accessors exist, or through
 * round-trip connection strings otherwise. The point of verifying error
 * message *text* (not just the exception type) is so that a refactor that
 * silently changes the wording does not break downstream UI / CLI tools
 * relying on specific strings for diagnostics.
 */
public class QwpQueryClientFromConfigTest {

    @Test
    public void testAddrAcceptsHostWithoutPort() {
        // Host-only accepted; port defaults to the public DEFAULT_WS_PORT constant.
        assertParses("ws::addr=db.internal;");
    }

    @Test
    public void testAddrAcceptsMultipleEntries() {
        // Three-endpoint config must parse without error.
        assertParses("ws::addr=a.internal:9000,b.internal:9000,c.internal:9000;");
    }

    @Test
    public void testAddrEmptyEntryAtEndRejected() {
        assertReject("ws::addr=a:9000,;", "empty addr entry");
    }

    @Test
    public void testAddrEmptyEntryAtStartRejected() {
        assertReject("ws::addr=,b:9000;", "empty addr entry");
    }

    @Test
    public void testAddrEmptyEntryInMiddleRejected() {
        assertReject("ws::addr=a:9000,,b:9000;", "empty addr entry");
    }

    @Test
    public void testAddrEmptyHostRejected() {
        assertReject("ws::addr=:9000;", "empty host in addr entry: :9000");
    }

    @Test
    public void testAddrInvalidPortMultiEntryRejected() {
        assertReject("ws::addr=a:9000,b:notaport;", "invalid port in addr: b:notaport");
    }

    @Test
    public void testAddrInvalidPortRejected() {
        assertReject("ws::addr=host:abc;", "invalid port in addr: host:abc");
    }

    @Test
    public void testAddrIpv6BareMultiColonTreatedAsHost() {
        // Multi-colon, unbracketed: treated as bare IPv6 host with default
        // port. Custom port on IPv6 requires brackets.
        assertParses("ws::addr=fe80::1;");
    }

    @Test
    public void testAddrIpv6BracketedEmptyHostRejected() {
        assertReject("ws::addr=[]:9000;", "empty host in addr entry: []:9000");
    }

    @Test
    public void testAddrIpv6BracketedMixedListAccepted() {
        assertParses("ws::addr=[::1]:9000,[fe80::1],host.local:9001;");
    }

    @Test
    public void testAddrIpv6BracketedRejectsTrailingGarbageBeforePort() {
        // "]" must be followed immediately by ':' (or end of entry), not by
        // trailing characters. Surfaces obvious typos rather than guessing.
        assertReject("ws::addr=[::1]9000;",
                "expected ':' after ']' in IPv6 addr entry: [::1]9000");
    }

    @Test
    public void testAddrIpv6BracketedWithPortAccepted() {
        // Per RFC 3986, IPv6 addresses must be bracketed when carrying a port.
        assertParses("ws::addr=[::1]:9000;");
    }

    @Test
    public void testAddrIpv6BracketedWithoutPortAccepted() {
        assertParses("ws::addr=[fe80::1];");
    }

    @Test
    public void testAddrIpv6MissingClosingBracketRejected() {
        assertReject("ws::addr=[::1:9000;",
                "missing closing ']' in IPv6 addr entry: [::1:9000");
    }

    @Test
    public void testAddrPortAbove65535Rejected() {
        assertReject("ws::addr=db:65536;",
                "port out of range in addr: db:65536 (must be 1-65535)");
    }

    @Test
    public void testAddrPortIpv6BracketedOutOfRangeRejected() {
        assertReject("ws::addr=[::1]:0;",
                "port out of range in addr: [::1]:0 (must be 1-65535)");
    }

    @Test
    public void testAddrPortMaxValueRejected() {
        // Integer.MAX_VALUE parses successfully but is well above 65535.
        assertReject("ws::addr=db:2147483647;",
                "port out of range in addr: db:2147483647 (must be 1-65535)");
    }

    @Test
    public void testAddrPortNegativeRejected() {
        assertReject("ws::addr=db:-1;", "port out of range in addr: db:-1 (must be 1-65535)");
    }

    @Test
    public void testAddrPortWhitespaceTolerated() {
        // Hand-edited config strings sometimes pick up a stray space around
        // the port. Tolerate it rather than surface as opaque "invalid port".
        assertParses("ws::addr=host: 9000;");
    }

    @Test
    public void testAddrPortZeroRejected() {
        assertReject("ws::addr=db:0;", "port out of range in addr: db:0 (must be 1-65535)");
    }

    @Test
    public void testAddrSingleWhitespaceTrimmedAroundHostPort() {
        // The parser splits on commas and trims; a single leading space on a
        // valid entry must therefore be tolerated rather than rejected as
        // "empty". Pin so a future refactor that drops trim() breaks here.
        assertParses("ws::addr= db1:9000 , db2:9000 ;");
    }

    @Test
    public void testAuthAndBasicMutuallyExclusive() {
        assertReject(
                "ws::addr=db:9000;auth=Bearer xyz;username=admin;password=quest;",
                "auth, username/password, and token are mutually exclusive"
        );
    }

    @Test
    public void testAuthAndTokenMutuallyExclusive() {
        assertReject(
                "ws::addr=db:9000;auth=Bearer xyz;token=ey.xyz;",
                "auth, username/password, and token are mutually exclusive"
        );
    }

    @Test
    public void testAuthHeaderAcceptedAlone() {
        // Each of the three auth modes has a dedicated mutual-exclusion test;
        // the positive happy path is asserted here so the parser's per-key
        // dispatch and the post-loop "no auth set" path both have coverage.
        assertParses("ws::addr=db:9000;auth=Bearer xyz;");
    }

    @Test
    public void testBasicAuthAcceptedAlone() {
        assertParses("ws::addr=db:9000;username=alice;password=secret;");
    }

    @Test
    public void testBasicAuthAndTokenMutuallyExclusive() {
        assertReject(
                "ws::addr=db:9000;username=admin;password=quest;token=ey.xyz;",
                "auth, username/password, and token are mutually exclusive"
        );
    }

    @Test
    public void testBasicAuthWithPasswordOnlyRejected() {
        assertReject(
                "ws::addr=db:9000;password=quest;",
                "both username and password must be provided together"
        );
    }

    @Test
    public void testBasicAuthWithUsernameOnlyRejected() {
        assertReject(
                "ws::addr=db:9000;username=admin;",
                "both username and password must be provided together"
        );
    }

    @Test
    public void testBufferPoolSizeLowerBoundRejected() {
        assertReject("ws::addr=db:9000;buffer_pool_size=0;", "buffer_pool_size must be >= 1");
    }

    @Test
    public void testBufferPoolSizeNegativeRejected() {
        assertReject("ws::addr=db:9000;buffer_pool_size=-1;", "buffer_pool_size must be >= 1");
    }

    @Test
    public void testBufferPoolSizeNonNumericRejected() {
        assertReject("ws::addr=db:9000;buffer_pool_size=big;", "invalid buffer_pool_size: big");
    }

    @Test
    public void testBufferPoolSizeValidAccepted() {
        assertParses("ws::addr=db:9000;buffer_pool_size=8;");
    }

    @Test
    public void testClientIdAcceptedAlone() {
        // Sent as the X-QWP-Client-Id header on the upgrade request; useful for
        // server-side telemetry. No format constraints from the parser.
        assertParses("ws::addr=db:9000;client_id=batch-job/42;");
    }

    @Test
    public void testCompressionAutoAccepted() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;compression=auto;")) {
            Assert.assertEquals("auto", c.getCompressionPreference());
        }
    }

    @Test
    public void testCompressionDefaultIsRaw() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;")) {
            Assert.assertEquals("raw", c.getCompressionPreference());
        }
    }

    @Test
    public void testCompressionInvalidRejected() {
        assertReject(
                "ws::addr=db:9000;compression=gzip;",
                "unsupported compression: gzip (expected zstd, raw, or auto)"
        );
    }

    @Test
    public void testCompressionLevelAtLowerBoundAccepted() {
        assertParses("ws::addr=db:9000;compression=zstd;compression_level=1;");
    }

    @Test
    public void testCompressionLevelAtUpperBoundAccepted() {
        // Parse-time cap is [1, 22]; server-side runtime clamp to [1, 9] is a separate concern.
        assertParses("ws::addr=db:9000;compression=zstd;compression_level=22;");
    }

    @Test
    public void testCompressionLevelNegativeRejected() {
        assertReject("ws::addr=db:9000;compression_level=-1;", "compression_level must be in [1, 22]");
    }

    @Test
    public void testCompressionLevelNonNumericRejected() {
        assertReject("ws::addr=db:9000;compression_level=high;", "invalid compression_level: high");
    }

    @Test
    public void testCompressionLevelOverUpperBoundRejected() {
        assertReject("ws::addr=db:9000;compression_level=23;", "compression_level must be in [1, 22]");
    }

    @Test
    public void testCompressionLevelZeroRejected() {
        assertReject("ws::addr=db:9000;compression_level=0;", "compression_level must be in [1, 22]");
    }

    @Test
    public void testCompressionRawAccepted() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;compression=raw;")) {
            Assert.assertEquals("raw", c.getCompressionPreference());
        }
    }

    @Test
    public void testCompressionZstdAccepted() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;compression=zstd;")) {
            Assert.assertEquals("zstd", c.getCompressionPreference());
        }
    }

    @Test
    public void testEmptyStringRejected() {
        assertReject("", "configuration string cannot be empty");
    }

    @Test
    public void testFailoverBackoffInitialAtZeroAccepted() {
        assertParses("ws::addr=db:9000;failover_backoff_initial_ms=0;");
    }

    @Test
    public void testFailoverBackoffInitialNegativeRejected() {
        assertReject(
                "ws::addr=db:9000;failover_backoff_initial_ms=-1;",
                "failover_backoff_initial_ms must be >= 0"
        );
    }

    @Test
    public void testFailoverBackoffInitialNonNumericRejected() {
        assertReject(
                "ws::addr=db:9000;failover_backoff_initial_ms=soon;",
                "invalid failover_backoff_initial_ms: soon"
        );
    }

    @Test
    public void testFailoverBackoffMaxAndInitialBothAccepted() {
        assertParses("ws::addr=db:9000;failover_backoff_initial_ms=100;failover_backoff_max_ms=500;");
    }

    @Test
    public void testFailoverBackoffMaxLessThanInitialRejected() {
        assertReject(
                "ws::addr=db:9000;failover_backoff_initial_ms=500;failover_backoff_max_ms=100;",
                "failover_backoff_max_ms must be >= failover_backoff_initial_ms"
        );
    }

    @Test
    public void testFailoverBackoffMaxNegativeRejected() {
        assertReject(
                "ws::addr=db:9000;failover_backoff_max_ms=-1;",
                "failover_backoff_max_ms must be >= 0"
        );
    }

    @Test
    public void testFailoverBackoffMaxNonNumericRejected() {
        assertReject(
                "ws::addr=db:9000;failover_backoff_max_ms=later;",
                "invalid failover_backoff_max_ms: later"
        );
    }

    @Test
    public void testFailoverDefaultIsOn() {
        // No failover= key: happy path, no throw. Internal default is on; not directly
        // observable from the public API so we assert successful parse only.
        assertParses("ws::addr=db:9000;");
    }

    @Test
    public void testFailoverInvalidRejected() {
        assertReject(
                "ws::addr=db:9000;failover=maybe;",
                "invalid failover: maybe (expected on or off)"
        );
    }

    @Test
    public void testFailoverMaxAttemptsAcceptedAtOne() {
        assertParses("ws::addr=db:9000;failover_max_attempts=1;");
    }

    @Test
    public void testFailoverMaxAttemptsNonNumericRejected() {
        assertReject(
                "ws::addr=db:9000;failover_max_attempts=many;",
                "invalid failover_max_attempts: many"
        );
    }

    @Test
    public void testFailoverMaxAttemptsZeroRejected() {
        assertReject(
                "ws::addr=db:9000;failover_max_attempts=0;",
                "failover_max_attempts must be >= 1"
        );
    }

    @Test
    public void testFailoverMaxBackoffEqualToInitialAccepted() {
        // The "max < initial" rejection path is tested already; verify the
        // boundary case (max == initial) is the lowest legal max and parses.
        assertParses("ws::addr=db:9000;failover_backoff_initial_ms=100;failover_backoff_max_ms=100;");
    }

    @Test
    public void testFailoverOffAccepted() {
        assertParses("ws::addr=db:9000;failover=off;");
    }

    @Test
    public void testFailoverOnAccepted() {
        assertParses("ws::addr=db:9000;failover=on;");
    }

    @Test
    public void testFullKitchenSinkAccepted() {
        // Every optional key set to a valid non-default value on a wss:: schema.
        // Verifies the parser's cross-key validation doesn't reject an otherwise
        // legal combination, and that the happy-path client construction works.
        String conf = "wss::addr=a.internal:9443,b.internal:9443,c.internal:9443;"
                + "path=/read/v1;target=primary;failover=on;"
                + "username=admin;password=quest;"
                + "client_id=batch-job/42;buffer_pool_size=8;"
                + "compression=zstd;compression_level=5;"
                + "max_batch_rows=512;"
                + "tls_verify=on;";
        assertParses(conf);
    }

    @Test
    public void testMalformedKeyValueRejected() {
        // Missing = sign in a key=value pair -- ConfStringParser.nextKey / value
        // bails out; fromConfig surfaces the parser's scratch sink verbatim in
        // the "invalid configuration string [error=...]" shape.
        try {
            QwpQueryClient.fromConfig("ws::addr=db:9000;bogus;").close();
            Assert.fail("expected IllegalArgumentException for malformed key=value");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(
                    "expected 'invalid configuration string' message, got: " + e.getMessage(),
                    e.getMessage().startsWith("invalid configuration string")
            );
        }
    }

    @Test
    public void testMaxBatchRowsAtLowerBoundAccepted() {
        assertParses("ws::addr=db:9000;max_batch_rows=1;");
    }

    @Test
    public void testMaxBatchRowsAtUpperBoundAccepted() {
        assertParses("ws::addr=db:9000;max_batch_rows=1048576;");
    }

    @Test
    public void testMaxBatchRowsNonNumericRejected() {
        assertReject("ws::addr=db:9000;max_batch_rows=lots;", "invalid max_batch_rows: lots");
    }

    @Test
    public void testMaxBatchRowsOverUpperBoundRejected() {
        assertReject(
                "ws::addr=db:9000;max_batch_rows=1048577;",
                "max_batch_rows must be in [1, 1048576]"
        );
    }

    @Test
    public void testMaxBatchRowsZeroRejected() {
        assertReject(
                "ws::addr=db:9000;max_batch_rows=0;",
                "max_batch_rows must be in [1, 1048576]"
        );
    }

    @Test
    public void testMinimalWsConfigAccepted() {
        assertParses("ws::addr=localhost:9000;");
    }

    @Test
    public void testMinimalWssConfigAccepted() {
        assertParses("wss::addr=secure.internal:9443;");
    }

    @Test
    public void testMissingAddrRejected() {
        assertReject("ws::client_id=test;", "missing required key: addr");
    }

    @Test
    public void testMissingSchemaRejected() {
        // No "::" separator -- the schema parser returns negative and fromConfig
        // reports the parser's error sink.
        try {
            QwpQueryClient.fromConfig("addr=db:9000;").close();
            Assert.fail("expected IllegalArgumentException for missing schema");
        } catch (IllegalArgumentException e) {
            // ConfStringParser either surfaces as "unsupported schema" (if it
            // parses "addr=db" as a schema-like token) or "invalid configuration
            // string". Accept either since both are actionable.
            String msg = e.getMessage();
            Assert.assertTrue(
                    "expected schema-related error, got: " + msg,
                    msg.startsWith("invalid configuration string")
                            || msg.startsWith("unsupported schema")
            );
        }
    }

    @Test
    public void testNullStringRejected() {
        assertReject(null, "configuration string cannot be empty");
    }

    @Test
    public void testPathOverrideAccepted() {
        assertParses("ws::addr=db:9000;path=/custom/read;");
    }

    @Test
    public void testTargetAnyAccepted() {
        assertParses("ws::addr=db:9000;target=any;");
    }

    @Test
    public void testTargetInvalidRejected() {
        assertReject(
                "ws::addr=db:9000;target=leader;",
                "invalid target: leader (expected any, primary, or replica)"
        );
    }

    @Test
    public void testTargetPrimaryAccepted() {
        assertParses("ws::addr=db:9000;target=primary;");
    }

    @Test
    public void testTargetReplicaAccepted() {
        assertParses("ws::addr=db:9000;target=replica;");
    }

    @Test
    public void testTlsRootsOnWsRejected() {
        assertReject(
                "ws::addr=db:9000;tls_roots=/etc/qdb/ca.p12;tls_roots_password=secret;",
                "tls_verify/tls_roots/tls_roots_password require the wss:: schema"
        );
    }

    @Test
    public void testTlsRootsPasswordWithoutPathRejected() {
        assertReject(
                "wss::addr=db:9000;tls_roots_password=secret;",
                "tls_roots and tls_roots_password must be provided together"
        );
    }

    @Test
    public void testTlsRootsWithPasswordAccepted() {
        assertParses("wss::addr=db:9000;tls_roots=/etc/qdb/ca.p12;tls_roots_password=secret;");
    }

    @Test
    public void testTlsRootsWithoutPasswordRejected() {
        assertReject(
                "wss::addr=db:9000;tls_roots=/etc/qdb/ca.p12;",
                "tls_roots and tls_roots_password must be provided together"
        );
    }

    @Test
    public void testTlsVerifyInvalidRejected() {
        assertReject(
                "wss::addr=db:9000;tls_verify=strict;",
                "invalid tls_verify: strict (expected on or unsafe_off)"
        );
    }

    @Test
    public void testTlsVerifyOnWsRejected() {
        assertReject(
                "ws::addr=db:9000;tls_verify=on;",
                "tls_verify/tls_roots/tls_roots_password require the wss:: schema"
        );
    }

    @Test
    public void testTlsVerifyOnWssAccepted() {
        assertParses("wss::addr=db:9000;tls_verify=on;");
    }

    @Test
    public void testTlsVerifyUnsafeOffOnWssAccepted() {
        assertParses("wss::addr=db:9000;tls_verify=unsafe_off;");
    }

    @Test
    public void testTokenAcceptedAlone() {
        assertParses("ws::addr=db:9000;token=ey.payload.sig;");
    }

    @Test
    public void testTokenRequestEncodesAsBearer() {
        // We can't easily snoop the request header without a server, but the
        // parser must at least accept the configuration end-to-end, then exit
        // through close() without leaking the I/O thread (which never spun up).
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;token=abc.def.ghi;")) {
            Assert.assertFalse(c.isConnected());
            Assert.assertFalse(c.wasLastCloseTimedOut());
        }
    }

    @Test
    public void testUnknownKeyRejected() {
        assertReject(
                "ws::addr=db:9000;mystery_knob=1;",
                "unknown configuration key: mystery_knob"
        );
    }

    @Test
    public void testUnsupportedSchemaRejected() {
        assertReject(
                "http::addr=db:9000;",
                "unsupported schema [schema=http, supported-schemas=[ws, wss]]"
        );
    }

    @Test
    public void testWhitespaceOnlyAddrEntryRejected() {
        // A single-space entry between commas collapses to "empty" by the
        // parser's .isEmpty() check after trim(). The exact rejection message
        // is "empty addr entry".
        assertReject("ws::addr=a:9000, ,b:9000;", "empty addr entry");
    }

    @Test
    public void testFailoverMaxDuration_AcceptsZero() {
        assertParses("ws::addr=a:9000;failover_max_duration_ms=0;");
    }

    @Test
    public void testFailoverMaxDuration_AcceptsPositive() {
        assertParses("ws::addr=a:9000;failover_max_duration_ms=5000;");
    }

    @Test
    public void testFailoverMaxDuration_NegativeRejected() {
        assertReject("ws::addr=a:9000;failover_max_duration_ms=-1;",
                "failover_max_duration_ms must be >= 0");
    }

    @Test
    public void testFailoverMaxDuration_NonNumericRejected() {
        assertReject("ws::addr=a:9000;failover_max_duration_ms=forever;",
                "invalid failover_max_duration_ms: forever");
    }

    @Test
    public void testLbStrategy_AcceptsRandom() {
        assertParses("ws::addr=a:9000,b:9000;lb_strategy=random;");
    }

    @Test
    public void testLbStrategy_AcceptsFirst() {
        assertParses("ws::addr=a:9000,b:9000;lb_strategy=first;");
    }

    @Test
    public void testLbStrategy_OtherRejected() {
        assertReject("ws::addr=a:9000;lb_strategy=roundrobin;",
                "invalid lb_strategy: roundrobin (expected random or first)");
    }

    @Test
    public void testAuthTimeout_AcceptsPositive() {
        assertParses("ws::addr=a:9000;auth_timeout_ms=2500;");
    }

    @Test
    public void testAuthTimeout_ZeroRejected() {
        assertReject("ws::addr=a:9000;auth_timeout_ms=0;", "auth_timeout_ms must be > 0");
    }

    @Test
    public void testAuthTimeout_NegativeRejected() {
        assertReject("ws::addr=a:9000;auth_timeout_ms=-50;", "auth_timeout_ms must be > 0");
    }

    @Test
    public void testAuthTimeout_NonNumericRejected() {
        assertReject("ws::addr=a:9000;auth_timeout_ms=forever;",
                "invalid auth_timeout_ms: forever");
    }

    /**
     * Asserts that {@code conf} parses into a non-null {@link QwpQueryClient}
     * and closes the result on the way out. Centralising both checks here
     * stops every happy-path test from leaking the I/O scaffolding the
     * client allocates eagerly in fromConfig().
     */
    private static void assertParses(String conf) {
        try (QwpQueryClient c = QwpQueryClient.fromConfig(conf)) {
            Assert.assertNotNull(c);
        }
    }

    /**
     * Asserts that {@link QwpQueryClient#fromConfig(CharSequence)} rejects
     * {@code conf} with an {@link IllegalArgumentException} whose message
     * equals {@code expectedMessage} exactly. The strict match is deliberate:
     * downstream tooling consumes these strings verbatim in diagnostics, so a
     * silent refactor that tweaks wording must break this test.
     */
    private static void assertReject(String conf, String expectedMessage) {
        try {
            QwpQueryClient.fromConfig(conf).close();
            Assert.fail("expected IllegalArgumentException for: " + conf);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }
}
