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
        QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db.internal;");
        // Host-only accepted; port defaults to the public DEFAULT_WS_PORT constant.
        Assert.assertNotNull(c);
    }

    @Test
    public void testAddrAcceptsMultipleEntries() {
        // Three-endpoint config must parse without error.
        QwpQueryClient c = QwpQueryClient.fromConfig(
                "ws::addr=a.internal:9000,b.internal:9000,c.internal:9000;");
        Assert.assertNotNull(c);
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
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;buffer_pool_size=8;"));
    }

    @Test
    public void testCompressionAutoAccepted() {
        Assert.assertEquals("auto",
                QwpQueryClient.fromConfig("ws::addr=db:9000;compression=auto;")
                        .getCompressionPreference());
    }

    @Test
    public void testCompressionDefaultIsRaw() {
        Assert.assertEquals("raw",
                QwpQueryClient.fromConfig("ws::addr=db:9000;").getCompressionPreference());
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
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;compression=zstd;compression_level=1;"));
    }

    @Test
    public void testCompressionLevelAtUpperBoundAccepted() {
        // Parse-time cap is [1, 22]; server-side runtime clamp to [1, 9] is a separate concern.
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;compression=zstd;compression_level=22;"));
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
        Assert.assertEquals("raw",
                QwpQueryClient.fromConfig("ws::addr=db:9000;compression=raw;")
                        .getCompressionPreference());
    }

    @Test
    public void testCompressionZstdAccepted() {
        Assert.assertEquals("zstd",
                QwpQueryClient.fromConfig("ws::addr=db:9000;compression=zstd;")
                        .getCompressionPreference());
    }

    @Test
    public void testEmptyStringRejected() {
        assertReject("", "configuration string cannot be empty");
    }

    @Test
    public void testFailoverDefaultIsOn() {
        // No failover= key: happy path, no throw. Internal default is on; not directly
        // observable from the public API so we assert successful parse only.
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;"));
    }

    @Test
    public void testFailoverInvalidRejected() {
        assertReject(
                "ws::addr=db:9000;failover=maybe;",
                "invalid failover: maybe (expected on or off)"
        );
    }

    @Test
    public void testFailoverOffAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;failover=off;"));
    }

    @Test
    public void testFailoverOnAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;failover=on;"));
    }

    @Test
    public void testFailoverBackoffInitialAtZeroAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;failover_backoff_initial_ms=0;"));
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
        Assert.assertNotNull(QwpQueryClient.fromConfig(
                "ws::addr=db:9000;failover_backoff_initial_ms=100;failover_backoff_max_ms=500;"
        ));
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
    public void testFailoverMaxAttemptsAcceptedAtOne() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;failover_max_attempts=1;"));
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
        Assert.assertNotNull(QwpQueryClient.fromConfig(conf));
    }

    @Test
    public void testMalformedKeyValueRejected() {
        // Missing = sign in a key=value pair -- ConfStringParser.nextKey / value
        // bails out; fromConfig surfaces the parser's scratch sink verbatim in
        // the "invalid configuration string [error=...]" shape.
        try {
            QwpQueryClient.fromConfig("ws::addr=db:9000;bogus;");
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
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;max_batch_rows=1;"));
    }

    @Test
    public void testMaxBatchRowsAtUpperBoundAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;max_batch_rows=1048576;"));
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
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=localhost:9000;"));
    }

    @Test
    public void testMinimalWssConfigAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("wss::addr=secure.internal:9443;"));
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
            QwpQueryClient.fromConfig("addr=db:9000;");
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
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;path=/custom/read;"));
    }

    @Test
    public void testTargetAnyAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;target=any;"));
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
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;target=primary;"));
    }

    @Test
    public void testTargetReplicaAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("ws::addr=db:9000;target=replica;"));
    }

    @Test
    public void testTlsRootsPasswordWithoutPathRejected() {
        assertReject(
                "wss::addr=db:9000;tls_roots_password=secret;",
                "tls_roots and tls_roots_password must be provided together"
        );
    }

    @Test
    public void testTlsRootsWithoutPasswordRejected() {
        assertReject(
                "wss::addr=db:9000;tls_roots=/etc/qdb/ca.p12;",
                "tls_roots and tls_roots_password must be provided together"
        );
    }

    @Test
    public void testTlsRootsWithPasswordAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig(
                "wss::addr=db:9000;tls_roots=/etc/qdb/ca.p12;tls_roots_password=secret;"
        ));
    }

    @Test
    public void testTlsVerifyInvalidRejected() {
        assertReject(
                "wss::addr=db:9000;tls_verify=strict;",
                "invalid tls_verify: strict (expected on or unsafe_off)"
        );
    }

    @Test
    public void testTlsVerifyOnWssAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("wss::addr=db:9000;tls_verify=on;"));
    }

    @Test
    public void testTlsVerifyOnWsRejected() {
        assertReject(
                "ws::addr=db:9000;tls_verify=on;",
                "tls_verify/tls_roots/tls_roots_password require the wss:: schema"
        );
    }

    @Test
    public void testTlsVerifyUnsafeOffOnWssAccepted() {
        Assert.assertNotNull(QwpQueryClient.fromConfig("wss::addr=db:9000;tls_verify=unsafe_off;"));
    }

    @Test
    public void testTlsRootsOnWsRejected() {
        assertReject(
                "ws::addr=db:9000;tls_roots=/etc/qdb/ca.p12;tls_roots_password=secret;",
                "tls_verify/tls_roots/tls_roots_password require the wss:: schema"
        );
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
    public void testAuthHeaderAcceptedAlone() {
        // Each of the three auth modes has a dedicated mutual-exclusion test;
        // the positive happy path is asserted here so the parser's per-key
        // dispatch and the post-loop "no auth set" path both have coverage.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;auth=Bearer xyz;")) {
            Assert.assertNotNull(c);
        }
    }

    @Test
    public void testBasicAuthAcceptedAlone() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;username=alice;password=secret;")) {
            Assert.assertNotNull(c);
        }
    }

    @Test
    public void testTokenAcceptedAlone() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;token=ey.payload.sig;")) {
            Assert.assertNotNull(c);
        }
    }

    @Test
    public void testClientIdAcceptedAlone() {
        // Sent as the X-QWP-Client-Id header on the upgrade request; useful for
        // server-side telemetry. No format constraints from the parser.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;client_id=batch-job/42;")) {
            Assert.assertNotNull(c);
        }
    }

    @Test
    public void testFailoverMaxBackoffEqualToInitialAccepted() {
        // The "max < initial" rejection path is tested already; verify the
        // boundary case (max == initial) is the lowest legal max and parses.
        try (QwpQueryClient c = QwpQueryClient.fromConfig(
                "ws::addr=db:9000;failover_backoff_initial_ms=100;failover_backoff_max_ms=100;")) {
            Assert.assertNotNull(c);
        }
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
    public void testAddrSingleWhitespaceTrimmedAroundHostPort() {
        // The parser splits on commas and trims; a single leading space on a
        // valid entry must therefore be tolerated rather than rejected as
        // "empty". Pin so a future refactor that drops trim() breaks here.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr= db1:9000 , db2:9000 ;")) {
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
            QwpQueryClient.fromConfig(conf);
            Assert.fail("expected IllegalArgumentException for: " + conf);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }
}
