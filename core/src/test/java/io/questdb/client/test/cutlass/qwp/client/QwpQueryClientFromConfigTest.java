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
    public void testAddrRepeatedKeysAccumulate() {
        // failover.md section 1: repeated addr= keys must accumulate into a
        // single ordered list, mirroring the ingress Sender behavior.
        try (QwpQueryClient c = QwpQueryClient.fromConfig(
                "ws::addr=alpha:9000;addr=bravo:9001;addr=charlie:9002;")) {
            Assert.assertEquals(3, c.getEndpointCountForTest());
            Assert.assertEquals("alpha", c.getEndpointHostForTest(0));
            Assert.assertEquals(9000, c.getEndpointPortForTest(0));
            Assert.assertEquals("bravo", c.getEndpointHostForTest(1));
            Assert.assertEquals(9001, c.getEndpointPortForTest(1));
            Assert.assertEquals("charlie", c.getEndpointHostForTest(2));
            Assert.assertEquals(9002, c.getEndpointPortForTest(2));
        }
    }

    @Test
    public void testAddrRepeatedKeysAndCommasMixed() {
        // The two accumulation forms must compose: comma-list followed by a
        // second addr= key extends the list, not replaces it.
        try (QwpQueryClient c = QwpQueryClient.fromConfig(
                "ws::addr=alpha:9000,bravo:9001;addr=charlie:9002,delta:9003;")) {
            Assert.assertEquals(4, c.getEndpointCountForTest());
            Assert.assertEquals("alpha", c.getEndpointHostForTest(0));
            Assert.assertEquals(9000, c.getEndpointPortForTest(0));
            Assert.assertEquals("bravo", c.getEndpointHostForTest(1));
            Assert.assertEquals(9001, c.getEndpointPortForTest(1));
            Assert.assertEquals("charlie", c.getEndpointHostForTest(2));
            Assert.assertEquals(9002, c.getEndpointPortForTest(2));
            Assert.assertEquals("delta", c.getEndpointHostForTest(3));
            Assert.assertEquals(9003, c.getEndpointPortForTest(3));
        }
    }

    @Test
    public void testAddrRepeatedKeysEmptyEntryStillRejected() {
        // The empty-entry rejection must fire on every addr= occurrence, not
        // just the first one, so a second key cannot smuggle in a malformed
        // value.
        assertReject("ws::addr=a:9000;addr=b:9000,;", "empty addr entry");
    }

    @Test
    public void testAddrSingleWhitespaceTrimmedAroundHostPort() {
        // The parser splits on commas and trims; a single leading space on a
        // valid entry must therefore be tolerated rather than rejected as
        // "empty". Pin so a future refactor that drops trim() breaks here.
        assertParses("ws::addr= db1:9000 , db2:9000 ;");
    }

    @Test
    public void testAuthKeyRejected() {
        // The raw auth= header key is removed. Credentials are supplied as
        // token= or username=/password=, from which the client synthesizes the
        // Authorization header downstream.
        assertReject("ws::addr=db:9000;auth=Bearer xyz;", "unknown configuration key: auth");
    }

    @Test
    public void testAuthTimeoutMsAccepted() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;auth_timeout_ms=2500;")) {
            Assert.assertEquals(2500, c.getAuthTimeoutMsForTest());
        }
    }

    @Test
    public void testAuthTimeoutMsDefaultIs15Seconds() {
        // failover.md §1.1: default is 15_000.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;")) {
            Assert.assertEquals(15_000, c.getAuthTimeoutMsForTest());
        }
    }

    @Test
    public void testAuthTimeoutMsNonIntegerRejected() {
        assertReject("ws::addr=db:9000;auth_timeout_ms=fast;", "invalid auth_timeout_ms: fast");
    }

    @Test
    public void testAuthTimeoutMsZeroRejected() {
        assertReject("ws::addr=db:9000;auth_timeout_ms=0;", "auth_timeout_ms must be > 0");
    }

    @Test
    public void testAuthTimeout_AcceptsPositive() {
        assertParses("ws::addr=a:9000;auth_timeout_ms=2500;");
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

    @Test
    public void testAuthTimeout_ZeroRejected() {
        assertReject("ws::addr=a:9000;auth_timeout_ms=0;", "auth_timeout_ms must be > 0");
    }

    @Test
    public void testBasicAuthAcceptedAlone() {
        assertParses("ws::addr=db:9000;username=alice;password=secret;");
    }

    @Test
    public void testBasicAuthAndTokenMutuallyExclusive() {
        assertReject(
                "ws::addr=db:9000;username=admin;password=quest;token=ey.xyz;",
                "cannot use both token and username/password authentication"
        );
    }

    @Test
    public void testBasicAuthEmptyPasswordRejected() {
        // A present-but-blank password is rejected up front, matching the
        // ingress Sender, so a shared ws/wss string fails the same way on both
        // sides instead of building a degenerate Basic auth header.
        assertReject(
                "ws::addr=db:9000;username=alice;password=;",
                "password cannot be empty nor null"
        );
    }

    @Test
    public void testBasicAuthEmptyUsernameRejected() {
        assertReject(
                "ws::addr=db:9000;username=;password=secret;",
                "username cannot be empty nor null"
        );
    }

    @Test
    public void testBasicAuthWithPasswordOnlyRejected() {
        assertReject(
                "ws::addr=db:9000;password=quest;",
                "username and password must be provided together"
        );
    }

    @Test
    public void testBasicAuthWithUsernameOnlyRejected() {
        assertReject(
                "ws::addr=db:9000;username=admin;",
                "username and password must be provided together"
        );
    }

    @Test
    public void testUserPassAliasesAuthenticate() {
        // user/pass are aliases of username/password: they synthesize the same
        // Basic auth header.
        try (QwpQueryClient viaAlias = QwpQueryClient.fromConfig("ws::addr=db:9000;user=alice;pass=secret;");
             QwpQueryClient viaCanonical = QwpQueryClient.fromConfig("ws::addr=db:9000;username=alice;password=secret;")) {
            Assert.assertNotNull(viaAlias.getAuthorizationHeaderForTest());
            Assert.assertEquals(
                    viaCanonical.getAuthorizationHeaderForTest(),
                    viaAlias.getAuthorizationHeaderForTest());
        }
    }

    @Test
    public void testUserAliasAloneRejected() {
        // user is an alias of username, so user-alone trips the both-or-neither rule.
        assertReject(
                "ws::addr=db:9000;user=alice;",
                "username and password must be provided together"
        );
    }

    @Test
    public void testPassAliasAloneRejected() {
        assertReject(
                "ws::addr=db:9000;pass=secret;",
                "username and password must be provided together"
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
                "invalid compression: gzip (expected zstd, raw, auto)"
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
    public void testCompressionLevelDefaultIsOneWhenCompressionAlsoOmitted() {
        // Defense-in-depth: the level default must also be 1 when the
        // user did not opt in to compression at all. compression=raw means
        // the X-QWP-Accept-Encoding header is omitted entirely, so the
        // level value is unreachable on the wire -- but pinning it here
        // guards against the parser-local default drifting away from the
        // field-init default and surprising callers that flip to zstd
        // via the programmatic builder afterwards.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;")) {
            Assert.assertEquals(1, c.getCompressionLevelForTest());
        }
    }

    @Test
    public void testCompressionLevelDefaultIsOneWhenOmitted() {
        // Pinned: compression=zstd without an explicit compression_level
        // resolves to level 1 -- the cheapest server-side CPU. Bumping this
        // default silently would inflate per-connection compress cost for
        // opt-in clients that don't pin the level themselves.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;compression=zstd;")) {
            Assert.assertEquals(1, c.getCompressionLevelForTest());
        }
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
    public void testEndpointOrderPreserved() {
        // The parser walks the declared host list in order and the client
        // walks it in priority order. Three distinct hosts make the
        // assertion robust against accidental rotation by any single step.
        try (QwpQueryClient c = QwpQueryClient.fromConfig(
                "ws::addr=alpha:9000,bravo:9001,charlie:9002;")) {
            Assert.assertEquals(3, c.getEndpointCountForTest());
            Assert.assertEquals("alpha", c.getEndpointHostForTest(0));
            Assert.assertEquals(9000, c.getEndpointPortForTest(0));
            Assert.assertEquals("bravo", c.getEndpointHostForTest(1));
            Assert.assertEquals(9001, c.getEndpointPortForTest(1));
            Assert.assertEquals("charlie", c.getEndpointHostForTest(2));
            Assert.assertEquals(9002, c.getEndpointPortForTest(2));
        }
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
    public void testFailoverBackoffMaxAloneBelowDefaultInitialRejected() {
        // failover_backoff_max_ms alone, below the 50 ms default initial backoff,
        // makes the effective max < initial once fromConfig fills the missing
        // initial with its default. validateConfig enforces the ordering against
        // those effective values, so it is rejected up front (and the facade's
        // fail-fast build path rejects it without constructing a client).
        assertReject(
                "ws::addr=db:9000;failover_backoff_max_ms=10;",
                "failover_backoff_max_ms must be >= failover_backoff_initial_ms"
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
                "invalid failover: maybe (expected on, off)"
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
    public void testFailoverMaxDurationMsAccepted() {
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;failover_max_duration_ms=12345;")) {
            Assert.assertEquals(12345L, c.getFailoverMaxDurationMsForTest());
        }
    }

    @Test
    public void testFailoverMaxDurationMsDefaultIs30Seconds() {
        // failover.md §1.3: default is 30_000.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;")) {
            Assert.assertEquals(30_000L, c.getFailoverMaxDurationMsForTest());
        }
    }

    @Test
    public void testFailoverMaxDurationMsNegativeRejected() {
        assertReject("ws::addr=db:9000;failover_max_duration_ms=-1;",
                "failover_max_duration_ms must be >= 0");
    }

    @Test
    public void testFailoverMaxDurationMsNonIntegerRejected() {
        assertReject("ws::addr=db:9000;failover_max_duration_ms=forever;",
                "invalid failover_max_duration_ms: forever");
    }

    @Test
    public void testFailoverMaxDurationMsZeroIsUnbounded() {
        // 0 means unbounded; the parser must accept it without complaint.
        try (QwpQueryClient c = QwpQueryClient.fromConfig("ws::addr=db:9000;failover_max_duration_ms=0;")) {
            Assert.assertEquals(0L, c.getFailoverMaxDurationMsForTest());
        }
    }

    @Test
    public void testFailoverMaxDuration_AcceptsPositive() {
        assertParses("ws::addr=a:9000;failover_max_duration_ms=5000;");
    }

    @Test
    public void testFailoverMaxDuration_AcceptsZero() {
        assertParses("ws::addr=a:9000;failover_max_duration_ms=0;");
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
                + "target=primary;failover=on;"
                + "username=admin;password=quest;"
                + "client_id=batch-job/42;buffer_pool_size=8;"
                + "compression=zstd;compression_level=5;"
                + "max_batch_rows=512;"
                + "tls_verify=on;";
        assertParses(conf);
    }

    @Test
    public void testGorillaKeyRejected() {
        // gorilla has been removed; QWP ingestion always uses Gorilla timestamp
        // encoding. The egress client rejects the key like any other unknown key.
        assertReject("ws::addr=db:9000;gorilla=off;", "unknown configuration key: gorilla");
        assertReject("ws::addr=db:9000;gorilla=on;", "unknown configuration key: gorilla");
    }

    @Test
    public void testInFlightWindowKeyRejected() {
        // in_flight_window has been removed; the egress client rejects it like
        // any other unknown key.
        assertReject("ws::addr=db:9000;in_flight_window=10000;", "unknown configuration key: in_flight_window");
    }

    @Test
    public void testIngressOnlyKeysSilentlyAcceptedOnEgress() {
        // connect-string.md: these keys configure the Sender (ingress) only.
        // QwpQueryClient.fromConfig silently consumes them so a single connect
        // string can be shared between the Sender and the QwpQueryClient
        // without an "unknown configuration key" error. The egress parser does
        // not interpret the value -- range, enum, and type checks are the
        // ingress parser's job. A malformed value at the egress parser is
        // still consumed.

        // Each ingress-only key on its own with a representative happy-path
        // value. Covers all categories: auto-flush, buffer sizing, store-and-
        // forward, durable ACK, reconnect, error inbox, ingress aliases.
        String[] keys = {
                "auto_flush=on",
                "auto_flush_bytes=8m",
                "auto_flush_interval=100",
                "auto_flush_rows=1000",
                "close_flush_timeout_millis=5000",
                "connection_listener_inbox_capacity=64",
                "drain_orphans=on",
                "durable_ack_keepalive_interval_millis=200",
                "error_inbox_capacity=256",
                "initial_connect_retry=on",
                "max_background_drainers=4",
                "max_name_len=127",
                "reconnect_initial_backoff_millis=100",
                "reconnect_max_backoff_millis=5000",
                "reconnect_max_duration_millis=300000",
                "request_durable_ack=on",
                "sender_id=ingest-1",
                "sf_append_deadline_millis=30000",
                "sf_dir=/var/lib/qdb-sf",
                "sf_durability=memory",
                "sf_max_bytes=4m",
                "sf_max_total_bytes=10g",
                "transaction=on",
        };
        StringBuilder all = new StringBuilder("ws::addr=db:9000;");
        for (String kv : keys) {
            assertParses("ws::addr=db:9000;" + kv + ";");
            all.append(kv).append(';');
        }
        // All ingress-only keys at once -- a typical shared-config connect string.
        assertParses(all.toString());

        // Out-of-range / malformed values are silently consumed too -- the
        // egress parser does not validate ingress-only keys.
        assertParses("ws::addr=db:9000;auto_flush_rows=-1;");
        assertParses("ws::addr=db:9000;reconnect_max_duration_millis=banana;");

        // Empty values are well-formed and silently consumed.
        assertParses("ws::addr=db:9000;auto_flush=;");
        assertParses("ws::addr=db:9000;sf_dir=;");
    }

    @Test
    public void testInitialCredit_AcceptsPositive() {
        assertParses("ws::addr=a:9000;initial_credit=1048576;");
    }

    @Test
    public void testInitialCredit_AcceptsZero() {
        // 0 is the documented default and means "unbounded" -- the byte-credit
        // flow-control loop is disabled entirely. Must be parseable.
        assertParses("ws::addr=a:9000;initial_credit=0;");
    }

    @Test
    public void testInitialCredit_NegativeRejected() {
        assertReject("ws::addr=a:9000;initial_credit=-1;", "initial_credit must be >= 0");
    }

    @Test
    public void testInitialCredit_NonNumericRejected() {
        assertReject("ws::addr=a:9000;initial_credit=lots;", "invalid initial_credit: lots");
    }

    @Test
    public void testLbStrategyKeyRejectedAsUnknown() {
        // lb_strategy was removed: cluster-level load balancing is the
        // server's concern, not the client's. The connect-string parser
        // must surface its absence as an unknown-key error rather than
        // silently swallowing the value.
        assertReject("ws::addr=db:9000;lb_strategy=first;",
                "unknown configuration key: lb_strategy");
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
    public void testMaxSchemasPerConnectionRejected() {
        // max_schemas_per_connection was removed with the QWP schema-reference
        // mechanism. It used to be silently accepted on egress (an ingress-only
        // key); now the parser must surface it as an unknown-key error rather
        // than swallowing it, matching the ingress Sender's rejection.
        assertReject("ws::addr=db:9000;max_schemas_per_connection=1024;",
                "unknown configuration key: max_schemas_per_connection");
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
    public void testNonQwpKeysRejectedOnEgress() {
        // request_timeout, retry_timeout, request_min_throughput, and
        // protocol_version are legacy ILP HTTP/TCP keys, absent from the QWP
        // connect-string vocabulary (connect-string.md Key index). The
        // QwpQueryClient is QWP-only, so a ws:: string carrying them is
        // malformed -- the parser rejects them as unknown rather than
        // silently consuming them.
        // Legacy keys reject with a relocated-key hint pointing at the right place.
        assertReject("ws::addr=db:9000;request_timeout=10000;",
                "unknown configuration key: request_timeout (applies to legacy http/tcp/udp transports only)");
        assertReject("ws::addr=db:9000;retry_timeout=10000;",
                "unknown configuration key: retry_timeout (use reconnect_max_duration_millis on ws/wss)");
        assertReject("ws::addr=db:9000;request_min_throughput=102400;",
                "unknown configuration key: request_min_throughput (applies to legacy http/tcp/udp transports only)");
        assertReject("ws::addr=db:9000;protocol_version=2;",
                "unknown configuration key: protocol_version (QWP negotiates the protocol version during the WebSocket upgrade)");
        // protocol_version is rejected regardless of value: the egress side
        // has no "auto" pass-through.
        assertReject("ws::addr=db:9000;protocol_version=auto;",
                "unknown configuration key: protocol_version (QWP negotiates the protocol version during the WebSocket upgrade)");
        // max_datagram_size and multicast_ttl apply to the UDP transport only;
        // the QWP ws:: vocabulary does not include them, so the egress parser
        // rejects them as unknown.
        assertReject("ws::addr=db:9000;max_datagram_size=1400;",
                "unknown configuration key: max_datagram_size (applies to legacy http/tcp/udp transports only)");
        assertReject("ws::addr=db:9000;multicast_ttl=4;",
                "unknown configuration key: multicast_ttl (applies to legacy http/tcp/udp transports only)");
        // init_buf_size and max_buf_size size the legacy http/tcp ingest buffer;
        // the ws Sender has fixed framing, so they are legacy-only and the egress
        // parser rejects them with the hint rather than silently consuming them.
        assertReject("ws::addr=db:9000;init_buf_size=65536;",
                "unknown configuration key: init_buf_size (applies to legacy http/tcp/udp transports only)");
        assertReject("ws::addr=db:9000;max_buf_size=100m;",
                "unknown configuration key: max_buf_size (applies to legacy http/tcp/udp transports only)");
    }

    @Test
    public void testNullStringRejected() {
        assertReject(null, "configuration string cannot be empty");
    }

    @Test
    public void testPathKeyRejected() {
        // path has been removed; the egress client rejects it like any other
        // unknown key.
        assertReject("ws::addr=db:9000;path=/custom/read;", "unknown configuration key: path");
    }

    @Test
    public void testReservedErrorPolicyKeysSilentlyAccepted() {
        // connect-string.md "Error handling": the on_*_error keys are reserved
        // by the spec, which directs new clients to accept them in the connect
        // string. The Java client does not wire them to a policy yet, so the
        // egress parser consumes them as an accepted no-op -- it must not reject
        // them as unknown keys. Mirror of the Sender (ingress) behavior so one
        // connect string carrying these keys configures both clients.
        String[] keys = {
                "on_internal_error=halt",
                "on_parse_error=halt",
                "on_schema_error=drop",
                "on_security_error=halt",
                "on_server_error=auto",
                "on_write_error=drop",
        };
        StringBuilder all = new StringBuilder("ws::addr=db:9000;");
        for (String kv : keys) {
            assertParses("ws::addr=db:9000;" + kv + ";");
            all.append(kv).append(';');
        }
        assertParses(all.toString());
    }

    @Test
    public void testTargetAnyAccepted() {
        assertParses("ws::addr=db:9000;target=any;");
    }

    @Test
    public void testTargetInvalidRejected() {
        assertReject(
                "ws::addr=db:9000;target=leader;",
                "invalid target: leader (expected any, primary, replica)"
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
                "invalid tls_verify: strict (expected on, unsafe_off)"
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
    public void testTokenEmptyRejected() {
        // A present-but-blank token is rejected up front, matching the ingress
        // Sender, so the client never sends an empty Bearer header.
        assertReject("ws::addr=db:9000;token=;", "token cannot be empty nor null");
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
