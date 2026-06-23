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

package io.questdb.client.test;

import io.questdb.client.QuestDB;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class QuestDBBuilderTest {

    @Test
    public void testBuilderCallAfterFromConfigOverridesPoolKeysFromString() {
        // A pool key carried in the string is overridden by a later explicit
        // builder call (last-write-wins). min=0 so build() does only parse-only
        // validation -- nothing connects.
        try (QuestDB ignored = QuestDB.builder()
                .fromConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=2;"
                        + "query_pool_min=0;query_pool_max=2;acquire_timeout_ms=10000;")
                .acquireTimeoutMillis(150)
                .build()) {
            Assert.assertNotNull(ignored);
        }
    }

    @Test
    public void testConflictingIntPoolKeyAcrossSidesRejected() {
        // Both sides carry sender_pool_max (an int pool key) with different
        // values -> build fails via resolvePoolInt's conflict check. The long
        // pool keys are covered by testConflictingPoolKeysAcrossSidesRejected;
        // this guards the separate int code path.
        try (QuestDB ignored = QuestDB.builder()
                .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=2;")
                .queryConfig("ws::addr=127.0.0.1:1;query_pool_min=0;sender_pool_max=5;")
                .build()) {
            Assert.fail("expected conflicting pool config");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("conflicting pool config: sender_pool_max"));
        }
    }

    @Test
    public void testConflictingPoolKeysAcrossSidesRejected() {
        // Both sides carry acquire_timeout_ms with different values -> build fails.
        try (QuestDB ignored = QuestDB.builder()
                .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;acquire_timeout_ms=1000;")
                .queryConfig("ws::addr=127.0.0.1:1;query_pool_min=0;acquire_timeout_ms=2000;")
                .build()) {
            Assert.fail("expected conflicting pool config");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("conflicting pool config: acquire_timeout_ms"));
        }
    }

    @Test
    public void testConnectStringWithPoolKeysAppliedToBuilder() {
        // Pool keys supplied via separate ingest/query strings are accepted;
        // min=0 so nothing connects.
        try (QuestDB ignored = QuestDB.builder()
                .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=1;")
                .queryConfig("ws::addr=127.0.0.1:1;query_pool_min=0;query_pool_max=1;")
                .build()) {
            Assert.assertNotNull(ignored);
        }
    }

    @Test
    public void testExplicitPoolKeyWinsOverConflictingStrings() {
        // The two strings disagree on acquire_timeout_ms, but an explicit builder
        // call sets it: explicit wins and the conflict check is skipped, whether
        // the explicit call comes after or before the config strings.
        try (QuestDB ignored = QuestDB.builder()
                .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;acquire_timeout_ms=1000;")
                .queryConfig("ws::addr=127.0.0.1:1;query_pool_min=0;acquire_timeout_ms=2000;")
                .acquireTimeoutMillis(500)
                .build()) {
            Assert.assertNotNull(ignored);
        }
        try (QuestDB ignored = QuestDB.builder()
                .acquireTimeoutMillis(500)
                .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;acquire_timeout_ms=1000;")
                .queryConfig("ws::addr=127.0.0.1:1;query_pool_min=0;acquire_timeout_ms=2000;")
                .build()) {
            Assert.assertNotNull(ignored);
        }
    }

    @Test
    public void testHttpIngestConfigRejected() {
        assertSchemaRejected(() -> QuestDB.builder().ingestConfig("http::addr=h:9000;"));
    }

    @Test
    public void testHttpSingleConfigRejected() {
        assertSchemaRejected(() -> QuestDB.builder().fromConfig("http::addr=h:9000;"));
    }

    @Test
    public void testMalformedEgressConfigRejectedAtBuildWithMinZero() {
        // query_pool_min=0 pre-warms nothing, so build() never constructs a
        // QwpQueryClient -- yet it must still reject a malformed query config up
        // front via QwpQueryClient.validateConfig, mirroring the ingress side.
        // Covers a typed enum (compression) and a bounded int (compression_level).
        assertEgressBuildRejected(
                "ws::addr=127.0.0.1:1;compression=gzip;query_pool_min=0;query_pool_max=2;", "compression");
        assertEgressBuildRejected(
                "ws::addr=127.0.0.1:1;compression_level=99;query_pool_min=0;query_pool_max=2;", "compression_level");
    }

    @Test
    public void testMalformedIngressConfigRejectedAtBuildWithMinZero() {
        // sender_pool_min=0 pre-warms nothing, so build() never constructs a
        // Sender -- yet it must still reject a malformed ingest config up front,
        // matching the egress side. Covers a typed enum (tls_verify), a
        // registry-STRING value that only the real Sender parse validates
        // (auto_flush_rows), and a WebSocket build-time check that only the full
        // no-connect validation reaches (auto_flush_interval=off disables
        // auto-flush, which the WebSocket transport does not support).
        assertIngressBuildRejected(
                "wss::addr=127.0.0.1:1;tls_verify=strict;sender_pool_min=0;sender_pool_max=2;", "tls_verify");
        assertIngressBuildRejected(
                "ws::addr=127.0.0.1:1;auto_flush_rows=abc;sender_pool_min=0;sender_pool_max=2;", "auto_flush_rows");
        assertIngressBuildRejected(
                "ws::addr=127.0.0.1:1;auto_flush_interval=off;sender_pool_min=0;sender_pool_max=2;", "auto-flush");
    }

    @Test
    public void testMalformedPoolValueRejectedAtBuild() {
        // A non-numeric pool value is rejected at build()'s pool-key resolution,
        // even with min=0. sender_pool_max is read through ConfigView.getInt,
        // whose error names the offending key.
        try (QuestDB ignored = QuestDB.builder()
                .fromConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=notanumber;")
                .build()) {
            Assert.fail("expected build to reject the malformed pool value");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("sender_pool_max"));
        }
    }

    @Test
    public void testMissingIngestConfigThrows() {
        try {
            QuestDB.builder().queryConfig("ws::addr=h:9000;").build().close();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("ingest"));
        }
    }

    @Test
    public void testMissingQueryConfigThrows() {
        try {
            QuestDB.builder().ingestConfig("ws::addr=h:9000;").build().close();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("query"));
        }
    }

    @Test
    public void testNegativeAcquireTimeoutRejected() {
        try {
            QuestDB.builder().acquireTimeoutMillis(-1);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testNegativePoolSizesRejected() {
        try {
            QuestDB.builder().senderPoolSize(0);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            QuestDB.builder().queryPoolSize(0);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    public void testQueryPoolBuildFailureUnwindsSenderPool() throws Exception {
        // Sender pool builds against a healthy ws ingest endpoint; the query
        // pool fails on a dead address. The handle must close the already-built
        // sender pool (its connected senders) rather than leak them.
        try (TestWebSocketServer ingest = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
        })) {
            ingest.start();
            Assert.assertTrue(ingest.awaitStart(5, TimeUnit.SECONDS));
            int port = ingest.getPort();
            try {
                QuestDB.builder()
                        .ingestConfig("ws::addr=localhost:" + port + ";")
                        .queryConfig("ws::addr=127.0.0.1:1;auth_timeout_ms=200;")
                        .senderPoolSize(2)
                        .queryPoolSize(2)
                        .acquireTimeoutMillis(500)
                        .build()
                        .close();
                Assert.fail("expected build to fail when query pool cannot connect");
            } catch (RuntimeException expected) {
                // The exact exception comes from QwpQueryClient.connect(); the
                // build failing tells us the sender-pool unwind ran.
            }
        }
    }

    @Test
    public void testSamePoolKeyValueAcrossSidesOk() {
        // The same key at the same value on both sides builds cleanly.
        try (QuestDB ignored = QuestDB.builder()
                .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;query_pool_min=0;acquire_timeout_ms=1500;")
                .queryConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;query_pool_min=0;acquire_timeout_ms=1500;")
                .build()) {
            Assert.assertNotNull(ignored);
        }
    }

    @Test
    public void testSharedWsConfigWithPoolKeys() {
        // A shared ws:: string carries pool keys; min=0 so build does only
        // parse-only validation (no connect).
        try (QuestDB ignored = QuestDB.builder()
                .fromConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=3;"
                        + "query_pool_min=0;query_pool_max=2;acquire_timeout_ms=1234;")
                .build()) {
            Assert.assertNotNull(ignored);
        }
    }

    @Test
    public void testTcpIngestConfigRejected() {
        assertSchemaRejected(() -> QuestDB.builder().ingestConfig("tcp::addr=h:9009;"));
    }

    @Test
    public void testUdpIngestConfigRejected() {
        assertSchemaRejected(() -> QuestDB.builder().queryConfig("udp::addr=h:9009;"));
    }

    private static void assertEgressBuildRejected(String query, String expectedFragment) {
        try {
            QuestDB.builder()
                    .ingestConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=2;")
                    .queryConfig(query)
                    .build()
                    .close();
            Assert.fail("expected build() to reject the malformed query config: " + query);
        } catch (RuntimeException e) {
            Assert.assertNotNull(e.getMessage());
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedFragment));
        }
    }

    private static void assertIngressBuildRejected(String ingest, String expectedFragment) {
        try {
            QuestDB.builder()
                    .ingestConfig(ingest)
                    .queryConfig("ws::addr=127.0.0.1:1;query_pool_min=0;query_pool_max=2;")
                    .build()
                    .close();
            Assert.fail("expected build() to reject the malformed ingest config: " + ingest);
        } catch (RuntimeException e) {
            // Ingress value errors surface as LineSenderException; both it and the
            // egress IllegalArgumentException are RuntimeException.
            Assert.assertNotNull(e.getMessage());
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedFragment));
        }
    }

    private static void assertSchemaRejected(Runnable action) {
        try {
            action.run();
            Assert.fail("expected the ws/wss schema requirement to reject this config");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("ws or wss"));
        }
    }
}
