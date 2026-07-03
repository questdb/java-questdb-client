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
import io.questdb.client.QuestDBBuilder;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class QuestDBBuilderTest {

    @Test
    public void testBuilderCallAfterFromConfigOverridesPoolKeysFromString() {
        // A pool key carried in the string is overridden by a later explicit
        // builder call (last-write-wins). min=0 so build() does only parse-only
        // validation -- nothing connects.
        QuestDBBuilder b = QuestDB.builder()
                .fromConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=2;"
                        + "query_pool_min=0;query_pool_max=2;acquire_timeout_ms=10000;")
                .acquireTimeoutMillis(150);
        try (QuestDB ignored = b.build()) {
            Assert.assertNotNull(ignored);
        }
        // The explicit acquireTimeoutMillis(150) wins over the string's 10000.
        Assert.assertEquals(150L, b.poolConfigSnapshotForTest().get("acquire_timeout_ms"));
    }

    @Test
    public void testMaxFrameRejectionsAcceptedThroughFacadeConfig() {
        // max_frame_rejections is an INGRESS key: the facade validates it
        // against the registry and hands the full string to every pooled
        // Sender (Sender.fromConfig applies it); the query pool accepts it as
        // a syntactic no-op. min=0 pools -> parse-only validation, no server.
        try (QuestDB ignored = QuestDB.connect(
                "ws::addr=127.0.0.1:1;sender_pool_min=0;query_pool_min=0;max_frame_rejections=6;")) {
            Assert.assertNotNull(ignored);
        }
        // An out-of-range value must be rejected up front, not when the first
        // pooled Sender is eventually built.
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;max_frame_rejections=0;sender_pool_min=0;query_pool_min=0;",
                "max_frame_rejections");
    }

    @Test
    public void testConnectSingleStringValidatesAndBuilds() {
        // QuestDB.connect(single string) hands the same ws:: cluster string to
        // both the ingest and query pools. min=0 on both pools validates both
        // clients without connecting, so build() returns a live handle.
        try (QuestDB ignored = QuestDB.connect(
                "ws::addr=127.0.0.1:1;sender_pool_min=0;query_pool_min=0;")) {
            Assert.assertNotNull(ignored);
        }
    }

    @Test
    public void testMalformedEgressConfigRejectedAtBuildWithMinZero() {
        // query_pool_min=0 pre-warms nothing, so build() never constructs a
        // QwpQueryClient -- yet it must still reject a malformed egress key in
        // the single cluster config up front, mirroring the ingress side.
        // Covers a typed enum (compression) and a bounded int (compression_level).
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;compression=gzip;sender_pool_min=0;query_pool_min=0;query_pool_max=2;",
                "compression");
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;compression_level=99;sender_pool_min=0;query_pool_min=0;query_pool_max=2;",
                "compression_level");
    }

    @Test
    public void testMalformedIngressConfigRejectedAtBuildWithMinZero() {
        // sender_pool_min=0 pre-warms nothing, so build() never constructs a
        // Sender -- yet it must still reject a malformed ingress key in the
        // single cluster config up front. Covers a typed enum (tls_verify), a
        // registry-STRING value that only the real Sender parse validates
        // (auto_flush_rows), and WebSocket build-time checks: auto_flush=off and
        // auto_flush_interval=off both disable auto-flush (unsupported on
        // WebSocket), and sf_durability=flush is not yet supported.
        assertBuildRejected(
                "wss::addr=127.0.0.1:1;tls_verify=strict;sender_pool_min=0;query_pool_min=0;", "tls_verify");
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;auto_flush_rows=abc;sender_pool_min=0;query_pool_min=0;", "auto_flush_rows");
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;auto_flush_interval=off;sender_pool_min=0;query_pool_min=0;", "auto-flush");
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;auto_flush=off;sender_pool_min=0;query_pool_min=0;", "auto-flush");
        assertBuildRejected(
                "ws::addr=127.0.0.1:1;sf_durability=flush;sender_pool_min=0;query_pool_min=0;", "not yet supported");
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
    public void testMissingConfigThrows() {
        try {
            QuestDB.builder().build().close();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("configuration"));
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
    public void testNonWsSchemaRejected() {
        // The single cluster config (and QuestDB.connect) must use ws/wss.
        assertSchemaRejected(() -> QuestDB.builder().fromConfig("http::addr=h:9000;"));
        assertSchemaRejected(() -> QuestDB.builder().fromConfig("tcp::addr=h:9009;"));
        assertSchemaRejected(() -> QuestDB.builder().fromConfig("udp::addr=h:9009;"));
        assertSchemaRejected(() -> QuestDB.connect("http::addr=h:9000;").close());
    }

    @Test
    public void testQueryPoolBuildFailureUnwindsSenderPool() throws Exception {
        // One server, one cluster config: the server accepts ingest write-path
        // upgrades but rejects egress read-path upgrades, so the sender pool
        // connects while the query pool's connect fails. The failed build() must
        // close the already-built sender pool (its connected senders) rather than
        // leak them.
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
        })) {
            server.setRejectReadUpgrade(true);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            int port = server.getPort();
            try {
                QuestDB.builder()
                        .fromConfig("ws::addr=localhost:" + port + ";auth_timeout_ms=200;")
                        .senderPoolSize(2)
                        .queryPoolSize(2)
                        .acquireTimeoutMillis(500)
                        .build()
                        .close();
                Assert.fail("expected build to fail when the query pool cannot connect");
            } catch (RuntimeException expected) {
                // The exact exception comes from QwpQueryClient.connect(). The
                // build failing only proves the query pool gave up; the
                // assertions below prove the unwind closed the senders the
                // sender pool had already connected, rather than leaking them.
            }
            // The sender pool eagerly warmed senderPoolSize(2), so the server
            // saw two ingest handshakes (proving the senders connected and the
            // assertion below is not vacuous)...
            awaitTrue("sender pool should have connected two ingest senders",
                    () -> server.handshakeCount() >= 2);
            // ...and the failed build() must have closed every one of them, so
            // no sender connection is left live on the server. The server
            // observes the client-side socket close asynchronously, so poll.
            awaitTrue("failed build() must close the already-built sender pool, leaving no live connection",
                    () -> server.liveConnectionCount() == 0);
        }
    }

    @Test
    public void testSharedVocabularyConnectsBothPoolsLive() throws Exception {
        // The headline use case: one cluster connect-string carrying BOTH
        // ingress-only keys (auto_flush_rows, sender_id) and egress-only keys
        // (compression, max_batch_rows, target, failover) drives both LIVE pools
        // -- each side applies the keys it owns and silently ignores the rest.
        // One mock server serves both: an ACK stream on the ingest write path and
        // a SERVER_INFO frame on the egress read path (the read path is gated so
        // the ingest connection's ACK stream is never disturbed).
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
        })) {
            server.setSendServerInfo(true); // the egress client's connect() waits for SERVER_INFO
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // A single cluster config carrying the mixed key set. The pools
            // pre-warm min=1, so the shared vocabulary connects a live sender AND
            // a live query client, not merely validates.
            String cfg = "ws::addr=localhost:" + server.getPort() + ";"
                    + "auto_flush_rows=100;sender_id=probe-1;"                          // ingress-only
                    + "compression=auto;max_batch_rows=512;target=any;failover=off;"    // egress-only
                    + "auth_timeout_ms=2000;"                                           // common
                    + "sender_pool_min=1;sender_pool_max=2;query_pool_min=1;query_pool_max=2;"; // pool
            try (QuestDB db = QuestDB.builder().fromConfig(cfg).build()) {
                // Close the lease: a borrowed sender left outstanding makes
                // db.close() wait out the acquire timeout and then leak the
                // delegate (close() never tears down a borrowed sender -- C1).
                try (io.questdb.client.Sender s = db.borrowSender()) {
                    Assert.assertNotNull(s);
                }
                try (io.questdb.client.Query q = db.borrowQuery()) {
                    Assert.assertNotNull(q);
                }
            }
        }
    }

    @Test
    public void testSharedWsConfigWithPoolKeys() {
        // A cluster ws:: string carries pool keys for both pools; min=0 so build
        // does only parse-only validation (no connect).
        try (QuestDB ignored = QuestDB.builder()
                .fromConfig("ws::addr=127.0.0.1:1;sender_pool_min=0;sender_pool_max=3;"
                        + "query_pool_min=0;query_pool_max=2;acquire_timeout_ms=1234;")
                .build()) {
            Assert.assertNotNull(ignored);
        }
    }

    private static void assertBuildRejected(String config, String expectedFragment) {
        try {
            QuestDB.builder().fromConfig(config).build().close();
            Assert.fail("expected build() to reject the malformed config: " + config);
        } catch (RuntimeException e) {
            // Ingress value errors surface as LineSenderException; egress errors
            // as IllegalArgumentException -- both are RuntimeException.
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

    private static void awaitTrue(String message, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assert.assertTrue(message, condition.getAsBoolean());
    }
}
