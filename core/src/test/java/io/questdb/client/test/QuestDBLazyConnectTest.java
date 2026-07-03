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
import io.questdb.client.Sender;
import io.questdb.client.test.cutlass.qwp.client.TestPorts;
import org.junit.Assert;
import org.junit.Test;

/**
 * {@code lazy_connect=true} makes a {@link QuestDB} facade tolerate the server
 * being down at startup <em>without</em> disabling reads: the ingest side
 * connects asynchronously (writes buffer until the wire is up) and the read pool
 * connects lazily on first use. Reads stay enabled and connect once the server
 * is up (the recovery lifecycle is covered end-to-end by
 * {@link QuestDBServerRecoveryTest}).
 * <p>
 * Because both sides must start non-blocking, a knob that forces a blocking /
 * fail-fast startup ({@code initial_connect_retry} other than {@code async}, or
 * an explicit {@code query_pool_min > 0}) is a configuration conflict and is
 * rejected up front with a clear remedy.
 */
public class QuestDBLazyConnectTest {

    @Test(timeout = 30_000)
    public void testLazyConnectStartsAndWritesWhileServerDown() {
        int port = TestPorts.findUnusedPort();
        // No server at `port`, sender_pool_min defaults to 1, and the only
        // resilience knob is lazy_connect=true. (a) build() must return promptly
        // -- the read pool defaults to min=0 and the ingest side goes async, so
        // neither side fail-fasts -- and (b) a write must buffer without throwing.
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:" + port
                + ";lazy_connect=true;reconnect_max_duration_millis=200"
                + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50"
                + ";close_flush_timeout_millis=0;")) {
            Sender sender = db.borrowSender();
            Assert.assertNotNull("a sender must be available with no server present", sender);
            sender.table("t").longColumn("v", 1L).atNow();
            // Return the lease before db.close(): a borrowed sender left
            // outstanding makes close() wait out the acquire timeout and then
            // leak the delegate (close() never tears down a borrowed sender --
            // C1). The server never comes up here, so the close-flush may
            // legitimately fail once the tiny reconnect budget (200ms) is
            // spent; either way the lease comes home -- a failed flush routes
            // the delegate to discardBroken on this thread.
            try {
                sender.close();
            } catch (RuntimeException ignored) {
                // acceptable: flush against the never-up server
            }
        }
    }

    @Test(timeout = 30_000)
    public void testLazyConnectKeepsReadsEnabledWhileServerDown() {
        int port = TestPorts.findUnusedPort();
        // Reads are ENABLED, just deferred: under lazy_connect the read pool
        // defaults to min=0, so build() does not eagerly connect or fail-fast
        // while the server is down. The read client connects lazily on the
        // first borrowQuery() once the server is up (covered end-to-end by
        // QuestDBServerRecoveryTest). This is the whole point of lazy_connect
        // over the old write-only mode, which disabled reads outright.
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:" + port
                + ";lazy_connect=true;close_flush_timeout_millis=0;")) {
            Assert.assertNotNull("the handle must build read-enabled while the server is down", db);
        }
    }

    @Test
    public void testLazyConnectAcceptsOnAndAllowsExplicitAsync() {
        int port = TestPorts.findUnusedPort();
        // lazy_connect accepts on/off as well as true/false, and an explicit
        // initial_connect_retry=async is consistent with it (no conflict).
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:" + port
                + ";lazy_connect=on;initial_connect_retry=async;query_pool_min=0"
                + ";close_flush_timeout_millis=0;")) {
            Assert.assertNotNull(db);
        }
    }

    @Test
    public void testLazyConnectConflictsWithBlockingInitialConnectRetry() {
        // off/false (OFF) and on/true/sync (SYNC) all block or fail-fast at
        // startup, so each conflicts with lazy_connect and must be rejected with
        // a clear remedy.
        assertLazyConflict("initial_connect_retry=off", "initial_connect_retry", "async");
        assertLazyConflict("initial_connect_retry=sync", "initial_connect_retry", "async");
        assertLazyConflict("initial_connect_retry=on", "initial_connect_retry", "async");
    }

    @Test
    public void testLazyConnectConflictsWithExplicitQueryPoolMinInConfig() {
        // An explicit query_pool_min > 0 makes the read pool eagerly fail-fast at
        // startup, contradicting lazy_connect.
        assertLazyConflict("query_pool_min=1", "query_pool_min", "0");
        assertLazyConflict("query_pool_min=2", "query_pool_min", "0");
        // query_pool_min=0 is exactly what lazy_connect wants -- no conflict.
        int port = TestPorts.findUnusedPort();
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:" + port
                + ";lazy_connect=true;query_pool_min=0;close_flush_timeout_millis=0;")) {
            Assert.assertNotNull(db);
        }
    }

    @Test
    public void testLazyConnectConflictsWithExplicitQueryPoolMinFromBuilder() {
        // The conflict also fires when query_pool_min > 0 comes from an explicit
        // builder call (queryPoolMin / queryPoolSize), not just the connect string.
        int port = TestPorts.findUnusedPort();
        assertLazyConflict(QuestDB.builder()
                .fromConfig("ws::addr=localhost:" + port + ";lazy_connect=true;close_flush_timeout_millis=0;")
                .queryPoolMin(1), "query_pool_min", "0");
        assertLazyConflict(QuestDB.builder()
                .fromConfig("ws::addr=localhost:" + port + ";lazy_connect=true;close_flush_timeout_millis=0;")
                .queryPoolSize(2), "query_pool_min", "0");
    }

    private static void assertLazyConflict(String extraKeys, String... expectedFragments) {
        int port = TestPorts.findUnusedPort();
        assertLazyConflict(QuestDB.builder().fromConfig("ws::addr=localhost:" + port
                + ";lazy_connect=true;" + extraKeys + ";close_flush_timeout_millis=0;"), expectedFragments);
    }

    private static void assertLazyConflict(QuestDBBuilder builder, String... expectedFragments) {
        try {
            builder.build().close();
            Assert.fail("expected lazy_connect configuration conflict");
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            Assert.assertNotNull(msg);
            Assert.assertTrue(msg, msg.contains("lazy_connect"));
            for (int i = 0; i < expectedFragments.length; i++) {
                Assert.assertTrue("'" + msg + "' should mention '" + expectedFragments[i] + "'",
                        msg.contains(expectedFragments[i]));
            }
        }
    }
}
