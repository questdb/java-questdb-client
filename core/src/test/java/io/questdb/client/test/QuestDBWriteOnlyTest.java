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
import io.questdb.client.Sender;
import io.questdb.client.test.cutlass.qwp.client.TestPorts;
import org.junit.Assert;
import org.junit.Test;

/**
 * A write-only (ingest-only) {@link QuestDB} facade skips the read pool, which
 * is otherwise fail-fast at startup -- so the handle builds even when the
 * server / read primary is unavailable.
 */
public class QuestDBWriteOnlyTest {

    @Test
    public void testWriteOnlyDoesNotRequireQueryConfig() {
        int port = TestPorts.findUnusedPort();
        // Only an ingest config is supplied -- no queryConfig()/fromConfig().
        try (QuestDB db = QuestDB.builder()
                .ingestConfig("ws::addr=localhost:" + port + ";sender_pool_min=0;")
                .writeOnly()
                .build()) {
            Assert.assertNotNull(db);
        }
    }

    @Test
    public void testWriteOnlyIngestWorksWhileServerless() {
        int port = TestPorts.findUnusedPort();
        // Warm one sender in async mode against a dead port: build() returns an
        // unconnected-but-usable sender; writes buffer until the wire is up.
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=localhost:" + port + ";sender_pool_min=1;sender_pool_max=1"
                        + ";initial_connect_retry=async"
                        + ";reconnect_max_duration_millis=400;reconnect_initial_backoff_millis=10"
                        + ";reconnect_max_backoff_millis=50;close_flush_timeout_millis=0;")
                .writeOnly()
                .build()) {
            Sender sender = db.borrowSender();
            Assert.assertNotNull("a sender must be available with no server present", sender);
            // Buffering a row against an unconnected sender must not throw.
            sender.table("t").longColumn("v", 1L).atNow();
        }
    }

    @Test
    public void testWriteOnlyStartsWithoutServerAndDisablesQuery() {
        int port = TestPorts.findUnusedPort();
        // No server at `port`. A normal facade builds the read pool eagerly
        // (query_pool_min defaults to 1) and would fail-fast here; write-only
        // skips it, and sender_pool_min=0 means no eager ingest connect either,
        // so build() performs no network I/O and succeeds.
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=localhost:" + port + ";sender_pool_min=0;")
                .writeOnly()
                .build()) {
            assertQueryDisabled(db::query);
            assertQueryDisabled(db::newQuery);
        }
    }

    private static void assertQueryDisabled(Runnable read) {
        try {
            read.run();
            Assert.fail("query/read must be disabled on a write-only client");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(
                    "message should explain write-only: " + expected.getMessage(),
                    expected.getMessage().toLowerCase().contains("write-only"));
        }
    }
}
