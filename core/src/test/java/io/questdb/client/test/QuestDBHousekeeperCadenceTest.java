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
import io.questdb.client.impl.QuestDBImpl;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * The housekeeper's two cadences under extreme settings: a startup
 * store-and-forward recovery that keeps failing, and a reap interval of
 * {@code Long.MAX_VALUE}.
 */
public class QuestDBHousekeeperCadenceTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testFailedStartupRecoveryRetriesOnItsOwnCadenceNotTheSettleCadence() throws Exception {
        File sfDir = temp.newFolder("sf");
        int port;
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            port = silent.getPort();
            // Strand one slot: the server never acks, so the frame stays on disk after close.
            try (QuestDB db = QuestDB.connect("ws::addr=localhost:" + port + ";sf_dir=" + sfDir.getAbsolutePath()
                    + ";sender_pool_min=1;sender_pool_max=1;query_pool_min=0;query_pool_max=1"
                    + ";close_flush_timeout_millis=200;housekeeper_interval_ms=60000;auto_flush_interval=60000;")) {
                try (Sender s = db.borrowSender()) {
                    s.table("recover").longColumn("v", 1L).atNow();
                    s.flush();
                }
            }
        }
        // The port is closed now: every recovery step fails fast on a refused connect.
        // Settle every 50 ms; recovery must not follow that cadence.
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:" + port + ";sf_dir=" + sfDir.getAbsolutePath()
                + ";sender_pool_min=0;sender_pool_max=1;query_pool_min=0;query_pool_max=1"
                + ";housekeeper_interval_ms=1000;auto_flush_interval=50;")) {
            Thread.sleep(2_600);
            int attempts = ((QuestDBImpl) db).getSenderPoolForTesting().startupRecoveryBuildAttemptsForTesting();
            Assert.assertTrue("about one attempt per second expected, got " + attempts
                    + "; sf_dir=" + Arrays.toString(sfDir.list()), attempts >= 1 && attempts <= 5);
        }
    }

    @Test
    public void testHousekeeperIntervalAtLongMaxStillSettles() throws Exception {
        QuestDBReleaseSemanticsTest.FrameLog log = new QuestDBReleaseSemanticsTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(QuestDBReleaseSemanticsTest.config(server.getPort(),
                    "housekeeper_interval_ms=" + Long.MAX_VALUE + ";"))) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                log.awaitDataFrames(1, 3_000);              // the 100 ms settle must still run
            }
        }
    }

    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // no ack: the frame stays unacked on disk
        }
    }
}
