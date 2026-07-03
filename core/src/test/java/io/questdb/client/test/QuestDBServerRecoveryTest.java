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
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * End-to-end resilience: the facade starts with the server down, the producer
 * keeps writing (buffered), and once the server comes up the write side
 * reconnects and the read side -- previously deferred so it could not fail-fast
 * the build -- can connect.
 * <p>
 * The mock cannot answer a real SELECT (result frames are exercised against a
 * real server in the parent repo), so the read step asserts the query client
 * <em>connects</em> once the server is up, not the row contents.
 */
public class QuestDBServerRecoveryTest {

    @Test(timeout = 60_000)
    public void testFacadeStartsWhileServerDownThenWritesAndReaderConnectsOnRecovery() throws Exception {
        // One mock server (the whole "cluster"), bound so the port is known but
        // NOT accepting yet: the address is reachable but no WebSocket upgrade
        // completes, so the server is effectively "down". It serves ingest ACK
        // on the write path and a SERVER_INFO frame on the read path -- the read
        // path is gated so the ingest connection's ACK stream is never disturbed.
        try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
        })) {
            server.setSendServerInfo(true); // the egress client's connect() waits for SERVER_INFO
            // One cluster config drives both pools:
            // lazy_connect=true expands to exactly this resilience: the ingest
            // side goes async (the producer never blocks; writes buffer until the
            // wire is up) and the read pool defaults to min=0 (the otherwise
            // fail-fast reader never sinks the build while the server is down,
            // and connects lazily on the first query).
            String cfg = "ws::addr=localhost:" + server.getPort()
                    + ";lazy_connect=true"
                    + ";sender_pool_min=1;sender_pool_max=1;query_pool_max=1"
                    + ";auth_timeout_ms=2000;reconnect_initial_backoff_millis=20"
                    + ";reconnect_max_backoff_millis=100;reconnect_max_duration_millis=600000"
                    + ";close_flush_timeout_millis=1000;";

            // (1) server down + (2) client starts:
            try (QuestDB db = QuestDB.builder().fromConfig(cfg).build()) {
                Assert.assertEquals("no handshake while the server is down", 0, server.handshakeCount());

                // lazy_connect keeps reads ENABLED, just deferred: the read pool
                // defaults to min=0, so nothing connects while the server is
                // down. The read client connects lazily on the first
                // borrowQuery() once the server is up (step 5).

                // (3) client writes -> buffers in the cursor SF engine; the call
                // must not throw even though the server is down.
                Sender sender = db.borrowSender();
                sender.table("t").longColumn("v", 1L).atNow();

                // (4) server starts:
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // The write side reconnects on its own once the server is up.
                awaitTrue("ingest must connect after the server comes up",
                        () -> server.handshakeCount() >= 1);

                // (5) client can now read: the deferred reader connects on the
                // first borrowQuery() (the mock does not serve rows, so we
                // assert the connection, not the result).
                int handshakesBeforeQuery = server.handshakeCount();
                db.borrowQuery().close();
                awaitTrue("query client must connect after the server comes up",
                        () -> server.handshakeCount() >= handshakesBeforeQuery + 1);

                // (6) return the ingest lease before db.close(): a borrowed
                // sender left outstanding makes close() wait out the acquire
                // timeout and then leak the delegate (close() never tears
                // down a borrowed sender -- C1).
                sender.close();
            }
        }
    }

    private static void awaitTrue(String message, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        Assert.assertTrue(message, condition.getAsBoolean());
    }
}
