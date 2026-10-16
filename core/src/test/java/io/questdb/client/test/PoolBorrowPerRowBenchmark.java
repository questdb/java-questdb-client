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
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * The customer's shape: one million rows, one borrow per row, against the
 * ack-all test server. Run by hand; prints wall time per configuration.
 * Target: the default is within noise of a single held lease; the window
 * numbers answer whether frames or commits dominate.
 */
@Ignore("manual benchmark")
public class PoolBorrowPerRowBenchmark {
    private static final int ROWS = 1_000_000;

    @Test
    public void run() throws Exception {
        time("held lease, default", "", false);
        time("borrow per row, default (buffer on close)", "", true);
        time("borrow per row, flush_on_close=on (today's shape)", "flush_on_close=on;", true);
        time("borrow per row, flush_on_close=on + window 1000/100", "flush_on_close=on;commit_rows=1000;commit_interval_ms=100;", true);
    }

    private static void time(String label, String extra, boolean borrowPerRow) throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            server.awaitStart(5, TimeUnit.SECONDS);
            String cfg = "ws::addr=localhost:" + server.getPort()
                    + ";sender_pool_min=1;sender_pool_max=1;query_pool_min=0;close_flush_timeout_millis=60000;" + extra;
            long start = System.nanoTime();
            try (QuestDB db = QuestDB.connect(cfg)) {
                if (borrowPerRow) {
                    for (int i = 0; i < ROWS; i++) {
                        try (Sender s = db.borrowSender()) {
                            s.table("bench").symbol("sym", "ABC").doubleColumn("px", i).longColumn("qty", i).atNow();
                        }
                    }
                } else {
                    try (Sender s = db.borrowSender()) {
                        for (int i = 0; i < ROWS; i++) {
                            s.table("bench").symbol("sym", "ABC").doubleColumn("px", i).longColumn("qty", i).atNow();
                        }
                    }
                }
            }
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.printf("%-60s %6d ms, %d data frames%n", label, millis, log.dataFrames());
        }
    }
}
