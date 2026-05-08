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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import org.junit.Assert;
import org.junit.Test;

public class QwpQueryClientExecutingFlagTest {

    private static final QwpColumnBatchHandler NOOP_HANDLER = new QwpColumnBatchHandler() {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    };

    @Test(timeout = 10_000)
    public void testExecutingFlagClearedAfterIllegalState() {
        try (QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=127.0.0.1:1;failover=off;target=any;")) {
            for (int i = 0; i < 3; i++) {
                try {
                    client.execute("SELECT 1", NOOP_HANDLER);
                    Assert.fail("expected IllegalStateException");
                } catch (IllegalStateException e) {
                    Assert.assertTrue(
                            "iteration " + i + ": expected 'not connected', got: " + e.getMessage(),
                            e.getMessage().contains("not connected"));
                }
            }
        }
    }

    @Test(timeout = 10_000)
    public void testExecuteAfterCloseRejected() {
        QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=127.0.0.1:1;failover=off;target=any;");
        client.close();
        try {
            client.execute("SELECT 1", NOOP_HANDLER);
            Assert.fail("expected execute on closed client to throw");
        } catch (IllegalStateException e) {
            Assert.assertTrue(
                    "expected 'closed', got: " + e.getMessage(),
                    e.getMessage().contains("closed"));
        }
    }

    @Test(timeout = 10_000)
    public void testConnectAfterCloseRejected() {
        QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=127.0.0.1:1;failover=off;target=any;");
        client.close();
        try {
            client.connect();
            Assert.fail("expected connect on closed client to throw");
        } catch (IllegalStateException e) {
            Assert.assertTrue(
                    "expected 'closed', got: " + e.getMessage(),
                    e.getMessage().contains("closed"));
        }
    }
}
