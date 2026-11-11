/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.client.QuestDB;
import io.questdb.client.QuestDBBuilder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the two properties that size the default symbol-dictionary starvation
 * wait ({@code symbol_dict_reset_max_wait_millis}).
 * <p>
 * It must be positive: a continuous producer keeps frames in flight at every
 * row start and never exposes a drained instant on its own, so a zero default
 * would make the recycle unreachable for exactly the population that crosses
 * the threshold.
 * <p>
 * It must stay well below the sender pool's default acquire timeout: a pooled
 * sender inherits an armed recycle at give-back, and the next borrower's first
 * {@code table()} call may pay the whole wait while holding its lease. Other
 * threads blocked in {@code acquire} must not time out before that borrower
 * releases. The pool default is read back through the builder's resolved
 * snapshot (a parse-only build with {@code sender_pool_min=0} connects
 * nothing), the same way {@code PoolConfigHonoredTest} does.
 */
public class SymbolDictResetDefaultsTest {

    @Test
    public void testDefaultWaitIsPositive() {
        Assert.assertTrue("the default starvation wait must be positive, or a continuous "
                        + "producer never recycles",
                QwpWebSocketSender.DEFAULT_SYMBOL_DICT_RESET_MAX_WAIT_MILLIS > 0);
    }

    @Test
    public void testDefaultWaitStaysBelowPoolAcquireTimeout() {
        QuestDBBuilder b = QuestDB.builder().fromConfig("ws::addr=127.0.0.1:1;"
                + "sender_pool_min=0;sender_pool_max=1;query_pool_min=0;query_pool_max=1;");
        b.build().close();
        long acquireTimeoutMillis = (Long) b.poolConfigSnapshotForTest().get("acquire_timeout_ms");
        long waitMillis = QwpWebSocketSender.DEFAULT_SYMBOL_DICT_RESET_MAX_WAIT_MILLIS;
        Assert.assertTrue("default starvation wait " + waitMillis + "ms must stay at or under half "
                        + "the default pool acquire timeout " + acquireTimeoutMillis + "ms, or a "
                        + "borrower paying the wait while holding its lease can starve other acquirers",
                waitMillis * 2 <= acquireTimeoutMillis);
    }
}
