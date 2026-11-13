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

import io.questdb.client.cutlass.qwp.client.QueryEvent;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The two default methods on {@link QwpColumnBatchHandler}
 * ({@link QwpColumnBatchHandler#onFailoverReset(QwpServerInfo) onFailoverReset}
 * and {@link QwpColumnBatchHandler#onExecDone(short, long) onExecDone}) are
 * documented as no-ops that handlers without explicit overrides can rely on.
 * The QwpQueryClient calls them unconditionally; if the defaults ever started
 * throwing we would silently break SELECT-only handlers and any handler that
 * does not opt into failover-aware behaviour.
 */
public class QwpColumnBatchHandlerTest {

    @Test
    public void testDetailedOverloadsDelegateToSimplerOnes() {
        // Each detailed overload delegates to the next simpler one, so a handler
        // that overrides only the abstract methods sees every terminal event.
        List<String> calls = new ArrayList<>();
        QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
            }

            @Override
            public void onEnd(long totalRows) {
                calls.add("onEnd " + totalRows);
            }

            @Override
            public void onError(byte status, String message) {
                calls.add("onError " + status + ' ' + message);
            }

            @Override
            public void onExecDone(short opType, long rowsAffected) {
                calls.add("onExecDone " + opType + ' ' + rowsAffected);
            }
        };
        QueryEvent event = new QueryEvent().asEnd(0).withRowCount(9).withStats(1, 2, 3, 4, true);
        handler.onEnd(1, 5, event.details);
        handler.onError(2, (byte) 5, "Invalid column: nope", 7);
        handler.onExecDone(3, (short) 2, 4, event.details);
        Assert.assertEquals(Arrays.asList("onEnd 5", "onError 5 Invalid column: nope", "onExecDone 2 4"), calls);

        // and the correlation-aware overloads get the request id
        calls.clear();
        QwpColumnBatchHandler correlating = new MinimalHandler() {
            @Override
            public void onEnd(long requestId, long totalRows) {
                calls.add("onEnd " + requestId + ' ' + totalRows);
            }

            @Override
            public void onError(long requestId, byte status, String message) {
                calls.add("onError " + requestId + ' ' + status + ' ' + message);
            }

            @Override
            public void onExecDone(long requestId, short opType, long rowsAffected) {
                calls.add("onExecDone " + requestId + ' ' + opType + ' ' + rowsAffected);
            }
        };
        correlating.onEnd(1, 5, event.details);
        correlating.onError(2, (byte) 5, "Invalid column: nope", 7);
        correlating.onExecDone(3, (short) 2, 4, event.details);
        Assert.assertEquals(Arrays.asList("onEnd 1 5", "onError 2 5 Invalid column: nope", "onExecDone 3 2 4"), calls);
    }

    @Test
    public void testDefaultMethodsAreNoOpAndDoNotThrow() {
        QwpColumnBatchHandler handler = new MinimalHandler();
        // Both callable with arbitrary args including null without raising:
        handler.onFailoverReset(null);
        handler.onFailoverReset(new QwpServerInfo((byte) 0, 0L, 0, 0L, "", "", null));
        handler.onExecDone((short) 0, 0L);
        handler.onExecDone((short) 1, 12345L);
        handler.onExecDone(Short.MAX_VALUE, Long.MAX_VALUE);
    }

    /**
     * Implements only the abstract methods. If onFailoverReset / onExecDone
     * lost their {@code default} modifier this class would no longer compile,
     * so this test doubles as a compile-time guard on the interface contract.
     */
    private static class MinimalHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    }
}
