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

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.client.QwpEgressFrames;
import io.questdb.client.test.cutlass.qwp.client.QwpEgressMockServer;
import io.questdb.client.test.cutlass.qwp.client.RecordingQueryHandler;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * {@link Query#options(QwpQueryOptions)} on the pooled facade: the options
 * reach the wire, the details the server reports reach the handler, the
 * error position reaches {@link QueryException#getPosition()}, and the handle
 * keeps its own copy of the options for its lifetime only.
 */
public class QuestDBQueryOptionsTest {
    private static final int ALL_CAPABILITIES = QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_DETAILS;
    private static final int WINDOW_FLAGS = QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS;

    @Test
    public void testAbandonDropsOptions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QuestDBQueryOptionsTest::model);
                 QuestDB db = connect(server);
                 Query q = db.borrowQuery()) {
                q.sql("SELECT x").options(new QwpQueryOptions().maxRows(1)).handler(new RecordingQueryHandler());
                q.abandon();
                q.sql("SELECT x").handler(new RecordingQueryHandler()).submit().await();
                Assert.assertFalse(server.getRequests().get(0).hasQueryFlags);
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testErrorPositionReachesQueryException() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QuestDBQueryOptionsTest::model);
                 QuestDB db = connect(server);
                 Query q = db.borrowQuery()) {
                RecordingQueryHandler handler = new RecordingQueryHandler();
                q.sql("FAIL").options(new QwpQueryOptions().errorPosition(true)).handler(handler);
                try {
                    q.submit().await();
                    Assert.fail("the query must fail");
                } catch (QueryException e) {
                    Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, e.getStatus());
                    Assert.assertEquals("Invalid column: nope", e.getMessage());
                    Assert.assertEquals(7, e.getPosition());
                }
                Assert.assertEquals(7, handler.errorPosition);

                q.options(null);
                try {
                    q.submit().await();
                    Assert.fail("the query must fail");
                } catch (QueryException e) {
                    Assert.assertEquals("[7] Invalid column: nope", e.getMessage());
                    Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, e.getPosition());
                }

                // a failure after one with a position reports its own
                q.sql("SELECT x").options(new QwpQueryOptions().maxRows(1));
                server.close();
                try {
                    q.submit().await();
                    Assert.fail("the query must fail");
                } catch (QueryException e) {
                    Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, e.getPosition());
                }
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testOptionsAreCopiedAndKeptAcrossSubmits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QuestDBQueryOptionsTest::model)) {
                try (QuestDB db = connect(server)) {
                    try (Query q = db.borrowQuery()) {
                        QwpQueryOptions options = new QwpQueryOptions().skipRows(1).maxRows(2).countRows(true).stats(true);
                        RecordingQueryHandler handler = new RecordingQueryHandler();
                        q.sql("SELECT x").options(options).handler(handler);
                        // the handle copied the options: changing them does not reach it
                        options.clear().maxRows(9);
                        q.submit().await();
                        Assert.assertEquals(Arrays.asList(1L, 2L), handler.values);
                        Assert.assertEquals(2, handler.totalRows);
                        Assert.assertEquals(10, handler.rowCount);
                        Assert.assertTrue(handler.hasStats);
                        Assert.assertEquals(123, handler.compileNanos);

                        // the next submit on the handle keeps them
                        handler.values.clear();
                        q.submit().await();
                        Assert.assertEquals(Arrays.asList(1L, 2L), handler.values);

                        // until they are replaced, or dropped
                        q.options(options);
                        q.submit().await();
                        q.options(null);
                        q.submit().await();
                        // the lease ends with options set
                        q.options(new QwpQueryOptions().stats(true).skipRows(3));
                    }
                    // a new borrow of the same pooled client starts without options
                    try (Query q = db.borrowQuery()) {
                        q.sql("SELECT x").handler(new RecordingQueryHandler()).submit().await();
                    }
                }
                List<QwpEgressFrames.QueryRequest> requests = server.getRequests();
                Assert.assertEquals(5, requests.size());
                for (int i = 0; i < 2; i++) {
                    QwpEgressFrames.QueryRequest r = requests.get(i);
                    Assert.assertEquals(WINDOW_FLAGS | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS,
                            r.queryFlags);
                    Assert.assertEquals(1, r.skipRows);
                    Assert.assertEquals(2, r.maxRows);
                }
                Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, requests.get(2).queryFlags);
                Assert.assertEquals(9, requests.get(2).maxRows);
                Assert.assertFalse(requests.get(3).hasQueryFlags);
                Assert.assertFalse(requests.get(4).hasQueryFlags);
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testStatsReachExecDone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QuestDBQueryOptionsTest::model);
                 QuestDB db = connect(server);
                 Query q = db.borrowQuery()) {
                RecordingQueryHandler handler = new RecordingQueryHandler();
                q.sql("INSERT INTO t VALUES (1)").options(new QwpQueryOptions().stats(true)).handler(handler).submit().await();
                Assert.assertEquals(1, handler.execDoneCount);
                Assert.assertEquals(4, handler.rowsAffected);
                Assert.assertTrue(handler.hasStats);
                Assert.assertEquals(123, handler.compileNanos);
                Assert.assertFalse(handler.isJitCompiled);
                server.assertNoFailure();
            }
        });
    }

    private static QuestDB connect(QwpEgressMockServer server) {
        return QuestDB.connect("ws::addr=" + server.getAddress()
                + ";sender_pool_min=0;query_pool_min=1;query_pool_max=1;failover=off;auth_timeout_ms=2000;");
    }

    /**
     * A 10-row table {@code x = 0..9} in one batch per query; an
     * {@code INSERT} that reports 4 rows; anything else fails at position 7.
     * Trailers follow the flags of the request, as on the server.
     */
    private static byte[][] model(QwpEgressFrames.QueryRequest r) {
        boolean hasStats = r.has(QwpEgressMsgKind.QUERY_FLAG_STATS);
        if (r.sql.startsWith("SELECT")) {
            long first = Math.max(0, Math.min(r.skipRows, 10));
            long count = Math.min(r.maxRows < 0 ? Long.MAX_VALUE : r.maxRows, 10 - first);
            long[] values = new long[(int) count];
            for (int i = 0; i < count; i++) {
                values[i] = first + i;
            }
            QwpEgressFrames.Body end = QwpEgressFrames.resultEnd(r.requestId, 0, count);
            if (r.has(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS)) {
                end.varint(10);
            }
            if (hasStats) {
                end.stats(123, 456, 0, 0, 0);
            }
            return new byte[][]{QwpEgressFrames.resultBatchOfLongs(r.requestId, 0, values), end.toFrame()};
        }
        if (r.sql.startsWith("INSERT")) {
            QwpEgressFrames.Body done = QwpEgressFrames.execDone(r.requestId, 2, 4);
            if (hasStats) {
                done.stats(123, 456, 0, 0, 0);
            }
            return new byte[][]{done.toFrame()};
        }
        String message = "Invalid column: nope";
        if (r.has(QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION)) {
            return new byte[][]{QwpEgressFrames.queryError(r.requestId, WebSocketResponse.STATUS_PARSE_ERROR, message).i32(7).toFrame()};
        }
        return new byte[][]{QwpEgressFrames.queryError(r.requestId, WebSocketResponse.STATUS_PARSE_ERROR, "[7] " + message).toFrame()};
    }
}
