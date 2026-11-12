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
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpQueryDetails;
import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end coverage of {@link QwpQueryOptions} through
 * {@link QwpQueryClient#execute}: the I/O thread decodes the trailers the
 * request asked for and the handler receives them. The mock server models the
 * server's {@code QwpEgressUpgradeProcessor}: a 10-row table {@code x = 0..9}
 * streamed in batches of up to 3 rows, cut by the row window; trailers only
 * for the flags the request carries; the bare message and the position of an
 * error only under {@code QUERY_FLAG_ERROR_POSITION}. The questdb repo checks
 * the same against a live server.
 */
public class QwpQueryClientQueryDetailsTest {
    private static final int ALL_CAPABILITIES = QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_DETAILS;
    private static final long CLIENT_WAIT_NANOS = 44;
    private static final long COMPILE_NANOS = 11;
    private static final long COUNT_NANOS = 33;
    private static final String ERROR_MESSAGE = "Invalid column: nope";
    private static final int ERROR_POSITION = 7;
    private static final long EXECUTE_NANOS = 22;
    private static final int OP_INSERT = 2;
    private static final long ROWS_AFFECTED = 5;
    private static final int TABLE_ROWS = 10;

    @Test
    public void testDetailsAbsentWithoutCapability() throws Exception {
        // the client leaves out what the server cannot report, and the server
        // answers with the baseline frames
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(QwpEgressMsgKind.CAP_QUERY_FLAGS,
                    QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                QwpQueryOptions options = new QwpQueryOptions().countRows(true).stats(true).errorPosition(true);

                RecordingQueryHandler select = new RecordingQueryHandler();
                client.execute("SELECT x FROM t", select, options);
                Assert.assertEquals(1, select.endCount);
                Assert.assertEquals(TABLE_ROWS, select.totalRows);
                select.assertNoDetails();

                RecordingQueryHandler insert = new RecordingQueryHandler();
                client.execute("INSERT INTO t VALUES (1)", insert, options);
                Assert.assertEquals(1, insert.execDoneCount);
                insert.assertNoDetails();

                RecordingQueryHandler failed = new RecordingQueryHandler();
                client.execute("FAIL", failed, options);
                failed.assertOnlyError();
                Assert.assertEquals("[7] " + ERROR_MESSAGE, failed.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, failed.errorPosition);

                for (QwpEgressFrames.QueryRequest r : server.getRequests()) {
                    Assert.assertFalse(r.toString(), r.hasQueryFlags);
                }
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testDetailsDoNotLeakToTheNextQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                QwpQueryOptions options = new QwpQueryOptions().countRows(true).stats(true).errorPosition(true).maxRows(2);
                for (int i = 0; i < 2; i++) {
                    RecordingQueryHandler detailed = new RecordingQueryHandler();
                    client.execute("SELECT x FROM t", detailed, options);
                    Assert.assertEquals(Arrays.asList(0L, 1L), detailed.values);
                    Assert.assertEquals(TABLE_ROWS, detailed.rowCount);
                    Assert.assertTrue(detailed.hasStats);

                    RecordingQueryHandler plain = new RecordingQueryHandler();
                    client.execute("SELECT x FROM t", plain);
                    Assert.assertEquals(TABLE_ROWS, plain.values.size());
                    Assert.assertEquals(TABLE_ROWS, plain.totalRows);
                    plain.assertNoDetails();

                    RecordingQueryHandler failed = new RecordingQueryHandler();
                    client.execute("FAIL", failed, options);
                    Assert.assertEquals(ERROR_POSITION, failed.errorPosition);

                    RecordingQueryHandler plainFailed = new RecordingQueryHandler();
                    client.execute("FAIL", plainFailed);
                    Assert.assertEquals("[7] " + ERROR_MESSAGE, plainFailed.errorMessage);
                    Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, plainFailed.errorPosition);

                    RecordingQueryHandler plainInsert = new RecordingQueryHandler();
                    client.execute("INSERT INTO t VALUES (1)", plainInsert);
                    plainInsert.assertNoDetails();
                }
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testErrorPositionReachesOnError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute("FAIL", handler, new QwpQueryOptions().errorPosition(true));
                handler.assertOnlyError();
                Assert.assertEquals(server.getRequests().get(0).requestId, handler.errorRequestId);
                Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, handler.errorStatus);
                Assert.assertEquals(ERROR_MESSAGE, handler.errorMessage);
                Assert.assertEquals(ERROR_POSITION, handler.errorPosition);
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testFailoverChecksTheNewServerAgain() throws Exception {
        // the first node dies mid-query; the replay goes to a node without
        // CAP_QUERY_DETAILS, which gets no detail flags and fails a row window
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer dying = new QwpEgressMockServer(ALL_CAPABILITIES, r -> null);
                 QwpEgressMockServer old = new QwpEgressMockServer(QwpEgressMsgKind.CAP_QUERY_FLAGS,
                         QwpQueryClientQueryDetailsTest::model)) {
                String config = "ws::addr=" + dying.getAddress() + ',' + old.getAddress()
                        + ";auth_timeout_ms=2000;failover_backoff_initial_ms=0;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(config)) {
                    client.connect();
                    RecordingQueryHandler handler = new RecordingQueryHandler();
                    client.execute("SELECT x FROM t", handler, new QwpQueryOptions().stats(true).countRows(true));
                    Assert.assertEquals(1, handler.failoverResetCount);
                    Assert.assertEquals(1, handler.endCount);
                    Assert.assertEquals(TABLE_ROWS, handler.values.size());
                    handler.assertNoDetails();
                    Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_STATS | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS,
                            dying.getRequests().get(0).queryFlags);
                    Assert.assertFalse(old.getRequests().get(0).hasQueryFlags);
                }
                try (QwpQueryClient client = QwpQueryClient.fromConfig(config)) {
                    client.connect();
                    RecordingQueryHandler handler = new RecordingQueryHandler();
                    client.execute("SELECT x FROM t", handler, new QwpQueryOptions().maxRows(2));
                    Assert.assertEquals(1, handler.failoverResetCount);
                    handler.assertOnlyError();
                    Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                    Assert.assertEquals("server does not support skipRows and maxRows [capabilities=0x2]; "
                            + "they need a server that advertises CAP_QUERY_DETAILS", handler.errorMessage);
                    Assert.assertEquals(2, dying.getRequests().size());
                    Assert.assertEquals(2, dying.getRequests().get(1).maxRows);
                    Assert.assertEquals("the replay must not reach the old node", 1, old.getRequests().size());
                }
                dying.assertNoFailure();
                old.assertNoFailure();
            }
        });
    }

    @Test
    public void testFailoverReplaysTheOptionsTheQueryStartedWith() throws Exception {
        // the client copies the options when execute() starts: a change during
        // the query does not reach the replay after a failover
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer dying = new QwpEgressMockServer(ALL_CAPABILITIES, r -> null);
                 QwpEgressMockServer healthy = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model)) {
                String config = "ws::addr=" + dying.getAddress() + ',' + healthy.getAddress()
                        + ";auth_timeout_ms=2000;failover_backoff_initial_ms=0;";
                try (QwpQueryClient client = QwpQueryClient.fromConfig(config)) {
                    client.connect();
                    QwpQueryOptions options = new QwpQueryOptions().skipRows(4).maxRows(3).countRows(true).stats(true);
                    RecordingQueryHandler handler = new RecordingQueryHandler() {
                        @Override
                        public void onFailoverReset(long requestId, QwpServerInfo newNode) {
                            super.onFailoverReset(requestId, newNode);
                            options.clear();
                        }
                    };
                    client.execute("SELECT x FROM t", handler, options);
                    Assert.assertEquals(1, handler.failoverResetCount);
                    Assert.assertEquals(Arrays.asList(4L, 5L, 6L), handler.values);
                    Assert.assertEquals(3, handler.totalRows);
                    Assert.assertEquals(TABLE_ROWS, handler.rowCount);
                    Assert.assertTrue(handler.hasStats);
                    QwpEgressFrames.QueryRequest replay = healthy.getRequests().get(0);
                    Assert.assertEquals(4, replay.skipRows);
                    Assert.assertEquals(3, replay.maxRows);
                }
                dying.assertNoFailure();
                healthy.assertNoFailure();
            }
        });
    }

    @Test
    public void testFlaggedRequestAnsweredWithBaselineError() throws Exception {
        // the server answers a request it could not decode with the baseline
        // frame, although the request asked for the position
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, r -> new byte[][]{
                    QwpEgressFrames.queryError(r.requestId, WebSocketResponse.STATUS_PARSE_ERROR,
                            "incomplete varint: buffer underflow").toFrame()
            });
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute("SELECT x FROM t", handler, new QwpQueryOptions().errorPosition(true).maxRows(1));
                handler.assertOnlyError();
                Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, handler.errorStatus);
                Assert.assertEquals("incomplete varint: buffer underflow", handler.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                Assert.assertTrue("the connection stays usable", client.isConnected());

                RecordingQueryHandler next = new RecordingQueryHandler();
                client.execute("SELECT x FROM t", next, new QwpQueryOptions().errorPosition(true));
                Assert.assertEquals(1, next.errorCount);
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testLegacyHandlerOverloadsStillFire() throws Exception {
        // a handler written before the details overloads gets every callback
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                QwpQueryOptions options = new QwpQueryOptions().countRows(true).stats(true).errorPosition(true).skipRows(1);
                List<String> calls = new ArrayList<>();
                QwpColumnBatchHandler legacy = new QwpColumnBatchHandler() {
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
                client.execute("SELECT x FROM t", legacy, options);
                client.execute("INSERT INTO t VALUES (1)", legacy, options);
                client.execute("FAIL", legacy, options);
                Assert.assertEquals(Arrays.asList(
                        "onEnd 9",
                        "onExecDone 2 5",
                        "onError 5 " + ERROR_MESSAGE
                ), calls);

                List<String> requestIds = new ArrayList<>();
                QwpColumnBatchHandler correlating = new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                    }

                    @Override
                    public void onEnd(long totalRows) {
                        Assert.fail();
                    }

                    @Override
                    public void onEnd(long requestId, long totalRows) {
                        requestIds.add("onEnd " + requestId);
                    }

                    @Override
                    public void onError(byte status, String message) {
                        Assert.fail();
                    }

                    @Override
                    public void onError(long requestId, byte status, String message) {
                        requestIds.add("onError " + requestId);
                    }

                    @Override
                    public void onExecDone(long requestId, short opType, long rowsAffected) {
                        requestIds.add("onExecDone " + requestId);
                    }
                };
                client.execute("SELECT x FROM t", correlating, options);
                client.execute("INSERT INTO t VALUES (1)", correlating, options);
                client.execute("FAIL", correlating, options);
                List<QwpEgressFrames.QueryRequest> requests = server.getRequests();
                Assert.assertEquals(Arrays.asList(
                        "onEnd " + requests.get(3).requestId,
                        "onExecDone " + requests.get(4).requestId,
                        "onError " + requests.get(5).requestId
                ), requestIds);
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testMissingTrailerIsTransportFailure() throws Exception {
        // A server that advertises CAP_QUERY_DETAILS must send the trailers the
        // request asks for; a terminator without them is a broken stream.
        Object[][] cases = {
                {"SELECT x FROM t", new QwpQueryOptions().countRows(true), "RESULT_END frame truncated mid row_count varint"},
                {"SELECT x FROM t", new QwpQueryOptions().stats(true), "RESULT_END frame truncated mid compile_ns varint"},
                {"INSERT INTO t VALUES (1)", new QwpQueryOptions().stats(true), "EXEC_DONE frame truncated mid compile_ns varint"},
        };
        for (Object[] c : cases) {
            TestUtils.assertMemoryLeak(() -> {
                try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES,
                        r -> model(r, 0));
                     QwpQueryClient client = newClient(server, "failover=off;")) {
                    client.connect();
                    RecordingQueryHandler handler = new RecordingQueryHandler();
                    client.execute((String) c[0], handler, (QwpQueryOptions) c[1]);
                    handler.assertOnlyError();
                    Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                    Assert.assertEquals(c[2], handler.errorMessage);
                    Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                    server.assertNoFailure();
                }
            });
        }
    }

    @Test
    public void testReentrantExecuteWithOptionsRejected() throws Exception {
        // one query at a time: an execute() from inside a handler fails, its
        // exception leaves the outer execute(), and the client stays usable
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                QwpQueryOptions options = new QwpQueryOptions().maxRows(1).stats(true);
                RecordingQueryHandler reentrant = new RecordingQueryHandler() {
                    @Override
                    public void onEnd(long requestId, long totalRows, QwpQueryDetails details) {
                        super.onEnd(requestId, totalRows, details);
                        client.execute("SELECT x FROM t", new RecordingQueryHandler(), options);
                    }
                };
                try {
                    client.execute("SELECT x FROM t", reentrant, options);
                    Assert.fail("a nested execute() must throw");
                } catch (IllegalStateException e) {
                    Assert.assertEquals("QwpQueryClient.execute called while another execute is in flight; "
                            + "one query at a time per client", e.getMessage());
                }
                Assert.assertEquals(1, reentrant.endCount);
                Assert.assertEquals("the nested query must not reach the server", 1, server.getRequests().size());

                RecordingQueryHandler next = new RecordingQueryHandler();
                client.execute("SELECT x FROM t", next, options);
                Assert.assertEquals(Arrays.asList(0L), next.values);
                Assert.assertTrue(next.hasStats);
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testRowWindowCountAndStatsReachOnEnd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                // {skip, max, expected first value, expected rows}
                long[][] windows = {{3, 4, 3, 4}, {0, 10, 0, 10}, {8, 5, 8, 2}, {10, 1, 0, 0}, {0, 0, 0, 0}, {1, Long.MAX_VALUE, 1, 9}};
                for (long[] w : windows) {
                    RecordingQueryHandler handler = new RecordingQueryHandler();
                    client.execute("SELECT x FROM t", handler,
                            new QwpQueryOptions().skipRows(w[0]).maxRows(w[1]).countRows(true).stats(true));
                    String msg = "skip=" + w[0] + ", max=" + w[1];
                    Assert.assertEquals(msg, 1, handler.endCount);
                    Assert.assertEquals(msg, w[3], handler.values.size());
                    for (int i = 0; i < w[3]; i++) {
                        Assert.assertEquals(msg, w[2] + i, (long) handler.values.get(i));
                    }
                    Assert.assertEquals(msg, w[3], handler.totalRows);
                    Assert.assertTrue(msg, handler.hasRowCount);
                    Assert.assertEquals(msg, TABLE_ROWS, handler.rowCount);
                    Assert.assertTrue(msg, handler.hasStats);
                    Assert.assertEquals(COMPILE_NANOS, handler.compileNanos);
                    Assert.assertEquals(EXECUTE_NANOS, handler.executeNanos);
                    Assert.assertEquals(COUNT_NANOS, handler.countNanos);
                    Assert.assertEquals(CLIENT_WAIT_NANOS, handler.clientWaitNanos);
                    Assert.assertTrue(handler.isJitCompiled);
                    Assert.assertEquals(server.getRequests().get(server.getRequests().size() - 1).requestId, handler.endRequestId);
                }
                server.assertNoFailure();
            }
        });
    }

    @Test
    public void testStatsReachOnExecDone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryDetailsTest::model);
                 QwpQueryClient client = newClient(server, "")) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                // the row options do not apply to a statement without rows
                client.execute("INSERT INTO t VALUES (1)", handler,
                        new QwpQueryOptions().stats(true).countRows(true).skipRows(1).maxRows(1));
                Assert.assertEquals(1, handler.execDoneCount);
                Assert.assertEquals(OP_INSERT, handler.opType);
                Assert.assertEquals(ROWS_AFFECTED, handler.rowsAffected);
                Assert.assertFalse(handler.hasRowCount);
                Assert.assertTrue(handler.hasStats);
                Assert.assertEquals(COMPILE_NANOS, handler.compileNanos);
                Assert.assertEquals(EXECUTE_NANOS, handler.executeNanos);
                Assert.assertEquals(COUNT_NANOS, handler.countNanos);
                Assert.assertEquals(CLIENT_WAIT_NANOS, handler.clientWaitNanos);
                Assert.assertTrue(handler.isJitCompiled);
                server.assertNoFailure();
            }
        });
    }

    private static byte[][] model(QwpEgressFrames.QueryRequest r) {
        return model(r, r.queryFlags);
    }

    /**
     * Answers {@code r} the way the server does, with the trailers of
     * {@code trailerFlags}: a {@code SELECT} streams the row window of the
     * 10-row table, an {@code INSERT} reports {@value #ROWS_AFFECTED} rows, and
     * anything else fails at position {@value #ERROR_POSITION}.
     */
    private static byte[][] model(QwpEgressFrames.QueryRequest r, long trailerFlags) {
        List<byte[]> frames = new ArrayList<>();
        if (r.sql.startsWith("SELECT")) {
            long skip = r.skipRows < 0 ? 0 : r.skipRows;
            long max = r.maxRows < 0 ? Long.MAX_VALUE : r.maxRows;
            long first = Math.min(skip, TABLE_ROWS);
            long end = first + Math.min(max, TABLE_ROWS - first);
            long seq = 0;
            long x = first;
            do {
                long n = Math.min(3, end - x);
                long[] values = new long[(int) n];
                for (int i = 0; i < n; i++) {
                    values[i] = x++;
                }
                frames.add(QwpEgressFrames.resultBatchOfLongs(r.requestId, seq++, values));
            } while (x < end);
            QwpEgressFrames.Body end0 = QwpEgressFrames.resultEnd(r.requestId, seq - 1, end - first);
            if ((trailerFlags & QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS) != 0) {
                end0.varint(TABLE_ROWS);
            }
            if ((trailerFlags & QwpEgressMsgKind.QUERY_FLAG_STATS) != 0) {
                end0.stats(COMPILE_NANOS, EXECUTE_NANOS, COUNT_NANOS, CLIENT_WAIT_NANOS, QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED);
            }
            frames.add(end0.toFrame());
        } else if (r.sql.startsWith("INSERT")) {
            QwpEgressFrames.Body done = QwpEgressFrames.execDone(r.requestId, OP_INSERT, ROWS_AFFECTED);
            if ((trailerFlags & QwpEgressMsgKind.QUERY_FLAG_STATS) != 0) {
                done.stats(COMPILE_NANOS, EXECUTE_NANOS, COUNT_NANOS, CLIENT_WAIT_NANOS, QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED);
            }
            frames.add(done.toFrame());
        } else if ((r.queryFlags & QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION) != 0) {
            frames.add(QwpEgressFrames.queryError(r.requestId, WebSocketResponse.STATUS_PARSE_ERROR, ERROR_MESSAGE)
                    .i32(ERROR_POSITION).toFrame());
        } else {
            frames.add(QwpEgressFrames.queryError(r.requestId, WebSocketResponse.STATUS_PARSE_ERROR,
                    "[" + ERROR_POSITION + "] " + ERROR_MESSAGE).toFrame());
        }
        return frames.toArray(new byte[0][]);
    }

    private static QwpQueryClient newClient(QwpEgressMockServer server, String extraConfig) {
        return QwpQueryClient.fromConfig("ws::addr=" + server.getAddress() + ";auth_timeout_ms=2000;" + extraConfig);
    }
}
