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

import io.questdb.client.Completion;
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link Query#timeout} through the {@link QuestDB} facade: a timed-out query is
 * reported as a {@link QueryException} whose {@link QueryException#isTimeout()}
 * holds, while the pooled, authenticated connection stays open and serves the
 * following queries. A worker still draining a timed-out query is not handed to
 * the next borrower before it is idle, and one whose connection stopped
 * responding is replaced rather than reused.
 */
public class QueryTimeoutFacadeTest {

    @Test(timeout = 30_000)
    public void testCancelOfASubmissionQueuedBehindADrainIsNotLost() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long timeoutMs = 100;
            final long graceMs = 1_000;
            Script script = new Script();
            script.onQuery = (s, c, id, n) -> {
                if (n == 1) {
                    // Past the grace period: the caller is released while the worker
                    // is still draining this query.
                    s.replyOnceLater(c, id, timeoutMs + graceMs + 600,
                            queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT, "timeout, query aborted"));
                }
                // n == 2: held until cancelled
            };
            script.onCancel = (s, c, id) -> s.replyOnce(c, id,
                    queryError(id, QwpConstants.STATUS_CANCELLED, "cancelled by client"));
            try (TestWebSocketServer server = startServer(script, QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
                 QuestDB db = QuestDB.connect(config(server) + "query_close_timeout_ms=" + graceMs + ";");
                 Query q = db.borrowQuery()) {
                q.sql("SELECT slow()").handler(new NoopHandler()).timeout(timeoutMs, TimeUnit.MILLISECONDS);
                assertTimesOut(q);

                // The worker is still draining the first query, so this submission
                // waits for it -- and is cancelled while it waits.
                Completion c = q.timeout(5, TimeUnit.SECONDS).submit();
                c.cancel();
                try {
                    c.await();
                    Assert.fail("the cancelled query must not complete");
                } catch (QueryException e) {
                    Assert.assertEquals("the cancel must reach the queued query: " + e.getMessage(),
                            QwpConstants.STATUS_CANCELLED, e.getStatus());
                }
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testConfiguredDefaultAppliesAndHandleCanOverrideIt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Script script = new Script();
            // An older server: the client cancels at the deadline. Query 2 is the
            // one run without a timeout, so it is allowed to take its time.
            script.onQuery = (s, c, id, n) -> {
                if (n == 2) {
                    s.sendLater(c, 300, resultEnd(id));
                }
            };
            script.onCancel = (s, c, id) -> s.replyOnce(c, id,
                    queryError(id, QwpConstants.STATUS_CANCELLED, "cancelled by client"));
            try (TestWebSocketServer server = startServer(script, QwpEgressMsgKind.CAP_QUERY_FLAGS);
                 QuestDB db = QuestDB.connect(config(server) + "query_timeout_ms=100;")) {
                try (Query q = db.borrowQuery()) {
                    q.sql("SELECT slow()").handler(new NoopHandler());
                    assertTimesOut(q);

                    q.timeout(0, TimeUnit.MILLISECONDS);
                    q.submit().await(); // no timeout: completes after 300ms
                }
                // A fresh borrow starts from the configured default again.
                try (Query q = db.borrowQuery()) {
                    q.sql("SELECT slow()").handler(new NoopHandler());
                    assertTimesOut(q);
                }
                Assert.assertEquals("all queries must share one connection", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTimedOutQueryKeepsThePooledConnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Script script = new Script();
            script.onQuery = (s, c, id, n) -> {
                if (n == 1) {
                    s.sendLater(c, 150, queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT,
                            "timeout, query aborted [runtime=150ms, timeout=100ms]"));
                } else {
                    s.send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
                 QuestDB db = QuestDB.connect(config(server))) {
                try (Query q = db.borrowQuery()) {
                    q.sql("SELECT slow()").handler(new NoopHandler()).timeout(100, TimeUnit.MILLISECONDS);
                    QueryException e = assertTimesOut(q);
                    Assert.assertTrue("the server's report must reach the caller: " + e.getMessage(),
                            e.getMessage().startsWith("timeout, query aborted"));

                    // The same handle, and so the same connection, runs the next query.
                    q.sql("INSERT INTO t VALUES (1)").submit().await();
                }
                try (Query q = db.borrowQuery()) {
                    q.sql("INSERT INTO t VALUES (2)").handler(new NoopHandler()).submit().await();
                }
                Assert.assertEquals("the authenticated connection must be kept across the timeout",
                        1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testWorkerDrainingATimedOutQueryIsReturnedOnlyWhenIdle() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long timeoutMs = 100;
            final long graceMs = 1_000;
            Script script = new Script();
            script.onQuery = (s, c, id, n) -> {
                if (n == 1) {
                    // Past the grace period but within the second one: the caller is
                    // released first, the connection drains the reply afterwards.
                    s.sendLater(c, timeoutMs + graceMs + 400,
                            queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT, "timeout, query aborted"));
                } else {
                    s.send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
                 QuestDB db = QuestDB.connect(config(server) + "query_close_timeout_ms=" + graceMs + ";")) {
                Query q = db.borrowQuery();
                q.sql("SELECT slow()").handler(new NoopHandler()).timeout(timeoutMs, TimeUnit.MILLISECONDS);
                long start = System.nanoTime();
                assertTimesOut(q);
                long awaitMs = elapsedMs(start);
                Assert.assertTrue("the caller is released at the end of the grace period, after " + awaitMs + "ms",
                        awaitMs >= timeoutMs + graceMs);
                Assert.assertEquals("the caller must not wait for the server's late reply", 0, script.sends.get());

                q.close();
                Assert.assertEquals("close() must wait for the drain before returning the worker",
                        1, script.sends.get());

                try (Query next = db.borrowQuery()) {
                    next.sql("INSERT INTO t VALUES (1)").handler(new NoopHandler()).submit().await();
                }
                Assert.assertEquals("the drained connection must be reused", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testWorkerWhoseConnectionStoppedRespondingIsReplaced() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Script script = new Script();
            script.onQuery = (s, c, id, n) -> {
                if (n > 1) {
                    s.send(c, execDone(id));
                }
                // n == 1: never answered, not even after a CANCEL.
            };
            // failover=off: the client cannot replace the connection by itself, so
            // the next query succeeds only if the pool discarded the worker.
            try (TestWebSocketServer server = startServer(script, QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
                 QuestDB db = QuestDB.connect(config(server) + "query_close_timeout_ms=200;failover=off;")) {
                try (Query q = db.borrowQuery()) {
                    q.sql("SELECT slow()").handler(new NoopHandler()).timeout(100, TimeUnit.MILLISECONDS);
                    assertTimesOut(q);
                }
                try (Query q = db.borrowQuery()) {
                    q.sql("INSERT INTO t VALUES (1)").handler(new NoopHandler()).submit().await();
                }
                Assert.assertEquals("the silent connection must be replaced by a new one", 2, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    private static QueryException assertTimesOut(Query q) throws InterruptedException {
        try {
            q.submit().await();
        } catch (QueryException e) {
            Assert.assertTrue("expected a query timeout, got status=" + e.getStatus() + ": " + e.getMessage(),
                    e.isTimeout());
            return e;
        }
        Assert.fail("the query must time out");
        return null;
    }

    private static String config(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort()
                + ";auth_timeout_ms=2000;sender_pool_min=0;query_pool_min=1;query_pool_max=1;";
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static byte[] execDone(long requestId) {
        return frame(QwpEgressMsgKind.EXEC_DONE, requestId, new byte[]{0, 0}); // op_type, rows_affected
    }

    private static byte[] frame(byte msgKind, long requestId, byte[] body) {
        int payloadLen = 1 + 8 + body.length;
        ByteBuffer bb = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(QwpConstants.MAGIC_MESSAGE);
        bb.put(QwpConstants.VERSION);
        bb.put((byte) 0);       // flags
        bb.putShort((short) 0); // table_count
        bb.putInt(payloadLen);
        bb.put(msgKind);
        bb.putLong(requestId);
        bb.put(body);
        return bb.array();
    }

    private static byte[] queryError(long requestId, byte status, String message) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        ByteBuffer body = ByteBuffer.allocate(1 + 2 + msg.length).order(ByteOrder.LITTLE_ENDIAN);
        body.put(status).putShort((short) msg.length).put(msg);
        return frame(QwpEgressMsgKind.QUERY_ERROR, requestId, body.array());
    }

    private static byte[] resultEnd(long requestId) {
        return frame(QwpEgressMsgKind.RESULT_END, requestId, new byte[]{0, 0}); // final_seq, total_rows
    }

    private static TestWebSocketServer startServer(Script script, int capabilities) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(script);
        server.setSendServerInfo(true);
        server.setCapabilities(capabilities);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    @FunctionalInterface
    private interface OnCancel {
        void run(Script script, TestWebSocketServer.ClientHandler client, long requestId);
    }

    @FunctionalInterface
    private interface OnQuery {
        void run(Script script, TestWebSocketServer.ClientHandler client, long requestId, int queryNumber);
    }

    private static final class NoopHandler implements QwpColumnBatchHandler {
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

    /**
     * Scripted egress server. Like a real one it sends one terminal frame per
     * request, so a cancel of a query that already ended gets no reply.
     */
    private static final class Script implements TestWebSocketServer.WebSocketServerHandler, AutoCloseable {
        // Delayed sends that have started, for ordering assertions. Counted before
        // the frame goes out, so a client that has received it sees the count.
        final AtomicInteger sends = new AtomicInteger();
        private final Set<Long> answered = ConcurrentHashMap.newKeySet();
        private final AtomicInteger queryCount = new AtomicInteger();
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scripted-egress-server");
            t.setDaemon(true);
            return t;
        });
        volatile OnCancel onCancel;
        volatile OnQuery onQuery;

        @Override
        public void close() {
            scheduler.shutdownNow();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length < 9) {
                return;
            }
            long requestId = ByteBuffer.wrap(data, 1, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
            if (data[0] == QwpEgressMsgKind.QUERY_REQUEST) {
                OnQuery script = onQuery;
                if (script != null) {
                    script.run(this, client, requestId, queryCount.incrementAndGet());
                }
            } else if (data[0] == QwpEgressMsgKind.CANCEL) {
                OnCancel script = onCancel;
                if (script != null) {
                    script.run(this, client, requestId);
                }
            }
        }

        void replyOnce(TestWebSocketServer.ClientHandler client, long requestId, byte[] frame) {
            if (answered.add(requestId)) {
                send(client, frame);
            }
        }

        void replyOnceLater(TestWebSocketServer.ClientHandler client, long requestId, long delayMs, byte[] frame) {
            if (answered.add(requestId)) {
                sendLater(client, delayMs, frame);
            }
        }

        void send(TestWebSocketServer.ClientHandler client, byte[] frame) {
            try {
                client.sendBinary(frame);
            } catch (IOException ignore) {
                // the client went away; the test's assertions report the outcome
            }
        }

        void sendLater(TestWebSocketServer.ClientHandler client, long delayMs, byte[] frame) {
            scheduler.schedule(() -> {
                sends.incrementAndGet();
                send(client, frame);
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }
}
