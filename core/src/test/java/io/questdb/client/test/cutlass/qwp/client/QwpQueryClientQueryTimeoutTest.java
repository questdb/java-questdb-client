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
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-query timeout of {@link QwpQueryClient}, against a scripted mock server.
 * Pins the wire contract (the capability-gated {@code timeout_ms} field after
 * {@code query_flags}), the client-side deadline and its grace periods, and the
 * central promise of the feature: a timed-out query ends gracefully -- reported
 * as {@link QwpConstants#STATUS_QUERY_TIMEOUT} -- on a connection that stays
 * open and authenticated for the next query. Only a connection that does not
 * answer at all is replaced. The same holds for a query its caller abandons --
 * the handler throws, or the thread is interrupted -- before the query ends: the
 * next query never sees the abandoned one's leftovers.
 * <p>
 * The server-side enforcement (breaker timeout, status mapping) is covered
 * against a live server in the questdb repository.
 */
public class QwpQueryClientQueryTimeoutTest {

    private static final int CAPS_FLAGS_ONLY = QwpEgressMsgKind.CAP_QUERY_FLAGS;
    private static final int CAPS_WITH_TIMEOUT = QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT;

    @Test(timeout = 30_000)
    public void testBatchesArrivingAfterTheDeadlineAreDiscarded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    send(c, firstBatch(id, "a"));
                    s.sendLater(c, 400, nextBatch(id, 1, "b"), nextBatch(id, 2, "c"),
                            queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT, "timeout, query aborted"));
                } else {
                    send(c, firstBatch(id, "z"));
                    send(c, resultEnd(id, 1));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                RecordingHandler h = new RecordingHandler();
                client.execute("SELECT s FROM t", null, h, false, 150);
                Assert.assertEquals("only the batch that arrived before the deadline reaches the handler",
                        1, h.batches.get());
                h.assertTimedOut();

                // The discarded batches were still decoded, so the connection-scoped
                // state stays in step with the server and the next query decodes.
                RecordingHandler next = new RecordingHandler();
                client.execute("SELECT s FROM t", null, next, false, 0);
                Assert.assertTrue("the next query must complete on the same connection", next.ended);
                Assert.assertEquals(1, next.batches.get());
                Assert.assertEquals("the connection must be reused, not replaced", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCallerIsReleasedAfterTheGraceWhileTheConnectionDrains() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long timeoutMs = 100;
            final long graceMs = 1_000;
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    // Ends the query well after the grace period, but well within the
                    // second one: the caller must not wait for it, the connection must.
                    s.sendLater(c, timeoutMs + graceMs + 400,
                            queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT, "timeout, query aborted"));
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.withQueryTimeoutGrace(graceMs);
                RecordingHandler h = new RecordingHandler();
                long start = System.nanoTime();
                client.execute("SELECT slow()", null, h, false, timeoutMs);
                long executeMs = elapsedMs(start);

                h.assertTimedOut();
                Assert.assertTrue("the message must say the server did not end the query in time: " + h.errorMessage,
                        h.errorMessage.contains("grace period"));
                long releasedMs = TimeUnit.NANOSECONDS.toMillis(h.terminalNanos - start);
                Assert.assertTrue("the caller must be released only once the grace period is over, after "
                        + releasedMs + "ms", releasedMs >= timeoutMs + graceMs);
                Assert.assertTrue("the caller must be released before the server's late reply was even sent",
                        h.terminalNanos < script.lastSendNanos);
                Assert.assertTrue("execute() must return only after draining the server's reply, after "
                        + executeMs + "ms", executeMs >= timeoutMs + graceMs + 400);
                Assert.assertEquals("one terminal callback per query", 1, h.terminals.get());
                Assert.assertNotNull("the end of the grace period must nudge the server with a CANCEL",
                        script.cancels.poll(5, TimeUnit.SECONDS));
                Assert.assertFalse("a drained connection is healthy", client.hasTerminalFailure());

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue(next.execDone);
                Assert.assertEquals("the drained connection must be reused", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCancelOfAQueryQueuedBehindLeftoversIsNotLost() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Set<Long> received = ConcurrentHashMap.newKeySet();
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    send(c, firstBatch(id, "a"));
                    // The rest arrives after the handler has thrown.
                    s.sendLater(c, 300, nextBatch(id, 1, "b"), resultEnd(id, 2));
                } else {
                    // Held: only a CANCEL ends it.
                    received.add(id);
                }
            };
            // Like a real server, ignores a cancel of a query it has not received.
            script.onCancel = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (received.contains(id)) {
                    s.replyOnce(c, id, 0, queryError(id, QwpConstants.STATUS_CANCELLED, "cancelled by client"));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.withQueryTimeoutGrace(500);
                try {
                    client.execute("SELECT s FROM t", null, failingOnBatch(new IllegalStateException("handler failed")), false, 0);
                    Assert.fail("the handler's exception must propagate out of execute()");
                } catch (IllegalStateException expected) {
                    // the first query is abandoned, its rest still on the way
                }

                // Cancelled before it is sent: the I/O thread still works through the
                // abandoned query, and must send the CANCEL only after this query.
                RecordingHandler next = new RecordingHandler();
                client.execute("SELECT s FROM t", binds -> client.cancel(), next, false, 2_000);
                Assert.assertEquals("the cancel must reach the server after its query, got message=" + next.errorMessage,
                        QwpConstants.STATUS_CANCELLED, next.errorStatus);
                Assert.assertEquals("the abandoned query's rows must not reach the next query", 0, next.batches.get());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCompleteResultJustPastTheDeadlineIsASuccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                // No result batch is withheld from the handler, so a reply that lands
                // after the client's deadline still reports the real outcome.
                s.sendLater(c, 300, n == 1 ? resultEnd(id, 0) : execDone(id));
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                RecordingHandler select = new RecordingHandler();
                client.execute("SELECT 1 WHERE false", null, select, false, 100);
                Assert.assertTrue("an empty result past the deadline is still complete", select.ended);
                Assert.assertEquals(0, select.errorStatus);

                RecordingHandler insert = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, insert, false, 100);
                Assert.assertTrue("a statement that completed past the deadline took effect", insert.execDone);
                Assert.assertEquals(0, insert.errorStatus);
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testFailoverReplayCarriesTheRemainingBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                if (n == 1) {
                    // A transport failure mid-query: the client fails over and replays.
                    s.closeLater(c, 200);
                } else {
                    send(c, execDone(requestIdOf(frame)));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server,
                         "failover_backoff_initial_ms=0;failover_backoff_max_ms=0;")) {
                RecordingHandler h = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, h, false, 10_000);
                Assert.assertTrue("the replay must complete", h.execDone);
                Assert.assertEquals(1, h.failoverResets.get());

                long firstTimeout = trailerOf(script.queries.take())[1];
                long replayTimeout = trailerOf(script.queries.take())[1];
                Assert.assertTrue("the first attempt carries about the whole budget: " + firstTimeout,
                        firstTimeout > 9_000 && firstTimeout <= 10_000);
                Assert.assertTrue("the replay must carry only the remaining budget, not a fresh one: "
                        + replayTimeout, replayTimeout > 0 && replayTimeout <= firstTimeout - 150);
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testInterruptReleasesTheCallerOnlyOnceItsRequestIsEncoded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> send(c, execDone(requestIdOf(frame)));
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                // The I/O thread picks the request up, and stops inside its SQL text.
                CountDownLatch reading = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                CharSequence sql = new BlockingSql("INSERT INTO t VALUES (1)", reading, release);
                RecordingHandler interrupted = new RecordingHandler();
                Thread caller = new Thread(() -> client.execute(sql, null, interrupted, false, 0));
                caller.start();
                try {
                    Assert.assertTrue("the I/O thread must start encoding the request", reading.await(5, TimeUnit.SECONDS));
                    caller.interrupt();
                    // The caller may change the SQL text once released, and the next
                    // query reuses the bind buffer: not before the I/O thread is done.
                    caller.join(300);
                    Assert.assertTrue("execute() must wait for the I/O thread to finish reading the request",
                            caller.isAlive());
                } finally {
                    release.countDown();
                }
                caller.join(5_000);
                Assert.assertFalse("execute() must return once the request is encoded", caller.isAlive());
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, interrupted.errorStatus);

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (2)", null, next, false, 0);
                Assert.assertTrue("the next statement must get its own reply", next.execDone);
                Assert.assertEquals("the connection must be reused, not replaced", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testInterruptWithAStuckIoThreadGivesUpTheConnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> send(c, execDone(requestIdOf(frame)));
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                final long shutdownJoinMs = 300;
                setShutdownJoinMs(client, shutdownJoinMs);
                // The I/O thread picks the request up, and stays inside its SQL text
                // until it is interrupted: stuck.
                CountDownLatch reading = new CountDownLatch(1);
                CountDownLatch release = new CountDownLatch(1);
                CharSequence sql = new BlockingSql("INSERT INTO t VALUES (1)", reading, release);
                RecordingHandler interrupted = new RecordingHandler();
                Thread caller = new Thread(() -> client.execute(sql, null, interrupted, false, 0));
                caller.start();
                long elapsed;
                try {
                    Assert.assertTrue("the I/O thread must start encoding the request", reading.await(5, TimeUnit.SECONDS));
                    long start = System.nanoTime();
                    caller.interrupt();
                    caller.join(10_000);
                    elapsed = elapsedMs(start);
                } finally {
                    release.countDown();
                }
                Assert.assertFalse("execute() must return", caller.isAlive());
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, interrupted.errorStatus);
                Assert.assertTrue("execute() must first wait for the I/O thread, returned after " + elapsed + "ms",
                        elapsed >= shutdownJoinMs);
                Assert.assertTrue("a connection whose I/O thread is stuck must be given up", client.hasTerminalFailure());

                // failover=on (the default) replaces the connection for the next query.
                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (2)", null, next, false, 0);
                Assert.assertTrue("the next statement must get its own reply", next.execDone);
                Assert.assertEquals("a replacement connection must have been opened", 2, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testInterruptedQueryDoesNotAnswerTheNextQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    // Answers only after its caller has stopped waiting.
                    s.sendLater(c, 500, firstBatch(id, "late"), resultEnd(id, 1));
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                RecordingHandler first = new RecordingHandler();
                Thread caller = new Thread(() -> client.execute("SELECT s FROM t", null, first, false, 0));
                caller.start();
                long firstId = requestIdOf(script.queries.take());
                caller.interrupt();
                caller.join(5_000);
                Assert.assertFalse("execute() must return when its thread is interrupted", caller.isAlive());
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, first.errorStatus);
                byte[] cancel = script.cancels.poll(5, TimeUnit.SECONDS);
                Assert.assertNotNull("the abandoned query must be cancelled", cancel);
                Assert.assertEquals(firstId, requestIdOf(cancel));

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue("the next statement must get its own reply, got status=" + next.errorStatus
                        + ", message=" + next.errorMessage, next.execDone);
                Assert.assertEquals("the abandoned query's rows must not reach the next statement", 0, next.batches.get());
                Assert.assertEquals("the connection must be reused, not replaced", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testInterruptedQueryNotYetSentIsWithdrawn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    send(c, firstBatch(id, "a"));
                    // The rest arrives late; until then the I/O thread does not pick up
                    // the next request.
                    s.sendLater(c, 500, nextBatch(id, 1, "b"), resultEnd(id, 2));
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                try {
                    client.execute("SELECT s FROM t", null, failingOnBatch(new IllegalStateException("handler failed")), false, 0);
                    Assert.fail("the handler's exception must propagate out of execute()");
                } catch (IllegalStateException expected) {
                    // the first query is abandoned, its rest still on the way
                }

                // Queued behind the abandoned query's leftovers, and interrupted there.
                RecordingHandler interrupted = new RecordingHandler();
                Thread caller = new Thread(() -> client.execute("INSERT INTO t VALUES (2)", null, interrupted, false, 0));
                caller.start();
                awaitParked(caller);
                caller.interrupt();
                caller.join(5_000);
                Assert.assertFalse("execute() must return when its thread is interrupted", caller.isAlive());
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, interrupted.errorStatus);

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (3)", null, next, false, 0);
                Assert.assertTrue("the next statement must get its own reply", next.execDone);
                RecordingHandler last = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (4)", null, last, false, 0);
                Assert.assertTrue("the last statement must get its own reply", last.execDone);

                // The interrupted statement, withdrawn before it was sent, never reaches
                // the server; every other statement reaches it exactly once.
                List<String> received = new ArrayList<>();
                for (byte[] frame : script.queries) {
                    received.add(sqlOf(frame));
                }
                Assert.assertEquals(Arrays.asList("SELECT s FROM t", "INSERT INTO t VALUES (3)", "INSERT INTO t VALUES (4)"),
                        received);
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testLeftoversOfAnAbandonedQueryDoNotPostponeTheNextTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long timeoutMs = 100;
            final long graceMs = 200;
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    send(c, firstBatch(id, "a"));
                    // Streams on, ignoring the CANCEL, for 3 seconds: far past the
                    // next query's deadline and grace period.
                    for (int i = 1; i <= 150; i++) {
                        s.sendLater(c, 20L * i, nextBatch(id, i, "b"));
                    }
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.withQueryTimeoutGrace(graceMs);
                try {
                    client.execute("SELECT s FROM t", null, failingOnBatch(new IllegalStateException("handler failed")), false, 0);
                    Assert.fail("the handler's exception must propagate out of execute()");
                } catch (IllegalStateException expected) {
                    // the first query is abandoned and keeps streaming
                }

                RecordingHandler next = new RecordingHandler();
                long start = System.nanoTime();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, timeoutMs);
                long elapsed = elapsedMs(start);

                next.assertTimedOut();
                Assert.assertEquals("the abandoned query's rows must not reach the next statement", 0, next.batches.get());
                Assert.assertTrue("the leftovers must not postpone the timeout, took " + elapsed + "ms",
                        elapsed < 1_500);
                Assert.assertTrue("a connection still busy with the abandoned query is given up",
                        client.hasTerminalFailure());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testNoTimeoutFieldWithoutTheCapabilityOrTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> send(c, execDone(requestIdOf(frame)));
            try (TestWebSocketServer server = startServer(script, CAPS_FLAGS_ONLY);
                 QwpQueryClient client = connect(server, "")) {
                client.execute("SELECT 1", null, new RecordingHandler(), false, 30_000);
                Assert.assertNull("a server without CAP_QUERY_TIMEOUT must get no timeout field",
                        trailerOf(script.queries.take()));
            } finally {
                script.close();
            }

            ScriptedServer script2 = new ScriptedServer();
            script2.onQuery = (s, c, frame, n) -> send(c, execDone(requestIdOf(frame)));
            try (TestWebSocketServer server = startServer(script2, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.execute("SELECT 1", null, new RecordingHandler(), false, 0);
                Assert.assertNull("a query without a timeout must send no timeout field",
                        trailerOf(script2.queries.take()));
            } finally {
                script2.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testOlderServerIsCancelledAtTheDeadlineAndTheConnectionIsKept() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                if (n > 1) {
                    send(c, execDone(requestIdOf(frame)));
                }
                // n == 1: hold the query; only a CANCEL ends it.
            };
            script.onCancel = (s, c, frame, n) -> s.replyOnce(c, requestIdOf(frame), 0,
                    queryError(requestIdOf(frame), QwpConstants.STATUS_CANCELLED, "cancelled by client"));
            try (TestWebSocketServer server = startServer(script, CAPS_FLAGS_ONLY);
                 QwpQueryClient client = connect(server, "")) {
                RecordingHandler h = new RecordingHandler();
                long start = System.nanoTime();
                client.execute("SELECT slow()", null, h, false, 150);
                long elapsed = elapsedMs(start);

                h.assertTimedOut();
                Assert.assertTrue("the timeout cannot fire early, fired after " + elapsed + "ms", elapsed >= 150);
                Assert.assertTrue("the cancelled query must end promptly, took " + elapsed + "ms",
                        elapsed < QwpQueryClient.DEFAULT_QUERY_TIMEOUT_GRACE_MS);
                byte[] cancel = script.cancels.poll(5, TimeUnit.SECONDS);
                Assert.assertNotNull("an older server must be sent a CANCEL at the deadline", cancel);
                Assert.assertEquals("the CANCEL must target the timed-out query",
                        requestIdOf(script.queries.take()), requestIdOf(cancel));
                Assert.assertFalse(client.hasTerminalFailure());

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue(next.execDone);
                Assert.assertEquals("the connection must be reused, not replaced", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testReconnectWalkIsBoundedByTheQueryDeadline() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                if (n == 1) {
                    send(c, closeFrame());
                } else {
                    send(c, execDone(requestIdOf(frame)));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 BlackHole blackHole = new BlackHole()) {
                // The failover prefers the never-tried black hole over the endpoint
                // that just failed. It accepts TCP but never answers the upgrade, so
                // without the query deadline the walk would wait out auth_timeout_ms.
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ",localhost:" + blackHole.port() + ";auth_timeout_ms=5000;"
                        + "failover_backoff_initial_ms=0;failover_backoff_max_ms=0;")) {
                    client.connect();
                    RecordingHandler h = new RecordingHandler();
                    long start = System.nanoTime();
                    client.execute("INSERT INTO t VALUES (1)", null, h, false, 500);
                    long elapsed = elapsedMs(start);

                    h.assertTimedOut();
                    Assert.assertTrue("the reconnect must stop at the query deadline, took " + elapsed + "ms",
                            elapsed < 3_000);
                    Assert.assertEquals(1, blackHole.accepted());

                    // The cut-short reconnect left the client disconnected; the next
                    // query reconnects instead of failing with "not connected".
                    RecordingHandler next = new RecordingHandler();
                    client.execute("INSERT INTO t VALUES (2)", null, next, false, 0);
                    Assert.assertTrue("the next query must reconnect and complete: " + next.errorMessage, next.execDone);
                    Assert.assertTrue(client.isConnected());
                }
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testServerReportedTimeoutKeepsTheConnectionOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    s.sendLater(c, 150, queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT,
                            "timeout, query aborted [runtime=150ms, timeout=100ms]"));
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                RecordingHandler h = new RecordingHandler();
                client.execute("SELECT slow()", null, h, false, 100);

                h.assertTimedOut();
                Assert.assertTrue("the server's own report must reach the handler: " + h.errorMessage,
                        h.errorMessage.startsWith("timeout, query aborted"));
                Assert.assertTrue("the client must leave a server-enforced timeout to the server",
                        script.cancels.isEmpty());
                Assert.assertFalse(client.hasTerminalFailure());

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue(next.execDone);
                Assert.assertEquals("the authenticated connection must be reused", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testThrowingBatchHandlerDoesNotLeakRowsIntoTheNextQuery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    send(c, firstBatch(id, "a"));
                    // The rest of the result arrives after the handler has thrown.
                    s.sendLater(c, 300, nextBatch(id, 1, "b"), resultEnd(id, 2));
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                final IllegalStateException failure = new IllegalStateException("handler failed");
                try {
                    client.execute("SELECT s FROM t", null, failingOnBatch(failure), false, 0);
                    Assert.fail("the handler's exception must propagate out of execute()");
                } catch (IllegalStateException e) {
                    Assert.assertSame(failure, e);
                }
                long firstId = requestIdOf(script.queries.take());
                byte[] cancel = script.cancels.poll(5, TimeUnit.SECONDS);
                Assert.assertNotNull("the abandoned query must be cancelled", cancel);
                Assert.assertEquals(firstId, requestIdOf(cancel));

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertEquals("the abandoned query's rows must not reach the next statement", 0, next.batches.get());
                Assert.assertTrue("the next statement must get its own reply, got status=" + next.errorStatus
                        + ", message=" + next.errorMessage, next.execDone);
                Assert.assertFalse(next.ended);
                Assert.assertEquals("the connection must be reused, not replaced", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testThrowingHandlerIsRethrownWhenTheConnectionIsGivenUp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long timeoutMs = 100;
            final long graceMs = 200;
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                if (n > 1) {
                    send(c, execDone(requestIdOf(frame)));
                }
                // n == 1: never answered, not even after a CANCEL.
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.withQueryTimeoutGrace(graceMs);
                // An Error, not an exception: the handler's throwable must come back as is.
                final Error failure = new Error("handler failed");
                QwpColumnBatchHandler throwing = new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        throw failure;
                    }
                };
                Throwable thrown = null;
                long start = System.nanoTime();
                try {
                    client.execute("SELECT slow()", null, throwing, false, timeoutMs);
                } catch (Throwable t) {
                    thrown = t;
                }
                long elapsed = elapsedMs(start);

                Assert.assertSame("the handler's throwable must propagate unwrapped", failure, thrown);
                Assert.assertTrue("execute() must give up on the connection before rethrowing, took " + elapsed + "ms",
                        elapsed >= timeoutMs + 2 * graceMs);
                Assert.assertTrue("a silent connection must be marked failed", client.hasTerminalFailure());

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue(next.execDone);
                Assert.assertEquals("a replacement connection must have been opened", 2, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testThrowingHandlerStillDrainsTheTimedOutQuery() throws Exception {
        // The handler contract lets any callback throw and keeps the connection
        // usable. At the end of the grace period onError fires while the aborted
        // query is still running, so a throw there must not skip the drain:
        // otherwise the aborted query's late reply answers the next query.
        TestUtils.assertMemoryLeak(() -> {
            final long timeoutMs = 100;
            final long graceMs = 1_000;
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                long id = requestIdOf(frame);
                if (n == 1) {
                    // Ends the query halfway through the second grace period: after the
                    // handler is told about the timeout, before the connection is given up.
                    s.sendLater(c, timeoutMs + graceMs + graceMs / 2,
                            queryError(id, QwpConstants.STATUS_QUERY_TIMEOUT, "timeout, query aborted"));
                } else {
                    send(c, execDone(id));
                }
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.withQueryTimeoutGrace(graceMs);
                QwpColumnBatchHandler throwing = new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        throw new IllegalStateException(message);
                    }
                };
                try {
                    client.execute("SELECT slow()", null, throwing, false, timeoutMs);
                    Assert.fail("the handler's exception must propagate out of execute()");
                } catch (IllegalStateException e) {
                    Assert.assertTrue("the handler must throw at the end of the grace period, while the query "
                            + "is still running: " + e.getMessage(), e.getMessage().contains("grace period"));
                }

                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue("the next statement must get its own reply, not the aborted query's: status="
                        + next.errorStatus + ", message=" + next.errorMessage, next.execDone);
                Assert.assertEquals("the drained connection must be reused", 1, server.handshakeCount());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTimeoutFieldCarriesTheBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> send(c, execDone(requestIdOf(frame)));
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "query_timeout_ms=20000;")) {
                Assert.assertEquals(20_000, client.getQueryTimeoutMs());

                client.execute("SELECT 1", null, new RecordingHandler(), false, 30_000);
                long[] trailer = trailerOf(script.queries.take());
                Assert.assertNotNull(trailer);
                Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_TIMEOUT, trailer[0]);
                Assert.assertTrue("timeout_ms must carry the budget left: " + trailer[1],
                        trailer[1] > 29_000 && trailer[1] <= 30_000);

                // The overloads without a timeout apply the configured default, and
                // the field composes with the other query flags.
                client.execute("SELECT 1", new RecordingHandler(), true);
                trailer = trailerOf(script.queries.take());
                Assert.assertNotNull(trailer);
                Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_RESET_DICT | QwpEgressMsgKind.QUERY_FLAG_TIMEOUT,
                        trailer[0]);
                Assert.assertTrue("the configured default must apply: " + trailer[1],
                        trailer[1] > 19_000 && trailer[1] <= 20_000);
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testTimeoutWhileFailingOverIsReportedAsTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            // Every attempt fails at the transport level.
            script.onQuery = (s, c, frame, n) -> send(c, closeFrame());
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server,
                         "failover_max_attempts=1000000;failover_backoff_initial_ms=10;failover_backoff_max_ms=20;")) {
                RecordingHandler h = new RecordingHandler();
                long start = System.nanoTime();
                client.execute("INSERT INTO t VALUES (1)", null, h, false, 300);
                long elapsed = elapsedMs(start);

                h.assertTimedOut();
                Assert.assertTrue("failover must stop at the deadline, took " + elapsed + "ms",
                        elapsed >= 300 && elapsed < 3_000);
                Assert.assertTrue("there must have been replays", script.queryCount.get() > 1);
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUnresponsiveConnectionIsReplaced() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                if (n > 1) {
                    send(c, execDone(requestIdOf(frame)));
                }
                // n == 1: never answered, not even after a CANCEL.
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "")) {
                client.withQueryTimeoutGrace(200);
                RecordingHandler h = new RecordingHandler();
                long start = System.nanoTime();
                client.execute("SELECT slow()", null, h, false, 100);
                long elapsed = elapsedMs(start);

                h.assertTimedOut();
                Assert.assertEquals(1, h.terminals.get());
                Assert.assertTrue("execute() must give up after two grace periods, took " + elapsed + "ms",
                        elapsed >= 100 + 2 * 200 && elapsed < 5_000);
                Assert.assertTrue("a silent connection must be marked failed", client.hasTerminalFailure());

                // failover=on (the default) replaces the connection for the next query.
                RecordingHandler next = new RecordingHandler();
                client.execute("INSERT INTO t VALUES (1)", null, next, false, 0);
                Assert.assertTrue(next.execDone);
                Assert.assertEquals("a replacement connection must have been opened", 2, server.handshakeCount());
                Assert.assertFalse(client.hasTerminalFailure());
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUnresponsiveConnectionWithFailoverOffReportsTheFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                // never answered
            };
            try (TestWebSocketServer server = startServer(script, CAPS_WITH_TIMEOUT);
                 QwpQueryClient client = connect(server, "failover=off;")) {
                client.withQueryTimeoutGrace(100);
                RecordingHandler h = new RecordingHandler();
                client.execute("SELECT slow()", null, h, false, 100);
                h.assertTimedOut();
                Assert.assertTrue(client.hasTerminalFailure());

                RecordingHandler next = new RecordingHandler();
                client.execute("SELECT 1", null, next, false, 0);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, next.errorStatus);
                Assert.assertTrue(next.errorMessage, next.errorMessage.contains("stopped responding"));
            } finally {
                script.close();
            }
        });
    }

    @Test(timeout = 30_000)
    public void testUserCancelBeforeTheDeadlineIsReportedAsCancel() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ScriptedServer script = new ScriptedServer();
            script.onQuery = (s, c, frame, n) -> {
                // held until cancelled
            };
            // The cancel is acknowledged only after the deadline has passed.
            script.onCancel = (s, c, frame, n) -> s.replyOnce(c, requestIdOf(frame), 400,
                    queryError(requestIdOf(frame), QwpConstants.STATUS_CANCELLED, "cancelled by client"));
            try (TestWebSocketServer server = startServer(script, CAPS_FLAGS_ONLY);
                 QwpQueryClient client = connect(server, "")) {
                RecordingHandler h = new RecordingHandler();
                Thread canceller = new Thread(() -> {
                    try {
                        script.queries.take();
                        client.cancel();
                    } catch (InterruptedException ignore) {
                        Thread.currentThread().interrupt();
                    }
                });
                canceller.start();
                client.execute("SELECT slow()", null, h, false, 200);
                canceller.join();

                Assert.assertEquals("the user's cancel came first and must be reported as such: " + h.errorMessage,
                        QwpConstants.STATUS_CANCELLED, h.errorStatus);
            } finally {
                script.close();
            }
        });
    }

    /**
     * Waits until {@code thread} blocks, here: parked waiting for its query's
     * events, with the request queued.
     */
    private static void awaitParked(Thread thread) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.WAITING && thread.getState() != Thread.State.TIMED_WAITING) {
            Assert.assertTrue("the thread must block", System.nanoTime() - deadlineNanos < 0);
            Thread.sleep(1);
        }
    }

    private static byte[] closeFrame() {
        // Marker understood by ScriptedServer's send(): close the connection.
        return new byte[0];
    }

    private static QwpQueryClient connect(TestWebSocketServer server, String extraConfig) {
        QwpQueryClient client = QwpQueryClient.fromConfig(
                "ws::addr=localhost:" + server.getPort() + ";auth_timeout_ms=2000;" + extraConfig);
        try {
            client.connect();
        } catch (RuntimeException e) {
            client.close();
            throw e;
        }
        return client;
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static byte[] execDone(long requestId) {
        return serverFrame(QwpEgressMsgKind.EXEC_DONE, requestId, 0, new byte[]{0, 0}); // op_type, rows_affected
    }

    private static QwpColumnBatchHandler failingOnBatch(RuntimeException failure) {
        return new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                throw failure;
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
            }
        };
    }

    /**
     * RESULT_BATCH {@code batch_seq == 0}: the schema (one VARCHAR column) and a
     * single row.
     */
    private static byte[] firstBatch(long requestId, String value) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        putVarint(body, 0);       // batch_seq
        putVarint(body, 0);       // table name length
        putVarint(body, 1);       // row_count
        putVarint(body, 1);       // column_count
        putVarint(body, 1);       // column name length
        body.write('s');
        body.write(QwpConstants.TYPE_VARCHAR);
        putVarcharCell(body, value);
        return serverFrame(QwpEgressMsgKind.RESULT_BATCH, requestId, 1, body.toByteArray());
    }

    /**
     * Continuation RESULT_BATCH ({@code batch_seq > 0}): rows only, against the
     * schema of {@link #firstBatch}.
     */
    private static byte[] nextBatch(long requestId, long batchSeq, String value) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        putVarint(body, batchSeq);
        putVarint(body, 0);       // table name length
        putVarint(body, 1);       // row_count
        putVarcharCell(body, value);
        return serverFrame(QwpEgressMsgKind.RESULT_BATCH, requestId, 1, body.toByteArray());
    }

    private static void putVarcharCell(ByteArrayOutputStream body, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        body.write(0);            // null_flag: no nulls
        ByteBuffer offsets = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        offsets.putInt(0).putInt(bytes.length);
        body.write(offsets.array(), 0, 8);
        body.write(bytes, 0, bytes.length);
    }

    private static void putVarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static byte[] queryError(long requestId, byte status, String message) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        ByteBuffer body = ByteBuffer.allocate(1 + 2 + msg.length).order(ByteOrder.LITTLE_ENDIAN);
        body.put(status).putShort((short) msg.length).put(msg);
        return serverFrame(QwpEgressMsgKind.QUERY_ERROR, requestId, 0, body.array());
    }

    private static long readVarint(byte[] buf, int[] pos) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = buf[pos[0]++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }

    private static long requestIdOf(byte[] clientFrame) {
        return ByteBuffer.wrap(clientFrame, 1, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private static byte[] resultEnd(long requestId, long totalRows) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        putVarint(body, 0);       // final_seq
        putVarint(body, totalRows);
        return serverFrame(QwpEgressMsgKind.RESULT_END, requestId, 0, body.toByteArray());
    }

    private static void send(TestWebSocketServer.ClientHandler client, byte[] frame) {
        try {
            if (frame.length == 0) {
                client.sendClose(1011, "scripted failure");
            } else {
                client.sendBinary(frame);
            }
        } catch (IOException ignore) {
            // the client went away; the test's assertions report the outcome
        }
    }

    private static byte[] serverFrame(byte msgKind, long requestId, int tableCount, byte[] body) {
        int payloadLen = 1 + 8 + body.length;
        ByteBuffer bb = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payloadLen).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(QwpConstants.MAGIC_MESSAGE);
        bb.put(QwpConstants.VERSION);
        bb.put((byte) 0);                 // flags
        bb.putShort((short) tableCount);
        bb.putInt(payloadLen);
        bb.put(msgKind);
        bb.putLong(requestId);
        bb.put(body);
        return bb.array();
    }

    private static void setShutdownJoinMs(QwpQueryClient client, long millis) throws Exception {
        Field field = QwpQueryClient.class.getDeclaredField("shutdownJoinMs");
        field.setAccessible(true);
        field.setLong(client, millis);
    }

    /**
     * The SQL text of a captured {@code QUERY_REQUEST}.
     */
    private static String sqlOf(byte[] queryRequest) {
        int[] p = {1 + 8};
        int len = (int) readVarint(queryRequest, p);
        return new String(queryRequest, p[0], len, StandardCharsets.UTF_8);
    }

    private static TestWebSocketServer startServer(ScriptedServer script, int capabilities) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(script);
        server.setSendServerInfo(true);
        server.setCapabilities(capabilities);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    /**
     * Decodes the trailer of a captured {@code QUERY_REQUEST}: {@code {query_flags,
     * timeout_ms}}, with {@code timeout_ms == -1} when the timeout flag is not
     * set, or {@code null} when the frame carries no trailer at all. Assumes no
     * binds.
     */
    private static long[] trailerOf(byte[] f) {
        Assert.assertEquals(QwpEgressMsgKind.QUERY_REQUEST, f[0]);
        int[] p = {1 + 8};
        long sqlLen = readVarint(f, p);
        p[0] += (int) sqlLen;
        readVarint(f, p);         // initial_credit
        Assert.assertEquals("test frames carry no binds", 0L, readVarint(f, p));
        if (p[0] >= f.length) {
            return null;
        }
        long flags = readVarint(f, p);
        long timeoutMs = (flags & QwpEgressMsgKind.QUERY_FLAG_TIMEOUT) != 0 ? readVarint(f, p) : -1L;
        Assert.assertEquals("the trailer must end the frame", f.length, p[0]);
        return new long[]{flags, timeoutMs};
    }

    @FunctionalInterface
    private interface Script {
        void run(ScriptedServer server, TestWebSocketServer.ClientHandler client, byte[] frame, int queryNumber);
    }

    /**
     * A TCP listener that accepts connections and never answers them -- an
     * endpoint whose WebSocket upgrade hangs.
     */
    private static final class BlackHole implements AutoCloseable {
        private final List<Socket> accepted = Collections.synchronizedList(new ArrayList<>());
        private final ServerSocket socket;
        private final Thread thread;
        private volatile boolean running = true;

        BlackHole() throws IOException {
            socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            socket.setSoTimeout(50);
            thread = new Thread(() -> {
                while (running) {
                    try {
                        accepted.add(socket.accept());
                    } catch (SocketTimeoutException ignore) {
                        // poll the running flag
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "black-hole-accept");
            thread.setDaemon(true);
            thread.start();
        }

        int accepted() {
            return accepted.size();
        }

        @Override
        public void close() throws Exception {
            running = false;
            thread.join(5_000);
            socket.close();
            synchronized (accepted) {
                for (Socket s : accepted) {
                    s.close();
                }
            }
        }

        int port() {
            return socket.getLocalPort();
        }
    }

    /**
     * SQL text that holds up whoever reads its first character -- the I/O
     * thread, which reads it while encoding the request -- until released or
     * interrupted.
     */
    private static final class BlockingSql implements CharSequence {
        private final CountDownLatch reading;
        private final CountDownLatch release;
        private final String text;

        BlockingSql(String text, CountDownLatch reading, CountDownLatch release) {
            this.text = text;
            this.reading = reading;
            this.release = release;
        }

        @Override
        public char charAt(int index) {
            if (index == 0) {
                reading.countDown();
                try {
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return text.charAt(index);
        }

        @Override
        public int length() {
            return text.length();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return text.subSequence(start, end);
        }

        @Override
        public String toString() {
            return text;
        }
    }

    private static final class RecordingHandler implements QwpColumnBatchHandler {
        final AtomicInteger batches = new AtomicInteger();
        final AtomicInteger failoverResets = new AtomicInteger();
        final AtomicInteger terminals = new AtomicInteger();
        volatile boolean ended;
        volatile String errorMessage;
        volatile byte errorStatus;
        volatile boolean execDone;
        volatile long terminalNanos;

        @Override
        public void onBatch(QwpColumnBatch batch) {
            batches.incrementAndGet();
        }

        @Override
        public void onEnd(long totalRows) {
            ended = true;
            terminal();
        }

        @Override
        public void onError(byte status, String message) {
            errorStatus = status;
            errorMessage = message;
            terminal();
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            execDone = true;
            terminal();
        }

        @Override
        public void onFailoverReset(QwpServerInfo newNode) {
            failoverResets.incrementAndGet();
        }

        void assertTimedOut() {
            Assert.assertEquals("expected a query timeout, got status=" + errorStatus + ", message=" + errorMessage,
                    QwpConstants.STATUS_QUERY_TIMEOUT, errorStatus);
            Assert.assertFalse("a timed-out query must not also complete", ended || execDone);
        }

        private void terminal() {
            terminalNanos = System.nanoTime();
            terminals.incrementAndGet();
        }
    }

    /**
     * Mock egress server scripted per test. Records every {@code QUERY_REQUEST}
     * and {@code CANCEL}, and replies like a real server: one terminal frame per
     * request, so a cancel of a query that already ended gets no reply.
     */
    private static final class ScriptedServer implements TestWebSocketServer.WebSocketServerHandler, AutoCloseable {
        final LinkedBlockingQueue<byte[]> cancels = new LinkedBlockingQueue<>();
        final LinkedBlockingQueue<byte[]> queries = new LinkedBlockingQueue<>();
        final AtomicInteger queryCount = new AtomicInteger();
        private final Set<Long> answered = ConcurrentHashMap.newKeySet();
        private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scripted-egress-server");
            t.setDaemon(true);
            return t;
        });
        volatile long lastSendNanos;
        volatile Script onCancel;
        volatile Script onQuery;

        @Override
        public void close() {
            scheduler.shutdownNow();
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length == 0) {
                return;
            }
            if (data[0] == QwpEgressMsgKind.QUERY_REQUEST) {
                int n = queryCount.incrementAndGet();
                queries.add(data);
                Script script = onQuery;
                if (script != null) {
                    script.run(this, client, data, n);
                }
            } else if (data[0] == QwpEgressMsgKind.CANCEL) {
                cancels.add(data);
                Script script = onCancel;
                if (script != null) {
                    script.run(this, client, data, 0);
                }
            }
        }

        void closeLater(TestWebSocketServer.ClientHandler client, long delayMs) {
            sendLater(client, delayMs, closeFrame());
        }

        // Sends the terminal frame for requestId unless one was already sent.
        void replyOnce(TestWebSocketServer.ClientHandler client, long requestId, long delayMs, byte[] frame) {
            if (answered.add(requestId)) {
                sendLater(client, delayMs, frame);
            }
        }

        void sendLater(TestWebSocketServer.ClientHandler client, long delayMs, byte[]... frames) {
            scheduler.schedule(() -> {
                lastSendNanos = System.nanoTime();
                for (byte[] frame : frames) {
                    send(client, frame);
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }
}
