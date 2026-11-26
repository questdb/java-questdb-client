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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.DurableAckTiers;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import io.questdb.client.test.tools.TestWebSocketClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * The rollback control frame on a connection whose server did not confirm
 * {@code X-QWP-Rollback}. The producer publishes the frame without waiting for
 * its ack, so a reconnect can reach a server without rollback support, such as
 * an older node after a failover, before the frame is acked. That server would
 * read it as an empty deferred frame and commit the rolled-back transaction
 * with the next commit frame.
 * <p>
 * Contract under test: the I/O loop never sends the rollback frame to such a
 * server. Once the server acked every commit-bearing frame sent before it, the
 * loop acknowledges the rollback frame itself and recycles the connection,
 * which discards the rolled-back frames server-side, and the next connection
 * starts past the rollback frame. A server that confirmed rollback receives the
 * frame. The same holds for a log recovered after a restart and for the
 * background drainer, which replay through the same loop.
 * <p>
 * {@link ServerModel} is the oracle: it commits what the QuestDB server commits,
 * and drops the frames a closed connection still had in flight, so a recycle
 * before the acks of earlier commits shows as a lost commit.
 */
public class CursorWebSocketSendLoopRollbackTest {
    // Random transaction logs per run of the fuzz test; raise it for a local soak.
    private static final int FUZZ_ITERATIONS = Integer.getInteger("questdb.fuzz.rollback.iterations", 100);
    // The message the model server refuses a strict frame with.
    private static final String REFUSAL = "strict transaction too large";
    private static final long SEGMENT_SIZE_BYTES = 64 * 1024;
    private static final String TABLE = "t";

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testBackgroundDrainerRetiresRollbackOnServerWithoutRollback() throws Exception {
        // An orphaned slot whose producer rolled back a transaction and committed the
        // next one before it went away, with nothing acked yet.
        String slotPath = temporaryFolder.newFolder("slot").getAbsolutePath();
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_SIZE_BYTES)) {
                appendCommit(engine, 1);
                appendDeferred(engine, 2);
                appendRollback(engine);
                appendDeferred(engine, 3);
                appendCommit(engine, 4);
            }
            ServerModel server = new ServerModel();
            List<ModelClient> clients = new ArrayList<>();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_SIZE_BYTES,
                    1L << 20,
                    () -> newClient(server, clients, false, false),
                    5_000L,
                    1L,
                    5L,
                    DurableAckTiers.NONE,
                    200L);

            // A drainer retries until it drains, so bound the run: a slot that never
            // drains fails the test instead of hanging it.
            Thread drain = new Thread(drainer::run, "rollback-drainer");
            drain.start();
            drain.join(10_000);
            if (drain.isAlive()) {
                drainer.requestStop();
                drain.join();
                Assert.fail("the slot did not drain: " + server.received());
            }

            Assert.assertEquals(BackgroundDrainer.DrainOutcome.SUCCESS, drainer.outcome());
            Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "1:D3", "1:C4"), server.received());
            Assert.assertEquals(Arrays.asList("C1", "D3", "C4"), server.committed());
            Assert.assertEquals(2, clientCount(clients));
        });
    }

    @Test
    public void testConsecutiveRollbacksRecycleOnceEach() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeferred(engine, 1);
                appendRollback(engine);
                appendDeferred(engine, 2);
                appendRollback(engine);
                appendDeferred(engine, 3);
                appendCommit(engine, 4);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, false), 5L);
                Assert.assertEquals(Arrays.asList("0:D1", "1:D2", "2:D3", "2:C4"), server.received());
                Assert.assertEquals(Arrays.asList("D3", "C4"), server.committed());
                Assert.assertEquals(3, clientCount(clients));
            }
        });
    }

    @Test
    public void testRandomTransactionsOverServersWithAndWithoutRollback() throws Exception {
        // Transactions of zero to three deferred frames, each closed by a commit frame or
        // a rollback frame, over connections that reach a server with or without rollback
        // support at random. Each rollback frame goes out to a server that confirmed
        // rollback, or costs exactly one recycle; only committed transactions commit,
        // each once.
        long s0 = System.nanoTime();
        long s1 = System.currentTimeMillis();
        Rnd rnd = TestUtils.generateRandom(null, s0, s1);
        try {
            TestUtils.assertMemoryLeak(() -> {
                for (int iteration = 0; iteration < FUZZ_ITERATIONS; iteration++) {
                    runRandomTransactions(rnd);
                }
            });
        } catch (Throwable t) {
            throw new AssertionError("seeds=" + s0 + "L, " + s1 + "L", t);
        }
    }

    @Test
    public void testRecoveredLogRetiresRollbackOnServerWithoutRollback() throws Exception {
        // A restart recovers the log with the rollback frame between two transactions, so
        // the orphaned-tail retirement does not apply: the frame is replayed like any other.
        String dir = temporaryFolder.newFolder("recovered").getAbsolutePath();
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(dir, SEGMENT_SIZE_BYTES)) {
                appendCommit(engine, 1);
                appendDeferred(engine, 2);
                appendRollback(engine);
                appendDeferred(engine, 3);
                appendCommit(engine, 4);
            }
            try (CursorSendEngine engine = new CursorSendEngine(dir, SEGMENT_SIZE_BYTES)) {
                Assert.assertTrue(engine.wasRecoveredFromDisk());
                Assert.assertEquals("no orphaned tail", -1L, engine.recoveredOrphanTipFsn());
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, false), 4L);
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "1:D3", "1:C4"), server.received());
                Assert.assertEquals(Arrays.asList("C1", "D3", "C4"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testRollbackAtReplayStartRetiresBeforeSendingAnything() throws Exception {
        // The frames ahead of the rollback frame are acked already, as when the server
        // committed the transaction's rows itself at its uncommitted-row cap, so the
        // rollback frame is the first frame of the connection.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeferred(engine, 1);
                appendRollback(engine);
                appendDeferred(engine, 2);
                appendCommit(engine, 3);
                engine.acknowledge(0L);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, false), 3L);
                Assert.assertEquals(Arrays.asList("1:D2", "1:C3"), server.received());
                Assert.assertEquals(Arrays.asList("D2", "C3"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testRollbackFrameGoesToServerWithRollback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendCommit(engine, 1);
                appendDeferred(engine, 2);
                appendDeferred(engine, 3);
                appendRollback(engine);
                appendDeferred(engine, 4);
                appendCommit(engine, 5);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, true), 5L);
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "0:R", "0:D4", "0:C5"), server.received());
                Assert.assertEquals(Arrays.asList("C1", "D4", "C5"), server.committed());
                Assert.assertEquals(1, clientCount(clients));
                // Nothing changes on this path: the whole log goes out in one write.
                Assert.assertEquals(1, clients.get(0).flushes);
            }
        });
    }

    @Test
    public void testRollbackFrameNeverReachesServerWithoutRollback() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendCommit(engine, 1);
                appendDeferred(engine, 2);
                appendDeferred(engine, 3);
                appendRollback(engine);
                appendDeferred(engine, 4);
                appendCommit(engine, 5);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, false), 5L);
                // The frames ahead of the rollback frame went out in one batch with it
                // held back; closing the connection discarded 2 and 3, and the next
                // connection starts past the rollback frame.
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "1:D4", "1:C5"), server.received());
                Assert.assertEquals(Arrays.asList("C1", "D4", "C5"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
                Assert.assertEquals(1, clients.get(0).flushes);
                Assert.assertEquals(1, clients.get(1).flushes);
            }
        });
    }

    @Test
    public void testRollbackSupportIsDecidedPerConnection() throws Exception {
        // The first connection reaches a server without rollback support, the second one
        // a server with it: the second rollback frame goes out.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeferred(engine, 1);
                appendRollback(engine);
                appendDeferred(engine, 2);
                appendRollback(engine);
                appendDeferred(engine, 3);
                appendCommit(engine, 4);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                boolean[] isRollbackConfirmed = {false, true};
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0L,
                        () -> newClient(server, clients, isRollbackConfirmed[clientCount(clients)], false),
                        1L, 5L);
                runToAck(engine, loop, 5L);
                Assert.assertEquals(Arrays.asList("0:D1", "1:D2", "1:R", "1:D3", "1:C4"), server.received());
                Assert.assertEquals(Arrays.asList("D3", "C4"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testOnlyTheRollbackFrameIsHeldBack() throws Exception {
        // Bare 12-byte headers that are not the rollback frame -- an empty commit, an empty
        // deferred frame and a foreign payload -- go out like any frame. Mistaking the
        // empty commit for a rollback frame would discard the transaction it commits.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendDeferred(engine, 1);
                appendHeader(engine, QwpConstants.MAGIC_MESSAGE, (byte) 0);
                appendHeader(engine, QwpConstants.MAGIC_MESSAGE, QwpConstants.FLAG_DEFER_COMMIT);
                appendHeader(engine, 0, QwpConstants.ROLLBACK_FRAME_FLAGS);
                appendDeferred(engine, 2);
                appendRollback(engine);
                appendDeferred(engine, 3);
                appendCommit(engine, 4);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, false), 7L);
                Assert.assertEquals(Arrays.asList("0:D1", "0:C0", "0:D0", "0:X12", "0:D2", "1:D3", "1:C4"),
                        server.received());
                Assert.assertEquals(Arrays.asList("D1", "C0", "D0", "X12", "D3", "C4"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testRollbackWaitsForCommitAcksBeforeRecycling() throws Exception {
        // The server holds the frames ahead of the rollback frame unprocessed. Recycling
        // now would lose commit 1 with the connection: the self-acknowledge of the
        // rollback frame covers it, so it would never be replayed.
        assertRollbackWaitsForAckOf(false, engine -> appendCommit(engine, 1), "C1");
    }

    @Test
    public void testRollbackWaitsForDurableAckBeforeRecycling() throws Exception {
        // In durable-ack mode the trim watermark advances only on the durable ack, which
        // the server holds back. The OK of commit 1 alone must not let the loop recycle,
        // or it would trim commit 1 before the server made it durable.
        assertRollbackWaitsForAckOf(true, engine -> appendCommit(engine, 1), "C1");
    }

    @Test
    public void testRollbackWaitsForForeignFrameAck() throws Exception {
        // A payload that is not a QWP message cannot be told to be deferred, so the loop
        // waits for its ack like for a commit's: the safe side.
        assertRollbackWaitsForAckOf(false, engine -> appendForeign(engine, 16), "X16");
    }

    @Test
    public void testRollbackWaitsForShortFrameAck() throws Exception {
        // Same for a payload too short to hold a QWP header.
        assertRollbackWaitsForAckOf(false, engine -> appendForeign(engine, 8), "X8");
    }

    @Test
    public void testCommittedRefusedTransactionHaltsTheLoop() throws Exception {
        // The producer committed the transaction before the refusal of its frame 3
        // arrived. Being strict, the transaction cannot commit in parts either, so the
        // loop halts with the refusal over its frames, which stay in the log, unacked.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                appendStrict(engine, true, 2, 6);
                appendStrict(engine, true, 3, 6);
                appendStrict(engine, false, 4, 1);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, server, clients, true);
                try {
                    loop.start();
                    awaitTrue("the loop halts", () -> loop.getTerminalError() != null);
                    assertRefusalHalted(loop, 1L, 3L, 2L);
                    Assert.assertEquals(-1L, loop.getRefusedTransactionFsn());
                } finally {
                    loop.close();
                }
                Assert.assertEquals("the transaction stays in the log", 0L, engine.ackedFsn());
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "0:C4"), server.received());
                Assert.assertEquals(Collections.singletonList("0:D3"), server.refused());
                Assert.assertEquals(Collections.singletonList("C1"), server.committed());
                Assert.assertEquals(1, clientCount(clients));
            }
        });
    }

    @Test
    public void testRefusalOfAFrameTheServerAckedHalts() throws Exception {
        // A server that acked a frame cannot refuse it: there is no transaction to walk,
        // so the loop halts rather than guess which frames the refusal covers. In
        // durable-ack mode the OK moves only the OK watermark, so the refusal does not
        // read as one of a dictionary catch-up below the replay start.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, true, 1, 1);
                appendStrict(engine, false, 2, 1);
                RecordingClient client = new RecordingClient();
                CursorWebSocketSendLoop loop = newManualLoop(engine, client, null, DurableAckTiers.REPLICATED);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    Assert.assertTrue(loop.trySendBatchForTest());
                    deliverOk(loop, 1L);
                    deliverRefusal(loop, 1L);
                    assertRefusalHalted(loop, 1L, 1L, 1L);
                    Assert.assertEquals(-1L, loop.getRefusedTransactionFsn());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRefusalPastTheEndOfTheTransactionHalts() throws Exception {
        // The server refuses frame 2, but the unacked transaction that starts at frame 0
        // ends at the rollback frame 1, ahead of it: the refusal is not about the frames
        // the loop would discard, so it halts instead.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, true, 1, 1);
                appendRollback(engine);
                appendStrict(engine, true, 2, 1);
                RecordingClient client = new RecordingClient();
                CursorWebSocketSendLoop loop = newManualLoop(engine, client, null, DurableAckTiers.NONE);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(Arrays.asList("D1", "R", "D2"), client.log);
                    deliverRefusal(loop, 2L);
                    Assert.assertEquals(0L, loop.getRefusedTransactionFsn());
                    Assert.assertEquals(REFUSAL, loop.getRefusedTransactionMessage());
                    Assert.assertFalse(loop.trySendBatchForTest());
                    assertRefusalHalted(loop, 0L, 1L, 2L);
                    Assert.assertEquals("nothing goes out again", 3, client.log.size());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRefusedOpenTransactionWaitsForItsEnd() throws Exception {
        // The refusal arrives while the transaction is open. The loop sends nothing more
        // and reports the refusal to the producer, which ends the transaction: here with
        // a rollback frame, after which the loop discards the transaction and the next
        // connection carries on with the next transaction.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                appendStrict(engine, true, 2, 6);
                appendStrict(engine, true, 3, 6);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, server, clients, true);
                try {
                    loop.start();
                    awaitTrue("the server refuses frame 3", () -> loop.getRefusedTransactionFsn() == 1L);
                    Assert.assertEquals(REFUSAL, loop.getRefusedTransactionMessage());
                    ModelClient first = client(clients, 0);
                    int polls = first.polls;
                    awaitTrue("the loop keeps waiting", () -> first.polls >= polls + 50);
                    Assert.assertEquals(1, clientCount(clients));
                    Assert.assertEquals(0L, engine.ackedFsn());
                    loop.checkError();

                    appendRollback(engine);
                    appendStrict(engine, true, 4, 2);
                    appendStrict(engine, false, 5, 1);
                    awaitAckedFsn(engine, 5L);
                    Assert.assertEquals(-1L, loop.getRefusedTransactionFsn());
                    Assert.assertNull(loop.getRefusedTransactionMessage());
                } finally {
                    loop.close();
                }
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "1:D4", "1:C5"), server.received());
                Assert.assertEquals(Collections.singletonList("0:D3"), server.refused());
                Assert.assertEquals(Arrays.asList("C1", "D4", "C5"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testRefusedTransactionRolledBackIsDiscarded() throws Exception {
        // The server refuses strict frame 3, whose rows take the transaction to its cap of
        // 10, and processes nothing after it on that connection. The producer had rolled
        // the transaction back already: the loop discards the transaction's frames and
        // the rollback frame, and the next connection starts with the next transaction.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                appendStrict(engine, true, 2, 6);
                appendStrict(engine, true, 3, 6);
                appendRollback(engine);
                appendStrict(engine, true, 4, 2);
                appendStrict(engine, false, 5, 1);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, true), 5L);
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "0:R", "0:D4", "0:C5", "1:D4", "1:C5"),
                        server.received());
                Assert.assertEquals(Collections.singletonList("0:D3"), server.refused());
                Assert.assertEquals(Arrays.asList("C1", "D4", "C5"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testRefusedTransactionStaysUnsentAcrossReconnects() throws Exception {
        // The connection drops while the loop waits for the refused transaction to end.
        // The next connection replays from the transaction's first frame, which the loop
        // walks again instead of sending it.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                appendStrict(engine, true, 2, 6);
                appendStrict(engine, true, 3, 6);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, server, clients, true);
                try {
                    loop.start();
                    awaitTrue("the server refuses frame 3", () -> loop.getRefusedTransactionFsn() == 1L);
                    client(clients, 0).closeFromServer();
                    awaitTrue("the loop reconnects", () -> clientCount(clients) == 2);
                    ModelClient second = client(clients, 1);
                    int polls = second.polls;
                    awaitTrue("the loop keeps waiting on the new connection", () -> second.polls >= polls + 50);
                    Assert.assertEquals("the refused transaction does not go out again", 0, second.flushedFrames);
                    Assert.assertEquals(1L, loop.getRefusedTransactionFsn());

                    appendRollback(engine);
                    appendStrict(engine, true, 4, 2);
                    appendStrict(engine, false, 5, 1);
                    awaitAckedFsn(engine, 5L);
                } finally {
                    loop.close();
                }
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "2:D4", "2:C5"), server.received());
                Assert.assertEquals(Arrays.asList("C1", "D4", "C5"), server.committed());
                Assert.assertEquals(3, clientCount(clients));
            }
        });
    }

    @Test
    public void testRefusedTransactionWaitsForDurableAckBeforeDiscarding() throws Exception {
        // Discarding the refused transaction acknowledges its frames, an acknowledgement
        // that also covers commit 1 ahead of them. In durable-ack mode the loop waits for
        // commit 1's durable ack first, which the server holds back here.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                appendStrict(engine, true, 2, 6);
                appendStrict(engine, true, 3, 6);
                appendRollback(engine);
                appendStrict(engine, true, 4, 2);
                appendStrict(engine, false, 5, 1);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                ModelClient first = newClient(server, clients, true, true);
                first.isHoldingAcks = true;
                AtomicBoolean isFirstConnect = new AtomicBoolean(true);
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0L,
                        () -> isFirstConnect.getAndSet(false) ? first : newClient(server, clients, true, true),
                        1L, 5L, DurableAckTiers.REPLICATED, 0L);
                try {
                    loop.start();
                    awaitTrue("the server refuses frame 3", () -> loop.getRefusedTransactionFsn() == 1L);
                    int polls = first.polls;
                    awaitTrue("the loop keeps waiting on its connection", () -> first.polls >= polls + 50);
                    Assert.assertEquals("no recycle before commit 1 is durable", 1, clientCount(clients));
                    Assert.assertEquals(-1L, engine.ackedFsn());
                    first.releaseAcks();
                    awaitAckedFsn(engine, 5L);
                } finally {
                    loop.close();
                }
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "0:R", "0:D4", "0:C5", "1:D4", "1:C5"),
                        server.received());
                Assert.assertEquals(Arrays.asList("C1", "D4", "C5"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    @Test
    public void testRefusedTransactionWalkStartsBehindReplayedCommits() throws Exception {
        // In durable-ack mode a reconnect replays from the durable watermark, here from
        // commit 1, whose durable ack the first connection held back. The loop sends
        // commit 1 again, as any replay does, then walks the refused transaction behind it
        // without sending it, and discards it once commit 1 is durable.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                appendStrict(engine, true, 2, 6);
                appendStrict(engine, true, 3, 6);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                ModelClient first = newClient(server, clients, true, true);
                first.isHoldingAcks = true;
                AtomicBoolean isFirstConnect = new AtomicBoolean(true);
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0L,
                        () -> isFirstConnect.getAndSet(false) ? first : newClient(server, clients, true, true),
                        1L, 5L, DurableAckTiers.REPLICATED, 0L);
                try {
                    loop.start();
                    awaitTrue("the server refuses frame 3", () -> loop.getRefusedTransactionFsn() == 1L);
                    first.closeFromServer();
                    awaitTrue("commit 1 becomes durable on the next connection", () -> engine.ackedFsn() == 0L);
                    Assert.assertEquals("only commit 1 goes out again", 1, client(clients, 1).flushedFrames);

                    appendRollback(engine);
                    appendStrict(engine, true, 4, 2);
                    appendStrict(engine, false, 5, 1);
                    awaitAckedFsn(engine, 5L);
                } finally {
                    loop.close();
                }
                Assert.assertEquals(Arrays.asList("0:C1", "0:D2", "0:D3", "1:C1", "2:D4", "2:C5"), server.received());
                Assert.assertEquals(Arrays.asList("C1", "C1", "D4", "C5"), server.committed());
                Assert.assertEquals(3, clientCount(clients));
            }
        });
    }

    @Test
    public void testRefusedTransactionWalkCrossesSegments() throws Exception {
        // The refused transaction's frames fill several store-and-forward segments, which
        // the walk follows up to the rollback frame. Frame 6 is the fifth of 2 rows each,
        // so it takes the transaction to the cap of 10.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrict(engine, false, 1, 1);
                for (int id = 2; id < 8; id++) {
                    appendStrict(engine, true, id, 2, 20_000);
                }
                appendRollback(engine);
                appendStrict(engine, false, 9, 1);
                ServerModel server = new ServerModel();
                server.setCap(10);
                List<ModelClient> clients = new ArrayList<>();
                runToAck(engine, newLoop(engine, server, clients, true), 8L);
                Assert.assertTrue("the transaction spans segments", engine.activeSegment().baseSeq() > 1L);
                Assert.assertEquals(Collections.singletonList("0:D6"), server.refused());
                Assert.assertEquals(Arrays.asList("C1", "C9"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
                List<String> secondConnection = new ArrayList<>();
                for (String frame : server.received()) {
                    if (frame.startsWith("1:")) {
                        secondConnection.add(frame);
                    }
                }
                Assert.assertEquals(Collections.singletonList("1:C9"), secondConnection);
            }
        });
    }

    @Test
    public void testRetiredTransactionSymbolsReachTheNextConnection() throws Exception {
        // Frames of the next transaction reference the refused transaction's symbols
        // without registering them again. Frame 1 of the refused transaction never went
        // out: the walk adds its symbol to the dictionary mirror, so the next
        // connection's catch-up registers it with the one frame 0 sent.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrictDelta(engine, true, 0, 'a');
                RecordingClient first = new RecordingClient();
                List<RecordingClient> next = new ArrayList<>();
                CursorWebSocketSendLoop loop = newManualLoop(engine, first, next, DurableAckTiers.NONE);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    Assert.assertTrue(loop.trySendBatchForTest());
                    deliverRefusal(loop, 0L);
                    appendStrictDelta(engine, true, 1, 'b');
                    appendRollback(engine);
                    appendStrictDelta(engine, false, 2, 'c');

                    Assert.assertFalse("the walk discards the transaction", loop.trySendBatchForTest());
                    Assert.assertEquals(2L, engine.ackedFsn());
                    Assert.assertEquals(-1L, loop.getRefusedTransactionFsn());
                    Assert.assertEquals(Collections.singletonList("D delta 0: a"), first.log);
                    Assert.assertEquals(1, next.size());
                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(Arrays.asList("catch-up 0: a b", "C delta 2: c"), next.get(0).log);
                    loop.checkError();
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testWalkStopsAtATornDictionary() throws Exception {
        // The torn-dictionary guard holds in the walk too: a frame whose delta starts
        // above every id the mirror holds halts the loop, as it does when sending.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendStrictDelta(engine, true, 0, 'a');
                RecordingClient client = new RecordingClient();
                CursorWebSocketSendLoop loop = newManualLoop(engine, client, null, DurableAckTiers.NONE);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    Assert.assertTrue(loop.trySendBatchForTest());
                    Assert.assertEquals(1, loop.sentDictCount());
                    deliverRefusal(loop, 0L);
                    appendStrictDelta(engine, true, 5, 'f');
                    Assert.assertFalse(loop.trySendBatchForTest());
                    try {
                        loop.checkError();
                        Assert.fail("the walk must halt at the gap");
                    } catch (LineSenderException e) {
                        Assert.assertEquals("recovered store-and-forward symbol dictionary is incomplete (likely a "
                                        + "host crash): frame delta start 5 exceeds recovered dictionary size 1; "
                                        + "cannot replay without corrupting data -- resend required",
                                e.getMessage());
                    }
                    Assert.assertEquals(Collections.singletonList("D delta 0: a"), client.log);
                } finally {
                    loop.close();
                }
            }
        });
    }

    private static void appendCommit(CursorSendEngine engine, int id) {
        appendFrame(engine, (byte) 0, id);
    }

    private static void appendDeferred(CursorSendEngine engine, int id) {
        appendFrame(engine, QwpConstants.FLAG_DEFER_COMMIT, id);
    }

    // A payload without the QWP magic.
    private static void appendForeign(CursorSendEngine engine, int size) {
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 'x');
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // A QWP header whose flags byte says what the frame is, followed by its id.
    private static void appendFrame(CursorSendEngine engine, byte flags, int id) {
        int size = QwpConstants.HEADER_SIZE + 4;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            Unsafe.getUnsafe().putInt(buf, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + 4, QwpConstants.VERSION);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS, flags);
            Unsafe.getUnsafe().putInt(buf + QwpConstants.HEADER_SIZE, id);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // A bare 12-byte header: no table blocks, no payload.
    private static void appendHeader(CursorSendEngine engine, int magic, byte flags) {
        int size = QwpConstants.HEADER_SIZE;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            Unsafe.getUnsafe().putInt(buf, magic);
            Unsafe.getUnsafe().putByte(buf + 4, QwpConstants.VERSION);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS, flags);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // The rollback control frame, as QwpWebSocketEncoder.encodeRollbackMessage writes it.
    private static void appendRollback(CursorSendEngine engine) {
        appendHeader(engine, QwpConstants.MAGIC_MESSAGE, QwpConstants.ROLLBACK_FRAME_FLAGS);
    }

    private static void appendStrict(CursorSendEngine engine, boolean isDeferred, int id, int rows) {
        appendStrict(engine, isDeferred, id, rows, 0);
    }

    // A frame of a strict sender: its flags carry FLAG_STRICT, and its id and its row
    // count, which the model server counts toward its cap, follow the header.
    private static void appendStrict(CursorSendEngine engine, boolean isDeferred, int id, int rows, int padding) {
        int size = QwpConstants.HEADER_SIZE + 8 + padding;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            Unsafe.getUnsafe().putInt(buf, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + 4, QwpConstants.VERSION);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS, strictFlags(isDeferred));
            Unsafe.getUnsafe().putInt(buf + QwpConstants.HEADER_SIZE, id);
            Unsafe.getUnsafe().putInt(buf + QwpConstants.HEADER_SIZE + 4, rows);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // A strict frame with a symbol-dictionary delta that registers one symbol at
    // deltaStart, and no table blocks.
    private static void appendStrictDelta(CursorSendEngine engine, boolean isDeferred, int deltaStart, char symbol) {
        int size = QwpConstants.HEADER_SIZE + 4;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            Unsafe.getUnsafe().putInt(buf, QwpConstants.MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + 4, QwpConstants.VERSION);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS,
                    (byte) (strictFlags(isDeferred) | QwpConstants.FLAG_DELTA_SYMBOL_DICT));
            long p = buf + QwpConstants.HEADER_SIZE;
            Unsafe.getUnsafe().putByte(p, (byte) deltaStart);
            Unsafe.getUnsafe().putByte(p + 1, (byte) 1);
            Unsafe.getUnsafe().putByte(p + 2, (byte) 1);
            Unsafe.getUnsafe().putByte(p + 3, (byte) symbol);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // The loop halted with the server's refusal, reported over the FSNs from..to.
    private static void assertRefusalHalted(CursorWebSocketSendLoop loop, long fromFsn, long toFsn, long seq) {
        try {
            loop.checkError();
            Assert.fail("the loop must halt");
        } catch (LineSenderServerException e) {
            SenderError err = e.getServerError();
            Assert.assertSame(err, loop.getLastTerminalServerError());
            Assert.assertEquals(SenderError.Category.TRANSACTION_TOO_LARGE, err.getCategory());
            Assert.assertEquals(SenderError.Policy.TERMINAL, err.getAppliedPolicy());
            Assert.assertEquals(WebSocketResponse.STATUS_TRANSACTION_TOO_LARGE, err.getServerStatusByte());
            Assert.assertEquals(REFUSAL, err.getServerMessage());
            Assert.assertEquals(seq, err.getMessageSequence());
            Assert.assertEquals(fromFsn, err.getFromFsn());
            Assert.assertEquals(toFsn, err.getToFsn());
        }
    }

    private static ModelClient client(List<ModelClient> clients, int connection) {
        synchronized (clients) {
            return clients.get(connection);
        }
    }

    // An OK covering wire sequence seq, for a commit of table TABLE at seqTxn 1. In
    // durable-ack mode it waits for a durable ack, which never comes here.
    private static void deliverOk(CursorWebSocketSendLoop loop, long seq) {
        byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
        int size = 1 + 8 + 2 + 2 + name.length + 8;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
            Unsafe.getUnsafe().putLong(ptr + 1, seq);
            Unsafe.getUnsafe().putShort(ptr + 9, (short) 1);
            Unsafe.getUnsafe().putShort(ptr + 11, (short) name.length);
            for (int i = 0; i < name.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + 13 + i, name[i]);
            }
            Unsafe.getUnsafe().putLong(ptr + 13 + name.length, 1L);
            loop.deliverResponseForTest(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void deliverRefusal(CursorWebSocketSendLoop loop, long seq) {
        byte[] message = REFUSAL.getBytes(StandardCharsets.UTF_8);
        int size = 1 + 8 + 2 + message.length;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_TRANSACTION_TOO_LARGE);
            Unsafe.getUnsafe().putLong(ptr + 1, seq);
            Unsafe.getUnsafe().putShort(ptr + 9, (short) message.length);
            for (int i = 0; i < message.length; i++) {
                Unsafe.getUnsafe().putByte(ptr + 11 + i, message[i]);
            }
            loop.deliverResponseForTest(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // A loop the test drives by hand, without starting its I/O thread: it sends to
    // client, and the test delivers the server's responses. A reconnect connects a new
    // RecordingClient, which it adds to next; with next null, it fails the test.
    private static CursorWebSocketSendLoop newManualLoop(
            CursorSendEngine engine,
            RecordingClient client,
            List<RecordingClient> next,
            int durableAckTiers
    ) {
        return new CursorWebSocketSendLoop(
                client, engine, 0L,
                () -> {
                    Assert.assertNotNull("no reconnect expected", next);
                    RecordingClient reconnected = new RecordingClient();
                    next.add(reconnected);
                    return reconnected;
                },
                0L, 1L, durableAckTiers, 0L);
    }

    private static byte strictFlags(boolean isDeferred) {
        return (byte) (QwpConstants.FLAG_STRICT | (isDeferred ? QwpConstants.FLAG_DEFER_COMMIT : 0));
    }

    // The frame appendAhead writes is the only commit-bearing frame ahead of the rolled-back
    // transaction, and the server holds it unprocessed, or in durable mode holds back its
    // durable ack. The loop must not recycle until that ack arrives.
    private static void assertRollbackWaitsForAckOf(
            boolean isDurable,
            Consumer<CursorSendEngine> appendAhead,
            String ahead
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendAhead.accept(engine);
                appendDeferred(engine, 2);
                appendDeferred(engine, 3);
                appendRollback(engine);
                appendDeferred(engine, 4);
                appendCommit(engine, 5);
                ServerModel server = new ServerModel();
                List<ModelClient> clients = new ArrayList<>();
                ModelClient first = newClient(server, clients, false, isDurable);
                // Durable mode: the server processes and OKs at once, but holds back the
                // durable ack. Otherwise it holds back processing, OKs included.
                first.isHoldingAcks = true;
                AtomicBoolean isFirstConnect = new AtomicBoolean(true);
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0L,
                        () -> isFirstConnect.getAndSet(false) ? first : newClient(server, clients, false, isDurable),
                        1L, 5L, isDurable ? DurableAckTiers.REPLICATED : DurableAckTiers.NONE, 0L);
                try {
                    loop.start();
                    awaitTrue("the frames ahead of the rollback frame go out", () -> first.flushedFrames == 3);
                    // The loop keeps polling the first connection while it waits for the acks.
                    int polls = first.polls;
                    awaitTrue("the loop keeps waiting on its connection", () -> first.polls >= polls + 50);
                    Assert.assertEquals("no recycle while " + ahead + " is not acked", 1, clientCount(clients));
                    Assert.assertEquals(-1L, engine.ackedFsn());
                    first.releaseAcks();
                    awaitAckedFsn(engine, 5L);
                } finally {
                    loop.close();
                }
                Assert.assertEquals(Arrays.asList("0:" + ahead, "0:D2", "0:D3", "1:D4", "1:C5"), server.received());
                Assert.assertEquals(Arrays.asList(ahead, "D4", "C5"), server.committed());
                Assert.assertEquals(2, clientCount(clients));
            }
        });
    }

    private static void awaitAckedFsn(CursorSendEngine engine, long target) throws InterruptedException {
        awaitTrue("ackedFsn reaches " + target, () -> engine.ackedFsn() >= target);
        Assert.assertEquals(target, engine.ackedFsn());
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                Assert.fail("timed out: " + what);
            }
            Thread.sleep(1);
        }
    }

    private static int clientCount(List<ModelClient> clients) {
        synchronized (clients) {
            return clients.size();
        }
    }

    private static ModelClient newClient(ServerModel server, List<ModelClient> clients, boolean isRollbackConfirmed, boolean isDurable) {
        synchronized (clients) {
            ModelClient client = new ModelClient(server, clients.size(), isRollbackConfirmed, isDurable);
            clients.add(client);
            return client;
        }
    }

    private static CursorSendEngine newEngine() {
        return new CursorSendEngine(null, SEGMENT_SIZE_BYTES);
    }

    private static CursorWebSocketSendLoop newLoop(CursorSendEngine engine, ServerModel server, List<ModelClient> clients, boolean isRollbackConfirmed) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L,
                () -> newClient(server, clients, isRollbackConfirmed, false),
                1L, 5L);
    }

    private static void runRandomTransactions(Rnd rnd) throws Exception {
        try (CursorSendEngine engine = newEngine()) {
            List<String> expectedCommitted = new ArrayList<>();
            List<String> transaction = new ArrayList<>();
            int rollbacks = 0;
            int nextId = 1;
            // Appended before the loop starts, the rest while it runs.
            List<Runnable> appends = new ArrayList<>();
            for (int t = 0, n = 1 + rnd.nextInt(12); t < n; t++) {
                transaction.clear();
                for (int i = 0, k = rnd.nextInt(4); i < k; i++) {
                    int id = nextId++;
                    transaction.add("D" + id);
                    appends.add(() -> appendDeferred(engine, id));
                }
                if (rnd.nextInt(5) < 3) {
                    int id = nextId++;
                    transaction.add("C" + id);
                    expectedCommitted.addAll(transaction);
                    appends.add(() -> appendCommit(engine, id));
                } else {
                    rollbacks++;
                    appends.add(() -> appendRollback(engine));
                }
            }
            int beforeStart = rnd.nextInt(appends.size() + 1);
            for (int i = 0; i < beforeStart; i++) {
                appends.get(i).run();
            }
            ServerModel server = new ServerModel();
            List<ModelClient> clients = new ArrayList<>();
            Rnd ioRnd = new Rnd(rnd.nextLong(), rnd.nextLong());
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    null, engine, 0L,
                    () -> newClient(server, clients, ioRnd.nextBoolean(), false),
                    1L, 5L);
            int retired;
            try {
                loop.start();
                for (int i = beforeStart, n = appends.size(); i < n; i++) {
                    appends.get(i).run();
                }
                awaitAckedFsn(engine, appends.size() - 1L);
                // Every rollback frame is received or retired by now. A retired one acks
                // itself before its recycle connects, so when it is the last frame the
                // recycle may still be under way.
                retired = rollbacks - server.rollbacksReceived();
                awaitTrue("one recycle per retired rollback frame", () -> clientCount(clients) == 1 + retired);
            } finally {
                loop.close();
            }
            String shape = "frames=" + appends.size() + ", received=" + server.received();
            Assert.assertEquals(shape, expectedCommitted, server.committed());
            Assert.assertEquals(shape, 0, server.rollbacksToServerWithoutRollback());
            Assert.assertEquals(shape, 1 + retired, clientCount(clients));
        }
    }

    private static void runToAck(CursorSendEngine engine, CursorWebSocketSendLoop loop, long lastFsn) throws Exception {
        try {
            loop.start();
            awaitAckedFsn(engine, lastFsn);
        } finally {
            loop.close();
        }
    }

    /**
     * One connection to {@link ServerModel}: frames a batch flush hands over are in
     * flight until the I/O loop polls, which is when the server processes them, unless
     * it holds its acks. Acks are cumulative, so only the highest pending one goes out.
     */
    private static final class ModelClient extends TestWebSocketClient {
        private final List<Frame> batch = new ArrayList<>();
        private final int connection;
        private final ArrayDeque<Frame> inFlight = new ArrayDeque<>();
        private final boolean isDurable;
        private final boolean isRollbackConfirmed;
        private final ServerModel server;
        volatile int flushedFrames;
        volatile int flushes;
        volatile boolean isHoldingAcks;
        volatile boolean isServerClosing;
        volatile int polls;
        private long durableReported;
        private long nextSeq;
        private long pendingErrorSeq = -1L;
        private long pendingOkSeq = -1L;
        private long pendingOkSeqTxn;

        ModelClient(ServerModel server, int connection, boolean isRollbackConfirmed, boolean isDurable) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.server = server;
            this.connection = connection;
            this.isRollbackConfirmed = isRollbackConfirmed;
            this.isDurable = isDurable;
        }

        @Override
        public synchronized void batchAppendBinary(long dataPtr, int length) {
            batch.add(Frame.read(dataPtr, length));
        }

        @Override
        public synchronized void batchFlush() {
            inFlight.addAll(batch);
            flushedFrames += batch.size();
            flushes++;
            batch.clear();
        }

        @Override
        public synchronized void close() {
            // Whatever the server has not processed yet never arrives.
            inFlight.clear();
            server.close(connection);
            super.close();
        }

        @Override
        public boolean isServerRollbackEnabled() {
            return isRollbackConfirmed;
        }

        // The server goes away, as on a restart: the next poll delivers its close.
        void closeFromServer() {
            isServerClosing = true;
        }

        synchronized void releaseAcks() {
            isHoldingAcks = false;
        }

        @Override
        public void sendPing() {
        }

        @Override
        public synchronized boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            polls++;
            if (isServerClosing) {
                isServerClosing = false;
                handler.onClose(WebSocketCloseCode.GOING_AWAY, "server restarts");
                return true;
            }
            // Durable mode: the server processes and OKs at once and holds back the
            // durable ack. Otherwise holding back the acks means not processing at all.
            if (isDurable || !isHoldingAcks) {
                for (Frame frame; (frame = inFlight.pollFirst()) != null; ) {
                    long seq = nextSeq++;
                    int outcome = server.process(connection, isRollbackConfirmed, frame);
                    if (outcome == ServerModel.ACK) {
                        pendingOkSeq = seq;
                        pendingOkSeqTxn = frame.kind == 'R' ? 0 : server.seqTxn();
                    } else if (outcome == ServerModel.REFUSE) {
                        pendingErrorSeq = seq;
                    }
                }
            }
            // As the QuestDB server: the acks of the frames ahead of a refused frame go
            // out before the refusal.
            if (pendingOkSeq >= 0) {
                sendOk(handler, pendingOkSeq, isDurable && pendingOkSeqTxn > 0 ? pendingOkSeqTxn : 0);
                pendingOkSeq = -1L;
                return true;
            }
            if (pendingErrorSeq >= 0) {
                sendRefusal(handler, pendingErrorSeq);
                pendingErrorSeq = -1L;
                return true;
            }
            if (isDurable && !isHoldingAcks && server.seqTxn() > durableReported) {
                durableReported = server.seqTxn();
                sendDurableAck(handler, durableReported);
                return true;
            }
            return false;
        }

        // STATUS_DURABLE_ACK: status, then the table entries.
        private static void sendDurableAck(WebSocketFrameHandler handler, long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            int size = 1 + 2 + 2 + name.length + 8;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_DURABLE_ACK);
                putTableEntry(ptr + 1, name, seqTxn);
                handler.onBinaryMessage(ptr, size);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        }

        // STATUS_TRANSACTION_TOO_LARGE: status, the refused frame's wire sequence, the message.
        private static void sendRefusal(WebSocketFrameHandler handler, long seq) {
            byte[] message = REFUSAL.getBytes(StandardCharsets.UTF_8);
            int size = 1 + 8 + 2 + message.length;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_TRANSACTION_TOO_LARGE);
                Unsafe.getUnsafe().putLong(ptr + 1, seq);
                Unsafe.getUnsafe().putShort(ptr + 9, (short) message.length);
                for (int i = 0; i < message.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + 11 + i, message[i]);
                }
                handler.onBinaryMessage(ptr, size);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        }

        // STATUS_OK: status, cumulative wire sequence, then the table entries -- one for
        // a commit in durable mode, so the OK waits for the durable ack covering it.
        private static void sendOk(WebSocketFrameHandler handler, long seq, long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            int size = 1 + 8 + 2 + (seqTxn > 0 ? 2 + name.length + 8 : 0);
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
                Unsafe.getUnsafe().putLong(ptr + 1, seq);
                if (seqTxn > 0) {
                    putTableEntry(ptr + 9, name, seqTxn);
                } else {
                    Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);
                }
                handler.onBinaryMessage(ptr, size);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        }

        private static void putTableEntry(long p, byte[] name, long seqTxn) {
            Unsafe.getUnsafe().putShort(p, (short) 1);
            Unsafe.getUnsafe().putShort(p + 2, (short) name.length);
            for (int i = 0; i < name.length; i++) {
                Unsafe.getUnsafe().putByte(p + 4 + i, name[i]);
            }
            Unsafe.getUnsafe().putLong(p + 4 + name.length, seqTxn);
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }

    /**
     * A connection the test drives by hand: it logs what the loop writes to it, and the
     * test delivers the server's responses through deliverResponseForTest. It confirms
     * rollback, so the loop sends the rollback frame like any other.
     */
    private static final class RecordingClient extends TestWebSocketClient {
        final List<String> log = new ArrayList<>();

        RecordingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void batchAppendBinary(long dataPtr, int length) {
            byte flags = Unsafe.getUnsafe().getByte(dataPtr + QwpConstants.HEADER_OFFSET_FLAGS);
            if (length == QwpConstants.HEADER_SIZE && flags == QwpConstants.ROLLBACK_FRAME_FLAGS) {
                log.add("R");
                return;
            }
            String kind = (flags & QwpConstants.FLAG_DEFER_COMMIT) != 0 ? "D" : "C";
            if ((flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0) {
                long p = dataPtr + QwpConstants.HEADER_SIZE;
                log.add(kind + " delta " + Unsafe.getUnsafe().getByte(p) + ":" + symbols(p + 2, Unsafe.getUnsafe().getByte(p + 1)));
            } else {
                log.add(kind + Unsafe.getUnsafe().getInt(dataPtr + QwpConstants.HEADER_SIZE));
            }
        }

        @Override
        public void batchFlush() {
        }

        @Override
        public int getServerMaxBatchSize() {
            return 16_384;
        }

        @Override
        public int getServerQwpVersion() {
            return 1;
        }

        @Override
        public boolean isServerRollbackEnabled() {
            return true;
        }

        // The dictionary catch-up: a header and the delta's start and count, then the
        // symbols, in two parts.
        @Override
        public void sendBinary(long firstPtr, int firstLength, long secondPtr, int secondLength) {
            long p = firstPtr + QwpConstants.HEADER_SIZE;
            log.add("catch-up " + Unsafe.getUnsafe().getByte(p) + ":" + symbols(secondPtr, Unsafe.getUnsafe().getByte(p + 1)));
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }

        // Symbols of one ASCII char each, every one after its length byte.
        private static String symbols(long p, int count) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < count; i++, p += 2) {
                sb.append(' ').append((char) Unsafe.getUnsafe().getByte(p + 1));
            }
            return sb.toString();
        }
    }

    // A frame as the server sees it: D (deferred), C (commit), R (the rollback frame), or
    // X, a payload that is not a QWP message, labelled with its size. A frame's id
    // follows its header; a bare header has id 0. A strict frame's rows follow its id.
    private static final class Frame {
        final boolean isStrict;
        final char kind;
        final String label;
        final int rows;

        Frame(char kind, String label) {
            this(kind, label, false, 0);
        }

        Frame(char kind, String label, boolean isStrict, int rows) {
            this.kind = kind;
            this.label = label;
            this.isStrict = isStrict;
            this.rows = rows;
        }

        static Frame read(long ptr, int length) {
            if (length < QwpConstants.HEADER_SIZE || Unsafe.getUnsafe().getInt(ptr) != QwpConstants.MAGIC_MESSAGE) {
                return new Frame('X', "X" + length);
            }
            byte flags = Unsafe.getUnsafe().getByte(ptr + QwpConstants.HEADER_OFFSET_FLAGS);
            if (length == QwpConstants.HEADER_SIZE && flags == QwpConstants.ROLLBACK_FRAME_FLAGS) {
                return new Frame('R', "R");
            }
            char kind = (flags & QwpConstants.FLAG_DEFER_COMMIT) != 0 ? 'D' : 'C';
            int id = length > QwpConstants.HEADER_SIZE ? Unsafe.getUnsafe().getInt(ptr + QwpConstants.HEADER_SIZE) : 0;
            boolean isStrict = (flags & QwpConstants.FLAG_STRICT) != 0;
            int rows = isStrict && length >= QwpConstants.HEADER_SIZE + 8
                    ? Unsafe.getUnsafe().getInt(ptr + QwpConstants.HEADER_SIZE + 4) : 0;
            return new Frame(kind, kind + Integer.toString(id), isStrict, rows);
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /**
     * What the QuestDB server makes of the frames, per connection: a deferred frame
     * joins the connection's open group; a commit frame commits the group and itself
     * and is acked; the rollback frame discards the group and is acked on a connection
     * that confirmed rollback. A server without rollback support reads the rollback
     * frame as an empty deferred frame, so the group stays open and the next commit on
     * the connection commits it. A closed connection leaves its group uncommitted. A
     * foreign payload is acked like a commit, so the loop's wait for it ends.
     * <p>
     * With a cap, a strict frame whose rows would take its connection's group to the
     * cap is refused with STATUS_TRANSACTION_TOO_LARGE instead: the group is discarded,
     * and no later frame of the connection is processed, nor acked.
     */
    private static final class ServerModel {
        static final int ACK = 1;
        static final int NONE = 0;
        static final int REFUSE = 2;
        private final List<Integer> broken = new ArrayList<>();
        private final List<String> committed = new ArrayList<>();
        private final List<Integer> groupRows = new ArrayList<>();
        private final List<List<String>> groups = new ArrayList<>();
        private final List<String> received = new ArrayList<>();
        private final List<String> refused = new ArrayList<>();
        private int cap;
        private int rollbacksReceived;
        private int rollbacksToServerWithoutRollback;
        private long seqTxn;

        synchronized void close(int connection) {
            discardGroup(connection);
        }

        synchronized List<String> committed() {
            return new ArrayList<>(committed);
        }

        // Whether the server acks the frame, refuses it, or neither.
        synchronized int process(int connection, boolean isRollbackConfirmed, Frame frame) {
            received.add(connection + ":" + frame);
            List<String> group = group(connection);
            if (broken.contains(connection)) {
                return NONE;
            }
            if (frame.isStrict && cap > 0) {
                int rows = groupRows.get(connection) + frame.rows;
                if (rows >= cap) {
                    refused.add(connection + ":" + frame);
                    discardGroup(connection);
                    broken.add(connection);
                    return REFUSE;
                }
                groupRows.set(connection, rows);
            }
            switch (frame.kind) {
                case 'C':
                case 'X':
                    committed.addAll(group);
                    committed.add(frame.label);
                    discardGroup(connection);
                    seqTxn++;
                    return ACK;
                case 'R':
                    rollbacksReceived++;
                    if (isRollbackConfirmed) {
                        discardGroup(connection);
                        return ACK;
                    }
                    rollbacksToServerWithoutRollback++;
                    return NONE;
                default:
                    group.add(frame.label);
                    return NONE;
            }
        }

        synchronized List<String> refused() {
            return new ArrayList<>(refused);
        }

        synchronized void setCap(int cap) {
            this.cap = cap;
        }

        private void discardGroup(int connection) {
            group(connection).clear();
            groupRows.set(connection, 0);
        }

        synchronized List<String> received() {
            return new ArrayList<>(received);
        }

        synchronized int rollbacksReceived() {
            return rollbacksReceived;
        }

        synchronized int rollbacksToServerWithoutRollback() {
            return rollbacksToServerWithoutRollback;
        }

        synchronized long seqTxn() {
            return seqTxn;
        }

        private List<String> group(int connection) {
            while (groups.size() <= connection) {
                groups.add(new ArrayList<>());
                groupRows.add(0);
            }
            return groups.get(connection);
        }
    }
}
