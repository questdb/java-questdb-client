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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The symbol-dictionary recycle swap ({@code QwpWebSocketSender.table()}'s
 * barrier hook + {@code recycleForDictReset()}): tears the cursor engine and
 * I/O loop down once the ring is proven drained, replaces the producer's
 * global symbol dictionary, and rebuilds the engine on the same (now-empty)
 * slot -- all synchronously inside a single {@code table()} call. The fresh
 * WebSocket handshake itself (the reconnect) is deferred to the I/O thread
 * and completes asynchronously.
 */
public class SymbolDictRecycleTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testRecycleAtEmptyBacklog() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-empty-backlog").toString();
            RecycleHandler handler = new RecycleHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(1, handler.connectionsAccepted.get());
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());

                    // The ring is drained (everything acked) and no row is in
                    // progress, so this table() call must recycle synchronously.
                    // The fresh WebSocket handshake is the I/O thread's job and
                    // completes asynchronously -- it is asserted below, after an
                    // acked post-recycle frame proves the connection is up.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get acked",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertEquals("recycle must open a fresh connection",
                            2, server.handshakeCount());
                    Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN "
                                    + "[fsn1=" + fsn1 + ", fsn2=" + fsn2 + ']',
                            fsn2 > fsn1);
                }

                Assert.assertEquals("exactly 2 connections total", 2, handler.connectionsAccepted.get());
                Assert.assertEquals("connection 2's first data frame must carry deltaStart == 0 "
                                + "(a fresh, empty dictionary)",
                        0, handler.conn2FirstFrameDeltaStart);
                Assert.assertEquals("connection 2's dictionary must hold only the post-recycle "
                                + "symbols, not a, b",
                        Arrays.asList("c", "d"), handler.dictFor(2));
            }
        });
    }

    /**
     * {@code engineRebuildFactory} is only installed by {@code Sender.build()}
     * ({@code Sender.java:1752}) -- every public {@code QwpWebSocketSender.connect(...)}
     * overload leaves it null. Since the recycle feature is default-on and
     * {@code resetSymbolDictionary()} is a public advisory API, a connect()-built
     * sender can become "armed" with no way to ever act on it.
     * {@code maybeRecycleForDictReset()} must refuse before any teardown in that
     * case, not attempt step 6 and NPE into a latched terminal state -- covers
     * both ways a sender can arm: the manual request and threshold crossing.
     */
    @Test
    public void testConnectBuiltSenderNeverRecyclesWithoutFactory() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();

                // Manual reset request on the simplest connect() overload.
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port)) {
                    sender.resetSymbolDictionary();
                    Assert.assertTrue("a manual request arms immediately (no row/flush in flight)",
                            sender.isResetArmed());

                    // Drained instant (nothing published yet, no row in progress): with a
                    // real factory this table() call would recycle. With none installed it
                    // must simply do nothing and let the row through normally.
                    sender.table("t").longColumn("v", 1L).atNow();
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("sender must keep working even though it can never recycle",
                            sender.awaitAckedFsn(fsn, 5_000));
                    Assert.assertEquals("no factory -> the recycle can never actually run",
                            0, sender.getSymbolDictEpoch());
                    Assert.assertTrue("stays armed forever -- nothing ever consumes the request",
                            sender.isResetArmed());
                }

                // Threshold-based arming needs a custom low threshold, only reachable (without
                // routing through Sender.build(), which WOULD install a factory) via the
                // widest connect() overload -- mirrors SymbolDictRecycleArmingTest.testArmsInFullDictMode.
                CursorSendEngine engine = new CursorSendEngine(
                        null, 4L * 1024 * 1024, 128L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS);
                QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        Collections.singletonList(new QwpWebSocketSender.Endpoint("localhost", port)),
                        null, // tlsConfig
                        0, 0, 0L, // autoFlushRows, autoFlushBytes, autoFlushIntervalNanos
                        null, // authorizationHeader
                        false, // requestDurableAck
                        engine,
                        5_000L, // closeFlushTimeoutMillis
                        CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_DURATION_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS,
                        Sender.InitialConnectMode.OFF,
                        null, // errorHandler
                        SenderErrorDispatcher.DEFAULT_CAPACITY,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        QwpWebSocketSender.DEFAULT_AUTH_TIMEOUT_MS,
                        0, // connectTimeoutMs
                        null, // connectionListener
                        SenderConnectionDispatcher.DEFAULT_CAPACITY,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        CursorWebSocketSendLoop.DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS,
                        CursorWebSocketSendLoop.DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS,
                        true, // symbolDictResetEnabled
                        2, // symbolDictResetThresholdSymbols -- low, deliberately crossed below
                        QwpWebSocketSender.DEFAULT_SYMBOL_DICT_RESET_MAX_WAIT_MILLIS);
                try {
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("threshold=2 crossed by a, b", sender.isResetArmed());

                    // Drained instant again: must not recycle, must not throw.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("sender must keep working with no factory installed",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertEquals("no factory -> the recycle can never actually run",
                            0, sender.getSymbolDictEpoch());
                    Assert.assertTrue("stays armed -- nothing ever consumes the threshold arming",
                            sender.isResetArmed());
                } finally {
                    sender.close();
                }
            }
        });
    }

    @Test
    public void testPostRecycleSlotContents() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-slot-contents").toString();
            String slot = Paths.get(sfDir, "default").toString();
            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    CursorSendEngine before = ws.getCursorEngineForTesting();

                    // Synchronous swap: by the time table() returns, the old engine
                    // is gone and a fresh one is rebuilt (the reconnect itself defers
                    // to the I/O thread). Asserting engine identity right here needs
                    // no polling -- there is no window to race for that.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();

                    CursorSendEngine after = ws.getCursorEngineForTesting();
                    Assert.assertNotSame("recycle must swap in a fresh engine instance",
                            before, after);
                    // A bare Files.exists(".../sf-initial.sfa") proves nothing on its own --
                    // that name is fixed and the outgoing engine had one too. Prove the
                    // rebuilt slot's structure instead: exactly the well-known set of state
                    // files a brand-new (never-recovered) slot has, nothing left over from
                    // the outgoing epoch's segments.
                    List<String> freshSlotFiles = Arrays.asList(
                            ".ack-watermark", ".lock", ".lock.pid", ".symbol-dict",
                            "sf-0000000000000000.sfa", "sf-initial.sfa", "sf-manifest.bin");
                    Assert.assertEquals("post-recycle slot must contain exactly a fresh engine's "
                                    + "own state files",
                            freshSlotFiles, listDir(slot));
                    Assert.assertEquals("post-recycle dictionary must start empty, not continue "
                                    + "the outgoing epoch's 2 entries",
                            0, after.getPersistedSymbolDict().size());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));

                    // The new epoch's persisted dictionary holds exactly c, d --
                    // proof it is a genuinely fresh dictionary, not a, b continued.
                    Assert.assertEquals("post-recycle dictionary must hold only the new epoch's "
                                    + "symbols",
                            2, after.getPersistedSymbolDict().size());
                }
            }
        });
    }

    @Test
    public void testRecycleUnderDurableAck() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-durable-ack").toString();
            DurableAckHandler handler = new DurableAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;request_durable_ack=on;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: the batch must be DURABLY acked before the recycle "
                                    + "-- isRingDrained() reads the durable-ack-gated watermark",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());
                    Assert.assertEquals(1, handler.connectionsAccepted.get());

                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get durably acked on the "
                                    + "fresh connection",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertEquals("recycle must open a fresh connection",
                            2, server.handshakeCount());
                    Assert.assertTrue(fsn2 > fsn1);
                }
            }
        });
    }

    /**
     * Exercises the same code path {@code engineRebuildFactory.rebuild()} calls
     * in production ({@code LineSenderBuilder.constructEngineOnSlot}) -- that
     * method is package-private to {@code io.questdb.client} and unreachable
     * directly from this package, so this observes its result through the
     * sender's own {@code @TestOnly} engine accessor instead of calling it in
     * isolation.
     */
    @Test
    public void testFactoryRebuildsOnEmptySlot() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-factory-rebuild").toString();
            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    sender.table("t");

                    CursorSendEngine rebuilt = ws.getCursorEngineForTesting();
                    Assert.assertTrue("a freshly rebuilt slot must support delta encoding",
                            rebuilt.isDeltaDictEnabled());
                    Assert.assertFalse("a freshly emptied slot has nothing to recover",
                            rebuilt.wasRecoveredFromDisk());
                    Assert.assertEquals(-1L, rebuilt.recoveredMaxSymbolId());
                    Assert.assertEquals("a freshly rebuilt engine has published nothing yet",
                            -1L, rebuilt.publishedFsn());
                }
            }
        });
    }

    /**
     * A step-6 rebuild failure (the {@link QwpWebSocketSender.EngineRebuildFactory}
     * itself throwing) must latch the sender terminal rather than leaving it in
     * the torn-down state the swap's earlier steps produced. Uses
     * {@code setEngineRebuildFactory} directly to inject the fault -- the real
     * factory has no seam for a custom {@code FilesFacade} (it always goes
     * through {@code LineSenderBuilder.constructEngineOnSlot} against the real
     * filesystem), and this isolates the exception-path behaviour under test
     * from any particular failure cause.
     */
    @Test
    public void testFailedRebuildLatchesTerminal() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    RuntimeException fault = new RuntimeException("injected engine rebuild fault");
                    ws.setEngineRebuildFactory(() -> {
                        throw fault;
                    });

                    // pendingRowCount == 0 -> resetSymbolDictionary() arms immediately.
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());

                    LineSenderException triggering = null;
                    try {
                        sender.table("t");
                        Assert.fail("expected the triggering table() call to throw");
                    } catch (LineSenderException e) {
                        triggering = e;
                    }
                    Assert.assertNotNull(triggering);
                    Assert.assertSame("the latched failure must be the exact rebuild fault",
                            fault, triggering.getCause());

                    // Every subsequent table()/flush-family call must rethrow --
                    // never touch the torn-down (null cursorEngine/cursorSendLoop) state.
                    assertRethrowsWithCause(fault, () -> sender.table("t"));
                    assertRethrowsWithCause(fault, sender::flush);
                    assertRethrowsWithCause(fault, sender::flushAndGetSequence);
                    assertRethrowsWithCause(fault, () -> sender.drain(0));
                    assertRethrowsWithCause(fault, () -> sender.awaitAckedFsn(0, 0));

                    // close() must still work despite the latched terminal failure.
                    sender.close();
                }
            }
        });
    }

    private static TestWebSocketServer ackingServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler());
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    /** Sorted list of entry names directly inside {@code dir} (no recursion, no "."/".."). */
    private static List<String> listDir(String dir) {
        List<String> names = new ArrayList<>();
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        names.add(name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Collections.sort(names);
        return names;
    }

    private static void assertRethrowsWithCause(Throwable expectedCause, ThrowingRunnable action)
            throws Exception {
        try {
            action.run();
            Assert.fail("expected a latched LineSenderException to rethrow");
        } catch (LineSenderException e) {
            Assert.assertSame("a latched terminal sender must keep rethrowing the same cause",
                    expectedCause, e.getCause());
        }
    }

    private static String cfg(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";";
    }

    private static byte[] buildDurableAckFrame(String tableName, long seqTxn) {
        byte[] name = tableName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x02); // STATUS_DURABLE_ACK
        bb.putShort((short) 1); // tableCount
        bb.putShort((short) name.length);
        bb.put(name);
        bb.putLong(seqTxn);
        return bb.array();
    }

    private static byte[] buildOkFrame(String tableName, long wireSeq, long seqTxn) {
        byte[] name = tableName.getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2 + 2 + name.length + 8).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(wireSeq);
        bb.putShort((short) 1); // tableCount
        bb.putShort((short) name.length);
        bb.put(name);
        bb.putLong(seqTxn);
        return bb.array();
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /** ACKs every frame it receives; does not otherwise inspect the wire. */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Immediately follows every OK ack with a durable ack for the same
     * transaction, so a durable-ack-mode sender's {@code ackedFsn} advances
     * without a separate release phase. Counters reset per connection, since
     * the recycle's fresh loop restarts its own wire sequence at 0.
     */
    private static class DurableAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE_NAME = "t";
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeqTxn;
        private long nextWireSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                nextWireSeq = 0;
                nextSeqTxn = 0;
            }
            try {
                long wireSeq = nextWireSeq++;
                long seqTxn = nextSeqTxn++;
                client.sendBinary(buildOkFrame(TABLE_NAME, wireSeq, seqTxn));
                client.sendBinary(buildDurableAckFrame(TABLE_NAME, seqTxn));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Reconstructs each connection's per-connection delta dictionary (mirrors
     * {@code DeltaDictCatchUpTest.CatchUpHandler}) and records the delta-start
     * id of connection 2's first non-empty data frame.
     */
    private static class RecycleHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        volatile int conn2FirstFrameDeltaStart = -1;
        private boolean conn2SeenFirstDataFrame;
        private TestWebSocketServer.ClientHandler currentClient;
        private final List<List<String>> dictsByConn = new CopyOnWriteArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

        synchronized List<String> dictFor(int connNumber) {
            return connNumber <= dictsByConn.size()
                    ? new ArrayList<>(dictsByConn.get(connNumber - 1))
                    : new ArrayList<>();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            boolean newConnection = currentClient != client;
            if (newConnection) {
                currentClient = client;
                connectionsAccepted.incrementAndGet();
                dictsByConn.add(new ArrayList<>());
                nextSeq.set(0);
                conn2SeenFirstDataFrame = false;
            }
            int connNumber = dictsByConn.size();
            List<String> dict = dictsByConn.get(connNumber - 1);
            QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            if (connNumber == 2 && !conn2SeenFirstDataFrame && QwpWireTestUtils.tableCount(data) > 0) {
                conn2SeenFirstDataFrame = true;
                if (QwpWireTestUtils.hasDelta(data)) {
                    int[] pos = {HEADER_SIZE};
                    conn2FirstFrameDeltaStart = QwpWireTestUtils.readVarint(data, pos);
                }
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
