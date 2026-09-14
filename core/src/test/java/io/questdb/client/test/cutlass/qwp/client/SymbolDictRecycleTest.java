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
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
     * ({@code Sender.java:1760}) -- every public {@code QwpWebSocketSender.connect(...)}
     * overload leaves it null. Since the recycle feature is default-on and
     * {@code resetSymbolDictionary()} is a public advisory API, a connect()-built
     * sender could previously become "armed" with no way to ever act on it --
     * {@code isResetArmed()} reading true forever alongside a permanently-0
     * resets counter misled monitoring. {@code armIfEligible()}
     * now folds the same capability check ({@code engineRebuildFactory != null
     * && ownsCursorEngine}) into the arming decision itself, so a sender that
     * cannot rebuild never arms in the first place -- covers both ways a
     * sender can otherwise arm: the manual request and threshold crossing.
     */
    @Test
    public void testConnectBuiltSenderNeverRecyclesWithoutFactory() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();

                // Manual reset request on the simplest connect() overload.
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port)) {
                    sender.resetSymbolDictionary();
                    Assert.assertFalse("a sender with no rebuild factory must never arm, not even "
                                    + "for a manual request",
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
                    Assert.assertFalse("still never armed -- nothing changed that would flip it",
                            sender.isResetArmed());
                }

                // Threshold-based arming needs a custom low threshold, only reachable (without
                // routing through Sender.build(), which WOULD install a factory) via the
                // widest connect() overload -- mirrors SymbolDictRecycleArmingTest.testDoesNotArmWithoutRebuildFactory.
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
                    Assert.assertFalse("a sender with no rebuild factory must never arm: "
                            + "isResetArmed()==true with a permanently-0 resets counter "
                            + "misleads monitoring", sender.isResetArmed());

                    // Drained instant again: must not recycle, must not throw.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("sender must keep working with no factory installed",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertEquals("no factory -> the recycle can never actually run",
                            0, sender.getSymbolDictEpoch());
                    Assert.assertFalse("still never armed -- crossing the threshold again changes "
                                    + "nothing",
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
                    // The hot spare is provisioned by the segment-manager worker, so await it.
                    List<String> freshSlotFiles = Arrays.asList(
                            ".ack-watermark", ".lock", ".lock.pid", ".symbol-dict",
                            "sf-0000000000000000.sfa", "sf-initial.sfa", "sf-manifest.bin");
                    Assert.assertEquals("post-recycle slot must contain exactly a fresh engine's "
                                    + "own state files",
                            freshSlotFiles, awaitDir(slot, freshSlotFiles));
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
     * A transient engine-rebuild failure must NOT latch the sender terminal:
     * the recycle is abandoned before the swap commits and resumes on the
     * next send. Replaces testFailedRebuildLatchesTerminal (build() has a
     * retry-and-quarantine loop for exactly these operational
     * failures; killing a healthy sender on a provably empty slot mid-life
     * was strictly worse than the build()-time behavior).
     */
    @Test
    public void testFailedRebuildAbandonsAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    QwpWebSocketSender.EngineRebuildFactory real =
                            ws.getEngineRebuildFactoryForTesting();
                    AtomicInteger remainingFaults = new AtomicInteger(1);
                    ws.setEngineRebuildFactory(() -> {
                        if (remainingFaults.getAndDecrement() > 0) {
                            throw new RuntimeException("injected engine rebuild fault");
                        }
                        return real.rebuild();
                    });

                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());
                    try {
                        sender.table("t");
                        Assert.fail("expected the triggering table() call to throw");
                    } catch (LineSenderException expected) {
                    }
                    // NOT latched, and the swap did NOT commit.
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());
                    // The next call resumes the pending recycle with the real
                    // factory and completes it.
                    sender.table("t").symbol("s", "post").longColumn("v", 1L).atNow();
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    long f = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f, 5_000));
                }
            }
        });
    }

    /**
     * A monitor thread that read the epoch base BEFORE a recycle and reads the
     * engine AFTER it -- the torn pair the accessor clamps -- must still see the
     * pre-recycle durable watermark, never the fresh engine's -1.
     */
    @Test
    public void testGetAckedFsnTornReadAcrossRecycleKeepsDurableWatermark() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=2;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long f2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f2, 5_000));
                    Assert.assertTrue("threshold=2 crossed", ws.isResetArmed());
                    long before = ws.getAckedFsn();
                    Assert.assertEquals("two acked frames on the external scale", f2, before);
                    Assert.assertEquals(1L, before);

                    CountDownLatch baseRead = new CountDownLatch(1);
                    CountDownLatch swapped = new CountDownLatch(1);
                    AtomicBoolean fired = new AtomicBoolean();
                    AtomicReference<Throwable> monitorError = new AtomicReference<>();
                    ws.setAckedFsnReadWitnessForTesting(() -> {
                        if (!fired.compareAndSet(false, true)) {
                            return;
                        }
                        baseRead.countDown();
                        try {
                            if (!swapped.await(10, TimeUnit.SECONDS)) {
                                throw new AssertionError("the recycle never released the monitor");
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    });
                    AtomicLong observed = new AtomicLong(Long.MIN_VALUE);
                    Thread monitor = new Thread(() -> {
                        try {
                            observed.set(ws.getAckedFsn());
                        } catch (Throwable t) {
                            monitorError.set(t);
                        }
                    }, "acked-fsn-monitor");
                    monitor.start();
                    try {
                        Assert.assertTrue("monitor never reached the witness",
                                baseRead.await(5, TimeUnit.SECONDS));
                        ws.setAckedFsnReadWitnessForTesting(null);
                        // The recycle runs synchronously inside this call: the
                        // base rolls past f2 and a fresh engine (ackedFsn == -1)
                        // is installed while the monitor still holds the OLD base.
                        sender.table("t");
                        Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    } finally {
                        swapped.countDown();
                        monitor.join(10_000);
                    }
                    if (monitorError.get() != null) {
                        throw new AssertionError("monitor failed", monitorError.get());
                    }
                    Assert.assertEquals("a torn (old base, fresh engine) read must clamp to the "
                            + "durable watermark", before, observed.get());
                    Assert.assertEquals("the producer-thread read after the swap", before, ws.getAckedFsn());
                }
            }
        });
    }

    /**
     * The close-drain timeout names FSNs on the same external scale
     * {@code flushAndGetSequence()} hands out, so an operator can feed the
     * printed target straight back into {@code awaitAckedFsn()} after a recycle.
     */
    @Test
    public void testCloseDrainTimeoutReportsExternalFsnsAfterRecycle() throws Exception {
        assertMemoryLeak(() -> {
            SwitchableAckHandler handler = new SwitchableAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String config = cfg(server) + "symbol_dict_reset_threshold=2;close_flush_timeout_millis=200;";
                try (Sender sender = Sender.fromConfig(config)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long f1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f1, 5_000));
                    Assert.assertTrue("threshold=2 crossed", ws.isResetArmed());

                    // Nothing published on the fresh epoch is ever acked.
                    handler.acking = false;
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow(); // recycles here
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    long f2 = sender.flushAndGetSequence();
                    Assert.assertEquals("the fresh epoch continues the external scale", f1 + 1, f2);
                    try {
                        sender.close();
                        Assert.fail("expected the close drain to time out");
                    } catch (LineSenderException e) {
                        String msg = e.getMessage();
                        Assert.assertTrue("drain message must name the external target and acked "
                                + "watermark [expected targetFsn=" + f2 + ", ackedFsn=" + (f2 - 1)
                                + "; msg=" + msg + ']',
                                msg.contains("[targetFsn=" + f2 + ", ackedFsn=" + (f2 - 1) + "]"));
                    }
                }
            }
        });
    }

    /**
     * A recycle tears the wire connection down and reconnects deliberately.
     * The fresh loop's success is classified against the sender-lifetime
     * flags, so it reports RECONNECTED (same endpoint), and no DISCONNECTED
     * precedes it -- a recycle is not an outage.
     */
    @Test
    public void testRecycleReportsReconnectedWithoutDisconnected() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=2;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    List<SenderConnectionEvent.Kind> kinds = Collections.synchronizedList(new ArrayList<>());
                    ws.setConnectionListener(event -> kinds.add(event.getKind()));

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow(); // recycles here
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    awaitKind(kinds, SenderConnectionEvent.Kind.RECONNECTED);

                    Assert.assertEquals("exactly one RECONNECTED for the recycle's reconnect [kinds=" + kinds + ']',
                            1, Collections.frequency(kinds, SenderConnectionEvent.Kind.RECONNECTED));
                    Assert.assertFalse("a recycle is not an outage: no DISCONNECTED [kinds=" + kinds + ']',
                            kinds.contains(SenderConnectionEvent.Kind.DISCONNECTED));
                    Assert.assertFalse("same endpoint: never FAILED_OVER [kinds=" + kinds + ']',
                            kinds.contains(SenderConnectionEvent.Kind.FAILED_OVER));
                }
            }
        });
    }

    @Test
    public void testRebuildFactoryReceivesTheLiveErrorHandler() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    SenderErrorHandler installedAfterBuild = error -> { };
                    ws.setErrorHandler(installedAfterBuild);

                    QwpWebSocketSender.EngineRebuildFactory real = ws.getEngineRebuildFactoryForTesting();
                    AtomicReference<SenderErrorHandler> handlerSeen = new AtomicReference<>();
                    AtomicBoolean handlerlessOverloadCalled = new AtomicBoolean();
                    ws.setEngineRebuildFactory(new QwpWebSocketSender.EngineRebuildFactory() {
                        @Override
                        public CursorSendEngine rebuild() {
                            handlerlessOverloadCalled.set(true);
                            return real.rebuild();
                        }

                        @Override
                        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
                            handlerSeen.set(liveHandler);
                            return real.rebuild(liveHandler);
                        }
                    });

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    sender.resetSymbolDictionary();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertEquals("the recycle must have committed", 1, ws.getSymbolDictEpoch());

                    Assert.assertSame("a rebuild-time quarantine must reach the handler installed after build()",
                            installedAfterBuild, handlerSeen.get());
                    Assert.assertFalse("the sender must call the handler-aware overload",
                            handlerlessOverloadCalled.get());
                }
            }
        });
    }

    /**
     * The builder's rebuild factory ({@code Sender.build()}) is the half that
     * actually forwards the live handler into {@code constructEngineOnSlot} ->
     * {@code quarantineTornSlot}, and that hand-off is only observable when a
     * rebuild really has a slot to set aside: the notification is dispatched
     * synchronously from inside the rebuild, to whatever handler it was handed.
     * Step 3's fully-drained close leaves the slot empty, so the fixture plants
     * {@code SegmentSkipQuarantineTest}'s tainted slot -- several never-acked
     * segments with the oldest one's magic overwritten, so recovery must skip
     * it -- into that empty slot just before the real factory runs. The sender
     * is built with no error handler at all, so a regression that forwarded the
     * BUILD-time handler instead of the live one would leave the recycle green
     * and the data-loss notification nowhere.
     */
    @Test
    public void testRebuildTimeQuarantineReachesTheHandlerInstalledAfterBuild() throws Exception {
        assertMemoryLeak(() -> {
            String taintedSfDir = temporaryFolder.newFolder("rebuild-quarantine-tainted").getAbsolutePath();
            writeSlotWithCorruptedOldestSegment(taintedSfDir);
            final String taintedSlot = taintedSfDir + "/default";

            String sfDir = temporaryFolder.newFolder("rebuild-quarantine-live").getAbsolutePath();
            final String slotPath = sfDir + "/default";
            final List<SenderError> errors = new CopyOnWriteArrayList<>();
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";sf_dir=" + sfDir + ";")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    ws.setErrorHandler(errors::add);

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

                    final QwpWebSocketSender.EngineRebuildFactory real = ws.getEngineRebuildFactoryForTesting();
                    ws.setEngineRebuildFactory(new QwpWebSocketSender.EngineRebuildFactory() {
                        @Override
                        public CursorSendEngine rebuild() {
                            plantSlotContents(taintedSlot, slotPath);
                            return real.rebuild();
                        }

                        @Override
                        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
                            plantSlotContents(taintedSlot, slotPath);
                            return real.rebuild(liveHandler);
                        }
                    });

                    sender.resetSymbolDictionary();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertEquals("the recycle must have committed", 1, ws.getSymbolDictEpoch());
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                }
            }

            SenderError quarantine = null;
            for (int i = 0, n = errors.size(); i < n; i++) {
                if (errors.get(i).getCategory() == SenderError.Category.DATA_LOSS) {
                    quarantine = errors.get(i);
                    break;
                }
            }
            Assert.assertNotNull("the rebuild's quarantine must reach the handler installed AFTER "
                    + "build(); it is the only programmatic channel telling the application that "
                    + "the set-aside bytes need resending [errors=" + errors + ']', quarantine);
            Assert.assertTrue("the notification must name where the bytes went [msg="
                            + quarantine.getServerMessage() + ']',
                    quarantine.getServerMessage() != null
                            && quarantine.getServerMessage().contains("slot set aside at"));
            Assert.assertNotNull("getQuarantinedPath() is the programmatic answer to \"where are "
                    + "my bytes\"", quarantine.getQuarantinedPath());
            Assert.assertTrue("the quarantined path must name the set-aside dir [path="
                            + quarantine.getQuarantinedPath() + ']',
                    quarantine.getQuarantinedPath().contains("unreplayable-"));
        });
    }

    /**
     * A producer thread whose interrupt flag is already set usually makes
     * step 2's loop close throw before the recycle can proceed
     * (CountDownLatch.await() checks the flag first) -- an abandon that must
     * be non-terminal. Under load the I/O thread can finish and count the
     * shutdown latch down concurrently, so the close can also complete
     * normally despite the flag; this asserts the failed-stop protocol when
     * the throw happens and skips visibly when it does not. Either way the
     * recovery half always runs: once the flag is cleared the next call
     * finishes the loop close and the sender recovers.
     */
    @Test
    public void testInterruptedRecycleAbandonsAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    long f1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f1, 5_000));
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());

                    Thread.currentThread().interrupt();
                    boolean threw = false;
                    try {
                        sender.table("t");
                    } catch (LineSenderException expected) {
                        threw = true;
                    }
                    // close() re-asserts the flag whenever the interrupted await
                    // fires, whether or not the abandon then propagates to the
                    // caller; clear it for the recovery half of the test.
                    boolean flagWasPreserved = Thread.interrupted();
                    // Whether the loop close threw (abandon) or completed
                    // normally (the recycle already ran), the sender must never
                    // be terminal and must finish the recycle by now. A
                    // CLOSE_LOOP abandon leaves the recycle armed but NOT yet
                    // run, and the barrier only recycles at a drained instant
                    // with nothing staged -- so flush the recovery row before
                    // the barrier that must swap.
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    long f2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f2, 5_000));
                    sender.table("t");
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                    long f3 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f3, 5_000));

                    Assume.assumeTrue("the interrupt raced past the loop close, so the "
                            + "abandon branch was not exercised in this run", threw);
                    Assert.assertTrue("the failed-stop protocol re-asserts the flag",
                            flagWasPreserved);
                }
            }
        });
    }

    /**
     * A CLOSE_LOOP abandon must not degrade every later flush to a full
     * re-registration (which a dictionary over the server batch cap can never
     * ship at all). The resume re-registers [0..sentMaxSymbolId] as deferred
     * dictionary chunks plus the commit that closes their group; the fresh
     * loop replays them ahead of any data frame, so the first data frame
     * keeps its delta baseline.
     */
    @Test(timeout = 60_000L)
    public void testCloseLoopAbandonReregistersBaseline() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("close-loop-rereg").toString();
            ChunkCaptureHandler handler = new ChunkCaptureHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                // The CLOSE_LOOP resume's re-registration only sizes chunks when
                // serverMaxBatchSize > 0 (see resumeRecycleIfPending); an
                // unadvertised cap degrades to the plain baseline drop, same as
                // full-dict mode. Advertise a generous cap so this test actually
                // exercises the re-registration path (precedent: CloseDrainTest).
                server.setAdvertisedMaxBatchSize(4096);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertTrue("precondition: SF slot must give delta mode",
                            ws.isDeltaDictEnabledForTest());
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertTrue("setup: baseline must be established", sender.drain(5_000));
                    int n = ws.getSentMaxSymbolIdForTesting();
                    Assert.assertTrue("setup: delta baseline advanced", n >= 0);

                    ws.forceCloseLoopAbandonForTesting();

                    // The resume runs at this table() call: chunks + commit go on
                    // the ring; the row's NEW symbol makes the data frame carry a
                    // delta whose start proves the baseline survived.
                    sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                    Assert.assertEquals("baseline must survive the abandon",
                            n, ws.getSentMaxSymbolIdForTesting());
                    Assert.assertFalse("the chunks' deferred group must be closed",
                            ws.hasDeferredMessagesForTesting());

                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("the post-abandon batch must land", sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertEquals("re-registered chunks then the new symbol, in order",
                            Arrays.asList("a", "b", "c"), handler.dict());
                    Assert.assertTrue("chunk frames must precede the first data frame",
                            handler.chunkFramesBeforeFirstData > 0);
                    Assert.assertEquals("first data frame must keep the delta baseline",
                            n + 1, handler.firstDataFrameDeltaStart);
                }
            }
        });
    }

    /**
     * When the resume's re-registration fails AFTER its chunks are all on the
     * ring (the commit path), the baseline must be KEPT -- the ring's coverage
     * is full, so the fresh loop's mirror will hold exactly what the watermark
     * says -- AND the chunks' deferred-commit group must be closed: a commit
     * failure is not covered by publishDictionaryChunks' internal orphan
     * handling, and an open group clamps ackedFsn forever. The next flush then
     * continues the delta from the kept baseline instead of re-registering
     * from 0.
     */
    @Test(timeout = 60_000L)
    public void testResumeCommitFailureKeepsWatermarkAndClosesDebt() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("resume-fallback").toString();
            ChunkCaptureHandler handler = new ChunkCaptureHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                // The resume's re-registration only sizes chunks when
                // serverMaxBatchSize > 0; an unadvertised cap degrades to the
                // plain baseline drop before the fault seam is even reached.
                server.setAdvertisedMaxBatchSize(4096);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertTrue("setup: baseline must be established", sender.drain(5_000));
                    int n = ws.getSentMaxSymbolIdForTesting();
                    Assert.assertTrue("setup: delta baseline advanced", n >= 0);

                    ws.forceCloseLoopAbandonForTesting();
                    ws.setResumeCommitFaultForTesting(() -> {
                        throw new RuntimeException("injected resume-commit fault");
                    });
                    try {
                        // The resume degrades inside this call; it must NOT throw.
                        sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                    } finally {
                        ws.setResumeCommitFaultForTesting(null);
                    }
                    Assert.assertEquals("commit-path failure must keep the baseline (coverage is full)",
                            n, ws.getSentMaxSymbolIdForTesting());
                    Assert.assertFalse("the failure path must close the chunks' deferred group",
                            ws.hasDeferredMessagesForTesting());
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("next send must land on the fresh loop", sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertEquals("re-registered chunks then the new symbol, in order",
                            Arrays.asList("a", "b", "c"), handler.dict());
                    Assert.assertEquals("first data frame must continue the delta from the kept baseline",
                            n + 1, handler.firstDataFrameDeltaStart);
                }
            }
        });
    }

    /**
     * The mirror of {@link #testResumeCommitFailureKeepsWatermarkAndClosesDebt()}
     * for the resume's Error arm: an Error injected between the chunk publish
     * and the commit must still close the chunks' deferred-commit group and
     * keep the baseline (coverage is full) (the Error arm rethrows instead of
     * swallowing, but it closes the same debt the Throwable arm does), and the
     * Error itself must propagate out of {@code table()} rather than being
     * absorbed.
     */
    @Test(timeout = 60_000L)
    public void testResumeCommitErrorStillClosesDebt() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("resume-commit-error").toString();
            ChunkCaptureHandler handler = new ChunkCaptureHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(4096);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertTrue("setup: baseline must be established", sender.drain(5_000));
                    int n = ws.getSentMaxSymbolIdForTesting();
                    Assert.assertTrue("setup: delta baseline advanced", n >= 0);

                    ws.forceCloseLoopAbandonForTesting();
                    ws.setResumeCommitFaultForTesting(() -> {
                        throw new AssertionError("injected resume-commit error");
                    });
                    try {
                        try {
                            // The resume's Error arm rethrows: this call must propagate
                            // the injected Error, not swallow it.
                            sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                            Assert.fail("the Error must propagate");
                        } catch (AssertionError expected) {
                            Assert.assertTrue("propagated Error must be the injected one [msg="
                                            + expected.getMessage() + ']',
                                    expected.getMessage() != null
                                            && expected.getMessage().contains("injected resume-commit error"));
                        }
                    } finally {
                        ws.setResumeCommitFaultForTesting(null);
                    }
                    Assert.assertEquals("the Error arm must keep the baseline too (coverage is full)",
                            n, ws.getSentMaxSymbolIdForTesting());
                    Assert.assertFalse("the Error arm must close the chunks' deferred group too",
                            ws.hasDeferredMessagesForTesting());
                    // The Error propagated out of table() before any row was staged, so
                    // stage one now -- otherwise flushAndGetSequence() returns -1 and the
                    // await below proves nothing.
                    sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("next send must land on the fresh loop", sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertEquals("re-registered chunks then the new symbol, in order",
                            Arrays.asList("a", "b", "c"), handler.dict());
                    Assert.assertEquals("first data frame must continue the delta from the kept baseline",
                            n + 1, handler.firstDataFrameDeltaStart);
                }
            }
        });
    }

    /**
     * A resume re-registration that fails part-way -- a prefix of the chunks
     * on the ring, the rest not -- must leave the delta baseline at exactly the
     * ringed coverage. Above it, the orphan commit that closes the chunks'
     * group would carry a delta start the fresh loop's mirror cannot reach and
     * its replay guard would fail the loop with a false "host crash"; below
     * it, reclaimUnsentSymbolIds could hand a ringed id to a different string.
     * Three ~1500-byte symbols against a 2048-byte cap need three chunks; the
     * fault hits the THIRD so the follow-up data frame (everything above
     * coverage plus the new symbol) still fits the cap.
     * <p>
     * This test does NOT claim the resume heals a remainder wider than the
     * cap: the re-registration is one-shot and delta mode ships no chunks on
     * the ordinary flush path, so such an epoch stays wedged on the split
     * pre-flight -- a narrower population than before the clamp, which
     * re-shipped all of [0..sentMaxSymbolId] on the next flush.
     */
    @Test(timeout = 60_000L)
    public void testResumePartialPublishClampsWatermarkToCoverage() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("resume-partial").toString();
            ChunkCaptureHandler handler = new ChunkCaptureHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(2048);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertTrue("precondition: SF slot must give delta mode",
                            ws.isDeltaDictEnabledForTest());
                    // One long symbol per flush: a single frame carrying all three
                    // (~4.5 KB) would trip the split pre-flight at cap 2048.
                    String[] longSymbols = {longSymbol('a'), longSymbol('b'), longSymbol('c')};
                    for (int i = 0; i < longSymbols.length; i++) {
                        sender.table("t").symbol("s", longSymbols[i]).longColumn("v", i).atNow();
                        sender.flush();
                        Assert.assertTrue("setup: flush " + i + " must drain", sender.drain(5_000));
                    }
                    Assert.assertEquals("setup: baseline covers the three long symbols",
                            2, ws.getSentMaxSymbolIdForTesting());

                    ws.forceCloseLoopAbandonForTesting();
                    AtomicInteger chunkCalls = new AtomicInteger();
                    ws.setChunkPublishFaultForTesting(() -> {
                        if (chunkCalls.incrementAndGet() == 3) {
                            throw new RuntimeException("injected mid-publish chunk fault");
                        }
                    });
                    try {
                        // The resume degrades inside this call; it must NOT throw.
                        sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    } finally {
                        ws.setChunkPublishFaultForTesting(null);
                    }
                    Assert.assertEquals("exactly three chunk publishes must have been attempted",
                            3, chunkCalls.get());
                    Assert.assertEquals("watermark must equal the ringed coverage (chunks [0..0], [1..1])",
                            1, ws.getSentMaxSymbolIdForTesting());
                    Assert.assertFalse("the orphaned chunks' deferred group must be closed",
                            ws.hasDeferredMessagesForTesting());

                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("the post-abandon batch must land on the fresh loop",
                            sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertEquals("chunks [0..1] replayed, then the data frame re-ships id 2 and adds id 3",
                            Arrays.asList(longSymbols[0], longSymbols[1], longSymbols[2], "d"), handler.dict());
                    Assert.assertEquals("first data frame's delta must start at coverage + 1",
                            2, handler.firstDataFrameDeltaStart);
                }
            }
        });
    }

    /**
     * The memory-mode half of the coverage invariant {@link
     * #testResumePartialPublishClampsWatermarkToCoverage()}'s javadoc argues but
     * cannot exercise on an SF slot: with no persisted dictionary,
     * {@code QwpWebSocketSender.reclaimUnsentSymbolIds}' floor IS the watermark
     * itself (no {@code pd.size()} to raise it), so a watermark left BELOW the
     * ringed coverage would let {@code reset()} reclaim an id a chunk already on
     * the ring defines, and a later row could rebind it to a different string.
     * Drives {@code reset()} after the same faulted resume and asserts only the
     * two ringed symbols survive -- the assertion the SF twin cannot make, since
     * the persisted dictionary's size pins its reclaim floor at 3 whatever the
     * watermark reads.
     */
    @Test(timeout = 60_000L)
    public void testResumePartialPublishInMemoryModeKeepsReclaimFloorAtCoverage() throws Exception {
        assertMemoryLeak(() -> {
            ChunkCaptureHandler handler = new ChunkCaptureHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(2048);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertTrue("memory mode is always delta", ws.isDeltaDictEnabledForTest());
                    String[] longSymbols = {longSymbol('a'), longSymbol('b'), longSymbol('c')};
                    for (int i = 0; i < longSymbols.length; i++) {
                        sender.table("t").symbol("s", longSymbols[i]).longColumn("v", i).atNow();
                        sender.flush();
                        Assert.assertTrue("setup: flush " + i + " must drain", sender.drain(5_000));
                    }
                    Assert.assertEquals("setup: baseline covers the three long symbols",
                            2, ws.getSentMaxSymbolIdForTesting());

                    ws.forceCloseLoopAbandonForTesting();
                    AtomicInteger chunkCalls = new AtomicInteger();
                    ws.setChunkPublishFaultForTesting(() -> {
                        if (chunkCalls.incrementAndGet() == 3) {
                            throw new RuntimeException("injected mid-publish chunk fault");
                        }
                    });
                    try {
                        // The resume degrades inside this call; it must NOT throw. The
                        // staged "d" row itself is never flushed -- reset() below discards it.
                        sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    } finally {
                        ws.setChunkPublishFaultForTesting(null);
                    }
                    Assert.assertEquals("exactly three chunk publishes must have been attempted",
                            3, chunkCalls.get());
                    Assert.assertEquals("watermark must equal the ringed coverage (chunks [0..0], [1..1])",
                            1, ws.getSentMaxSymbolIdForTesting());
                    Assert.assertFalse("the orphaned chunks' deferred group must be closed",
                            ws.hasDeferredMessagesForTesting());

                    // The reclaim-floor half: abandon the staged row and reclaim
                    // everything above the watermark. With no persisted dictionary to
                    // raise the floor, only the two symbols the ring actually holds
                    // (longA, longB) may survive.
                    sender.reset();
                    Assert.assertEquals("reclaim floor must sit at coverage + 1: ids at or below "
                                    + "the watermark are on the ring",
                            2, ws.getGlobalSymbolDictionaryForTest().size());

                    sender.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                    long fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("the post-reset batch must land on the fresh loop",
                            sender.awaitAckedFsn(fsn, 10_000));
                    Assert.assertEquals("chunks [0..1] replayed, then the data frame defines id 2 = e",
                            Arrays.asList(longSymbols[0], longSymbols[1], "e"), handler.dict());
                    Assert.assertEquals("the reclaimed id must restart the delta at coverage + 1",
                            2, handler.firstDataFrameDeltaStart);
                }
            }
        });
    }

    private static String longSymbol(char c) {
        char[] chars = new char[1500];
        Arrays.fill(chars, c);
        return new String(chars);
    }

    /**
     * A live symbol set larger than the threshold must not thrash the
     * recycle: after a swap, re-arming requires the dictionary to reach
     * max(threshold, 2 * size-at-swap).
     */
    @Test
    public void testLiveSetAboveThresholdDoesNotThrash() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                // threshold=4; the live set has 6 distinct symbols
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=4;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    String[] live = {"s0", "s1", "s2", "s3", "s4", "s5"};
                    sendLiveSet(sender, live);              // registers 6 distinct -> arms
                    sender.table("t");                       // barrier -> recycle #1
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    // Refill from the SAME live pool three times over: 6 is above
                    // the threshold but below the doubled floor (12) -> no re-arm.
                    for (int pass = 0; pass < 3; pass++) {
                        sendLiveSet(sender, live);
                        sender.table("t");
                    }
                    Assert.assertEquals("a bounded live set must not re-trigger the recycle",
                            1, ws.getSymbolDictEpoch());
                    // Genuine growth past the floor DOES re-arm: 12 fresh symbols.
                    String[] grown = new String[12];
                    for (int i = 0; i < 12; i++) {
                        grown[i] = "g" + i;
                    }
                    sendLiveSet(sender, grown);
                    sender.table("t");
                    Assert.assertEquals(2, ws.getSymbolDictEpoch());
                }
            }
        });
    }

    private void sendLiveSet(Sender sender, String[] symbols) throws Exception {
        for (String s : symbols) {
            sender.table("t").symbol("s", s).longColumn("v", 1L).atNow();
        }
        long f = sender.flushAndGetSequence();
        Assert.assertTrue(sender.awaitAckedFsn(f, 5_000));
    }

    /**
     * Polls {@link #listDir(String)} until it equals {@code expected} or 5 s pass, and
     * returns the last listing either way so the caller's assertEquals reports the
     * actual contents on timeout.
     */
    private static List<String> awaitDir(String dir, List<String> expected) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        List<String> names = listDir(dir);
        while (!expected.equals(names) && System.nanoTime() < deadlineNanos) {
            Thread.sleep(20L);
            names = listDir(dir);
        }
        return names;
    }

    private static void awaitKind(List<SenderConnectionEvent.Kind> kinds, SenderConnectionEvent.Kind kind)
            throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!kinds.contains(kind)) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new AssertionError("listener never saw " + kind + " [seen=" + kinds + ']');
            }
            Thread.sleep(20L);
        }
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

    /**
     * Overwrites the 4-byte {@code FILE_MAGIC} field at offset 0 so
     * {@code MmapSegment.openExisting} throws at the magic check, landing in
     * {@code SegmentRing}'s per-file skip arm without disturbing any other byte.
     * {@code SegmentSkipQuarantineTest}'s technique, unchanged.
     */
    private static void corruptMagic(String path) {
        int fd = Files.openRW(path);
        Assert.assertTrue("openRW failed", fd >= 0);
        long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(buf, 0xBADBAD00);
            Files.write(fd, buf, 4, 0);
        } finally {
            Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
            Files.close(fd);
        }
    }

    /**
     * Copies a slot's recoverable content into {@code slotPath}, creating it if
     * the outgoing close removed it. Both lock files are left behind: the
     * directory-local {@code .lock} belongs to the engine about to be built,
     * and the logical lock lives outside the slot directory entirely.
     */
    private static void plantSlotContents(String sourceSlot, String slotPath) {
        try {
            java.nio.file.Path target = Paths.get(slotPath);
            java.nio.file.Files.createDirectories(target);
            java.nio.file.DirectoryStream<java.nio.file.Path> entries =
                    java.nio.file.Files.newDirectoryStream(Paths.get(sourceSlot));
            try {
                for (java.nio.file.Path entry : entries) {
                    String name = entry.getFileName().toString();
                    if (".lock".equals(name) || ".lock.pid".equals(name)) {
                        continue;
                    }
                    java.nio.file.Files.copy(entry, target.resolve(name),
                            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                entries.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("could not plant the slot contents", e);
        }
    }

    /**
     * {@code SegmentSkipQuarantineTest}'s fixture, written under {@code sfDir}:
     * a real slot produced against a never-acking server so several segments
     * survive on disk unacked, with the oldest one's magic bytes then
     * overwritten. {@code sf_max_segment_bytes} forces a genuine rotation, so
     * the corruption is a skip among data-bearing survivors rather than the
     * only file present.
     */
    private static void writeSlotWithCorruptedOldestSegment(String sfDir) throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
        })) {
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String pad = TestUtils.repeat("x", 64);
            String cfg = "ws::addr=localhost:" + silent.getPort()
                    + ";sf_dir=" + sfDir
                    + ";sf_max_segment_bytes=512"
                    + ";close_flush_timeout_millis=0;";
            try (Sender s = Sender.fromConfig(cfg)) {
                for (int i = 0; i < 20; i++) {
                    s.table("foo").stringColumn("p", pad).longColumn("v", i).atNow();
                    s.flush();
                }
            }
        }
        String slot = sfDir + "/default";
        String oldest = slot + "/sf-initial.sfa";
        Assert.assertTrue("setup: nothing acked, so sf-initial.sfa must survive", Files.exists(oldest));
        int segments = 0;
        List<String> names = listDir(slot);
        for (int i = 0, n = names.size(); i < n; i++) {
            if (names.get(i).endsWith(".sfa")) {
                segments++;
            }
        }
        Assert.assertTrue("setup: the slot must hold more than one segment so the corruption is a "
                + "skip among survivors [names=" + names + ']', segments > 1);
        corruptMagic(oldest);
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

    /** Acks every frame while {@link #acking} is set; withholds every ack afterwards. */
    private static class SwitchableAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);
        volatile boolean acking = true;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (!acking) {
                return;
            }
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

    /**
     * Tracks the most recent connection only (identity-keyed, like
     * OutageRecycleHandler): accumulates the dictionary in arrival order,
     * counts table-less delta frames (dictionary chunks) seen before the
     * first data frame, and records the first data frame's delta start.
     * Acks everything.
     */
    private static class ChunkCaptureHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final List<String> dict = new ArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler currentClient;
        private boolean seenFirstDataFrame;
        volatile int chunkFramesBeforeFirstData;
        volatile int firstDataFrameDeltaStart = -1;

        synchronized List<String> dict() {
            return new ArrayList<>(dict);
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                dict.clear();
                nextSeq.set(0);
                seenFirstDataFrame = false;
                chunkFramesBeforeFirstData = 0;
                firstDataFrameDeltaStart = -1;
            }
            QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            boolean isDataFrame = QwpWireTestUtils.tableCount(data) > 0;
            if (!seenFirstDataFrame) {
                if (isDataFrame) {
                    seenFirstDataFrame = true;
                    if (QwpWireTestUtils.hasDelta(data)) {
                        int[] pos = {HEADER_SIZE};
                        firstDataFrameDeltaStart = QwpWireTestUtils.readVarint(data, pos);
                    }
                } else if (QwpWireTestUtils.hasDelta(data)) {
                    chunkFramesBeforeFirstData++;
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
