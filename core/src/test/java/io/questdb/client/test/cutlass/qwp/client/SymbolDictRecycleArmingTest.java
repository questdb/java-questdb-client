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
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Covers the arming half of the symbol-dictionary recycle feature:
 * {@code QwpWebSocketSender.armIfEligible()}, called at the tail of
 * {@code resetTableBuffersAfterFlush()}, and the manual advisory API
 * {@link Sender#resetSymbolDictionary()}.
 */
public class SymbolDictRecycleArmingTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testArmsAtThreshold() throws Exception {
        // threshold=3, send rows with symbols a,b -> flush -> not armed;
        // add c -> flush -> armed
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=3;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1).atNow();
                    sender.flush();
                    Assert.assertFalse(ws.isResetArmed());
                    sender.table("t").symbol("s", "c").longColumn("v", 1).atNow();
                    sender.flush();
                    Assert.assertTrue(ws.isResetArmed());
                }
            }
        });
    }

    /**
     * Decision 5: arming ignores {@code deltaDictEnabled} -- threshold-based
     * evaluation must still run once the sender has degraded to full self-sufficient
     * frames. Reaching a custom low {@code symbol_dict_reset_threshold} on a
     * sender that also carries the fault-injecting {@code FilesFacade} needs
     * {@code QwpWebSocketSender}'s widest {@code connect(List<Endpoint>, ...)}
     * overload: {@code Sender.fromConfig} has no {@code FilesFacade} seam, and
     * every narrower {@code connect(host, port, ...)} overload hard-codes the
     * default threshold (100,000). That overload sets
     * {@code sender.resetThresholdSymbols} directly and also accepts the
     * hand-built {@code CursorSendEngine}, so both requirements are reachable
     * together.
     * <p>
     * This {@code connect(...)} overload installs no {@link
     * io.questdb.client.cutlass.qwp.client.QwpWebSocketSender.EngineRebuildFactory
     * EngineRebuildFactory} (only {@code Sender.build()} does), so
     * crossing the threshold must never actually arm -- {@code
     * armIfEligible()} folds the capability check in ahead of the threshold
     * comparison. Decision 5 is instead pinned negatively here: full-dict
     * degradation does not change that verdict either way.
     */
    @Test
    public void testDoesNotArmWithoutRebuildFactory() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("arm-full-dict-sf").toString();
            String slot = Paths.get(sfDir, "default").toString();
            Assert.assertEquals(0, io.questdb.client.std.Files.mkdir(sfDir,
                    io.questdb.client.std.Files.DIR_MODE_DEFAULT));

            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();

                MmapFaultDictFacade ff = new MmapFaultDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
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
                        3, // symbolDictResetThresholdSymbols -- low, deliberately crossed below
                        QwpWebSocketSender.DEFAULT_SYMBOL_DICT_RESET_MAX_WAIT_MILLIS);
                try {
                    ff.armed = true; // next dictionary mmap growth raises a recognised fault
                    sender.table("m").symbol("s", "a").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the injected mmap fault to fail this flush");
                    } catch (LineSenderException expected) {
                        // Same guard MmapFaultDegradesTest pins: the fault degrades the
                        // sender to full self-sufficient frames instead of propagating raw.
                    }
                    Assert.assertFalse("a recognised mmap access fault must degrade the sender "
                                    + "to full-dict mode",
                            sender.isDeltaDictEnabledForTest());
                    Assert.assertFalse("dictionary has only 1 entry, below the threshold of 3",
                            sender.isResetArmed());

                    // The fault facade disarms itself after firing once, so this retry
                    // succeeds and clears pendingRowCount back to 0; "a" is now published.
                    sender.flush();
                    Assert.assertFalse("still degraded, dictionary still below threshold",
                            sender.isDeltaDictEnabledForTest());
                    Assert.assertFalse(sender.isResetArmed());

                    sender.table("m").symbol("s", "b").longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertFalse("dictionary has 2 entries, still below the threshold of 3",
                            sender.isResetArmed());

                    // No manual resetSymbolDictionary() call anywhere in this test: crossing
                    // the threshold, even while degraded, still must not arm -- this
                    // connect(...) overload installs no engineRebuildFactory,
                    // and that capability check now runs ahead of the threshold
                    // comparison in armIfEligible().
                    sender.table("m").symbol("s", "c").longColumn("v", 3L).atNow();
                    sender.flush();
                    Assert.assertFalse("a sender with no rebuild factory must never arm, even once "
                                    + "the threshold is crossed in full-dict mode",
                            sender.isResetArmed());
                } finally {
                    sender.close();
                }
            }
        });
    }

    @Test
    public void testDoesNotArmWhenDisabled() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(
                        cfg(server) + "symbol_dict_reset=off;symbol_dict_reset_threshold=2;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertFalse("symbol_dict_reset=off must never arm", ws.isResetArmed());
                    sender.table("t").symbol("s", "c").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertFalse("symbol_dict_reset=off must never arm", ws.isResetArmed());
                }
            }
        });
    }

    @Test
    public void testManualResetRequestArms() throws Exception {
        assertMemoryLeak(() -> {
            // pendingRowCount == 0: resetSymbolDictionary() arms immediately.
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());
                }
            }

            // Mid-batch: a request while a row is buffered (pendingRowCount != 0)
            // only arms once the next flush runs armIfEligible() at its tail.
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.resetSymbolDictionary();
                    Assert.assertFalse("a mid-batch request must not arm before the next flush",
                            ws.isResetArmed());
                    sender.flush();
                    Assert.assertTrue(ws.isResetArmed());
                }
            }
        });
    }

    @Test
    public void testReArmFloorDoublesPerSwapAndBlocksOrganicReArm() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("floor-sf").toString();
            try (TestWebSocketServer server = ackingServer()) {
                String config = cfg(server) + "sf_dir=" + sfDir + ";symbol_dict_reset_threshold=2;";
                try (Sender sender = Sender.fromConfig(config)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertEquals("no swap yet: floor is 0", 0, ws.getResetFloorSymbolsForTesting());

                    // epoch 0: two symbols == threshold -> arms
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    // swap #1 runs inside this table() with dictSizeAtSwap == 2
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    Assert.assertEquals("floor = 2 x size-at-swap", 4, ws.getResetFloorSymbolsForTesting());

                    // epoch 1: c,d,e,f == floor -> arms again
                    sender.table("t").symbol("s", "d").longColumn("v", 2L).atNow();
                    sender.table("t").symbol("s", "e").longColumn("v", 2L).atNow();
                    sender.table("t").symbol("s", "f").longColumn("v", 2L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    Assert.assertTrue("size 4 >= max(threshold 2, floor 4) must arm", ws.isResetArmed());

                    // swap #2 with dictSizeAtSwap == 4
                    sender.table("t").symbol("s", "g").longColumn("v", 3L).atNow();
                    Assert.assertEquals(2, ws.getSymbolDictEpoch());
                    Assert.assertEquals("floor doubles again", 8, ws.getResetFloorSymbolsForTesting());

                    // epoch 2: four symbols is above the threshold but below the floor
                    sender.table("t").symbol("s", "h").longColumn("v", 4L).atNow();
                    sender.table("t").symbol("s", "i").longColumn("v", 4L).atNow();
                    sender.table("t").symbol("s", "j").longColumn("v", 4L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    Assert.assertFalse("size 4 < floor 8 must not re-arm organically", ws.isResetArmed());

                    sender.resetSymbolDictionary();
                    Assert.assertTrue("the advisory request bypasses the floor", ws.isResetArmed());
                }
            }
        });
    }

    @Test
    public void testResetSymbolDictionaryOnNonWsSenderIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // protocolVersion(2) skips the eager server-side settings detection
            // connect that build() otherwise performs, so no live server is needed
            // (see LineSenderBuilderTest.testCustomPemRootsDoNotRequirePassword).
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost")
                    .protocolVersion(2)
                    .build()) {
                sender.resetSymbolDictionary();
            }
        });
    }

    @Test
    public void testSplitFlushPathArms() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                server.setAdvertisedMaxBatchSize(150); // forces the two-table batch to split
                // Padding inflates each table past half the cap, so the combined
                // two-table message exceeds it while each single-table split frame fits.
                String pad = TestUtils.repeat("x", 60);
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=2;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t1").symbol("s", "a").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "b").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertTrue("the split-flush path shares resetTableBuffersAfterFlush's tail",
                            ws.isResetArmed());
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

    private static String cfg(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";";
    }

    /**
     * ACKs every frame it receives; does not otherwise inspect the wire.
     */
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
     * Raises a RECOGNISED mmap access fault out of the persisted dictionary's next
     * mmap growth, once, when {@link #armed}. Copied from
     * {@code MmapFaultDegradesTest.MmapFaultDictFacade}.
     */
    private static final class MmapFaultDictFacade extends DelegatingFilesFacade {
        boolean armed;

        @Override
        public boolean isMmapAllowed() {
            return true;
        }

        @Override
        public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
            if (armed) {
                armed = false;
                throw new InternalError(
                        "a fault occurred in a recent unsafe memory access operation in compiled Java code");
            }
            return INSTANCE.mmap(fd, len, offset, flags, memoryTag);
        }
    }
}
