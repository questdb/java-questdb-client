/*******************************************************************************
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The healing half of the symbol-dictionary recycle feature: a sender that degraded to
 * full self-sufficient frames ({@code QwpWebSocketSender.disableDeltaDict}, e.g. after a
 * recognised mmap access fault on the persisted dictionary -- see {@link MmapFaultDegradesTest})
 * is not stuck there forever. {@code recycleForDictReset()} rebuilds the cursor engine from
 * scratch (step 5), and a fresh engine re-derives {@code deltaDictEnabled} independently of
 * whatever the outgoing epoch's engine decided -- so once the underlying fault clears, the
 * next recycle heals the sender back into delta mode. If the fault has not cleared, the fresh
 * engine simply degrades again on its own first append: a normal, catchable
 * {@link LineSenderException}, not a latched {@code recycleFailure} terminal state.
 * <p>
 * Also covers the three permanent recycle-metrics getters ({@code getSymbolDictEpoch()},
 * {@code getSymbolDictResetsPerformed()}, {@code getSymbolDictResetStarvationTimeouts()}).
 */
public class SymbolDictRecycleHealingTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testMetricsAfterTwoRecycles() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("metrics-sf").toString();
            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertEquals(0, ws.getSymbolDictEpoch());
                    Assert.assertEquals(0, ws.getSymbolDictResetsPerformed());
                    Assert.assertEquals(0, ws.getSymbolDictResetStarvationTimeouts());

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("armed: 2 distinct symbols crossed threshold=2", ws.isResetArmed());

                    // Ring drained -> this table() call recycles synchronously: epoch 1.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    Assert.assertEquals(1, ws.getSymbolDictResetsPerformed());
                    Assert.assertEquals("no starvation wait was deliberately triggered",
                            0, ws.getSymbolDictResetStarvationTimeouts());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));
                    // The anti-thrash floor (resetFloorSymbols = 2x the first swap's
                    // dictSizeAtSwap = 4) keeps c,d (2 symbols, == threshold but < floor)
                    // from re-arming on their own; a manual request bypasses the floor by
                    // design, so drive the second recycle through resetSymbolDictionary().
                    sender.resetSymbolDictionary();
                    Assert.assertTrue("manual reset request bypasses the re-arm floor",
                            ws.isResetArmed());

                    // Ring drained again -> second recycle: epoch 2.
                    sender.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                    Assert.assertEquals(2, ws.getSymbolDictEpoch());
                    Assert.assertEquals(2, ws.getSymbolDictResetsPerformed());
                    Assert.assertEquals("still no starvation wait was deliberately triggered",
                            0, ws.getSymbolDictResetStarvationTimeouts());

                    long fsn3 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn3, 5_000));
                }
            }
        });
    }

    /**
     * The recovery-side sibling of {@code MmapFaultDegradesTest.testMmapAccessFaultDegradesPersistInsteadOfPropagating}:
     * once the sender has degraded to full self-sufficient frames, the underlying fault clears,
     * and a recycle rebuilds the engine, the fresh engine must re-derive delta-dict mode from
     * scratch rather than staying degraded forever. Wire evidence: the first post-recycle frame
     * (a fresh, empty dictionary) starts a delta at 0; the SECOND post-recycle frame, which
     * introduces exactly one more symbol, starts its delta where the first one left off and
     * carries only that one new entry -- the shape only delta mode produces. In full-dict mode
     * every frame re-ships the whole dictionary from id 0 (see
     * {@code QwpWebSocketSender.symbolDeltaBaseline()}: confirmedMaxId is permanently -1), so
     * this pair of frames could not look like this if healing had not taken effect.
     */
    @Test
    public void testRecycleHealsFullDictDegradeBackToDeltaMode() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("heal-sf").toString();
            String slot = Paths.get(sfDir, "default").toString();
            Assert.assertEquals(0, io.questdb.client.std.Files.mkdir(sfDir,
                    io.questdb.client.std.Files.DIR_MODE_DEFAULT));

            CapturingAckHandler handler = new CapturingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();

                HealableMmapFaultFacade ff = new HealableMmapFaultFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
                QwpWebSocketSender sender = buildSender(port, engine, 100_000);
                // connect() never installs an engineRebuildFactory (only Sender.build() does),
                // so the recycle would otherwise be a no-op. Install one that rebuilds on the
                // SAME slot with the SAME (healable) facade -- mirroring the real factory
                // Sender.build() installs, minus the FilesFacade seam Sender.fromConfig lacks.
                sender.setEngineRebuildFactory(() -> new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff));
                try {
                    Assert.assertTrue("must start in delta mode", sender.isDeltaDictEnabledForTest());

                    // Degrade mid-life: fault the dictionary's next mmap growth.
                    ff.armed = true;
                    sender.table("m").symbol("s", "a").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the injected mmap fault to fail this flush");
                    } catch (LineSenderException expected) {
                        // same guard MmapFaultDegradesTest pins
                        Assert.assertTrue("the fault must be reported as a sender error, not a "
                                        + "raw InternalError: " + expected.getMessage(),
                                expected.getMessage().contains(
                                        "failed to persist symbol dictionary before publish"));
                    }
                    Assert.assertFalse("a recognised mmap access fault must degrade the sender",
                            sender.isDeltaDictEnabledForTest());

                    // Heal the facade. The retry below does not itself touch mmap --
                    // persistNewSymbolsBeforePublish short-circuits once !deltaDictEnabled --
                    // so healing here matters only for what the fresh post-recycle engine sees.
                    ff.armed = false;
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("the degraded retry must still ingest the row",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertEquals(0, sender.getSymbolDictEpoch());

                    // Drained: arm and trigger the recycle.
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(sender.isResetArmed());
                    sender.table("m").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertFalse("recycle must disarm", sender.isResetArmed());
                    Assert.assertEquals(1, sender.getSymbolDictEpoch());
                    Assert.assertEquals(1, sender.getSymbolDictResetsPerformed());

                    // The rebuilt engine re-derives delta-dict mode from scratch (a fresh,
                    // empty dictionary always opens cleanly at construction -- see this
                    // test's persistent-fault sibling for why this alone does not prove the
                    // facade was healed). The discriminating check is below, after the first
                    // post-recycle append.
                    Assert.assertTrue(sender.isDeltaDictEnabledForTest());

                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));

                    // Discriminating check: fsn2's flush was the fresh engine's first
                    // append. A still-armed facade would have degraded it there (as the
                    // persistent-fault sibling proves against the identical setup) -- staying
                    // true here is real evidence the heal took effect, not just an artifact
                    // of fresh-engine construction never touching mmap.
                    Assert.assertTrue("a healed facade must let the fresh engine's first "
                                    + "post-recycle append succeed and keep delta mode enabled",
                            sender.isDeltaDictEnabledForTest());

                    sender.table("m").symbol("s", "c").longColumn("v", 3L).atNow();
                    long fsn3 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn3, 5_000));

                    List<byte[]> postRecycleFrames = handler.framesFor(2);
                    Assert.assertTrue("post-recycle connection must have sent at least 2 data frames",
                            postRecycleFrames.size() >= 2);
                    int[] first = deltaStartAndCount(postRecycleFrames.get(0));
                    int[] second = deltaStartAndCount(postRecycleFrames.get(1));
                    Assert.assertEquals("first post-recycle frame starts a fresh dictionary at 0",
                            0, first[0]);
                    Assert.assertEquals("first post-recycle frame carries only the one new symbol (b)",
                            1, first[1]);
                    Assert.assertEquals("delta mode: the second frame's delta starts where the first left off",
                            first[0] + first[1], second[0]);
                    Assert.assertEquals("delta mode: the second frame carries only the newly-added symbol (c), "
                                    + "not the whole dictionary re-shipped from 0 as full-dict mode would",
                            1, second[1]);
                } finally {
                    sender.close();
                }
            }
        });
    }

    /**
     * Persistent-fault sibling of {@link #testRecycleHealsFullDictDegradeBackToDeltaMode}: the
     * facade is never healed, so the freshly rebuilt engine hits the SAME fault on its own first
     * append and degrades again. Construction alone does not touch mmap (a brand-new, empty
     * dictionary file needs only {@code openCleanRW}/{@code write} for its header -- see
     * {@code PersistedSymbolDict.openFresh}), so the fresh engine transiently reports delta mode
     * right after the recycle; the degrade only becomes observable once something actually
     * appends to it. Either way this must stay a degrade, not a break: a plain, catchable
     * {@link LineSenderException} on the one flush that hits the fault, no raw {@code Error}
     * escaping, no latched {@code recycleFailure} terminal state, and the sender keeps ingesting
     * rows (in full-dict mode) right after.
     */
    @Test
    public void testRecycleDegradesAgainWhenFaultPersists() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("persistent-fault-sf").toString();
            String slot = Paths.get(sfDir, "default").toString();
            Assert.assertEquals(0, io.questdb.client.std.Files.mkdir(sfDir,
                    io.questdb.client.std.Files.DIR_MODE_DEFAULT));

            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();

                HealableMmapFaultFacade ff = new HealableMmapFaultFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
                QwpWebSocketSender sender = buildSender(port, engine, 100_000);
                sender.setEngineRebuildFactory(() -> new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff));
                try {
                    // Degrade once before the recycle, same setup as the healing test.
                    ff.armed = true;
                    sender.table("m").symbol("s", "a").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the injected mmap fault to fail this flush");
                    } catch (LineSenderException expected) {
                        // expected -- same guard as MmapFaultDegradesTest
                        Assert.assertTrue("the fault must be reported as a sender error, not a "
                                        + "raw InternalError: " + expected.getMessage(),
                                expected.getMessage().contains(
                                        "failed to persist symbol dictionary before publish"));
                    }
                    Assert.assertFalse(sender.isDeltaDictEnabledForTest());
                    long fsn1 = sender.flushAndGetSequence(); // retry succeeds in full-dict mode
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));

                    // Do NOT heal: the facade is still armed when the recycle rebuilds the
                    // engine, so the fresh engine's own first append hits it again.
                    sender.resetSymbolDictionary();
                    sender.table("m").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertEquals(1, sender.getSymbolDictEpoch());
                    Assert.assertEquals(1, sender.getSymbolDictResetsPerformed());

                    // Construction alone never touches mmap (see this test's javadoc), so the
                    // fresh engine transiently re-derives delta mode before its first append.
                    Assert.assertTrue("a fresh engine's construction never touches mmap, so it "
                                    + "transiently re-derives delta mode before its first append",
                            sender.isDeltaDictEnabledForTest());

                    // ...until this flush's append hits the still-armed facade and degrades it
                    // again, exactly like the pre-recycle fault: a clean LineSenderException,
                    // never a raw Error, and the sender is not latched terminal.
                    try {
                        sender.flush();
                        Assert.fail("expected the still-armed facade to fault the post-recycle append too");
                    } catch (LineSenderException expected) {
                        // degrade, not break: a normal, catchable sender error
                        Assert.assertTrue("the fault must be reported as a sender error, not a "
                                        + "raw InternalError: " + expected.getMessage(),
                                expected.getMessage().contains(
                                        "failed to persist symbol dictionary before publish"));
                    }
                    Assert.assertFalse("the fresh engine must degrade again, not stay in delta mode",
                            sender.isDeltaDictEnabledForTest());

                    // Degrade, not break: the sender keeps working (full-dict mode now), no
                    // latched recycleFailure and no exception escaping this retry.
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("the row must still get ingested after the second degrade",
                            sender.awaitAckedFsn(fsn2, 5_000));
                } finally {
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

    /**
     * Widest {@code QwpWebSocketSender.connect(...)} overload with everything but the
     * fault-injecting engine and the reset threshold pinned to defaults -- mirrors
     * {@code SymbolDictRecycleArmingTest.testDoesNotArmWithoutRebuildFactory}. {@code Sender.fromConfig} has
     * no {@code FilesFacade} seam, so this is the only way to combine a custom facade with a
     * custom low threshold; it leaves {@code engineRebuildFactory} null, same as every other
     * {@code connect(...)} overload, so callers that need a working recycle must install one
     * with {@code setEngineRebuildFactory} afterwards.
     */
    private static QwpWebSocketSender buildSender(int port, CursorSendEngine engine, int thresholdSymbols)
            throws Exception {
        return QwpWebSocketSender.connect(
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
                thresholdSymbols,
                QwpWebSocketSender.DEFAULT_SYMBOL_DICT_RESET_MAX_WAIT_MILLIS);
    }

    /** {@code [deltaStart, deltaCount]} read from a frame that must carry a symbol-dict delta. */
    private static int[] deltaStartAndCount(byte[] frame) {
        Assert.assertTrue("frame must carry a symbol-dict delta", QwpWireTestUtils.hasDelta(frame));
        int[] position = {HEADER_SIZE};
        int deltaStart = QwpWireTestUtils.readVarint(frame, position);
        int deltaCount = QwpWireTestUtils.readVarint(frame, position);
        return new int[]{deltaStart, deltaCount};
    }

    /**
     * ACKs every frame it receives; does not otherwise inspect the wire.
     * <p>
     * WARNING -- recycle-only handler, do not copy into a plain-reconnect test.
     * The per-connection sequence reset below assumes every connection change
     * is a recycle, i.e. that a fresh engine is behind the new connection and
     * its raw FSNs really do restart at 0. On an ordinary reconnect the SAME
     * engine survives and keeps counting, so resetting here would ack frames
     * the sender never published and silently advance its watermark past
     * unsent data.
     */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                // A rebuilt engine restarts its raw FSNs at 0 (externalFsnBase absorbs the
                // offset), and the ack sequence below is applied as a raw engine FSN -- so
                // acking a recycle's fresh connection against the outgoing connection's
                // sequence would ack frames that were never published. Reset per connection,
                // matching CapturingAckHandler below.
                currentClient = client;
                nextSeq.set(0);
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** ACKs every frame and records the raw bytes of every data frame, grouped by connection. */
    private static class CapturingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final List<List<byte[]>> framesByConn = new CopyOnWriteArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

        synchronized List<byte[]> framesFor(int connNumber) {
            return connNumber <= framesByConn.size()
                    ? new CopyOnWriteArrayList<>(framesByConn.get(connNumber - 1))
                    : Collections.emptyList();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                framesByConn.add(new CopyOnWriteArrayList<>());
                nextSeq.set(0);
            }
            if (QwpWireTestUtils.tableCount(data) > 0) {
                framesByConn.get(framesByConn.size() - 1).add(data);
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Raises a RECOGNISED mmap access fault out of the persisted dictionary's every mmap growth
     * while {@link #armed}, and stops (returns to normal behaviour) as soon as a test flips
     * {@link #armed} back to {@code false}. Unlike the self-disarming
     * {@code MmapFaultDegradesTest.MmapFaultDictFacade} / {@code SymbolDictRecycleArmingTest.MmapFaultDictFacade},
     * this one stays armed across as many mmap calls as the test wants -- so a test can
     * explicitly "heal" the underlying storage by flipping the flag, or deliberately leave it
     * faulting across a recycle to prove the fresh engine degrades again instead of masking the
     * still-broken medium.
     */
    private static final class HealableMmapFaultFacade extends DelegatingFilesFacade {
        volatile boolean armed;

        @Override
        public boolean isMmapAllowed() {
            return true;
        }

        @Override
        public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
            if (armed) {
                throw new InternalError(
                        "a fault occurred in a recent unsafe memory access operation in compiled Java code");
            }
            return INSTANCE.mmap(fd, len, offset, flags, memoryTag);
        }
    }
}
