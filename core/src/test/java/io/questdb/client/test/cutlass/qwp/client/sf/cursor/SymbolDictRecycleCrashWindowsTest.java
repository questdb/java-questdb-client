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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Crash-window recovery for {@code QwpWebSocketSender.recycleForDictReset()}
 * (Task 5's 8-step symbol-dictionary recycle swap, quoted here for reference):
 * <pre>
 * 1. lastPublishedFsn = cursorEngine.publishedFsn()
 * 2. close cursorSendLoop (I/O thread + client)
 * 3. cursorEngine.close() -- FULLY DRAINED (the barrier only fires the swap once
 *    isRingDrained() is true), so this unlinks every *.sfa, the ack watermark,
 *    the persisted dictionary and the logical slot lock, leaving the slot empty.
 * 4. rollFsnEpochBase(lastPublishedFsn)
 * 5. producer state swap: fresh GlobalSymbolDictionary, sentMaxSymbolId=-1,
 *    symbolDictEpoch++, resetArmed=false
 * 6. cursorEngine = engineRebuildFactory.rebuild() -- a brand-new CursorSendEngine
 *    on the now-empty slot (fresh .lock/.ack-watermark/.symbol-dict/segments)
 * 7. reconnect (ensureConnected())
 * 8. (catch) recycleFailure latch on any throw
 * </pre>
 * This suite pins what a restarted sender recovers if the process dies at each
 * of four points around that sequence, per the phase-11 brief:
 * <ul>
 *     <li>(a) {@link #testCrashBeforeRecycleStartsRecoversAckedResidueOnly()} --
 *     before step 2. The pre-recycle epoch's slot holds fully-acked residue.</li>
 *     <li>(b) {@link #testCrashBetweenEngineCloseAndRebuildRecoversAsFreshStart()}
 *     -- between steps 3 and 6. The slot is empty.</li>
 *     <li>(c) {@link #testCrashAfterRebuildBeforeFirstFlushRecoversAsRecoveredButEmpty()}
 *     -- after step 7, before the new epoch's first flush. The slot holds a
 *     freshly-rebuilt engine's own state files, but no data.</li>
 *     <li>(d) {@link #testCrashDuringSteadyStateEpochReplaysOnlyTheUnackedBacklog()}
 *     -- ordinary mid-operation crash recovery, but performed one epoch INTO the
 *     post-recycle steady state, to prove the epoch swap left nothing behind
 *     that could corrupt ordinary backlog replay.</li>
 * </ul>
 *
 * <h2>Why these are simulated, not paused mid-sequence</h2>
 * {@code recycleForDictReset()} runs synchronously inside one {@code table()}
 * call with no external hook between its steps, so a test cannot literally
 * suspend a live sender between step 3 and step 6. And unlike
 * {@code CursorSendEngineCrashConsistencyTest}'s bare {@code CursorSendEngine} +
 * fault-injecting {@code FilesFacade}, a {@code Sender} built through the public
 * API (as production always does) has no seam for a custom {@code FilesFacade}
 * -- {@code LineSenderBuilder.constructEngineOnSlot} always goes through the real
 * filesystem. Each arm below instead constructs the exact on-disk image a crash
 * at that point would leave, using only real production code paths plus
 * filesystem-level fixtures already established elsewhere in this test suite
 * ({@code DeltaDictRecoveryTest}'s {@code writeAckWatermark}, {@code
 * RecoveryReplayTest}'s close-fast-with-a-silent-server idiom):
 * <ul>
 *     <li>(a) closes fast against a server that never acks (so the fully-acked
 *     unlink branch never fires and the residue survives), then stamps the ack
 *     watermark directly to declare it acked retroactively -- simulating a real
 *     ack that landed on the wire a moment before the process died, before step
 *     2 of the recycle ever started.</li>
 *     <li>(b) drives a real recycle through all 7 steps, then closes IMMEDIATELY,
 *     before any flush touches the freshly-rebuilt engine. {@code
 *     close(boolean)} classifies {@code publishedFsn() < 0} as fully drained
 *     exactly like the everything-acked case (that check lives there, not in
 *     {@code finishClose}, which only receives the resulting flag), so this
 *     close unlinks every SF state file step 6 just created -- everything but
 *     the reusable {@code .lock}/{@code .lock.pid} pair, which no close in this
 *     suite ever removes -- leaving the slot in the same empty state that step
 *     3 alone (on the OLD engine) would have left between tearing down and
 *     rebuilding. No rebuild-then-immediately-empty distinction survives on
 *     disk, since an empty directory carries no provenance.</li>
 *     <li>(c) also drives a real recycle to completion, but instead of closing
 *     it, snapshots the freshly-rebuilt slot's bytes to the side FIRST. The
 *     live sender is then closed normally -- for accounting purposes only, so
 *     nothing leaks -- and the snapshot is written back on top of the (now
 *     empty except for the reusable lock pair) directory. This is the only way
 *     to freeze that state: there is no supported way to release just the
 *     slot's OS flock without running {@code finishClose}'s unlink, and
 *     {@code finishClose} is exactly what this arm needs to NOT run. One
 *     immaterial divergence: {@code close()} also reclaims the LOGICAL slot
 *     lock, which lives outside the slot dir in the sibling {@code
 *     .slot-locks/} directory and so is untouched by the snapshot/restore -- a
 *     real post-step-7 crash would leave that file present with its flock
 *     kernel-released, but {@code acquireLogical} recreates a missing one, so
 *     nothing observable changes.</li>
 *     <li>(d) drives a real recycle, appends more rows in the new epoch against
 *     a handler that stops acking after the first connection, then closes fast
 *     -- the established at-least-once backlog idiom, now exercised one epoch
 *     into the recycled sender's life.</li>
 * </ul>
 *
 * <h2>(b) and (c) are NOT the same recoverable state</h2>
 * Both look empty of data and both replay nothing, but they are not
 * byte-identical on disk, and a restarted engine can tell them apart. Arm (b)'s
 * directory holds nothing this engine ever created -- no manifest, no segment.
 * (The crashed sender's own fully-drained close already removed {@code
 * sf-manifest.bin} along with the last segment, so recovery finds NO {@code
 * .sfa} files and NO manifest, and falls straight through to {@code
 * Recovery.empty()}.) Arm (c)'s directory holds the fresh rebuild's own
 * {@code sf-manifest.bin} (boundaries collapsed at 0) and its zero-frame
 * {@code sf-initial.sfa} / {@code sf-...0000.sfa} pair. {@code
 * SegmentRing.recover()}'s manifest branch (the {@code chain.size() == 0}
 * check) accepts a manifest whose {@code headBase == activeBase} alongside a
 * same-based, zero-frame active segment as a RECOVERED (if empty) chain -- a
 * different branch entirely from the one arm (b) falls through to. So {@code
 * wasRecoveredFromDisk()} comes back {@code false} for (b) and {@code true} for
 * (c): the pinned, distinguishing observable between the two, asserted
 * explicitly below instead of writing two assertion-for-assertion duplicate
 * tests.
 *
 * <h2>Oracle</h2>
 * Every arm asserts the same three things about the RECOVERED sender: it keeps
 * ingesting after recovery; the symbols it and its predecessor registered are
 * exactly and correctly reconstructable from the wire (each fresh server
 * handler rebuilds the per-connection delta dictionary via {@link
 * QwpWireTestUtils#accumulateDeltaDictionary}); and no data (table-carrying)
 * frame is delivered more than the at-least-once contract allows (each handler
 * also counts data frames, so a spurious re-send shows up as an unexpected
 * count).
 */
public class SymbolDictRecycleCrashWindowsTest {

    /**
     * The exact file set a freshly-rebuilt (never-flushed) engine's own slot
     * holds -- matches {@code SymbolDictRecycleTest#testPostRecycleSlotContents}'s
     * {@code freshSlotFiles}. Shared by arm (c)'s pre-snapshot wait and its
     * post-restore assertion so the two can never drift apart.
     */
    private static final List<String> FRESH_REBUILD_FILES = Arrays.asList(
            ".ack-watermark", ".lock", ".lock.pid", ".symbol-dict",
            "sf-0000000000000000.sfa", "sf-initial.sfa", "sf-manifest.bin");

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * Arm (a): before step 2. The pre-recycle epoch has flushed a fully-acked
     * batch but the barrier that starts {@code recycleForDictReset()} has not
     * fired yet -- the crash lands with an intact, acked epoch-0 slot on disk.
     * Recovery must find that residue, recognize it as already acked (nothing
     * to replay) and resume the SAME (epoch-0) dictionary rather than starting
     * fresh.
     */
    @Test
    public void testCrashBeforeRecycleStartsRecoversAckedResidueOnly() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("crash-a-pre-swap").toString();
            String slot = Paths.get(sfDir, "default").toString();
            long fsn;
            SilentHandler crashedHandler = new SilentHandler();
            try (TestWebSocketServer crashed = startedServer(crashedHandler)) {
                String cfg = "ws::addr=localhost:" + crashed.getPort() + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;close_flush_timeout_millis=0;";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    fsn = sender.flushAndGetSequence();
                    Assert.assertTrue("threshold=2 crossed by a, b must arm the recycle "
                                    + "immediately (arming does not wait for an ack)",
                            ws.isResetArmed());
                    // close() below hits close_flush_timeout_millis=0 against a server
                    // that never acks: not fully drained, so finishClose does NOT unlink
                    // the segment/manifest/dictionary. It still releases the slot flock
                    // unconditionally (CursorSendEngine.finishClose's retryFlockReleaseIfReady
                    // runs in the outer finally regardless of drain state), so the
                    // successor below can acquire the slot cleanly. No recycle step ever
                    // ran -- the crash is strictly "before step 2".
                }
            }
            // Retroactively declare the flush's only fsn acked: simulates the ack
            // having actually landed moments before the process died, mirroring
            // DeltaDictRecoveryTest#writeAckWatermark.
            writeAckWatermark(slot, fsn);

            AckAllHandler freshHandler = new AckAllHandler();
            try (TestWebSocketServer fresh = startedServer(freshHandler)) {
                String cfg2 = "ws::addr=localhost:" + fresh.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender successor = Sender.fromConfig(cfg2)) {
                    QwpWebSocketSender ws2 = (QwpWebSocketSender) successor;
                    CursorSendEngine recovered = ws2.getCursorEngineForTesting();
                    Assert.assertTrue("acked residue on disk must be recognized as recovered",
                            recovered.wasRecoveredFromDisk());
                    Assert.assertTrue("the watermark stamp must seed ackedFsn at least up to "
                                    + "the only published fsn -- nothing left to replay",
                            recovered.ackedFsn() >= fsn);
                    Assert.assertEquals("the recovered producer must resume epoch 0's a, b "
                                    + "dictionary (ids 0, 1), not restart at -1",
                            1L, recovered.recoveredMaxSymbolId());

                    successor.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long fsn2 = successor.flushAndGetSequence();
                    Assert.assertTrue("recovered sender must keep ingesting",
                            successor.awaitAckedFsn(fsn2, 5_000));
                }
                Assert.assertEquals("no replay of the already-acked a, b frame -- exactly "
                                + "the one new post-recovery frame reaches the server",
                        1, freshHandler.dataFrameCount());
                Assert.assertEquals("the reconstructed dictionary is epoch 0's a, b plus "
                                + "the new c, correct and in order",
                        Arrays.asList("a", "b", "c"), freshHandler.dict());
            }
        });
    }

    /**
     * Arm (b): between steps 3 and 6. See the class javadoc for how this is
     * constructed (drive a real recycle to completion, then close before any
     * flush touches the rebuilt engine -- the "never published" fully-drained
     * branch empties it exactly like step 3 alone would have).
     */
    @Test
    public void testCrashBetweenEngineCloseAndRebuildRecoversAsFreshStart() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("crash-b-mid-swap").toString();
            String slot = Paths.get(sfDir, "default").toString();
            try (TestWebSocketServer crashed = startedServer(new AckAllHandler())) {
                String cfg = "ws::addr=localhost:" + crashed.getPort() + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    // A bare table() call fires the barrier hook before any row is
                    // constructed (mirrors SymbolDictRecycleTest#testFactoryRebuildsOnEmptySlot),
                    // so steps 1-7 run to completion with nothing left pending to flush.
                    sender.table("t");
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    // close() below: the fresh engine has published nothing, so
                    // close(boolean)'s "never published" check (CursorSendEngine's
                    // publishedFsn() < 0 branch) classifies it fully-drained too --
                    // finishClose unlinks every SF state file step 6 just created
                    // (everything but .lock/.lock.pid), leaving the slot as empty
                    // as it was right after step 3 alone emptied the OLD engine.
                }
            }
            Assert.assertEquals("a crash between steps 3 and 6 leaves the slot dir "
                            + "holding only the reusable lock pair -- this is the "
                            + "disk image this arm exists to pin",
                    Arrays.asList(".lock", ".lock.pid"), listDir(slot));

            AckAllHandler freshHandler = new AckAllHandler();
            try (TestWebSocketServer fresh = startedServer(freshHandler)) {
                String cfg2 = "ws::addr=localhost:" + fresh.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender successor = Sender.fromConfig(cfg2)) {
                    QwpWebSocketSender ws2 = (QwpWebSocketSender) successor;
                    CursorSendEngine recovered = ws2.getCursorEngineForTesting();
                    Assert.assertFalse("an empty slot has nothing to recover -- see the "
                                    + "class javadoc for why this differs from arm (c)",
                            recovered.wasRecoveredFromDisk());
                    Assert.assertEquals(-1L, recovered.recoveredMaxSymbolId());
                    Assert.assertEquals(-1L, recovered.publishedFsn());

                    successor.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn = successor.flushAndGetSequence();
                    Assert.assertTrue(successor.awaitAckedFsn(fsn, 5_000));
                }
                Assert.assertEquals("exactly the one post-crash frame reaches the server",
                        1, freshHandler.dataFrameCount());
                Assert.assertEquals("the new epoch's dictionary tiles from id 0 -- none of "
                                + "the pre-crash a, b survive",
                        Arrays.asList("d"), freshHandler.dict());
            }
            // The successor's own row is fully acked by now, so its own close is
            // fully drained too and the slot settles back to the same lock-only
            // image -- confirms the cycle is stable, not a one-shot coincidence.
            Assert.assertEquals("the successor's fully-drained close leaves the slot "
                            + "dir back down to just the reusable lock pair",
                    Arrays.asList(".lock", ".lock.pid"), listDir(slot));
        });
    }

    /**
     * Arm (c): after step 7, pre-first-flush. See the class javadoc for how
     * this is constructed (snapshot the freshly-rebuilt slot before closing,
     * close for real so nothing leaks, then restore the snapshot on top of the
     * vacated directory) and for why this recovers differently from arm (b)
     * despite carrying no data either.
     */
    @Test
    public void testCrashAfterRebuildBeforeFirstFlushRecoversAsRecoveredButEmpty() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("crash-c-post-swap").toString();
            String slot = Paths.get(sfDir, "default").toString();
            Map<String, byte[]> snapshot;
            try (TestWebSocketServer crashed = startedServer(new AckAllHandler())) {
                String cfg = "ws::addr=localhost:" + crashed.getPort() + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    sender.table("t"); // bare call: drives steps 1-7, nothing left pending
                    Assert.assertFalse(ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());

                    // The manager worker provisions the fresh engine's hot-spare
                    // segment asynchronously (its own service pass, off the
                    // producer thread), so the slot is not guaranteed to have
                    // settled to its steady rebuilt-engine file set the instant
                    // table() returns. Wait for it before snapshotting -- a
                    // mid-provision snapshot could capture a zero-magic spare
                    // that recovery would then hard-fail on.
                    awaitExactFileSet(slot, FRESH_REBUILD_FILES);

                    // The true pre-first-flush crash image, frozen before the
                    // upcoming close() would otherwise unlink it (arm (b)).
                    snapshot = snapshotDir(slot);
                }
            }
            restoreDir(slot, snapshot);

            Assert.assertEquals("the restored image is exactly a freshly rebuilt (never "
                            + "flushed) engine's own state files",
                    FRESH_REBUILD_FILES, listDir(slot));

            AckAllHandler freshHandler = new AckAllHandler();
            try (TestWebSocketServer fresh = startedServer(freshHandler)) {
                String cfg2 = "ws::addr=localhost:" + fresh.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender successor = Sender.fromConfig(cfg2)) {
                    QwpWebSocketSender ws2 = (QwpWebSocketSender) successor;
                    CursorSendEngine recovered = ws2.getCursorEngineForTesting();
                    // The pinned discriminator vs arm (b): a manifest with collapsed
                    // (headBase == activeBase) boundaries alongside a same-based,
                    // zero-frame active segment recovers as RECOVERED, not EMPTY --
                    // see SegmentRing.recover()'s chain.size()==0 branch and the class
                    // javadoc.
                    Assert.assertTrue("a manifest + zero-frame active segment recovers as "
                                    + "RECOVERED even though it carries no data, unlike arm "
                                    + "(b)'s genuinely empty directory",
                            recovered.wasRecoveredFromDisk());
                    Assert.assertEquals(-1L, recovered.recoveredMaxSymbolId());
                    Assert.assertEquals(-1L, recovered.publishedFsn());

                    successor.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                    long fsn = successor.flushAndGetSequence();
                    Assert.assertTrue(successor.awaitAckedFsn(fsn, 5_000));
                }
                Assert.assertEquals("exactly the one post-crash frame reaches the server -- "
                                + "there was never anything else to replay",
                        1, freshHandler.dataFrameCount());
                Assert.assertEquals("the new epoch's dictionary tiles from id 0 -- none of "
                                + "the pre-crash a, b survive",
                        Arrays.asList("e"), freshHandler.dict());
            }
        });
    }

    /**
     * Arm (d): an ordinary mid-operation crash, but one epoch into the
     * post-recycle steady state (epoch g+1), to prove the recycle's epoch
     * bookkeeping does not corrupt normal backlog recovery. Uses the same
     * close-fast-with-an-unacking-server idiom as {@code RecoveryReplayTest},
     * except the handler only stops acking AFTER the recycle's fresh
     * connection is established, so epoch 0's setup batch is genuinely acked
     * (arming the recycle) and only epoch 1's backlog survives unacked.
     */
    @Test
    public void testCrashDuringSteadyStateEpochReplaysOnlyTheUnackedBacklog() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("crash-d-steady-state").toString();
            AckFirstConnectionSilentAfterHandler crashedHandler =
                    new AckFirstConnectionSilentAfterHandler();
            try (TestWebSocketServer crashed = startedServer(crashedHandler)) {
                String cfg = "ws::addr=localhost:" + crashed.getPort() + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;close_flush_timeout_millis=0;";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup batch on connection 1 must be genuinely acked",
                            sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    // Triggers the recycle (opens connection 2, which the handler never
                    // acks) and immediately queues c, d into the fresh epoch.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    Assert.assertFalse(ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    sender.flushAndGetSequence();
                    // Not drained (connection 2 never acks): close_flush_timeout_millis=0
                    // returns immediately without unlinking, preserving c, d's segment
                    // and dictionary on disk while still releasing the slot flock.
                }
            }

            AckAllHandler freshHandler = new AckAllHandler();
            try (TestWebSocketServer fresh = startedServer(freshHandler)) {
                String cfg2 = "ws::addr=localhost:" + fresh.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender successor = Sender.fromConfig(cfg2)) {
                    QwpWebSocketSender ws2 = (QwpWebSocketSender) successor;
                    CursorSendEngine recovered = ws2.getCursorEngineForTesting();
                    Assert.assertTrue("epoch 1's unacked c, d segment must be recovered",
                            recovered.wasRecoveredFromDisk());
                    Assert.assertEquals("epoch 1's own dictionary (c, d only) must be recovered, "
                                    + "never epoch 0's a, b -- the recycle's slot wipe erased them",
                            1L, recovered.recoveredMaxSymbolId());

                    Assert.assertTrue("the recovered sender must replay the backlog and "
                                    + "get it acked",
                            successor.awaitAckedFsn(recovered.publishedFsn(), 5_000));

                    successor.table("t").symbol("s", "e").longColumn("v", 4L).atNow();
                    long fsn = successor.flushAndGetSequence();
                    Assert.assertTrue(successor.awaitAckedFsn(fsn, 5_000));
                }
                Assert.assertEquals("the unacked c, d backlog frame replays exactly once, "
                                + "plus exactly one new frame for e -- no extra replay "
                                + "attempts beyond the at-least-once contract",
                        2, freshHandler.dataFrameCount());
                Assert.assertEquals("the reconstructed dictionary is epoch 1's own c, d plus "
                                + "the new e -- none of epoch 0's a, b ever reappear",
                        Arrays.asList("c", "d", "e"), freshHandler.dict());
            }
        });
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
     * Polls {@code listDir(dir)} until it equals {@code expected} or a 5s
     * deadline elapses, then asserts the final state -- the manager worker
     * provisions a fresh engine's hot-spare segment asynchronously (its own
     * service pass, off the producer thread), so the slot dir is not
     * guaranteed to hold its steady-state file set the instant a producer-side
     * call returns. Same shape as the deadline loops in
     * {@code DeltaDictRecoveryTest}.
     */
    private static void awaitExactFileSet(String dir, List<String> expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && !expected.equals(listDir(dir))) {
            Thread.sleep(20);
        }
        Assert.assertEquals("slot dir must settle to its steady-state file set "
                        + "before it can be snapshotted",
                expected, listDir(dir));
    }

    /** Copies every file directly inside {@code dir} (by name -> bytes) for later {@link #restoreDir}. */
    private static Map<String, byte[]> snapshotDir(String dir) throws IOException {
        Map<String, byte[]> snapshot = new LinkedHashMap<>();
        for (String name : listDir(dir)) {
            snapshot.put(name, java.nio.file.Files.readAllBytes(Paths.get(dir, name)));
        }
        return snapshot;
    }

    /** Writes back a {@link #snapshotDir} capture, creating or overwriting each file by name. */
    private static void restoreDir(String dir, Map<String, byte[]> snapshot) throws IOException {
        for (Map.Entry<String, byte[]> entry : snapshot.entrySet()) {
            java.nio.file.Files.write(Paths.get(dir, entry.getKey()), entry.getValue());
        }
    }

    private static TestWebSocketServer startedServer(TestWebSocketServer.WebSocketServerHandler handler)
            throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    /** Directly stamps {@code <slotDir>/.ack-watermark}, mirroring DeltaDictRecoveryTest#writeAckWatermark. */
    private static void writeAckWatermark(String slotDir, long fsn) {
        AckWatermark watermark = AckWatermark.open(slotDir);
        Assert.assertNotNull("ack watermark must open for the test fixture", watermark);
        try {
            watermark.write(fsn);
            watermark.sync();
        } finally {
            watermark.close();
        }
    }

    /**
     * Acks connection 1 in full; every later connection (2, 3, ...) is silently
     * dropped, as if the process died the instant that connection opened.
     */
    private static class AckFirstConnectionSilentAfterHandler
            implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicInteger connectionsAccepted = new AtomicInteger();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private boolean ackCurrentConnection;
        private TestWebSocketServer.ClientHandler currentClient;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                ackCurrentConnection = connectionsAccepted.incrementAndGet() == 1;
                nextSeq.set(0);
            }
            if (ackCurrentConnection) {
                try {
                    client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            // else: intentionally dropped, simulating a crash on this connection.
        }
    }

    /**
     * Acks every frame; reconstructs the connection's delta dictionary and
     * counts data (table-carrying) frames so a test can catch an unwanted
     * duplicate replay.
     */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final List<String> dict = new ArrayList<>();
        private int dataFrameCount;
        private final AtomicLong nextSeq = new AtomicLong(0);

        synchronized int dataFrameCount() {
            return dataFrameCount;
        }

        synchronized List<String> dict() {
            return new ArrayList<>(dict);
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
            if (QwpWireTestUtils.tableCount(data) > 0) {
                dataFrameCount++;
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Never acks -- the crashed-process side of every close-fast fixture in this suite. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally empty
        }
    }
}
