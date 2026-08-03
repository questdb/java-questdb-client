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
import io.questdb.client.std.ObjList;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Pins down how the symbol dictionary is framed on the wire.
 * <p>
 * Both engine modes ship <b>monotonic</b> deltas -- each symbol id travels once,
 * not the whole dictionary per message -- which is the bandwidth win this feature
 * adds. The I/O thread re-registers the dictionary with a catch-up frame whenever
 * it (re)connects, so a fresh server can resolve the non-self-sufficient delta
 * frames that follow.
 * <p>
 * The modes differ only in where the catch-up's dictionary comes from: memory
 * mode keeps it in an in-process mirror; file-backed store-and-forward keeps it in
 * a per-slot {@code .symbol-dict} file so a recovered or orphan-drained slot (a
 * fresh process with no in-memory mirror) can rebuild it. This test asserts the
 * monotonic wire framing in both modes and the presence of that dictionary file.
 */
public class SelfSufficientFramesTest {

    /** First byte of the symbol-dict delta payload after the 12-byte QWP header. */
    private static final int DELTA_START_OFFSET = 12;

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testFileModeShipsMonotonicDeltaAndPersistsDict() throws Exception {
        // File-backed SF also ships monotonic deltas now: batch 2 carries only
        // "beta" (deltaStart=1). The dictionary is durably kept in .symbol-dict
        // so a recovered/orphan-drained slot can rebuild it.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-selfsufficient").toPath();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                // The engine places slot files under sf_dir/<sender_id> (default "default").
                Path dictFile = sfDir.resolve("default").resolve(".symbol-dict");
                try (Sender sender = Sender.fromConfig(config)) {
                    sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 5_000);

                    sender.table("foo").symbol("s", "beta").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);

                    // Check the persisted dictionary while the sender is live: a
                    // fully-drained close intentionally unlinks it (slot cleanup).
                    Assert.assertTrue("persisted dictionary file exists", Files.exists(dictFile));
                    byte[] dict = Files.readAllBytes(dictFile);
                    Assert.assertTrue("dictionary retains alpha", containsUtf8(dict, "alpha"));
                    Assert.assertTrue("dictionary retains beta", containsUtf8(dict, "beta"));
                }

                Assert.assertEquals("expected 2 captured batches", 2, handler.batches.size());
                byte[] b1 = handler.batches.get(0);
                byte[] b2 = handler.batches.get(1);

                Assert.assertEquals("batch 1 deltaStart must be 0",
                        0, readVarint(b1, DELTA_START_OFFSET));
                Assert.assertEquals("batch 1 deltaCount must be 1", 1, readVarint(b1, DELTA_START_OFFSET + 1));
                // batch 2 ships ONLY beta as a delta from id 1.
                Assert.assertEquals("batch 2 deltaStart must be 1 (monotonic)",
                        1, readVarint(b2, DELTA_START_OFFSET));
                Assert.assertEquals("batch 2 deltaCount must be 1 (only the new symbol)",
                        1, readVarint(b2, DELTA_START_OFFSET + 1));
            }
        });
    }

    @Test
    public void testDiskModeFallsBackToFullDictWhenPersistedDictUnopenable() throws Exception {
        // When the per-slot .symbol-dict cannot be created in disk mode,
        // isDeltaDictEnabled() is false and the sender must fall back to
        // self-sufficient frames: every batch re-ships the WHOLE dictionary from
        // id 0. A recovered / orphan-drained slot then has no dictionary to
        // rebuild deltas from, so a monotonic delta would dangle ids on the fresh
        // server -- the full-dict frame is the safe degradation.
        //
        // Force the open failure through an injected FilesFacade whose
        // openCleanRW always fails, rather than planting a directory where the
        // dictionary file belongs: openClean() now REFUSES a fresh slot whose
        // existing dictionary cannot be truncated (see PersistedSymbolDictTest's
        // truncate-refusal coverage), so a planted blocker would throw instead of
        // degrading. The facade fault instead reproduces the case openClean()
        // still tolerates -- a transient failure (EIO, fd exhaustion) with
        // nothing at the path to lose. Sender.fromConfig has no seam for a custom
        // FilesFacade, so this bypasses it and builds the engine directly, the
        // same entry point DeltaDictRecoveryTest's dictionary-fault-injection
        // tests use.
        // newFolder already creates sfDir itself, so -- unlike
        // DeltaDictRecoveryTest's bare sfDir path -- no extra mkdir is needed
        // before constructing the engine directly on the slot beneath it.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-fallback").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 0, 0, 0L, null, false, engine)) {
                    sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 5_000);

                    sender.table("foo").symbol("s", "beta").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);
                }

                // No dictionary was ever created -- openCleanRW failed against
                // nothing on disk, so delta encoding stayed disabled with
                // nothing left behind.
                Assert.assertFalse("dictionary must not exist: creation failed cleanly",
                        Files.exists(sfDir.resolve("default").resolve(".symbol-dict")));

                Assert.assertEquals("expected 2 captured batches", 2, handler.batches.size());
                byte[] b1 = handler.batches.get(0);
                byte[] b2 = handler.batches.get(1);

                // Full-dict fallback: BOTH batches start at id 0, and batch 2
                // re-ships the WHOLE dictionary (alpha + beta), NOT a monotonic
                // delta (which would be deltaStart=1, deltaCount=1 as above).
                Assert.assertEquals("batch 1 deltaStart must be 0",
                        0, readVarint(b1, DELTA_START_OFFSET));
                Assert.assertEquals("batch 1 deltaCount must be 1",
                        1, readVarint(b1, DELTA_START_OFFSET + 1));
                Assert.assertEquals("batch 2 deltaStart must be 0 (full-dict fallback, not monotonic)",
                        0, readVarint(b2, DELTA_START_OFFSET));
                Assert.assertEquals("batch 2 deltaCount must be 2 (whole dictionary re-shipped)",
                        2, readVarint(b2, DELTA_START_OFFSET + 1));
            }
        });
    }

    private static boolean containsUtf8(byte[] haystack, String needle) {
        byte[] n = needle.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        outer:
        for (int i = 0; i + n.length <= haystack.length; i++) {
            for (int j = 0; j < n.length; j++) {
                if (haystack[i + j] != n[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    @Test
    public void testMemoryModeShipsMonotonicDelta() throws Exception {
        // Memory-mode (no sf_dir): each symbol id ships once. Batch 2 carries
        // only "beta" as a delta starting at id 1, not the whole dictionary.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    sender.table("foo").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 5_000);

                    sender.table("foo").symbol("s", "beta").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);
                }

                Assert.assertEquals("expected 2 captured batches", 2, handler.batches.size());
                byte[] b1 = handler.batches.get(0);
                byte[] b2 = handler.batches.get(1);

                // Batch 1 introduces alpha at id 0.
                Assert.assertEquals("batch 1 deltaStart must be 0",
                        0, readVarint(b1, DELTA_START_OFFSET));
                Assert.assertEquals("batch 1 deltaCount must be 1",
                        1, readVarint(b1, DELTA_START_OFFSET + 1));

                // Batch 2 ships ONLY beta as a delta from id 1.
                Assert.assertEquals("batch 2 deltaStart must be 1 (monotonic)",
                        1, readVarint(b2, DELTA_START_OFFSET));
                Assert.assertEquals("batch 2 deltaCount must be 1 (only the new symbol)",
                        1, readVarint(b2, DELTA_START_OFFSET + 1));
            }
        });
    }

    @Test
    public void testSplitBatchShipsDeltaOnFirstFrameOnly() throws Exception {
        // A single flush whose encoded size exceeds the server's batch cap is
        // split into one frame per table (flushPendingRowsSplit). The FIRST split
        // frame carries the whole batch's symbol-dict delta and advances the
        // baseline; the remaining frames carry an EMPTY delta and only reference
        // ids the first frame already registered. A regression that shipped each
        // table's own symbols (wrong deltaStart) would dangle ids on the server.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(150); // forces the two-table batch to split
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Padding inflates each table past half the cap, so the combined
                // two-table message exceeds it while each single-table frame fits.
                String pad = new String(new char[60]).replace('\0', 'x');
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    // Buffer TWO tables (symbols "a" id 0, "b" id 1), then ONE flush.
                    sender.table("t1").symbol("s", "a").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "b").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);
                }

                Assert.assertEquals("the oversized two-table batch must split into 2 frames",
                        2, handler.batches.size());
                byte[] f1 = handler.batches.get(0);
                byte[] f2 = handler.batches.get(1);

                // First split frame ships the whole batch's dictionary (a + b).
                Assert.assertEquals("first split frame deltaStart must be 0",
                        0, readVarint(f1, DELTA_START_OFFSET));
                Assert.assertEquals("first split frame ships both new symbols",
                        2, readVarint(f1, DELTA_START_OFFSET + 1));
                // Second split frame carries an empty delta above the advanced baseline.
                Assert.assertEquals("second split frame deltaStart must be 2 (baseline advanced)",
                        2, readVarint(f2, DELTA_START_OFFSET));
                Assert.assertEquals("second split frame carries no new symbols",
                        0, readVarint(f2, DELTA_START_OFFSET + 1));
            }
        });
    }

    /**
     * A split that fails at frame k&gt;1 must leave the symbol dictionary consistent for
     * the retry.
     * <p>
     * flushPendingRowsSplit publishes frames one at a time and documents that a failure
     * partway through leaves frames 1..k-1 on the ring as deferred, with the next flush
     * re-emitting the whole batch -- at-least-once, absorbed by DEDUP or a durable-ack
     * await. Nothing exercised the k&gt;1 shape at all: every other failed-publish test
     * fails at frame 1.
     * <p>
     * This is a CHARACTERISATION test, not a mutation guard, and the reason is worth
     * recording. It confirms the javadoc's claim that "the re-sent frames carry empty
     * deltas and the write-ahead persist is a pd.size() no-op" -- but it does not
     * discriminate against reverting that pd.size() key, because frame 1 published
     * SUCCESSFULLY and advanceSentMaxSymbolId therefore already moved the baseline past
     * the whole batch. The retry is an early return under either key. That is precisely
     * why the k&gt;1 path is safe, and it is the fact the test pins.
     * <p>
     * The failure is injected at the RING, not the cap: both frames pass the pre-flight
     * (each is under the advertised cap) but "big"'s frame does not fit a fresh
     * sf_max_segment_bytes segment, so it fails in sealAndSwapBuffer AFTER "a" was published.
     */
    @Test(timeout = 60_000L)
    public void testMidSplitPublishFailureLeavesTheDictionaryIdempotent() throws Exception {
        Path sfDir = temporaryFolder.newFolder("qwp-sf-midsplit").toPath();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(3_400);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String pad = new String(new char[100]).replace('\0', 'x');
                String cfg = "ws::addr=localhost:" + port
                        + ";sf_dir=" + sfDir
                        + ";sf_max_segment_bytes=1024"
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000"
                        + ";close_flush_timeout_millis=0;";
                Sender sender = Sender.fromConfig(cfg);
                try {
                    // "a" first (small: fits a segment), "big" second (does not).
                    sender.table("a").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    for (int i = 0; i < 28; i++) {
                        sender.table("big").symbol("s", "bravo")
                                .stringColumn("p", pad).longColumn("v", (long) i).atNow();
                    }

                    ObjList<String> afterFirst = null;
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try {
                            sender.flush();
                            Assert.fail("the second split frame cannot fit a fresh segment");
                        } catch (LineSenderException expected) {
                            // Ring-level failure, not the cap pre-flight.
                            Assert.assertFalse("must fail at the ring, not the cap pre-flight: "
                                            + expected.getMessage(),
                                    expected.getMessage().contains("too large for server batch cap"));
                        }
                        ObjList<String> persisted =
                                ((QwpWebSocketSender) sender).getPersistedSymbolsForTest();
                        Assert.assertNotNull(persisted);
                        if (attempt == 0) {
                            afterFirst = persisted;
                        } else {
                            Assert.assertEquals("a mid-split retry must not re-append the "
                                            + "symbols the failed attempt already persisted",
                                    afterFirst.size(), persisted.size());
                        }
                    }
                    Assert.assertEquals("both symbols are persisted exactly once",
                            2, afterFirst.size());
                } finally {
                    try {
                        // close() re-flushes the retained batch, which is permanently
                        // unflushable at this sf_max_segment_bytes -- not what we assert.
                        sender.close();
                    } catch (LineSenderException ignored) {
                    }
                }
            }
        });
    }

    @Test
    public void testOversizedTableSplitStrandsNothing() throws Exception {
        // Regression: flushPendingRowsSplit publishes each table's frame one at a
        // time (all but the last deferred, i.e. appended but uncommitted). If a LATER
        // table's frame exceeds the cap, the split must not have already published an
        // earlier table's frame -- otherwise that prefix strands on the ring, a later
        // commit delivers it as a partial batch, and resetTableBuffersAfterFlush
        // discards every source row, all while flush() reported failure to the
        // caller. The pre-flight size pass makes the split all-or-nothing: an
        // oversized table throws BEFORE any frame is published. Pre-fix, the "small"
        // table's frame was published and committed on close, so the server saw it.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(200);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // auto_flush_bytes=off lets "big" accumulate PAST the cap (byte-based
                // auto-flush is otherwise clamped to 90% of the cap and would flush
                // first); the row/interval limits are set high so nothing auto-flushes
                // during the test. Each row stays under the per-row guard (< cap), but
                // 12 rows make "big"'s single frame exceed the cap, which no split can
                // shrink. "small" (added first) fits; "big" (added second) does not, so
                // the split hits it AFTER publishing "small" pre-fix.
                String pad = new String(new char[40]).replace('\0', 'x');
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    sender.table("small").symbol("s", "a").longColumn("v", 1L).atNow();
                    for (int i = 0; i < 12; i++) {
                        sender.table("big").stringColumn("p", pad).longColumn("v", (long) i).atNow();
                    }
                    try {
                        sender.flush();
                        Assert.fail("an oversized single-table split frame must throw");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(),
                                e.getMessage().contains("too large for server batch cap"));
                    }
                    // Sender.flush() is retryable: a rejected flush must leave the
                    // source rows intact. Repeating it under the same cap therefore
                    // has to reject the same batch again. Before the fix, the first
                    // failure reset every table buffer and this second flush was a
                    // silent no-op -- the caller's rows had been lost.
                    try {
                        sender.flush();
                        Assert.fail("retrying the retained oversized batch must throw again");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(),
                                e.getMessage().contains("too large for server batch cap"));
                    }
                    // Explicit reset is the caller-controlled discard boundary and
                    // prevents close() from making a third retry in this test.
                    sender.reset();
                    // close() drains the ring: pre-fix, the stranded "small" frame
                    // would be sent (and committed) here.
                }

                // No DATA frame (tableCount > 0) may have reached the server: the
                // oversized-table split published nothing. Pre-fix, "small" arrived.
                long dataFrames = 0;
                for (byte[] frame : handler.batches) {
                    if (frame.length >= 8 && (((frame[6] & 0xFF) | ((frame[7] & 0xFF) << 8)) > 0)) {
                        dataFrames++;
                    }
                }
                Assert.assertEquals("an oversized-table split must publish NO data frame -- an "
                                + "earlier table's frame must not strand on the ring", 0, dataFrames);
            }
        });
    }

    @Test
    public void testFileModeSplitPersistsDictBeforeAFAILEDPublish() throws Exception {
        // The SPLIT path's write-ahead ordering: persistNewSymbolsBeforePublish must run
        // BEFORE sealAndSwapBuffer publishes the frame to the ring, exactly as it does on
        // the non-split path.
        //
        // Its sibling below (testFileModeSplitPersistsDictBeforePublish) checks the dict
        // after a SUCCESSFUL flush, which proves nothing about ORDER -- the symbols land
        // in the file either way. Only a publish that FAILS while the symbols are new can
        // tell the two apart: with the write-ahead intact the symbols are already durable
        // when the publish throws; with the persist moved after the publish, the throw
        // beats it and the dictionary is still empty. (The non-split path is pinned this
        // way by DeltaDictRecoveryTest.testFailedPublishDoesNotDuplicatePersistedSymbols;
        // the split path had no equivalent.)
        //
        // Why it matters: store-and-forward is process-crash durable. If a split frame
        // reaches the ring before its symbols reach .symbol-dict, a JVM crash in that
        // window leaves a recorded frame whose deltaStart exceeds the recovered
        // dictionary -- so recovery declares the slot unreplayable and quarantines a slot
        // that should have drained cleanly.
        //
        // Setup: the server cap (150) splits the two-table batch, and each split frame
        // still FITS that cap, so the split pre-flight passes and the publish is actually
        // attempted. sf_max_segment_bytes=64 then makes every frame larger than the segment, so
        // appendBlocking fails with PAYLOAD_TOO_LARGE -- deterministically, no
        // backpressure timing needed.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-split-persist-fail").toPath();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(150); // forces the two-table batch to split
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";sf_max_segment_bytes=64;";
                String pad = TestUtils.repeat("x", 60);
                Sender sender = Sender.fromConfig(config);
                try {
                    sender.table("t1").symbol("s", "alpha").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "bravo").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("a split frame larger than the segment must fail to publish");
                    } catch (LineSenderException expected) {
                        // PAYLOAD_TOO_LARGE -- the publish, not the pre-flight.
                    }
                    Assert.assertEquals("the publish must fail, so no frame reaches the server",
                            0, handler.batches.size());

                    // The write-ahead already ran: both of the batch's new symbols are
                    // durable even though the frame that references them never
                    // published. Move the persist after sealAndSwapBuffer and this is 0.
                    ObjList<String> persisted =
                            ((QwpWebSocketSender) sender).getPersistedSymbolsForTest();
                    Assert.assertNotNull(persisted);
                    Assert.assertEquals("the split path must persist its new symbols BEFORE "
                            + "publishing the frame that references them", 2, persisted.size());
                    Assert.assertEquals("alpha", persisted.getQuick(0));
                    Assert.assertEquals("bravo", persisted.getQuick(1));
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() re-flushes the still-buffered oversized rows and fails
                        // again; expected, and not what this test asserts. close() still
                        // runs its resource cleanup, so no native memory leaks.
                    }
                }
            }
        });
    }

    @Test
    public void testFileModeSplitPersistsDictBeforePublish() throws Exception {
        // File-mode store-and-forward + a SPLIT flush: a two-table batch whose combined
        // size exceeds the server cap splits into one frame per table
        // (flushPendingRowsSplit). The first split frame's write-ahead persist
        // (persistNewSymbolsBeforePublish, the appendRawEntries fast path) must record
        // the batch's new symbols in .symbol-dict BEFORE the frames publish, so a
        // recovered / orphan-drained slot can rebuild what the delta frames reference.
        // The other split tests run in memory mode, so this is the only coverage of the
        // split x persist path with a live PersistedSymbolDict.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-split-persist").toPath();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(150); // forces the two-table batch to split
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                Path dictFile = sfDir.resolve("default").resolve(".symbol-dict");
                String pad = new String(new char[60]).replace('\0', 'x');
                try (Sender sender = Sender.fromConfig(config)) {
                    // Two tables, two new symbols, ONE flush -> the combined message
                    // exceeds cap 150 and splits into two frames.
                    sender.table("t1").symbol("s", "alpha").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "bravo").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);

                    // Check .symbol-dict while the sender is live: a fully-drained
                    // close would unlink it. The split's first-frame write-ahead
                    // persist must have recorded BOTH new symbols.
                    Assert.assertTrue("persisted dictionary file exists", Files.exists(dictFile));
                    byte[] dict = Files.readAllBytes(dictFile);
                    Assert.assertTrue("split-flush persist must record alpha", containsUtf8(dict, "alpha"));
                    Assert.assertTrue("split-flush persist must record bravo", containsUtf8(dict, "bravo"));
                }

                Assert.assertEquals("the oversized two-table batch must split into 2 frames",
                        2, handler.batches.size());
            }
        });
    }

    @Test
    public void testSplitPreflightAdvancesBaselineSoLaterFramesArentSizedWithTheDelta() throws Exception {
        // Regression for the split pre-flight baseline advance in flushPendingRowsSplit
        // (the "Mirror advanceSentMaxSymbolId" step). Only the FIRST split frame ships
        // the batch's symbol-dict delta; the rest ship an EMPTY delta and reference ids
        // the first frame already registered. The pre-flight size pass must advance
        // simBaseline after the first table so it STOPS adding combinedDeltaEntriesLen
        // to the later frames' estimated sizes. Without that advance, a later table
        // whose real (empty-delta) frame fits the cap is mis-estimated as still carrying
        // the whole delta and wrongly rejected with "single table batch too large" --
        // discarding a legitimately shippable batch (fail-closed data loss).
        //
        // Shape (memory mode, delta enabled): a LARGE combined delta (two ~64-char
        // symbols) rides only the first split frame. The first table (added first, so
        // the first split frame) has a tiny body, so delta + body1 fits the cap. The
        // second table has a big body: body2 alone fits the cap, but delta + body2 does
        // NOT. The real code splits and ships both frames; the un-advanced pre-flight
        // would size the second frame WITH the delta and throw. Table order is
        // insertion order (CharSequenceObjHashMap.keys()), so t1 is the delta frame.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(200);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Two long symbols => a large combined delta section (~130 bytes) that
                // rides ONLY the first split frame. The symbol STRINGS live in the delta,
                // not in either table body (the body carries only the varint global id).
                String longSymA = new String(new char[64]).replace('\0', 'a');
                String longSymB = new String(new char[64]).replace('\0', 'b');
                String bigPad = new String(new char[100]).replace('\0', 'x');
                // auto_flush off so both rows batch into one flush (byte-based auto-flush
                // is otherwise clamped under the cap and would flush before the split).
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    // t1 (added first -> first split frame): carries the whole delta but a
                    // tiny body, so delta + body1 fits the 200-byte cap.
                    sender.table("t1").symbol("s", longSymA).longColumn("v", 1L).atNow();
                    // t2 (second split frame): empty delta but a big body. body2 alone
                    // fits the cap; delta + body2 does NOT -- the mis-size the advance
                    // prevents.
                    sender.table("t2").symbol("s", longSymB).stringColumn("p", bigPad).longColumn("v", 2L).atNow();
                    // Must NOT throw: with the baseline advanced, t2's frame is sized
                    // WITHOUT the delta and fits. A broken advance throws "too large" here.
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);
                }

                Assert.assertEquals("the batch must split into 2 frames, neither spuriously rejected",
                        2, handler.batches.size());
                byte[] f1 = handler.batches.get(0);
                byte[] f2 = handler.batches.get(1);
                // First split frame ships the whole delta (both new symbols, ids 0 and 1).
                Assert.assertEquals("first split frame deltaStart must be 0",
                        0, readVarint(f1, DELTA_START_OFFSET));
                Assert.assertEquals("first split frame ships both new symbols",
                        2, readVarint(f1, DELTA_START_OFFSET + 1));
                // Second split frame carries an EMPTY delta above the advanced baseline --
                // the whole point: it is not re-sized (or re-sent) with the delta.
                Assert.assertEquals("second split frame deltaStart must be 2 (baseline advanced)",
                        2, readVarint(f2, DELTA_START_OFFSET));
                Assert.assertEquals("second split frame carries no new symbols",
                        0, readVarint(f2, DELTA_START_OFFSET + 1));
            }
        });
    }

    @Test
    public void testDictionaryLargerThanTheCapShipsAsChunkedDictionaryFrames() throws Exception {
        // A full-dictionary sender carries the whole dictionary in EVERY frame, so once
        // that section alone fills the cap no frame of any size fits: the split
        // pre-flight rejected the batch, reset() could not clear the dictionary, and the
        // sender was dead for new data forever. The dictionary is now registered up
        // front as deferred dictionary-only frames, each chunked under the cap, and the
        // data frames carry an empty delta.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-dict-chunked").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            final int cap = 512;
            final int symbols = 40;
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(cap);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // openCleanRW fails against nothing on disk, so the slot runs full-dict.
                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                List<String> expected = new ArrayList<>();
                // 40 x ~60-byte entries is ~2.4 KB of dictionary against a 512-byte cap,
                // so no single frame can carry it and at least five chunks are required.
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine)) {
                    String pad = TestUtils.repeat("x", 55);
                    for (int i = 0; i < symbols; i++) {
                        String sym = String.format("%04d", i) + pad;
                        expected.add(sym);
                        sender.table("t").symbol("s", sym).longColumn("v", (long) i).atNow();
                    }
                    // Pre-fix this threw BatchTooLargeForCapException and every retry
                    // threw again; reset() could not clear the dictionary.
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 10_000);
                }

                // Every frame must respect the cap the server advertised.
                for (byte[] frame : handler.batches) {
                    Assert.assertTrue("frame of " + frame.length + " bytes exceeds cap " + cap,
                            frame.length <= cap);
                }

                // The dictionary chunks must tile [0, symbols) exactly: reassembling
                // through the same decoder the delta suites use raises
                // DictionaryGapException on a gap, and a shift or overlap shows up as a
                // per-id mismatch below.
                List<String> rebuilt = new ArrayList<>();
                long dictOnlyFrames = 0;
                long dataFrames = 0;
                for (byte[] frame : handler.batches) {
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, rebuilt);
                    if (QwpWireTestUtils.tableCount(frame) == 0) {
                        dictOnlyFrames++;
                    } else {
                        dataFrames++;
                    }
                }
                Assert.assertTrue("the dictionary must span several chunks, saw " + dictOnlyFrames,
                        dictOnlyFrames >= 5);
                Assert.assertTrue("at least one data frame must be sent", dataFrames >= 1);
                Assert.assertEquals("every symbol must be registered exactly once",
                        expected.size(), rebuilt.size());
                for (int i = 0; i < expected.size(); i++) {
                    Assert.assertEquals("symbol id " + i, expected.get(i), rebuilt.get(i));
                }
            }
        });
    }

    @Test
    public void testSingleSymbolLargerThanTheCapThrowsWithNothingPublished() throws Exception {
        // A symbol value cannot be split across frames, so one larger than the cap is a
        // genuine terminal for this batch. It must be detected BEFORE any dictionary
        // chunk is published, or the ring strands deferred chunks with no data.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-dict-huge-symbol").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(256);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine)) {
                    // Small enough to pass sendRow's per-row guard, too large for a
                    // dictionary frame of its own once the header and varints are added.
                    sender.table("t").symbol("s", TestUtils.repeat("y", 250))
                            .longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("a symbol larger than the cap must throw");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(),
                                e.getMessage().contains("a single symbol value is too large for the server batch cap"));
                    }
                    sender.reset();
                }

                // Nothing may have been published -- not even a dictionary chunk.
                Assert.assertEquals("no frame may reach the server", 0, handler.batches.size());
            }
        });
    }

    @Test
    public void testCloseStillDrainsWhenTheRetainedBatchIsOverCap() throws Exception {
        // close() step 1 flushes; an over-cap batch throws there. Letting that throw
        // escape skips sendCommitMessage, sealAndSwapBuffer AND drainOnClose --
        // abandoning every row an earlier successful flush already published. close()
        // therefore discards the retained batch, remembers the error, and runs the rest.
        //
        // The observable is drainOnClose: with a server that never acks and a short
        // close budget, reaching step 3 produces a drain timeout. Remove the
        // catch(BatchTooLargeForCapException) and no drain timeout appears anywhere,
        // because close() never gets there. Every other close() assertion in the suite
        // survives that mutation -- the sites all catch the parent LineSenderException,
        // so caught-inside and escaping are indistinguishable to them.
        assertMemoryLeak(() -> {
            NeverAckingHandler handler = new NeverAckingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(200);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String pad = new String(new char[40]).replace('\0', 'x');
                boolean threw = false;
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000"
                        + ";close_flush_timeout_millis=250;")) {
                    // A batch that fits: published and sent, but never acked.
                    sender.table("small").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 5_000);

                    // A batch no split can shrink: retained, and close() must discard it.
                    for (int i = 0; i < 12; i++) {
                        sender.table("big").stringColumn("p", pad).longColumn("v", (long) i).atNow();
                    }
                } catch (LineSenderException e) {
                    threw = true;
                    String all = collectMessages(e);
                    Assert.assertTrue("close() must reach drainOnClose after discarding the "
                            + "over-cap batch, but saw: " + all, all.contains("drain timed out"));
                }
                Assert.assertTrue("close() must surface the retained over-cap batch", threw);
            }
        });
    }

    // Flattens a throwable's message with its cause and suppressed chain, so an
    // assertion does not depend on which of close()'s several errors ends up outermost.
    private static String collectMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur.getMessage()).append(' ');
            for (Throwable sup : cur.getSuppressed()) {
                sb.append(collectMessages(sup)).append(' ');
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return sb.toString();
    }

    // Captures frames but never acks, so close()'s bounded drain always times out.
    private static class NeverAckingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final java.util.List<byte[]> batches =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            batches.add(data.clone());
        }
    }

    private static int readVarint(byte[] buf, int offset) {
        return QwpWireTestUtils.readVarint(buf, new int[]{offset});
    }

    /**
     * Refuses every attempt to (re)create the symbol dictionary, WITHOUT ever
     * leaving a file behind at its path -- the fault shape a transient failure
     * (EIO, fd exhaustion) has on a slot with nothing there to lose. Used instead
     * of a planted directory blocker: a blocker left on disk is exactly what
     * openClean() now REFUSES to start on top of (see PersistedSymbolDictTest's
     * truncate-refusal coverage), so it can no longer simulate the "nothing to
     * lose" case this facade represents.
     */
    private static final class UnopenableDictFacade extends DelegatingFilesFacade {
        @Override
        public int openCleanRW(String path) {
            return -1;
        }
    }

    private static void waitFor(BoolCondition cond, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted");
            }
        }
        Assert.fail("waitFor timed out");
    }

    @FunctionalInterface
    private interface BoolCondition {
        boolean test();
    }

    /** Captures every binary frame for later inspection AND ACKs it. */
    private static class CapturingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final java.util.List<byte[]> batches =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            batches.add(data.clone());
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
