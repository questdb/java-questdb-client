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
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.ObjList;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import io.questdb.client.test.tools.UnopenableDictFacade;
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
import java.util.Arrays;
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
    public void testFullRecordSealsAndEachFrameShipsOnlyItsOwnNewSymbols() throws Exception {
        // Delta mode bounds a frame by the open record it materialises, which the
        // sender sizes to the server's batch cap: a row the record cannot take seals
        // it and goes into the next one, so a batch over the cap never needs a split.
        // Each frame carries only the symbols its own rows introduced, continuing the
        // delta exactly where the previous frame stopped.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(4096);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // two ~2.5 KB rows: the second does not fit the 4 KiB record the first is in
                String pad = TestUtils.repeat("x", 2500);
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    sender.table("t1").symbol("s", "a").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "b").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    Assert.assertEquals("the second row sealed the first row's record", 1, sender.pendingRowCount());
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);
                }

                Assert.assertEquals("one frame per record", 2, handler.batches.size());
                byte[] f1 = handler.batches.get(0);
                byte[] f2 = handler.batches.get(1);
                Assert.assertTrue("a record's frame fits the cap", f1.length <= 4096 && f2.length <= 4096);
                Assert.assertEquals("first frame deltaStart must be 0", 0, readVarint(f1, DELTA_START_OFFSET));
                Assert.assertEquals("first frame ships only its own symbol", 1, readVarint(f1, DELTA_START_OFFSET + 1));
                Assert.assertEquals("second frame continues the delta", 1, readVarint(f2, DELTA_START_OFFSET));
                Assert.assertEquals("second frame ships only its own symbol", 1, readVarint(f2, DELTA_START_OFFSET + 1));
                List<String> dictionary = new ArrayList<>();
                for (byte[] frame : handler.batches) {
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                }
                Assert.assertEquals(Arrays.asList("a", "b"), dictionary);
            }
        });
    }

    /**
     * A split that fails at frame k&gt;1 leaves the symbol dictionary consistent for
     * the retry. Full-dictionary mode keeps the frame-per-flush path, and with it the
     * split of a batch over the cap: flushPendingRowsSplit publishes the frames one at
     * a time, so a failure partway through leaves frames 1..k-1 on the ring as
     * deferred and the source rows buffered, and the next flush re-emits the whole
     * batch. Every full-dictionary frame carries the whole dictionary from id 0, so a
     * re-sent frame defines exactly the ids its first copy defined.
     * <p>
     * The failure is injected at the ring, not the cap: both frames pass the
     * pre-flight (each is under the advertised cap), but "big"'s frame does not fit a
     * fresh 1 KiB segment, so it fails in sealAndSwapBuffer after "a" was published.
     */
    @Test(timeout = 60_000L)
    public void testMidSplitPublishFailureLeavesTheDictionaryIdempotent() throws Exception {
        Path sfDir = temporaryFolder.newFolder("qwp-sf-midsplit").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(3_400);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                CursorSendEngine engine = new CursorSendEngine(
                        slot, 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, new UnopenableDictFacade());
                QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 0, 0, 0L, null, false, engine);
                try {
                    Assert.assertFalse("the engine runs in full-dictionary mode", sender.isOpenRecordMode());
                    String pad = TestUtils.repeat("x", 100);
                    // "a" first (fits a segment), "big" second (does not); together they
                    // exceed the cap, so the flush splits them into one frame each.
                    sender.table("a").symbol("s", "alpha")
                            .stringColumn("p", TestUtils.repeat("y", 400)).longColumn("v", 1L).atNow();
                    for (int i = 0; i < 28; i++) {
                        sender.table("big").symbol("s", "bravo")
                                .stringColumn("p", pad).longColumn("v", (long) i).atNow();
                    }
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try {
                            sender.flush();
                            Assert.fail("the second split frame cannot fit a fresh segment");
                        } catch (LineSenderException expected) {
                            // Ring-level failure, not the cap pre-flight.
                            Assert.assertFalse("must fail at the ring, not the cap pre-flight: "
                                            + expected.getMessage(),
                                    expected.getMessage().contains("too large for server batch cap"));
                            TestUtils.assertContains(expected.getMessage(), "cursor SF append failed");
                        }
                        Assert.assertEquals("each attempt publishes frame 1 before frame 2 fails",
                                attempt, engine.publishedFsn());
                        Assert.assertEquals("the batch stays buffered for the retry", 29, sender.pendingRowCount());
                    }
                } finally {
                    try {
                        // close() re-flushes the retained batch, which is permanently
                        // unflushable at this segment size -- not what we assert.
                        sender.close();
                    } catch (LineSenderException ignored) {
                    }
                }

                waitFor(() -> handler.batches.size() >= 2, 5_000);
                byte[] first = handler.batches.get(0);
                for (int i = 0, n = handler.batches.size(); i < n; i++) {
                    byte[] frame = handler.batches.get(i);
                    Assert.assertEquals("frame " + i + " starts its dictionary at id 0",
                            0, readVarint(frame, DELTA_START_OFFSET));
                    List<String> dictionary = new ArrayList<>();
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                    Assert.assertEquals("frame " + i + " defines the whole dictionary on its own",
                            Arrays.asList("alpha", "bravo"), dictionary);
                    Assert.assertArrayEquals("a retry re-sends the same frame", first, frame);
                }
            }
        });
    }

    @Test
    public void testRowOverTheFrameBoundFailsAloneAndStrandsNothing() throws Exception {
        // Delta mode enforces the frame bound on the row, at at(): a row no record of
        // the cap's size can hold fails there, and nothing of it reaches the ring --
        // not its values, not its symbols. The rows before it are unaffected: the
        // record holding them is sealed while the sender tries a fresh record for the
        // oversized row, so they ship exactly once. A retry of the same row fails the
        // same way.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(4096);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String pad = TestUtils.repeat("x", 5000);
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    sender.table("small").symbol("s", "a").longColumn("v", 1L).atNow();
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try {
                            sender.table("big").symbol("s", "never-shipped").stringColumn("p", pad).longColumn("v", 2L).atNow();
                            Assert.fail("a row over the frame bound must fail on its at()");
                        } catch (LineSenderException e) {
                            TestUtils.assertContains(e.getMessage(), "row too large for the frame bound");
                            TestUtils.assertContains(e.getMessage(), "http.recv.buffer.size");
                        }
                        Assert.assertFalse(sender.hasRowInProgress());
                        Assert.assertEquals("the failed row stays out of the record", 0, sender.pendingRowCount());
                    }
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 5_000);
                }

                Assert.assertEquals("only the small row's frame ships", 1, handler.batches.size());
                byte[] frame = handler.batches.get(0);
                Assert.assertEquals(1, QwpWireTestUtils.tableCount(frame));
                Assert.assertEquals("the failed row's symbol is not in the dictionary", 1, readVarint(frame, DELTA_START_OFFSET + 1));
                Assert.assertFalse(containsUtf8(frame, "never-shipped"));
                Assert.assertFalse(containsUtf8(frame, TestUtils.repeat("x", 100)));
            }
        });
    }

    @Test
    public void testFileModePersistsTheRecordsSymbolsBeforeAFailedSeal() throws Exception {
        // The write-ahead ordering of a record: its seal persists the record's new
        // symbols to .symbol-dict BEFORE the engine seals it into a frame the server
        // can acknowledge and the ring can trim. A seal that fails in the engine
        // tells the order apart: the symbols are already durable when it throws;
        // with the persist moved after the engine's seal, the throw beats it and the
        // dictionary is still empty.
        //
        // Why it matters: once trimmed, a frame's own copy of its delta is gone, and a
        // later recovery rebuilds the ids its successors reference from .symbol-dict
        // alone; a missing entry makes the slot unreplayable.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-record-persist-fail").toPath();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000"
                        + ";close_flush_timeout_millis=0;";
                QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(config);
                try {
                    sender.table("t1").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "bravo").longColumn("v", 2L).atNow();
                    sender.setRecordSealFaultForTesting(() -> {
                        throw new IllegalStateException("injected seal fault");
                    });
                    try {
                        sender.flush();
                        Assert.fail("the engine's seal must fail");
                    } catch (LineSenderException expected) {
                        TestUtils.assertContains(expected.getMessage(), "seal");
                    }
                    Assert.assertEquals("the seal failed, so no frame reaches the server",
                            0, handler.batches.size());

                    // The write-ahead already ran: both of the record's new symbols are
                    // durable although the record never became a frame. Move the persist
                    // after the engine's seal and this is 0.
                    ObjList<String> persisted = sender.getPersistedSymbolsForTest();
                    Assert.assertNotNull(persisted);
                    Assert.assertEquals("the seal must persist the record's new symbols BEFORE "
                            + "the engine seals it", 2, persisted.size());
                    Assert.assertEquals("alpha", persisted.getQuick(0));
                    Assert.assertEquals("bravo", persisted.getQuick(1));
                } finally {
                    sender.close();
                }
            }
        });
    }

    @Test
    public void testFileModePersistsTheSymbolsOfEveryRecord() throws Exception {
        // File-mode store-and-forward with a cap that holds one row per record: the
        // second row does not fit the record holding the first, so at() seals that
        // record and the row goes into the next. Each seal persists its record's new
        // symbol, so .symbol-dict holds both while their frames are unacked, which is
        // what a recovered or orphan-drained slot rebuilds the ids from.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-record-persist").toPath();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(1024);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String config = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;";
                Path dictFile = sfDir.resolve("default").resolve(".symbol-dict");
                String pad = TestUtils.repeat("x", 500);
                try (Sender sender = Sender.fromConfig(config)) {
                    sender.table("t1").symbol("s", "alpha").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "bravo").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);

                    // Check .symbol-dict while the sender is live: a fully-drained
                    // close would unlink it.
                    Assert.assertTrue("persisted dictionary file exists", Files.exists(dictFile));
                    byte[] dict = Files.readAllBytes(dictFile);
                    Assert.assertTrue("the first record's seal must persist alpha", containsUtf8(dict, "alpha"));
                    Assert.assertTrue("the second record's seal must persist bravo", containsUtf8(dict, "bravo"));
                }

                Assert.assertEquals("one frame per record", 2, handler.batches.size());
            }
        });
    }

    @Test
    public void testRowMovedToTheNextRecordShipsAnEmptyDeltaForSymbolsAlreadySealed() throws Exception {
        // A row the full record cannot take goes into the next record, whose
        // dictionary starts after every id the sealed record registered. A row that
        // reuses one of those symbols therefore adds no entry: its frame carries an
        // empty delta above the advanced baseline and is sized without it.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(4096);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String longSymA = TestUtils.repeat("a", 64);
                String pad = TestUtils.repeat("x", 3000);
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    sender.table("t1").symbol("s", longSymA).stringColumn("p", pad).longColumn("v", 1L).atNow();
                    // the same symbol, and a body the first record has no room left for
                    sender.table("t2").symbol("s", longSymA).stringColumn("p", pad).longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 5_000);
                }

                Assert.assertEquals("one frame per record", 2, handler.batches.size());
                byte[] f1 = handler.batches.get(0);
                byte[] f2 = handler.batches.get(1);
                Assert.assertEquals("first frame deltaStart must be 0", 0, readVarint(f1, DELTA_START_OFFSET));
                Assert.assertEquals("first frame registers the symbol", 1, readVarint(f1, DELTA_START_OFFSET + 1));
                Assert.assertEquals("second frame deltaStart must be 1 (baseline advanced)", 1, readVarint(f2, DELTA_START_OFFSET));
                Assert.assertEquals("second frame carries no new symbols", 0, readVarint(f2, DELTA_START_OFFSET + 1));
            }
        });
    }

    @Test
    public void testFullDictionaryRegionSealsTheRecordAndEveryFrameIsADataFrame() throws Exception {
        // Every new entry lives in the dictionary region of the record whose rows use
        // it: a region too small for the next row's symbols seals the record and the
        // next record reserves a larger one. Delta mode therefore never ships a
        // dictionary-only frame (the frame path's full-dictionary fallback, which
        // delta mode never reaches): every frame is a data frame within the cap, and
        // the dictionary arrives gap-free, each id once.
        assertMemoryLeak(() -> {
            final int cap = 4096;
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(cap);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                List<String> expected = new ArrayList<>();
                String symPad = TestUtils.repeat("s", 46);
                String rowPad = TestUtils.repeat("x", 2000);
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    // 8 new 48-char symbols: more entry bytes than a 4 KiB record's
                    // default dictionary region holds
                    for (int i = 0; i < 8; i++) {
                        String sym = String.format("%02d", i) + symPad;
                        expected.add(sym);
                        sender.table("t1").symbol("s", sym).longColumn("v", (long) i).atNow();
                    }
                    sender.table("t2").symbol("s", expected.get(0))
                            .stringColumn("p", rowPad).longColumn("v", 99L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 10_000);
                }

                Assert.assertTrue("the dictionary region filled and sealed at least once", handler.batches.size() >= 2);
                List<String> dictionary = new ArrayList<>();
                for (byte[] frame : handler.batches) {
                    Assert.assertTrue("every frame must be a DATA frame (tableCount > 0); a "
                                    + "tableCount == 0 frame means the dictionary was chunked "
                                    + "in delta mode",
                            QwpWireTestUtils.tableCount(frame) > 0);
                    Assert.assertTrue("a record's frame fits the cap", frame.length <= cap);
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                }
                Assert.assertEquals(expected, dictionary);
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
    public void testFullDictNearCapFallsBackToChunkedDictionary() throws Exception {
        // M1 regression (review round 5). preRegisterDictionaryChunks declines to chunk
        // whenever the dict-only frame fits the cap -- but that proves the dictionary
        // fits with a ZERO-byte body. With 10 x 48-char symbols the dict-only frame is
        // exactly 504 bytes against a 512 cap: under the cap alone, over it with any
        // table body. Pre-fix, the split pre-flight rejected the batch as "single table
        // batch too large" although it IS shippable (chunk the dictionary, ship the body
        // with an empty delta), reset() could not help (the next batch re-references the
        // same symbols), and the producer was wedged. Post-fix flushPendingRows falls
        // back: one deferred dictionary chunk, then the data frame with an empty delta.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-dict-nearcap").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            final int cap = 512;
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
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine)) {
                    // 10 x 48-char symbols: dict section 10 x 49 = 490 bytes, dict-only
                    // frame 504 -- one row under the 512 cap.
                    String pad = TestUtils.repeat("s", 46);
                    for (int i = 0; i < 10; i++) {
                        String sym = String.format("%02d", i) + pad;
                        expected.add(sym);
                        sender.table("t").symbol("s", sym).longColumn("v", (long) i).atNow();
                    }
                    // Pre-fix this threw BatchTooLargeForCapException ("single table
                    // batch too large") on a shippable batch.
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 2, 10_000);
                }

                Assert.assertEquals("expected exactly one dictionary chunk + one data frame",
                        2, handler.batches.size());
                for (byte[] frame : handler.batches) {
                    Assert.assertTrue("frame of " + frame.length + " bytes exceeds cap " + cap,
                            frame.length <= cap);
                }
                byte[] chunk = handler.batches.get(0);
                byte[] data = handler.batches.get(1);
                Assert.assertEquals("first frame must be dictionary-only",
                        0, QwpWireTestUtils.tableCount(chunk));
                Assert.assertEquals("chunk deltaStart", 0, readVarint(chunk, DELTA_START_OFFSET));
                Assert.assertEquals("chunk carries the whole dictionary",
                        10, readVarint(chunk, DELTA_START_OFFSET + 1));
                Assert.assertEquals("second frame must carry the table",
                        1, QwpWireTestUtils.tableCount(data));
                Assert.assertEquals("data frame deltaStart must sit above the chunked ids",
                        10, readVarint(data, DELTA_START_OFFSET));
                Assert.assertEquals("data frame must carry an empty delta",
                        0, readVarint(data, DELTA_START_OFFSET + 1));

                // The chunk must register every symbol exactly once, in id order.
                List<String> rebuilt = new ArrayList<>();
                for (byte[] frame : handler.batches) {
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, rebuilt);
                }
                Assert.assertEquals(expected.size(), rebuilt.size());
                for (int i = 0; i < expected.size(); i++) {
                    Assert.assertEquals("symbol id " + i, expected.get(i), rebuilt.get(i));
                }
            }
        });
    }

    @Test
    public void testFullDictNearCapFallbackSplitsMultiTableBatch() throws Exception {
        // The fallback's second exit: after the dictionary is chunked, the re-encoded
        // combined frame can still exceed the cap and must go through the ordinary
        // split -- one empty-delta frame per table, each under the cap. Three tables
        // with ~350-byte bodies guarantee the re-encoded combined frame (~1 KB) still
        // splits, while every per-table empty-delta frame fits 512 comfortably.
        // Pre-fix this threw exactly like the single-table shape: every split frame
        // was sized WITH the 490-byte dictionary section and rejected.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-dict-nearcap-split").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            final int cap = 512;
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(cap);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                List<String> expected = new ArrayList<>();
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine)) {
                    String symPad = TestUtils.repeat("s", 46);
                    // t1 registers the 10 symbols (ids 0..9): small body, whole dict.
                    for (int i = 0; i < 10; i++) {
                        String sym = String.format("%02d", i) + symPad;
                        expected.add(sym);
                        sender.table("t1").symbol("s", sym).longColumn("v", (long) i).atNow();
                    }
                    // t2 and t3 re-reference existing symbols and add ~350-byte bodies.
                    String rowPad = TestUtils.repeat("x", 50);
                    for (int i = 0; i < 5; i++) {
                        sender.table("t2").symbol("s", expected.get(i))
                                .stringColumn("p", rowPad).longColumn("v", (long) i).atNow();
                        sender.table("t3").symbol("s", expected.get(i))
                                .stringColumn("p", rowPad).longColumn("v", (long) i).atNow();
                    }
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 4, 10_000);
                }

                long dictOnlyFrames = 0;
                long dataFrames = 0;
                List<String> rebuilt = new ArrayList<>();
                for (byte[] frame : handler.batches) {
                    Assert.assertTrue("frame of " + frame.length + " bytes exceeds cap " + cap,
                            frame.length <= cap);
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, rebuilt);
                    if (QwpWireTestUtils.tableCount(frame) == 0) {
                        dictOnlyFrames++;
                    } else {
                        dataFrames++;
                        // Every data frame rides the chunked registration: empty delta.
                        Assert.assertEquals("data frame deltaStart",
                                10, readVarint(frame, DELTA_START_OFFSET));
                        Assert.assertEquals("data frame must carry an empty delta",
                                0, readVarint(frame, DELTA_START_OFFSET + 1));
                    }
                }
                Assert.assertEquals("the 504-byte dictionary fits one chunk", 1, dictOnlyFrames);
                Assert.assertEquals("the split emits one frame per table", 3, dataFrames);
                Assert.assertEquals("every symbol registered exactly once",
                        expected.size(), rebuilt.size());
                for (int i = 0; i < expected.size(); i++) {
                    Assert.assertEquals("symbol id " + i, expected.get(i), rebuilt.get(i));
                }
            }
        });
    }

    @Test
    public void testFullDictNearCapOversizedBodyStrandsNoChunks() throws Exception {
        // All-or-nothing guard on the fallback itself. Same near-cap dictionary, but the
        // single table's body (~1 KB) exceeds the cap even with an EMPTY delta -- the
        // batch is genuinely unshippable, so the fallback must decline BEFORE publishing
        // any dictionary chunk. A fallback that chunked first and discovered the
        // oversized body second would strand deferred dict-only frames on the ring; this
        // test fails (batches > 0) under that mutation. Behaviour here is identical
        // pre-fix and post-fix: throw, retain the batch, publish nothing.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-dict-nearcap-bigbody").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(512);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine)) {
                    String symPad = TestUtils.repeat("s", 46);
                    String rowPad = TestUtils.repeat("x", 40);
                    // 20 rows cycling through 10 symbols: the same 504-byte dictionary
                    // section, plus a body no split can shrink under the cap. Each row
                    // stays well under the per-row guard.
                    for (int i = 0; i < 20; i++) {
                        String sym = String.format("%02d", i % 10) + symPad;
                        sender.table("big").symbol("s", sym)
                                .stringColumn("p", rowPad).longColumn("v", (long) i).atNow();
                    }
                    try {
                        sender.flush();
                        Assert.fail("a body over the cap must throw even in the fallback window");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(),
                                e.getMessage().contains("single table batch too large for server batch cap"));
                    }
                    // Retryable: the batch is retained and rejected identically again.
                    try {
                        sender.flush();
                        Assert.fail("retrying the retained oversized batch must throw again");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage(),
                                e.getMessage().contains("single table batch too large for server batch cap"));
                    }
                    sender.reset();
                }

                Assert.assertEquals("nothing may reach the server -- neither a data frame "
                        + "nor a stranded dictionary chunk", 0, handler.batches.size());
            }
        });
    }

    @Test
    public void testHeldCrossingRowFrameCarriesTheDictionaryInFullDictMode() throws Exception {
        // A row that pushes an auto-flushed batch over the cap stays buffered while
        // the rows before it ship. The sender registered the row's symbol before it
        // encoded that frame, so the frame defines the id. In full-dict mode every
        // frame must define every id its rows reference, so the frame that later
        // ships the held row must carry the dictionary again: the sender keeps the
        // batch's symbol watermark when it retains the row.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-held-row").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(4 * 1024);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // openCleanRW fails against nothing on disk, so the slot runs full-dict.
                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                // the default auto_flush_bytes binds the byte trigger to 90% of the 4 KiB cap
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                        0L, null, false, engine)) {
                    Assert.assertFalse("the slot runs full-dict",
                            ((QwpWebSocketSender) sender).isDeltaDictEnabledForTest());
                    String wide = TestUtils.repeat("x", 1000);
                    for (int i = 0; i < 3; i++) {
                        sender.table("t").symbol("y", "a").stringColumn("s", wide).atNow();
                    }
                    // the auto-flushed batch would be 4.5 KB: the three rows before this
                    // one ship, and this row, which registered "b", stays buffered
                    sender.table("t").symbol("y", "b").stringColumn("s", TestUtils.repeat("x", 1500)).atNow();
                    Assert.assertEquals(1, sender.pendingRowCount());
                    sender.flush();
                    sender.table("t").symbol("y", "b").atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 3, 10_000);
                }

                Assert.assertEquals(3, handler.batches.size());
                for (int i = 0; i < 3; i++) {
                    byte[] frame = handler.batches.get(i);
                    Assert.assertEquals("frame " + i + " carries table t", 1, QwpWireTestUtils.tableCount(frame));
                    List<String> dictionary = new ArrayList<>();
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                    Assert.assertEquals("frame " + i + " defines every id its rows reference",
                            Arrays.asList("a", "b"), dictionary);
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
                    try {
                        sender.checkError();
                        Assert.fail("the probe must report the retained batch");
                    } catch (BatchTooLargeForCapException expected) {
                        // latched by the dictionary pre-flight
                    }
                    sender.reset();
                    sender.checkError();
                }

                // Nothing may have been published -- not even a dictionary chunk.
                Assert.assertEquals("no frame may reach the server", 0, handler.batches.size());
            }
        });
    }

    @Test
    public void testResetClearsTheBatchSymbolWatermarkSoDeltaModeCanFlushAgain() throws Exception {
        // reset() must drop the batch symbol watermark with the rows that raised it:
        // the next record's dictionary spans [sentMaxSymbolId+1 .. watermark], so a
        // watermark left at the discarded rows' tip would make even a single-row
        // record carry the whole abandoned range. reset() also reclaims the never-
        // shipped ids, so the surviving row re-registers at id 0. (It would pass
        // without the reclaim too, by reusing the already-registered id 0 -- see the
        // sibling test for the case that would not.)
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String symPad = TestUtils.repeat("s", 46);
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    // 40 x 48-byte symbols, all in one open record that nothing published
                    for (int i = 0; i < 40; i++) {
                        sender.table("t").symbol("s", String.format("%02d", i) + symPad)
                                .longColumn("v", (long) i).atNow();
                    }
                    Assert.assertEquals(40, sender.pendingRowCount());
                    sender.reset();
                    Assert.assertEquals("reset() drops every unpublished row", 0, sender.pendingRowCount());
                    Assert.assertEquals("nothing shipped before the reset", 0, handler.batches.size());

                    sender.table("t").symbol("s", "00" + symPad).longColumn("v", 0L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 10_000);
                }

                Assert.assertEquals("exactly one frame ships after reset()", 1, handler.batches.size());
                byte[] frame = handler.batches.get(0);
                Assert.assertEquals("the post-reset frame must re-register from id 0",
                        0, readVarint(frame, DELTA_START_OFFSET));
                Assert.assertEquals("it carries only the one symbol the surviving row references, "
                                + "not the discarded rows' 40",
                        1, readVarint(frame, DELTA_START_OFFSET + 1));
            }
        });
    }

    @Test
    public void testResetReclaimsUnsentSymbolIdsSoANewSymbolCanFlush() throws Exception {
        // The sibling above recovers because its post-reset row happens to reuse a
        // symbol the discarded rows already registered. That is luck, not a contract:
        // a record's dictionary starts at sentMaxSymbolId+1, which only advances at a
        // seal, so ids discarded rows registered would sit between the watermark and
        // the dictionary tip and a post-reset row carrying a NEW symbol would land
        // above them all, dragging the whole abandoned range into its record. reset()
        // reclaims those never-shipped ids, so the new symbol starts at 0.
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String symPad = TestUtils.repeat("s", 46);
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertTrue("memory mode must select delta mode",
                            ws.isDeltaDictEnabledForTest());

                    for (int i = 0; i < 40; i++) {
                        sender.table("t").symbol("s", String.format("%02d", i) + symPad)
                                .longColumn("v", (long) i).atNow();
                    }
                    Assert.assertEquals("nothing shipped, so no id is on the wire yet",
                            0, handler.batches.size());

                    sender.reset();
                    Assert.assertEquals("reset() must return every never-shipped id: none of the "
                                    + "40 reached the wire or the .symbol-dict",
                            0, ws.getGlobalSymbolDictionaryForTest().size());

                    // A symbol none of the discarded rows used. Without the reclaim it
                    // takes id 40 and drags [0..40] back into the record's dictionary.
                    sender.table("t").symbol("s", "brand-new" + symPad).longColumn("v", 99L).atNow();
                    sender.flush();
                    waitFor(() -> handler.batches.size() >= 1, 10_000);
                }

                Assert.assertEquals("exactly one frame ships after reset()", 1, handler.batches.size());
                byte[] frame = handler.batches.get(0);
                Assert.assertEquals("the post-reset frame re-registers from id 0",
                        0, readVarint(frame, DELTA_START_OFFSET));
                Assert.assertEquals("it carries only the one new symbol, not the abandoned 40",
                        1, readVarint(frame, DELTA_START_OFFSET + 1));
            }
        });
    }

    @Test
    public void testSectionOverCapWithAnOversizedBodyPublishesNothingOnEveryRetry() throws Exception {
        // The section-alone-over-cap shape, which used to be pre-registered eagerly:
        // 20 x 48-byte symbols make a ~1 KB dictionary section against a 512-byte cap,
        // and the body is over the cap too, so the batch is genuinely unshippable.
        //
        // The chunker used to publish before the bodies were sized, so this shape left
        // deferred, table-less chunks on the ring with no data frame behind them. They
        // carry FLAG_DEFER_COMMIT and the server withholds the ack for every deferred
        // frame until its group commits, so ackedFsn froze and trim stopped for the
        // whole connection -- and because the throw RETAINS the batch by design and its
        // message tells the caller to retry, EVERY retry appended another full copy of
        // the dictionary to a ring that could no longer be trimmed. Publishing now
        // happens only after the bodies are proven to fit, so this batch reaches the
        // wire not at all, however many times it is retried.
        //
        // The retry loop is the regression guard: pre-fix batches grew by one chunk
        // group per flush(); post-fix it stays at zero.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-dict-section-over-cap").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(512);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine);
                try {
                    String symPad = TestUtils.repeat("s", 46);
                    String rowPad = TestUtils.repeat("x", 60);
                    for (int i = 0; i < 20; i++) {
                        sender.table("big").symbol("s", String.format("%02d", i) + symPad)
                                .stringColumn("p", rowPad).longColumn("v", (long) i).atNow();
                    }
                    for (int attempt = 0; attempt < 3; attempt++) {
                        try {
                            sender.flush();
                            Assert.fail("a body over the cap must throw");
                        } catch (LineSenderException e) {
                            Assert.assertTrue(e.getMessage(),
                                    e.getMessage().contains("single table batch too large for server batch cap"));
                        }
                        // Measure the RING, not the wire: publishedFsn is advanced
                        // synchronously by the publishing thread, so this cannot race
                        // the I/O thread the way a frame count would.
                        Assert.assertEquals("retry " + attempt + " must publish nothing: a retained "
                                        + "batch that cannot ship must not append a dictionary copy "
                                        + "to a ring whose ack watermark is frozen",
                                -1L, engine.publishedFsn());
                    }
                    // reset() discards the batch, and the sender is still usable.
                    sender.reset();
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException retainedBatch) {
                        // Only reachable when an assertion above failed before
                        // reset(): the batch is then still retained and close()
                        // rethrows it. Swallow so the real assertion failure is
                        // what surfaces, instead of being masked by the finally.
                    }
                }

                Assert.assertEquals("nothing may reach the server at all",
                        0, handler.batches.size());
            }
        });
    }

    @Test
    public void testDictionaryChunksAreCommittedWhenTheDataFrameFailsToPublish() throws Exception {
        // The uncovered twin of testSectionOverCapWithAnOversizedBodyPublishesNothing-
        // OnEveryRetry. That one pins the route the pre-flight closes: a batch that can
        // never ship publishes no chunks at all. This one pins the route it does NOT
        // close -- splitFramesFit proves the batch is SHIPPABLE once chunked, so the
        // chunks are published, and then the data frame fails to publish anyway.
        //
        // sealAndSwapBuffer throws on a buffer-recycle timeout and on appendBlocking's
        // backpressure/PAYLOAD_TOO_LARGE paths, none of which a size check can see. The
        // chunks are already on the ring carrying FLAG_DEFER_COMMIT with no rollback, so
        // without the commit below the server withholds their ack and clamps the
        // connection's cumulative watermark for its whole life: trim stops for every
        // frame, the ring fills, and each retry appends ANOTHER full copy of the
        // dictionary (full-dict mode re-derives deltaBaseline == -1, so chunking
        // restarts from id 0). Ingestion in the process is dead until restart.
        //
        // Sizing: sf_max_segment_bytes sits between the chunk frame and the data frame,
        // so the chunks publish and the data frame's append fails with PAYLOAD_TOO_LARGE
        // -- deterministic, no backpressure timing.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-chunk-orphan-commit").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            CapturingHandler handler = new CapturingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(150);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                UnopenableDictFacade dictFf = new UnopenableDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 100L, 4L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, dictFf);
                Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine);
                try {
                    // One 40-char symbol makes the dictionary section big enough that
                    // section+body busts the 150-byte cap, while the body alone fits it
                    // -- exactly the shape that fires the chunker. The chunk frame
                    // carries only the section, so it is SMALLER than the data frame.
                    String sym = TestUtils.repeat("s", 40);
                    String rowPad = TestUtils.repeat("x", 96);
                    sender.table("t").symbol("s", sym).stringColumn("p", rowPad).atNow();
                    try {
                        sender.flush();
                        Assert.fail("the data frame must fail to publish");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage(),
                                expected.getMessage().contains("cursor SF append failed"));
                    }

                    Assert.assertTrue("the chunker must have published at least one chunk, "
                                    + "or this test is not on the route it claims",
                            engine.publishedFsn() >= 0L);

                    // Let the I/O thread ship what is on the ring.
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline && handler.batches.size() < 2) {
                        Thread.sleep(20);
                    }
                    Assert.assertTrue("expected the chunk(s) plus a commit frame, saw "
                            + handler.batches.size(), handler.batches.size() >= 2);

                    byte[] last = handler.batches.get(handler.batches.size() - 1);
                    Assert.assertEquals("the orphaned dictionary group must be CLOSED: the last "
                                    + "frame has to clear FLAG_DEFER_COMMIT, or the server withholds "
                                    + "every ack and the ring can never be trimmed again",
                            0, last[QwpConstants.HEADER_OFFSET_FLAGS] & QwpConstants.FLAG_DEFER_COMMIT);
                    for (int i = 0; i < handler.batches.size() - 1; i++) {
                        Assert.assertEquals("frame " + i + " must be a deferred dictionary chunk",
                                QwpConstants.FLAG_DEFER_COMMIT,
                                handler.batches.get(i)[QwpConstants.HEADER_OFFSET_FLAGS]
                                        & QwpConstants.FLAG_DEFER_COMMIT);
                    }
                    sender.reset();
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // The retained batch can rethrow here; the assertions above are
                        // what this test is about.
                    }
                }
            }
        });
    }

    @Test
    public void testFullDictCommitFrameCarriesNoDictionaryAfterACancelledRow() throws Exception {
        // sendCommitMessage bounded its delta with currentBatchMaxSymbolId in full-dict
        // mode, on the premise -- stated in its own comment -- that "the prior flush
        // reset it to -1". flushPendingRows returns early WITHOUT resetting it when
        // pendingRowCount is 0, and cancelRow leaves behind the id of a symbol it
        // registered. A commit reached through that window therefore re-shipped the
        // ENTIRE dictionary from id 0, in the one frame no cap check and no chunker
        // covers -- the same oversized-frame wall the chunking exists to remove.
        // The commit carries no rows, so it must register nothing in either mode.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-commit-empty-delta").toPath();
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
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    Assert.assertFalse("an unopenable .symbol-dict must select full-dict mode",
                            ws.isDeltaDictEnabledForTest());

                    // A deferred data frame leaves the group open (hasDeferredMessages).
                    ws.setDeferCommit(true);
                    sender.table("t").symbol("s", "AAPL").longColumn("v", 1L).atNow();
                    sender.flush();
                    ws.setDeferCommit(false);

                    // Register a symbol, then throw the row away: cancelRow rolls back the
                    // row but not currentBatchMaxSymbolId, and pendingRowCount stays 0 so
                    // the next flush's flushPendingRows returns before resetting it.
                    sender.table("t").symbol("s", "GOOG").longColumn("v", 2L);
                    sender.cancelRow();

                    // The I/O thread ships frames asynchronously, so settle on the data
                    // frame before counting the commit that follows it.
                    waitFor(() -> handler.batches.size() >= 1, 10_000);
                    int framesBefore = handler.batches.size();
                    sender.flush(); // no rows to flush -> straight to sendCommitMessage
                    waitFor(() -> handler.batches.size() > framesBefore, 10_000);
                    Assert.assertEquals("the flush must emit exactly the commit frame",
                            framesBefore + 1, handler.batches.size());

                    byte[] commit = handler.batches.get(handler.batches.size() - 1);
                    Assert.assertEquals("a commit frame carries no tables",
                            0, QwpWireTestUtils.tableCount(commit));
                    java.util.List<String> registered = new java.util.ArrayList<>();
                    QwpWireTestUtils.accumulateDeltaDictionary(commit, registered);
                    Assert.assertTrue("the commit frame must register no symbols; it re-shipped "
                                    + registered.size() + " -- the whole dictionary from id 0",
                            registered.isEmpty());
                }
            }
        });
    }

    @Test
    public void testCloseStillDrainsWhenTheRetainedBatchIsOverCap() throws Exception {
        // close() step 1 flushes; an over-cap batch throws there. Letting that throw
        // escape skips sendCommitMessage, sealAndSwapBuffer AND drainOnClose --
        // abandoning every row an earlier successful flush already published. close()
        // therefore discards the retained batch, remembers the error, and runs the rest.
        // Only the frame-per-flush path retains an over-cap batch (delta mode bounds
        // every record by the cap), so this runs on a full-dictionary engine: its
        // .symbol-dict cannot open.
        //
        // The observable is drainOnClose: with a server that never acks and a short
        // close budget, reaching step 3 produces a drain timeout. Remove the
        // catch(BatchTooLargeForCapException) and no drain timeout appears anywhere,
        // because close() never gets there. Every other close() assertion in the suite
        // survives that mutation -- the sites all catch the parent LineSenderException,
        // so caught-inside and escaping are indistinguishable to them.
        Path sfDir = temporaryFolder.newFolder("qwp-sf-retained-close").toPath();
        String slot = sfDir.resolve("default").toString();
        assertMemoryLeak(() -> {
            NeverAckingHandler handler = new NeverAckingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.setAdvertisedMaxBatchSize(200);
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String pad = new String(new char[40]).replace('\0', 'x');
                boolean threw = false;
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, new UnopenableDictFacade());
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 1_000_000, 0, 0L, null, false, engine, 250L)) {
                    Assert.assertFalse("setup: the full-dictionary frame path", sender.isDeltaDictEnabledForTest());
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

    /** Byte 5 of the QWP header is the flags byte; FLAG_DEFER_COMMIT is its low bit. */
    private static boolean hasDeferCommit(byte[] frame) {
        return frame.length >= QwpConstants.HEADER_SIZE
                && (frame[5] & QwpConstants.FLAG_DEFER_COMMIT) != 0;
    }

    private static int readVarint(byte[] buf, int offset) {
        return QwpWireTestUtils.readVarint(buf, new int[]{offset});
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
