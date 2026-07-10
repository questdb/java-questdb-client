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
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * End-to-end recovery for the delta symbol dictionary in store-and-forward mode.
 * <p>
 * A file-mode sender writes delta-encoded SYMBOL frames (each frame carries only
 * the ids it introduces) to a slot but never drains it -- simulating a crash. A
 * fresh sender then recovers the slot and replays those non-self-sufficient
 * frames to a brand-new server whose dictionary starts empty. Correctness hinges
 * on the persisted {@code .symbol-dict}: the recovering sender loads it, the I/O
 * thread re-registers the whole dictionary via a catch-up frame, and only then do
 * the delta frames replay. This test reconstructs the server-side dictionary from
 * the wire and asserts it comes out complete and gap-free.
 */
public class DeltaDictRecoveryTest {

    private static final int DISTINCT_SYMBOLS = 8;
    private static final int ROWS = 40;
    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-delta-recov-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) {
            rmDirRec(sfDir);
        }
    }

    @Test
    public void testRecoveredSlotReplaysDeltaFramesAgainstFreshServer() throws Exception {
        assertMemoryLeak(() -> {
            // Phase 1: silent server (no acks). Sender 1 writes symbol rows and
            // close-fast (no drain), leaving unacked delta frames + a persisted
            // dictionary in the slot.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));

                String pad = TestUtils.repeat("x", 64);
                String cfg = "ws::addr=localhost:" + port
                        + ";sf_dir=" + sfDir
                        + ";sf_max_bytes=4096"
                        + ";close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    for (int i = 0; i < ROWS; i++) {
                        s1.table("m")
                                .symbol("s", "sym-" + (i % DISTINCT_SYMBOLS))
                                .stringColumn("p", pad)
                                .longColumn("v", i)
                                .atNow();
                        s1.flush();
                    }
                }
            }

            // Ack a prefix so recovery does NOT replay from the self-sufficient head.
            // Rows 0..DISTINCT_SYMBOLS-1 register all the symbols, so stamping the
            // watermark at FSN DISTINCT_SYMBOLS-1 makes recovery replay from FSN
            // DISTINCT_SYMBOLS onward -- frames whose delta starts at
            // DISTINCT_SYMBOLS and carries NO new symbols (rows past the first cycle
            // reuse existing ids). The early ids those frames reference then exist
            // ONLY in the persisted dictionary, so the reconstructed dictionary below
            // is complete solely because the catch-up frame re-registered them. That
            // pins the content assertions to the catch-up: without it (or with a
            // broken one) the fresh server would null-pad ids 0..DISTINCT_SYMBOLS-1
            // and the per-id checks would fail.
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            writeAckWatermark(slot.resolve(".ack-watermark"), DISTINCT_SYMBOLS - 1);

            // Phase 2: fresh server that reconstructs its per-connection dictionary
            // from the delta sections. Sender 2 recovers the slot and replays.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender ignored = Sender.fromConfig(cfg)) {
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline
                            && handler.maxDictSize() < DISTINCT_SYMBOLS) {
                        Thread.sleep(20);
                    }
                }

                // The recovering sender must have re-registered the dictionary via a
                // catch-up (0-table) frame before replaying delta frames.
                Assert.assertTrue("recovery sent a full-dictionary catch-up frame",
                        handler.sawCatchUpFrame);
                // The reconstructed dictionary must be complete and gap-free: exactly
                // the DISTINCT_SYMBOLS symbols, no null padding left by a missing id.
                List<String> dict = handler.dictSnapshot();
                Assert.assertEquals("reconstructed dictionary size", DISTINCT_SYMBOLS, dict.size());
                for (int i = 0; i < DISTINCT_SYMBOLS; i++) {
                    Assert.assertEquals("dictionary id " + i, "sym-" + i, dict.get(i));
                }
            }
        });
    }

    @Test
    public void testTornDictionaryFailsCleanlyInsteadOfCorrupting() throws Exception {
        assertMemoryLeak(() -> {
            // Phase 1: each row introduces a new symbol, so frame i carries deltaStart=i.
            // Silent server + close-fast leaves all frames unacked in the slot.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    for (int i = 0; i < 6; i++) {
                        s1.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                        s1.flush();
                    }
                }
            }

            // Simulate a host/power crash: the segment frames survive but the persisted
            // dictionary is lost, and the ack watermark was left mid-stream. Truncate
            // .symbol-dict to its 8-byte header (0 symbols) and stamp the watermark at
            // FSN 2, so recovery replays from FSN 3 -- a frame with deltaStart=3.
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            byte[] header = Arrays.copyOf(java.nio.file.Files.readAllBytes(dict), 8);
            java.nio.file.Files.write(dict, header);
            writeAckWatermark(slot.resolve(".ack-watermark"), 2);

            // Phase 2: recover against a fresh counting server. The replay guard must
            // fire (frame deltaStart 3 > recovered dictionary size 0) and fail terminally
            // rather than send a gapped frame that would corrupt the table.
            CountingHandler handler = new CountingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                LineSenderException terminal = null;
                Sender s2 = Sender.fromConfig(cfg);
                try {
                    // Poll for the replay guard to fire (it recordFatal's on the I/O
                    // thread); flush() surfaces the latched terminal to the producer.
                    // A bounded poll replaces a fixed sleep and captures it as soon as
                    // it fires; close() below is the fallback if it surfaces only there.
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline && terminal == null) {
                        try {
                            s2.flush();
                            Thread.sleep(20);
                        } catch (LineSenderException e) {
                            terminal = e;
                        }
                    }
                } finally {
                    try {
                        s2.close();
                    } catch (LineSenderException e) {
                        if (terminal == null) {
                            terminal = e;
                        }
                    }
                }
                Assert.assertEquals("no frame may be replayed to a fresh server with a torn dictionary",
                        0, handler.frames.get());
                Assert.assertNotNull("a torn dictionary must surface a terminal error", terminal);
                Assert.assertTrue(terminal.getMessage(),
                        terminal.getMessage().contains("symbol dictionary is incomplete"));
            }
        });
    }

    @Test
    public void testRecoveredSenderContinuesIngestingNewSymbols() throws Exception {
        // M2 regression: seedGlobalDictionaryFromPersisted resumes the producer's
        // dictionary and delta baseline from the persisted .symbol-dict, so a
        // recovered sender that continues ingesting assigns the NEXT id, not a
        // colliding low one. Without it the new symbol reuses a recovered id and the
        // fresh server sees a redefinition -> silent misattribution. No prior test
        // ingests on the recovered sender. Replay is from FSN 0 (no acks), so the
        // recovered frames legitimately overlap the seeded dictionary -- this also
        // pins that the redefinition guard does not false-positive on normal
        // recovery.
        assertMemoryLeak(() -> {
            // Phase 1: ingest DISTINCT_SYMBOLS symbols, silent server, close-fast ->
            // unacked frames + a full persisted dictionary.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";sf_max_bytes=4096;close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    for (int i = 0; i < DISTINCT_SYMBOLS; i++) {
                        s1.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                        s1.flush();
                    }
                }
            }

            // Phase 2: recover against a fresh server, then ingest a genuinely NEW
            // symbol. The producer must continue at id DISTINCT_SYMBOLS.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender s2 = Sender.fromConfig(cfg)) {
                    s2.table("m").symbol("s", "brand-new").longColumn("v", 99L).atNow();
                    s2.flush();
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline
                            && handler.maxDictSize() < DISTINCT_SYMBOLS + 1) {
                        Thread.sleep(20);
                    }
                }
                List<String> dict = handler.dictSnapshot();
                Assert.assertEquals("recovered sender must continue the dictionary, not collide: " + dict,
                        DISTINCT_SYMBOLS + 1, dict.size());
                for (int i = 0; i < DISTINCT_SYMBOLS; i++) {
                    Assert.assertEquals("sym-" + i, dict.get(i));
                }
                Assert.assertEquals("brand-new", dict.get(DISTINCT_SYMBOLS));
            }
        });
    }

    @Test
    public void testUnopenablePersistedDictStillGuardsAgainstReplayingDeltaFrames() throws Exception {
        // C1 regression: when a recovered disk slot's persisted dictionary cannot be
        // OPENED (fd exhaustion, a read-only remount, ENOSPC -- simulated here by a
        // .symbol-dict that is a DIRECTORY, so both openRW and openCleanRW fail),
        // CursorSendEngine.isDeltaDictEnabled() returns false. The recorded frames
        // are still DELTA frames, and replaying them against a fresh
        // empty-dictionary server would null-pad the missing ids and SILENTLY
        // corrupt the table. The torn-dictionary guard must fire regardless of
        // deltaDictEnabled -- pre-fix it was gated on that very flag, so the
        // corrupting frame sailed through unguarded. Unlike
        // testTornDictionaryFailsCleanlyInsteadOfCorrupting (dict present but empty,
        // deltaDictEnabled=true), here the dict is UNOPENABLE (deltaDictEnabled=false).
        assertMemoryLeak(() -> {
            // Phase 1: each row introduces a new symbol => frame i carries deltaStart=i.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    for (int i = 0; i < 6; i++) {
                        s1.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                        s1.flush();
                    }
                }
            }

            // Make the persisted dictionary UNOPENABLE: replace the .symbol-dict file
            // with a directory of the same name so PersistedSymbolDict.open() returns
            // null (both openRW and openCleanRW fail) and the engine reports
            // deltaDictEnabled=false. Stamp the watermark at FSN 2 so replay starts
            // at FSN 3 -- a frame whose delta starts at id 3, with ids 0..2 living
            // only in the now-unreadable dictionary.
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            java.nio.file.Files.delete(dict);
            java.nio.file.Files.createDirectory(dict);
            writeAckWatermark(slot.resolve(".ack-watermark"), 2);

            // Phase 2: recover against a fresh counting server. The guard must fire
            // (frame deltaStart 3 > recovered dictionary size 0) and fail terminally
            // rather than send a gapped frame that would corrupt the table.
            CountingHandler handler = new CountingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                LineSenderException terminal = null;
                Sender s2 = Sender.fromConfig(cfg);
                try {
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline && terminal == null) {
                        try {
                            s2.flush();
                            Thread.sleep(20);
                        } catch (LineSenderException e) {
                            terminal = e;
                        }
                    }
                } finally {
                    try {
                        s2.close();
                    } catch (LineSenderException e) {
                        if (terminal == null) {
                            terminal = e;
                        }
                    }
                }
                Assert.assertEquals("no delta frame may be replayed when the persisted dictionary is unopenable",
                        0, handler.frames.get());
                Assert.assertNotNull("an unopenable dictionary must surface a terminal error", terminal);
                Assert.assertTrue(terminal.getMessage(),
                        terminal.getMessage().contains("symbol dictionary is incomplete"));
            }
        });
    }

    @Test
    public void testCommitMessageDoesNotShipUnpersistedLeakedSymbol() throws Exception {
        // C3 regression: sendCommitMessage does NOT write-ahead-persist the
        // dictionary, so its frame must carry NO new symbol. A symbol left in the
        // batch by a cancelled row -- cancelRow rolls back neither
        // currentBatchMaxSymbolId nor the global-dictionary registration -- must not
        // ride out on the commit frame: doing so puts an id on the wire that a
        // recovered slot cannot rebuild from .symbol-dict, diverging the producer
        // dictionary from the surviving frames and silently misattributing reused
        // ids after a crash. The commit's delta must be bounded by sentMaxSymbolId
        // (empty here), not currentBatchMaxSymbolId. Memory mode suffices to observe
        // the wire behaviour; close() drains every frame to the server first.
        assertMemoryLeak(() -> {
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Transactional + autoFlushRows=1: every completed row auto-flushes
                // as a DEFERRED batch (setting hasDeferredMessages); an explicit
                // flush() then emits the commit message.
                Sender sender = Sender.builder("ws::addr=localhost:" + port + ";")
                        .transactional(true)
                        .autoFlushRows(1)
                        .build();
                try {
                    // Row 1 registers "a"@0 and auto-flushes it deferred.
                    sender.table("m").symbol("s", "a").longColumn("v", 1L).atNow();
                    // Register "b"@1 on a row that is then cancelled: "b" stays in
                    // the global dictionary and currentBatchMaxSymbolId advances to
                    // 1, but nothing persists or sends it.
                    sender.table("m").symbol("s", "b");
                    sender.cancelRow();
                    // Commit the deferred batch. The commit frame must carry an
                    // EMPTY delta -- NOT "b"@1.
                    sender.flush();
                } finally {
                    sender.close(); // drains every frame (incl. the commit) to the server
                }

                // The server's reconstructed dictionary must hold ONLY "a". Pre-fix
                // the commit shipped "b"@1, so the server saw a second symbol.
                List<String> dict = handler.dictSnapshot();
                Assert.assertEquals("commit frame must not ship the cancelled row's leaked symbol "
                                + "(recovery would then diverge from the persisted dictionary): " + dict,
                        1, dict.size());
                Assert.assertEquals("a", dict.get(0));
            }
        });
    }

    @Test
    public void testFailedPublishDoesNotDuplicatePersistedSymbols() throws Exception {
        // Regression: persistNewSymbolsBeforePublish is a write-ahead -- it runs
        // BEFORE the frame is published (sealAndSwapBuffer -> appendBlocking). If
        // publish fails (here PAYLOAD_TOO_LARGE, a frame bigger than the SF
        // segment; a backpressure deadline in production), the frame's symbols are
        // already on disk but sentMaxSymbolId is NOT advanced and the rows stay
        // buffered -- so a retry re-runs the persist. Keying the persist range off
        // pd.size() (not sentMaxSymbolId+1) makes it idempotent. Before that fix,
        // the retry appended the symbol a SECOND time, breaking the dense
        // id->position invariant; on recovery every later global id shifts by one
        // and symbol column values are silently misattributed.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));

                // Small segment; a heavily padded row's frame cannot fit, so
                // appendBlocking throws PAYLOAD_TOO_LARGE deterministically -- no
                // backpressure timing needed. The server never acks (SilentHandler).
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";sf_max_bytes=1024;";
                String pad = TestUtils.repeat("x", 2000); // frame >> 1024-byte segment
                Sender sender = Sender.fromConfig(cfg);
                try {
                    // Buffer ONE new-symbol row, then flush it TWICE. Each flush
                    // runs the write-ahead persist and then fails to publish; the
                    // failed flush leaves the row buffered, so the second flush is
                    // the retry that (pre-fix) duplicated the persisted symbol.
                    sender.table("m").symbol("s", "s0").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    for (int attempt = 0; attempt < 2; attempt++) {
                        try {
                            sender.flush();
                            Assert.fail("oversized frame must fail to publish");
                        } catch (LineSenderException expected) {
                            // frame too large -- expected on every attempt
                        }
                    }

                    // The persisted dictionary must hold "s0" EXACTLY ONCE.
                    // Pre-fix, the retry duplicated it (size == 2).
                    PersistedSymbolDict pd = PersistedSymbolDict.open(Paths.get(sfDir, "default").toString());
                    Assert.assertNotNull(pd);
                    try {
                        Assert.assertEquals("failed-publish retry must not duplicate the persisted symbol",
                                1, pd.size());
                        Assert.assertEquals("s0", pd.readLoadedSymbols().getQuick(0));
                    } finally {
                        pd.close();
                    }
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() re-flushes the still-buffered oversized row and
                        // fails again (PAYLOAD_TOO_LARGE); expected here and not
                        // what we assert. close() still runs its resource cleanup,
                        // so no native memory leaks.
                    }
                }
            }
        });
    }

    @Test
    public void testRecoveryAfterFailedPublishReplaysGapFree() throws Exception {
        // M3 end-to-end: chains a failed publish -> fresh-process recovery -> replay.
        // A failed publish persists the frame's symbol (write-ahead) but does NOT
        // record the frame, so the persisted dictionary becomes a strict SUPERSET of
        // the recorded frames' references. A recovering sender must still replay
        // gap-free: it re-registers the whole (superset) dictionary via the catch-up
        // -- including the symbol whose frame never reached disk -- so the fresh
        // server reconstructs a complete, gap-free dictionary. The sibling
        // testFailedPublishDoesNotDuplicatePersistedSymbols proves the dict has no
        // duplicate after the failed publish; this proves the resulting slot then
        // recovers and replays end-to-end against a real server.
        assertMemoryLeak(() -> {
            // Phase 1: a silent server (no acks) + a small SF segment. Four small
            // rows register sym-0..sym-3 and their frames are recorded; a fifth,
            // oversized row registers (persists) sym-4 but its frame is too large for
            // the segment, so appendBlocking throws and the frame is NOT recorded.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port
                        + ";sf_dir=" + sfDir
                        + ";sf_max_bytes=4096"
                        + ";close_flush_timeout_millis=0;";
                Sender s1 = Sender.fromConfig(cfg);
                try {
                    for (int i = 0; i < 4; i++) {
                        s1.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                        s1.flush();
                    }
                    // Oversized frame: sym-4 is persisted (write-ahead) before the
                    // publish fails, so the dictionary runs one ahead of the frames.
                    s1.table("m").symbol("s", "sym-4")
                            .stringColumn("p", TestUtils.repeat("x", 8000))
                            .longColumn("v", 4).atNow();
                    try {
                        s1.flush();
                        Assert.fail("oversized frame must fail to publish");
                    } catch (LineSenderException expected) {
                        // PAYLOAD_TOO_LARGE -- frame not recorded, sym-4 stays persisted
                    }
                } finally {
                    try {
                        // Re-flushes the still-buffered oversized row and fails again
                        // (expected); resources are still released, and the idempotent
                        // write-ahead does not re-append sym-4.
                        s1.close();
                    } catch (LineSenderException ignored) {
                    }
                }
            }

            // The persisted dictionary must hold the superset: sym-0..sym-4 (5 ids),
            // one more than the four recorded frames reference, with no duplicate.
            PersistedSymbolDict pd = PersistedSymbolDict.open(Paths.get(sfDir, "default").toString());
            Assert.assertNotNull(pd);
            try {
                Assert.assertEquals("failed publish must leave the dict a superset (sym-0..sym-4)",
                        5, pd.size());
            } finally {
                pd.close();
            }

            // Phase 2: recover against a fresh server that reconstructs its
            // dictionary from the wire. The recovering sender must re-register all 5
            // symbols via a catch-up (sym-4 exists ONLY in the dictionary -- no frame
            // carries it) and replay the 4 recorded frames, leaving a complete,
            // gap-free server dictionary.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender ignored = Sender.fromConfig(cfg)) {
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline && handler.maxDictSize() < 5) {
                        Thread.sleep(20);
                    }
                }
                Assert.assertTrue("recovery must send a full-dictionary catch-up frame",
                        handler.sawCatchUpFrame);
                List<String> dict = handler.dictSnapshot();
                Assert.assertEquals("recovered dictionary must include the failed-publish symbol",
                        5, dict.size());
                for (int i = 0; i < 5; i++) {
                    Assert.assertEquals("dictionary id " + i + " must be gap-free",
                            "sym-" + i, dict.get(i));
                }
            }
        });
    }

    private static void writeAckWatermark(java.nio.file.Path path, long fsn) throws IOException {
        byte[] buf = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(0x31574B41); // 'AKW1'
        bb.putInt(0);          // reserved
        bb.putLong(fsn);
        java.nio.file.Files.write(path, buf);
    }

    private static int readVarint(byte[] buf, int[] pos) {
        int result = 0;
        int shift = 0;
        while (pos[0] < buf.length) {
            int b = buf[pos[0]++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalStateException("varint too long");
            }
        }
        throw new IllegalStateException("varint truncated");
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            rmDirRec(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /**
     * Reconstructs the per-connection symbol dictionary from delta sections,
     * mirroring the server's {@code setQuick(deltaStart + i)} + null-padding.
     */
    private static class DictReconstructingHandler implements TestWebSocketServer.WebSocketServerHandler {
        volatile boolean sawCatchUpFrame;
        private final List<String> dict = new ArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler currentClient;

        synchronized List<String> dictSnapshot() {
            return new ArrayList<>(dict);
        }

        synchronized int maxDictSize() {
            return dict.size();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                dict.clear(); // fresh server dictionary per connection
            }
            accumulate(data);
            if (tableCount(data) == 0 && hasDelta(data)) {
                sawCatchUpFrame = true;
            }
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }

        private static boolean hasDelta(byte[] frame) {
            return frame.length >= 12 && (frame[5] & 0x08) != 0;
        }

        private static int tableCount(byte[] frame) {
            return (frame[6] & 0xFF) | ((frame[7] & 0xFF) << 8);
        }

        private void accumulate(byte[] frame) {
            if (!hasDelta(frame)) {
                return;
            }
            int[] pos = {12};
            int deltaStart = readVarint(frame, pos);
            int deltaCount = readVarint(frame, pos);
            while (dict.size() < deltaStart) {
                dict.add(null);
            }
            for (int i = 0; i < deltaCount; i++) {
                int len = readVarint(frame, pos);
                String sym = new String(frame, pos[0], len, StandardCharsets.UTF_8);
                pos[0] += len;
                int idx = deltaStart + i;
                while (dict.size() <= idx) {
                    dict.add(null);
                }
                dict.set(idx, sym);
            }
        }
    }

    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks -- sender leaves everything unacked in the slot
        }
    }

    /** Counts every binary frame it receives and acks it. */
    private static class CountingHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger frames = new AtomicInteger();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
