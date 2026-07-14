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
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
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
        TestUtils.removeTmpDirRec(sfDir);
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
    public void testUnreplayableSlotIsSetAsideAndTheProducerKeepsProducing() throws Exception {
        // The point of setting an unreplayable slot aside rather than failing: the
        // producer stays ALIVE and its new data still reaches the server. This is the
        // property store-and-forward exists to guarantee, and the one bricking build()
        // destroyed -- one host crash and the application could never construct a Sender
        // again, so every row after the tear was lost too, not just the torn batch.
        //
        // Asserted end to end against a real wire: the fresh rows arrive and the server's
        // reconstructed dictionary holds EXACTLY the post-recovery symbols. Not one
        // symbol from the unreplayable slot appears -- so the producer kept producing
        // AND the torn frames stayed off the wire.
        assertMemoryLeak(() -> {
            // Phase 1: record delta frames, then lose the whole dictionary (host crash).
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    for (int i = 0; i < 6; i++) {
                        s1.table("m").symbol("s", "torn-" + i).longColumn("v", i).atNow();
                        s1.flush();
                    }
                }
            }
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            java.nio.file.Files.write(dict,
                    Arrays.copyOf(java.nio.file.Files.readAllBytes(dict), 8)); // header only: 0 symbols
            writeAckWatermark(slot.resolve(".ack-watermark"), 2);

            // Phase 2: the recovering sender sets the slot aside and keeps producing.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender s2 = Sender.fromConfig(cfg)) {
                    s2.table("m").symbol("s", "fresh-a").longColumn("v", 100).atNow();
                    s2.table("m").symbol("s", "fresh-b").longColumn("v", 101).atNow();
                    s2.flush();
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline && handler.dictSnapshot().size() < 2) {
                        Thread.sleep(20);
                    }
                    Assert.assertEquals("the new rows must reach the server",
                            2, handler.dictSnapshot().size());
                }

                assertUnreplayableSlotSetAside();
                List<String> dict2 = handler.dictSnapshot();
                Assert.assertEquals("the server must hold exactly the post-recovery symbols",
                        Arrays.asList("fresh-a", "fresh-b"), dict2);
                for (String s : dict2) {
                    Assert.assertFalse("no symbol of the unreplayable slot may reach the server: " + s,
                            s.startsWith("torn-"));
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

            // Simulate a host/power crash: the segment frames survive but the WHOLE
            // persisted dictionary is lost (truncate .symbol-dict to its 8-byte header,
            // 0 symbols), and the ack watermark was left mid-stream (FSN 2). The
            // surviving frames still reference the lost ids.
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            byte[] header = Arrays.copyOf(java.nio.file.Files.readAllBytes(dict), 8);
            java.nio.file.Files.write(dict, header);
            writeAckWatermark(slot.resolve(".ack-watermark"), 2);

            // Phase 2: recover against a fresh counting server. With the whole
            // dictionary lost (size 0) while the surviving frames still reference its
            // ids, seedGlobalDictionaryFromPersisted must refuse the resume at BUILD --
            // its size==0 short-circuit must NOT skip the torn-dict guard -- rather than
            // let the producer resume unseeded and silently misattribute reused ids.
            CountingHandler handler = new CountingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                // The dictionary cannot be rebuilt from any source, so this slot is genuinely
                // unreplayable -- and build() SETS IT ASIDE rather than failing. Failing here
                // is what bricked the sender: senderId is stable and a not-fully-drained slot
                // is retained on close, so every retry re-recovered the same slot and threw
                // again, and the application could not construct a Sender at all -- it could
                // not even buffer new rows. Now the producer comes up on a clean slot and the
                // unreplayable bytes are kept aside for resend.
                Sender.fromConfig(cfg).close();

                // The safety property is unchanged: not one frame of the unreplayable slot
                // may reach the server.
                Assert.assertEquals("no frame may be replayed to a fresh server with a torn dictionary",
                        0, handler.frames.get());
                assertUnreplayableSlotSetAside();
            }
        });
    }

    @Test
    public void testTornDictionaryOneIdGapFailsCleanly() throws Exception {
        // One-id-gap boundary variant of
        // testUnopenablePersistedDictStillGuardsAgainstReplayingDeltaFrames: the first
        // replayed frame starts EXACTLY ONE id past the empty mirror -- the tightest
        // gap the I/O-thread replay guard must still reject (deltaStart ==
        // sentDictCount + 1). It reaches that guard via the unopenable-dictionary path
        // (deltaDictEnabled=false), NOT a size-0 openable dict, which now fails clean
        // earlier in seedGlobalDictionaryFromPersisted. A one-entry-short tail is the
        // common host-crash outcome, so this pins the guard's false-negative edge: a
        // "deltaStart > size + 1" mutation would let this single-id gap through and
        // null-pad the missing symbol on the server.
        assertMemoryLeak(() -> {
            // Phase 1: each row introduces a new symbol (frame i carries deltaStart=i).
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

            // Make the persisted dictionary UNOPENABLE (replace .symbol-dict with a
            // directory, so both openRW and openCleanRW fail and the engine reports
            // deltaDictEnabled=false), then stamp the watermark at FSN 0 so recovery
            // replays from FSN 1 -- a frame with deltaStart=1. The mirror is unseeded
            // (size 0), so id 0 is the single missing entry: deltaStart(1) ==
            // sentDictCount(0) + 1. No catch-up is sent, so any counted frame would be
            // the gapped data frame.
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            java.nio.file.Files.delete(dict);
            java.nio.file.Files.createDirectory(dict);
            writeAckWatermark(slot.resolve(".ack-watermark"), 0);

            // Phase 2: recover against a fresh counting server. The guard must fire on
            // the very first replay frame (deltaStart 1 > recovered size 0) and fail
            // terminally rather than send a frame that null-pads id 0.
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
                Assert.assertEquals("a one-id gap must still block replay to a fresh server",
                        0, handler.frames.get());
                Assert.assertNotNull("a one-id-short dictionary must surface a terminal error", terminal);
                Assert.assertTrue(terminal.getMessage(),
                        terminal.getMessage().contains("delta start 1 exceeds recovered dictionary size 0"));
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
    public void testUnopenablePersistedDictReplaysSelfSufficientFrameSequence() throws Exception {
        // Untrimmed counterpart of
        // testUnopenablePersistedDictStillGuardsAgainstReplayingDeltaFrames.
        //
        // Same failure mode -- the persisted dictionary cannot be OPENED, so
        // CursorSendEngine.isDeltaDictEnabled() is false -- but NOTHING was acked, so
        // replay starts at the FIRST frame. Frames 0..5 carry deltaStart 0..5 and are
        // COLLECTIVELY self-sufficient: replayed in order from id 0, the server
        // accumulates the dictionary contiguously, needing no dictionary file at all.
        //
        // Pre-fix, accumulateSentDict was gated on deltaDictEnabled while the
        // torn-dict guard was NOT, so sentDictCount froze at 0: frame 0 (deltaStart=0)
        // passed the guard, but frame 1 (deltaStart=1 > 0) tripped it and latched a
        // terminal -- and for the background drainer that means markFailed + a .failed
        // sentinel, permanently quarantining a slot that drains perfectly. A store-
        // and-forward contract violation: a TRANSIENT disk condition (fd exhaustion, a
        // read-only remount, ENOSPC) stranding recoverable data.
        //
        // The trimmed sibling pins the other half of the contract: when replay starts
        // ABOVE the frames that introduced the ids, the terminal is still correct.
        assertMemoryLeak(() -> {
            // Phase 1: each row introduces a new symbol => frame i carries deltaStart=i.
            // The server never acks, so nothing is trimmed and replay starts at frame 0
            // (the common orphan-drain profile: the server was down the whole time).
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

            // Make the persisted dictionary UNOPENABLE: a directory of the same name
            // defeats both openRW and openCleanRW, so PersistedSymbolDict.open()
            // returns null. Deliberately do NOT stamp the ack watermark -- replay must
            // start at frame 0, which is what makes the sequence self-sufficient.
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            java.nio.file.Files.delete(dict);
            java.nio.file.Files.createDirectory(dict);

            // Phase 2: recover against a fresh server that reconstructs its per-
            // connection dictionary from the wire exactly as the real one does --
            // null-padding any gap, so a gap surfaces as a null rather than passing
            // silently.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                LineSenderException terminal = null;
                Sender s2 = Sender.fromConfig(cfg);
                try {
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline
                            && handler.maxDictSize() < 6
                            && terminal == null) {
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

                // Pre-fix: the "symbol dictionary is incomplete" terminal fires on frame 1.
                Assert.assertNull("a self-sufficient frame sequence must replay without a terminal: "
                                + (terminal == null ? "" : terminal.getMessage()),
                        terminal);
                // Pre-fix: the server dictionary stops at ["sym-0"] -- frame 1 never shipped.
                List<String> serverDict = handler.dictSnapshot();
                Assert.assertEquals("every delta frame must replay [dict=" + serverDict + ']',
                        6, serverDict.size());
                for (int i = 0; i < 6; i++) {
                    Assert.assertEquals("id " + i + " must resolve; a null here is a server-side "
                                    + "null-pad, i.e. a silently NULL symbol column [dict=" + serverDict + ']',
                            "sym-" + i, serverDict.get(i));
                }
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
    public void testPersistFailureSurfacesAsLineSenderException() throws Exception {
        // A dictionary write that fails mid-flush -- a full disk, an exhausted quota --
        // must reach the caller as a LineSenderException, like every other flush-path
        // failure. PersistedSymbolDict throws a low-level IllegalStateException on a short
        // write; persistNewSymbolsBeforePublish wraps it. Without the wrap the raw
        // IllegalStateException sails straight past every user's
        // `catch (LineSenderException)` around flush() and takes the application down.
        //
        // Nothing could reach that translation before: PersistedSymbolDict has
        // facade-aware overloads, but CursorSendEngine called only the
        // FilesFacade.INSTANCE ones, so no test could inject a dictionary I/O fault
        // through the real producer path. The engine now takes a FilesFacade for exactly
        // this, and the sender is built on that engine directly -- the same
        // QwpWebSocketSender.connect(..., CursorSendEngine) entry point Sender.build uses.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));

                String slot = Paths.get(sfDir, "default").toString();
                Assert.assertEquals(0, io.questdb.client.std.Files.mkdir(sfDir,
                        io.questdb.client.std.Files.DIR_MODE_DEFAULT));
                ShortDictWriteFacade ff = new ShortDictWriteFacade();
                // The engine owns the dictionary; the fault facade reaches only it, so the
                // segment files still write normally and the ONLY failure is the persist.
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
                try (Sender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 0, 0, 0L, null, false, engine)) {
                    ff.armed = true; // the next dictionary append short-writes
                    sender.table("m").symbol("s", "boom").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("a short write to the symbol dictionary must fail the flush");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue("the persist failure must be reported as a sender error, "
                                        + "not leak a raw IllegalStateException: " + expected.getMessage(),
                                expected.getMessage().contains("failed to persist symbol dictionary before publish"));
                    }
                } catch (LineSenderException ignored) {
                    // close() re-flushes the still-buffered row; the facade has disarmed, so
                    // this normally succeeds. Either way it is not what we assert.
                }
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
    public void testFailedPublishThenNewSymbolPersistsSuffixWithoutDuplicating() throws Exception {
        // Regression for the re-encode (else) branch of persistNewSymbolsBeforePublish.
        // After a failed publish leaves the durable dictionary size ahead of the wire
        // baseline (pd.size() > sentMaxSymbolId+1), a later flush that introduces a NEW
        // symbol cannot reuse the frame's already-encoded fast-path bytes -- it
        // re-encodes just the [pd.size() .. currentBatchMaxSymbolId] suffix via
        // appendSymbols. Keying that off pd.size() (not sentMaxSymbolId+1) keeps it
        // idempotent: the already-persisted prefix is NOT re-appended.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                // Small segment: any frame carrying the padded row fails to publish with
                // PAYLOAD_TOO_LARGE deterministically (no backpressure timing).
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";sf_max_bytes=1024;";
                String pad = TestUtils.repeat("x", 2000); // frame >> 1024-byte segment
                Sender sender = Sender.fromConfig(cfg);
                try {
                    // Flush 1: s0 is persisted (write-ahead) before the oversized frame
                    // fails to publish. pd.size()=1, sentMaxSymbolId stays -1.
                    sender.table("m").symbol("s", "s0").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("oversized frame must fail to publish");
                    } catch (LineSenderException expected) {
                    }

                    // Add a NEW symbol s1 (id 1). The failed s0 row is still buffered, so
                    // the batch is {s0, s1} and the durable size (1) has run ahead of the
                    // wire baseline (-1) -- the state that selects the appendSymbols branch.
                    sender.table("m").symbol("s", "s1").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("oversized frame must fail to publish");
                    } catch (LineSenderException expected) {
                    }

                    // The else branch persisted ONLY s1 (the suffix). The dictionary holds
                    // s0, s1 exactly once each. Pre-fix (appendSymbols from
                    // sentMaxSymbolId+1) re-appended s0, giving size 3.
                    PersistedSymbolDict pd = PersistedSymbolDict.open(Paths.get(sfDir, "default").toString());
                    Assert.assertNotNull(pd);
                    try {
                        Assert.assertEquals("re-encode suffix must not duplicate the persisted prefix",
                                2, pd.size());
                        Assert.assertEquals("s0", pd.readLoadedSymbols().getQuick(0));
                        Assert.assertEquals("s1", pd.readLoadedSymbols().getQuick(1));
                    } finally {
                        pd.close();
                    }
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() re-flushes the still-buffered oversized rows and fails
                        // again (PAYLOAD_TOO_LARGE); expected, resources still released.
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

    @Test
    public void testRecoverySeedKeepsUtf8CollidingSymbolsInLockstep() throws Exception {
        // M2 regression: two DISTINCT source symbols that collapse to the SAME UTF-8
        // bytes -- lone UTF-16 surrogates, which the encoder maps to '?' -- get
        // distinct producer ids and persist as separate entries. On recovery the
        // producer must rebuild its id space to match the persisted entry count
        // exactly. Pre-fix, seedGlobalDictionaryFromPersisted used getOrAddSymbol,
        // which de-duped the two decoded "?" strings, leaving the producer
        // dictionary (and sentMaxSymbolId) one short of pd.size() -- desyncing from
        // the send-loop catch-up mirror (which uses pd.size()) and silently
        // misattributing later symbols after a reconnect. addRecoveredSymbol appends
        // without de-duping, keeping producer and mirror in lockstep.
        assertMemoryLeak(() -> {
            // Phase 1: ingest two lone-surrogate symbols in file mode, close-fast.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    s1.table("m").symbol("s", "\uD800").longColumn("v", 1L).atNow(); // lone surrogate -> '?'
                    s1.flush();
                    s1.table("m").symbol("s", "\uD801").longColumn("v", 2L).atNow(); // a DIFFERENT one -> '?'
                    s1.flush();
                }
            }

            // The persisted dictionary holds TWO entries (both encode to '?').
            int persistedSize;
            PersistedSymbolDict pd = PersistedSymbolDict.open(Paths.get(sfDir, "default").toString());
            Assert.assertNotNull(pd);
            try {
                persistedSize = pd.size();
            } finally {
                pd.close();
            }
            Assert.assertEquals("two colliding symbols must persist as two entries", 2, persistedSize);

            // Phase 2: recover. The seeded producer id space must match pd.size().
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender s2 = Sender.fromConfig(cfg)) {
                    Assert.assertEquals("recovered producer dictionary must match the persisted "
                                    + "entry count (not de-duped), else the delta baseline desyncs from the mirror",
                            persistedSize, globalDictSize(s2));
                    Assert.assertEquals("delta baseline must resume at the persisted tip",
                            persistedSize - 1, intField(s2, "sentMaxSymbolId"));
                }
            }
        });
    }

    @Test
    public void testTornDictSubsetRebuildsFromSurvivingFrames() throws Exception {
        // The persisted .symbol-dict is NOT fsync'd, so a host crash can lose its
        // highest-id tail entries while the segment frames that introduced those ids
        // survive (out-of-order page loss). Those frames carry the torn-off symbols in
        // their OWN delta sections -- which is exactly why the background drainer replays
        // such a slot perfectly: accumulateSentDict rebuilds the dictionary from them.
        //
        // The producer must do the same. Pre-fix it seeded ONLY from the short dictionary,
        // saw that the frames out-reached it, and threw -- permanently bricking
        // Sender.build() for a slot the drainer drains fine. And the brick is permanent:
        // build()'s catch releases the slot lock, but the sender that would have hosted the
        // orphan drainer is the one that just failed, so nothing drains the slot and the
        // retry recovers it and throws again. seedGlobalDictionaryFromPersisted now seeds
        // from the dictionary's intact prefix AND THEN from the surviving frames' deltas,
        // so the producer's baseline lands on exactly the coverage the send loop's mirror
        // will reach.
        //
        // testTornDictionaryFailsCleanlyInsteadOfCorrupting pins the other half of the
        // contract: when the symbol-introducing frames were acked and TRIMMED away, the ids
        // really are unrecoverable and the resume must still fail clean.
        assertMemoryLeak(() -> {
            // Phase 1: three delta frames (a@0, b@1, c@2) with nothing acked, so all three
            // survive and replay from frame 0.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                Sender s1 = Sender.fromConfig(cfg);
                try {
                    s1.table("m").symbol("s", "a").longColumn("v", 0).atNow();
                    s1.flush();
                    s1.table("m").symbol("s", "b").longColumn("v", 1).atNow();
                    s1.flush();
                    s1.table("m").symbol("s", "c").longColumn("v", 2).atNow();
                    s1.flush();
                } finally {
                    s1.close();
                }
            }

            // Simulate the host-crash tear: rewrite the dictionary to drop its highest
            // entry (c@2), keeping a@0,b@1 -- while the frame that introduced c@2 stays on
            // disk. openClean truncates; the two appends leave exactly the two-entry
            // dictionary a torn tail would recover to.
            String slotDir = Paths.get(sfDir, "default").toString();
            try (PersistedSymbolDict torn = PersistedSymbolDict.openClean(slotDir)) {
                Assert.assertNotNull(torn);
                torn.appendSymbol("a");
                torn.appendSymbol("b");
                Assert.assertEquals(2, torn.size());
            }

            // Phase 2: the resuming sender must REBUILD c@2 from that surviving frame,
            // replay everything, and keep ingesting ABOVE the recovered tip.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                // Pre-fix this line THREW ("subset of the surviving frames").
                try (Sender resumed = Sender.fromConfig(cfg)) {
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline && handler.maxDictSize() < 3) {
                        Thread.sleep(20);
                    }
                    // A NEW symbol must land ABOVE the recovered tip. Seeded short, the
                    // producer would hand "d" an id the surviving frames already define,
                    // silently overwriting that symbol on the server -- the corruption the
                    // old throw existed to prevent, and which this seeding removes at the
                    // source rather than by refusing to start.
                    resumed.table("m").symbol("s", "d").longColumn("v", 3).atNow();
                    resumed.flush();
                    deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline && handler.maxDictSize() < 4) {
                        Thread.sleep(20);
                    }
                }

                List<String> dict = handler.dictSnapshot();
                Assert.assertEquals("dictionary must rebuild gap-free [" + dict + ']', 4, dict.size());
                Assert.assertEquals("a", dict.get(0));
                Assert.assertEquals("b", dict.get(1));
                // Rebuilt from the surviving frame's own delta, not from the torn file.
                Assert.assertEquals("c", dict.get(2));
                // The new symbol, placed above the recovered tip -- no id reuse.
                Assert.assertEquals("d", dict.get(3));
            }
        });
    }

    @Test
    public void testTornDictTotalLossRebuildsFromSurvivingFrames() throws Exception {
        // The total-loss counterpart of testTornDictSubsetRebuildsFromSurvivingFrames: the
        // whole dictionary is gone (size 0) but the file still opens, so
        // isDeltaDictEnabled() stays true and the engine does NOT discard it (the frames
        // carry deltaStart 0,1,2, so maxSymbolDeltaStart != 0 and the full-dict-fallback
        // discard correctly stays out of the way).
        //
        // The surviving frames start at id 0 and are collectively self-sufficient, so the
        // producer can rebuild the ENTIRE dictionary from their deltas -- exactly as the
        // send loop's mirror does. Pre-fix this threw and bricked build().
        assertMemoryLeak(() -> {
            // Phase 1: a@0, b@1, c@2; nothing acked, so all three replay from frame 0.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                Sender s1 = Sender.fromConfig(cfg);
                try {
                    s1.table("m").symbol("s", "a").longColumn("v", 0).atNow();
                    s1.flush();
                    s1.table("m").symbol("s", "b").longColumn("v", 1).atNow();
                    s1.flush();
                    s1.table("m").symbol("s", "c").longColumn("v", 2).atNow();
                    s1.flush();
                } finally {
                    s1.close();
                }
            }

            // Total tear: the dictionary recovers EMPTY but still opens.
            String slotDir = Paths.get(sfDir, "default").toString();
            try (PersistedSymbolDict torn = PersistedSymbolDict.openClean(slotDir)) {
                Assert.assertNotNull(torn);
                Assert.assertEquals(0, torn.size());
            }

            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                // Pre-fix this line THREW.
                try (Sender resumed = Sender.fromConfig(cfg)) {
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline && handler.maxDictSize() < 3) {
                        Thread.sleep(20);
                    }
                    resumed.table("m").symbol("s", "d").longColumn("v", 3).atNow();
                    resumed.flush();
                    deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline && handler.maxDictSize() < 4) {
                        Thread.sleep(20);
                    }
                }

                List<String> dict = handler.dictSnapshot();
                Assert.assertEquals("the whole dictionary must rebuild from the frames [" + dict + ']',
                        4, dict.size());
                Assert.assertEquals("a", dict.get(0));
                Assert.assertEquals("b", dict.get(1));
                Assert.assertEquals("c", dict.get(2));
                Assert.assertEquals("d", dict.get(3));
            }
            // The side-file is healed in passing -- persistNewSymbolsBeforePublish resumes
            // from pd.size() (0 here), so the flush above re-persisted the rebuilt ids
            // a,b,c along with d. That is not asserted on disk because it is not
            // observable here: the slot drains fully, and a fully-drained close removes
            // the dictionary (CursorSendEngine -> PersistedSymbolDict.removeOrphan), so
            // re-opening it would just yield a fresh empty file. The persist-resumes-from-
            // pd.size() behaviour is pinned by the testFailedPublish* tests.
        });
    }

    @Test
    public void testFullDictFramesRecoverInFullDictModeInsteadOfBricking() throws Exception {
        // M1 regression (counterpoint to testTornDictTotalLossFailsCleanOnResume): a
        // slot written in FULL-DICT fallback -- the .symbol-dict could not open when
        // writing, so isDeltaDictEnabled() was false and every frame re-ships the
        // whole dictionary from id 0 (deltaStart=0) -- leaves SELF-SUFFICIENT frames
        // and NO side-file. On recovery the engine opens a FRESH EMPTY .symbol-dict,
        // so the surviving frames out-reach it (recoveredMaxSymbolId >= pd.size()==0).
        // Those frames carry their whole dictionary inline and need no side-file, so
        // they must RECOVER, not brick build(). CursorSendEngine detects them
        // (maxSymbolDeltaStart == 0) and discards the empty side-file, recovering the
        // slot in full-dict mode. Before the fix the sender's seed-time guard treated
        // the empty dictionary as a host-crash tear and threw from Sender.build(),
        // even though the orphan drainer drains the very same frames fine. A torn
        // DELTA dictionary (deltaStart > 0) still fails clean -- see that other test.
        assertMemoryLeak(() -> {
            java.nio.file.Path slot = Paths.get(sfDir, "default");
            java.nio.file.Path dict = slot.resolve(".symbol-dict");
            // Force full-dict fallback in phase 1: plant a non-empty DIRECTORY where
            // the dictionary file belongs, so openCleanRW fails and delta encoding
            // stays disabled -- the frames are then written self-sufficient.
            java.nio.file.Files.createDirectories(dict);
            java.nio.file.Path blocker = dict.resolve("blocker");
            java.nio.file.Files.createFile(blocker);

            // Phase 1: silent server (no acks). Sender 1 writes new-symbol rows in
            // full-dict mode and close-fast, leaving unacked self-sufficient frames.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port
                        + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (Sender s1 = Sender.fromConfig(cfg)) {
                    for (int i = 0; i < DISTINCT_SYMBOLS; i++) {
                        s1.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                        s1.flush();
                    }
                }
                // The planted directory is untouched -- the dictionary never opened,
                // so the frames were written full-dict with no side-file.
                Assert.assertTrue("full-dict fallback: .symbol-dict must stay a directory",
                        java.nio.file.Files.isDirectory(dict));
            }

            // Drop the planted directory so recovery opens a FRESH EMPTY .symbol-dict
            // where it belongs -- exactly the state a full-dict-fallback slot recovers
            // into (frames on disk, no dictionary behind them).
            java.nio.file.Files.delete(blocker);
            java.nio.file.Files.delete(dict);

            // Phase 2: recover. build() must SUCCEED (not throw the torn-dict guard),
            // and the self-sufficient frames replay against a fresh server gap-free.
            DictReconstructingHandler handler = new DictReconstructingHandler();
            try (TestWebSocketServer good = new TestWebSocketServer(handler)) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir + ";";
                try (Sender ignored = Sender.fromConfig(cfg)) { // must NOT throw
                    long deadline = System.currentTimeMillis() + 5_000;
                    while (System.currentTimeMillis() < deadline
                            && handler.maxDictSize() < DISTINCT_SYMBOLS) {
                        Thread.sleep(20);
                    }
                }
                // Full-dict recovery re-ships the whole dictionary inline on every
                // frame, so there is NO 0-table catch-up frame (that is the delta-mode
                // reconnect path). The reconstructed dictionary is still gap-free.
                Assert.assertFalse("full-dict recovery must NOT send a catch-up frame",
                        handler.sawCatchUpFrame);
                List<String> reconstructed = handler.dictSnapshot();
                Assert.assertEquals("reconstructed dictionary size", DISTINCT_SYMBOLS, reconstructed.size());
                for (int i = 0; i < DISTINCT_SYMBOLS; i++) {
                    Assert.assertEquals("dictionary id " + i, "sym-" + i, reconstructed.get(i));
                }
            }
        });
    }

    private static int globalDictSize(Sender sender) throws Exception {
        Field f = sender.getClass().getDeclaredField("globalSymbolDictionary");
        f.setAccessible(true);
        Object dict = f.get(sender);
        return (int) dict.getClass().getMethod("size").invoke(dict);
    }

    private static int intField(Sender sender, String name) throws Exception {
        Field f = sender.getClass().getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(sender);
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
    /**
     * Short-writes the FIRST dictionary append after it is armed, modelling a disk that
     * fills mid-flush. Offset 0 is left alone so the file header still writes -- only the
     * entry append fails, which is the path under test.
     */
    private static final class ShortDictWriteFacade extends DelegatingFilesFacade {
        boolean armed;

        @Override
        public long write(int fd, long addr, long len, long offset) {
            if (armed && offset > 0 && len > 1) {
                armed = false;
                return INSTANCE.write(fd, addr, len - 1, offset);
            }
            return INSTANCE.write(fd, addr, len, offset);
        }
    }


    /**
     * The unreplayable-slot contract: a recovered slot whose symbol dictionary cannot be
     * rebuilt -- not from its own intact prefix, not from the surviving frames' delta
     * sections -- is SET ASIDE, never silently drained and never allowed to brick the
     * sender.
     * <p>
     * The bytes are kept for forensics and resend, the {@code .failed} sentinel tells the
     * orphan drainer to treat the copy as human-in-the-loop rather than retry it forever,
     * and the producer continues on a fresh, empty slot.
     */
    private void assertUnreplayableSlotSetAside() {
        java.nio.file.Path aside = Paths.get(sfDir, "default.unreplayable-0");
        Assert.assertTrue("the unreplayable slot must be set aside, not deleted: " + aside,
                java.nio.file.Files.isDirectory(aside));
        Assert.assertTrue("the set-aside slot must keep its recorded frames for resend",
                hasSegmentFile(aside));
        Assert.assertTrue("the set-aside slot must carry the .failed sentinel",
                java.nio.file.Files.exists(aside.resolve(".failed")));
        Assert.assertTrue("the sender must continue on a live slot",
                java.nio.file.Files.isDirectory(Paths.get(sfDir, "default")));
    }

    private static boolean hasSegmentFile(java.nio.file.Path dir) {
        java.io.File[] files = dir.toFile().listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.getName().endsWith(".sfa") && f.length() > 0) {
                    return true;
                }
            }
        }
        return false;
    }

}
