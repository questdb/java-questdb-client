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
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Critical-review pin-down for the {@code Sender.build()} half of the C5 fix: a slot whose
 * recovery had to skip an unreadable segment must be quarantined -- the same whole-directory
 * rename-and-continue treatment already proven for the symbol-dictionary refusal -- not merely
 * refuse to construct.
 * <p>
 * Before this fix, {@code CursorSendEngine}'s constructor could throw
 * {@code UnreplayableSlotException} ({@code SegmentRing.openExisting} refuses when it had to skip
 * an unreadable segment), but {@code Sender.build()}'s {@code catch (UnreplayableSlotException e)}
 * wrapped only {@code QwpWebSocketSender.connect(...)}, not the constructor call itself -- so the
 * refusal escaped {@code build()} entirely, uncaught. Worse: because the skipped file gets renamed
 * to {@code <path>.corrupt}, a SECOND recovery attempt against the SAME directory no longer even
 * sees it as a {@code .sfa} candidate, and would silently recover a ring missing the oldest
 * segment's data -- the exact C5 silent loss, deferred by exactly one restart.
 * <p>
 * The fix reuses {@code quarantineTornSlot} (with a {@code null} live engine) to rename the WHOLE
 * slot directory aside, the same mechanism already proven for the dictionary-refusal case in
 * {@code DeltaDictRecoveryTest}. That is what closes the gap: the replacement engine is built at
 * the original {@code slotPath}, which is now a genuinely fresh, empty directory with nothing
 * left to skip.
 */
public class SegmentSkipQuarantineTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-skip-quarantine-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test(timeout = 30_000L)
    public void testConstructionTimeSkipQuarantinesTheWholeSlotAndProducerContinues() throws Exception {
        assertMemoryLeak(() -> {
            java.nio.file.Path liveSlot = writeMultiSegmentSlotWithCorruptedOldest();

            // Phase 2: a fresh server. The recovering sender's CONSTRUCTOR now hits an
            // unreadable oldest segment. build() must not throw -- the slot must be
            // quarantined and the producer must keep working on a fresh one.
            AtomicBoolean sawBinary = new AtomicBoolean();
            try (TestWebSocketServer good = new TestWebSocketServer(new MarkerHandler(sawBinary))) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;";
                try (Sender s2 = Sender.fromConfig(cfg)) {
                    s2.table("foo").stringColumn("p", "after-quarantine").longColumn("v", 1).atNow();
                    s2.flush();
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline && !sawBinary.get()) {
                        Thread.sleep(20);
                    }
                }
                Assert.assertTrue("the producer must keep producing after the construction-time "
                        + "refusal, not brick build()", sawBinary.get());
            }

            // The tainted directory was renamed aside WHOLESALE -- the same treatment
            // DeltaDictRecoveryTest proves for the dictionary-refusal case -- not a narrower
            // workaround that leaves it sitting at the original path.
            java.nio.file.Path quarantined = Paths.get(sfDir, "default.unreplayable-0");
            Assert.assertTrue("the tainted slot must be quarantined wholesale, not left in place",
                    java.nio.file.Files.isDirectory(quarantined));
            Assert.assertTrue("the quarantined copy must carry the .failed sentinel so the orphan "
                            + "drainer never re-adopts it (OrphanScanner also excludes it by the "
                            + "\"unreplayable-\" infix, independent of this sentinel)",
                    java.nio.file.Files.exists(quarantined.resolve(".failed")));
            // Recovery failed CLOSED, so it never reached its deferred quarantine step:
            // the corrupt segment keeps its original name and every byte stays put.
            // Preserve-by-rename happens at the SLOT level here (the whole directory
            // moved aside), which is what keeps the data available for a manual resend.
            Assert.assertTrue("the corrupted segment's bytes must survive inside the "
                            + "quarantined copy, untouched by the failed recovery",
                    java.nio.file.Files.exists(quarantined.resolve("sf-initial.sfa")));

            // The critical regression check: the live slot is a GENUINELY FRESH directory, not
            // the tainted one left in place with the corrupt file merely invisible to a
            // re-scan. Before this fix, a second SegmentRing.openExisting on the ORIGINAL
            // directory would silently recover a ring missing the oldest segment (the
            // corrupt-renamed file no longer matches ".sfa"), seeding ackedFsn past its
            // frames. Here there is no "original directory" left to re-scan at all --
            // sf-initial.sfa in the LIVE slot must be a fresh baseSeq=0 segment holding only
            // what sender 2 wrote, not a continuation of sender 1's FSN sequence.
            Assert.assertTrue("the live slot must exist as a fresh directory",
                    java.nio.file.Files.isDirectory(liveSlot));
            java.nio.file.Path liveInitial = liveSlot.resolve("sf-initial.sfa");
            Assert.assertTrue("the live slot must have its own fresh sf-initial.sfa",
                    java.nio.file.Files.exists(liveInitial));
            try (MmapSegment seg = MmapSegment.openExisting(liveInitial.toString())) {
                Assert.assertEquals("a genuinely fresh ring must restart FSNs at 0, not continue "
                                + "sender 1's sequence -- continuing would mean the live slot is "
                                + "really the tainted one, just relabelled",
                        0L, seg.baseSeq());
                Assert.assertEquals("the fresh slot must hold only sender 2's one row",
                        1L, seg.frameCount());
            }
        });
    }

    /**
     * The construction-time quarantine reuses {@code quarantineTornSlot} unchanged, so the
     * existing {@code MAX_QUARANTINE_SLOT_ATTEMPTS} (64) cap on {@code default.unreplayable-<i>}
     * candidates applies here exactly as it does to the connect()-time dictionary-refusal case
     * ({@code DeltaDictRecoveryTest#testQuarantineFailsLoudlyWhenAllSlotNamesSaturated}).
     * Mirrors that test for the construction-time trigger: when every candidate name is already
     * taken, {@code build()} must fail LOUDLY -- a {@link LineSenderException} naming the
     * problem -- rather than silently dropping the tainted slot's bytes. This is the failure
     * mode an operator eventually meets if quarantined slots pile up faster than they are
     * cleaned out.
     * <p>
     * No live server is needed: quarantine (and its failure) happens entirely inside
     * {@code build()}, before {@code connect()} is ever attempted.
     */
    @Test(timeout = 30_000L)
    public void testConstructionTimeQuarantineFailsLoudlyWhenAllSlotNamesSaturated() throws Exception {
        assertMemoryLeak(() -> {
            java.nio.file.Path liveSlot = writeMultiSegmentSlotWithCorruptedOldest();
            for (int i = 0; i < 64; i++) {
                java.nio.file.Files.createDirectories(Paths.get(sfDir, "default.unreplayable-" + i));
            }

            String cfg = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";
            Sender s = null;
            try {
                s = Sender.fromConfig(cfg);
                Assert.fail("build() must throw when the unreplayable slot cannot be set aside");
            } catch (LineSenderException expected) {
                Assert.assertTrue("unexpected message: " + expected.getMessage(),
                        expected.getMessage().contains("too many quarantined slots already under")
                                && expected.getMessage().contains("moved or removed by hand"));
            } finally {
                if (s != null) {
                    s.close();
                }
            }
            // The tainted slot's bytes must survive on disk for a manual resend -- the guard
            // fails loudly rather than dropping data, exactly like the connect()-time case.
            // Recovery fails closed BEFORE its deferred quarantine step, so the corrupted
            // segment is still at its original name: a failed recovery never mutates the
            // slot it could not prove safe.
            Assert.assertTrue("the slot dir must be preserved", java.nio.file.Files.exists(liveSlot));
            Assert.assertTrue("the corrupted segment's frame data must be preserved",
                    java.nio.file.Files.exists(liveSlot.resolve("sf-initial.sfa")));
        });
    }

    /**
     * A build()-time quarantine abandons buffered rows, and the synchronous {@link SenderError}
     * dispatch (fired from inside {@code build()} itself -- see {@code Sender.java} around the
     * quarantine block) is the only programmatic channel telling the application that data
     * needs resending: the client ships {@code slf4j-api} with no binding, so {@code LOG.error}
     * alone can vanish into a NOP logger. Pins that a capturing handler actually receives the
     * error, and that it carries the expected category, policy, and a message naming the
     * quarantined location.
     */
    @Test(timeout = 30_000L)
    public void testQuarantineDispatchesSenderErrorToHandler() throws Exception {
        assertMemoryLeak(() -> {
            writeMultiSegmentSlotWithCorruptedOldest();
            AtomicReference<SenderError> received = new AtomicReference<>();
            try (TestWebSocketServer good = new TestWebSocketServer(new MarkerHandler(new AtomicBoolean()))) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                try (Sender s2 = Sender.builder("ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;")
                        .errorHandler(e -> received.compareAndSet(null, e))
                        .build()) {
                    // build() itself quarantined the torn slot; no rows are needed --
                    // the dispatch is synchronous inside build().
                }
            }
            SenderError err = received.get();
            Assert.assertNotNull("a build()-time quarantine abandons buffered rows; the error "
                    + "handler is the ONLY programmatic channel telling the app to resend "
                    + "(slf4j ships unbound, so LOG.error alone can be a NOP)", err);
            Assert.assertEquals(SenderError.Category.DATA_LOSS, err.getCategory());
            Assert.assertEquals(SenderError.Policy.ABANDONED, err.getAppliedPolicy());
            Assert.assertNotNull("getQuarantinedPath() is the programmatic answer to "
                    + "\"where are my bytes\"; message parsing must not be required", err.getQuarantinedPath());
            Assert.assertTrue("quarantined path must name the set-aside dir [path="
                            + err.getQuarantinedPath() + ']',
                    err.getQuarantinedPath().contains("unreplayable-"));
            Assert.assertTrue("the message must still name where the data went [msg="
                            + err.getServerMessage() + ']',
                    err.getServerMessage() != null
                            && err.getServerMessage().contains("set aside at"));
        });
    }

    /**
     * A handler that throws must stay contained: a build()-time quarantine is a bounded,
     * already-handled outage, and a misbehaving handler must not turn it back into a failed
     * {@code build()} or stop the producer from coming up on the fresh slot. Pins both halves --
     * the throw is swallowed AND the quarantine itself still completes -- over the same fixture
     * as {@link #testQuarantineDispatchesSenderErrorToHandler()}.
     */
    @Test(timeout = 30_000L)
    public void testThrowingErrorHandlerDoesNotFailBuild() throws Exception {
        assertMemoryLeak(() -> {
            writeMultiSegmentSlotWithCorruptedOldest();
            AtomicBoolean sawBinary = new AtomicBoolean();
            try (TestWebSocketServer good = new TestWebSocketServer(new MarkerHandler(sawBinary))) {
                int port = good.getPort();
                good.start();
                Assert.assertTrue(good.awaitStart(5, TimeUnit.SECONDS));
                try (Sender s2 = Sender.builder("ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                        + ";close_flush_timeout_millis=0;")
                        .errorHandler(e -> {
                            throw new RuntimeException("handler failure must stay contained");
                        })
                        .build()) {
                    // A throwing handler must not turn the contained outage back into a
                    // failed build: the sender must come up on the fresh slot and work.
                    s2.table("foo").stringColumn("p", "after-throwing-handler").longColumn("v", 1).atNow();
                    s2.flush();
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (System.currentTimeMillis() < deadline && !sawBinary.get()) {
                        Thread.sleep(20);
                    }
                }
                Assert.assertTrue("the producer must keep producing", sawBinary.get());
            }
            Assert.assertTrue("the quarantine itself must still have completed",
                    java.nio.file.Files.isDirectory(Paths.get(sfDir, "default.unreplayable-0")));
        });
    }

    /**
     * Writes a real slot via a silent (never-acking) server so multiple segments survive on
     * disk unacked, then corrupts the oldest one's magic bytes. Returns the live slot path.
     * sf-initial.sfa (baseSeq 0) is guaranteed to be the oldest since nothing ever acked it.
     * <p>
     * {@code sf_max_segment_bytes} is set well below the 20 rows' total encoded size (each
     * row seals to a ~108-byte frame, so 20 rows never fill the default 4096-byte segment) so
     * the append path itself performs a genuine rotation -- {@code SegmentRing.appendOrFsn}
     * only rotates when {@code tryAppend} reports the active segment full. Without this, the
     * setup's old {@code > 1} segment-file precondition could be satisfied by nothing more than
     * the empty hot spare {@code SegmentManager} provisions asynchronously the moment the ring
     * registers ({@code SegmentRing.needsHotSpare} is simply {@code hotSpare == null}, independent
     * of bytes written) -- a background-thread race against this method's {@code Sender} closing,
     * observed to fail consistently on Windows CI and to coin-flip on the JDK 8 Linux leg. Forcing
     * a genuine rotation makes the precondition -- and the corruption scenario it sets up, a skip
     * among data-bearing survivors -- true by construction rather than by timing.
     */
    private java.nio.file.Path writeMultiSegmentSlotWithCorruptedOldest() throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String pad = io.questdb.client.test.tools.TestUtils.repeat("x", 64);
            String cfg = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sf_max_segment_bytes=512"
                    + ";close_flush_timeout_millis=0;";
            try (Sender s1 = Sender.fromConfig(cfg)) {
                for (int i = 0; i < 20; i++) {
                    s1.table("foo").stringColumn("p", pad).longColumn("v", i).atNow();
                    s1.flush();
                }
            }
        }

        java.nio.file.Path liveSlot = Paths.get(sfDir, "default");
        java.nio.file.Path oldest = liveSlot.resolve("sf-initial.sfa");
        Assert.assertTrue("setup: sf-initial.sfa must survive -- nothing acked it",
                java.nio.file.Files.exists(oldest));
        java.util.List<String> segmentFiles = listSegmentFileNames(liveSlot.toString());
        Assert.assertTrue("setup: the slot must hold more than one segment so the corruption "
                        + "below is a skip among survivors, not the only file present",
                segmentFiles.size() > 1);
        Assert.assertTrue("setup: at least one segment besides sf-initial.sfa must carry real "
                        + "frames -- an empty hot spare alone does not construct the intended "
                        + "\"skip among data-bearing survivors\" scenario",
                survivorCarriesFrames(liveSlot.toString(), segmentFiles));
        corruptMagic(oldest.toString());
        return liveSlot;
    }

    /**
     * Overwrites the 4-byte {@code FILE_MAGIC} field at offset 0 so
     * {@link MmapSegment#openExisting} throws at the magic check -- the same technique
     * {@code SegmentRingTest} and {@code PrReviewRedTests} use, landing in
     * {@code SegmentRing}'s per-file skip arm without disturbing any other byte (real frame
     * data included).
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

    private static java.util.List<String> listSegmentFileNames(String dir) {
        java.util.List<String> names = new java.util.ArrayList<>();
        if (!Files.exists(dir)) return names;
        long find = Files.findFirst(dir);
        if (find <= 0) return names;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && name.endsWith(".sfa")) {
                    names.add(name);
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
        return names;
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) rmDirRec(child);
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
     * True if any segment other than {@code sf-initial.sfa} holds at least one real frame.
     * Distinguishes a genuine rotation survivor from the asynchronously provisioned, always-empty
     * hot spare that the segment manager may or may not have installed by the time the producer
     * closes -- the setup precondition must not depend on winning that race.
     */
    private static boolean survivorCarriesFrames(String dir, java.util.List<String> segmentFileNames) {
        for (String name : segmentFileNames) {
            if ("sf-initial.sfa".equals(name)) continue;
            try (MmapSegment seg = MmapSegment.openExisting(dir + "/" + name)) {
                if (seg.frameCount() > 0) return true;
            }
        }
        return false;
    }

    /** Records that at least one binary frame arrived; never acks. */
    private static class MarkerHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicBoolean sawBinary;

        MarkerHandler(AtomicBoolean sawBinary) {
            this.sawBinary = sawBinary;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            sawBinary.set(true);
        }
    }

    /** Receives binary frames but never acks. Sender drops them on close. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally empty
        }
    }
}
