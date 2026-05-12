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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CursorSendEngineTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-eng-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testAppendBlockingNeverFailsUnderManagerSupply() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
                for (int i = 0; i < 200; i++) {
                    Unsafe.getUnsafe().putInt(buf, i);
                    long fsn = engine.appendBlocking(buf, 64);
                    assertEquals(i, fsn);
                }
                assertEquals(199, engine.publishedFsn());
                assertNotNull("active segment is always non-null", engine.activeSegment());
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAppendOrFsnReturnsBackpressureWhenSpareUnavailable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Run with a deliberately stalled manager: poll cadence so slow
            // it never installs a spare in the test window. The first segment
            // fills, then appendOrFsn returns BACKPRESSURE_NO_SPARE.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                // Fill the active deterministically (this is the initial segment;
                // manager hasn't had a chance to provision a spare yet on a fast box,
                // so we use a short spin deadline so the test runs quickly).
                long deadline = System.nanoTime();
                engine.appendOrFsn(buf, 64, deadline);
                engine.appendOrFsn(buf, 64, deadline);
                // Third append: active is full, spare may or may not be ready
                // depending on race with manager. With a zero-deadline spin we
                // get either the FSN (if manager beat us) or backpressure.
                long fsn = engine.appendOrFsn(buf, 64, deadline);
                assertTrue("unexpected fsn=" + fsn, fsn == 2L || fsn == -1L);
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAcknowledgePropagatesToRing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
                engine.appendBlocking(buf, 16);
                engine.appendBlocking(buf, 16);
                engine.appendBlocking(buf, 16);
                engine.acknowledge(2L);
                assertEquals(2L, engine.ackedFsn());
                // Regression — should be ignored.
                engine.acknowledge(0L);
                assertEquals(2L, engine.ackedFsn());
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096);
            engine.close();
            engine.close();
        });
    }

    @Test
    public void testAppendBlockingThrowsOnDeadlineExpiryUnderCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Cap counts every segment the ring owns (initial active + sealed +
            // hot spare), including bytes already on disk at register-time. With
            // cap = 3*segSize and segSize fitting 2 frames, the producer can land
            // initial (2) + spare1 (2) + spare2 (2) = 6 frames. The 7th rotation
            // needs a spare3 that the cap forbids → backpressure → deadline.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long cap = 3 * segSize;
            long shortDeadlineNanos = 200_000_000L; // 200 ms
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize, cap, shortDeadlineNanos)) {
                for (int i = 0; i < 6; i++) {
                    long fsn = engine.appendBlocking(buf, 64);
                    assertEquals(i, fsn);
                }
                // Next append must wait for a third spare that the cap won't allow.
                long t0 = System.nanoTime();
                try {
                    engine.appendBlocking(buf, 64);
                    fail("expected backpressure deadline exception");
                } catch (LineSenderException expected) {
                    long elapsed = System.nanoTime() - t0;
                    assertTrue("threw too early: " + elapsed + "ns",
                            elapsed >= shortDeadlineNanos);
                    assertTrue("message must mention backpressure: " + expected.getMessage(),
                            expected.getMessage().contains("backpressured"));
                }
                // Counter must record the stall.
                assertTrue("stall counter must increment: " + engine.getTotalBackpressureStalls(),
                        engine.getTotalBackpressureStalls() >= 1);
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRestartIntoNonEmptySfDirContinuesFsnSequence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Red regression: restart against a populated SF dir must derive the
            // new active's baseSeq from the highest sealed segment on disk, not
            // hardcode 0. Previously CursorSendEngine always created a fresh
            // sf-initial.sfa at baseSeq=0, so the second session's FSNs collided
            // with frames the first session had already durably persisted.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            int totalFrames = 5;
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                // Session 1: write totalFrames, leaving the dir populated with
                // sealed segments + a (partially-filled) active at the end.
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    for (int i = 0; i < totalFrames; i++) {
                        long fsn = engine.appendBlocking(buf, 64);
                        assertEquals(i, fsn);
                    }
                    assertEquals(totalFrames - 1, engine.publishedFsn());
                }
                // Confirm the dir really has *.sfa files left over — otherwise
                // the test would pass for the wrong reason (empty dir == no bug).
                long find = Files.findFirst(tmpDir);
                assertTrue("findFirst() must succeed on populated tmpDir", find > 0);
                int sfaCount = 0;
                try {
                    int rc = 1;
                    while (rc > 0) {
                        String name = Files.utf8ToString(Files.findName(find));
                        if (name != null && name.endsWith(".sfa")) sfaCount++;
                        rc = Files.findNext(find);
                    }
                } finally {
                    Files.findClose(find);
                }
                assertTrue("session 1 must leave .sfa files behind: count=" + sfaCount,
                        sfaCount >= 1);

                // Session 2: open the same dir. The next FSN must continue from
                // where session 1 left off, NOT restart at 0. Today this assertion
                // fails because the engine constructs a fresh ring at baseSeq=0
                // and ignores the on-disk segments.
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    long fsn = engine.appendBlocking(buf, 64);
                    assertEquals("FSN must continue, not restart — overlapping "
                                    + "FSNs would corrupt ACK translation, trim, and replay",
                            totalFrames, fsn);
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMemoryModeSkipsDirAndStillWorks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // sfDir == null → memory-only ring. No files, no mkdir, no path.
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(null, 4096)) {
                assertNull(engine.sfDir());
                for (int i = 0; i < 16; i++) {
                    long fsn = engine.appendBlocking(buf, 32);
                    assertEquals(i, fsn);
                }
                // Active segment must be a memory-backed MmapSegment (path == null).
                assertNull(engine.activeSegment().path());
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSealedSegmentsAccumulateAfterRotation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // segSize fits exactly 2 frames -> the 3rd append seals the active
            // segment. Writing 5 frames forces two rotations, leaving 2 sealed
            // segments and a (partially-filled) active.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                for (int i = 0; i < 5; i++) {
                    assertEquals(i, engine.appendBlocking(buf, 64));
                }
                ObjList<MmapSegment> sealed = engine.sealedSegments();
                assertEquals("two rotations should produce two sealed segments",
                        2, sealed.size());
                // Sealed list is oldest-first: baseSeq must be strictly increasing
                // and below the active's baseSeq.
                assertEquals(0L, sealed.get(0).baseSeq());
                assertEquals(2L, sealed.get(1).baseSeq());
                assertEquals(4L, engine.activeSegment().baseSeq());
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSealedSegmentsEmptyOnFreshEngine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
                assertEquals("fresh engine has no sealed segments",
                        0, engine.sealedSegments().size());
            }
        });
    }

    @Test
    public void testSealedSegmentsSnapshotCopiesReferences() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                for (int i = 0; i < 5; i++) {
                    engine.appendBlocking(buf, 64);
                }
                MmapSegment[] target = new MmapSegment[4];
                int copied = engine.sealedSegmentsSnapshot(target);
                assertEquals(2, copied);
                // Oldest-first packing into target[0..copied) — the rest is left
                // untouched by the snapshot contract.
                assertEquals(0L, target[0].baseSeq());
                assertEquals(2L, target[1].baseSeq());
                assertNull("untouched slot must remain null", target[2]);
                assertNull("untouched slot must remain null", target[3]);
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSealedSegmentsSnapshotEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
                MmapSegment[] target = new MmapSegment[2];
                assertEquals("no sealed segments to copy",
                        0, engine.sealedSegmentsSnapshot(target));
                assertNull(target[0]);
                assertNull(target[1]);
            }
        });
    }

    @Test
    public void testSealedSegmentsSnapshotReturnsMinusOneOnUndersize() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Same setup as testSealedSegmentsAccumulateAfterRotation: 2 sealed.
            // Snapshot into a 1-slot buffer triggers the "grow and retry" signal.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                for (int i = 0; i < 5; i++) {
                    engine.appendBlocking(buf, 64);
                }
                MmapSegment[] tooSmall = new MmapSegment[1];
                assertEquals("undersize buffer must signal grow-and-retry",
                        -1, engine.sealedSegmentsSnapshot(tooSmall));
                // The contract says the buffer is filled to capacity even on the
                // -1 sentinel, so the caller can still observe the oldest entry.
                assertNotNull(tooSmall[0]);
                assertEquals(0L, tooSmall[0].baseSeq());
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSegmentSizeBytesReturnsConfiguredValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long configured = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, configured)) {
                assertEquals(configured, engine.segmentSizeBytes());
            }
            // Memory mode reads back the configured size identically — sfDir is
            // orthogonal to the segment-size field.
            try (CursorSendEngine engine = new CursorSendEngine(null, 8192L)) {
                assertEquals(8192L, engine.segmentSizeBytes());
            }
        });
    }

    @Test
    public void testWasRecoveredFromDiskFalseInMemoryMode() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(null, 4096)) {
                assertFalse("memory mode never recovers from disk",
                        engine.wasRecoveredFromDisk());
            }
        });
    }

    @Test
    public void testWasRecoveredFromDiskFalseOnFreshDir() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
                assertFalse("empty sfDir is not a recovery",
                        engine.wasRecoveredFromDisk());
            }
        });
    }

    @Test
    public void testWasRecoveredFromDiskTrueOnReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Session 1 leaves at least one .sfa behind — see the existing
            // testRestartIntoNonEmptySfDirContinuesFsnSequence for the file-
            // residue assertion. We rely on it here without re-asserting.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    for (int i = 0; i < 3; i++) {
                        engine.appendBlocking(buf, 64);
                    }
                    assertFalse("session 1 starts fresh", engine.wasRecoveredFromDisk());
                }
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    assertTrue("session 2 must observe disk recovery",
                            engine.wasRecoveredFromDisk());
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRecoveryAdvancesAckedFsnPastWatermark() throws Exception {
        // Pin the spec invariant: when a valid .ack-watermark sits above
        // the segment-derived seed but at or below publishedFsn,
        // recovery picks the watermark (advancing the cursor past the
        // already-durable-acked prefix of the lowest surviving segment).
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                // Session 1: append four frames so publishedFsn = 3.
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    for (int i = 0; i < 4; i++) {
                        engine.appendBlocking(buf, 64);
                    }
                }
                // Forge a watermark of 2 -- as if the previous session
                // had received durable acks up through FSN 2 before
                // dying. lowestBase is 0 so the bare baseSeed is -1;
                // recovery must pick max(-1, 2) == 2.
                try (AckWatermark w = AckWatermark.open(tmpDir)) {
                    assertNotNull(w);
                    w.write(2L);
                }
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    assertEquals("watermark must advance recovered ackedFsn",
                            2L, engine.ackedFsn());
                    assertEquals("publishedFsn must reflect all four recovered frames",
                            3L, engine.publishedFsn());
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRecoveryIgnoresWatermarkAbovePublishedFsn() throws Exception {
        // Pin the corruption defence: a watermark higher than what the
        // on-disk segments could account for must be rejected so
        // recovery doesn't seed ackedFsn past publishedFsn (which would
        // silently drop every un-acked frame on the next replay).
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                // Session 1: publishedFsn = 3 (four frames, FSNs 0..3).
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    for (int i = 0; i < 4; i++) {
                        engine.appendBlocking(buf, 64);
                    }
                }
                // Corrupt-high watermark -- 100 is far above the 0..3
                // range the on-disk segments could justify.
                try (AckWatermark w = AckWatermark.open(tmpDir)) {
                    assertNotNull(w);
                    w.write(100L);
                }
                try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
                    // baseSeed = lowestBase - 1 = 0 - 1 = -1. Recovery
                    // must fall back to baseSeed because the watermark
                    // is past publishedFsn=3.
                    assertEquals("corrupt-high watermark must fall back to lowestBase - 1",
                            -1L, engine.ackedFsn());
                    assertEquals(3L, engine.publishedFsn());
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
