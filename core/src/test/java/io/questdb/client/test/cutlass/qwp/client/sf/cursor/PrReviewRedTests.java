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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

/**
 * Red tests for the critical findings raised during the PR-17 code review.
 * Each {@code @Test} here is intentionally written to FAIL on current
 * {@code vi_sf} HEAD; once the corresponding finding is fixed, the test
 * should pass. See the inline javadoc on each test for the matching
 * finding identifier.
 */
public class PrReviewRedTests {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-pr-red-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
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

    /**
     * Finding C1 / C10 — first-frame CRC corruption silently deletes the segment.
     * <p>
     * If frame[0] of a recovered .sfa fails CRC validation, scanFrames returns
     * lastGood=HEADER_SIZE, countFrames returns 0, and SegmentRing.openExisting
     * unlinks the file as an "empty hot-spare leftover" — destroying every frame
     * that physically followed the corrupt header. The torn-tail WARN inside
     * MmapSegment.openExisting is dropped on the floor.
     * <p>
     * Trigger: a single bit flip on the CRC field of frame[0] (bit rot, partial
     * page write at crash, etc.).
     */
    @Test
    public void testC1_recoveryMustNotUnlinkSegmentWithCorruptFirstFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String segPath = tmpDir + "/sf-data.sfa";
            // Build a segment with several real frames so we have something to lose.
            MmapSegment seg = MmapSegment.create(segPath, 0L, 64 * 1024);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 32; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }
                Assert.assertTrue("setup: first append must succeed", seg.tryAppend(buf, 32) >= 0);
                Assert.assertTrue("setup: second append must succeed", seg.tryAppend(buf, 32) >= 0);
                Assert.assertTrue("setup: third append must succeed", seg.tryAppend(buf, 32) >= 0);
                Assert.assertEquals("setup: three frames written", 3L, seg.frameCount());
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
            seg.close();
            Assert.assertTrue("setup: file must exist on disk", Files.exists(segPath));

            // Corrupt the CRC field of frame[0] (offset HEADER_SIZE..HEADER_SIZE+4).
            // A single bit flip is enough; we overwrite the whole 4-byte field with
            // a value statistically guaranteed to mismatch any real CRC.
            int fd = Files.openRW(segPath);
            Assert.assertTrue("setup: openRW failed", fd >= 0);
            long badCrcBuf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putInt(badCrcBuf, 0xDEADBEEF);
                Files.write(fd, badCrcBuf, 4, MmapSegment.HEADER_SIZE);
            } finally {
                Unsafe.free(badCrcBuf, 4, MemoryTag.NATIVE_DEFAULT);
                Files.close(fd);
            }
            Assert.assertTrue("setup: file should still exist after CRC clobber",
                    Files.exists(segPath));

            // Run recovery.
            SegmentRing recovered = SegmentRing.openExisting(tmpDir, 64 * 1024);
            try {
                // The bug: openExisting sees frameCount=0 (because scanFrames
                // bailed at the corrupt frame[0]) and treats the segment as
                // an "empty hot-spare leftover" — closing AND UNLINKING the
                // file. The user's frames 1, 2, 3 are gone forever; the only
                // record was a WARN log line that's already been emitted.
                //
                // Spec / desired behavior: a segment with non-zero contents
                // past the header (tornTailBytes > 0) must be preserved or
                // quarantined to <path>.corrupt for postmortem. Silent unlink
                // is the data-loss bug the spec calls out.
                // Spec: a segment with non-zero contents past the header
                // (tornTailBytes > 0) must be preserved at its original path
                // OR quarantined to <path>.corrupt so a postmortem can
                // recover the surviving frames.
                boolean preserved = Files.exists(segPath) || Files.exists(segPath + ".corrupt");
                Assert.assertTrue(
                        "FINDING C1: SegmentRing.openExisting silently unlinked a segment "
                                + "whose first frame failed CRC. Three valid frames followed the "
                                + "corrupt header; recovery destroyed all of them with only a "
                                + "WARN log. Mission-critical data loss path.",
                        preserved);
            } finally {
                if (recovered != null) {
                    recovered.close();
                }
            }
        });
    }

    /**
     * Finding C2 (engine-level) — {@link SegmentRing#acknowledge(long)} accepts
     * an arbitrarily large {@code seq} and unconditionally advances
     * {@code ackedFsn}, even past {@code publishedFsn}.
     * <p>
     * Combined with the unclamped DROP path in
     * {@code CursorWebSocketSendLoop.handleServerRejection}, a malformed/poisoned
     * server NACK with a bogus {@code wireSeq} can move {@code ackedFsn} far
     * beyond what the I/O thread has actually sent. The segment manager then
     * trims segments that the I/O thread is still iterating; the next
     * {@code Unsafe.getInt} on the unmapped region SEGVs the JVM.
     * <p>
     * Defense-in-depth fix: clamp inside {@code acknowledge} —
     * {@code if (seq > publishedFsn) seq = publishedFsn;}
     */
    @Test
    public void testC2_acknowledgeMustClampAtPublishedFsn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            MmapSegment seg = MmapSegment.create(tmpDir + "/c2.sfa", 0L, 64 * 1024);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 32; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }
                try (SegmentRing ring = new SegmentRing(seg, 64 * 1024)) {
                    Assert.assertEquals("setup: first append yields FSN 0", 0L,
                            ring.appendOrFsn(buf, 32));
                    Assert.assertEquals("setup: publishedFsn matches", 0L,
                            ring.publishedFsn());
                    Assert.assertEquals("setup: nothing acked yet", -1L,
                            ring.ackedFsn());

                    // Hostile input: a server bug, fuzzer, or version-skew
                    // could send a NACK / ACK with any wireSeq. The DROP-policy
                    // path (CursorWebSocketSendLoop.handleServerRejection) does
                    // not clamp — so this maps to engine.acknowledge(huge) under
                    // a real adversarial server.
                    long bogusSeq = Long.MAX_VALUE / 2L;
                    ring.acknowledge(bogusSeq);

                    // Defense-in-depth invariant: ackedFsn MUST NEVER exceed
                    // publishedFsn. The segment manager's drainTrimmable uses
                    // ackedFsn to decide which segments to munmap+unlink. If
                    // ackedFsn races past publishedFsn, the manager can trim
                    // a segment the I/O thread is currently iterating —
                    // SEGV in the JVM.
                    Assert.assertTrue(
                            "FINDING C2: SegmentRing.acknowledge accepted "
                                    + bogusSeq + " against publishedFsn=" + ring.publishedFsn()
                                    + ". ackedFsn is now " + ring.ackedFsn()
                                    + " — far past anything the I/O thread has actually sent. "
                                    + "The segment manager will trim segments the I/O thread is "
                                    + "still reading; next Unsafe.getInt on the unmapped region "
                                    + "SEGVs the JVM. acknowledge must clamp at publishedFsn.",
                            ring.ackedFsn() <= ring.publishedFsn());
                }
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Finding C7 — {@code QWP_CLIENT_REVIEW.md} at the repo root is review notes
     * for a different branch ({@code vi_egress}, not {@code vi_sf}) and was
     * accidentally committed in this PR.
     */
    @Test
    public void testC7_strayBranchReviewMarkdownAbsent() {
        // The test runs from the repo root or a subdirectory (typically `core/`).
        // Walk up looking for `.git`, which only exists at the project root —
        // stopping at the first `pom.xml` would land at the `core/` module.
        java.io.File cwd = new java.io.File(".").getAbsoluteFile();
        java.io.File root = cwd;
        while (root != null && !new java.io.File(root, ".git").exists()) {
            root = root.getParentFile();
        }
        Assert.assertNotNull("could not locate repo root from " + cwd, root);
        java.io.File stray = new java.io.File(root, "QWP_CLIENT_REVIEW.md");
        Assert.assertFalse(
                "FINDING C7: " + stray.getAbsolutePath() + " is review notes for branch "
                        + "vi_egress (not vi_sf) and was accidentally committed in PR #17. "
                        + "Run `git rm QWP_CLIENT_REVIEW.md`.",
                stray.exists());
    }
}
