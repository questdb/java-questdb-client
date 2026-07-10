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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Os;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * Regression pins for the fail-closed edge-position recovery contract.
 * Historically {@link SegmentRing#openExisting} silently skipped every
 * {@link MmapSegmentException} and treated directory-enumeration failure as
 * an empty store. The interior FSN-contiguity check only exposes a skipped
 * segment when it sits BETWEEN two survivors — a sole, lowest, or highest
 * unreadable segment escaped it:
 * <ul>
 *   <li><b>Sole</b> — recovery returns {@code null}; {@link CursorSendEngine}
 *       then creates a fresh {@code sf-initial.sfa} via truncating
 *       {@code openCleanRW} ({@code O_CREAT|O_TRUNC} / {@code CREATE_ALWAYS}),
 *       destroying the only surviving SF file.</li>
 *   <li><b>Lowest</b> — survivors are still mutually contiguous; the engine
 *       seeds {@code ackedFsn = newLowestBase - 1}, permanently classifying
 *       the lost segment's unacked frames as acked. Silent data loss.</li>
 *   <li><b>Highest</b> — the second-highest segment becomes active and
 *       {@code nextSeqHint()} continues from it, so newly appended frames
 *       duplicate FSNs still durable in the skipped file.</li>
 *   <li><b>Enumeration failure</b> — {@code findFirst < 0} returns
 *       {@code null} exactly like an empty directory, so a single transient
 *       readdir failure sends the engine down the truncating fresh-start
 *       path against fully VALID data.</li>
 * </ul>
 * Recovery now distinguishes "no segment files" from "segment files existed
 * but could not be recovered" and fails closed on unreadable {@code .sfa}
 * files and enumeration failures; these tests pin that contract.
 * <p>
 * Corruption method: the tests clobber the header VERSION byte (offset 4)
 * while leaving the magic intact. This is unambiguously a recognizable SF
 * segment that fails recovery — not the stray-foreign-file case the per-file
 * skip was designed for.
 */
public class SegmentRingEdgeRecoveryTest {

    private static final int FRAME_PAYLOAD_LEN = 32;
    private static final long SEGMENT_BYTES = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-ring-edge-red-");
    }

    @After
    public void tearDown() {
        if (tmpDir == null) {
            return;
        }
        // The enumeration-failure test drops read permission on the dir;
        // restore it so removeTmpDir can enumerate. No-op on Windows.
        try {
            java.nio.file.Files.setPosixFilePermissions(
                    Paths.get(tmpDir), PosixFilePermissions.fromString("rwx------"));
        } catch (Exception ignored) {
        }
        TestUtils.removeTmpDir(tmpDir);
    }

    /**
     * Enumeration failure: {@code Files.findFirst} fails (here: EACCES via a
     * write+exec-only directory), and openExisting returns {@code null} —
     * indistinguishable from an empty store. The engine's fresh-start path
     * would then truncate a fully VALID {@code sf-initial.sfa} and delete the
     * prior ack watermark. A transient readdir failure must be an error, not
     * "no data".
     */
    @Test
    public void testEnumerationFailureMustNotBeTreatedAsEmptyStore() throws Exception {
        Assume.assumeTrue("POSIX-only: relies on chmod to make readdir fail",
                Os.type != Os.WINDOWS);
        Assume.assumeFalse("root bypasses permission checks",
                "root".equals(System.getProperty("user.name")));
        TestUtils.assertMemoryLeak(() -> {
            // A fully VALID segment with real frames -- nothing about this
            // file is corrupt.
            writeFrames(tmpDir + "/sf-initial.sfa", 0L, 3);
            // Write+exec, no read: opendir/readdir fails with EACCES while
            // file creation and open-by-path inside the dir still work --
            // exactly the state in which the engine's fresh-start path
            // would truncate the valid segment.
            java.nio.file.Files.setPosixFilePermissions(
                    Paths.get(tmpDir), PosixFilePermissions.fromString("-wx------"));
            try {
                SegmentRing ring;
                try {
                    ring = SegmentRing.openExisting(tmpDir, SEGMENT_BYTES);
                } catch (MmapSegmentException expected) {
                    // Fixed behavior: enumeration failure surfaces as an
                    // error the caller must handle -- never as "empty".
                    Assert.assertTrue(expected.getMessage(),
                            expected.getMessage().contains("could not enumerate"));
                    return;
                }
                if (ring != null) {
                    ring.close();
                    Assert.fail("openExisting returned a ring despite readdir failing -- "
                            + "unexpected; revisit this test's permission setup");
                }
                Assert.fail("FINDING: openExisting treated a directory-enumeration failure "
                        + "as an empty store (returned null). CursorSendEngine's fresh-start "
                        + "path would now truncate the fully VALID sf-initial.sfa via "
                        + "openCleanRW and delete the ack watermark -- destroying readable "
                        + "data after one transient readdir failure.");
            } finally {
                java.nio.file.Files.setPosixFilePermissions(
                        Paths.get(tmpDir), PosixFilePermissions.fromString("rwx------"));
            }
        });
    }

    /**
     * Engine level, the destructive case: the slot's only segment is
     * {@code sf-initial.sfa} (a low-volume sender that never rotated) whose
     * header was torn by a crash. Recovery skips it, returns {@code null},
     * and the engine's fresh-start path re-creates {@code sf-initial.sfa}
     * with truncating {@code openCleanRW} -- zeroing the only surviving SF
     * file. The frames that physically followed the torn header are
     * destroyed with no quarantine and no error.
     */
    @Test
    public void testFreshStartMustNotTruncateSoleUnreadableInitialSegment() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String initialPath = tmpDir + "/sf-initial.sfa";
            writeFrames(initialPath, 0L, 3);
            clobberVersionByte(initialPath);

            // First payload byte of frame[0]; writeFrames fills payloads with
            // non-zero bytes, so a zero here means the file was re-created.
            long probeOffset = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE;
            Assert.assertTrue("setup: probe byte must be non-zero",
                    readByteAt(initialPath, probeOffset) != 0);

            CursorSendEngine engine = null;
            boolean failedClosed = false;
            try {
                engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES);
            } catch (Throwable expected) {
                // Fixed behavior: recognizable-but-unreadable sole segment
                // must fail engine construction (or be quarantined below).
                failedClosed = true;
            }
            try {
                if (!failedClosed) {
                    // Probe BEFORE close(): a fully-drained engine unlinks
                    // residual .sfa files on close, which would mask the
                    // truncation as a deletion.
                    boolean quarantined = Files.exists(initialPath + ".corrupt");
                    boolean preserved = Files.exists(initialPath)
                            && readByteAt(initialPath, probeOffset) != 0;
                    Assert.assertTrue(
                            "FINDING: CursorSendEngine silently TRUNCATED the sole surviving "
                                    + "sf-initial.sfa. Recovery skipped it (torn header -> "
                                    + "MmapSegmentException -> logged-and-ignored), returned null, "
                                    + "and the fresh-start path re-created the same path via "
                                    + "openCleanRW (O_CREAT|O_TRUNC). Every frame that survived "
                                    + "the crash is destroyed; postmortem recovery is impossible. "
                                    + "Expected: fail construction, or quarantine to "
                                    + "sf-initial.sfa.corrupt before creating a fresh file.",
                            quarantined || preserved);
                }
            } finally {
                if (engine != null) {
                    engine.close();
                }
            }
        });
    }

    /**
     * Highest segment unreadable: the second-highest survivor becomes the
     * active segment and {@code nextSeqHint()} resumes inside the FSN range
     * still durable in the skipped file -- future appends mint duplicate
     * FSNs, and the skipped file poisons the next recovery's contiguity
     * check. Must fail closed instead.
     */
    @Test
    public void testHighestUnreadableSegmentMustFailRecoveryClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeFrames(tmpDir + "/sf-0000000000000000.sfa", 0L, 2); // FSN 0..1
            String highPath = tmpDir + "/sf-0000000000000001.sfa";
            writeFrames(highPath, 2L, 2);                            // FSN 2..3
            clobberVersionByte(highPath);

            SegmentRing ring;
            try {
                ring = SegmentRing.openExisting(tmpDir, SEGMENT_BYTES);
            } catch (MmapSegmentException expected) {
                // Fixed behavior: fail closed on the unreadable file itself
                // (not e.g. an incidental FSN-gap throw).
                Assert.assertTrue(expected.getMessage(),
                        expected.getMessage().contains("unreadable segment file"));
                return;
            }
            try {
                long nextFsn = ring != null ? ring.nextSeqHint() : -1L;
                Assert.fail("FINDING: openExisting silently skipped the HIGHEST segment "
                        + "(unsupported version -> MmapSegmentException -> logged-and-ignored). "
                        + "The survivors are mutually contiguous so the gap check cannot fire. "
                        + "The ring now resumes at FSN " + nextFsn + ", duplicating FSNs 2..3 "
                        + "that are still durable in the skipped file -- duplicate frames now, "
                        + "poisoned contiguity check on the next recovery. Expected: "
                        + "MmapSegmentException.");
            } finally {
                if (ring != null) {
                    ring.close();
                }
            }
        });
    }

    /**
     * Lowest segment unreadable: the survivors are mutually contiguous, so
     * the interior gap check cannot fire and recovery succeeds without the
     * lowest segment's frames. Worse, {@link CursorSendEngine} then seeds
     * {@code ackedFsn = newLowestBase - 1}, permanently classifying the lost
     * unacked frames as acked -- they are never replayed. Must fail closed
     * instead.
     */
    @Test
    public void testLowestUnreadableSegmentMustFailRecoveryClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String lowPath = tmpDir + "/sf-0000000000000000.sfa";
            writeFrames(lowPath, 0L, 2);                             // FSN 0..1
            writeFrames(tmpDir + "/sf-0000000000000001.sfa", 2L, 2); // FSN 2..3
            clobberVersionByte(lowPath);

            SegmentRing ring;
            try {
                ring = SegmentRing.openExisting(tmpDir, SEGMENT_BYTES);
            } catch (MmapSegmentException expected) {
                // Fixed behavior: fail closed on the unreadable file itself
                // (not e.g. an incidental FSN-gap throw).
                Assert.assertTrue(expected.getMessage(),
                        expected.getMessage().contains("unreadable segment file"));
                return;
            }
            try {
                boolean lowFramesRecovered = ring != null
                        && ring.findSegmentContaining(0L) != null;
                Assert.assertTrue(
                        "FINDING: openExisting silently skipped the LOWEST segment "
                                + "(unsupported version -> MmapSegmentException -> "
                                + "logged-and-ignored). The survivors are mutually contiguous so "
                                + "the gap check cannot fire; FSNs 0..1 are gone and "
                                + "CursorSendEngine would seed ackedFsn = 1, marking the lost "
                                + "unacked frames as acked forever. Silent data loss. "
                                + "Expected: MmapSegmentException.",
                        lowFramesRecovered);
            } finally {
                if (ring != null) {
                    ring.close();
                }
            }
        });
    }

    /**
     * Sole segment unreadable, ring level: recovery must not report "no
     * data" ({@code null}) when a recognizable SF segment with frames exists
     * but cannot be opened -- {@code null} is precisely what routes the
     * caller onto the truncating fresh-start path.
     */
    @Test
    public void testSoleUnreadableSegmentMustFailRecoveryClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String segPath = tmpDir + "/sf-0000000000000000.sfa";
            writeFrames(segPath, 0L, 3);
            clobberVersionByte(segPath);

            SegmentRing ring;
            try {
                ring = SegmentRing.openExisting(tmpDir, SEGMENT_BYTES);
            } catch (MmapSegmentException expected) {
                // Fixed behavior: fail closed on the unreadable file itself
                // (not e.g. an incidental FSN-gap throw).
                Assert.assertTrue(expected.getMessage(),
                        expected.getMessage().contains("unreadable segment file"));
                return;
            }
            if (ring != null) {
                ring.close();
                Assert.fail("openExisting returned a ring from a sole unreadable segment -- "
                        + "unexpected; revisit this test's corruption setup");
            }
            Assert.fail("FINDING: openExisting returned null for a slot whose ONLY segment "
                    + "is a recognizable SF file with 3 frames that failed recovery "
                    + "(unsupported version). The caller cannot distinguish this from an "
                    + "empty store and will restart at FSN 0, truncating sf-initial.sfa "
                    + "if that is the surviving file's name. Expected: MmapSegmentException.");
        });
    }

    /**
     * Clobbers the header VERSION byte (offset 4) while leaving the magic
     * intact: unambiguously a recognizable SF segment that fails recovery
     * with {@link MmapSegmentException} ("unsupported version"), not a stray
     * foreign file.
     */
    private static void clobberVersionByte(String path) {
        int fd = Files.openRW(path);
        Assert.assertTrue("openRW failed for " + path, fd >= 0);
        long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(buf, (byte) 0x63);
            Assert.assertEquals("version-byte clobber failed", 1L, Files.write(fd, buf, 1, 4));
        } finally {
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            Files.close(fd);
        }
    }

    private static byte readByteAt(String path, long offset) {
        int fd = Files.openRW(path);
        Assert.assertTrue("openRW failed for " + path, fd >= 0);
        long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals("probe read failed", 1L, Files.read(fd, buf, 1, offset));
            return Unsafe.getUnsafe().getByte(buf);
        } finally {
            Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            Files.close(fd);
        }
    }

    /**
     * Creates a segment at {@code path} and appends {@code frameCount}
     * frames whose payload bytes are all non-zero, so tests can tell
     * surviving data from a zero-filled re-created file.
     */
    private static void writeFrames(String path, long baseSeq, int frameCount) {
        long buf = Unsafe.malloc(FRAME_PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < FRAME_PAYLOAD_LEN; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i | 1));
            }
            try (MmapSegment seg = MmapSegment.create(path, baseSeq, SEGMENT_BYTES)) {
                for (int i = 0; i < frameCount; i++) {
                    Assert.assertTrue("setup: append must fit",
                            seg.tryAppend(buf, FRAME_PAYLOAD_LEN) >= 0);
                }
            }
        } finally {
            Unsafe.free(buf, FRAME_PAYLOAD_LEN, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
