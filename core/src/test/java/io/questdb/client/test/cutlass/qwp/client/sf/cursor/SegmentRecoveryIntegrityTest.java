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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfManifest;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Regression tests for the SF recovery fail-open findings: directory
 * enumeration errors, per-file open/read/mmap errors and boundary (leading /
 * trailing) segment loss must fail startup without mutating the slot, while
 * positively-identified corruption is quarantined only after the surviving
 * chain validates. Also pins the crash-window states of the manifest
 * protocol (fresh start, rotation, clean-drain) as recoverable.
 */
public class SegmentRecoveryIntegrityTest {
    private static final String MANIFEST_NAME = "sf-manifest.bin";
    private static final long SEGMENT_SIZE = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-sf-integrity-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    // ------------------------------------------------------------------
    // Operational failures must fail closed without touching a byte.
    // ------------------------------------------------------------------

    @Test
    public void testFindFirstFailureFailsRecoveryAndPreservesBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000001.sfa", 2, 3);
            Map<String, byte[]> before = snapshotDir();

            FilesFacade facade = new DelegatingFacade() {
                @Override
                public long findFirst(String dir) {
                    return tmpDir.equals(dir) ? -1L : super.findFirst(dir);
                }
            };
            try {
                SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE).close();
                Assert.fail("recovery must fail when the SF directory cannot be enumerated");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "could not enumerate");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testPartialFindNextFailsRecoveryAndPreservesBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000001.sfa", 2, 3);
            Map<String, byte[]> before = snapshotDir();

            FilesFacade facade = new DelegatingFacade() {
                private int calls;

                @Override
                public int findNext(long findPtr) {
                    // First advance succeeds, then the listing "fails" the way
                    // a readdir I/O error does. Recovery must treat the
                    // partial listing as fatal, not as end-of-directory.
                    return ++calls >= 2 ? -1 : super.findNext(findPtr);
                }
            };
            try {
                SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE).close();
                Assert.fail("recovery must fail when the directory listing is incomplete");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "could not fully enumerate");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testOpenRWFailureOnValidSegmentFailsRecoveryAndPreservesBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            String victim = tmpDir + "/sf-0000000000000001.sfa";
            writeSegmentWithFrames(victim, 2, 3);
            Map<String, byte[]> before = snapshotDir();

            FilesFacade facade = new DelegatingFacade() {
                @Override
                public int openRW(String path) {
                    // EMFILE/EACCES-style failure on a perfectly valid file.
                    return victim.equals(path) ? -1 : super.openRW(path);
                }
            };
            try {
                SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE).close();
                Assert.fail("recovery must fail when a valid segment cannot be opened");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "recovery failed for recognized segment");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testMmapFailureOnValidSegmentFailsRecoveryAndPreservesBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            String victim = tmpDir + "/sf-0000000000000001.sfa";
            writeSegmentWithFrames(victim, 2, 3);
            Map<String, byte[]> before = snapshotDir();

            FilesFacade facade = new DelegatingFacade() {
                private int victimFd = Integer.MIN_VALUE;

                @Override
                public int openRW(String path) {
                    int fd = super.openRW(path);
                    if (victim.equals(path)) {
                        victimFd = fd;
                    }
                    return fd;
                }

                @Override
                public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
                    if (fd == victimFd) {
                        return Files.FAILED_MMAP_ADDRESS;
                    }
                    return super.mmap(fd, len, offset, flags, memoryTag);
                }
            };
            try {
                SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE).close();
                Assert.fail("recovery must fail when a valid segment cannot be mapped");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "recovery failed for recognized segment");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testReadFailureOnValidSegmentFailsRecoveryAndPreservesBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            String victim = tmpDir + "/sf-0000000000000001.sfa";
            writeSegmentWithFrames(victim, 2, 3);
            Map<String, byte[]> before = snapshotDir();

            FilesFacade facade = new DelegatingFacade() {
                private int victimFd = Integer.MIN_VALUE;

                @Override
                public int openRW(String path) {
                    int fd = super.openRW(path);
                    if (victim.equals(path)) {
                        victimFd = fd;
                    }
                    return fd;
                }

                @Override
                public long read(int fd, long addr, long len, long offset) {
                    return fd == victimFd ? -1L : super.read(fd, addr, len, offset);
                }
            };
            try {
                SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE).close();
                Assert.fail("recovery must fail when a valid segment cannot be read");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "recovery failed for recognized segment");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testUnsupportedVersionFailsRecoveryWithoutQuarantine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            String foreign = tmpDir + "/sf-0000000000000001.sfa";
            writeRawSegmentHeader(foreign, MmapSegment.FILE_MAGIC, (byte) 99, 2L);
            Map<String, byte[]> before = snapshotDir();

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE).close();
                Assert.fail("a segment written by a different client version must fail recovery");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "recovery failed for recognized segment");
            }
            // NOT renamed .corrupt: the file belongs to the client build that
            // can read it; recovery keeps the slot intact for that writer.
            assertDirUnchanged(before);
        });
    }

    // ------------------------------------------------------------------
    // Corruption is quarantined, but never before validation decides.
    // ------------------------------------------------------------------

    @Test
    public void testCorruptStrayFileQuarantinedInLegacyDirAndSiblingsRecover() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000001.sfa", 2, 3);
            String stray = tmpDir + "/zz-stray.sfa";
            writeRawSegmentHeader(stray, 0xDEADBEEF, (byte) 1, 0L);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("valid chain must recover around a corrupt stray file", ring);
            try {
                Assert.assertEquals("all five durable frames must be recovered",
                        4, ring.publishedFsn());
            } finally {
                ring.close();
            }
            Assert.assertFalse("corrupt stray must be quarantined away from the .sfa scan",
                    Files.exists(stray));
            Assert.assertTrue("quarantine preserves the bytes as evidence",
                    Files.exists(stray + ".corrupt"));
            Assert.assertTrue("legacy recovery must migrate the slot to the manifest",
                    Files.exists(tmpDir + "/" + MANIFEST_NAME));
        });
    }

    @Test
    public void testCorruptChainSegmentWithManifestFailsWithoutMutation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String seg = tmpDir + "/sf-initial.sfa";
            writeSegmentWithFrames(seg, 0, 3);
            // Migrate once so the manifest exists and the segment is flagged.
            SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE).close();
            Assert.assertTrue(Files.exists(tmpDir + "/" + MANIFEST_NAME));
            // Bit-rot the magic in place.
            overwriteInt(seg, 0, 0xBADC0DE);
            Map<String, byte[]> before = snapshotDir();

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("losing the only chain segment to corruption must fail recovery");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "corrupt");
            }
            // Deferred quarantine: a FAILED recovery must not have renamed,
            // deleted, or otherwise mutated anything.
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testFlaggedSegmentWithDeletedManifestFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 3);
            SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE).close();
            Assert.assertTrue(Files.remove(tmpDir + "/" + MANIFEST_NAME));

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("a manifest-required segment without a manifest must fail recovery");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "missing");
            }
        });
    }

    // ------------------------------------------------------------------
    // Boundary evasion: missing leading/trailing segments must be caught.
    // ------------------------------------------------------------------

    @Test
    public void testMissingActiveSegmentWithManifestFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Chain [0..2) exists but the manifest says the active starts at 2.
            // Pre-manifest recovery would silently promote the highest present
            // segment and hand out overlapping FSNs.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            writeManifest(1, 0, 2);

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("a missing trailing/active segment must fail recovery");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "missing expected SF active");
            }
        });
    }

    @Test
    public void testMissingHeadSegmentWithManifestFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The manifest promises a head at base 0, but only [2..5) survived.
            // Pre-manifest recovery would pass contiguity on the remainder and
            // silently lose the leading rows.
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000001.sfa", 2, 3);
            writeManifest(1, 0, 2);

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("a missing leading/head segment must fail recovery");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "missing expected SF head");
            }
        });
    }

    @Test
    public void testInteriorGapStillFailsRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000002.sfa", 5, 2);
            writeManifest(1, 0, 5);

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("an interior FSN gap must fail recovery");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "FSN gap");
            }
        });
    }

    // ------------------------------------------------------------------
    // Legal crash-window states must recover without operator action.
    // ------------------------------------------------------------------

    @Test
    public void testFreshStartCrashWithTwoEmptyBaseZeroSegmentsRecovers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Fresh engine start provisions sf-initial plus a hot spare, both
            // empty at baseSeq 0. A process kill in that window must not brick
            // the slot on "ambiguous" empties.
            MmapSegment a = MmapSegment.create(tmpDir + "/sf-initial.sfa", 0, SEGMENT_SIZE);
            a.close();
            SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE).close();
            MmapSegment b = MmapSegment.create(tmpDir + "/sf-0000000000000001.sfa", 0, SEGMENT_SIZE);
            b.close();

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("two equivalent empty segments must not brick recovery", ring);
            try {
                Assert.assertEquals(-1, ring.publishedFsn());
                Assert.assertEquals(0, ring.getActive().baseSeq());
            } finally {
                ring.close();
            }
            Assert.assertEquals("the redundant empty must have been cleaned up",
                    1, countSfaFiles());
        });
    }

    @Test
    public void testRotationCrashWindowEmptyActiveAtChainEndRecovers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Crash after rotation committed (manifest fsync'd, spare header
            // synced) but before any frame reached the new active: sealed
            // chain [0..2) plus an empty active at base 2.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            MmapSegment spare = MmapSegment.create(tmpDir + "/sf-0000000000000001.sfa", 2, SEGMENT_SIZE);
            spare.close();
            writeManifest(1, 0, 2);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("freshly-rotated empty active is a legal crash state", ring);
            try {
                Assert.assertEquals(1, ring.publishedFsn());
                Assert.assertEquals(2, ring.getActive().baseSeq());
                Assert.assertNotNull(ring.firstSealed());
                Assert.assertEquals(0, ring.firstSealed().baseSeq());
            } finally {
                ring.close();
            }
        });
    }

    @Test
    public void testDrainWindowManifestWithoutSegmentsRecoversEmptyAndRemovesManifest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Clean-drain close durably collapses the boundaries to
            // head == active before unlinking, then removes segments and
            // finally the manifest; a crash between the last unlink and the
            // manifest removal leaves collapsed boundaries with no files.
            writeManifest(3, 9, 9);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNull("segment-less slot must recover as EMPTY", ring);
            Assert.assertFalse("the stale manifest must be discarded",
                    Files.exists(tmpDir + "/" + MANIFEST_NAME));
        });
    }

    @Test
    public void testZeroSegmentFilesWithUncollapsedBoundariesFailsClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // headBase(7) < activeBase(9): the manifest durably committed that
            // segments [7..9] existed and were never declared acked. No
            // in-protocol crash leaves this next to zero .sfa files -- the
            // close-time drain collapses boundaries to head == active BEFORE
            // its first unlink, and a fresh start writes (0,0). Uncollapsed
            // boundaries with no segment files therefore prove durable data
            // vanished outside the protocol: recovery must fail closed and
            // keep the manifest as evidence, not silently start fresh.
            writeManifest(3, 7, 9);
            Map<String, byte[]> before = snapshotDir();

            try {
                SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                if (ring != null) {
                    ring.close();
                }
                Assert.fail("recovery must fail closed: manifest boundaries (7,9) reference "
                        + "durable frames but no segment file survives");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "references durable data");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testUnquarantinableCorruptManifestClosesFdExactlyOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Correctly-sized manifest with BOTH records CRC-broken: open()
            // closes the fd and then tries to quarantine the debris. Make
            // rename AND remove fail (permission-degraded slot dir) so
            // quarantineDebris throws after the fd is already closed. The
            // propagating failure must not close the fd a second time -- the
            // OS may already have handed that number to another thread, and
            // a double-close would silently kill an unrelated descriptor.
            writeManifestBothRecordsCrcBroken();
            Map<String, byte[]> before = snapshotDir();

            String manifestPath = tmpDir + "/" + MANIFEST_NAME;
            int[] manifestFd = {-1};
            int[] manifestFdCloses = {0};
            FilesFacade facade = new DelegatingFacade() {
                @Override
                public int close(int fd) {
                    if (fd >= 0 && fd == manifestFd[0]) {
                        manifestFdCloses[0]++;
                    }
                    return super.close(fd);
                }

                @Override
                public int openRW(String path) {
                    int fd = super.openRW(path);
                    if (manifestPath.equals(path)) {
                        manifestFd[0] = fd;
                    }
                    return fd;
                }

                @Override
                public boolean remove(String path) {
                    return !manifestPath.equals(path) && super.remove(path);
                }

                @Override
                public int rename(String oldPath, String newPath) {
                    return manifestPath.equals(oldPath) ? -1 : super.rename(oldPath, newPath);
                }
            };
            try {
                SegmentRing ring = SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE);
                if (ring != null) {
                    ring.close();
                }
                Assert.fail("recovery must fail when corrupt-manifest quarantine cannot proceed");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "could not quarantine");
            }
            Assert.assertTrue("manifest was never opened", manifestFd[0] >= 0);
            Assert.assertEquals("manifest fd must be closed exactly once (a double-close can "
                    + "kill an unrelated descriptor)", 1, manifestFdCloses[0]);
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testFreshStartCrashBeforeManifestCreationRecoversViaLegacyPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Engine crash between creating sf-initial.sfa (unflagged) and
            // creating the manifest: recovers via legacy migration.
            MmapSegment initial = MmapSegment.create(tmpDir + "/sf-initial.sfa", 0, SEGMENT_SIZE);
            initial.close();

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull(ring);
            try {
                Assert.assertEquals(-1, ring.publishedFsn());
            } finally {
                ring.close();
            }
            Assert.assertTrue("legacy migration must create the manifest",
                    Files.exists(tmpDir + "/" + MANIFEST_NAME));
        });
    }

    @Test
    public void testSingleSectorTearLeavesPriorManifestRecordRecoverable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 64;
            long payload = Unsafe.malloc(65, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment initial = MmapSegment.create(
                        tmpDir + "/sf-initial.sfa", 0, segmentSize);
                try {
                    Assert.assertTrue(initial.tryAppend(payload, 64) >= 0);
                } finally {
                    initial.close();
                }

                SegmentRing ring = SegmentRing.openExisting(
                        FilesFacade.INSTANCE, tmpDir, segmentSize);
                Assert.assertNotNull(ring);
                try {
                    MmapSegment spare = MmapSegment.create(
                            tmpDir + "/sf-0000000000000001.sfa", 1, segmentSize);
                    ring.installHotSpare(spare);
                    Assert.assertEquals("oversized append must rotate but leave the new active empty",
                            SegmentRing.PAYLOAD_TOO_LARGE, ring.appendOrFsn(payload, 65));
                    Assert.assertEquals(0L, ring.publishedFsn());
                    Assert.assertEquals(1L, ring.getActive().baseSeq());
                } finally {
                    ring.close();
                }

                String manifestPath = tmpDir + "/" + MANIFEST_NAME;
                int tornBytes = (int) Math.min(512L, Files.length(manifestPath));
                overwriteRange(manifestPath, 0, tornBytes, (byte) 0xA5);

                SegmentRing recovered = SegmentRing.openExisting(
                        FilesFacade.INSTANCE, tmpDir, segmentSize);
                Assert.assertNotNull("one aligned 512-byte tear must leave a manifest record valid",
                        recovered);
                try {
                    Assert.assertEquals("recovery must fall back to the prior committed boundary",
                            0L, recovered.publishedFsn());
                    Assert.assertEquals(0L, recovered.getActive().baseSeq());
                } finally {
                    recovered.close();
                }
                Assert.assertFalse("a surviving record must prevent manifest quarantine",
                        Files.exists(tmpDir + "/" + MANIFEST_NAME + ".corrupt"));
            } finally {
                Unsafe.free(payload, 65, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    // ------------------------------------------------------------------
    // Engine level: enumeration failure must not truncate the durable log.
    // ------------------------------------------------------------------

    @Test
    public void testFindFirstFailureCannotCreateInitialSegment() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade facade = new DelegatingFacade() {
                @Override
                public long findFirst(String dir) {
                    return tmpDir.equals(dir) ? -1L : super.findFirst(dir);
                }
            };
            SegmentManager manager = new SegmentManager(
                    SEGMENT_SIZE, SegmentManager.DEFAULT_POLL_NANOS,
                    SegmentManager.UNLIMITED_TOTAL_BYTES, facade);
            manager.start();
            try {
                try {
                    new CursorSendEngine(tmpDir, SEGMENT_SIZE, manager).close();
                    Assert.fail("startup must fail when SF directory enumeration fails");
                } catch (RuntimeException expected) {
                    Assert.assertFalse("startup failure created sf-initial.sfa",
                            Files.exists(tmpDir + "/sf-initial.sfa"));
                }
            } finally {
                manager.close();
            }
        });
    }

    @Test
    public void testFindFirstFailureDoesNotTruncateExistingLog() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The original failure mode: enumeration error -> treated as empty
            // -> fresh start openCleanRW(O_TRUNC) destroys the durable log.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 4);
            Map<String, byte[]> before = snapshotDir();

            FilesFacade facade = new DelegatingFacade() {
                @Override
                public long findFirst(String dir) {
                    return tmpDir.equals(dir) ? -1L : super.findFirst(dir);
                }
            };
            SegmentManager manager = new SegmentManager(
                    SEGMENT_SIZE, SegmentManager.DEFAULT_POLL_NANOS,
                    SegmentManager.UNLIMITED_TOTAL_BYTES, facade);
            manager.start();
            try {
                try {
                    new CursorSendEngine(tmpDir, SEGMENT_SIZE, manager).close();
                    Assert.fail("startup must fail when SF directory enumeration fails");
                } catch (RuntimeException expected) {
                    // expected
                }
            } finally {
                manager.close();
            }
            assertDirUnchanged(before);
        });
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Creates a real segment at {@code path} and appends {@code frames} 64-byte frames. */
    private static void writeSegmentWithFrames(String path, long baseSeq, int frames) {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            MmapSegment segment = MmapSegment.create(path, baseSeq, SEGMENT_SIZE);
            try {
                for (int i = 0; i < frames; i++) {
                    for (int b = 0; b < 64; b++) {
                        Unsafe.getUnsafe().putByte(buf + b, (byte) (i * 31 + b));
                    }
                    Assert.assertTrue("test frame must fit", segment.tryAppend(buf, 64) >= 0);
                }
            } finally {
                segment.close();
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /** Writes a raw 64-byte pseudo segment header (+zero padding) for corruption tests. */
    private static void writeRawSegmentHeader(String path, int magic, byte version, long baseSeq) {
        int fd = Files.openCleanRW(path);
        Assert.assertTrue("could not create " + path, fd >= 0);
        try {
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 64, (byte) 0);
                Unsafe.getUnsafe().putInt(buf, magic);
                Unsafe.getUnsafe().putByte(buf + 4, version);
                Unsafe.getUnsafe().putLong(buf + 8, baseSeq);
                Assert.assertEquals(64, Files.write(fd, buf, 64, 0));
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(fd);
        }
    }

    /** Overwrites a byte range in an existing file and makes the modeled tear durable. */
    private static void overwriteRange(String path, long offset, int length, byte value) {
        int fd = Files.openRW(path);
        Assert.assertTrue("could not open " + path, fd >= 0);
        try {
            long buf = Unsafe.malloc(length, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, length, value);
                Assert.assertEquals(length, Files.write(fd, buf, length, offset));
                Assert.assertEquals(0, Files.fsync(fd));
            } finally {
                Unsafe.free(buf, length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(fd);
        }
    }

    /** Overwrites a single int at {@code offset} in an existing file. */
    private static void overwriteInt(String path, long offset, int value) {
        int fd = Files.openRW(path);
        Assert.assertTrue("could not open " + path, fd >= 0);
        try {
            long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putInt(buf, value);
                Assert.assertEquals(4, Files.write(fd, buf, 4, offset));
            } finally {
                Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(fd);
        }
    }

    /**
     * Writes a valid {@code sf-manifest.bin} with one CRC-protected record,
     * mirroring SfManifest's on-disk layout (two alternating 64-byte records
     * at offsets 0 and 4096 in an 8192-byte file).
     */
    private void writeManifest(long generation, long headBase, long activeBase) {
        String path = tmpDir + "/" + MANIFEST_NAME;
        // openRW (not openCleanRW): callers may layer a second generation's
        // record into the sibling slot of an existing manifest.
        int fd = Files.openRW(path);
        Assert.assertTrue("could not create manifest", fd >= 0);
        try {
            if (Files.length(path) < 8192) {
                Assert.assertTrue(Files.truncate(fd, 8192));
            }
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 64, (byte) 0);
                Unsafe.getUnsafe().putInt(buf, 0x314d4653); // SFM1
                Unsafe.getUnsafe().putInt(buf + 4, 1);      // version
                Unsafe.getUnsafe().putLong(buf + 8, generation);
                Unsafe.getUnsafe().putLong(buf + 16, headBase);
                Unsafe.getUnsafe().putLong(buf + 24, activeBase);
                int crc = Crc32c.update(Crc32c.INIT, buf, 60);
                Unsafe.getUnsafe().putInt(buf + 60, crc);
                long offset = (generation & 1L) * 4096;
                Assert.assertEquals(64, Files.write(fd, buf, 64, offset));
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(fd);
        }
    }

    /**
     * Writes a correctly-sized (8192-byte) manifest whose A and B records are
     * BOTH structurally plausible (magic, version, boundaries) but fail their
     * CRC check -- the "no valid CRC-protected record" quarantine trigger.
     */
    private void writeManifestBothRecordsCrcBroken() {
        String path = tmpDir + "/" + MANIFEST_NAME;
        int fd = Files.openRW(path);
        Assert.assertTrue("could not create manifest", fd >= 0);
        try {
            Assert.assertTrue(Files.truncate(fd, 8192));
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                for (long generation = 1; generation <= 2; generation++) {
                    Unsafe.getUnsafe().setMemory(buf, 64, (byte) 0);
                    Unsafe.getUnsafe().putInt(buf, 0x314d4653); // SFM1
                    Unsafe.getUnsafe().putInt(buf + 4, 1);      // version
                    Unsafe.getUnsafe().putLong(buf + 8, generation);
                    Unsafe.getUnsafe().putLong(buf + 16, 0);    // headBase
                    Unsafe.getUnsafe().putLong(buf + 24, 2);    // activeBase
                    int crc = Crc32c.update(Crc32c.INIT, buf, 60);
                    Unsafe.getUnsafe().putInt(buf + 60, crc + 1); // broken CRC
                    long offset = (generation & 1L) * 4096;
                    Assert.assertEquals(64, Files.write(fd, buf, 64, offset));
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(fd);
        }
    }

    /**
     * Snapshot of the durable SF payload files (segments, quarantined
     * segments, manifest): name -> content. Lifecycle noise such as the slot
     * lock and ack watermark is deliberately excluded -- "recovery must not
     * mutate the slot" is a statement about the durable log, not about lock
     * bookkeeping.
     */
    private Map<String, byte[]> snapshotDir() {
        Map<String, byte[]> out = new HashMap<>();
        Path dir = Paths.get(tmpDir);
        try (java.util.stream.Stream<Path> stream = java.nio.file.Files.list(dir)) {
            stream.filter(java.nio.file.Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.endsWith(".sfa") || name.endsWith(".corrupt")
                                || MANIFEST_NAME.equals(name);
                    })
                    .forEach(p -> {
                        try {
                            out.put(p.getFileName().toString(), java.nio.file.Files.readAllBytes(p));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /** Asserts the directory holds exactly the snapshotted files, byte for byte. */
    private void assertDirUnchanged(Map<String, byte[]> before) {
        Map<String, byte[]> after = snapshotDir();
        Assert.assertEquals("file set must be unchanged", before.keySet(), after.keySet());
        for (Map.Entry<String, byte[]> e : before.entrySet()) {
            Assert.assertArrayEquals("bytes of " + e.getKey() + " must be unchanged",
                    e.getValue(), after.get(e.getKey()));
        }
    }

    private int countSfaFiles() {
        int count = 0;
        for (String name : snapshotDir().keySet()) {
            if (name.endsWith(".sfa")) {
                count++;
            }
        }
        return count;
    }

    // ------------------------------------------------------------------
    // Manifest crash windows and record selection
    // ------------------------------------------------------------------

    @Test
    public void testCreationCrashZeroByteManifestSelfHeals() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // kill -9 between the manifest's O_EXCL create and its first
            // durable record leaves a zero-byte file. No boundary was ever
            // committed, so nothing can depend on it: startup must self-heal,
            // not demand an operator delete the file.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 3);
            int fd = Files.openCleanRW(tmpDir + "/" + MANIFEST_NAME);
            Assert.assertTrue(fd >= 0);
            Files.close(fd);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("creation-crash manifest debris must not brick startup", ring);
            try {
                Assert.assertEquals(2, ring.publishedFsn());
            } finally {
                ring.close();
            }
            Assert.assertTrue("debris must be quarantined for postmortem",
                    Files.exists(tmpDir + "/" + MANIFEST_NAME + ".corrupt"));
            Assert.assertTrue("a fresh valid manifest must replace the debris",
                    Files.exists(tmpDir + "/" + MANIFEST_NAME));
        });
    }

    @Test
    public void testCreationCrashRecordlessManifestSelfHeals() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Same window, later stage: allocate() completed (8192 zero bytes)
            // but the first record write/fsync never landed.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 3);
            int fd = Files.openCleanRW(tmpDir + "/" + MANIFEST_NAME);
            Assert.assertTrue(fd >= 0);
            Assert.assertTrue(Files.truncate(fd, 8192));
            Files.close(fd);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("record-less manifest debris must not brick startup", ring);
            try {
                Assert.assertEquals(2, ring.publishedFsn());
            } finally {
                ring.close();
            }
            Assert.assertTrue(Files.exists(tmpDir + "/" + MANIFEST_NAME + ".corrupt"));
            Assert.assertTrue(Files.exists(tmpDir + "/" + MANIFEST_NAME));
        });
    }

    @Test
    public void testRecordlessManifestWithFlaggedSegmentsFailsClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Flagged segments prove a manifest create COMPLETED at some
            // point, so a record-less manifest here is double-slot bit rot,
            // not creation debris -- boundaries were lost. Fail closed.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 3);
            SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE).close();
            int fd = Files.openCleanRW(tmpDir + "/" + MANIFEST_NAME); // truncates
            Assert.assertTrue(fd >= 0);
            Assert.assertTrue(Files.truncate(fd, 8192));
            Files.close(fd);

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("boundary loss next to flagged segments must fail closed");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "missing");
            }
        });
    }

    @Test
    public void testDrainCrashSurvivingSpareRecoversEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Clean-drain crash window: boundaries collapsed to head==active,
            // the active-base file already unlinked, but an empty hot spare
            // (provisional base > activeBase) survived. Everything was acked;
            // this must recover as EMPTY, not brick on "missing active".
            MmapSegment spare = MmapSegment.create(tmpDir + "/sf-0000000000000007.sfa", 5, SEGMENT_SIZE);
            spare.close();
            writeManifest(4, 3, 3);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNull("drain crash window must recover as EMPTY", ring);
            Assert.assertEquals("surviving spare must be cleaned up", 0, countSfaFiles());
            Assert.assertFalse("collapsed manifest must be removed",
                    Files.exists(tmpDir + "/" + MANIFEST_NAME));
        });
    }

    /**
     * The clean-drain crash window discards leftover segment files and reports
     * EMPTY, which makes the caller start fresh at baseSeq 0. If a leftover
     * survives that removal, the fresh session writes its own segments into a
     * directory that still holds a prior generation's file: two producer
     * generations sharing one slot, with overlapping ids describing different
     * strings. Nothing downstream can see it -- both generations number their
     * FSNs from the same origin, so neither validateContiguous nor a manifest
     * boundary can tell them apart. Refuse instead, leaving every byte on disk.
     */
    @Test
    public void testDrainWindowFailsClosedWhenLeftoverCannotBeRemoved() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String leftoverPath = tmpDir + "/sf-000000000000000a.sfa";
            MmapSegment leftover = MmapSegment.create(leftoverPath, 0L, SEGMENT_SIZE);
            leftover.close();
            // Collapsed boundaries above the leftover's base: the drain declared
            // everything acked, and no file sits at the committed active base.
            SfManifest.create(FilesFacade.INSTANCE, tmpDir, 5L, 5L).close();

            FilesFacade facade = new DelegatingFacade() {
                @Override
                public boolean remove(String path) {
                    return !leftoverPath.equals(path) && super.remove(path);
                }
            };
            try {
                SegmentRing ring = SegmentRing.openExisting(facade, tmpDir, SEGMENT_SIZE);
                if (ring != null) {
                    ring.close();
                }
                throw new AssertionError("a surviving leftover must refuse the slot");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "could not remove drained SF leftover");
            }
            Assert.assertTrue("the refused leftover must stay on disk",
                    Files.exists(leftoverPath));
        });
    }

    @Test
    public void testCorruptActiveWithSameBaseEmptyStandInFailsClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The active (holding unacked frames) is corrupted while a
            // leftover spare coincidentally carries the same base. Accepting
            // the clean empty as the "rotation crash" active would quarantine
            // the unacked frames and re-issue their FSNs -- recovery must
            // fail closed instead.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            String corruptActive = tmpDir + "/sf-0000000000000001.sfa";
            writeSegmentWithFrames(corruptActive, 2, 2);
            overwriteInt(corruptActive, 0, 0xBADC0DE); // bit-rot the magic
            MmapSegment standIn = MmapSegment.create(tmpDir + "/sf-0000000000000002.sfa", 2, SEGMENT_SIZE);
            standIn.close();
            writeManifest(1, 0, 2);
            Map<String, byte[]> before = snapshotDir();

            try {
                SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
                Assert.fail("an empty stand-in must not mask a corrupt active");
            } catch (MmapSegmentException expected) {
                TestUtils.assertContains(expected.getMessage(), "missing expected SF active");
            }
            assertDirUnchanged(before);
        });
    }

    @Test
    public void testManifestHigherGenerationRecordWins() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            MmapSegment next = MmapSegment.create(tmpDir + "/sf-0000000000000001.sfa", 2, SEGMENT_SIZE);
            next.close();
            // gen1 (slot 1) says active=0; gen2 (slot 0) says active=2. If
            // selection picked gen1, the empty at base 2 would sit beyond the
            // committed active boundary; gen2 accepts it as the active.
            writeManifest(1, 0, 0);
            writeManifest(2, 0, 2);

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull(ring);
            try {
                Assert.assertEquals("the higher-generation record must win",
                        2, ring.getActive().baseSeq());
            } finally {
                ring.close();
            }
        });
    }

    @Test
    public void testManifestTornNewerRecordFallsBackToOlder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 2);
            MmapSegment next = MmapSegment.create(tmpDir + "/sf-0000000000000001.sfa", 2, SEGMENT_SIZE);
            next.close();
            // gen1 (slot 1) is valid and matches the segments; gen2 (slot 0)
            // was torn mid-write (bad CRC). Selection must fall back to gen1
            // rather than reject the manifest or trust torn boundaries.
            writeManifest(1, 0, 2);
            writeManifest(2, 0, 4);
            overwriteInt(tmpDir + "/" + MANIFEST_NAME, 60, 0xBADC0DE); // tear gen2's CRC (slot 0)

            SegmentRing ring = SegmentRing.openExisting(FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("a torn newer record must fall back to the older slot", ring);
            try {
                Assert.assertEquals(2, ring.getActive().baseSeq());
            } finally {
                ring.close();
            }
        });
    }

    /** FilesFacade delegating everything to the production instance. */
    private static class DelegatingFacade implements FilesFacade {
        @Override public boolean allocate(int fd, long size) { return INSTANCE.allocate(fd, size); }
        @Override public long allocNativePath(String path) { return INSTANCE.allocNativePath(path); }
        @Override public int close(int fd) { return INSTANCE.close(fd); }
        @Override public boolean exists(String path) { return INSTANCE.exists(path); }
        @Override public void findClose(long findPtr) { INSTANCE.findClose(findPtr); }
        @Override public long findFirst(String dir) { return INSTANCE.findFirst(dir); }
        @Override public long findName(long findPtr) { return INSTANCE.findName(findPtr); }
        @Override public int findNext(long findPtr) { return INSTANCE.findNext(findPtr); }
        @Override public int findType(long findPtr) { return INSTANCE.findType(findPtr); }
        @Override public void freeNativePath(long pathPtr) { INSTANCE.freeNativePath(pathPtr); }
        @Override public int fsync(int fd) { return INSTANCE.fsync(fd); }
        @Override public long length(int fd) { return INSTANCE.length(fd); }
        @Override public long length(String path) { return INSTANCE.length(path); }
        @Override public int lock(int fd) { return INSTANCE.lock(fd); }
        @Override public int mkdir(String path, int mode) { return INSTANCE.mkdir(path, mode); }
        @Override public int openCleanRW(String path) { return INSTANCE.openCleanRW(path); }
        @Override public int openCleanRW(long pathPtr) { return INSTANCE.openCleanRW(pathPtr); }
        @Override public int openRW(String path) { return INSTANCE.openRW(path); }
        @Override public int openRW(long pathPtr) { return INSTANCE.openRW(pathPtr); }
        @Override public long length(long pathPtr) { return INSTANCE.length(pathPtr); }
        @Override public long read(int fd, long addr, long len, long offset) { return INSTANCE.read(fd, addr, len, offset); }
        @Override public boolean remove(String path) { return INSTANCE.remove(path); }
        @Override public boolean remove(long pathPtr) { return INSTANCE.remove(pathPtr); }
        @Override public int rename(String oldPath, String newPath) { return INSTANCE.rename(oldPath, newPath); }
        @Override public boolean truncate(int fd, long size) { return INSTANCE.truncate(fd, size); }
        @Override public long write(int fd, long addr, long len, long offset) { return INSTANCE.write(fd, addr, len, offset); }
    }
}
