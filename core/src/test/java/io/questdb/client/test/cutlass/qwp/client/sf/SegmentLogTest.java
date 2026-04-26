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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.cairo.CairoException;
import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.cutlass.qwp.client.sf.SfDiskFullException;
import io.questdb.client.cutlass.qwp.client.sf.SfException;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentLogTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-test-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, 0755));
    }

    @After
    public void tearDown() {
        rmTree(tmpDir);
    }

    private static void rmTree(String dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find != 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    private static long alloc(byte[] bytes) {
        long buf = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(buf + i, bytes[i]);
        }
        return buf;
    }

    private static byte[] readBytes(long addr, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return out;
    }

    @Test
    public void testAppendThenReplay() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[][] payloads = {"alpha".getBytes(), "beta".getBytes(), "gamma".getBytes()};
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                for (int i = 0; i < payloads.length; i++) {
                    long buf = alloc(payloads[i]);
                    try {
                        long seq = log.append(buf, payloads[i].length);
                        assertEquals((long) i, seq);
                    } finally {
                        Unsafe.free(buf, payloads[i].length, MemoryTag.NATIVE_DEFAULT);
                    }
                }
                log.fsync();
                List<byte[]> seen = new ArrayList<>();
                List<Long> seqs = new ArrayList<>();
                log.replay((seq, addr, len) -> {
                    seqs.add(seq);
                    seen.add(readBytes(addr, len));
                    return true;
                });
                assertEquals(3, seen.size());
                for (int i = 0; i < 3; i++) {
                    assertEquals(Long.valueOf(i), seqs.get(i));
                    assertArrayEquals(payloads[i], seen.get(i));
                }
            }
        });
    }

    @Test
    public void testReopenAndReplay() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[][] payloads = {"one".getBytes(), "two".getBytes(), "three".getBytes()};
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                for (byte[] p : payloads) {
                    long buf = alloc(p);
                    try {
                        log.append(buf, p.length);
                    } finally {
                        Unsafe.free(buf, p.length, MemoryTag.NATIVE_DEFAULT);
                    }
                }
                log.fsync();
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                assertEquals(3, log.nextSeq());
                List<byte[]> seen = new ArrayList<>();
                log.replay((seq, addr, len) -> {
                    seen.add(readBytes(addr, len));
                    return true;
                });
                assertEquals(3, seen.size());
                for (int i = 0; i < 3; i++) {
                    assertArrayEquals(payloads[i], seen.get(i));
                }
            }
        });
    }

    @Test
    public void testRotateAcrossSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Tiny segment cap: each frame is ~108B so ~9 frames per segment.
            long cap = SegmentLog.HEADER_SIZE + 5L * (SegmentLog.FRAME_HEADER_SIZE + 100);
            int frames = 25;
            byte[] payload = new byte[100];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) i;
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, cap)) {
                long buf = alloc(payload);
                try {
                    for (int i = 0; i < frames; i++) {
                        long seq = log.append(buf, payload.length);
                        assertEquals(i, seq);
                    }
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                log.fsync();
                assertTrue("expected multiple segments", log.segmentCount() >= 2);

                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    assertEquals((long) count[0], seq);
                    assertEquals(payload.length, len);
                    assertArrayEquals(payload, readBytes(addr, len));
                    count[0]++;
                    return true;
                });
                assertEquals(frames, count[0]);
            }
        });
    }

    @Test
    public void testTrimDeletesSealedFullyAcked() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long cap = SegmentLog.HEADER_SIZE + 3L * (SegmentLog.FRAME_HEADER_SIZE + 50);
            int frames = 20;
            byte[] payload = new byte[50];
            try (SegmentLog log = SegmentLog.open(tmpDir, cap)) {
                long buf = alloc(payload);
                try {
                    for (int i = 0; i < frames; i++) {
                        log.append(buf, payload.length);
                    }
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                log.fsync();
                int before = log.segmentCount();
                assertTrue("multiple segments expected", before >= 3);

                // ack everything up through the second-to-last frame
                log.trim(frames - 2);
                int after = log.segmentCount();
                assertTrue("trim should drop some segments: before=" + before + ", after=" + after,
                        after < before);
                // active segment never trimmed
                assertTrue(after >= 1);

                // remaining frames replay starts at oldestSeq (frames in still-not-fully-acked
                // sealed segment + active)
                long oldest = log.oldestSeq();
                int[] count = {0};
                long[] firstSeq = {-1};
                log.replay((seq, addr, len) -> {
                    if (firstSeq[0] < 0) firstSeq[0] = seq;
                    count[0]++;
                    return true;
                });
                assertTrue("oldestSeq should match first replayed seq",
                        firstSeq[0] == oldest);
                assertTrue("at least the active segment's frames remain",
                        count[0] >= 1 && count[0] <= frames);
            }
        });
    }

    /**
     * When ACK covers some-but-not-all of the active segment's frames, the
     * active segment must remain on disk (force-rotate only fires when
     * every frame is acked). Without this guard a partially-acked active
     * would be sealed and the unacked frames would be silently lost.
     */
    @Test
    public void testTrimPartialAckOfActiveLeavesItIntact() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] payload = "x".getBytes();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                long buf = alloc(payload);
                try {
                    log.append(buf, payload.length);
                    log.append(buf, payload.length);
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                log.fsync();
                // Ack only the first frame; second is still in flight. The
                // active must NOT be force-rotated yet — that would seal a
                // segment containing un-acked data.
                log.trim(0);
                assertEquals(1, log.segmentCount());
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    count[0]++;
                    return true;
                });
                assertEquals("both frames must still be on disk", 2, count[0]);
            }
        });
    }

    /**
     * When ACK covers every frame in the active segment, the active is
     * force-rotated and the just-sealed segment removed. nextSeq is
     * preserved across the auto-rotate so subsequent appends keep
     * monotonic FSNs. After reopen, replay yields zero frames — this is
     * what makes "trimmed when the server acknowledges it" honest in the
     * public Sender API.
     */
    @Test
    public void testTrimRotatesAndDropsFullyAckedActiveSegment() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] payload = "x".getBytes();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                long buf = alloc(payload);
                try {
                    for (int i = 0; i < 5; i++) {
                        log.append(buf, payload.length);
                    }
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                log.fsync();

                long preTrimBytes = log.bytesOnDisk();
                assertTrue("data must be on disk before trim",
                        preTrimBytes > SegmentLog.HEADER_SIZE);
                assertEquals(5L, log.nextSeq());

                // Ack every frame; force-rotate kicks in, sealed segment
                // removed in the same trim() call.
                log.trim(4);

                assertEquals("a fresh empty active must remain", 1, log.segmentCount());
                assertEquals("nextSeq must survive the auto-rotate", 5L, log.nextSeq());
                assertEquals("oldestSeq must report empty (no frames)", -1L, log.oldestSeq());
                assertEquals("only the new active's header should be on disk",
                        (long) SegmentLog.HEADER_SIZE, log.bytesOnDisk());
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    count[0]++;
                    return true;
                });
                assertEquals("no frames should remain after force-rotate-trim",
                        0, count[0]);
            }
            // Reopen with a fresh SegmentLog; replay must visit zero frames.
            try (SegmentLog log2 = SegmentLog.open(tmpDir, 1L << 20)) {
                int[] count = {0};
                log2.replay((seq, addr, len) -> {
                    count[0]++;
                    return true;
                });
                assertEquals(
                        "acked-and-trimmed frames must not replay on restart",
                        0, count[0]);
                assertEquals("nextSeq must round-trip", 5L, log2.nextSeq());
            }
        });
    }

    @Test
    public void testRecoveryTruncatesTornTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] p1 = "first".getBytes();
            byte[] p2 = "second".getBytes();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                long b1 = alloc(p1);
                long b2 = alloc(p2);
                try {
                    log.append(b1, p1.length);
                    log.append(b2, p2.length);
                    log.fsync();
                } finally {
                    Unsafe.free(b1, p1.length, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(b2, p2.length, MemoryTag.NATIVE_DEFAULT);
                }
            }

            // Append junk to the active segment to simulate a torn tail.
            String activePath = findActivePath(tmpDir);
            assertTrue("active segment expected", activePath != null);
            int fd = Files.openRW(activePath);
            try {
                long fileLen = Files.length(fd);
                long junk = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < 16; i++) {
                        Unsafe.getUnsafe().putByte(junk + i, (byte) 0xAB);
                    }
                    Files.write(fd, junk, 16, fileLen);
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(junk, 16, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }

            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    count[0]++;
                    return true;
                });
                assertEquals("torn tail should be truncated; only 2 valid frames remain", 2, count[0]);
                assertEquals(2, log.nextSeq());
            }
        });
    }

    @Test
    public void testCrcMismatchInMiddleThrowsOnReplay() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] p1 = "alpha".getBytes();
            byte[] p2 = "beta".getBytes();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                long b1 = alloc(p1);
                long b2 = alloc(p2);
                try {
                    log.append(b1, p1.length);
                    log.append(b2, p2.length);
                    log.fsync();
                } finally {
                    Unsafe.free(b1, p1.length, MemoryTag.NATIVE_DEFAULT);
                    Unsafe.free(b2, p2.length, MemoryTag.NATIVE_DEFAULT);
                }
            }

            // Flip a byte deep inside the first frame's payload (header is 24, frame is
            // [4-crc][4-len][5-payload], so payload starts at 32).
            String active = findActivePath(tmpDir);
            int fd = Files.openRW(active);
            try {
                long bytePos = SegmentLog.HEADER_SIZE + SegmentLog.FRAME_HEADER_SIZE + 1;
                long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                try {
                    Files.read(fd, buf, 1, bytePos);
                    byte b = Unsafe.getUnsafe().getByte(buf);
                    Unsafe.getUnsafe().putByte(buf, (byte) (b ^ 0xFF));
                    Files.write(fd, buf, 1, bytePos);
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }

            // On reopen the corrupted frame is in a "valid-length but bad-CRC"
            // state with a second valid frame still on disk after it. This is
            // mid-stream bit-rot, not a torn tail — silently truncating would
            // drop the trailing valid frame too. Recovery surfaces the
            // corruption loudly (bug M1).
            try {
                SegmentLog.open(tmpDir, 1L << 20);
                fail("expected SfException for mid-stream CRC mismatch");
            } catch (SfException expected) {
                assertTrue(
                        "SfException must reference CRC, got: " + expected.getMessage(),
                        expected.getMessage().toLowerCase().contains("crc"));
            }
        });
    }

    @Test
    public void testLockPreventsConcurrentOpen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                assertTrue(log.nextSeq() >= 0);
                try {
                    SegmentLog.open(tmpDir, 1L << 20);
                    fail("second open should have failed due to lock");
                } catch (SfException expected) {
                    assertTrue(expected.getMessage(), expected.getMessage().contains("locked"));
                }
            }
            // After close, a new open should succeed.
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                assertEquals(0, log.nextSeq());
            }
        });
    }

    @Test
    public void testEmptyPayloadRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    log.append(buf, 0);
                    fail("expected SfException for zero-length payload");
                } catch (SfException expected) {
                    // ok
                } finally {
                    Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testRotationPreservesFramesAfterReopen() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long cap = SegmentLog.HEADER_SIZE + 3L * (SegmentLog.FRAME_HEADER_SIZE + 64);
            int frames = 30;
            byte[] payload = new byte[64];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (i * 7);
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, cap)) {
                long buf = alloc(payload);
                try {
                    for (int i = 0; i < frames; i++) {
                        log.append(buf, payload.length);
                    }
                    log.fsync();
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, cap)) {
                assertEquals(frames, log.nextSeq());
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    assertArrayEquals(payload, readBytes(addr, len));
                    count[0]++;
                    return true;
                });
                assertEquals(frames, count[0]);
            }
        });
    }

    @Test
    public void testReplayStopsWhenVisitorReturnsFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] payload = "x".getBytes();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                long buf = alloc(payload);
                try {
                    for (int i = 0; i < 10; i++) {
                        log.append(buf, payload.length);
                    }
                    log.fsync();
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    count[0]++;
                    return seq < 4;
                });
                assertEquals(5, count[0]); // visited 0..4 then stopped
            }
        });
    }

    @Test
    public void testMaxTotalBytesTriggersDiskFull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // tiny: header (24) + ~4 frames of 50 bytes
            long perSeg = SegmentLog.HEADER_SIZE + 2L * (SegmentLog.FRAME_HEADER_SIZE + 50);
            long totalCap = perSeg * 2; // ~4 frames worth across 2 segments
            byte[] payload = new byte[50];
            try (SegmentLog log = SegmentLog.open(tmpDir, perSeg, totalCap)) {
                long buf = alloc(payload);
                try {
                    int appended = 0;
                    SfDiskFullException dfe = null;
                    for (int i = 0; i < 100 && dfe == null; i++) {
                        try {
                            log.append(buf, payload.length);
                            appended++;
                        } catch (SfDiskFullException e) {
                            dfe = e;
                        }
                    }
                    Assert.assertNotNull("eventually disk-full", dfe);
                    Assert.assertTrue("appended at least one frame before disk-full", appended > 0);

                    // Trim what we have; active segment never trims, but if any sealed
                    // exists it should go.
                    log.trim(appended - 1);
                    // Try one more append after trim — could succeed if sealed segment was
                    // dropped, freeing space.
                    try {
                        log.append(buf, payload.length);
                    } catch (SfDiskFullException ignored) {
                        // Acceptable: only the active was on disk and active doesn't trim.
                        // The point is the disk-full exception fires, not that trim always
                        // recovers from a single segment scenario.
                    }
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testMaxTotalBytesValidationMustExceedSegment() {
        try {
            SegmentLog.open(tmpDir, 8192, 4096).close();
            fail("expected open to reject maxTotalBytes < maxBytesPerSegment");
        } catch (SfException expected) {
            Assert.assertTrue(expected.getMessage(),
                    expected.getMessage().contains("maxTotalBytes"));
        }
    }

    @Test
    public void testOldestSeqAfterTrim() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long cap = SegmentLog.HEADER_SIZE + 2L * (SegmentLog.FRAME_HEADER_SIZE + 32);
            byte[] payload = new byte[32];
            try (SegmentLog log = SegmentLog.open(tmpDir, cap)) {
                long buf = alloc(payload);
                try {
                    for (int i = 0; i < 10; i++) {
                        log.append(buf, payload.length);
                    }
                    log.fsync();
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                assertEquals(0, log.oldestSeq());
                log.trim(3);
                long oldest = log.oldestSeq();
                assertTrue("oldest seq should advance past 1: " + oldest, oldest > 1);
            }
        });
    }

    /**
     * Red test for bug C4 — fd leak in {@code SegmentLog.createActive} when
     * {@code writeHeader} or {@code fsync} throws between {@code openCleanRW}
     * and {@code segments.add(s)}.
     * <p>
     * The fd is opened (line 536), assigned to a local {@code Segment s} not
     * yet added to the {@code segments} list. If the subsequent
     * {@code writeHeader} short-write or {@code fsync} non-zero return throws,
     * the local Segment is discarded; {@code close()}'s cleanup loop only
     * walks {@code segments}, so the fd is unreachable and leaks. Reachable
     * from {@code openInternal()} (one-shot) and {@code rotate()} (per
     * rotation): under disk pressure or NFS flakiness every failed rotation
     * leaks one fd; sustained loops will exhaust the process fd table.
     * <p>
     * Repro: a {@link FilesFacade} that wraps the default but forces
     * {@code fsync} to fail on the very first {@code createActive} call. The
     * test records every {@code openCleanRW} return value and verifies that
     * each opened fd was {@code close}d before the {@link SfException}
     * propagated out of {@code SegmentLog.open}.
     */
    /**
     * Red test for the fd-leak gap between {@code openCleanRW} and the
     * {@code try} block in {@code SegmentLog.createActive}.
     * <p>
     * Production order at lines 580-595:
     * <pre>
     *   int fd = ff.openCleanRW(path, 0);                  // fd opened
     *   ...
     *   s.pathPtrNative = ff.allocNativePath(path);        // CAN throw OOM
     *   s.fd = fd;                                          // never reached on throw
     *   try { ... } catch { ff.close(fd); ... }             // try not entered
     * </pre>
     * If {@code allocNativePath} throws (the {@code Unsafe.malloc} inside
     * {@link io.questdb.client.std.Files#pathPtr(String)} wraps {@link OutOfMemoryError}
     * in {@link CairoException}), the local {@code fd} is leaked: {@code s} was
     * never added to {@code segments}, so {@code close()}'s cleanup loop never
     * sees it. The orphan {@code .sfa} file also remains on disk and trips the
     * "multiple active segments" guard on the next process restart that
     * legitimately rotates.
     * <p>
     * On a long-running spacecraft client under intermittent memory pressure,
     * each failed rotation leaks one fd; sustained loops will exhaust the
     * process fd table.
     * <p>
     * The fix: register {@code s.fd = fd} BEFORE the throwing call, and
     * extend the {@code try/catch} cleanup to cover the path allocation
     * (and {@code ff.remove(path)} the orphan file).
     */
    @Test
    public void testCreateActiveDoesNotLeakFdOnAllocNativePathOom() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FdTrackingFacade tracker = new FdTrackingFacade();
            tracker.failNextActiveAllocNativePath = true;
            try {
                SegmentLog.open(tmpDir, tracker, 4096, 4096, false);
                fail("expected open to fail because allocNativePath was forced to throw");
            } catch (Throwable expected) {
                String msg = expected.getMessage() == null ? "" : expected.getMessage();
                String causeMsg = expected.getCause() == null || expected.getCause().getMessage() == null
                        ? "" : expected.getCause().getMessage();
                assertTrue(
                        "wrong failure surfaced: " + expected + " / cause=" + expected.getCause(),
                        msg.contains("simulated") || msg.contains("OOM")
                                || causeMsg.contains("simulated") || causeMsg.contains("OOM"));
            }
            Set<Integer> leaked = new HashSet<>(tracker.opened);
            leaked.removeAll(tracker.closed);
            assertEquals(
                    "createActive must close every fd it opened when allocNativePath throws "
                            + "between openCleanRW and the try-block; leaked=" + leaked,
                    0, leaked.size());

            // Also: no orphan .sfa file should remain on disk. The fix should
            // ff.remove the half-created file so the next open sees a clean dir.
            long find = Files.findFirst(tmpDir);
            if (find != 0) {
                try {
                    int rc = 1;
                    while (rc > 0) {
                        String name = Files.utf8ToString(Files.findName(find));
                        if (name != null && name.endsWith(".sfa")) {
                            fail("orphan .sfa file remains after partial-init failure: " + name);
                        }
                        rc = Files.findNext(find);
                    }
                } finally {
                    Files.findClose(find);
                }
            }
        });
    }

    /**
     * Regression test for {@code rotate}'s mid-reseal OOM window.
     * <p>
     * Production order at lines 564-570 (pre-fix):
     * <pre>
     *   ff.freeNativePath(old.pathPtrNative);                  // ptr freed
     *   old.path = sealedPath;
     *   old.pathPtrNative = ff.allocNativePath(sealedPath);    // CAN throw OOM
     *   old.sealed = true;
     *   old.lastSeqOnDisk = lastSeq;
     * </pre>
     * If {@code allocNativePath} throws after the freed pointer is left in
     * the field and before {@code sealed/lastSeqOnDisk} are set:
     * <ul>
     *   <li><b>native double-free on close:</b> {@code SegmentLog.close()}
     *       walks {@code segments} and calls {@code freeNativePath} on the
     *       stale freed pointer.</li>
     *   <li><b>permanent on-disk leak:</b> {@code trim()}'s {@code !s.sealed}
     *       guard skips the segment, so the {@code .sfs} file on disk is
     *       never reclaimed within the lifetime of this process. Even after
     *       restart it would re-replay forever (no ACK ever advances past
     *       its lastSeq because the in-memory state lost it).</li>
     * </ul>
     * <p>
     * The fix sets {@code pathPtrNative=0} immediately after the free and
     * marks {@code sealed=true; lastSeqOnDisk=lastSeq} BEFORE allocating
     * the new pointer. {@code trim()} falls back to {@code ff.remove(path)}
     * when {@code pathPtrNative} is 0.
     */
    @Test
    public void testRotateOomLeavesSegmentInRecoverableSealedState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FdTrackingFacade tracker = new FdTrackingFacade();
            // maxBytes = HEADER_SIZE + FRAME_HEADER_SIZE + 16 = 48; first
            // append fits the active segment exactly, the second forces
            // rotation.
            final long maxBytes = 48;
            final int payloadSize = 16;

            long buf = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payloadSize; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }

                SegmentLog log = SegmentLog.open(tmpDir, tracker, maxBytes, 1024, false);
                try {
                    long s0 = log.append(buf, payloadSize);
                    assertEquals(0L, s0);

                    // Arm the OOM at the rotate's allocNativePath(sealedPath).
                    tracker.failNextSealedAllocNativePath = true;
                    try {
                        log.append(buf, payloadSize);
                        fail("expected OOM during rotate's allocNativePath(sealedPath)");
                    } catch (Throwable expected) {
                        String msg = expected.getMessage() == null ? "" : expected.getMessage();
                        String causeMsg = expected.getCause() == null
                                || expected.getCause().getMessage() == null
                                ? "" : expected.getCause().getMessage();
                        assertTrue("wrong failure: " + expected,
                                msg.contains("simulated") || msg.contains("OOM")
                                        || causeMsg.contains("simulated") || causeMsg.contains("OOM"));
                    }

                    // The segment is sealed on disk and must be classified
                    // as sealed in memory so trim() can reclaim it. Drop
                    // every acked seq up to and including the (now-sealed)
                    // segment's lastSeq, then assert the file is gone.
                    log.trim(0);
                } finally {
                    // close() walks the segments list and frees pathPtrNative
                    // for each. Under the bug the rotated segment's stale
                    // freed pointer would be passed to freeNativePath again
                    // → native double-free. The fix sets pathPtrNative=0
                    // after the original free so close() skips it.
                    log.close();
                }

                // No .sfs file should remain after trim().
                long find = Files.findFirst(tmpDir);
                if (find != 0) {
                    try {
                        int rc = 1;
                        while (rc > 0) {
                            String name = Files.utf8ToString(Files.findName(find));
                            if (name != null && name.endsWith(".sfs")) {
                                fail("sealed .sfs file leaked after trim: " + name
                                        + " — rotate's mid-OOM left the segment unsealed in "
                                        + "memory so trim's !s.sealed guard skipped it");
                            }
                            rc = Files.findNext(find);
                        }
                    } finally {
                        Files.findClose(find);
                    }
                }
            } finally {
                Unsafe.free(buf, payloadSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCreateActiveDoesNotLeakFdOnFsyncFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FdTrackingFacade tracker = new FdTrackingFacade();
            tracker.failNextFsyncOnNewFd = true;
            try {
                SegmentLog.open(tmpDir, tracker, 4096, 4096, false);
                fail("expected SfException because fsync was forced to fail");
            } catch (SfException expected) {
                Assert.assertTrue(
                        "wrong failure surfaced: " + expected.getMessage(),
                        expected.getMessage().contains("fsync"));
            }
            Set<Integer> leaked = new HashSet<>(tracker.opened);
            leaked.removeAll(tracker.closed);
            assertEquals(
                    "createActive must close every fd it opened on the failure path; leaked=" + leaked,
                    0, leaked.size());
        });
    }

    /**
     * Red test for bug M1 — {@code SegmentLog.scanActive} silently truncates
     * every frame after a mid-stream CRC mismatch.
     * <p>
     * The {@code while (pos &lt; fileLen)} loop in {@code scanActive} treats a
     * CRC mismatch identically to a torn tail: {@code break}, then truncate
     * the file to {@code pos}. A single bit flip in the middle of a 5-frame
     * segment causes silent loss of every valid frame after the corruption,
     * with no log line and no exception.
     * <p>
     * Repro: write 5 frames to an active segment, close, flip a bit in
     * frame 2's CRC field on disk, reopen. The fix must either preserve
     * frames 3 and 4 (somehow scan past the corruption) or refuse to open
     * the segment so an operator notices. It must NOT silently delete the
     * tail.
     */
    @Test
    public void testScanActiveRejectsMidStreamCrcMismatch() throws Exception {
        final int frameCount = 5;
        final int payloadSize = 32;

        // Step 1: write 5 frames using the default facade.
        long buf = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payloadSize; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i + 1));
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, 4096)) {
                for (int i = 0; i < frameCount; i++) {
                    log.append(buf, payloadSize);
                }
            }
        } finally {
            Unsafe.free(buf, payloadSize, MemoryTag.NATIVE_DEFAULT);
        }

        // Step 2: corrupt the CRC field of frame 2 (zero-indexed) on disk.
        // Layout: [24-byte file header][frame0:8+32][frame1:8+32][frame2:8+32]...
        // CRC of frame 2 starts at offset 24 + 2*(8+32) = 104.
        String activePath = findActivePath(tmpDir);
        Assert.assertNotNull("active segment file must exist", activePath);
        long crcOffsetOfFrame2 = SegmentLog.HEADER_SIZE + 2L * (SegmentLog.FRAME_HEADER_SIZE + payloadSize);
        int rwFd = Files.openRW(activePath);
        Assert.assertTrue("openRW must succeed", rwFd >= 0);
        try {
            long bitflipBuf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
            try {
                long r = Files.read(rwFd, bitflipBuf, 4, crcOffsetOfFrame2);
                Assert.assertEquals(4, r);
                int crc = Unsafe.getUnsafe().getInt(bitflipBuf);
                Unsafe.getUnsafe().putInt(bitflipBuf, crc ^ 0x00000001);
                long w = Files.write(rwFd, bitflipBuf, 4, crcOffsetOfFrame2);
                Assert.assertEquals(4, w);
            } finally {
                Unsafe.free(bitflipBuf, 4, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(rwFd);
        }

        // Step 3: reopen and observe how the corruption is handled.
        // Bug M1: open succeeds, scanActive silently truncates the file to
        // pos == start-of-frame-2, dropping frames 2, 3, 4. Replay sees 2.
        try (SegmentLog log = SegmentLog.open(tmpDir, 4096)) {
            int[] visited = {0};
            log.replay((seq, addr, len) -> {
                visited[0]++;
                return true;
            });
            // Either the implementation preserves frames 3+4 somehow (we
            // don't expect this — it'd require resync logic), or it refuses
            // to open and the close/SfException path runs. Silent truncate
            // to 2 is the bug we're flagging.
            Assert.assertNotEquals(
                    "scanActive silently truncated frames 3 and 4 after a CRC mismatch in frame 2; "
                            + "must error or preserve them, not drop silently",
                    2, visited[0]);
        } catch (SfException expected) {
            // Acceptable: hard error referencing CRC.
            Assert.assertTrue(
                    "SfException must reference CRC corruption, got: " + expected.getMessage(),
                    expected.getMessage().toLowerCase().contains("crc")
                            || expected.getMessage().toLowerCase().contains("corrupt"));
        }
    }

    /**
     * Coverage gap from M9 — segment header version byte rejection.
     * Production at {@code openSegment} line 581-583 throws
     * {@code "unsupported version N"} when the header's version byte is not 1.
     * Untested before this. Writes a header with valid magic but version byte
     * 99 and verifies the exception surfaces.
     */
    @Test
    public void testUnsupportedVersionRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String junkPath = tmpDir + "/0000000000000000.sfa";
            int fd = Files.openCleanRW(junkPath, SegmentLog.HEADER_SIZE);
            try {
                long buf = Unsafe.malloc(SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(buf, SegmentLog.FILE_MAGIC);
                    Unsafe.getUnsafe().putByte(buf + 4, (byte) 99);  // unsupported version
                    Unsafe.getUnsafe().putByte(buf + 5, (byte) 0);
                    Unsafe.getUnsafe().putShort(buf + 6, (short) 0);
                    Unsafe.getUnsafe().putLong(buf + 8, 0L);
                    Unsafe.getUnsafe().putLong(buf + 16, 0L);
                    Files.write(fd, buf, SegmentLog.HEADER_SIZE, 0);
                } finally {
                    Unsafe.free(buf, SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }
            try {
                SegmentLog.open(tmpDir, 1L << 20).close();
                fail("expected open to reject unsupported version");
            } catch (SfException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("unsupported version"));
            }
        });
    }

    /**
     * Coverage gap from M9 — header baseSeq must match the value embedded in
     * the filename. Production at {@code openSegment} line 585-588 throws
     * {@code "baseSeq mismatch"} when the on-disk header carries a different
     * value than the filename advertises.
     */
    @Test
    public void testBaseSeqMismatchRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Filename advertises baseSeq=0; header carries baseSeq=99.
            String junkPath = tmpDir + "/0000000000000000.sfa";
            int fd = Files.openCleanRW(junkPath, SegmentLog.HEADER_SIZE);
            try {
                long buf = Unsafe.malloc(SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(buf, SegmentLog.FILE_MAGIC);
                    Unsafe.getUnsafe().putByte(buf + 4, (byte) 1);
                    Unsafe.getUnsafe().putByte(buf + 5, (byte) 0);
                    Unsafe.getUnsafe().putShort(buf + 6, (short) 0);
                    Unsafe.getUnsafe().putLong(buf + 8, 99L);  // mismatches filename
                    Unsafe.getUnsafe().putLong(buf + 16, 0L);
                    Files.write(fd, buf, SegmentLog.HEADER_SIZE, 0);
                } finally {
                    Unsafe.free(buf, SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }
            try {
                SegmentLog.open(tmpDir, 1L << 20).close();
                fail("expected open to reject baseSeq mismatch");
            } catch (SfException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("baseSeq mismatch"));
            }
        });
    }

    /**
     * Coverage gap from M9 — multiple active segments in the directory must
     * be rejected. Production at {@code scanDirectory} line 406-408 throws
     * {@code "multiple active segments"} when more than one .sfa is found
     * (indicates a corrupted directory or a crash mid-rotation that left
     * orphan files).
     */
    @Test
    public void testMultipleActiveSegmentsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // First, create a legitimate SegmentLog with rotation enabled so we
            // end up with a sealed segment + an active one.
            long cap = SegmentLog.HEADER_SIZE + SegmentLog.FRAME_HEADER_SIZE + 16;
            byte[] payload = new byte[16];
            try (SegmentLog log = SegmentLog.open(tmpDir, cap)) {
                long buf = alloc(payload);
                try {
                    log.append(buf, payload.length);  // first segment
                    log.append(buf, payload.length);  // forces rotation
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
            }

            // Now plant a second .sfa file with a higher baseSeq. After sort,
            // the original active is no longer last and triggers the check.
            String orphanActive = tmpDir + "/00000000000000ff.sfa";
            int fd = Files.openCleanRW(orphanActive, SegmentLog.HEADER_SIZE);
            try {
                long buf = Unsafe.malloc(SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(buf, SegmentLog.FILE_MAGIC);
                    Unsafe.getUnsafe().putByte(buf + 4, (byte) 1);
                    Unsafe.getUnsafe().putByte(buf + 5, (byte) 0);
                    Unsafe.getUnsafe().putShort(buf + 6, (short) 0);
                    Unsafe.getUnsafe().putLong(buf + 8, 0xffL);
                    Unsafe.getUnsafe().putLong(buf + 16, 0L);
                    Files.write(fd, buf, SegmentLog.HEADER_SIZE, 0);
                } finally {
                    Unsafe.free(buf, SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }

            try {
                SegmentLog.open(tmpDir, 1L << 20).close();
                fail("expected open to reject multiple active segments");
            } catch (SfException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("multiple active segments"));
            }
        });
    }

    /**
     * Coverage gap from M9 — {@code oldestSeq()} edge cases that the existing
     * tests didn't cover: a freshly-opened log and a log whose only segment
     * is the empty active segment (post-trim of every sealed segment).
     */
    @Test
    public void testOldestSeqEdgeCases() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 1. Freshly opened log (no append yet) — oldestSeq must be -1.
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                assertEquals("fresh log oldestSeq", -1L, log.oldestSeq());
                assertEquals("fresh log nextSeq", 0L, log.nextSeq());
            }

            // 2. Log with one frame appended, then trimmed past it. Active is
            //    never trimmed, so oldestSeq still reports the active's seq.
            //    But if active is empty (no frames, only header), oldestSeq
            //    must report -1.
            //    To reach this state without rotation: open + close without
            //    writing.
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                assertEquals("never-appended log oldestSeq", -1L, log.oldestSeq());
            }
        });
    }

    /**
     * Coverage gap from M9 — short-write recovery on the actual durability
     * path. {@code SegmentLog.append} truncates the file back when
     * {@code Files.write} reports a short write (typical ENOSPC) and throws
     * {@link SfDiskFullException}. Production lines 211-216 (frame header
     * short write) and 218-225 (payload short write). The fault facade
     * forces the second {@code write(fd, ...)} (the payload) to return a
     * short count.
     */
    @Test
    public void testShortPayloadWriteTruncatesAndThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ShortPayloadWriteFacade tracker = new ShortPayloadWriteFacade();
            byte[] payload = new byte[64];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (i + 1);
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, tracker, 4096, 4096, false)) {
                long buf = alloc(payload);
                try {
                    // First append succeeds normally.
                    log.append(buf, payload.length);
                    // Arm the fault for the next append's payload write.
                    tracker.failNextPayloadWrite = true;
                    try {
                        log.append(buf, payload.length);
                        fail("expected SfDiskFullException for short payload write");
                    } catch (SfDiskFullException expected) {
                        assertTrue(expected.getMessage(),
                                expected.getMessage().contains("short write"));
                    }
                    // After the failure, the segment must be in a clean state:
                    // a third append at the same writePos must succeed.
                    log.append(buf, payload.length);
                } finally {
                    Unsafe.free(buf, payload.length, MemoryTag.NATIVE_DEFAULT);
                }
                // 2 successful appends out of 3 attempts.
                assertEquals(2L, log.nextSeq());
            }
        });
    }

    /**
     * Red test for bug C5 — {@code Files.length(fd)} returns -1 on
     * {@code fstat} failure, but {@code SegmentLog.scanActive} (line 418)
     * and {@code SegmentLog.replaySegment} (line 461) then run
     * {@code while (pos &lt; fileLen)} which never iterates when
     * {@code fileLen == -1}. The segment is silently treated as empty:
     * {@code scanActive} returns 0 frames with {@code writePos == HEADER_SIZE},
     * and {@code replay} visits zero frames. SF FSN monotonicity quietly
     * breaks and any persisted-but-not-yet-acked data is hidden from replay.
     * <p>
     * {@code openSegment} (line 578) does check {@code len &lt; HEADER_SIZE}
     * which catches a -1 from the FIRST {@code length} call. The unprotected
     * paths are the subsequent calls inside {@code scanActive} and
     * {@code replaySegment}. The fault facade lets the first call through and
     * returns -1 on every subsequent one.
     */
    @Test
    public void testReplayRejectsLengthFstatFailure() throws Exception {
        // Step 1: write a real frame using the default facade so disk has data.
        long payloadSize = 32;
        long buf = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
        try {
            for (long i = 0; i < payloadSize; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i + 1));
            }
            try (SegmentLog log = SegmentLog.open(tmpDir, 4096)) {
                log.append(buf, (int) payloadSize);
                log.append(buf, (int) payloadSize);
                log.append(buf, (int) payloadSize);
            }
        } finally {
            Unsafe.free(buf, payloadSize, MemoryTag.NATIVE_DEFAULT);
        }

        // Step 2: reopen with a facade whose length(fd) call after openSegment
        // returns -1 (simulating fstat failure). Replay must not silently
        // observe zero frames.
        TestUtils.assertMemoryLeak(() -> {
            FaultyLengthFacade tracker = new FaultyLengthFacade();
            // Let the openSegment length-check pass (first 1 call), then start
            // failing. scanActive does a second length() per active segment.
            tracker.passFirstNLengthCalls = 1;

            try (SegmentLog log = SegmentLog.open(tmpDir, tracker, 4096, 4096, false)) {
                int[] visited = {0};
                log.replay((seq, addr, len) -> {
                    visited[0]++;
                    return true;
                });
                Assert.assertNotEquals(
                        "replay must not silently observe zero frames when length(fd) reports -1; " +
                                "fault was triggered " + tracker.lengthFaultsTriggered + " time(s)",
                        0, visited[0]);
            } catch (SfException expected) {
                // Acceptable alternative: surface a hard error instead of silent empty.
                Assert.assertTrue(
                        "SfException must reference fstat/length failure, got: " + expected.getMessage(),
                        expected.getMessage().toLowerCase().contains("length")
                                || expected.getMessage().toLowerCase().contains("fstat")
                                || expected.getMessage().toLowerCase().contains("stat"));
            }
        });
    }

    /**
     * Tracks every fd that {@code openCleanRW} or {@code openRW} returns and
     * every fd that {@code close} consumes. Lets a test fault {@code fsync} on
     * the freshly-opened fd (the one currently being initialized in
     * {@code createActive}). All other calls delegate to the default facade.
     */
    private static class FdTrackingFacade implements FilesFacade {
        final List<Integer> opened = new ArrayList<>();
        final List<Integer> closed = new ArrayList<>();
        // Set true to fault the NEXT fsync that targets a fd which was just
        // opened (i.e., not yet closed). Auto-reset after firing once.
        volatile boolean failNextFsyncOnNewFd;
        // Set true to fault the NEXT allocNativePath whose path ends in
        // ACTIVE_SUFFIX. Simulates an OOM at the exact moment between
        // openCleanRW and the try-block in createActive. Auto-reset.
        volatile boolean failNextActiveAllocNativePath;
        // Set true to fault the NEXT allocNativePath whose path ends in
        // SEALED_SUFFIX. Simulates an OOM in the rotate-then-reseal path
        // after the file rename succeeded but before the new pointer is
        // installed. Auto-reset.
        volatile boolean failNextSealedAllocNativePath;

        @Override
        public long allocNativePath(String path) {
            // ".sfa" / ".sfs" are SegmentLog.{ACTIVE,SEALED}_SUFFIX
            // (package-private, hardcoded here).
            if (failNextActiveAllocNativePath && path.endsWith(".sfa")) {
                failNextActiveAllocNativePath = false;
                throw CairoException.nonCritical()
                        .put("simulated OOM in allocNativePath: ").put(path);
            }
            if (failNextSealedAllocNativePath && path.endsWith(".sfs")) {
                failNextSealedAllocNativePath = false;
                throw CairoException.nonCritical()
                        .put("simulated OOM in allocNativePath: ").put(path);
            }
            return FilesFacade.INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            int rc = FilesFacade.INSTANCE.close(fd);
            if (rc == 0) {
                closed.add(fd);
            }
            return rc;
        }

        @Override
        public boolean exists(String path) {
            return FilesFacade.INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            FilesFacade.INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return FilesFacade.INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return FilesFacade.INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return FilesFacade.INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return FilesFacade.INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            FilesFacade.INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            if (failNextFsyncOnNewFd && opened.contains(fd) && !closed.contains(fd)) {
                failNextFsyncOnNewFd = false;
                return -1; // simulate EIO
            }
            return FilesFacade.INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return FilesFacade.INSTANCE.length(fd);
        }

        @Override
        public int lock(int fd) {
            return FilesFacade.INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return FilesFacade.INSTANCE.mkdir(path, mode);
        }

        @Override
        public int openCleanRW(String path, long size) {
            int fd = FilesFacade.INSTANCE.openCleanRW(path, size);
            if (fd >= 0) {
                opened.add(fd);
            }
            return fd;
        }

        @Override
        public int openRW(String path) {
            int fd = FilesFacade.INSTANCE.openRW(path);
            if (fd >= 0) {
                opened.add(fd);
            }
            return fd;
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return FilesFacade.INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return FilesFacade.INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return FilesFacade.INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return FilesFacade.INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.write(fd, addr, len, offset);
        }
    }

    /**
     * Lets the first N {@code length(fd)} calls succeed, then returns -1
     * (simulating an {@code fstat} failure on a previously-readable fd).
     */
    private static class FaultyLengthFacade implements FilesFacade {
        int passFirstNLengthCalls;
        int lengthFaultsTriggered;
        private int lengthCalls;

        @Override
        public long allocNativePath(String path) {
            return FilesFacade.INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            return FilesFacade.INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return FilesFacade.INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            FilesFacade.INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return FilesFacade.INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return FilesFacade.INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return FilesFacade.INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return FilesFacade.INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            FilesFacade.INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            return FilesFacade.INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            int n = ++lengthCalls;
            if (n > passFirstNLengthCalls) {
                lengthFaultsTriggered++;
                return -1;
            }
            return FilesFacade.INSTANCE.length(fd);
        }

        @Override
        public int lock(int fd) {
            return FilesFacade.INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return FilesFacade.INSTANCE.mkdir(path, mode);
        }

        @Override
        public int openCleanRW(String path, long size) {
            return FilesFacade.INSTANCE.openCleanRW(path, size);
        }

        @Override
        public int openRW(String path) {
            return FilesFacade.INSTANCE.openRW(path);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return FilesFacade.INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return FilesFacade.INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return FilesFacade.INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return FilesFacade.INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.write(fd, addr, len, offset);
        }
    }

    /**
     * Wraps the default facade and forces the next payload-sized
     * {@code write(...)} call (i.e., the second write of an append, the one
     * that writes the payload bytes) to return a short count, simulating
     * mid-payload ENOSPC.
     */
    private static class ShortPayloadWriteFacade implements FilesFacade {
        // Header writes are exactly FRAME_HEADER_SIZE bytes; payload writes
        // are larger. Use length to disambiguate without inspecting content.
        volatile boolean failNextPayloadWrite;

        @Override
        public long allocNativePath(String path) {
            return FilesFacade.INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            return FilesFacade.INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return FilesFacade.INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            FilesFacade.INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return FilesFacade.INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return FilesFacade.INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return FilesFacade.INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return FilesFacade.INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            FilesFacade.INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            return FilesFacade.INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return FilesFacade.INSTANCE.length(fd);
        }

        @Override
        public int lock(int fd) {
            return FilesFacade.INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return FilesFacade.INSTANCE.mkdir(path, mode);
        }

        @Override
        public int openCleanRW(String path, long size) {
            return FilesFacade.INSTANCE.openCleanRW(path, size);
        }

        @Override
        public int openRW(String path) {
            return FilesFacade.INSTANCE.openRW(path);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return FilesFacade.INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return FilesFacade.INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return FilesFacade.INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return FilesFacade.INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return FilesFacade.INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            // Frame header writes are FRAME_HEADER_SIZE bytes; anything larger
            // is a payload write. Fault only the payload, and only once.
            if (failNextPayloadWrite && len > SegmentLog.FRAME_HEADER_SIZE) {
                failNextPayloadWrite = false;
                // Return a short count to simulate ENOSPC partway through.
                long actual = FilesFacade.INSTANCE.write(fd, addr, len - 1, offset);
                return actual >= 0 ? actual : 0;
            }
            return FilesFacade.INSTANCE.write(fd, addr, len, offset);
        }
    }

    private static String findActivePath(String dir) {
        long find = Files.findFirst(dir);
        if (find == 0) {
            return null;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && name.endsWith(".sfa")) {
                    return dir + "/" + name;
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
        return null;
    }
}
