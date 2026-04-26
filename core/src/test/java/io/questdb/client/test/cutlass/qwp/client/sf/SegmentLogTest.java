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

import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.cutlass.qwp.client.sf.SfDiskFullException;
import io.questdb.client.cutlass.qwp.client.sf.SfException;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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

    @Test
    public void testTrimNeverDeletesActive() throws Exception {
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
                // ack way past everything; active is unsealed so must remain.
                log.trim(Long.MAX_VALUE / 2);
                assertEquals(1, log.segmentCount());
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    count[0]++;
                    return true;
                });
                assertEquals(2, count[0]);
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

            // On reopen the corrupted frame is in a "valid-length but bad-CRC" state.
            // Recovery scan stops at first bad CRC and truncates: the file becomes
            // header-only, so 0 frames replay.
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                int[] count = {0};
                log.replay((seq, addr, len) -> {
                    count[0]++;
                    return true;
                });
                assertEquals(0, count[0]);
                assertEquals(0, log.nextSeq());
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
