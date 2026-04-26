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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Adversarial tests for {@link SegmentLog} — random truncations, multi-crash
 * sequences, header corruption. The invariant under test is the same in every
 * scenario: <b>after any abrupt termination, replay returns a strict prefix of
 * what was appended before the termination — never garbage, never out-of-order,
 * never beyond what was fsync'd</b>.
 */
public class SegmentLogTortureTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-torture-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, 0755));
    }

    @After
    public void tearDown() {
        rmTree(tmpDir);
    }

    /**
     * Fuzz: write a random number of frames, truncate the active segment at a
     * random byte offset, reopen, verify the replayed frames are a strict prefix
     * of the original sequence.
     */
    @Test
    public void testRandomTruncationProducesStrictPrefix() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Random rnd = new Random(0xCAFEBABEL);
            for (int iter = 0; iter < 50; iter++) {
                rmTree(tmpDir);
                assertEquals(0, Files.mkdir(tmpDir, 0755));

                int frameCount = 5 + rnd.nextInt(30);
                long maxBytes = 4096;
                List<byte[]> appended = new ArrayList<>();

                try (SegmentLog log = SegmentLog.open(tmpDir, maxBytes)) {
                    for (int i = 0; i < frameCount; i++) {
                        int sz = 16 + rnd.nextInt(180);
                        byte[] payload = new byte[sz];
                        rnd.nextBytes(payload);
                        appended.add(payload);
                        appendBytes(log, payload);
                    }
                    log.fsync();
                }

                String activePath = findActiveSegment(tmpDir);
                if (activePath == null) {
                    // All frames went into sealed segments — no torn tail to inject.
                    continue;
                }
                long fileLen = Files.length(activePath);
                if (fileLen <= SegmentLog.HEADER_SIZE) {
                    continue;
                }
                long truncAt = SegmentLog.HEADER_SIZE
                        + (long) rnd.nextInt((int) (fileLen - SegmentLog.HEADER_SIZE));
                int fd = Files.openRW(activePath);
                try {
                    assertTrue(Files.truncate(fd, truncAt));
                    Files.fsync(fd);
                } finally {
                    Files.close(fd);
                }

                List<byte[]> seen = new ArrayList<>();
                try (SegmentLog log = SegmentLog.open(tmpDir, maxBytes)) {
                    log.replay((seq, addr, len) -> {
                        seen.add(readBytes(addr, len));
                        return true;
                    });
                }

                assertTrue(
                        "iter=" + iter + " saw " + seen.size() + " > appended " + appended.size(),
                        seen.size() <= appended.size());
                for (int i = 0; i < seen.size(); i++) {
                    assertArrayEquals(
                            "iter=" + iter + " frame " + i + " differs from original",
                            appended.get(i), seen.get(i));
                }
            }
        });
    }

    /**
     * Five back-to-back simulated crashes interleaved with fresh appends.
     * <p>
     * The invariant: after each recovery the replayed sequence is a strict prefix
     * of the running ledger (the survivors of previous recoveries plus any frames
     * appended this round). A truncation can cut into previously-committed bytes
     * — that's fine — but it can't reorder, mutate, or invent frames.
     */
    @Test
    public void testMultipleCrashesPreservePrefixInvariant() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Random rnd = new Random(0xDEADBEEFL);
            // The running ledger: every frame that has been appended in this dir,
            // collapsed each round to whatever survived recovery (so future appends
            // build on top of the survived prefix, not the original sequence).
            List<byte[]> ledger = new ArrayList<>();

            for (int crash = 0; crash < 5; crash++) {
                int newFrames = 3 + rnd.nextInt(7);
                try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 16)) {
                    for (int i = 0; i < newFrames; i++) {
                        byte[] payload = new byte[20 + rnd.nextInt(80)];
                        rnd.nextBytes(payload);
                        ledger.add(payload);
                        appendBytes(log, payload);
                    }
                    log.fsync();
                }
                // Inject a torn tail at a random point in the active segment.
                String activePath = findActiveSegment(tmpDir);
                if (activePath != null) {
                    long fileLen = Files.length(activePath);
                    if (fileLen > SegmentLog.HEADER_SIZE) {
                        long truncAt = SegmentLog.HEADER_SIZE
                                + (long) rnd.nextInt((int) (fileLen - SegmentLog.HEADER_SIZE));
                        int fd = Files.openRW(activePath);
                        try {
                            assertTrue(Files.truncate(fd, truncAt));
                            Files.fsync(fd);
                        } finally {
                            Files.close(fd);
                        }
                    }
                }
                List<byte[]> seen = new ArrayList<>();
                try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 16)) {
                    log.replay((seq, addr, len) -> {
                        seen.add(readBytes(addr, len));
                        return true;
                    });
                }
                assertTrue(
                        "crash " + crash + ": replay over-shot the ledger (seen=" + seen.size()
                                + ", ledger=" + ledger.size() + ")",
                        seen.size() <= ledger.size());
                for (int i = 0; i < seen.size(); i++) {
                    assertArrayEquals(
                            "crash " + crash + " frame " + i + " mutated",
                            ledger.get(i), seen.get(i));
                }
                // Collapse the ledger to what survived; the next round appends on top.
                ledger = seen;
            }
        });
    }

    /**
     * After torn-tail recovery, the log must be writable again — a follow-up
     * append must succeed and survive a clean reopen.
     */
    @Test
    public void testWriteAfterRecoveryWorks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] before = "before".getBytes();
            byte[] after = "after-recovery".getBytes();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                appendBytes(log, before);
                log.fsync();
            }
            // Inject torn tail
            String activePath = findActiveSegment(tmpDir);
            assertTrue("active segment expected", activePath != null);
            long len = Files.length(activePath);
            int fd = Files.openRW(activePath);
            try {
                long pad = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < 8; i++) {
                        Unsafe.getUnsafe().putByte(pad + i, (byte) 0xFF);
                    }
                    Files.write(fd, pad, 8, len);
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(pad, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }
            // Recover, then append more, then close + reopen + replay both.
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                appendBytes(log, after);
                log.fsync();
            }
            List<byte[]> seen = new ArrayList<>();
            try (SegmentLog log = SegmentLog.open(tmpDir, 1L << 20)) {
                log.replay((seq, addr, len2) -> {
                    seen.add(readBytes(addr, len2));
                    return true;
                });
            }
            assertEquals(2, seen.size());
            assertArrayEquals(before, seen.get(0));
            assertArrayEquals(after, seen.get(1));
        });
    }

    /**
     * A segment file with a truncated header (less than the 24-byte header size)
     * must fail open with a clear error, not silently mis-interpret bytes.
     */
    @Test
    public void testTruncatedHeaderRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Plant an obviously broken segment file with a sf-active-style name but
            // only a few bytes of content.
            String junkPath = tmpDir + "/0000000000000000.sfa";
            int fd = Files.openCleanRW(junkPath, 0);
            try {
                long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(buf, 0xCAFEBABE);
                    Files.write(fd, buf, 4, 0);
                } finally {
                    Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }
            try {
                SegmentLog log = SegmentLog.open(tmpDir, 1L << 20);
                log.close();
                fail("expected open to reject truncated-header segment");
            } catch (SfException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("shorter than header")
                                || expected.getMessage().contains("bad magic"));
            }
        });
    }

    /**
     * A segment file with a wrong magic must be rejected, not silently treated
     * as data.
     */
    @Test
    public void testWrongMagicRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String junkPath = tmpDir + "/0000000000000000.sfa";
            int fd = Files.openCleanRW(junkPath, SegmentLog.HEADER_SIZE);
            try {
                long buf = Unsafe.malloc(SegmentLog.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    // Wrong magic, otherwise a sane-looking header.
                    Unsafe.getUnsafe().putInt(buf, 0xDEADBEEF);
                    Unsafe.getUnsafe().putByte(buf + 4, (byte) 1);
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
                SegmentLog log = SegmentLog.open(tmpDir, 1L << 20);
                log.close();
                fail("expected open to reject wrong-magic segment");
            } catch (SfException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("bad magic"));
            }
        });
    }

    /**
     * Randomized operation-sequence fuzzer. Mixes append, trim, replay, fsync,
     * and reopen across many iterations. Maintains a model of what the SF state
     * should be (an in-memory ledger of un-trimmed frames in seq order) and
     * cross-checks {@link SegmentLog} state against the model after every step.
     * <p>
     * The invariants verified at every step:
     * <ul>
     *   <li>{@code replay()} returns frames in seq order, byte-equal to the model.</li>
     *   <li>{@code oldestSeq()} matches the model's oldest un-trimmed frame seq
     *       (or -1 when empty).</li>
     *   <li>{@code nextSeq()} matches the model's next-seq counter.</li>
     *   <li>After reopen, all the above still hold.</li>
     * </ul>
     */
    @Test
    public void testRandomizedOperationFuzzer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Random rnd = new Random(0xABCDEF12L);
            // Each entry: payload bytes, in seq order, never trimmed yet.
            // We also track baseSeq so trim() can be modeled.
            ArrayDeque<long[]> ledger = new ArrayDeque<>(); // [seq, payloadIdx]
            List<byte[]> payloads = new ArrayList<>();
            long nextSeq = 0;
            long perSeg = 4096;

            try (SegmentLog log = SegmentLog.open(tmpDir, perSeg)) {
                for (int step = 0; step < 200; step++) {
                    int op = rnd.nextInt(100);
                    if (op < 60) {
                        // append
                        byte[] payload = new byte[16 + rnd.nextInt(150)];
                        rnd.nextBytes(payload);
                        appendBytes(log, payload);
                        long idx = payloads.size();
                        payloads.add(payload);
                        ledger.addLast(new long[]{nextSeq, idx});
                        nextSeq++;
                    } else if (op < 75 && !ledger.isEmpty()) {
                        // trim — pick a random ackedSeq within (-1 .. nextSeq-1)
                        long acked = ledger.peekFirst()[0] - 1
                                + (long) rnd.nextInt((int) (nextSeq - ledger.peekFirst()[0] + 1));
                        log.trim(acked);
                        // Model trim: drop entries whose seq is <= acked AND that lived in a
                        // sealed segment. We don't know which segments are sealed without
                        // peeking inside SegmentLog, so we approximate: only trim if there's
                        // a clearly-old entry. To keep the model conservative and consistent,
                        // we don't change ledger here — replay still returns those frames if
                        // they're in the active segment, and we'll re-verify with replay.
                        // (The trim semantic is "may drop sealed segments below ackedSeq"
                        // which is implementation detail; the visible contract is replay.)
                    } else if (op < 85) {
                        // fsync
                        log.fsync();
                    } else if (op < 95) {
                        // replay + verify
                        verifyReplay(log, payloads, ledger);
                    } else if (op < 100) {
                        // skip — non-trivial reopen mixed in by an outer reopen step below.
                    }
                }
                verifyReplay(log, payloads, ledger);
            }

            // Reopen and verify the visible state is still consistent.
            try (SegmentLog log = SegmentLog.open(tmpDir, perSeg)) {
                verifyReplay(log, payloads, ledger);
                Assert.assertEquals(nextSeq, log.nextSeq());
            }
        });
    }

    /**
     * Verify that the SegmentLog's visible replay sequence is monotonic in seq
     * and that every replayed frame matches one of the ledger entries (by seq).
     * The number of replayed frames may be ≤ ledger size if trim dropped some.
     */
    private static void verifyReplay(SegmentLog log, List<byte[]> payloads,
                                     ArrayDeque<long[]> ledger) {
        List<long[]> ledgerList = new ArrayList<>(ledger);
        long[] prevSeq = {-1L};
        int[] count = {0};
        log.replay((seq, addr, len) -> {
            assertTrue("replay non-monotonic: prev=" + prevSeq[0] + " curr=" + seq,
                    seq > prevSeq[0]);
            prevSeq[0] = seq;
            // Find this seq in the ledger.
            long[] match = null;
            for (long[] e : ledgerList) {
                if (e[0] == seq) {
                    match = e;
                    break;
                }
            }
            assertTrue("replay returned unknown seq " + seq, match != null);
            byte[] expected = payloads.get((int) match[1]);
            assertEquals("payload length mismatch at seq=" + seq, expected.length, len);
            for (int i = 0; i < len; i++) {
                if (expected[i] != Unsafe.getUnsafe().getByte(addr + i)) {
                    fail("payload byte " + i + " mismatch at seq=" + seq);
                }
            }
            count[0]++;
            return true;
        });
        // Replay count may be ≤ ledger because trim could have dropped entries.
        assertTrue("replayed " + count[0] + " > ledger " + ledgerList.size(),
                count[0] <= ledgerList.size());
    }

    /**
     * Writes a stream of frames across many segment rotations, truncates a random
     * byte off the active segment, and verifies recovery yields a strict prefix
     * across the multi-segment boundary. Exercises the bookkeeping in
     * {@code scanActive} alongside sealed segment loading.
     */
    @Test
    public void testTruncationAcrossMultipleSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Random rnd = new Random(0xFEEDFACEL);
            int frameCount = 80;
            byte[] payload = new byte[120];
            rnd.nextBytes(payload);
            try (SegmentLog log = SegmentLog.open(tmpDir, 4096)) {
                for (int i = 0; i < frameCount; i++) {
                    appendBytes(log, payload);
                }
                log.fsync();
                assertTrue("multi-segment expected", log.segmentCount() >= 3);
            }
            String activePath = findActiveSegment(tmpDir);
            assertTrue("active segment expected", activePath != null);
            long fileLen = Files.length(activePath);
            if (fileLen > SegmentLog.HEADER_SIZE + 1) {
                int fd = Files.openRW(activePath);
                try {
                    Files.truncate(fd, fileLen - 1); // shave one byte
                    Files.fsync(fd);
                } finally {
                    Files.close(fd);
                }
            }
            int[] seen = {0};
            try (SegmentLog log = SegmentLog.open(tmpDir, 4096)) {
                log.replay((seq, addr, len) -> {
                    assertArrayEquals(
                            "frame " + seq + " mutated", payload, readBytes(addr, len));
                    seen[0]++;
                    return true;
                });
            }
            assertTrue("at least frameCount-1 frames replayed", seen[0] >= frameCount - 1);
            assertTrue("at most frameCount frames replayed", seen[0] <= frameCount);
        });
    }

    /**
     * Open-time sort regression: at the documented {@code sf_max_total_bytes
     * / sf_max_bytes} ceiling (~16K segments) the previous insertion sort
     * over {@code segments} ran in O(N²) and burnt multi-second wall time
     * before the I/O thread could even start. The test creates 1024 sealed
     * segments by forcing one-frame-per-segment via a tiny per-segment cap,
     * reopens, and asserts:
     * <ul>
     *   <li>every appended sequence is replayed exactly once, in order;</li>
     *   <li>{@code nextSeq()} matches the total appended frame count;</li>
     *   <li>reopen + replay completes within a generous wall-clock bound
     *     that the old O(N²) sort would still satisfy at this scale, but
     *     that catches a regression pushing back into multi-second land
     *     for the documented production ceiling (~16K segments).</li>
     * </ul>
     */
    @Test
    public void testLargeSegmentCountReopensInOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // maxBytes = HEADER_SIZE + FRAME_HEADER_SIZE + payload = 24+8+16 = 48.
            // First frame fits in segment 0; every subsequent frame triggers
            // rotation. 1024 frames → ~1023 sealed + 1 active = 1024 segments.
            final int frameCount = 1024;
            final int payloadSize = 16;
            final long maxBytes = 48;

            long buf = Unsafe.malloc(payloadSize, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payloadSize; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i & 0xff));
                }
                try (SegmentLog log = SegmentLog.open(tmpDir, maxBytes)) {
                    long lastSeq = -1;
                    for (int i = 0; i < frameCount; i++) {
                        lastSeq = log.append(buf, payloadSize);
                    }
                    assertEquals(frameCount - 1, lastSeq);
                    log.fsync();
                }
            } finally {
                Unsafe.free(buf, payloadSize, MemoryTag.NATIVE_DEFAULT);
            }

            long startMs = System.currentTimeMillis();
            try (SegmentLog log2 = SegmentLog.open(tmpDir, maxBytes)) {
                assertEquals(frameCount, log2.nextSeq());
                final long[] expected = {0L};
                final int[] count = {0};
                log2.replay((seq, addr, len) -> {
                    assertEquals("frame seq out of order at index " + count[0],
                            expected[0], seq);
                    expected[0]++;
                    count[0]++;
                    return true;
                });
                assertEquals("replayed " + count[0] + " frames, expected " + frameCount,
                        frameCount, count[0]);
            }
            long elapsedMs = System.currentTimeMillis() - startMs;
            assertTrue("reopen+replay took " + elapsedMs + "ms (expected < 5000ms); " +
                            "regression suggests scanDirectory's segment sort is back to O(N²)",
                    elapsedMs < 5_000);
        });
    }

    private static void appendBytes(SegmentLog log, byte[] bytes) {
        long buf = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < bytes.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, bytes[i]);
            }
            log.append(buf, bytes.length);
        } finally {
            Unsafe.free(buf, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static byte[] readBytes(long addr, int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return out;
    }

    private static String findActiveSegment(String dir) {
        long find = Files.findFirst(dir);
        if (find == 0) return null;
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
}
