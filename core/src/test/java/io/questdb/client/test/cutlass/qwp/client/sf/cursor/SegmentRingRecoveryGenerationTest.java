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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
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
import java.util.ArrayList;
import java.util.List;

/**
 * {@link SegmentRing#openExisting} computes the maximum on-disk spare file
 * generation during its recovery scan and carries it on the returned ring
 * ({@link SegmentRing#maxRecoveredFileGeneration()}), so that
 * {@link SegmentManager#register} can seed its spare file-generation counter
 * without re-enumerating the directory recovery just walked. These tests pin
 * the two safety properties that make the single-scan optimization sound:
 * <ul>
 *   <li>the carried maximum covers every scanned entry, including files
 *       recovery itself unlinks (empty leftovers) — so generations are never
 *       reused even for files that are gone by registration time;</li>
 *   <li>after recovery + registration, freshly minted spares never collide
 *       with (and therefore never truncate) a recovered on-disk segment.</li>
 * </ul>
 */
public class SegmentRingRecoveryGenerationTest {

    private static final long SEGMENT_SIZE = 64 * 1024;
    private String slotDir;

    @Before
    public void setUp() {
        slotDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ring-recover-gen-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (slotDir == null) return;
        rmDirRec(slotDir);
    }

    @Test
    public void testRecoveredRingCarriesMaxFileGeneration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Mixed naming, mirroring a real slot: the legacy initial file
            // (no generation) plus two generation-named spares that were
            // rotated into service. Non-generation names must not contribute.
            createSegmentWithOneFrame(slotDir + "/sf-initial.sfa", 0L);
            createSegmentWithOneFrame(slotDir + "/sf-0000000000000005.sfa", 1L);
            createSegmentWithOneFrame(slotDir + "/sf-0000000000000007.sfa", 2L);

            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);
            try {
                Assert.assertEquals(
                        "recovered ring must report the highest generation seen by the scan",
                        7L, ring.maxRecoveredFileGeneration());
            } finally {
                ring.close();
            }
        });
    }

    @Test
    public void testConstructorRingReportsUnknownGeneration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            MmapSegment initial = MmapSegment.createInMemory(0L, SEGMENT_SIZE);
            SegmentRing ring = new SegmentRing(initial, SEGMENT_SIZE);
            try {
                Assert.assertEquals(
                        "constructor-built rings carry no recovery scan result; register()"
                                + " must fall back to its own directory scan for them",
                        SegmentRing.FILE_GENERATION_UNKNOWN, ring.maxRecoveredFileGeneration());
            } finally {
                ring.close();
            }
        });
    }

    @Test
    public void testGenerationIncludesFilesRecoveryUnlinks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // One valid segment at generation 2 and one EMPTY hot-spare
            // leftover at generation 0xa. Recovery unlinks the empty leftover,
            // but its generation must still count towards the maximum: a
            // generation is never reused, so the bound must cover every entry
            // the scan visited, not just the survivors.
            createSegmentWithOneFrame(slotDir + "/sf-0000000000000002.sfa", 0L);
            MmapSegment empty = MmapSegment.create(
                    slotDir + "/sf-000000000000000a.sfa", 0L, SEGMENT_SIZE);
            empty.close();

            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);
            try {
                Assert.assertFalse("empty leftover should have been unlinked",
                        Files.exists(slotDir + "/sf-000000000000000a.sfa"));
                Assert.assertEquals(
                        "the unlinked leftover's generation (0xa) must dominate the"
                                + " survivor's (2) in the carried maximum",
                        10L, ring.maxRecoveredFileGeneration());
            } finally {
                ring.close();
            }
        });
    }

    @Test
    public void testManagerMintsNonCollidingSpareAfterRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // End-to-end guard for the property the whole generation dance
            // exists for: after recovery + registration, a freshly minted
            // spare must land at a generation strictly above everything on
            // disk — never at a name whose truncating create would destroy a
            // recovered segment.
            createSegmentWithOneFrame(slotDir + "/sf-0000000000000005.sfa", 0L);
            createSegmentWithOneFrame(slotDir + "/sf-0000000000000007.sfa", 1L);

            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);
            try {
                Assert.assertEquals(7L, ring.maxRecoveredFileGeneration());

                // Generous cap so the manager provisions a spare for the
                // born-full recovered active on its first ticks.
                SegmentManager manager = new SegmentManager(
                        SEGMENT_SIZE, 1_000_000L /* 1ms */, 100 * SEGMENT_SIZE);
                try (SegmentManager ignored = manager) {
                    manager.start();
                    manager.register(ring, slotDir);
                    // Poll (bounded) for the spare to appear instead of a fixed
                    // sleep — keeps the test fast locally and non-flaky in CI.
                    long deadline = System.currentTimeMillis() + 10_000;
                    while (countSfaFiles(slotDir) < 3 && System.currentTimeMillis() < deadline) {
                        Thread.sleep(1);
                    }
                }
                Assert.assertTrue("manager should have provisioned at least one spare",
                        countSfaFiles(slotDir) >= 3);

                // Both recovered segments must still exist, and every
                // generation-named file must be one of the originals (5, 7) or a
                // newly minted spare strictly above the recovered maximum.
                Assert.assertTrue(Files.exists(slotDir + "/sf-0000000000000005.sfa"));
                Assert.assertTrue(Files.exists(slotDir + "/sf-0000000000000007.sfa"));
                for (long gen : listGenerations(slotDir)) {
                    Assert.assertTrue(
                            "unexpected spare generation " + gen + " — a spare minted at or"
                                    + " below the recovered maximum would have truncated a"
                                    + " recovered segment via openCleanRW",
                            gen == 5L || gen == 7L || gen > 7L);
                }
            } finally {
                ring.close();
            }
        });
    }

    /**
     * Creates a valid {@code .sfa} segment at {@code path} holding exactly one
     * frame so {@link SegmentRing#openExisting} doesn't filter it as an empty
     * orphan. Callers keep baseSeqs contiguous (one frame per segment).
     */
    private static void createSegmentWithOneFrame(String path, long baseSeq) {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 64; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) i);
            }
            try (MmapSegment seg = MmapSegment.create(path, baseSeq, SEGMENT_SIZE)) {
                Assert.assertTrue("setup append should succeed", seg.tryAppend(buf, 64) >= 0);
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /** Test-side oracle for {@code sf-<gen:016x>.sfa} names. */
    private static List<Long> listGenerations(String dir) {
        List<Long> generations = new ArrayList<>();
        long find = Files.findFirst(dir);
        if (find <= 0) return generations;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
                if (name == null || !name.startsWith("sf-") || !name.endsWith(".sfa")) {
                    continue;
                }
                String hex = name.substring(3, name.length() - 4);
                if (hex.length() != 16) continue;
                try {
                    generations.add(Long.parseUnsignedLong(hex, 16));
                } catch (NumberFormatException ignored) {
                }
            }
        } finally {
            Files.findClose(find);
        }
        return generations;
    }

    private static int countSfaFiles(String dir) {
        if (!Files.exists(dir)) return 0;
        long find = Files.findFirst(dir);
        if (find <= 0) return 0;
        int n = 0;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && name.endsWith(".sfa")) n++;
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
        return n;
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
}
