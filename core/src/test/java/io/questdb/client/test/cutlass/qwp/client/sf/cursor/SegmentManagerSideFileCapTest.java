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
import java.util.concurrent.TimeUnit;

/**
 * Regression for P-C8: the {@code sf_max_total_bytes} cap check compared
 * {@code .sfa} segment bytes only, while the {@code .symbol-dict} side-file
 * is lifetime-monotonic on symbol-heavy workloads -- so a producer could
 * fill the SF filesystem while the cap reported headroom. The manager now
 * reads a per-slot side-file gauge at the provisioning cap check. The two
 * tests differ ONLY in the gauge, proving the gauge alone flips the
 * provisioning decision.
 */
public class SegmentManagerSideFileCapTest {

    private static final long SEGMENT_SIZE = 64 * 1024;
    private String slotDir;

    @Before
    public void setUp() {
        slotDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mgr-sidefile-cap-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (slotDir == null) return;
        rmDirRec(slotDir);
    }

    @Test
    public void testCapCountsSideFileBytesAgainstProvisioning() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Cap = 3 segments. The slot holds 2 recovered segments plus a
            // side-file gauge reporting one further segment's worth of
            // dictionary bytes: 2 x seg (segments) + 1 x seg (side-file)
            // + 1 x seg (the spare under consideration) > cap, so the
            // manager must refuse to provision. Pre-fix the gauge was
            // invisible and the manager provisioned a third .sfa, taking
            // real disk usage past the cap by the side-file's size.
            long cap = 3 * SEGMENT_SIZE;
            prepopulate(slotDir, 2);
            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);

            SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 1_000_000L /* 1ms */, cap);
            try (SegmentManager ignored = manager) {
                manager.start();
                manager.register(ring, slotDir, null, 0L, () -> SEGMENT_SIZE);
                Assert.assertEquals("accounted bytes must be segments + side-file gauge",
                        3 * SEGMENT_SIZE, manager.getCapAccountedBytesForTesting());
                Thread.sleep(100);
            }

            Assert.assertEquals("side-file bytes must count against sf_max_total_bytes -- "
                            + "pre-fix the cap check saw only .sfa bytes and provisioned "
                            + "past the cap",
                    2, countSfaFiles(slotDir));
            ring.close();
        });
    }

    @Test
    public void testControlWithoutSideFileGaugeProvisionsToTheCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Identical setup, no gauge: 2 x seg + 1 x seg spare == cap
            // (not above it), so the manager provisions exactly one spare
            // and then stops. Pins that the refusal in the sibling test is
            // caused by the gauge alone.
            long cap = 3 * SEGMENT_SIZE;
            prepopulate(slotDir, 2);
            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);

            SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 1_000_000L /* 1ms */, cap);
            try (SegmentManager ignored = manager) {
                manager.start();
                manager.register(ring, slotDir, null, 0L, null);
                // Poll instead of a fixed sleep: this is a must-provision assertion, and
                // a flat 100ms sleep flakes on a loaded CI box where the worker's 1ms tick
                // gets delayed past the sleep window. Bound the poll with a deadline so a
                // genuine regression still fails instead of hanging.
                long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (countSfaFiles(slotDir) != 3 && System.nanoTime() < deadlineNanos) {
                    Thread.sleep(5);
                }
            }

            Assert.assertEquals("without the gauge the manager must fill the cap exactly",
                    3, countSfaFiles(slotDir));
            ring.close();
        });
    }

    @Test
    public void testLivenessFloorProvisionsDespiteSideFileBytesOverTheCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The dictionary is lifetime-monotonic and no trim reclaims it, so a
            // side-file that alone eats the cap used to hold the ring at ONE
            // segment forever: no spare, no rotation, and no ack could ever free
            // the shortfall -- a permanent stall that survived restarts, while
            // the disk-full warning pointed at a trim that cannot help. The cap
            // must still guarantee each ring its minimum working set (the active
            // segment plus one spare) and exceed itself by the dictionary's
            // overshoot instead.
            long cap = 3 * SEGMENT_SIZE;
            prepopulate(slotDir, 1);
            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);

            SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 1_000_000L /* 1ms */, cap);
            try (SegmentManager ignored = manager) {
                manager.start();
                // 10 segments' worth of dictionary against a 3-segment cap: no
                // arithmetic makes this fit, so only the floor can provision.
                manager.register(ring, slotDir, null, 0L, () -> 10 * SEGMENT_SIZE);
                long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (countSfaFiles(slotDir) < 2 && System.nanoTime() < deadlineNanos) {
                    Thread.sleep(5);
                }
                Assert.assertEquals("a ring below its minimum working set must be provisioned "
                                + "even when non-reclaimable side-file bytes have eaten the cap",
                        2, countSfaFiles(slotDir));
                // ...and the floor is a floor, not a bypass: once the working set
                // is whole, the cap governs again.
                Thread.sleep(100);
                Assert.assertEquals("the floor must not license unbounded provisioning past the cap",
                        2, countSfaFiles(slotDir));
            }
            ring.close();
        });
    }

    /**
     * Pre-populates {@code dir} with {@code n} valid {@code .sfa} segment
     * files, each containing one frame so {@link SegmentRing#openExisting}
     * doesn't filter them as empty orphans. Each segment's baseSeq is
     * positioned so the contiguity check in {@code openExisting} passes.
     */
    private static void prepopulate(String dir, int n) {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 64; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) i);
            }
            for (int i = 0; i < n; i++) {
                try (MmapSegment seg = MmapSegment.create(
                        dir + "/sf-pre-" + i + ".sfa",
                        i, // baseSeq=0,1 each holding 1 frame -> contiguous
                        SEGMENT_SIZE)) {
                    Assert.assertTrue("setup append should succeed",
                            seg.tryAppend(buf, 64) >= 0);
                }
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
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
