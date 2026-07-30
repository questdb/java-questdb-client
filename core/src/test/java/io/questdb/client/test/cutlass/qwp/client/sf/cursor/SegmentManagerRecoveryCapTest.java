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

/**
 * Regression: {@link SegmentManager#register} must account for bytes
 * already on disk in the registered ring's slot when seeding its
 * {@code totalBytes} accounting. Pre-fix the manager only adjusted
 * {@code totalBytes} for spares it provisioned and segments it trimmed,
 * so after restart or orphan adoption a slot already at-or-above the
 * cap looked like 0 bytes used and the manager kept provisioning new
 * spares — effectively doubling (or worse) the documented
 * {@code sf_max_total_bytes} cap.
 */
public class SegmentManagerRecoveryCapTest {

    private static final long SEGMENT_SIZE = 64 * 1024;
    private String slotDir;

    @Before
    public void setUp() {
        slotDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mgr-recover-cap-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (slotDir == null) return;
        rmDirRec(slotDir);
    }

    @Test
    public void testManagerHonorsCapAgainstRecoveredSegmentsOnRegister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Cap = exactly 3 segments. Pre-fill the slot with 3 populated
            // segments — that fills the cap on disk before any manager
            // activity. The manager must observe the cap is full and refuse
            // to provision additional spares. Pre-fix: it ignores the
            // recovered bytes and provisions another segment, taking real
            // disk usage to 4 × SEGMENT_SIZE — past the cap.
            long cap = 3 * SEGMENT_SIZE;
            prepopulate(slotDir, 3);

            // Sanity: on-disk state matches expectation.
            Assert.assertEquals("setup precondition: 3 .sfa files on disk",
                    3, countSfaFiles(slotDir));

            SegmentRing ring = SegmentRing.openExisting(slotDir, SEGMENT_SIZE);
            Assert.assertNotNull("recovery should produce a ring", ring);

            SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 1_000_000L /* 1ms */, cap);
            try (SegmentManager ignored = manager) {
                manager.start();
                manager.register(ring, slotDir);
                // Give the manager several ticks. With the bug, it provisions
                // because totalBytes stays at 0 even though the ring already
                // owns 3 × SEGMENT_SIZE.
                Thread.sleep(100);
            }
            // Stop the manager before counting to avoid races with the
            // worker thread mid-provision.

            int sfaAfter = countSfaFiles(slotDir);
            Assert.assertEquals(
                    "manager must respect sf_max_total_bytes against recovered "
                            + "on-disk state — pre-fix register ignored the bytes "
                            + "the recovered ring already owns and over-provisioned "
                            + "past the cap. Saw " + sfaAfter + " .sfa files; "
                            + "expected the original 3 (cap full).",
                    3, sfaAfter);

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
                // baseSeq=0,1,2 each holding 1 frame → contiguous
                try (MmapSegment seg = MmapSegment.create(
                        dir + "/sf-pre-" + i + ".sfa",
                        i, // baseSeq=0,1,2 each holding 1 frame → contiguous
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
