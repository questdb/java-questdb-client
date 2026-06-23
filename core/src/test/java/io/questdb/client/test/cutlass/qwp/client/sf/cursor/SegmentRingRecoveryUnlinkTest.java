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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

/**
 * Regression: {@link SegmentRing#openExisting} must unlink empty
 * {@code .sfa} segments it discards during recovery. Pre-fix it only
 * unmaps + closes the fd, leaving the file on disk forever — every
 * crash cycle that left an unrotated hot spare adds another orphan
 * {@code sf-*.sfa} file that nothing will ever clean up.
 */
public class SegmentRingRecoveryUnlinkTest {

    private static final long SEGMENT_SIZE = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ring-recover-unlink-" + System.nanoTime()).toString();
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

    @Test
    public void testRecoveryUnlinksEmptyOrphanSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Simulate a crashed prior session that left an unrotated hot spare
            // (valid SF01 header, frameCount=0). MmapSegment.create stamps the
            // header but writes no frames.
            String orphanPath = tmpDir + "/sf-orphan.sfa";
            MmapSegment empty = MmapSegment.create(orphanPath, 0L, SEGMENT_SIZE);
            empty.close();
            Assert.assertTrue("setup: orphan .sfa should exist on disk",
                    Files.exists(orphanPath));

            SegmentRing recovered = SegmentRing.openExisting(tmpDir, SEGMENT_SIZE);

            Assert.assertNull(
                    "recovery returned a ring even though the only segment was empty",
                    recovered);
            Assert.assertFalse(
                    "recovery left the empty orphan .sfa on disk — disk leak grows "
                            + "with every crash cycle",
                    Files.exists(orphanPath));
        });
    }

    @Test
    public void testRecoveryUnlinksEmptyOrphansAlongsideValidSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Mix: one valid segment (frameCount > 0) and one empty orphan.
            // Recovery should keep the valid one (return a ring) and unlink the
            // empty one (no longer on disk).
            String validPath = tmpDir + "/sf-valid.sfa";
            MmapSegment valid = MmapSegment.create(validPath, 0L, SEGMENT_SIZE);
            // Append one frame so frameCount = 1 → kept on recovery.
            long buf = io.questdb.client.std.Unsafe.malloc(32,
                    io.questdb.client.std.MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 32; i++) {
                    io.questdb.client.std.Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }
                Assert.assertTrue("setup: valid append should land", valid.tryAppend(buf, 32) >= 0);
            } finally {
                io.questdb.client.std.Unsafe.free(buf, 32,
                        io.questdb.client.std.MemoryTag.NATIVE_DEFAULT);
            }
            valid.close();

            String orphanPath = tmpDir + "/sf-empty-orphan.sfa";
            MmapSegment empty = MmapSegment.create(orphanPath, 1L, SEGMENT_SIZE);
            empty.close();

            Assert.assertTrue("setup: valid .sfa should exist", Files.exists(validPath));
            Assert.assertTrue("setup: orphan .sfa should exist", Files.exists(orphanPath));

            SegmentRing recovered = SegmentRing.openExisting(tmpDir, SEGMENT_SIZE);
            try (SegmentRing ignored = recovered) {
                Assert.assertNotNull("recovery dropped the valid segment", recovered);
                Assert.assertTrue(
                        "recovery should keep the valid segment on disk",
                        Files.exists(validPath));
                Assert.assertFalse(
                        "recovery should unlink the empty orphan .sfa — currently leaks",
                        Files.exists(orphanPath));
            }
        });
    }
}
