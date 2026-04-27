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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;

/**
 * Concurrent regression for the {@code SegmentManager} worker race vs
 * ring deregister/close.
 * <p>
 * The manager's worker loop snapshots {@code rings} under a lock, then
 * services each ring outside the lock. If a user thread calls
 * {@code deregister(ring)} + {@code ring.close()} between the snapshot
 * and {@code installHotSpare}, the manager:
 * <ul>
 *   <li>creates a new {@code MmapSegment} (mmap + fd + on-disk file)</li>
 *   <li>calls {@code ring.installHotSpare(spare)} on the closed ring —
 *       which sees {@code hotSpare == null} (just zeroed by close) and
 *       silently accepts the install</li>
 * </ul>
 * The spare's mmap + fd are now permanently leaked: nothing will ever
 * close them because {@code close()} already ran.
 * <p>
 * Detection: after the manager has joined, reflect into each closed
 * ring's {@code hotSpare} field. A non-null value means a spare was
 * installed AFTER {@code close()} zeroed the field — i.e. exactly the
 * leak path. We close any survivors so the test itself doesn't leak.
 */
public class SegmentManagerCloseRaceTest {

    private static final int ITERATIONS = 200;
    private static final long SEGMENT_SIZE = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mgr-close-race-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, 0755));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        cleanupRecursively(tmpDir);
        Files.remove(tmpDir);
    }

    @Test
    public void testManagerDoesNotInstallSpareIntoClosedRing() throws Exception {
        // Aggressive 1us poll so the worker is almost always running
        // serviceRing — maximizes overlap with concurrent deregister/close.
        SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 1_000L,
                Long.MAX_VALUE);
        manager.start();

        SegmentRing[] rings = new SegmentRing[ITERATIONS];
        String[] slots = new String[ITERATIONS];
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                String slot = tmpDir + "/slot-" + i;
                Assert.assertEquals(0, Files.mkdir(slot, 0755));
                slots[i] = slot;
                MmapSegment initial = MmapSegment.create(
                        slot + "/sf-initial.sfa", 0L, SEGMENT_SIZE);
                rings[i] = new SegmentRing(initial, SEGMENT_SIZE);
                manager.register(rings[i], slot);
                // Immediately deregister + close. The manager may be mid-
                // serviceRing for this very ring, having already created a
                // spare and not yet installed it — that's the race window.
                manager.deregister(rings[i]);
                rings[i].close();
            }
        } finally {
            // join the worker so any in-flight serviceRing finishes
            // BEFORE we inspect the rings — otherwise a later install
            // could escape detection.
            manager.close();
        }

        Field hotSpareField = SegmentRing.class.getDeclaredField("hotSpare");
        hotSpareField.setAccessible(true);

        int leaked = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            Object hs = hotSpareField.get(rings[i]);
            if (hs != null) {
                leaked++;
                // Don't leak in the test: close the survivor.
                ((MmapSegment) hs).close();
            }
        }

        Assert.assertEquals(
                "SegmentManager installed hot spares into closed rings — "
                        + "spare mmap/fd permanently leaked",
                0, leaked);
    }

    private static void cleanupRecursively(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find == 0) return;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && !".".equals(name) && !"..".equals(name)) {
                    String child = dir + "/" + name;
                    // best-effort: try as file; if remove fails, recurse.
                    if (!Files.remove(child)) {
                        cleanupRecursively(child);
                        Files.remove(child);
                    }
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
    }
}
