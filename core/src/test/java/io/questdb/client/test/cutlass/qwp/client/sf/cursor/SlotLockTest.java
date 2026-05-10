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

import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SlotLockTest {

    private String parentDir;

    @Before
    public void setUp() {
        parentDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-slotlock-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(parentDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (parentDir == null) return;
        // Recursively (one level deep is enough for our test layout) wipe.
        rmDir(parentDir);
    }

    @Test
    public void testAcquireCreatesSlotDirAndLockFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/alpha";
            try (SlotLock lock = SlotLock.acquire(slot)) {
                assertTrue("slot dir created", Files.exists(slot));
                assertTrue(".lock file created", Files.exists(slot + "/.lock"));
                assertEquals(slot, lock.slotDir());
            }
        });
    }

    @Test
    public void testSecondAcquireFailsOnLockContention() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/contended";
            try (SlotLock ignored1 = SlotLock.acquire(slot)) {
                try (SlotLock ignored = SlotLock.acquire(slot)) {
                    fail("expected slot contention to throw");
                } catch (IllegalStateException expected) {
                    String msg = expected.getMessage();
                    assertTrue("error must mention contention: " + msg,
                            msg.contains("already in use"));
                    assertTrue("error must include slot path: " + msg,
                            msg.contains(slot));
                    // Holder PID must be in the diagnostic — that's the whole
                    // point of writing PID into the lock file.
                    assertTrue("error must mention pid: " + msg,
                            msg.contains("pid="));
                }
            }
        });
    }

    @Test
    public void testCloseReleasesLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/release";
            try (SlotLock ignored = SlotLock.acquire(slot)) {
                // explicit no-op; close happens via try-with-resources
            }
            // After release, a fresh acquire should succeed.
            try (SlotLock again = SlotLock.acquire(slot)) {
                assertEquals(slot, again.slotDir());
            }
        });
    }

    @Test
    public void testTwoDifferentSlotsCoexist() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slotA = parentDir + "/a";
            String slotB = parentDir + "/b";
            try (SlotLock la = SlotLock.acquire(slotA);
                 SlotLock lb = SlotLock.acquire(slotB)) {
                assertEquals(slotA, la.slotDir());
                assertEquals(slotB, lb.slotDir());
            }
        });
    }

    private static void rmDir(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        // One level recursion — our test layout never goes deeper.
                        if (Files.exists(child) && isDir(child)) {
                            rmDir(child);
                        } else {
                            Files.remove(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    private static boolean isDir(String path) {
        // Cheap heuristic: directories have a readable findFirst handle.
        long find = Files.findFirst(path);
        if (find <= 0) return false;
        Files.findClose(find);
        return true;
    }
}
