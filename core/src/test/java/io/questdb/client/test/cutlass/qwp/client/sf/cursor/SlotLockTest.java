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
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    public void testLogicalLockRemainsContendedAcrossSlotRenameAndRecreate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Use the platform-native separator. In particular, this exercises
            // acquireLogical with a backslash-only path on Windows.
            String slot = Paths.get(parentDir, "rename").toString();
            String moved = Paths.get(parentDir, "rename.quarantined").toString();
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));

            try (SlotLock ignored = SlotLock.acquireLogical(slot)) {
                assertEquals(0, Files.rename(slot, moved));
                assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));

                try (SlotLock unexpected = SlotLock.acquireLogical(slot)) {
                    fail("logical slot lock must survive rename and recreate");
                } catch (IllegalStateException expected) {
                    assertTrue(expected.getMessage().contains("already in use"));
                }
            }

            try (SlotLock reacquired = SlotLock.acquireLogical(slot)) {
                assertEquals(slot, reacquired.slotDir());
            }
        });
    }

    @Test
    public void testLogicalLockRejectsInvalidPaths() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertLogicalPathRejected(null, "slotDir must not be empty");
            assertLogicalPathRejected("", "slotDir must not be empty");
            assertLogicalPathRejected("slot",
                    "slotDir must contain a parent and slot name: slot");
        });
    }

    @Test
    public void testSharedLockDirectoryIsCreatedUmaskGoverned() throws Exception {
        // .slot-locks is the one directory every sender under an sf_dir must
        // CREATE a file in, so unlike a slot directory it cannot be created
        // 0755: the first process to start would own it and a sender running as
        // a different uid could not create its lock file, failing build(). Pass
        // 0777 and let the deployment's umask decide, exactly as it already does
        // for the sf_dir these live under.
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/mode-check";
            String lockDir = parentDir + "/.slot-locks";
            RecordingMkdirFacade ff = new RecordingMkdirFacade();
            try (SlotLock ignored = SlotLock.acquireLogical(ff, slot)) {
                assertEquals("the shared lock dir must be created umask-governed",
                        Files.DIR_MODE_SHARED, ff.modeFor(lockDir));
            }
            // The per-slot directory keeps the restrictive mode: one process
            // creates it and only that process writes inside it.
            RecordingMkdirFacade slotFf = new RecordingMkdirFacade();
            String ownSlot = parentDir + "/own-slot";
            slotFf.mkdir(ownSlot, Files.DIR_MODE_DEFAULT);
            assertEquals("a slot dir must not be loosened",
                    Files.DIR_MODE_DEFAULT, slotFf.modeFor(ownSlot));
        });
    }

    @Test
    public void testLogicalLockReportsLockDirectoryCreationFailure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/mkdir-failure";
            // Build the lock-dir path exactly as SlotLock.acquireLogical does
            // (parent + "/" + ".slot-locks"), NOT via Paths.get: on Windows
            // Paths.get yields a '\' separator, so the facade's lockDir.equals(path)
            // check never matched the production forward-slash path and the mkdir
            // failure was never injected -- the sole cause of the Windows-only
            // failure of this test.
            String lockDir = parentDir + "/.slot-locks";
            LockDirectoryFailureFacade ff = new LockDirectoryFailureFacade(lockDir);
            try {
                SlotLock.acquireLogical(ff, slot);
                fail("expected logical lock directory creation failure");
            } catch (IllegalStateException expected) {
                assertEquals("could not create logical slot lock dir: " + lockDir + " rc=-1",
                        expected.getMessage());
            }
            assertEquals("lock file must not be opened after directory creation fails",
                    0, ff.openRwCalls);
            assertFalse("failed mkdir must not leave the lock directory behind",
                    Files.exists(lockDir));
        });
    }

    @Test
    public void testRemoveOrphanLogicalDeletesLockAndPidFiles() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/alpha";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            // acquireLogical anchors the lock under the shared parent .slot-locks dir.
            String lockFile = parentDir + "/.slot-locks/alpha.lock";
            String pidFile = parentDir + "/.slot-locks/alpha.lock.pid";
            try (SlotLock ignored = SlotLock.acquireLogical(slot)) {
                assertTrue("logical .lock created", Files.exists(lockFile));
                assertTrue("logical .lock.pid created", Files.exists(pidFile));
            }
            // close() releases the flock but deliberately keeps the file (it must
            // outlast a slot rename); only the fully-drained retirement reclaims it.
            assertTrue("logical .lock survives close", Files.exists(lockFile));
            SlotLock.removeOrphanLogical(slot);
            assertFalse("logical .lock removed on retirement", Files.exists(lockFile));
            assertFalse("logical .lock.pid removed on retirement", Files.exists(pidFile));
        });
    }

    @Test
    public void testRemoveOrphanLogicalIsSilentNoOpWhenAbsentOrInvalid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Never-locked slot: nothing to remove, must not throw.
            SlotLock.removeOrphanLogical(parentDir + "/never-locked");
            // Unlike acquireLogical (which throws on these), the retirement cleanup
            // is best-effort and tolerates unusable input silently.
            SlotLock.removeOrphanLogical(null);
            SlotLock.removeOrphanLogical("");
            SlotLock.removeOrphanLogical("slot"); // no parent component
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

    private static void assertLogicalPathRejected(String slotDir, String expectedMessage) {
        Throwable thrown = null;
        try {
            SlotLock.acquireLogical(slotDir);
        } catch (Throwable t) {
            thrown = t;
        }
        assertTrue("expected IllegalArgumentException", thrown instanceof IllegalArgumentException);
        assertEquals(expectedMessage, thrown.getMessage());
    }

    private static boolean isDir(String path) {
        // Cheap heuristic: directories have a readable findFirst handle.
        long find = Files.findFirst(path);
        if (find <= 0) return false;
        Files.findClose(find);
        return true;
    }

    /** Records the mode each directory was created with, then delegates. */
    private static final class RecordingMkdirFacade extends DelegatingFilesFacade {
        private final java.util.Map<String, Integer> modes = new java.util.HashMap<>();

        int modeFor(String path) {
            Integer mode = modes.get(path);
            return mode == null ? -1 : mode;
        }

        @Override
        public int mkdir(String path, int mode) {
            modes.put(path, mode);
            return super.mkdir(path, mode);
        }
    }

    private static final class LockDirectoryFailureFacade extends DelegatingFilesFacade {
        private final String lockDir;
        private int openRwCalls;

        private LockDirectoryFailureFacade(String lockDir) {
            this.lockDir = lockDir;
        }

        @Override
        public boolean exists(String path) {
            return !lockDir.equals(path) && super.exists(path);
        }

        @Override
        public int mkdir(String path, int mode) {
            return lockDir.equals(path) ? -1 : super.mkdir(path, mode);
        }

        @Override
        public int openRW(String path) {
            openRwCalls++;
            return super.openRW(path);
        }
    }
}
