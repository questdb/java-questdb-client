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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLockContentionException;
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
                    assertTrue("contention must have a typed signal",
                            expected instanceof SlotLockContentionException);
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
    public void testReleaseConfirmsAndIsIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/verified-release";
            SlotLock lock = SlotLock.acquire(slot);
            assertTrue("first release must confirm success", lock.release());
            // Idempotent: an already-released lock keeps reporting true —
            // callers gating a "slot reusable" signal on it must never see
            // a spurious false after a confirmed release.
            assertTrue("repeat release must stay true", lock.release());
            // close() after release() is a safe no-op (QuietCloseable path).
            lock.close();
            // Confirmed release means the slot is genuinely acquirable.
            try (SlotLock again = SlotLock.acquire(slot)) {
                assertEquals(slot, again.slotDir());
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
            try (SlotLock ignored = SlotLock.acquireLogical(ff, slot, Files.DIR_MODE_SHARED)) {
                assertEquals("the shared lock dir must be created 01777 (sticky), umask-governed",
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
                SlotLock.acquireLogical(ff, slot, Files.DIR_MODE_SHARED);
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

    /**
     * The {@code release() == false} branch: when the OS reports an explicit
     * unlock failure, release must (a) return {@code false} so owners gating a
     * "slot reusable" signal never see a lie, (b) retain the fd because the
     * non-consuming unlock can safely be retried, and (c) keep returning
     * {@code false} while the failure persists. Once unlock succeeds, release
     * confirms and stays confirmed. Swapping in a known-bad descriptor gives
     * the slot-specific native primitive a deterministic unlock failure.
     */
    @Test
    public void testFailedCloseRetainsRetryOwnerUntilNextAcquire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/failed-close";
            SlotLock lock = SlotLock.acquire(slot);
            SlotLock.ReleaseFailureForTesting releaseFailure =
                    lock.injectReleaseFailureForTesting();
            try {
                // CursorSendEngine construction cleanup uses QuietCloseable.close().
                // A failed close must retain an owner that a later acquire can
                // drive, rather than dropping the sole reference and flock fd.
                lock.close();
                assertTrue("failed close must retain the injected descriptor",
                        lock.isReleaseFailureInjectedForTesting());
                try (SlotLock ignored = SlotLock.acquire(slot)) {
                    fail("slot must stay locked while the release failure persists");
                } catch (SlotLockContentionException expected) {
                    // The retained real flock still protects the slot.
                }

                releaseFailure.close();
                try (SlotLock again = SlotLock.acquire(slot)) {
                    assertEquals("the next acquire must retry the retained release owner",
                            slot, again.slotDir());
                } catch (SlotLockContentionException stillHeld) {
                    fail("failed construction cleanup dropped its retry owner: "
                            + stillHeld.getMessage());
                }
            } finally {
                releaseFailure.close();
                lock.release();
            }
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
    public void testFailedCloseRetainsRetryOwnerWithEquivalentPathAlias() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/failed-close-alias";
            SlotLock lock = SlotLock.acquire(slot);
            SlotLock.ReleaseFailureForTesting releaseFailure =
                    lock.injectReleaseFailureForTesting();
            try {
                lock.close();
                assertTrue("failed close must retain the injected descriptor",
                        lock.isReleaseFailureInjectedForTesting());

                // Restore the real descriptor so the retained owner can make
                // progress. The trailing separator preserves the caller's
                // spelling but names the same physical .lock file.
                releaseFailure.close();
                String slotAlias = slot + "/";
                try (SlotLock again = SlotLock.acquire(slotAlias)) {
                    assertEquals("an equivalent path must drive the retained release owner",
                            slotAlias, again.slotDir());
                } catch (SlotLockContentionException stillHeld) {
                    fail("equivalent path spelling could not find its retry owner: "
                            + stillHeld.getMessage());
                }
            } finally {
                releaseFailure.close();
                lock.release();
            }
        });
    }

    @Test
    public void testRemoveOrphanLogicalLeavesAHeldLockFileIntact() throws Exception {
        // removeOrphanLogical must NEVER unlink a lock file another party still holds.
        // Sender.build() holds the logical lock across its construct -> connect ->
        // quarantine transition, and a connect failure closes the engine from inside that
        // scope -- reaching this cleanup while build() is still holding the lock one frame
        // up. Unlinking it there frees the pathname without releasing the flock, so the
        // next acquireLogical creates a SECOND inode and locks it: two owners of a lock
        // whose only job is mutual exclusion. flock is per open-file-description, so a
        // second open+lock contends even within one process -- which is exactly what makes
        // the acquire-before-unlink guard observable here.
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/beta";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            String lockFile = parentDir + "/.slot-locks/beta.lock";
            String pidFile = parentDir + "/.slot-locks/beta.lock.pid";
            try (SlotLock held = SlotLock.acquireLogical(slot)) {
                assertTrue("logical .lock created", Files.exists(lockFile));

                // Cleanup fires while `held` still owns the lock. It must find the lock
                // contended and leave BOTH files on disk.
                SlotLock.removeOrphanLogical(slot);
                assertTrue("a held logical .lock must survive removeOrphanLogical",
                        Files.exists(lockFile));
                assertTrue("a held logical .lock.pid must survive removeOrphanLogical",
                        Files.exists(pidFile));

                // And the holder must still be the sole owner: a fresh acquire contends.
                try {
                    SlotLock.acquireLogical(slot).close();
                    fail("a second acquireLogical must contend while the first is held");
                } catch (IllegalStateException expected) {
                    assertTrue(expected.getMessage(), expected.getMessage().contains("already in use"));
                }
            }
            // Once released, retirement reclaims the files as before -- no leak of the
            // dead lock+pid pair.
            SlotLock.removeOrphanLogical(slot);
            assertFalse("logical .lock reclaimed after release", Files.exists(lockFile));
            assertFalse("logical .lock.pid reclaimed after release", Files.exists(pidFile));
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
    public void testPersistentFailedCloseDoesNotBlockDifferentSlot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String failedSlot = parentDir + "/persistent-failed-close";
            SlotLock failedLock = SlotLock.acquire(failedSlot);
            SlotLock.ReleaseFailureForTesting releaseFailure =
                    failedLock.injectReleaseFailureForTesting();
            try {
                failedLock.close();
                // A repeat close must not enqueue the same intrusive node twice.
                failedLock.close();

                String independentSlot = parentDir + "/independent";
                try (SlotLock independent = SlotLock.acquire(independentSlot)) {
                    assertEquals("a persistent failure on one slot must not block another",
                            independentSlot, independent.slotDir());
                }

                releaseFailure.close();
                String progressSlot = parentDir + "/progress";
                try (SlotLock ignored = SlotLock.acquire(progressSlot)) {
                    // Any cold acquisition drives every failed-close owner.
                }
                try (SlotLock reacquired = SlotLock.acquire(failedSlot)) {
                    assertEquals("successful retry must remove the pending list entry",
                            failedSlot, reacquired.slotDir());
                }
            } finally {
                releaseFailure.close();
                failedLock.release();
            }
        });
    }

    @Test
    public void testFailedUnlockRetainsFdAndReportsFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = parentDir + "/failed-release";
            SlotLock lock = SlotLock.acquire(slot);
            try (SlotLock.ReleaseFailureForTesting ignored = lock.injectReleaseFailureForTesting()) {
                assertFalse("release must report false when explicit unlock fails",
                        lock.release());
                assertTrue("failed unlock must retain the injected fd for a safe retry",
                        lock.isReleaseFailureInjectedForTesting());
                assertFalse("repeat release must stay false while unlock keeps failing",
                        lock.release());
                // While the release is unconfirmed the real flock remains held.
                try (SlotLock ignoredLock = SlotLock.acquire(slot)) {
                    fail("slot must not be acquirable while the original flock fd is still open");
                } catch (IllegalStateException expected) {
                    // good - unconfirmed release really means "still locked".
                }
            }
            assertTrue("release must confirm once explicit unlock succeeds", lock.release());
            assertTrue("confirmed release must stay confirmed", lock.release());
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
