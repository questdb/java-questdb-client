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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Red test for M5 — {@link CursorSendEngine#close()} leaks the slot lock
 * if any step between {@code manager.deregister} and the slotLock cleanup
 * throws.
 *
 * <p>The current sequence in {@code close()} is bare statements, no
 * try/finally:
 * <pre>
 *   manager.deregister(ring);
 *   if (ownsManager) manager.close();
 *   ring.close();                           // can throw
 *   if (fullyDrained) unlinkAllSegmentFiles(sfDir);  // can throw
 *   if (slotLock != null) try { slotLock.close(); } catch (Throwable ignored) {}
 * </pre>
 * If any of the first four steps throws, the slotLock cleanup is skipped
 * — the {@code .lock} fd survives until JVM exit. Tests, multi-engine
 * usage and any path that constructs a fresh sender for the same slot
 * after a close failure will collide on a lock the kernel still holds for
 * the dead engine.
 *
 * <p>The test injects an NPE into {@code ring.close()} by reflectively
 * setting the engine's {@code ring} field to {@code null}. The current
 * code propagates the NPE before reaching slotLock cleanup. After the
 * fix (wrap the close steps in try/finally so slotLock.close() always
 * runs), the slot is releasable by a fresh sender and the test goes green.
 *
 * <p>The end-to-end signal is "can a fresh {@code SlotLock.acquire} on
 * the same slot dir succeed?" — the user-visible consequence of a leaked
 * flock.
 */
public class EngineCloseSlotLockReleaseTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-engine-close-leak-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (sfDir == null) return;
        rmDirRecursive(sfDir);
    }

    private static void rmDirRecursive(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        long probe = Files.findFirst(child);
                        if (probe > 0) {
                            Files.findClose(probe);
                            rmDirRecursive(child);
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

    /**
     * A close driven by a caller that HOLDS the logical slot lock must not unlink it.
     * <p>
     * A fresh slot is "fully drained" by definition ({@code publishedFsn() < 0}), and
     * {@code Sender.build()} closes the engine from inside its {@code acquireLogical}
     * scope whenever connect fails -- the ordinary "server isn't up yet" startup. An
     * unconditional unlink there frees the pathname while build() still holds the flock,
     * so on POSIX the next acquirer creates a SECOND inode and locks it successfully:
     * two owners of the lock that exists solely to serialise the quarantine
     * close-&gt;rename-&gt;recreate window.
     * <p>
     * Swap {@code close(false)} for {@code close()} and the second acquireLogical below
     * succeeds instead of throwing.
     */
    @Test(timeout = 10_000L)
    public void testCloseDoesNotUnlinkALogicalLockItsCallerHolds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slotDir = sfDir + "/slot0";
            assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
            String logicalLockPath = sfDir + "/.slot-locks/slot0.lock";

            try (SlotLock held = SlotLock.acquireLogical(slotDir)) {
                assertTrue("scaffolding: the logical lock file must exist once acquired",
                        Files.exists(logicalLockPath));

                // A fresh slot: publishedFsn() < 0, so close() takes the fully-drained arm.
                CursorSendEngine engine = new CursorSendEngine(slotDir, 4L * 1024 * 1024);
                engine.close(false);

                assertTrue("close(false) must leave the logical lock file alone -- its caller "
                                + "still holds the flock on it",
                        Files.exists(logicalLockPath));
                try {
                    SlotLock stolen = SlotLock.acquireLogical(slotDir);
                    stolen.close();
                    fail("the logical lock was voided while still held: a second acquireLogical "
                            + "succeeded, so two parties now believe they own the slot");
                } catch (IllegalStateException expected) {
                    assertTrue("expected lock contention, got: " + expected.getMessage(),
                            expected.getMessage().contains("already in use"));
                }
            }
        });
    }

    /**
     * The counterpart: a close by a caller that does NOT hold the logical lock still
     * reclaims it, so {@code .slot-locks} does not accumulate a dead lock+pid pair per
     * distinct slot name for the lifetime of {@code sf_dir}.
     */
    @Test(timeout = 10_000L)
    public void testFullyDrainedCloseStillReclaimsAnUnheldLogicalLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slotDir = sfDir + "/slot1";
            assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
            String logicalLockPath = sfDir + "/.slot-locks/slot1.lock";

            SlotLock.acquireLogical(slotDir).close(); // materialise the lock file, release it
            assertTrue(Files.exists(logicalLockPath));

            CursorSendEngine engine = new CursorSendEngine(slotDir, 4L * 1024 * 1024);
            engine.close();

            assertFalse("a fully-drained close with no logical-lock holder must reclaim it",
                    Files.exists(logicalLockPath));
        });
    }

    @Test(timeout = 10_000L)
    public void testSlotLockReleasedEvenIfRingCloseThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024);

            // Sanity: a second acquire on the same slot must fail while
            // the engine is alive (test scaffolding is correctly aimed).
            try {
                SlotLock probe = SlotLock.acquire(sfDir);
                probe.close();
                fail("scaffolding error: expected the engine to hold the slot lock, "
                        + "but a fresh SlotLock.acquire succeeded");
            } catch (Exception expected) {
                // good — slot is locked.
            }

            // Sabotage: zero out ring so engine.close() NPEs before reaching
            // the slotLock cleanup. Any close-path exception (manager.close,
            // ring.close, unlinkAllSegmentFiles) lands in the same place.
            //
            // Capture the ring + manager references first so we can free
            // their native resources ourselves after the sabotage — engine.close()
            // can no longer reach ring.close() / manager.close() once we null
            // the ring field, and assertMemoryLeak (+ the manager's worker
            // thread) would otherwise trip.
            Field ringField = CursorSendEngine.class.getDeclaredField("ring");
            ringField.setAccessible(true);
            SegmentRing capturedRing = (SegmentRing) ringField.get(engine);

            Field managerField = CursorSendEngine.class.getDeclaredField("manager");
            managerField.setAccessible(true);
            SegmentManager capturedManager = (SegmentManager) managerField.get(engine);

            // The watermark's 16-byte mmap is also unreachable to the sabotaged
            // close() (it NPEs before getting there), so capture and free it
            // manually too or the leak check trips on MMAP_DEFAULT.
            Field watermarkField = CursorSendEngine.class.getDeclaredField("watermark");
            watermarkField.setAccessible(true);
            io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark capturedWatermark =
                    (io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark) watermarkField.get(engine);

            ringField.set(engine, null);

            try {
                engine.close();
            } catch (Throwable t) {
                // Expected — close() walks ring.publishedFsn() and trips an NPE.
                // The fix must release slotLock anyway, in finally.
            }

            // Manually release the ring + manager resources that engine.close()
            // skipped because of the NPE. The slotLock contract is the only
            // thing the test is verifying; the rest of the close-path resources
            // are an artifact of the sabotage.
            capturedRing.close();
            capturedManager.close();
            if (capturedWatermark != null) {
                capturedWatermark.close();
            }

            // The user-visible test: can a fresh SlotLock acquire the
            // same slot? If the original lock fd is still held, the
            // kernel's flock blocks this acquire and we throw.
            try (SlotLock ignored = SlotLock.acquire(sfDir)) {
                // good — slot was released despite the close-path throw.
            } catch (Exception leaked) {
                fail("slotLock was leaked: a follow-up SlotLock.acquire on the "
                        + "same dir failed because engine.close() threw before "
                        + "reaching slotLock cleanup. Wrap the close steps in "
                        + "try/finally so slotLock.close() always runs. "
                        + "Underlying: " + leaked.getMessage());
            }
        });
    }

    @Test(timeout = 10_000L)
    public void testFullyDrainedCloseRemovesLogicalSlotLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The engine's slot lives one level under sfDir, so the parent-anchored
            // logical lock lands at <sfDir>/.slot-locks/slot0.lock -- inside the tree
            // tearDown cleans.
            String slotDir = sfDir + "/slot0";

            // Simulate the build's transient logical lock: acquire and release it,
            // leaving the shared-dir lock file behind exactly as a real build does
            // (close() releases the flock but keeps the file so it survives a rename).
            SlotLock.acquireLogical(slotDir).close();
            String lockFile = sfDir + "/.slot-locks/slot0.lock";
            assertTrue("precondition: a build's logical lock leaves a file behind",
                    Files.exists(lockFile));

            // A fresh engine that never publishes is fully drained on close, so its
            // close() runs the retirement cleanup that must reclaim the logical lock.
            try (CursorSendEngine engine = new CursorSendEngine(slotDir, 4L * 1024 * 1024)) {
                assertEquals(slotDir, engine.sfDir());
            }

            assertFalse("fully-drained engine close must reclaim the orphaned logical "
                            + "slot lock; otherwise .slot-locks grows unbounded under "
                            + "rotating senderIds",
                    Files.exists(lockFile));
        });
    }
}
