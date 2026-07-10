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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Regression test for the completion/release ordering in
 * {@code CursorSendEngine.finishClose()}: {@code closeCompleted} must be
 * published strictly AFTER the slot flock release is confirmed, never
 * before.
 *
 * <p>The bug being pinned: {@code closeCompleted = true} used to be written
 * before {@code slotLock} was closed. A pool thread could observe completion
 * through {@code QwpWebSocketSender.isSlotLockReleased()}, free the slot
 * index in {@code SenderPool.reprobeRetiredSlots()}, and admit a replacement
 * sender whose {@code SlotLock.acquire} then collided with the still-open
 * flock fd — a spurious "sf slot already in use" construction failure naming
 * the process's own pid as the holder.
 *
 * <p>The window between the publish and the {@code Files.close(fd)} is
 * microseconds wide in the wild; {@code setBeforeFlockReleaseHook} makes it
 * deterministic. The closing thread is parked between terminal cleanup and
 * the flock release, and the test asserts from outside the window's two
 * halves of the contract:
 * <ul>
 *   <li>inside the window: {@code isCloseCompleted()} is still false AND a
 *       fresh {@code SlotLock.acquire} on the slot fails (proving the flock
 *       is genuinely held — i.e. reporting completion here would have been
 *       a lie);</li>
 *   <li>after the window: {@code isCloseCompleted()} is true AND a fresh
 *       {@code SlotLock.acquire} succeeds (completion still implies
 *       reusability — the reorder did not break the happy path).</li>
 * </ul>
 */
public class EngineClosePublishAfterFlockReleaseTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-engine-close-publish-order-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir == null) return;
        long find = Files.findFirst(sfDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(sfDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(sfDir);
    }

    @Test(timeout = 30_000L)
    public void testCloseCompletedPublishedOnlyAfterConfirmedFlockRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024);
            CountDownLatch inWindow = new CountDownLatch(1);
            CountDownLatch proceed = new CountDownLatch(1);
            engine.setBeforeFlockReleaseHook(() -> {
                inWindow.countDown();
                try {
                    // Bounded so a failed assertion on the main thread can
                    // never wedge the closer past the test timeout.
                    proceed.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            AtomicReference<Throwable> closerError = new AtomicReference<>();
            Thread closer = new Thread(() -> {
                try {
                    engine.close();
                } catch (Throwable t) {
                    closerError.set(t);
                }
            }, "engine-closer");
            closer.start();

            try {
                assertTrue("closer thread never reached the cleanup/release window",
                        inWindow.await(10, TimeUnit.SECONDS));

                // Inside the window: terminal cleanup has run, the flock has
                // NOT been released. Completion must not be observable yet —
                // this is the exact read a pool thread performs before
                // freeing the slot index.
                assertFalse("closeCompleted was published before the flock release; "
                                + "a pool observing this would free the slot index and a "
                                + "replacement sender would collide with the still-held flock",
                        engine.isCloseCompleted());

                // Prove the window is real: the flock is genuinely still
                // held, so acquisition by a "replacement engine" fails.
                try {
                    SlotLock probe = SlotLock.acquire(sfDir);
                    probe.close();
                    fail("scaffolding error: expected the slot flock to still be "
                            + "held inside the pre-release window, but a fresh "
                            + "SlotLock.acquire succeeded");
                } catch (IllegalStateException expected) {
                    // good — slot is locked, which is why completion must
                    // not have been published yet.
                }
            } finally {
                proceed.countDown();
                closer.join(10_000L);
            }
            assertFalse("closer thread did not finish", closer.isAlive());
            assertNull("engine.close() threw", closerError.get());

            // After the window: the release is confirmed, so completion must
            // now be latched, and completion must still imply reusability.
            assertTrue("closeCompleted must latch once the flock release is confirmed",
                    engine.isCloseCompleted());
            try (SlotLock ignored = SlotLock.acquire(sfDir)) {
                // good — completion implies the slot dir is acquirable.
            } catch (IllegalStateException stillHeld) {
                fail("closeCompleted reported true but the slot flock is still held: "
                        + stillHeld.getMessage());
            }
        });
    }
}
