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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Close-time unlink STOP-ON-FIRST-FAILURE on a legacy (manifest-less) slot.
 * Removal runs in ascending generation order and must stop at the first
 * failed unlink so the residue is always a contiguous top slice that passes
 * FSN-contiguity at the next recovery. Continuing past a failed
 * low-generation remove deletes higher generations and can leave
 * non-contiguous residue -- a startup brick on a slot that lost nothing.
 * <p>
 * The siblings cover the OTHER close-cleanup contracts:
 * {@link CursorSendEngineClosePartialEnumerationTest} proves a torn
 * enumeration drives zero unlinks, and the unlink-failure sibling fails ALL
 * removals (permission trick), which cannot discriminate stop-vs-continue
 * (every file survives either way). A continue-past-first-failure mutant in
 * {@code unlinkAllSegmentFiles} previously survived the whole suite; this
 * test kills it via the higher-generation-survival assertions. Same
 * determinism trick as the siblings: the shared manager is never started, so
 * no worker touches the slot and no concurrent cleanup can trip the armed
 * fault.
 */
public class CursorSendEngineCloseUnlinkStopOnFirstFailureTest {

    private static final long SEGMENT_SIZE = 4096L;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-engine-close-unlink-stop-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test(timeout = 20_000L)
    public void testCloseUnlinkStopsAtFirstFailedRemoveOnLegacySlot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String slot = tmpDir + "/legacy-slot";
            final String lowPath = slot + "/sf-initial.sfa";
            final String highPath = slot + "/sf-0000000000000002.sfa";
            final AtomicBoolean armLowestRemoveFailure = new AtomicBoolean();
            final AtomicInteger lowRemoveAttempts = new AtomicInteger();
            final AtomicInteger highRemoveAttempts = new AtomicInteger();
            FilesFacade faultFacade = (FilesFacade) Proxy.newProxyInstance(
                    FilesFacade.class.getClassLoader(),
                    new Class<?>[]{FilesFacade.class},
                    (proxy, method, args) -> {
                        if ("remove".equals(method.getName()) && args[0] instanceof String) {
                            String path = (String) args[0];
                            if (path.equals(lowPath)) {
                                lowRemoveAttempts.incrementAndGet();
                                if (armLowestRemoveFailure.get()) {
                                    // EBUSY-style transient refusal: no unlink happens.
                                    return false;
                                }
                            } else if (path.equals(highPath)) {
                                highRemoveAttempts.incrementAndGet();
                            }
                        }
                        try {
                            return method.invoke(FilesFacade.INSTANCE, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });

            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            // Legacy slot: two FSN-contiguous segments on disk, NO manifest.
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            try {
                MmapSegment low = MmapSegment.create(lowPath, 0L, SEGMENT_SIZE);
                Assert.assertTrue("setup: append must land", low.tryAppend(buf, 32) >= 0);
                Assert.assertTrue("setup: append must land", low.tryAppend(buf, 32) >= 0);
                low.close();
                MmapSegment high = MmapSegment.create(highPath, 2L, SEGMENT_SIZE);
                Assert.assertTrue("setup: append must land", high.tryAppend(buf, 32) >= 0);
                high.close();

                manager = new SegmentManager(
                        SEGMENT_SIZE,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        SEGMENT_SIZE * 4L,
                        faultFacade,
                        System::nanoTime);

                CursorSendEngine engine = new CursorSendEngine(slot, SEGMENT_SIZE, manager);
                boolean engineClosed = false;
                try {
                    Assert.assertTrue("engine must recover the legacy two-segment chain",
                            engine.wasRecoveredFromDisk());
                    Assert.assertEquals("recovered chain must publish FSNs 0..2",
                            2L, engine.publishedFsn());
                    Assert.assertTrue(engine.acknowledge(2L));

                    // Fully drained close with the LOWEST-generation removal
                    // refused once: ascending-order removal must STOP there.
                    armLowestRemoveFailure.set(true);
                    engine.close();
                    engineClosed = true;
                    Assert.assertTrue("close must complete despite the aborted cleanup",
                            engine.isCloseCompleted());
                    Assert.assertEquals("cleanup must attempt the lowest generation first",
                            1, lowRemoveAttempts.get());
                    Assert.assertEquals(
                            "removal must STOP at the first failed unlink -- continuing would "
                                    + "delete higher generations and could leave non-contiguous "
                                    + "residue that fails FSN-contiguity at the next recovery",
                            0, highRemoveAttempts.get());
                    Assert.assertTrue("the refused lowest segment must survive",
                            Files.exists(lowPath));
                    Assert.assertTrue("higher-generation segment must survive the stopped cleanup",
                            Files.exists(highPath));
                } finally {
                    if (!engineClosed) {
                        engine.close();
                    }
                }

                // Heal the fault; a successor adopts the contiguous residue
                // and its fully-drained close completes the cleanup.
                armLowestRemoveFailure.set(false);
                CursorSendEngine successor = new CursorSendEngine(slot, SEGMENT_SIZE, manager);
                boolean successorClosed = false;
                try {
                    Assert.assertTrue("successor must recover the contiguous residue",
                            successor.wasRecoveredFromDisk());
                    successor.close();
                    successorClosed = true;
                    Assert.assertTrue(successor.isCloseCompleted());
                    Assert.assertFalse("successor's fully-drained close must complete the unlink",
                            Files.exists(lowPath));
                    Assert.assertFalse("successor's fully-drained close must complete the unlink",
                            Files.exists(highPath));
                } finally {
                    if (!successorClosed) {
                        successor.close();
                    }
                }
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
                if (manager != null) {
                    manager.close();
                }
            }
        });
    }
}
