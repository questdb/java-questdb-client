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
 * Close-time unlink under a TORN directory enumeration ({@code findNext}
 * fails after the listing already produced entries). A partial listing must
 * not drive any unlink: removing only the files the walk happened to see
 * could delete the segment holding the highest frame while a lower one
 * survives, leaving residual state the retained ack watermark can no longer
 * vouch for. The contract is all-or-nothing: abort the cleanup, keep every
 * {@code .sfa} file and the watermark, and let the next recovery (or a
 * successor's fully-drained close) retry.
 * <p>
 * The sibling {@link CursorSendEngineCloseUnlinkFailureTest} injects a
 * failing UNLINK (permission trick, root-skipped); nothing exercised the
 * enumeration-abort branch itself. Same determinism trick as the sibling:
 * the shared manager is never started, so no worker touches the slot and no
 * concurrent enumeration can trip the armed fault.
 */
public class CursorSendEngineClosePartialEnumerationTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-engine-close-enum-fault-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test(timeout = 20_000L)
    public void testTornCloseTimeEnumerationUnlinksNothingAndSuccessorRetries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicBoolean failFindNext = new AtomicBoolean();
            final AtomicInteger sfaRemoveAttempts = new AtomicInteger();
            FilesFacade faultFacade = (FilesFacade) Proxy.newProxyInstance(
                    FilesFacade.class.getClassLoader(),
                    new Class<?>[]{FilesFacade.class},
                    (proxy, method, args) -> {
                        if (failFindNext.get() && "findNext".equals(method.getName())) {
                            return -1;
                        }
                        if ("remove".equals(method.getName())
                                && args != null && args.length == 1
                                && args[0] instanceof String
                                && ((String) args[0]).endsWith(".sfa")) {
                            sfaRemoveAttempts.incrementAndGet();
                        }
                        try {
                            return method.invoke(FilesFacade.INSTANCE, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });

            long segSize = 4096L;
            String slot = tmpDir + "/slot";
            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            SegmentManager manager = new SegmentManager(
                    segSize,
                    SegmentManager.DEFAULT_POLL_NANOS,
                    segSize * 4L,
                    faultFacade,
                    System::nanoTime);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                CursorSendEngine engine = new CursorSendEngine(slot, segSize, manager);
                boolean engineClosed = false;
                try {
                    Assert.assertEquals(0L, engine.appendBlocking(buf, 16));
                    Assert.assertTrue(engine.acknowledge(0L));

                    // Fully drained close, but the directory listing tears
                    // mid-walk: the cleanup must abort BEFORE the first
                    // unlink.
                    failFindNext.set(true);
                    engine.close();
                    engineClosed = true;
                    Assert.assertTrue("close must complete despite the aborted cleanup",
                            engine.isCloseCompleted());
                    Assert.assertEquals(
                            "a torn enumeration must drive ZERO segment unlinks -- "
                                    + "removing only the files the walk happened to see can "
                                    + "strand residual state the watermark cannot vouch for",
                            0, sfaRemoveAttempts.get());
                    Assert.assertTrue("the segment file must survive the aborted cleanup",
                            Files.exists(slot + "/sf-initial.sfa"));
                } finally {
                    if (!engineClosed) {
                        engine.close();
                    }
                }

                // Heal the directory walk; a successor adopts the slot,
                // recovers the residual (fully acknowledged) state, and its
                // own fully-drained close retries the cleanup successfully.
                failFindNext.set(false);
                CursorSendEngine successor = new CursorSendEngine(slot, segSize, manager);
                boolean successorClosed = false;
                try {
                    Assert.assertTrue("successor must recover the residual slot state",
                            successor.wasRecoveredFromDisk());
                    successor.close();
                    successorClosed = true;
                    Assert.assertTrue(successor.isCloseCompleted());
                    Assert.assertFalse(
                            "successor's fully-drained close must retry and complete the unlink",
                            Files.exists(slot + "/sf-initial.sfa"));
                } finally {
                    if (!successorClosed) {
                        successor.close();
                    }
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                manager.close();
            }
        });
    }
}
