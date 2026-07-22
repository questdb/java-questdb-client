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

import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class BackgroundDrainerSetupFailureTest {

    private static final long SEGMENT_BYTES = 1L << 20;
    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-drainer-setup-failure-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        removeRecursive(slotPath);
    }

    @Test
    public void testConnectErrorPropagatesWithoutQuarantine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedUnackedFrame();
            LinkageError injected = new LinkageError("injected drainer connect error");
            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_BYTES,
                    Long.MAX_VALUE,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw injected;
                    },
                    5_000L,
                    1L,
                    10L,
                    true,
                    200L);

            LinkageError thrown = null;
            try {
                drainer.run();
            } catch (LinkageError e) {
                thrown = e;
            }

            Assert.assertSame("post-publication Error must remain visible to the caller",
                    injected, thrown);
            Assert.assertEquals("connect error must occur on the first attempt",
                    1, connectAttempts.get());
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            TestUtils.assertContains(drainer.getLastErrorMessage(),
                    "injected drainer connect error");
            Assert.assertFalse("a post-publication Error must not quarantine recoverable data",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            Assert.assertTrue("unacknowledged segment must remain scanner-eligible",
                    OrphanScanner.isCandidateOrphan(slotPath));
            try (CursorSendEngine ignored = new CursorSendEngine(slotPath, SEGMENT_BYTES)) {
                Assert.assertTrue("drainer teardown must release the slot lock",
                        OrphanScanner.isCandidateOrphan(slotPath));
            }
        });
    }

    @Test
    public void testConstructionErrorPropagatesWithoutQuarantine() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedUnackedFrame();
            OutOfMemoryError injected = new OutOfMemoryError("injected drainer construction error");
            CursorSendEngine.setBeforeDeferredCloseCreationHook(() -> {
                throw injected;
            });
            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_BYTES,
                    Long.MAX_VALUE,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError("construction error must happen before connect");
                    },
                    5_000L,
                    1L,
                    10L,
                    true,
                    200L);

            OutOfMemoryError thrown = null;
            try {
                drainer.run();
            } catch (OutOfMemoryError e) {
                thrown = e;
            } finally {
                CursorSendEngine.setBeforeDeferredCloseCreationHook(null);
            }

            Assert.assertSame("construction Error must remain visible to the caller",
                    injected, thrown);
            Assert.assertEquals("construction error must precede connect",
                    0, connectAttempts.get());
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            TestUtils.assertContains(drainer.getLastErrorMessage(),
                    "injected drainer construction error");
            Assert.assertFalse("a VM Error must not quarantine recoverable data",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            Assert.assertTrue("unacknowledged segment must remain scanner-eligible",
                    OrphanScanner.isCandidateOrphan(slotPath));
            try (CursorSendEngine ignored = new CursorSendEngine(slotPath, SEGMENT_BYTES)) {
                Assert.assertTrue("constructor teardown must release the slot lock",
                        OrphanScanner.isCandidateOrphan(slotPath));
            }
        });
    }

    @Test
    public void testCorruptRecoveredChainIsQuarantined() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedUnackedFrame();

            String segmentPath = slotPath + "/sf-initial.sfa";
            long corruptByte = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            int fd = Files.openRW(segmentPath);
            Assert.assertTrue("could not open segment for corruption", fd >= 0);
            try {
                Unsafe.getUnsafe().putByte(corruptByte, (byte) 0);
                Assert.assertEquals(1L, Files.write(fd, corruptByte, 1, 0L));
            } finally {
                Files.close(fd);
                Unsafe.free(corruptByte, 1, MemoryTag.NATIVE_DEFAULT);
            }

            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_BYTES,
                    Long.MAX_VALUE,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError("recovery failure must happen before connect");
                    },
                    5_000L,
                    1L,
                    10L,
                    true,
                    200L);

            drainer.run();

            Assert.assertEquals("corrupt recovery must precede connect",
                    0, connectAttempts.get());
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            Assert.assertTrue("a corrupt durable chain requires operator quarantine",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            Assert.assertTrue("corrupt segment evidence must remain on disk",
                    Files.exists(segmentPath));
        });
    }

    @Test
    public void testLockOpenFailureDoesNotQuarantineRecoverableData() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedUnackedFrame();

            String lockPath = slotPath + "/.lock";
            Assert.assertTrue("seeded lock file must exist", Files.exists(lockPath));
            Assert.assertTrue("could not remove seeded lock file", Files.remove(lockPath));
            Assert.assertEquals("could not plant deterministic lock-open failure",
                    0, Files.mkdir(lockPath, Files.DIR_MODE_DEFAULT));

            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_BYTES,
                    Long.MAX_VALUE,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError("lock setup failure must happen before connect");
                    },
                    5_000L,
                    1L,
                    10L,
                    true,
                    200L);

            drainer.run();

            Assert.assertEquals("lock failure must precede connect",
                    0, connectAttempts.get());
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            TestUtils.assertContains(drainer.getLastErrorMessage(),
                    "could not open slot lock file");
            Assert.assertFalse("an operational lock-open failure must remain retryable",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            Assert.assertTrue("unacknowledged segment must remain scanner-eligible",
                    OrphanScanner.isCandidateOrphan(slotPath));
        });
    }

    @Test
    public void testWatermarkOpenFailureDoesNotQuarantineRecoverableData() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedUnackedFrame();

            String watermarkPath = slotPath + "/" + AckWatermark.FILE_NAME;
            Assert.assertTrue("seeded watermark must exist", Files.exists(watermarkPath));
            Assert.assertTrue("could not remove seeded watermark", Files.remove(watermarkPath));
            Assert.assertEquals("could not plant deterministic watermark-open failure",
                    0, Files.mkdir(watermarkPath, Files.DIR_MODE_DEFAULT));

            AtomicInteger connectAttempts = new AtomicInteger();
            BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_BYTES,
                    Long.MAX_VALUE,
                    () -> {
                        connectAttempts.incrementAndGet();
                        throw new AssertionError("setup failure must happen before connect");
                    },
                    5_000L,
                    1L,
                    10L,
                    true,
                    200L);

            drainer.run();

            Assert.assertEquals("watermark failure must precede connect",
                    0, connectAttempts.get());
            Assert.assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            TestUtils.assertContains(drainer.getLastErrorMessage(),
                    "could not open required ack watermark");
            Assert.assertFalse("an operational watermark-open failure must remain retryable",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            Assert.assertTrue("unacknowledged segment must remain on disk",
                    OrphanScanner.isCandidateOrphan(slotPath));
        });
    }

    private static void removeRecursive(String path) {
        if (path == null || !Files.exists(path)) {
            return;
        }
        long find = Files.findFirst(path);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = path + "/" + name;
                        if (!Files.remove(child)) {
                            removeRecursive(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(path);
    }

    private void seedUnackedFrame() {
        long buffer = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_BYTES)) {
            Unsafe.getUnsafe().setMemory(buffer, 16, (byte) 1);
            Assert.assertEquals(0L, engine.appendBlocking(buffer, 16));
        } finally {
            Unsafe.free(buffer, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }
}
