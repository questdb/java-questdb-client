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
 *******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SegmentManagerPinnedTrimTest {

    @Test(timeout = 15_000L)
    public void testPinnedActiveSurvivesRotationUntilIoRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            SegmentRing ring = null;
            try {
                ring = new SegmentRing(MmapSegment.createInMemory(0L, segmentSize), segmentSize);
                Unsafe.getUnsafe().putByte(payload, (byte) 1);
                Assert.assertEquals(0L, ring.appendOrFsn(payload, 1));
                MmapSegment pinned = ring.pinSegmentContainingForTest(0L);
                Assert.assertNotNull(pinned);
                Assert.assertTrue(ring.acknowledge(0L));

                ring.installHotSpare(MmapSegment.createInMemory(1L, segmentSize));
                Assert.assertEquals(1L, ring.appendOrFsn(payload, 1));
                Assert.assertSame(pinned, ring.firstSealed());

                CountDownLatch trimPass = new CountDownLatch(1);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8L)) {
                    manager.setBeforeTrimSyncHook(trimPass::countDown);
                    manager.register(ring, null);
                    manager.start();
                    Assert.assertTrue("manager did not attempt trim",
                            trimPass.await(5, TimeUnit.SECONDS));
                }

                Assert.assertSame("manager trimmed the I/O-pinned segment",
                        pinned, ring.firstSealed());
                Assert.assertNotEquals("manager freed the I/O-pinned mapping",
                        0L, pinned.address());
                Assert.assertEquals(0, ring.getPendingTrimCount());

                ring.releasePinnedSegmentForTest(pinned);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8L)) {
                    manager.register(ring, null);
                    manager.start();
                    awaitTrimmed(ring);
                }
                Assert.assertEquals("released segment mapping was not freed",
                        0L, pinned.address());
            } finally {
                if (ring != null) {
                    ring.close();
                }
                Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void awaitTrimmed(SegmentRing ring) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (ring.firstSealed() != null || ring.getPendingTrimCount() != 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("manager did not trim the released segment");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }
}
