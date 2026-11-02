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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentSource;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In disk mode the manager worker pre-creates the segment a ring's next
 * rotation needs, so the producer's rotation only rebases it and updates the
 * manifest. The prefetch is optional: refused or failed, the producer creates
 * the segment itself, and nothing retries on a timer.
 */
public class SegmentPrefetchTest {
    private static final int PAYLOAD = 64;
    // Exactly two frames per segment.
    private static final long SEG = MmapSegment.HEADER_SIZE + 2 * (MmapSegment.FRAME_HEADER_SIZE + PAYLOAD);
    private long buf;
    private String dir;

    @Before
    public void setUp() {
        dir = TestUtils.createTmpDir("qdb-sf-prefetch-");
        buf = Unsafe.malloc(PAYLOAD, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().setMemory(buf, PAYLOAD, (byte) 7);
    }

    @After
    public void tearDown() {
        Unsafe.free(buf, PAYLOAD, MemoryTag.NATIVE_DEFAULT);
        TestUtils.removeTmpDir(dir);
    }

    /** A close that is not fully drained still removes the pre-created file; the manifest never named it. */
    @Test(timeout = 30_000L)
    public void testCloseRemovesPreCreatedFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(100 * SEG);
            try (SegmentManager manager = new SegmentManager(SEG, budget, new DelegatingFilesFacade(), System::nanoTime)) {
                manager.start();
                List<String> ringFiles;
                try (CursorSendEngine engine = new CursorSendEngine(dir, SEG, manager)) {
                    SegmentRing ring = engine.getRingForTesting();
                    appendFrames(engine, 3);
                    awaitTotalBytes(ring, 3 * SEG);
                    List<String> files = segmentFiles();
                    Assert.assertEquals(3, files.size());
                    files.remove(newest(files));
                    ringFiles = files;
                }
                Assert.assertEquals("unacked frames keep the ring's files, not the pre-created one",
                        ringFiles, segmentFiles());
                Assert.assertEquals(0L, budget.getSegmentBytes());
            }
        });
    }

    /**
     * Deregister lands between the prefetch's create and its install: the
     * segment is abandoned, its charge released and its file removed.
     */
    @Test
    public void testDeregisterBeforeInstallAbandonsPrefetch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(100 * SEG);
            try (SegmentManager manager = new SegmentManager(SEG, budget, new DelegatingFilesFacade(), System::nanoTime)) {
                try (CursorSendEngine engine = new CursorSendEngine(dir, SEG, manager)) {
                    SegmentRing ring = engine.getRingForTesting();
                    appendFrames(engine, 3);
                    Assert.assertEquals(2 * SEG, budget.getSegmentBytes());
                    AtomicInteger filesInWindow = new AtomicInteger();
                    manager.setBeforePrefetchInstallHook(() -> {
                        filesInWindow.set(segmentFiles().size());
                        manager.deregister(ring);
                    });
                    manager.serviceRingForTesting(ring);
                    manager.setBeforePrefetchInstallHook(null);
                    Assert.assertEquals("the prefetch had created its file", 3, filesInWindow.get());
                    Assert.assertEquals("deregister released the ring, the abandon its prefetch",
                            0L, budget.getSegmentBytes());
                    Assert.assertEquals(2, segmentFiles().size());
                    Assert.assertEquals(2 * SEG, ring.totalSegmentBytes());
                }
            }
        });
    }

    /**
     * A failed prefetch is not retried by the worker: it waits for the next
     * rotation, which the producer serves itself.
     */
    @Test(timeout = 30_000L)
    public void testFailedPrefetchIsNotRetriedUntilNextRotation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger workerAttempts = new AtomicInteger();
            Thread[] worker = new Thread[1];
            DelegatingFilesFacade ff = new DelegatingFilesFacade() {
                @Override
                public boolean allocate(int fd, long size) {
                    if (Thread.currentThread() == worker[0]) {
                        workerAttempts.incrementAndGet();
                        return false;
                    }
                    return super.allocate(fd, size);
                }
            };
            try (SegmentManager manager = new SegmentManager(SEG, new SegmentBudget(100 * SEG), ff, System::nanoTime)) {
                manager.start();
                worker[0] = manager.getWorkerThreadForTesting();
                try (CursorSendEngine engine = new CursorSendEngine(dir, SEG, manager)) {
                    appendFrames(engine, 3); // rotation 1, served by the producer
                    awaitAttempts(workerAttempts, 1);
                    // ACKs and trims wake the worker; none of them retries the prefetch.
                    Assert.assertTrue(engine.acknowledge(1L));
                    Thread.sleep(300);
                    Assert.assertEquals(1, workerAttempts.get());

                    appendFrames(engine, 2); // rotation 2: the producer creates the segment itself
                    Assert.assertEquals(4L, engine.activeSegment().baseSeq());
                    awaitAttempts(workerAttempts, 2);
                    Thread.sleep(300);
                    Assert.assertEquals(2, workerAttempts.get());
                }
            }
        });
    }

    /**
     * A prefetch only takes free room: refused, it charges nothing and waits
     * for the next natural pass, here the trim of an acknowledged segment.
     */
    @Test
    public void testRefusedPrefetchChargesNothingAndRunsOnNextPass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Room for two segments plus the slot's small symbol-dictionary file, not three.
            SegmentBudget budget = new SegmentBudget(2 * SEG + SEG / 2);
            try (SegmentManager manager = new SegmentManager(SEG, budget, new DelegatingFilesFacade(), System::nanoTime)) {
                try (CursorSendEngine engine = new CursorSendEngine(dir, SEG, manager)) {
                    Assert.assertTrue(budget.getSideFileBytes() < SEG / 2);
                    SegmentRing ring = engine.getRingForTesting();
                    appendFrames(engine, 3); // rotation: active + sealed fill the budget
                    manager.serviceRingForTesting(ring);
                    Assert.assertEquals("no room: nothing pre-created", 2 * SEG, ring.totalSegmentBytes());
                    Assert.assertEquals(2 * SEG, budget.getSegmentBytes());
                    Assert.assertEquals(2, segmentFiles().size());

                    // The ACK frees the sealed segment; the same pass that trims it pre-creates.
                    Assert.assertTrue(engine.acknowledge(1L));
                    manager.serviceRingForTesting(ring);
                    Assert.assertEquals("active + pre-created", 2 * SEG, ring.totalSegmentBytes());
                    Assert.assertEquals(2 * SEG, budget.getSegmentBytes());

                    appendFrames(engine, 2); // rotates into the pre-created segment: no new charge
                    Assert.assertEquals(4L, engine.activeSegment().baseSeq());
                    Assert.assertEquals(2 * SEG, budget.getSegmentBytes());
                }
                Assert.assertEquals(0L, budget.getSegmentBytes());
            }
        });
    }

    /**
     * A pre-created segment's header is synced again on a retry after a failed
     * sync, although its base was already pinned in memory: the manifest must
     * never name a base the on-disk header may not carry.
     */
    @Test
    public void testRetryAfterFailedHeaderSyncResyncsBeforeManifest() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            HeaderSyncFacade ff = new HeaderSyncFacade();
            writeLegacySegment(dir + "/sf-initial.sfa", 1);
            SegmentRing ring = SegmentRing.openExisting(ff, dir, SEG);
            Assert.assertNotNull(ring);
            try {
                RetainingSource source = new RetainingSource(MmapSegment.create(ff, dir + "/sf-00000000000000aa.sfa", 99L, SEG));
                ring.setSegmentSource(source);
                Assert.assertEquals(1L, ring.appendOrFsn(buf, PAYLOAD)); // fills the active (seqs 0..1)
                ff.failHeaderSyncs = true;
                try {
                    ring.appendOrFsn(buf, PAYLOAD);
                    Assert.fail("the header sync must fail the rotation");
                } catch (MmapSegmentException expected) {
                    // retained by the source, base already rebased in memory
                }
                ff.failHeaderSyncs = false;
                Assert.assertNotNull(source.retained);
                Assert.assertEquals(2L, source.retained.baseSeq());
                int syncsBefore = ff.headerSyncs.get();
                Assert.assertEquals(2L, ring.appendOrFsn(buf, PAYLOAD));
                Assert.assertEquals("the retry re-syncs the rebased header before the manifest update",
                        syncsBefore + 1, ff.headerSyncs.get());
                Assert.assertEquals(2L, ring.getActive().baseSeq());
            } finally {
                ring.close();
            }
        });
    }

    /** A rotation takes the worker's segment and creates no file on the producer thread. */
    @Test(timeout = 30_000L)
    public void testRotationTakesSegmentPreCreatedByWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CreateRecordingFacade ff = new CreateRecordingFacade();
            try (SegmentManager manager = new SegmentManager(SEG, new SegmentBudget(100 * SEG), ff, System::nanoTime)) {
                manager.start();
                try (CursorSendEngine engine = new CursorSendEngine(dir, SEG, manager)) {
                    SegmentRing ring = engine.getRingForTesting();
                    appendFrames(engine, 3); // the third frame is the first rotation: no prefetch yet
                    // active + sealed + the successor the worker pre-creates
                    awaitTotalBytes(ring, 3 * SEG);
                    List<String> before = segmentFiles();
                    Assert.assertEquals(3, before.size());
                    String precreated = newest(before);
                    Assert.assertEquals("the worker created it", 1, ff.createsOn(manager.getWorkerThreadForTesting()));

                    ff.reset();
                    appendFrames(engine, 2); // frame 5 rotates
                    Assert.assertEquals("the rotation created no file on the producer thread",
                            0, ff.createsOn(Thread.currentThread()));
                    Assert.assertEquals(precreated, new File(engine.activeSegment().path()).getName());
                    Assert.assertEquals("rebased to the ring's next FSN", 4L, engine.activeSegment().baseSeq());

                    // ...and the worker already pre-creates the next one
                    awaitTotalBytes(ring, 4 * SEG);
                    Assert.assertEquals(1, ff.createsOn(manager.getWorkerThreadForTesting()));
                }
            }
        });
    }

    /**
     * The ring's segment source closes between create and install: the install
     * is refused, so no mapping, fd, file or charge is left behind.
     */
    @Test
    public void testSourceCloseBeforeInstallAbandonsPrefetch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(100 * SEG);
            try (SegmentManager manager = new SegmentManager(SEG, budget, new DelegatingFilesFacade(), System::nanoTime)) {
                try (CursorSendEngine engine = new CursorSendEngine(dir, SEG, manager)) {
                    SegmentRing ring = engine.getRingForTesting();
                    appendFrames(engine, 3);
                    SegmentSource source = ring.getSegmentSourceForTesting();
                    manager.setBeforePrefetchInstallHook(source::close);
                    manager.serviceRingForTesting(ring);
                    manager.setBeforePrefetchInstallHook(null);
                    Assert.assertEquals(2 * SEG, budget.getSegmentBytes());
                    Assert.assertEquals(2, segmentFiles().size());
                    Assert.assertEquals(2 * SEG, ring.totalSegmentBytes());
                }
            }
        });
    }

    private void appendFrames(CursorSendEngine engine, int n) {
        for (int i = 0; i < n; i++) {
            Assert.assertTrue(engine.appendBlocking(buf, PAYLOAD) >= 0);
        }
    }

    private static void awaitAttempts(AtomicInteger attempts, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (attempts.get() < expected) {
            Assert.assertTrue("worker attempts " + attempts.get() + " < " + expected, System.nanoTime() < deadline);
            Thread.sleep(5);
        }
    }

    private static void awaitTotalBytes(SegmentRing ring, long expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (ring.totalSegmentBytes() != expected) {
            Assert.assertTrue("ring holds " + ring.totalSegmentBytes() + " bytes, expected " + expected,
                    System.nanoTime() < deadline);
            Thread.sleep(5);
        }
    }

    // Generation-named files sort by generation; sf-initial.sfa sorts last.
    private static String newest(List<String> files) {
        String newest = null;
        for (String f : files) {
            if (f.startsWith("sf-0") && (newest == null || f.compareTo(newest) > 0)) {
                newest = f;
            }
        }
        return newest;
    }

    private List<String> segmentFiles() {
        String[] names = new File(dir).list((d, name) -> name.endsWith(".sfa"));
        List<String> list = new ArrayList<>(Arrays.asList(names == null ? new String[0] : names));
        list.sort(null);
        return list;
    }

    private void writeLegacySegment(String path, int frames) {
        MmapSegment segment = MmapSegment.create(path, 0L, SEG);
        try {
            for (int i = 0; i < frames; i++) {
                Assert.assertTrue(segment.tryAppend(buf, PAYLOAD) >= 0);
            }
        } finally {
            segment.close();
        }
    }

    private static final class CreateRecordingFacade extends DelegatingFilesFacade {
        private final List<Thread> creators = new ArrayList<>();

        @Override
        public synchronized int openRWExclusive(long pathPtr) {
            creators.add(Thread.currentThread());
            return super.openRWExclusive(pathPtr);
        }

        synchronized int createsOn(Thread thread) {
            int n = 0;
            for (Thread t : creators) {
                if (t == thread) {
                    n++;
                }
            }
            return n;
        }

        synchronized void reset() {
            creators.clear();
        }
    }

    private static final class HeaderSyncFacade extends DelegatingFilesFacade {
        private final AtomicInteger headerSyncs = new AtomicInteger();
        private volatile boolean failHeaderSyncs;

        @Override
        public int msync(long addr, long len, boolean async) {
            if (len == MmapSegment.HEADER_SIZE) {
                headerSyncs.incrementAndGet();
                if (failHeaderSyncs) {
                    return -1;
                }
            }
            return super.msync(addr, len, async);
        }
    }

    /** Hands out one pre-created segment and, like the provisioner, keeps a discarded one for the retry. */
    private static final class RetainingSource implements SegmentSource {
        private final AtomicBoolean handedOut = new AtomicBoolean();
        private final MmapSegment segment;
        private MmapSegment retained;

        RetainingSource(MmapSegment segment) {
            this.segment = segment;
        }

        @Override
        public void close() {
            if (!handedOut.get() || retained != null) {
                segment.close();
            }
        }

        @Override
        public void discard(MmapSegment segment) {
            retained = segment;
        }

        @Override
        public MmapSegment next(long baseSeq) {
            if (retained != null) {
                MmapSegment r = retained;
                retained = null;
                return r;
            }
            return handedOut.compareAndSet(false, true) ? segment : null;
        }
    }
}
