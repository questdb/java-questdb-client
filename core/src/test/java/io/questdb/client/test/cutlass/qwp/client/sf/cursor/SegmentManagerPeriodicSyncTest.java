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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentManagerPeriodicSyncTest {

    @Test
    public void testDeadlineAndFailurePropagation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long intervalNanos = 100L;
            final long segmentSize = 4096L;
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-manager-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                ring.installHotSpare(MmapSegment.create(
                        filesFacade, dir + "/spare.sfa", 1L, segmentSize));
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 4L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                manager.serviceRingForTesting(ring);
                assertTrue(active.isPublishedDurable());
                assertEquals(1, filesFacade.msyncCalls);
                assertEquals(1, filesFacade.fsyncCalls);

                assertEquals(1L, ring.appendOrFsn(payload, 16));
                ticks.set(intervalNanos - 1L);
                manager.serviceRingForTesting(ring);
                assertEquals("checkpoint ran before its deadline", 1, filesFacade.msyncCalls);
                assertEquals("checkpoint ran before its deadline", 1, filesFacade.fsyncCalls);

                ticks.set(intervalNanos);
                manager.serviceRingForTesting(ring);
                assertEquals(2, filesFacade.msyncCalls);
                assertEquals(2, filesFacade.fsyncCalls);

                assertEquals(2L, ring.appendOrFsn(payload, 16));
                filesFacade.isFsyncFailureEnabled = true;
                ticks.set(intervalNanos * 2L);
                manager.serviceRingForTesting(ring);
                assertEquals(3, filesFacade.msyncCalls);
                assertEquals(3, filesFacade.fsyncCalls);
                try {
                    ring.appendOrFsn(payload, 16);
                    fail("expected manager data-sync failure to reach the producer");
                } catch (MmapSegmentException expected) {
                    assertTrue(expected.getMessage().contains("sync segment file"));
                }
                assertEquals("failed append must not enter the ring", 2L, ring.publishedFsn());
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testRotationGateDefersRotationUntilPredecessorDurable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The PERIODIC-mode rotation gate (requestSyncBeforeRotation):
            // rotation must NOT seal a predecessor whose published range is
            // not yet durable. This is the exact gate commit 88d6b792
            // accidentally shipped neutralized (`return false; // MUTANT`)
            // with the whole suite staying green -- this test is the mutant
            // killer. Three independent kill points:
            //   1. the rotating append must return BACKPRESSURE_NO_SPARE
            //      while the predecessor is non-durable (a neutralized gate
            //      rotates immediately and returns the FSN);
            //   2. the gate's sync request must run the barrier on the very
            //      next service pass BEFORE the interval deadline (a gate
            //      that fails to set syncRequested leaves the pass idle);
            //   3. after the barrier, the retried append must rotate.
            final long intervalNanos = 100L;
            // Exactly two 16-byte frames fit: header 24 + 2 * (8 + 16) = 72.
            final long segmentSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-gate-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                ring.installHotSpare(MmapSegment.create(
                        filesFacade, dir + "/spare.sfa", 2L, segmentSize));
                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 4L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                assertEquals(0L, ring.appendOrFsn(payload, 16));
                manager.serviceRingForTesting(ring);
                assertTrue("first pass must leave the active durable", active.isPublishedDurable());
                assertEquals(1, filesFacade.msyncCalls);
                assertEquals(1, filesFacade.fsyncCalls);

                // Fill the segment; the second frame is published but NOT
                // yet durable.
                assertEquals(1L, ring.appendOrFsn(payload, 16));
                assertTrue("segment must be full", active.isFull());
                assertTrue("published range must be ahead of the durable cursor",
                        !active.isPublishedDurable());

                // Kill point 1: the gate must refuse to rotate and
                // backpressure the producer instead. A spare IS installed,
                // so this return can only come from the durability gate.
                assertEquals("rotation must be deferred while the predecessor's "
                                + "published range is not durable",
                        SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(payload, 16));

                // Kill point 2: the deadline (tick 100) has NOT been
                // reached, so this pass runs the barrier only because the
                // gate requested it.
                manager.serviceRingForTesting(ring);
                assertEquals("gate-requested barrier must run before the deadline",
                        2, filesFacade.msyncCalls);
                assertEquals("gate-requested barrier must run before the deadline",
                        2, filesFacade.fsyncCalls);
                assertTrue(active.isPublishedDurable());

                // Kill point 3: with the predecessor durable the retried
                // append rotates into the spare.
                assertEquals(2L, ring.appendOrFsn(payload, 16));
                assertEquals(1, ring.getSealedSegments().size());
                assertEquals(2L, ring.getActive().baseSeq());
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testSyncPassStopsAtFirstFailureThenRetryCoversAllSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // servicePeriodicSync barriers every LIVE segment (sealed first,
            // active last) and aborts at the first failure. With >= 2
            // non-durable segments a mid-pass failure must skip the later
            // segments, latch the producer, and the healed retry must cover
            // EVERY segment before the latch clears. Recovered segments are
            // the deterministic way to hold two non-durable live segments:
            // recovery constructs them with durableCursor at the header, and
            // syncPublished() skips already-durable segments.
            final long intervalNanos = 100L;
            final long segmentSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-multiseg-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                // Sealed chain member (full: FSNs 0..1) + active (FSN 2).
                try (MmapSegment s0 = MmapSegment.create(
                        filesFacade, dir + "/r0.sfa", 0L, segmentSize)) {
                    s0.tryAppend(payload, 16);
                    s0.tryAppend(payload, 16);
                    s0.msync();
                }
                try (MmapSegment s1 = MmapSegment.create(
                        filesFacade, dir + "/r1.sfa", 2L, segmentSize)) {
                    s1.tryAppend(payload, 16);
                    s1.msync();
                }
                ring = SegmentRing.openExisting(filesFacade, dir, segmentSize);
                assertTrue(ring != null);
                assertEquals(1, ring.getSealedSegments().size());
                // Pre-install the hot spare so the service pass does not
                // provision one mid-test: its header write would pollute the
                // exact barrier-call accounting below.
                ring.installHotSpare(MmapSegment.create(
                        filesFacade, dir + "/spare.sfa", 3L, segmentSize));

                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 8L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                // First pass with the disk failing: the sealed member's
                // barrier is attempted first (msync succeeds, fsync fails)
                // and the pass must STOP there -- the active's barrier must
                // not run after a failure.
                int msyncBefore = filesFacade.msyncCalls;
                int fsyncBefore = filesFacade.fsyncCalls;
                filesFacade.isFsyncFailureEnabled = true;
                manager.serviceRingForTesting(ring);
                assertEquals("only the first (sealed) segment's barrier may be attempted",
                        msyncBefore + 1, filesFacade.msyncCalls);
                assertEquals("the pass must abort at the first fsync failure",
                        fsyncBefore + 1, filesFacade.fsyncCalls);
                try {
                    ring.appendOrFsn(payload, 16);
                    fail("mid-pass barrier failure must latch the producer");
                } catch (MmapSegmentException expected) {
                    assertTrue(expected.getMessage().contains("sync segment file"));
                }

                // Heal. The retry (scheduled at now + min(interval, 1s))
                // must cover BOTH segments -- the one that failed and the
                // one the aborted pass never reached -- before unlatching.
                filesFacade.isFsyncFailureEnabled = false;
                msyncBefore = filesFacade.msyncCalls;
                fsyncBefore = filesFacade.fsyncCalls;
                ticks.set(intervalNanos);
                manager.serviceRingForTesting(ring);
                assertEquals("the healed retry must barrier every live segment",
                        msyncBefore + 2, filesFacade.msyncCalls);
                assertEquals("the healed retry must barrier every live segment",
                        fsyncBefore + 2, filesFacade.fsyncCalls);
                assertEquals("producer must resume once the pass covered all segments",
                        3L, ring.appendOrFsn(payload, 16));
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testPeriodicFrontierSkipsSealedPrefixOnceDurable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Recovery resumes a non-durable sealed segment (durableCursor at
            // the header) plus a non-durable active. The first periodic copy
            // must offer BOTH -- the sealed one still needs a barrier. Once a
            // pass has made them durable, the durability frontier advances and
            // later copies must offer ONLY the active: the proven-durable
            // sealed prefix is never re-copied/re-scanned under the ring
            // monitor. This guards the O(1)-steady-state contract that
            // replaced the old O(live-sealed) copyLiveSegmentsForSync pass.
            final long intervalNanos = 100L;
            final long segmentSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-frontier-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                // Sealed chain member (full: FSNs 0..1) + active (FSN 2).
                try (MmapSegment s0 = MmapSegment.create(
                        filesFacade, dir + "/r0.sfa", 0L, segmentSize)) {
                    s0.tryAppend(payload, 16);
                    s0.tryAppend(payload, 16);
                    s0.msync();
                }
                try (MmapSegment s1 = MmapSegment.create(
                        filesFacade, dir + "/r1.sfa", 2L, segmentSize)) {
                    s1.tryAppend(payload, 16);
                    s1.msync();
                }
                ring = SegmentRing.openExisting(filesFacade, dir, segmentSize);
                assertTrue(ring != null);
                assertEquals(1, ring.getSealedSegments().size());

                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 8L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                // Before any successful barrier the recovered sealed segment is
                // non-durable, so the frontier cannot advance past it: the copy
                // must offer sealed + active.
                assertEquals("non-durable recovered sealed segment must be offered for sync",
                        2, ring.pendingSyncSegmentCountForTest());

                // Run the pass (enablePeriodicSync requested it): both segments
                // are barriered and become durable.
                manager.serviceRingForTesting(ring);

                // The sealed segment is still live (nothing ACKed, so no trim)
                // but now durable, so the frontier skips it: only the active is
                // offered. count == 1 with a still-present sealed segment proves
                // the skip.
                assertEquals("durable sealed segment must remain live (no trim without ACKs)",
                        1, ring.getSealedSegments().size());
                assertEquals("durable sealed prefix must be skipped by the periodic copy",
                        1, ring.pendingSyncSegmentCountForTest());
                // Idempotent: re-running the frontier advance changes nothing.
                assertEquals(1, ring.pendingSyncSegmentCountForTest());
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testTransientSyncFailureClearsOnNextSuccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long intervalNanos = 100L;
            final long segmentSize = 4096L;
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-recovery-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                ring.installHotSpare(MmapSegment.create(
                        filesFacade, dir + "/spare.sfa", 1L, segmentSize));
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 4L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                // First tick: initial sync succeeds.
                manager.serviceRingForTesting(ring);
                assertTrue(active.isPublishedDurable());

                // One transient fsync failure on the next periodic barrier.
                assertEquals(1L, ring.appendOrFsn(payload, 16));
                filesFacade.isFsyncFailureEnabled = true;
                ticks.set(intervalNanos);
                manager.serviceRingForTesting(ring);
                try {
                    ring.appendOrFsn(payload, 16);
                    fail("expected the failed data sync to reach the producer");
                } catch (MmapSegmentException expected) {
                    assertTrue(expected.getMessage().contains("sync segment file"));
                }
                assertEquals("failed append must not enter the ring", 1L, ring.publishedFsn());

                // Disk recovers; the manager's retry (scheduled at
                // now + min(interval, 1s)) succeeds on the next tick.
                filesFacade.isFsyncFailureEnabled = false;
                ticks.set(intervalNanos * 2L);
                int msyncBefore = filesFacade.msyncCalls;
                int fsyncBefore = filesFacade.fsyncCalls;
                manager.serviceRingForTesting(ring);
                assertTrue("retry barrier must have run msync", filesFacade.msyncCalls > msyncBefore);
                assertTrue("retry barrier must have run fsync", filesFacade.fsyncCalls > fsyncBefore);

                // A transient failure must not brick the producer: the retry
                // covered the published range, so appends resume.
                assertEquals(2L, ring.appendOrFsn(payload, 16));
                assertEquals(2L, ring.publishedFsn());
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testBarrierPinsPublishedRangeAndUnpinsOnSuccess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long segmentSize = 4096L;
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-pin-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                active.syncPublished();

                assertEquals("barrier must pin the not-yet-durable range", 1, filesFacade.mlockCalls);
                assertEquals("successful barrier must release the pin", 1, filesFacade.munlockCalls);
                // durableCursor starts at HEADER_SIZE, which aligns down to
                // page 0, so the pin covers [0, published).
                assertEquals("pin must cover the whole not-yet-durable range",
                        active.publishedOffset(), filesFacade.lastMlockLen);
                assertEquals("success path must not re-dirty", 0L, active.redirtyPassesForTest());
                assertTrue(active.isPublishedDurable());
            } finally {
                if (ring != null) {
                    ring.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testFailedBarrierRedirtiesUnderPinBeforeUnlock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long segmentSize = 4096L;
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-redirty-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                long[] redirtyAtUnlock = new long[1];
                filesFacade.onMunlock = () -> redirtyAtUnlock[0] = active.redirtyPassesForTest();
                filesFacade.isFsyncFailureEnabled = true;
                try {
                    active.syncPublished();
                    fail("expected the fsync failure to surface");
                } catch (MmapSegmentException expected) {
                    assertTrue(expected.getMessage().contains("sync segment file"));
                }

                assertEquals("failed barrier must re-dirty the covered range",
                        1L, active.redirtyPassesForTest());
                assertEquals("failed barrier must still release the pin", 1, filesFacade.munlockCalls);
                assertEquals("re-dirty must happen BEFORE the pin is released",
                        1L, redirtyAtUnlock[0]);
                assertFalse("failed barrier must not advance durability", active.isPublishedDurable());
            } finally {
                if (ring != null) {
                    ring.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testConsumedErrorRetryClearsLatchOnlyOverRedirtiedPages() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long intervalNanos = 100L;
            final long segmentSize = 4096L;
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-fsyncgate-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                ring.installHotSpare(MmapSegment.create(
                        filesFacade, dir + "/spare.sfa", 1L, segmentSize));
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 4L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                // First tick: initial sync genuinely succeeds.
                manager.serviceRingForTesting(ring);
                assertTrue(active.isPublishedDurable());

                // fsyncgate model: the next barrier's fsync fails once; from
                // then on the facade behaves like the real kernel after EIO --
                // pages clean, error consumed -- returning 0 from msync/fsync
                // WITHOUT persisting anything.
                assertEquals(1L, ring.appendOrFsn(payload, 16));
                filesFacade.isFsyncGateModeEnabled = true;
                ticks.set(intervalNanos);
                manager.serviceRingForTesting(ring);
                try {
                    ring.appendOrFsn(payload, 16);
                    fail("expected the failed data sync to latch the producer");
                } catch (MmapSegmentException expected) {
                }
                assertEquals("the failed barrier must have re-dirtied its range before any vacuous retry",
                        1L, active.redirtyPassesForTest());

                // Retry pass: the facade's vacuous 0 is backed by genuinely
                // re-dirtied pages, so unlatching is honest. Without the
                // re-dirty (the C-1 mutant) this scenario is exactly the
                // unsound clear: latch gone, durableCursor advanced, nothing
                // persisted and no dirty page left for any future barrier.
                ticks.set(intervalNanos * 2L);
                manager.serviceRingForTesting(ring);
                assertTrue(active.isPublishedDurable());
                assertEquals("producer must resume after the covered retry",
                        2L, ring.appendOrFsn(payload, 16));
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testMlockRefusalDegradesWithoutAffectingBarrier() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long segmentSize = 4096L;
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            filesFacade.isMlockRefusalEnabled = true;
            String dir = TestUtils.createTmpDir("qdb-periodic-mlock-refusal-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                int msyncBefore = filesFacade.msyncCalls;
                int fsyncBefore = filesFacade.fsyncCalls;
                active.syncPublished();

                assertEquals("refused pin must not skip the mapping barrier",
                        msyncBefore + 1, filesFacade.msyncCalls);
                assertEquals("refused pin must not skip the fd barrier",
                        fsyncBefore + 1, filesFacade.fsyncCalls);
                assertEquals(1, filesFacade.mlockCalls);
                assertEquals("a refused pin must not be unlocked", 0, filesFacade.munlockCalls);
                assertTrue("refusal must not affect the barrier outcome", active.isPublishedDurable());
                assertEquals(0L, active.redirtyPassesForTest());
                assertEquals("producer must remain unaffected", 1L, ring.appendOrFsn(payload, 16));
            } finally {
                if (ring != null) {
                    ring.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    private static final class CountingFilesFacade implements FilesFacade {
        private boolean isFsyncFailureEnabled;
        // fsyncgate model: the first fsync fails; afterwards every msync and
        // fsync returns 0 WITHOUT delegating to the real syscall -- the
        // kernel-accurate shape of a vacuous retry after a consumed EIO
        // (clean pages, seen errseq cursor).
        private boolean isFsyncGateModeEnabled;
        private boolean fsyncGateErrorConsumed;
        private boolean isMlockRefusalEnabled;
        private int fsyncCalls;
        private int msyncCalls;
        private int mlockCalls;
        private int munlockCalls;
        private long lastMlockLen;
        private Runnable onMunlock;

        @Override
        public boolean allocate(int fd, long size) {
            return INSTANCE.allocate(fd, size);
        }

        @Override
        public long allocNativePath(String path) {
            return INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            return INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            fsyncCalls++;
            if (isFsyncGateModeEnabled) {
                if (!fsyncGateErrorConsumed) {
                    fsyncGateErrorConsumed = true;
                    return -1;
                }
                return 0;
            }
            return isFsyncFailureEnabled ? -1 : INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return INSTANCE.length(fd);
        }

        @Override
        public long length(String path) {
            return INSTANCE.length(path);
        }

        @Override
        public long length(long pathPtr) {
            return INSTANCE.length(pathPtr);
        }

        @Override
        public int lock(int fd) {
            return INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return INSTANCE.mkdir(path, mode);
        }

        @Override
        public int mlock(long addr, long len) {
            mlockCalls++;
            lastMlockLen = len;
            return isMlockRefusalEnabled ? -1 : 0;
        }

        @Override
        public int msync(long addr, long len, boolean async) {
            msyncCalls++;
            if (isFsyncGateModeEnabled && fsyncGateErrorConsumed) {
                // consumed-error semantics: no dirty pages, seen errseq -> 0
                return 0;
            }
            return INSTANCE.msync(addr, len, async);
        }

        @Override
        public int munlock(long addr, long len) {
            munlockCalls++;
            Runnable hook = onMunlock;
            if (hook != null) {
                hook.run();
            }
            return 0;
        }

        @Override
        public int openCleanRW(String path) {
            return INSTANCE.openCleanRW(path);
        }

        @Override
        public int openCleanRW(long pathPtr) {
            return INSTANCE.openCleanRW(pathPtr);
        }

        @Override
        public int openRW(String path) {
            return INSTANCE.openRW(path);
        }

        @Override
        public int openRW(long pathPtr) {
            return INSTANCE.openRW(pathPtr);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            return INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            return INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return INSTANCE.write(fd, addr, len, offset);
        }
    }
}
