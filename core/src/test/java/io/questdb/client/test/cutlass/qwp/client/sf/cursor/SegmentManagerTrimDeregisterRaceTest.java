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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Red test for {@code SegmentManager.totalBytes} drift on the
 * <b>trim</b> path of {@code serviceRing} (lines 365-378). The trim loop
 * subtracts {@code s.sizeBytes()} per drained segment with no
 * {@code stillRegistered} re-check; if {@code deregister(ring)} fires
 * between the worker's {@code rings} snapshot and its
 * {@code drainTrimmable} call, deregister's
 * {@code totalBytes -= ring.totalSegmentBytes()} already accounts for
 * those sealed segments, and the loop subtracts them again. Drift is
 * negative (over-subtraction) and persists for the lifetime of the
 * manager, so {@code sf_max_total_bytes} backpressure either fires too
 * early (false-positive cap) or the cap as a memory bound is broken.
 *
 * <p>Counterpart to {@link SegmentManagerTotalBytesRaceTest}, which
 * targets the spare-install commit drift on the same path. The
 * spare-install path has a {@code stillRegistered} guard
 * ({@code SegmentManager} lines 323-336); the trim path was missed.
 *
 * <p>Setup per ring: one frame fits in a segment, so a second append
 * always rotates. After rotation the prior active is sealed; ack'ing
 * FSN 0 makes it trimmable. Producer holds the ring open across
 * deregister so {@code SegmentRing.close} (which clears
 * {@code sealedSegments}) cannot pre-empt the worker's drain — close
 * before drain would close the bug's window.
 *
 * <p>Drift direction: in the bug, {@code totalBytes} ends below 0;
 * after the fix it is exactly 0.
 */
public class SegmentManagerTrimDeregisterRaceTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-trim-race-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        rmDirRecursive(tmpDir);
    }

    @Test(timeout = 60_000L)
    public void testTrimPathDoesNotDoubleSubtractAfterDeregister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // One frame per segment, so a second append always rotates.
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            long maxTotal = segSize * 32_768L;

            try (SegmentManager mgr = new SegmentManager(
                    segSize, 1_000L /* 1us tick — busy-poll */, maxTotal)) {
                mgr.start();

                final int threads = 8;
                final int perThread = 200;
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(threads);
                final AtomicReference<Throwable> failure = new AtomicReference<>();

                // Hold rings open until after the assertion so close() does
                // not clear sealedSegments before the worker's drain — the
                // race window only stays open while sealedSegments has the
                // trimmable seg0 in it.
                final List<List<SegmentRing>> outstanding = new ArrayList<>();
                for (int t = 0; t < threads; t++) outstanding.add(new ArrayList<>());

                for (int t = 0; t < threads; t++) {
                    final int threadId = t;
                    final List<SegmentRing> myRings = outstanding.get(t);
                    Thread worker = new Thread(() -> {
                        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                        try {
                            start.await();
                            for (int i = 0; i < perThread; i++) {
                                String dir = tmpDir + "/t" + threadId + "_r" + i;
                                assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
                                String activePath = dir + "/sf-initial.sfa";
                                MmapSegment seg0 = MmapSegment.create(activePath, 0L, segSize);
                                SegmentRing ring = new SegmentRing(seg0, segSize);
                                myRings.add(ring);

                                mgr.register(ring, dir);
                                // FSN 0 fills seg0. Wait for the manager to
                                // park a spare. Then FSN 1 rotates: seg0
                                // joins sealedSegments, the spare becomes
                                // active.
                                ring.appendOrFsn(buf, 32);
                                long deadline = System.nanoTime() + 1_000_000_000L;
                                while (ring.needsHotSpare()) {
                                    if (System.nanoTime() > deadline) {
                                        throw new AssertionError(
                                                "spare never arrived for ring t" + threadId + "_r" + i);
                                    }
                                    Thread.onSpinWait();
                                }
                                ring.appendOrFsn(buf, 32);
                                // ackedFsn = 0 makes seg0 (baseSeq=0,
                                // frameCount=1, lastSeq=0) trimmable. The
                                // very next worker tick will drain it
                                // unless deregister beats it.
                                ring.acknowledge(0L);

                                // Tiny burn so the worker has a realistic
                                // chance to land in serviceRing(this ring)
                                // with a snapshot that still includes us
                                // BEFORE deregister fires.
                                spinNanos(20_000L);
                                mgr.deregister(ring);
                                // DO NOT close: close() would clear
                                // sealedSegments under the ring monitor and
                                // foreclose the worker's drain.
                            }
                        } catch (Throwable t1) {
                            failure.compareAndSet(null, t1);
                        } finally {
                            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
                            done.countDown();
                        }
                    }, "trim-race-producer-" + t);
                    worker.setDaemon(true);
                    worker.start();
                }

                start.countDown();
                assertTrue("producers should finish",
                        done.await(40, TimeUnit.SECONDS));
                Throwable f = failure.get();
                if (f != null) throw new AssertionError("producer thread failed", f);

                // Let any in-flight serviceRing iterations land their trim
                // subtraction before reading totalBytes.
                Thread.sleep(200L);

                long observed = readTotalBytes(mgr);

                // Now safe to close. Some rings will already have empty
                // sealedSegments (worker drained); others won't (close
                // beat drain). close() handles both.
                for (List<SegmentRing> rings : outstanding) {
                    for (SegmentRing ring : rings) ring.close();
                }

                assertEquals(
                        "totalBytes drifted away from 0. Observed " + observed
                                + " bytes (segSize=" + segSize + ", drift = "
                                + (observed / (double) segSize) + " segments). "
                                + "Negative drift means SegmentManager.serviceRing's "
                                + "trim loop (lines 365-378) subtracted bytes that "
                                + "deregister had already subtracted via "
                                + "ring.totalSegmentBytes(): no stillRegistered guard "
                                + "mirrors the spare-install path at lines 323-336. "
                                + "Fix: gate `totalBytes -= sz` on a stillRegistered "
                                + "re-check, or move totalBytes accounting fully into "
                                + "SegmentRing so it can't be split across two threads' "
                                + "bookkeeping.",
                        0L, observed);
            }
        });
    }

    private static long readTotalBytes(SegmentManager mgr) throws Exception {
        Field f = SegmentManager.class.getDeclaredField("totalBytes");
        f.setAccessible(true);
        Field lockF = SegmentManager.class.getDeclaredField("lock");
        lockF.setAccessible(true);
        Object lock = lockF.get(mgr);
        synchronized (lock) {
            return f.getLong(mgr);
        }
    }

    private static void rmDirRecursive(String dir) {
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            rmDirRecursive(child);
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

    private static void spinNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }
}
