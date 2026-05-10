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
 * Red test for M2 — {@code SegmentManager.totalBytes} accounting drift
 * under register/serviceRing/deregister contention.
 *
 * <p>The bug fires in this exact window inside {@code serviceRing}:
 * <pre>
 *   1. snapshot observedTotal under lock
 *   2. drop lock; create MmapSegment (slow IO — race window opens)
 *   3. ring.installHotSpare(spare)
 *   4. re-acquire lock; totalBytes += segmentSize       (commit)
 * </pre>
 * If {@code deregister(ring)} fires between (1) and (3), it subtracts
 * {@code ring.totalSegmentBytes()} — which at that moment <em>does not</em>
 * include the in-flight spare — and the commit at (4) adds {@code
 * segmentSize} with no future subtractor. {@code totalBytes} permanently
 * inflates by one segment per occurrence.
 *
 * <p>The test runs many parallel producer threads that register a ring,
 * pause briefly to let the worker enter {@code MmapSegment.create}, then
 * deregister, then close the ring later. Across thousands of iterations
 * with the worker polling at sub-microsecond intervals the race fires
 * many times and {@code totalBytes} accumulates drift.
 *
 * <p>The deferred {@code ring.close()} matters: if the producer closes
 * the ring before the worker calls {@code installHotSpare}, the install
 * throws ISE, the spare is cleaned up by the manager's catch, and no
 * commit fires (safe path). The bug requires the ring to be deregistered
 * but still open when the worker installs.
 */
public class SegmentManagerTotalBytesRaceTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-race-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        rmDirRecursive(tmpDir);
    }

    @Test(timeout = 60_000L)
    public void testTotalBytesIsZeroAfterAllRingsDeregistered() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            // Cap large enough that the manager keeps provisioning spares
            // (cap is not the rate-limiter for this test).
            long maxTotal = segSize * 8192L;

            try (SegmentManager mgr = new SegmentManager(
                    segSize, 1_000L /* 1us tick — busy-poll */, maxTotal)) {
                mgr.start();

                final int threads = 8;
                final int perThread = 200;
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(threads);
                final AtomicReference<Throwable> failure = new AtomicReference<>();

                // Each producer holds onto its rings until the end so the
                // worker can install spares on already-deregistered rings
                // (the bug scenario).
                final List<List<SegmentRing>> outstanding = new ArrayList<>();
                for (int t = 0; t < threads; t++) outstanding.add(new ArrayList<>());

                for (int t = 0; t < threads; t++) {
                    final int threadId = t;
                    final List<SegmentRing> myRings = outstanding.get(t);
                    Thread worker = new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < perThread; i++) {
                                String dir = tmpDir + "/t" + threadId + "_r" + i;
                                assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
                                String activePath = dir + "/sf-initial.sfa";
                                MmapSegment active = MmapSegment.create(activePath, 0L, segSize);
                                SegmentRing ring = new SegmentRing(active, segSize);
                                myRings.add(ring);
                                mgr.register(ring, dir);
                                // Tiny burn so the manager's worker has a
                                // realistic chance to start serviceRing on
                                // this ring before we deregister.
                                spinNanos(20_000L);
                                mgr.deregister(ring);
                                // DO NOT close the ring yet. The bug window
                                // requires installHotSpare to succeed on a
                                // deregistered-but-open ring.
                            }
                        } catch (Throwable t1) {
                            failure.compareAndSet(null, t1);
                        } finally {
                            done.countDown();
                        }
                    }, "race-producer-" + t);
                    worker.setDaemon(true);
                    worker.start();
                }

                start.countDown();
                assertTrue("producers should finish",
                        done.await(40, TimeUnit.SECONDS));
                Throwable f = failure.get();
                if (f != null) throw new AssertionError("producer thread failed", f);

                // Let any in-flight serviceRing iterations land their
                // commits before we read totalBytes.
                Thread.sleep(200L);

                long observed = readTotalBytes(mgr);

                // Now safe to close every ring (closes any spare the
                // worker may have installed after deregister).
                for (List<SegmentRing> rings : outstanding) {
                    for (SegmentRing ring : rings) ring.close();
                }

                assertEquals(
                        "totalBytes should be 0 after every ring is deregistered. "
                                + "Drift means the manager's worker installed a hot spare "
                                + "into a deregistered ring AFTER deregister had already "
                                + "subtracted ring.totalSegmentBytes(), and then committed "
                                + "+= segmentSize with no future subtractor. Observed "
                                + "drift bytes: " + observed,
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

    private static void spinNanos(long nanos) {
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline) {
            Thread.onSpinWait();
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
}
