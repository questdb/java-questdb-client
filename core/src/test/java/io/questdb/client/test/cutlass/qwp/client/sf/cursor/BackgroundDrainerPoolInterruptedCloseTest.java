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

import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerPool;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Regression guard for the interrupted-close busy-spin hazard in
 * {@link BackgroundDrainerPool#close()}: the {@code InterruptedException}
 * arm called only {@code executor.shutdownNow()} — never
 * {@link BackgroundDrainer#requestStop()} on the active drainers. The
 * pre-shutdown split-stop sweep deliberately spares actively-draining
 * drainers ({@code ackedFsn >= 0}), and the graceful-timeout sweep never
 * runs when {@code awaitTermination} throws instead of returning false, so
 * on this path an actively-draining drainer kept
 * {@code stopRequested == false} while shutdownNow's interrupt turned every
 * {@code LockSupport.parkNanos} in its poll/backoff loops into an immediate
 * return (park does not clear the interrupt status, and the drainer never
 * checks it): a 100% CPU busy-spin for as long as the outage lasted, with
 * the slot's on-disk lock pinned against re-adoption.
 */
public class BackgroundDrainerPoolInterruptedCloseTest {

    private static final long SEGMENT_BYTES = 1L << 20;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-pool-int-close-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    /**
     * Pre-fix: after the interrupted close(), {@code stopRequested} stays
     * false — the drainer thread (its interrupt status set by shutdownNow,
     * every park now a no-op) hot-spins in its connect-retry loop forever
     * and the slot lock is never released. Post-fix the catch sweeps
     * {@code active -> requestStop()}: the drainer observes the flag
     * instantly (the parks are hot anyway), exits STOPPED, and its engine
     * close releases the slot lock.
     */
    @Test(timeout = 60_000)
    public void interruptedCloseMustStopActivelyDrainingDrainers() throws Exception {
        // 1. Slot with one published, unacked frame so the drainer opens a
        //    real engine (publishedFsn=0 > ackedFsn=-1 -> not "already
        //    drained") and takes the on-disk slot lock.
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            CursorSendEngine prep = new CursorSendEngine(tmpDir, SEGMENT_BYTES);
            try {
                for (int i = 0; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }
                Assert.assertEquals(0L, prep.appendBlocking(buf, 16));
            } finally {
                // Unacked data on disk -> close() keeps the .sfa files.
                prep.close();
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }

        // 2. Unreachable cluster: every connect attempt fails with a plain
        //    transport error, which the drainer retries indefinitely under
        //    Invariant B -- it can only exit via requestStop().
        final CountDownLatch firstAttempt = new CountDownLatch(1);
        final BackgroundDrainer drainer = new BackgroundDrainer(
                tmpDir, SEGMENT_BYTES, Long.MAX_VALUE,
                () -> {
                    firstAttempt.countDown();
                    throw new IOException("connection refused (test)");
                },
                5_000L, 1L, 10L, false, 0L);

        BackgroundDrainerPool pool = new BackgroundDrainerPool(1);
        try {
            pool.submit(drainer);
            Assert.assertTrue("drainer never reached its connect loop",
                    firstAttempt.await(10, TimeUnit.SECONDS));

            // 3. Make the pool classify the drainer as ACTIVELY DRAINING so
            //    close()'s pre-shutdown split-stop (PENDING && ackedFsn < 0)
            //    spares it -- the production shape is a drainer that shipped
            //    frames before the outage and is now riding out reconnects.
            //    The published watermark is only ever written by the drain
            //    poll loop, which this connect-phase drainer never reaches,
            //    so the injected value is stable for the test's duration.
            Field ackedFsn = BackgroundDrainer.class.getDeclaredField("ackedFsn");
            ackedFsn.setAccessible(true);
            ackedFsn.setLong(drainer, 0L);

            // 4. Deterministic interrupted close: with this thread's
            //    interrupt status already set, awaitTermination throws
            //    InterruptedException on entry -- close() runs its
            //    interrupted arm without any timing race.
            Thread.currentThread().interrupt();
            boolean interruptReasserted;
            try {
                pool.close();
            } finally {
                // Clear the flag unconditionally so the polls below don't
                // trip over it; assert outside the finally so an unexpected
                // close() failure is not masked by the assertion.
                interruptReasserted = Thread.interrupted();
            }
            Assert.assertTrue("close() must re-assert the interrupt flag",
                    interruptReasserted);

            // 5. The pin: the interrupted arm must stop-signal active
            //    drainers SYNCHRONOUSLY -- the flag is up the moment close()
            //    returns. This isolates the pool's sweep from the drainer's
            //    own interrupt->stop fallback (which raises the flag only
            //    when the drainer thread next runs a park-loop check):
            //    without the sweep this immediate read races that fallback
            //    at best, and pre-hardening the drainer busy-spun at 100%
            //    CPU holding the slot lock with the flag never rising.
            Assert.assertTrue(
                    "interrupted pool.close() returned without stop-signaling an "
                            + "actively-draining drainer: the InterruptedException arm must "
                            + "sweep active -> requestStop() before executor.shutdownNow()",
                    drainer.isStopRequested());

            // 6. And the drainer honors it promptly: exits STOPPED...
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (drainer.outcome() == BackgroundDrainer.DrainOutcome.PENDING
                    && System.nanoTime() < deadlineNanos) {
                Thread.yield();
            }
            Assert.assertEquals("stopped drainer must exit with outcome STOPPED",
                    BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());

            // 7. ...and releases the slot lock (engine closed), so the slot
            //    can be re-adopted.
            deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!isSlotLockFree() && System.nanoTime() < deadlineNanos) {
                Thread.yield();
            }
            Assert.assertTrue("slot lock never released after the interrupted close",
                    isSlotLockFree());
        } finally {
            // Red-run cleanup: unhook a still-spinning drainer so a failing
            // assertion doesn't leave a hot daemon thread behind for the
            // rest of the suite.
            drainer.requestStop();
        }
    }

    /**
     * Public, behavioral probe of the slot lock: opening an engine on the
     * slot succeeds iff no other engine holds the on-disk lock. The probe
     * engine is closed immediately; the slot's unacked data keeps its files
     * on disk, so probing is observation-only.
     */
    private boolean isSlotLockFree() {
        try {
            new CursorSendEngine(tmpDir, SEGMENT_BYTES).close();
            return true;
        } catch (IllegalStateException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("already in use")) {
                return false;
            }
            throw e;
        }
    }
}
