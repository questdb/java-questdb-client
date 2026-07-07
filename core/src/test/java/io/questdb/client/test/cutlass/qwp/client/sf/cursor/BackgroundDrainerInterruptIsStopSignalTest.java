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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Pins the drainer-side half of the interrupted-shutdown hardening: a bare
 * thread interrupt — no {@link BackgroundDrainer#requestStop()} — must act
 * as a stop signal, not degrade the drainer into a busy-spin.
 * <p>
 * {@code BackgroundDrainerPool.close()} pairs every executor interrupt
 * ({@code shutdownNow}) with a {@code requestStop()} sweep, but that pairing
 * is caller discipline: any interrupt reaching a drainer thread without the
 * flag (a future shutdownNow call site, a stray interrupt on the pool's
 * threads) would otherwise hit loops whose only wait primitive is
 * {@code LockSupport.parkNanos} — which returns immediately while the
 * interrupt status is pending and never clears it. With the status pending
 * and {@code stopRequested} false, the backoff/poll loops spin at 100% CPU
 * for as long as the outage lasts, holding the slot lock. The drainer
 * therefore folds a pending interrupt into {@code stopRequested} at its
 * park sites (status deliberately left set — the interrupted-teardown
 * delegation protocol depends on it, see
 * {@code BackgroundDrainerInterruptedTeardownTest}).
 */
public class BackgroundDrainerInterruptIsStopSignalTest {

    private static final long SEGMENT_BYTES = 1L << 20;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-int-stop-" + System.nanoTime()).toString();
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

    @Test(timeout = 60_000)
    public void bareInterruptStopsConnectPhaseDrainer() throws Exception {
        // Slot with one published, unacked frame so the drainer opens a real
        // engine (takes the slot lock) and enters its connect-retry loop.
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            CursorSendEngine prep = new CursorSendEngine(tmpDir, SEGMENT_BYTES);
            try {
                for (int i = 0; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }
                Assert.assertEquals(0L, prep.appendBlocking(buf, 16));
            } finally {
                prep.close();
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }

        // Unreachable cluster: plain transport failures retried indefinitely
        // under Invariant B -- absent a stop signal the drainer never exits.
        final CountDownLatch firstAttempt = new CountDownLatch(1);
        final BackgroundDrainer drainer = new BackgroundDrainer(
                tmpDir, SEGMENT_BYTES, Long.MAX_VALUE,
                () -> {
                    firstAttempt.countDown();
                    throw new IOException("connection refused (test)");
                },
                5_000L, 1L, 10L, false, 0L);

        Thread runner = new Thread(drainer::run, "drainer-runner");
        runner.setDaemon(true);
        runner.start();
        try {
            Assert.assertTrue("drainer never reached its connect loop",
                    firstAttempt.await(10, TimeUnit.SECONDS));

            // Bare interrupt -- deliberately NOT paired with requestStop().
            runner.interrupt();

            // The mapping must fold the pending status into stopRequested and
            // exit STOPPED. Pre-hardening: parks are no-ops, the flag never
            // rises, outcome stays PENDING while the loop spins -- this join
            // times out.
            runner.join(10_000L);
            Assert.assertFalse(
                    "a bare interrupt did not stop the drainer: with the status pending "
                            + "every parkNanos is an immediate return, so the connect-retry "
                            + "loop busy-spins at 100% CPU holding the slot lock",
                    runner.isAlive());
            Assert.assertEquals("interrupt must map onto the normal STOPPED exit",
                    BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
            Assert.assertTrue("interrupt must be folded into stopRequested",
                    drainer.isStopRequested());
            // Engine closed on the way out: the slot lock must be free.
            new CursorSendEngine(tmpDir, SEGMENT_BYTES).close();
        } finally {
            // Red-run cleanup: unhook a still-spinning drainer so a failing
            // assertion doesn't leave a hot daemon thread behind.
            drainer.requestStop();
        }
    }
}
