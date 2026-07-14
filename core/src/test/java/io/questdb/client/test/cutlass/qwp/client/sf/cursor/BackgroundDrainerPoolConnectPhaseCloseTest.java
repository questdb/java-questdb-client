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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerPool;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Coverage of {@link BackgroundDrainerPool#close()}'s split stop policy
 * (M11): a drainer that never started draining — still inside its
 * connect-retry loop, e.g. during a cluster outage — is stop-signaled
 * BEFORE the graceful-drain window, so {@code close()} returns in roughly
 * one stop-check park chunk (~50ms) instead of burning the full
 * {@code GRACEFUL_DRAIN_MILLIS + STOP_GRACE_MILLIS} (~3s) on a drainer
 * that cannot possibly finish.
 * <p>
 * The factory throws a plain transport-shaped {@link LineSenderException}
 * (the shape of every outage-time connect failure), which doubles this
 * test as the contract check that such failures are retried under
 * Invariant B — outcome stays PENDING while running, becomes STOPPED on
 * close, and NEVER drops a {@code .failed} sentinel.
 */
public class BackgroundDrainerPoolConnectPhaseCloseTest {

    /**
     * Well below the pool's 2.5s graceful window: generous enough for CI
     * scheduling jitter, tight enough that a regression to
     * "graceful-wait-first" (>= 2500ms) fails loudly.
     */
    private static final long CLOSE_BUDGET_MILLIS = 2_000L;
    /** Longer than the close budget: close() must not sleep a backoff out. */
    private static final long LONG_BACKOFF_MILLIS = 30_000L;
    private static final long SEGMENT_SIZE_BYTES = 16384L;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 20;

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-pool-connect-close-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        rmDirRec(slotPath);
    }

    @Test
    public void testCloseStopsConnectPhaseDrainerWithoutBurningGracefulWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Unacked data on disk: without it run() exits SUCCESS before
            // ever entering the connect-retry loop this test needs.
            seedSlot(3);
            final CountDownLatch firstAttempt = new CountDownLatch(1);
            final AtomicInteger attempts = new AtomicInteger();
            final CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                attempts.incrementAndGet();
                firstAttempt.countDown();
                // Plain transport-shaped failure: the shape of every
                // outage-time connect error. Must be retried, never
                // quarantined.
                throw new LineSenderException(
                        "Failed to connect: all endpoints unreachable (simulated outage)");
            };
            final BackgroundDrainer drainer = new BackgroundDrainer(
                    slotPath,
                    SEGMENT_SIZE_BYTES,
                    SF_MAX_TOTAL_BYTES,
                    factory,
                    /* reconnectMaxDurationMillis */ 60_000L,
                    /* reconnectInitialBackoffMillis */ LONG_BACKOFF_MILLIS,
                    /* reconnectMaxBackoffMillis */ LONG_BACKOFF_MILLIS,
                    /* requestDurableAck */ false,
                    /* durableAckKeepaliveIntervalMillis */ 0L);
            final BackgroundDrainerPool pool = new BackgroundDrainerPool(1);
            pool.submit(drainer);
            assertTrue("drainer must enter its connect-retry loop",
                    firstAttempt.await(5, TimeUnit.SECONDS));

            final long startNanos = System.nanoTime();
            pool.close();
            final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;

            assertTrue("close() must stop a connect-phase drainer immediately (split stop "
                            + "policy), not wait out the graceful-drain window; took "
                            + elapsedMillis + "ms with a " + LONG_BACKOFF_MILLIS
                            + "ms drainer backoff in flight",
                    elapsedMillis < CLOSE_BUDGET_MILLIS);
            assertEquals("a stop-signaled connect-phase drainer exits STOPPED (slot stays "
                            + "adoptable), never FAILED",
                    BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
            assertEquals("a connect-phase drainer must never have advanced ackedFsn",
                    -1L, drainer.getAckedFsn());
            assertTrue("the drainer must have attempted at least one connect sweep",
                    attempts.get() >= 1);
            assertFalse("outage-shaped connect failures must never quarantine the slot "
                            + "(.failed sentinel)",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    /**
     * Seeds {@code frames} frames and returns nothing the test needs beyond
     * the on-disk unacked state; mirrors
     * {@code BackgroundDrainerTransportOutageRecoveryTest.seedSlot}.
     */
    private void seedSlot(int frames) {
        try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_SIZE_BYTES)) {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, payload[i]);
                }
                for (int i = 0; i < frames; i++) {
                    engine.appendBlocking(buf, 16);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private static void rmDirRec(String dir) {
        if (dir == null || !Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) rmDirRec(child);
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
