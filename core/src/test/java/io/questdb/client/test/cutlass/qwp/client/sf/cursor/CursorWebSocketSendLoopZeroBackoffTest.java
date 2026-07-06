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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Regression guard for the I/O-thread silent-death hazard in
 * {@link CursorWebSocketSendLoop}: a {@code RuntimeException} escaping
 * {@code fail()}/{@code connectLoop} pierced {@code ioLoop}'s catch-all and
 * killed the I/O thread with {@code running} still {@code true} and no
 * terminal latched — the producer kept buffering into SF against a dead loop,
 * the exact silent stall Invariant B exists to prevent.
 * <p>
 * The concrete trigger was the retry-loop jitter:
 * {@code ThreadLocalRandom.nextLong(backoffMillis)} throws
 * {@code IllegalArgumentException} when the bound is {@code <= 0}. The
 * builder validates {@code reconnect_initial_backoff_millis > 0}, but the
 * loop constructors are public and do not — direct construction with a
 * non-positive backoff (as this test does) turned the very first backoff
 * into an unlatched thread death. Fixed twice over: the jitter bounds are
 * clamped with {@code Math.max(1L, …)} (mirroring {@code BackgroundDrainer}'s
 * sweep loop), and {@code ioLoop}'s catch guards the {@code fail(t)} call so
 * any residual secondary failure latches a terminal (latch-and-die) instead
 * of escaping.
 */
public class CursorWebSocketSendLoopZeroBackoffTest {

    @Rule
    public final TemporaryFolder sfDir = TemporaryFolder.builder().assureDeletion().build();

    /**
     * Pins the static bounded-connect variant ({@code connectWithRetry},
     * user-thread blocking initial connect): a non-positive initial backoff
     * must exhaust the budget and surface the real (transport) failure as a
     * {@link LineSenderException} — not blow up in the jitter computation
     * with an {@code IllegalArgumentException}.
     */
    @Test(timeout = 30_000)
    public void connectWithRetryToleratesNonPositiveBackoff() {
        try {
            CursorWebSocketSendLoop.connectWithRetry(
                    () -> {
                        throw new IOException("connection refused (test)");
                    },
                    50L,  // budget: fail fast
                    0L,   // non-positive initial backoff: pre-fix IAE in nextLong
                    1L,
                    "test-connect");
            Assert.fail("expected LineSenderException on budget exhaustion");
        } catch (LineSenderException expected) {
            Assert.assertTrue(
                    "budget exhaustion should carry the attempt summary, got: "
                            + expected.getMessage(),
                    expected.getMessage().contains("failed after"));
        }
    }

    /**
     * Pre-fix: the I/O thread walked the async-initial-connect retry loop,
     * hit {@code nextLong(0)} on the first backoff, and the IAE — re-thrown
     * once more from the {@code fail(t)} re-entry — escaped {@code ioLoop}'s
     * catch. The thread died after at most two recorded attempts with
     * {@code isRunning() == true} and {@code getTerminalError() == null}:
     * invisible to the producer. Post-fix the clamped jitter keeps the loop
     * retrying indefinitely (Invariant B), so attempts keep climbing and no
     * terminal is latched.
     */
    @Test(timeout = 30_000)
    public void zeroBackoffConnectFailuresKeepRetryingInsteadOfKillingIoThread() throws Exception {
        try (CursorSendEngine engine = new CursorSendEngine(
                sfDir.getRoot().getAbsolutePath(), 16_384)) {
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    null, engine, 0, 1_000_000L,
                    () -> {
                        // Plain connect failure: retried forever under
                        // Invariant B — never terminal, never latched.
                        throw new IOException("connection refused (test)");
                    },
                    0,
                    0,   // non-positive initial backoff: pre-fix IAE in nextLong
                    1);
            try {
                loop.start();
                // Post-fix the zero-backoff loop records attempts at a very
                // high rate; pre-fix the count freezes at <= 2 when the IAE
                // kills the thread, and this poll times out.
                long deadlineNanos = System.nanoTime() + 20_000_000_000L;
                while (loop.getTotalReconnectAttempts() < 5
                        && System.nanoTime() < deadlineNanos) {
                    Thread.yield();
                }
                Assert.assertTrue(
                        "I/O thread stopped retrying after "
                                + loop.getTotalReconnectAttempts()
                                + " attempts -- it died without latching a terminal "
                                + "(the Invariant B silent stall)",
                        loop.getTotalReconnectAttempts() >= 5);
                Assert.assertTrue("loop must still be running", loop.isRunning());
                Assert.assertNull(
                        "plain connect failures must not latch a terminal (Invariant B)",
                        loop.getTerminalError());
            } finally {
                loop.close();
            }
        }
    }
}
