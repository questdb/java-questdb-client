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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpIngressRoleRejectedException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Coverage of {@link BackgroundDrainer#connectWithDurableAckRetry()} —
 * the asymmetric-tolerance path the drainer uses when its initial
 * connect lands on a cluster that doesn't echo {@code X-QWP-Durable-Ack}.
 * <p>
 * The foreground sender treats this condition as terminal because the
 * producer is actively writing data; the drainer treats it as transient
 * because source data is pinned by {@code durableAckMode=true} (the
 * loop only trims on STATUS_DURABLE_ACK frames). These tests verify
 * the per-attempt observability callback, the bounded retry budget, the
 * eventual {@code .failed} sentinel on persistent failure, and that
 * unrelated exceptions still mark the slot failed immediately.
 */
public class BackgroundDrainerDurableAckRetryTest {

    /**
     * Every {@link StubWebSocketClient} allocated by a test, so its ~128 KB
     * of eagerly-malloced native buffers (recv + send + control) can be
     * released before the leak check fires (M7).
     */
    private static final List<StubWebSocketClient> LIVE_STUBS =
            Collections.synchronizedList(new ArrayList<>());

    private static final long FAST_BACKOFF_MAX_MILLIS = 4L;
    private static final long FAST_BACKOFF_MILLIS = 1L;
    private static final long FAST_RECONNECT_MAX_DURATION_MILLIS = 60_000L;
    private static final long SEGMENT_SIZE_BYTES = 1L << 20;
    private static final long SF_MAX_TOTAL_BYTES = 1L << 22;

    private String slotPath;

    @Before
    public void setUp() {
        slotPath = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-da-retry-" + System.nanoTime()).toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        // Safety net for exits that bypass the assertMemoryLeak wrapper;
        // normally a no-op because the wrapper's finally already closed
        // and cleared the stubs (close() is idempotent).
        closeAllStubs();
        if (slotPath == null) return;
        long find = Files.findFirst(slotPath);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(slotPath + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(slotPath);
    }

    @Test
    public void testCallbackArgumentsCarrySlotPathAndAttemptNumber() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            ScriptedFactory factory = ScriptedFactory.failingTimes(3,
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertSame(factory.successSentinel(), out);
            assertEquals(3, listener.unavailableSlotPaths.size());
            for (int i = 0; i < 3; i++) {
                assertEquals(slotPath, listener.unavailableSlotPaths.get(i));
                assertEquals(Integer.valueOf(i + 1), listener.unavailableAttempts.get(i));
            }
            assertEquals(0, listener.persistentFailures.get());
        });
    }

    @Test
    public void testEscalatesAfterMaxAttemptsAndDropsSentinel() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull("escalation must signal failure to caller", out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            // The escalation attempt itself must not also fire onDurableAckUnavailable.
            // Threshold attempts trigger one persistent-failure callback and
            // (threshold - 1) unavailable callbacks.
            int threshold = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
            assertEquals(threshold - 1, listener.unavailableAttempts.size());
            assertEquals(1, listener.persistentFailures.get());
            assertEquals(threshold, listener.lastPersistentTotalAttempts.get());
            assertTrue("elapsed >= 0", listener.lastPersistentElapsedMs.get() >= 0);
            // Sentinel dropped with the right reason prefix.
            String sentinel = slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME;
            assertTrue("expected .failed sentinel at " + sentinel, Files.exists(sentinel));
            assertNotNull("lastErrorMessage populated", drainer.getLastErrorMessage());
        });
    }

    @Test
    public void testListenerThrowingOnPersistentFailureStillMarksFailed() throws Exception {
        assertMemoryLeak(() -> {
            BackgroundDrainerListener throwing = new BackgroundDrainerListener() {
                @Override
                public void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
                    throw new RuntimeException("listener boom (persistent)");
                }

                @Override
                public void onDurableAckUnavailable(String slotPath, int attemptNumber) {
                    // no-op
                }
            };
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(throwing);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            // Sentinel must be dropped even though the listener threw.
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testListenerThrowingOnUnavailableContinuesRetrying() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger unavailableCalls = new AtomicInteger();
            BackgroundDrainerListener throwing = new BackgroundDrainerListener() {
                @Override
                public void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
                    Assert.fail("must not escalate");
                }

                @Override
                public void onDurableAckUnavailable(String slotPath, int attemptNumber) {
                    unavailableCalls.incrementAndGet();
                    throw new RuntimeException("listener boom (transient)");
                }
            };
            ScriptedFactory factory = ScriptedFactory.failingTimes(3,
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(throwing);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertSame(factory.successSentinel(), out);
            assertEquals(3, unavailableCalls.get());
            assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
            // No sentinel dropped on success.
            assertFalse(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testNoListenerNoNullPointerOnEscalation() throws Exception {
        assertMemoryLeak(() -> {
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainer(factory);
            // Intentionally leave listener null.
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testTerminalUpgradeMarksFailedImmediately() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            // A genuinely non-retriable upgrade error (non-421 5xx upgrade reject) is
            // terminal -- waiting will not fix it -- so the drainer quarantines on the
            // first attempt, exactly like the live sender's background loop halts on
            // auth/upgrade. A TRANSPORT error, by contrast, is transient and is
            // retried (see testTransportErrorNeverQuarantinesInvariantB).
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new WebSocketUpgradeException(500, null, "server error during upgrade"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            // Listener must not have been touched — this path doesn't fire either callback.
            assertEquals(0, listener.unavailableAttempts.size());
            assertEquals(0, listener.persistentFailures.get());
            // Sentinel dropped for a genuine terminal.
            String sentinel = slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME;
            assertTrue(Files.exists(sentinel));
            // The factory must have been invoked exactly once — no retry on a terminal.
            assertEquals(1, factory.attempts());
        });
    }

    @Test
    public void testReturnsClientOnSuccessFirstAttempt() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            ScriptedFactory factory = ScriptedFactory.alwaysSucceeding();
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertSame(factory.successSentinel(), out);
            assertEquals(1, factory.attempts());
            assertEquals(0, listener.unavailableAttempts.size());
            assertEquals(0, listener.persistentFailures.get());
            assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
            assertFalse(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testRetriesOnDurableAckMismatchThenSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            int failTimes = 5;
            ScriptedFactory factory = ScriptedFactory.failingTimes(failTimes,
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertSame(factory.successSentinel(), out);
            assertEquals(failTimes + 1, factory.attempts());
            assertEquals(failTimes, listener.unavailableAttempts.size());
            for (int i = 0; i < failTimes; i++) {
                assertEquals(Integer.valueOf(i + 1), listener.unavailableAttempts.get(i));
            }
            assertEquals(0, listener.persistentFailures.get());
            assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
            assertFalse(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testStopRequestedDuringRetryAbortsWithStoppedOutcome() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            // Slow factory: each attempt blocks for ~30ms throwing DA mismatch.
            // Combined with a 50ms reconnectMaxDuration we'd hit budget too,
            // so set a long budget and rely on requestStop() to break the loop.
            CountDownLatch firstFailureSeen = new CountDownLatch(1);
            ScriptedFactory factory = new ScriptedFactory(
                    /* successSentinel */ stubClient(),
                    /* throwingTimes */ Integer.MAX_VALUE,
                    /* throwSupplier */ () -> {
                firstFailureSeen.countDown();
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, /*reconnectMaxDurationMillis*/ 60_000L,
                    /*backoffInit*/ 5L, /*backoffMax*/ 10L);
            drainer.setListener(listener);
            Thread t = new Thread(drainer::connectWithDurableAckRetry, "test-helper");
            t.setDaemon(true);
            t.start();
            // Wait until at least one attempt has fired, then signal stop.
            Assert.assertTrue("first failure must occur promptly",
                    firstFailureSeen.await(2, TimeUnit.SECONDS));
            drainer.requestStop();
            t.join(5_000);
            Assert.assertFalse("helper must exit after stop", t.isAlive());
            assertEquals(BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
            // No persistent-failure callback on stop; no sentinel dropped.
            assertEquals(0, listener.persistentFailures.get());
            assertFalse(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testWallTimeBudgetEscalatesBeforeAttemptCap() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            // Each failure sleeps 12ms; budget is 25ms — second iteration must
            // observe deadline crossed without reaching the 16-attempt cap.
            ScriptedFactory factory = new ScriptedFactory(
                    /* successSentinel */ stubClient(),
                    /* throwingTimes */ Integer.MAX_VALUE,
                    /* throwSupplier */ () -> {
                try {
                    Thread.sleep(12);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, /*reconnectMaxDurationMillis*/ 25L,
                    /*backoffInit*/ 1L, /*backoffMax*/ 1L);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals("must escalate by wall time, not attempts", 1, listener.persistentFailures.get());
            int total = listener.lastPersistentTotalAttempts.get();
            assertTrue("escalated before reaching attempt cap (got " + total + ")",
                    total < BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS);
            assertTrue(total >= 1);
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testAllReplicaWindowNeverEscalatesInvariantB() throws Exception {
        assertMemoryLeak(() -> {
            // INVARIANT B (orphan drainer): a store-and-forward drainer must NEVER
            // quarantine a slot just because every reachable endpoint is a REPLICA.
            // A replica is promotable and a primary will reappear, so an all-replica
            // window is a TRANSIENT failover state -- the drainer must keep retrying
            // (capped backoff) until a primary is reachable, stopRequested, or SF
            // exhaustion. NEITHER the 16-attempt cap NOR the wall-clock reconnect
            // budget may escalate it to a .failed sentinel.
            //
            // Distinct from testEscalatesAfterMaxAttemptsAndDropsSentinel /
            // testWallTimeBudgetEscalatesBeforeAttemptCap, which use a genuine
            // durable-ack CAPABILITY gap (QwpDurableAckMismatchException -- a server
            // upgrades but does not advertise durable ack): that is a real config
            // problem and stays terminal. This test uses a role reject (every
            // endpoint is a replica right now), which must NOT be terminal.
            //
            // Red-first: connectWithDurableAckRetry() currently lumps role rejects in
            // with the durable-ack-mismatch give-up, so after the 16-attempt cap /
            // the budget it markFailed()s and returns -> the helper thread dies. Goes
            // green once the drainer treats an all-replica window as retry-forever
            // (split the catch: role reject -> retry; capability gap -> quarantine).
            CountingListener listener = new CountingListener();
            AtomicInteger attempts = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                attempts.incrementAndGet();
                return new QwpIngressRoleRejectedException(
                        QwpIngressRoleRejectedException.ROLE_REPLICA, "127.0.0.1", 9000);
            });
            // SHORT budget + tiny backoff so BOTH give-up triggers (the 16-attempt
            // cap and the 200ms wall clock) would fire promptly under the bug.
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, /*reconnectMaxDurationMillis*/ 200L, /*backoffInit*/ 1L, /*backoffMax*/ 2L);
            drainer.setListener(listener);
            Thread t = new Thread(drainer::connectWithDurableAckRetry, "invariant-b-orphan-drainer");
            t.setDaemon(true);
            t.start();

            // Observe well past BOTH the 200ms budget and the 16-attempt cap. Under
            // the bug the drainer escalates (within the cap time) and the helper
            // thread dies; a contract-honoring drainer is still retrying here.
            long observeUntilNanos = System.nanoTime() + 600_000_000L; // 600ms >> 200ms budget
            while (System.nanoTime() < observeUntilNanos && t.isAlive()) {
                Thread.sleep(10);
            }

            try {
                assertTrue("orphan drainer gave up on a transient all-replica window (attempts="
                                + attempts.get() + ", outcome=" + drainer.outcome() + "): Invariant B "
                                + "forbids quarantining a slot on the 16-attempt cap or the wall-clock "
                                + "reconnect budget -- a replica is promotable, so the drainer must keep "
                                + "retrying until a primary reappears or SF is exhausted",
                        t.isAlive());
                assertEquals("must not escalate a transient all-replica window to FAILED",
                        BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
                assertEquals("must not fire persistent-failure on an all-replica window",
                        0, listener.persistentFailures.get());
                assertFalse("must not quarantine (.failed sentinel) an all-replica window",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertTrue("must have retried past the 16-attempt cap (got " + attempts.get() + ")",
                        attempts.get() > BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS);
            } finally {
                drainer.requestStop();
                t.join(5_000);
            }
            assertFalse("helper must exit after stop", t.isAlive());
            assertEquals(BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
        });
    }

    @Test
    public void testTransportErrorNeverQuarantinesInvariantB() throws Exception {
        assertMemoryLeak(() -> {
            // INVARIANT B (orphan drainer): a fully-unreachable cluster (server down,
            // network partition -- every endpoint refuses / times out) is TRANSIENT,
            // not terminal. The server will come back; the drainer must keep retrying
            // (capped backoff) until it does, stopRequested, or SF exhaustion -- it
            // must NEVER quarantine the slot on the first failed sweep. This is the
            // exact behaviour of the live sender's background loop
            // (CursorWebSocketSendLoop.connectLoop: a transport error backs off and
            // retries), which the orphan drainer must match.
            //
            // Red-first: connectWithDurableAckRetry() currently routes any non-role,
            // non-durable-ack Throwable (including "all endpoints unreachable") to an
            // IMMEDIATE markFailed / .failed sentinel on the first attempt. Green once
            // transport errors are retried indefinitely like connectLoop. (Genuine
            // terminals -- auth / non-421 upgrade -- must still fail fast.)
            CountingListener listener = new CountingListener();
            AtomicInteger attempts = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                attempts.incrementAndGet();
                return new LineSenderException(
                        "Failed to connect: all 2 endpoint(s) unreachable; last=127.0.0.1:9000");
            });
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, /*reconnectMaxDurationMillis*/ 200L, /*backoffInit*/ 1L, /*backoffMax*/ 2L);
            drainer.setListener(listener);
            Thread t = new Thread(drainer::connectWithDurableAckRetry, "invariant-b-transport-drainer");
            t.setDaemon(true);
            t.start();

            // Observe well past the 200ms budget: the drainer must still be retrying.
            long observeUntilNanos = System.nanoTime() + 600_000_000L; // 600ms >> 200ms budget
            while (System.nanoTime() < observeUntilNanos && t.isAlive()) {
                Thread.sleep(10);
            }

            try {
                assertTrue("orphan drainer quarantined a fully-unreachable (server-down) cluster "
                                + "(attempts=" + attempts.get() + ", outcome=" + drainer.outcome()
                                + "): Invariant B says a down server is transient -- the drainer must "
                                + "retry indefinitely (exactly like the live background loop), never "
                                + "quarantine on a transport error",
                        t.isAlive());
                assertEquals("must not escalate a transient transport error to FAILED",
                        BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
                assertEquals("transport retry must not fire a persistent-failure escalation",
                        0, listener.persistentFailures.get());
                assertFalse("must not quarantine (.failed sentinel) a down server",
                        Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
                assertTrue("must have retried the down server well past the first sweep (got "
                                + attempts.get() + ")",
                        attempts.get() > BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS);
            } finally {
                drainer.requestStop();
                t.join(5_000);
            }
            assertFalse("helper must exit after stop", t.isAlive());
            assertEquals(BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
        });
    }

    @Test
    public void testJvmErrorEscapesConnectRetryLoop() throws Exception {
        assertMemoryLeak(() -> {
            // Regression (M3): catch (Throwable) in connectWithDurableAckRetry used
            // to swallow java.lang.Error (OOM, LinkageError, StackOverflowError)
            // into the indefinite "cluster unreachable" retry -- pinning the slot
            // .lock forever with no .failed sentinel and only a throttled WARN as
            // a trace. A JVM/programming failure is not a transport outage:
            // retrying cannot clear it, so it must escape the loop on the FIRST
            // sweep. run()'s outer catch then quarantines the slot (markFailed +
            // FAILED) and its finally releases the lock -- quarantine-and-exit.
            CountingListener listener = new CountingListener();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new LinkageError("simulated JVM failure"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            try {
                drainer.connectWithDurableAckRetry();
                Assert.fail("a JVM Error must escape the retry loop, "
                        + "not spin as a transport outage");
            } catch (LinkageError expected) {
                assertEquals("simulated JVM failure", expected.getMessage());
            }
            // No retry: the Error propagated on the very first attempt.
            assertEquals(1, factory.attempts());
            // Neither observability callback fires -- this is not a durable-ack
            // episode, and no escalation decision was made inside the loop.
            assertEquals(0, listener.unavailableAttempts.size());
            assertEquals(0, listener.persistentFailures.get());
        });
    }

    @Test
    public void testRoleRejectChurnDoesNotConsumeCapabilityGapBudgetInvariantB() throws Exception {
        assertMemoryLeak(() -> {
            // Rolling-upgrade interleave: a long all-replica window (role rejects),
            // then an old-build node is promoted and upgrades WITHOUT durable ack
            // (genuine capability gap). The transient window must not consume the
            // 16-attempt settle budget -- the gap phase gets the full budget.
            int roleRejects = 20; // > the attempt cap: under the bug the first gap attempt escalates
            int cap = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
            CountingListener listener = new CountingListener();
            AtomicInteger sweeps = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (sweeps.incrementAndGet() <= roleRejects) {
                    return new QwpIngressRoleRejectedException(
                            QwpIngressRoleRejectedException.ROLE_REPLICA, "127.0.0.1", 9000);
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            // 60s wall budget: only the attempt cap can fire in this test.
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals(1, listener.persistentFailures.get());
            assertEquals("escalation must count capability-gap attempts only",
                    cap, listener.lastPersistentTotalAttempts.get());
            assertEquals("full settle budget must be granted after the transient window",
                    roleRejects + cap, factory.attempts());
            // M10 split: the transient all-replica window lands on the
            // onPrimaryUnavailable stream (1..20), the capability-gap episode
            // on onDurableAckUnavailable (1..15; the 16th fires
            // persistent-failure instead). Neither stream sees the other's
            // counter, so a listener alerting on "attemptNumber approaching
            // the cap" no longer false-positives on role-reject churn.
            assertEquals(roleRejects, listener.primaryUnavailableAttempts.size());
            for (int i = 0; i < roleRejects; i++) {
                assertEquals(Integer.valueOf(i + 1), listener.primaryUnavailableAttempts.get(i));
            }
            assertEquals(cap - 1, listener.unavailableAttempts.size());
            for (int i = 0; i < cap - 1; i++) {
                assertEquals(Integer.valueOf(i + 1), listener.unavailableAttempts.get(i));
            }
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testFailoverWindowDoesNotBurnCapabilityGapWallClockInvariantB() throws Exception {
        assertMemoryLeak(() -> {
            // The wall-clock half of the settle budget must be anchored at the
            // FIRST capability-gap error, not at connect entry: an all-replica
            // window that outlives reconnectMaxDurationMillis must not cause the
            // first genuine capability-gap attempt to escalate on an already-
            // expired deadline. Catches the partial fix (separate counter but
            // entry-anchored deadline) that the attempt-cap test cannot see.
            int roleRejects = 20;
            long budgetMillis = 1_000L;
            int cap = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
            CountingListener listener = new CountingListener();
            AtomicInteger sweeps = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (sweeps.incrementAndGet() <= roleRejects) {
                    // Burn well past the wall-clock budget inside the transient
                    // window: 20 * 60ms = 1200ms of sleep alone >> 1000ms budget.
                    try {
                        Thread.sleep(60);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    return new QwpIngressRoleRejectedException(
                            QwpIngressRoleRejectedException.ROLE_REPLICA, "127.0.0.1", 9000);
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, budgetMillis, /*backoffInit*/ 1L, /*backoffMax*/ 2L);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals(1, listener.persistentFailures.get());
            assertEquals("first gap attempt must not observe a deadline burned by the "
                            + "transient window -- full attempt budget expected",
                    cap, listener.lastPersistentTotalAttempts.get());
            assertEquals(roleRejects + cap, factory.attempts());
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testRoleRejectResetsCapabilityGapEpisode() throws Exception {
        assertMemoryLeak(() -> {
            // An intervening role reject proves the topology changed (the node
            // that produced earlier gap errors is gone), so the settle budget
            // restarts: 15 gap errors, one role reject, then gaps again -- the
            // second episode gets the full 16 attempts, it does not inherit the
            // first episode's 15.
            int cap = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
            CountingListener listener = new CountingListener();
            AtomicInteger sweeps = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (sweeps.incrementAndGet() == cap) { // 16th sweep: role reject between the gap runs
                    return new QwpIngressRoleRejectedException(
                            QwpIngressRoleRejectedException.ROLE_REPLICA, "127.0.0.1", 9000);
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals(1, listener.persistentFailures.get());
            assertEquals("second episode must get the full budget after the reset",
                    cap, listener.lastPersistentTotalAttempts.get());
            // 15 gap + 1 role reject + 16 gap = 32 sweeps total.
            assertEquals(2 * cap, factory.attempts());
            // M10 split, per-stream: the DA stream carries both episodes'
            // per-episode numbering (1..15, then 1..15 again -- the second
            // episode's 16th attempt fires persistent-failure instead), and
            // the reset between them is attributable: exactly one role reject
            // on the primary stream. Before the split the reset was an
            // ambiguous non-monotonic drop in a single stream.
            List<Integer> expectedDaStream = new ArrayList<>();
            for (int episode = 0; episode < 2; episode++) {
                for (int i = 1; i <= cap - 1; i++) {
                    expectedDaStream.add(i);
                }
            }
            assertEquals(expectedDaStream, listener.unavailableAttempts);
            assertEquals(Collections.singletonList(1), listener.primaryUnavailableAttempts);
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testRoleRejectAndCapabilityGapLandOnSeparateStreams() throws Exception {
        assertMemoryLeak(() -> {
            // M10 discriminator: gap -> role reject -> gap -> success. The
            // released 1.3.4 contract fed BOTH conditions to
            // onDurableAckUnavailable, so this script produced the ambiguous
            // stream [1, 1, 1] -- a listener could not tell a budget-bound
            // capability-gap episode from a never-escalating role-reject
            // window, and could not see WHY the episode counter reset. With
            // the split, the DA stream carries only the two one-attempt gap
            // episodes ([1, 1] -- the reset stays visible) and the role
            // reject that caused the reset lands on the primary stream ([1]).
            CountingListener listener = new CountingListener();
            AtomicInteger sweeps = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.failingTimes(3, () -> {
                if (sweeps.incrementAndGet() == 2) {
                    return new QwpIngressRoleRejectedException(
                            QwpIngressRoleRejectedException.ROLE_REPLICA, "127.0.0.1", 9000);
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertSame(factory.successSentinel(), out);
            assertEquals(4, factory.attempts());
            assertEquals("DA stream must carry only the gap episodes, each"
                            + " restarting at 1 after the role-reject reset",
                    Arrays.asList(1, 1), listener.unavailableAttempts);
            assertEquals("role reject must land on the primary stream",
                    Collections.singletonList(1), listener.primaryUnavailableAttempts);
            assertEquals(Collections.singletonList(slotPath), listener.primaryUnavailableSlotPaths);
            assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
            assertEquals(0, listener.persistentFailures.get());
            assertFalse(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testTransportErrorDoesNotResetCapabilityGapEpisode() throws Exception {
        assertMemoryLeak(() -> {
            // A transport blip between gap attempts does not prove promotion
            // churn: it must neither consume the budget (no increment) nor
            // restart it (no reset) -- otherwise a flaky-but-misconfigured
            // cluster would evade the cap forever. 15 gaps, one transport error,
            // one gap: escalates on that 16th gap attempt.
            int cap = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
            CountingListener listener = new CountingListener();
            AtomicInteger sweeps = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (sweeps.incrementAndGet() == cap) { // 16th sweep: transport error between the gap runs
                    return new LineSenderException("Failed to connect: all 2 endpoint(s) "
                            + "unreachable; last=127.0.0.1:9000");
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals(1, listener.persistentFailures.get());
            assertEquals("transport blip must not restart the episode",
                    cap, listener.lastPersistentTotalAttempts.get());
            // 15 gap + 1 transport + 1 gap = 17 sweeps total.
            assertEquals(cap + 1, factory.attempts());
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testTransportWindowDoesNotBurnCapabilityGapWallClock() throws Exception {
        assertMemoryLeak(() -> {
            // Red-first: the wall-clock half of the settle budget is anchored at
            // gap #1, and a transport window BETWEEN gap sweeps must PAUSE it --
            // only gap-to-gap time is the cluster "failing to settle". Under the
            // bug the deadline keeps ticking while the cluster is unreachable:
            // gap #1 anchors the deadline, the cluster then drops off the network
            // for longer than the entire budget (transport errors are retried
            // "forever" and charge nothing else), and when it comes back still
            // briefly gapped, gap #2 observes an expired deadline and quarantines
            // the slot after just 2 gap sweeps -- contradicting both the
            // 16-attempt settle intent and Invariant B's "transients never
            // consume the budget". Evasion is not a concern: the attempt counter
            // survives the window untouched, which
            // testTransportErrorDoesNotResetCapabilityGapEpisode pins.
            // Here the cluster actually settles after the outage (two more gap
            // sweeps, then durable-ack-capable), so the drain must proceed --
            // no escalation, no sentinel.
            long budgetMillis = 250L;
            CountingListener listener = new CountingListener();
            AtomicInteger sweeps = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.failingTimes(4, () -> {
                if (sweeps.incrementAndGet() == 2) {
                    // Cluster fully unreachable for ~2.5x the wall-clock budget.
                    // A real outage is time spent inside reconnect() walking
                    // unreachable endpoints, so model it inside the factory.
                    try {
                        Thread.sleep(budgetMillis * 2 + 100);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                    return new LineSenderException("Failed to connect: all 2 endpoint(s) "
                            + "unreachable; last=127.0.0.1:9000");
                }
                return new QwpDurableAckMismatchException("h", 1234, "primary");
            });
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, budgetMillis, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
            drainer.setListener(listener);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertSame("cluster recovered after the outage -- the drain must proceed, not "
                            + "quarantine on a wall clock burned by the transport window",
                    factory.successSentinel(), out);
            // gap #1 + outage + gap #2 + gap #3 + success = 5 sweeps.
            assertEquals(5, factory.attempts());
            assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
            assertEquals("transport window must not trigger persistent-failure escalation",
                    0, listener.persistentFailures.get());
            assertFalse("no .failed sentinel: the slot was never in a terminal state",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test
    public void testRoleRejectGrantsFreshWallClockToNextGapEpisode() {
        // Companion to testRoleRejectResetsCapabilityGapEpisode, which pins the
        // ATTEMPT-counter half of the episode reset but runs under a 60s budget
        // where the wall-clock half is unobservable: a mutant that resets only
        // capabilityGapAttempts (leaving capabilityGapElapsedNanos /
        // lastCapabilityGapNanos ticking) passes it. This test pins the
        // WALL-CLOCK half: gap sweeps burn most of the budget, a role reject
        // proves the topology churned, and the next gap episode must start
        // from a zero wall clock -- under the counter-only mutant the stale
        // elapsed (plus the still-anchored lastCapabilityGapNanos charging
        // straight across the role-reject window) exhausts the budget and
        // quarantines a cluster that was about to settle.
        long budgetMillis = 800L;
        CountingListener listener = new CountingListener();
        AtomicInteger sweeps = new AtomicInteger();
        ScriptedFactory factory = ScriptedFactory.failingTimes(5, () -> {
            switch (sweeps.incrementAndGet()) {
                case 2:
                    // Burn ~600ms of the 800ms budget inside the first gap
                    // episode (charged by this sweep's gap-to-gap interval).
                    sleepQuietly(600);
                    return new QwpDurableAckMismatchException("h", 1234, "primary");
                case 3:
                    // Topology churn: the settle budget must restart in full.
                    return new QwpIngressRoleRejectedException(
                            QwpIngressRoleRejectedException.ROLE_REPLICA, "127.0.0.1", 9000);
                case 5:
                    // Second episode burns ~350ms -- well inside a fresh 800ms
                    // budget, but 600 + 350 > 800 under the mutant's carried-over
                    // wall clock.
                    sleepQuietly(350);
                    return new QwpDurableAckMismatchException("h", 1234, "primary");
                default:
                    return new QwpDurableAckMismatchException("h", 1234, "primary");
            }
        });
        BackgroundDrainer drainer = newDrainerWithBudgets(
                factory, budgetMillis, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
        drainer.setListener(listener);
        WebSocketClient out = drainer.connectWithDurableAckRetry();
        assertSame("role reject restarts the episode wall clock -- the second gap "
                        + "episode must get the full settle budget, not the first "
                        + "episode's leftovers",
                factory.successSentinel(), out);
        // gap, gap(+600ms), roleReject, gap, gap(+350ms), success = 6 sweeps.
        assertEquals(6, factory.attempts());
        assertEquals(BackgroundDrainer.DrainOutcome.PENDING, drainer.outcome());
        assertEquals("a settling cluster must never see a persistent-failure escalation",
                0, listener.persistentFailures.get());
        assertFalse("no .failed sentinel: both gap episodes stayed inside their budgets",
                Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        // Per-stream attempt numbering across the reset (M10 split): the DA
        // stream carries gaps 1,2 then the fresh episode's 1,2; the role
        // reject that restarted the episode lands on the primary stream.
        assertEquals(Arrays.asList(1, 2, 1, 2), listener.unavailableAttempts);
        assertEquals(Collections.singletonList(1), listener.primaryUnavailableAttempts);
    }

    @Test
    public void testRequestStopInterruptsLongBackoffParkPromptly() throws Exception {
        // Pins the stop-promptness contract of the backoff park: requestStop()
        // must break the drainer out of a LONG park (unpark, backstopped by
        // the 50ms STOP_CHECK_PARK_CHUNK_NANOS chunking) instead of sleeping
        // out the remainder. testStopRequestedDuringRetryAbortsWithStoppedOutcome
        // cannot see this: its 5-10ms backoffs complete faster than any
        // reasonable join timeout, so a monolithic park with no unpark passes
        // it. Here the backoff is 5s and the exit bound is 2s -- an
        // implementation that parks the full backoff in one shot fails.
        CountDownLatch firstFailureSeen = new CountDownLatch(1);
        ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
            firstFailureSeen.countDown();
            // Transport error: the un-clamped (boundedByBudget=false) sleep
            // path, so the park is backoff+jitter (5-10s), never trimmed to
            // the wall-clock budget.
            return new LineSenderException(
                    "Failed to connect: all 2 endpoint(s) unreachable; last=127.0.0.1:9000");
        });
        BackgroundDrainer drainer = newDrainerWithBudgets(
                factory, /*reconnectMaxDurationMillis*/ 60_000L,
                /*backoffInit*/ 5_000L, /*backoffMax*/ 5_000L);
        Thread t = new Thread(drainer::connectWithDurableAckRetry, "long-park-stop-drainer");
        t.setDaemon(true);
        t.start();
        Assert.assertTrue("first failure must occur promptly",
                firstFailureSeen.await(2, TimeUnit.SECONDS));
        // Give the drainer a moment to enter the 5-10s park. If requestStop()
        // instead lands before the park, the pre-park stopRequested check
        // skips it entirely -- either way the exit must be prompt.
        Thread.sleep(100);
        long stopNanos = System.nanoTime();
        drainer.requestStop();
        t.join(2_000);
        long exitMillis = (System.nanoTime() - stopNanos) / 1_000_000L;
        Assert.assertFalse("requestStop() must break the drainer out of a 5-10s "
                        + "backoff park promptly (exit took >" + exitMillis + "ms); "
                        + "a monolithic park with no unpark sleeps out the full backoff",
                t.isAlive());
        assertEquals(BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
        assertFalse("stop is not a failure: no .failed sentinel",
                Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private BackgroundDrainer newDrainer(ScriptedFactory factory) {
        return newDrainerWithBudgets(
                factory, FAST_RECONNECT_MAX_DURATION_MILLIS,
                FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
    }

    private BackgroundDrainer newDrainerWithBudgets(
            ScriptedFactory factory,
            long reconnectMaxDurationMillis,
            long backoffInitMillis,
            long backoffMaxMillis) {
        return new BackgroundDrainer(
                slotPath,
                SEGMENT_SIZE_BYTES,
                SF_MAX_TOTAL_BYTES,
                factory,
                reconnectMaxDurationMillis,
                backoffInitMillis,
                backoffMaxMillis,
                /* requestDurableAck */ true,
                /* durableAckKeepaliveIntervalMillis */ 200L);
    }

    /**
     * Wraps a test body in {@link TestUtils#assertMemoryLeak} and closes every
     * stub the body allocated BEFORE the leak check fires -- LeakCheck closes
     * at the end of the wrapped lambda, so an @After-only close would run too
     * late and fail every wrapped test.
     */
    private static void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                code.run();
            } finally {
                closeAllStubs();
            }
        });
    }

    private static void closeAllStubs() {
        synchronized (LIVE_STUBS) {
            for (int i = 0, n = LIVE_STUBS.size(); i < n; i++) {
                LIVE_STUBS.get(i).close();
            }
            LIVE_STUBS.clear();
        }
    }

    private static StubWebSocketClient stubClient() {
        StubWebSocketClient client = new StubWebSocketClient();
        LIVE_STUBS.add(client);
        return client;
    }

    /**
     * Listener that records every invocation in order so tests can assert
     * exact counts and per-call arguments.
     */
    private static final class CountingListener implements BackgroundDrainerListener {
        final AtomicInteger lastPersistentElapsedMs = new AtomicInteger(-1);
        final AtomicInteger lastPersistentTotalAttempts = new AtomicInteger(-1);
        final AtomicInteger persistentFailures = new AtomicInteger();
        final List<Integer> primaryUnavailableAttempts = new ArrayList<>();
        final List<String> primaryUnavailableSlotPaths = new ArrayList<>();
        final List<Integer> unavailableAttempts = new ArrayList<>();
        final List<String> unavailableSlotPaths = new ArrayList<>();

        @Override
        public synchronized void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
            persistentFailures.incrementAndGet();
            lastPersistentTotalAttempts.set(totalAttempts);
            lastPersistentElapsedMs.set((int) elapsedMillis);
        }

        @Override
        public synchronized void onDurableAckUnavailable(String slotPath, int attemptNumber) {
            unavailableSlotPaths.add(slotPath);
            unavailableAttempts.add(attemptNumber);
        }

        @Override
        public synchronized void onPrimaryUnavailable(String slotPath, int attemptNumber) {
            primaryUnavailableSlotPaths.add(slotPath);
            primaryUnavailableAttempts.add(attemptNumber);
        }
    }

    /**
     * Programmable {@link CursorWebSocketSendLoop.ReconnectFactory} for tests.
     * Throws the supplied exception N times, then returns the success sentinel
     * on every subsequent invocation. {@link #attempts()} reports total calls.
     */
    private static final class ScriptedFactory implements CursorWebSocketSendLoop.ReconnectFactory {
        private final AtomicInteger calls = new AtomicInteger();
        private final WebSocketClient successSentinel;
        private final ThrowableSupplier throwSupplier;
        private final int throwingTimes;

        ScriptedFactory(WebSocketClient successSentinel,
                        int throwingTimes,
                        ThrowableSupplier throwSupplier) {
            this.successSentinel = successSentinel;
            this.throwingTimes = throwingTimes;
            this.throwSupplier = throwSupplier;
        }

        static ScriptedFactory alwaysFailing(ThrowableSupplier supplier) {
            return new ScriptedFactory(stubClient(), Integer.MAX_VALUE, supplier);
        }

        static ScriptedFactory alwaysSucceeding() {
            return new ScriptedFactory(stubClient(), 0, () -> new RuntimeException("unreachable"));
        }

        static ScriptedFactory failingTimes(int n, ThrowableSupplier supplier) {
            return new ScriptedFactory(stubClient(), n, supplier);
        }

        int attempts() {
            return calls.get();
        }

        @Override
        public WebSocketClient reconnect() throws Exception {
            int n = calls.incrementAndGet();
            if (n <= throwingTimes) {
                Throwable t = throwSupplier.get();
                if (t instanceof RuntimeException) throw (RuntimeException) t;
                if (t instanceof Exception) throw (Exception) t;
                if (t instanceof Error) throw (Error) t;
                throw new RuntimeException(t);
            }
            return successSentinel;
        }

        WebSocketClient successSentinel() {
            return successSentinel;
        }
    }

    /**
     * Minimal concrete {@link WebSocketClient} returned as the success
     * sentinel — tests only need referential identity, never invoke I/O.
     */
    private static final class StubWebSocketClient extends WebSocketClient {
        StubWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new UnsupportedOperationException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }

    @FunctionalInterface
    private interface ThrowableSupplier {
        Throwable get();
    }
}
