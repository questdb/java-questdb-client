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
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.std.Os;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
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

    // one shared temp-directory mechanism instead of a per-class java.io.tmpdir path plus a hand-rolled
    // recursive delete: the rule cleans up on failure and on an exception thrown out of a test too
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Before
    public void setUp() {
        slotPath = temp.getRoot().toPath().resolve("slot").toString();
        assertEquals("mkdir slot dir", 0, Files.mkdir(slotPath, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        // Safety net for exits that bypass the assertMemoryLeak wrapper;
        // normally a no-op because the wrapper's finally already closed
        // and cleared the stubs (close() is idempotent). The slot directory
        // itself is the TemporaryFolder rule's job.
        closeAllStubs();
    }

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
    public void testTerminalUpgradeMarksFailedImmediately() throws Exception {
        assertMemoryLeak(() -> {
            CountingListener listener = new CountingListener();
            // A genuinely non-retriable upgrade error (non-421 5xx upgrade reject) is
            // terminal -- waiting will not fix it -- so the drainer quarantines on the
            // first attempt under the orphan reconnect policy. A TRANSPORT error,
            // by contrast, is transient and is
            // retried (see testTransportErrorNeverQuarantinesInvariantB).
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new WebSocketUpgradeException(500, null, "server error during upgrade"));
            BackgroundDrainer drainer = newDrainer(factory);
            drainer.setListener(listener);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);
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
            // The error sink must learn about the abandonment too, tagged with
            // the same "auth/upgrade: " reason prefix the sentinel carries.
            assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
            SenderError err = captured.get(0);
            assertEquals(SenderError.Category.DATA_LOSS, err.getCategory());
            assertEquals(SenderError.Policy.ABANDONED, err.getAppliedPolicy());
            assertEquals(slotPath, err.getQuarantinedPath());
            assertTrue("reason must carry the site prefix [msg=" + err.getServerMessage() + ']',
                    err.getServerMessage() != null && err.getServerMessage().startsWith("auth/upgrade: "));
        });
    }

    @Test(timeout = 60_000)
    public void testFixedCredentialAuthRejectionStillQuarantinesImmediately() throws Exception {
        assertMemoryLeak(() -> {
            // A 401 against a CONSTANT credential is a permanent misconfiguration: re-presenting the same
            // header cannot change the answer, so the pre-existing fail-fast quarantine must stay exactly as
            // it was. This is the control for the rotating-credential case below - the settle budget must
            // key off the credential's nature, not relax auth handling across the board.
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new QwpAuthFailedException(401, "127.0.0.1", 9000));
            BackgroundDrainer drainer = newDrainer(factory);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            WebSocketClient out = drainer.connectWithDurableAckRetry();

            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals("a constant credential must not be retried", 1, factory.attempts());
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
            assertEquals(SenderError.Category.DATA_LOSS, captured.get(0).getCategory());
        });
    }

    @Test(timeout = 60_000)
    public void testRotatingCredentialAuthRejectionQuarantinesOnceBudgetExhausted() throws Exception {
        assertMemoryLeak(() -> {
            // The settle budget must be BOUNDED, not "retry forever". Quarantine is permitted only once both
            // the attempt threshold and the wall-clock dwell floor are exhausted; this short test budget pins
            // the terminal behavior without waiting for the production five-minute default.
            ScriptedFactory factory = ScriptedFactory
                    .alwaysFailing(() -> new QwpAuthFailedException(401, "127.0.0.1", 9000))
                    .withDynamicCredential();
            // Far above what the six attempts cost on their own (six capped backoffs of 1-4ms, ~15ms
            // total): at the 25ms this used to use, the assertion below cleared the floor with ~10ms of
            // margin, so a machine that merely ran the attempts slowly satisfied it without the dwell being
            // honoured at all. A quarter second is still a quarter second of test time, and an elapsed of
            // ~15ms against it is unmistakable.
            long authDwellFloorMillis = 250L;
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, authDwellFloorMillis, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            long startNanos = System.nanoTime();
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertTrue("the attempt threshold must be reached",
                    factory.attempts() >= BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS);
            assertTrue("quarantine must not precede the auth dwell floor [elapsedMillis="
                            + elapsedMillis + "]",
                    elapsedMillis >= authDwellFloorMillis);
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
            assertEquals(SenderError.Category.DATA_LOSS, captured.get(0).getCategory());
        });
    }

    @Test(timeout = 60_000)
    public void testRotatingCredentialAuthRejectionRidesPastAttemptThresholdBeforeDwellFloor() throws Exception {
        assertMemoryLeak(() -> {
            // Six fast 401s must not strand the slot while the configured self-healing window is still open.
            // The seventh attempt succeeds, proving that attempt count alone cannot quarantine replayable data.
            ScriptedFactory factory = ScriptedFactory
                    .failingTimes(BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS,
                            () -> new QwpAuthFailedException(401, "127.0.0.1", 9000))
                    .withDynamicCredential();
            BackgroundDrainer drainer = newDrainer(factory);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            WebSocketClient out = drainer.connectWithDurableAckRetry();

            assertSame("the drainer must keep trying after the fast attempt threshold",
                    factory.successSentinel(), out);
            assertEquals(BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS + 1,
                    factory.attempts());
            assertNotEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertFalse("a recovered credential must leave no .failed sentinel",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            assertTrue("no abandonment may be reported: " + captured, captured.isEmpty());
        });
    }

    @Test(timeout = 60_000)
    public void testRotatingCredentialAuthRejectionRidesOutBoundedBudget() throws Exception {
        assertMemoryLeak(() -> {
            // With a ROTATING credential the Authorization header is re-derived from the token provider on
            // every sweep, so a 401 can be a window that heals itself: a revocation landing mid-flight, an
            // identity provider rotating signing keys, a token expiring during the settle so the next pull
            // refreshes it. Quarantining on the first one permanently abandons replayable data - nothing in
            // production clears the .failed sentinel - on a fault that repairs itself in seconds.
            ScriptedFactory factory = ScriptedFactory
                    .failingTimes(2, () -> new QwpAuthFailedException(401, "127.0.0.1", 9000))
                    .withDynamicCredential();
            BackgroundDrainer drainer = newDrainer(factory);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            WebSocketClient out = drainer.connectWithDurableAckRetry();

            assertSame("the drainer must recover once the credential is accepted",
                    factory.successSentinel(), out);
            assertEquals(3, factory.attempts());
            assertNotEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertFalse("a recovered credential must leave no .failed sentinel",
                    Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            assertTrue("no abandonment may be reported: " + captured, captured.isEmpty());
        });
    }

    @Test(timeout = 60_000)
    public void testRotatingCredentialAuthDwellIsClampedSoEscalationStaysReachable() {
        // reconnect_max_duration_millis is validated only as > 0, and Long.MAX_VALUE is the documented way
        // to ask a reconnect never to give up. TimeUnit saturates it, so the dwell half of the rotating-401
        // gate - an AND, unlike the capability-gap gate's OR - became unsatisfiable and the ride-out never
        // ended: the drainer swept forever, never wrote the .failed sentinel, never reported DATA_LOSS, and
        // pinned the slot lock plus one worker of a FIXED-size drainer pool for the life of the process.
        //
        // This pins the clamp as a FUNCTION. On its own that proves nothing about the connect loop, which
        // could go on using the raw budget - so testConnectLoopAppliesTheClampedRotating401Dwell drives the
        // loop itself against a saturated reconnect_max_duration_millis, pre-ageing the rejection anchor past
        // the ceiling rather than waiting out five minutes of wall clock. Keep the two together: neither
        // half is worth much alone. The third leg - that a FINITE dwell does quarantine - is
        // testRotatingCredentialAuthRejectionQuarantinesOnceBudgetExhausted, with a 250ms budget.
        long ceilingNanos = TimeUnit.MILLISECONDS.toNanos(
                BackgroundDrainer.MAX_DYNAMIC_CREDENTIAL_AUTH_DWELL_MILLIS);

        assertEquals("a saturated budget must not saturate the dwell",
                ceilingNanos, BackgroundDrainer.dynamicCredentialAuthDwellNanos(Long.MAX_VALUE));
        assertTrue("and the clamped dwell must be reachable at all",
                BackgroundDrainer.dynamicCredentialAuthDwellNanos(Long.MAX_VALUE) < Long.MAX_VALUE);
        assertEquals("a budget above the ceiling is clamped to it", ceilingNanos,
                BackgroundDrainer.dynamicCredentialAuthDwellNanos(
                        BackgroundDrainer.MAX_DYNAMIC_CREDENTIAL_AUTH_DWELL_MILLIS * 10));
        // a smaller configured budget is honoured as-is, so tuning down still works - and this is the value
        // the existing end-to-end quarantine tests rely on
        assertEquals("a budget below the ceiling is used as configured",
                TimeUnit.MILLISECONDS.toNanos(25L), BackgroundDrainer.dynamicCredentialAuthDwellNanos(25L));
        assertEquals("including the default, which IS the ceiling", ceilingNanos,
                BackgroundDrainer.dynamicCredentialAuthDwellNanos(
                        CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_DURATION_MILLIS));
    }

    @Test(timeout = 60_000)
    public void testConnectLoopAppliesTheClampedRotating401Dwell() throws Exception {
        assertMemoryLeak(() -> {
            // The clamp asserted above is a pure function; this asserts the CALL SITE applies it. Nothing
            // else does: every other end-to-end test here configures a dwell far below the ceiling, where
            // Math.min returns its first argument either way, so the connect loop reverting to the raw
            // TimeUnit.MILLISECONDS.toNanos(reconnectMaxDurationMillis) leaves all of them green - and
            // reconnect_max_duration_millis is validated only as > 0, with Long.MAX_VALUE the documented way
            // to ask a reconnect never to give up. Saturated, the dwell conjunct can never be satisfied, so
            // the ride-out never ends: no .failed sentinel, no DATA_LOSS report, and the slot lock plus one
            // worker of a FIXED-size drainer pool pinned for the life of the process.
            //
            // Reaching the ceiling honestly costs five minutes of wall clock, so the rejection anchor is
            // pre-aged past it instead and the loop runs for real against it.
            final long anchorAgeMillis = BackgroundDrainer.MAX_DYNAMIC_CREDENTIAL_AUTH_DWELL_MILLIS * 2;
            // Bounds the counterfactual: unclamped, the loop never escalates, and without this it would run
            // to the test timeout with nothing to say. Stopping it turns that into a named assertion
            // failure on outcome() instead. Well clear of the six attempts a clamped run needs.
            final int stopAfterAttempts = BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS * 5;
            final BackgroundDrainer[] ref = new BackgroundDrainer[1];
            AtomicInteger scripted = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (scripted.incrementAndGet() >= stopAfterAttempts) {
                    ref[0].requestStop();
                }
                return new QwpAuthFailedException(401, "127.0.0.1", 9000);
            }).withDynamicCredential();

            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, Long.MAX_VALUE, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
            ref[0] = drainer;
            drainer.ageDynamicCredentialAuthAnchorForTesting(
                    TimeUnit.MILLISECONDS.toNanos(anchorAgeMillis));
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            assertNull(drainer.connectWithDurableAckRetry());
            // FAILED, not STOPPED: STOPPED means the loop was still riding out rejections when the factory
            // pulled the plug, which is precisely what an unclamped dwell does.
            assertEquals("a saturated reconnect_max_duration_millis must not disable the escalation - the "
                            + "connect loop has to use the CLAMPED dwell [attempts=" + factory.attempts() + "]",
                    BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            // The dwell was already satisfied before the first sweep, so the attempt threshold is what the
            // quarantine waited for - it must fire on exactly that attempt, not later.
            assertEquals("quarantine must fall on the attempt threshold once the dwell is behind it",
                    BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS, factory.attempts());
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
            assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
            assertEquals(SenderError.Category.DATA_LOSS, captured.get(0).getCategory());
        });
    }

    @Test(timeout = 60_000)
    public void testTransientOutageDoesNotCountTowardTheRotating401Dwell() throws Exception {
        assertMemoryLeak(() -> {
            // The dwell measures how long the REJECTION persisted, so an unrelated outage in the middle of a
            // 401 run is not part of it. Anchored at the first 401 and never restarted, a 401, then an outage
            // outlasting the dwell, then a sixth rejection satisfied both thresholds at once and quarantined
            // the slot on a credential that had been rejected for seconds - abandoning replayable rows behind
            // a .failed sentinel nothing in production clears.
            final long dwellMillis = 100L;
            AtomicInteger scripted = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (scripted.incrementAndGet() == 6) {
                    // an unrelated cluster outage, longer than the whole dwell
                    Os.sleep(dwellMillis * 3);
                    return new RuntimeException("cluster unreachable");
                }
                return new QwpAuthFailedException(401, "127.0.0.1", 9000);
            }).withDynamicCredential();
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, dwellMillis, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            assertNull(drainer.connectWithDurableAckRetry());
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());

            // five 401s, the outage, then the sixth 401 - attempt seven overall. Charging the outage to the
            // dwell quarantines exactly there; restarting it means the sixth rejection must be followed by a
            // fresh dwell of uninterrupted 401s first.
            // The configured dwell here is far below MAX_DYNAMIC_CREDENTIAL_AUTH_DWELL_MILLIS, so the clamp
            // is inert and the dwell is unambiguously what ends the ride-out.
            assertTrue("the outage must not have satisfied the dwell [attempts=" + factory.attempts() + "]",
                    factory.attempts() > 7);
            assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
        });
    }

    @Test(timeout = 60_000)
    public void testCapabilityGapDoesNotCountTowardTheRotating401Dwell() throws Exception {
        assertMemoryLeak(() -> {
            // The same defect as the transient case, in the one arm that arm did not cover. A durable-ack
            // capability gap means we REACHED a node and it answered - it simply cannot do durable ack - so
            // it is not time the credential spent rejected. Its own settle budget can legitimately run for
            // the whole reconnect budget, which is exactly the span the rotating-401 dwell is meant to
            // require of an UNINTERRUPTED rejection, so charging it lets a rolling upgrade satisfy that
            // floor for free and quarantine a slot over a credential rejected for seconds.
            final long dwellMillis = 100L;
            AtomicInteger scripted = new AtomicInteger();
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(() -> {
                if (scripted.incrementAndGet() == 6) {
                    // one gap sweep, longer than the whole dwell. The first gap charges nothing to the
                    // capability-gap episode (lastCapabilityGapNanos is still 0), so it cannot escalate on
                    // its own and the rotating-401 accounting is what this observes.
                    Os.sleep(dwellMillis * 3);
                    return new QwpDurableAckMismatchException("h", 1234, "primary");
                }
                return new QwpAuthFailedException(401, "127.0.0.1", 9000);
            }).withDynamicCredential();
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    factory, dwellMillis, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);
            List<SenderError> captured = Collections.synchronizedList(new ArrayList<SenderError>());
            drainer.setErrorSink(captured::add);

            assertNull(drainer.connectWithDurableAckRetry());
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());

            // five 401s, the gap, then the sixth 401 - attempt seven overall. Charging the gap to the dwell
            // quarantines exactly there; restarting it means the sixth rejection must be followed by a fresh
            // dwell of uninterrupted 401s first.
            assertTrue("a capability gap must not have satisfied the dwell [attempts="
                            + factory.attempts() + "]",
                    factory.attempts() > 7);
            assertEquals("exactly one abandonment report: " + captured, 1, captured.size());
        });
    }

    @Test(timeout = 60_000)
    public void testFlappingCredentialEscalatesAcrossMidDrainRecycles() throws Exception {
        assertMemoryLeak(() -> {
            // run() re-enters connectWithDurableAckRetry() after every mid-drain terminal. While the
            // escalation counters were locals, each recycle refilled the budget it is meant to spend, so a
            // cluster that flaps - connect accepted, drop, 401, recycle, repeat - looped forever with no ack
            // progress: no quarantine, the slot lock never released, and one of max_background_drainers
            // workers (four by default) pinned, starving every other orphan slot of a drainer.
            //
            // Driven by calling connectWithDurableAckRetry() repeatedly, which is what the recycle does.
            final AtomicInteger calls = new AtomicInteger();
            CursorWebSocketSendLoop.ReconnectFactory flapping = new CursorWebSocketSendLoop.ReconnectFactory() {
                @Override
                public boolean hasDynamicCredential() {
                    return true;
                }

                @Override
                public WebSocketClient reconnect() {
                    // reject once, then let the connect through - the shape that recycles forever
                    if (calls.incrementAndGet() % 2 == 1) {
                        throw new QwpAuthFailedException(401, "127.0.0.1", 9000);
                    }
                    return stubClient();
                }
            };
            BackgroundDrainer drainer = newDrainerWithBudgets(
                    flapping, 25L, FAST_BACKOFF_MILLIS, FAST_BACKOFF_MAX_MILLIS);

            WebSocketClient out = null;
            int recycles = 0;
            for (; recycles < 40; recycles++) {
                out = drainer.connectWithDurableAckRetry();
                if (out == null) {
                    break;
                }
                Os.sleep(2); // stand in for the drain between two mid-drain terminals
            }

            assertNull("a flapping credential must reach the escalation instead of recycling forever", out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertTrue("it must escalate once both thresholds are met, not at the very first recycle "
                            + "[recycles=" + recycles + "]",
                    recycles >= BackgroundDrainer.DEFAULT_MAX_DYNAMIC_CREDENTIAL_AUTH_ATTEMPTS - 1);
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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
            // The regression this pins: lumping role rejects in with the
            // durable-ack-mismatch give-up. Under that shape the 16-attempt cap or
            // the wall-clock budget markFailed()s and returns, so the helper thread
            // started below dies inside the observation window. The drainer keeps the
            // two apart - a role reject backs off and retries, a capability gap
            // quarantines - which is what the still-alive assertions rest on.
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

    @Test(timeout = 60_000)
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
            // The regression this pins: routing any non-role, non-durable-ack
            // Throwable - "all endpoints unreachable" included - to an IMMEDIATE
            // markFailed / .failed sentinel on the first attempt. The catch-all
            // retries a transport failure indefinitely, exactly as connectLoop does;
            // the genuine terminals (auth, non-421 upgrade, durable-ack capability
            // gap) are caught ahead of it and still fail fast.
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

    @Test(timeout = 60_000)
    public void testJvmErrorEscapesConnectRetryLoop() throws Exception {
        assertMemoryLeak(() -> {
            // Regression (M3): catch (Throwable) in connectWithDurableAckRetry used
            // to swallow java.lang.Error (OOM, LinkageError, StackOverflowError)
            // into the indefinite "cluster unreachable" retry -- pinning the slot
            // .lock forever with no .failed sentinel and only a throttled WARN as
            // a trace. A JVM/programming failure is not a transport outage:
            // retrying cannot clear it, so it must escape the loop on the FIRST
            // sweep. run() records FAILED without quarantining recoverable data,
            // releases the lock in finally, and propagates the Error.
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
    public void testSaturatingCapabilityGapBudgetDoesNotQuarantineOnTheFirstSweep() throws Exception {
        assertMemoryLeak(() -> {
            // reconnect_max_duration_millis is validated only as > 0, and Long.MAX_VALUE
            // is the natural way to ask for "never give up". A raw millis * 1_000_000L
            // wraps that NEGATIVE, and because the escalation gate is an OR against the
            // attempt cap, capabilityGapElapsedNanos (0 on the first sweep) already
            // clears a negative budget -- so the slot quarantines on sweep ONE, skipping
            // the whole 16-sweep settle budget. Asking for more tolerance bought none.
            //
            // The CursorWebSocketSendLoop twin of this conversion is pinned by
            // CursorWebSocketSendLoopZeroBackoffTest; this site had no test, despite the
            // commit that fixed it citing that one as prior art.
            int cap = BackgroundDrainer.DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS;
            ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                    () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
            BackgroundDrainer drainer = newDrainerWithBudgets(factory, Long.MAX_VALUE, 1L, 4L);
            WebSocketClient out = drainer.connectWithDurableAckRetry();
            assertNull(out);
            assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
            assertEquals("a saturated budget must leave the attempt cap as the only gate, "
                            + "so the full settle budget is spent before quarantine",
                    cap, factory.attempts());
        });
    }

    @Test(timeout = 60_000)
    public void testTransportErrorResetsCapabilityGapEpisode() throws Exception {
        assertMemoryLeak(() -> {
            // A transport state breaks a consecutive capability-gap episode.
            // After 15 gaps and one transport error, the drainer must observe a
            // fresh run of 16 gaps before quarantining the slot.
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
            assertEquals("the fresh episode must exhaust its own full attempt budget",
                    cap, listener.lastPersistentTotalAttempts.get());
            // 15 gaps + 1 transport + a fresh 16-gap episode.
            assertEquals(2 * cap, factory.attempts());
            assertEquals(2 * (cap - 1), listener.unavailableAttempts.size());
            assertEquals("attempt numbering must restart after transport",
                    1, (int) listener.unavailableAttempts.get(cap - 1));
            assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
        });
    }

    @Test(timeout = 60_000)
    public void testTransportWindowResetsCapabilityGapWallClock() throws Exception {
        assertMemoryLeak(() -> {
            // The wall-clock half of the settle budget is anchored at gap #1.
            // A transport window breaks that episode completely, so the next gap
            // receives a fresh clock rather than inheriting time accumulated before
            // or during the unrelated outage.
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

    @Test(timeout = 60_000)
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

    @Test(timeout = 60_000)
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
            CursorWebSocketSendLoop.ReconnectFactory factory,
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
        // models a sender wired to an httpTokenProvider: the Authorization header is re-derived on every
        // attempt, so a 401 can be a window that heals rather than a permanent misconfiguration
        private boolean dynamicCredential;

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

        /**
         * The signal BackgroundDrainer branches its terminal policy on. Stubbed here, deliberately: these
         * tests pin the POLICY (fail fast on a constant credential, ride out the settle budget on a rotating
         * one), not the classification. What decides it in production is
         * {@code QwpWebSocketSender.hasDynamicCredential()} - a {@code FixedAuthHeader} identity check on the
         * configured supplier - and that is pinned on a real built sender, for httpToken,
         * httpUsernamePassword, httpTokenProvider and no-credential alike, by
         * {@code WebSocketTokenProviderTest.testCredentialKindTaggedForTheOrphanDrainerTerminalPolicy},
         * which reads it both directly and through the background reconnect factory a drainer is handed.
         * Neither half means much without the other: keep them named in each other's comments.
         */
        @Override
        public boolean hasDynamicCredential() {
            return dynamicCredential;
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

        ScriptedFactory withDynamicCredential() {
            this.dynamicCredential = true;
            return this;
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
