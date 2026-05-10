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
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
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
    public void testCallbackArgumentsCarrySlotPathAndAttemptNumber() {
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
    }

    @Test
    public void testEscalatesAfterMaxAttemptsAndDropsSentinel() {
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
    }

    @Test
    public void testListenerThrowingOnPersistentFailureStillMarksFailed() {
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
    }

    @Test
    public void testListenerThrowingOnUnavailableContinuesRetrying() {
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
    }

    @Test
    public void testNoListenerNoNullPointerOnEscalation() {
        ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                () -> new QwpDurableAckMismatchException("h", 1234, "primary"));
        BackgroundDrainer drainer = newDrainer(factory);
        // Intentionally leave listener null.
        WebSocketClient out = drainer.connectWithDurableAckRetry();
        assertNull(out);
        assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
        assertTrue(Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
    }

    @Test
    public void testNonDurableAckExceptionMarksFailedImmediately() {
        CountingListener listener = new CountingListener();
        ScriptedFactory factory = ScriptedFactory.alwaysFailing(
                () -> new IOException("transport down"));
        BackgroundDrainer drainer = newDrainer(factory);
        drainer.setListener(listener);
        WebSocketClient out = drainer.connectWithDurableAckRetry();
        assertNull(out);
        assertEquals(BackgroundDrainer.DrainOutcome.FAILED, drainer.outcome());
        // Listener must not have been touched — this path doesn't fire either callback.
        assertEquals(0, listener.unavailableAttempts.size());
        assertEquals(0, listener.persistentFailures.get());
        // Sentinel reason should reflect the non-DA path (initial connect: ...).
        String sentinel = slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME;
        assertTrue(Files.exists(sentinel));
        // The factory must have been invoked exactly once — no retry on this path.
        assertEquals(1, factory.attempts());
    }

    @Test
    public void testReturnsClientOnSuccessFirstAttempt() {
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
    }

    @Test
    public void testRetriesOnDurableAckMismatchThenSucceeds() {
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
    }

    @Test
    public void testStopRequestedDuringRetryAbortsWithStoppedOutcome() throws Exception {
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
    }

    @Test
    public void testWallTimeBudgetEscalatesBeforeAttemptCap() {
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

    private static StubWebSocketClient stubClient() {
        return new StubWebSocketClient();
    }

    /**
     * Listener that records every invocation in order so tests can assert
     * exact counts and per-call arguments.
     */
    private static final class CountingListener implements BackgroundDrainerListener {
        final AtomicInteger lastPersistentElapsedMs = new AtomicInteger(-1);
        final AtomicInteger lastPersistentTotalAttempts = new AtomicInteger(-1);
        final AtomicInteger persistentFailures = new AtomicInteger();
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
