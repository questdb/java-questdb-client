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

package io.questdb.client.test.cutlass.auth;

import io.questdb.client.cutlass.auth.CredentialRedaction;
import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.test.cutlass.auth.TokenTestKit.FakeClock;
import io.questdb.client.test.cutlass.auth.TokenTestKit.ManualScheduler;
import io.questdb.client.test.cutlass.auth.TokenTestKit.ScriptedSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.cutlass.auth.TokenTestKit.await;

/**
 * Conformance tests C1-C8 and the cache half of C20 of the dynamic-credential specification
 * (design/qwp-token-provider-spec.md, section 10) for {@link RefreshingTokenProvider}.
 * <p>
 * Schedule, backoff, hand-out and forced-refresh tests run on a fake clock and a scheduler the test drives, so
 * they assert exact delays without waiting. The concurrency, interrupt and lifecycle tests run on the real
 * refresher thread.
 */
public class RefreshingTokenProviderTest {
    private static final long MIN = 60_000L;
    private static final long HOUR = 60 * MIN;
    private static final long DAY = 24 * HOUR;
    // A fixed wall-clock origin keeps the expected instants literal.
    private static final long T0 = 1_800_000_000_000L;

    @Test
    public void testAlreadyExpiredResultIsARetryableFailure() {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .thenToken("EXPIRED", T0 - 1)
                .thenToken("FRESH", T0 + HOUR);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.0).build()) {
            scheduler.runNext();
            Assert.assertEquals(1, provider.getConsecutiveFailures());
            RefreshingTokenProvider.FetchFailure failure = provider.getLastFailure();
            Assert.assertNotNull(failure);
            Assert.assertTrue("a result that has already expired is a retryable failure", failure.isRetryable());
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("expired"));
            Assert.assertEquals(RefreshingTokenProvider.NONE, provider.getTokenExpiresAtEpochMillis());
            // retried after backoff, not dropped
            scheduler.advanceAndRunNext();
            Assert.assertEquals("FRESH", provider.getToken().toString());
            Assert.assertEquals(0, provider.getConsecutiveFailures());
        }
    }

    @Test(timeout = 30_000)
    public void testAwaitReadyGatesOnTheFirstFetch() {
        ScriptedSource source = new ScriptedSource().thenToken("READY", System.currentTimeMillis() + HOUR);
        source.setGate();
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build()) {
            Assert.assertFalse("the first fetch has not finished", provider.awaitReady(50));
            source.openGate();
            Assert.assertTrue(provider.awaitReady(10_000));
            Assert.assertEquals("READY", provider.getToken().toString());
        }
    }

    @Test
    public void testBackoffFollowsTheSpec() {
        // section 5.4: base = min(backoff_max, backoff_initial * 2^(n-1)); delay = base/2 + U(0, base/2).
        // u = 0 pins the delay to the lower bound base/2, u = 0.5 to 3/4 of base.
        assertBackoffDelays(0.0, new long[]{250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000});
        assertBackoffDelays(0.5, new long[]{375, 750, 1_500, 3_000, 6_000, 12_000, 24_000, 45_000, 45_000});
    }

    @Test
    public void testBackoffHasPositiveDelayAtOneMillisecond() {
        for (double u : new double[]{0.0, 0.5, Math.nextDown(1.0)}) {
            for (long retryAfterMillis : new long[]{TokenUnavailableException.NO_RETRY_AFTER, 0, 2}) {
                FakeClock clock = new FakeClock(T0);
                ManualScheduler scheduler = new ManualScheduler(clock);
                ScriptedSource source = new ScriptedSource().thenThrow(
                        TokenUnavailableException.retryable("down", retryAfterMillis));
                try (RefreshingTokenProvider provider = provider(source, clock, scheduler, u)
                        .backoffInitialMillis(1)
                        .backoffMaxMillis(1)
                        .build()) {
                    Assert.assertEquals("the initial prefetch is still immediate", 0, scheduler.peek().delayNanos);
                    final long expectedDelayNanos = TimeUnit.MILLISECONDS.toNanos(retryAfterMillis == 2 ? 2 : 1);
                    for (int failure = 1; failure <= 64; failure++) {
                        scheduler.advanceAndRunNext();
                        Assert.assertEquals("backoff after failure " + failure + " [u=" + u
                                        + ", retryAfterMillis=" + retryAfterMillis + ']',
                                expectedDelayNanos, scheduler.peek().delayNanos);
                        Assert.assertEquals(failure, provider.getConsecutiveFailures());
                    }
                    Assert.assertEquals(64, source.calls());
                }
            }
        }
    }

    @Test
    public void testCloseFailsGetTokenPermanentlyAndStopsTheRefresher() {
        ScriptedSource source = new ScriptedSource().thenToken("TOKEN", System.currentTimeMillis() + HOUR);
        List<Thread> before = refresherThreads();
        RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build();
        Assert.assertTrue(provider.awaitReady(10_000));
        List<Thread> started = refresherThreads();
        started.removeAll(before);
        Assert.assertEquals("one refresher thread per provider", 1, started.size());

        provider.close();
        provider.close(); // idempotent
        Assert.assertTrue(provider.isClosed());
        await(() -> !started.get(0).isAlive(), 5_000, "the refresher thread to exit");
        try {
            provider.getToken();
            Assert.fail("getToken() after close() must fail");
        } catch (TokenUnavailableException e) {
            Assert.assertFalse("a closed provider is a permanent failure", e.isRetryable());
        }
        Assert.assertFalse(provider.awaitReady(10));
        provider.onTokenRejected("TOKEN", 401); // no-op, no throw
    }

    @Test(timeout = 30_000)
    public void testColdBurstAfterTheWallClockJumpsCausesOneFetch() throws Exception {
        // C3, second shape: a token is held, but the wall clock jumped past its expiry (a host that slept
        // through its scheduled refresh). 64 callers find no usable token at once; exactly one fetch runs.
        SkewedClock clock = new SkewedClock();
        ScriptedSource source = new ScriptedSource()
                .thenToken("OLD", System.currentTimeMillis() + HOUR)
                .then(() -> new ExpiringToken("NEW", clock.wallClockMillis() + HOUR));
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).clock(clock).build()) {
            Assert.assertTrue(provider.awaitReady(10_000));
            Assert.assertEquals("OLD", provider.getToken().toString());
            source.setGate();
            clock.skewMillis = 2 * HOUR;
            Assert.assertEquals(listOf("NEW"), burst(provider, 64, source));
            Assert.assertEquals("the prefetch plus exactly one fetch for the whole burst", 2, source.calls());
        }
    }

    @Test(timeout = 30_000)
    public void testColdBurstCausesExactlyOneFetch() throws Exception {
        // C3: 64 concurrent callers with no usable token cause exactly one fetch.
        ScriptedSource source = new ScriptedSource().thenToken("TOKEN-1", System.currentTimeMillis() + HOUR);
        source.setGate(); // the prefetch blocks until every caller is waiting
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build()) {
            Assert.assertEquals(listOf("TOKEN-1"), burst(provider, 64, source));
            Assert.assertEquals(1, source.calls());
        }
    }

    @Test(timeout = 30_000)
    public void testColdFailureFailsImmediatelyWhenBackoffOutlastsTheWait() {
        // section 5.3: "If the earliest permitted fetch is later than now + cold_wait, fail immediately."
        ScriptedSource source = new ScriptedSource()
                .thenThrow(TokenUnavailableException.retryable("HTTP 429 from the token endpoint", 60_000));
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .coldWaitMillis(5_000)
                .build()) {
            await(() -> provider.getConsecutiveFailures() == 1, 10_000, "the prefetch to fail");
            long start = System.nanoTime();
            try {
                provider.getToken();
                Assert.fail("expected the cold wait to fail");
            } catch (TokenUnavailableException e) {
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                Assert.assertTrue("must fail at once, not wait out cold_wait; took " + elapsedMillis + " ms",
                        elapsedMillis < 2_000);
                Assert.assertTrue(e.isRetryable());
                Assert.assertTrue("the remaining backoff is the hint: " + e.getRetryAfterMillis(),
                        e.getRetryAfterMillis() > 30_000);
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("HTTP 429"));
            }
            Assert.assertEquals("the caller must not bypass the backoff with a fetch of its own", 1, source.calls());
        }
    }

    @Test(timeout = 30_000)
    public void testColdFailurePassesAPermanentClassificationThrough() {
        // C4: a permanent classification is passed through to the caller.
        ScriptedSource source = new ScriptedSource()
                .thenThrow(TokenUnavailableException.permanent("no credential is configured"));
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .coldWaitMillis(300)
                .backoffInitialMillis(20)
                .backoffMaxMillis(40)
                .build()) {
            try {
                provider.getToken();
                Assert.fail("expected a token-unavailable error");
            } catch (TokenUnavailableException e) {
                Assert.assertFalse("the source's permanent classification must reach the caller", e.isRetryable());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("no credential is configured"));
            }
            // the provider keeps retrying a "permanent" failure: an operator can repair it without a restart
            int calls = source.calls();
            await(() -> source.calls() > calls, 5_000, "another retry of the permanent failure");
        }
    }

    @Test(timeout = 30_000)
    public void testColdFailureRaisesARetryableErrorWithinColdWait() {
        // C4: a retryable error is raised within cold_wait.
        ScriptedSource source = new ScriptedSource()
                .thenThrow(TokenUnavailableException.retryable("IMDS answered HTTP 503"));
        long coldWaitMillis = 400;
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .coldWaitMillis(coldWaitMillis)
                .backoffInitialMillis(20)
                .backoffMaxMillis(40)
                .build()) {
            long start = System.nanoTime();
            try {
                provider.getToken();
                Assert.fail("expected a token-unavailable error");
            } catch (TokenUnavailableException e) {
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                Assert.assertTrue(e.isRetryable());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("HTTP 503"));
                Assert.assertTrue("the wait must be bounded by cold_wait; took " + elapsedMillis + " ms",
                        elapsedMillis < coldWaitMillis + 2_000);
            }
            Assert.assertTrue("short failures inside cold_wait are retried, not surfaced on the first one",
                    source.calls() > 1);
        }
    }

    @Test
    public void testCredentialRedactionHelpers() {
        String token = "abc.def.ghi";
        String fingerprint = CredentialRedaction.fingerprint(token);
        Assert.assertTrue(fingerprint, fingerprint.matches("[0-9a-f]{8}"));
        String described = CredentialRedaction.describeToken(token);
        Assert.assertEquals("<redacted, 11 chars, sha256:" + fingerprint + '>', described);
        Assert.assertEquals("<none>", CredentialRedaction.describeToken(null));

        // controls, CR/LF and bidi overrides are removed; the result is capped at 256 characters
        String hostile = "line1\r\nFORGED: yes\u202Eevil\u0007" + repeat('x', 1_000);
        String clean = CredentialRedaction.sanitizeErrorText(hostile);
        Assert.assertTrue(clean.length() <= CredentialRedaction.MAX_ERROR_TEXT_LENGTH);
        Assert.assertTrue(clean.endsWith("..."));
        Assert.assertFalse(clean.contains("\r") || clean.contains("\n") || clean.contains("\u202E")
                || clean.contains("\u0007"));
        Assert.assertTrue(clean.startsWith("line1FORGED: yesevil"));
        // text that fits is kept whole
        String exact = repeat('y', CredentialRedaction.MAX_ERROR_TEXT_LENGTH);
        Assert.assertEquals(exact, CredentialRedaction.sanitizeErrorText(exact));
        Assert.assertNull(CredentialRedaction.sanitizeErrorText(null));
    }

    @Test
    public void testExpiringTokenRejectsInvalidTokensWithoutEchoingThem() {
        String secret = "SECRET-" + UUID.randomUUID();
        assertRejected(null);
        assertRejected("");
        assertRejected("   ");
        assertRejected(secret + "\r\nX-Injected: 1");
        assertRejected(secret + "\u00e9");
        try {
            new ExpiringToken(secret + '\n', T0);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertFalse("the token must never be echoed", e.getMessage().contains(secret));
        }
        ExpiringToken ok = new ExpiringToken(secret, T0, -5);
        Assert.assertFalse("a non-positive refresh hint means none", ok.hasRefreshAt());
        Assert.assertEquals(ExpiringToken.NO_REFRESH_AT, ok.getRefreshAtEpochMillis());
        Assert.assertTrue(new ExpiringToken(secret, T0 + HOUR, T0 + MIN).hasRefreshAt());
    }

    @Test(timeout = 30_000)
    public void testForcedRefreshIsRateLimitedIgnoresStaleTokensAndAcceptsTheSameToken() throws Exception {
        // C8: rate limited; a stale token is ignored; a same-token result is accepted.
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .thenToken("T1", T0 + HOUR)
                .thenToken("T2", T0 + HOUR)
                .thenToken("T2", T0 + HOUR);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5)
                .forcedMinIntervalMillis(30_000)
                .forcedWaitMillis(5_000)
                .build()) {
            scheduler.runNext();
            Assert.assertEquals("T1", provider.getToken().toString());

            // 403 never triggers a forced refresh (decision D2)
            assertReturnsPromptly(() -> provider.onTokenRejected("T1", 403));
            Assert.assertNotEquals("a 403 must not schedule a fetch", 0, scheduler.peek().delayNanos);

            // 401 for the current token: a fetch starts now and the caller waits for it
            Thread rejecter = start(() -> provider.onTokenRejected("T1", 401));
            await(scheduler::isNextImmediate, 5_000, "the forced fetch to be scheduled");
            scheduler.runNext();
            rejecter.join(5_000);
            Assert.assertFalse("onTokenRejected must return once the forced fetch completes", rejecter.isAlive());
            Assert.assertEquals("T2", provider.getToken().toString());
            Assert.assertEquals(2, source.calls());

            // rate limited: a second forced refresh within forced_min_interval is ignored
            clock.advanceMillis(30_000 - 1);
            assertReturnsPromptly(() -> provider.onTokenRejected("T2", 401));
            Assert.assertNotEquals("no forced fetch may be scheduled", 0, scheduler.peek().delayNanos);
            Assert.assertEquals(2, source.calls());

            // a stale token - it has already rotated - is ignored. The interval has now passed, so the rate limit
            // no longer applies and only the stale-token check keeps this from forcing a fetch.
            clock.advanceMillis(1);
            assertReturnsPromptly(() -> provider.onTokenRejected("T1", 401));
            Assert.assertNotEquals("a stale token must not schedule a fetch", 0, scheduler.peek().delayNanos);
            Assert.assertEquals(2, source.calls());

            // a forced fetch that returns the same token is a normal success
            rejecter = start(() -> provider.onTokenRejected("T2", 401));
            await(scheduler::isNextImmediate, 5_000, "the second forced fetch to be scheduled");
            scheduler.runNext();
            rejecter.join(5_000);
            Assert.assertFalse(rejecter.isAlive());
            Assert.assertEquals(3, source.calls());
            Assert.assertEquals("T2", provider.getToken().toString());
            Assert.assertEquals(0, provider.getConsecutiveFailures());
            Assert.assertNull("a same-token result is not a failure", provider.getLastFailure());
            Assert.assertEquals(clock.wallClockMillis(), provider.getLastSuccessEpochMillis());
        }
    }

    @Test(timeout = 30_000)
    public void testHandoutFloorTokenIsNeverHandedOut() throws Exception {
        // C6: a token inside handout_floor is never handed out.
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .thenToken("T1", T0 + 10 * MIN)
                .thenToken("T2", T0 + 70 * MIN);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            clock.jumpWallMillis(10 * MIN - RefreshingTokenProvider.DEFAULT_HANDOUT_FLOOR_MILLIS - 1);
            Assert.assertEquals("one millisecond outside the floor the token is still usable",
                    "T1", provider.getToken().toString());
            clock.jumpWallMillis(1);

            // exactly at expires_at - handout_floor the token is unusable: the caller must get a new one
            AtomicReference<Object> got = new AtomicReference<>();
            Thread caller = start(() -> got.set(provider.getToken().toString()));
            await(scheduler::isNextImmediate, 5_000, "the cold caller to start a fetch");
            Assert.assertNull("nothing may be handed out before the fetch", got.get());
            scheduler.runNext();
            caller.join(5_000);
            Assert.assertEquals("T2", got.get());
        }
    }

    @Test(timeout = 30_000)
    public void testHandoutFloorTokenIsNotHandedOutEvenWhenTheSourceRepeatsIt() throws Exception {
        // C6, second shape: the source keeps returning the token that sits inside the floor. The caller waits
        // out cold_wait and fails rather than receive it, and its error names the cause (section 4).
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().thenToken("T1", T0 + 10 * MIN);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5)
                .coldWaitMillis(1_000)
                .build()) {
            scheduler.runNext();
            clock.jumpWallMillis(10 * MIN - 30_000);
            AtomicReference<Object> got = new AtomicReference<>();
            Thread caller = start(() -> {
                try {
                    got.set(provider.getToken().toString());
                } catch (TokenUnavailableException e) {
                    got.set(e);
                }
            });
            await(scheduler::isNextImmediate, 5_000, "the cold caller to start a fetch");
            scheduler.runNext(); // the source returns T1 again, still inside the floor
            Assert.assertNull("an unusable token must not be handed out", got.get());
            clock.advanceMillis(1_001); // cold_wait elapses
            caller.join(5_000);
            Assert.assertTrue("expected a token-unavailable error, got " + got.get(),
                    got.get() instanceof TokenUnavailableException);
            TokenUnavailableException error = (TokenUnavailableException) got.get();
            Assert.assertTrue("the repeated token is a retryable failed fetch", error.isRetryable());
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("within handout_floor (60000 ms)"));
            Assert.assertEquals(1, provider.getConsecutiveFailures());
            Assert.assertEquals("the caller started one fetch, not one per completed fetch", 2, source.calls());
        }
    }

    @Test
    public void testHandoutFloorTokenOnArrivalIsARetryableFailure() {
        // C6, section 4: a token that expires within handout_floor of its receipt could never be handed out, so
        // its fetch fails - retryable, named in the failure record and in a caller's error - rather than count as
        // a success that leaves every caller to wait out cold_wait for an error with no cause.
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        final long floor = RefreshingTokenProvider.DEFAULT_HANDOUT_FLOOR_MILLIS;
        ScriptedSource source = new ScriptedSource()
                .then(() -> new ExpiringToken("SHORT", clock.wallClockMillis() + floor))
                .then(() -> new ExpiringToken("EDGE", clock.wallClockMillis() + floor + 1));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.0)
                .coldWaitMillis(0)
                .build()) {
            scheduler.runNext();
            Assert.assertEquals(1, provider.getConsecutiveFailures());
            RefreshingTokenProvider.FetchFailure failure = provider.getLastFailure();
            Assert.assertNotNull("a token that is unusable on arrival is a failed fetch", failure);
            Assert.assertTrue(failure.isRetryable());
            Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("within handout_floor (60000 ms)"));
            Assert.assertEquals("an unusable token is never held", RefreshingTokenProvider.NONE,
                    provider.getTokenExpiresAtEpochMillis());
            try {
                provider.getToken();
                Assert.fail("expected a token-unavailable error");
            } catch (TokenUnavailableException e) {
                Assert.assertTrue(e.isRetryable());
                Assert.assertTrue("the caller's error must name the cause: " + e.getMessage(),
                        e.getMessage().contains("within handout_floor (60000 ms)"));
            }

            // retried after backoff, not dropped: one millisecond beyond the floor is usable
            scheduler.advanceAndRunNext();
            Assert.assertEquals("EDGE", provider.getToken().toString());
            Assert.assertEquals(0, provider.getConsecutiveFailures());
        }
    }

    @Test(timeout = 30_000)
    public void testInterruptedWaitEndsPromptlyAndKeepsTheFlag() throws Exception {
        // C7: a cancelled wait returns promptly, and the cancellation signal is preserved.
        ScriptedSource source = new ScriptedSource().thenToken("NEVER", System.currentTimeMillis() + HOUR);
        source.setGate();
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).build()) {
            AtomicReference<TokenUnavailableException> error = new AtomicReference<>();
            AtomicBoolean flagKept = new AtomicBoolean();
            Thread caller = start(() -> {
                try {
                    provider.getToken();
                } catch (TokenUnavailableException e) {
                    error.set(e);
                    flagKept.set(Thread.currentThread().isInterrupted());
                }
            });
            await(() -> caller.getState() == Thread.State.TIMED_WAITING, 5_000, "the caller to wait");
            long start = System.nanoTime();
            caller.interrupt();
            caller.join(5_000);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertFalse(caller.isAlive());
            Assert.assertNotNull("an interrupted wait must end with a token-unavailable error", error.get());
            Assert.assertTrue("a cancelled wait is retryable", error.get().isRetryable());
            Assert.assertTrue("the interrupt flag must be preserved", flagKept.get());
            Assert.assertTrue("must return promptly; took " + elapsedMillis + " ms", elapsedMillis < 2_000);

            // a caller that arrives already interrupted fails at once, flag intact
            Thread.currentThread().interrupt();
            try {
                provider.getToken();
                Assert.fail();
            } catch (TokenUnavailableException e) {
                Assert.assertTrue(Thread.interrupted());
            }
            source.openGate();
        }
    }

    @Test(timeout = 30_000)
    public void testProactiveRefreshRunsWithoutACaller() {
        // section 5.2: the provider starts a fetch at the refresh time without waiting for a caller.
        long now = System.currentTimeMillis();
        ScriptedSource source = new ScriptedSource()
                .thenToken("SHORT", now + 1_000)
                .then(() -> new ExpiringToken("NEXT", System.currentTimeMillis() + HOUR));
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .handoutFloorMillis(0)
                .refreshMarginMillis(0)
                .minRefreshIntervalMillis(0)
                .build()) {
            Assert.assertTrue(provider.awaitReady(10_000));
            await(() -> source.calls() == 2, 10_000, "the proactive refresh");
            await(() -> provider.getTokenExpiresAtEpochMillis() > now + 10_000, 10_000, "the new token");
            Assert.assertEquals("NEXT", provider.getToken().toString());
        }
    }

    @Test
    public void testPromptReturnAssertionPropagatesErrors() throws Exception {
        AssertionError failure = new AssertionError("worker error");
        AssertionError reported = Assert.assertThrows(AssertionError.class, () -> assertReturnsPromptly(() -> {
            throw failure;
        }));
        Assert.assertSame(failure, reported.getCause());
    }

    @Test
    public void testPromptReturnAssertionPropagatesExceptions() throws Exception {
        IllegalStateException failure = new IllegalStateException("worker exception");
        AssertionError reported = Assert.assertThrows(AssertionError.class, () -> assertReturnsPromptly(() -> {
            throw failure;
        }));
        Assert.assertSame(failure, reported.getCause());
    }

    @Test
    public void testRefreshScheduleFollowsTheSpec() {
        // C1: refresh times follow section 5.2 for L = 24 h, 60 min and 5 min, with and without refresh_at.
        // u = 0 is the earliest jitter (0.45 L), u = 0.5 the middle (0.5 L), u = 0.999 near the latest (0.55 L).
        // u = 0 is the earliest jitter (0.45 L), u = 0.5 the middle (0.5 L), u = 1 the latest (0.55 L).
        assertRefreshDelay(DAY, 0.0, 0, 38_880_000L);  // 10.8 h
        assertRefreshDelay(DAY, 0.5, 0, 43_200_000L);  // 12 h
        assertRefreshDelay(DAY, 1.0, 0, 47_520_000L);  // 13.2 h
        assertRefreshDelay(HOUR, 0.0, 0, 1_620_000L);  // 27 min
        assertRefreshDelay(HOUR, 0.5, 0, 1_800_000L);  // 30 min
        assertRefreshDelay(HOUR, 1.0, 0, 1_980_000L);  // 33 min
        // L = 5 min: the margin is min(5 min, L/2) = 2.5 min, so the latest refresh is at 2.5 min
        assertRefreshDelay(5 * MIN, 0.0, 0, 135_000L);
        assertRefreshDelay(5 * MIN, 0.5, 0, 150_000L);
        assertRefreshDelay(5 * MIN, 1.0, 0, 150_000L);
        // a refresh hint earlier than the jittered half-life wins ...
        assertRefreshDelay(HOUR, 0.5, T0 + 10 * MIN, 10 * MIN);
        assertRefreshDelay(DAY, 0.5, T0 + 6 * HOUR, 6 * HOUR);
        // ... but never earlier than min_refresh_interval
        assertRefreshDelay(HOUR, 0.5, T0 + 5_000, 30_000L);
        assertRefreshDelay(HOUR, 0.5, T0 - HOUR, 30_000L);
        // a later hint does not delay the refresh
        assertRefreshDelay(HOUR, 0.5, T0 + 50 * MIN, 30 * MIN);
        assertRefreshDelay(5 * MIN, 0.5, T0 + 4 * MIN, 150_000L);
        // a 50 s token: min_refresh_interval shrinks to L/2 = 25 s, the margin to 25 s
        assertRefreshDelay(50_000L, 0.0, 0, 25_000L);
        // the pure function agrees with the provider
        Assert.assertEquals(T0 + 1_800_000L,
                RefreshingTokenProvider.computeRefreshAtMillis(T0, T0 + HOUR, 0, 0.5, 5 * MIN, 30_000, 0));
    }

    @Test
    public void testRefreshScheduleKeepsTheRefreshInsideTheHandoutWindow() {
        // C1 with the default 60 s handout_floor: a refresh runs at least min_refresh_interval (30 s) before the token
        // leaves its hand-out window, unless that is sooner than 30 s after the fetch.
        final long floor = RefreshingTokenProvider.DEFAULT_HANDOUT_FLOOR_MILLIS;
        // long-lived tokens keep their schedule
        assertRefreshDelay(floor, DAY, 0.5, 0, 43_200_000L);
        assertRefreshDelay(floor, HOUR, 1.0, 0, 1_980_000L);
        assertRefreshDelay(floor, 5 * MIN, 0.5, 0, 150_000L);
        assertRefreshDelay(floor, 180_000L, 0.5, 0, 90_000L); // the shortest lifetime the bound leaves alone
        // a shorter-lived token is refreshed 30 s before its hand-out window closes ...
        assertRefreshDelay(floor, 150_000L, 0.0, 0, 60_000L); // handed out for 90 s; the half-life is 67.5-75 s
        assertRefreshDelay(floor, 150_000L, 0.5, 0, 60_000L);
        assertRefreshDelay(floor, 120_000L, 0.5, 0, 30_000L); // handed out for 60 s; the half-life is 60 s
        // ... but no sooner than 30 s after the fetch, which is still inside a window longer than 30 s
        assertRefreshDelay(floor, 100_000L, 0.0, 0, 30_000L); // handed out for 40 s; the half-life is 45-50 s
        assertRefreshDelay(floor, 100_000L, 1.0, 0, 30_000L);
        // an earlier refresh hint still wins
        assertRefreshDelay(floor, 150_000L, 0.5, T0 + 40_000L, 40_000L);
        // a token handed out for 30 s or less cannot be refreshed inside its window: the 30 s spacing wins
        assertRefreshDelay(floor, 75_000L, 0.5, 0, 30_000L); // handed out for 15 s
        // the pure function agrees with the provider
        Assert.assertEquals(T0 + 30_000L,
                RefreshingTokenProvider.computeRefreshAtMillis(T0, T0 + 100_000L, 0, 0.5, 5 * MIN, 30_000, floor));
    }

    @Test
    public void testRetryAfterIsHonouredAndCapped() {
        // C5: retry_after is honoured and capped at retry_after_max.
        assertFirstBackoff(10_000L, 10_000L);               // longer than the backoff: honoured
        assertFirstBackoff(10 * MIN, 5 * MIN);              // capped at retry_after_max (5 min)
        assertFirstBackoff(100L, 250L);                     // shorter than the backoff: the backoff wins
        assertFirstBackoff(TokenUnavailableException.NO_RETRY_AFTER, 250L);
    }

    @Test
    public void testSameTokenConvergesAndStaysUsable() {
        // C2: the platform keeps returning the same 24 h token. Each refresh lands at about half the remaining
        // lifetime, so the token is fetched only a handful of times before it enters the hand-out floor, and
        // it stays usable all the while.
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        final long expiry = T0 + DAY;
        ScriptedSource source = new ScriptedSource().thenToken("SAME", expiry);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            int fetchesWhileUsable = 1;
            while (true) {
                long remaining = expiry - clock.wallClockMillis();
                ManualScheduler.Task next = scheduler.peek();
                long expectedDelay = RefreshingTokenProvider.computeRefreshAtMillis(
                        0, remaining, 0, 0.5, 5 * MIN, 30_000, RefreshingTokenProvider.DEFAULT_HANDOUT_FLOOR_MILLIS);
                Assert.assertEquals("refresh at about half the remaining lifetime [remaining=" + remaining + ']',
                        expectedDelay, next.delayMillis());
                Assert.assertTrue(next.delayMillis() <= remaining / 2 + 1);
                scheduler.advanceAndRunNext();
                if (clock.wallClockMillis() >= expiry - RefreshingTokenProvider.DEFAULT_HANDOUT_FLOOR_MILLIS) {
                    break;
                }
                Assert.assertEquals("SAME", provider.getToken().toString());
                fetchesWhileUsable++;
            }
            // The remaining lifetime halves over the first 10 fetches (86400 s -> ... -> 168.75 s). The 11th runs
            // 30 s before the token would leave its hand-out window, at 90 s left, and the next one finds it inside
            // the floor.
            Assert.assertEquals(11, fetchesWhileUsable);
            // the fetch that returned the token inside the floor is a failed fetch (section 4)
            Assert.assertEquals(1, provider.getConsecutiveFailures());
            Assert.assertTrue(provider.getLastFailure().getMessage(),
                    provider.getLastFailure().getMessage().contains("within handout_floor"));
        }
    }

    @Test
    public void testSentinelTokenIsNeverRendered() throws Exception {
        // C20 for the cache: a sentinel token never appears in a string representation, an error, a failure
        // record or a log line - including when the source returns it in a malformed result.
        final String sentinel = "SENTINEL-" + UUID.randomUUID();
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger(RefreshingTokenProvider.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        ch.qos.logback.classic.Level savedLevel = logger.getLevel();
        logger.setLevel(ch.qos.logback.classic.Level.ALL);
        logger.addAppender(appender);
        List<String> rendered = new ArrayList<>();
        try {
            ExpiringToken token = new ExpiringToken(sentinel, T0 + HOUR, T0 + MIN);
            rendered.add(token.toString());
            try {
                new ExpiringToken(sentinel + "\r\n", T0 + HOUR);
            } catch (IllegalArgumentException e) {
                rendered.add(e.getMessage());
            }

            FakeClock clock = new FakeClock(T0);
            ManualScheduler scheduler = new ManualScheduler(clock);
            ScriptedSource source = new ScriptedSource()
                    .thenToken(sentinel, T0 + 10 * MIN)
                    .thenToken(sentinel, T0 - 1) // malformed: already expired
                    .thenThrow(new IllegalStateException("token endpoint returned a response without access_token"));
            RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).coldWaitMillis(0).build();
            try {
                scheduler.runNext();
                rendered.add(provider.toString());
                scheduler.advanceAndRunNext();
                rendered.add(String.valueOf(provider.getLastFailure()));
                scheduler.advanceAndRunNext();
                rendered.add(String.valueOf(provider.getLastFailure()));
                rendered.add(provider.toString());
                clock.jumpWallMillis(HOUR);
                try {
                    provider.getToken();
                    Assert.fail("no usable token is held");
                } catch (TokenUnavailableException e) {
                    rendered.add(e.getMessage());
                    rendered.add(e.toString());
                }
            } finally {
                provider.close();
            }
            rendered.add(provider.toString());
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
            appender.stop();
        }
        for (ch.qos.logback.classic.spi.ILoggingEvent event : appender.list) {
            rendered.add(event.getFormattedMessage());
        }
        Assert.assertFalse("expected log lines to inspect", appender.list.isEmpty());
        for (String s : rendered) {
            Assert.assertFalse("the token leaked into: " + s, s.contains(sentinel));
        }
        Assert.assertTrue(rendered.get(0), rendered.get(0).contains("sha256:"));
    }

    @Test
    public void testShortLivedTokenIsRefreshedWhileItIsStillHandedOut() {
        // section 5.2: a 100 s token is handed out for 40 s. An immediately returning refresh at +30 s keeps a
        // usable token available. Its half-life alone would schedule the refresh at +45-50 s, after the cutoff.
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().then(() -> new ExpiringToken(
                "T" + (clock.wallClockMillis() - T0), clock.wallClockMillis() + 100_000L));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            long previousFetch = T0;
            for (int refresh = 1; refresh <= 3; refresh++) {
                Assert.assertEquals("refresh 30 s after the fetch", 30_000L, scheduler.peek().delayMillis());
                scheduler.advanceAndRunNext();
                Assert.assertEquals(previousFetch + 30_000L, clock.wallClockMillis());
                // one second after the previous token left its hand-out window: fetch + 100 s - 60 s floor
                clock.advanceMillis(previousFetch + 41_000L - clock.wallClockMillis());
                Assert.assertEquals("an idle client holds a usable token",
                        "T" + refresh * 30_000L, provider.getToken().toString());
                Assert.assertFalse("no caller had to start a fetch", scheduler.isNextImmediate());
                previousFetch += 30_000L;
            }
            Assert.assertEquals(4, source.calls());
        }
    }

    @Test(timeout = 30_000)
    public void testSlowSuccessfulRefreshCanOutlastTheHandoutWindow() throws Exception {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .thenToken("T1", T0 + 100_000L)
                .then(() -> new ExpiringToken("T2", clock.wallClockMillis() + 100_000L));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            Assert.assertEquals("T1", provider.getToken().toString());
            Assert.assertEquals(30_000L, scheduler.peek().delayMillis());
            source.setGate();
            FutureTask<ManualScheduler.Task> refresh = new FutureTask<>(scheduler::advanceAndRunNext);
            FutureTask<String> token = new FutureTask<>(() -> provider.getToken().toString());
            Thread refresher = start(refresh);
            Thread caller = null;
            try {
                await(() -> source.calls() == 2, 5_000, "the scheduled refresh to enter the source");
                Assert.assertEquals(T0 + 30_000L, clock.wallClockMillis());
                clock.advanceMillis(9_999);
                Assert.assertEquals("T1", provider.getToken().toString());
                clock.advanceMillis(1);
                Assert.assertFalse("the old token has reached its handout floor", provider.awaitReady(0));

                caller = start(token);
                Thread waitingCaller = caller;
                await(() -> waitingCaller.getState() == Thread.State.TIMED_WAITING,
                        5_000, "the caller to wait for the in-flight refresh");
                clock.advanceMillis(9_999);
                Assert.assertFalse(provider.awaitReady(0));
                Assert.assertFalse("a successful but unfinished fetch cannot supply a token", token.isDone());

                clock.advanceMillis(1);
                source.openGate();
                Assert.assertEquals("T2", token.get(5, TimeUnit.SECONDS));
                refresh.get(5, TimeUnit.SECONDS);
                Assert.assertEquals(T0 + 50_000L, provider.getLastSuccessEpochMillis());
                Assert.assertEquals(2, source.calls());
                Assert.assertEquals(0, provider.getConsecutiveFailures());
                Assert.assertNull(provider.getLastFailure());
            } finally {
                source.openGate();
                refresher.interrupt();
                refresher.join(5_000);
                if (caller != null) {
                    caller.interrupt();
                    caller.join(5_000);
                    Assert.assertFalse("the token wait must finish", caller.isAlive());
                }
                Assert.assertFalse("the refresh must finish", refresher.isAlive());
            }
        }
    }

    @Test
    public void testSourceFailureTextIsSanitized() {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().thenThrow(TokenUnavailableException.retryable(
                "AADSTS50000:\r\nX-Forged: 1\u202E" + repeat('z', 500)));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.0).build()) {
            scheduler.runNext();
            String message = provider.getLastFailure().getMessage();
            Assert.assertTrue(message.length() <= CredentialRedaction.MAX_ERROR_TEXT_LENGTH);
            Assert.assertFalse(message.contains("\r") || message.contains("\n") || message.contains("\u202E"));
            Assert.assertTrue(message, message.startsWith("AADSTS50000:X-Forged: 1zzz"));
        }
    }

    @Test
    public void testSuccessResetsTheBackoff() {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .thenThrow(TokenUnavailableException.retryable("down"))
                .thenThrow(TokenUnavailableException.retryable("down"))
                .thenThrow(TokenUnavailableException.retryable("down"))
                .thenToken("UP", T0 + HOUR)
                .thenThrow(TokenUnavailableException.retryable("down again"));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.0).build()) {
            scheduler.runNext();
            Assert.assertEquals(250, scheduler.peek().delayMillis());
            scheduler.advanceAndRunNext();
            Assert.assertEquals(500, scheduler.peek().delayMillis());
            scheduler.advanceAndRunNext();
            Assert.assertEquals(1_000, scheduler.peek().delayMillis());
            scheduler.advanceAndRunNext();
            Assert.assertEquals("UP", provider.getToken().toString());
            Assert.assertEquals(0, provider.getConsecutiveFailures());
            Assert.assertEquals("down", provider.getLastFailure().getMessage());
            scheduler.advanceAndRunNext(); // the proactive refresh fails
            Assert.assertEquals(1, provider.getConsecutiveFailures());
            Assert.assertEquals("a success resets n, so the backoff starts over", 250, scheduler.peek().delayMillis());
            Assert.assertEquals("the current token keeps being served while usable",
                    "UP", provider.getToken().toString());
        }
    }

    @Test
    public void testTokenHandedOutTooBrieflyToRefreshAheadIsReportedOnce() {
        // section 5.2: a 75 s token is handed out for 15 s, less than min_refresh_interval, so it cannot be refreshed
        // while it is still handed out. The refresh keeps its 30 s spacing - no fetch storm - and the provider says
        // once why callers wait for a fetch.
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger)
                org.slf4j.LoggerFactory.getLogger(RefreshingTokenProvider.class);
        ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        ch.qos.logback.classic.Level savedLevel = logger.getLevel();
        logger.setLevel(ch.qos.logback.classic.Level.ALL);
        logger.addAppender(appender);
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().then(
                () -> new ExpiringToken("BRIEF", clock.wallClockMillis() + 75_000L));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            Assert.assertEquals("the refresh keeps its spacing", 30_000L, scheduler.peek().delayMillis());
            scheduler.advanceAndRunNext();
            Assert.assertEquals(30_000L, scheduler.peek().delayMillis());
            Assert.assertEquals(2, source.calls());
        } finally {
            logger.detachAppender(appender);
            logger.setLevel(savedLevel);
            appender.stop();
        }
        List<String> warnings = new ArrayList<>();
        for (ch.qos.logback.classic.spi.ILoggingEvent event : appender.list) {
            if (event.getLevel() == ch.qos.logback.classic.Level.WARN) {
                warnings.add(event.getFormattedMessage());
            }
        }
        Assert.assertEquals("reported once, not once per fetch: " + warnings, 1, warnings.size());
        Assert.assertTrue(warnings.get(0), warnings.get(0).contains("handed out for only 15000 ms"));
        Assert.assertTrue(warnings.get(0), warnings.get(0).contains("lower handout_floor"));
    }

    @Test(timeout = 30_000)
    public void testUnclassifiedSourceExceptionIsRetryable() {
        ScriptedSource source = new ScriptedSource().thenThrow(new IllegalStateException("socket reset"));
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .coldWaitMillis(200)
                .backoffInitialMillis(20)
                .backoffMaxMillis(40)
                .build()) {
            try {
                provider.getToken();
                Assert.fail();
            } catch (TokenUnavailableException e) {
                Assert.assertTrue("an error that carries no classification is retryable", e.isRetryable());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("IllegalStateException: socket reset"));
            }
        }
    }

    @Test
    public void testWarmPathDoesNotFetch() {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().thenToken("WARM", T0 + HOUR);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            ManualScheduler.Task scheduled = scheduler.peek();
            for (int i = 0; i < 1_000; i++) {
                Assert.assertEquals("WARM", provider.getToken().toString());
            }
            Assert.assertEquals(1, source.calls());
            Assert.assertSame("a warm read must not touch the schedule", scheduled, scheduler.peek());
        }
    }

    @Test(timeout = 30_000)
    public void testWarmReaderDoesNotRescheduleAfterAConcurrentRefresh() throws Exception {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .thenToken("T1", T0 + HOUR)
                .then(() -> new ExpiringToken("T2", clock.wallClockMillis() + HOUR));
        CountDownLatch snapshotRead = new CountDownLatch(1);
        CountDownLatch resumeReader = new CountDownLatch(1);
        AtomicBoolean isGateArmed = new AtomicBoolean();
        RefreshingTokenProvider.Clock gatedClock = new RefreshingTokenProvider.Clock() {
            @Override
            public long monotonicNanos() {
                return clock.monotonicNanos();
            }

            @Override
            public long wallClockMillis() {
                if (isGateArmed.compareAndSet(true, false)) {
                    snapshotRead.countDown();
                    try {
                        Assert.assertTrue("the reader must resume", resumeReader.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
                return clock.wallClockMillis();
            }
        };
        try (RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source)
                .clock(gatedClock).scheduler(scheduler).random(() -> 0.5).build()) {
            scheduler.runNext();
            Assert.assertEquals("T1", provider.getToken().toString());
            clock.advanceMillis(scheduler.peek().delayMillis());
            isGateArmed.set(true);
            FutureTask<String> token = new FutureTask<>(() -> provider.getToken().toString());
            Thread reader = start(token);
            try {
                // Pause after getToken() captures T1, then publish T2 before the reader checks T1's deadline.
                Assert.assertTrue("the reader must capture the old token", snapshotRead.await(5, TimeUnit.SECONDS));
                scheduler.runNext();
                Assert.assertEquals("T2", provider.getToken().toString());
                ManualScheduler.Task nextRefresh = scheduler.peek();
                Assert.assertEquals(30 * MIN, nextRefresh.delayMillis());
                resumeReader.countDown();
                Assert.assertEquals("T1", token.get(5, TimeUnit.SECONDS));
                Assert.assertSame("the stale reader must preserve the new refresh schedule",
                        nextRefresh, scheduler.peek());
                Assert.assertEquals(2, source.calls());
            } finally {
                resumeReader.countDown();
                reader.interrupt();
                reader.join(5_000);
                Assert.assertFalse("the reader must finish", reader.isAlive());
            }
        }
    }

    @Test
    public void testWallClockOverdueRefreshStartsInTheBackground() {
        // section 5.3 step 1: a usable token is returned at once, and when its refresh time has passed - here the
        // wall clock moved past it while the monotonic schedule did not - a fetch starts in the background.
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().thenToken("T1", T0 + HOUR).thenToken("T2", T0 + 2 * HOUR);
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.5).build()) {
            scheduler.runNext();
            Assert.assertEquals(30 * MIN, scheduler.peek().delayMillis());
            clock.jumpWallMillis(31 * MIN);
            Assert.assertEquals("the usable token is returned at once", "T1", provider.getToken().toString());
            Assert.assertEquals("the overdue refresh is pulled forward", 0, scheduler.peek().delayNanos);
            scheduler.runNext();
            Assert.assertEquals("T2", provider.getToken().toString());
        }
    }

    private static void assertBackoffDelays(double u, long[] expectedMillis) {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().thenThrow(TokenUnavailableException.retryable("down"));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, u).build()) {
            scheduler.runNext();
            for (int n = 1; n <= expectedMillis.length; n++) {
                Assert.assertEquals("backoff after failure " + n + " [u=" + u + ']',
                        expectedMillis[n - 1], scheduler.peek().delayMillis());
                Assert.assertEquals(n, provider.getConsecutiveFailures());
                scheduler.advanceAndRunNext();
            }
        }
    }

    private static void assertFirstBackoff(long retryAfterMillis, long expectedDelayMillis) {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource().thenThrow(
                new TokenUnavailableException("throttled", true, retryAfterMillis));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, 0.0).build()) {
            scheduler.runNext();
            Assert.assertEquals("retry_after=" + retryAfterMillis, expectedDelayMillis, scheduler.peek().delayMillis());
            Assert.assertEquals(retryAfterMillis, provider.getLastFailure().getRetryAfterMillis());
        }
    }

    private static void assertRefreshDelay(long lifetimeMillis, double u, long refreshAt, long expectedDelayMillis) {
        assertRefreshDelay(0, lifetimeMillis, u, refreshAt, expectedDelayMillis);
    }

    private static void assertRefreshDelay(
            long handoutFloorMillis,
            long lifetimeMillis,
            double u,
            long refreshAt,
            long expectedDelayMillis
    ) {
        FakeClock clock = new FakeClock(T0);
        ManualScheduler scheduler = new ManualScheduler(clock);
        ScriptedSource source = new ScriptedSource()
                .then(() -> new ExpiringToken("TOKEN", T0 + lifetimeMillis, refreshAt));
        try (RefreshingTokenProvider provider = provider(source, clock, scheduler, u)
                .handoutFloorMillis(handoutFloorMillis)
                .build()) {
            Assert.assertEquals("the prefetch runs at once", 0, scheduler.peek().delayNanos);
            scheduler.runNext();
            Assert.assertEquals("refresh delay [handoutFloor=" + handoutFloorMillis + ", L=" + lifetimeMillis
                            + ", u=" + u + ", refreshAt=" + refreshAt + ']',
                    expectedDelayMillis, scheduler.peek().delayMillis());
            Assert.assertEquals(T0 + lifetimeMillis, provider.getTokenExpiresAtEpochMillis());
        }
    }

    private static void assertRejected(String token) {
        try {
            new ExpiringToken(token, T0);
            Assert.fail("expected the token to be rejected");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    private static void assertReturnsPromptly(Runnable r) throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = start(() -> {
            try {
                r.run();
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        t.join(2_000);
        Assert.assertFalse("the call must return without waiting", t.isAlive());
        if (failure.get() != null) {
            throw new AssertionError("the call must return normally", failure.get());
        }
    }

    private static List<String> burst(RefreshingTokenProvider provider, int callers, ScriptedSource source)
            throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(callers);
        List<Thread> threads = new ArrayList<>();
        String[] results = new String[callers];
        for (int i = 0; i < callers; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                ready.countDown();
                results[idx] = provider.getToken().toString();
            });
            threads.add(t);
            t.start();
        }
        Assert.assertTrue(ready.await(10, TimeUnit.SECONDS));
        // Give every caller time to reach the wait before the single fetch completes. The assertion on the
        // fetch count holds however the callers interleave; this only makes the burst a real burst.
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() - deadline < 0 && !allParked(threads)) {
            Thread.sleep(5);
        }
        source.openGate();
        List<String> distinct = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            threads.get(i).join(10_000);
            Assert.assertFalse(threads.get(i).isAlive());
            if (!distinct.contains(results[i])) {
                distinct.add(results[i]);
            }
        }
        return distinct;
    }

    private static boolean allParked(List<Thread> threads) {
        for (Thread t : threads) {
            Thread.State s = t.getState();
            if (t.isAlive() && s != Thread.State.TIMED_WAITING && s != Thread.State.WAITING) {
                return false;
            }
        }
        return true;
    }

    private static List<String> listOf(String s) {
        List<String> l = new ArrayList<>();
        l.add(s);
        return l;
    }

    private static RefreshingTokenProvider.Builder provider(
            ScriptedSource source,
            FakeClock clock,
            ManualScheduler scheduler,
            double u
    ) {
        return RefreshingTokenProvider.builder(source)
                .clock(clock)
                .scheduler(scheduler)
                .random(() -> u);
    }

    private static List<Thread> refresherThreads() {
        List<Thread> threads = new ArrayList<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().startsWith("qdb-token-refresh-") && t.isAlive()) {
                threads.add(t);
            }
        }
        return threads;
    }

    private static String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    private static Thread start(Runnable r) {
        Thread t = new Thread(r, "token-test-caller");
        t.setDaemon(true);
        t.start();
        return t;
    }

    // Real monotonic time, with a wall clock the test can push forward.
    private static final class SkewedClock implements RefreshingTokenProvider.Clock {
        volatile long skewMillis;

        @Override
        public long monotonicNanos() {
            return System.nanoTime();
        }

        @Override
        public long wallClockMillis() {
            return System.currentTimeMillis() + skewMillis;
        }
    }
}
