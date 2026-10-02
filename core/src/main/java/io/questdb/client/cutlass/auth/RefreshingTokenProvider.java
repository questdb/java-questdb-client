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

package io.questdb.client.cutlass.auth;

import io.questdb.client.HttpTokenProvider;
import io.questdb.client.std.Chars;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.DoubleSupplier;

/**
 * A shared, proactively refreshing cache in front of a {@link TokenSource}: the refreshing provider of the
 * dynamic-credential specification (design/qwp-token-provider-spec.md, section 5). Hand one instance to
 * {@code Sender.builder(...).httpTokenProvider(...)}, {@code QwpQueryClient.withBearerTokenProvider(...)} or
 * {@code QuestDB.builder().httpTokenProvider(...)}; every connection then reads the cached token, and the
 * provider fetches a new one in the background well before the current one expires.
 * <pre>{@code
 * TokenCredential credential = new DefaultAzureCredentialBuilder().build();
 * TokenRequestContext scope = new TokenRequestContext().addScopes("api://<questdb-app-id>/.default");
 * RefreshingTokenProvider tokens = RefreshingTokenProvider.builder(() -> {
 *     AccessToken t = credential.getTokenSync(scope);
 *     return new ExpiringToken(t.getToken(), t.getExpiresAt().toInstant().toEpochMilli());
 * }).build();
 * try (QuestDB db = QuestDB.connect("wss::addr=qdb1:9000,qdb2:9000;", tokens)) {
 *     ...
 * } finally {
 *     tokens.close();
 * }
 * }</pre>
 * <h2>Behaviour</h2>
 * <ul>
 *   <li><b>Prefetch.</b> The first fetch starts as soon as the provider is built, on the provider's own daemon
 *   thread. {@link #awaitReady(long)} lets an application gate startup on it.</li>
 *   <li><b>Proactive refresh.</b> After a fetch received at {@code f} with expiry {@code e}, the next fetch is
 *   scheduled at a jittered half-life, {@code f + (e - f) * U(0.45, 0.55)}, no later than
 *   {@code e - min(refresh_margin, (e - f) / 2)}, no earlier than {@code f + min(min_refresh_interval,
 *   (e - f) / 2)}, and no later than the platform's refresh hint when it gave one. While the source is healthy,
 *   even an idle client therefore always holds a usable token.</li>
 *   <li><b>Hand-out.</b> A token is usable only while {@code now < e - handout_floor}. {@link #getToken()}
 *   returns a usable token at once - a volatile read, no I/O, no lock. With no usable token it starts a fetch
 *   (unless one is running or the provider is backing off after failures) and waits up to {@code cold_wait},
 *   then throws a {@link TokenUnavailableException} carrying the classification of the most recent failed
 *   fetch, or a retryable one when no fetch failed. It never hands out an unusable token. Any number of
 *   concurrent callers cause at most one fetch.</li>
 *   <li><b>Failures.</b> A failed fetch is retried with jittered exponential backoff, from
 *   {@code backoff_initial} up to {@code backoff_max}, honouring the source's {@code retry_after} up to
 *   {@code retry_after_max}. Retries continue whatever the classification for as long as the provider is open,
 *   the current token keeps being served while it remains usable, and failures are logged as warnings at most
 *   once a minute, with only the classification and a sanitized message.</li>
 *   <li><b>Forced refresh.</b> {@link #onTokenRejected(CharSequence, int)} with {@code 401} for the current
 *   token starts a fetch, at most once per {@code forced_min_interval}, and waits up to {@code forced_wait} for
 *   it. A stale token (one that has already rotated) and any other status are ignored.</li>
 *   <li><b>Interrupts.</b> A caller interrupted while waiting gets a retryable {@link TokenUnavailableException}
 *   promptly, with its interrupt flag still set.</li>
 *   <li><b>Close.</b> {@link #close()} stops the refresher and wakes every waiter; afterwards
 *   {@link #getToken()} fails with a permanent error. Clients never close a provider the application supplied:
 *   close it yourself once every client using it is closed.</li>
 *   <li><b>Clocks.</b> Expiry comparisons use the wall clock; delays and waits use a monotonic clock.</li>
 * </ul>
 * Nothing here ever renders the token: {@link #toString()}, log lines and error messages show at most its
 * length and an 8-hex-digit SHA-256 fingerprint.
 */
public final class RefreshingTokenProvider implements HttpTokenProvider, QuietCloseable {
    public static final long DEFAULT_BACKOFF_INITIAL_MILLIS = 500;
    public static final long DEFAULT_BACKOFF_MAX_MILLIS = 60_000;
    public static final long DEFAULT_COLD_WAIT_MILLIS = 30_000;
    public static final long DEFAULT_FORCED_MIN_INTERVAL_MILLIS = 30_000;
    public static final long DEFAULT_FORCED_WAIT_MILLIS = 5_000;
    public static final long DEFAULT_HANDOUT_FLOOR_MILLIS = 60_000;
    public static final long DEFAULT_MIN_REFRESH_INTERVAL_MILLIS = 30_000;
    public static final long DEFAULT_REFRESH_MARGIN_MILLIS = 300_000;
    public static final long DEFAULT_RETRY_AFTER_MAX_MILLIS = 300_000;
    /**
     * Value returned by the observability getters when there is nothing to report.
     */
    public static final long NONE = -1;
    private static final long CLOSE_AWAIT_MILLIS = 1_000;
    private static final long FAILURE_LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final Logger LOG = LoggerFactory.getLogger(RefreshingTokenProvider.class);
    // Delays are bookkept as monotonic deadlines compared by subtraction, which is only sound while every
    // difference stays below 2^63. Clamping each delay to 2^61 ns (~73 years) keeps that true for any token
    // lifetime a source can report, including Long.MAX_VALUE "never expires".
    private static final long MAX_DELAY_NANOS = Long.MAX_VALUE >> 2;
    private static final AtomicInteger THREAD_IDS = new AtomicInteger();
    // A waiter re-checks its deadline at least this often when the clock is not the system clock, so a test
    // that advances a fake clock is noticed. With the system clock the waits are untimed beyond their deadline.
    private static final long WAIT_SLICE_NANOS = TimeUnit.MILLISECONDS.toNanos(20);
    private final long backoffInitialMillis;
    private final long backoffMaxMillis;
    private final Clock clock;
    private final long coldWaitMillis;
    private final long forcedMinIntervalNanos;
    private final long forcedWaitNanos;
    private final long handoutFloorMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private final long maxWaitSliceNanos;
    private final long minRefreshIntervalMillis;
    private final String name;
    private final DoubleSupplier random;
    private final long refreshMarginMillis;
    private final long retryAfterMaxMillis;
    private final Scheduler scheduler;
    private final TokenSource source;
    // Signalled whenever a fetch completes and on close.
    private final Condition stateChanged = lock.newCondition();
    // ---- guarded by lock ----
    private boolean backoffActive;
    private long backoffUntilNanos;
    private volatile boolean closed;
    private long completedFetches;
    private volatile int consecutiveFailures;
    private boolean failureLogged;
    private volatile boolean fetchRunning;
    private boolean forcedRefreshed;
    // The current token, or null. Immutable snapshot, published with a volatile write so the warm path in
    // getToken() needs no lock.
    private volatile Held held;
    private volatile FetchFailure lastFailure;
    private long lastFailureLogNanos;
    private long lastForcedNanos;
    private volatile long lastSuccessEpochMillis = NONE;
    private long scheduledDueNanos;
    private long scheduledSeq;
    private ScheduledTask scheduledTask;

    private RefreshingTokenProvider(Builder b) {
        this.source = b.source;
        this.name = b.name;
        this.clock = b.clock;
        this.maxWaitSliceNanos = b.clock == SystemClock.INSTANCE ? Long.MAX_VALUE : WAIT_SLICE_NANOS;
        this.scheduler = b.scheduler != null ? b.scheduler : new DefaultScheduler();
        this.random = b.random;
        this.refreshMarginMillis = b.refreshMarginMillis;
        this.minRefreshIntervalMillis = b.minRefreshIntervalMillis;
        this.handoutFloorMillis = b.handoutFloorMillis;
        this.coldWaitMillis = b.coldWaitMillis;
        this.backoffInitialMillis = b.backoffInitialMillis;
        this.backoffMaxMillis = b.backoffMaxMillis;
        this.retryAfterMaxMillis = b.retryAfterMaxMillis;
        this.forcedMinIntervalNanos = TimeUnit.MILLISECONDS.toNanos(b.forcedMinIntervalMillis);
        this.forcedWaitNanos = TimeUnit.MILLISECONDS.toNanos(b.forcedWaitMillis);
    }

    /**
     * Starts building a provider around {@code source}.
     *
     * @param source obtains new tokens; called only from the provider's refresher thread
     * @return a builder whose defaults are the specification's
     */
    public static Builder builder(TokenSource source) {
        if (source == null) {
            throw new IllegalArgumentException("token source must not be null");
        }
        return new Builder(source);
    }

    /**
     * The refresh instant of the specification's section 5.2 for a token received at {@code f} that expires at
     * {@code e}, given a uniform sample {@code u} in {@code [0, 1)}. Pure; exposed for the conformance tests.
     */
    @TestOnly
    public static long computeRefreshAtMillis(
            long f,
            long e,
            long refreshAt,
            double u,
            long refreshMarginMillis,
            long minRefreshIntervalMillis
    ) {
        final long life = e - f;
        final long half = life / 2;
        final long margin = Math.min(refreshMarginMillis, half);
        final long floor = Math.min(minRefreshIntervalMillis, half);
        final double fraction = 0.45 + 0.10 * Math.max(0.0, Math.min(1.0, u));
        long r = f + (long) (life * fraction);
        r = Math.min(r, e - margin);
        r = Math.max(r, f + floor);
        if (refreshAt > 0) {
            r = Math.min(r, Math.max(refreshAt, f + floor));
        }
        return r;
    }

    private static String instant(long epochMillis) {
        try {
            return Instant.ofEpochMilli(epochMillis).toString();
        } catch (RuntimeException e) {
            return Long.toString(epochMillis);
        }
    }

    private static long millisToNanos(long millis) {
        return Math.min(TimeUnit.MILLISECONDS.toNanos(Math.max(0, millis)), MAX_DELAY_NANOS);
    }

    /**
     * Waits until the provider holds a usable token, starting a fetch if none is running and the provider is
     * not backing off. Use it to gate application startup on the first fetch.
     *
     * @param timeoutMillis the longest to wait
     * @return true once a usable token is held; false on timeout, when the provider is closed, or when the
     * calling thread is interrupted (its interrupt flag is then left set)
     */
    public boolean awaitReady(long timeoutMillis) {
        lock.lock();
        try {
            final long start = clock.monotonicNanos();
            final long deadline = start + millisToNanos(timeoutMillis);
            if (!usable(held, clock.wallClockMillis())) {
                requestFetchNowLocked(start);
            }
            while (true) {
                if (closed) {
                    return false;
                }
                if (usable(held, clock.wallClockMillis())) {
                    return true;
                }
                final long remaining = deadline - clock.monotonicNanos();
                if (remaining <= 0) {
                    return false;
                }
                try {
                    stateChanged.awaitNanos(Math.min(remaining, maxWaitSliceNanos));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops scheduled fetches, interrupts a fetch in progress and wakes every waiter. Afterwards
     * {@link #getToken()} fails with a permanent {@link TokenUnavailableException}. Idempotent. Waits at most
     * one second for the refresher thread to exit.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            cancelScheduledLocked();
            held = null;
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
        scheduler.shutdown();
        LOG.debug("token provider {} closed", name);
    }

    /**
     * @return the number of consecutive failed fetches since the last successful one
     */
    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    /**
     * The most recent failed fetch - its classification, sanitized message and time - or null when no fetch
     * has failed. Kept after the source recovers.
     */
    public FetchFailure getLastFailure() {
        return lastFailure;
    }

    /**
     * @return the wall-clock time of the last successful fetch, or {@link #NONE}
     */
    public long getLastSuccessEpochMillis() {
        return lastSuccessEpochMillis;
    }

    /**
     * @return the non-secret label this provider uses in log lines and errors
     */
    public String getName() {
        return name;
    }

    /**
     * Returns a usable token: at once when one is held, otherwise after waiting up to {@code cold_wait} for a
     * fetch.
     *
     * @return the current token, without the {@code "Bearer "} prefix
     * @throws TokenUnavailableException when no usable token arrives in time (classified as the most recent
     *                                   failed fetch, or retryable when none failed), when the calling thread
     *                                   is interrupted (retryable; the interrupt flag stays set), or when the
     *                                   provider is closed (permanent)
     */
    @Override
    public CharSequence getToken() {
        if (closed) {
            throw closedException();
        }
        final Held h = held;
        if (h != null) {
            final long wallNow = clock.wallClockMillis();
            if (usable(h, wallNow)) {
                // Warm path: a volatile read, no I/O and no lock. Only when the refresh is overdue - its
                // scheduled fetch has not run, say after the host slept through it - does a caller take the
                // lock, to start that fetch in the background.
                if (!fetchRunning && consecutiveFailures == 0
                        && (wallNow >= h.refreshAtEpochMillis || clock.monotonicNanos() - h.refreshAtNanos >= 0)) {
                    startOverdueRefresh();
                }
                return h.token;
            }
        }
        return awaitUsableToken();
    }

    /**
     * @return the current token's expiry, or {@link #NONE} when no token is held
     */
    public long getTokenExpiresAtEpochMillis() {
        final Held h = held;
        return h == null ? NONE : h.expiresAtEpochMillis;
    }

    /**
     * @return true once {@link #close()} has been called
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Forced refresh. When a server answered {@code 401} to {@code token} and that token is still the current
     * one, starts a fetch now - joining one already running, and respecting failure backoff - and waits up to
     * {@code forced_wait} for it. Ignored for any other status, for a token that has already rotated, and when
     * a forced refresh ran less than {@code forced_min_interval} ago. A forced fetch that returns the same token
     * counts as a normal success. Never throws; returns promptly when interrupted, with the flag still set.
     */
    @Override
    public void onTokenRejected(CharSequence token, int httpStatus) {
        if (httpStatus != 401 || token == null) {
            return;
        }
        lock.lock();
        try {
            if (closed) {
                return;
            }
            final Held h = held;
            if (h == null || !Chars.equals(h.token, token)) {
                return; // stale: the token has already rotated, so the caller's next pull gets the new one
            }
            final long now = clock.monotonicNanos();
            if (forcedRefreshed && now - lastForcedNanos < forcedMinIntervalNanos) {
                return;
            }
            forcedRefreshed = true;
            lastForcedNanos = now;
            LOG.info("token provider {}: the server rejected the current token with 401, refreshing early", name);
            final long target = completedFetches + 1;
            requestFetchNowLocked(now);
            final long deadline = now + forcedWaitNanos;
            while (!closed && completedFetches < target) {
                final long remaining = deadline - clock.monotonicNanos();
                if (remaining <= 0) {
                    break;
                }
                stateChanged.awaitNanos(Math.min(remaining, maxWaitSliceNanos));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        final Held h = held;
        final FetchFailure f = lastFailure;
        final StringBuilder sb = new StringBuilder("RefreshingTokenProvider{name=").append(name);
        if (h == null) {
            sb.append(", token=<none>");
        } else {
            sb.append(", token=").append(CredentialRedaction.describeToken(h.token))
                    .append(", expiresAt=").append(instant(h.expiresAtEpochMillis))
                    .append(", refreshAt=").append(instant(h.refreshAtEpochMillis));
        }
        final long success = lastSuccessEpochMillis;
        if (success != NONE) {
            sb.append(", lastSuccess=").append(instant(success));
        }
        sb.append(", consecutiveFailures=").append(consecutiveFailures);
        if (f != null) {
            sb.append(", lastFailure=").append(f);
        }
        if (closed) {
            sb.append(", closed");
        }
        return sb.append('}').toString();
    }

    private CharSequence awaitUsableToken() {
        lock.lock();
        try {
            final long start = clock.monotonicNanos();
            final long deadline = start + millisToNanos(coldWaitMillis);
            // A caller that arrives without a usable token starts one fetch - unless one is running or the
            // provider is backing off - and then waits for it or for the fetches already scheduled. It does not
            // start another each time a fetch completes: a source that keeps returning a token inside the
            // hand-out floor would otherwise be fetched in a tight loop for the whole wait.
            if (!usable(held, clock.wallClockMillis())) {
                requestFetchNowLocked(start);
            }
            while (true) {
                if (closed) {
                    throw closedException();
                }
                final Held h = held;
                if (usable(h, clock.wallClockMillis())) {
                    return h.token;
                }
                final long now = clock.monotonicNanos();
                // The earliest permitted fetch lies beyond the caller's deadline: nothing can arrive in time,
                // so say so now rather than parking the caller for nothing.
                if (!fetchRunning && inBackoffLocked(now) && backoffUntilNanos - deadline > 0) {
                    throw unavailableLocked(now, start);
                }
                final long remaining = deadline - now;
                if (remaining <= 0) {
                    throw unavailableLocked(now, start);
                }
                try {
                    stateChanged.awaitNanos(Math.min(remaining, maxWaitSliceNanos));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new TokenUnavailableException("interrupted while waiting for a token from " + name, true);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private long backoffMillis(int failures, long retryAfterMillis) {
        final int shift = Math.min(failures - 1, 40);
        long base = backoffInitialMillis << shift;
        if (base < 0 || (base >> shift) != backoffInitialMillis) {
            base = backoffMaxMillis; // overflowed
        }
        base = Math.min(base, backoffMaxMillis);
        final long half = base / 2;
        long delay = half + (long) (half * uniform());
        if (retryAfterMillis >= 0) {
            delay = Math.max(delay, Math.min(retryAfterMillis, retryAfterMaxMillis));
        }
        return delay;
    }

    private void cancelScheduledLocked() {
        scheduledSeq++;
        final ScheduledTask t = scheduledTask;
        scheduledTask = null;
        if (t != null) {
            t.cancel();
        }
    }

    private FetchFailure checkResult(ExpiringToken result, long receivedAt) {
        if (result == null) {
            return new FetchFailure(true, TokenUnavailableException.NO_RETRY_AFTER,
                    "token source returned null", receivedAt);
        }
        final String problem = ExpiringToken.describeInvalidToken(result.getToken());
        if (problem != null) {
            return new FetchFailure(true, TokenUnavailableException.NO_RETRY_AFTER,
                    "token source returned an unusable token: " + problem, receivedAt);
        }
        if (result.getExpiresAtEpochMillis() <= receivedAt) {
            return new FetchFailure(true, TokenUnavailableException.NO_RETRY_AFTER,
                    "token source returned a token that expired at " + instant(result.getExpiresAtEpochMillis())
                            + ", not after it was received at " + instant(receivedAt),
                    receivedAt);
        }
        return null;
    }

    private FetchFailure classify(Throwable t, long receivedAt) {
        if (t instanceof TokenUnavailableException) {
            final TokenUnavailableException e = (TokenUnavailableException) t;
            return new FetchFailure(e.isRetryable(), e.getRetryAfterMillis(), e.getMessage(), receivedAt);
        }
        // An unclassified failure is retryable (the specification's rule when unsure). Name its type: the
        // message alone ("connect timed out") often does not say what failed.
        final String message = t.getMessage();
        return new FetchFailure(true, TokenUnavailableException.NO_RETRY_AFTER,
                message == null ? t.getClass().getName() : t.getClass().getName() + ": " + message, receivedAt);
    }

    private TokenUnavailableException closedException() {
        return new TokenUnavailableException("token provider " + name + " is closed", false);
    }

    private boolean inBackoffLocked(long now) {
        return backoffActive && now - backoffUntilNanos < 0;
    }

    private String onFailureLocked(FetchFailure failure, long receivedNanos) {
        final int failures = consecutiveFailures + 1;
        consecutiveFailures = failures;
        lastFailure = failure;
        final long delayMillis = backoffMillis(failures, failure.retryAfterMillis);
        final long delayNanos = millisToNanos(delayMillis);
        backoffActive = true;
        backoffUntilNanos = receivedNanos + delayNanos;
        scheduleFetchLocked(delayNanos, receivedNanos);
        if (failureLogged && receivedNanos - lastFailureLogNanos < FAILURE_LOG_INTERVAL_NANOS) {
            return null;
        }
        failureLogged = true;
        lastFailureLogNanos = receivedNanos;
        return "token provider " + name + ": fetch failed [retryable=" + failure.retryable
                + ", consecutiveFailures=" + failures + ", retryInMillis=" + delayMillis + "]: " + failure.message;
    }

    private void onSuccessLocked(ExpiringToken result, long receivedAt, long receivedNanos) {
        final long refreshAt = computeRefreshAtMillis(
                receivedAt,
                result.getExpiresAtEpochMillis(),
                result.getRefreshAtEpochMillis(),
                uniform(),
                refreshMarginMillis,
                minRefreshIntervalMillis
        );
        final long delayNanos = millisToNanos(refreshAt - receivedAt);
        final int failures = consecutiveFailures;
        held = new Held(result.getToken(), result.getExpiresAtEpochMillis(), refreshAt, receivedNanos + delayNanos);
        lastSuccessEpochMillis = receivedAt;
        consecutiveFailures = 0;
        backoffActive = false;
        scheduleFetchLocked(delayNanos, receivedNanos);
        if (failures > 0) {
            LOG.info("token provider {}: fetch succeeded after {} consecutive failure(s) [token={}, expiresAt={}]",
                    name, failures, CredentialRedaction.describeToken(result.getToken()),
                    instant(result.getExpiresAtEpochMillis()));
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("token provider {}: fetched [token={}, expiresAt={}, refreshAt={}]",
                    name, CredentialRedaction.describeToken(result.getToken()),
                    instant(result.getExpiresAtEpochMillis()), instant(refreshAt));
        }
    }

    // Starts a fetch now unless one is running, one is already due, or the provider is in failure backoff.
    private void requestFetchNowLocked(long now) {
        if (closed || fetchRunning || inBackoffLocked(now)) {
            return;
        }
        if (scheduledTask != null && now - scheduledDueNanos >= 0) {
            return; // already due: the scheduler runs it as soon as it can
        }
        scheduleFetchLocked(0, now);
    }

    private void runFetch(long seq) {
        lock.lock();
        try {
            if (closed || seq != scheduledSeq || fetchRunning) {
                return; // cancelled, superseded, or a fetch is already in flight
            }
            scheduledTask = null;
            fetchRunning = true;
        } finally {
            lock.unlock();
        }
        ExpiringToken result = null;
        Throwable error = null;
        try {
            result = source.fetchToken();
        } catch (Throwable t) {
            // Every failure, an Error included, is a failed fetch to retry with backoff: rethrowing would only
            // be swallowed by the scheduler and leave the provider with no fetch scheduled, ever again.
            error = t;
        }
        final long receivedAt = clock.wallClockMillis();
        final long receivedNanos = clock.monotonicNanos();
        FetchFailure failure = error != null ? classify(error, receivedAt) : checkResult(result, receivedAt);
        if (failure != null) {
            failure = failure.sanitized();
        }
        String warning = null;
        lock.lock();
        try {
            fetchRunning = false;
            completedFetches++;
            if (!closed) {
                if (failure == null) {
                    onSuccessLocked(result, receivedAt, receivedNanos);
                } else {
                    warning = onFailureLocked(failure, receivedNanos);
                }
            }
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
        if (warning != null) {
            LOG.warn(warning);
        }
    }

    private void scheduleFetchLocked(long delayNanos, long now) {
        if (closed) {
            return;
        }
        cancelScheduledLocked();
        final long seq = scheduledSeq;
        scheduledDueNanos = now + delayNanos;
        scheduledTask = scheduler.schedule(() -> runFetch(seq), delayNanos);
    }

    private void startOverdueRefresh() {
        lock.lock();
        try {
            requestFetchNowLocked(clock.monotonicNanos());
        } finally {
            lock.unlock();
        }
    }

    private double uniform() {
        return random.getAsDouble();
    }

    private TokenUnavailableException unavailableLocked(long now, long start) {
        final FetchFailure f = consecutiveFailures > 0 ? lastFailure : null;
        final long waitedMillis = TimeUnit.NANOSECONDS.toMillis(Math.max(0, now - start));
        if (f == null) {
            return new TokenUnavailableException("no usable token from " + name + " after waiting "
                    + waitedMillis + " ms", true);
        }
        final long retryAfter = backoffActive && backoffUntilNanos - now > 0
                ? TimeUnit.NANOSECONDS.toMillis(backoffUntilNanos - now)
                : TokenUnavailableException.NO_RETRY_AFTER;
        return new TokenUnavailableException("no usable token from " + name + " after waiting " + waitedMillis
                + " ms [retryable=" + f.retryable + ", consecutiveFailures=" + consecutiveFailures + "]: "
                + f.message, f.retryable, retryAfter);
    }

    private boolean usable(Held h, long wallNow) {
        return h != null && wallNow < h.expiresAtEpochMillis - handoutFloorMillis;
    }

    /**
     * Time source. Expiry comparisons use {@link #wallClockMillis()}; delays and waits use
     * {@link #monotonicNanos()}.
     */
    public interface Clock {
        long monotonicNanos();

        long wallClockMillis();
    }

    /**
     * Runs fetches on the provider's background context. The default owns one daemon thread. A test may supply
     * its own to run fetches deterministically.
     */
    public interface Scheduler {
        /**
         * Runs {@code task} once, after {@code delayNanos} on the monotonic clock.
         */
        ScheduledTask schedule(Runnable task, long delayNanos);

        /**
         * Called once from {@link RefreshingTokenProvider#close()}: stop running tasks and interrupt a task in
         * progress.
         */
        void shutdown();
    }

    /**
     * Handle to a task submitted to a {@link Scheduler}.
     */
    public interface ScheduledTask {
        /**
         * Best-effort: a task that has already started may still run, and the provider ignores it.
         */
        void cancel();
    }

    /**
     * Builder for {@link RefreshingTokenProvider}. The defaults are the specification's (section 5.1).
     */
    public static final class Builder {
        private final TokenSource source;
        private long backoffInitialMillis = DEFAULT_BACKOFF_INITIAL_MILLIS;
        private long backoffMaxMillis = DEFAULT_BACKOFF_MAX_MILLIS;
        private Clock clock = SystemClock.INSTANCE;
        private long coldWaitMillis = DEFAULT_COLD_WAIT_MILLIS;
        private long forcedMinIntervalMillis = DEFAULT_FORCED_MIN_INTERVAL_MILLIS;
        private long forcedWaitMillis = DEFAULT_FORCED_WAIT_MILLIS;
        private long handoutFloorMillis = DEFAULT_HANDOUT_FLOOR_MILLIS;
        private long minRefreshIntervalMillis = DEFAULT_MIN_REFRESH_INTERVAL_MILLIS;
        private String name = "token-provider";
        private DoubleSupplier random = () -> ThreadLocalRandom.current().nextDouble();
        private long refreshMarginMillis = DEFAULT_REFRESH_MARGIN_MILLIS;
        private long retryAfterMaxMillis = DEFAULT_RETRY_AFTER_MAX_MILLIS;
        private Scheduler scheduler;

        private Builder(TokenSource source) {
            this.source = source;
        }

        private static long nonNegative(String name, long value) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must be >= 0: " + value);
            }
            return value;
        }

        /**
         * First retry delay after a failed fetch. Default 500 ms.
         */
        public Builder backoffInitialMillis(long millis) {
            if (millis <= 0) {
                throw new IllegalArgumentException("backoff_initial must be > 0: " + millis);
            }
            this.backoffInitialMillis = millis;
            return this;
        }

        /**
         * Largest retry delay after failed fetches. Default 60 s.
         */
        public Builder backoffMaxMillis(long millis) {
            if (millis <= 0) {
                throw new IllegalArgumentException("backoff_max must be > 0: " + millis);
            }
            this.backoffMaxMillis = millis;
            return this;
        }

        /**
         * Builds the provider and starts its first fetch in the background.
         */
        public RefreshingTokenProvider build() {
            if (backoffMaxMillis < backoffInitialMillis) {
                throw new IllegalArgumentException("backoff_max must be >= backoff_initial [backoff_max="
                        + backoffMaxMillis + ", backoff_initial=" + backoffInitialMillis + ']');
            }
            RefreshingTokenProvider provider = new RefreshingTokenProvider(this);
            provider.lock.lock();
            try {
                provider.scheduleFetchLocked(0, provider.clock.monotonicNanos());
            } finally {
                provider.lock.unlock();
            }
            return provider;
        }

        /**
         * Test seam: the clock for expiry comparisons, delays and waits.
         */
        @TestOnly
        public Builder clock(Clock clock) {
            if (clock == null) {
                throw new IllegalArgumentException("clock must not be null");
            }
            this.clock = clock;
            return this;
        }

        /**
         * The longest {@code getToken()} waits when no usable token is held. Default 30 s.
         */
        public Builder coldWaitMillis(long millis) {
            this.coldWaitMillis = nonNegative("cold_wait", millis);
            return this;
        }

        /**
         * Minimum spacing between forced refreshes. Default 30 s.
         */
        public Builder forcedMinIntervalMillis(long millis) {
            this.forcedMinIntervalMillis = nonNegative("forced_min_interval", millis);
            return this;
        }

        /**
         * The longest {@code onTokenRejected} waits for a forced refresh. Default 5 s.
         */
        public Builder forcedWaitMillis(long millis) {
            this.forcedWaitMillis = nonNegative("forced_wait", millis);
            return this;
        }

        /**
         * A token is usable only while {@code now < expires_at - handout_floor}. Default 60 s.
         */
        public Builder handoutFloorMillis(long millis) {
            this.handoutFloorMillis = nonNegative("handout_floor", millis);
            return this;
        }

        /**
         * Lower bound on the time between a fetch and the next scheduled refresh. Default 30 s.
         */
        public Builder minRefreshIntervalMillis(long millis) {
            this.minRefreshIntervalMillis = nonNegative("min_refresh_interval", millis);
            return this;
        }

        /**
         * A non-secret label for log lines, errors and the refresher thread, e.g.
         * {@code azure[resource=api://...]}. Default {@code token-provider}.
         */
        public Builder name(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name must not be empty");
            }
            this.name = name;
            return this;
        }

        /**
         * Test seam: the source of uniform samples in {@code [0, 1)} for the refresh and backoff jitter.
         */
        @TestOnly
        public Builder random(DoubleSupplier uniform) {
            if (uniform == null) {
                throw new IllegalArgumentException("random must not be null");
            }
            this.random = uniform;
            return this;
        }

        /**
         * Upper bound on how close to expiry a scheduled refresh may run. Default 5 min.
         */
        public Builder refreshMarginMillis(long millis) {
            this.refreshMarginMillis = nonNegative("refresh_margin", millis);
            return this;
        }

        /**
         * Cap applied to a source's {@code retry_after}. Default 5 min.
         */
        public Builder retryAfterMaxMillis(long millis) {
            this.retryAfterMaxMillis = nonNegative("retry_after_max", millis);
            return this;
        }

        /**
         * Test seam: where fetches run. The provider calls {@link Scheduler#shutdown()} from {@code close()}.
         */
        @TestOnly
        public Builder scheduler(Scheduler scheduler) {
            if (scheduler == null) {
                throw new IllegalArgumentException("scheduler must not be null");
            }
            this.scheduler = scheduler;
            return this;
        }
    }

    /**
     * A failed fetch: its classification, sanitized message and time. Never carries the token.
     */
    public static final class FetchFailure {
        private final long epochMillis;
        private final String message;
        private final long retryAfterMillis;
        private final boolean retryable;

        FetchFailure(boolean retryable, long retryAfterMillis, String message, long epochMillis) {
            this.retryable = retryable;
            this.retryAfterMillis = retryAfterMillis < 0 ? TokenUnavailableException.NO_RETRY_AFTER : retryAfterMillis;
            this.message = message;
            this.epochMillis = epochMillis;
        }

        /**
         * @return the wall-clock time of the failure
         */
        public long getEpochMillis() {
            return epochMillis;
        }

        /**
         * @return the failure message, sanitized and at most 256 characters
         */
        public String getMessage() {
            return message;
        }

        /**
         * @return the source's suggested wait before retrying, or {@link TokenUnavailableException#NO_RETRY_AFTER}
         */
        public long getRetryAfterMillis() {
            return retryAfterMillis;
        }

        /**
         * @return whether the source classified the failure as retryable
         */
        public boolean isRetryable() {
            return retryable;
        }

        @Override
        public String toString() {
            return "FetchFailure{retryable=" + retryable + ", at=" + instant(epochMillis) + ", message=" + message + '}';
        }

        FetchFailure sanitized() {
            final String clean = CredentialRedaction.sanitizeErrorText(message);
            return new FetchFailure(retryable, retryAfterMillis, clean == null || clean.isEmpty()
                    ? "token source failed without a message" : clean, epochMillis);
        }
    }

    private static final class DefaultScheduler implements Scheduler {
        private final ScheduledThreadPoolExecutor executor;
        private volatile Thread thread;

        DefaultScheduler() {
            final String threadName = "qdb-token-refresh-" + THREAD_IDS.incrementAndGet();
            this.executor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, threadName);
                t.setDaemon(true);
                thread = t;
                return t;
            });
            executor.setRemoveOnCancelPolicy(true);
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        }

        @Override
        public ScheduledTask schedule(Runnable task, long delayNanos) {
            final ScheduledFuture<?> future = executor.schedule(task, delayNanos, TimeUnit.NANOSECONDS);
            return () -> future.cancel(false);
        }

        @Override
        public void shutdown() {
            executor.shutdownNow();
            if (Thread.currentThread() == thread) {
                return; // closed from inside a fetch: the thread exits once this task unwinds
            }
            // Interrupt-neutral, like the other close paths in this library: a carried interrupt flag would
            // turn the bounded wait into an immediate return.
            final boolean interrupted = Thread.interrupted();
            try {
                executor.awaitTermination(CLOSE_AWAIT_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static final class Held {
        final long expiresAtEpochMillis;
        final long refreshAtEpochMillis;
        final long refreshAtNanos;
        final String token;

        Held(String token, long expiresAtEpochMillis, long refreshAtEpochMillis, long refreshAtNanos) {
            this.token = token;
            this.expiresAtEpochMillis = expiresAtEpochMillis;
            this.refreshAtEpochMillis = refreshAtEpochMillis;
            this.refreshAtNanos = refreshAtNanos;
        }
    }

    private static final class SystemClock implements Clock {
        static final SystemClock INSTANCE = new SystemClock();

        @Override
        public long monotonicNanos() {
            return System.nanoTime();
        }

        @Override
        public long wallClockMillis() {
            return System.currentTimeMillis();
        }
    }
}
