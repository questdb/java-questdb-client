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

import io.questdb.client.cutlass.auth.ExpiringToken;
import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenSource;
import org.junit.Assert;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Deterministic fixtures for the dynamic-credential conformance tests (design/qwp-token-provider-spec.md, section
 * 10): a fake clock, a scheduler the test drives by hand, and a scripted token source.
 */
public final class TokenTestKit {

    private TokenTestKit() {
    }

    public static void await(BooleanSupplier condition, long timeoutMillis, String what) {
        final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - deadline > 0) {
                Assert.fail("timed out after " + timeoutMillis + " ms waiting for " + what);
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted while waiting for " + what);
            }
        }
    }

    /**
     * A wall clock and a monotonic clock that only move when the test moves them. {@link #advanceMillis(long)}
     * moves both; {@link #jumpWallMillis(long)} moves the wall clock alone, the way a suspended host or an NTP
     * step does.
     */
    public static final class FakeClock implements RefreshingTokenProvider.Clock {
        private final AtomicLong mono = new AtomicLong(1_000_000_000L);
        private final AtomicLong wall;

        public FakeClock(long wallStartMillis) {
            this.wall = new AtomicLong(wallStartMillis);
        }

        public void advanceMillis(long millis) {
            wall.addAndGet(millis);
            mono.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        }

        public void jumpWallMillis(long millis) {
            wall.addAndGet(millis);
        }

        @Override
        public long monotonicNanos() {
            return mono.get();
        }

        @Override
        public long wallClockMillis() {
            return wall.get();
        }
    }

    /**
     * Runs nothing on its own: the test picks the next task, optionally advances the fake clock to its due time,
     * and runs it on the test thread. Tasks run outside the scheduler's monitor, because the provider calls
     * {@link #schedule} while holding its own lock.
     */
    public static final class ManualScheduler implements RefreshingTokenProvider.Scheduler {
        private final FakeClock clock;
        private final List<Task> tasks = new ArrayList<>();
        private volatile boolean shutdown;

        public ManualScheduler(FakeClock clock) {
            this.clock = clock;
        }

        /**
         * Advances the clock to the next task's due time (if it lies ahead) and runs it.
         *
         * @return the task that ran
         */
        public Task advanceAndRunNext() {
            Task t = takeNext();
            Assert.assertNotNull("no fetch is scheduled", t);
            long ahead = t.dueNanos - clock.monotonicNanos();
            if (ahead > 0) {
                clock.advanceMillis(TimeUnit.NANOSECONDS.toMillis(ahead));
            }
            t.runnable.run();
            return t;
        }

        public boolean isShutdown() {
            return shutdown;
        }

        /**
         * The earliest live task, without removing it, or null.
         */
        public synchronized Task peek() {
            Task best = null;
            for (int i = 0, n = tasks.size(); i < n; i++) {
                Task t = tasks.get(i);
                if (!t.cancelled && (best == null || t.dueNanos - best.dueNanos < 0)) {
                    best = t;
                }
            }
            return best;
        }

        /**
         * Runs the earliest live task now, without moving the clock.
         */
        public Task runNext() {
            Task t = takeNext();
            Assert.assertNotNull("no fetch is scheduled", t);
            t.runnable.run();
            return t;
        }

        @Override
        public synchronized Task schedule(Runnable task, long delayNanos) {
            Task t = new Task(task, clock.monotonicNanos() + delayNanos, delayNanos);
            tasks.add(t);
            return t;
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        private synchronized Task takeNext() {
            Task t = peek();
            if (t != null) {
                tasks.remove(t);
            }
            return t;
        }

        public static final class Task implements RefreshingTokenProvider.ScheduledTask {
            public final long delayNanos;
            final long dueNanos;
            final Runnable runnable;
            volatile boolean cancelled;

            Task(Runnable runnable, long dueNanos, long delayNanos) {
                this.runnable = runnable;
                this.dueNanos = dueNanos;
                this.delayNanos = delayNanos;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }

            public long delayMillis() {
                return TimeUnit.NANOSECONDS.toMillis(delayNanos);
            }
        }
    }

    /**
     * A token source that replays a script of results. Each step either returns a token, throws, or computes
     * its result from the clock at call time. When the script runs out the last step repeats. An optional gate
     * blocks every fetch until the test opens it.
     */
    public static final class ScriptedSource implements TokenSource {
        private final AtomicInteger calls = new AtomicInteger();
        private final ArrayDeque<Supplier<ExpiringToken>> script = new ArrayDeque<>();
        private volatile CountDownLatch gate;
        private Supplier<ExpiringToken> last;

        public int calls() {
            return calls.get();
        }

        @Override
        public ExpiringToken fetchToken() {
            calls.incrementAndGet();
            CountDownLatch g = gate;
            if (g != null) {
                try {
                    g.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("fetch interrupted");
                }
            }
            Supplier<ExpiringToken> step;
            synchronized (this) {
                step = script.poll();
                if (step == null) {
                    step = last;
                } else {
                    last = step;
                }
            }
            Assert.assertNotNull("the source was called with nothing scripted", step);
            return step.get();
        }

        public void openGate() {
            CountDownLatch g = gate;
            gate = null;
            if (g != null) {
                g.countDown();
            }
        }

        public void setGate() {
            gate = new CountDownLatch(1);
        }

        public ScriptedSource then(Supplier<ExpiringToken> step) {
            synchronized (this) {
                script.add(step);
            }
            return this;
        }

        public ScriptedSource thenThrow(RuntimeException e) {
            return then(() -> {
                throw e;
            });
        }

        public ScriptedSource thenToken(String token, long expiresAtEpochMillis) {
            return then(() -> new ExpiringToken(token, expiresAtEpochMillis));
        }
    }
}
