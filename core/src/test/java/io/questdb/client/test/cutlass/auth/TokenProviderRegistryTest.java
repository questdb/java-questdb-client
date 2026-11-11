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

import io.questdb.client.cutlass.auth.RefreshingTokenProvider;
import io.questdb.client.cutlass.auth.TokenProviderRegistry;
import io.questdb.client.cutlass.auth.TokenProviderSpec;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TokenProviderRegistryTest {
    private TestTokenProviderFactory factory;

    @Before
    public void setUp() {
        factory = new TestTokenProviderFactory("vault");
        TestTokenProviderFactory.install(factory);
    }

    @After
    public void tearDown() {
        TestTokenProviderFactory.uninstall();
    }

    @Test(timeout = 30_000)
    public void testSupersededExpirationDoesNotCloseIdleProvider() throws Exception {
        assertSupersededExpirationDoesNotCloseProvider(false);
    }

    @Test(timeout = 30_000)
    public void testSupersededExpirationDoesNotCloseLeasedProvider() throws Exception {
        assertSupersededExpirationDoesNotCloseProvider(true);
    }

    private void assertSupersededExpirationDoesNotCloseProvider(boolean hasActiveLease) throws Exception {
        PausingLingerTimer timer = new PausingLingerTimer();
        TokenProviderRegistry registry = new TokenProviderRegistry(TokenProviderRegistry.DEFAULT_LINGER_MILLIS, timer);
        TokenProviderSpec spec = TokenProviderSpec.parse(new ConfigView(ConfigString.parse(
                "wss::addr=localhost:9000;token_provider=vault;")), true);
        try (TokenProviderRegistry.Lease first = registry.acquire(spec)) {
            RefreshingTokenProvider provider = first.provider();
            try {
                Assert.assertTrue(provider.awaitReady(10_000));
                first.close();
                PausedExpiration superseded = timer.nextExpiration();
                try (TokenProviderRegistry.Lease second = registry.acquire(spec)) {
                    Assert.assertSame(provider, second.provider());
                    Assert.assertTrue("acquiring a lease cancels the already-started expiration",
                            superseded.future.isCancelled());
                    if (!hasActiveLease) {
                        second.close();
                    }
                    superseded.finish();
                    Assert.assertFalse("the old callback must not close the re-leased provider", provider.isClosed());
                    Assert.assertTrue(registry.isActive(spec));
                    Assert.assertEquals(hasActiveLease ? 1 : 0, registry.leaseCount(spec));
                    Assert.assertEquals("the warm provider must not need replacement", 1, factory.created.get());
                }
                PausedExpiration current = timer.nextExpiration();
                Assert.assertFalse("the old callback must leave the current expiration intact", current.future.isCancelled());
                current.finish();
                Assert.assertTrue("the current callback closes the idle provider", provider.isClosed());
                Assert.assertFalse(registry.isActive(spec));
            } finally {
                provider.close();
            }
        } finally {
            timer.shutdownNow();
            Assert.assertTrue("the linger executor must stop", timer.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static final class PausedExpiration implements Runnable {
        private final Runnable command;
        private final CountDownLatch finished = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final CountDownLatch started = new CountDownLatch(1);
        private volatile Throwable failure;
        private ScheduledFuture<?> future;

        private PausedExpiration(Runnable command) {
            this.command = command;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                proceed.await();
                command.run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                failure = e;
            } finally {
                finished.countDown();
            }
        }

        private void finish() throws InterruptedException {
            proceed.countDown();
            Assert.assertTrue("the linger callback must finish", finished.await(10, TimeUnit.SECONDS));
            if (failure != null) {
                throw new AssertionError("the linger callback failed", failure);
            }
        }
    }

    private static final class PausingLingerTimer extends ScheduledThreadPoolExecutor {
        private final BlockingQueue<PausedExpiration> expirations = new LinkedBlockingQueue<>();

        private PausingLingerTimer() {
            super(1);
            setRemoveOnCancelPolicy(true);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            // Drive deadlines explicitly, pausing each real executor task after it starts. cancel(false)
            // cannot stop this already-running task from reaching expire() when the test releases it.
            PausedExpiration expiration = new PausedExpiration(command);
            expiration.future = super.schedule(expiration, 0, TimeUnit.NANOSECONDS);
            expirations.add(expiration);
            return expiration.future;
        }

        private PausedExpiration nextExpiration() throws InterruptedException {
            PausedExpiration expiration = expirations.poll(10, TimeUnit.SECONDS);
            Assert.assertNotNull("a linger expiration must be scheduled", expiration);
            Assert.assertTrue("the linger task must start", expiration.started.await(10, TimeUnit.SECONDS));
            return expiration;
        }
    }
}
