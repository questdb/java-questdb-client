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

import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-wide registry of the token providers that connect strings select with {@code token_provider}
 * (design/qwp-token-provider-spec.md, section 7.4). Every client instance built from a connect string - each
 * sender, each query client, each pooled connection - acquires a {@link Lease} when it connects and releases it
 * when it closes. All leases for the same configuration - the provider name plus its normalized parameters -
 * share one {@link RefreshingTokenProvider}, so the process runs one refresher, and makes at most one call to
 * the identity platform at a time, per configuration.
 * <p>
 * When the last lease is released the provider stays alive for the registry's linger period (60 s for the
 * global registry) and is closed only if no new lease arrives in the meantime: a pool that recycles its
 * connections, or an application that closes one sender and opens another, keeps the warm cache.
 * <p>
 * Providers that an application supplies itself never pass through here; sharing those is the application's
 * responsibility.
 */
public final class TokenProviderRegistry {
    public static final long DEFAULT_LINGER_MILLIS = 60_000;
    private static final TokenProviderRegistry GLOBAL = new TokenProviderRegistry(DEFAULT_LINGER_MILLIS);
    private static final Logger LOG = LoggerFactory.getLogger(TokenProviderRegistry.class);
    @TestOnly
    private static volatile List<TokenProviderFactory> factoriesForTesting;
    private final Map<String, Entry> entries = new HashMap<>();
    private final long lingerMillis;
    private final Object lock = new Object();
    private ScheduledThreadPoolExecutor lingerTimer;

    /**
     * Creates a private registry. Production code uses {@link #global()}; a separate instance is for tests and
     * for applications that want to scope provider sharing themselves.
     *
     * @param lingerMillis how long a provider outlives its last lease; {@code <= 0} closes it at once
     */
    public TokenProviderRegistry(long lingerMillis) {
        this.lingerMillis = lingerMillis;
    }

    /**
     * Finds the factory serving {@code name} through {@link ServiceLoader}, trying the thread context class
     * loader first and then the loader of this library.
     *
     * @return the factory, or null when none is installed
     */
    public static TokenProviderFactory findFactory(String name) {
        for (TokenProviderFactory factory : factories()) {
            if (name.equals(factory.name())) {
                return factory;
            }
        }
        return null;
    }

    /**
     * The registry every client built from a connect string uses.
     */
    public static TokenProviderRegistry global() {
        return GLOBAL;
    }

    /**
     * Test seam: replaces {@link ServiceLoader} discovery with a fixed list of factories. Pass null to restore
     * discovery.
     */
    @TestOnly
    public static void setFactoriesForTesting(List<TokenProviderFactory> factories) {
        factoriesForTesting = factories;
    }

    /**
     * The names of every installed factory, sorted: the values {@code token_provider} accepts.
     */
    public static List<String> supportedProviders() {
        TreeSet<String> names = new TreeSet<>();
        for (TokenProviderFactory factory : factories()) {
            names.add(factory.name());
        }
        return new ArrayList<>(names);
    }

    private static List<TokenProviderFactory> factories() {
        final List<TokenProviderFactory> override = factoriesForTesting;
        if (override != null) {
            return override;
        }
        final List<TokenProviderFactory> found = new ArrayList<>();
        final ClassLoader context = Thread.currentThread().getContextClassLoader();
        load(context == null ? ServiceLoader.load(TokenProviderFactory.class)
                : ServiceLoader.load(TokenProviderFactory.class, context), found);
        final ClassLoader own = TokenProviderFactory.class.getClassLoader();
        if (own != context) {
            load(ServiceLoader.load(TokenProviderFactory.class, own), found);
        }
        return found;
    }

    private static void load(ServiceLoader<TokenProviderFactory> loader, List<TokenProviderFactory> into) {
        final Iterator<TokenProviderFactory> it = loader.iterator();
        while (true) {
            final TokenProviderFactory factory;
            try {
                if (!it.hasNext()) {
                    return;
                }
                factory = it.next();
            } catch (ServiceConfigurationError e) {
                // one broken provider must not hide the others
                LOG.warn("could not load a token provider factory: {}",
                        CredentialRedaction.sanitizeErrorText(String.valueOf(e.getMessage())));
                continue;
            }
            boolean duplicate = false;
            for (int i = 0, n = into.size(); i < n; i++) {
                if (into.get(i).getClass() == factory.getClass()) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                into.add(factory);
            }
        }
    }

    /**
     * Acquires a lease on the shared provider for {@code spec}, creating the provider - and starting its first
     * fetch - when none is alive. Cancels a pending linger close.
     *
     * @throws IllegalArgumentException when the factory rejects the parameters
     */
    public Lease acquire(TokenProviderSpec spec) {
        synchronized (lock) {
            Entry entry = entries.get(spec.registryKey());
            if (entry == null || entry.provider.isClosed()) {
                TokenSource source = spec.factory().createSource(spec.params());
                if (source == null) {
                    throw new IllegalStateException("token provider factory " + spec.name() + " returned no token source");
                }
                RefreshingTokenProvider provider = RefreshingTokenProvider.builder(source).name(spec.describe()).build();
                entry = new Entry(spec.registryKey(), provider);
                entries.put(entry.key, entry);
                LOG.info("started token provider {}", entry.provider.getName());
            }
            entry.refs++;
            if (entry.lingerTask != null) {
                entry.lingerTask.cancel(false);
                entry.lingerTask = null;
            }
            return new Lease(entry);
        }
    }

    /**
     * Whether a provider for {@code spec} is alive - leased, or lingering after its last lease.
     */
    public boolean isActive(TokenProviderSpec spec) {
        synchronized (lock) {
            Entry entry = entries.get(spec.registryKey());
            return entry != null && !entry.provider.isClosed();
        }
    }

    /**
     * Number of leases currently held on the provider for {@code spec}.
     */
    public int leaseCount(TokenProviderSpec spec) {
        synchronized (lock) {
            Entry entry = entries.get(spec.registryKey());
            return entry == null ? 0 : entry.refs;
        }
    }

    private void expire(Entry entry) {
        synchronized (lock) {
            if (entry.refs > 0 || entries.get(entry.key) != entry) {
                return; // re-leased during the linger, or already replaced
            }
            entries.remove(entry.key);
            entry.lingerTask = null;
        }
        LOG.info("closing token provider {}: no client has used it for {} ms", entry.provider.getName(), lingerMillis);
        entry.provider.close();
    }

    private ScheduledThreadPoolExecutor lingerTimer() {
        if (lingerTimer == null) {
            lingerTimer = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "qdb-token-provider-registry");
                t.setDaemon(true);
                return t;
            });
            lingerTimer.setRemoveOnCancelPolicy(true);
            lingerTimer.setKeepAliveTime(5, TimeUnit.SECONDS);
            lingerTimer.allowCoreThreadTimeOut(true);
        }
        return lingerTimer;
    }

    private void release(Entry entry) {
        RefreshingTokenProvider toClose = null;
        synchronized (lock) {
            if (--entry.refs > 0) {
                return;
            }
            if (lingerMillis <= 0) {
                entries.remove(entry.key, entry);
                toClose = entry.provider;
            } else {
                entry.lingerTask = lingerTimer().schedule(() -> expire(entry), lingerMillis, TimeUnit.MILLISECONDS);
            }
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    private static final class Entry {
        final String key;
        final RefreshingTokenProvider provider;
        ScheduledFuture<?> lingerTask;
        int refs;

        Entry(String key, RefreshingTokenProvider provider) {
            this.key = key;
            this.provider = provider;
        }
    }

    /**
     * One client's hold on a shared provider. Release it exactly when the client closes; releasing twice is
     * harmless.
     */
    public final class Lease implements QuietCloseable {
        private final Entry entry;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(Entry entry) {
            this.entry = entry;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release(entry);
            }
        }

        /**
         * The shared provider. Do not close it: release the lease instead.
         */
        public RefreshingTokenProvider provider() {
            return entry.provider;
        }

        @Override
        public String toString() {
            return "TokenProviderRegistry.Lease{" + entry.provider.getName() + (released.get() ? ", released}" : "}");
        }
    }
}
