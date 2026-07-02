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

package io.questdb.client;

import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.impl.QuestDBImpl;
import org.jetbrains.annotations.TestOnly;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Builder for {@link QuestDB}. Most callers use {@link QuestDB#connect(CharSequence)};
 * this builder adds pool sizing, idle/lifetime knobs, the acquire timeout, and
 * the ingest callbacks.
 * <p>
 * To tolerate the server being down at startup, set {@code lazy_connect=true}
 * in the config: the ingest side connects asynchronously (writes buffer until
 * the wire is up) and the read pool connects lazily on first use. Reads stay
 * fully enabled -- they just connect once the server is available.
 * <p>
 * One configuration string describes the whole QuestDB cluster (see
 * {@link #fromConfig}): list every node in a single {@code addr} server list and
 * both the ingest and query pools connect across it. The schema must be
 * {@code ws} or {@code wss} (QWP over WebSocket). A pool key (e.g.
 * {@code sender_pool_min}) may be carried in the connect string or set with an
 * explicit builder call; an explicit call always wins.
 */
public final class QuestDBBuilder {

    static final long DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 5_000;
    static final long DEFAULT_HOUSEKEEPER_INTERVAL_MILLIS = 5_000;
    static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 60_000;
    static final long DEFAULT_MAX_LIFETIME_MILLIS = 30 * 60_000L;
    static final int DEFAULT_POOL_MAX = 4;
    static final int DEFAULT_POOL_MIN = 1;
    static final long DEFAULT_QUERY_CLOSE_TIMEOUT_MILLIS = 5_000;

    // Every valid pool value is >= 0, so -1 unambiguously marks "not set
    // explicitly". The public pool setters are the only writers of these
    // fields, so field != UNSET is exactly the "set explicitly" bit.
    private static final int UNSET = -1;

    private long acquireTimeoutMillis = UNSET;
    // Optional ingest-side async callbacks. Null -> each pooled Sender uses its
    // loud-not-silent default. Applied to every Sender the pool builds.
    private SenderConnectionListener connectionListener;
    private BackgroundDrainerListener drainerListener;
    private SenderErrorHandler errorHandler;
    private long housekeeperIntervalMillis = UNSET;
    private String config;
    private long idleTimeoutMillis = UNSET;
    private long maxLifetimeMillis = UNSET;
    private long queryCloseTimeoutMillis = UNSET;
    private int queryPoolMax = UNSET;
    private int queryPoolMin = UNSET;
    private int senderPoolMax = UNSET;
    private int senderPoolMin = UNSET;

    QuestDBBuilder() {
    }

    /**
     * Maximum time {@link QuestDB#borrowSender()} and {@link Query#submit()}
     * block when the pool is exhausted (every slot in use and {@code max}
     * already reached) before throwing. Defaults to 5000ms.
     */
    public QuestDBBuilder acquireTimeoutMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("acquireTimeoutMillis must be >= 0");
        }
        this.acquireTimeoutMillis = millis;
        return this;
    }

    /**
     * Maximum time {@link Query#close()} waits for an in-flight query to drain
     * (after issuing a cancel) before discarding the leased query client and
     * letting the pool grow a fresh one. Bounds the close of a handle whose
     * {@code submit()} is still running -- e.g. when the caller's own
     * {@code await(timeout)} expired and they gave up. Defaults to 5000ms.
     */
    public QuestDBBuilder queryCloseTimeoutMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("queryCloseTimeoutMillis must be >= 0");
        }
        this.queryCloseTimeoutMillis = millis;
        return this;
    }

    /**
     * Sets the async connection-event listener applied to every pooled ingest
     * {@link Sender}. The listener observes connect / disconnect / failover
     * transitions across the whole sender pool; events are delivered on the
     * senders' I/O threads, so the listener must be thread-safe and must not
     * block. Pass {@code null} (the default) to keep each sender's
     * loud-not-silent default listener.
     *
     * @param listener the shared connection listener, or {@code null} for the default
     * @return this instance for method chaining
     */
    public QuestDBBuilder connectionListener(SenderConnectionListener listener) {
        this.connectionListener = listener;
        return this;
    }

    /**
     * Sets the background orphan-slot drainer listener applied to every pooled
     * ingest {@link Sender}. The listener observes the background drainer
     * events of every sender the pool builds: durable-ack capability-gap
     * retries, transient all-replica failover windows, and the eventual
     * escalation to a {@code .failed} sentinel. Events are delivered on the
     * drainers' own threads, so the listener must be thread-safe and must not
     * block. Only meaningful when the configuration enables
     * {@code drain_orphans}. Pass {@code null} (the default) to keep the
     * drainers' default (no listener).
     *
     * @param listener the shared drainer listener, or {@code null} for the default
     * @return this instance for method chaining
     */
    public QuestDBBuilder drainerListener(BackgroundDrainerListener listener) {
        this.drainerListener = listener;
        return this;
    }

    /**
     * Sets the async error handler applied to every pooled ingest
     * {@link Sender}. The handler receives terminal/async ingest errors
     * (terminal upgrade failures, write errors)
     * from across the whole sender pool; notifications are delivered on the
     * senders' I/O threads, so the handler must be thread-safe and must not
     * block. Pass {@code null} (the default) to keep each sender's
     * loud-not-silent default handler.
     *
     * @param handler the shared error handler, or {@code null} for the default
     * @return this instance for method chaining
     */
    public QuestDBBuilder errorHandler(SenderErrorHandler handler) {
        this.errorHandler = handler;
        return this;
    }

    /**
     * Builds the {@link QuestDB} handle. Validates both connect strings up
     * front -- so a malformed config fails here even when both pools have
     * {@code min == 0} and nothing connects -- then eagerly creates {@code min}
     * connections in each pool; further slots are allocated lazily up to
     * {@code max} when load demands and reaped back to {@code min} when
     * idle.
     * <p>
     * Non-blocking on startup recovery: when store-and-forward is enabled,
     * unacked data a previous run left in this pool's managed slots is
     * recovered on a background housekeeper thread shortly after this method
     * returns -- so {@code build()} does not block on a slow or
     * reachable-but-not-acking server. The recovered data is durable on disk
     * and is delivered once the server acks; until then it stays preserved.
     */
    public QuestDB build() {
        if (config == null) {
            throw new IllegalStateException("configuration is required; call fromConfig()");
        }
        ConfigString cs = ConfigString.parse(config);
        ConfigView view = new ConfigView(cs);
        // Validate the single cluster config exactly as both pools will, but
        // without connecting: the full Sender parse plus validateParameters
        // (ingress value keys are registry-STRING, so only the real parse
        // validates their values), then the typed egress validateConfig. Each
        // side applies the keys it owns and silently ignores the rest, so one
        // string drives both. A malformed config therefore fails here even when
        // a pool min is 0 and nothing connects.
        Sender.LineSenderBuilder.validateWsConfigString(config);
        QwpQueryClient.validateConfig(view, "wss".equals(cs.schema()));

        // lazy_connect: tolerate a down server at startup without disabling
        // reads. The ingest side connects asynchronously (writes buffer until the
        // wire is up) and the read pool defaults to min=0 -- it connects lazily
        // on the first query once the server is up. Reads stay enabled.
        boolean lazyConnect = view.getBool("lazy_connect", false);
        String ingestConfig = config;
        if (lazyConnect) {
            ingestConfig = resolveLazyConnect(view);
        }

        resolvePoolInt(senderPoolMin, "sender_pool_min", view, DEFAULT_POOL_MIN, this::senderPoolMin);
        resolvePoolInt(senderPoolMax, "sender_pool_max", view, DEFAULT_POOL_MAX, this::senderPoolMax);
        // lazy_connect makes the read pool lazy (min=0); without it the default min is 1.
        resolvePoolInt(queryPoolMin, "query_pool_min", view, lazyConnect ? 0 : DEFAULT_POOL_MIN, this::queryPoolMin);
        resolvePoolInt(queryPoolMax, "query_pool_max", view, DEFAULT_POOL_MAX, this::queryPoolMax);
        resolvePoolLong(acquireTimeoutMillis, "acquire_timeout_ms", view, DEFAULT_ACQUIRE_TIMEOUT_MILLIS, this::acquireTimeoutMillis);
        resolvePoolLong(queryCloseTimeoutMillis, "query_close_timeout_ms", view, DEFAULT_QUERY_CLOSE_TIMEOUT_MILLIS, this::queryCloseTimeoutMillis);
        resolvePoolLong(idleTimeoutMillis, "idle_timeout_ms", view, DEFAULT_IDLE_TIMEOUT_MILLIS, this::idleTimeoutMillis);
        resolvePoolLong(maxLifetimeMillis, "max_lifetime_ms", view, DEFAULT_MAX_LIFETIME_MILLIS, this::maxLifetimeMillis);
        resolvePoolLong(housekeeperIntervalMillis, "housekeeper_interval_ms", view, DEFAULT_HOUSEKEEPER_INTERVAL_MILLIS, this::housekeeperIntervalMillis);

        return new QuestDBImpl(
                ingestConfig,
                config,
                senderPoolMin,
                senderPoolMax,
                queryPoolMin,
                queryPoolMax,
                acquireTimeoutMillis,
                idleTimeoutMillis,
                maxLifetimeMillis,
                housekeeperIntervalMillis,
                queryCloseTimeoutMillis,
                errorHandler,
                connectionListener,
                drainerListener
        );
    }

    // Validates the lazy_connect contract and returns the ingest config to use:
    // the original string with a non-blocking async initial connect injected
    // when the user did not set one. lazy_connect requires BOTH sides to start
    // non-blocking, so an explicit knob that forces a blocking / fail-fast
    // startup is a configuration conflict and is rejected with a clear remedy.
    private String resolveLazyConnect(ConfigView view) {
        // (1) ingest side: only initial_connect_retry=async is non-blocking;
        // off/false/on/true/sync all block or fail-fast at startup.
        String mode = view.getStr("initial_connect_retry");
        if (mode != null && !"async".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException(
                    "conflicting configuration: lazy_connect=true needs a non-blocking startup, but "
                            + "initial_connect_retry=" + mode + " makes the initial connect block / fail-fast. "
                            + "Resolve by removing initial_connect_retry (lazy_connect implies "
                            + "initial_connect_retry=async) or setting initial_connect_retry=async.");
        }
        // (2) read side: lazy_connect requires query_pool_min=0 so the read pool
        // does not eagerly fail-fast at startup. An explicit query_pool_min > 0
        // (builder call or connect string) contradicts that.
        int explicitQueryMin;
        if (queryPoolMin != UNSET) {
            explicitQueryMin = queryPoolMin; // explicit builder call
        } else if (view.has("query_pool_min")) {
            explicitQueryMin = view.getInt("query_pool_min", UNSET); // connect string
        } else {
            explicitQueryMin = 0; // unset -> lazy default of 0
        }
        if (explicitQueryMin > 0) {
            throw new IllegalArgumentException(
                    "conflicting configuration: lazy_connect=true needs query_pool_min=0 (the read pool "
                            + "connects lazily on first use and must not fail-fast at startup), but query_pool_min="
                            + explicitQueryMin + " was set. Resolve by removing query_pool_min (lazy_connect "
                            + "defaults it to 0) or setting query_pool_min=0.");
        }
        // No explicit initial_connect_retry -> inject async so the ingest build
        // is non-blocking. An explicit async needs no injection.
        return mode == null ? withDefaultAsyncConnect(config) : config;
    }

    /**
     * Sets the single configuration string for the whole QuestDB cluster --
     * used for both ingest and egress. List every cluster node in one
     * {@code addr} (comma-separated, or by repeating the key); the ingest and
     * query pools each connect across that one server list. The schema must be
     * {@code ws} or {@code wss}.
     */
    public QuestDBBuilder fromConfig(CharSequence configurationString) {
        requireWebSocketSchema(configurationString, "cluster");
        this.config = configurationString.toString();
        return this;
    }

    /**
     * Sweep interval for the daemon housekeeper that reaps idle and over-age
     * pool slots. Defaults to 5000ms. Reduce if you set very short
     * {@link #idleTimeoutMillis} values; otherwise the default is fine.
     */
    public QuestDBBuilder housekeeperIntervalMillis(long millis) {
        if (millis < 100) {
            throw new IllegalArgumentException("housekeeperIntervalMillis must be >= 100");
        }
        this.housekeeperIntervalMillis = millis;
        return this;
    }

    /**
     * How long a connection may remain idle in the pool before the
     * housekeeper closes it. {@code minSize} is always respected -- the pool
     * never shrinks below it. Defaults to 60000ms.
     */
    public QuestDBBuilder idleTimeoutMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("idleTimeoutMillis must be >= 0");
        }
        this.idleTimeoutMillis = millis == 0 ? Long.MAX_VALUE : millis;
        return this;
    }

    /**
     * Maximum age of a pooled connection before the housekeeper recycles it
     * (next time it is idle). Useful for picking up DNS / load-balancer
     * changes and bounding leaked server state. Defaults to 30 minutes.
     */
    public QuestDBBuilder maxLifetimeMillis(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("maxLifetimeMillis must be >= 0");
        }
        this.maxLifetimeMillis = millis == 0 ? Long.MAX_VALUE : millis;
        return this;
    }

    /**
     * Maximum query-pool size. Defaults to 4.
     */
    public QuestDBBuilder queryPoolMax(int max) {
        if (max < 1) {
            throw new IllegalArgumentException("queryPoolMax must be >= 1");
        }
        this.queryPoolMax = max;
        return this;
    }

    /**
     * Minimum query-pool size (always kept warm). Defaults to 1. Set to 0
     * to allow the pool to drain fully when idle.
     */
    public QuestDBBuilder queryPoolMin(int min) {
        if (min < 0) {
            throw new IllegalArgumentException("queryPoolMin must be >= 0");
        }
        this.queryPoolMin = min;
        return this;
    }

    /**
     * Fixed query-pool size shortcut: equivalent to
     * {@code queryPoolMin(size).queryPoolMax(size)}. Eager allocation,
     * no growth or reaping -- matches the original (non-elastic) behavior.
     */
    public QuestDBBuilder queryPoolSize(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("queryPoolSize must be >= 1");
        }
        this.queryPoolMin = size;
        this.queryPoolMax = size;
        return this;
    }

    /**
     * Maximum sender-pool size. Defaults to 4.
     */
    public QuestDBBuilder senderPoolMax(int max) {
        if (max < 1) {
            throw new IllegalArgumentException("senderPoolMax must be >= 1");
        }
        this.senderPoolMax = max;
        return this;
    }

    /**
     * Minimum sender-pool size (always kept warm). Defaults to 1. Set to 0
     * to allow the pool to drain fully when idle.
     */
    public QuestDBBuilder senderPoolMin(int min) {
        if (min < 0) {
            throw new IllegalArgumentException("senderPoolMin must be >= 0");
        }
        this.senderPoolMin = min;
        return this;
    }

    /**
     * Fixed sender-pool size shortcut: equivalent to
     * {@code senderPoolMin(size).senderPoolMax(size)}. Eager allocation,
     * no growth or reaping -- matches the original (non-elastic) behavior.
     */
    public QuestDBBuilder senderPoolSize(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("senderPoolSize must be >= 1");
        }
        this.senderPoolMin = size;
        this.senderPoolMax = size;
        return this;
    }

    /**
     * Snapshot of the resolved pool config, keyed by connect-string key name.
     * Valid after {@link #build()} has run pool-key resolution. Drives the
     * per-key "honored" guard test.
     */
    @TestOnly
    public java.util.Map<String, Object> poolConfigSnapshotForTest() {
        java.util.Map<String, Object> m = new java.util.HashMap<>();
        m.put("sender_pool_min", senderPoolMin);
        m.put("sender_pool_max", senderPoolMax);
        m.put("query_pool_min", queryPoolMin);
        m.put("query_pool_max", queryPoolMax);
        m.put("acquire_timeout_ms", acquireTimeoutMillis);
        m.put("query_close_timeout_ms", queryCloseTimeoutMillis);
        m.put("idle_timeout_ms", idleTimeoutMillis);
        m.put("max_lifetime_ms", maxLifetimeMillis);
        m.put("housekeeper_interval_ms", housekeeperIntervalMillis);
        return m;
    }

    // Inject a non-blocking async initial connect right after the schema
    // separator so lazy_connect's build never blocks or fail-fast on a down
    // server. Only used when the user set no initial_connect_retry of their own
    // (resolveLazyConnect rejects an explicit blocking mode rather than silently
    // overriding it), so placement is immaterial -- there is no competing value.
    private static String withDefaultAsyncConnect(String config) {
        int sep = config.indexOf("::");
        // sep >= 0: fromConfig() validated a ws/wss schema, so "::" is present.
        return config.substring(0, sep + 2) + "initial_connect_retry=async;" + config.substring(sep + 2);
    }

    private static void requireWebSocketSchema(CharSequence config, String role) {
        String schema = ConfigString.parse(config).schema();
        if (!"ws".equals(schema) && !"wss".equals(schema)) {
            throw new IllegalArgumentException(
                    role + " configuration must use the ws or wss schema; got: " + schema);
        }
    }

    private void resolvePoolInt(int current, String key, ConfigView view, int dflt, IntConsumer setter) {
        if (current != UNSET) {
            return; // explicit builder call wins
        }
        setter.accept(view.has(key) ? view.getInt(key, UNSET) : dflt);
    }

    private void resolvePoolLong(long current, String key, ConfigView view, long dflt, LongConsumer setter) {
        if (current != UNSET) {
            return; // explicit builder call wins
        }
        setter.accept(view.has(key) ? view.getLong(key, UNSET) : dflt);
    }
}
