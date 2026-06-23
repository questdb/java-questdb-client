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
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.impl.QuestDBImpl;
import io.questdb.client.impl.Side;
import org.jetbrains.annotations.TestOnly;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Builder for {@link QuestDB}. Most callers use {@link QuestDB#connect(CharSequence)};
 * this builder is for pool sizing, idle/lifetime knobs, acquire timeout,
 * and the case where ingest and egress configs differ.
 * <p>
 * Both configs must use the {@code ws} or {@code wss} schema (QWP over
 * WebSocket). A pool key (e.g. {@code sender_pool_min}) may be carried in the
 * connect string or set with an explicit builder call; an explicit call always
 * wins. When both connect strings carry the same pool key with different values,
 * {@link #build()} fails.
 */
public final class QuestDBBuilder {

    static final long DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 5_000;
    static final long DEFAULT_HOUSEKEEPER_INTERVAL_MILLIS = 5_000;
    static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 60_000;
    static final long DEFAULT_MAX_LIFETIME_MILLIS = 30 * 60_000L;
    static final int DEFAULT_POOL_MAX = 4;
    static final int DEFAULT_POOL_MIN = 1;

    // Every valid pool value is >= 0, so -1 unambiguously marks "not set
    // explicitly". The public pool setters are the only writers of these
    // fields, so field != UNSET is exactly the "set explicitly" bit.
    private static final int UNSET = -1;

    private long acquireTimeoutMillis = UNSET;
    private long housekeeperIntervalMillis = UNSET;
    private long idleTimeoutMillis = UNSET;
    private String ingestConfig;
    private long maxLifetimeMillis = UNSET;
    private String queryConfig;
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
     * Builds the {@link QuestDB} handle. Validates both connect strings up
     * front -- so a malformed config fails here even when both pools have
     * {@code min == 0} and nothing connects -- then eagerly creates {@code min}
     * connections in each pool; further slots are allocated lazily up to
     * {@code max} when load demands and reaped back to {@code min} when idle.
     */
    public QuestDB build() {
        if (ingestConfig == null) {
            throw new IllegalStateException("ingest configuration is required; call fromConfig() or ingestConfig()");
        }
        if (queryConfig == null) {
            throw new IllegalStateException("query configuration is required; call fromConfig() or queryConfig()");
        }
        ConfigString ingestCs = ConfigString.parse(ingestConfig);
        ConfigString queryCs = ConfigString.parse(queryConfig);
        ConfigView ingestView = new ConfigView(ingestCs, Side.INGRESS);
        ConfigView queryView = new ConfigView(queryCs, Side.EGRESS);
        // Validate both connect strings exactly as the pools will, but without
        // connecting. The ingest string runs the full Sender parse plus
        // validateParameters -- ingress value keys are registry-STRING, so only
        // the real parse validates their values. The egress string runs the
        // typed validateConfig. A malformed config therefore fails here even
        // when a pool min is 0 and nothing connects.
        Sender.LineSenderBuilder.validateWsConfigString(ingestConfig);
        QwpQueryClient.validateConfig(queryView, "wss".equals(queryCs.schema()));

        // getInt/getLong ignore the view's side, so the INGRESS/EGRESS views
        // also serve the POOL reads.
        resolvePoolInt(senderPoolMin, "sender_pool_min", ingestView, queryView, DEFAULT_POOL_MIN, this::senderPoolMin);
        resolvePoolInt(senderPoolMax, "sender_pool_max", ingestView, queryView, DEFAULT_POOL_MAX, this::senderPoolMax);
        resolvePoolInt(queryPoolMin, "query_pool_min", ingestView, queryView, DEFAULT_POOL_MIN, this::queryPoolMin);
        resolvePoolInt(queryPoolMax, "query_pool_max", ingestView, queryView, DEFAULT_POOL_MAX, this::queryPoolMax);
        resolvePoolLong(acquireTimeoutMillis, "acquire_timeout_ms", ingestView, queryView, DEFAULT_ACQUIRE_TIMEOUT_MILLIS, this::acquireTimeoutMillis);
        resolvePoolLong(idleTimeoutMillis, "idle_timeout_ms", ingestView, queryView, DEFAULT_IDLE_TIMEOUT_MILLIS, this::idleTimeoutMillis);
        resolvePoolLong(maxLifetimeMillis, "max_lifetime_ms", ingestView, queryView, DEFAULT_MAX_LIFETIME_MILLIS, this::maxLifetimeMillis);
        resolvePoolLong(housekeeperIntervalMillis, "housekeeper_interval_ms", ingestView, queryView, DEFAULT_HOUSEKEEPER_INTERVAL_MILLIS, this::housekeeperIntervalMillis);

        return new QuestDBImpl(
                ingestConfig,
                queryConfig,
                senderPoolMin,
                senderPoolMax,
                queryPoolMin,
                queryPoolMax,
                acquireTimeoutMillis,
                idleTimeoutMillis,
                maxLifetimeMillis,
                housekeeperIntervalMillis
        );
    }

    /**
     * Sets a single configuration string used for both ingest and egress. The
     * schema must be {@code ws} or {@code wss}.
     */
    public QuestDBBuilder fromConfig(CharSequence configurationString) {
        requireWebSocketSchema(configurationString, "connection");
        String s = configurationString.toString();
        this.ingestConfig = s;
        this.queryConfig = s;
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
     * Sets the ingest-side configuration. The schema must be {@code ws} or
     * {@code wss}.
     */
    public QuestDBBuilder ingestConfig(CharSequence configurationString) {
        requireWebSocketSchema(configurationString, "ingest");
        this.ingestConfig = configurationString.toString();
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
     * Sets the query-side configuration. The schema must be {@code ws} or
     * {@code wss}.
     */
    public QuestDBBuilder queryConfig(CharSequence configurationString) {
        requireWebSocketSchema(configurationString, "query");
        this.queryConfig = configurationString.toString();
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
        m.put("idle_timeout_ms", idleTimeoutMillis);
        m.put("max_lifetime_ms", maxLifetimeMillis);
        m.put("housekeeper_interval_ms", housekeeperIntervalMillis);
        return m;
    }

    private static void requireWebSocketSchema(CharSequence config, String role) {
        String schema = ConfigString.parse(config).schema();
        if (!"ws".equals(schema) && !"wss".equals(schema)) {
            throw new IllegalArgumentException(
                    role + " configuration must use the ws or wss schema; got: " + schema);
        }
    }

    private void resolvePoolInt(int current, String key, ConfigView ingest, ConfigView query, int dflt, IntConsumer setter) {
        if (current != UNSET) {
            return; // explicit builder call wins; skip the conflict check
        }
        boolean inIngest = ingest.has(key);
        boolean inQuery = query.has(key);
        int value;
        if (inIngest && inQuery) {
            int vi = ingest.getInt(key, UNSET);
            int vq = query.getInt(key, UNSET);
            if (vi != vq) {
                throw new IllegalArgumentException(
                        "conflicting pool config: " + key + " (ingest=" + vi + ", query=" + vq + ")");
            }
            value = vi;
        } else if (inIngest) {
            value = ingest.getInt(key, UNSET);
        } else if (inQuery) {
            value = query.getInt(key, UNSET);
        } else {
            value = dflt;
        }
        setter.accept(value);
    }

    private void resolvePoolLong(long current, String key, ConfigView ingest, ConfigView query, long dflt, LongConsumer setter) {
        if (current != UNSET) {
            return; // explicit builder call wins; skip the conflict check
        }
        boolean inIngest = ingest.has(key);
        boolean inQuery = query.has(key);
        long value;
        if (inIngest && inQuery) {
            long vi = ingest.getLong(key, UNSET);
            long vq = query.getLong(key, UNSET);
            if (vi != vq) {
                throw new IllegalArgumentException(
                        "conflicting pool config: " + key + " (ingest=" + vi + ", query=" + vq + ")");
            }
            value = vi;
        } else if (inIngest) {
            value = ingest.getLong(key, UNSET);
        } else if (inQuery) {
            value = query.getLong(key, UNSET);
        } else {
            value = dflt;
        }
        setter.accept(value);
    }
}
