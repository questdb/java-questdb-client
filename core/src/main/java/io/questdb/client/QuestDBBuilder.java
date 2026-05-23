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

import io.questdb.client.impl.ConfigStringTranslator;
import io.questdb.client.impl.QuestDBImpl;

/**
 * Builder for {@link QuestDB}. Most callers use {@link QuestDB#connect(CharSequence)};
 * this builder is for pool sizing, idle/lifetime knobs, acquire timeout,
 * and the case where ingest and egress configs differ.
 */
public final class QuestDBBuilder {

    static final long DEFAULT_ACQUIRE_TIMEOUT_MILLIS = 5_000;
    static final long DEFAULT_HOUSEKEEPER_INTERVAL_MILLIS = 5_000;
    static final long DEFAULT_IDLE_TIMEOUT_MILLIS = 60_000;
    static final long DEFAULT_MAX_LIFETIME_MILLIS = 30 * 60_000L;
    static final int DEFAULT_POOL_MAX = 4;
    static final int DEFAULT_POOL_MIN = 1;

    private long acquireTimeoutMillis = DEFAULT_ACQUIRE_TIMEOUT_MILLIS;
    private long housekeeperIntervalMillis = DEFAULT_HOUSEKEEPER_INTERVAL_MILLIS;
    private long idleTimeoutMillis = DEFAULT_IDLE_TIMEOUT_MILLIS;
    private String ingestConfig;
    private long maxLifetimeMillis = DEFAULT_MAX_LIFETIME_MILLIS;
    private String queryConfig;
    private int queryPoolMax = DEFAULT_POOL_MAX;
    private int queryPoolMin = DEFAULT_POOL_MIN;
    private int senderPoolMax = DEFAULT_POOL_MAX;
    private int senderPoolMin = DEFAULT_POOL_MIN;

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
     * Builds the {@link QuestDB} handle. Eagerly creates {@code min}
     * connections in each pool; further slots are allocated lazily up to
     * {@code max} when load demands and reaped back to {@code min} when
     * idle.
     */
    public QuestDB build() {
        if (ingestConfig == null) {
            throw new IllegalStateException("ingest configuration is required; call fromConfig() or ingestConfig()");
        }
        if (queryConfig == null) {
            throw new IllegalStateException("query configuration is required; call fromConfig() or queryConfig()");
        }
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
     * Sets a single unified configuration string used to derive both the
     * ingest and the egress config. Schema must be {@code http}, {@code https},
     * {@code ws} or {@code wss}; the other half is derived by schema
     * translation.
     */
    public QuestDBBuilder fromConfig(CharSequence configurationString) {
        ConfigStringTranslator.Bundle bundle = ConfigStringTranslator.deriveBothSides(configurationString);
        this.ingestConfig = bundle.ingestConfig;
        this.queryConfig = bundle.queryConfig;
        ConfigStringTranslator.PoolConfig pc = bundle.poolConfig;
        // Apply pool keys carried in the string. Explicit builder calls AFTER
        // fromConfig() will overwrite these -- last write wins.
        if (pc.senderPoolMin != ConfigStringTranslator.PoolConfig.UNSET) {
            senderPoolMin(pc.senderPoolMin);
        }
        if (pc.senderPoolMax != ConfigStringTranslator.PoolConfig.UNSET) {
            senderPoolMax(pc.senderPoolMax);
        }
        if (pc.queryPoolMin != ConfigStringTranslator.PoolConfig.UNSET) {
            queryPoolMin(pc.queryPoolMin);
        }
        if (pc.queryPoolMax != ConfigStringTranslator.PoolConfig.UNSET) {
            queryPoolMax(pc.queryPoolMax);
        }
        if (pc.acquireTimeoutMillis != ConfigStringTranslator.PoolConfig.UNSET) {
            acquireTimeoutMillis(pc.acquireTimeoutMillis);
        }
        if (pc.idleTimeoutMillis != ConfigStringTranslator.PoolConfig.UNSET) {
            idleTimeoutMillis(pc.idleTimeoutMillis);
        }
        if (pc.maxLifetimeMillis != ConfigStringTranslator.PoolConfig.UNSET) {
            maxLifetimeMillis(pc.maxLifetimeMillis);
        }
        if (pc.housekeeperIntervalMillis != ConfigStringTranslator.PoolConfig.UNSET) {
            housekeeperIntervalMillis(pc.housekeeperIntervalMillis);
        }
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
     * Sets the ingest-side configuration in {@link Sender#fromConfig} format.
     */
    public QuestDBBuilder ingestConfig(CharSequence configurationString) {
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
     * Sets the query-side configuration in
     * {@link io.questdb.client.cutlass.qwp.client.QwpQueryClient#fromConfig}
     * format.
     */
    public QuestDBBuilder queryConfig(CharSequence configurationString) {
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
}
