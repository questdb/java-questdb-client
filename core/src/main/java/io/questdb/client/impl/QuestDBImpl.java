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

package io.questdb.client.impl;

import io.questdb.client.QuestDB;
import io.questdb.client.Query;
import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import org.jetbrains.annotations.TestOnly;

import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Implementation of {@link QuestDB}. Owns the elastic {@link SenderPool} and
 * {@link QueryClientPool} and a {@link PoolHousekeeper} that reaps idle slots.
 * {@link #borrowQuery()} leases a pooled {@link QueryWorker} and hands back a
 * thin {@link QueryLease} over its reused {@link QueryImpl}; the heavy per-query
 * state is pre-allocated on the worker and the per-submit path is
 * allocation-free, so only the small lease handle is created per borrow (and is
 * routinely scalar-replaced by the JIT in the try-with-resources case).
 */
public final class QuestDBImpl implements QuestDB {

    private final PoolHousekeeper housekeeper;
    private final QueryClientPool queryPool;
    private final SenderPool senderPool;
    private volatile boolean closed;

    public QuestDBImpl(
            String ingestConfig,
            String queryConfig,
            int senderMin,
            int senderMax,
            int queryMin,
            int queryMax,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            long housekeeperIntervalMillis,
            long queryCloseTimeoutMillis,
            SenderErrorHandler errorHandler,
            SenderConnectionListener connectionListener,
            BackgroundDrainerListener drainerListener
    ) {
        this(ingestConfig, queryConfig, senderMin, senderMax, queryMin, queryMax,
                acquireTimeoutMillis, idleTimeoutMillis, maxLifetimeMillis,
                housekeeperIntervalMillis, queryCloseTimeoutMillis, null, null,
                errorHandler, connectionListener, drainerListener);
    }

    // Test-only constructor exposing the senderFactory and connectHook seams:
    // production uses the public overload above, which passes null for both ->
    // the real native build/connect paths. White-box error-safety tests in
    // io.questdb.client.test.impl call this to make SenderPool prewarm an
    // observable delegate while QueryClientPool construction throws an Error,
    // exercising the cleanup catch below.
    @TestOnly
    public QuestDBImpl(
            String ingestConfig,
            String queryConfig,
            int senderMin,
            int senderMax,
            int queryMin,
            int queryMax,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            long housekeeperIntervalMillis,
            IntFunction<Sender> senderFactory,
            Consumer<QwpQueryClient> connectHook
    ) {
        this(ingestConfig, queryConfig, senderMin, senderMax, queryMin, queryMax,
                acquireTimeoutMillis, idleTimeoutMillis, maxLifetimeMillis,
                housekeeperIntervalMillis, QueryClientPool.DEFAULT_CLOSE_QUERY_TIMEOUT_MILLIS,
                senderFactory, connectHook, null, null, null);
    }

    // Full constructor adding the ingest-side errorHandler/connectionListener/
    // drainerListener, applied by SenderPool to every Sender it builds. The
    // 12-arg overload above is the unchanged white-box test seam and delegates
    // here with null callbacks; the public overload delegates here with null
    // test seams.
    QuestDBImpl(
            String ingestConfig,
            String queryConfig,
            int senderMin,
            int senderMax,
            int queryMin,
            int queryMax,
            long acquireTimeoutMillis,
            long idleTimeoutMillis,
            long maxLifetimeMillis,
            long housekeeperIntervalMillis,
            long queryCloseTimeoutMillis,
            IntFunction<Sender> senderFactory,
            Consumer<QwpQueryClient> connectHook,
            SenderErrorHandler errorHandler,
            SenderConnectionListener connectionListener,
            BackgroundDrainerListener drainerListener
    ) {
        SenderPool builtSenderPool = null;
        QueryClientPool builtQueryPool = null;
        PoolHousekeeper builtHousekeeper = null;
        try {
            builtSenderPool = new SenderPool(
                    ingestConfig, senderMin, senderMax, acquireTimeoutMillis,
                    idleTimeoutMillis, maxLifetimeMillis, senderFactory,
                    // Defer SF startup recovery to the PoolHousekeeper thread so
                    // build() never blocks on a slow / reachable-but-not-acking
                    // server; the housekeeper drives it via runStartupRecoveryStep().
                    true,
                    errorHandler, connectionListener, drainerListener);
            builtQueryPool = new QueryClientPool(
                    queryConfig, queryMin, queryMax, acquireTimeoutMillis,
                    idleTimeoutMillis, maxLifetimeMillis, connectHook);
            builtQueryPool.closeQueryTimeoutMillis(queryCloseTimeoutMillis);
            builtHousekeeper = new PoolHousekeeper(builtSenderPool, builtQueryPool, housekeeperIntervalMillis);
            builtHousekeeper.start();
        } catch (Throwable e) {
            // Catch Throwable, not just RuntimeException: this orchestrator is the
            // direct caller of the SenderPool and QueryClientPool constructors,
            // both of which run heavy native build/connect paths that can throw an
            // Error under -ea (AssertionError, OutOfMemoryError). The pools widened
            // their own prewarm catches to Throwable for exactly this reason; if we
            // only caught RuntimeException here, an Error from QueryClientPool
            // construction (or the housekeeper start) would propagate without
            // closing the already-built SenderPool, stranding its prewarmed
            // delegates' flocks, mmap'd rings, and I/O threads -- the precise leak
            // class this teardown-hardening work exists to kill. The cleanup below
            // is best-effort and rethrows the original failure.
            //
            // Each cleanup step is independently guarded: a Throwable (e.g. an OOM
            // from QueryClientPool.close()'s `new ArrayList<>(all)`, or an Error
            // from the housekeeper join) must never skip the remaining closes. The
            // SenderPool -- owner of the flock/mmap/I/O-thread resources -- is
            // closed last, so an unguarded earlier failure would strand exactly
            // the resources this catch exists to reclaim.
            closeQuietly(builtHousekeeper);
            closeQuietly(builtQueryPool);
            closeQuietly(builtSenderPool);
            throw e;
        }
        this.senderPool = builtSenderPool;
        this.queryPool = builtQueryPool;
        this.housekeeper = builtHousekeeper;
    }

    @Override
    public Query borrowQuery() {
        return queryPool.acquire().lease();
    }

    @Override
    public Sender borrowSender() {
        return senderPool.borrow();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Cancel any in-flight startup SF recovery BEFORE stopping the
        // housekeeper: markClosing() raises the pool's shutdown signal so a
        // recovery step driven on the housekeeper thread bails between slots and
        // the housekeeper join below cannot time out waiting on a fresh slot's
        // recovery drain (C1 fix #1). This only raises the flag; full pool
        // teardown still happens in senderPool.close() further down.
        senderPool.markClosing();
        // Independently guarded so a Throwable from one teardown step cannot skip
        // the rest. senderPool is closed last and owns the flock/mmap/I/O-thread
        // resources, so it must run even if housekeeper.stop()/queryPool.close()
        // throws an Error or OOM.
        closeQuietly(housekeeper);
        closeQuietly(queryPool);
        closeQuietly(senderPool);
    }

    private static void closeQuietly(PoolHousekeeper housekeeper) {
        if (housekeeper == null) {
            return;
        }
        try {
            housekeeper.stop();
        } catch (Throwable ignored) {
            // Best-effort teardown: never let a stop() failure skip the
            // subsequent pool closes.
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Throwable ignored) {
            // Best-effort teardown: never let one close() failure skip the
            // remaining closes.
        }
    }

}
