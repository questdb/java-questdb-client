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

import io.questdb.client.Completion;
import io.questdb.client.QuestDB;
import io.questdb.client.Query;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Implementation of {@link QuestDB}. Owns the elastic {@link SenderPool}
 * and {@link QueryClientPool}, a {@link PoolHousekeeper} that reaps idle
 * slots, and a {@link ThreadLocal} of {@link QueryImpl} instances so that
 * {@link #query()} is allocation-free after the first call on each thread.
 */
public final class QuestDBImpl implements QuestDB {

    private final PoolHousekeeper housekeeper;
    private final QueryClientPool queryPool;
    private final ThreadLocal<QueryImpl> queryThreadLocal;
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
            long housekeeperIntervalMillis
    ) {
        SenderPool builtSenderPool = null;
        QueryClientPool builtQueryPool = null;
        PoolHousekeeper builtHousekeeper = null;
        try {
            builtSenderPool = new SenderPool(
                    ingestConfig, senderMin, senderMax, acquireTimeoutMillis,
                    idleTimeoutMillis, maxLifetimeMillis);
            builtQueryPool = new QueryClientPool(
                    queryConfig, queryMin, queryMax, acquireTimeoutMillis,
                    idleTimeoutMillis, maxLifetimeMillis);
            builtHousekeeper = new PoolHousekeeper(builtSenderPool, builtQueryPool, housekeeperIntervalMillis);
            builtHousekeeper.start();
        } catch (RuntimeException e) {
            if (builtHousekeeper != null) {
                builtHousekeeper.stop();
            }
            if (builtQueryPool != null) {
                builtQueryPool.close();
            }
            if (builtSenderPool != null) {
                builtSenderPool.close();
            }
            throw e;
        }
        this.senderPool = builtSenderPool;
        this.queryPool = builtQueryPool;
        this.housekeeper = builtHousekeeper;
        this.queryThreadLocal = ThreadLocal.withInitial(() -> new QueryImpl(queryPool));
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
        housekeeper.stop();
        queryPool.close();
        senderPool.close();
    }

    @Override
    public Completion executeSql(CharSequence sql, QwpColumnBatchHandler handler) {
        return query().sql(sql).handler(handler).submit();
    }

    @Override
    public Query newQuery() {
        return new QueryImpl(queryPool);
    }

    @Override
    public Query query() {
        return queryThreadLocal.get();
    }

    @Override
    public void releaseSender() {
        senderPool.releaseCurrentThread();
    }

    @Override
    public Sender sender() {
        return senderPool.pinToCurrentThread();
    }
}
