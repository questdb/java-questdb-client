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
import io.questdb.client.Query;
import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;

import java.util.concurrent.TimeUnit;

/**
 * Thin per-borrow handle returned by {@link QuestDBImpl#borrowQuery()}. A fresh
 * instance is created on every borrow, capturing the immutable lease
 * {@code generation} stamped by {@link QueryClientPool#acquire()}; it delegates
 * every {@link Query} and {@link Completion} operation to the worker's reused
 * {@link QueryImpl}, threading that generation through so a stale handle cannot
 * disturb a later borrow on the same worker (see {@link QueryImpl}).
 * <p>
 * It implements {@link Completion} as well as {@link Query} so {@link #submit()}
 * can return {@code this} -- the per-submit path stays allocation-free, and the
 * single small allocation happens once per borrow (and is routinely
 * scalar-replaced by the JIT in the common try-with-resources case).
 */
final class QueryLease implements Query, Completion {

    private final long generation;
    private final QueryImpl impl;

    QueryLease(QueryImpl impl, long generation) {
        this.impl = impl;
        this.generation = generation;
    }

    @Override
    public void abandon() {
        impl.abandon(generation);
    }

    @Override
    public void await() throws InterruptedException {
        impl.await(generation);
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return impl.await(generation, timeout, unit);
    }

    @Override
    public Query binds(QwpBindSetter binds) {
        impl.setBinds(generation, binds);
        return this;
    }

    @Override
    public void cancel() {
        impl.cancel(generation);
    }

    @Override
    public void close() {
        impl.close(generation);
    }

    @Override
    public Query handler(QwpColumnBatchHandler handler) {
        impl.setHandler(generation, handler);
        return this;
    }

    @Override
    public boolean isDone() {
        return impl.isDone(generation);
    }

    @Override
    public Query sql(CharSequence sql) {
        impl.setSql(generation, sql);
        return this;
    }

    @Override
    public Completion submit() {
        impl.submit(generation);
        return this;
    }

    @Override
    public QwpServerInfo serverInfo() {
        return impl.serverInfo(generation);
    }
}
