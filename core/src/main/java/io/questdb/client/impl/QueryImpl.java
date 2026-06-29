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
import io.questdb.client.QueryException;
import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpBindValues;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.std.str.StringSink;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implementation of {@link Query}: the pooled lease handle. One instance is
 * pre-allocated per {@link QueryWorker} and reused across borrows. Holds the
 * configured query state (SQL, optional binds, handler), an inner
 * {@link Completion}, and a wrapping {@link QwpColumnBatchHandler} that forwards
 * callbacks to the user handler and signals the Completion on terminal events.
 * <p>
 * Lifecycle: {@link QueryWorker#lease()} resets this handle and hands it out
 * when {@link QuestDBImpl#borrowQuery()} acquires the worker. {@link #submit()}
 * dispatches on the held worker (single-flight) and returns the cached
 * {@link Completion}; {@link #close()} returns the worker to the pool.
 */
final class QueryImpl implements Query {

    private final InnerCompletion completion = new InnerCompletion();
    private final Condition doneCondition;
    private final ReentrantLock doneLock = new ReentrantLock();
    private final StringSink sqlBuffer = new StringSink();
    private final QueryWorker worker;
    private final WrappingHandler wrappingHandler = new WrappingHandler();
    private boolean borrowed;
    private volatile boolean done = true;
    private volatile String resultMessage;
    private volatile byte resultStatus;
    private volatile Throwable unexpectedError;
    private QwpBindSetter userBinds;
    private final QwpBindSetter wireBinds = this::applyBinds;
    private QwpColumnBatchHandler userHandler;

    QueryImpl(QueryWorker worker) {
        this.worker = worker;
        this.doneCondition = doneLock.newCondition();
    }

    @Override
    public void abandon() {
        if (!done) {
            throw new IllegalStateException("a previous submit() is still in flight; await the Completion first");
        }
        userBinds = null;
        userHandler = null;
        sqlBuffer.clear();
    }

    @Override
    public Query binds(QwpBindSetter binds) {
        this.userBinds = binds;
        return this;
    }

    @Override
    public void close() {
        if (!borrowed) {
            return;
        }
        // If a submit is still in flight (the caller did not await), cancel it
        // and wait for the terminal event so the leased worker is idle before
        // it returns to the pool -- otherwise the next borrower would inherit a
        // running execute().
        if (!done) {
            worker.cancelInFlight();
            doneLock.lock();
            try {
                while (!done) {
                    doneCondition.awaitUninterruptibly();
                }
            } finally {
                doneLock.unlock();
            }
        }
        borrowed = false;
        worker.releaseToPool();
    }

    @Override
    public Query handler(QwpColumnBatchHandler handler) {
        this.userHandler = handler;
        return this;
    }

    @Override
    public Query sql(CharSequence sql) {
        sqlBuffer.clear();
        sqlBuffer.put(sql);
        return this;
    }

    @Override
    public Completion submit() {
        if (!borrowed) {
            throw new IllegalStateException("query handle is not borrowed (closed or never leased)");
        }
        if (sqlBuffer.length() == 0) {
            throw new IllegalStateException("sql is required");
        }
        if (userHandler == null) {
            throw new IllegalStateException("handler is required");
        }
        if (!done) {
            throw new IllegalStateException("a previous submit() is still in flight; await the Completion first");
        }
        // Reset terminal state under the lock so a stale signal from a prior
        // run can't be observed by the upcoming await().
        doneLock.lock();
        try {
            done = false;
            resultStatus = 0;
            resultMessage = null;
            unexpectedError = null;
        } finally {
            doneLock.unlock();
        }
        worker.dispatch(this);
        return completion;
    }

    private void applyBinds(QwpBindValues binds) {
        QwpBindSetter setter = userBinds;
        if (setter != null) {
            setter.apply(binds);
        }
    }

    private void signalDone(byte status, String message, Throwable unexpected) {
        doneLock.lock();
        try {
            if (done) {
                return;
            }
            this.resultStatus = status;
            this.resultMessage = message;
            this.unexpectedError = unexpected;
            this.done = true;
            doneCondition.signalAll();
        } finally {
            doneLock.unlock();
        }
    }

    /**
     * Resets builder state to empty and marks this handle borrowed. Called by
     * {@link QueryWorker#lease()} when {@link QuestDBImpl#borrowQuery()} hands
     * the pre-allocated handle out, so each borrow starts from the documented
     * "reset to empty" contract on {@link io.questdb.client.Query}. The leased
     * worker is idle at this point (just acquired from the pool), so the reset
     * is unconditional.
     */
    void resetForBorrow() {
        userBinds = null;
        userHandler = null;
        sqlBuffer.clear();
        resultStatus = 0;
        resultMessage = null;
        unexpectedError = null;
        done = true;
        borrowed = true;
    }

    void runOn(QwpQueryClient client) {
        // Pass the StringSink directly as a CharSequence -- the wire encoder
        // reads chars and writes UTF-8 bytes straight into the send buffer.
        // sqlBuffer is stable for the duration of execute(): the calling
        // worker thread is blocked here until a terminal event arrives, and
        // sql(...) cannot be invoked again until done==true.
        client.execute(sqlBuffer, wireBinds, wrappingHandler);
    }

    /**
     * Signals an unexpected error from the worker thread (for example, an
     * exception escaping {@code execute()} before any handler callback).
     */
    void signalUnexpected(Throwable t) {
        signalDone((byte) 0, t.getMessage() != null ? t.getMessage() : t.getClass().getSimpleName(), t);
    }

    private final class InnerCompletion implements Completion {

        @Override
        public void await() throws InterruptedException {
            doneLock.lock();
            try {
                while (!done) {
                    doneCondition.await();
                }
            } finally {
                doneLock.unlock();
            }
            throwIfFailed();
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            long remaining = unit.toNanos(timeout);
            doneLock.lock();
            try {
                while (!done) {
                    if (remaining <= 0) {
                        return false;
                    }
                    remaining = doneCondition.awaitNanos(remaining);
                }
            } finally {
                doneLock.unlock();
            }
            throwIfFailed();
            return true;
        }

        @Override
        public void cancel() {
            if (!done) {
                worker.cancelInFlight();
            }
        }

        @Override
        public boolean isDone() {
            return done;
        }

        private void throwIfFailed() {
            Throwable unexpected = unexpectedError;
            if (unexpected != null) {
                throw new QueryException(resultStatus, resultMessage, unexpected);
            }
            if (resultStatus != 0) {
                throw new QueryException(resultStatus, resultMessage);
            }
        }
    }

    private final class WrappingHandler implements QwpColumnBatchHandler {

        @Override
        public void onBatch(QwpColumnBatch batch) {
            userHandler.onBatch(batch);
        }

        @Override
        public void onEnd(long totalRows) {
            try {
                userHandler.onEnd(totalRows);
            } finally {
                signalDone((byte) 0, null, null);
            }
        }

        @Override
        public void onEnd(long requestId, long totalRows) {
            try {
                userHandler.onEnd(requestId, totalRows);
            } finally {
                signalDone((byte) 0, null, null);
            }
        }

        @Override
        public void onError(byte status, String message) {
            try {
                userHandler.onError(status, message);
            } finally {
                signalDone(status, message, null);
            }
        }

        @Override
        public void onError(long requestId, byte status, String message) {
            try {
                userHandler.onError(requestId, status, message);
            } finally {
                signalDone(status, message, null);
            }
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            try {
                userHandler.onExecDone(opType, rowsAffected);
            } finally {
                signalDone((byte) 0, null, null);
            }
        }

        @Override
        public void onExecDone(long requestId, short opType, long rowsAffected) {
            try {
                userHandler.onExecDone(requestId, opType, rowsAffected);
            } finally {
                signalDone((byte) 0, null, null);
            }
        }

        @Override
        public void onFailoverReset(QwpServerInfo newNode) {
            userHandler.onFailoverReset(newNode);
        }

        @Override
        public void onFailoverReset(long requestId, QwpServerInfo newNode) {
            userHandler.onFailoverReset(requestId, newNode);
        }
    }
}
