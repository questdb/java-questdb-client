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
 * Reusable per-{@link QueryWorker} query state: the configured SQL, optional
 * binds, handler, terminal-event signalling, and a wrapping
 * {@link QwpColumnBatchHandler} that forwards callbacks to the user handler and
 * signals completion on terminal events. One instance is pre-allocated per
 * worker in the constructor and reused across every borrow.
 * <p>
 * Because the instance is shared across borrows, it must never be handed to a
 * caller directly -- a stale reference would leak into a later borrow's
 * lifecycle. Callers instead receive a thin, per-borrow {@link QueryLease} that
 * carries the lease {@code generation} stamped at borrow time and passes it
 * into every operation here. Each operation validates that generation against
 * {@link QueryWorker#generation()}:
 * <ul>
 *   <li>builder/await operations on a stale generation throw
 *       {@code IllegalStateException} ("query handle is closed"),</li>
 *   <li>{@link #close(long)} and {@link #cancel(long)} on a stale generation are
 *       no-ops -- this is what makes {@code Query.close()} idempotent and
 *       prevents a stale handle from releasing, or cancelling the in-flight
 *       query of, a worker a different borrower now owns.</li>
 * </ul>
 * <p>
 * Lifecycle: {@link QueryWorker#lease()} resets this state and wraps it in a
 * fresh {@link QueryLease} when {@link QuestDBImpl#borrowQuery()} acquires the
 * worker. {@link #submit(long)} dispatches on the held worker (single-flight);
 * {@link #close(long)} returns the worker to the pool.
 */
final class QueryImpl {

    private final Condition doneCondition;
    private final ReentrantLock doneLock = new ReentrantLock();
    private final StringSink sqlBuffer = new StringSink();
    private final QueryWorker worker;
    private final QwpBindSetter wireBinds = this::applyBinds;
    private final WrappingHandler wrappingHandler = new WrappingHandler();
    private volatile boolean done = true;
    private volatile String resultMessage;
    private volatile byte resultStatus;
    private volatile Throwable unexpectedError;
    private QwpBindSetter userBinds;
    private QwpColumnBatchHandler userHandler;

    QueryImpl(QueryWorker worker) {
        this.worker = worker;
        this.doneCondition = doneLock.newCondition();
    }

    void abandon(long gen) {
        checkLive(gen);
        if (!done) {
            throw new IllegalStateException("a previous submit() is still in flight; await the Completion first");
        }
        userBinds = null;
        userHandler = null;
        sqlBuffer.clear();
    }

    void await(long gen) throws InterruptedException {
        rejectHandlerReentry("await");
        checkLive(gen);
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

    boolean await(long gen, long timeout, TimeUnit unit) throws InterruptedException {
        rejectHandlerReentry("await");
        checkLive(gen);
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

    void cancel(long gen) {
        // Fast-path drop of an obviously-stale or already-finished cancel,
        // without taking the pool lock. This is only a hint -- the
        // authoritative re-check runs under the pool lock inside
        // worker.cancelInFlight(gen).
        if (gen != worker.generation() || done) {
            return;
        }
        // Re-check the lease generation and issue the wire cancel atomically
        // under the pool lock (the same lock acquire()/release() bump the
        // generation under). An unlocked check followed by an unlocked cancel
        // is a TOCTOU: a cross-thread watchdog can pass the check, get
        // preempted while this lease is released and the worker re-borrowed by
        // another caller, then resume and abort that caller's in-flight query.
        worker.cancelInFlight(gen);
    }

    void close(long gen) {
        rejectHandlerReentry("close");
        // A stale generation means this lease was already released and the
        // worker may now be owned by another borrower. Dropping the call is
        // what keeps close() idempotent without releasing someone else's
        // worker or cancelling their in-flight query. release() re-checks the
        // generation under the pool lock, so the worker can never be enqueued
        // twice even if two threads race a close on the same live lease.
        if (gen != worker.generation()) {
            return;
        }
        // If a submit is still in flight (the caller did not await, or its
        // await timed out), cancel it and wait for the terminal event so the
        // leased worker is idle before it returns to the pool -- otherwise the
        // next borrower would inherit a running execute().
        //
        // The wait is bounded (closeQueryTimeoutMillis) and interruptible, so a
        // caller that bounded its own await() is never pinned to the full
        // remaining query duration here. If the query does NOT drain in time (a
        // server slow to honor the cancel, or the caller interrupting), the
        // worker is still running execute() on a connection whose protocol state
        // is now uncertain -- a late RESULT_* for the abandoned query could
        // corrupt the next borrower's stream -- so it is discarded rather than
        // returned. The pool grows a fresh worker on the next borrow.
        if (!done) {
            worker.cancelInFlight(gen);
            if (!awaitDone(worker.closeQueryTimeoutMillis())) {
                worker.discardFromPool(gen);
                return;
            }
        }
        worker.releaseToPool(gen);
    }

    boolean isDone(long gen) {
        checkLive(gen);
        return done;
    }

    /**
     * Returns the leased client's cached {@link QwpServerInfo} (the decoded
     * {@code SERVER_INFO} frame from its most recent successful bind) without
     * issuing a query. Read-only: it does not dispatch to the worker or drive
     * the failover reconnect loop, so it is safe to call between submits to
     * observe the bound endpoint without perturbing it. Validates the lease
     * generation like the other builder ops so a stale handle throws rather
     * than reading a worker a different borrower now owns.
     */
    QwpServerInfo serverInfo(long gen) {
        checkLive(gen);
        return worker.client().getServerInfo();
    }

    void setBinds(long gen, QwpBindSetter binds) {
        checkLive(gen);
        this.userBinds = binds;
    }

    void setHandler(long gen, QwpColumnBatchHandler handler) {
        checkLive(gen);
        this.userHandler = handler;
    }

    void setSql(long gen, CharSequence sql) {
        checkLive(gen);
        sqlBuffer.clear();
        sqlBuffer.put(sql);
    }

    void submit(long gen) {
        checkLive(gen);
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
    }

    private void applyBinds(QwpBindValues binds) {
        QwpBindSetter setter = userBinds;
        if (setter != null) {
            setter.apply(binds);
        }
    }

    /**
     * Waits up to {@code timeoutMillis} for the in-flight query's terminal
     * event. Returns {@code true} once {@code done} is set, {@code false} on
     * timeout or interrupt. Unlike an uninterruptible drain, an interrupt aborts
     * the wait and re-raises the thread's interrupt flag, so {@code close()}
     * stays responsive to a caller that wants to give up.
     */
    private boolean awaitDone(long timeoutMillis) {
        long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        doneLock.lock();
        try {
            while (!done) {
                if (remaining <= 0) {
                    return false;
                }
                try {
                    remaining = doneCondition.awaitNanos(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            doneLock.unlock();
        }
    }

    private void checkLive(long gen) {
        if (gen != worker.generation()) {
            throw new IllegalStateException("query handle is not borrowed (closed or never leased)");
        }
    }

    private void rejectHandlerReentry(String op) {
        // Result handlers (onBatch/onEnd/onError) run inline on the worker's
        // dispatch thread. A blocking lease op called from there would wait for
        // a terminal event that only this same thread can deliver -- a
        // permanent, uninterruptible self-deadlock plus a leaked worker. Fail
        // loudly at the call site instead. cancel() is the non-blocking stop.
        if (worker.isCurrentThreadWorker()) {
            throw new IllegalStateException(
                    op + "() must not be called from a result handler. Handlers "
                            + "(onBatch/onEnd/onError) run on the worker thread, so " + op
                            + "() would block forever waiting for a terminal event that only "
                            + "this same thread can deliver. To stop a query from inside a "
                            + "handler, call cancel() (non-blocking).");
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

    private void throwIfFailed() {
        Throwable unexpected = unexpectedError;
        if (unexpected != null) {
            throw new QueryException(resultStatus, resultMessage, unexpected);
        }
        if (resultStatus != 0) {
            throw new QueryException(resultStatus, resultMessage);
        }
    }

    /**
     * Resets builder and terminal state to empty. Called by
     * {@link QueryWorker#lease()} when {@link QuestDBImpl#borrowQuery()} hands a
     * freshly stamped {@link QueryLease} out, so each borrow starts from the
     * documented "reset to empty" contract on {@link io.questdb.client.Query}.
     * The leased worker is idle at this point (just acquired from the pool), so
     * the reset is unconditional.
     */
    void resetForBorrow() {
        userBinds = null;
        userHandler = null;
        sqlBuffer.clear();
        resultStatus = 0;
        resultMessage = null;
        unexpectedError = null;
        done = true;
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
