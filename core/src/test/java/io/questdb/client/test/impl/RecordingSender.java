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


package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.std.bytes.DirectByteSlice;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link Sender} double for pool tests. Records calls, models the
 * buffer state the pool asks about, and can be told to fail or to block.
 * Never touches the network. Not final, so a test can override a single method.
 */
class RecordingSender implements Sender {
    final AtomicInteger cancelCount = new AtomicInteger();
    final AtomicInteger closeCount = new AtomicInteger();
    final AtomicInteger deferredFlushCount = new AtomicInteger();
    final AtomicInteger flushCount = new AtomicInteger();
    volatile RuntimeException cancelError;     // thrown by cancelRow() when set
    volatile RuntimeException deferredFlushError; // thrown by flushDeferred() when set
    volatile boolean deferredFramesOpen;
    volatile RuntimeException flushError;      // thrown by flush() when set
    volatile Error flushFatal;                 // thrown by flush() when set, before flushError
    volatile CountDownLatch flushGate;         // flush() blocks on it when set
    volatile RuntimeException latchedError;    // thrown by checkError() when set
    volatile CountDownLatch markSavepointGate; // markSavepoint() blocks on it when set
    volatile int pendingRows;
    volatile boolean rowInProgress;
    volatile long rowsAppended;
    volatile int savepointRows;

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        rowInProgress = false;
        pendingRows++;
        rowsAppended++;
    }

    @Override
    public void at(Instant timestamp) {
        at(0L, ChronoUnit.MICROS);
    }

    @Override
    public void atNow() {
        at(0L, ChronoUnit.MICROS);
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        return null;
    }

    @Override
    public void cancelRow() {
        cancelCount.incrementAndGet();
        RuntimeException e = cancelError;
        if (e != null) {
            throw e;
        }
        rowInProgress = false;
    }

    @Override
    public void checkError() {
        RuntimeException e = latchedError;
        if (e != null) {
            throw e;
        }
    }

    @Override
    public void close() {
        closeCount.incrementAndGet();
    }

    @Override
    public Sender doubleArray(CharSequence name, double[] values) {
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, double[][] values) {
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, double[][][] values) {
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray values) {
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        return this;
    }

    @Override
    public boolean drain(long timeoutMillis) {
        flush();
        return true;
    }

    @Override
    public void flush() {
        awaitGate(flushGate, "flushGate");
        Error fatal = flushFatal;
        if (fatal != null) {
            throw fatal;
        }
        RuntimeException e = flushError;
        if (e != null) {
            throw e;
        }
        // Like the real sender: publishes nothing when there is nothing to
        // ship and no open group to commit, so count only real work.
        if (pendingRows > 0 || deferredFramesOpen) {
            flushCount.incrementAndGet();
        }
        pendingRows = 0;
        savepointRows = 0;
        deferredFramesOpen = false;
    }

    @Override
    public long flushDeferred() {
        awaitGate(flushGate, "flushGate");
        RuntimeException deferredError = deferredFlushError;
        if (deferredError != null) {
            throw deferredError;
        }
        boolean hadRows = pendingRows > 0;
        if (!hadRows) {
            return -1L;                       // nothing pending: the real sender publishes nothing
        }
        pendingRows = 0;
        savepointRows = 0;
        deferredFramesOpen = true;
        return deferredFlushCount.incrementAndGet();
    }

    @Override
    public boolean hasDeferredFrames() {
        return deferredFramesOpen;
    }

    @Override
    public boolean hasRowInProgress() {
        return rowInProgress;
    }

    @Override
    public Sender longArray(CharSequence name, long[] values) {
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, long[][] values) {
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, long[][][] values) {
        return this;
    }

    @Override
    public Sender longArray(CharSequence name, LongArray values) {
        return this;
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        return this;
    }

    @Override
    public void markSavepoint() {
        awaitGate(markSavepointGate, "markSavepointGate");
        savepointRows = pendingRows;
    }

    @Override
    public int pendingRowCount() {
        return pendingRows;
    }

    @Override
    public void reset() {
        pendingRows = 0;
        savepointRows = 0;
        rowInProgress = false;
    }

    @Override
    public void resetToSavepoint() {
        pendingRows = savepointRows;
        rowInProgress = false;
    }

    @Override
    public long rowsAppended() {
        return rowsAppended;
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        return this;
    }

    @Override
    public Sender table(CharSequence table) {
        rowInProgress = true;
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant value) {
        return this;
    }

    private static void awaitGate(CountDownLatch gate, String name) {
        if (gate != null) {
            try {
                if (!gate.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError(name + " was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
