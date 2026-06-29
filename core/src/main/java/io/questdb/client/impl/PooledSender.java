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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Decorator that lends a real {@link Sender} from {@link SenderPool}. The
 * decorator is pre-allocated once per pool slot and reused for every borrow.
 * <p>
 * Behavior difference from a raw Sender: {@link #close()} on a pooled Sender
 * flushes the buffer and returns the decorator to the pool. The underlying
 * Sender is only truly closed when {@link io.questdb.client.QuestDB#close()}
 * shuts down the pool.
 */
public final class PooledSender implements Sender {

    private final long createdAtMillis;
    private final Sender delegate;
    private final SenderPool pool;
    // Index of the store-and-forward slot this wrapper owns within the pool,
    // or -1 when SF is disabled. Stable for the wrapper's whole life; the
    // pool returns it to the free set only when the wrapper is evicted from
    // {@code all} (discardBroken / reapIdle). Used to derive a distinct
    // {@code sender_id} per pooled sender so concurrent SF senders sharing
    // one {@code sf_dir} never collide on the slot {@code flock}.
    private final int slotIndex;
    private volatile long idleSinceMillis;
    private volatile boolean inUse;

    PooledSender(Sender delegate, SenderPool pool, int slotIndex) {
        this.delegate = delegate;
        this.pool = pool;
        this.slotIndex = slotIndex;
        this.createdAtMillis = System.currentTimeMillis();
        this.idleSinceMillis = this.createdAtMillis;
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        delegate.at(timestamp, unit);
    }

    @Override
    public void at(Instant timestamp) {
        delegate.at(timestamp);
    }

    @Override
    public void atNow() {
        delegate.atNow();
    }

    @Override
    public boolean awaitAckedFsn(long targetFsn, long timeoutMillis) {
        return delegate.awaitAckedFsn(targetFsn, timeoutMillis);
    }

    @Override
    public Sender binaryColumn(CharSequence name, byte[] value) {
        delegate.binaryColumn(name, value);
        return this;
    }

    @Override
    public Sender binaryColumn(CharSequence name, long ptr, long len) {
        delegate.binaryColumn(name, ptr, len);
        return this;
    }

    @Override
    public Sender binaryColumn(CharSequence name, DirectByteSlice slice) {
        delegate.binaryColumn(name, slice);
        return this;
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        delegate.boolColumn(name, value);
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        return delegate.bufferView();
    }

    @Override
    public Sender byteColumn(CharSequence name, byte value) {
        delegate.byteColumn(name, value);
        return this;
    }

    @Override
    public void cancelRow() {
        delegate.cancelRow();
    }

    @Override
    public Sender charColumn(CharSequence name, char value) {
        delegate.charColumn(name, value);
        return this;
    }

    /**
     * Flushes pending rows and returns this decorator to the pool. Does not
     * actually close the underlying {@link Sender}; that only happens when
     * the owning {@code QuestDB} is closed.
     * <p>
     * Idempotent: a second call after a return is a no-op.
     */
    @Override
    public void close() {
        if (!inUse) {
            return;
        }
        // Track normal completion rather than catching a specific throwable
        // type. flush() can exit abnormally with an Error (AssertionError
        // under -ea, OutOfMemoryError, ...) as well as a RuntimeException;
        // keying the recycle decision off normal completion treats every
        // abnormal exit as unrecyclable, which is the fail-safe default.
        boolean flushed = false;
        try {
            delegate.flush();
            flushed = true;
        } finally {
            inUse = false;
            if (flushed) {
                pool.giveBack(this);
            } else {
                // flush() did not complete normally. Sender does not clear
                // its buffer on flush failure (see Sender Javadoc), and
                // WebSocket transport latches the failure for good. Either
                // way the wrapper is unsafe to recycle: the next borrower
                // would inherit the failed rows or a dead connection. The
                // original throwable propagates naturally once this finally
                // returns -- no explicit rethrow needed.
                pool.discardBroken(this);
            }
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        delegate.decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        delegate.decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        delegate.decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        delegate.decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        delegate.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        delegate.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        delegate.doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        delegate.doubleArray(name, array);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        delegate.doubleColumn(name, value);
        return this;
    }

    @Override
    public boolean drain(long timeoutMillis) {
        return delegate.drain(timeoutMillis);
    }

    @Override
    public Sender floatColumn(CharSequence name, float value) {
        delegate.floatColumn(name, value);
        return this;
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public long flushAndGetSequence() {
        return delegate.flushAndGetSequence();
    }

    @Override
    public Sender geoHashColumn(CharSequence name, long bits, int precisionBits) {
        delegate.geoHashColumn(name, bits, precisionBits);
        return this;
    }

    @Override
    public Sender geoHashColumn(CharSequence name, CharSequence value) {
        delegate.geoHashColumn(name, value);
        return this;
    }

    @Override
    public long getAckedFsn() {
        return delegate.getAckedFsn();
    }

    @Override
    public Sender intColumn(CharSequence name, int value) {
        delegate.intColumn(name, value);
        return this;
    }

    @Override
    public Sender ipv4Column(CharSequence name, int address) {
        delegate.ipv4Column(name, address);
        return this;
    }

    @Override
    public Sender ipv4Column(CharSequence name, CharSequence address) {
        delegate.ipv4Column(name, address);
        return this;
    }

    @Override
    public Sender long256Column(CharSequence name, long l0, long l1, long l2, long l3) {
        delegate.long256Column(name, l0, l1, l2, l3);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        delegate.longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        delegate.longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        delegate.longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray values) {
        delegate.longArray(name, values);
        return this;
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        delegate.longColumn(name, value);
        return this;
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public Sender shortColumn(CharSequence name, short value) {
        delegate.shortColumn(name, value);
        return this;
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        delegate.stringColumn(name, value);
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        delegate.symbol(name, value);
        return this;
    }

    @Override
    public Sender table(CharSequence table) {
        delegate.table(table);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        delegate.timestampColumn(name, value, unit);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant value) {
        delegate.timestampColumn(name, value);
        return this;
    }

    @Override
    public Sender uuidColumn(CharSequence name, long lo, long hi) {
        delegate.uuidColumn(name, lo, hi);
        return this;
    }

    long createdAtMillis() {
        return createdAtMillis;
    }

    int slotIndex() {
        return slotIndex;
    }

    Sender delegate() {
        return delegate;
    }

    long idleSinceMillis() {
        return idleSinceMillis;
    }

    boolean isInUse() {
        return inUse;
    }

    void markIdleAt(long nowMillis) {
        idleSinceMillis = nowMillis;
    }

    void markInUse() {
        inUse = true;
    }
}
