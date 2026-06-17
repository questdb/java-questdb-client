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
    // Stable index of this slot within the pool's [0, maxSize) id space. In SF
    // mode it names the on-disk slot dir suffix (<sf_dir>/<base>-<slotIndex>);
    // the pool reuses it for capacity accounting in non-SF mode too.
    private final int slotIndex;
    private volatile long idleSinceMillis;
    private volatile boolean inUse;
    private volatile boolean invalidated;
    // Highest FSN published by the last return-to-pool flush, or -1 if nothing
    // was published (or non-WebSocket transport). Compared against the delegate's
    // acked watermark to tell whether this idle slot still has un-acked SF data
    // on disk -- the reaper must not close it until that data is durable.
    private volatile long lastPublishedFsn = -1L;

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
     * <p>
     * Clears the current thread's pin (if any) before the slot becomes
     * borrowable again. Without this step a thread that pinned this
     * wrapper and then closed it via the public {@link Sender#close()}
     * (the natural try-with-resources idiom) would still hold the pin
     * in its {@link ThreadLocal}; a subsequent {@code QuestDB.sender()}
     * call on that thread would return the cached wrapper even though
     * another consumer has since borrowed the slot, and the two
     * consumers would write to the same underlying delegate.
     */
    @Override
    public void close() {
        if (!inUse) {
            return;
        }
        boolean broken = false;
        try {
            // flushAndGetSequence() performs the same flush as flush() but hands
            // back the highest published FSN, which the reaper later compares
            // against the acked watermark (see isDurablyAcked()). -1 on a
            // non-WebSocket transport, which makes isDurablyAcked() a no-op.
            lastPublishedFsn = delegate.flushAndGetSequence();
        } catch (RuntimeException e) {
            // Sender does not clear its buffer on flush failure (see
            // Sender Javadoc), and WebSocket transport latches the failure
            // for good. Either way, the wrapper is unsafe to recycle: the
            // next borrower would inherit the failed rows or a dead
            // connection.
            broken = true;
            throw e;
        } finally {
            inUse = false;
            // Clear the pin BEFORE returning the slot. If we cleared
            // after giveBack(), a concurrent borrower could grab the
            // slot while this thread's pin still references it, and a
            // re-pin on this thread would return the (now in-use)
            // wrapper -- the same race this clear is meant to close.
            pool.clearPinIfCurrent(this);
            if (broken) {
                pool.discardBroken(this);
            } else {
                pool.giveBack(this);
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

    Sender delegate() {
        return delegate;
    }

    long idleSinceMillis() {
        return idleSinceMillis;
    }

    /**
     * True iff everything this slot has published is durably acked by the server,
     * i.e. there is no un-acked SF data left in its slot dir. Non-blocking: just a
     * volatile read of the acked watermark. {@code lastPublishedFsn < 0} means
     * nothing was published (or non-WebSocket transport), so the slot is trivially
     * drained. The reaper uses this to avoid closing a slot mid-flight.
     */
    boolean isDurablyAcked() {
        return lastPublishedFsn < 0 || delegate.getAckedFsn() >= lastPublishedFsn;
    }

    boolean isInUse() {
        return inUse;
    }

    int slotIndex() {
        return slotIndex;
    }

    boolean isInvalidated() {
        return invalidated;
    }

    void markIdleAt(long nowMillis) {
        idleSinceMillis = nowMillis;
    }

    void markInUse() {
        inUse = true;
    }

    void markInvalidated() {
        invalidated = true;
    }
}
