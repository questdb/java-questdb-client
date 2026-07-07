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
 * Thin per-borrow handle returned by {@link SenderPool#borrow()}. A fresh
 * instance is created on every borrow, capturing the immutable lease
 * {@code generation} stamped by {@code borrow()}; it forwards every
 * {@link Sender} call to the reused {@link SenderSlot}'s delegate, validating
 * that generation first via {@link SenderSlot#live(long)}.
 * <p>
 * Behaviour difference from a raw Sender: {@link #close()} flushes the buffer
 * and returns the slot to the pool. The underlying Sender is only truly closed
 * when {@link io.questdb.client.QuestDB#close()} shuts the pool down.
 * <p>
 * Because the slot is reused across borrows, this wrapper -- not the slot --
 * carries the lease identity. A stale handle (held after {@link #close()}, with
 * the slot since re-borrowed) fails its generation check: data calls throw and
 * {@link #close()} is a no-op, so it can never flush into, release, or be
 * enqueued twice for a slot a different borrower now owns. This mirrors the
 * egress {@code QueryLease} guard.
 */
public final class PooledSender implements Sender {

    private final long generation;
    private final SenderSlot slot;

    PooledSender(SenderSlot slot, long generation) {
        this.slot = slot;
        this.generation = generation;
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        slot.live(generation).at(timestamp, unit);
    }

    @Override
    public void at(Instant timestamp) {
        slot.live(generation).at(timestamp);
    }

    @Override
    public void atNow() {
        slot.live(generation).atNow();
    }

    @Override
    public boolean awaitAckedFsn(long targetFsn, long timeoutMillis) {
        return slot.live(generation).awaitAckedFsn(targetFsn, timeoutMillis);
    }

    @Override
    public Sender binaryColumn(CharSequence name, byte[] value) {
        slot.live(generation).binaryColumn(name, value);
        return this;
    }

    @Override
    public Sender binaryColumn(CharSequence name, long ptr, long len) {
        slot.live(generation).binaryColumn(name, ptr, len);
        return this;
    }

    @Override
    public Sender binaryColumn(CharSequence name, DirectByteSlice slice) {
        slot.live(generation).binaryColumn(name, slice);
        return this;
    }

    @Override
    public Sender boolColumn(CharSequence name, boolean value) {
        slot.live(generation).boolColumn(name, value);
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        return slot.live(generation).bufferView();
    }

    @Override
    public Sender byteColumn(CharSequence name, byte value) {
        slot.live(generation).byteColumn(name, value);
        return this;
    }

    @Override
    public void cancelRow() {
        slot.live(generation).cancelRow();
    }

    @Override
    public Sender charColumn(CharSequence name, char value) {
        slot.live(generation).charColumn(name, value);
        return this;
    }

    /**
     * Flushes pending rows and returns this lease's slot to the pool. Does not
     * actually close the underlying {@link Sender}; that only happens when the
     * owning {@code QuestDB} is closed. If the pool is already shutting down
     * when this lease comes home, the underlying Sender is torn down here, on
     * the calling thread (see {@link SenderPool#close()}).
     * <p>
     * Idempotent for sequential reuse: a stale generation (the lease was
     * already returned and the slot possibly re-borrowed) is a no-op, and the
     * pool re-checks the generation under its lock, so a duplicate close can
     * never re-enqueue a slot a different borrower now owns. Concurrent
     * double-close of ONE lease from two threads is outside the Sender
     * threading contract; the flush below revalidates the lease via
     * {@link SenderSlot#live(long)} so a racing duplicate typically fails with
     * {@code IllegalStateException} rather than flushing into a re-borrowed
     * slot, but two threads racing inside a single lease are not fully
     * protected.
     */
    @Override
    public void close() {
        if (generation != slot.generation()) {
            return;
        }
        // Track normal completion rather than catching a specific throwable
        // type. flush() can exit abnormally with an Error (AssertionError
        // under -ea, OutOfMemoryError, ...) as well as a RuntimeException;
        // keying the recycle decision off normal completion treats every
        // abnormal exit as unrecyclable, which is the fail-safe default.
        boolean flushed = false;
        try {
            // live(), not delegate(): re-validating the generation right
            // before the flush narrows the duplicate-close race documented
            // above. A stale duplicate throws here, and the finally routes it
            // to discardBroken, which drops it on the same stale-generation
            // check under the pool lock.
            slot.live(generation).flush();
            flushed = true;
        } finally {
            if (flushed) {
                slot.pool().giveBack(this);
            } else {
                // flush() did not complete normally. Sender does not clear its
                // buffer on flush failure (see Sender Javadoc), and WebSocket
                // transport latches the failure for good. Either way the slot
                // is unsafe to recycle: the next borrower would inherit the
                // failed rows or a dead connection. The original throwable
                // propagates naturally once this finally returns -- no explicit
                // rethrow needed.
                slot.pool().discardBroken(this);
            }
        }
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        slot.live(generation).decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        slot.live(generation).decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        slot.live(generation).decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        slot.live(generation).decimalColumn(name, value);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        slot.live(generation).doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        slot.live(generation).doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        slot.live(generation).doubleArray(name, values);
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        slot.live(generation).doubleArray(name, array);
        return this;
    }

    @Override
    public Sender doubleColumn(CharSequence name, double value) {
        slot.live(generation).doubleColumn(name, value);
        return this;
    }

    @Override
    public boolean drain(long timeoutMillis) {
        return slot.live(generation).drain(timeoutMillis);
    }

    @Override
    public Sender floatColumn(CharSequence name, float value) {
        slot.live(generation).floatColumn(name, value);
        return this;
    }

    @Override
    public void flush() {
        slot.live(generation).flush();
    }

    @Override
    public long flushAndGetSequence() {
        return slot.live(generation).flushAndGetSequence();
    }

    @Override
    public Sender geoHashColumn(CharSequence name, long bits, int precisionBits) {
        slot.live(generation).geoHashColumn(name, bits, precisionBits);
        return this;
    }

    @Override
    public Sender geoHashColumn(CharSequence name, CharSequence value) {
        slot.live(generation).geoHashColumn(name, value);
        return this;
    }

    @Override
    public long getAckedFsn() {
        return slot.live(generation).getAckedFsn();
    }

    @Override
    public Sender intColumn(CharSequence name, int value) {
        slot.live(generation).intColumn(name, value);
        return this;
    }

    @Override
    public Sender ipv4Column(CharSequence name, int address) {
        slot.live(generation).ipv4Column(name, address);
        return this;
    }

    @Override
    public Sender ipv4Column(CharSequence name, CharSequence address) {
        slot.live(generation).ipv4Column(name, address);
        return this;
    }

    @Override
    public Sender long256Column(CharSequence name, long l0, long l1, long l2, long l3) {
        slot.live(generation).long256Column(name, l0, l1, l2, l3);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        slot.live(generation).longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        slot.live(generation).longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        slot.live(generation).longArray(name, values);
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray values) {
        slot.live(generation).longArray(name, values);
        return this;
    }

    @Override
    public Sender longColumn(CharSequence name, long value) {
        slot.live(generation).longColumn(name, value);
        return this;
    }

    @Override
    public void reset() {
        slot.live(generation).reset();
    }

    @Override
    public Sender shortColumn(CharSequence name, short value) {
        slot.live(generation).shortColumn(name, value);
        return this;
    }

    @Override
    public Sender stringColumn(CharSequence name, CharSequence value) {
        slot.live(generation).stringColumn(name, value);
        return this;
    }

    @Override
    public Sender symbol(CharSequence name, CharSequence value) {
        slot.live(generation).symbol(name, value);
        return this;
    }

    @Override
    public Sender table(CharSequence table) {
        slot.live(generation).table(table);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, long value, ChronoUnit unit) {
        slot.live(generation).timestampColumn(name, value, unit);
        return this;
    }

    @Override
    public Sender timestampColumn(CharSequence name, Instant value) {
        slot.live(generation).timestampColumn(name, value);
        return this;
    }

    @Override
    public Sender uuidColumn(CharSequence name, long lo, long hi) {
        slot.live(generation).uuidColumn(name, lo, hi);
        return this;
    }

    long generation() {
        return generation;
    }

    SenderSlot slot() {
        return slot;
    }
}
