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

import io.questdb.client.ConnectionHealth;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Thin per-borrow handle returned by {@link SenderPool#borrow()}. A fresh
 * instance is created on every borrow, capturing the immutable lease
 * {@code generation} stamped by {@code borrow()}; it forwards every
 * {@link Sender} call to the reused {@link SenderSlot}'s delegate, validating
 * that generation first via {@link SenderSlot#live(long)}.
 * <p>
 * Behaviour difference from a raw Sender: {@link #close()} makes the lease's
 * rows recoverable and returns the slot to the pool. It publishes the rows into
 * the slot's open record without shipping them, and ships a frame only when the
 * commit window closes or an auto-flush threshold trips, usually the age one;
 * otherwise the rows wait for a later append or release, for the record to fill,
 * or for the pool's housekeeper settle. The underlying Sender is only truly
 * closed when {@link io.questdb.client.QuestDB#close()} shuts the pool down.
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
    public void autoFlushIfDue() {
        slot.live(generation).autoFlushIfDue();
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

    @Override
    public void checkError() {
        slot.live(generation).checkError();
    }

    /**
     * Returns this lease's slot to the pool. Close makes the rows recoverable and
     * releases, flush ships, drain waits: the release publishes the rows appended
     * through this lease into the slot's open record ({@link Sender#publishPending()})
     * without shipping them. Published rows survive a later lease's {@link #reset()}
     * and, with {@code sf_dir}, a crash of the process: recovery replays them from
     * the slot's store-and-forward directory. Recoverable means in the client's
     * ring, not acknowledged by the server; {@link #flush()} and then
     * {@link #drain(long)} is the point where the server has the rows. The release
     * ships a frame only when the commit window closes (see below) or an auto-flush
     * threshold trips, usually the age one, the oldest row older than
     * {@code auto_flush_interval}; otherwise the rows wait in the open record for a
     * later append or release, for the record to fill, or for the pool's
     * housekeeper settle. A sender in full-dictionary mode (its slot's symbol
     * dictionary file could not be opened or written) has no open record: its
     * publish is a {@link Sender#flush()}, so each release ships the lease's rows
     * in a frame of its own, which keeps them recoverable at the cost a release had
     * before the open record. {@link #reset()} on a lease discards only that
     * lease's rows. Does not close the underlying {@link Sender}; that only
     * happens when the owning {@code QuestDB} is closed. If the pool is already
     * shutting down when this lease comes home, the rows are flushed here and the
     * underlying Sender is torn down on the calling thread.
     * <p>
     * A row left in progress is cancelled before the publish, so nothing
     * half-built is ever published, the slot is returned, and then a
     * {@link LineSenderException} is thrown so the mistake is visible. A
     * delegate that has latched a terminal error is retired instead of being
     * returned, and a new {@link LineSenderException} is thrown here whose
     * cause is the delegate's error and whose message carries the number of rows
     * not yet shipped at the retirement. The count lives on the wrapper of a
     * non-server failure; the typed server branch below carries none. A lease body
     * that already threw the typed error keeps it as the primary exception, with
     * close's wrapper suppressed. The typed error and its classification live on
     * the cause.
     * <p>
     * A server batch cap rejection ({@link BatchTooLargeForCapException}, retained
     * by an auto-flush or thrown by the release's own flush) can only come from a
     * sender in full-dictionary mode: an open record never grows past the frame
     * bound, and a row too large for it fails on its own {@code at()}. It does not
     * retire the slot at once. The rejected batch can hold mostly rows earlier
     * leases left. The lease is rolled back to its borrow mark, which drops only
     * this lease's rows, and the flush is retried: when the rows left by earlier
     * leases ship, the slot is returned and a {@link LineSenderException} whose
     * cause is the rejection names the number of this lease's rows dropped. Only
     * an inherited batch the cap still rejects retires the slot, with the wrapper
     * described above.
     * <p>
     * Idempotent for sequential reuse: a stale generation (the lease was
     * already returned and the slot possibly re-borrowed) is a no-op. Concurrent
     * double-close of ONE lease from two threads is outside the Sender
     * threading contract.
     * <p>
     * With a commit window ({@link SenderPool#setCommitWindow}), the release that
     * fills the window (rows appended or age of the open group) flushes after the
     * publish, which seals the record unflagged, or sends the commit message, and
     * commits the group; the other releases only publish, and an auto-flush
     * threshold that trips at one of them ships a frame flagged defer-commit. The
     * window's age is measured from the first release that found work. A failed
     * publish or ship retires the slot and throws the wrapper described above.
     * <p>
     * Close-time errors are wrapped: the delegate's error is the cause. A latched
     * {@link LineSenderServerException} is rethrown as a new one carrying the same
     * {@code SenderError}. While the pool is shutting down, the rows are flushed here
     * so a ship failure propagates to the caller.
     */
    @Override
    public void close() {
        if (generation != slot.generation()) {
            return;
        }
        SenderPool pool = slot.pool();
        boolean cancelledRow = false;
        boolean recycle = false;
        RuntimeException failure = null;
        BatchTooLargeForCapException capRejection = null;
        int droppedLeaseRows = 0;
        int pendingRowsAtRetire = 0;
        try {
            // live(), not delegate(): re-validate the generation right before
            // touching the delegate, narrowing the duplicate-close race.
            Sender delegate = slot.live(generation);
            try {
                delegate.checkError();
                if (delegate.hasRowInProgress()) {
                    delegate.cancelRow();
                    cancelledRow = true;
                }
                if (pool.isClosed()) {
                    // Pool shutting down: ship here so a failure reaches the caller.
                    delegate.flush();
                } else {
                    ship(pool, delegate);
                }
                recycle = true;
            } catch (RuntimeException e) {
                // The delegate rethrows its latched instance, which a lease
                // body may already have thrown: wrap it below, after the
                // slot is retired.
                failure = e;
            }
            if (failure instanceof BatchTooLargeForCapException) {
                // Only a sender in full-dictionary mode can raise it: an open record
                // never grows past the frame bound. On that frame path the rejection
                // comes, for example, from the lease's own rows pushing the batch
                // over the cap through an explicit flush() with the byte trigger off,
                // or from a held row whose own frame exceeds the cap. The
                // rejected batch can hold mostly rows earlier leases left.
                // Drop only this lease's rows and retry: the slot stays in service
                // when the rest ships, and an inherited batch the cap still
                // rejects falls through to the retirement below.
                try {
                    int pendingBeforeRollback = delegate.pendingRowCount();
                    delegate.resetToSavepoint();
                    droppedLeaseRows = pendingBeforeRollback - delegate.pendingRowCount();
                    delegate.flush();
                    slot.markCommitted(delegate.rowsAppended());
                    capRejection = (BatchTooLargeForCapException) failure;
                    failure = null;
                    recycle = true;
                } catch (RuntimeException e) {
                    failure = e;
                }
            }
        } finally {
            if (recycle) {
                pool.giveBack(this);
            } else {
                // The probe, the cancel or the ship threw: the delegate has
                // latched an error, the flush failed, or the generation went
                // stale. Retire it; a wrapper around the original propagates
                // once this finally returns. Rows a rollback above dropped are
                // lost with the slot as well.
                try {
                    pendingRowsAtRetire = droppedLeaseRows + slot.delegate().pendingRowCount();
                } catch (Throwable ignored) {
                    // best effort: the count is only for the message
                }
                pool.discardBroken(this);
            }
        }
        if (failure != null) {
            if (failure instanceof LineSenderServerException) {
                LineSenderServerException typed = new LineSenderServerException(((LineSenderServerException) failure).getServerError());
                typed.initCause(failure);
                throw typed;
            }
            // A cap rejection's message ends with the standalone remedy, which does
            // not apply to a retired slot: report only its detail.
            String msg = failure instanceof BatchTooLargeForCapException
                    ? ((BatchTooLargeForCapException) failure).getDetail()
                    : failure.getMessage();
            boolean retryable = failure instanceof LineSenderException && ((LineSenderException) failure).isRetryable();
            LineSenderException wrapper = new LineSenderException("pooled sender close failed, slot retired with "
                    + pendingRowsAtRetire + " row(s) not yet shipped (recoverable from the slot's store-and-forward "
                    + "directory when sf_dir is set): " + msg, retryable);
            wrapper.initCause(failure);
            throw wrapper;
        }
        if (capRejection != null) {
            LineSenderException wrapper = new LineSenderException("pooled sender close dropped " + droppedLeaseRows
                    + " row(s) of this lease after the server batch cap rejected the batch; rows left by earlier leases shipped",
                    capRejection.isRetryable());
            wrapper.initCause(capRejection);
            throw wrapper;
        }
        if (cancelledRow) {
            throw new LineSenderException("row in progress at close(): the row was cancelled and the sender returned to the pool; "
                    + "finish rows with at(), atNow() or cancelRow() before close()");
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
        Sender delegate = slot.live(generation);
        boolean drained = delegate.drain(timeoutMillis);
        slot.markCommitted(delegate.rowsAppended());
        return drained;
    }

    @Override
    public Sender floatColumn(CharSequence name, float value) {
        slot.live(generation).floatColumn(name, value);
        return this;
    }

    @Override
    public void flush() {
        Sender delegate = slot.live(generation);
        delegate.flush();
        slot.markCommitted(delegate.rowsAppended());
    }

    @Override
    public long flushAndGetSequence() {
        Sender delegate = slot.live(generation);
        long sequence = delegate.flushAndGetSequence();
        slot.markCommitted(delegate.rowsAppended());
        return sequence;
    }

    @Override
    public long flushDeferred() {
        return slot.live(generation).flushDeferred();
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
    public boolean hasDeferredFrames() {
        return slot.live(generation).hasDeferredFrames();
    }

    @Override
    public boolean hasRowInProgress() {
        return slot.live(generation).hasRowInProgress();
    }

    @Override
    public ConnectionHealth health() {
        return slot.live(generation).health();
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
    public int pendingRowCount() {
        return slot.live(generation).pendingRowCount();
    }

    /**
     * Publishes the rows appended so far into the slot's open record without
     * shipping them, exactly what {@link #close()} does first: they become
     * recoverable and a later {@link #reset()} keeps them. A sender in
     * full-dictionary mode has no open record and flushes instead. Forwards to
     * the delegate's {@link Sender#publishPending()}, not to this handle's
     * {@link #flush()}, so it neither ships a frame nor commits a window.
     */
    @Override
    public void publishPending() {
        slot.live(generation).publishPending();
    }

    /**
     * Discards the rows appended through this lease and keeps the rows earlier
     * leases left in the slot's buffer: the pool marks a savepoint at borrow and
     * this rolls back to it ({@link Sender#resetToSavepoint()}). After a flush
     * inside the lease, explicit or by auto-flush, every pending row is this
     * lease's own and all of them go. Use it to drop a batch the server cannot
     * accept, for example after a {@code BatchTooLargeForCapException}.
     */
    @Override
    public void reset() {
        slot.live(generation).resetToSavepoint();
    }

    @Override
    public void resetSymbolDictionary() {
        slot.live(generation).resetSymbolDictionary();
    }

    @Override
    public long rowsAppended() {
        return slot.live(generation).rowsAppended();
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
    public boolean tryFlush() {
        Sender delegate = slot.live(generation);
        boolean flushed = delegate.tryFlush();
        if (flushed) {
            slot.markCommitted(delegate.rowsAppended());
        }
        return flushed;
    }

    @Override
    public Sender uuidColumn(CharSequence name, long lo, long hi) {
        slot.live(generation).uuidColumn(name, lo, hi);
        return this;
    }

    long generation() {
        return generation;
    }

    @TestOnly
    public Sender getDelegateForTesting() {
        return slot.delegate();
    }

    @TestOnly
    public int getSlotIndexForTesting() {
        return slot.slotIndex();
    }

    @TestOnly
    public boolean hasSameSlotForTesting(PooledSender that) {
        return slot == that.slot;
    }

    SenderSlot slot() {
        return slot;
    }

    /**
     * What a release does once the probe passed and a row in progress was
     * cancelled:
     * <ul>
     *   <li>{@code publishPending()}: makes the lease's rows recoverable in the
     *       open record without shipping them (a flush in full-dictionary
     *       mode);</li>
     *   <li>commit window set and closing now: {@code flush()}, which seals the
     *       record unflagged, or sends the commit message, and commits the
     *       group;</li>
     *   <li>otherwise {@code autoFlushIfDue()}: seals the record once the oldest
     *       row is older than {@code auto_flush_interval} or another auto-flush
     *       threshold trips.</li>
     * </ul>
     * The clock is read only with a commit window.
     */
    private void ship(SenderPool pool, Sender delegate) {
        delegate.publishPending();
        if (pool.hasCommitWindow()) {
            boolean closes = slot.windowCloses(delegate.rowsAppended(), delegate.hasDeferredFrames(),
                    System.nanoTime(), pool.commitRows(), pool.commitIntervalNanos());
            if (closes) {
                delegate.flush();
                slot.markCommitted(delegate.rowsAppended());
                return;
            }
        }
        delegate.autoFlushIfDue();
    }
}
