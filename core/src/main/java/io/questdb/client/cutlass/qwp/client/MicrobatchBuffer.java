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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * A buffer for accumulating ILP data into microbatches before sending.
 * <p>
 * This class implements a state machine for buffer lifecycle management in the
 * double-buffering scheme used by {@link QwpWebSocketSender}:
 * <pre>
 * Buffer States:
 * ┌─────────────┐    seal()     ┌─────────────┐    markSending()  ┌─────────────┐
 * │   FILLING   │──────────────►│   SEALED    │──────────────────►│   SENDING   │
 * │ (user owns) │               │ (in queue)  │                   │ (I/O owns)  │
 * └─────────────┘               └─────────────┘                   └──────┬──────┘
 *        ▲                                                               │
 *        │                         markRecycled()                        │
 *        └───────────────────────────────────────────────────────────────┘
 *                              (after send complete)
 * </pre>
 * <p>
 * Thread safety: This class is NOT thread-safe for concurrent writes. However, it
 * supports safe hand-over between user thread and I/O thread through the state
 * machine. State transitions use volatile fields to ensure visibility.
 */
public class MicrobatchBuffer implements QuietCloseable {

    // Buffer states
    public static final int STATE_FILLING = 0;
    public static final int STATE_RECYCLED = 3;
    public static final int STATE_SEALED = 1;
    public static final int STATE_SENDING = 2;
    private static final AtomicLong nextBatchId = new AtomicLong();
    // Flush trigger thresholds
    // Batch identification
    private long batchId;
    private int bufferCapacity;
    private int bufferPos;
    // Native memory buffer
    private long bufferPtr;
    private long firstRowTimeNanos;
    // For waiting on recycle (user thread waits for I/O thread to finish)
    private volatile Thread recycleWaiter;
    // Row tracking
    private int rowCount;
    // State machine
    private volatile int state = STATE_FILLING;

    /**
     * Creates a new MicrobatchBuffer with default thresholds (no auto-flush).
     *
     * @param initialCapacity initial buffer size in bytes
     */
    public MicrobatchBuffer(int initialCapacity) {
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("initialCapacity must be positive");
        }
        this.bufferCapacity = initialCapacity;
        this.bufferPtr = Unsafe.malloc(initialCapacity, MemoryTag.NATIVE_ILP_RSS);
        this.bufferPos = 0;
        this.rowCount = 0;
        this.firstRowTimeNanos = 0;
        this.batchId = nextBatchId.getAndIncrement();
    }

    /**
     * Returns a human-readable name for the given state.
     */
    public static String stateName(int state) {
        switch (state) {
            case STATE_FILLING:
                return "FILLING";
            case STATE_SEALED:
                return "SEALED";
            case STATE_SENDING:
                return "SENDING";
            case STATE_RECYCLED:
                return "RECYCLED";
            default:
                return "UNKNOWN(" + state + ")";
        }
    }

    /**
     * Waits for the buffer to be recycled (transition to RECYCLED state).
     * Only the user thread should call this.
     */
    public void awaitRecycled() {
        final Thread current = Thread.currentThread();
        recycleWaiter = current;
        try {
            while (state != STATE_RECYCLED) {
                LockSupport.park(this);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } finally {
            if (recycleWaiter == current) {
                recycleWaiter = null;
            }
        }
    }

    /**
     * Waits for the buffer to be recycled with a timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit
     * @return true if recycled, false if timeout elapsed
     */
    public boolean awaitRecycled(long timeout, TimeUnit unit) {
        if (state == STATE_RECYCLED) {
            // fast-path
            return true;
        }

        final Thread current = Thread.currentThread();
        recycleWaiter = current;
        final long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        try {
            while (state != STATE_RECYCLED) {
                final long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                LockSupport.parkNanos(this, remaining);
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            if (recycleWaiter == current) {
                recycleWaiter = null;
            }
        }
    }

    @Override
    public void close() {
        if (bufferPtr != 0) {
            Unsafe.free(bufferPtr, bufferCapacity, MemoryTag.NATIVE_ILP_RSS);
            bufferPtr = 0;
            bufferCapacity = 0;
        }
    }

    /**
     * Ensures the buffer has at least the specified capacity.
     * Grows the buffer if necessary.
     *
     * @param requiredCapacity minimum required capacity
     */
    public void ensureCapacity(int requiredCapacity) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot resize when state is " + stateName(state));
        }
        if (requiredCapacity > bufferCapacity) {
            int newCapacity = (int) Math.min(Math.max((long) bufferCapacity * 2, requiredCapacity), Integer.MAX_VALUE);
            bufferPtr = Unsafe.realloc(bufferPtr, bufferCapacity, newCapacity, MemoryTag.NATIVE_ILP_RSS);
            bufferCapacity = newCapacity;
        }
    }

    /**
     * Returns the age of the first row in nanoseconds, or 0 if no rows.
     */
    public long getAgeNanos() {
        if (rowCount == 0) {
            return 0;
        }
        return System.nanoTime() - firstRowTimeNanos;
    }

    /**
     * Returns the batch ID for this buffer.
     */
    public long getBatchId() {
        return batchId;
    }

    /**
     * Returns the buffer capacity.
     */
    public int getBufferCapacity() {
        return bufferCapacity;
    }

    /**
     * Returns the current write position in the buffer.
     */
    public int getBufferPos() {
        return bufferPos;
    }

    /**
     * Returns the buffer pointer for writing data.
     * Only valid when state is FILLING.
     */
    public long getBufferPtr() {
        return bufferPtr;
    }

    /**
     * Returns the number of rows in this buffer.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns the current state.
     */
    public int getState() {
        return state;
    }

    /**
     * Returns true if the buffer has any data.
     */
    public boolean hasData() {
        return bufferPos > 0;
    }

    /**
     * Increments the row count and records the first row time if this is the first row.
     */
    public void incrementRowCount() {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot increment row count when state is " + stateName(state));
        }
        if (rowCount == 0) {
            firstRowTimeNanos = System.nanoTime();
        }
        rowCount++;
    }

    /**
     * Returns true if the buffer is in FILLING state (available for writing).
     */
    public boolean isFilling() {
        return state == STATE_FILLING;
    }

    /**
     * Returns true if the buffer is currently in use (not available for the user thread).
     */
    public boolean isInUse() {
        int s = state;
        return s == STATE_SEALED || s == STATE_SENDING;
    }

    /**
     * Returns true if the buffer is in RECYCLED state (available for reset).
     */
    public boolean isRecycled() {
        return state == STATE_RECYCLED;
    }

    /**
     * Returns true if the buffer is in SEALED state (ready to send).
     */
    public boolean isSealed() {
        return state == STATE_SEALED;
    }

    /**
     * Returns true if the buffer is in SENDING state (being sent by I/O thread).
     */
    public boolean isSending() {
        return state == STATE_SENDING;
    }

    /**
     * Marks the buffer as recycled, transitioning from SENDING to RECYCLED.
     * This signals to the user thread that the buffer can be reused.
     * Only the I/O thread should call this.
     *
     * @throws IllegalStateException if not in SENDING state
     */
    public void markRecycled() {
        if (state != STATE_SENDING) {
            throw new IllegalStateException("Cannot mark recycled in state " + stateName(state));
        }
        state = STATE_RECYCLED;
        Thread w = recycleWaiter;
        if (w != null) {
            LockSupport.unpark(w);
        }
    }

    /**
     * Marks the buffer as being sent, transitioning from SEALED to SENDING.
     * Only the I/O thread should call this.
     *
     * @throws IllegalStateException if not in SEALED state
     */
    public void markSending() {
        if (state != STATE_SEALED) {
            throw new IllegalStateException("Cannot mark sending in state " + stateName(state));
        }
        state = STATE_SENDING;
    }

    /**
     * Resets the buffer to FILLING state, clearing all data.
     * Only valid when in RECYCLED state or when the buffer is fresh.
     *
     * @throws IllegalStateException if in SEALED or SENDING state
     */
    public void reset() {
        int s = state;
        if (s == STATE_SEALED || s == STATE_SENDING) {
            throw new IllegalStateException("Cannot reset buffer in state " + stateName(s));
        }
        bufferPos = 0;
        rowCount = 0;
        firstRowTimeNanos = 0;
        batchId = nextBatchId.getAndIncrement();
        recycleWaiter = null;
        state = STATE_FILLING;
    }

    /**
     * Rolls back a seal operation, transitioning from SEALED back to FILLING.
     * <p>
     * Used when enqueue fails after a buffer has been sealed but before ownership
     * was transferred to the I/O thread.
     *
     * @throws IllegalStateException if not in SEALED state
     */
    public void rollbackSealForRetry() {
        if (state != STATE_SEALED) {
            throw new IllegalStateException("Cannot rollback seal in state " + stateName(state));
        }
        state = STATE_FILLING;
    }

    /**
     * Seals the buffer, transitioning from FILLING to SEALED.
     * After sealing, no more data can be written.
     * Only the user thread should call this.
     *
     * @throws IllegalStateException if not in FILLING state
     */
    public void seal() {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot seal buffer in state " + stateName(state));
        }
        state = STATE_SEALED;
    }

    /**
     * Sets the buffer position after external writes.
     * Only valid when state is FILLING.
     *
     * @param pos new position
     */
    public void setBufferPos(int pos) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot set position when state is " + stateName(state));
        }
        if (pos < 0 || pos > bufferCapacity) {
            throw new IllegalArgumentException("Position out of bounds: " + pos);
        }
        this.bufferPos = pos;
    }

    @Override
    public String toString() {
        return "MicrobatchBuffer{" +
                "batchId=" + batchId +
                ", state=" + stateName(state) +
                ", rows=" + rowCount +
                ", bytes=" + bufferPos +
                ", capacity=" + bufferCapacity +
                '}';
    }

    /**
     * Writes bytes to the buffer at the current position.
     * Grows the buffer if necessary.
     *
     * @param src    source address
     * @param length number of bytes to write
     */
    public void write(long src, int length) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot write when state is " + stateName(state));
        }
        ensureCapacity((int) Math.min((long) bufferPos + length, Integer.MAX_VALUE));
        Unsafe.getUnsafe().copyMemory(src, bufferPtr + bufferPos, length);
        bufferPos += length;
    }

    /**
     * Writes a single byte to the buffer.
     *
     * @param b byte to write
     */
    public void writeByte(byte b) {
        if (state != STATE_FILLING) {
            throw new IllegalStateException("Cannot write when state is " + stateName(state));
        }
        ensureCapacity((int) Math.min((long) bufferPos + 1, Integer.MAX_VALUE));
        Unsafe.getUnsafe().putByte(bufferPtr + bufferPos, b);
        bufferPos++;
    }
}
