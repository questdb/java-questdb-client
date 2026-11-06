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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderProgressHandler;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Single-slot watermark mailbox + lazy-started daemon thread that delivers
 * ack-watermark advances to a user-supplied {@link SenderProgressHandler} off
 * the I/O thread.
 *
 * <h2>Why a separate thread</h2>
 * The I/O thread must never block on user code. The same rationale as the
 * {@link SenderErrorDispatcher} sibling: a slow handler (e.g. journal write)
 * cannot stall send progress. The I/O thread {@link #offer offers} the new
 * watermark and continues; the daemon dispatcher takes the latest value and
 * invokes the handler.
 *
 * <h2>Why a single-slot mailbox is sufficient</h2>
 * Watermarks are <em>monotonically increasing</em>. If the handler is busy
 * when several offers arrive in succession, only the highest watermark is
 * worth delivering — earlier advances are subsumed by it. The mailbox is
 * therefore a single {@code volatile long} that the producer overwrites
 * unconditionally; the consumer reads it and delivers anything strictly
 * greater than its previous delivery.
 *
 * <p>This collapses a high-volume signal to a constant memory footprint and
 * keeps the I/O thread on a zero-allocation hot path: no autoboxing, no
 * per-offer node allocation, no {@code java.util.*} structures.
 *
 * <h2>Lifecycle</h2>
 * The dispatcher thread is started lazily on the first successful
 * {@link #offer} made while a real handler is installed. With the no-op
 * {@link DefaultSenderProgressHandler} (the common case: nobody called
 * {@code setProgressHandler}) an offer only records the watermark, so acks
 * cost no thread, no unpark and no syscall; installing a handler later
 * delivers the newest recorded watermark. {@link #close()} is idempotent.
 *
 * <h2>Exception safety</h2>
 * Any {@link Throwable} thrown by the handler is caught and logged. The
 * dispatcher and the sender continue running.
 */
public final class SenderProgressDispatcher implements QuietCloseable {

    private static final long DRAIN_DEADLINE_NANOS = 100_000_000L; // 100 ms
    private static final Logger LOG = LoggerFactory.getLogger(SenderProgressDispatcher.class);
    // Sentinel for "no value ever offered". Real FSNs are non-negative, so
    // Long.MIN_VALUE is unambiguous and the dispatcher's "deliver if greater
    // than lastDelivered" check naturally skips it.
    private static final long EMPTY = Long.MIN_VALUE;
    // volatile so the producer of progress events can swap the handler
    // post-connect. Each delivery reads the current reference; concurrent
    // updates may interleave a final-old / first-new delivery, which is
    // acceptable for the watermark contract (monotonic + idempotent).
    private volatile SenderProgressHandler handler;
    private final Object lock = new Object();
    private final String threadName;
    private final AtomicLong totalDelivered = new AtomicLong();
    private final AtomicLong totalOffered = new AtomicLong();
    private volatile boolean closed;
    // volatile to give the off-lock reads in offer() a happens-before with
    // the write under `lock` in startDispatcherIfNeeded(). Without it the
    // double-checked first-null guard is a JMM race -- benign in practice
    // (the synchronized re-check covers double-start; a stale-null unpark
    // is just skipped) but spec-incorrect.
    private volatile Thread dispatcherThread;
    // Single-slot inbox. Producer (I/O thread) overwrites unconditionally;
    // consumer reads via volatile and skips when the value has already been
    // delivered. Monotonic FSNs make overwrite safe -- the latest value
    // subsumes any prior undelivered value.
    private volatile long latestFsn = EMPTY;

    public SenderProgressDispatcher(SenderProgressHandler handler) {
        this(handler, "qdb-sf-progress-dispatcher");
    }

    public SenderProgressDispatcher(SenderProgressHandler handler, String threadName) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must be non-null");
        }
        this.handler = handler;
        this.threadName = threadName;
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            Thread t = dispatcherThread;
            if (t != null) {
                // Wake the dispatcher in case it is parked. After observing
                // closed=true with no pending value, the loop returns.
                LockSupport.unpark(t);
                long deadline = System.nanoTime() + DRAIN_DEADLINE_NANOS;
                long remainingMillis;
                while ((remainingMillis = (deadline - System.nanoTime()) / 1_000_000L) > 0) {
                    try {
                        t.join(remainingMillis);
                        break;
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (t.isAlive()) {
                    LOG.warn("progress-dispatcher thread did not exit within drain deadline; "
                            + "abandoning latest pending advance");
                    t.interrupt();
                }
                dispatcherThread = null;
            }
        }
    }

    /**
     * Watermark advances overwritten before the handler observed them, since
     * startup. Offers made while the no-op default handler was installed are
     * not counted: there was no handler to observe them. Computed as
     * {@code totalOffered - totalDelivered}; a small non-zero value typically
     * reflects a single in-flight advance, while a persistently growing value
     * signals a handler slower than the ack rate.
     * Useful as an ops signal when handler latency matters.
     */
    public long getDroppedNotifications() {
        long offered = totalOffered.get();
        long delivered = totalDelivered.get();
        long diff = offered - delivered;
        return diff > 0L ? diff : 0L;
    }

    /**
     * Total advances delivered to the handler since startup. Includes calls
     * the handler threw on, since exceptions are caught and logged but the
     * delivery itself counts as "happened".
     */
    public long getTotalDelivered() {
        return totalDelivered.get();
    }

    @TestOnly
    public Thread getWorkerThreadForTesting() {
        return dispatcherThread;
    }

    /**
     * Replace the user-supplied handler. Effective immediately for any
     * subsequent delivery. Pass {@code null} to install the no-op default.
     * Callable both before and after {@link #offer(long) start of dispatch}.
     */
    public void setHandler(SenderProgressHandler handler) {
        SenderProgressHandler h = handler != null ? handler : DefaultSenderProgressHandler.INSTANCE;
        this.handler = h;
        // Watermarks offered under the no-op default were skipped (see
        // offer()); hand the newest one to the real handler now. The
        // volatile handler write above / latestFsn read below pair with the
        // reverse order in offer(), so a concurrent offer cannot be lost.
        if (h != DefaultSenderProgressHandler.INSTANCE && latestFsn != EMPTY) {
            wakeDispatcher();
        }
    }

    /**
     * Non-blocking enqueue of an ack-watermark advance. Returns {@code true}
     * if the value will be visible to the dispatcher (eventually delivered,
     * possibly subsumed by a later advance). Returns {@code false} if the
     * dispatcher was closed.
     *
     * <p>Watermarks are monotonic, so the slot is overwritten unconditionally:
     * a later advance subsumes any earlier one not yet observed by the
     * handler. Lazy-starts the dispatcher thread on the first offer.
     */
    public boolean offer(long ackedFsn) {
        if (closed) {
            return false;
        }
        latestFsn = ackedFsn;
        if (handler == DefaultSenderProgressHandler.INSTANCE) {
            // No-op handler installed: nothing to deliver, so no thread
            // start and no unpark. The value stays in the slot for a
            // handler installed later (see setHandler()).
            return true;
        }
        totalOffered.incrementAndGet();
        wakeDispatcher();
        return true;
    }

    private void wakeDispatcher() {
        Thread t = dispatcherThread;
        if (t == null) {
            startDispatcherIfNeeded();
            t = dispatcherThread;
        }
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    private void dispatchLoop() {
        // Strictly-monotonic delivery: skip the slot when its value is not
        // greater than the last value handed to the handler.
        long lastDelivered = EMPTY;
        while (true) {
            long v = latestFsn;
            if (v > lastDelivered) {
                lastDelivered = v;
                // Increment before invoking the handler so observers using a
                // CountDownLatch in the handler can read the updated counter
                // once their latch fires.
                totalDelivered.incrementAndGet();
                SenderProgressHandler h = handler;
                try {
                    h.onAcked(v);
                } catch (Throwable t) {
                    LOG.error("SenderProgressHandler threw on fsn={}: {}", v, t.getMessage(), t);
                }
                continue;
            }
            if (closed) {
                return;
            }
            // No new value; park until offer() or close() unparks us. Both write
            // their state (latestFsn / closed) before the unpark and this loop
            // re-reads it before parking, so no wake can be lost.
            // Spurious wakeups loop back to the top and re-check.
            LockSupport.park(this);
        }
    }

    private void startDispatcherIfNeeded() {
        synchronized (lock) {
            if (closed || dispatcherThread != null) {
                return;
            }
            Thread t = new Thread(this::dispatchLoop, threadName);
            t.setDaemon(true);
            dispatcherThread = t;
            t.start();
        }
    }
}
