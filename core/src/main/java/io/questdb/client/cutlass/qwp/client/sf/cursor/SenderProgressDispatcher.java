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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded inbox + lazy-started daemon thread that delivers ack-watermark
 * advances to a user-supplied {@link SenderProgressHandler} off the I/O thread.
 *
 * <h2>Why a separate thread</h2>
 * The I/O thread must never block on user code. The same rationale as the
 * {@link SenderErrorDispatcher} sibling: a slow handler (e.g. journal write)
 * cannot stall send progress. The I/O thread {@link #offer offers} the new
 * watermark and continues; the daemon dispatcher takes from the queue and
 * invokes the handler.
 *
 * <h2>Why drop-on-overflow is safe here</h2>
 * Watermarks are <em>monotonically increasing</em>. If the inbox is full and an
 * offer is dropped, the next successful offer carries a higher (or equal) FSN
 * — so any handler watching for "ackedFsn &gt;= target" still catches up. This
 * is the key reason a bounded queue is acceptable for what is otherwise a
 * high-volume signal: drops compress, they don't lose information.
 *
 * <p>Implementation detail: a {@link LinkedBlockingDeque} is used so on full
 * inbox we drop the <em>oldest</em> entry rather than the newest. The freshest
 * watermark is the one the handler cares about, and a sustained burst should
 * not bury it behind stale values.
 *
 * <h2>Lifecycle</h2>
 * The dispatcher thread is started lazily on the first successful
 * {@link #offer}, so workloads that never receive acks (none in practice) pay
 * zero thread cost. {@link #close()} is idempotent.
 *
 * <h2>Exception safety</h2>
 * Any {@link Throwable} thrown by the handler is caught and logged. The
 * dispatcher and the sender continue running.
 */
public final class SenderProgressDispatcher implements QuietCloseable {

    public static final int DEFAULT_CAPACITY = 256;
    private static final long DRAIN_DEADLINE_NANOS = 100_000_000L; // 100 ms
    private static final Logger LOG = LoggerFactory.getLogger(SenderProgressDispatcher.class);
    // Sentinel pushed during close() to nudge the dispatcher out of poll().
    // Identity-compared in the loop body; never delivered to the handler.
    // Box the sentinel so it has reference identity distinct from any real
    // value the I/O thread might offer.
    private static final Long POISON = Long.MIN_VALUE;
    private final AtomicLong dropped = new AtomicLong();
    // volatile so the producer of progress events can swap the handler
    // post-connect. Each delivery reads the current reference; concurrent
    // updates may interleave a final-old / first-new delivery, which is
    // acceptable for the watermark contract (monotonic + idempotent).
    private volatile SenderProgressHandler handler;
    private final LinkedBlockingDeque<Long> inbox;
    private final Object lock = new Object();
    private final String threadName;
    private final AtomicLong totalDelivered = new AtomicLong();
    private volatile boolean closed;
    private Thread dispatcherThread;

    public SenderProgressDispatcher(SenderProgressHandler handler, int capacity) {
        this(handler, capacity, "qdb-sf-progress-dispatcher");
    }

    public SenderProgressDispatcher(SenderProgressHandler handler, int capacity, String threadName) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must be non-null");
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.handler = handler;
        this.inbox = new LinkedBlockingDeque<>(capacity);
        this.threadName = threadName;
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            inbox.offer(POISON);
            Thread t = dispatcherThread;
            if (t != null) {
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
                            + "abandoning {} queued advances", inbox.size());
                    t.interrupt();
                }
                dispatcherThread = null;
            }
        }
    }

    /**
     * Total advances dropped since startup due to inbox-overflow. Non-zero
     * means the user's handler is slower than the ack rate — typically not a
     * correctness issue (later advances catch up), but useful as an ops signal
     * when handler latency matters.
     */
    public long getDroppedNotifications() {
        return dropped.get();
    }

    /**
     * Total advances delivered to the handler since startup. Includes calls
     * the handler threw on, since exceptions are caught and logged but the
     * delivery itself counts as "happened".
     */
    public long getTotalDelivered() {
        return totalDelivered.get();
    }

    /**
     * Replace the user-supplied handler. Effective immediately for any
     * subsequent delivery. Pass {@code null} to install the no-op default.
     * Callable both before and after {@link #offer(long) start of dispatch}.
     */
    public void setHandler(SenderProgressHandler handler) {
        this.handler = handler != null ? handler : DefaultSenderProgressHandler.INSTANCE;
    }

    /**
     * Non-blocking enqueue of an ack-watermark advance. Returns {@code true}
     * if the value will be delivered (eventually, on the dispatcher thread).
     * Returns {@code false} if the dispatcher was closed.
     *
     * <p>On a full inbox this method evicts the oldest queued value to make
     * room for the new one — the freshest watermark is what observers want.
     * The dropped-old count is bumped for ops visibility.
     *
     * <p>Lazy-starts the dispatcher thread on the first successful offer.
     */
    public boolean offer(long ackedFsn) {
        if (closed) {
            return false;
        }
        // Try the fast path first: a non-full inbox accepts immediately.
        if (inbox.offerLast(ackedFsn)) {
            if (dispatcherThread == null) {
                startDispatcherIfNeeded();
            }
            return true;
        }
        // Inbox full: evict the oldest entry and try again. We do this rather
        // than dropping the newest so the handler always sees the most recent
        // watermark even under sustained backpressure.
        inbox.pollFirst();
        dropped.incrementAndGet();
        boolean accepted = inbox.offerLast(ackedFsn);
        if (accepted && dispatcherThread == null) {
            startDispatcherIfNeeded();
        }
        return accepted;
    }

    private void dispatchLoop() {
        // Local high-water filter: skip any queued value <= the last one we
        // already delivered, in case multiple advances queued while we were
        // running the handler. The contract guarantees the user only sees
        // strictly-increasing values.
        long lastDelivered = Long.MIN_VALUE;
        while (!closed || !inbox.isEmpty()) {
            Long v;
            try {
                v = inbox.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (closed) {
                    return;
                }
                Thread.currentThread().interrupt();
                continue;
            }
            if (v == null || v.equals(POISON)) {
                continue;
            }
            long fsn = v;
            if (fsn <= lastDelivered) {
                continue;
            }
            lastDelivered = fsn;
            // Increment before invoking the handler so observers using a
            // CountDownLatch in the handler can read the updated counter
            // once their latch fires.
            totalDelivered.incrementAndGet();
            try {
                handler.onAcked(fsn);
            } catch (Throwable t) {
                LOG.error("SenderProgressHandler threw on fsn={}: {}", fsn, t.getMessage(), t);
            }
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
