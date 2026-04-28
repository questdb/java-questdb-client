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

import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.std.QuietCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded inbox + lazy-started daemon thread that delivers {@link SenderError}
 * notifications to a user-supplied {@link SenderErrorHandler} off the I/O
 * thread.
 *
 * <h2>Why a separate thread</h2>
 * The I/O thread must never block on user code. A slow handler (say, posting
 * to a remote dead-letter queue) cannot stall send progress. Instead, the I/O
 * thread {@link #offer offers} the error onto a bounded queue and continues;
 * the daemon dispatcher takes from the queue and invokes the handler.
 *
 * <h2>Backpressure</h2>
 * The queue is bounded ({@code capacity}, default 256). When full,
 * {@link #offer} returns {@code false} immediately and bumps
 * {@link #getDroppedNotifications()}. The I/O thread does NOT spin or block.
 * A non-zero dropped count means the handler is too slow to keep up — visible
 * to operators via the sender's accessor.
 *
 * <h2>Lifecycle</h2>
 * The dispatcher thread is started lazily on the first successful
 * {@link #offer}, so workloads that never produce server errors pay zero thread
 * cost. {@link #close()} is idempotent: it stops the dispatcher, drains
 * remaining queue entries with a short deadline, and joins the thread.
 *
 * <h2>Exception safety</h2>
 * Any {@link Throwable} thrown by the handler is caught and logged by the
 * dispatcher. The dispatcher and the sender continue running.
 */
public final class SenderErrorDispatcher implements QuietCloseable {

    public static final int DEFAULT_CAPACITY = 256;
    private static final long DRAIN_DEADLINE_NANOS = 100_000_000L; // 100 ms
    private static final Logger LOG = LoggerFactory.getLogger(SenderErrorDispatcher.class);
    // Sentinel pushed during close() to nudge the dispatcher out of take().
    // Identity-compared in the loop body; never delivered to the handler.
    private static final SenderError POISON = new SenderError(
            SenderError.Category.UNKNOWN, SenderError.Policy.HALT,
            SenderError.NO_STATUS_BYTE, null, SenderError.NO_MESSAGE_SEQUENCE,
            -1L, -1L, null, 0L);
    private final AtomicLong dropped = new AtomicLong();
    private final SenderErrorHandler handler;
    private final BlockingQueue<SenderError> inbox;
    // Threads are started lazily under this monitor; takes the same role as
    // SegmentManager.start() — first offer() that observes a null thread
    // wins the race to spawn it.
    private final Object lock = new Object();
    private final String threadName;
    private final AtomicLong totalDelivered = new AtomicLong();
    private volatile boolean closed;
    private Thread dispatcherThread;

    public SenderErrorDispatcher(SenderErrorHandler handler) {
        this(handler, DEFAULT_CAPACITY, "qdb-sf-error-dispatcher");
    }

    public SenderErrorDispatcher(SenderErrorHandler handler, int capacity) {
        this(handler, capacity, "qdb-sf-error-dispatcher");
    }

    public SenderErrorDispatcher(SenderErrorHandler handler, int capacity, String threadName) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must be non-null");
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.handler = handler;
        this.inbox = new ArrayBlockingQueue<>(capacity);
        this.threadName = threadName;
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            // Wake the dispatcher even if the inbox is empty — POISON also
            // forces it past any pending poll() without losing real entries
            // already queued (they're delivered before POISON since the
            // queue is FIFO). The offer's return value is intentionally
            // ignored: if the inbox is at capacity the dispatcher will
            // still wake on its 100ms poll timeout and re-check `closed`,
            // so failure to enqueue POISON only adds at most one tick of
            // shutdown latency — not a correctness issue.
            //noinspection ResultOfMethodCallIgnored
            inbox.offer(POISON);
            Thread t = dispatcherThread;
            if (t != null) {
                long deadline = System.nanoTime() + DRAIN_DEADLINE_NANOS;
                long remainingMillis;
                while ((remainingMillis = (deadline - System.nanoTime()) / 1_000_000L) > 0) {
                    try {
                        t.join(remainingMillis);
                        // join() returned: either the thread exited, or the
                        // requested timeout elapsed. Either way we're done
                        // waiting — the next loop iter would compute a
                        // non-positive remainingMillis and exit anyway.
                        break;
                    } catch (InterruptedException ignored) {
                        // Spurious interrupt while waiting on shutdown —
                        // re-flag the thread and retry join() against the
                        // refreshed deadline so a stray interrupt cannot
                        // cut shutdown short.
                        Thread.currentThread().interrupt();
                    }
                }
                if (t.isAlive()) {
                    LOG.warn("error-dispatcher thread did not exit within drain deadline; "
                            + "abandoning {} queued errors", inbox.size());
                    t.interrupt();
                }
                dispatcherThread = null;
            }
        }
    }

    /**
     * Total errors delivered via inbox-overflow drop since startup. Non-zero
     * means the user's handler is slower than the error rate — typically a
     * symptom of a misbehaving handler or a misconfigured server. Reported by
     * the sender for ops dashboards.
     */
    public long getDroppedNotifications() {
        return dropped.get();
    }

    /**
     * Total errors delivered to the handler since startup. Includes errors
     * the handler threw on, since exceptions are caught and logged but the
     * delivery itself counts as "happened".
     */
    public long getTotalDelivered() {
        return totalDelivered.get();
    }

    /**
     * Non-blocking enqueue. Returns {@code true} if the error will be
     * delivered to the handler (eventually, on the dispatcher thread). Returns
     * {@code false} if the inbox was full or the dispatcher was closed —
     * caller's only obligation is to not block.
     *
     * <p>Lazy-starts the dispatcher thread on the first successful offer.
     */
    public boolean offer(SenderError error) {
        if (closed || error == null) {
            return false;
        }
        boolean accepted = inbox.offer(error);
        if (!accepted) {
            dropped.incrementAndGet();
            return false;
        }
        // Common case after the first offer: thread already running, hot
        // path is one volatile read. Lazy start happens once per dispatcher
        // lifetime.
        if (dispatcherThread == null) {
            startDispatcherIfNeeded();
        }
        return true;
    }

    private void dispatchLoop() {
        while (!closed || !inbox.isEmpty()) {
            SenderError err;
            try {
                err = inbox.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (closed) {
                    return;
                }
                Thread.currentThread().interrupt();
                continue;
            }
            if (err == null || err == POISON) {
                // POISON is enqueued by close() to nudge us out of poll().
                // Closed-check at the loop head will catch the rest.
                continue;
            }
            try {
                handler.onError(err);
            } catch (Throwable t) {
                LOG.error("SenderErrorHandler threw on {}: {}", err, t.getMessage(), t);
            } finally {
                totalDelivered.incrementAndGet();
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
