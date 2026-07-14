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

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * The inbox is bounded ({@code capacity}, default 256). When full, {@link #offer}
 * drops the oldest entry to admit the new one and bumps
 * {@link #getDroppedNotifications()}. The I/O thread does NOT spin or block.
 * A non-zero dropped count means the handler is too slow to keep up — visible
 * to operators via the sender's accessor. Drop-oldest is mandated by
 * sf-client.md section 14.6: watermarks are monotonic, so the latest entry is
 * always the most informative and drops compress information rather than
 * lose it.
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
            SenderError.Category.UNKNOWN, SenderError.Policy.TERMINAL,
            SenderError.NO_STATUS_BYTE, null, SenderError.NO_MESSAGE_SEQUENCE,
            -1L, -1L, null, 0L);
    // Set the first time the dispatcher delivers an error to a non-default
    // handler. Stays true even if the user later swaps the handler back to
    // the default -- the signal is "did the user-installed handler ever see
    // this stream of errors". Kept for ops/diagnostic visibility; NOT used
    // by close() for the safety-net decision -- see
    // deliveredTerminalToCustomHandler for why "any error ever" is too coarse.
    private final AtomicBoolean deliveredToCustomHandler = new AtomicBoolean();
    // Set the first time the dispatcher delivers THE terminal error (the exact
    // SenderError the I/O loop latched via recordFatal, marked here through
    // markTerminal) to a non-default handler. This -- not
    // deliveredToCustomHandler -- is what close() consults: a routine
    // RETRIABLE rejection delivered earlier must NOT suppress the
    // close() safety net for a later, genuinely-unsurfaced TERMINAL error.
    private final AtomicBoolean deliveredTerminalToCustomHandler = new AtomicBoolean();
    private final AtomicLong dropped = new AtomicLong();
    // volatile so the user can swap the handler post-connect, mirroring
    // SenderProgressDispatcher. A final field would make handler config a
    // strict pre-connect concern; making it dynamic lets builders, tests,
    // and reconfigurable apps install a new handler at any time without
    // tearing down the dispatcher thread.
    private volatile SenderErrorHandler handler;
    // Deque (not plain queue) so offer() can drop the head when the inbox is
    // full, per spec section 14.6. SPSC in steady state: the I/O thread is
    // the sole producer, the dispatcher is the sole consumer; close() also
    // enqueues POISON, but only once and under `lock`.
    private final LinkedBlockingDeque<SenderError> inbox;
    // Threads are started lazily under this monitor; takes the same role as
    // SegmentManager.start() — first offer() that observes a null thread
    // wins the race to spawn it.
    private final Object lock = new Object();
    private final String threadName;
    // The exact SenderError instance the I/O loop latched as terminal, set
    // once via markTerminal (first-writer-wins, mirroring recordFatal's
    // latch). The dispatch loop identity-compares delivered errors against
    // it so only delivery of THIS terminal -- not an earlier
    // RETRIABLE rejection -- flips deliveredTerminalToCustomHandler. volatile:
    // written on the I/O thread, read on the dispatcher thread.
    private volatile SenderError terminalServerError;
    private final AtomicLong totalDelivered = new AtomicLong();
    private volatile boolean closed;
    // volatile to give the off-lock read in offer() a happens-before with
    // the write under `lock` in startDispatcherIfNeeded(). Without it the
    // double-checked first-null guard is a JMM race -- benign in practice
    // (the synchronized re-check covers double-start) but spec-incorrect.
    private volatile Thread dispatcherThread;

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
     * Total errors discarded by the drop-oldest overflow policy since startup.
     * Non-zero means the user's handler is slower than the error rate —
     * typically a symptom of a misbehaving handler or a misconfigured server.
     * Reported by the sender for ops dashboards.
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
     * True if at least one error has been delivered to a user-installed
     * (non-default) handler since this dispatcher started. Used by
     * {@code QwpWebSocketSender.close()} to decide whether the safety-net
     * rethrow is still needed: when this returns true, the user has seen
     * the error stream through their handler, so close() should not
     * additionally rethrow.
     */
    public boolean hasDeliveredToCustomHandler() {
        return deliveredToCustomHandler.get();
    }

    /**
     * True once the dispatcher has actually delivered THE terminal error -- the
     * exact {@link SenderError} the I/O loop latched and passed to
     * {@link #markTerminal} -- to a user-installed (non-default) handler.
     * <p>
     * This is the signal {@code QwpWebSocketSender.close()} uses to decide
     * whether its safety-net rethrow is still needed. Unlike
     * {@link #hasDeliveredToCustomHandler()} ("any error ever"), this stays
     * {@code false} when the only thing the custom handler saw was an earlier
     * {@code RETRIABLE} rejection, or when the terminal reached only
     * the default handler after a {@code setErrorHandler(null)} revert, or when
     * the terminal is still queued/abandoned because the handler is slow. In
     * all those cases close() must still surface the terminal loudly.
     */
    public boolean hasDeliveredTerminalToCustomHandler() {
        return deliveredTerminalToCustomHandler.get();
    }

    /**
     * Record the exact {@link SenderError} instance the I/O loop latched as its
     * terminal failure, so the dispatch loop can recognise it on delivery.
     * Called by {@code CursorWebSocketSendLoop.recordFatal} on the I/O thread
     * before the matching {@link #offer}, so the marker is visible by the time
     * the dispatcher delivers it. First-writer-wins, mirroring recordFatal's
     * own write-once latch -- a stray later TERMINAL cannot re-point the marker at
     * an error that is not the latched terminal.
     */
    public void markTerminal(SenderError err) {
        if (terminalServerError == null) {
            terminalServerError = err;
        }
    }

    /**
     * Replace the user-supplied handler. Effective for the next delivery.
     * Null reverts to the loud-not-silent default.
     */
    public void setHandler(SenderErrorHandler handler) {
        this.handler = handler != null ? handler : DefaultSenderErrorHandler.INSTANCE;
    }

    /**
     * Non-blocking enqueue. Always admits the new error unless the dispatcher
     * is closed or {@code error} is null. Returns {@code true} when the new
     * error is enqueued for delivery, {@code false} when rejected outright.
     *
     * <p>When the inbox is full, drops the oldest pending entry to make room
     * (sf-client.md section 14.6) and bumps {@link #getDroppedNotifications()}.
     * The newest entry is always retained because it carries the most recent
     * watermark information.
     *
     * <p>Lazy-starts the dispatcher thread on the first successful offer.
     */
    public boolean offer(SenderError error) {
        if (closed || error == null) {
            return false;
        }
        // Drop-oldest overflow policy. The pollFirst()/offerLast() pair is
        // not atomic with the consumer, but the consumer can only remove
        // entries (never add). The retry loop converges in at most one extra
        // iteration under SPSC; close()'s POISON enqueue widens the race
        // briefly but the `closed` re-check exits cleanly.
        while (!inbox.offerLast(error)) {
            if (inbox.pollFirst() != null) {
                dropped.incrementAndGet();
            }
            if (closed) {
                return false;
            }
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
            // Increment before invoking the handler: observers using a
            // CountDownLatch in the handler must be able to read the
            // updated counter once their latch fires. With the increment
            // after, the handler-released observer races the dispatcher
            // and can see totalDelivered short by one.
            totalDelivered.incrementAndGet();
            SenderErrorHandler h = handler;
            if (h != DefaultSenderErrorHandler.INSTANCE) {
                deliveredToCustomHandler.set(true);
                // Identity match: only THIS delivery of the latched terminal
                // counts as the custom handler owning the terminal. An earlier
                // RETRIABLE rejection (err != terminalServerError) does not, so
                // close() will not suppress a later genuine terminal.
                if (err == terminalServerError) {
                    deliveredTerminalToCustomHandler.set(true);
                }
            }
            try {
                h.onError(err);
            } catch (Throwable t) {
                LOG.error("SenderErrorHandler threw on {}: {}", err, t.getMessage(), t);
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
