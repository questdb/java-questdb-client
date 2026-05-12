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

import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.std.QuietCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded inbox + lazy-started daemon thread that delivers
 * {@link SenderConnectionEvent} notifications to a user-supplied
 * {@link SenderConnectionListener} off the I/O thread.
 *
 * <p>Modelled directly on {@link SenderErrorDispatcher}; see that class for
 * the general "why a separate thread", backpressure, lifecycle and exception-
 * safety contract. The only differences here are the payload type and the
 * smaller default capacity (connection events are sparse compared to per-batch
 * server errors).
 */
public final class SenderConnectionDispatcher implements QuietCloseable {

    public static final int DEFAULT_CAPACITY = 64;
    private static final long DRAIN_DEADLINE_NANOS = 100_000_000L; // 100 ms
    private static final Logger LOG = LoggerFactory.getLogger(SenderConnectionDispatcher.class);
    // Sentinel pushed during close() to nudge the dispatcher out of poll().
    // Identity-compared in the loop body; never delivered to the listener.
    private static final SenderConnectionEvent POISON = new SenderConnectionEvent(
            SenderConnectionEvent.Kind.DISCONNECTED,
            null, SenderConnectionEvent.NO_PORT,
            null, SenderConnectionEvent.NO_PORT,
            SenderConnectionEvent.NO_ATTEMPT_NUMBER,
            SenderConnectionEvent.NO_ROUND_NUMBER,
            null, 0L);
    private final AtomicLong dropped = new AtomicLong();
    // volatile so the user can swap the listener post-connect, mirroring
    // SenderErrorDispatcher's setHandler contract.
    private volatile SenderConnectionListener listener;
    private final BlockingQueue<SenderConnectionEvent> inbox;
    // First offer() that observes a null thread wins the race to spawn it.
    private final Object lock = new Object();
    private final String threadName;
    private final AtomicLong totalDelivered = new AtomicLong();
    private volatile boolean closed;
    // volatile to give the off-lock read in offer() a happens-before with
    // the write under `lock` in startDispatcherIfNeeded(). Without it the
    // double-checked first-null guard is a JMM race -- benign in practice
    // (the synchronized re-check covers double-start) but spec-incorrect.
    private volatile Thread dispatcherThread;

    public SenderConnectionDispatcher(SenderConnectionListener listener) {
        this(listener, DEFAULT_CAPACITY, "qdb-sf-connection-dispatcher");
    }

    public SenderConnectionDispatcher(SenderConnectionListener listener, int capacity) {
        this(listener, capacity, "qdb-sf-connection-dispatcher");
    }

    public SenderConnectionDispatcher(SenderConnectionListener listener, int capacity, String threadName) {
        if (listener == null) {
            throw new IllegalArgumentException("listener must be non-null");
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, was " + capacity);
        }
        this.listener = listener;
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
            //noinspection ResultOfMethodCallIgnored
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
                    LOG.warn("connection-dispatcher thread did not exit within drain deadline; "
                            + "abandoning {} queued events", inbox.size());
                    t.interrupt();
                }
                dispatcherThread = null;
            }
        }
    }

    /**
     * Total events dropped via inbox-overflow since startup. Non-zero means the
     * listener is slower than the event rate -- typically a symptom of a
     * misbehaving listener. Reported by the sender for ops dashboards.
     */
    public long getDroppedNotifications() {
        return dropped.get();
    }

    /**
     * Total events delivered to the listener since startup. Counts every
     * delivery attempt, including those where the listener threw.
     */
    public long getTotalDelivered() {
        return totalDelivered.get();
    }

    /**
     * Replace the user-supplied listener. Effective for the next delivery.
     * Null reverts to the loud-not-silent default.
     */
    public void setListener(SenderConnectionListener listener) {
        this.listener = listener != null ? listener : DefaultSenderConnectionListener.INSTANCE;
    }

    /**
     * Non-blocking enqueue. Returns {@code true} if the event will be
     * delivered to the listener (eventually, on the dispatcher thread).
     * Returns {@code false} if the inbox was full or the dispatcher was
     * closed -- caller's only obligation is to not block.
     *
     * <p>Lazy-starts the dispatcher thread on the first successful offer.
     */
    public boolean offer(SenderConnectionEvent event) {
        if (closed || event == null) {
            return false;
        }
        boolean accepted = inbox.offer(event);
        if (!accepted) {
            dropped.incrementAndGet();
            return false;
        }
        if (dispatcherThread == null) {
            startDispatcherIfNeeded();
        }
        return true;
    }

    private void dispatchLoop() {
        while (!closed || !inbox.isEmpty()) {
            SenderConnectionEvent ev;
            try {
                ev = inbox.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                if (closed) {
                    return;
                }
                Thread.currentThread().interrupt();
                continue;
            }
            if (ev == null || ev == POISON) {
                continue;
            }
            // Increment before invoking the listener so tests using a latch
            // inside the listener can read the updated counter once the latch
            // fires.
            totalDelivered.incrementAndGet();
            SenderConnectionListener l = listener;
            try {
                l.onEvent(ev);
            } catch (Throwable t) {
                LOG.error("SenderConnectionListener threw on {}: {}", ev, t.getMessage(), t);
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
