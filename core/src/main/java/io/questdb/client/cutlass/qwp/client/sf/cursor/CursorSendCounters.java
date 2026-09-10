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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Sender-lifetime observability counters shared by every
 * {@link CursorWebSocketSendLoop} generation and every {@link CursorSendEngine}
 * a {@code QwpWebSocketSender} attaches.
 * <p>
 * A symbol-dictionary recycle rebuilds both the loop and the engine; the
 * counters they report must not restart with them, so the sender owns one
 * instance for its whole life and hands it to each new loop and engine before
 * use ({@code adoptCounters}). A loop or engine built standalone (tests,
 * background drainers) starts on a fresh instance of its own, so its getters
 * behave exactly as they did when the counters were per-instance fields.
 * <p>
 * Each field is written by exactly one thread -- the loop's I/O thread for the
 * six loop counters, the producer thread for {@link #backpressureStalls} -- and
 * read by any monitor thread; {@link AtomicLong} makes every read a single
 * atomic load, so a value sampled across a recycle is never a torn sum.
 */
public final class CursorSendCounters {
    /** ACK frames received and applied; {@code CursorWebSocketSendLoop#getTotalAcks()}. */
    public final AtomicLong acks = new AtomicLong();
    /** Producer appends that hit a full ring and parked; {@code CursorSendEngine#getTotalBackpressureStalls()}. */
    public final AtomicLong backpressureStalls = new AtomicLong();
    /** Frames re-sent inside post-reconnect catch-up windows. */
    public final AtomicLong framesReplayed = new AtomicLong();
    /** Binary frames issued to the wire, replays included. */
    public final AtomicLong framesSent = new AtomicLong();
    /** Reconnect attempts, failed and successful alike. */
    public final AtomicLong reconnectAttempts = new AtomicLong();
    /** Successful reconnects. */
    public final AtomicLong reconnects = new AtomicLong();
    /** Non-OK / non-DURABLE_ACK frames received from the server, retriable and terminal. */
    public final AtomicLong serverErrors = new AtomicLong();

    /**
     * Folds every counter of {@code other} into this instance. Used once, when a
     * loop or engine that started on its own default instance is handed the
     * sender's shared one, so anything counted before adoption is carried
     * rather than dropped.
     */
    public void addAll(CursorSendCounters other) {
        acks.addAndGet(other.acks.get());
        backpressureStalls.addAndGet(other.backpressureStalls.get());
        framesReplayed.addAndGet(other.framesReplayed.get());
        framesSent.addAndGet(other.framesSent.get());
        reconnectAttempts.addAndGet(other.reconnectAttempts.get());
        reconnects.addAndGet(other.reconnects.get());
        serverErrors.addAndGet(other.serverErrors.get());
    }
}
