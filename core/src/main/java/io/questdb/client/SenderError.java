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

package io.questdb.client;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable description of a server-side rejection of an asynchronously published batch.
 *
 * <p>Delivered to user code through two paths:
 * <ul>
 *   <li>Asynchronously via {@link SenderErrorHandler} registered on the builder.</li>
 *   <li>Synchronously as the payload of a {@link LineSenderServerException} thrown
 *       from the next producer-thread API call after a {@link Policy#TERMINAL} error has
 *       been latched.</li>
 * </ul>
 *
 * <p>The {@code [fromFsn, toFsn]} span is the load-bearing correlation key — join it to
 * whatever the producer thread logged alongside the published-sequence value returned by
 * the sender to identify the rejected data.
 *
 * @see SenderErrorHandler
 * @see LineSenderServerException
 */
public final class SenderError {

    /**
     * Sentinel for {@link #messageSequence} when the wire layer carries no QWP frame sequence.
     */
    public static final long NO_MESSAGE_SEQUENCE = -1L;
    /**
     * Sentinel for {@link #serverStatusByte} when the error is a {@link Category#PROTOCOL_VIOLATION}.
     */
    public static final int NO_STATUS_BYTE = -1;
    private final Policy appliedPolicy;
    private final Category category;
    private final long detectedAtNanos;
    private final long fromFsn;
    private final long messageSequence;
    private final String serverMessage;
    private final int serverStatusByte;
    private final String tableName;
    private final long toFsn;
    public SenderError(
            @NotNull Category category,
            @NotNull Policy appliedPolicy,
            int serverStatusByte,
            @Nullable String serverMessage,
            long messageSequence,
            long fromFsn,
            long toFsn,
            @Nullable String tableName,
            long detectedAtNanos
    ) {
        this.category = category;
        this.appliedPolicy = appliedPolicy;
        this.serverStatusByte = serverStatusByte;
        this.serverMessage = serverMessage;
        this.messageSequence = messageSequence;
        this.fromFsn = fromFsn;
        this.toFsn = toFsn;
        this.tableName = tableName;
        this.detectedAtNanos = detectedAtNanos;
    }

    /**
     * @return the policy the I/O loop actually applied — RETRIABLE / RETRIABLE_OTHER means
     * the batch stays in the store-and-forward log and is replayed after a reconnect (no data
     * loss, informational delivery); TERMINAL means a {@link LineSenderServerException} will be
     * thrown on the next producer-thread API call.
     */
    public @NotNull Policy getAppliedPolicy() {
        return appliedPolicy;
    }

    /**
     * @return the rejection category.
     */
    public @NotNull Category getCategory() {
        return category;
    }

    /**
     * @return wall-clock-independent receipt time on the I/O thread, from {@link System#nanoTime()}.
     */
    public long getDetectedAtNanos() {
        return detectedAtNanos;
    }

    /**
     * @return inclusive lower bound of the FSN span for the rejected batch — correlation key for producer-side logs.
     */
    public long getFromFsn() {
        return fromFsn;
    }

    /**
     * @return server's per-frame messageSequence as mirrored back in the rejection frame, or
     * {@link #NO_MESSAGE_SEQUENCE} for {@link Category#PROTOCOL_VIOLATION} (WS close frames carry no QWP sequence).
     */
    public long getMessageSequence() {
        return messageSequence;
    }

    /**
     * @return the human-readable message provided by the server (≤1024 UTF-8 bytes for QWP error frames,
     * or the WebSocket close reason for protocol violations). May be null if the server provided no text.
     */
    public @Nullable String getServerMessage() {
        return serverMessage;
    }

    /**
     * @return raw status byte from the server (e.g. {@code 0x03} for SCHEMA_MISMATCH), or
     * {@link #NO_STATUS_BYTE} for {@link Category#PROTOCOL_VIOLATION}.
     */
    public int getServerStatusByte() {
        return serverStatusByte;
    }

    /**
     * @return the rejected table name, if the server attributed the error to a single table.
     * Null when the rejected batch carried rows for multiple tables, or when the server did
     * not include attribution.
     */
    public @Nullable String getTableName() {
        return tableName;
    }

    /**
     * @return inclusive upper bound of the FSN span for the rejected batch.
     */
    public long getToFsn() {
        return toFsn;
    }

    @Override
    public String toString() {
        return "SenderError{category=" + category +
                ", policy=" + appliedPolicy +
                ", status=0x" + Integer.toHexString(serverStatusByte & 0xFF) +
                ", seq=" + messageSequence +
                ", fsn=[" + fromFsn + ',' + toFsn + ']' +
                ", table=" + (tableName == null ? "(multi)" : tableName) +
                ", msg=" + serverMessage +
                '}';
    }

    /**
     * Server-distinguishable rejection categories. Aligned 1:1 with the stable
     * QWP wire status bytes for ingress, plus {@link #PROTOCOL_VIOLATION} for
     * WebSocket-level close frames and {@link #UNKNOWN} for forward compatibility.
     */
    public enum Category {
        /**
         * Server-side schema mismatch (column missing, type clash, NOT NULL violated, no such table). Wire {@code 0x03}.
         */
        SCHEMA_MISMATCH,
        /**
         * QWP-level malformed payload — most likely a client bug. Wire {@code 0x05}.
         */
        PARSE_ERROR,
        /**
         * Server-side fault, catch-all (CairoException.isCritical, unhandled Throwable). Wire {@code 0x06}.
         */
        INTERNAL_ERROR,
        /**
         * Authentication or authorization failure. Wire {@code 0x08}.
         */
        SECURITY_ERROR,
        /**
         * Non-critical Cairo error, table not accepting writes. Wire {@code 0x09}.
         */
        WRITE_ERROR,
        /**
         * Node cannot serve writes at all right now (read-only replica, primary demoting).
         * Wire {@code 0x0C} — reserved: current servers signal this state with a
         * reconnect-eligible close instead of a mid-stream NACK, so this category is
         * mapped for forward compatibility with servers that NACK it explicitly.
         */
        NOT_WRITABLE,
        /**
         * A frame the server (or an intermediary) deterministically rejects: the
         * poison-frame detector observed the same head-of-line frame fail
         * {@link io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop#DEFAULT_MAX_HEAD_FRAME_REJECTIONS}
         * consecutive times with no ack progress — replaying it cannot succeed.
         */
        PROTOCOL_VIOLATION,
        /**
         * Status byte the client does not recognize — forward compatibility for new server codes.
         */
        UNKNOWN
    }

    /**
     * Policy applied by the client when a category fires. Resolution precedence (highest first):
     * builder {@code errorPolicyResolver} → builder per-category {@code errorPolicy} →
     * connect-string per-category {@code on_*_error} → connect-string global {@code on_server_error}
     * → spec defaults.
     *
     * <p>There is no drop policy by design: the client never silently discards data. A rejected
     * batch is either replayed ({@link #RETRIABLE} / {@link #RETRIABLE_OTHER}) or halts the
     * sender loudly with the bytes preserved on disk ({@link #TERMINAL}).
     *
     * <p>{@link Category#PROTOCOL_VIOLATION} is forced {@link #TERMINAL} and
     * {@link Category#UNKNOWN} is forced {@link #RETRIABLE} (fail open: a status byte from a
     * newer server must degrade to retry, not to a dead sender); user overrides for those
     * categories are ignored.
     */
    public enum Policy {
        /**
         * Recycle the connection and replay from the store-and-forward log: reconnect with
         * capped exponential backoff and reposition at {@code ackedFsn + 1}. No data is
         * dropped and the producer keeps writing; delivery through {@link SenderErrorHandler}
         * is informational. A frame that keeps being rejected with no ack progress escalates
         * to {@link #TERMINAL} via the poison-frame detector.
         */
        RETRIABLE,
        /**
         * Same replay semantics as {@link #RETRIABLE}, but the rejection says this node cannot
         * serve writes at all (read-only replica / demoting primary), so the reconnect rotates
         * to the next configured endpoint rather than waiting out a backoff against the same
         * node.
         */
        RETRIABLE_OTHER,
        /**
         * Latch the error as terminal. The next producer-thread API call (e.g. {@link Sender#flush()})
         * throws {@link LineSenderServerException}. The sender does not drain further until the
         * caller closes and rebuilds it. The rejected bytes remain in the store-and-forward log
         * on disk — nothing is silently discarded.
         */
        TERMINAL
    }
}
