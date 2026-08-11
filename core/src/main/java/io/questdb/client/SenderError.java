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
 * Immutable description of a server-side rejection of an asynchronously published batch —
 * or, for {@link Category#DATA_LOSS}, of a client-side verdict that buffered data has been
 * permanently abandoned and must be re-ingested from its source.
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
    private final String quarantinedPath;
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
        this(category, appliedPolicy, serverStatusByte, serverMessage, messageSequence,
                fromFsn, toFsn, tableName, detectedAtNanos, null);
    }

    private SenderError(
            @NotNull Category category,
            @NotNull Policy appliedPolicy,
            int serverStatusByte,
            @Nullable String serverMessage,
            long messageSequence,
            long fromFsn,
            long toFsn,
            @Nullable String tableName,
            long detectedAtNanos,
            @Nullable String quarantinedPath
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
        this.quarantinedPath = quarantinedPath;
    }

    /**
     * The only way to build a {@link Category#DATA_LOSS} report. Binds the
     * category/policy pair the two enum constants promise each other and fills
     * the server-shaped fields with their sentinels — there is no server
     * verdict to report. The FSN span is {@link #NO_MESSAGE_SEQUENCE} on both
     * bounds: the abandoned span is unknown at quarantine time (the engine is
     * closed or was never built).
     *
     * @param detail          why the data is unreachable, from the recovery verdict;
     *                        becomes {@link #getServerMessage()}
     * @param quarantinedPath where the abandoned bytes remain on disk, for
     *                        forensics and a manual resend
     */
    public static SenderError dataLoss(@NotNull String detail, @NotNull String quarantinedPath) {
        return new SenderError(Category.DATA_LOSS, Policy.ABANDONED, NO_STATUS_BYTE, detail,
                NO_MESSAGE_SEQUENCE, NO_MESSAGE_SEQUENCE, NO_MESSAGE_SEQUENCE, null,
                System.nanoTime(), quarantinedPath);
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
     * For {@link Category#DATA_LOSS} this is {@link #NO_MESSAGE_SEQUENCE} — the abandoned span is unknown at quarantine time.
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
     * @return for {@link Category#DATA_LOSS}: the on-disk path where the abandoned
     * bytes remain (a quarantined {@code .unreplayable-N} directory, or the slot
     * directory itself when a drainer left it behind a {@code .failed} sentinel).
     * Null for every other category.
     */
    public @Nullable String getQuarantinedPath() {
        return quarantinedPath;
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
     * For {@link Category#DATA_LOSS} this is {@link #NO_MESSAGE_SEQUENCE} — the abandoned span is unknown at quarantine time.
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
                (quarantinedPath == null ? "" : ", quarantined=" + quarantinedPath) +
                '}';
    }

    /**
     * Server-distinguishable rejection categories, aligned 1:1 with the stable
     * QWP wire status bytes for ingress, plus three client-originated ones:
     * {@link #PROTOCOL_VIOLATION} for the poison-frame detector,
     * {@link #DATA_LOSS} for permanently abandoned store-and-forward data (the
     * only category with no server involvement at all), and {@link #UNKNOWN}
     * for forward compatibility.
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
         * A delta symbol dictionary began above the server's per-connection dictionary.
         * Wire {@code 0x0D}. Unlike {@link #PARSE_ERROR} this is a function of server
         * state, not of the frame's bytes, so the same frame succeeds after the
         * connection's dictionary catch-up has run.
         */
        DICTIONARY_GAP,
        /**
         * A frame the server (or an intermediary) deterministically rejects: the
         * poison-frame detector observed the same head-of-line frame fail
         * {@link io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop#DEFAULT_MAX_HEAD_FRAME_REJECTIONS}
         * consecutive times with no ack progress — replaying it cannot succeed.
         */
        PROTOCOL_VIOLATION,
        /**
         * Rows this client had durably buffered will never be sent, and no retry
         * will change that. The only category with no server involvement: the
         * server never saw these bytes and issued no verdict on them, so
         * {@link #getServerStatusByte()} is always {@link #NO_STATUS_BYTE} and
         * {@link #getServerMessage()} carries a client-side explanation. Fired
         * when store-and-forward recovery sets an unreplayable slot aside
         * (its symbol dictionary cannot be rebuilt from any source, or its
         * durable chain is proven corrupt or incomplete) and when an orphan
         * drainer abandons a slot behind a {@code .failed} sentinel that
         * nothing clears automatically.
         *
         * <p>Always paired with {@link Policy#ABANDONED}; the two are never
         * issued apart, and only {@link SenderError#dataLoss} constructs them.
         * The bytes are preserved on disk — {@link #getQuarantinedPath()}
         * names where — so the data can be inspected and re-ingested from its
         * source. This is the event to page on. (The Rust client models the
         * same verdict as {@code ErrorCode::StoreResendRequired}.)
         */
        DATA_LOSS,
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
     * <p>There is no silent-drop policy by design: the client never discards
     * data without telling anyone. A rejected batch is replayed
     * ({@link #RETRIABLE} / {@link #RETRIABLE_OTHER}), halts the sender loudly
     * with the bytes preserved on disk ({@link #TERMINAL}), or — the one case
     * where the bytes can never be sent — is abandoned in place and announced
     * as {@link #ABANDONED}, which is precisely what keeps the abandonment
     * non-silent.
     *
     * <p>{@link Category#PROTOCOL_VIOLATION} is forced {@link #TERMINAL},
     * {@link Category#UNKNOWN} is forced {@link #RETRIABLE} (fail open: a
     * status byte from a newer server must degrade to retry, not to a dead
     * sender), and {@link Category#DATA_LOSS} is forced {@link #ABANDONED};
     * user overrides for these categories are ignored.
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
        TERMINAL,
        /**
         * The rows are gone. Nothing replays them, and — unlike
         * {@link #TERMINAL} — nothing throws: no {@link LineSenderServerException}
         * is latched, and the sender that reported this keeps running (a
         * quarantining {@code build()} returns a working sender on a fresh,
         * empty slot). The bytes stay on disk under the path named by
         * {@link SenderError#getQuarantinedPath()}, for forensics and a manual
         * resend.
         *
         * <p>Issued only with {@link Category#DATA_LOSS} and never resolvable
         * from user configuration: no resolver or {@code on_*_error} key can
         * select it or override it away. It reports a fact about bytes already
         * abandoned, not a choice about how to react.
         */
        ABANDONED
    }
}
