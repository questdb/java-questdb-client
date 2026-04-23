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

/**
 * Tagged event pushed from the I/O thread onto the user-facing event queue.
 * One event per {@code RESULT_BATCH} / {@code RESULT_END} / {@code QUERY_ERROR}
 * received from the server, plus a synthetic error event if the connection drops
 * mid-query.
 */
public class QueryEvent {

    public static final int KIND_BATCH = 0;
    public static final int KIND_END = 1;
    public static final int KIND_ERROR = 2;
    public static final int KIND_EXEC_DONE = 3;
    /**
     * Synthesised on the I/O thread when the server closes the socket,
     * receiveFrame / decode raises, or the I/O thread abnormally exits.
     * Distinct from {@link #KIND_ERROR} (server-emitted {@code QUERY_ERROR})
     * so {@code execute()} can decide whether to trigger failover without
     * having to reconstruct the classification from a side-channel latch.
     */
    public static final int KIND_TRANSPORT_ERROR = 4;

    public QwpBatchBuffer buffer;     // valid for KIND_BATCH (must be released to pool by consumer)
    public String errorMessage;       // valid for KIND_ERROR
    public byte errorStatus;          // valid for KIND_ERROR
    public int kind;
    public short opType;              // valid for KIND_EXEC_DONE (matches CompiledQuery.SELECT/INSERT/etc.)
    public long rowsAffected;         // valid for KIND_EXEC_DONE
    public long totalRows;            // valid for KIND_END

    public QueryEvent asBatch(QwpBatchBuffer buffer) {
        this.kind = KIND_BATCH;
        this.buffer = buffer;
        return this;
    }

    public QueryEvent asEnd(long totalRows) {
        this.kind = KIND_END;
        this.buffer = null;
        this.totalRows = totalRows;
        return this;
    }

    public QueryEvent asError(byte status, String message) {
        this.kind = KIND_ERROR;
        this.buffer = null;
        this.errorStatus = status;
        this.errorMessage = message;
        return this;
    }

    public QueryEvent asExecDone(short opType, long rowsAffected) {
        this.kind = KIND_EXEC_DONE;
        this.buffer = null;
        this.opType = opType;
        this.rowsAffected = rowsAffected;
        return this;
    }

    public QueryEvent asTransportError(byte status, String message) {
        this.kind = KIND_TRANSPORT_ERROR;
        this.buffer = null;
        this.errorStatus = status;
        this.errorMessage = message;
        return this;
    }

    /**
     * Clears object references and resets primitive fields so a pooled event is
     * safe to reuse across queries. The I/O thread calls the {@code asX(...)}
     * builders after borrowing, which overwrites fields anyway; resetting on
     * release still helps because it (1) drops the buffer / message references
     * promptly so they are not held past the consumer callback, and (2) keeps
     * the event in a consistent "no kind bound" state for diagnostics.
     */
    public void reset() {
        this.kind = -1;
        this.buffer = null;
        this.errorMessage = null;
        this.errorStatus = 0;
        this.opType = 0;
        this.rowsAffected = 0;
        this.totalRows = 0;
    }
}
