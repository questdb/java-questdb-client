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
 * What a successful query reported besides its rows, on the request of
 * {@link QwpQueryOptions}: the row count of the whole result set and execution
 * statistics. Passed to
 * {@link QwpColumnBatchHandler#onEnd(long, long, QwpQueryDetails)} and
 * {@link QwpColumnBatchHandler#onExecDone(long, short, long, QwpQueryDetails)},
 * never null.
 * <p>
 * The instance is valid only for the duration of that call; the client reuses
 * it afterwards. Copy any values you need to retain.
 */
public final class QwpQueryDetails {

    private long clientWaitNanos = -1;
    private long compileNanos = -1;
    private long countNanos = -1;
    private long executeNanos = -1;
    private boolean hasRowCount;
    private boolean hasStats;
    private boolean isJitCompiled;
    private long rowCount = -1;

    /**
     * Returns how long, in nanoseconds, the query waited for this client to
     * read its results or grant credit, or -1 when {@link #hasStats()} is false.
     * Part of {@link #getExecuteNanos()}.
     */
    public long getClientWaitNanos() {
        return clientWaitNanos;
    }

    /**
     * Returns how long, in nanoseconds, the server took to compile the SQL text,
     * or -1 when {@link #hasStats()} is false. Zero when the server reused a
     * compiled query from its cache. Part of {@link #getExecuteNanos()}.
     */
    public long getCompileNanos() {
        return compileNanos;
    }

    /**
     * Returns how long, in nanoseconds, the server took to count the rows the
     * row window left out, or -1 when {@link #hasStats()} is false. Zero when
     * the row count was not requested, or needed no counting. Part of
     * {@link #getExecuteNanos()}.
     */
    public long getCountNanos() {
        return countNanos;
    }

    /**
     * Returns how long, in nanoseconds, the query ran on the server, from the
     * arrival of the request to its last frame, or -1 when {@link #hasStats()}
     * is false. Includes the compile, count and client-wait times.
     */
    public long getExecuteNanos() {
        return executeNanos;
    }

    /**
     * Returns the number of rows in the whole result set, including the rows
     * the row window left out of the stream, or -1 when {@link #hasRowCount()}
     * is false.
     */
    public long getRowCount() {
        return rowCount;
    }

    /**
     * Returns true when the server reported the row count of the whole result
     * set: the query asked for it with {@link QwpQueryOptions#countRows(boolean)},
     * the server advertises {@link QwpEgressMsgKind#CAP_QUERY_DETAILS}, and the
     * statement produced rows.
     */
    public boolean hasRowCount() {
        return hasRowCount;
    }

    /**
     * Returns true when the server reported execution statistics: the query
     * asked for them with {@link QwpQueryOptions#stats(boolean)} and the server
     * advertises {@link QwpEgressMsgKind#CAP_QUERY_DETAILS}.
     */
    public boolean hasStats() {
        return hasStats;
    }

    /**
     * Returns true when the query filter ran as JIT-compiled native code; false
     * when it did not, or when {@link #hasStats()} is false.
     */
    public boolean isJitCompiled() {
        return isJitCompiled;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("QwpQueryDetails{");
        if (hasRowCount) {
            sb.append("rowCount=").append(rowCount);
        }
        if (hasStats) {
            if (hasRowCount) {
                sb.append(", ");
            }
            sb.append("compileNanos=").append(compileNanos)
                    .append(", executeNanos=").append(executeNanos)
                    .append(", countNanos=").append(countNanos)
                    .append(", clientWaitNanos=").append(clientWaitNanos)
                    .append(", jitCompiled=").append(isJitCompiled);
        }
        return sb.append('}').toString();
    }

    void clear() {
        clientWaitNanos = -1;
        compileNanos = -1;
        countNanos = -1;
        executeNanos = -1;
        hasRowCount = false;
        hasStats = false;
        isJitCompiled = false;
        rowCount = -1;
    }

    void setRowCount(long rowCount) {
        this.rowCount = rowCount;
        this.hasRowCount = true;
    }

    void setStats(long compileNanos, long executeNanos, long countNanos, long clientWaitNanos, boolean isJitCompiled) {
        this.compileNanos = compileNanos;
        this.executeNanos = executeNanos;
        this.countNanos = countNanos;
        this.clientWaitNanos = clientWaitNanos;
        this.isJitCompiled = isJitCompiled;
        this.hasStats = true;
    }
}
