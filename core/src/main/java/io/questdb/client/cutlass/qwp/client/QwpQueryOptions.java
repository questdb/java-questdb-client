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
 * Per-query options of
 * {@link QwpQueryClient#execute(CharSequence, QwpBindSetter, QwpColumnBatchHandler, QwpQueryOptions)}
 * and {@link io.questdb.client.Query#options(QwpQueryOptions)}: a row window
 * that pages through the result set, and what the server reports besides the
 * rows -- the row count of the whole result set, execution statistics and the
 * position of an error in the SQL text. A new instance has every option off,
 * and a query run with it is the same on the wire as one run without options.
 * <p>
 * The row window, the row count, the statistics and the error position need a
 * server that advertises {@link QwpEgressMsgKind#CAP_QUERY_DETAILS}. Against a
 * server without it, the client fails a query that sets a row window, since the
 * server would stream every row, and runs a query that asks only for the row
 * count, the statistics or the error position without them: the handler then
 * sees {@link QwpQueryDetails#hasRowCount()} and
 * {@link QwpQueryDetails#hasStats()} false and the position
 * {@link QwpEgressMsgKind#NO_ERROR_POSITION}.
 * <p>
 * The client reads the options when the query starts, so one instance can be
 * reused for any number of queries, and changing it does not affect a query
 * already running. Not thread-safe.
 */
public final class QwpQueryOptions {

    private boolean isErrorPositionRequested;
    private boolean isRowCountRequested;
    private boolean isStatsRequested;
    private boolean isSymbolDictResetRequested;
    private long maxRows = Long.MAX_VALUE;
    private long skipRows;

    /**
     * Resets every option to its default: no row window, nothing reported
     * besides the rows, and the connection-scoped SYMBOL dictionary kept.
     *
     * @return this instance
     */
    public QwpQueryOptions clear() {
        isErrorPositionRequested = false;
        isRowCountRequested = false;
        isStatsRequested = false;
        isSymbolDictResetRequested = false;
        maxRows = Long.MAX_VALUE;
        skipRows = 0;
        return this;
    }

    /**
     * Copies every option of {@code other} into this instance, or resets them
     * when {@code other} is null.
     *
     * @return this instance
     */
    public QwpQueryOptions copyFrom(QwpQueryOptions other) {
        if (other == null) {
            return clear();
        }
        isErrorPositionRequested = other.isErrorPositionRequested;
        isRowCountRequested = other.isRowCountRequested;
        isStatsRequested = other.isStatsRequested;
        isSymbolDictResetRequested = other.isSymbolDictResetRequested;
        maxRows = other.maxRows;
        skipRows = other.skipRows;
        return this;
    }

    /**
     * Asks the server to count the rows of the whole result set, whatever the
     * row window leaves out of the stream, as the {@code count} of the
     * {@code /exec} HTTP endpoint does. The count reaches
     * {@link QwpColumnBatchHandler#onEnd(long, long, QwpQueryDetails)}.
     * Statements that do not produce rows report no count. When the row window
     * ends before the result set does, the server reads the rest of the result
     * set to count it, without sending it.
     *
     * @return this instance
     */
    public QwpQueryOptions countRows(boolean isRequested) {
        isRowCountRequested = isRequested;
        return this;
    }

    /**
     * Asks the server for the position of an error in the SQL text, as the
     * {@code position} of the {@code /exec} HTTP endpoint. A failed query then
     * reaches {@link QwpColumnBatchHandler#onError(long, byte, String, int)}
     * with the bare error message, without the {@code [position]} prefix the
     * server puts in front of it otherwise, and the position as a {@code char}
     * index into the SQL text.
     *
     * @return this instance
     */
    public QwpQueryOptions errorPosition(boolean isRequested) {
        isErrorPositionRequested = isRequested;
        return this;
    }

    /**
     * Returns the most rows the query streams, {@link Long#MAX_VALUE} when the
     * stream is unbounded.
     */
    public long getMaxRows() {
        return maxRows;
    }

    /**
     * Returns how many rows the query leaves out at the start of the result set.
     */
    public long getSkipRows() {
        return skipRows;
    }

    public boolean isErrorPositionRequested() {
        return isErrorPositionRequested;
    }

    public boolean isRowCountRequested() {
        return isRowCountRequested;
    }

    public boolean isStatsRequested() {
        return isStatsRequested;
    }

    public boolean isSymbolDictResetRequested() {
        return isSymbolDictResetRequested;
    }

    /**
     * Streams at most {@code rows} rows of the result set, after the rows
     * {@link #skipRows(long)} leaves out, like the {@code limit} of the
     * {@code /exec} HTTP endpoint. Zero streams no rows, only the schema.
     * {@link Long#MAX_VALUE}, the default, leaves the stream unbounded.
     * Statements that do not produce rows ignore it.
     *
     * @param rows the most rows to stream, zero or more
     * @return this instance
     * @throws IllegalArgumentException if {@code rows} is negative
     */
    public QwpQueryOptions maxRows(long rows) {
        if (rows < 0) {
            throw new IllegalArgumentException("maxRows must be >= 0 [maxRows=" + rows + ']');
        }
        maxRows = rows;
        return this;
    }

    /**
     * Asks the server to reset the connection-scoped SYMBOL dictionary before
     * the query, scoping the dictionary to its symbols. Needs a server that
     * advertises {@link QwpEgressMsgKind#CAP_QUERY_FLAGS}; against any other the
     * dictionary is kept.
     *
     * @return this instance
     */
    public QwpQueryOptions resetSymbolDict(boolean isRequested) {
        isSymbolDictResetRequested = isRequested;
        return this;
    }

    /**
     * Leaves out the first {@code rows} rows of the result set, like the lower
     * bound of the {@code limit} of the {@code /exec} HTTP endpoint. Statements
     * that do not produce rows ignore it.
     *
     * @param rows the rows to leave out, zero or more; zero, the default,
     *             leaves out none
     * @return this instance
     * @throws IllegalArgumentException if {@code rows} is negative
     */
    public QwpQueryOptions skipRows(long rows) {
        if (rows < 0) {
            throw new IllegalArgumentException("skipRows must be >= 0 [skipRows=" + rows + ']');
        }
        skipRows = rows;
        return this;
    }

    /**
     * Asks the server for execution statistics -- the compile, execute, count
     * and client-wait times, and whether the filter is JIT-compiled -- as the
     * {@code timings} and {@code explain} of the {@code /exec} HTTP endpoint
     * report them. They reach
     * {@link QwpColumnBatchHandler#onEnd(long, long, QwpQueryDetails)} or
     * {@link QwpColumnBatchHandler#onExecDone(long, short, long, QwpQueryDetails)};
     * a failed query reports none.
     *
     * @return this instance
     */
    public QwpQueryOptions stats(boolean isRequested) {
        isStatsRequested = isRequested;
        return this;
    }

    @Override
    public String toString() {
        return "QwpQueryOptions{skipRows=" + skipRows
                + ", maxRows=" + maxRows
                + ", countRows=" + isRowCountRequested
                + ", stats=" + isStatsRequested
                + ", errorPosition=" + isErrorPositionRequested
                + ", resetSymbolDict=" + isSymbolDictResetRequested
                + '}';
    }

    /**
     * Returns the {@code query_flags} bits these options ask for, before the
     * client drops the ones the server does not advertise.
     */
    long toQueryFlags() {
        long flags = 0;
        if (isSymbolDictResetRequested) {
            flags |= QwpEgressMsgKind.QUERY_FLAG_RESET_DICT;
        }
        if (isErrorPositionRequested) {
            flags |= QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION;
        }
        if (isStatsRequested) {
            flags |= QwpEgressMsgKind.QUERY_FLAG_STATS;
        }
        if (isRowCountRequested) {
            flags |= QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS;
        }
        if (skipRows > 0) {
            flags |= QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS;
        }
        if (maxRows != Long.MAX_VALUE) {
            flags |= QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS;
        }
        return flags;
    }
}
