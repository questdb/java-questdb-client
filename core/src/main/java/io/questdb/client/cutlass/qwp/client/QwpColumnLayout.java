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

import io.questdb.client.std.ObjList;
import io.questdb.client.std.str.DirectUtf8String;

/**
 * Per-column parsed layout for one batch. Holds native pointers INTO the
 * currently-active WS payload buffer plus pre-computed per-row indices for
 * O(1) access. Reused across batches to eliminate allocations on the hot path
 * (pooled arrays grow to max observed size and never shrink).
 */
public class QwpColumnLayout {

    /**
     * Schema column metadata (name, wire type, scale, precisionBits).
     */
    QwpEgressColumnInfo info;

    /**
     * Absolute payload address where this column's non-null values start. For
     * fixed-width types this is the dense values array. For strings/varchars
     * it's the offsets array. For symbols it's where the dict starts; the
     * per-row IDs are materialised into {@link #symbolRowIds} during parse.
     */
    long valuesAddr;

    /**
     * Absolute payload address of the null bitmap, or 0 if the column has no NULL rows.
     */
    long nullBitmapAddr;

    /**
     * Count of non-null rows in this column.
     */
    int nonNullCount;

    /**
     * Per-row lookup: {@code nonNullIdx[row]} is the dense index of row {@code row} within
     * the non-null values, or -1 if the row is NULL. Sized to {@code rowCount}.
     * Pool-owned; re-used across batches.
     */
    int[] nonNullIdx;

    /**
     * STRING / VARCHAR: absolute address of the concatenated UTF-8 bytes (right after the offsets array).
     */
    long stringBytesAddr;

    /**
     * SYMBOL: decoded dictionary entries as reusable native views.
     * <p>
     * Without {@code FLAG_DELTA_SYMBOL_DICT}, this is a per-batch list of
     * {@link DirectUtf8String}s pointing INTO the current payload buffer (valid
     * only for the lifetime of that buffer). With the flag set, the decoder
     * replaces this reference with its connection-scoped list, whose entries
     * point into a heap owned by the decoder that survives across batches.
     */
    ObjList<DirectUtf8String> symbolDict = new ObjList<>();

    /**
     * SYMBOL: number of valid entries in {@link #symbolDict} for this batch.
     */
    int symbolDictSize;

    /**
     * SYMBOL: per-row dictionary ID. Sized to {@code rowCount}; NULL rows are
     * left with stale values -- use {@link #nonNullIdx}/null-check first.
     */
    int[] symbolRowIds;

    /**
     * ARRAY: per-row starting offset (absolute address) of the array bytes. -1 for NULL rows.
     */
    long[] arrayRowAddr;

    /**
     * ARRAY: per-row length in bytes of the array payload.
     */
    int[] arrayRowLen;

    /**
     * Absolute address of the first byte after this column's data -- used to walk to the next column.
     */
    long nextAddr;

    public void clear() {
        info = null;
        valuesAddr = 0;
        nullBitmapAddr = 0;
        nonNullCount = 0;
        stringBytesAddr = 0;
        symbolDictSize = 0;
        nextAddr = 0;
    }
}
