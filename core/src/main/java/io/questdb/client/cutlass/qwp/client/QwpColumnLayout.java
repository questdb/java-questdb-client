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

import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

/**
 * Per-column parsed layout for one batch. Holds native pointers INTO the
 * currently-active WS payload buffer plus pre-computed per-row indices for
 * O(1) access. Reused across batches to eliminate allocations on the hot path
 * (pooled arrays grow to max observed size and never shrink).
 */
public class QwpColumnLayout implements QuietCloseable {

    /**
     * SYMBOL: lazy String cache indexed by dict ID. Populated on first
     * {@code getSymbol}/{@code getSymbolForId}/{@code getString}(SYMBOL) call
     * so a query over 10M rows with N distinct symbols materialises only N
     * Strings per batch. The cache survives across batches in delta mode (the
     * connection dict is stable) and invalidates lazily when
     * {@link #symbolDictVersion} disagrees with {@link #symbolCacheVersion} --
     * see {@code QwpColumnBatch#lookupCachedSymbol}.
     */
    final ObjList<String> symbolStringCache = new ObjList<>();
    /**
     * ARRAY: per-row starting offset (absolute address) of the array bytes. -1 for NULL rows.
     */
    long[] arrayRowAddr;
    /**
     * ARRAY: per-row length in bytes of the array payload.
     */
    int[] arrayRowLen;
    /**
     * Schema column metadata (name, wire type, scale, precisionBits).
     */
    QwpEgressColumnInfo info;
    /**
     * Absolute address of the first byte after this column's data -- used to walk to the next column.
     */
    long nextAddr;
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
     * Absolute payload address of the null bitmap, or 0 if the column has no NULL rows.
     */
    long nullBitmapAddr;
    /**
     * STRING / VARCHAR: absolute address of the concatenated UTF-8 bytes (right after the offsets array).
     */
    long stringBytesAddr;
    /**
     * Last {@link #symbolDictVersion} the {@link #symbolStringCache} was known
     * valid under. Updated at cache read time (not write time) -- if the dict
     * has been re-stamped since the last lookup, the cache is wiped and this
     * field catches up to the current version before the lookup proceeds.
     */
    long symbolCacheVersion;
    /**
     * SYMBOL: absolute address of the native entries array. Each entry is a packed
     * 8-byte pair {@code (offset:i32 | length:i32<<32)} relative to
     * {@link #symbolDictHeapAddr}. Access in the hot path is a single 64-bit
     * load + two int extractions, no object dereferences.
     */
    long symbolDictEntriesAddr;
    /**
     * SYMBOL: absolute address of the UTF-8 bytes heap holding all dict entries. In
     * delta mode this points into the decoder's connection-scoped native heap; in
     * non-delta mode it points into the payload buffer (wire bytes directly).
     */
    long symbolDictHeapAddr;
    /**
     * SYMBOL: number of valid entries referenced by {@link #symbolDictEntriesAddr}.
     */
    int symbolDictSize;
    /**
     * Version of the dict currently bound to this layout. Stamped by the
     * decoder at parse time. Encoding:
     * <ul>
     *   <li>delta mode: {@code connDictGeneration << 1} (bit 0 clear)</li>
     *   <li>non-delta:  {@code dictBase | 1} (bit 0 set, address changes each batch)</li>
     * </ul>
     * The split low bit stops a delta generation count from colliding with a
     * non-delta address token. Consumers compare this against
     * {@link #symbolCacheVersion} to decide whether the cache is still valid.
     */
    long symbolDictVersion;
    /**
     * SYMBOL: per-row dictionary ID. Sized to {@code rowCount}; NULL rows are
     * left with stale values -- use {@link #nonNullIdx}/null-check first.
     */
    int[] symbolRowIds;
    /**
     * Absolute payload address where this column's non-null values start. For
     * fixed-width types this is the dense values array. For strings/varchars
     * it's the offsets array. For symbols it's where the dict starts; the
     * per-row IDs are materialised into {@link #symbolRowIds} during parse.
     */
    long valuesAddr;
    /**
     * SYMBOL non-delta only: native buffer owned by this layout that holds the
     * per-batch packed entries when the column carries its own dict. In delta
     * mode this is 0 and {@link #symbolDictEntriesAddr} points at the decoder's
     * shared array instead.
     */
    private long ownedEntriesAddr;
    private int ownedEntriesCapacity;
    /**
     * TIMESTAMP / TIMESTAMP_NANOS / DATE Gorilla decode buffer owned by this
     * layout. Populated when the per-column encoding discriminator is
     * {@code 0x01}; {@link #valuesAddr} is then pointed at this buffer so the
     * user-facing {@code getLong(col, row)} path sees decoded int64s. Unused
     * (0) when the column was shipped uncompressed.
     */
    private long timestampDecodeAddr;
    private int timestampDecodeCapacity;

    public void clear() {
        info = null;
        valuesAddr = 0;
        nullBitmapAddr = 0;
        nonNullCount = 0;
        stringBytesAddr = 0;
        symbolDictHeapAddr = 0;
        symbolDictEntriesAddr = 0;
        symbolDictSize = 0;
        // symbolStringCache is intentionally NOT wiped here. Lazy invalidation
        // via symbolDictVersion / symbolCacheVersion keeps connection-stable
        // dict entries cached across batches and wipes on the first lookup
        // after a dict-version mismatch (non-delta batch or CACHE_RESET).
        nextAddr = 0;
    }

    @Override
    public void close() {
        if (ownedEntriesAddr != 0) {
            Unsafe.free(ownedEntriesAddr, ownedEntriesCapacity, MemoryTag.NATIVE_DEFAULT);
            ownedEntriesAddr = 0;
            ownedEntriesCapacity = 0;
        }
        if (timestampDecodeAddr != 0) {
            Unsafe.free(timestampDecodeAddr, timestampDecodeCapacity, MemoryTag.NATIVE_DEFAULT);
            timestampDecodeAddr = 0;
            timestampDecodeCapacity = 0;
        }
    }

    /**
     * Returns the dense index of {@code row} into the non-null values array.
     * For columns with no nulls in this batch ({@code nullBitmapAddr == 0}),
     * dense index equals row and {@link #nonNullIdx} is left unread (the
     * decoder skips the per-row array fill on this path). Otherwise the
     * pre-computed slot is returned.
     * <p>
     * Caller MUST have null-checked the cell first via the surrounding
     * {@code isNull} / {@code isLayoutNull} guard -- this method does not
     * detect null rows on its own.
     */
    public int denseIndex(int row) {
        return nullBitmapAddr == 0 ? row : nonNullIdx[row];
    }

    /**
     * Ensures the per-layout owned entries buffer is at least {@code requiredBytes}
     * and returns its address. Used by non-delta mode where the column carries its
     * own dictionary inline and we need a dedicated buffer for the packed entries.
     */
    long ensureOwnedEntriesAddr(int requiredBytes) {
        if (ownedEntriesCapacity < requiredBytes) {
            int newCap = Math.max(ownedEntriesCapacity * 2, Math.max(64, requiredBytes));
            ownedEntriesAddr = Unsafe.realloc(ownedEntriesAddr, ownedEntriesCapacity, newCap, MemoryTag.NATIVE_DEFAULT);
            ownedEntriesCapacity = newCap;
        }
        return ownedEntriesAddr;
    }

    /**
     * Ensures the per-layout Gorilla decode buffer is at least {@code requiredBytes}
     * and returns its address.
     */
    long ensureTimestampDecodeAddr(int requiredBytes) {
        if (timestampDecodeCapacity < requiredBytes) {
            int newCap = Math.max(timestampDecodeCapacity * 2, Math.max(64, requiredBytes));
            timestampDecodeAddr = Unsafe.realloc(timestampDecodeAddr, timestampDecodeCapacity, newCap, MemoryTag.NATIVE_DEFAULT);
            timestampDecodeCapacity = newCap;
        }
        return timestampDecodeAddr;
    }
}
