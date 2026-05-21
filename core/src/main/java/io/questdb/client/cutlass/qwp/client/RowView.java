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

import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Long256Sink;
import io.questdb.client.std.Uuid;
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.str.CharSink;
import io.questdb.client.std.str.DirectUtf8Sequence;

/**
 * Row-pinned view over a {@link QwpColumnBatch}. Exposes the per-cell accessors
 * with the row index implicit, so consumers iterate rows in the conventional
 * "current row + column index" style instead of passing both indices to every
 * call:
 * <pre>
 *   batch.forEachRow(row -&gt; {
 *       long ts    = row.getLongValue(0);
 *       String sym = row.getString(1);
 *       double px  = row.getDoubleValue(2);
 *   });
 * </pre>
 * <p>
 * <strong>Lifetime.</strong> The instance is owned by the batch, reused across
 * rows, and valid only for the duration of the {@code forEachRow} (or
 * surrounding {@code onBatch}) call. Do not retain past the callback.
 * <p>
 * <strong>Type contract / NULL handling.</strong> Each accessor delegates to
 * the corresponding {@code QwpColumnBatch} primitive; the contracts in
 * {@link QwpColumnBatch} apply unchanged. In particular, calling an accessor
 * whose type doesn't match the column's wire type yields undefined results,
 * and NULL rows return type-specific zero values.
 * <p>
 * <strong>Column metadata.</strong> Per-column attributes (name, wire type,
 * decimal scale, geohash precision, symbol dict size) live on the batch. Use
 * {@link #batch()} to reach them without capturing a separate reference.
 */
public class RowView {

    private final QwpColumnBatch batch;
    private int row;

    RowView(QwpColumnBatch batch) {
        this.batch = batch;
    }

    /**
     * Returns the parent batch. Use it to read column-level metadata
     * ({@link QwpColumnBatch#getColumnName}, {@link QwpColumnBatch#getColumnWireType},
     * etc.) or to reach for less common per-cell accessors not exposed on this view.
     */
    public QwpColumnBatch batch() {
        return batch;
    }

    /**
     * @see QwpColumnBatch#getArrayNDims(int, int)
     */
    public int getArrayNDims(int col) {
        return batch.getArrayNDims(col, row);
    }

    /**
     * @see QwpColumnBatch#getBinary(int, int)
     */
    public byte[] getBinary(int col) {
        return batch.getBinary(col, row);
    }

    /**
     * @see QwpColumnBatch#getBinaryA(int, int)
     */
    public DirectByteSequence getBinaryA(int col) {
        return batch.getBinaryA(col, row);
    }

    /**
     * @see QwpColumnBatch#getBinaryB(int, int)
     */
    public DirectByteSequence getBinaryB(int col) {
        return batch.getBinaryB(col, row);
    }

    /**
     * @see QwpColumnBatch#getBoolValue(int, int)
     */
    public boolean getBoolValue(int col) {
        return batch.getBoolValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getByteValue(int, int)
     */
    public byte getByteValue(int col) {
        return batch.getByteValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getCharValue(int, int)
     */
    public char getCharValue(int col) {
        return batch.getCharValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getDecimal128(int, int, Decimal128)
     */
    public boolean getDecimal128(int col, Decimal128 sink) {
        return batch.getDecimal128(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getDecimal128High(int, int)
     */
    public long getDecimal128High(int col) {
        return batch.getDecimal128High(col, row);
    }

    /**
     * @see QwpColumnBatch#getDecimal128Low(int, int)
     */
    public long getDecimal128Low(int col) {
        return batch.getDecimal128Low(col, row);
    }

    /**
     * @see QwpColumnBatch#getDecimal256(int, int, Decimal256)
     */
    public boolean getDecimal256(int col, Decimal256 sink) {
        return batch.getDecimal256(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getDecimal64(int, int, Decimal64)
     */
    public boolean getDecimal64(int col, Decimal64 sink) {
        return batch.getDecimal64(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getDoubleArrayElements(int, int)
     */
    public double[] getDoubleArrayElements(int col) {
        return batch.getDoubleArrayElements(col, row);
    }

    /**
     * @see QwpColumnBatch#getDoubleValue(int, int)
     */
    public double getDoubleValue(int col) {
        return batch.getDoubleValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getFloatValue(int, int)
     */
    public float getFloatValue(int col) {
        return batch.getFloatValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getGeohashValue(int, int)
     */
    public long getGeohashValue(int col) {
        return batch.getGeohashValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getIntValue(int, int)
     */
    public int getIntValue(int col) {
        return batch.getIntValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getLong256(int, int, Long256Sink)
     */
    public boolean getLong256(int col, Long256Sink sink) {
        return batch.getLong256(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getLong256Word(int, int, int)
     */
    public long getLong256Word(int col, int wordIndex) {
        return batch.getLong256Word(col, row, wordIndex);
    }

    /**
     * @see QwpColumnBatch#getLongValue(int, int)
     */
    public long getLongValue(int col) {
        return batch.getLongValue(col, row);
    }

    /**
     * Index of the row this view is currently pinned to.
     */
    public int getRowIndex() {
        return row;
    }

    /**
     * @see QwpColumnBatch#getShortValue(int, int)
     */
    public short getShortValue(int col) {
        return batch.getShortValue(col, row);
    }

    /**
     * @see QwpColumnBatch#getStrA(int, int)
     */
    public DirectUtf8Sequence getStrA(int col) {
        return batch.getStrA(col, row);
    }

    /**
     * @see QwpColumnBatch#getStrB(int, int)
     */
    public DirectUtf8Sequence getStrB(int col) {
        return batch.getStrB(col, row);
    }

    /**
     * @see QwpColumnBatch#getString(int, int)
     */
    public String getString(int col) {
        return batch.getString(col, row);
    }

    /**
     * @see QwpColumnBatch#getString(int, int, CharSink)
     */
    public boolean getString(int col, CharSink<?> sink) {
        return batch.getString(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getSymbol(int, int)
     */
    public String getSymbol(int col) {
        return batch.getSymbol(col, row);
    }

    /**
     * @see QwpColumnBatch#getSymbolId(int, int)
     */
    public int getSymbolId(int col) {
        return batch.getSymbolId(col, row);
    }

    /**
     * @see QwpColumnBatch#getUuid(int, int, Uuid)
     */
    public boolean getUuid(int col, Uuid sink) {
        return batch.getUuid(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getUuidHi(int, int)
     */
    public long getUuidHi(int col) {
        return batch.getUuidHi(col, row);
    }

    /**
     * @see QwpColumnBatch#getUuidLo(int, int)
     */
    public long getUuidLo(int col) {
        return batch.getUuidLo(col, row);
    }

    /**
     * @see QwpColumnBatch#isNull(int, int)
     */
    public boolean isNull(int col) {
        return batch.isNull(col, row);
    }

    /**
     * Re-points this flyweight at {@code row}. Returns {@code this} so callers
     * can chain a single accessor: {@code batch.row().of(r).getLongValue(0)}.
     */
    public RowView of(int row) {
        this.row = row;
        return this;
    }
}
