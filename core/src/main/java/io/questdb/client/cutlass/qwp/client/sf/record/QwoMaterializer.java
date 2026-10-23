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


package io.questdb.client.cutlass.qwp.client.sf.record;

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.protocol.OffHeapAppendMemory;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

/**
 * Turns a sealed QWO1 record into one ordinary QWP frame. The frame is
 * byte-identical to what {@code QwpWebSocketEncoder} produces from the same
 * rows given one table buffer per record block, in block order, with the
 * record's dictionary range (confirmed id {@code dictStartId - 1}) and flags:
 * <pre>
 * ['Q','W','P','1'][version][flags][u16 tableCount][u32 payloadLength]
 * [varint dictStartId][varint dictCount][dictionary entries]
 * per block with rows: [varint nameLen][name][varint rowCount][varint colCount]
 *                      per column [varint nameLen][name][u8 type]
 *                      per column [null section][values]
 * </pre>
 * {@code tableCount} counts only blocks with rows; a block whose committed row
 * count is 0 is left out. Each column reads its chunks through
 * {@link QwoColumnCursor}: fixed values, SYMBOL varints and variable-size bytes
 * are copied chunk by chunk, null bitmaps and BOOLEAN bits are concatenated
 * across chunks, variable-size offsets are rebased onto the column's bytes, and
 * timestamps are gathered so the Gorilla encoder sees them contiguously.
 * <p>
 * The record is trusted: a caller materialising recovered memory validates it
 * first. Owns a Gorilla encoder and two scratch buffers that grow amortised;
 * {@link #write} allocates nothing once they are big enough. Not thread-safe.
 */
public final class QwoMaterializer implements QuietCloseable {
    private static final byte ENCODING_GORILLA = 0x01;
    private static final byte ENCODING_UNCOMPRESSED = 0x00;
    private static final int OFF_PAYLOAD_LENGTH = 8;
    private final OffHeapAppendMemory bitScratch = new OffHeapAppendMemory();
    private final QwoColumnCursor cursor = new QwoColumnCursor();
    private final QwpGorillaEncoder gorillaEncoder = new QwpGorillaEncoder();
    private final OffHeapAppendMemory scratch = new OffHeapAppendMemory();
    private int bitPos;

    @Override
    public void close() {
        bitScratch.close();
        scratch.close();
    }

    /**
     * Appends one complete QWP frame for the sealed record {@code rec} at
     * {@code out}'s current position and patches its payload length.
     *
     * @return the frame's size in bytes
     * @throws IllegalArgumentException when the record's dictionary entries do
     *                                  not parse inside its dictionary region
     */
    public int write(QwoRecordReader rec, QwpBufferWriter out) {
        final int entriesLen = rec.dictEntriesLen();
        if (entriesLen < 0) {
            throw new IllegalArgumentException("malformed record dictionary [dictCount=" + rec.dictCount() + ']');
        }
        final int start = out.getPosition();
        final int blockCount = rec.blockCount();
        int tableCount = 0;
        for (int i = 0, b = rec.firstBlock(); i < blockCount; i++, b = rec.nextBlock(b)) {
            if (rec.rowCount(b) > 0) {
                tableCount++;
            }
        }
        out.putInt(QwpConstants.MAGIC_MESSAGE);
        out.putByte(rec.version());
        out.putByte(rec.flags());
        out.putShort((short) tableCount);
        out.putInt(0);
        out.putVarint(rec.dictStartId());
        out.putVarint(rec.dictCount());
        out.putBlockOfBytes(rec.dictEntriesAddr(), entriesLen);
        for (int i = 0, b = rec.firstBlock(); i < blockCount; i++, b = rec.nextBlock(b)) {
            int rows = rec.rowCount(b);
            if (rows > 0) {
                writeBlock(rec, b, rows, out);
            }
        }
        final int size = out.getPosition() - start;
        out.patchInt(start + OFF_PAYLOAD_LENGTH, size - QwpConstants.HEADER_SIZE);
        return size;
    }

    /**
     * Appends the low {@code count} bits of the LSB-first bit string at
     * {@code src} to the bit scratch at {@link #bitPos}. Every byte it writes is
     * written whole, so bits past the end are zero.
     */
    private void appendBits(long src, int count) {
        if (count == 0) {
            return;
        }
        final int shift = bitPos & 7;
        final long dst = bitScratch.pageAddress() + (bitPos >>> 3);
        int carry = shift == 0 ? 0 : Unsafe.getUnsafe().getByte(dst) & ((1 << shift) - 1);
        final int fullBytes = count >>> 3;
        for (int i = 0; i < fullBytes; i++) {
            int v = carry | (Unsafe.getUnsafe().getByte(src + i) & 0xFF) << shift;
            Unsafe.getUnsafe().putByte(dst + i, (byte) v);
            carry = v >>> 8;
        }
        final int rem = count & 7;
        if (rem != 0) {
            int v = carry | (Unsafe.getUnsafe().getByte(src + fullBytes) & ((1 << rem) - 1)) << shift;
            Unsafe.getUnsafe().putByte(dst + fullBytes, (byte) v);
            if (shift + rem > 8) {
                Unsafe.getUnsafe().putByte(dst + fullBytes + 1, (byte) (v >>> 8));
            }
        } else if (shift != 0) {
            Unsafe.getUnsafe().putByte(dst + fullBytes, (byte) carry);
        }
        bitPos += count;
    }

    /**
     * Empties the bit scratch and makes room for {@code maxBits} bits.
     */
    private void beginBits(int maxBits) {
        bitScratch.truncate();
        bitScratch.skip(((long) maxBits + 7) >>> 3);
        bitPos = 0;
    }

    private void copyChunkBytes(QwpBufferWriter out) {
        while (cursor.nextChunk()) {
            out.putBlockOfBytes(cursor.chunkBytesAddr(), cursor.chunkBytesLen());
        }
    }

    private void writeBlock(QwoRecordReader rec, int b, int rows, QwpBufferWriter out) {
        int nameLen = rec.tableNameLen(b);
        out.putVarint(nameLen);
        out.putBlockOfBytes(rec.tableNameAddr(b), nameLen);
        out.putVarint(rows);
        final int colCount = rec.colCount(b);
        out.putVarint(colCount);
        final int firstDef = rec.columnDef(b, 0);
        for (int i = 0, def = firstDef; i < colCount; i++, def = rec.nextColumnDef(def)) {
            nameLen = rec.columnNameLen(def);
            out.putVarint(nameLen);
            out.putBlockOfBytes(rec.columnNameAddr(def), nameLen);
            out.putByte(rec.columnType(def));
        }
        for (int i = 0, def = firstDef; i < colCount; i++, def = rec.nextColumnDef(def)) {
            writeColumn(rec, def, rows, out);
        }
    }

    private void writeBooleans(int rows, QwpBufferWriter out) {
        beginBits(rows);
        while (cursor.nextChunk()) {
            appendBits(cursor.chunkValuesAddr(), cursor.chunkValueCount());
        }
        out.putBlockOfBytes(bitScratch.pageAddress(), (bitPos + 7) >>> 3);
    }

    private void writeColumn(QwoRecordReader rec, int def, int rows, QwpBufferWriter out) {
        cursor.of(rec, def, rows);
        if (cursor.usesBitmap()) {
            writeNullSection(rows, out);
            cursor.rewind();
        } else {
            out.putByte((byte) 0);
        }
        switch (cursor.type()) {
            case QwpConstants.TYPE_BOOLEAN:
                writeBooleans(rows, out);
                break;
            case QwpConstants.TYPE_GEOHASH: {
                // a column whose every value is null has no precision yet; the legacy writer announces 1 bit
                int precision = cursor.prefix();
                out.putVarint(Math.max(1, precision));
                if (precision > 0) {
                    copyChunkBytes(out);
                }
                break;
            }
            case QwpConstants.TYPE_DECIMAL64:
            case QwpConstants.TYPE_DECIMAL128:
            case QwpConstants.TYPE_DECIMAL256:
                // the scale, or -1 like the legacy writer while every value is null
                out.putByte((byte) cursor.prefix());
                copyChunkBytes(out);
                break;
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
                writeTimestamps(out);
                break;
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_BINARY:
                writeOffsets(out);
                cursor.rewind();
                copyChunkBytes(out);
                break;
            default:
                // fixed-size values, SYMBOL varints, array values
                copyChunkBytes(out);
                break;
        }
    }

    /**
     * {@code 0x00} when no row is null, else {@code 0x01} and the rows' null
     * bits concatenated across chunks.
     */
    private void writeNullSection(int rows, QwpBufferWriter out) {
        int nulls = 0;
        while (cursor.nextChunk()) {
            nulls += cursor.chunkRows() - cursor.chunkValueCount();
        }
        if (nulls == 0) {
            out.putByte((byte) 0);
            return;
        }
        out.putByte((byte) 1);
        cursor.rewind();
        beginBits(rows);
        while (cursor.nextChunk()) {
            appendBits(cursor.chunkBitmapAddr(), cursor.chunkRows());
        }
        out.putBlockOfBytes(bitScratch.pageAddress(), (rows + 7) >>> 3);
    }

    /**
     * {@code valueCount + 1} {@code u32} offsets into the column's bytes, the
     * first one 0, each value's end rebased across chunks.
     */
    private void writeOffsets(QwpBufferWriter out) {
        out.putInt(0);
        while (cursor.next()) {
            if (!cursor.isNull()) {
                out.putInt((int) cursor.valueEnd());
            }
        }
    }

    /**
     * {@code [0x01][Gorilla]} when there are more than two values and every
     * delta-of-delta fits an int32, else {@code [0x00][raw int64 values]}.
     */
    private void writeTimestamps(QwpBufferWriter out) {
        scratch.truncate();
        while (cursor.nextChunk()) {
            scratch.putBlockOfBytes(cursor.chunkBytesAddr(), cursor.chunkBytesLen());
        }
        final long src = scratch.pageAddress();
        final int count = (int) (scratch.getAppendOffset() >>> 3);
        if (count > 2) {
            int encodedSize = QwpGorillaEncoder.calculateEncodedSizeIfSupported(src, count);
            if (encodedSize >= 0) {
                out.putByte(ENCODING_GORILLA);
                out.ensureCapacity(encodedSize);
                out.skip(gorillaEncoder.encodeTimestamps(out.getWriteAddress(), out.getWritableBytes(), src, count));
                return;
            }
        }
        out.putByte(ENCODING_UNCOMPRESSED);
        out.putBlockOfBytes(src, (long) count << 3);
    }
}
