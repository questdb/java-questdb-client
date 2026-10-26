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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;

/**
 * Builds small QWP frames that carry a first-table row count and an 8-byte id,
 * and appends them to a {@link CursorSendEngine}, so a test can follow each
 * frame from the ring through the wire to {@link QwpServerModel#committedFrames()}.
 * <p>
 * Layout: the 12-byte header (magic {@code QWP1}, version 1,
 * {@code flags | FLAG_DELTA_SYMBOL_DICT}, table count 1, payload length), an
 * empty delta section {@code [0][0]}, one table {@code [1]['t'][rows varint][0]}
 * (name {@code t}, no columns), {@code padBytes} zero bytes, then the id as 8
 * little-endian bytes.
 */
public final class WindowTestFrames {
    private static final int ID_BYTES = Long.BYTES;
    private static final int QWP_VERSION = 1;

    private WindowTestFrames() {
    }

    /**
     * Appends a frame of {@code rows} rows (at least 1) tagged with {@code id}.
     */
    public static void append(CursorSendEngine engine, int flags, int rows, long id) {
        append(engine, flags, rows, id, 0);
    }

    /**
     * As {@link #append(CursorSendEngine, int, int, long)}, with {@code padBytes}
     * zero bytes before the id to give the frame a chosen size.
     */
    public static void append(CursorSendEngine engine, int flags, int rows, long id, int padBytes) {
        if (rows < 1) {
            throw new IllegalArgumentException("rows must be at least 1: " + rows);
        }
        if (padBytes < 0) {
            throw new IllegalArgumentException("padBytes must not be negative: " + padBytes);
        }
        // delta start, delta count, name length, name, rows, column count, pad, id
        int payloadLen = 1 + 1 + 1 + 1 + NativeBufferWriter.varintSize(rows) + 1 + padBytes + ID_BYTES;
        int size = QwpConstants.HEADER_SIZE + payloadLen;
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, size, (byte) 0);
            Unsafe.getUnsafe().putByte(buf, (byte) 'Q');
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 'W');
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(buf + 3, (byte) '1');
            Unsafe.getUnsafe().putByte(buf + 4, (byte) QWP_VERSION);
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS,
                    (byte) (flags | QwpConstants.FLAG_DELTA_SYMBOL_DICT));
            putLittleEndian(buf + 6, 1, 2);
            putLittleEndian(buf + 8, payloadLen, 4);
            long p = buf + QwpConstants.HEADER_SIZE;
            Unsafe.getUnsafe().putByte(p++, (byte) 0); // delta start
            Unsafe.getUnsafe().putByte(p++, (byte) 0); // delta count
            Unsafe.getUnsafe().putByte(p++, (byte) 1); // table name length
            Unsafe.getUnsafe().putByte(p++, (byte) 't');
            p = NativeBufferWriter.writeVarint(p, rows);
            Unsafe.getUnsafe().putByte(p++, (byte) 0); // column count
            p += padBytes;
            putLittleEndian(p, id, ID_BYTES);
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * The id of a frame {@link #append} built: its last 8 bytes, little-endian;
     * -1 for a table-less frame, such as a commit-only or dictionary frame.
     */
    public static long id(byte[] frame) {
        if (frame.length < QwpConstants.HEADER_SIZE + ID_BYTES || QwpServerModel.tableCount(frame) == 0) {
            return -1;
        }
        long id = 0;
        for (int i = 0; i < ID_BYTES; i++) {
            id |= (frame[frame.length - ID_BYTES + i] & 0xFFL) << (8 * i);
        }
        return id;
    }

    private static void putLittleEndian(long addr, long value, int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) (value >>> (8 * i)));
        }
    }
}
