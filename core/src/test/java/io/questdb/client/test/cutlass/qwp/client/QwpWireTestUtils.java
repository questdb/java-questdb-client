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

package io.questdb.client.test.cutlass.qwp.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.FLAG_DELTA_SYMBOL_DICT;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE;

final class QwpWireTestUtils {

    static void accumulateDeltaDictionary(byte[] frame, List<String> dictionary) {
        if (!hasDelta(frame)) {
            return;
        }
        int[] position = {HEADER_SIZE};
        int deltaStart = readVarint(frame, position);
        int deltaCount = readVarint(frame, position);
        while (dictionary.size() < deltaStart) {
            dictionary.add(null);
        }
        for (int i = 0; i < deltaCount; i++) {
            int length = readVarint(frame, position);
            String symbol = new String(frame, position[0], length, StandardCharsets.UTF_8);
            position[0] += length;
            int symbolId = deltaStart + i;
            while (dictionary.size() <= symbolId) {
                dictionary.add(null);
            }
            dictionary.set(symbolId, symbol);
        }
    }

    static byte[] buildAck(long sequence) {
        byte[] buffer = new byte[1 + Long.BYTES + Short.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put((byte) 0x00);
        byteBuffer.putLong(sequence);
        byteBuffer.putShort((short) 0);
        return buffer;
    }

    static boolean hasDelta(byte[] frame) {
        return frame.length >= HEADER_SIZE && (frame[5] & FLAG_DELTA_SYMBOL_DICT) != 0;
    }

    static int readVarint(byte[] buffer, int[] position) {
        int result = 0;
        int shift = 0;
        while (position[0] < buffer.length) {
            int b = buffer[position[0]++] & 0xFF;
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 28) {
                throw new IllegalStateException("varint too long");
            }
        }
        throw new IllegalStateException("varint truncated");
    }

    static int tableCount(byte[] frame) {
        return (frame[6] & 0xFF) | ((frame[7] & 0xFF) << 8);
    }

    private QwpWireTestUtils() {
    }
}
