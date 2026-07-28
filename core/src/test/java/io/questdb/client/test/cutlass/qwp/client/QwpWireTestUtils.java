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

public final class QwpWireTestUtils {

    /**
     * As {@link #accumulateDeltaDictionary(byte[], List, boolean)} with {@code allowGap}
     * false: the default, server-accurate model.
     */
    public static void accumulateDeltaDictionary(byte[] frame, List<String> dictionary) {
        accumulateDeltaDictionary(frame, dictionary, false);
    }

    /**
     * Folds one frame's symbol-dict delta into {@code dictionary} exactly as the server
     * does -- including REJECTING a delta whose start runs past the dictionary, which is
     * what QwpMessageCursor.parseDeltaSymbolDict raises DELTA_DICT_GAP for. Modelling
     * the pre-rejection server here made this suite unable to fail on the regression
     * class the delta dictionary introduces, and it diverged permissively: a sequence
     * that passed locally is a NACK against a real server.
     * <p>
     * {@code allowGap} keeps the old null-padding so a caller can OBSERVE a gap instead
     * of having it thrown. Only catch-up tiling assertions -- which exist to PROVE there
     * is no hole -- should pass {@code true}.
     */
    public static void accumulateDeltaDictionary(byte[] frame, List<String> dictionary, boolean allowGap) {
        if (!hasDelta(frame)) {
            return;
        }
        int[] position = {HEADER_SIZE};
        int deltaStart = readVarint(frame, position);
        int deltaCount = readVarint(frame, position);
        if (deltaStart > dictionary.size()) {
            if (!allowGap) {
                throw new DictionaryGapException(deltaStart, dictionary.size());
            }
            while (dictionary.size() < deltaStart) {
                dictionary.add(null);
            }
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

    public static byte[] buildAck(long sequence) {
        byte[] buffer = new byte[1 + Long.BYTES + Short.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put((byte) 0x00);
        byteBuffer.putLong(sequence);
        byteBuffer.putShort((short) 0);
        return buffer;
    }

    /** Builds a minimal 0-table frame carrying only a symbol-dict delta. */
    public static byte[] buildDeltaFrame(int deltaStart, String... symbols) {
        byte[][] encoded = new byte[symbols.length][];
        int payload = varintLength(deltaStart) + varintLength(symbols.length);
        for (int i = 0; i < symbols.length; i++) {
            encoded[i] = symbols[i].getBytes(StandardCharsets.UTF_8);
            payload += varintLength(encoded[i].length) + encoded[i].length;
        }
        byte[] frame = new byte[HEADER_SIZE + payload];
        ByteBuffer buffer = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(0x31505751); // "QWP1"
        buffer.put((byte) 1);
        buffer.put(FLAG_DELTA_SYMBOL_DICT);
        buffer.putShort((short) 0);
        buffer.putInt(payload);
        int[] position = {HEADER_SIZE};
        writeVarint(frame, position, deltaStart);
        writeVarint(frame, position, symbols.length);
        for (int i = 0; i < encoded.length; i++) {
            writeVarint(frame, position, encoded[i].length);
            System.arraycopy(encoded[i], 0, frame, position[0], encoded[i].length);
            position[0] += encoded[i].length;
        }
        return frame;
    }

    /** Builds a rejection response carrying {@code status} (e.g. STATUS_DICTIONARY_GAP). */
    public static byte[] buildNack(long sequence, byte status) {
        byte[] buffer = new byte[1 + Long.BYTES + Short.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put(status);
        byteBuffer.putLong(sequence);
        byteBuffer.putShort((short) 0);
        return buffer;
    }

    public static boolean hasDelta(byte[] frame) {
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
                throw new IllegalStateException("varint too long at offset " + position[0]);
            }
        }
        throw new IllegalStateException("varint truncated");
    }

    static int tableCount(byte[] frame) {
        return (frame[6] & 0xFF) | ((frame[7] & 0xFF) << 8);
    }

    private static int varintLength(int value) {
        int length = 1;
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            remaining >>>= 7;
            length++;
        }
        return length;
    }

    private static void writeVarint(byte[] target, int[] position, int value) {
        int remaining = value;
        while ((remaining & ~0x7F) != 0) {
            target[position[0]++] = (byte) ((remaining & 0x7F) | 0x80);
            remaining >>>= 7;
        }
        target[position[0]++] = (byte) remaining;
    }

    private QwpWireTestUtils() {
    }

    /** Thrown where the real decoder raises DELTA_DICT_GAP. */
    public static class DictionaryGapException extends RuntimeException {
        DictionaryGapException(int deltaStart, int dictionarySize) {
            super("delta symbol dictionary gap: deltaStartId " + deltaStart
                    + " exceeds dictionary size " + dictionarySize);
        }
    }
}
