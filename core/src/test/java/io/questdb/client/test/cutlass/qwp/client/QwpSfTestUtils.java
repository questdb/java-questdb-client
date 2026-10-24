/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Helpers the store-and-forward, record and pool tests share: a test-server
 * handler that logs and acknowledges every QWP frame ({@link FrameLog}), and
 * readers and a corrupter for a slot's segment file.
 */
public final class QwpSfTestUtils {

    private QwpSfTestUtils() {
    }

    /**
     * Whether {@code frame} contains {@code text} as ASCII bytes.
     */
    public static boolean containsAscii(byte[] frame, String text) {
        byte[] needle = text.getBytes(StandardCharsets.US_ASCII);
        for (int i = 0, n = frame.length - needle.length; i <= n; i++) {
            int j = 0;
            while (j < needle.length && frame[i + j] == needle[j]) {
                j++;
            }
            if (j == needle.length) {
                return true;
            }
        }
        return false;
    }

    /**
     * Flips a byte of the first committed value of the record at
     * {@code recordOffset} in the segment file at {@code segmentPath}, so its
     * row CRC no longer matches. The record's first column must be a LONG.
     */
    public static void flipCommittedValueByte(String segmentPath, long recordOffset) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(segmentPath, "rw")) {
            int capacity = Integer.reverseBytes(readIntAt(file, recordOffset + 4));
            byte[] payload = new byte[capacity];
            file.seek(recordOffset + MmapSegment.FRAME_HEADER_SIZE);
            file.readFully(payload);
            long addr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < capacity; i++) {
                    Unsafe.getUnsafe().putByte(addr + i, payload[i]);
                }
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(addr, capacity);
                int def = reader.columnDef(reader.firstBlock(), 0);
                long value = reader.chunkValuesAddr(reader.firstChunk(def), QwpConstants.TYPE_LONG,
                        reader.columnUsesBitmap(def));
                long valueOffset = recordOffset + MmapSegment.FRAME_HEADER_SIZE + (value - addr);
                file.seek(valueOffset);
                int b = file.readByte();
                file.seek(valueOffset);
                file.writeByte(b ^ 0x40);
            } finally {
                Unsafe.free(addr, capacity, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    /**
     * Reads the little-endian int at {@code offset} of the file at {@code path}.
     */
    public static int readInt(String path, long offset) throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
            return Integer.reverseBytes(readIntAt(file, offset));
        }
    }

    private static int readIntAt(RandomAccessFile file, long offset) throws IOException {
        file.seek(offset);
        return file.readInt();
    }

    /**
     * Parses every QWP frame in each WebSocket message (a read can carry more
     * than one on replay), records its flags byte, table count and bytes, and acks
     * cumulatively so the sender never backs up.
     */
    public static final class FrameLog implements TestWebSocketServer.WebSocketServerHandler {
        private static final int HEADER_SIZE = 12;
        private static final int OFFSET_FLAGS = 5;
        private static final int OFFSET_PAYLOAD_LENGTH = 8;
        private static final int OFFSET_TABLE_COUNT = 6;
        private final List<Integer> flags = new ArrayList<>();
        private final List<byte[]> frames = new ArrayList<>();
        private final List<Integer> tableCounts = new ArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        public void awaitAscii(String text, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (!hasAscii(text) && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue("expected a frame containing \"" + text + "\" within " + timeoutMillis + " ms", hasAscii(text));
        }

        public void awaitCommitFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (commitFrames() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue("expected at least " + atLeast + " commit frame(s) within " + timeoutMillis + " ms, got " + commitFrames(),
                    commitFrames() >= atLeast);
        }

        public void awaitDataFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (dataFrames() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue("expected at least " + atLeast + " data frame(s) within " + timeoutMillis + " ms, got " + dataFrames(),
                    dataFrames() >= atLeast);
        }

        /**
         * Count of rowless frames with the defer flag clear: the commit messages.
         */
        public synchronized int commitFrames() {
            int n = 0;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) == 0 && (flags.get(i) & 0x01) == 0) {
                    n++;
                }
            }
            return n;
        }

        public synchronized int dataFrames() {
            int n = 0;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) > 0) {
                    n++;
                }
            }
            return n;
        }

        /**
         * Defer flag of the i-th data frame (frames with a table count above zero).
         */
        public synchronized boolean deferFlag(int dataFrameIndex) {
            int seen = -1;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) > 0 && ++seen == dataFrameIndex) {
                    return (flags.get(i) & 0x01) != 0;
                }
            }
            throw new AssertionError("no data frame at index " + dataFrameIndex);
        }

        /**
         * A copy of every frame {@link #onBinaryMessage} parsed, in arrival order,
         * each frame's header and payload as one array.
         */
        public synchronized List<byte[]> frames() {
            return new ArrayList<>(frames);
        }

        /**
         * Whether any frame parsed so far contains {@code text} as ASCII bytes.
         */
        public synchronized boolean hasAscii(String text) {
            for (int i = 0, n = frames.size(); i < n; i++) {
                if (containsAscii(frames.get(i), text)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            int pos = 0;
            int framesInMessage = 0;
            while (pos + HEADER_SIZE <= data.length) {
                int tableCount = (data[pos + OFFSET_TABLE_COUNT] & 0xFF) | ((data[pos + OFFSET_TABLE_COUNT + 1] & 0xFF) << 8);
                long payloadLength = (data[pos + OFFSET_PAYLOAD_LENGTH] & 0xFFL)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 1] & 0xFFL) << 8)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 2] & 0xFFL) << 16)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 3] & 0xFFL) << 24);
                if (payloadLength < 0 || pos + HEADER_SIZE + payloadLength > data.length) {
                    break;
                }
                flags.add(data[pos + OFFSET_FLAGS] & 0xFF);
                frames.add(Arrays.copyOfRange(data, pos, pos + HEADER_SIZE + (int) payloadLength));
                tableCounts.add(tableCount);
                framesInMessage++;
                pos += HEADER_SIZE + (int) payloadLength;
            }
            nextSeq += Math.max(1, framesInMessage);
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq - 1));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Table count of the i-th data frame (frames with a table count above zero).
         */
        public synchronized int tableCount(int dataFrameIndex) {
            int seen = -1;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) > 0 && ++seen == dataFrameIndex) {
                    return tableCounts.get(i);
                }
            }
            throw new AssertionError("no data frame at index " + dataFrameIndex);
        }
    }
}
