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

package io.questdb.client.test.cutlass.qwp.websocket;

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The server's commit semantics for QWP frames, independent of any transport, so
 * tests can assert what a real server would have committed.
 * <p>
 * Per connection, wire sequences count from 0, one per received frame. For each
 * frame, in order:
 * <ul>
 *   <li>after a rejection on that connection, the frame is recorded and otherwise
 *       ignored: no response, no commit;</li>
 *   <li>a frame the {@link Rejector} rejects rolls back the connection's open
 *       transaction and yields {@code NACK(seq, status)}, followed by every ack
 *       {@link #delayAckUntilNextNack} withheld on that connection;</li>
 *   <li>a frame carrying {@code FLAG_DEFER_COMMIT} joins the open transaction and
 *       yields nothing (the server withholds the ack of a deferred frame);</li>
 *   <li>any other frame commits the open transaction plus itself and yields
 *       {@code ACK(seq)}, unless that ack is delayed.</li>
 * </ul>
 * The model assumes a single live connection at a time, the way one sender
 * uses the server: {@link #openConnection()} rolls back the open transaction of
 * every earlier connection, as a server does when the previous socket dies, so
 * two connections that interleave frames do not model two concurrent senders.
 * <p>
 * Thread-safe: an I/O thread and a test thread may use one model at once. The
 * returned lists are snapshots.
 */
public final class QwpServerModel {
    private final List<byte[]> committedFrames = new ArrayList<>();
    private final List<ConnectionState> connectionStates = new ArrayList<>();
    private final Map<Integer, Set<Long>> delayedAcks = new HashMap<>();
    private final List<Integer> frameConnections = new ArrayList<>();
    private final List<byte[]> frames = new ArrayList<>();
    private final List<Integer> socketWriteBytes = new ArrayList<>();
    private long committedRows;
    private int commits;
    private Rejector rejector;

    /**
     * Row count of the frame's first table, or -1 when the frame is not QWP, has no
     * table, is zstd-compressed, or is truncated before the row count.
     */
    public static long firstTableRowCount(byte[] frame) {
        if (frame.length < QwpConstants.HEADER_SIZE
                || readIntLe(frame, 0) != QwpConstants.MAGIC_MESSAGE
                || tableCount(frame) == 0
                || (flags(frame) & QwpConstants.FLAG_ZSTD) != 0) {
            return -1;
        }
        int[] position = {QwpConstants.HEADER_SIZE};
        if ((flags(frame) & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0) {
            if (readVarint(frame, position) < 0) {
                return -1;
            }
            long deltaCount = readVarint(frame, position);
            if (deltaCount < 0) {
                return -1;
            }
            for (long i = 0; i < deltaCount; i++) {
                if (!skipLengthPrefixed(frame, position)) {
                    return -1;
                }
            }
        }
        if (!skipLengthPrefixed(frame, position)) {
            return -1;
        }
        return readVarint(frame, position);
    }

    /**
     * The flags byte (header offset 5) of a QWP frame, as an unsigned value.
     */
    public static int flags(byte[] frame) {
        return frame[QwpConstants.HEADER_OFFSET_FLAGS] & 0xFF;
    }

    /**
     * The table count (header offsets 6-7, little-endian) of a QWP frame.
     */
    public static int tableCount(byte[] frame) {
        return QwpWireTestUtils.tableCount(frame);
    }

    /**
     * Number of commits: accepted frames without {@code FLAG_DEFER_COMMIT}.
     */
    public synchronized int commits() {
        return commits;
    }

    /**
     * Every frame each commit covered, in commit order. A frame committed twice
     * (a replay after a lost ack) appears twice.
     */
    public synchronized List<byte[]> committedFrames() {
        return new ArrayList<>(committedFrames);
    }

    /**
     * Sum of {@link #firstTableRowCount} over {@link #committedFrames()}: exact
     * for frames of one table, which is what the tests using it ship.
     */
    public synchronized long committedRows() {
        return committedRows;
    }

    /**
     * Number of connections opened so far.
     */
    public synchronized int connections() {
        return connectionStates.size();
    }

    /**
     * Withholds the ack of {@code seq} on {@code connection}; it is sent right
     * after the next NACK on that connection, or never. Call it before the frame
     * arrives; the connection need not be open yet.
     */
    public synchronized void delayAckUntilNextNack(int connection, long seq) {
        Set<Long> seqs = delayedAcks.get(connection);
        if (seqs == null) {
            seqs = new HashSet<>();
            delayedAcks.put(connection, seqs);
        }
        seqs.add(seq);
    }

    /**
     * The connection of each frame in {@link #frames()}, index for index.
     */
    public synchronized List<Integer> frameConnections() {
        return new ArrayList<>(frameConnections);
    }

    /**
     * Every frame received, on every connection, in arrival order.
     */
    public synchronized List<byte[]> frames() {
        return new ArrayList<>(frames);
    }

    /**
     * Opens a connection and returns its id (0, 1, ...). Rolls back the open
     * transaction of every earlier connection.
     */
    public synchronized int openConnection() {
        for (int i = 0, n = connectionStates.size(); i < n; i++) {
            connectionStates.get(i).openTransaction.clear();
        }
        connectionStates.add(new ConnectionState());
        return connectionStates.size() - 1;
    }

    /**
     * Applies one received frame and returns the responses to send for it, in
     * order (possibly none). The model keeps {@code frame}; the caller must not
     * change it afterwards.
     */
    public synchronized List<byte[]> receive(int connection, byte[] frame) {
        if (connection < 0 || connection >= connectionStates.size()) {
            throw new IllegalArgumentException("unknown connection " + connection);
        }
        ConnectionState state = connectionStates.get(connection);
        long seq = state.nextSeq++;
        frames.add(frame);
        frameConnections.add(connection);
        List<byte[]> responses = new ArrayList<>();
        if (state.isRejected) {
            return responses;
        }
        byte status = rejector != null ? rejector.statusFor(connection, seq, frame) : 0;
        if (status != 0) {
            state.openTransaction.clear();
            state.isRejected = true;
            responses.add(QwpWireTestUtils.buildNack(seq, status));
            responses.addAll(state.heldAcks);
            state.heldAcks.clear();
            return responses;
        }
        state.openTransaction.add(frame);
        if (frame.length >= QwpConstants.HEADER_SIZE
                && (flags(frame) & QwpConstants.FLAG_DEFER_COMMIT) != 0) {
            return responses;
        }
        for (int i = 0, n = state.openTransaction.size(); i < n; i++) {
            byte[] committed = state.openTransaction.get(i);
            committedFrames.add(committed);
            long rows = firstTableRowCount(committed);
            if (rows > 0) {
                committedRows += rows;
            }
        }
        state.openTransaction.clear();
        commits++;
        byte[] ack = QwpWireTestUtils.buildAck(seq);
        Set<Long> delayed = delayedAcks.get(connection);
        if (delayed != null && delayed.remove(seq)) {
            state.heldAcks.add(ack);
        } else {
            responses.add(ack);
        }
        return responses;
    }

    /**
     * Records one socket write of {@code bytes} bytes. Transports that see socket
     * writes (an in-process client) call it; {@link CommitModelHandler} does not.
     */
    public synchronized void recordSocketWrite(int bytes) {
        socketWriteBytes.add(bytes);
    }

    /**
     * Installs the rejector consulted for every frame; {@code null} accepts all.
     */
    public synchronized void setRejector(Rejector rejector) {
        this.rejector = rejector;
    }

    /**
     * The byte count of each recorded socket write, in order.
     */
    public synchronized List<Integer> socketWriteBytes() {
        return new ArrayList<>(socketWriteBytes);
    }

    /**
     * Number of recorded socket writes.
     */
    public synchronized int socketWrites() {
        return socketWriteBytes.size();
    }

    private static int readIntLe(byte[] frame, int offset) {
        return (frame[offset] & 0xFF)
                | ((frame[offset + 1] & 0xFF) << 8)
                | ((frame[offset + 2] & 0xFF) << 16)
                | ((frame[offset + 3] & 0xFF) << 24);
    }

    // Unsigned LEB128 at position[0]; advances it. -1 when truncated or wider than 63 bits.
    private static long readVarint(byte[] frame, int[] position) {
        long result = 0;
        int shift = 0;
        while (position[0] < frame.length && shift < 63) {
            int b = frame[position[0]++] & 0xFF;
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
        return -1;
    }

    private static boolean skipLengthPrefixed(byte[] frame, int[] position) {
        long length = readVarint(frame, position);
        if (length < 0 || position[0] + length > frame.length) {
            return false;
        }
        position[0] += (int) length;
        return true;
    }

    /**
     * Decides the fate of each received frame.
     */
    public interface Rejector {
        /**
         * @return 0 to accept the frame, otherwise the NACK status to answer with
         */
        byte statusFor(int connection, long seq, byte[] frame);
    }

    private static final class ConnectionState {
        private final List<byte[]> heldAcks = new ArrayList<>();
        private final List<byte[]> openTransaction = new ArrayList<>();
        private boolean isRejected;
        private long nextSeq;
    }
}
