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

import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Builds QWP egress server frames byte by byte, and parses the
 * {@code QUERY_REQUEST} a client sends, so tests spell out the wire layout
 * they exercise. Encodes the layouts of the server's
 * {@code QwpEgressFrameWriter} and decodes the one of its
 * {@code QwpEgressRequestDecoder}.
 */
public final class QwpEgressFrames {

    private QwpEgressFrames() {
    }

    /**
     * Starts a frame body with {@code msg_kind} and {@code request_id}, the
     * prelude of every per-query server frame.
     */
    public static Body body(byte msgKind, long requestId) {
        return new Body().u8(msgKind).i64(requestId);
    }

    /**
     * {@code EXEC_DONE} body up to {@code rows_affected}; append the trailers
     * the test needs.
     */
    public static Body execDone(long requestId, int opType, long rowsAffected) {
        return body(QwpEgressMsgKind.EXEC_DONE, requestId).u8(opType).varint(rowsAffected);
    }

    /**
     * Parses a {@code QUERY_REQUEST} payload. Supports the LONG binds tests
     * send; the {@code query_flags} trailer and the row-window values follow
     * the binds.
     */
    public static QueryRequest parseQueryRequest(byte[] payload) {
        Reader rd = new Reader(payload);
        Assert.assertEquals("frame must be a QUERY_REQUEST", QwpEgressMsgKind.QUERY_REQUEST, (byte) rd.u8());
        long requestId = rd.i64();
        String sql = rd.utf8((int) rd.varint());
        long initialCredit = rd.varint();
        int bindCount = (int) rd.varint();
        long[] longBinds = new long[bindCount];
        for (int i = 0; i < bindCount; i++) {
            Assert.assertEquals("tests bind LONG values only", QwpConstants.TYPE_LONG, (byte) rd.u8());
            Assert.assertEquals("tests bind non-NULL values only", 0, rd.u8());
            longBinds[i] = rd.i64();
        }
        boolean hasQueryFlags = rd.remaining() > 0;
        long queryFlags = hasQueryFlags ? rd.varint() : 0;
        long skipRows = (queryFlags & QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS) != 0 ? rd.varint() : -1;
        long maxRows = (queryFlags & QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS) != 0 ? rd.varint() : -1;
        return new QueryRequest(payload, requestId, sql, initialCredit, longBinds, hasQueryFlags, queryFlags, skipRows, maxRows, rd.remaining());
    }

    /**
     * {@code QUERY_ERROR} body up to the message; append the position trailer
     * when the test needs it.
     */
    public static Body queryError(long requestId, byte status, String message) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        return body(QwpEgressMsgKind.QUERY_ERROR, requestId).u8(status).u16(msg.length).bytes(msg);
    }

    /**
     * {@code RESULT_BATCH} carrying one LONG column {@code x}; the schema rides
     * the first batch of a query only.
     */
    public static byte[] resultBatchOfLongs(long requestId, long batchSeq, long... values) {
        Body b = body(QwpEgressMsgKind.RESULT_BATCH, requestId)
                .varint(batchSeq)
                .varint(0) // table name length
                .varint(values.length);
        if (batchSeq == 0) {
            b.varint(1).varint(1).u8('x').u8(QwpConstants.TYPE_LONG);
        }
        b.u8(0); // no NULLs
        for (long v : values) {
            b.i64(v);
        }
        return b.toFrame();
    }

    /**
     * {@code RESULT_END} body up to {@code total_rows}; append the trailers the
     * test needs.
     */
    public static Body resultEnd(long requestId, long finalSeq, long totalRows) {
        return body(QwpEgressMsgKind.RESULT_END, requestId).varint(finalSeq).varint(totalRows);
    }

    /**
     * Growable little-endian frame body. {@link #toFrame()} prepends the
     * 12-byte QWP message header.
     */
    public static final class Body {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        public Body bytes(byte[] bytes) {
            out.write(bytes, 0, bytes.length);
            return this;
        }

        public Body i32(int v) {
            for (int i = 0; i < 4; i++) {
                out.write(v >>> (8 * i));
            }
            return this;
        }

        public Body i64(long v) {
            for (int i = 0; i < 8; i++) {
                out.write((int) (v >>> (8 * i)));
            }
            return this;
        }

        public int size() {
            return out.size();
        }

        /**
         * Appends the {@code QUERY_FLAG_STATS} block: four varint durations and
         * the {@code stats_flags} byte.
         */
        public Body stats(long compileNanos, long executeNanos, long countNanos, long clientWaitNanos, int statsFlags) {
            return varint(compileNanos).varint(executeNanos).varint(countNanos).varint(clientWaitNanos).u8(statsFlags);
        }

        public byte[] toBody() {
            return out.toByteArray();
        }

        public byte[] toFrame() {
            byte[] body = out.toByteArray();
            return new Body()
                    .i32(QwpConstants.MAGIC_MESSAGE)
                    .u8(QwpConstants.VERSION)
                    .u8(0) // flags
                    .u16(0) // table_count
                    .i32(body.length)
                    .bytes(body)
                    .toBody();
        }

        public Body u16(int v) {
            out.write(v);
            out.write(v >>> 8);
            return this;
        }

        public Body u8(int v) {
            out.write(v);
            return this;
        }

        /**
         * Unsigned LEB128, the encoding of the server's {@code QwpVarint}.
         */
        public Body varint(long v) {
            while ((v & ~0x7FL) != 0) {
                out.write((int) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            out.write((int) v);
            return this;
        }
    }

    /**
     * A {@code QUERY_REQUEST} as the server decodes it. {@link #skipRows} and
     * {@link #maxRows} are -1 when the request does not carry them.
     */
    public static final class QueryRequest {
        public final boolean hasQueryFlags;
        public final long initialCredit;
        public final long[] longBinds;
        public final long maxRows;
        // the raw payload, as the client sent it
        public final byte[] payload;
        public final long queryFlags;
        public final long requestId;
        public final long skipRows;
        public final String sql;
        public final int trailingBytes;

        QueryRequest(
                byte[] payload,
                long requestId,
                String sql,
                long initialCredit,
                long[] longBinds,
                boolean hasQueryFlags,
                long queryFlags,
                long skipRows,
                long maxRows,
                int trailingBytes
        ) {
            this.payload = payload;
            this.requestId = requestId;
            this.sql = sql;
            this.initialCredit = initialCredit;
            this.longBinds = longBinds;
            this.hasQueryFlags = hasQueryFlags;
            this.queryFlags = queryFlags;
            this.skipRows = skipRows;
            this.maxRows = maxRows;
            this.trailingBytes = trailingBytes;
        }

        public boolean has(int queryFlag) {
            return (queryFlags & queryFlag) != 0;
        }

        @Override
        public String toString() {
            return "QueryRequest{requestId=" + requestId
                    + ", sql='" + sql + '\''
                    + ", hasQueryFlags=" + hasQueryFlags
                    + ", queryFlags=0x" + Long.toHexString(queryFlags)
                    + ", skipRows=" + skipRows
                    + ", maxRows=" + maxRows
                    + ", trailingBytes=" + trailingBytes
                    + '}';
        }
    }

    private static final class Reader {
        private final byte[] buf;
        private int pos;

        Reader(byte[] buf) {
            this.buf = buf;
        }

        long i64() {
            Assert.assertTrue("request ends inside an i64", remaining() >= 8);
            long v = 0;
            for (int i = 0; i < 8; i++) {
                v |= (long) (buf[pos++] & 0xFF) << (8 * i);
            }
            return v;
        }

        int remaining() {
            return buf.length - pos;
        }

        int u8() {
            Assert.assertTrue("request ends before the next byte", remaining() > 0);
            return buf[pos++] & 0xFF;
        }

        String utf8(int len) {
            Assert.assertTrue("request ends inside the SQL text", remaining() >= len);
            String s = new String(buf, pos, len, StandardCharsets.UTF_8);
            pos += len;
            return s;
        }

        long varint() {
            long v = 0;
            for (int shift = 0; ; shift += 7) {
                int b = u8();
                v |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return v;
                }
                Assert.assertTrue("varint longer than 10 bytes", shift < 63);
            }
        }
    }
}
