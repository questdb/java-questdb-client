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

import io.questdb.client.cutlass.qwp.client.QueryEvent;
import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.cutlass.qwp.client.QwpEgressIoThread;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryDetails;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Decoding of the trailers the query-details flags add to the terminator
 * frames: {@code row_count} and the statistics block on {@code RESULT_END},
 * the statistics block on {@code EXEC_DONE}, and {@code position} on
 * {@code QUERY_ERROR}. The frames are built byte by byte in the layout of the
 * server's {@code QwpEgressFrameWriter}, and decoded through the same static
 * decoders the I/O thread runs, with the flags the request carried.
 */
public class QwpEgressTerminatorDecodeTest {
    private static final int ALL_FLAGS = QwpEgressMsgKind.QUERY_FLAG_RESET_DICT
            | QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION
            | QwpEgressMsgKind.QUERY_FLAG_STATS
            | QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS
            | QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS
            | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS;
    private static final int PRELUDE = QwpConstants.HEADER_SIZE + 1 + 8;
    private static final long[] VARINT_VALUES = {
            0, 1, 127, 128, 16_383, 16_384, 1_000_000_000L, Long.MAX_VALUE - 1, Long.MAX_VALUE
    };

    @Test
    public void testExecDoneCarriesNoRowCount() throws Exception {
        // COUNT_ROWS does not apply to statements without rows: the statistics
        // follow rows_affected directly, and a frame without trailers is whole.
        TestUtils.assertMemoryLeak(() -> {
            byte[] withStats = QwpEgressFrames.execDone(3, 2, 30).stats(11, 22, 0, 44, 1).toFrame();
            QueryEvent ev = decodeExecDone(withStats, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS);
            Assert.assertEquals(30, ev.rowsAffected);
            assertStats(ev.details, 11, 22, 0, 44, true);
            Assert.assertFalse(ev.details.hasRowCount());
            Assert.assertEquals(-1, ev.details.getRowCount());

            ev = decodeExecDone(QwpEgressFrames.execDone(3, 2, 30).toFrame(), QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS);
            Assert.assertEquals(30, ev.rowsAffected);
            assertNoDetails(ev.details);
        });
    }

    @Test
    public void testExecDoneStats() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (long v : VARINT_VALUES) {
                for (int jit = 0; jit < 2; jit++) {
                    byte[] frame = QwpEgressFrames.execDone(3, 7, v).stats(v, Long.MAX_VALUE - v, v / 2, v / 3, jit).toFrame();
                    QueryEvent ev = decodeExecDone(frame, QwpEgressMsgKind.QUERY_FLAG_STATS);
                    Assert.assertEquals(QueryEvent.KIND_EXEC_DONE, ev.kind);
                    Assert.assertEquals(7, ev.opType);
                    Assert.assertEquals(v, ev.rowsAffected);
                    assertStats(ev.details, v, Long.MAX_VALUE - v, v / 2, v / 3, jit == 1);
                    Assert.assertFalse(ev.details.hasRowCount());
                }
            }
        });
    }

    @Test
    public void testExecDoneTruncatedAtEveryByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            List<String> fields = new ArrayList<>();
            QwpEgressFrames.Body body = new QwpEgressFrames.Body();
            body.u8(QwpEgressMsgKind.EXEC_DONE).i64(3);
            appendField(body, fields, "op_type", b -> b.u8(2));
            appendField(body, fields, "rows_affected", b -> b.varint(16_384));
            appendStatsFields(body, fields);
            assertTruncatedAtEveryByte(body.toFrame(), fields, "EXEC_DONE", QwpEgressMsgKind.QUERY_FLAG_STATS, true);
        });
    }

    @Test
    public void testExecDoneVarintOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (byte[] bad : overflowingVarints()) {
                byte[] frame = new QwpEgressFrames.Body().u8(QwpEgressMsgKind.EXEC_DONE).i64(3).u8(2).bytes(bad).toFrame();
                assertDecodeFails("EXEC_DONE rows_affected varint overflow", frame, 0, true);
            }
            for (String field : new String[]{"compile_ns", "execute_ns", "count_ns", "client_wait_ns"}) {
                for (byte[] bad : overflowingVarints()) {
                    byte[] frame = statsWithBadField(QwpEgressFrames.execDone(3, 2, 30), field, bad).toFrame();
                    assertDecodeFails("EXEC_DONE " + field + " varint overflow", frame, QwpEgressMsgKind.QUERY_FLAG_STATS, true);
                }
            }
        });
    }

    @Test
    public void testExecDoneWithoutFlagsIgnoresTrailers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] frame = QwpEgressFrames.execDone(3, 2, 30).stats(11, 22, 33, 44, 1).toFrame();
            QueryEvent ev = decodeExecDone(frame, 0);
            Assert.assertEquals(2, ev.opType);
            Assert.assertEquals(30, ev.rowsAffected);
            assertNoDetails(ev.details);
        });
    }

    @Test
    public void testQueryErrorEmptyMessageWithPosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] frame = QwpEgressFrames.queryError(5, WebSocketResponse.STATUS_PARSE_ERROR, "").i32(3).toFrame();
            QueryEvent ev = decodeError(frame, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
            Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
            Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, ev.errorStatus);
            Assert.assertEquals("", ev.errorMessage);
            Assert.assertEquals(3, ev.errorPosition);
        });
    }

    @Test
    public void testQueryErrorMalformedWithPositionRequested() throws Exception {
        // the guards that predate the position trailer hold when it is requested
        TestUtils.assertMemoryLeak(() -> {
            byte[] truncated = new QwpEgressFrames.Body().u8(QwpEgressMsgKind.QUERY_ERROR).i64(5).u8(5).u8(4).toFrame();
            QueryEvent ev = decodeError(truncated, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
            Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, ev.errorStatus);
            Assert.assertEquals("QUERY_ERROR frame truncated before msg_len", ev.errorMessage);
            Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);

            // msg_len claims the message and the position, and one byte more
            byte[] overrun = new QwpEgressFrames.Body()
                    .u8(QwpEgressMsgKind.QUERY_ERROR).i64(5).u8(5).u16(9).bytes("boom".getBytes()).i32(7)
                    .toFrame();
            ev = decodeError(overrun, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
            Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, ev.errorStatus);
            Assert.assertEquals("QUERY_ERROR msg_len 9 exceeds frame remainder 8", ev.errorMessage);
            Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);
        });
    }

    @Test
    public void testQueryErrorPosition() throws Exception {
        // 'é' and '€' are one char each but 2 and 3 UTF-8 bytes, and the emoji is
        // two chars and 4 bytes: the message survives the trailer whole
        final String message = "table does not exist [table=é€\uD83D\uDE00]";
        TestUtils.assertMemoryLeak(() -> {
            for (int position : new int[]{QwpEgressMsgKind.NO_ERROR_POSITION, 0, 1, 14, 65_535, Integer.MAX_VALUE}) {
                byte[] frame = QwpEgressFrames.queryError(5, WebSocketResponse.STATUS_PARSE_ERROR, message).i32(position).toFrame();
                for (long flags : new long[]{QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION, ALL_FLAGS}) {
                    QueryEvent ev = decodeError(frame, flags);
                    Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
                    Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, ev.errorStatus);
                    Assert.assertEquals(message, ev.errorMessage);
                    Assert.assertEquals(position, ev.errorPosition);
                }
            }
        });
    }

    @Test
    public void testQueryErrorPositionFollowedByMoreBytes() throws Exception {
        // bytes after the known fields are ignored, as a newer server may add some
        TestUtils.assertMemoryLeak(() -> {
            byte[] frame = QwpEgressFrames.queryError(5, QwpConstants.STATUS_CANCELLED, "cancelled by client")
                    .i32(QwpEgressMsgKind.NO_ERROR_POSITION).u8(1).u8(2).u8(3).toFrame();
            QueryEvent ev = decodeError(frame, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
            Assert.assertEquals(QwpConstants.STATUS_CANCELLED, ev.errorStatus);
            Assert.assertEquals("cancelled by client", ev.errorMessage);
            Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);

            frame = QwpEgressFrames.queryError(5, QwpConstants.STATUS_CANCELLED, "cancelled by client")
                    .i32(42).u8(1).toFrame();
            Assert.assertEquals(42, decodeError(frame, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION).errorPosition);
        });
    }

    @Test
    public void testQueryErrorPositionTrailerCutShort() throws Exception {
        // 1 to 3 bytes after the message: neither a baseline frame nor a whole
        // position, so the event names the defect and keeps what the server said
        TestUtils.assertMemoryLeak(() -> {
            for (int extra = 1; extra < Integer.BYTES; extra++) {
                QwpEgressFrames.Body body = QwpEgressFrames.queryError(5, WebSocketResponse.STATUS_PARSE_ERROR, "Invalid column: nope");
                for (int i = 0; i < extra; i++) {
                    body.u8(7);
                }
                QueryEvent ev = decodeError(body.toFrame(), QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
                Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
                Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, ev.errorStatus);
                Assert.assertEquals("QUERY_ERROR frame truncated mid position [status=5, message=Invalid column: nope]", ev.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);
            }
        });
    }

    @Test
    public void testQueryErrorTrailerIgnoredWhenNotRequested() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] frame = QwpEgressFrames.queryError(5, WebSocketResponse.STATUS_PARSE_ERROR, "[14] table does not exist")
                    .i32(14).toFrame();
            for (long flags : new long[]{0, ALL_FLAGS & ~QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION}) {
                QueryEvent ev = decodeError(frame, flags);
                Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, ev.errorStatus);
                Assert.assertEquals("[14] table does not exist", ev.errorMessage);
                Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);
            }
            // the decoder of a request without flags
            QueryEvent ev = decodeBaselineError(frame);
            Assert.assertEquals("[14] table does not exist", ev.errorMessage);
            Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);
        });
    }

    @Test
    public void testQueryErrorWithoutTrailerWhenPositionRequested() throws Exception {
        // the server answers a request it could not decode, or one it rejects
        // while another query streams, with the baseline frame
        TestUtils.assertMemoryLeak(() -> {
            String message = "Phase 1 egress supports a single in-flight query per connection";
            byte[] frame = QwpEgressFrames.queryError(5, WebSocketResponse.STATUS_PARSE_ERROR, message).toFrame();
            QueryEvent ev = decodeError(frame, QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION);
            Assert.assertEquals(QueryEvent.KIND_ERROR, ev.kind);
            Assert.assertEquals(WebSocketResponse.STATUS_PARSE_ERROR, ev.errorStatus);
            Assert.assertEquals(message, ev.errorMessage);
            Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, ev.errorPosition);
        });
    }

    @Test
    public void testResultEndRowCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (long rowCount : VARINT_VALUES) {
                byte[] frame = QwpEgressFrames.resultEnd(7, 0, 5).varint(rowCount).toFrame();
                QueryEvent ev = decodeResultEnd(frame, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS);
                Assert.assertEquals(QueryEvent.KIND_END, ev.kind);
                Assert.assertEquals(5, ev.totalRows);
                Assert.assertTrue(ev.details.hasRowCount());
                Assert.assertEquals(rowCount, ev.details.getRowCount());
                Assert.assertFalse(ev.details.hasStats());
            }
        });
    }

    @Test
    public void testResultEndRowCountThenStats() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] frame = QwpEgressFrames.resultEnd(7, 2, 50).varint(100).stats(1_500, 900_000, 300, 0, 0).toFrame();
            for (long flags : new long[]{QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS, ALL_FLAGS}) {
                QueryEvent ev = decodeResultEnd(frame, flags);
                Assert.assertEquals(50, ev.totalRows);
                Assert.assertTrue(ev.details.hasRowCount());
                Assert.assertEquals(100, ev.details.getRowCount());
                assertStats(ev.details, 1_500, 900_000, 300, 0, false);
                Assert.assertEquals("QwpQueryDetails{rowCount=100, compileNanos=1500, executeNanos=900000, "
                        + "countNanos=300, clientWaitNanos=0, jitCompiled=false}", ev.details.toString());
            }
        });
    }

    @Test
    public void testResultEndStats() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (long v : VARINT_VALUES) {
                for (int jit = 0; jit < 2; jit++) {
                    byte[] frame = QwpEgressFrames.resultEnd(7, 3, 40).stats(v, Long.MAX_VALUE - v, v / 2, v / 3, jit).toFrame();
                    QueryEvent ev = decodeResultEnd(frame, QwpEgressMsgKind.QUERY_FLAG_STATS);
                    Assert.assertEquals(40, ev.totalRows);
                    assertStats(ev.details, v, Long.MAX_VALUE - v, v / 2, v / 3, jit == 1);
                    Assert.assertFalse(ev.details.hasRowCount());
                    Assert.assertEquals(-1, ev.details.getRowCount());
                }
            }
        });
    }

    @Test
    public void testResultEndTrailersFollowedByMoreBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] frame = QwpEgressFrames.resultEnd(7, 2, 50).varint(100).stats(1, 2, 3, 4, 1).u8(0xFF).u8(0x80).toFrame();
            QueryEvent ev = decodeResultEnd(frame, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS);
            Assert.assertEquals(100, ev.details.getRowCount());
            assertStats(ev.details, 1, 2, 3, 4, true);
        });
    }

    @Test
    public void testResultEndTruncatedAtEveryByte() throws Exception {
        // multi-byte varints in every field, so cuts land inside each of them
        TestUtils.assertMemoryLeak(() -> {
            List<String> fields = new ArrayList<>();
            QwpEgressFrames.Body body = new QwpEgressFrames.Body();
            body.u8(QwpEgressMsgKind.RESULT_END).i64(7);
            appendField(body, fields, "final_seq", b -> b.varint(300));
            appendField(body, fields, "total_rows", b -> b.varint(16_384));
            appendField(body, fields, "row_count", b -> b.varint(1_000_000));
            appendStatsFields(body, fields);
            assertTruncatedAtEveryByte(body.toFrame(), fields, "RESULT_END",
                    QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS, false);
        });
    }

    @Test
    public void testResultEndVarintOverflow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (byte[] bad : overflowingVarints()) {
                byte[] frame = QwpEgressFrames.body(QwpEgressMsgKind.RESULT_END, 7).bytes(bad).varint(5).toFrame();
                assertDecodeFails("RESULT_END final_seq varint overflow", frame, 0, false);
                frame = QwpEgressFrames.body(QwpEgressMsgKind.RESULT_END, 7).varint(0).bytes(bad).toFrame();
                assertDecodeFails("RESULT_END total_rows varint overflow", frame, 0, false);
                frame = QwpEgressFrames.resultEnd(7, 0, 5).bytes(bad).toFrame();
                assertDecodeFails("RESULT_END row_count varint overflow", frame, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS, false);
            }
            for (String field : new String[]{"compile_ns", "execute_ns", "count_ns", "client_wait_ns"}) {
                for (byte[] bad : overflowingVarints()) {
                    byte[] frame = statsWithBadField(QwpEgressFrames.resultEnd(7, 0, 5), field, bad).toFrame();
                    assertDecodeFails("RESULT_END " + field + " varint overflow", frame, QwpEgressMsgKind.QUERY_FLAG_STATS, false);
                }
            }
        });
    }

    @Test
    public void testResultEndVarintUsesAll64Bits() throws Exception {
        // a tenth byte of 0x01 sets bit 63, the last bit a long holds
        TestUtils.assertMemoryLeak(() -> {
            byte[] bit63 = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
                    (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01};
            byte[] frame = QwpEgressFrames.resultEnd(7, 0, 5).bytes(bit63).toFrame();
            QueryEvent ev = decodeResultEnd(frame, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS);
            Assert.assertTrue(ev.details.hasRowCount());
            Assert.assertEquals(Long.MIN_VALUE, ev.details.getRowCount());
        });
    }

    @Test
    public void testResultEndWithoutFlagsIgnoresTrailers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            byte[] baseline = QwpEgressFrames.resultEnd(7, 2, 30).toFrame();
            byte[] withTrailers = QwpEgressFrames.resultEnd(7, 2, 30).varint(100).stats(1, 2, 3, 4, 1).toFrame();
            for (byte[] frame : new byte[][]{baseline, withTrailers}) {
                for (long flags : new long[]{0, ALL_FLAGS & ~(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_STATS)}) {
                    QueryEvent ev = decodeResultEnd(frame, flags);
                    Assert.assertEquals(QueryEvent.KIND_END, ev.kind);
                    Assert.assertEquals(30, ev.totalRows);
                    assertNoDetails(ev.details);
                }
            }
            // COUNT_ROWS alone reads the row count and leaves what follows alone
            QueryEvent ev = decodeResultEnd(withTrailers, QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS);
            Assert.assertEquals(100, ev.details.getRowCount());
            Assert.assertFalse(ev.details.hasStats());
        });
    }

    @Test
    public void testStatsFlagsReservedBitsIgnored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int[] statsFlags = {0x00, 0x01, 0x02, 0x80, 0x81, 0xFE, 0xFF};
            for (int flags : statsFlags) {
                byte[] frame = QwpEgressFrames.resultEnd(7, 0, 1).stats(1, 2, 3, 4, flags).toFrame();
                QueryEvent ev = decodeResultEnd(frame, QwpEgressMsgKind.QUERY_FLAG_STATS);
                Assert.assertTrue(ev.details.hasStats());
                Assert.assertEquals("stats_flags 0x" + Integer.toHexString(flags),
                        (flags & QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED) != 0, ev.details.isJitCompiled());
            }
        });
    }

    private static void appendField(QwpEgressFrames.Body body, List<String> fields, String field, FieldWriter writer) {
        int before = body.size();
        writer.write(body);
        for (int i = before; i < body.size(); i++) {
            fields.add(field);
        }
    }

    private static void appendStatsFields(QwpEgressFrames.Body body, List<String> fields) {
        appendField(body, fields, "compile_ns", b -> b.varint(128));
        appendField(body, fields, "execute_ns", b -> b.varint(Long.MAX_VALUE));
        appendField(body, fields, "count_ns", b -> b.varint(0));
        appendField(body, fields, "client_wait_ns", b -> b.varint(200));
        appendField(body, fields, "stats_flags", b -> b.u8(QwpEgressMsgKind.STATS_FLAG_JIT_COMPILED));
    }

    private static void assertDecodeFails(String expectedMessage, byte[] frame, long flags, boolean isExecDone) throws Exception {
        try {
            if (isExecDone) {
                decodeExecDone(frame, flags);
            } else {
                decodeResultEnd(frame, flags);
            }
            Assert.fail("decode must fail: " + expectedMessage);
        } catch (QwpDecodeException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }

    private static void assertNoDetails(QwpQueryDetails details) {
        Assert.assertFalse(details.hasRowCount());
        Assert.assertEquals(-1, details.getRowCount());
        Assert.assertFalse(details.hasStats());
        Assert.assertEquals(-1, details.getCompileNanos());
        Assert.assertEquals(-1, details.getExecuteNanos());
        Assert.assertEquals(-1, details.getCountNanos());
        Assert.assertEquals(-1, details.getClientWaitNanos());
        Assert.assertFalse(details.isJitCompiled());
        Assert.assertEquals("QwpQueryDetails{}", details.toString());
    }

    private static void assertStats(
            QwpQueryDetails details,
            long compileNanos,
            long executeNanos,
            long countNanos,
            long clientWaitNanos,
            boolean isJitCompiled
    ) {
        Assert.assertTrue(details.hasStats());
        Assert.assertEquals(compileNanos, details.getCompileNanos());
        Assert.assertEquals(executeNanos, details.getExecuteNanos());
        Assert.assertEquals(countNanos, details.getCountNanos());
        Assert.assertEquals(clientWaitNanos, details.getClientWaitNanos());
        Assert.assertEquals(isJitCompiled, details.isJitCompiled());
    }

    /**
     * Cuts {@code frame} after every byte from the end of the prelude to the
     * last but one: each cut lands in the field {@code fields} names for the
     * first missing byte, and the decode fails naming that field.
     */
    private static void assertTruncatedAtEveryByte(
            byte[] frame,
            List<String> fields,
            String frameName,
            long flags,
            boolean isExecDone
    ) throws Exception {
        Assert.assertEquals(PRELUDE + fields.size(), frame.length);
        for (int len = PRELUDE; len < frame.length; len++) {
            String field = fields.get(len - PRELUDE);
            boolean isByteField = "op_type".equals(field) || "stats_flags".equals(field);
            String expected = frameName + " frame truncated " + (isByteField ? "before " + field : "mid " + field + " varint");
            assertDecodeFails(expected, Arrays.copyOf(frame, len), flags, isExecDone);
        }
        // the whole frame decodes
        if (isExecDone) {
            decodeExecDone(frame, flags);
        } else {
            decodeResultEnd(frame, flags);
        }
    }

    private static QueryEvent decodeBaselineError(byte[] frame) {
        long buf = toNative(frame);
        try {
            return QwpEgressIoThread.decodeError(buf, frame.length);
        } finally {
            Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static QueryEvent decodeError(byte[] frame, long flags) {
        long buf = toNative(frame);
        try {
            return QwpEgressIoThread.decodeError(buf, frame.length, flags);
        } finally {
            Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static QueryEvent decodeExecDone(byte[] frame, long flags) throws QwpDecodeException {
        long buf = toNative(frame);
        try {
            return QwpEgressIoThread.decodeExecDone(buf, frame.length, flags);
        } finally {
            Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static QueryEvent decodeResultEnd(byte[] frame, long flags) throws QwpDecodeException {
        long buf = toNative(frame);
        try {
            return QwpEgressIoThread.decodeResultEnd(buf, frame.length, flags);
        } finally {
            Unsafe.free(buf, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Varints that do not fit in 64 bits: a continuation bit on the tenth
     * byte, whose data bit still fits, and data bits above bit 63 on the
     * tenth byte.
     */
    private static byte[][] overflowingVarints() {
        byte[] continued = new byte[11];
        Arrays.fill(continued, (byte) 0x80);
        continued[9] = (byte) 0x81;
        continued[10] = 0x00;
        byte[] highBits = new byte[10];
        Arrays.fill(highBits, (byte) 0x80);
        highBits[9] = 0x02;
        byte[] allHighBits = new byte[10];
        Arrays.fill(allHighBits, (byte) 0xFF);
        allHighBits[9] = 0x7F;
        return new byte[][]{continued, highBits, allHighBits};
    }

    /**
     * Appends a statistics block whose {@code field} carries {@code bad} and
     * whose other fields are well-formed.
     */
    private static QwpEgressFrames.Body statsWithBadField(QwpEgressFrames.Body body, String field, byte[] bad) {
        String[] names = {"compile_ns", "execute_ns", "count_ns", "client_wait_ns"};
        for (String name : names) {
            if (name.equals(field)) {
                body.bytes(bad);
            } else {
                body.varint(1);
            }
        }
        return body.u8(0);
    }

    private static long toNative(byte[] frame) {
        long buf = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < frame.length; i++) {
            Unsafe.getUnsafe().putByte(buf + i, frame[i]);
        }
        return buf;
    }

    @FunctionalInterface
    private interface FieldWriter {
        void write(QwpEgressFrames.Body body);
    }
}
