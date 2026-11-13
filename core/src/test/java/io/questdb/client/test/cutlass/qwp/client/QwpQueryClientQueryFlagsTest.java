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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wire coverage for the per-query {@code query_flags} trailer that
 * {@link QwpQueryClient#execute(CharSequence, QwpColumnBatchHandler, boolean)}
 * adds. The mock server captures the raw {@code QUERY_REQUEST} the client emits
 * and replies with an {@code EXEC_DONE} so {@code execute} returns; the test
 * then inspects the captured bytes. Pins:
 * <ul>
 *   <li>{@code resetSymbolDict=true} against a server advertising
 *       {@link QwpEgressMsgKind#CAP_QUERY_FLAGS} appends the
 *       {@link QwpEgressMsgKind#QUERY_FLAG_RESET_DICT} trailer;</li>
 *   <li>the trailer is the only difference from the flag-off baseline, which
 *       stays byte-identical;</li>
 *   <li>without the capability the flag is dropped (no trailer);</li>
 *   <li>the flag-less overloads never append a trailer.</li>
 * </ul>
 * The decode -&gt; dict-reset effect on the server, and the end-to-end honouring
 * of the flag, are covered against a live server in the questdb repo
 * (QwpEgressQueryFlagsResetTest, QwpEgressCacheResetWireTest).
 * <p>
 * The {@link QwpQueryOptions} tests pin the query-details bits and the
 * row-window values the server's {@code QwpEgressRequestDecoder} reads after
 * the flags: each option sets its own bit, {@code skip_rows} and
 * {@code max_rows} follow the flags in that order, a server without
 * {@link QwpEgressMsgKind#CAP_QUERY_DETAILS} gets none of them, and a row
 * window it would ignore fails the query before it is sent.
 */
public class QwpQueryClientQueryFlagsTest {
    private static final int ALL_CAPABILITIES = QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_DETAILS;
    private static final QwpColumnBatchHandler NOOP_HANDLER = new QwpColumnBatchHandler() {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    };
    private static final String SQL = "SELECT 1";

    @Test
    public void testDetailOptionsDroppedWithoutCapability() throws Exception {
        // the query runs without what the server cannot report
        QwpQueryOptions options = new QwpQueryOptions().errorPosition(true).stats(true).countRows(true).resetSymbolDict(true);
        QwpEgressFrames.QueryRequest r = executeOnce(QwpEgressMsgKind.CAP_QUERY_FLAGS, options);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, r.queryFlags);
        Assert.assertEquals(0, r.trailingBytes);

        // a server with per-query timeouts, but without the query details
        r = executeOnce(QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT, options);
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, r.queryFlags);
        Assert.assertEquals(0, r.trailingBytes);

        r = executeOnce(0, options);
        Assert.assertFalse("a server without CAP_QUERY_FLAGS gets no trailer", r.hasQueryFlags);
        Assert.assertEquals(0, r.trailingBytes);
    }

    @Test
    public void testEachBooleanOptionSendsItsFlag() throws Exception {
        Object[][] cases = {
                {new QwpQueryOptions().errorPosition(true), QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION},
                {new QwpQueryOptions().stats(true), QwpEgressMsgKind.QUERY_FLAG_STATS},
                {new QwpQueryOptions().countRows(true), QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS},
                {new QwpQueryOptions().resetSymbolDict(true), QwpEgressMsgKind.QUERY_FLAG_RESET_DICT},
        };
        for (Object[] c : cases) {
            QwpEgressFrames.QueryRequest r = executeOnce(ALL_CAPABILITIES, (QwpQueryOptions) c[0]);
            Assert.assertEquals(c[0].toString(), (long) (int) c[1], r.queryFlags);
            Assert.assertEquals(-1, r.skipRows);
            Assert.assertEquals(-1, r.maxRows);
            Assert.assertEquals(0, r.trailingBytes);
        }
    }

    @Test
    public void testEveryOptionOnTheWire() throws Exception {
        QwpQueryOptions options = new QwpQueryOptions()
                .resetSymbolDict(true).errorPosition(true).stats(true).countRows(true)
                .skipRows(300).maxRows(16_384);
        QwpEgressFrames.QueryRequest r = executeOnce(ALL_CAPABILITIES, options);
        // every bit but 0x02, which per-query timeouts own
        Assert.assertEquals(0x7D, r.queryFlags);
        Assert.assertEquals(300, r.skipRows);
        Assert.assertEquals(16_384, r.maxRows);
        Assert.assertEquals(0, r.trailingBytes);
        // flags, skip_rows, max_rows: the last bytes of the request, in that order
        byte[] tail = new QwpEgressFrames.Body().varint(0x7D).varint(300).varint(16_384).toBody();
        Assert.assertArrayEquals(tail, Arrays.copyOfRange(r.payload, r.payload.length - tail.length, r.payload.length));
    }

    @Test
    public void testFlagDroppedWhenServerLacksCapability() throws Exception {
        byte[] frame = runAndCapture(0, c -> c.execute("SELECT 1", NOOP_HANDLER, true));
        Assert.assertEquals("flag must be dropped when the server does not advertise CAP_QUERY_FLAGS",
                -1L, queryFlagsTrailer(frame));
    }

    @Test
    public void testLegacyThreeArgOverloadSendsNoTrailer() throws Exception {
        byte[] frame = runAndCapture(QwpEgressMsgKind.CAP_QUERY_FLAGS, c -> c.execute("SELECT 1", null, NOOP_HANDLER));
        Assert.assertEquals(-1L, queryFlagsTrailer(frame));
    }

    @Test
    public void testLegacyTwoArgOverloadSendsNoTrailer() throws Exception {
        byte[] frame = runAndCapture(QwpEgressMsgKind.CAP_QUERY_FLAGS, c -> c.execute("SELECT 1", NOOP_HANDLER));
        Assert.assertEquals(-1L, queryFlagsTrailer(frame));
    }

    @Test
    public void testMaxRowsOnTheWire() throws Exception {
        // zero streams only the schema, so it goes on the wire like any bound
        for (long maxRows : new long[]{0, 1, 127, 128, 16_383, 16_384, Long.MAX_VALUE - 1}) {
            QwpEgressFrames.QueryRequest r = executeOnce(ALL_CAPABILITIES, new QwpQueryOptions().maxRows(maxRows));
            Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, r.queryFlags);
            Assert.assertEquals(-1, r.skipRows);
            Assert.assertEquals(maxRows, r.maxRows);
            Assert.assertEquals(0, r.trailingBytes);
        }
        // the unbounded default sends nothing
        QwpEgressFrames.QueryRequest r = executeOnce(ALL_CAPABILITIES, new QwpQueryOptions().maxRows(5).maxRows(Long.MAX_VALUE));
        Assert.assertFalse(r.hasQueryFlags);
        Assert.assertEquals(0, r.trailingBytes);
    }

    @Test
    public void testOptionsFollowBinds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryFlagsTest::execDone);
                 QwpQueryClient client = newClient(server)) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute("SELECT x FROM t WHERE x > $1 AND x < $2", binds -> binds.setLong(0, -7).setLong(1, Long.MAX_VALUE),
                        handler, new QwpQueryOptions().countRows(true).skipRows(2).maxRows(3));
                Assert.assertEquals(1, handler.execDoneCount);
                server.assertNoFailure();
                QwpEgressFrames.QueryRequest r = server.getRequests().get(0);
                Assert.assertArrayEquals(new long[]{-7, Long.MAX_VALUE}, r.longBinds);
                Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS | QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS
                        | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, r.queryFlags);
                Assert.assertEquals(2, r.skipRows);
                Assert.assertEquals(3, r.maxRows);
                Assert.assertEquals(0, r.trailingBytes);
            }
        });
    }

    @Test
    public void testOptionsWithNothingSetAreTheBaseline() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(ALL_CAPABILITIES, QwpQueryClientQueryFlagsTest::execDone);
                 QwpQueryClient client = newClient(server)) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler);
                client.execute(SQL, handler, new QwpQueryOptions());
                client.execute(SQL, handler, (QwpQueryOptions) null);
                client.execute(SQL, null, handler, new QwpQueryOptions().skipRows(5).maxRows(5).clear());
                Assert.assertEquals(4, handler.execDoneCount);
                server.assertNoFailure();
                List<QwpEgressFrames.QueryRequest> requests = server.getRequests();
                Assert.assertEquals(4, requests.size());
                byte[] baseline = requests.get(0).payload;
                zeroRequestId(baseline);
                for (int i = 1; i < requests.size(); i++) {
                    byte[] payload = requests.get(i).payload;
                    zeroRequestId(payload);
                    Assert.assertArrayEquals("request " + i, baseline, payload);
                }
            }
        });
    }

    @Test
    public void testResetDictDroppedWithoutQueryFlagsCapability() throws Exception {
        QwpEgressFrames.QueryRequest r = executeOnce(QwpEgressMsgKind.CAP_QUERY_DETAILS,
                new QwpQueryOptions().resetSymbolDict(true).stats(true).maxRows(9));
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_STATS | QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, r.queryFlags);
        Assert.assertEquals(9, r.maxRows);
        Assert.assertEquals(0, r.trailingBytes);
    }

    @Test
    public void testResetFlagAppendsTrailerWhenCapabilityAdvertised() throws Exception {
        byte[] frame = runAndCapture(QwpEgressMsgKind.CAP_QUERY_FLAGS, c -> c.execute("SELECT 1", NOOP_HANDLER, true));
        Assert.assertEquals((long) QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, queryFlagsTrailer(frame));
    }

    @Test
    public void testResetTrailerIsTheOnlyDifferenceFromBaseline() throws Exception {
        byte[] off = runAndCapture(QwpEgressMsgKind.CAP_QUERY_FLAGS, c -> c.execute("SELECT 1", NOOP_HANDLER, false));
        byte[] on = runAndCapture(QwpEgressMsgKind.CAP_QUERY_FLAGS, c -> c.execute("SELECT 1", NOOP_HANDLER, true));
        // request_id increments per execute; both run on a fresh client so they
        // already match, but zero it to keep the comparison about the trailer.
        zeroRequestId(off);
        zeroRequestId(on);
        Assert.assertEquals("reset frame must be the baseline plus a one-byte trailer", off.length + 1, on.length);
        Assert.assertArrayEquals("baseline bytes must be untouched", off, Arrays.copyOf(on, off.length));
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_RESET_DICT, on[on.length - 1]);
    }

    @Test
    public void testRowWindowFailsWithoutCapability() throws Exception {
        // A server without CAP_QUERY_DETAILS would stream every row. The query
        // fails before it is sent, and the connection stays usable.
        QwpQueryOptions[] windows = {
                new QwpQueryOptions().skipRows(1),
                new QwpQueryOptions().maxRows(0),
                new QwpQueryOptions().skipRows(10).maxRows(10).stats(true),
        };
        final int[] capabilitiesWithoutDetails = {
                0,
                QwpEgressMsgKind.CAP_QUERY_FLAGS,
                QwpEgressMsgKind.CAP_QUERY_FLAGS | QwpEgressMsgKind.CAP_QUERY_TIMEOUT
        };
        for (int capabilities : capabilitiesWithoutDetails) {
            TestUtils.assertMemoryLeak(() -> {
                try (QwpEgressMockServer server = new QwpEgressMockServer(capabilities, QwpQueryClientQueryFlagsTest::execDone);
                     QwpQueryClient client = newClient(server)) {
                    client.connect();
                    for (QwpQueryOptions window : windows) {
                        RecordingQueryHandler handler = new RecordingQueryHandler();
                        client.execute(SQL, binds -> Assert.fail("binds of a refused query must not be encoded"), handler, window);
                        handler.assertOnlyError();
                        Assert.assertEquals(-1, handler.errorRequestId);
                        Assert.assertEquals(WebSocketResponse.STATUS_INTERNAL_ERROR, handler.errorStatus);
                        Assert.assertEquals("server does not support skipRows and maxRows [capabilities=0x"
                                        + Integer.toHexString(capabilities) + "]; they need a server that advertises CAP_QUERY_DETAILS",
                                handler.errorMessage);
                        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, handler.errorPosition);
                    }
                    Assert.assertEquals("a refused query must not reach the server", 0, server.getRequests().size());

                    RecordingQueryHandler handler = new RecordingQueryHandler();
                    client.execute(SQL, handler, new QwpQueryOptions().stats(true));
                    Assert.assertEquals(1, handler.execDoneCount);
                    Assert.assertEquals(1, server.getRequests().size());
                    server.assertNoFailure();
                }
            });
        }
    }

    @Test
    public void testSkipRowsOnTheWire() throws Exception {
        for (long skipRows : new long[]{1, 127, 128, 16_383, 16_384, Long.MAX_VALUE}) {
            QwpEgressFrames.QueryRequest r = executeOnce(ALL_CAPABILITIES, new QwpQueryOptions().skipRows(skipRows));
            Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS, r.queryFlags);
            Assert.assertEquals(skipRows, r.skipRows);
            Assert.assertEquals(-1, r.maxRows);
            Assert.assertEquals(0, r.trailingBytes);
        }
        // skipping no rows sends nothing
        QwpEgressFrames.QueryRequest r = executeOnce(ALL_CAPABILITIES, new QwpQueryOptions().skipRows(5).skipRows(0).maxRows(10));
        Assert.assertEquals(QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS, r.queryFlags);
        Assert.assertEquals(-1, r.skipRows);
        Assert.assertEquals(10, r.maxRows);
    }

    /**
     * Each capability and each query flag is a bit of its own, and the query
     * details leave alone the per-query timeout bits that released clients
     * already use: capability 0x08 and flag 0x02, followed by {@code timeout_ms}
     * (c-questdb-client 7.2.0 and py-questdb-client 6.0.0).
     */
    @Test
    public void testWireBitsAreDistinctAndLeaveTimeoutBitsFree() {
        Assert.assertEquals(0x08, QwpEgressMsgKind.CAP_QUERY_TIMEOUT);
        Assert.assertEquals(0x02, QwpEgressMsgKind.QUERY_FLAG_TIMEOUT);
        assertDistinctBits(
                QwpEgressMsgKind.CAP_ZONE,
                QwpEgressMsgKind.CAP_QUERY_FLAGS,
                0x04, // the server's CAP_COMPRESSION, which this client does not read
                QwpEgressMsgKind.CAP_QUERY_TIMEOUT,
                QwpEgressMsgKind.CAP_QUERY_DETAILS
        );
        assertDistinctBits(
                QwpEgressMsgKind.QUERY_FLAG_RESET_DICT,
                QwpEgressMsgKind.QUERY_FLAG_TIMEOUT,
                QwpEgressMsgKind.QUERY_FLAG_ERROR_POSITION,
                QwpEgressMsgKind.QUERY_FLAG_STATS,
                QwpEgressMsgKind.QUERY_FLAG_COUNT_ROWS,
                QwpEgressMsgKind.QUERY_FLAG_SKIP_ROWS,
                QwpEgressMsgKind.QUERY_FLAG_MAX_ROWS
        );
    }

    private static void assertDistinctBits(int... bits) {
        int union = 0;
        for (int bit : bits) {
            Assert.assertEquals("0x" + Integer.toHexString(bit) + " must be a single bit", 1, Integer.bitCount(bit));
            Assert.assertEquals("0x" + Integer.toHexString(bit) + " is taken twice", 0, union & bit);
            union |= bit;
        }
    }

    private static byte[] buildExecDone(byte[] queryRequest) {
        int bodyLen = 1 + 8 + 1 + 1; // msg_kind + request_id + op_type + rows_affected varint
        byte[] frame = new byte[QwpConstants.HEADER_SIZE + bodyLen];
        ByteBuffer bb = ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 'Q').put((byte) 'W').put((byte) 'P').put((byte) '1');
        bb.put((byte) 1);       // version
        bb.put((byte) 0);       // flags
        bb.putShort((short) 0); // table_count
        bb.putInt(bodyLen);     // payload_length
        bb.put(QwpEgressMsgKind.EXEC_DONE);
        bb.put(queryRequest, 1, 8); // echo request_id verbatim
        bb.put((byte) 0);       // op_type
        bb.put((byte) 0);       // rows_affected = 0
        return frame;
    }

    /**
     * Answers a request with an {@code EXEC_DONE}, carrying the statistics
     * block when the request asked for it, as the server does.
     */
    private static byte[][] execDone(QwpEgressFrames.QueryRequest r) {
        QwpEgressFrames.Body body = QwpEgressFrames.execDone(r.requestId, 0, 0);
        if (r.has(QwpEgressMsgKind.QUERY_FLAG_STATS)) {
            body.stats(0, 0, 0, 0, 0);
        }
        return new byte[][]{body.toFrame()};
    }

    /**
     * Runs {@link #SQL} once with {@code options} against a server that
     * advertises {@code capabilities}, and returns the request it received.
     */
    private static QwpEgressFrames.QueryRequest executeOnce(int capabilities, QwpQueryOptions options) throws Exception {
        AtomicReference<QwpEgressFrames.QueryRequest> request = new AtomicReference<>();
        TestUtils.assertMemoryLeak(() -> {
            try (QwpEgressMockServer server = new QwpEgressMockServer(capabilities, QwpQueryClientQueryFlagsTest::execDone);
                 QwpQueryClient client = newClient(server)) {
                client.connect();
                RecordingQueryHandler handler = new RecordingQueryHandler();
                client.execute(SQL, handler, options);
                Assert.assertEquals("the query must complete", 1, handler.execDoneCount);
                server.assertNoFailure();
                Assert.assertEquals(1, server.getRequests().size());
                request.set(server.getRequests().get(0));
            }
        });
        return request.get();
    }

    private static QwpQueryClient newClient(QwpEgressMockServer server) {
        return QwpQueryClient.fromConfig("ws::addr=" + server.getAddress() + ";auth_timeout_ms=2000;failover=off;");
    }

    /**
     * Parses the {@code QUERY_REQUEST} the client emitted and returns the
     * decoded {@code query_flags} trailer, or {@code -1} when no trailer was
     * appended. Assumes no binds (the tests use none).
     */
    private static long queryFlagsTrailer(byte[] f) {
        Assert.assertTrue("captured frame is too short", f.length > 1 + 8);
        Assert.assertEquals("captured frame must be a QUERY_REQUEST", QwpEgressMsgKind.QUERY_REQUEST, f[0]);
        int[] p = {1 + 8}; // skip msg_kind + request_id
        long sqlLen = readVarint(f, p);
        p[0] += (int) sqlLen;
        readVarint(f, p);         // initial_credit
        long bindCount = readVarint(f, p);
        Assert.assertEquals("test frames carry no binds", 0L, bindCount);
        if (p[0] >= f.length) {
            return -1L;
        }
        return readVarint(f, p);
    }

    private static long readVarint(byte[] buf, int[] pos) {
        long value = 0;
        int shift = 0;
        while (true) {
            byte b = buf[pos[0]++];
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
        }
    }

    private static byte[] runAndCapture(int serverCapabilities, ExecuteAction action) throws Exception {
        CapturingQueryServer handler = new CapturingQueryServer();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.setSendServerInfo(true);
            server.setCapabilities(serverCapabilities);
            int port = server.getPort();
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QwpQueryClient client = QwpQueryClient.fromConfig(
                    "ws::addr=localhost:" + port + ";auth_timeout_ms=2000;")) {
                client.connect();
                Assert.assertTrue("client must connect", client.isConnected());
                action.run(client);
            }
        }
        byte[] frame = handler.captured.get();
        Assert.assertNotNull("server never received a QUERY_REQUEST", frame);
        return frame;
    }

    private static void zeroRequestId(byte[] frame) {
        Arrays.fill(frame, 1, 1 + 8, (byte) 0);
    }

    @FunctionalInterface
    private interface ExecuteAction {
        void run(QwpQueryClient client);
    }

    private static final class CapturingQueryServer implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicReference<byte[]> captured = new AtomicReference<>();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (data.length == 0 || data[0] != QwpEgressMsgKind.QUERY_REQUEST) {
                return;
            }
            captured.compareAndSet(null, data);
            try {
                client.sendBinary(buildExecDone(data));
            } catch (IOException e) {
                // best-effort: a failed reply surfaces to the client as a transport error
            }
        }
    }
}
