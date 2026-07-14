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
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
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
 */
public class QwpQueryClientQueryFlagsTest {

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
