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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

/**
 * Regression for a QWP store-and-forward exactly-once gap: on a failover
 * reconnect, {@link CursorWebSocketSendLoop#swapClient} closes the old
 * connection and repositions the replay cursor at {@code ackedFsn + 1} without
 * first draining the old connection's receive buffer, so a final
 * {@code STATUS_DURABLE_ACK} that arrived on the old wire but was not yet
 * processed is discarded. {@code ackedFsn} then stays stale and the loop
 * replays frames the server already confirmed durable -- duplicates on the
 * failover target (which already holds them) when the table has no dedup keys.
 *
 * <p>Deterministic, thread-free: the loop is built but never started (frames
 * delivered directly into the response handler, à la
 * {@link CursorWebSocketSendLoopDurableAckTest}); the <em>old</em>
 * {@link WebSocketClient} carries one covering durable ack followed by a CLOSE
 * -- the exact shape a demoting primary leaves buffered. The test asserts the
 * ack is drained and applied (so the cursor anchors past those frames) and that
 * the trailing CLOSE does not re-enter the failover path (the reconnect factory
 * throws, so any re-entry would surface as an error).
 *
 * <p>Before the fix this fails with {@code ackedFsn == -1}: the ack is dropped
 * and the loop rewinds to FSN 0.
 */
public class CursorWebSocketSendLoopReconnectDurableAckDrainTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-reconnect-da-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testReconnectDrainsBufferedDurableAckBeforeRepositioning() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long ackPacked = buildDurableAckPayload(names("trades"), txns(12L));
            long ackPtr = ackPacked & 0xFFFFFFFFFFFFL;
            int ackSize = (int) (ackPacked >>> 48);
            // Old wire: the covering durable ack (trades<=12), then a CLOSE --
            // the shape a demoting primary leaves buffered once it has flushed
            // its final ack and shut the write side.
            BufferedAckClient oldClient = new BufferedAckClient(ackPtr, ackSize, true);
            BufferedAckClient newClient = new BufferedAckClient(0L, 0, false); // inert new wire
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 3);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, oldClient);
                setSentCount(loop, 3);

                // Three frames OK'd but, in durable mode, still awaiting the
                // durable ack -- nothing trimmed yet.
                deliverOk(loop, 0, names("trades"), txns(10L));
                deliverOk(loop, 1, names("trades"), txns(11L));
                deliverOk(loop, 2, names("trades"), txns(12L));
                assertEquals("precondition: no frame durably acked before reconnect",
                        -1L, engine.ackedFsn());
                assertEquals("precondition: ack + CLOSE still buffered on the old wire",
                        2, oldClient.pending());

                // Fail over to the promoted node.
                swapClient(loop, newClient);

                // The buffered durable ack must be drained and applied before the
                // replay cursor is repositioned; otherwise the loop rewinds to
                // FSN 0 and replays frames the server already confirmed durable.
                assertEquals("buffered durable ack on the old connection must be drained "
                                + "and applied on reconnect (else already-durable frames replay -> duplicates)",
                        2L, engine.ackedFsn());
                assertEquals("replay cursor must resume past the durably-acked frames, not rewind over them",
                        3L, getLongField(loop, "fsnAtZero"));
                // Whole buffer consumed, including the trailing CLOSE -- which must
                // be a no-op during the swap, not a re-entry into failover (the
                // reconnect factory throws, so re-entry would have blown up above).
                assertEquals("the old connection's recv buffer must be fully drained",
                        0, oldClient.pending());
            } finally {
                oldClient.close(); // idempotent; swapClient already closed it
                newClient.close();
                Unsafe.free(ackPtr, ackSize, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void appendFrames(CursorSendEngine engine, int count) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < payload.length; i++) {
                Unsafe.getUnsafe().putByte(buf + i, payload[i]);
            }
            for (int i = 0; i < count; i++) {
                engine.appendBlocking(buf, 16);
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long buildDurableAckPayload(String[] tableNames, long[] seqTxns) {
        // STATUS_DURABLE_ACK frame: status(1) + tableCount(2) + entries(nameLen(2)+name+seqTxn(8))
        int size = 3;
        for (String t : tableNames) size += 2 + t.getBytes(StandardCharsets.UTF_8).length + 8;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        int offset = 0;
        Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_DURABLE_ACK);
        offset += 1;
        Unsafe.getUnsafe().putShort(ptr + offset, (short) tableNames.length);
        offset += 2;
        for (int i = 0; i < tableNames.length; i++) {
            byte[] name = tableNames[i].getBytes(StandardCharsets.UTF_8);
            Unsafe.getUnsafe().putShort(ptr + offset, (short) name.length);
            offset += 2;
            for (int j = 0; j < name.length; j++) {
                Unsafe.getUnsafe().putByte(ptr + offset + j, name[j]);
            }
            offset += name.length;
            Unsafe.getUnsafe().putLong(ptr + offset, seqTxns[i]);
            offset += 8;
        }
        return ptr | (((long) size) << 48);
    }

    private static long buildOkPayload(long wireSeq, String[] tableNames, long[] seqTxns) {
        // STATUS_OK frame: status(1) + sequence(8) + tableCount(2) + entries
        int size = 11;
        for (String t : tableNames) size += 2 + t.getBytes(StandardCharsets.UTF_8).length + 8;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        int offset = 0;
        Unsafe.getUnsafe().putByte(ptr + offset, WebSocketResponse.STATUS_OK);
        offset += 1;
        Unsafe.getUnsafe().putLong(ptr + offset, wireSeq);
        offset += 8;
        Unsafe.getUnsafe().putShort(ptr + offset, (short) tableNames.length);
        offset += 2;
        for (int i = 0; i < tableNames.length; i++) {
            byte[] name = tableNames[i].getBytes(StandardCharsets.UTF_8);
            Unsafe.getUnsafe().putShort(ptr + offset, (short) name.length);
            offset += 2;
            for (int j = 0; j < name.length; j++) {
                Unsafe.getUnsafe().putByte(ptr + offset + j, name[j]);
            }
            offset += name.length;
            Unsafe.getUnsafe().putLong(ptr + offset, seqTxns[i]);
            offset += 8;
        }
        return ptr | (((long) size) << 48);
    }

    private static void deliverOk(CursorWebSocketSendLoop loop, long wireSeq, String[] tableNames, long[] seqTxns) throws Exception {
        long packed = buildOkPayload(wireSeq, tableNames, seqTxns);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long getLongField(CursorWebSocketSendLoop loop, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(loop);
    }

    private static void invokeOnBinaryMessage(CursorWebSocketSendLoop loop, long ptr, int size) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("responseHandler");
        f.setAccessible(true);
        Object handler = f.get(loop);
        Method m = handler.getClass().getDeclaredMethod("onBinaryMessage", long.class, int.class);
        m.setAccessible(true);
        m.invoke(handler, ptr, size);
    }

    private static String[] names(String... v) {
        return v;
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    private CursorWebSocketSendLoop newDurableLoop(CursorSendEngine engine, WebSocketClient client) {
        return new CursorWebSocketSendLoop(
                client, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new UnsupportedOperationException("reconnect factory unused: swapClient is driven directly");
                },
                5_000L, 100L, 5_000L, true);
    }

    private static void setSentCount(CursorWebSocketSendLoop loop, long count) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("nextWireSeq");
        f.setAccessible(true);
        f.setLong(loop, count);
    }

    private static void swapClient(CursorWebSocketSendLoop loop, WebSocketClient newClient) throws Exception {
        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod("swapClient", WebSocketClient.class);
        m.setAccessible(true);
        m.invoke(loop, newClient);
    }

    private static long[] txns(long... v) {
        return v;
    }

    /**
     * In-memory transport that optionally carries a single buffered durable-ack
     * frame followed by a CLOSE -- the shape left on the wire by a primary that
     * flushed its final ack and then shut down. {@code tryReceiveFrame} delivers
     * the ack, then the CLOSE, then nothing. Everything else is inert.
     */
    private static final class BufferedAckClient extends WebSocketClient {
        private final long framePtr;
        private final int frameSize;
        private boolean ackPending;
        private boolean closePending;

        BufferedAckClient(long framePtr, int frameSize, boolean deliverClose) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.framePtr = framePtr;
            this.frameSize = frameSize;
            this.ackPending = frameSize > 0;
            this.closePending = deliverClose;
        }

        int pending() {
            return (ackPending ? 1 : 0) + (closePending ? 1 : 0);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            // replayed frames are irrelevant to this test
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (ackPending) {
                ackPending = false;
                handler.onBinaryMessage(framePtr, frameSize);
                return true;
            }
            if (closePending) {
                closePending = false;
                handler.onClose(1000, "role-change handoff"); // 1000 = NORMAL_CLOSURE
                return true;
            }
            return false;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
