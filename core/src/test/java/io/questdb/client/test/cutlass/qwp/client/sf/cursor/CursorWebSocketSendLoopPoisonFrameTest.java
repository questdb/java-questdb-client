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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
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
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Poison-frame detector and NACK-recycle pacing under durable-ack mode.
 * <p>
 * The detector's contract (design/qwp-nack-policy-v2.md): a frame the server
 * deterministically rejects — {@code maxHeadFrameRejections} consecutive
 * server-active rejections of the same frame with no acceptance progress in
 * between — escalates to a typed {@code PROTOCOL_VIOLATION} terminal instead
 * of recycling the connection forever. "Acceptance progress" must be measured
 * at the OK level (highest {@code STATUS_OK}-acknowledged FSN), NOT at the
 * engine's trim watermark: in durable-ack mode {@code ackedFsn} advances only
 * on {@code STATUS_DURABLE_ACK} coverage, so every post-NACK recycle replays
 * from the durable watermark and re-OKs frames BEHIND the poisoned one.
 * Keying/resetting strikes on the trim watermark lets those re-OKs launder the
 * strike count each cycle — a deterministically-NACKing frame then recycles
 * the connection indefinitely (each cycle a full connect + window replay).
 * <p>
 * Related pacing promise from the same doc: RETRIABLE recycles go through
 * "capped backoff + jitter". A NACK against a healthy, reachable server must
 * therefore not spin the recycle loop at server NACK rate.
 */
public class CursorWebSocketSendLoopPoisonFrameTest {

    private static final int MAX_REJECTIONS = 3;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-poison-" + System.nanoTime()).toString();
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
    public void testDurableModeDetectorFiresDespiteReplayReOks() throws Exception {
        // Durable-ack mode, two frames in the SF log. Frame 0 is OK'd but its
        // durable ack never arrives (upload lag), so the trim watermark stays
        // at -1. Frame 1 is deterministically NACK'd (WRITE_ERROR, RETRIABLE).
        // Each recycle replays from the durable watermark: the server re-OKs
        // frame 0, then re-NACKs frame 1. After MAX_REJECTIONS such cycles the
        // detector MUST escalate frame 1 to a PROTOCOL_VIOLATION terminal --
        // the rejection is deterministic and replay cannot succeed. The re-OKs
        // of frame 0 are progress BEHIND the poisoned frame and must not
        // reset the strike count.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);

                for (int cycle = 1; cycle <= MAX_REJECTIONS; cycle++) {
                    // One connection generation: both frames (re)sent...
                    setSentCount(loop, 2);
                    // ...server re-OKs the already-applied frame 0 (fsn 0)...
                    deliverOk(loop, 0, names("trades"), txns(7L));
                    // Durable trim never advances: the OK is queued awaiting
                    // its STATUS_DURABLE_ACK, which never arrives.
                    assertEquals(-1L, engine.ackedFsn());
                    // ...then the server deterministically NACKs frame 1
                    // (fsn 1). The RETRIABLE policy recycles the wire inline
                    // through the real connectLoop/swapClient machinery:
                    // durable-ack tracking is dropped, wire sequencing
                    // restarts, replay resumes from ackedFsn+1 = fsn 0.
                    deliverRetriableNack(loop, 1, "disk full");
                }

                try {
                    loop.checkError();
                    fail("after " + MAX_REJECTIONS + " consecutive rejections of the same frame "
                            + "with no OK-level progress at or beyond it, the poison-frame "
                            + "detector must latch a PROTOCOL_VIOLATION terminal -- otherwise a "
                            + "deterministically-NACKing frame recycles the connection forever");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION,
                            e.getServerError().getCategory());
                    assertEquals(SenderError.Policy.TERMINAL,
                            e.getServerError().getAppliedPolicy());
                }
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testPoisonTerminalNamesTheRejectedFsn() throws Exception {
        // The escalated terminal must name the frame the server rejected, not
        // "durable trim watermark + 1". Frame 0 is OK'd (awaiting its durable
        // ack, so ackedFsn stays -1); frame 1 draws MAX_REJECTIONS consecutive
        // NACKs. The poisoned frame is fsn 1 -- reporting fsn 0 points the
        // operator at bytes the server ACCEPTED.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                setSentCount(loop, 2);

                deliverOk(loop, 0, names("trades"), txns(7L));
                for (int i = 0; i < MAX_REJECTIONS; i++) {
                    // Each RETRIABLE NACK recycles the wire (real swapClient,
                    // wire seq reset) -- restore the "both frames replayed"
                    // state before the next rejection so the NACK is
                    // attributable to a sent frame.
                    setSentCount(loop, 2);
                    deliverRetriableNack(loop, 1, "disk full");
                }

                try {
                    loop.checkError();
                    fail("poison-frame detector must have escalated by now");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION,
                            e.getServerError().getCategory());
                    assertEquals("the poisoned-frame terminal must name the FSN the server "
                                    + "rejected (fsn 1), not the durable trim watermark + 1 "
                                    + "(fsn 0 -- a frame the server accepted)",
                            1L, e.getServerError().getFromFsn());
                    // A NACK names the exact frame, so the poison span is that
                    // single frame -- not [fsn, publishedFsn], which would
                    // sweep in frames that are merely head-of-line blocked.
                    assertEquals(1L, e.getServerError().getToFsn());
                }
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNonOrderlyClosePoisonKeysOnOkLevelHeadOfLine() throws Exception {
        // The close-strike variant of the FSN-attribution rule: a non-orderly
        // close cannot name a frame, so the detector charges the OK-level
        // head-of-line frame -- the first frame the server has NOT accepted
        // at the OK level. Frame 0 is OK'd (awaiting its durable ack, trim
        // still -1); the connection then dies non-orderly after each replay.
        // The poisoned suspect is fsn 1, not "durable trim + 1" = fsn 0.
        TestUtils.assertMemoryLeak(() -> {
            List<WebSocketClient> clients = new ArrayList<>();
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine, clients);
                setSentCount(loop, 2);

                deliverOk(loop, 0, names("trades"), txns(7L));
                for (int i = 0; i < MAX_REJECTIONS; i++) {
                    // Non-orderly close after at least one send on this
                    // connection counts a strike; each close recycles the
                    // wire (real swapClient), so restore the sent count
                    // before the next close event.
                    setSentCount(loop, 2);
                    deliverNonOrderlyClose(loop);
                }

                try {
                    loop.checkError();
                    fail("poison-frame detector must have escalated by now");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION,
                            e.getServerError().getCategory());
                    assertEquals("a close-attributed poison must charge the OK-level "
                                    + "head-of-line frame (fsn 1), not the durable trim "
                                    + "watermark + 1 (fsn 0 -- accepted at the OK level)",
                            1L, e.getServerError().getFromFsn());
                }
            } finally {
                closeAll(clients);
            }
        });
    }

    @Test
    public void testNackRecycleIsPacedAgainstHealthyServer() throws Exception {
        // A reachable, healthy server that NACKs the head frame (RETRIABLE)
        // must not drive the recycle loop at server NACK rate: connectLoop's
        // backoff only engages after a FAILED connect attempt, and every
        // connect here succeeds instantly. design/qwp-nack-policy-v2.md
        // promises capped backoff + jitter for RETRIABLE recycles, so
        // NACK-initiated recycles must be paced by at least the initial
        // backoff. Red behavior: hundreds of full recycles per second.
        final long initialBackoffMillis = 200L;
        final long runMillis = 1_200L;
        // Generous ceiling: with >=200ms pacing, a 1.2s window fits at most
        // 6 recycles; allow 10 for scheduling noise. The unpaced bug
        // overshoots this by orders of magnitude, so no flakiness risk.
        final int maxReconnects = 10;

        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                final List<Long> reconnectNanos = new ArrayList<>();
                CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                    synchronized (reconnectNanos) {
                        reconnectNanos.add(System.nanoTime());
                    }
                    return new NackingClient();
                };
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        new NackingClient(), engine, 0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        factory,
                        5_000L, initialBackoffMillis, 1_000L,
                        false,
                        CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                        // Keep the detector out of the way: this test measures
                        // pacing, not escalation.
                        1_000_000);
                try {
                    loop.start();
                    Thread.sleep(runMillis);
                } finally {
                    loop.close();
                }
                int count;
                synchronized (reconnectNanos) {
                    count = reconnectNanos.size();
                }
                assertTrue("NACK-triggered recycles against a healthy server must be paced "
                                + "by the initial reconnect backoff (" + initialBackoffMillis
                                + "ms): observed " + count + " reconnects in " + runMillis
                                + "ms -- the recycle loop is running at server NACK rate "
                                + "with zero pacing",
                        count <= maxReconnects);
                assertTrue("sanity: the NACK must actually recycle the connection at least once",
                        count >= 1);
            }
        });
    }

    // ---------------------------------------------------------------------
    // harness
    // ---------------------------------------------------------------------

    /**
     * In-memory transport emulating a healthy server that deterministically
     * NACKs the head frame: accepts the connection, waits for one send on
     * this connection, then delivers exactly one WRITE_ERROR (RETRIABLE)
     * rejection for wireSeq 0.
     */
    private static final class NackingClient extends WebSocketClient {
        private boolean nackDelivered;
        private volatile int sentCount;

        NackingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sentCount++;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (nackDelivered || sentCount == 0) {
                return false;
            }
            nackDelivered = true;
            long packed = buildErrorPayload(0L, WebSocketResponse.STATUS_WRITE_ERROR, "disk full");
            long ptr = packed & 0xFFFFFFFFFFFFL;
            int size = (int) (packed >>> 48);
            try {
                handler.onBinaryMessage(ptr, size);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
            return true;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
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

    private static long buildErrorPayload(long wireSeq, byte status, String message) {
        // Error frame: status(1) + sequence(8) + msgLen(2) + bytes
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        int size = 11 + msg.length;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putByte(ptr, status);
        Unsafe.getUnsafe().putLong(ptr + 1, wireSeq);
        Unsafe.getUnsafe().putShort(ptr + 9, (short) msg.length);
        for (int i = 0; i < msg.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + 11 + i, msg[i]);
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

    private static void deliverOk(CursorWebSocketSendLoop loop, long wireSeq,
                                  String[] tableNames, long[] seqTxns) throws Exception {
        long packed = buildOkPayload(wireSeq, tableNames, seqTxns);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void deliverNonOrderlyClose(CursorWebSocketSendLoop loop) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("responseHandler");
        f.setAccessible(true);
        Object handler = f.get(loop);
        Method m = handler.getClass().getDeclaredMethod("onClose", int.class, String.class);
        m.setAccessible(true);
        m.invoke(handler, 1006, "connection reset"); // ABNORMAL_CLOSURE
    }

    private static void deliverRetriableNack(CursorWebSocketSendLoop loop, long wireSeq,
                                             String msg) throws Exception {
        long packed = buildErrorPayload(wireSeq, WebSocketResponse.STATUS_WRITE_ERROR, msg);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void closeAll(List<WebSocketClient> clients) {
        // swapClient already closed all but the most recently installed
        // client; close() is idempotent, so sweep them all.
        for (WebSocketClient c : clients) {
            try {
                c.close();
            } catch (Throwable ignored) {
                // best-effort
            }
        }
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

    private static void setSentCount(CursorWebSocketSendLoop loop, long count) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("nextWireSeq");
        f.setAccessible(true);
        f.setLong(loop, count);
    }

    private static long[] txns(long... v) {
        return v;
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    /**
     * A durable-ack loop wired the way production wires it -- a live
     * reconnect factory and {@code running == true} -- but with the I/O
     * thread never spun: frames are delivered by reflection, and a RETRIABLE
     * NACK's inline recycle takes the REAL connectLoop/swapClient path
     * (factory connect, durable tracking drop, wire-seq reset, cursor
     * repositioning). Backoffs are shrunk so paced recycles don't slow the
     * test down.
     */
    private CursorWebSocketSendLoop newDurableLoop(CursorSendEngine engine,
                                                   List<WebSocketClient> clients) throws Exception {
        CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    NackingClient c = new NackingClient();
                    clients.add(c);
                    return c;
                },
                5_000L, 5L, 10L, true,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS,
                MAX_REJECTIONS);
        // The loop is driven by reflection, not by its own I/O thread, but
        // the recycle machinery must see a live loop or connectLoop's guard
        // treats the first retriable NACK as fatal.
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("running");
        f.setAccessible(true);
        f.setBoolean(loop, true);
        return loop;
    }
}
