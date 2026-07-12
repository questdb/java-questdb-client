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
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Guards the reconnect/failover symbol-dictionary catch-up ACK alignment in
 * {@link CursorWebSocketSendLoop#setWireBaselineWithCatchUp}.
 * <p>
 * On a fresh connection the loop re-registers the whole dictionary with a
 * catch-up frame BEFORE replaying data frames. Each catch-up frame consumes a
 * wire sequence, so the loop anchors {@code fsnAtZero = replayStart - catchUpFrames}
 * to keep every catch-up frame mapped to an already-acked FSN. Dropping the
 * {@code - catchUpFrames} term is silent data loss: a server ACK for a catch-up
 * frame then translates through {@code engine.acknowledge(fsnAtZero + wireSeq)}
 * to an FSN at or above {@code replayStart}, trimming a not-yet-delivered data
 * frame from the store-and-forward log.
 * <p>
 * The loop is constructed but never {@link CursorWebSocketSendLoop#start started};
 * the catch-up runs against a stub {@link WebSocketClient} that counts frames, and
 * the OK is delivered straight into the inner {@code ResponseHandler} -- the same
 * white-box idiom {@code CursorWebSocketSendLoopDurableAckTest} uses, because
 * {@code setWireBaselineWithCatchUp} and the wire ports have no public entry point.
 * {@link CursorSendEngine#ackedFsn()} is the authoritative trim watermark asserted
 * against.
 */
public class CursorWebSocketSendLoopCatchUpAlignmentTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-cursor-catchup-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test
    public void testCatchUpFrameAckDoesNotAdvanceTrimWatermark() throws Exception {
        // Single catch-up frame (server advertises no cap). Two frames were
        // acked before the reconnect (ackedFsn=1), FSN 2 is unacked. The catch-up
        // frame's OK must NOT advance the watermark past 1 -- it carries no data,
        // only the dictionary the fresh server needs before replay.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0); // 0 => no cap => one frame
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 3);            // FSN 0,1,2 published
                engine.acknowledge(1);              // ackedFsn=1 => replayStart=2, FSN 2 still unacked
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "s0", "s1");   // sentDictCount=2 => catch-up fires
                    long replayStart = engine.ackedFsn() + 1L; // = 2

                    invokeSetWireBaselineWithCatchUp(loop, replayStart);

                    assertEquals("whole dictionary fits one frame under no cap",
                            1, client.framesSent);

                    // Behavioural (the harm): the catch-up frame (wire seq 0) is
                    // OK'd by the fresh server. It carries no data, so it must
                    // resolve to an already-acked FSN and leave the trim watermark
                    // untouched -- advancing it would trim the undelivered FSN 2.
                    deliverOk(loop, 0);
                    assertEquals("catch-up frame ACK must not advance the trim watermark "
                                    + "(would trim an undelivered data frame -> silent data loss)",
                            1L, engine.ackedFsn());
                    // Mechanism: the catch-up frames are anchored below replayStart.
                    assertEquals("fsnAtZero must be anchored catchUpFrames below replayStart",
                            replayStart - client.framesSent, readLong(loop, "fsnAtZero"));
                } finally {
                    loop.close(); // frees the seeded mirror + the stub client's buffers
                }
            }
        });
    }

    @Test
    public void testSplitCatchUpFramesAcksDoNotAdvanceTrimWatermark() throws Exception {
        // A small advertised cap splits the dictionary across several catch-up
        // frames, so the fsnAtZero offset must subtract the full frame count. Ack
        // the LAST catch-up wire sequence: it still maps below replayStart. With
        // the offset dropped it would translate to replayStart+1 and over-trim.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(40); // budget 12 => one 11-byte symbol per frame
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 5);            // FSN 0..4 published
                engine.acknowledge(2);              // ackedFsn=2 => replayStart=3, FSN 3,4 unacked
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "symbol0000", "symbol0001"); // 11 bytes each -> two frames
                    long replayStart = engine.ackedFsn() + 1L; // = 3

                    invokeSetWireBaselineWithCatchUp(loop, replayStart);

                    assertEquals("cap must split the two symbols across two frames",
                            2, client.framesSent);

                    // ACK the highest catch-up wire sequence (the last catch-up
                    // frame). It too must map below replayStart -- with the offset
                    // dropped it translates to replayStart+1 and over-trims.
                    deliverOk(loop, client.framesSent - 1);
                    assertEquals("no catch-up frame ACK may advance the trim watermark",
                            2L, engine.ackedFsn());
                    assertEquals("fsnAtZero must subtract the full split frame count",
                            replayStart - client.framesSent, readLong(loop, "fsnAtZero"));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testTransientCatchUpSendFailureIsRetriableNotTerminal() throws Exception {
        // A transient wire failure WHILE shipping the catch-up (the fresh
        // connection drops mid-handshake) must surface as a retriable
        // CatchUpSendException for the reconnect loop to handle -- it must NOT
        // call fail(). From inside the catch-up fail() re-enters connectLoop
        // (corrupting the fsnAtZero/nextWireSeq mapping, or overflowing the stack
        // on a flapping connection) or, with no reconnect attempt reachable,
        // latches a terminal -- turning a transient outage into a hard failure and
        // breaking store-and-forward. Only the oversized-entry (non-retriable)
        // terminal was covered; this pins the retriable path.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0, true); // sendBinary throws
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                engine.acknowledge(0); // ackedFsn=0 => a real unacked frame exists behind the catch-up
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "s0", "s1"); // non-empty dict => catch-up fires and hits the failing send
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("a transient catch-up send failure must raise a retriable "
                                + "CatchUpSendException, not be swallowed into fail()/a terminal");
                    } catch (InvocationTargetException e) {
                        assertEquals("transient catch-up send failure must surface as CatchUpSendException",
                                "CatchUpSendException", e.getCause().getClass().getSimpleName());
                    }
                    // Retriable, not terminal: the producer-facing error latch stays clear.
                    loop.checkError();
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testAccumulateSentDictPartialOverlapExtendsMirror() throws Exception {
        // M3: accumulateSentDict must handle a delta that STRADDLES the mirror tip
        // (deltaStart < sentDictCount < deltaStart+deltaCount) by copying only the
        // new tail, not dropping the whole frame. The monotonic producer never emits
        // a straddling delta in steady state (so the pre-fix drop-whole-frame guard
        // passed every test), but a torn-dict replay can seed the mirror smaller than
        // a frame's coverage. Seed the mirror with 1 symbol, feed a [0..2] delta, and
        // assert the mirror extends to all 3 -- pre-fix it stayed at 1, leaving the
        // reconnect catch-up incomplete and shifting server-side ids.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, "aa"); // sentDictCount = 1, mirror holds "aa"
                    int[] frameLen = new int[1];
                    long frame = buildDeltaFrame(0, new String[]{"aa", "bb", "cc"}, frameLen);
                    try {
                        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                                "accumulateSentDict", long.class, int.class, int.class);
                        m.setAccessible(true);
                        m.invoke(loop, frame, frameLen[0], 0);
                    } finally {
                        Unsafe.free(frame, frameLen[0], MemoryTag.NATIVE_DEFAULT);
                    }
                    assertEquals("straddling delta must extend the mirror to all 3 ids",
                            3, readInt(loop, "sentDictCount"));
                    assertEquals("mirror must hold the two new tail symbols after the "
                                    + "already-held prefix, gap-free",
                            Arrays.asList("aa", "bb", "cc"), readMirrorSymbols(loop));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCatchUpChunkFrameSizeOverflowFailsLoud() throws Exception {
        // M3: sendDictCatchUp caps each chunk under the budget, so the single-frame
        // catch-up path cannot overflow its int frameLen at any real cardinality. The
        // guard must still be LOCAL -- a future caller must not be able to feed a
        // wrapped-negative frameLen to Unsafe.malloc. An oversized symbolsLen must
        // fail loud (CatchUpSendException) BEFORE the malloc; the guard fires before
        // symbolsAddr is read, so a dummy address is fine.
        TestUtils.assertMemoryLeak(() -> {
            CatchUpCapturingClient client = new CatchUpCapturingClient(0);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    Method m = CursorWebSocketSendLoop.class.getDeclaredMethod(
                            "sendCatchUpChunk", int.class, int.class, long.class, int.class);
                    m.setAccessible(true);
                    // symbolsLen past the mirror ceiling: HEADER + varints + symbolsLen
                    // overflows an int, so the guard must reject it before malloc.
                    m.invoke(loop, 0, 1, 0L, Integer.MAX_VALUE - 4);
                    fail("an overflowing catch-up frame size must fail loud, not malloc negative");
                } catch (InvocationTargetException e) {
                    assertEquals("overflow must surface as CatchUpSendException",
                            "CatchUpSendException", e.getCause().getClass().getSimpleName());
                    assertTrue("message must name the frame-size guard: " + e.getCause().getMessage(),
                            e.getCause().getMessage().contains("catch-up frame exceeds the maximum size"));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCatchUpCapGapRetriesUntilBudgetThenLatches() throws Exception {
        // M1: an entry too large for the fresh server's cap during catch-up (a
        // heterogeneous / rolling-cap failover to a smaller-cap node) must NOT latch
        // on first sight. sendDictCatchUp throws a RETRIABLE CatchUpSendException so
        // the reconnect loop rides it out -- a larger-cap node may return -- and only
        // after MAX_CATCHUP_CAP_GAP_ATTEMPTS consecutive cap gaps does it recordFatal.
        // Pre-fix the first cap gap latched a terminal, so one transient failover to a
        // smaller-cap node killed the sender. (A successful catch-up resets the budget;
        // the other catch-up tests, which use a fitting cap, never trip it.)
        TestUtils.assertMemoryLeak(() -> {
            Field maxField = CursorWebSocketSendLoop.class.getDeclaredField("MAX_CATCHUP_CAP_GAP_ATTEMPTS");
            maxField.setAccessible(true);
            int maxAttempts = maxField.getInt(null);
            // cap 160 => catch-up budget is below a ~216-byte solo frame for a 200-char symbol.
            CatchUpCapturingClient client = new CatchUpCapturingClient(160);
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    seedMirror(loop, TestUtils.repeat("x", 200));
                    // Attempts 1 .. max-1 are retriable: no terminal is latched.
                    for (int i = 1; i < maxAttempts; i++) {
                        try {
                            invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                            fail("cap gap must raise a retriable CatchUpSendException (attempt " + i + ')');
                        } catch (InvocationTargetException e) {
                            assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                            assertTrue("attempt " + i + " must name the catch-up cap gap: "
                                            + e.getCause().getMessage(),
                                    e.getCause().getMessage().contains("during catch-up"));
                        }
                        loop.checkError(); // under budget => retriable => no terminal
                    }
                    // The exhausting attempt still throws, and now latches the terminal.
                    try {
                        invokeSetWireBaselineWithCatchUp(loop, engine.ackedFsn() + 1L);
                        fail("the exhausting cap gap must still raise CatchUpSendException");
                    } catch (InvocationTargetException e) {
                        assertEquals("CatchUpSendException", e.getCause().getClass().getSimpleName());
                    }
                    try {
                        loop.checkError();
                        fail("exhausting the cap-gap settle budget must latch a terminal");
                    } catch (LineSenderException terminal) {
                        assertTrue("terminal must name the exhausted catch-up cap gap: " + terminal.getMessage(),
                                terminal.getMessage().contains("during catch-up")
                                        && terminal.getMessage().contains("must be resent"));
                    }
                } finally {
                    loop.close();
                }
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

    // Builds a QWP delta frame [12-byte header][deltaStart varint][deltaCount
    // varint][ [len varint][utf8] ... ] for the given symbols. accumulateSentDict
    // skips the header, so its content is irrelevant; the caller frees the frame.
    private static long buildDeltaFrame(int deltaStart, String[] symbols, int[] outLen) {
        int deltaCount = symbols.length;
        int size = 12 + varintSize(deltaStart) + varintSize(deltaCount);
        for (String s : symbols) {
            size += varintSize(s.getBytes(StandardCharsets.UTF_8).length)
                    + s.getBytes(StandardCharsets.UTF_8).length;
        }
        long addr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        long p = writeVarint(addr + 12, deltaStart);
        p = writeVarint(p, deltaCount);
        for (String s : symbols) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            p = writeVarint(p, b.length);
            for (byte x : b) {
                Unsafe.getUnsafe().putByte(p++, x);
            }
        }
        outLen[0] = size;
        return addr;
    }

    private static int readInt(CursorWebSocketSendLoop loop, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(loop);
    }

    // Parses the loop's native sent-dictionary mirror ([len varint][utf8]...) back
    // into the symbol strings a reconnect catch-up would re-register.
    private static List<String> readMirrorSymbols(CursorWebSocketSendLoop loop) throws Exception {
        long addr = readLong(loop, "sentDictBytesAddr");
        int len = readInt(loop, "sentDictBytesLen");
        List<String> out = new ArrayList<>();
        long p = addr;
        long limit = addr + len;
        while (p < limit) {
            long l = 0;
            int shift = 0;
            while (p < limit) {
                byte b = Unsafe.getUnsafe().getByte(p++);
                l |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            byte[] bytes = new byte[(int) l];
            for (int i = 0; i < l; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(p++);
            }
            out.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return out;
    }

    // Delivers a 0-table STATUS_OK for {@code wireSeq} into the loop's response
    // handler, mimicking the server acking a catch-up frame (which carries no tables).
    private static void deliverOk(CursorWebSocketSendLoop loop, long wireSeq) throws Exception {
        int size = 11; // status(1) + sequence(8) + tableCount(2)
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
            Unsafe.getUnsafe().putLong(ptr + 1, wireSeq);
            Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);
            Field f = CursorWebSocketSendLoop.class.getDeclaredField("responseHandler");
            f.setAccessible(true);
            Object handler = f.get(loop);
            Method m = handler.getClass().getDeclaredMethod("onBinaryMessage", long.class, int.class);
            m.setAccessible(true);
            m.invoke(handler, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void invokeSetWireBaselineWithCatchUp(CursorWebSocketSendLoop loop, long replayStart) throws Exception {
        Method m = CursorWebSocketSendLoop.class.getDeclaredMethod("setWireBaselineWithCatchUp", long.class);
        m.setAccessible(true);
        m.invoke(loop, replayStart);
    }

    private CursorWebSocketSendLoop newLoop(CursorSendEngine engine, WebSocketClient client) {
        return new CursorWebSocketSendLoop(
                client, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new UnsupportedOperationException("test loop is never started");
                },
                5_000L, 100L, 5_000L, false);
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    private static long readLong(CursorWebSocketSendLoop loop, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(loop);
    }

    // Populates the loop's native sent-dictionary mirror with {@code symbols} in
    // the on-wire [len varint][utf8] layout, so setWireBaselineWithCatchUp sees a
    // non-empty dictionary to re-register. loop.close() frees it.
    private static void seedMirror(CursorWebSocketSendLoop loop, String... symbols) throws Exception {
        int total = 0;
        for (String s : symbols) {
            int len = s.getBytes(StandardCharsets.UTF_8).length;
            total += varintSize(len) + len;
        }
        long addr = Unsafe.malloc(total, MemoryTag.NATIVE_DEFAULT);
        long p = addr;
        for (String s : symbols) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            p = writeVarint(p, bytes.length);
            for (byte b : bytes) {
                Unsafe.getUnsafe().putByte(p++, b);
            }
        }
        setField(loop, "sentDictBytesAddr", addr);
        setIntField(loop, "sentDictBytesCapacity", total);
        setIntField(loop, "sentDictBytesLen", total);
        setIntField(loop, "sentDictCount", symbols.length);
    }

    private static void setField(Object target, String name, long value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static void setIntField(Object target, String name, int value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    private static int varintSize(long value) {
        int n = 1;
        while (value > 0x7F) {
            value >>>= 7;
            n++;
        }
        return n;
    }

    private static long writeVarint(long addr, long value) {
        while (value > 0x7F) {
            Unsafe.getUnsafe().putByte(addr++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(addr++, (byte) value);
        return addr;
    }

    // Stub transport: completes no real I/O. getServerMaxBatchSize drives the
    // catch-up split; sendBinary counts the frames the catch-up emitted, or --
    // when throwOnSend is set -- raises a transient wire error to model the fresh
    // connection dropping mid-catch-up.
    private static final class CatchUpCapturingClient extends WebSocketClient {
        private final int cap;
        private final boolean throwOnSend;
        private int framesSent;

        CatchUpCapturingClient(int cap) {
            this(cap, false);
        }

        CatchUpCapturingClient(int cap, boolean throwOnSend) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.cap = cap;
            this.throwOnSend = throwOnSend;
        }

        @Override
        public int getServerMaxBatchSize() {
            return cap;
        }

        @Override
        public int getServerQwpVersion() {
            return 1;
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            if (throwOnSend) {
                throw new RuntimeException("transient wire failure during catch-up");
            }
            framesSent++;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }
}
