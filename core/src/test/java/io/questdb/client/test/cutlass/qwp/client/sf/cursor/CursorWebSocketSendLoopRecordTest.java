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
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * The send loop ships a sealed QWO1 record as one ordinary QWP frame, byte for
 * byte the frame the legacy encoder builds for the same rows, and keeps the
 * wire sequence and the symbol-dictionary mirror exactly as for a plain frame.
 */
public class CursorWebSocketSendLoopRecordTest {
    private static final int RESERVATION = 64 * 1024;
    private static final long SEGMENT_SIZE = 256 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-loop-rec-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDirRec(tmpDir);
    }

    @Test
    public void testMalformedRecordDictionaryLatchesATerminal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            RecordingClient client = new RecordingClient();
            try (Wire wire = new Wire();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                long off = wire.reserveAndWrite(engine, "a", "b");
                long payload = engine.recordPayloadAddress(off);
                Assert.assertEquals(0L, wire.seal(engine, off));
                // a dictionary count no dictionary region can hold
                Unsafe.getUnsafe().putInt(payload + QwoFormat.OFF_DICT_WORD + 4, Integer.MAX_VALUE);
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();

                    Assert.assertFalse(loop.trySendOneForTest());
                    Assert.assertEquals("a corrupt record must not reach the server", 0, client.frameCount());
                    Assert.assertEquals(0, loop.sentDictCount());
                    try {
                        loop.checkError();
                        Assert.fail("a corrupt record must latch a terminal, not be retried");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "has a malformed symbol dictionary");
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testMirrorAccumulatesRecordEntries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            RecordingClient client = new RecordingClient();
            try (Wire wire = new Wire();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                wire.registerSentSymbols("sent0", "sent1");
                Assert.assertEquals(0L, wire.sealRecord(engine, "x", "y", "sent0", "z", "x"));
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    // after positioning, so the seeded mirror sends no catch-up
                    seedMirror(loop, "sent0", "sent1");

                    Assert.assertTrue(loop.trySendOneForTest());
                    Assert.assertEquals(1, client.frameCount());
                    Assert.assertArrayEquals(wire.recordOracle(), client.frame(0));
                    Assert.assertEquals("the mirror covers dictStartId + dictCount ids", 2 + 3, loop.sentDictCount());
                    byte[] expected = entries("sent0", "sent1", "x", "y", "z");
                    Assert.assertEquals(expected.length, loop.sentDictBytesLen());
                    Assert.assertArrayEquals(expected, copy(loop.sentDictBytesAddr(), loop.sentDictBytesLen()));
                    loop.checkError();
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRecordAfterPlainFramesKeepsWireSequence() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            RecordingClient client = new RecordingClient();
            try (Wire wire = new Wire();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                byte[][] expected = new byte[4][];
                expected[0] = wire.appendPlainFrame(engine, 0L, "a", "b");
                expected[1] = wire.appendPlainFrame(engine, 1L, "c");
                Assert.assertEquals(2L, wire.sealRecord(engine, "a", "d", "d", "c", "e"));
                expected[2] = wire.recordOracle();
                expected[3] = wire.appendPlainFrame(engine, 3L, "f", "a");
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    for (int i = 0; i < expected.length; i++) {
                        Assert.assertTrue("frame " + i + " must go out", loop.trySendOneForTest());
                        Assert.assertEquals("one send per call", i + 1, client.frameCount());
                        Assert.assertEquals(i + 1, loop.getTotalFramesSent());
                    }
                    Assert.assertFalse("nothing is left to send", loop.trySendOneForTest());
                    Assert.assertEquals(expected.length, client.frameCount());
                    for (int i = 0; i < expected.length; i++) {
                        Assert.assertArrayEquals("frame " + i, expected[i], client.frame(i));
                    }
                    Assert.assertEquals("every frame's dictionary reached the mirror", 6, loop.sentDictCount());

                    // the record took exactly one wire sequence: wireSeq 2 acks FSN 2
                    deliverOk(loop, 1L);
                    Assert.assertEquals(1L, engine.ackedFsn());
                    deliverOk(loop, 2L);
                    Assert.assertEquals(2L, engine.ackedFsn());
                    deliverOk(loop, 3L);
                    Assert.assertEquals(3L, engine.ackedFsn());
                    loop.checkError();
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRecordDeltaGuardFailsWhenMirrorIsShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            RecordingClient client = new RecordingClient();
            try (Wire wire = new Wire();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                wire.registerSentSymbols("sent0", "sent1");
                Assert.assertEquals(0L, wire.sealRecord(engine, "x", "sent1"));
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    loop.setRunningForTest(true);
                    loop.positionCursorForStartForTest();
                    seedMirror(loop, "sent0");

                    Assert.assertFalse("the guard must refuse the record", loop.trySendOneForTest());
                    Assert.assertEquals("no frame may reach the server through a gap", 0, client.frameCount());
                    Assert.assertEquals(0L, loop.getTotalFramesSent());
                    Assert.assertEquals("the mirror is untouched", 1, loop.sentDictCount());
                    try {
                        loop.checkError();
                        Assert.fail("the guard must latch a terminal error");
                    } catch (LineSenderException e) {
                        TestUtils.assertContains(e.getMessage(), "frame delta start 2 exceeds recovered dictionary size 1");
                        TestUtils.assertContains(e.getMessage(), "resend required");
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testSealedRecordIsSentAsOneFrameEqualToTheOracle() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            RecordingClient client = new RecordingClient();
            try (Wire wire = new Wire();
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_SIZE)) {
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        client, engine, 0L, 10_000L,
                        () -> {
                            throw new IOException("no reconnect in this test");
                        },
                        100L, 5_000L, false);
                try {
                    loop.start();
                    long off = wire.reserveAndWrite(engine, "a", "b", "a", "c", "b");
                    // the open record is not published, so the loop cannot see it
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                    Assert.assertEquals("nothing goes out while the record is open", 0, client.frameCount());

                    Assert.assertEquals(0L, wire.seal(engine, off));
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (client.frameCount() < 1 && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                    }
                    Assert.assertEquals(1, client.frameCount());
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                    Assert.assertEquals("one record, one frame", 1, client.frameCount());
                    byte[] sent = client.frame(0);
                    Assert.assertEquals("a QWP frame, not the raw record", QwpConstants.MAGIC_MESSAGE, intAt(sent, 0));
                    Assert.assertArrayEquals(wire.recordOracle(), sent);
                    loop.checkError();
                } finally {
                    loop.close();
                }
            }
        });
    }

    private static byte[] copy(long addr, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return bytes;
    }

    // a 0-table STATUS_OK for wireSeq, as the server acks a data frame
    private static void deliverOk(CursorWebSocketSendLoop loop, long wireSeq) {
        int size = 11; // status(1) + sequence(8) + tableCount(2)
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
            Unsafe.getUnsafe().putLong(ptr + 1, wireSeq);
            Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);
            loop.deliverResponseForTest(ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // the mirror's and the wire's entry layout: [varint len][utf8] per symbol
    private static byte[] entries(String... symbols) {
        ArrayList<Byte> out = new ArrayList<>();
        for (String s : symbols) {
            byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
            long v = utf8.length;
            while (v > 0x7F) {
                out.add((byte) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            out.add((byte) v);
            for (byte b : utf8) {
                out.add(b);
            }
        }
        byte[] bytes = new byte[out.size()];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = out.get(i);
        }
        return bytes;
    }

    private static int intAt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) | (bytes[offset + 1] & 0xFF) << 8
                | (bytes[offset + 2] & 0xFF) << 16 | (bytes[offset + 3] & 0xFF) << 24;
    }

    // never started: the tests drive positionCursorForStart and trySendOne directly
    private static CursorWebSocketSendLoop newLoop(CursorSendEngine engine, WebSocketClient client) {
        return new CursorWebSocketSendLoop(
                client, engine, 0, 1_000_000L,
                () -> {
                    throw new IOException("no reconnect in this test");
                },
                0, 1);
    }

    // the loop takes ownership of the seeded mirror and frees it on close
    private static void seedMirror(CursorWebSocketSendLoop loop, String... symbols) {
        byte[] bytes = entries(symbols);
        long addr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(addr + i, bytes[i]);
        }
        loop.seedSentDictMirrorForTest(addr, bytes.length, symbols.length);
    }

    /**
     * In-memory transport: {@code sendBinary} keeps a copy of every frame. The
     * frame list is read by the test thread while the I/O thread appends.
     */
    private static final class RecordingClient extends WebSocketClient {
        private final ArrayList<byte[]> frames = new ArrayList<>();

        RecordingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            byte[] frame = copy(dataPtr, length);
            synchronized (frames) {
                frames.add(frame);
            }
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            return false;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }

        byte[] frame(int i) {
            synchronized (frames) {
                return frames.get(i);
            }
        }

        int frameCount() {
            synchronized (frames) {
                return frames.size();
            }
        }
    }

    /**
     * Builds the ring's frames and the bytes the wire must carry for them. A
     * record is written row by row through a {@link QwoRecordWriter}; its oracle
     * is the legacy {@link QwpWebSocketEncoder} frame for the same rows, with the
     * same dictionary range, as in {@code QwoDifferentialTest}.
     */
    private static final class Wire implements QuietCloseable {
        private final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        private final QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
        private final QwoRecordWriter writer = new QwoRecordWriter();
        private QwpTableBuffer legacy;
        private int recordBatchMax;
        private int recordDictStartId;
        private QwpTableBuffer staged;

        @Override
        public void close() {
            closeTables();
            encoder.close();
            writer.close();
        }

        /**
         * Appends a plain QWP frame of one row per symbol, carrying the symbols
         * it registers, and returns its bytes.
         */
        byte[] appendPlainFrame(CursorSendEngine engine, long expectedFsn, String... symbols) {
            int confirmedMax = dict.size() - 1;
            try (QwpTableBuffer table = new QwpTableBuffer("plain")) {
                for (int r = 0; r < symbols.length; r++) {
                    putRow(table, symbols[r], r);
                }
                encoder.beginMessage(1, dict, confirmedMax, dict.size() - 1);
                encoder.addTable(table);
                int size = encoder.finishMessage();
                long ptr = encoder.getBuffer().getBufferPtr();
                Assert.assertEquals(expectedFsn, engine.appendBlocking(ptr, size));
                return copy(ptr, size);
            }
        }

        byte[] recordOracle() {
            encoder.beginMessage(1, dict, recordDictStartId - 1, recordBatchMax);
            encoder.addTable(legacy);
            int size = encoder.finishMessage();
            return copy(encoder.getBuffer().getBufferPtr(), size);
        }

        /**
         * Stands for symbols an earlier connection already registered: they
         * are in the producer's dictionary, so a record starts after them.
         */
        void registerSentSymbols(String... symbols) {
            for (String s : symbols) {
                dict.getOrAddSymbol(s);
            }
        }

        /**
         * Reserves a record, writes one committed row per symbol into it and
         * the same rows into the oracle's table; returns the envelope offset.
         */
        long reserveAndWrite(CursorSendEngine engine, String... symbols) {
            closeTables();
            staged = new QwpTableBuffer("trades");
            legacy = new QwpTableBuffer("trades");
            recordDictStartId = dict.size();
            recordBatchMax = recordDictStartId - 1;
            long off = engine.reserveRecordBlocking(RESERVATION);
            writer.open(engine.recordPayloadAddress(off), engine.reservedRecordCapacity(),
                    QwpConstants.VERSION, recordDictStartId, QwoFormat.DEFAULT_DICT_RESERVE);
            for (int r = 0; r < symbols.length; r++) {
                putRow(staged, symbols[r], r);
                recordBatchMax = dict.size() - 1;
                Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(staged, dict, recordBatchMax));
                staged.clearStagedRow();
                putRow(legacy, symbols[r], r);
            }
            writer.publish();
            return off;
        }

        long seal(CursorSendEngine engine, long off) {
            return engine.sealRecord(off, writer.seal(false));
        }

        long sealRecord(CursorSendEngine engine, String... symbols) {
            return seal(engine, reserveAndWrite(engine, symbols));
        }

        private void closeTables() {
            if (staged != null) {
                staged.close();
                staged = null;
            }
            if (legacy != null) {
                legacy.close();
                legacy = null;
            }
        }

        private void putRow(QwpTableBuffer table, String symbol, int row) {
            table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)
                    .addSymbolWithGlobalId(symbol, dict.getOrAddSymbol(symbol));
            table.getOrCreateColumn("price", QwpConstants.TYPE_DOUBLE, true).addDouble(1.5 * row);
            table.getOrCreateColumn("qty", QwpConstants.TYPE_LONG, true).addLong(100L + row);
            table.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + row);
            table.nextRow();
        }
    }
}
