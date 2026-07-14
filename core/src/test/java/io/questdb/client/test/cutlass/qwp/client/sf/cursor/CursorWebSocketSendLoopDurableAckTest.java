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

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the durable-ack-driven trim path in {@link CursorWebSocketSendLoop}.
 * <p>
 * The loop is constructed normally but never {@link CursorWebSocketSendLoop#start started};
 * frames are delivered directly into the inner {@code ResponseHandler.onBinaryMessage}
 * via reflection, mimicking the wire dispatch the I/O thread would otherwise drive.
 * The {@link CursorSendEngine} is real -- {@link CursorSendEngine#ackedFsn} is the
 * authoritative trim watermark we assert against.
 */
public class CursorWebSocketSendLoopDurableAckTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-da-" + System.nanoTime()).toString();
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
    public void testCumulativeAdvanceAcrossManyEntries() throws Exception {
        // Six OKs queued -- trades:0 trades:1 orders:5 trades:2 (orders+trades) (orders+trades)
        // A single durable-ack with cumulative watermarks (trades=2, orders=10) clears
        // the head until it hits an entry that requires a higher watermark.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 6);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 6);
                deliverOk(loop, 0, names("trades"), txns(0));
                deliverOk(loop, 1, names("trades"), txns(1));
                deliverOk(loop, 2, names("orders"), txns(5));
                deliverOk(loop, 3, names("trades"), txns(2));
                deliverOk(loop, 4, names("trades", "orders"), txns(3, 7));
                deliverOk(loop, 5, names("trades", "orders"), txns(4, 8));
                assertEquals(-1L, engine.ackedFsn());

                // Cumulative watermarks: trades up to 2, orders up to 10.
                deliverDurableAck(loop, names("trades", "orders"), txns(2L, 10L));
                // Entries 0..3 are durable (trades<=2 OR orders<=5<=10 OR trades<=2).
                // Entry 4 needs trades>=3 -- not yet -> stops here.
                assertEquals(3L, engine.ackedFsn());

                deliverDurableAck(loop, names("trades"), txns(4L));
                // Entries 4 and 5 now durable (trades>=4, orders already at 10).
                assertEquals(5L, engine.ackedFsn());
                assertEquals(0, pendingSize(loop));
            }
        });
    }

    @Test
    public void testDefaultModeIgnoresStrayDurableAck() throws Exception {
        // Spec says servers must not emit durable-ack unless the client opted in.
        // If one does anyway, the loop logs a warning and drops the frame --
        // never advances trim. ackedFsn stays put.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDefaultLoop(engine);
                setSentCount(loop, 1);
                deliverDurableAck(loop, names("anything"), txns(99L));
                assertEquals(-1L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testDefaultModeOkAdvancesTrim() throws Exception {
        // Sanity: the existing OK-driven path is unchanged when durableAckMode=false.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 3);
                CursorWebSocketSendLoop loop = newDefaultLoop(engine);
                setSentCount(loop, 3);
                deliverOk(loop, 1, names("t1"), txns(10L));
                assertEquals(1L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testDurableAckBeforeOkAdvancesOnEnqueue() throws Exception {
        // A durable-ack arriving before any OK just stashes watermarks; the
        // queue is empty so drainPendingDurable is a no-op. The next OK whose
        // (table, seqTxn) is already covered by that watermark drains
        // immediately on enqueue -- no extra durable-ack required.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);

                deliverDurableAck(loop, names("trades"), txns(50L));
                assertEquals(-1L, engine.ackedFsn());

                deliverOk(loop, 0, names("trades"), txns(50L));
                assertEquals(0L, engine.ackedFsn());
                assertEquals(0, pendingSize(loop));
            }
        });
    }

    @Test
    public void testDurableModeBackwardsWatermarkIgnored() throws Exception {
        // A delayed/duplicate durable-ack that names a smaller seqTxn for a table
        // that already advanced must not move the watermark backwards. drainPendingDurable
        // continues to use the higher value.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 2);
                deliverOk(loop, 0, names("trades"), txns(10L));
                deliverOk(loop, 1, names("trades"), txns(20L));

                deliverDurableAck(loop, names("trades"), txns(20L));
                assertEquals(1L, engine.ackedFsn());

                // Older cumulative frame -- must not unwind anything.
                deliverDurableAck(loop, names("trades"), txns(5L));
                assertEquals(1L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testDurableModeEmptyOkChainsBehindPendingEntries() throws Exception {
        // An empty OK is trivially durable, but it still respects FIFO order:
        // an earlier non-empty entry that has not yet been durable-acked blocks
        // the empty entry from advancing past it.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 2);
                deliverOk(loop, 0, names("trades"), txns(7L));
                deliverOk(loop, 1, new String[0], new long[0]);
                assertEquals(-1L, engine.ackedFsn());

                deliverDurableAck(loop, names("trades"), txns(7L));
                // Both entries clear: 0 because watermark covers it, 1 because trivially durable.
                assertEquals(1L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testDurableModeEmptyOkIsTriviallyDurable() throws Exception {
        // Empty messages produce no WAL commit and are durable as soon as any
        // preceding entries are durable. Spec: §13 Durable-Upload Acknowledgment.
        // With on-enqueue drain, an empty OK at the head trims immediately --
        // no durable-ack frame needed.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);

                deliverOk(loop, 0, new String[0], new long[0]);
                assertEquals(0L, engine.ackedFsn());
                assertEquals(0, pendingSize(loop));

                // A subsequent empty durable-ack is harmless -- nothing to drain.
                deliverDurableAck(loop, new String[0], new long[0]);
                assertEquals(0L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testDurableModeFullCoverageAdvances() throws Exception {
        // Multi-table OK requires all tables' watermarks to be at or beyond
        // the OK's per-table seqTxns before the entry pops.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);
                deliverOk(loop, 0, names("trades", "orders"), txns(10L, 20L));

                deliverDurableAck(loop, names("trades", "orders"), txns(10L, 20L));
                assertEquals(0L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testDurableModeOkDoesNotAdvanceTrim() throws Exception {
        // Single OK in durable mode buffers the entry and leaves ackedFsn alone.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);
                deliverOk(loop, 0, names("trades"), txns(42L));
                assertEquals(-1L, engine.ackedFsn());
                assertEquals(1, pendingSize(loop));
            }
        });
    }

    @Test
    public void testDurableModePartialCoverageDoesNotAdvance() throws Exception {
        // Multi-table OK whose watermark only covers one of two tables: still pending.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);
                deliverOk(loop, 0, names("trades", "orders"), txns(10L, 20L));

                deliverDurableAck(loop, names("trades"), txns(10L));
                assertEquals(-1L, engine.ackedFsn());
                assertEquals(1, pendingSize(loop));

                deliverDurableAck(loop, names("orders"), txns(20L));
                assertEquals(0L, engine.ackedFsn());
                assertEquals(0, pendingSize(loop));
            }
        });
    }

    @Test
    public void testNackInDurableModeIsTerminalAndDoesNotAdvanceTrim() throws Exception {
        // A SCHEMA_MISMATCH NACK is TERMINAL: it latches the typed error and
        // never enqueues a placeholder or advances trim. OK'd entries ahead of
        // it keep their durable-ack lifecycle; the rejected frame stays on
        // disk -- nothing is silently discarded.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 3);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 3);

                deliverOk(loop, 0, names("trades"), txns(7L));
                // Inject a SCHEMA_MISMATCH NACK for wireSeq=1 (TERMINAL).
                deliverNack(loop, 1, "bad column");

                // Terminal latched, typed, loud on the next producer call.
                try {
                    loop.checkError();
                    fail("SCHEMA_MISMATCH NACK must latch a terminal error");
                } catch (LineSenderServerException e) {
                    assertEquals(SenderError.Category.SCHEMA_MISMATCH,
                            e.getServerError().getCategory());
                    assertEquals(SenderError.Policy.TERMINAL,
                            e.getServerError().getAppliedPolicy());
                }

                // No placeholder was enqueued for the NACK and trim never
                // crossed the rejected frame: only the OK'd head is pending.
                assertEquals(1, pendingSize(loop));
                assertEquals(-1L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testStandaloneNackInDurableModeIsTerminal() throws Exception {
        // First in-flight batch is rejected: TERMINAL latches immediately,
        // nothing enqueues, trim stays untouched -- the frame is preserved
        // on disk for operator action.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);
                deliverNack(loop, 0, "bad column");
                assertEquals(-1L, engine.ackedFsn());
                assertEquals(0, pendingSize(loop));
                try {
                    loop.checkError();
                    fail("NACK must latch a terminal error");
                } catch (LineSenderServerException expected) {
                }
            }
        });
    }

    @Test
    public void testReconnectClearsPendingAndWatermarks() throws Exception {
        // After a swapClient (reconnect), the new connection re-OKs replayed
        // batches and the server re-issues cumulative durable-acks from scratch.
        // The loop must drop its previous queue and watermark map -- otherwise
        // it could either double-count or refuse to advance because old
        // watermarks no longer line up with the new wire sequencing.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 2);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 2);
                deliverOk(loop, 0, names("trades"), txns(10L));
                deliverOk(loop, 1, names("trades"), txns(11L));
                deliverDurableAck(loop, names("trades"), txns(10L));
                assertEquals(0L, engine.ackedFsn());
                assertEquals(1, pendingSize(loop));

                Method m = CursorWebSocketSendLoop.class.getDeclaredMethod("clearDurableAckTracking");
                m.setAccessible(true);
                m.invoke(loop);

                assertEquals(0, pendingSize(loop));
                assertEquals(0L, engine.ackedFsn()); // ackedFsn unchanged by clear
                // After reset, fresh OK-then-durable-ack cycle works as if first time.
                setSentCount(loop, 1); // pretend we re-sent one batch on the new connection
                setField(loop, "fsnAtZero", 1L);
                deliverOk(loop, 0, names("trades"), txns(11L));
                deliverDurableAck(loop, names("trades"), txns(11L));
                assertEquals(1L, engine.ackedFsn());
            }
        });
    }

    @Test
    public void testTotalDurableAcksDefaultModeIgnoresFrame() throws Exception {
        // Default mode: a stray durable-ack frame is logged-and-dropped before
        // the counter can increment. Spec: servers must not emit one without
        // opt-in, but if they do, getTotalDurableAcks() stays at 0.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDefaultLoop(engine);
                setSentCount(loop, 1);
                deliverDurableAck(loop, names("anything"), txns(99L));
                assertEquals("default mode never counts durable-acks",
                        0L, loop.getTotalDurableAcks());
                assertEquals(0L, loop.getTotalDurableTrimAdvances());
            }
        });
    }

    @Test
    public void testTotalDurableAcksIncrementsPerFrame() throws Exception {
        // Each STATUS_DURABLE_ACK frame received bumps the counter by exactly
        // one, regardless of whether it advances trim. Three frames, two of
        // which can't possibly cover anything (empty queue) -> still counts 3.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);

                deliverDurableAck(loop, new String[0], new long[0]);
                deliverDurableAck(loop, names("trades"), txns(5L));
                deliverDurableAck(loop, names("orders"), txns(7L));

                assertEquals(3L, loop.getTotalDurableAcks());
            }
        });
    }

    @Test
    public void testTotalDurableCountersStartAtZero() throws Exception {
        // Sanity baseline: a freshly-built loop reports zero for both counters
        // in either mode, before any frames have been delivered.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop defaultLoop = newDefaultLoop(engine);
                assertEquals(0L, defaultLoop.getTotalDurableAcks());
                assertEquals(0L, defaultLoop.getTotalDurableTrimAdvances());
            }
            try (CursorSendEngine engine = newEngine()) {
                CursorWebSocketSendLoop durableLoop = newDurableLoop(engine);
                assertEquals(0L, durableLoop.getTotalDurableAcks());
                assertEquals(0L, durableLoop.getTotalDurableTrimAdvances());
            }
        });
    }

    @Test
    public void testTotalDurableTrimAdvancesIncrementsOnEnqueueDrain() throws Exception {
        // A durable-ack that arrives before any OK only stashes watermarks; the
        // queue is empty so drainPendingDurable observes nothing to pop and the
        // trim-advance counter stays at 0. The next OK whose (table, seqTxn)
        // is already covered drains immediately on enqueue -- THAT path bumps
        // trimAdvances. End result: durableAcks=1, trimAdvances=1.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);

                deliverDurableAck(loop, names("trades"), txns(50L));
                assertEquals(1L, loop.getTotalDurableAcks());
                assertEquals("empty queue means no trim advance yet",
                        0L, loop.getTotalDurableTrimAdvances());

                deliverOk(loop, 0, names("trades"), txns(50L));
                assertEquals(1L, loop.getTotalDurableAcks());
                assertEquals("on-enqueue drain bumps the trim counter",
                        1L, loop.getTotalDurableTrimAdvances());
            }
        });
    }

    @Test
    public void testTotalDurableTrimAdvancesIncrementsOnlyWhenQueueDrains() throws Exception {
        // Two durable-acks delivered: the first only partially covers the head
        // entry, so drainPendingDurable returns without popping -> trimAdvances
        // stays at 0. The second covers the missing table -> one pop ->
        // trimAdvances increments to 1. Both deliveries count against
        // durableAcks (2 total), proving trimAdvances <= durableAcks.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 1);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 1);
                deliverOk(loop, 0, names("trades", "orders"), txns(10L, 20L));

                deliverDurableAck(loop, names("trades"), txns(10L));
                assertEquals(1L, loop.getTotalDurableAcks());
                assertEquals("partial coverage must not advance trim",
                        0L, loop.getTotalDurableTrimAdvances());

                deliverDurableAck(loop, names("orders"), txns(20L));
                assertEquals(2L, loop.getTotalDurableAcks());
                assertEquals("full coverage advances trim once",
                        1L, loop.getTotalDurableTrimAdvances());
                assertTrue("trimAdvances must stay bounded by durableAcks",
                        loop.getTotalDurableTrimAdvances() <= loop.getTotalDurableAcks());
            }
        });
    }

    @Test
    public void testTotalDurableTrimAdvancesSkipsRedundantEngineAck() throws Exception {
        // After a successful drain advances ackedFsn, a subsequent enqueue+drain
        // that resolves to the same or lower fsn must NOT bump trimAdvances. The
        // counter's contract is "watermark actually advanced", so it gates on
        // engine.acknowledge's return value rather than the pop event. Repro
        // here: deliver OK 4 + matching durable-ack -> trim advances to 1, then
        // deliver a duplicate OK 4 + redundant durable-ack -> engine.acknowledge
        // returns false (no watermark move) and trimAdvances stays at 1.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrames(engine, 5);
                CursorWebSocketSendLoop loop = newDurableLoop(engine);
                setSentCount(loop, 5);

                deliverOk(loop, 4, names("trades"), txns(50L));
                deliverDurableAck(loop, names("trades"), txns(50L));
                assertEquals("first cycle advances trim",
                        1L, loop.getTotalDurableTrimAdvances());
                long ackedAfterFirst = engine.ackedFsn();

                // Duplicate OK for the same wireSeq with a watermark that already
                // covers it. drainPendingDurable pops the entry, computes fsn at
                // ackedAfterFirst, and engine.acknowledge no-ops. Pre-fix this
                // path bumped trimAdvances anyway, overcounting.
                deliverOk(loop, 4, names("trades"), txns(50L));
                assertEquals("redundant ack must not bump trim counter",
                        1L, loop.getTotalDurableTrimAdvances());
                assertEquals("redundant ack must not move ackedFsn",
                        ackedAfterFirst, engine.ackedFsn());
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
        // Pack ptr (low 48 bits) and size (high 16 bits) into one long so callers
        // get both back without a tuple class. Sizes fit in 16 bits for these tests.
        return ptr | (((long) size) << 48);
    }

    private static void deliverDurableAck(CursorWebSocketSendLoop loop, String[] tableNames, long[] seqTxns) throws Exception {
        long packed = buildDurableAckPayload(tableNames, seqTxns);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void deliverNack(CursorWebSocketSendLoop loop, long wireSeq, String msg) throws Exception {
        long packed = buildErrorPayload(wireSeq, WebSocketResponse.STATUS_SCHEMA_MISMATCH, msg);
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
            invokeOnBinaryMessage(loop, ptr, size);
        } finally {
            Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
        }
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

    private static void invokeOnBinaryMessage(CursorWebSocketSendLoop loop, long ptr, int size) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("responseHandler");
        f.setAccessible(true);
        Object handler = f.get(loop);
        Method m = handler.getClass().getDeclaredMethod("onBinaryMessage", long.class, int.class);
        m.setAccessible(true);
        m.invoke(handler, ptr, size);
    }

    private static long[] txns(long... v) {
        return v;
    }

    private static String[] names(String... v) {
        return v;
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    private CursorWebSocketSendLoop newDefaultLoop(CursorSendEngine engine) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new UnsupportedOperationException("test loop is never started");
                },
                5_000L, 100L, 5_000L, false);
    }

    private CursorWebSocketSendLoop newDurableLoop(CursorSendEngine engine) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new UnsupportedOperationException("test loop is never started");
                },
                5_000L, 100L, 5_000L, true);
    }

    private static int pendingSize(CursorWebSocketSendLoop loop) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("pendingDurable");
        f.setAccessible(true);
        return ((java.util.ArrayDeque<?>) f.get(loop)).size();
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static void setSentCount(CursorWebSocketSendLoop loop, long count) throws Exception {
        // Force the loop's nextWireSeq to {@code count}, simulating that
        // {@code count} frames have been sent. The onBinaryMessage safety
        // clamp uses {@code nextWireSeq - 1} as the highest accepted wireSeq,
        // so setSentCount(N) permits OK acks for wireSeq 0..N-1.
        Field f = CursorWebSocketSendLoop.class.getDeclaredField("nextWireSeq");
        f.setAccessible(true);
        f.setLong(loop, count);
    }
}
