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
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Randomised stress test for the durable-ack-driven trim path. Generates a
 * stream of OK and durable-ack frames against a small table set, mixing in
 * occasional NACKs, empty OKs, and reorderings the protocol allows. After
 * each operation the test checks the global invariant: the loop's ackedFsn
 * must equal the largest contiguous prefix of wireSeqs whose every
 * (table, seqTxn) is covered by the watermarks reported so far. Any drift
 * either advances trim past undurable data (corruption) or stalls trim
 * behind durable data (correctness leak).
 * <p>
 * A NACK (SCHEMA_MISMATCH) is TERMINAL: it latches the typed error and the
 * loop is dead — the iteration ends there, after asserting that trim never
 * crossed the rejected frame (nothing is dropped, nothing silently trimmed).
 */
public class CursorWebSocketSendLoopDurableAckFuzzTest {

    private static final int ITERATIONS = 500;
    private static final int MAX_FRAMES = 64;
    private static final String[] TABLE_POOL = {"trades", "orders", "fills", "positions"};

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-da-fuzz-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
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
    public void testFuzzInvariantHolds() {
        // Capture both seeds so a fuzz failure prints a reproducer the
        // operator can plug straight back into TestUtils.generateRandom(
        // null, s0, s1). generateRandom also prints them on its own, but
        // including them in the AssertionError message keeps the repro
        // recipe co-located with the failure in CI logs.
        long s0 = System.nanoTime();
        long s1 = System.currentTimeMillis();
        Rnd rnd = TestUtils.generateRandom(null, s0, s1);
        try {
            for (int iter = 0; iter < ITERATIONS; iter++) {
                runOneIteration(rnd, iter);
            }
        } catch (Throwable t) {
            throw new AssertionError("fuzz failure with seeds=" + s0 + "L," + s1 + "L", t);
        }
    }

    private static long buildDurableAckPayload(String[] tableNames, long[] seqTxns) {
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

    private static void deliver(CursorWebSocketSendLoop loop, long packed) throws Exception {
        long ptr = packed & 0xFFFFFFFFFFFFL;
        int size = (int) (packed >>> 48);
        try {
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

    private static void runOneIteration(Rnd rnd, int iter) throws Exception {
        // Pre-build: pick frame count, per-batch tables. Track expected
        // (table, seqTxn) so the fuzz oracle can compute the contiguous
        // durable prefix at any point.
        TestUtils.assertMemoryLeak(() -> {
            int frames = 1 + rnd.nextInt(MAX_FRAMES);
            String tmp = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-da-fuzz-iter-" + System.nanoTime() + "-" + iter).toString();
            Assert.assertEquals(0, Files.mkdir(tmp, Files.DIR_MODE_DEFAULT));
            try {
                long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try (CursorSendEngine engine = new CursorSendEngine(tmp, 65536)) {
                    for (int i = 0; i < frames; i++) {
                        engine.appendBlocking(buf, 8);
                    }
                    CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                            null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                            () -> {
                                throw new UnsupportedOperationException();
                            },
                            5_000L, 100L, 5_000L, true);
                    Field f = CursorWebSocketSendLoop.class.getDeclaredField("nextWireSeq");
                    f.setAccessible(true);
                    f.setLong(loop, frames);

                    // Generate per-frame (tables, seqTxns) and feed OKs/NACKs
                    // in random interleavings with durable-acks.
                    String[][] frameTables = new String[frames][];
                    long[][] frameSeqTxns = new long[frames][];
                    boolean[] isNack = new boolean[frames];
                    Map<String, Long> nextSeqTxn = new HashMap<>();
                    for (int i = 0; i < frames; i++) {
                        int tableCount = rnd.nextInt(4); // 0..3 tables (0 = empty OK)
                        String[] tables = new String[tableCount];
                        long[] seqTxns = new long[tableCount];
                        for (int t = 0; t < tableCount; t++) {
                            String name;
                            do {
                                name = TABLE_POOL[rnd.nextInt(TABLE_POOL.length)];
                            } while (containsName(tables, t, name));
                            tables[t] = name;
                            long next = nextSeqTxn.getOrDefault(name, 0L);
                            seqTxns[t] = next;
                            nextSeqTxn.put(name, next + 1);
                        }
                        frameTables[i] = tables;
                        frameSeqTxns[i] = seqTxns;
                        isNack[i] = rnd.nextInt(20) == 0; // 5% NACK rate
                    }

                    // Oracle: durable watermark per table, observed by oracle.
                    Map<String, Long> oracleWatermarks = new HashMap<>();
                    int nextOk = 0;
                    while (nextOk < frames || rnd.nextInt(4) == 0) {
                        // Mix OK and DURABLE_ACK frames at random.
                        int op = rnd.nextInt(3);
                        if (op == 0 && nextOk < frames) {
                            // Send OK or NACK for nextOk
                            if (isNack[nextOk]) {
                                // TERMINAL: the NACK latches the typed error;
                                // no placeholder is enqueued and trim must
                                // never cross the rejected frame. A production
                                // loop is dead at this point -- end the
                                // iteration after checking both facts.
                                deliver(loop, buildErrorPayload(nextOk));
                                try {
                                    loop.checkError();
                                    Assert.fail("iter=" + iter + ": NACK at wireSeq=" + nextOk
                                            + " must latch a terminal error");
                                } catch (LineSenderServerException expected) {
                                }
                                long acked = engine.ackedFsn();
                                Assert.assertTrue(
                                        "iter=" + iter + " trim crossed a rejected frame:"
                                                + " ackedFsn=" + acked + " nackedWireSeq=" + nextOk,
                                        acked < nextOk);
                                return; // iteration over: the loop is terminal
                            }
                            deliver(loop, buildOkPayload(nextOk, frameTables[nextOk], frameSeqTxns[nextOk]));
                            nextOk++;
                        } else {
                            // Emit a durable-ack covering some random prefix of seqTxns.
                            String[] daTables = new String[TABLE_POOL.length];
                            long[] daSeqTxns = new long[TABLE_POOL.length];
                            int n = 0;
                            for (String t : TABLE_POOL) {
                                long maxIssued = nextSeqTxn.getOrDefault(t, 0L) - 1;
                                if (maxIssued < 0) continue;
                                long w = oracleWatermarks.getOrDefault(t, -1L);
                                long candidate = w + rnd.nextInt((int) (maxIssued - w) + 1);
                                if (candidate <= w) continue;
                                daTables[n] = t;
                                daSeqTxns[n] = candidate;
                                oracleWatermarks.put(t, candidate);
                                n++;
                            }
                            if (n == 0) continue;
                            String[] tableSlice = new String[n];
                            long[] txnSlice = new long[n];
                            System.arraycopy(daTables, 0, tableSlice, 0, n);
                            System.arraycopy(daSeqTxns, 0, txnSlice, 0, n);
                            deliver(loop, buildDurableAckPayload(tableSlice, txnSlice));
                        }

                        // Compute oracle expected ackedFsn: largest k such that
                        // every entry 0..k is durable. (Only OK'd frames reach
                        // here -- a NACK ends the iteration above.)
                        long expected = -1L;
                        for (int i = 0; i < nextOk; i++) {
                            boolean durable = true;
                            for (int t = 0; t < frameTables[i].length; t++) {
                                long w = oracleWatermarks.getOrDefault(frameTables[i][t], -1L);
                                if (w < frameSeqTxns[i][t]) {
                                    durable = false;
                                    break;
                                }
                            }
                            if (!durable) break;
                            expected = i;
                        }
                        long actual = engine.ackedFsn();
                        Assert.assertTrue(
                                "iter=" + iter + " frame=" + nextOk + " ackedFsn=" + actual + " expected=" + expected
                                        + " frames=" + frames,
                                actual <= expected);
                        Assert.assertTrue(
                                "iter=" + iter + " frame=" + nextOk + " ackedFsn=" + actual + " expected=" + expected
                                        + " stalled below durable prefix",
                                actual >= expected);
                    }
                } finally {
                    Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                rmDir(tmp);
            }
        });
    }

    private static long buildErrorPayload(long wireSeq) {
        byte[] msg = "fuzz".getBytes(StandardCharsets.UTF_8);
        int size = 11 + msg.length;
        long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_SCHEMA_MISMATCH);
        Unsafe.getUnsafe().putLong(ptr + 1, wireSeq);
        Unsafe.getUnsafe().putShort(ptr + 9, (short) msg.length);
        for (int i = 0; i < msg.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + 11 + i, msg[i]);
        }
        return ptr | (((long) size) << 48);
    }

    private static boolean containsName(String[] arr, int len, String name) {
        for (int i = 0; i < len; i++) if (name.equals(arr[i])) return true;
        return false;
    }

    private static void rmDir(String dir) {
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }
}
