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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Orphaned deferred tail containment (the skip-and-self-ack replay policy).
 * <p>
 * A producer that crashes (or closes) mid-transaction leaves FLAG_DEFER_COMMIT
 * frames with no covering commit frame at the top of its disk-recovered SF
 * log. Those frames belong to a transaction that was never committed -- it is
 * aborted by definition, and replaying them to the server would let a
 * commit-bearing frame from the NEW session commit them, resurrecting a
 * partial transaction and violating {@code transaction=on} atomicity.
 * <p>
 * Contract under test:
 * <ul>
 *   <li>Recovery detects the tail: {@code recoveredCommitBoundaryFsn} is the
 *       last commit-bearing frame, {@code recoveredOrphanTipFsn} the tail's
 *       top (or -1 when the log ends with a commit frame).</li>
 *   <li>Fast path: when everything below the tail is already acked
 *       (trivially so when the whole log is the tail), the loop retires the
 *       tail via cumulative self-acknowledge before any frame is sent --
 *       zero wire cost, no reconnect.</li>
 *   <li>Slow path: committed-covered frames below the tail replay first; the
 *       cursor never enters the tail; once the server acks the boundary the
 *       tail retires and the connection recycles exactly once so the linear
 *       wireSeq&lt;-&gt;FSN mapping re-anchors past the gap. Frames appended
 *       by the new session then flow with correct ack attribution.</li>
 * </ul>
 */
public class CursorWebSocketSendLoopOrphanTailTest {

    private static final int FLAG_DEFER_COMMIT = 0x01;
    private static final int HEADER_OFFSET_FLAGS = 5;
    private static final int MAGIC_MESSAGE = 0x31505751; // "QWP1" little-endian

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-orphan-" + System.nanoTime()).toString();
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
    public void testRecoveryDetectsOrphanedDeferredTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, false); // fsn 0: commit-bearing
                appendFrame(engine, true);  // fsn 1: deferred
                appendFrame(engine, true);  // fsn 2: deferred -- orphan tail [1..2]
            }
            try (CursorSendEngine engine = newEngine()) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertEquals("last commit-bearing frame", 0L, engine.recoveredCommitBoundaryFsn());
                assertEquals("orphan tail tip", 2L, engine.recoveredOrphanTipFsn());
            }
        });
    }

    @Test
    public void testRecoveryReportsNoOrphansWhenLogEndsWithCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, true);  // fsn 0: deferred
                appendFrame(engine, true);  // fsn 1: deferred
                appendFrame(engine, false); // fsn 2: the group-closing commit frame
            }
            try (CursorSendEngine engine = newEngine()) {
                assertTrue(engine.wasRecoveredFromDisk());
                assertEquals("commit frame at the top: whole log is commit-covered",
                        2L, engine.recoveredCommitBoundaryFsn());
                assertEquals("no orphan tail", -1L, engine.recoveredOrphanTipFsn());
            }
        });
    }

    @Test
    public void testFastPathRetiresWholeDeferredLogBeforeAnySend() throws Exception {
        // The whole recovered log is one uncommitted deferred group
        // (boundary = -1): nothing below the tail needs acks, so the tail
        // retires at start() before any connection exists. No frame may ever
        // reach the wire and no reconnect may occur.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, true);
                appendFrame(engine, true);
                appendFrame(engine, true);
            }
            try (CursorSendEngine engine = newEngine()) {
                assertEquals(-1L, engine.recoveredCommitBoundaryFsn());
                assertEquals(2L, engine.recoveredOrphanTipFsn());

                List<AckingClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, clients);
                try {
                    loop.start();
                    awaitAckedFsn(engine, 2L);
                    assertEquals("orphaned deferred frames must never be transmitted",
                            0, totalSent(clients));
                    // The retirement happens in start() on the user thread; the
                    // I/O thread's initial connect may or may not have run yet.
                    // Either way there must never be a RE-connect.
                    assertTrue("fast path must not need more than the initial connection",
                            clientCount(clients) <= 1);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testSlowPathReplaysBelowTailThenRetiresAndRecyclesOnce() throws Exception {
        // fsn 0 is commit-covered and unacked: it must replay. fsns 1..2 are
        // the orphan tail: never transmitted. Once the server acks fsn 0 the
        // tail retires and the connection recycles exactly once (wire-seq
        // realignment). Frames appended by the new session then flow with
        // correct cumulative-ack attribution across the FSN gap.
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = newEngine()) {
                appendFrame(engine, false); // fsn 0
                appendFrame(engine, true);  // fsn 1
                appendFrame(engine, true);  // fsn 2
            }
            try (CursorSendEngine engine = newEngine()) {
                assertEquals(0L, engine.recoveredCommitBoundaryFsn());
                assertEquals(2L, engine.recoveredOrphanTipFsn());
                assertEquals("recovery must not pre-ack anything", -1L, engine.ackedFsn());

                List<AckingClient> clients = new ArrayList<>();
                CursorWebSocketSendLoop loop = newLoop(engine, clients);
                try {
                    loop.start();
                    // Replay fsn 0, server acks it, tail retires.
                    awaitAckedFsn(engine, 2L);
                    assertEquals("only the commit-covered frame below the tail may be sent",
                            1, totalSent(clients));
                    // Wire-seq realignment: exactly one recycle beyond the
                    // initial connection.
                    awaitClientCount(clients, 2);
                    assertEquals(2, clientCount(clients));

                    // New-session traffic flows across the FSN gap with
                    // correct ack attribution: fsn 3 is wireSeq 0 on the new
                    // connection, and its cumulative ack lands on fsn 3.
                    appendFrame(engine, false); // fsn 3
                    awaitAckedFsn(engine, 3L);
                    assertEquals("fsn 0 before the recycle + fsn 3 after it",
                            2, totalSent(clients));
                } finally {
                    loop.close();
                }
            }
        });
    }

    // ---------------------------------------------------------------------
    // harness
    // ---------------------------------------------------------------------

    /**
     * In-memory transport emulating a healthy server: counts sends and
     * answers every received frame with a cumulative empty-table STATUS_OK
     * for the highest wire seq seen. sendBinary and tryReceiveFrame both run
     * on the I/O thread; sentCount is volatile for test-thread assertions.
     */
    private static final class AckingClient extends WebSocketClient {
        private int ackedUpTo;
        private volatile int sentCount;

        AckingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sentCount++;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            int sent = sentCount;
            if (sent <= ackedUpTo) {
                return false;
            }
            ackedUpTo = sent;
            // STATUS_OK frame: status(1) + sequence(8) + tableCount(2)
            int size = 11;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_OK);
                Unsafe.getUnsafe().putLong(ptr + 1, sent - 1); // cumulative wire seq
                Unsafe.getUnsafe().putShort(ptr + 9, (short) 0);
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

    private static void appendFrame(CursorSendEngine engine, boolean defer) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) 'x');
            }
            // The recovery walk only classifies frames that positively parse
            // as QWP messages, so write the real magic + flags byte.
            Unsafe.getUnsafe().putInt(buf, MAGIC_MESSAGE);
            Unsafe.getUnsafe().putByte(buf + HEADER_OFFSET_FLAGS,
                    (byte) (defer ? FLAG_DEFER_COMMIT : 0));
            engine.appendBlocking(buf, 16);
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void awaitAckedFsn(CursorSendEngine engine, long target) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (engine.ackedFsn() < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for ackedFsn", target, engine.ackedFsn());
            }
            Thread.sleep(1);
        }
    }

    private static void awaitClientCount(List<AckingClient> clients, int target) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (clientCount(clients) < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for reconnect", target, clientCount(clients));
            }
            Thread.sleep(1);
        }
    }

    private static int clientCount(List<AckingClient> clients) {
        synchronized (clients) {
            return clients.size();
        }
    }

    private static int totalSent(List<AckingClient> clients) {
        synchronized (clients) {
            int total = 0;
            for (AckingClient c : clients) {
                total += c.sentCount;
            }
            return total;
        }
    }

    private CursorSendEngine newEngine() {
        return new CursorSendEngine(tmpDir, 16384);
    }

    private CursorWebSocketSendLoop newLoop(CursorSendEngine engine, List<AckingClient> clients) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    AckingClient c = new AckingClient();
                    synchronized (clients) {
                        clients.add(c);
                    }
                    return c;
                },
                5_000L, 1L, 5L);
    }
}
