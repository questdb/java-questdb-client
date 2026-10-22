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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Every state change an {@code awaitAckedFsn} / {@code drain} waiter re-checks
 * must unpark it. Two of them bypass the I/O loop's ack-progress listener:
 * the orphan-tail retirement self-ack and the periodic-sync durability latch.
 * Without a wake, the waiter only notices at its deadline.
 */
public class AckWaiterSignalTest {
    private static final long SEGMENT_SIZE = 64 * 1024L;

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * A store-and-forward producer dies mid-transaction: two committed frames
     * (FSN 0, 1) and two FLAG_DEFER_COMMIT frames (FSN 2, 3) whose commit never
     * went out. The successor recovers, replays 0 and 1, and once both are
     * acked retires the orphan tail with a cumulative self-acknowledge of 3 on
     * the I/O thread. {@code drain()} targets publishedFsn = 3, so that
     * self-ack is exactly what it waits for.
     */
    @Test
    public void testDrainWakesOnOrphanTailRetirement() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("orphan-tail").toString();
            String opts = ";sf_dir=" + sfDir + ";close_flush_timeout_millis=0;";
            try (TestWebSocketServer silent = startedServer(new TestWebSocketServer.WebSocketServerHandler() {
            })) {
                // Defer-commit mode leaves the tail uncommitted through close(), and
                // close_flush_timeout_millis=0 against a never-acking server does not
                // drain, so the segment and manifest survive exactly as a crash leaves them.
                try (Sender s = Sender.fromConfig("ws::addr=localhost:" + silent.getPort() + opts)) {
                    s.table("t").longColumn("v", 1).atNow();
                    s.flush(); // FSN 0, commit-bearing
                    s.table("t").longColumn("v", 2).atNow();
                    s.flush(); // FSN 1, commit-bearing
                    ((QwpWebSocketSender) s).setDeferCommit(true);
                    s.table("t").longColumn("v", 3).atNow();
                    s.flush(); // FSN 2, deferred
                    s.table("t").longColumn("v", 4).atNow();
                    s.flush(); // FSN 3, deferred; the commit never goes out
                }
            }
            try (TestWebSocketServer acking = startedServer(new DelayingAckHandler(100))) {
                try (Sender s = Sender.fromConfig("ws::addr=localhost:" + acking.getPort() + opts)) {
                    CursorSendEngine recovered = ((QwpWebSocketSender) s).getCursorEngineForTesting();
                    Assert.assertEquals("the recovered log must end in the orphan tail [2..3]",
                            3L, recovered.recoveredOrphanTipFsn());
                    Assert.assertEquals(1L, recovered.recoveredCommitBoundaryFsn());
                    long t0 = System.nanoTime();
                    boolean drained = s.drain(3000);
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                    Assert.assertTrue("drain must reach the retired orphan tip", drained);
                    Assert.assertEquals(3L, s.getAckedFsn());
                    Assert.assertTrue("drain returned only after " + elapsedMs + "ms: the orphan-tail "
                            + "self-ack did not wake the waiter", elapsedMs < 1000L);
                }
            }
        });
    }

    /**
     * sf_durability=periodic: the segment manager's sync pass hits an fsync
     * failure and latches it on the ring. A thread parked in awaitAckedFsn
     * must see the MmapSegmentException on the latch, not at its deadline.
     */
    @Test
    public void testAwaitAckedFsnWakesOnPeriodicSyncDurabilityFailure() throws Exception {
        assertMemoryLeak(() -> {
            AtomicBoolean failFsync = new AtomicBoolean();
            FilesFacade ff = (FilesFacade) Proxy.newProxyInstance(FilesFacade.class.getClassLoader(),
                    new Class<?>[]{FilesFacade.class}, (p, m, a) -> {
                        if (m.getName().equals("fsync") && failFsync.get()) {
                            return -1; // EIO
                        }
                        return m.invoke(FilesFacade.INSTANCE, a);
                    });
            SegmentManager manager = new SegmentManager(SEGMENT_SIZE, SegmentManager.UNLIMITED_TOTAL_BYTES, ff);
            manager.start();
            CursorSendEngine engine = new CursorSendEngine(
                    temporaryFolder.newFolder().getAbsolutePath(), SEGMENT_SIZE, manager,
                    TimeUnit.MILLISECONDS.toNanos(100));
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 1);
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                sender.setCursorEngine(engine, false);
                AtomicReference<Throwable> outcome = new AtomicReference<>();
                AtomicLong doneAt = new AtomicLong();
                Thread waiter = new Thread(() -> {
                    try {
                        sender.awaitAckedFsn(0L, 3000);
                    } catch (Throwable t) {
                        outcome.set(t);
                    }
                    doneAt.set(System.nanoTime());
                }, "awaiter");
                waiter.start();
                Thread.sleep(200); // parked
                failFsync.set(true);
                engine.getRingForTesting().appendOrFsn(payload, 16); // dirties the active segment
                long triggerAt = System.nanoTime();
                waiter.join(5000);
                Assert.assertFalse("waiter stuck", waiter.isAlive());
                long elapsedMs = (doneAt.get() - triggerAt) / 1_000_000L;
                Assert.assertTrue("awaitAckedFsn must throw the latched durability failure, got: "
                        + outcome.get(), outcome.get() instanceof MmapSegmentException);
                Assert.assertTrue("the latch surfaced only after " + elapsedMs + "ms: it did not wake "
                        + "the waiter", elapsedMs < 500L);
            } finally {
                failFsync.set(false);
                sender.close();
                engine.close();
                manager.close();
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static TestWebSocketServer startedServer(TestWebSocketServer.WebSocketServerHandler handler)
            throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static class DelayingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong();

        DelayingAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                Thread.sleep(delayMs);
                client.sendBinary(CloseDrainTest.buildAck(nextSeq.getAndIncrement()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
