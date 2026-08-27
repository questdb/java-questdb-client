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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The two verdicts {@code QwpWebSocketSender.completeRecycleRebuild} reaches
 * when the recycle's step-5 rebuild comes back
 * {@code wasRecoveredFromDisk()} -- i.e. when the outgoing engine's
 * fully-drained close did NOT leave the slot empty.
 * <ul>
 *   <li>Benign: the leftovers are fully acked. A close-time segment unlink
 *       failed transiently, so the watermark stayed behind to cover the
 *       residue by design (see {@code CursorSendEngineCloseUnlinkFailureTest})
 *       and the SF contract is that the NEXT engine on the slot recovers them
 *       as acked and retries the unlink on its own close. The recycle must
 *       HEAL -- close the recovered engine, rebuild once more -- not brick.</li>
 *   <li>Breach: the leftovers hold UNACKED frames. The everything-acked
 *       barrier the swap rests on was violated, so the fresh producer
 *       dictionary and the slot's on-disk state have genuinely diverged. This
 *       is the recycle's one surviving terminal latch.</li>
 * </ul>
 * Both tests doctor a slot directly at the engine level and then point a live
 * sender's rebuild factory at it, so the verdict is driven by real on-disk
 * state rather than by a mocked engine.
 * <p>
 * The acked-leftover recipe injects the unlink failure the way
 * {@code CursorSendEngineCloseUnlinkFailureTest} does: it drops write
 * permission on the slot directory (POSIX unlink needs a writable parent), so
 * that test skips on Windows and wherever permissions are not enforced (root).
 */
public class SymbolDictRecycleSlotHealTest {

    /**
     * Big enough for the real QWP frames the sender appends to the rebuilt
     * engine after the heal, and identical in the prep helpers so recovery
     * reads the doctored segments back at the size they were written with.
     */
    private static final long SEGMENT_BYTES = 1024L * 1024L;
    private static final int PAYLOAD_BYTES = 32;

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * Review r3, C2(a): a benign fully-drained close verdict (segment unlink
     * transiently failed; watermark retained by design) must not brick the
     * recycle. The rebuild recovers fully-acked leftovers; the sender heals by
     * closing the recovered engine (which retries the unlink) and rebuilding
     * once more.
     */
    @Test(timeout = 60_000L)
    public void testRecoveredFullyAckedLeftoversHealAndRecycleCompletes() throws Exception {
        assertMemoryLeak(() -> {
            // Phase 1: doctor a slot -- fully-acked frames whose close-time
            // unlink failed (CursorSendEngineCloseUnlinkFailureTest's recipe).
            String doctoredSlot = temporaryFolder.getRoot().toPath()
                    .resolve("doctored-slot").toString();
            prepareFullyAckedLeftoverSlot(doctoredSlot);

            // Phase 2: a live sender whose rebuild factory lands on that slot.
            String sfDir = temporaryFolder.getRoot().toPath().resolve("heal-sf").toString();
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(config(server, sfDir))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));

                    AtomicInteger rebuilds = new AtomicInteger();
                    ws.setEngineRebuildFactory(() -> {
                        rebuilds.incrementAndGet();
                        return new CursorSendEngine(doctoredSlot, SEGMENT_BYTES);
                    });

                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());
                    // Recycle: rebuild #1 recovers the acked leftovers -> heal
                    // -> rebuild #2 stands on a genuinely empty slot.
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();

                    Assert.assertEquals("heal must close the recovered engine and rebuild again",
                            2, rebuilds.get());
                    Assert.assertEquals("the recycle must have committed",
                            1, ws.getSymbolDictEpoch());
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    // The heal's close retried the unlink the outgoing close
                    // could not do, so the engine the swap committed on stands
                    // on a genuinely emptied slot -- not on the leftovers.
                    Assert.assertFalse("the recycle must commit on a non-recovered engine",
                            ws.getCursorEngineForTesting().wasRecoveredFromDisk());

                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-heal batch must still get acked",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN "
                            + "[fsn1=" + fsn1 + ", fsn2=" + fsn2 + ']', fsn2 > fsn1);
                }
            }
        });
    }

    /**
     * The heal closes the engine that recovered the leftovers. When that engine's
     * SF worker is wedged, its close returns with the slot flock retained, exactly
     * like the outgoing engine's close in step 3 -- and must be awaited the same
     * way, or rebuild #2 collides with the retained flock.
     */
    @Test(timeout = 60_000L)
    public void testHealRidesOutADeferredCloseOfTheRecoveredEngine() throws Exception {
        assertMemoryLeak(() -> {
            String doctoredSlot = temporaryFolder.getRoot().toPath().resolve("doctored-deferred").toString();
            prepareFullyAckedLeftoverSlot(doctoredSlot);
            String sfDir = temporaryFolder.getRoot().toPath().resolve("heal-deferred-sf").toString();
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicReference<Throwable> auxErr = new AtomicReference<>();
            Thread releaser = null;
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(config(server, sfDir))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

                    AtomicInteger rebuilds = new AtomicInteger();
                    ws.setEngineRebuildFactory(() -> {
                        CursorSendEngine engine = new CursorSendEngine(doctoredSlot, SEGMENT_BYTES);
                        if (rebuilds.incrementAndGet() == 1) {
                            // Wedge the RECOVERED engine's worker so the heal's close defers.
                            SegmentManager manager = engine.getManagerForTesting();
                            manager.setBeforeTrimSyncHook(() -> {
                                workerBlocked.countDown();
                                try {
                                    if (!releaseWorker.await(30, TimeUnit.SECONDS)) {
                                        auxErr.compareAndSet(null, new AssertionError("worker never released"));
                                    }
                                } catch (Throwable t) {
                                    auxErr.compareAndSet(null, t);
                                }
                            });
                            manager.wakeWorker();
                            try {
                                Assert.assertTrue("worker never reached the wedge hook",
                                        workerBlocked.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            manager.setWorkerJoinTimeoutMillis(50L);
                        }
                        return engine;
                    });

                    CountDownLatch parked = new CountDownLatch(1);
                    ws.setDeferredCloseParkWitnessForTesting(parked::countDown);
                    releaser = new Thread(() -> {
                        try {
                            Assert.assertTrue("the heal's close must park in the deferred-close await",
                                    parked.await(10, TimeUnit.SECONDS));
                        } catch (Throwable t) {
                            auxErr.compareAndSet(null, t);
                        } finally {
                            releaseWorker.countDown();
                        }
                    }, "heal-deferred-close-releaser");
                    releaser.start();

                    sender.resetSymbolDictionary();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();

                    Assert.assertEquals("heal must close the recovered engine and rebuild again", 2, rebuilds.get());
                    Assert.assertEquals("the recycle must have committed", 1, ws.getSymbolDictEpoch());
                    Assert.assertFalse(ws.getCursorEngineForTesting().wasRecoveredFromDisk());
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                } finally {
                    releaseWorker.countDown();
                    if (releaser != null) {
                        releaser.join(10_000);
                    }
                }
            }
            if (auxErr.get() != null) {
                throw new AssertionError(auxErr.get());
            }
        });
    }

    /**
     * When the recovered engine's deferred close outlives the await budget, the
     * recycle must retain that engine exactly like an outgoing engine: close()
     * must not report the slot flock released while the wedged worker still
     * holds it, and the re-probe must latch once the worker exits.
     */
    @Test(timeout = 60_000L)
    public void testHealDeferredCloseExhaustionRetainsTheRecoveredEngine() throws Exception {
        assertMemoryLeak(() -> {
            String doctoredSlot = temporaryFolder.getRoot().toPath().resolve("doctored-retained").toString();
            prepareFullyAckedLeftoverSlot(doctoredSlot);
            String sfDir = temporaryFolder.getRoot().toPath().resolve("heal-retained-sf").toString();
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicReference<Throwable> auxErr = new AtomicReference<>();
            try (TestWebSocketServer server = ackingServer()) {
                QwpWebSocketSender ws = (QwpWebSocketSender) Sender.fromConfig(config(server, sfDir));
                try {
                    ws.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(ws.awaitAckedFsn(ws.flushAndGetSequence(), 5_000));

                    ws.setEngineRebuildFactory(() -> {
                        CursorSendEngine engine = new CursorSendEngine(doctoredSlot, SEGMENT_BYTES);
                        SegmentManager manager = engine.getManagerForTesting();
                        manager.setBeforeTrimSyncHook(() -> {
                            workerBlocked.countDown();
                            try {
                                if (!releaseWorker.await(30, TimeUnit.SECONDS)) {
                                    auxErr.compareAndSet(null, new AssertionError("worker never released"));
                                }
                            } catch (Throwable t) {
                                auxErr.compareAndSet(null, t);
                            }
                        });
                        manager.wakeWorker();
                        try {
                            Assert.assertTrue("worker never reached the wedge hook",
                                    workerBlocked.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        manager.setWorkerJoinTimeoutMillis(50L);
                        return engine;
                    });
                    ws.setRecycleDeferredCloseMaxWaitMillisForTesting(150L);

                    ws.resetSymbolDictionary();
                    try {
                        ws.table("t");
                        Assert.fail("the exhausted await must throw a resumable failure");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage(),
                                expected.getMessage().contains("could not yet reclaim its slot"));
                    }
                    Assert.assertEquals("the swap must not have committed", 0, ws.getSymbolDictEpoch());
                } finally {
                    ws.close();
                }
                Assert.assertFalse("close() must not report the flock released while the recovered "
                        + "engine's wedged worker still holds it", ws.isSlotLockReleased());

                releaseWorker.countDown();
                long deadline = System.currentTimeMillis() + 10_000;
                while (!ws.isSlotLockReleased() && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10);
                }
                Assert.assertTrue("the re-probe must latch once the worker exits", ws.isSlotLockReleased());
            }
            if (auxErr.get() != null) {
                throw new AssertionError(auxErr.get());
            }
        });
    }

    /**
     * Review r3: the terminal latch's one surviving case. A rebuild that
     * recovers UNACKED frames proves the fully-drained-close contract was
     * breached -- the fresh producer dictionary and the slot's state have
     * genuinely diverged, so the sender must refuse further use ({@code close()}
     * still works).
     */
    @Test(timeout = 60_000L)
    public void testRecoveredUnackedFramesLatchTerminal() throws Exception {
        assertMemoryLeak(() -> {
            String doctoredSlot = temporaryFolder.getRoot().toPath()
                    .resolve("breach-slot").toString();
            prepareUnackedLeftoverSlot(doctoredSlot);

            String sfDir = temporaryFolder.getRoot().toPath().resolve("breach-sf").toString();
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(config(server, sfDir))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));

                    ws.setEngineRebuildFactory(() -> new CursorSendEngine(doctoredSlot, SEGMENT_BYTES));
                    sender.resetSymbolDictionary();
                    try {
                        sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                        Assert.fail("a breached slot must latch terminal");
                    } catch (LineSenderException expected) {
                        TestUtils.assertContains(expected.getMessage(), "unacknowledged");
                    }
                    Assert.assertEquals("the swap must not have committed",
                            0, ws.getSymbolDictEpoch());
                    try {
                        sender.flush();
                        Assert.fail("the latch must gate every later call");
                    } catch (LineSenderException expected) {
                        TestUtils.assertContains(expected.getMessage(),
                                "sender is terminal: symbol dictionary recycle failed");
                    }
                    try {
                        sender.table("t");
                        Assert.fail("the latch must gate every later call");
                    } catch (LineSenderException expected) {
                        TestUtils.assertContains(expected.getMessage(),
                                "sender is terminal: symbol dictionary recycle failed");
                    }
                    sender.close(); // must still work
                }
            }
        });
    }

    private static TestWebSocketServer ackingServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler());
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static String config(TestWebSocketServer server, String sfDir) {
        return "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
    }

    private static void fill(long address, int len, byte value) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, value);
        }
    }

    /**
     * Leaves {@code slot} holding one segment whose only frame the server
     * already acknowledged, plus the ack watermark that covers it -- the exact
     * residue a fully-drained close produces when its segment unlink fails
     * transiently. Follows {@code CursorSendEngineCloseUnlinkFailureTest}: a
     * shared {@link SegmentManager} that is deliberately never started (no
     * worker thread can persist the watermark or trim behind the test's back,
     * and the close-path quiescence barrier is trivially satisfied), and the
     * unlink failure injected by dropping write permission on the slot dir.
     * Restores the permissions before returning, so the successor engine the
     * sender's rebuild factory constructs can heal the slot.
     */
    private static void prepareFullyAckedLeftoverSlot(String slot) throws Exception {
        Path slotPath = Paths.get(slot);
        long payload = Unsafe.malloc(PAYLOAD_BYTES, MemoryTag.NATIVE_DEFAULT);
        SegmentManager manager = new SegmentManager(SEGMENT_BYTES, TimeUnit.SECONDS.toNanos(60));
        CursorSendEngine pred = null;
        boolean slotDirReadOnly = false;
        try {
            fill(payload, PAYLOAD_BYTES, (byte) 0x33);
            pred = new CursorSendEngine(slot, SEGMENT_BYTES, manager);
            Assert.assertEquals(0L, pred.appendBlocking(payload, PAYLOAD_BYTES));
            Assert.assertEquals(0L, pred.publishedFsn());
            // The server durably acknowledged FSN 0 in this session.
            Assert.assertTrue(pred.acknowledge(0L));
            Assert.assertEquals(0L, pred.ackedFsn());

            // Inject the close-time unlink failure, and prove the injection
            // works with a probe file -- root (and some filesystems) ignore
            // directory permissions.
            String probePath = slot + "/probe";
            Assert.assertTrue(java.nio.file.Files.exists(
                    java.nio.file.Files.createFile(Paths.get(probePath))));
            try {
                setPermissions(slotPath, "r-xr-xr-x");
            } catch (UnsupportedOperationException e) {
                Assume.assumeNoException("POSIX permissions unavailable on this platform", e);
            }
            slotDirReadOnly = true;
            boolean probeRemoved = Files.remove(probePath);
            if (probeRemoved) {
                setPermissions(slotPath, "rwxr-xr-x");
                slotDirReadOnly = false;
            }
            Assume.assumeFalse("directory permissions not enforced (running as root?)",
                    probeRemoved);

            // Fully-drained close: tries to unlink the acknowledged segment
            // file and fails, so the watermark stays behind to cover it.
            pred.close();
            Assert.assertTrue("flock release needs no dir write; close must complete",
                    pred.isCloseCompleted());
            pred = null;

            // The transient failure clears before the sender's rebuild arrives.
            setPermissions(slotPath, "rwxr-xr-x");
            slotDirReadOnly = false;
            Files.remove(probePath);

            Assert.assertTrue("prep: the injected unlink failure must leave the acked segment",
                    Files.exists(slot + "/sf-initial.sfa"));
            Assert.assertTrue("prep: the watermark must be retained to cover the residue",
                    Files.exists(slot + "/" + AckWatermark.FILE_NAME));
        } finally {
            if (slotDirReadOnly) {
                try {
                    setPermissions(slotPath, "rwxr-xr-x");
                } catch (Throwable ignored) {
                }
            }
            if (pred != null) {
                pred.close();
            }
            manager.close();
            Unsafe.free(payload, PAYLOAD_BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Leaves {@code slot} holding one published-but-never-acknowledged frame.
     * A close that is not fully drained retains the segment files by design
     * (they still have to reach the server), so the successor recovers them
     * with {@code publishedFsn > ackedFsn} -- the breach signature. Same
     * never-started shared {@link SegmentManager} as the acked variant.
     */
    private static void prepareUnackedLeftoverSlot(String slot) {
        long payload = Unsafe.malloc(PAYLOAD_BYTES, MemoryTag.NATIVE_DEFAULT);
        SegmentManager manager = new SegmentManager(SEGMENT_BYTES, TimeUnit.SECONDS.toNanos(60));
        CursorSendEngine pred = null;
        try {
            fill(payload, PAYLOAD_BYTES, (byte) 0x44);
            pred = new CursorSendEngine(slot, SEGMENT_BYTES, manager);
            Assert.assertEquals(0L, pred.appendBlocking(payload, PAYLOAD_BYTES));
            Assert.assertEquals(0L, pred.publishedFsn());
            Assert.assertTrue("prep: the frame must stay unacknowledged",
                    pred.ackedFsn() < pred.publishedFsn());
            pred.close();
            Assert.assertTrue(pred.isCloseCompleted());
            pred = null;
            Assert.assertTrue("prep: an unacknowledged frame must survive the close",
                    Files.exists(slot + "/sf-initial.sfa"));
        } finally {
            if (pred != null) {
                pred.close();
            }
            manager.close();
            Unsafe.free(payload, PAYLOAD_BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void setPermissions(Path path, String posix) throws Exception {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString(posix);
        java.nio.file.Files.setPosixFilePermissions(path, perms);
    }

    /** ACKs every frame it receives; does not otherwise inspect the wire. */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
