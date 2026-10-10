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
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLockContentionException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The {@code senderId} contract says the slot is locked for the sender's
 * lifetime and a second sender with a colliding id fails fast. The
 * symbol-dictionary recycle closes and rebuilds the engine on that slot;
 * these tests pin that the slot stays owned through the swap -- across a
 * clean rebuild, an abandoned one, a terminal one, and the sender's close --
 * by attempting a colliding {@code Sender.fromConfig} at the rebuild
 * boundary through the real builder's factory.
 */
public class SymbolDictRecycleSlotOwnershipTest {

    private static final long SEGMENT_BYTES = 1024L * 1024L;
    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * A colliding build between the outgoing close and the rebuild must fail
     * with slot contention, and the recycle must then complete on the
     * incumbent.
     */
    @Test(timeout = 60_000L)
    public void testCollidingBuildDuringRecycleIsRefusedAndTheRecycleCompletes() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = config(server);
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    assertCollidingBuildRefused(cfg, "against a live engine");

                    AtomicReference<Throwable> collision = new AtomicReference<>();
                    QwpWebSocketSender.EngineRebuildFactory original = ws.getEngineRebuildFactoryForTesting();
                    ws.setEngineRebuildFactory(new DelegatingFactory(original) {
                        @Override
                        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
                            collision.set(attemptCollidingBuild(cfg));
                            return original.rebuild(liveHandler);
                        }
                    });
                    sender.resetSymbolDictionary();
                    sender.table("t"); // drained barrier: the recycle runs inside this call
                    Assert.assertEquals(1L, ws.getSymbolDictEpoch());
                    assertIsSlotContention("between the outgoing close and the rebuild", collision.get());

                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertTrue("the incumbent keeps ingesting on the fresh epoch",
                            sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                }
            }
        });
    }

    /**
     * A transient rebuild failure abandons the recycle with REBUILD pending.
     * The slot must stay owned across that abandon, and the next row start
     * must complete the swap.
     */
    @Test(timeout = 60_000L)
    public void testCollidingBuildIsRefusedWhileARebuildIsPendingAndTheResumeCompletes() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = config(server);
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

                    QwpWebSocketSender.EngineRebuildFactory original = ws.getEngineRebuildFactoryForTesting();
                    ws.setEngineRebuildFactory(new DelegatingFactory(original) {
                        private boolean failedOnce;

                        @Override
                        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
                            if (!failedOnce) {
                                failedOnce = true;
                                throw new IllegalStateException("simulated transient rebuild fault");
                            }
                            return original.rebuild(liveHandler);
                        }
                    });
                    sender.resetSymbolDictionary();
                    try {
                        sender.table("t");
                        Assert.fail("the faulted rebuild must abandon the recycle");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage(),
                                expected.getMessage().contains("could not rebuild its engine"));
                    }
                    Assert.assertEquals("abandoned, not committed", 0L, ws.getSymbolDictEpoch());

                    assertCollidingBuildRefused(cfg, "while the rebuild is pending");

                    sender.table("t"); // REBUILD resume: rebuilds and commits
                    Assert.assertEquals(1L, ws.getSymbolDictEpoch());
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                }
            }
        });
    }

    /**
     * Closing a sender whose recycle is stuck at REBUILD must release the
     * logical lock it holds, or a successor on the same slot contends with a
     * lock leaked by its own process.
     */
    @Test(timeout = 60_000L)
    public void testCloseReleasesTheLogicalLockHeldByAPendingRecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = config(server);
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

                    QwpWebSocketSender.EngineRebuildFactory original = ws.getEngineRebuildFactoryForTesting();
                    ws.setEngineRebuildFactory(new DelegatingFactory(original) {
                        @Override
                        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
                            throw new IllegalStateException("simulated persistent rebuild fault");
                        }
                    });
                    sender.resetSymbolDictionary();
                    try {
                        sender.table("t");
                        Assert.fail("the faulted rebuild must abandon the recycle");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage(),
                                expected.getMessage().contains("could not rebuild its engine"));
                    }
                    assertCollidingBuildRefused(cfg, "while the rebuild is pending");
                }
                // The incumbent is closed with REBUILD pending: the slot is free again.
                try (Sender successor = Sender.fromConfig(cfg)) {
                    successor.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertTrue("a successor must own the slot after the incumbent's close",
                            successor.awaitAckedFsn(successor.flushAndGetSequence(), 5_000));
                }
            }
        });
    }

    /**
     * The breach latch (a rebuild that recovers unacknowledged frames) is
     * terminal for the sender but must not hold the slot hostage: a
     * successor on the slot recovers those frames.
     */
    @Test(timeout = 60_000L)
    public void testBreachLatchReleasesTheLogicalLock() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                String sfDir = temporaryFolder.newFolder("sf").getAbsolutePath();
                // sf_max_segment_bytes matches SymbolDictRecycleSlotHealTest.SEGMENT_BYTES, the
                // size the planting helper writes the leftover segment with.
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir
                        + ";sender_id=same_identity;drain_orphans=off;sf_max_segment_bytes=1048576;";
                String slotDir = sfDir + "/same_identity";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

                    QwpWebSocketSender.EngineRebuildFactory original = ws.getEngineRebuildFactoryForTesting();
                    ws.setEngineRebuildFactory(new DelegatingFactory(original) {
                        @Override
                        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
                            // The outgoing close emptied the slot; plant an unacked frame the
                            // way a breach of the fully-drained contract would leave one.
                            SymbolDictRecycleSlotHealTest.prepareUnackedLeftoverSlot(slotDir);
                            return original.rebuild(liveHandler);
                        }
                    });
                    sender.resetSymbolDictionary();
                    try {
                        sender.table("t");
                        Assert.fail("recovered unacked frames must latch the recycle terminal");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage(),
                                expected.getMessage().contains("unacknowledged"));
                    }
                    // Terminal sender still open: the slot must already be free.
                    try (Sender successor = Sender.fromConfig(cfg)) {
                        successor.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                        Assert.assertTrue("a successor must own the slot after the breach latch",
                                successor.awaitAckedFsn(successor.flushAndGetSequence(), 5_000));
                    }
                }
            }
        });
    }

    /**
     * A contention on the step-0 lock acquisition (a sibling's orphan drainer
     * holding the logical lock for microseconds) costs the producer nothing:
     * rows keep flowing, the outgoing engine stays attached, the arming stays,
     * and the first drained barrier after the lock frees takes it and
     * recycles. Distinguishes "deferred before teardown" from "torn down,
     * then resumed": a resume would leave the engine null.
     */
    @Test(timeout = 60_000L)
    public void testLockContentionBeforeTeardownDefersTheRecycleWithNothingTornDown() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = config(server);
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                    CursorSendEngine outgoing = ws.getCursorEngineForTesting();
                    String slotDir = outgoing.sfDir();

                    sender.resetSymbolDictionary();
                    // Hold the slot's logical lock from this thread, the way a sibling's
                    // startup orphan drainer does for a few microseconds: flock treats a
                    // second descriptor in the same process as another holder.
                    try (SlotLock heldElsewhere = SlotLock.acquireLogical(slotDir)) {
                        Assert.assertNotNull(heldElsewhere);
                        for (int i = 0; i < 3; i++) {
                            // Each row start is a drained barrier that finds the lock contended.
                            sender.table("t").symbol("s", "c" + i).longColumn("v", i).atNow();
                            Assert.assertTrue("a contended step-0 acquisition must not cost a row",
                                    sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                        }
                        Assert.assertEquals("nothing swapped", 0L, ws.getSymbolDictEpoch());
                        Assert.assertTrue("still armed", ws.isResetArmed());
                        Assert.assertSame("nothing torn down: the outgoing engine is still attached",
                                outgoing, ws.getCursorEngineForTesting());
                    }

                    sender.table("t"); // drained barrier again: the lock is free this time
                    Assert.assertEquals(1L, ws.getSymbolDictEpoch());
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                }
            }
        });
    }

    /**
     * A {@code connect()}-built sender whose slot is a bare relative name has
     * no parent directory to anchor the logical lock in, so its recycle can
     * never own the slot across the swap. It must not refuse rows over that:
     * the swap is skipped, the sender stops arming, and ingestion continues.
     */
    @Test(timeout = 60_000L)
    public void testBareRelativeSlotStopsArmingAndKeepsIngesting() throws Exception {
        // Resolved against the forked test JVM's working directory.
        String slot = "sf-unlockable-bare-" + System.nanoTime();
        try {
            assertMemoryLeak(() -> {
                try (TestWebSocketServer server = ackingServer()) {
                    assertUnlockableSlotKeepsIngesting(server, slot);
                }
            });
        } finally {
            deleteRecursively(Paths.get(slot));
        }
    }

    /**
     * The same for a slot whose lock directory cannot exist: a regular file
     * already sits where {@code .slot-locks} would go. Portable stand-in for
     * a parent the process cannot write.
     */
    @Test(timeout = 60_000L)
    public void testSlotWhoseLockDirCannotExistStopsArmingAndKeepsIngesting() throws Exception {
        assertMemoryLeak(() -> {
            File parent = temporaryFolder.newFolder("blocked-parent");
            Assert.assertTrue(new File(parent, ".slot-locks").createNewFile());
            try (TestWebSocketServer server = ackingServer()) {
                assertUnlockableSlotKeepsIngesting(server, new File(parent, "slot").getAbsolutePath());
            }
        });
    }

    /**
     * The same for a slot under a parent the process cannot write, with no
     * {@code .slot-locks} directory in it yet. Skipped where directory
     * permissions are not enforced (Windows, or running as root).
     */
    @Test(timeout = 60_000L)
    public void testSlotUnderReadOnlyParentStopsArmingAndKeepsIngesting() throws Exception {
        File parent = temporaryFolder.newFolder("ro-parent");
        File slot = new File(parent, "slot");
        Assert.assertTrue(slot.mkdir());
        parent.setWritable(false, false);
        try {
            Assume.assumeFalse("directory permissions are not enforced here",
                    new File(parent, "probe").mkdir());
            assertMemoryLeak(() -> {
                try (TestWebSocketServer server = ackingServer()) {
                    assertUnlockableSlotKeepsIngesting(server, slot.getAbsolutePath());
                }
            });
        } finally {
            parent.setWritable(true, false); // the TemporaryFolder rule must be able to delete the tree
        }
    }

    private static Throwable attemptCollidingBuild(String cfg) {
        try (Sender unexpected = Sender.fromConfig(cfg)) {
            unexpected.table("t").symbol("s", "x").longColumn("v", 0L).atNow();
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    private static void assertCollidingBuildRefused(String cfg, String when) {
        assertIsSlotContention(when, attemptCollidingBuild(cfg));
    }

    private static void assertIsSlotContention(String when, Throwable t) {
        Assert.assertNotNull("a colliding build must be refused " + when, t);
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof SlotLockContentionException) {
                return;
            }
        }
        throw new AssertionError("a colliding build " + when + " must fail with slot contention, got: " + t, t);
    }

    private static void assertUnlockableSlotKeepsIngesting(TestWebSocketServer server, String slot) throws Exception {
        CursorSendEngine engine = new CursorSendEngine(slot, SEGMENT_BYTES);
        try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                "localhost", server.getPort(), null, 0, 0, 0L, null, false, engine)) {
            sender.setEngineRebuildFactory(() -> new CursorSendEngine(slot, SEGMENT_BYTES));
            sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
            Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

            sender.resetSymbolDictionary();
            Assert.assertTrue("precondition: an owned engine plus a rebuild factory arms on request",
                    sender.isResetArmed());
            sender.table("t"); // drained barrier on a live link: step 0 runs and cannot take the lock
            Assert.assertFalse("the barrier itself must disarm a sender that cannot lock its slot",
                    sender.isResetArmed());
            Assert.assertEquals("no swap without the slot's logical lock", 0L, sender.getSymbolDictEpoch());
            Assert.assertSame("nothing torn down", engine, sender.getCursorEngineForTesting());

            for (int i = 0; i < 20; i++) {
                sender.table("t").symbol("s", "b" + i).longColumn("v", i).atNow();
                Assert.assertTrue("row " + i + " must be accepted and acked",
                        sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
            }
            Assert.assertEquals(0L, sender.getSymbolDictEpoch());
            Assert.assertFalse(sender.isResetArmed());

            sender.resetSymbolDictionary();
            Assert.assertFalse("a later request must not re-arm it", sender.isResetArmed());
            sender.table("t").symbol("s", "z").longColumn("v", 99L).atNow();
            Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!java.nio.file.Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = java.nio.file.Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static TestWebSocketServer ackingServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new PerConnectionAckHandler());
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private String config(TestWebSocketServer server) throws IOException {
        return "ws::addr=localhost:" + server.getPort()
                + ";sf_dir=" + temporaryFolder.newFolder("sf").getAbsolutePath()
                + ";sender_id=same_identity;drain_orphans=off;";
    }

    /** Forwards every factory method to the real builder's factory; tests override one. */
    private static class DelegatingFactory implements QwpWebSocketSender.EngineRebuildFactory {
        final QwpWebSocketSender.EngineRebuildFactory original;

        DelegatingFactory(QwpWebSocketSender.EngineRebuildFactory original) {
            this.original = original;
        }

        @Override
        public CursorSendEngine rebuild() {
            return original.rebuild();
        }

        @Override
        public CursorSendEngine rebuild(SenderErrorHandler liveHandler) {
            return original.rebuild(liveHandler);
        }
    }

    /**
     * Acks every frame, numbering per connection: each connection's wire
     * sequence restarts at 0, so a second sender or a post-recycle connection
     * is never over-acked into a synthetic drain.
     */
    private static class PerConnectionAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final Map<TestWebSocketServer.ClientHandler, Long> nextSeq = new IdentityHashMap<>();

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            Long n = nextSeq.get(client);
            long seq = n == null ? 0L : n;
            nextSeq.put(client, seq + 1L);
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(seq));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
