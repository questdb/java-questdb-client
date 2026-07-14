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

package io.questdb.client.test;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Proves the {@link io.questdb.client.QuestDBBuilder#drainerListener} and
 * {@link Sender.LineSenderBuilder#drainerListener} hooks actually reach the
 * background orphan-slot drainers, end-to-end against a real
 * {@link TestWebSocketServer} — and that the M10 stream split holds on the
 * wire: a durable-ack capability gap (server upgrades but withholds
 * {@code X-QWP-Durable-Ack}) lands on {@code onDurableAckUnavailable} while a
 * transient all-replica failover window (421 + {@code X-QuestDB-Role:
 * REPLICA}) lands on {@code onPrimaryUnavailable}, with the other stream
 * staying silent.
 * <p>
 * Fixture shape: an orphan slot is seeded under {@code sf_dir} with unacked
 * frames; the config enables {@code drain_orphans} and
 * {@code request_durable_ack=on}. The server starts in the failure condition
 * under test (durable-ack header suppressed, or role-rejecting), so the
 * drainer deterministically observes it — no race against the drainer's first
 * connect. Once the listener has recorded the scripted attempts, the server
 * "settles" (header restored / reject cleared) and the drain must run to
 * completion: no escalation, no {@code .failed} sentinel, slot emptied. The
 * foreground sender uses {@code initial_connect_retry=async} so build() never
 * blocks or fails on the same scripted condition.
 */
public class QuestDBFacadeDrainerListenerTest {

    private static final int SEEDED_FRAMES = 5;
    private static final long SEGMENT_SIZE_BYTES = 16384L;

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-facade-drainer-listener-" + System.nanoTime()).toString();
        Assert.assertEquals("mkdir sf_dir", 0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    /**
     * Facade plumbing E2E: the {@code QuestDB.builder().drainerListener(...)}
     * hook must observe the pooled senders' drainer events. The server
     * completes the WS upgrade WITHOUT advertising durable ack for the first
     * attempts (capability gap), then advertises it; the listener must see
     * {@code onDurableAckUnavailable} with attempts {@code 1..N} (one
     * uninterrupted episode) and the drain must then succeed.
     */
    @Test
    public void testFacadeDrainerListenerObservesCapabilityGapThenDrainSucceeds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedOrphanSlot("ghost");
            DurableAckAllHandler handler = new DurableAckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                // Deterministic capability gap: withheld BEFORE the first
                // drainer connect, restored only after the listener has
                // recorded the gap episode.
                server.setSuppressDurableAckHeader(true);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                RecordingDrainerListener listener = new RecordingDrainerListener();
                try (QuestDB ignored = QuestDB.builder()
                        .fromConfig(facadeConfig(server.getPort()))
                        .drainerListener(listener)
                        .build()) {
                    awaitTrue(10_000, () -> listener.daAttempts.size() >= 3,
                            "facade-wired drainer listener must observe the capability-gap "
                                    + "retries via onDurableAckUnavailable");
                    // Cluster "settles": the next sweep connects and drains.
                    server.setSuppressDurableAckHeader(false);
                    awaitDrainedSlot("ghost");
                }
                assertSingleGapEpisodeThenSilence(listener);
            }
        });
    }

    /**
     * Role-reject discrimination E2E: with every handshake answered by 421 +
     * {@code X-QuestDB-Role: REPLICA} (transient all-replica failover
     * window), the facade-wired listener must receive
     * {@code onPrimaryUnavailable} — and {@code onDurableAckUnavailable} must
     * stay SILENT for the whole window (the released 1.3.4 contract fed both
     * conditions to the DA callback; this pins the M10 split on the wire).
     */
    @Test
    public void testFacadeDrainerListenerDiscriminatesRoleRejectWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedOrphanSlot("ghost");
            DurableAckAllHandler handler = new DurableAckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                // Deterministic all-replica window: rejecting BEFORE the first
                // drainer connect; the durable-ack header is never withheld,
                // so no capability gap can ever fire in this test.
                server.setRejectWithRole("REPLICA");
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                RecordingDrainerListener listener = new RecordingDrainerListener();
                try (QuestDB ignored = QuestDB.builder()
                        .fromConfig(facadeConfig(server.getPort()))
                        .drainerListener(listener)
                        .build()) {
                    awaitTrue(10_000, () -> listener.primaryAttempts.size() >= 3,
                            "facade-wired drainer listener must observe the all-replica "
                                    + "window via onPrimaryUnavailable");
                    Assert.assertEquals("onDurableAckUnavailable must stay SILENT during a "
                                    + "role-reject window — that is the whole point of the M10 split",
                            0, listener.daAttempts.size());
                    // Primary reappears: the next sweep connects and drains.
                    server.setRejectWithRole(null);
                    awaitDrainedSlot("ghost");
                }
                // Post-close exact assertions on the complete stream.
                List<Integer> primary = listener.primaryAttemptsSnapshot();
                Assert.assertTrue("expected at least the awaited role-reject attempts, got "
                        + primary, primary.size() >= 3);
                for (int i = 0; i < primary.size(); i++) {
                    Assert.assertEquals("primary stream must be the uninterrupted 1-based "
                                    + "role-reject count, got " + primary,
                            Integer.valueOf(i + 1), primary.get(i));
                }
                Assert.assertEquals("no capability gap ever existed: the DA stream must be "
                                + "empty end-to-end", 0, listener.daAttempts.size());
                Assert.assertEquals("a role-reject window must NEVER escalate (Invariant B)",
                        0, listener.persistentFailures.get());
                Assert.assertFalse("no .failed sentinel for a transient window",
                        Files.exists(sfDir + "/ghost/" + OrphanScanner.FAILED_SENTINEL_NAME));
            }
        });
    }

    /**
     * Same capability-gap scenario as the facade test, one level down through
     * {@code Sender.builder().drainerListener(...)} — pins the plumbing that
     * the pool path composes (builder field → {@code setDrainerListener} →
     * drainer pool → drainer), and awaits the drain outcome via the sender's
     * public drainer counters.
     */
    @Test
    public void testSenderBuilderDrainerListenerObservesCapabilityGapThenDrainSucceeds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedOrphanSlot("ghost");
            DurableAckAllHandler handler = new DurableAckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler, true)) {
                server.setSuppressDurableAckHeader(true);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + server.getPort()
                        + ";sf_dir=" + sfDir
                        + ";sender_id=primary"
                        + ";request_durable_ack=on"
                        + ";drain_orphans=true"
                        + ";max_background_drainers=1"
                        + ";initial_connect_retry=async"
                        + ";reconnect_initial_backoff_millis=25"
                        + ";reconnect_max_backoff_millis=200"
                        + ";close_flush_timeout_millis=0;";
                RecordingDrainerListener listener = new RecordingDrainerListener();
                Sender sender = Sender.builder(cfg)
                        .drainerListener(listener)
                        .build();
                try {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    awaitTrue(10_000, () -> listener.daAttempts.size() >= 3,
                            "builder-wired drainer listener must observe the capability-gap "
                                    + "retries via onDurableAckUnavailable");
                    server.setSuppressDurableAckHeader(false);
                    awaitTrue(15_000, () -> ws.getTotalBackgroundDrainersSucceeded() >= 1,
                            "drainer must drain the slot fully once the gap clears");
                } finally {
                    // The FOREGROUND sender's async initial connect hit the
                    // scripted capability gap and latched a terminal HALT
                    // before the server settled (durable ack is loud-fail for
                    // a foreground producer). close() completes its full
                    // teardown and then rethrows that latched terminal --
                    // expected here, and orthogonal to the drainer stream
                    // this test pins. The pool facade swallows the same
                    // rethrow in SenderPool.close(), which is why the facade
                    // tests use plain try-with-resources.
                    try {
                        sender.close();
                        Assert.fail("close() must loudly rethrow the foreground's "
                                + "latched capability-gap terminal");
                    } catch (io.questdb.client.cutlass.line.LineSenderException expected) {
                        Assert.assertTrue("expected the foreground durable-ack terminal, got: "
                                        + expected.getMessage(),
                                expected.getMessage().contains("durable-ack"));
                    }
                }
                assertSingleGapEpisodeThenSilence(listener);
            }
        });
    }

    // One cluster config drives the facade. sender_pool_min=1 eagerly prewarms
    // the one sender whose build() dispatches the orphan drainer;
    // query_pool_min=0 keeps the read pool out of the picture. async initial
    // connect: the foreground sender must not block or fail build() on the
    // very condition the drainer is scripted to observe. Small drainer
    // backoffs make the awaited attempts prompt while leaving plenty of
    // headroom under the 16-attempt capability-gap settle budget between
    // "third callback recorded" and "header restored".
    private String facadeConfig(int port) {
        return "ws::addr=localhost:" + port
                + ";sf_dir=" + sfDir
                + ";sender_id=pool"
                + ";request_durable_ack=on"
                + ";drain_orphans=true"
                + ";max_background_drainers=1"
                + ";sender_pool_min=1;sender_pool_max=1"
                + ";query_pool_min=0;query_pool_max=1"
                + ";initial_connect_retry=async"
                + ";reconnect_initial_backoff_millis=25"
                + ";reconnect_max_backoff_millis=200"
                + ";close_flush_timeout_millis=0;";
    }

    // The two capability-gap tests end the same way: one uninterrupted gap
    // episode numbered 1..K (no role reject ever intervened, so no reset and
    // no primary-stream traffic), then the drain succeeded without escalation.
    private void assertSingleGapEpisodeThenSilence(RecordingDrainerListener listener) {
        List<Integer> da = listener.daAttemptsSnapshot();
        Assert.assertTrue("expected at least the awaited gap attempts, got " + da,
                da.size() >= 3);
        for (int i = 0; i < da.size(); i++) {
            Assert.assertEquals("DA stream must be the 1-based attempt count of a single "
                            + "uninterrupted capability-gap episode, got " + da,
                    Integer.valueOf(i + 1), da.get(i));
        }
        Assert.assertEquals("expected slot path on every DA delivery",
                Collections.nCopies(da.size(), sfDir + "/ghost"), listener.daSlotPaths);
        Assert.assertEquals("no role reject was scripted: the primary stream must be empty",
                0, listener.primaryAttempts.size());
        Assert.assertEquals("the gap cleared inside the settle budget: no escalation",
                0, listener.persistentFailures.get());
        Assert.assertFalse("no .failed sentinel after a successful drain",
                Files.exists(sfDir + "/ghost/" + OrphanScanner.FAILED_SENTINEL_NAME));
    }

    // The drainer unlinks the slot's segment files once fully drained, so the
    // slot stops being a candidate orphan. Probed per-slot (not via a
    // whole-dir scan) because the foreground sender's own LIVE slot holds a
    // pre-created segment file for as long as the sender is up, so a
    // dir-level scan never reaches zero. A .failed sentinel would ALSO make
    // the slot a non-candidate, so the sentinel is asserted absent explicitly.
    private void awaitDrainedSlot(String slotName) throws InterruptedException {
        String slotPath = sfDir + "/" + slotName;
        awaitTrue(15_000, () -> !OrphanScanner.isCandidateOrphan(slotPath),
                "drainer must empty the seeded orphan slot once the server settles");
        Assert.assertFalse("slot must drain cleanly, not quarantine",
                Files.exists(slotPath + "/" + OrphanScanner.FAILED_SENTINEL_NAME));
    }

    private static void awaitTrue(long timeoutMillis, BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        Assert.assertTrue(message + " (timed out after " + timeoutMillis + "ms)",
                condition.getAsBoolean());
    }

    // Seeds <sfDir>/<slotName> with unacked frames — the on-disk shape a
    // crashed sender leaves behind (same recipe as
    // BackgroundDrainerMidDrainCapabilityGapTest). The engine creates the
    // slot dir itself; closing it with unacked data leaves the .sfa segments
    // in place, so the slot is a candidate orphan.
    private void seedOrphanSlot(String slotName) {
        String slotPath = sfDir + "/" + slotName;
        try (CursorSendEngine engine = new CursorSendEngine(slotPath, SEGMENT_SIZE_BYTES)) {
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                byte[] payload = "frame-bytes-padd".getBytes(StandardCharsets.US_ASCII);
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, payload[i]);
                }
                for (int i = 0; i < SEEDED_FRAMES; i++) {
                    engine.appendBlocking(buf, 16);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        }
        Assert.assertEquals("seeded slot must be a candidate orphan",
                1, OrphanScanner.scan(sfDir, "observer").size());
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) rmDirRec(child);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    /**
     * Thread-safe recording listener. Snapshot accessors copy under the same
     * monitor the callbacks append under, so end-of-test assertions never
     * observe a list mid-append.
     */
    private static final class RecordingDrainerListener implements BackgroundDrainerListener {
        final List<Integer> daAttempts = Collections.synchronizedList(new ArrayList<>());
        final List<String> daSlotPaths = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger persistentFailures = new AtomicInteger();
        final List<Integer> primaryAttempts = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
            persistentFailures.incrementAndGet();
        }

        @Override
        public void onDurableAckUnavailable(String slotPath, int attemptNumber) {
            daSlotPaths.add(slotPath);
            daAttempts.add(attemptNumber);
        }

        @Override
        public void onPrimaryUnavailable(String slotPath, int attemptNumber) {
            primaryAttempts.add(attemptNumber);
        }

        List<Integer> daAttemptsSnapshot() {
            synchronized (daAttempts) {
                return new ArrayList<>(daAttempts);
            }
        }

        List<Integer> primaryAttemptsSnapshot() {
            synchronized (primaryAttempts) {
                return new ArrayList<>(primaryAttempts);
            }
        }
    }

    /**
     * Acks every inbound frame with STATUS_OK + STATUS_DURABLE_ACK on a
     * per-connection wire sequence, so a durable-ack-mode drain runs to
     * completion on whichever connection finally gets through (same ack
     * shape as BackgroundDrainerMidDrainCapabilityGapTest's handler, without
     * the scripted drop). State is keyed per ClientHandler identity; acks are
     * best-effort because a connection may be racing its own close.
     */
    private static final class DurableAckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final String TABLE = "trades";
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long[] counter = wireSeqByConn.get(client);
            if (counter == null) {
                counter = new long[1];
                wireSeqByConn.put(client, counter);
            }
            long seq = counter[0]++;
            try {
                client.sendBinary(okFrame(seq, seq));
                client.sendBinary(durableAckFrame(seq));
            } catch (IOException ignored) {
                // best-effort: the drainer replays on its next connection
            }
        }

        private static byte[] durableAckFrame(long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x02); // STATUS_DURABLE_ACK
            bb.putShort((short) 1); // tableCount
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seqTxn);
            return bb.array();
        }

        private static byte[] okFrame(long wireSeq, long seqTxn) {
            byte[] name = TABLE.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(1 + 8 + 2 + 2 + name.length + 8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(wireSeq);
            bb.putShort((short) 1); // tableCount
            bb.putShort((short) name.length);
            bb.put(name);
            bb.putLong(seqTxn);
            return bb.array();
        }
    }
}
