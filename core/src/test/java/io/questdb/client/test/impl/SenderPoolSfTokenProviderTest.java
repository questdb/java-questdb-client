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

package io.questdb.client.test.impl;

import io.questdb.client.HttpTokenProvider;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Token-provider wiring for POOLED senders that also run store-and-forward —
 * pooled WebSocket + SF + OIDC, the configuration this client is built for and
 * the one combination no test covered.
 * <p>
 * {@code SenderPool.buildManagedSlotSender} has two legs. The
 * {@code !storeAndForward} leg applies the provider inline and is exercised by
 * {@code QuestDBBuilderTest#testConnectTokenProviderSuppliesBothPoolsAndPoolGrowth},
 * which configures no {@code sf_dir}. The SF leg builds the delegate through the
 * slot-id / orphan-exclusion / recovery-mode chain and applies the provider at the
 * end of it, for both ordinary and recovery delegates — and nothing asserted either.
 * <p>
 * What an unwired provider costs is not a missing header in isolation. Every SF
 * pooled sender's upgrade would go out unauthenticated, take a 401, and hand the
 * rows to store-and-forward; the operator would not learn at connect time but much
 * later, through ring backpressure or a quarantined slot. On the recovery leg it is
 * worse: a recovery delegate drains the PREVIOUS run's data, so an unauthenticated
 * build quarantines the slot and reports {@code DATA_LOSS} for rows that were
 * replayable all along.
 * <p>
 * Both tests are black-box through the public facade — {@code QuestDB.connect(cfg,
 * provider)} against a real {@link TestWebSocketServer} — and assert on the
 * Authorization header the server actually received, so they hold for any wiring
 * that gets the credential onto the wire.
 */
public class SenderPoolSfTokenProviderTest {

    private String sfDir;

    // one shared temp-directory mechanism instead of a per-class java.io.tmpdir path plus a hand-rolled
    // recursive delete: the rule cleans up on failure and on an exception thrown out of a test too
    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Before
    public void setUp() {
        sfDir = temp.getRoot().toPath().resolve("slot").toString();
    }


    @Test
    public void testSfPooledSendersCarryTheProviderToken() throws Exception {
        // The SF leg of buildManagedSlotSender: every pooled SF sender must pull
        // its own current token, exactly as the non-SF leg does. Two prewarmed
        // slots, so this also pins that the provider is consulted per sender and
        // not once for the pool.
        TestUtils.assertMemoryLeak(() -> {
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                AtomicInteger tokenCalls = new AtomicInteger();
                HttpTokenProvider provider = () -> "ROTATING-" + tokenCalls.incrementAndGet();
                // query_pool_min=0 keeps the egress pool from connecting, so every
                // captured header belongs to an SF pooled sender.
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";"
                        + "sender_pool_min=2;sender_pool_max=2;"
                        + "query_pool_min=0;query_pool_max=1;";

                try (QuestDB db = QuestDB.connect(cfg, provider)) {
                    assertAuthorizationHeaders(server, "Bearer ROTATING-1", "Bearer ROTATING-2");
                    // The senders are genuinely usable on those credentials, not
                    // merely upgraded: borrow both slots and ship a row through each.
                    try (Sender s1 = db.borrowSender(); Sender s2 = db.borrowSender()) {
                        s1.table("pooled").longColumn("v", 1).atNow();
                        s1.flush();
                        s2.table("pooled").longColumn("v", 2).atNow();
                        s2.flush();
                    }
                    Assert.assertTrue("both pooled SF senders must reach the server",
                            awaitAtLeast(handler.frames, 2, 10_000));
                }
                Assert.assertEquals("one token pull per pooled SF sender", 2, tokenCalls.get());
            }
        });
    }

    @Test(timeout = 60_000)
    public void testCloseBreaksARecoveryDelegateStuckInACredentialPull() throws Exception {
        // A recovery build pulls a credential before it connects, and that pull can block far longer than
        // close()'s join: OidcDeviceAuth.getToken() documents a wait of up to six times httpTimeoutMillis
        // behind a peer's refresh, and FileTokenStore's in-process lock wait has no budget at all, against a
        // PoolHousekeeper.STOP_TIMEOUT_MILLIS of 2s. The stop flag reaches the recovery loop only BETWEEN
        // steps, so it cannot reach a step parked inside the pull.
        //
        // Returning from close() anyway leaves the recoverer holding its slot flock, which is what the
        // pool's per-slot ids and the drain_orphans(false) forced on recovery builds exist to prevent: an
        // immediate reopen fails with "sf slot already in use", and the detached build's engine, mmaps and
        // I/O thread are leaked. So the stop path escalates to an interrupt, which every wait on that path
        // honours.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1 -- strand unacked frames on disk, so phase 2 has recovery work to do.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir + ";"
                        + "sender_pool_min=1;sender_pool_max=1;"
                        + "query_pool_min=0;query_pool_max=1;"
                        + "close_flush_timeout_millis=500;";
                try (QuestDB db = QuestDB.connect(cfg, () -> "PHASE1-TOKEN")) {
                    try (Sender s = db.borrowSender()) {
                        for (int i = 0; i < 3; i++) {
                            s.table("recover").longColumn("v", i).atNow();
                            s.flush();
                        }
                    }
                }
            }
            Assert.assertTrue("unacked data must persist on disk for recovery to have work",
                    hasSegmentFile(sfDir + "/default-0"));

            // Phase 2 -- a provider that parks the way a contended token-store lock wait does.
            CountDownLatch pullEntered = new CountDownLatch(1);
            AtomicBoolean pullInterrupted = new AtomicBoolean();
            HttpTokenProvider blockingProvider = () -> {
                pullEntered.countDown();
                try {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(5));
                } catch (InterruptedException e) {
                    pullInterrupted.set(true);
                    Thread.currentThread().interrupt();
                    // what OidcDeviceAuth.getToken() does on a cancelled wait: report, do not hang
                    throw new RuntimeException("credential pull cancelled");
                }
                return "NEVER-ARRIVES";
            };

            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";"
                        + "sender_pool_min=0;sender_pool_max=1;"
                        + "query_pool_min=0;query_pool_max=1;";

                QuestDB db = QuestDB.connect(cfg, blockingProvider);
                Assert.assertTrue("the recovery delegate must reach the credential pull",
                        pullEntered.await(20, TimeUnit.SECONDS));

                long startNanos = System.nanoTime();
                db.close();
                long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;

                // The load-bearing assertion. Without the escalation close() joins its budget, gives up and
                // returns with the pull still parked -- so this stays false and the flock is still held.
                Assert.assertTrue("close() must interrupt a recovery delegate parked in a credential pull, "
                                + "or it returns while that delegate still holds the slot flock",
                        pullInterrupted.get());
                // Bounded well above the two joins so a loaded box does not turn this red, and far below
                // the provider's 5-minute park, which is what an un-escalated close() would wait out.
                Assert.assertTrue("close() must not wait out the parked pull; took " + elapsedMillis + "ms",
                        elapsedMillis < 30_000);
            }
        });
    }

    @Test
    public void testSfStartupRecoveryDelegateCarriesTheProviderToken() throws Exception {
        // The forRecovery leg of buildManagedSlotSender. A recovery delegate replays
        // the user's own data from a previous run, so an unauthenticated build does
        // not merely fail to connect: it quarantines the slot and reports DATA_LOSS
        // for rows that were replayable.
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1 -- a server that never acks, so three frames stay unacked on
            // disk under default-0 after the pool closes.
            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + silent.getPort() + ";sf_dir=" + sfDir + ";"
                        + "sender_pool_min=1;sender_pool_max=1;"
                        + "query_pool_min=0;query_pool_max=1;"
                        + "close_flush_timeout_millis=500;";
                try (QuestDB db = QuestDB.connect(cfg, () -> "PHASE1-TOKEN")) {
                    try (Sender s = db.borrowSender()) {
                        for (int i = 0; i < 3; i++) {
                            s.table("recover").longColumn("v", i).atNow();
                            s.flush();
                        }
                    }
                }
            }
            Assert.assertTrue("unacked data must persist on disk for recovery to have work",
                    hasSegmentFile(sfDir + "/default-0"));

            // Phase 2 -- an ack-ing server and a brand-new pool over the same sf_dir.
            // sender_pool_min=0 prewarms nothing, so the ONLY connect this server can
            // see is the startup-recovery delegate's.
            CountingAckHandler handler = new CountingAckHandler();
            try (TestWebSocketServer ack = new TestWebSocketServer(handler)) {
                ack.start();
                Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));

                AtomicInteger tokenCalls = new AtomicInteger();
                HttpTokenProvider provider = () -> {
                    tokenCalls.incrementAndGet();
                    return "RECOVERY-TOKEN";
                };
                String cfg = "ws::addr=localhost:" + ack.getPort() + ";sf_dir=" + sfDir + ";"
                        + "sender_pool_min=0;sender_pool_max=1;"
                        + "query_pool_min=0;query_pool_max=1;";

                try (QuestDB db = QuestDB.connect(cfg, provider)) {
                    Assert.assertNotNull(db);
                    String header = ack.pollAuthorizationHeader(10, TimeUnit.SECONDS);
                    Assert.assertNotNull("the recovery delegate must connect", header);
                    Assert.assertEquals(
                            "a recovery delegate must present the provider's credential -- "
                                    + "without it the replay is rejected and the slot is quarantined, "
                                    + "reporting DATA_LOSS for replayable rows",
                            "Bearer RECOVERY-TOKEN", header);
                    // Tie that header to recovery rather than to any other connect:
                    // the previous run's frames actually reach the new server.
                    Assert.assertTrue("the recovered frames must be replayed",
                            awaitAtLeast(handler.frames, 1, 10_000));
                }
                Assert.assertTrue("the recovery delegate must consult the provider",
                        tokenCalls.get() >= 1);
            }
        });
    }

    private static void assertAuthorizationHeaders(
            TestWebSocketServer server,
            String... expected
    ) throws InterruptedException {
        Set<String> actual = new HashSet<>();
        for (int i = 0; i < expected.length; i++) {
            String header = server.pollAuthorizationHeader(10, TimeUnit.SECONDS);
            Assert.assertNotNull("timed out waiting for an Authorization header", header);
            // The server records "" for an upgrade that carried no Authorization
            // header at all -- the exact shape of an unwired provider. Name it,
            // rather than letting two of them collide as a "duplicate".
            Assert.assertFalse("an SF pooled sender upgraded with NO Authorization header",
                    header.isEmpty());
            Assert.assertTrue("duplicate Authorization header: " + header, actual.add(header));
        }
        Set<String> want = new HashSet<>();
        for (int i = 0; i < expected.length; i++) {
            want.add(expected[i]);
        }
        Assert.assertEquals(want, actual);
    }

    private static boolean awaitAtLeast(AtomicInteger counter, int target, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (counter.get() >= target) {
                return true;
            }
            Thread.sleep(10);
        }
        return counter.get() >= target;
    }

    private static boolean hasSegmentFile(String slotPath) {
        if (!Files.exists(slotPath)) {
            return false;
        }
        long find = Files.findFirst(slotPath);
        if (find <= 0) {
            return false;
        }
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
                if (name != null && name.endsWith(".sfa")) {
                    return true;
                }
            }
        } finally {
            Files.findClose(find);
        }
        return false;
    }


    private static final class CountingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger frames = new AtomicInteger();
        private final Map<TestWebSocketServer.ClientHandler, AtomicLong> seqByClient =
                new ConcurrentHashMap<>();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            frames.incrementAndGet();
            AtomicLong seq = seqByClient.computeIfAbsent(client, c -> new AtomicLong(0));
            try {
                client.sendBinary(buildAck(seq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00); // STATUS_OK
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }

    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // No ack -- the frames stay unacked on disk for phase 2 to recover.
        }
    }
}
