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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.std.Files;
import io.questdb.client.std.Rnd;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

/**
 * Randomised drain-liveness fuzz for the {@link SenderPool} SF recovery scan,
 * the retire/re-probe machinery, and their interleaving with ordinary
 * borrow/return traffic.
 * <p>
 * Every iteration builds the same fault family the deterministic tests pin
 * one shape of (see {@code SenderPoolSfTest}
 * {@code testRecoveryScanStaysAliveWhileRetiredSlotHoldsStrandedData}):
 * <ol>
 *   <li>a previous run strands unacked durable data under a random subset of
 *       the in-range slot dirs (silent server, close without acks);</li>
 *   <li>a new pool over the same {@code sf_dir} faces an ack-ing server, but a
 *       random subset of its recovery builds are forged into the wedged-close
 *       shape: {@code close()} returns with the slot flock still held, so the
 *       scan retires the slot ({@code leakedSlots++}, index stays reserved)
 *       with its data still on disk;</li>
 *   <li>a random single-threaded schedule of recovery steps, housekeeper
 *       ticks, borrows (returned or discarded) and late flock releases runs
 *       the scan, the retire bookkeeping and the borrow cap math against each
 *       other in randomized order.</li>
 * </ol>
 * After the schedule every wedge is healed (the "worker" exits, the flock
 * genuinely drops) and the pool is driven quiescently -- housekeeper tick +
 * recovery step, no new load. The oracle is the store-and-forward delivery
 * contract at the heart of the drain-liveness bug class: once faults heal and
 * the system goes quiet, EVERY durably-accepted row must reach the server IN
 * THIS PROCESS through nothing but the pool's ordinary lifecycle -- no
 * restart, no lucky borrow required. Concretely: {@code recoveryComplete}
 * latches and {@code leakedSlots} returns to zero under quiescent driving,
 * and after {@code pool.close()} (which drains live-owned delegates the
 * ordinary way -- a slot ADOPTED by a borrow mid-schedule is its owner's to
 * deliver, not the scan's) no slot dir may still hold a segment file. Pre-fix,
 * any iteration that retires a stranded slot mid-scan latches
 * {@code recoveryComplete} past the stranded data (the retired index was
 * skipped as "reserved" without counting a deferral); nothing ever drains that
 * dir -- close() never owned it -- and the post-close audit fails.
 * <p>
 * The schedule is single-threaded on purpose: with one driver the whole
 * iteration is a pure function of the seed, so any failure replays exactly
 * with {@code TestUtils.generateRandom(null, s0, s1)} (seeds are printed by
 * the harness and repeated in the failure message). Iteration 0 is pinned to
 * the worst case -- every slot stranded, slot 0 wedged, and a deterministic
 * two-step prologue that drives the scan into the wedged-retire shape BEFORE
 * any random traffic (a random schedule could otherwise draw a borrow first,
 * adopt slot 0 and never materialize the wedge) -- so the suite cannot go
 * green on an unlucky seed while the bug is present; later iterations
 * randomize freely.
 */
public class SenderPoolSfFuzzTest {

    private static final int ITERATIONS = 4;
    private static final long CONVERGE_BUDGET_MILLIS = 20_000;

    @Test
    public void testRetiredSlotDrainLivenessFuzz() throws Exception {
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

    private void runOneIteration(Rnd rnd, int iter) throws Exception {
        String sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-pool-fuzz-" + System.nanoTime() + "-" + iter).toString();
        try {
            TestUtils.assertMemoryLeak(() -> {
                int maxSize = 1 + rnd.nextInt(3); // 1..3
                // Iteration 0 pins the guaranteed-red shape: every slot
                // stranded so the wedged recovery build below has data behind
                // it. Later iterations may strand any subset (0 = a plain
                // clean-scan iteration, still a valid latch/borrow interplay).
                int stranded = iter == 0 ? maxSize : rnd.nextInt(maxSize + 1);

                // Phase 1: strand unacked data under default-0..(stranded-1).
                if (stranded > 0) {
                    try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                        int silentPort = silent.getPort();
                        silent.start();
                        Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
                        String seedCfg = "ws::addr=localhost:" + silentPort + ";sf_dir=" + sfDir
                                + ";close_flush_timeout_millis=0;";
                        try (SenderPool seed = new SenderPool(seedCfg, stranded, stranded,
                                5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                            PooledSender[] s = new PooledSender[stranded];
                            for (int i = 0; i < stranded; i++) {
                                s[i] = seed.borrow();
                            }
                            for (int i = 0; i < stranded; i++) {
                                int nRows = 1 + rnd.nextInt(3);
                                for (int r = 0; r < nRows; r++) {
                                    s[i].table("fuzz").longColumn("v", r).atNow();
                                    s[i].flush();
                                }
                            }
                            for (int i = stranded - 1; i >= 0; i--) {
                                s[i].close();
                            }
                        }
                    }
                    for (int i = 0; i < stranded; i++) {
                        Assert.assertTrue("iter " + iter + ": default-" + i + " must hold unacked data",
                                hasSegmentFile(sfDir + "/default-" + i));
                    }
                }

                // Phase 2: ack-ing server; wedge a random subset of RECOVERY
                // builds (iteration 0 always wedges slot 0). The forge is
                // scoped by inRecoveryStep so a borrow can never receive a
                // forged delegate -- exactly like a real wedged close(), which
                // only the recovery/reclaim paths ever observe.
                boolean[] wedge = new boolean[maxSize];
                for (int i = 0; i < maxSize; i++) {
                    wedge[i] = i < stranded && (iter == 0 ? i == 0 : rnd.nextBoolean());
                }
                CountingAckHandler handler = new CountingAckHandler();
                try (TestWebSocketServer ack = new TestWebSocketServer(handler);
                     TestWebSocketServer wedgeSink = new TestWebSocketServer(new SilentHandler())) {
                    int ackPort = ack.getPort();
                    ack.start();
                    Assert.assertTrue(ack.awaitStart(5, TimeUnit.SECONDS));
                    wedgeSink.start();
                    Assert.assertTrue(wedgeSink.awaitStart(5, TimeUnit.SECONDS));
                    // close_flush_timeout bounds the ordinary-lifecycle drain
                    // pool.close() performs on live-owned delegates; against
                    // the ack-ing server it completes in milliseconds.
                    String cfg = "ws::addr=localhost:" + ackPort + ";sf_dir=" + sfDir
                            + ";close_flush_timeout_millis=10000;";
                    // The forged recoverers model a WEDGED worker faithfully:
                    // alive (setClosedForTesting only forges the flag; the I/O
                    // loop keeps pumping) but delivering nothing. They are
                    // therefore built against a SILENT sink -- an alive worker
                    // streaming into a black hole -- so the adopted chain is
                    // never acked and never trimmed; and with close-flush OFF,
                    // so the heal close() drops the flock WITHOUT draining the
                    // chain on the data's behalf. Either kindness (an ack-ing
                    // endpoint or a draining close) would hand the oracle a
                    // delivery the production wedge semantics do not provide,
                    // masking scan abandonment.
                    String cfgWedge = "ws::addr=localhost:" + wedgeSink.getPort() + ";sf_dir=" + sfDir
                            + ";close_flush_timeout_millis=0;";

                    boolean[] inRecoveryStep = new boolean[1];
                    Sender[] forged = new Sender[maxSize];
                    List<Sender> unhealed = new ArrayList<>();
                    IntFunction<Sender> factory = idx -> {
                        boolean forgeNow = inRecoveryStep[0] && idx < maxSize
                                && wedge[idx] && forged[idx] == null;
                        Sender real = Sender.builder(forgeNow ? cfgWedge : cfg)
                                .senderId("default-" + idx).build();
                        if (forgeNow) {
                            try {
                                ((QwpWebSocketSender) real).setClosedForTesting(true);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                            forged[idx] = real;
                            unhealed.add(real);
                        }
                        return real;
                    };

                    try (SenderPool pool = new SenderPool(cfg, 0, maxSize, 100,
                            Long.MAX_VALUE, Long.MAX_VALUE, factory, true)) {
                        try {
                            if (iter == 0) {
                                // Deterministic prologue for the pinned
                                // iteration: step the scan until it builds the
                                // forged recoverer for slot 0 and RETIRES it
                                // with its data still on disk (step 1), then
                                // walk once more so the unfixed skip-without-
                                // deferral path is on the table (step 2).
                                // Without this a random schedule could borrow
                                // first, adopt slot 0 as a LIVE slot, and the
                                // bug shape would never materialize.
                                for (int k = 0; k < 2; k++) {
                                    inRecoveryStep[0] = true;
                                    try {
                                        pool.runStartupRecoveryStepForTesting();
                                    } finally {
                                        inRecoveryStep[0] = false;
                                    }
                                }
                                Assert.assertEquals(
                                        "iter 0 prologue must retire the wedged slot with "
                                                + "its stranded data still on disk",
                                        1, pool.leakedSlotCount());
                                Assert.assertTrue("iter 0 prologue must leave slot 0 stranded",
                                        hasSegmentFile(sfDir + "/default-0"));
                            }
                            // The randomized schedule. Single-threaded, so the
                            // iteration replays exactly from the seed.
                            int ops = 8 + rnd.nextInt(12);
                            for (int op = 0; op < ops; op++) {
                                switch (rnd.nextInt(5)) {
                                    case 0:
                                    case 1: // bias toward driving the scan
                                        inRecoveryStep[0] = true;
                                        try {
                                            pool.runStartupRecoveryStepForTesting();
                                        } finally {
                                            inRecoveryStep[0] = false;
                                        }
                                        break;
                                    case 2: // housekeeper tick: reap + re-probe retired
                                        pool.reapIdle();
                                        break;
                                    case 3: { // ordinary traffic, returned to the pool
                                        // The lease ALWAYS comes home: a leaked
                                        // lease would (correctly) block close()'s
                                        // ordinary-lifecycle drain and turn a
                                        // write hiccup into a bogus audit red.
                                        PooledSender ps = null;
                                        try {
                                            ps = pool.borrow();
                                            ps.table("fuzz").longColumn("live", op).atNow();
                                            ps.flush();
                                        } catch (LineSenderException e) {
                                            // Legal under wedged-retire capacity
                                            // starvation: borrow timed out.
                                        } finally {
                                            if (ps != null) {
                                                ps.close();
                                            }
                                        }
                                        break;
                                    }
                                    case 4: // late flock release for one wedged slot
                                        // Pinned iteration 0 keeps its wedge held
                                        // through the WHOLE schedule: a mid-schedule
                                        // heal would let a later borrow adopt the
                                        // freed index and deliver the data through
                                        // its own lifecycle -- legal, but it would
                                        // mask the scan-abandonment bug the pinned
                                        // iteration exists to catch. With the wedge
                                        // held, no borrow can ever own slot 0, so
                                        // post-heal the SCAN is the only possible
                                        // deliverer (fixed) versus nobody (bug).
                                        if (iter != 0 && !unhealed.isEmpty()) {
                                            Sender s = unhealed.remove(rnd.nextInt(unhealed.size()));
                                            ((QwpWebSocketSender) s).setClosedForTesting(false);
                                            s.close();
                                        }
                                        break;
                                }
                            }

                            // Heal every remaining wedge: the "workers" exit and
                            // the flocks genuinely drop.
                            while (!unhealed.isEmpty()) {
                                Sender s = unhealed.remove(unhealed.size() - 1);
                                ((QwpWebSocketSender) s).setClosedForTesting(false);
                                s.close();
                            }

                            // Quiescent convergence: housekeeper tick + recovery
                            // step only -- NO new borrows. The delivery contract
                            // must not depend on future load.
                            long deadline = System.currentTimeMillis() + CONVERGE_BUDGET_MILLIS;
                            while (!pool.isRecoveryCompleteForTesting()
                                    && System.currentTimeMillis() < deadline) {
                                pool.reapIdle();
                                boolean more;
                                inRecoveryStep[0] = true;
                                try {
                                    more = pool.runStartupRecoveryStepForTesting();
                                } finally {
                                    inRecoveryStep[0] = false;
                                }
                                if (!more && !pool.isRecoveryCompleteForTesting()) {
                                    Thread.sleep(5);
                                }
                            }

                            // In-pool audit: the scan itself must have
                            // converged -- a latched-early scan is exactly the
                            // drain-liveness bug shape.
                            Assert.assertTrue(
                                    "iter " + iter + ": recovery scan must complete once every "
                                            + "flock is healed -- a latched-early scan strands "
                                            + "retired slots' data until restart",
                                    pool.isRecoveryCompleteForTesting());
                            Assert.assertEquals(
                                    "iter " + iter + ": healed flocks must restore all retired capacity",
                                    0, pool.leakedSlotCount());
                        } finally {
                            // A failed assertion must not leak forged natives:
                            // un-forge and close for real (idempotent).
                            for (Sender s : unhealed) {
                                try {
                                    ((QwpWebSocketSender) s).setClosedForTesting(false);
                                    s.close();
                                } catch (Throwable ignore) {
                                    // best-effort teardown
                                }
                            }
                            unhealed.clear();
                        }
                    }

                    // Post-close audit: the pool exited through its ordinary
                    // lifecycle (close() drains live-owned delegates; the scan
                    // drained everything nobody owned). NOTHING may remain
                    // durably-accepted-but-undelivered in this process -- no
                    // restart, no lucky borrow. Pre-fix, a slot retired
                    // mid-scan fails exactly here: the latched scan abandoned
                    // it and close() never owned it.
                    for (int i = 0; i < maxSize; i++) {
                        Assert.assertTrue(
                                "iter " + iter + ": default-" + i + " must not hold "
                                        + "undelivered durable data after quiescent convergence "
                                        + "and an ordinary-lifecycle close",
                                awaitNoSegmentFile(sfDir + "/default-" + i, 15_000));
                    }
                }
            });
        } finally {
            rmDir(sfDir);
        }
    }

    // ------------------------------------------------------------------
    // Local copies of the SenderPoolSfTest harness helpers (private there).
    // ------------------------------------------------------------------

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

    private static boolean awaitNoSegmentFile(String slotPath, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (!hasSegmentFile(slotPath)) {
                return true;
            }
            Thread.sleep(10);
        }
        return !hasSegmentFile(slotPath);
    }

    private static void rmDir(String dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            rmDir(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    private static final class CountingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final Map<TestWebSocketServer.ClientHandler, AtomicLong> seqByClient =
                new ConcurrentHashMap<>();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
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
            // No ack -- frames stay unacked on disk so recovery sees candidates.
        }
    }
}
