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

package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.TimeUnit;

/**
 * Every sender a pool builds charges one {@code sf_max_total_bytes} budget, so
 * the key caps the pool rather than each pooled sender: growing the pool adds
 * connections, not buffer memory. Before, each pooled sender buffered up to the
 * whole cap on its own, and a server that stopped acknowledging grew the
 * client's native memory by the cap times the pool size.
 */
public class SenderPoolSharedBudgetTest {

    private static final long SEGMENT_BYTES = 64 * 1024;
    private static final long BUDGET_BYTES = 16 * SEGMENT_BYTES;
    // Every live sender is always granted its active segment plus one spare,
    // budget or not.
    private static final long MIN_WORKING_SET_BYTES = 2 * SEGMENT_BYTES;
    private static final int POOL_SIZE = 4;

    @Rule
    public final TemporaryFolder temp = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testBudgetIsSizedFromTheResolvedSfMaxTotalBytes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // min=0 builds no sender, so nothing connects to these addresses.
            try (SenderPool pool = newIdlePool("ws::addr=127.0.0.1:1;")) {
                Assert.assertEquals("memory-mode default",
                        128L * 1024 * 1024, pool.getSegmentBudgetForTesting().getCapacityBytes());
            }
            try (SenderPool pool = newIdlePool("ws::addr=127.0.0.1:1;sf_max_total_bytes=5m;")) {
                Assert.assertEquals(5L * 1024 * 1024, pool.getSegmentBudgetForTesting().getCapacityBytes());
            }
            String sfDir = temp.getRoot().toPath().resolve("sf").toString();
            try (SenderPool pool = newIdlePool("ws::addr=127.0.0.1:1;sf_dir=" + sfDir + ";")) {
                Assert.assertEquals("store-and-forward default",
                        10L * 1024 * 1024 * 1024, pool.getSegmentBudgetForTesting().getCapacityBytes());
            }
            try (SenderPool pool = newIdlePool("http::addr=127.0.0.1:1;protocol_version=2;")) {
                Assert.assertNull("an HTTP sender has no segment ring to budget",
                        pool.getSegmentBudgetForTesting());
            }
        });
    }

    @Test
    public void testPooledSendersShareOneBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The server never acknowledges, so everything the senders write stays
            // buffered client-side -- the outage that used to cost the cap times
            // the pool size.
            try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            })) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";"
                        + "sf_max_segment_bytes=" + SEGMENT_BYTES + ";"
                        + "sf_max_total_bytes=" + BUDGET_BYTES + ";"
                        + "sf_append_deadline_millis=200;"
                        + "close_flush_timeout_millis=100;";
                SegmentBudget budget;
                try (SenderPool pool = new SenderPool(cfg, POOL_SIZE, POOL_SIZE, 5_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
                    budget = pool.getSegmentBudgetForTesting();
                    Assert.assertEquals(BUDGET_BYTES, budget.getCapacityBytes());
                    Sender[] senders = new Sender[POOL_SIZE];
                    try {
                        for (int i = 0; i < POOL_SIZE; i++) {
                            senders[i] = pool.borrow();
                        }
                        // Every sender is built and holds its starting segments, so
                        // from here on native memory grows only by buffered data.
                        long nativeBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                        long bufferedBefore = budget.getSegmentBytes();
                        for (int i = 0; i < POOL_SIZE; i++) {
                            writeUntilBackpressured(senders[i]);
                        }
                        // Measured independently of the budget's own accounting: with a
                        // cap per sender this grew by about POOL_SIZE budgets.
                        long nativeGrowth = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT) - nativeBefore;
                        Assert.assertTrue("native memory must grow by no more than the rest of the one budget "
                                        + "[growth=" + nativeGrowth + ", bufferedBefore=" + bufferedBefore + ']',
                                nativeGrowth <= BUDGET_BYTES - bufferedBefore + 4 * SEGMENT_BYTES);
                        long buffered = budget.getSegmentBytes();
                        Assert.assertTrue("the senders together must fill the budget [buffered=" + buffered + ']',
                                buffered >= BUDGET_BYTES - SEGMENT_BYTES);
                        // With a cap per sender, the first sender alone would have
                        // buffered the whole budget and each of the others up to
                        // another one: POOL_SIZE budgets in all.
                        Assert.assertTrue("the pool must buffer at most one budget plus each sender's minimum "
                                        + "working set [buffered=" + buffered + ']',
                                buffered <= BUDGET_BYTES + POOL_SIZE * MIN_WORKING_SET_BYTES);
                    } finally {
                        for (Sender sender : senders) {
                            closeQuietly(sender);
                        }
                    }
                }
                Assert.assertEquals("closing the pool must release every charge", 0, budget.getSegmentBytes());
            }
        });
    }

    private static void closeQuietly(Sender sender) {
        if (sender == null) {
            return;
        }
        try {
            // The ring is still full, so the flush in close() is backpressured too
            // and the pool discards the sender instead of recycling it.
            sender.close();
        } catch (LineSenderException ignored) {
        }
    }

    private static SenderPool newIdlePool(String cfg) {
        return new SenderPool(cfg, 0, POOL_SIZE, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    // Writes ~16 KiB batches until the sender's flush gives up waiting for room
    // in its segment ring.
    private static void writeUntilBackpressured(Sender sender) {
        StringBuilder payload = new StringBuilder(1024);
        for (int i = 0; i < 1024; i++) {
            payload.append((char) ('a' + i % 26));
        }
        try {
            for (int batch = 0; batch < 10_000; batch++) {
                for (int row = 0; row < 16; row++) {
                    sender.table("budget").stringColumn("payload", payload).longColumn("row", row).atNow();
                }
                sender.flush();
            }
        } catch (LineSenderException e) {
            // The append failure arrives wrapped; its cause names the deadline.
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t.getMessage() != null && t.getMessage().contains("backpressured")) {
                    return;
                }
            }
            throw new AssertionError("expected a backpressure failure", e);
        }
        Assert.fail("the sender never ran out of room to buffer");
    }
}
