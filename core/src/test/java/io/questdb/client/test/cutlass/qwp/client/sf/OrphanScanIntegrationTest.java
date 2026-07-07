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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.ObjList;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration check: with {@code drain_orphans=true} the foreground sender
 * sees sibling slots holding unacked data, adopts them via the background
 * drainer pool, and replays their unacked frames — after which
 * {@link OrphanScanner#scan} reports no candidates, both while the adopting
 * sender is still open and after it closes.
 */
public class OrphanScanIntegrationTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-orphan-int-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test
    public void testScanFindsOrphanFromPriorSenderUnderSameGroupRoot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // First sender uses sender_id=ghost. We give it data + flush, but
            // close the server BEFORE acks land — so the slot retains
            // unacked .sfa files when the sender shuts down. Then the same
            // slot should be reported as an orphan when a second sender opens
            // with sender_id=primary and drain_orphans=true.

            // Phase 1: ghost writes + closes; never acked.
            SilentHandler silent = new SilentHandler();
            try (TestWebSocketServer ghostServer = new TestWebSocketServer(silent)) {
                ghostServer.start();
                Assert.assertTrue(ghostServer.awaitStart(5, TimeUnit.SECONDS));

                int ghostPort = ghostServer.getPort();
                String ghostCfg = "ws::addr=localhost:" + ghostPort
                        + ";sf_dir=" + sfDir + ";sender_id=ghost;close_flush_timeout_millis=0;";
                try (Sender ghost = Sender.fromConfig(ghostCfg)) {
                    ghost.table("foo").longColumn("v", 7L).atNow();
                    ghost.flush();
                    // The frame must reach the wire before we close: on-the-wire
                    // implies the I/O loop read it back from the slot's .sfa, so
                    // the recovered slot holds publishedFsn >= 1 and the drain in
                    // phase 2 proves something. Without this await,
                    // close_flush_timeout=0 can close before the async publish
                    // lands and the "drain" would trivially succeed on an empty
                    // slot (observed as "fully drained (target=0)").
                    Assert.assertTrue("ghost frame must reach the wire before close",
                            silent.awaitFrame(5, TimeUnit.SECONDS));
                    // No wait for ACK — close right away; close_flush_timeout=0
                    // means we don't drain.
                }
            }
            // Independent verification: the scanner sees the ghost slot.
            ObjList<String> seen = OrphanScanner.scan(sfDir, "primary");
            Assert.assertEquals("ghost slot must be a candidate orphan", 1, seen.size());
            Assert.assertEquals(sfDir + "/ghost", seen.get(0));

            // Phase 2: open the primary sender with drain_orphans=true. The
            // background drainer pool adopts the ghost slot, replays its
            // unacked frames against the ACKing primaryServer, and the
            // drained slot's .sfa files are removed when the drainer's
            // engine closes fully drained.
            try (TestWebSocketServer primaryServer = new TestWebSocketServer(new AckHandler())) {
                primaryServer.start();
                Assert.assertTrue(primaryServer.awaitStart(5, TimeUnit.SECONDS));

                int primaryPort = primaryServer.getPort();
                String primaryCfg = "ws::addr=localhost:" + primaryPort
                        + ";sf_dir=" + sfDir
                        + ";sender_id=primary"
                        + ";drain_orphans=true;";
                try (Sender primary = Sender.fromConfig(primaryCfg)) {
                    primary.table("foo").longColumn("v", 8L).atNow();
                    primary.flush();
                    // Await the drain while the primary is still open so this
                    // assertion exercises the drainer runtime itself and does
                    // not depend on close()'s bounded graceful-drain window.
                    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while (OrphanScanner.scan(sfDir, "primary").size() > 0
                            && System.nanoTime() < deadlineNanos) {
                        Thread.sleep(10);
                    }
                    Assert.assertEquals(
                            "drainer should have adopted + drained the ghost slot "
                                    + "while the primary sender is open",
                            0, OrphanScanner.scan(sfDir, "primary").size());
                }
                // Primary's own slot drains cleanly on close() and is filtered
                // out by sender_id; the drained ghost slot must not resurface
                // (e.g. as a spurious .failed quarantine). Net: scanner sees
                // neither.
                ObjList<String> postRun = OrphanScanner.scan(sfDir, "primary");
                Assert.assertEquals(
                        "drain_orphans=true should have drained + removed the "
                                + "ghost slot; primary's own slot is sender_id-filtered",
                        0, postRun.size());
            }
        });
    }

    @Test
    public void testFailedSentinelHidesOrphanFromScan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Manually construct an orphan slot, then drop a .failed sentinel.
            // The scan must hide it — automation has already given up on this
            // slot and a human needs to act before it gets touched again.
            Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
            String orphan = sfDir + "/manual";
            Assert.assertEquals(0, Files.mkdir(orphan, Files.DIR_MODE_DEFAULT));
            touchFile(orphan + "/sf-0001.sfa");

            Assert.assertEquals(1, OrphanScanner.scan(sfDir, "x").size());
            OrphanScanner.markFailed(orphan, "operator-induced");
            Assert.assertEquals(0, OrphanScanner.scan(sfDir, "x").size());
        });
    }

    private static void touchFile(String path) {
        int fd = Files.openRW(path);
        if (fd >= 0) Files.close(fd);
    }

    /** Receives binary frames but never acks. Causes the sender to
     *  leave unacked data on disk on close. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch frameReceived = new CountDownLatch(1);

        boolean awaitFrame(long timeout, TimeUnit unit) throws InterruptedException {
            return frameReceived.await(timeout, unit);
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // Drop on the floor — no ACK. Record receipt so the test can
            // prove the frame reached the wire (hence the slot's .sfa)
            // before the ghost sender closes.
            frameReceived.countDown();
        }
    }

    /**
     * Acks every binary frame. Sequence numbers are per-connection: the
     * primary sender and the orphan drainer each open their own WebSocket,
     * and each connection numbers its frames from 0. A single shared
     * counter would hand the second connection an ack seq it never sent
     * ("ACK wire seq N exceeds highest sent 0"), making the drain succeed
     * only via the client's clamping fallback.
     */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final ConcurrentHashMap<TestWebSocketServer.ClientHandler, AtomicLong> seqByClient =
                new ConcurrentHashMap<>();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long seq = seqByClient.computeIfAbsent(client, k -> new AtomicLong()).getAndIncrement();
            try {
                client.sendBinary(buildAck(seq));
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
                        if (!Files.remove(child)) {
                            rmDirRec(child);
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
}
