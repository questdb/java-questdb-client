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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Integration check: with {@code drain_orphans=true} the foreground sender
 * sees sibling slots holding unacked data and a follow-up call to
 * {@link OrphanScanner#scan} from outside the sender returns the same.
 * <p>
 * The drainer runtime that actually empties orphan slots is a follow-up;
 * this test pins down the visibility/scan piece.
 */
public class OrphanScanIntegrationTest {

    private static final int TEST_PORT = 19_500 + (int) (System.nanoTime() % 100);
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
            int port = TEST_PORT + 1;

            // Phase 1: ghost writes + closes; never acked.
            TestWebSocketServer ghostServer = new TestWebSocketServer(port, new SilentHandler());
            try {
                ghostServer.start();
                Assert.assertTrue(ghostServer.awaitStart(5, TimeUnit.SECONDS));

                String ghostCfg = "ws::addr=localhost:" + port
                        + ";sf_dir=" + sfDir + ";sender_id=ghost;close_flush_timeout_millis=0;";
                try (Sender ghost = Sender.fromConfig(ghostCfg)) {
                    ghost.table("foo").longColumn("v", 7L).atNow();
                    ghost.flush();
                    // No wait for ACK — close right away; close_flush_timeout=0
                    // means we don't drain.
                }
            } finally {
                try {
                    ghostServer.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
            // Independent verification: the scanner sees the ghost slot.
            ObjList<String> seen = OrphanScanner.scan(sfDir, "primary");
            Assert.assertEquals("ghost slot must be a candidate orphan", 1, seen.size());
            Assert.assertEquals(sfDir + "/ghost", seen.get(0));

            // Phase 2: open the primary sender with drain_orphans=true. We
            // can't directly assert the log output in this test, but the
            // call must not throw, and the primary's own slot must NOT
            // appear in a fresh scan (sender_id-filtered).
            TestWebSocketServer primaryServer = new TestWebSocketServer(port + 1000, new AckHandler());
            try {
                primaryServer.start();
                Assert.assertTrue(primaryServer.awaitStart(5, TimeUnit.SECONDS));

                String primaryCfg = "ws::addr=localhost:" + (port + 1000)
                        + ";sf_dir=" + sfDir
                        + ";sender_id=primary"
                        + ";drain_orphans=true;";
                try (Sender primary = Sender.fromConfig(primaryCfg)) {
                    primary.table("foo").longColumn("v", 8L).atNow();
                    primary.flush();
                }
                // With drain_orphans=true, the background drainer pool adopts
                // the ghost slot, replays its unacked frames against the now-
                // ACKing primaryServer, and removes the drained slot dir.
                // Primary's own slot drains cleanly on close() and is filtered
                // out by sender_id. Net: scanner sees neither.
                ObjList<String> postRun = OrphanScanner.scan(sfDir, "primary");
                Assert.assertEquals(
                        "drain_orphans=true should have drained + removed the "
                                + "ghost slot; primary's own slot is sender_id-filtered",
                        0, postRun.size());
            } finally {
                try {
                    primaryServer.close();
                } catch (Exception ignored) {
                    // best-effort
                }
            }
        });
    }

    @Test
    public void testFailedSentinelHidesOrphanFromScan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Manually construct an orphan slot, then drop a .failed sentinel.
            // The scan must hide it — automation has already given up on this
            // slot and a human needs to act before it gets touched again.
            Assert.assertEquals(0, Files.mkdir(sfDir, 0755));
            String orphan = sfDir + "/manual";
            Assert.assertEquals(0, Files.mkdir(orphan, 0755));
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
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // Drop on the floor — no ACK.
        }
    }

    /** Acks every binary frame. */
    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
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
