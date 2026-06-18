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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
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
 * Regression: a clean shutdown with every frame ACK'd by the server
 * must not replay any frames on the next session. Pre-fix, the cursor
 * engine never trims the active segment (only sealed segments go through
 * {@code drainTrimmable}), so a fully-ACK'd active persists on disk
 * across close, and the next sender's recovery walks every frame in it
 * starting from {@code baseSeq}. That replays already-ACK'd data against
 * a (potentially fresh) server — wasted bandwidth at best, duplicate
 * writes when the server has no dedup state for those messageSequences.
 * <p>
 * Hits the path the existing {@link RecoveryReplayTest} doesn't cover:
 * sender finishes work, server ACKs everything, sender closes cleanly,
 * next sender against same slot / different server should send nothing.
 */
public class CleanShutdownNoReplayTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-clean-shutdown-replay-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test
    public void testFullyAckedActiveDoesNotReplayAfterCleanRestart() throws Exception {
        // Phase 1: server ACKs every frame. Sender writes a few rows,
        // flushes, then close() blocks for the default 5s drain — by the
        // time close returns, every frame has been ACK'd.
        AckHandler ack1 = new AckHandler();
        try (TestWebSocketServer s1 = new TestWebSocketServer(ack1)) {
            s1.start();
            Assert.assertTrue(s1.awaitStart(5, TimeUnit.SECONDS));

            int port1 = s1.getPort();
            String cfg1 = "ws::addr=localhost:" + port1
                    + ";sf_dir=" + sfDir + ";";
            try (Sender sender = Sender.fromConfig(cfg1)) {
                for (int i = 0; i < 5; i++) {
                    sender.table("foo").longColumn("v", i).atNow();
                    sender.flush();
                }
                // Wait until the server has ACK'd everything we sent. The
                // close() drain timeout is 5s by default but we want a
                // tighter assert that the precondition really holds.
                long deadline = System.currentTimeMillis() + 3_000L;
                while (System.currentTimeMillis() < deadline
                        && ack1.totalAcksSent.get() < 5) {
                    Thread.sleep(20);
                }
                Assert.assertTrue(
                        "precondition: server should have ACK'd all 5 frames; saw "
                                + ack1.totalAcksSent.get(),
                        ack1.totalAcksSent.get() >= 5);
            }
        }

        // Phase 2: fresh server on a different port. New sender against the
        // SAME slot dir. There is no unacked work — both rings should agree
        // there's nothing to send. The expected count of binary frames at
        // server 2 is zero.
        AckHandler ack2 = new AckHandler();
        try (TestWebSocketServer s2 = new TestWebSocketServer(ack2)) {
            s2.start();
            Assert.assertTrue(s2.awaitStart(5, TimeUnit.SECONDS));

            int port2 = s2.getPort();
            String cfg2 = "ws::addr=localhost:" + port2
                    + ";sf_dir=" + sfDir + ";";
            try (Sender ignored = Sender.fromConfig(cfg2)) {
                // No new appends — purely observe whether recovery replays
                // anything. Give the I/O loop ample room to push any
                // replayed bytes onto the wire.
                Thread.sleep(500);

                Assert.assertEquals(
                        "fully-ACK'd data from a clean shutdown must not "
                                + "replay against the next server; observed "
                                + ack2.totalReceived.get() + " frame(s) at "
                                + "server 2",
                        0L, ack2.totalReceived.get());
            }
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

    private static class AckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalReceived = new AtomicLong();
        final AtomicLong totalAcksSent = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalReceived.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
                totalAcksSent.incrementAndGet();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static byte[] buildAck(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }
    }
}
