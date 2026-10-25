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
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A flush ships the tables listed since the last successful flush, not every
 * table the sender has seen. Adapted from Sergei Minaev's fb2cd98b.
 */
public class FlushPendingTablesTest {

    @Test
    public void testFailedFlushKeepsTheListForTheTablesWrittenAfterReset() throws Exception {
        List<byte[]> frames = new CopyOnWriteArrayList<>();
        try (TestWebSocketServer server = startServer(frames, 4 * 1024);
             Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                     + ";auto_flush_rows=off;auto_flush_bytes=off;auto_flush_interval=60000;")) {
            sender.table("w").longColumn("v", 1).atNow();
            sender.flush();
            awaitDataFrames(frames, 1);

            sender.table("alpha").longColumn("v", 2).atNow();
            sender.table("bravo").longColumn("v", 3).atNow();
            String wide = wideValue(1500);
            for (int i = 0; i < 3; i++) {
                sender.table("charlie_dropped").stringColumn("s", wide).atNow();
            }
            try {
                sender.flush();
                Assert.fail("charlie_dropped alone exceeds the 4 KiB cap");
            } catch (BatchTooLargeForCapException expected) {
                // thrown before anything was published
            }
            sender.reset();

            sender.table("alpha").longColumn("v", 4).atNow();
            sender.table("bravo").longColumn("v", 5).atNow();
            sender.flush();
            awaitDataFrames(frames, 2);
            byte[] frame = frames.get(1);
            Assert.assertEquals(2, tableCount(frame));
            Assert.assertTrue("alpha ships", indexOf(frame, "alpha") >= 0);
            Assert.assertTrue("bravo ships", indexOf(frame, "bravo") >= 0);
            for (int i = 0, n = frames.size(); i < n; i++) {
                Assert.assertEquals("frame " + i + " must not carry the dropped table",
                        -1, indexOf(frames.get(i), "charlie_dropped"));
            }
        }
    }

    @Test
    public void testFlushShipsOnlyTablesWithPendingRows() throws Exception {
        List<byte[]> frames = new CopyOnWriteArrayList<>();
        try (TestWebSocketServer server = startServer(frames, 0);
             Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                     + ";auto_flush_interval=60000;")) {
            sender.table("alpha").longColumn("v", 1).atNow();
            sender.table("bravo").longColumn("v", 2).atNow();
            sender.table("alpha").longColumn("v", 3).atNow();
            sender.flush();
            awaitDataFrames(frames, 1);
            byte[] frame0 = frames.get(0);
            Assert.assertEquals(2, tableCount(frame0));
            int alphaAt = indexOf(frame0, "alpha");
            int bravoAt = indexOf(frame0, "bravo");
            Assert.assertTrue("frame 0 carries alpha", alphaAt >= 0);
            Assert.assertTrue("frame 0 carries bravo", bravoAt >= 0);
            Assert.assertTrue("tables ship in first-row order", alphaAt < bravoAt);

            sender.table("charlie").longColumn("v", 4).atNow();
            sender.flush();
            awaitDataFrames(frames, 2);
            Assert.assertEquals("only charlie has rows", 1, tableCount(frames.get(1)));

            sender.table("bravo").longColumn("v", 5).atNow();
            sender.flush();
            awaitDataFrames(frames, 3);
            byte[] frame2 = frames.get(2);
            Assert.assertEquals("only bravo has rows", 1, tableCount(frame2));
            Assert.assertTrue("frame 2 carries bravo", indexOf(frame2, "bravo") >= 0);
            Assert.assertEquals("frame 2 carries no alpha", -1, indexOf(frame2, "alpha"));
            Assert.assertEquals("frame 2 carries no charlie", -1, indexOf(frame2, "charlie"));
        }
    }

    @Test
    public void testResetKeepsListedTablesAndSkipsTheEmptyOnes() throws Exception {
        List<byte[]> frames = new CopyOnWriteArrayList<>();
        try (TestWebSocketServer server = startServer(frames, 0);
             Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                     + ";auto_flush_interval=60000;")) {
            sender.table("alpha").longColumn("v", 1).atNow();
            sender.table("bravo").longColumn("v", 2).atNow();
            sender.reset();

            sender.table("bravo").longColumn("v", 3).atNow();
            sender.table("charlie").longColumn("v", 4).atNow();
            sender.flush();
            awaitDataFrames(frames, 1);
            byte[] frame = frames.get(0);
            Assert.assertEquals(2, tableCount(frame));
            Assert.assertEquals("alpha's rows were reset", -1, indexOf(frame, "alpha"));
            Assert.assertTrue("bravo, still listed from before reset(), ships", indexOf(frame, "bravo") >= 0);
            Assert.assertTrue("charlie ships", indexOf(frame, "charlie") >= 0);
        }
    }

    private static void awaitDataFrames(List<byte[]> frames, int n) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (frames.size() < n && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        Assert.assertTrue("expected " + n + " data frame(s), saw " + frames.size(), frames.size() >= n);
    }

    private static int indexOf(byte[] frame, String ascii) {
        int n = ascii.length();
        for (int i = 0, last = frame.length - n; i <= last; i++) {
            int j = 0;
            while (j < n && frame[i + j] == (byte) ascii.charAt(j)) {
                j++;
            }
            if (j == n) {
                return i;
            }
        }
        return -1;
    }

    private static TestWebSocketServer startServer(List<byte[]> frames, int advertisedMaxBatchSize) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            private long seq;

            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                if (tableCount(data) > 0) {
                    frames.add(data);
                }
                // STATUS_OK ack: status, cumulative wire sequence, table count.
                byte[] ack = new byte[11];
                ByteBuffer.wrap(ack).order(ByteOrder.LITTLE_ENDIAN).put((byte) 0).putLong(seq++).putShort((short) 0);
                try {
                    client.sendBinary(ack);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        if (advertisedMaxBatchSize > 0) {
            server.setAdvertisedMaxBatchSize(advertisedMaxBatchSize);
        }
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    // QWP message header: 'QWP1', version, flags, then the table count (u16 LE).
    private static int tableCount(byte[] message) {
        return message.length < 8 ? 0 : (message[6] & 0xFF) | (message[7] & 0xFF) << 8;
    }

    private static String wideValue(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + i % 26));
        }
        return sb.toString();
    }
}
