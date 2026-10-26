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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.QuestDBFlushOnCloseTest;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Which ring frames the sender marks as commit-window members. Only the
 * committing frame that {@link QwpWebSocketSender#flushAsWindowMember()} itself
 * publishes, with a window set and outside transactional, deferred and
 * durable-ack modes, carries {@code FLAG_WINDOW_MEMBER}; the send loop clears
 * the bit before the wire.
 */
public class WindowMemberMarkerTest {
    private static final int DEFER = QwpConstants.FLAG_DEFER_COMMIT;
    private static final int MARKER = QwpConstants.FLAG_WINDOW_MEMBER;

    @Test
    public void testAutoFlushInsideTheLeaseIsNotMarked() throws Exception {
        withSender("auto_flush_rows=2;", 0, (s, log) -> {
            s.setCommitWindow(1000, 100);
            for (int i = 0; i < 3; i++) {
                row(s, "t", i);
            }
            s.flushAsWindowMember();
            Assert.assertEquals(2, ringFrameCount(s));
            Assert.assertEquals("the auto-flush frame is unmarked", 0, ringFlags(s, 0) & MARKER);
            Assert.assertNotEquals("the release frame is marked", 0, ringFlags(s, 1) & MARKER);
        });
    }

    @Test
    public void testFlushIsNeverMarked() throws Exception {
        withSender("", 0, (s, log) -> {
            s.setCommitWindow(1000, 100);
            row(s, "t", 1);
            s.flush();
            Assert.assertEquals(0, ringFlags(s, 0) & MARKER);
        });
    }

    @Test
    public void testReleaseFrameIsMarkedInTheRingAndStrippedOnTheWire() throws Exception {
        withSender("", 0, (s, log) -> {
            s.setCommitWindow(1000, 100);
            row(s, "t", 1);
            s.flushAsWindowMember();
            Assert.assertNotEquals("the ring copy carries the marker", 0, ringFlags(s, 0) & MARKER);
            Assert.assertEquals("the ring copy is committing", 0, ringFlags(s, 0) & DEFER);

            log.awaitDataFrames(1, 5_000);
            byte[] wire = firstDataFrame(log);
            Assert.assertEquals("the marker never reaches the wire",
                    0, wire[QwpConstants.HEADER_OFFSET_FLAGS] & MARKER);
        });
    }

    @Test
    public void testReleaseFrameIsPlainWithoutAWindow() throws Exception {
        withSender("", 0, (s, log) -> {
            row(s, "t", 1);
            s.flushAsWindowMember();
            Assert.assertEquals(0, ringFlags(s, 0) & MARKER);
        });
    }

    @Test
    public void testSplitReleaseMarksOnlyItsClosingFrame() throws Exception {
        // auto_flush_bytes=off: the default byte trigger (90 percent of the
        // advertised cap) would ship table t1 before the release, and the batch
        // would never reach the split.
        withSender("auto_flush_bytes=off;", 4 * 1024, (s, log) -> {
            s.setCommitWindow(1000, 100);
            String pad = new String(new char[100]).replace('\0', 'x');
            for (int i = 0; i < 30; i++) {
                s.table("t1").stringColumn("p", pad).longColumn("v", i).atNow();
            }
            for (int i = 0; i < 30; i++) {
                s.table("t2").stringColumn("p", pad).longColumn("v", i).atNow();
            }
            s.flushAsWindowMember();
            int frames = ringFrameCount(s);
            Assert.assertTrue("the release must split, got " + frames + " frame(s)", frames >= 2);
            for (int i = 0; i < frames - 1; i++) {
                int flags = ringFlags(s, i);
                Assert.assertNotEquals("leading frame " + i + " is deferred", 0, flags & DEFER);
                Assert.assertEquals("leading frame " + i + " is unmarked", 0, flags & MARKER);
            }
            int last = ringFlags(s, frames - 1);
            Assert.assertNotEquals("the closing frame is marked", 0, last & MARKER);
            Assert.assertEquals("the closing frame commits", 0, last & DEFER);
        });
    }

    @Test
    public void testTransactionalSenderIsNeverMarked() throws Exception {
        withSender("transaction=on;", 0, (s, log) -> {
            s.setCommitWindow(1000, 100);
            for (int i = 0; i < 3; i++) {
                row(s, "t", i);
            }
            s.flushAsWindowMember();
            int frames = ringFrameCount(s);
            Assert.assertTrue(frames >= 1);
            for (int i = 0; i < frames; i++) {
                Assert.assertEquals("ring frame " + i, 0, ringFlags(s, i) & MARKER);
            }
        });
    }

    private static byte[] firstDataFrame(QuestDBFlushOnCloseTest.FrameLog log) {
        List<byte[]> frames = log.frames();
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] frame = frames.get(i);
            if (QwpWireTestUtils.tableCount(frame) > 0) {
                return frame;
            }
        }
        throw new AssertionError("no data frame on the wire");
    }

    private static int ringFlags(QwpWebSocketSender s, int index) {
        MmapSegment segment = s.getCursorEngineForTesting().activeSegment();
        long offset = ringFrameOffset(segment, index);
        Assert.assertTrue("the ring holds no frame " + index, offset >= 0);
        long payload = segment.address() + offset + MmapSegment.FRAME_HEADER_SIZE;
        return Unsafe.getUnsafe().getByte(payload + QwpConstants.HEADER_OFFSET_FLAGS) & 0xFF;
    }

    private static int ringFrameCount(QwpWebSocketSender s) {
        MmapSegment segment = s.getCursorEngineForTesting().activeSegment();
        int count = 0;
        while (ringFrameOffset(segment, count) >= 0) {
            count++;
        }
        return count;
    }

    // Offset of ring frame `index` in the active segment ([u32 crc][u32 len][payload]...),
    // or -1 when the segment holds fewer frames.
    private static long ringFrameOffset(MmapSegment segment, int index) {
        long base = segment.address();
        long published = segment.publishedOffset();
        long offset = MmapSegment.HEADER_SIZE;
        for (int i = 0; offset + MmapSegment.FRAME_HEADER_SIZE <= published; i++) {
            if (i == index) {
                return offset;
            }
            offset += MmapSegment.FRAME_HEADER_SIZE + Unsafe.getUnsafe().getInt(base + offset + 4);
        }
        return -1;
    }

    private static void row(Sender s, String table, long value) {
        s.table(table).longColumn("v", value).atNow();
    }

    private static void withSender(String extraConfig, int maxBatchSize, SenderBody body) throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = new TestWebSocketServer(log)) {
                if (maxBatchSize > 0) {
                    server.setAdvertisedMaxBatchSize(maxBatchSize);
                }
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";" + extraConfig)) {
                    body.run((QwpWebSocketSender) sender, log);
                }
            }
        });
    }

    private interface SenderBody {
        void run(QwpWebSocketSender sender, QuestDBFlushOnCloseTest.FrameLog log) throws Exception;
    }
}
