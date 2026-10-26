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
import io.questdb.client.test.cutlass.qwp.websocket.CommitModelHandler;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The commit window through the WebSocket sender, against a server that
 * commits like a real one ({@link CommitModelHandler}): every call that waits
 * for acks, or that must not leave rows uncommitted, closes an open window
 * group instead of waiting for its bounds.
 */
public class CommitWindowSenderTest {
    private static final int DEFER = QwpConstants.FLAG_DEFER_COMMIT;
    private static final int MARKER = QwpConstants.FLAG_WINDOW_MEMBER;
    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testAwaitAckedFsnInsideAnOpenWindowReturnsPromptly() throws Exception {
        withSender((s, model) -> {
            s.setCommitWindow(1000, 60_000);
            row(s, 1);
            s.flushAsWindowMember();
            long start = System.nanoTime();
            Assert.assertTrue(s.awaitAckedFsn(0, 5_000));
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue("awaitAckedFsn took " + millis + " ms", millis < 2_000);
            Assert.assertEquals(1, model.commits());
        });
    }

    @Test
    public void testCloseInsideAnOpenWindowCommitsAndReturnsPromptly() throws Exception {
        assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (TestWebSocketServer server = startServer(model)) {
                Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort() + ";");
                long millis;
                try {
                    QwpWebSocketSender s = (QwpWebSocketSender) sender;
                    s.setCommitWindow(1000, 60_000);
                    row(s, 1);
                    s.flushAsWindowMember();
                } finally {
                    long start = System.nanoTime();
                    sender.close();
                    millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                }
                Assert.assertTrue("close() took " + millis + " ms", millis < 2_000);
                Assert.assertEquals(1, model.committedRows());
            }
        });
    }

    @Test
    public void testDrainInsideAnOpenWindowReturnsWithinTheFlushTimeout() throws Exception {
        withSender((s, model) -> {
            s.setCommitWindow(1000, 60_000);
            row(s, 1);
            s.flushAsWindowMember();
            long start = System.nanoTime();
            Assert.assertTrue(s.drain(5_000));
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue("drain took " + millis + " ms", millis < 2_000);
            Assert.assertEquals(1, model.commits());
        });
    }

    @Test
    public void testRecycleClosesAnOpenRowsOnlyGroup() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-rows-only-window").toString();
            QwpServerModel model = new QwpServerModel();
            try (TestWebSocketServer server = startServer(model)) {
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir
                        + ";symbol_dict_reset_threshold=2;";
                long rows = 0;
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender s = (QwpWebSocketSender) sender;
                    s.setCommitWindow(1000, 0);
                    String[] symbols = {"a", "b", "c"};
                    for (String symbol : symbols) {
                        s.table("t").symbol("s", symbol).longColumn("v", rows++).atNow();
                        s.flushAsWindowMember();
                    }
                    // A rows-only window never closes these members by itself; only the
                    // close request at the recycle barrier lets the ring drain.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (s.getSymbolDictEpoch() != 1) {
                        Assert.assertTrue("the recycle must run within 5 s", System.nanoTime() < deadline);
                        row(s, rows++);
                        s.flushAsWindowMember();
                        Thread.sleep(50);
                    }
                    Assert.assertFalse("the recycle disarms", s.isResetArmed());
                }
                Assert.assertEquals(rows, model.committedRows());
            }
        });
    }

    @Test
    public void testRowlessFlushClosesTheGroup() throws Exception {
        withSender((s, model) -> {
            s.setCommitWindow(1000, 60_000);
            row(s, 1);
            s.flushAsWindowMember();
            // the member must be on the wire, deferred, before the rowless flush asks for the commit
            awaitDataFrames(model, 1);
            Assert.assertEquals(0, model.commits());
            s.flush();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (model.commits() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            Assert.assertEquals(1, model.commits());
            Assert.assertEquals("the model saw a commit-only frame", 1, commitOnlyFrames(model));
        });
    }

    @Test
    public void testWireCarriesDeferredMembersAndACommittingFlush() throws Exception {
        withSender((s, model) -> {
            s.setCommitWindow(1000, 60_000);
            row(s, 1);
            s.flushAsWindowMember();
            row(s, 2);
            s.flush();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (model.commits() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            List<byte[]> data = dataFrames(model);
            Assert.assertEquals(2, data.size());
            Assert.assertEquals("the member goes out deferred, without the marker",
                    (ringFlags(s, 0) & ~MARKER) | DEFER, QwpServerModel.flags(data.get(0)));
            Assert.assertEquals("the flush goes out as stored",
                    ringFlags(s, 1), QwpServerModel.flags(data.get(1)));
            Assert.assertEquals(0, QwpServerModel.flags(data.get(1)) & (DEFER | MARKER));
            Assert.assertEquals(1, model.commits());
            Assert.assertEquals(2, model.committedRows());
            Assert.assertEquals(1, s.getTotalFramesDeferred());
            Assert.assertEquals(1, s.getTotalGroupsClosedByFrame());
        });
    }

    private static void awaitDataFrames(QwpServerModel model, int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (dataFrames(model).size() < count) {
            Assert.assertTrue("timed out waiting for " + count + " data frame(s)", System.nanoTime() < deadline);
            Thread.sleep(1);
        }
    }

    private static int commitOnlyFrames(QwpServerModel model) {
        int count = 0;
        List<byte[]> frames = model.frames();
        for (int i = 0, n = frames.size(); i < n; i++) {
            byte[] frame = frames.get(i);
            if (QwpServerModel.tableCount(frame) == 0 && (QwpServerModel.flags(frame) & DEFER) == 0) {
                count++;
            }
        }
        return count;
    }

    private static List<byte[]> dataFrames(QwpServerModel model) {
        List<byte[]> data = new ArrayList<>();
        List<byte[]> frames = model.frames();
        for (int i = 0, n = frames.size(); i < n; i++) {
            if (QwpServerModel.tableCount(frames.get(i)) > 0) {
                data.add(frames.get(i));
            }
        }
        return data;
    }

    // Flags byte of ring frame `index` in the active segment ([u32 crc][u32 len][payload]...).
    private static int ringFlags(QwpWebSocketSender s, int index) {
        MmapSegment segment = s.getCursorEngineForTesting().activeSegment();
        long base = segment.address();
        long published = segment.publishedOffset();
        long offset = MmapSegment.HEADER_SIZE;
        for (int i = 0; offset + MmapSegment.FRAME_HEADER_SIZE <= published; i++) {
            if (i == index) {
                long payload = base + offset + MmapSegment.FRAME_HEADER_SIZE;
                return Unsafe.getUnsafe().getByte(payload + QwpConstants.HEADER_OFFSET_FLAGS) & 0xFF;
            }
            offset += MmapSegment.FRAME_HEADER_SIZE + Unsafe.getUnsafe().getInt(base + offset + 4);
        }
        throw new AssertionError("the ring holds no frame " + index);
    }

    private static void row(Sender s, long value) {
        s.table("t").longColumn("v", value).atNow();
    }

    private static TestWebSocketServer startServer(QwpServerModel model) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new CommitModelHandler(model));
        try {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            return server;
        } catch (Throwable t) {
            server.close();
            throw t;
        }
    }

    private static void withSender(SenderBody body) throws Exception {
        assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (TestWebSocketServer server = startServer(model)) {
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort() + ";")) {
                    body.run((QwpWebSocketSender) sender, model);
                }
            }
        });
    }

    private interface SenderBody {
        void run(QwpWebSocketSender sender, QwpServerModel model) throws Exception;
    }
}
