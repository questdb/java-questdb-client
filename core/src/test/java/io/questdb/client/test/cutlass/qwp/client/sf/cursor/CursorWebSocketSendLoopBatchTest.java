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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Network batching in the send loop: consecutive fully published ring frames go
 * out with one socket write, each still its own WebSocket message, while the
 * write stays within a 64 KiB budget counted with WebSocket headers. A frame
 * inside a batch gets the window decision and the dictionary handling the
 * single-frame path gives it: the mirror grows frame by frame, and the
 * torn-dictionary guard stops the batch before a gapped frame. A replay and a
 * client without batch support send one frame per write.
 * <p>
 * Harness as in {@link CursorWebSocketSendLoopWindowTest}, with a 4 MiB segment
 * for the 10 KiB and 100 KiB frames. Each test builds the loop, appends and only
 * then starts it, so the first pass finds every frame published. Each recorded
 * socket write carries its WebSocket headers (see {@link ServerModelClient}).
 */
public class CursorWebSocketSendLoopBatchTest {
    private static final int BUDGET = 64 * 1024;
    private static final int DEFER = QwpConstants.FLAG_DEFER_COMMIT;
    private static final int MARKER = QwpConstants.FLAG_WINDOW_MEMBER;
    private static final long SEGMENT_BYTES = 4L * 1024 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-batch-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) {
            return;
        }
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testBatchRespectsTheBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), true);
                try {
                    for (int i = 0; i < 20; i++) {
                        WindowTestFrames.append(engine, 0, 1, i, 10_240);
                    }
                    loop.start();
                    awaitAckedFsn(engine, 19);

                    assertEquals("all frames arrive in order", range(20), ids(model.frames()));
                    List<Integer> writes = model.socketWriteBytes();
                    for (int i = 0, n = writes.size(); i < n; i++) {
                        assertTrue("write " + i + " of " + writes.get(i) + " bytes exceeds the budget",
                                writes.get(i) <= BUDGET);
                    }
                    assertTrue("six 10 KiB frames fit a write, got " + loop.getTotalSocketWrites() + " write(s)",
                            loop.getTotalSocketWrites() >= 4);
                    assertEquals(writes.size(), loop.getTotalSocketWrites());
                    assertEquals(20, loop.getTotalFramesSent());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testClientWithoutBatchSupportSendsFrameByFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), false);
                try {
                    for (int i = 0; i < 10; i++) {
                        WindowTestFrames.append(engine, 0, 1, i);
                    }
                    loop.start();
                    awaitAckedFsn(engine, 9);

                    assertEquals(range(10), ids(model.frames()));
                    assertEquals(10, loop.getTotalSocketWrites());
                    assertEquals(10, model.socketWrites());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testConsecutiveFramesShareOneSocketWrite() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), true);
                try {
                    for (int i = 0; i < 50; i++) {
                        WindowTestFrames.append(engine, 0, 1, i);
                    }
                    loop.start();
                    awaitAckedFsn(engine, 49);

                    List<byte[]> frames = model.frames();
                    assertEquals(range(50), ids(frames));
                    assertEquals(1, loop.getTotalSocketWrites());
                    assertEquals(50, loop.getTotalFramesSent());
                    assertEquals(1, model.socketWrites());
                    assertEquals("the write carries every frame with its header",
                            wireBytes(frames, 0, 50), (int) model.socketWriteBytes().get(0));
                    assertEquals("one commit per frame", 50, model.commits());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testDictionaryDeltasExtendTheMirrorInsideABatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), true);
                try {
                    // each frame's delta starts where the previous one ended: frame i is
                    // sendable only once frame i - 1 has extended the mirror
                    appendDeltaFrame(engine, 0, 0, "a");
                    appendDeltaFrame(engine, 1, 1, "b", "c");
                    appendDeltaFrame(engine, 2, 3, "d");
                    loop.start();
                    awaitAckedFsn(engine, 2);

                    assertEquals(range(3), ids(model.frames()));
                    assertEquals("the mirror grows frame by frame inside the batch", 1, loop.getTotalSocketWrites());
                    assertEquals(4, loop.sentDictCount());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testFrameAboveTheBudgetGoesAlone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), true);
                try {
                    WindowTestFrames.append(engine, 0, 1, 0, 102_400);
                    for (int i = 1; i < 4; i++) {
                        WindowTestFrames.append(engine, 0, 1, i);
                    }
                    loop.start();
                    awaitAckedFsn(engine, 3);

                    List<byte[]> frames = model.frames();
                    assertEquals(range(4), ids(frames));
                    List<Integer> writes = model.socketWriteBytes();
                    assertEquals("the first write holds exactly the big frame",
                            wireBytes(frames, 0, 1), (int) writes.get(0));
                    assertEquals("the small frames share the next write", 2, writes.size());
                    assertEquals(wireBytes(frames, 1, 4), (int) writes.get(1));
                    assertEquals(2, loop.getTotalSocketWrites());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testReplayIsNotBatched() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, clients, true);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();
                    awaitFrames(model, 3);
                    assertEquals("the members share one write", 1, model.socketWrites());
                    assertEquals("the group is open", 0, model.commits());

                    clients.get(0).dropConnection();
                    awaitAckedFsn(engine, 2);

                    List<byte[]> frames = model.frames();
                    List<Integer> connections = model.frameConnections();
                    List<Integer> writes = model.socketWriteBytes();
                    assertEquals(6, frames.size());
                    assertEquals("one write on connection 0, then one per replayed frame", 4, writes.size());
                    for (int i = 3; i < 6; i++) {
                        assertEquals("replayed on connection 1", 1, (int) connections.get(i));
                        assertEquals(i - 3, WindowTestFrames.id(frames.get(i)));
                        assertEquals("replayed frame " + i + " goes alone",
                                wireBytes(frames, i, i + 1), (int) writes.get(i - 2));
                    }
                    assertEquals(3, loop.getTotalFramesReplayed());
                    assertEquals(4, loop.getTotalSocketWrites());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testTornDictionaryGuardStopsTheBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), true);
                try {
                    appendDeltaFrame(engine, 0, 0, "a");
                    appendDeltaFrame(engine, 1, 1, "b");
                    // ids 2..4 were never registered: the guard must keep this frame off the wire
                    appendDeltaFrame(engine, 2, 5, "f");
                    loop.start();
                    long deadline = System.nanoTime() + 10_000_000_000L;
                    while (loop.getTerminalError() == null) {
                        if (System.nanoTime() > deadline) {
                            fail("timed out waiting for the torn-dictionary terminal");
                        }
                        Thread.sleep(1);
                    }

                    String message = loop.getTerminalError().getMessage();
                    assertTrue(message, message.contains("incomplete") && message.contains("resend required"));
                    // the I/O thread frees the mirror on exit, so the wire is the witness
                    assertEquals("the batch stops before the gapped frame", range(2), ids(model.frames()));
                    assertEquals(1, model.socketWrites());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testWindowDecisionsHoldInsideABatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, SEGMENT_BYTES)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, new ArrayList<ServerModelClient>(), true);
                try {
                    loop.setCommitWindow(3, 0);
                    for (int i = 0; i < 6; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 5);

                    assertWireShape(ringPayloads(segmentBefore), model.frames(), 1, 1, 0, 1, 1, 0);
                    assertEquals(1, loop.getTotalSocketWrites());
                    assertEquals(2, model.commits());
                    assertEquals(4, loop.getTotalFramesDeferred());
                    assertEquals(2, loop.getTotalGroupsClosedByRows());
                } finally {
                    loop.close();
                }
            }
        });
    }

    private static byte[] activeSegmentBytes(CursorSendEngine engine) {
        MmapSegment segment = engine.activeSegment();
        long base = segment.address();
        int length = (int) segment.publishedOffset();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(base + i);
        }
        return bytes;
    }

    // Appends a committing frame laid out like WindowTestFrames' (one table "t" of
    // one row, no columns, the 8-byte id last), whose delta section registers the
    // symbols as ids deltaStart, deltaStart + 1, ...
    private static void appendDeltaFrame(CursorSendEngine engine, long id, int deltaStart, String... symbols) {
        List<Byte> body = new ArrayList<>();
        body.add((byte) deltaStart);
        body.add((byte) symbols.length);
        for (String symbol : symbols) {
            byte[] utf8 = symbol.getBytes(StandardCharsets.UTF_8);
            body.add((byte) utf8.length);
            for (byte b : utf8) {
                body.add(b);
            }
        }
        body.add((byte) 1); // table name length
        body.add((byte) 't');
        body.add((byte) 1); // rows
        body.add((byte) 0); // column count
        for (int i = 0; i < Long.BYTES; i++) {
            body.add((byte) (id >>> (8 * i)));
        }
        int size = QwpConstants.HEADER_SIZE + body.size();
        long buf = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(buf, QwpConstants.HEADER_SIZE, (byte) 0);
            Unsafe.getUnsafe().putByte(buf, (byte) 'Q');
            Unsafe.getUnsafe().putByte(buf + 1, (byte) 'W');
            Unsafe.getUnsafe().putByte(buf + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(buf + 3, (byte) '1');
            Unsafe.getUnsafe().putByte(buf + 4, (byte) 1); // version
            Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_OFFSET_FLAGS, (byte) QwpConstants.FLAG_DELTA_SYMBOL_DICT);
            Unsafe.getUnsafe().putByte(buf + 6, (byte) 1); // table count, little-endian
            Unsafe.getUnsafe().putByte(buf + 8, (byte) body.size()); // payload length, little-endian
            for (int i = 0, n = body.size(); i < n; i++) {
                Unsafe.getUnsafe().putByte(buf + QwpConstants.HEADER_SIZE + i, body.get(i));
            }
            engine.appendBlocking(buf, size);
        } finally {
            Unsafe.free(buf, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // Each sent frame equals its stored frame except the flags byte, which is the
    // stored one without the marker, plus FLAG_DEFER_COMMIT exactly where deferBits is 1.
    private static void assertWireShape(List<byte[]> ring, List<byte[]> wire, int... deferBits) {
        assertEquals("wire frame count", deferBits.length, wire.size());
        for (int i = 0; i < deferBits.length; i++) {
            byte[] stored = ring.get(i);
            byte[] sent = wire.get(i);
            assertEquals("wire frame " + i + " length", stored.length, sent.length);
            int expectedFlags = (QwpServerModel.flags(stored) & ~MARKER) | (deferBits[i] == 1 ? DEFER : 0);
            assertEquals("wire frame " + i + " flags", expectedFlags, QwpServerModel.flags(sent));
            for (int j = 0; j < stored.length; j++) {
                if (j != QwpConstants.HEADER_OFFSET_FLAGS) {
                    assertEquals("wire frame " + i + " byte " + j, stored[j], sent[j]);
                }
            }
        }
    }

    private static void awaitAckedFsn(CursorSendEngine engine, long target) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (engine.ackedFsn() < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for ackedFsn", target, engine.ackedFsn());
            }
            Thread.sleep(1);
        }
    }

    private static void awaitFrames(QwpServerModel model, int count) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (model.frames().size() < count) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for wire frames", count, model.frames().size());
            }
            Thread.sleep(1);
        }
    }

    private static List<Long> ids(List<byte[]> frames) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0, n = frames.size(); i < n; i++) {
            ids.add(WindowTestFrames.id(frames.get(i)));
        }
        return ids;
    }

    private static CursorWebSocketSendLoop newLoop(
            CursorSendEngine engine,
            QwpServerModel model,
            List<ServerModelClient> clients,
            boolean isBatchSendSupported
    ) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    ServerModelClient client = new ServerModelClient(model);
                    client.setBatchSendSupported(isBatchSendSupported);
                    clients.add(client);
                    return client;
                },
                1L, 5L);
    }

    private static List<Long> range(int count) {
        List<Long> ids = new ArrayList<>();
        for (long i = 0; i < count; i++) {
            ids.add(i);
        }
        return ids;
    }

    // Splits a copy of a segment ([header][u32 crc][u32 len][payload]...) into payloads.
    private static List<byte[]> ringPayloads(byte[] segment) {
        List<byte[]> payloads = new ArrayList<>();
        int offset = MmapSegment.HEADER_SIZE;
        while (offset + MmapSegment.FRAME_HEADER_SIZE <= segment.length) {
            int payloadLen = (segment[offset + 4] & 0xFF)
                    | ((segment[offset + 5] & 0xFF) << 8)
                    | ((segment[offset + 6] & 0xFF) << 16)
                    | ((segment[offset + 7] & 0xFF) << 24);
            int start = offset + MmapSegment.FRAME_HEADER_SIZE;
            byte[] payload = new byte[payloadLen];
            System.arraycopy(segment, start, payload, 0, payloadLen);
            payloads.add(payload);
            offset = start + payloadLen;
        }
        return payloads;
    }

    // Bytes frames [from, to) take on the wire: each with its masked WebSocket header.
    private static int wireBytes(List<byte[]> frames, int from, int to) {
        int bytes = 0;
        for (int i = from; i < to; i++) {
            int length = frames.get(i).length;
            bytes += WebSocketFrameWriter.headerSize(length, true) + length;
        }
        return bytes;
    }
}
