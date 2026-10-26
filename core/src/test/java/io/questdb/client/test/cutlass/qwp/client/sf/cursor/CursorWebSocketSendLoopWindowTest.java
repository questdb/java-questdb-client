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
import io.questdb.client.std.Files;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * The commit window as the send loop applies it: a ring frame's wire form
 * differs from its stored bytes only in the flags byte, and the window-member
 * marker never reaches the wire.
 * <p>
 * Harness: a file-backed engine, a loop whose reconnect factory makes a
 * {@link ServerModelClient} per connection over one {@link QwpServerModel}, and
 * frames from {@link WindowTestFrames}. Tests construct the loop before they
 * append, because frames published before construction count as recovered.
 */
public class CursorWebSocketSendLoopWindowTest {
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-window-" + System.nanoTime()).toString();
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
    public void testMarkerNeverReachesTheWire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, QwpConstants.FLAG_WINDOW_MEMBER, 1, i);
                    }
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 2);

                    List<byte[]> wire = model.frames();
                    assertEquals(3, wire.size());
                    List<byte[]> ring = ringPayloads(segmentBefore);
                    assertEquals(3, ring.size());
                    for (int i = 0; i < 3; i++) {
                        byte[] stored = ring.get(i);
                        byte[] sent = wire.get(i);
                        assertNotEquals("ring frame " + i + " carries the marker",
                                0, QwpServerModel.flags(stored) & QwpConstants.FLAG_WINDOW_MEMBER);
                        assertEquals("wire frame " + i + " has the marker cleared",
                                0, QwpServerModel.flags(sent) & QwpConstants.FLAG_WINDOW_MEMBER);
                        assertEquals("wire frame " + i + " commits",
                                0, QwpServerModel.flags(sent) & QwpConstants.FLAG_DEFER_COMMIT);
                        assertEquals("wire frame " + i + " length", stored.length, sent.length);
                        for (int j = 0; j < stored.length; j++) {
                            if (j != QwpConstants.HEADER_OFFSET_FLAGS) {
                                assertEquals("wire frame " + i + " byte " + j, stored[j], sent[j]);
                            }
                        }
                        assertEquals(i, WindowTestFrames.id(sent));
                    }
                    assertArrayEquals("the loop never writes to ring memory",
                            segmentBefore, activeSegmentBytes(engine));
                    assertEquals(3, model.commits());
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

    private static void awaitAckedFsn(CursorSendEngine engine, long target) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (engine.ackedFsn() < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for ackedFsn", target, engine.ackedFsn());
            }
            Thread.sleep(1);
        }
    }

    private static CursorWebSocketSendLoop newLoop(CursorSendEngine engine, QwpServerModel model) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> new ServerModelClient(model),
                1L, 5L);
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
}
