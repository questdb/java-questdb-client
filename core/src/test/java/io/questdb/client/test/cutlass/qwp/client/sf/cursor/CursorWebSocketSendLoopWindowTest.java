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

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The commit window as the send loop applies it: window members go out
 * deferred until a bound or a close request closes their group, a quiet group
 * closes with a synthesised commit-only frame, and a ring frame's wire form
 * differs from its stored bytes only in the flags byte. The window-member
 * marker never reaches the wire.
 * <p>
 * Harness: a file-backed engine, a loop whose reconnect factory makes a
 * {@link ServerModelClient} per connection over one {@link QwpServerModel}, and
 * frames from {@link WindowTestFrames}. Tests construct the loop, set the
 * window, append and only then start it, because frames published before
 * construction count as recovered and go out as stored.
 * <p>
 * The rejection tests build the loop the way the sender does and report errors
 * to a {@link SenderErrorDispatcher} with a recording handler; error FSNs are
 * external with base 0, so they equal the ring FSNs.
 */
public class CursorWebSocketSendLoopWindowTest {
    private static final int DEFER = QwpConstants.FLAG_DEFER_COMMIT;
    private static final int MARKER = QwpConstants.FLAG_WINDOW_MEMBER;
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
    public void testAckedFsnStaysExactAcrossSynthesisedCommits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(1000, 20);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    loop.start();
                    awaitAckedFsn(engine, 0);
                    assertEquals("the synthesised commit acks the member below it", 0, engine.ackedFsn());

                    WindowTestFrames.append(engine, MARKER, 1, 1);
                    WindowTestFrames.append(engine, MARKER, 1, 2);
                    awaitAckedFsn(engine, 2);
                    assertEquals(2, engine.ackedFsn());

                    WindowTestFrames.append(engine, 0, 1, 3);
                    awaitAckedFsn(engine, 3);
                    assertEquals(3, engine.ackedFsn());

                    assertEquals(Arrays.asList(0L, 1L, 2L, 3L), committedIds(model));
                    assertEquals(4, dataFrames(model).size());
                    int commitOnly = commitOnlyFrames(model);
                    assertTrue("at least two synthesised commits, got " + commitOnly, commitOnly >= 2);
                    assertEquals(commitOnly, loop.getTotalCommitFramesSynthesised());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testAgeBoundNeverCommitsInsideASplitRelease() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(1000, 20);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    WindowTestFrames.append(engine, DEFER, 1, 1);
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitFrames(model, 2);
                    Thread.sleep(100);

                    assertEquals("no commit while a stored deferred frame is open", 0, commitOnlyFrames(model));
                    assertEquals(-1, engine.ackedFsn());
                    List<byte[]> ring = ringPayloads(segmentBefore);
                    assertArrayEquals("the deferred frame goes out as stored", ring.get(1), model.frames().get(1));

                    WindowTestFrames.append(engine, MARKER, 1, 2);
                    awaitAckedFsn(engine, 2, 1_000);
                    assertEquals(1, model.commits());
                    assertEquals(Arrays.asList(0L, 1L, 2L), committedIds(model));
                    assertEquals(0, loop.getTotalCommitFramesSynthesised());
                    assertWireShape(ringPayloads(activeSegmentBytes(engine)), model.frames(), 1, 0, 0);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testCloseRequestCommitsAQuietGroupImmediately() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    WindowTestFrames.append(engine, MARKER, 1, 1);
                    loop.start();
                    awaitFrames(model, 2);
                    assertEquals(-1, engine.ackedFsn());

                    loop.requestGroupClose(1);
                    awaitAckedFsn(engine, 1, 1_000);
                    assertEquals(1, engine.ackedFsn());
                    assertEquals(1, loop.getTotalGroupsClosedOnDemand());
                    assertEquals(1, loop.getTotalCommitFramesSynthesised());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testDelayedAckBeforeATerminalRejectionDuplicatesOnlyTheCommittedGroup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    WindowTestFrames.id(frame) == 4 ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
            // The server commits group [0, 2] on connection 0, but its ack reaches the
            // loop only after the NACK of frame 4, which the replay outruns.
            model.delayAckUntilNextNack(0, 2);
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model, clients, true,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    loop.setCommitWindow(3, 0);
                    for (int i = 0; i < 5; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();
                    awaitFrames(model, 5);
                    // member 5 closes the group [3, 5] by rows before the loop sees the NACK
                    WindowTestFrames.append(engine, MARKER, 1, 5);
                    awaitFrames(model, 6);
                    clients.get(0).holdResponses(false);

                    SenderError error = awaitLatchedServerError(loop);
                    assertEquals("the duplicate is exactly the group whose ack was late",
                            Arrays.asList(0L, 1L, 2L, 0L, 1L, 2L, 3L), committedIds(model));
                    assertEquals(4, error.getFromFsn());
                    assertEquals(4, error.getToFsn());
                    assertEquals(5, engine.publishedFsn());
                    assertEquals(3, engine.ackedFsn());
                    assertFalse("frame 5 stays in the ring", committedIds(model).contains(5L));
                    assertEquals(2, model.connections());
                    assertEquals(1, loop.getTotalTerminalReplays());
                } finally {
                    loop.close();
                    dispatcher.close();
                }
                assertEquals(1, errors.size());
            }
        });
    }

    @Test
    public void testDurableAckModeSendsMembersAsStored() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        () -> new ServerModelClient(model),
                        1L, 5L,
                        true,
                        0L, // no durable-ack PING: the model client has no socket
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        0L, 0L,
                        CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND,
                        0L);
                try {
                    loop.setCommitWindow(3, 0);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitFrames(model, 3);

                    assertWireShape(ringPayloads(segmentBefore), model.frames().subList(0, 3), 0, 0, 0);
                    assertEquals(0, loop.getTotalFramesDeferred());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testFirstTableRowCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertEquals("two tables of 2 and 5 rows", 2, rowCountOf(qwpFrame(0, 2,
                    1, 'a', 2, 0,
                    1, 'b', 5, 0)));
            assertEquals("a delta section of two symbols", 7, rowCountOf(qwpFrame(QwpConstants.FLAG_DELTA_SYMBOL_DICT, 1,
                    0, 2, 3, 'f', 'o', 'o', 2, 'b', 'a',
                    1, 't', 7, 0)));
            assertEquals("truncated inside the table name", -1, rowCountOf(qwpFrame(0, 1,
                    5, 't', 'a')));
            assertEquals("table-less", -1, rowCountOf(qwpFrame(QwpConstants.FLAG_DELTA_SYMBOL_DICT, 0,
                    0, 0)));
            byte[] notQwp = qwpFrame(0, 1, 1, 't', 3, 0);
            notQwp[0] = 'X';
            assertEquals("not QWP", -1, rowCountOf(notQwp));
        });
    }

    @Test
    public void testMarkerNeverReachesTheWire() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 2);

                    List<byte[]> wire = model.frames();
                    List<byte[]> ring = ringPayloads(segmentBefore);
                    assertEquals(3, ring.size());
                    for (int i = 0; i < 3; i++) {
                        assertNotEquals("ring frame " + i + " carries the marker",
                                0, QwpServerModel.flags(ring.get(i)) & MARKER);
                        assertEquals(i, WindowTestFrames.id(wire.get(i)));
                    }
                    assertWireShape(ring, wire, 0, 0, 0);
                    assertArrayEquals("the loop never writes to ring memory",
                            segmentBefore, activeSegmentBytes(engine));
                    assertEquals(3, model.commits());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testQuietGroupClosesWithASynthesisedCommit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, clients);
                try {
                    loop.setCommitWindow(1000, 50);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    WindowTestFrames.append(engine, MARKER, 1, 1);
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 1, 2_000);
                    assertEquals(1, engine.ackedFsn());

                    List<byte[]> wire = model.frames();
                    assertEquals(3, wire.size());
                    assertWireShape(ringPayloads(segmentBefore), wire.subList(0, 2), 1, 1);
                    assertArrayEquals("the synthesised commit",
                            syntheticCommit(clients.get(0).getServerQwpVersion()), wire.get(2));
                    assertEquals(1, loop.getTotalCommitFramesSynthesised());
                    assertEquals(1, loop.getTotalGroupsClosedByAge());
                    assertEquals(2, loop.getTotalFramesDeferred());
                    assertEquals(1, model.commits());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testReconnectMidGroupReplaysCommittingWithoutDuplicates() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model, clients);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();
                    awaitFrames(model, 3);
                    assertEquals("the group is open", 0, model.commits());

                    clients.get(0).dropConnection();
                    awaitCommittedIds(model, 3);
                    assertEquals(Arrays.asList(0L, 1L, 2L), committedIds(model));
                    awaitAckedFsn(engine, 2);
                    List<byte[]> wire = model.frames();
                    List<Integer> connections = model.frameConnections();
                    assertEquals(6, wire.size());
                    for (int i = 3; i < 6; i++) {
                        assertEquals("replayed on connection 1", 1, (int) connections.get(i));
                        assertEquals("replay " + i + " commits and is unmarked",
                                0, QwpServerModel.flags(wire.get(i)) & (DEFER | MARKER));
                    }

                    WindowTestFrames.append(engine, MARKER, 1, 3);
                    awaitFrames(model, 7);
                    wire = model.frames();
                    assertEquals(3, WindowTestFrames.id(wire.get(6)));
                    assertEquals(1, (int) model.frameConnections().get(6));
                    assertEquals("member 3 goes out deferred",
                            DEFER, QwpServerModel.flags(wire.get(6)) & (DEFER | MARKER));

                    loop.requestGroupClose(3);
                    awaitAckedFsn(engine, 3);
                    assertEquals(3, engine.ackedFsn());
                    assertEquals(Arrays.asList(0L, 1L, 2L, 3L), committedIds(model));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRecoveredMembersReplayCommitting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                for (int i = 0; i < 3; i++) {
                    WindowTestFrames.append(engine, MARKER, 1, i);
                }
            }
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    loop.start();
                    awaitAckedFsn(engine, 2);

                    List<byte[]> wire = model.frames();
                    assertEquals(3, wire.size());
                    for (int i = 0; i < 3; i++) {
                        assertEquals("recovered frame " + i + " commits and is unmarked",
                                0, QwpServerModel.flags(wire.get(i)) & (DEFER | MARKER));
                    }
                    assertEquals(3, model.commits());
                    assertEquals(0, loop.getTotalFramesDeferred());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRetriableRejectionInsideAGroupStrikesAsOnMaster() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    WindowTestFrames.id(frame) == 2 ? WebSocketResponse.STATUS_INTERNAL_ERROR : 0);
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model,
                        new CopyOnWriteArrayList<ServerModelClient>(), false, 3, dispatcher);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();

                    SenderError error = awaitLatchedServerError(loop);
                    assertEquals("the replays commit the good frames, each once",
                            Arrays.asList(0L, 1L), committedIds(model));
                    assertEquals(SenderError.Category.PROTOCOL_VIOLATION, error.getCategory());
                    assertEquals(2, error.getFromFsn());
                    assertEquals("one strike per connection", 3, model.connections());
                    assertEquals(0, loop.getTotalTerminalReplays());
                } finally {
                    loop.close();
                    dispatcher.close();
                }
            }
        });
    }

    @Test
    public void testRowBoundLeavesTheRingBytesAlone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(3, 0);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 2);
                    assertArrayEquals("the loop never writes to ring memory",
                            segmentBefore, activeSegmentBytes(engine));
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testRowBoundSendsDeferredFramesThenOneCommittingFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(3, 0);
                    for (int i = 0; i < 6; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 5);
                    assertEquals(5, engine.ackedFsn());

                    assertWireShape(ringPayloads(segmentBefore), model.frames(), 1, 1, 0, 1, 1, 0);
                    assertEquals(2, model.commits());
                    assertEquals(4, loop.getTotalFramesDeferred());
                    assertEquals(2, loop.getTotalGroupsClosedByRows());
                    assertEquals(0, loop.getTotalCommitFramesSynthesised());
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testTerminalNackAfterTheGroupClosedStillReplays() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    WindowTestFrames.id(frame) == 1 ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model, clients, true,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    loop.setCommitWindow(3, 0);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();
                    awaitFrames(model, 3);
                    assertEquals("frame 2 closed the group before the NACK arrived",
                            1, loop.getTotalGroupsClosedByRows());
                    assertEquals(0, QwpServerModel.flags(model.frames().get(2)) & DEFER);
                    clients.get(0).holdResponses(false);

                    SenderError error = awaitLatchedServerError(loop);
                    assertEquals(1, loop.getTotalTerminalReplays());
                    assertEquals(Arrays.asList(0L), committedIds(model));
                    assertEquals(1, error.getFromFsn());
                    assertEquals(1, error.getToFsn());
                    assertEquals(0, engine.ackedFsn());
                    assertFalse("frame 2 stays in the ring", committedIds(model).contains(2L));
                } finally {
                    loop.close();
                    dispatcher.close();
                }
                assertEquals(1, errors.size());
            }
        });
    }

    @Test
    public void testTerminalRejectionAcceptedOnReplayLatchesNothing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    connection == 0 && WindowTestFrames.id(frame) == 2 ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model,
                        new CopyOnWriteArrayList<ServerModelClient>(), false,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();
                    awaitAckedFsn(engine, 2);

                    assertEquals(Arrays.asList(0L, 1L, 2L), committedIds(model));
                    assertNull(loop.getTerminalError());
                    assertEquals(2, model.connections());
                    assertEquals(1, loop.getTotalTerminalReplays());
                } finally {
                    loop.close();
                    dispatcher.close();
                }
                assertEquals("nothing is dispatched", 0, errors.size());
            }
        });
    }

    @Test
    public void testTerminalRejectionInsideAGroupReplaysOnceThenLatchesTheBadFrameAlone() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    WindowTestFrames.id(frame) == 2 ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model,
                        new CopyOnWriteArrayList<ServerModelClient>(), false,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    for (int i = 0; i < 3; i++) {
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    loop.start();

                    SenderError error = awaitLatchedServerError(loop);
                    assertEquals(SenderError.Category.SCHEMA_MISMATCH, error.getCategory());
                    assertEquals(2, error.getFromFsn());
                    assertEquals(2, error.getToFsn());
                    assertEquals(Arrays.asList(0L, 1L), committedIds(model));
                    assertEquals(1, engine.ackedFsn());
                    assertEquals(2, model.connections());
                    assertEquals(1, loop.getTotalTerminalReplays());
                } finally {
                    loop.close();
                    dispatcher.close();
                }
                assertEquals("the error is dispatched once, at the replay's rejection", 1, errors.size());
                assertEquals(2, errors.get(0).getFromFsn());
            }
        });
    }

    @Test
    public void testTerminalRejectionOfASynthesisedCommitReplaysTheGroup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    connection == 0 && QwpServerModel.tableCount(frame) == 0 ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model,
                        new CopyOnWriteArrayList<ServerModelClient>(), false,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    loop.setCommitWindow(1000, 200);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    WindowTestFrames.append(engine, MARKER, 1, 1);
                    loop.start();
                    awaitAckedFsn(engine, 1);

                    // The NACK names the commit-only frame, which maps to FSN 1, the last
                    // ring frame before it and inside the closed span [0, 1].
                    assertEquals(Arrays.asList(0L, 1L), committedIds(model));
                    assertNull(loop.getTerminalError());
                    assertEquals(1, loop.getTotalTerminalReplays());
                    assertEquals(1, loop.getTotalCommitFramesSynthesised());
                    List<byte[]> wire = model.frames();
                    List<Integer> connections = model.frameConnections();
                    assertEquals(5, wire.size());
                    assertEquals("the rejected commit-only frame", 0, QwpServerModel.tableCount(wire.get(2)));
                    for (int i = 3; i < 5; i++) {
                        assertEquals("replayed on connection 1", 1, (int) connections.get(i));
                        assertEquals(i - 3, WindowTestFrames.id(wire.get(i)));
                        assertEquals("replay " + i + " goes out as stored, committing",
                                0, QwpServerModel.flags(wire.get(i)) & (DEFER | MARKER));
                    }
                } finally {
                    loop.close();
                    dispatcher.close();
                }
                assertEquals("nothing is dispatched", 0, errors.size());
            }
        });
    }

    @Test
    public void testTerminalRejectionOutsideAGroupLatchesAtOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) ->
                    WindowTestFrames.id(frame) == 0 ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newRejectingLoop(engine, model,
                        new CopyOnWriteArrayList<ServerModelClient>(), false,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    loop.setCommitWindow(1, 0);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    loop.start();

                    SenderError error = awaitLatchedServerError(loop);
                    assertEquals("member 0 went out committing",
                            0, QwpServerModel.flags(model.frames().get(0)) & DEFER);
                    assertEquals(0, error.getFromFsn());
                    assertEquals(0, error.getToFsn());
                    assertEquals(1, model.connections());
                    assertEquals(0, loop.getTotalTerminalReplays());
                } finally {
                    loop.close();
                    dispatcher.close();
                }
                assertEquals(1, errors.size());
            }
        });
    }

    @Test
    public void testTrickleBelowRowBoundCommitsByAgeAtSendTime() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(1000, 50);
                    loop.start();
                    for (int i = 0; i < 40; i++) {
                        if (i > 0) {
                            Thread.sleep(10);
                        }
                        WindowTestFrames.append(engine, MARKER, 1, i);
                    }
                    int commits = model.commits();
                    assertTrue("the age bound commits a trickle, got " + commits + " commit(s)", commits >= 3);
                } finally {
                    loop.close();
                }
            }
        });
    }

    @Test
    public void testUnmarkedCommittingFrameClosesTheGroup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = newLoop(engine, model);
                try {
                    loop.setCommitWindow(1000, 60_000);
                    WindowTestFrames.append(engine, MARKER, 1, 0);
                    WindowTestFrames.append(engine, MARKER, 1, 1);
                    WindowTestFrames.append(engine, 0, 1, 2);
                    byte[] segmentBefore = activeSegmentBytes(engine);
                    loop.start();
                    awaitAckedFsn(engine, 2);
                    assertEquals(2, engine.ackedFsn());

                    assertWireShape(ringPayloads(segmentBefore), model.frames(), 1, 1, 0);
                    assertEquals(1, loop.getTotalGroupsClosedByFrame());
                    assertEquals(1, model.commits());
                    assertEquals(Arrays.asList(0L, 1L, 2L), committedIds(model));
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
        awaitAckedFsn(engine, target, 10_000);
    }

    private static void awaitAckedFsn(CursorSendEngine engine, long target, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (engine.ackedFsn() < target) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for ackedFsn", target, engine.ackedFsn());
            }
            Thread.sleep(1);
        }
    }

    private static void awaitCommittedIds(QwpServerModel model, int count) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (committedIds(model).size() < count) {
            if (System.nanoTime() > deadline) {
                assertEquals("timed out waiting for committed frames", count, committedIds(model).size());
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

    private static SenderError awaitLatchedServerError(CursorWebSocketSendLoop loop) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (loop.getTerminalError() == null) {
            if (System.nanoTime() > deadline) {
                fail("timed out waiting for the terminal error");
            }
            Thread.sleep(1);
        }
        Throwable error = loop.getTerminalError();
        assertTrue("a server rejection latches, got " + error, error instanceof LineSenderServerException);
        return ((LineSenderServerException) error).getServerError();
    }

    // Table-less committing frames: the commits the loop synthesised.
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

    // Ids of the committed data frames, in commit order; commit-only frames carry none.
    private static List<Long> committedIds(QwpServerModel model) {
        List<Long> ids = new ArrayList<>();
        List<byte[]> committed = model.committedFrames();
        for (int i = 0, n = committed.size(); i < n; i++) {
            long id = WindowTestFrames.id(committed.get(i));
            if (id >= 0) {
                ids.add(id);
            }
        }
        return ids;
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

    private static CursorWebSocketSendLoop newLoop(CursorSendEngine engine, QwpServerModel model) {
        return newLoop(engine, model, new ArrayList<ServerModelClient>());
    }

    private static CursorWebSocketSendLoop newLoop(
            CursorSendEngine engine,
            QwpServerModel model,
            List<ServerModelClient> clients
    ) {
        return new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    ServerModelClient client = new ServerModelClient(model);
                    clients.add(client);
                    return client;
                },
                1L, 5L);
    }

    // The loop as the sender builds it, with the given strike threshold, no
    // escalation dwell, and the dispatcher; with holdFirstConnection, connection 0
    // queues its responses until the test releases them.
    private static CursorWebSocketSendLoop newRejectingLoop(
            CursorSendEngine engine,
            QwpServerModel model,
            List<ServerModelClient> clients,
            boolean holdFirstConnection,
            int maxFrameRejections,
            SenderErrorDispatcher dispatcher
    ) {
        CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                null, engine, 0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    ServerModelClient client = new ServerModelClient(model);
                    if (holdFirstConnection && client.connection() == 0) {
                        client.holdResponses(true);
                    }
                    clients.add(client);
                    return client;
                },
                1L, 5L,
                false,
                0L, // no durable-ack PING: the model client has no socket
                maxFrameRejections,
                0L, 0L,
                CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND,
                0L);
        loop.setErrorDispatcher(dispatcher);
        return loop;
    }

    // A QWP frame: magic, version 1, the flags, the table count, the payload length, then the body.
    private static byte[] qwpFrame(int flags, int tableCount, int... body) {
        byte[] frame = new byte[QwpConstants.HEADER_SIZE + body.length];
        frame[0] = 'Q';
        frame[1] = 'W';
        frame[2] = 'P';
        frame[3] = '1';
        frame[4] = 1;
        frame[QwpConstants.HEADER_OFFSET_FLAGS] = (byte) flags;
        frame[6] = (byte) tableCount;
        frame[7] = (byte) (tableCount >>> 8);
        frame[8] = (byte) body.length;
        frame[9] = (byte) (body.length >>> 8);
        for (int i = 0; i < body.length; i++) {
            frame[QwpConstants.HEADER_SIZE + i] = (byte) body[i];
        }
        return frame;
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

    private static long rowCountOf(byte[] frame) {
        long addr = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.length; i++) {
                Unsafe.getUnsafe().putByte(addr + i, frame[i]);
            }
            return CursorWebSocketSendLoop.firstTableRowCountForTest(addr, frame.length);
        } finally {
            Unsafe.free(addr, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    // The 14-byte commit-only frame the loop synthesises: version, gorilla and
    // delta flags, no table, an empty delta section.
    private static byte[] syntheticCommit(int version) {
        return new byte[]{
                'Q', 'W', 'P', '1',
                (byte) version,
                QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT,
                0, 0,
                2, 0, 0, 0,
                0, 0
        };
    }
}
