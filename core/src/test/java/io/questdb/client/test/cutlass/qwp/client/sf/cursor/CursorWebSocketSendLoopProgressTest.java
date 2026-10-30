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

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.ProgressNotifier;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The signal sites of the progress notifier: every event an ack wait can be
 * waiting for bumps the generation. An acked-watermark advance and the terminal
 * error latch go through the I/O loop; the durability failure latch goes
 * through the ring. Harness as in {@link CursorWebSocketSendLoopBatchTest}.
 */
public class CursorWebSocketSendLoopProgressTest {
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-progress-" + System.nanoTime()).toString();
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
    public void testAckAdvancesTheGeneration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = CursorWebSocketSendLoopWindowTest.newRejectingLoop(
                        engine, model, clients, true,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    ProgressNotifier notifier = new ProgressNotifier();
                    loop.setProgressNotifier(notifier);
                    WindowTestFrames.append(engine, 0, 1, 0);
                    loop.start();
                    awaitFrames(model, 1);
                    long gen0 = notifier.generation();
                    assertTrue(engine.ackedFsn() < 0);
                    clients.get(0).holdResponses(false);
                    awaitAckedFsn(engine, 0);
                    assertTrue(notifier.generation() > gen0);
                } finally {
                    loop.close();
                    dispatcher.close();
                }
            }
        });
    }

    @Test
    public void testDurabilityFailureAdvancesTheGeneration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                ProgressNotifier notifier = new ProgressNotifier();
                engine.setProgressNotifier(notifier);
                long gen0 = notifier.generation();
                engine.getRingForTesting().recordDurabilityFailureForTesting(new IOException("sync failed"));
                assertEquals(gen0 + 1, notifier.generation());
                try {
                    engine.checkDurability();
                    fail("latched failure must throw");
                } catch (MmapSegmentException expected) {
                    // the latched failure surfaces to the producer
                }
            }
        });
    }

    @Test
    public void testTerminalRejectionAdvancesTheGeneration() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpServerModel model = new QwpServerModel();
            model.setRejector((connection, seq, frame) -> WebSocketResponse.STATUS_SCHEMA_MISMATCH);
            List<ServerModelClient> clients = new CopyOnWriteArrayList<>();
            List<SenderError> errors = new CopyOnWriteArrayList<>();
            SenderErrorDispatcher dispatcher = new SenderErrorDispatcher(errors::add, 16);
            try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 16384)) {
                CursorWebSocketSendLoop loop = CursorWebSocketSendLoopWindowTest.newRejectingLoop(
                        engine, model, clients, false,
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, dispatcher);
                try {
                    ProgressNotifier notifier = new ProgressNotifier();
                    loop.setProgressNotifier(notifier);
                    WindowTestFrames.append(engine, 0, 1, 0);
                    long gen0 = notifier.generation();
                    loop.start();
                    CursorWebSocketSendLoopWindowTest.awaitLatchedServerError(loop);
                    assertTrue(notifier.generation() > gen0);
                } finally {
                    loop.close();
                    dispatcher.close();
                }
            }
        });
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
}
