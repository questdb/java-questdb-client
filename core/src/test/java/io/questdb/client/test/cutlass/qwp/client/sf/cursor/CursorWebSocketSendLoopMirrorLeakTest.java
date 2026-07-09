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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Guards the recovery-seeded symbol-dictionary mirror against leaking when the
 * I/O loop is constructed but never run.
 * <p>
 * On recovery / orphan-drain the {@link CursorWebSocketSendLoop} constructor
 * seeds a native mirror ({@code sentDictBytesAddr}) from the slot's persisted
 * dictionary so the first connection can re-register it. That mirror is freed on
 * the I/O thread's exit path -- so if the loop is closed WITHOUT ever starting
 * (start() never called, or Thread.start() failing before the loop runs), the
 * free never happens. {@code close()} must free it in that case.
 */
public class CursorWebSocketSendLoopMirrorLeakTest {

    private static final int DISTINCT_SYMBOLS = 8;
    private static final int ROWS = 40;

    @Test
    public void testSeededMirrorFreedWhenLoopClosedWithoutStart() throws Exception {
        Path sfDir = Files.createTempDirectory("qwp-mirror-leak");
        try {
            // Populate a slot with delta frames + a non-empty .symbol-dict, then
            // abandon it (silent server, close-fast) -- outside assertMemoryLeak,
            // because a full Sender+server round trip is not net-zero on its own.
            populateRecoverableSlot(sfDir);

            Path slot = sfDir.resolve("default");
            Assert.assertTrue("populate must leave a persisted dictionary",
                    Files.exists(slot.resolve(".symbol-dict")));

            // Only the recovery construct + close is leak-checked: the engine
            // recovers (loading the dict), the loop ctor seeds the mirror from it,
            // and close() -- with NO start() -- must free every native allocation.
            // Pre-fix the seeded mirror leaks here and this assertion fails.
            assertMemoryLeak(() -> {
                try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
                    PersistedSymbolDict pd = engine.getPersistedSymbolDict();
                    Assert.assertNotNull("disk-mode engine must open a persisted dict", pd);
                    Assert.assertTrue("recovery must load the persisted symbols (seeds the mirror)",
                            pd.size() > 0 && pd.loadedEntriesLen() > 0);

                    CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                            null, engine, 0, 1_000_000L,
                            () -> {
                                throw new IOException("no reconnect in this test");
                            },
                            0, 0, 1);
                    // Close without start(): the ctor-seeded mirror is this
                    // thread's to free, since the I/O loop never ran.
                    loop.close();
                }
            });
        } finally {
            rmDir(sfDir);
        }
    }

    private static void populateRecoverableSlot(Path sfDir) throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sf_max_bytes=4096"
                    + ";close_flush_timeout_millis=0;";
            try (Sender s1 = Sender.fromConfig(cfg)) {
                for (int i = 0; i < ROWS; i++) {
                    s1.table("m")
                            .symbol("s", "sym-" + (i % DISTINCT_SYMBOLS))
                            .longColumn("v", i)
                            .atNow();
                    s1.flush();
                }
            }
        }
    }

    private static void rmDir(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                        // best-effort
                    }
                });
    }

    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks -- the sender leaves everything unacked in the slot
        }
    }
}
