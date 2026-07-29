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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * White-box coverage of the send loop's torn-dictionary guard
 * ({@code trySendOne}: {@code deltaStart > sentDictCount}) firing on a genuine gap.
 * <p>
 * That guard is the drainer path's SOLE defense: an orphan drainer adopts a slot
 * without running {@code Sender.build()}'s seed-time guard, so on a slot whose
 * registering frames were trimmed away and whose {@code .symbol-dict} was torn, the
 * send loop must detect the gap itself, ship ZERO frames, and latch a terminal --
 * never null-pad the hole on the server. The foreground sender is always quarantined
 * earlier (at build time), so this fire direction runs only here. Driven at the send
 * loop level against a frame-counting stub client so it exercises the guard
 * deterministically, without a real network connection.
 */
public class CursorWebSocketSendLoopTornDictGuardTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = TestUtils.createTmpDir("qdb-torn-dict-guard-");
    }

    @After
    public void tearDown() {
        // Recursive: this test builds the store-and-forward slot layout
        // (<dir>/default/... plus <dir>/.slot-locks/...), and the flat variant
        // cannot remove a non-empty subdirectory -- it also discards its result,
        // so the whole tree survived every run unnoticed.
        TestUtils.removeTmpDirRec(sfDir);
    }

    /**
     * DISABLED by the upstream merge, for the same reason as
     * {@code DeltaDictRecoveryTest.testFullyAckedTornSlotResumesInPlaceWithoutQuarantine}:
     * {@link #writeAndTearGappedSlot()} deletes {@code sf-initial.sfa} to model an
     * ack-driven trim, but since {@code SfManifest} landed a real trim durably
     * advances {@code headBase} before unlinking. Recovery now fails closed on the
     * missing head boundary inside {@code new CursorSendEngine(...)}, so the send
     * loop -- and therefore the torn-dict guard this test exists to pin -- is never
     * reached. The guard itself is unchanged by this merge.
     * <p>
     * Re-enable once the fixture can trim the head the way the manager does
     * (acknowledge, let the manager's trim pass advance the manifest, then tear
     * the dictionary).
     */
    @org.junit.Ignore("fixture no longer models an ack-driven trim; see javadoc")
    @Test
    public void testGuardFiresOnGenuineGapAndShipsNoFrame() throws Exception {
        assertMemoryLeak(() -> {
            writeAndTearGappedSlot();

            CountingClient client = new CountingClient();
            try (CursorSendEngine engine = new CursorSendEngine(sfDir + "/default", 16_384)) {
                // The persisted dict was torn away and the registering frames trimmed, so
                // the mirror cannot be seeded from either source: sentDictCount stays 0
                // while the first surviving frame's delta starts above it.
                CursorWebSocketSendLoop loop = newLoop(engine, client);
                try {
                    loop.setRunningForTest(true);
                    // Position at the first unsent frame exactly as the I/O loop does
                    // before its first send (no catch-up: the mirror is empty).
                    loop.positionCursorForStartForTest();

                    boolean sent = loop.trySendOneForTest();

                    Assert.assertFalse("the torn-dict guard must refuse to send the gapped frame", sent);
                    Assert.assertEquals("no frame may reach the server through a gap",
                            0, client.framesSent);
                    try {
                        loop.checkError();
                        Assert.fail("the guard must latch a terminal error");
                    } catch (LineSenderException e) {
                        Assert.assertTrue("unexpected terminal: " + e.getMessage(),
                                e.getMessage().contains("incomplete")
                                        && e.getMessage().contains("resend required"));
                    }
                } finally {
                    loop.close();
                }
            }
        });
    }

    private static int countSegmentFiles(Path dir) {
        File[] files = dir.toFile().listFiles();
        int n = 0;
        if (files != null) {
            for (File f : files) {
                if (f.getName().endsWith(".sfa")) {
                    n++;
                }
            }
        }
        return n;
    }

    // Constructs a recovery send loop that is never started -- the test drives
    // positionCursorForStart + trySendOne directly. The reconnect factory throws
    // because no reconnect is expected before the guard latches its terminal.
    private CursorWebSocketSendLoop newLoop(CursorSendEngine engine, WebSocketClient client) {
        return new CursorWebSocketSendLoop(
                client, engine, 0, 1_000_000L,
                () -> {
                    throw new IOException("no reconnect in this test");
                },
                0, 1);
    }

    // Writes 12 delta frames (each a new symbol) into the default slot across several
    // small segments, then makes the slot carry a GENUINE gap: trims the segment
    // holding the earliest ids (munmap + unlink, as SegmentManager does once they are
    // acked) and tears the .symbol-dict down to its header. The surviving frames' deltas
    // then start above ids nothing on disk still holds, so the mirror cannot be rebuilt.
    private void writeAndTearGappedSlot() throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port + ";sf_dir=" + sfDir
                    + ";sf_max_segment_bytes=256;close_flush_timeout_millis=0;";
            try (Sender s = Sender.fromConfig(cfg)) {
                for (int i = 0; i < 12; i++) {
                    s.table("m").symbol("s", "sym-" + i).longColumn("v", i).atNow();
                    s.flush();
                }
            }
        }
        Path slot = Paths.get(sfDir, "default");
        Assert.assertTrue("the frames must have rolled into more than one segment",
                countSegmentFiles(slot) > 1);
        Files.delete(slot.resolve("sf-initial.sfa"));
        Path dict = slot.resolve(".symbol-dict");
        Files.write(dict, Arrays.copyOf(Files.readAllBytes(dict), 8));
    }

    // Frame-counting stub transport: completes no real I/O. If the guard ever lets a
    // gapped frame through, sendBinary bumps framesSent and the test fails.
    private static final class CountingClient extends WebSocketClient {
        private int framesSent;

        CountingClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public int getServerMaxBatchSize() {
            return 16_384;
        }

        @Override
        public int getServerQwpVersion() {
            return 1;
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            framesSent++;
        }

        @Override
        protected void ioWait(int timeout, int op) {
        }

        @Override
        protected void setupIoWait() {
        }
    }

    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks -- the sender leaves everything unacked in the slot
        }
    }
}
