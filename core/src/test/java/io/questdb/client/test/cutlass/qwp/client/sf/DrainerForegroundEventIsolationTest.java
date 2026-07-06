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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Contract: background orphan-slot drainers are invisible in the foreground
 * sender's connection-event stream. {@link SenderConnectionEvent}s describe
 * the FOREGROUND connection's lifecycle — the documented meaning a monitoring
 * integration depends on: {@code CONNECTED} fires once when the sender first
 * comes up, {@code RECONNECTED}/{@code FAILED_OVER} fire when the sender's own
 * connection was re-established, {@code DISCONNECTED} fires when the sender's
 * own connection dropped. A drainer connecting, reconnecting after a wire
 * drop, or failing over is background bookkeeping for an orphan slot and must
 * not masquerade as foreground lifecycle transitions.
 * <p>
 * Both tests are black-box: real {@code Sender} built from config, real
 * {@link TestWebSocketServer}, events captured through the public
 * {@code connectionListener} builder hook. They do not care HOW drainer
 * connects are isolated from foreground state — any implementation that keeps
 * drainer activity out of the user-visible event stream passes.
 * <p>
 * Barriers: the drain outcome is awaited via the public drainer counters
 * before close; sender close drains the event-dispatcher inbox before
 * returning, so post-close assertions observe the complete delivered stream;
 * {@code getDroppedConnectionNotifications() == 0} guards the
 * absence-assertions against inbox-overflow false greens.
 */
public class DrainerForegroundEventIsolationTest {

    private static final int GHOST_ROWS = 5;

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-drainer-event-iso-" + System.nanoTime()).toString();
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    /**
     * A drainer's successful connect must not fire a foreground success event.
     * The foreground connects exactly once against a healthy server and never
     * drops, so the event stream must contain exactly one success-kind event:
     * the initial {@code CONNECTED}. A second success-kind event means the
     * drainer's connect leaked into the foreground lifecycle stream (today it
     * surfaces as a fabricated {@code RECONNECTED}/{@code FAILED_OVER} while
     * the foreground connection never went away).
     */
    @Test
    public void testDrainerConnectMustNotFireForegroundSuccessEvents() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedGhostSlot();

            RecordingListener events = new RecordingListener();
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + server.getPort()
                        + ";sf_dir=" + sfDir
                        + ";sender_id=primary"
                        + ";drain_orphans=true"
                        + ";max_background_drainers=1;";
                try (Sender sender = Sender.builder(cfg)
                        .connectionListener(events)
                        .build()) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    awaitDrainSuccess(ws, handler.distinctPayloads, 10_000);
                    Assert.assertEquals(
                            "absence-assertions require a lossless event stream",
                            0, ws.getDroppedConnectionNotifications());
                }
                // Sender is closed: the dispatcher inbox has been drained, the
                // captured list is the complete delivered stream.
                List<SenderConnectionEvent> successes = events.ofKinds(
                        SenderConnectionEvent.Kind.CONNECTED,
                        SenderConnectionEvent.Kind.RECONNECTED,
                        SenderConnectionEvent.Kind.FAILED_OVER);
                Assert.assertEquals(
                        "background drainer connects must be invisible in the "
                                + "foreground connection-event stream; expected the "
                                + "initial CONNECTED only, got: " + successes,
                        1, successes.size());
                Assert.assertEquals(
                        "the single success event must be the foreground's "
                                + "first-connect CONNECTED",
                        SenderConnectionEvent.Kind.CONNECTED,
                        successes.get(0).getKind());
            }
        });
    }

    /**
     * A drainer's mid-drain wire drop must not fire a foreground
     * {@code DISCONNECTED}. The server deterministically drops the drainer's
     * first connection after acking one frame; the drainer reconnects and
     * finishes the slot. The foreground connection is healthy for the whole
     * test (it never sends and is never dropped), so a {@code DISCONNECTED}
     * in the stream is a phantom: it reports an outage, against an endpoint
     * the foreground is healthily using, that the foreground never had.
     */
    @Test
    public void testDrainerWireDropMustNotFirePhantomForegroundDisconnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            seedGhostSlot();

            RecordingListener events = new RecordingListener();
            DropFirstDataConnectionHandler handler = new DropFirstDataConnectionHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + server.getPort()
                        + ";sf_dir=" + sfDir
                        + ";sender_id=primary"
                        + ";drain_orphans=true"
                        + ";max_background_drainers=1;";
                try (Sender sender = Sender.builder(cfg)
                        .connectionListener(events)
                        .build()) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    awaitDrainSuccess(ws, handler.distinctPayloads, 15_000);
                    // Fixture sanity: the drain really did span a wire drop —
                    // at least two distinct data connections served frames.
                    Assert.assertTrue(
                            "expected the drainer to reconnect after the scripted "
                                    + "drop; data connections=" + handler.dataConnections(),
                            handler.dataConnections() >= 2);
                    Assert.assertEquals(
                            "absence-assertions require a lossless event stream",
                            0, ws.getDroppedConnectionNotifications());
                }
                List<SenderConnectionEvent> disconnects = events.ofKinds(
                        SenderConnectionEvent.Kind.DISCONNECTED);
                Assert.assertEquals(
                        "a background drainer's wire drop must not surface as a "
                                + "foreground DISCONNECTED — the foreground connection "
                                + "never dropped; got: " + disconnects,
                        0, disconnects.size());
            }
        });
    }

    // Ghost sender against a silent server leaves an unacked orphan slot with
    // GHOST_ROWS frames under the group root (same recipe as
    // BackgroundDrainerEndToEndTest).
    private void seedGhostSlot() throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + silent.getPort()
                    + ";sf_dir=" + sfDir
                    + ";sender_id=ghost"
                    + ";close_flush_timeout_millis=0;";
            try (Sender g = Sender.fromConfig(cfg)) {
                for (int i = 0; i < GHOST_ROWS; i++) {
                    g.table("foo").longColumn("v", i).atNow();
                    g.flush();
                }
            }
        }
        Assert.assertEquals("ghost slot must be a candidate orphan",
                1, OrphanScanner.scan(sfDir, "primary").size());
    }

    private static void awaitDrainSuccess(
            QwpWebSocketSender ws,
            java.util.Set<String> distinctPayloads,
            long timeoutMillis
    ) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline
                && (distinctPayloads.size() < GHOST_ROWS
                || ws.getTotalBackgroundDrainersSucceeded() < 1)) {
            Thread.sleep(20);
        }
        Assert.assertEquals("drainer must replay every ghost-slot row",
                GHOST_ROWS, distinctPayloads.size());
        Assert.assertEquals("drainer must drain the slot fully and exit cleanly",
                1, ws.getTotalBackgroundDrainersSucceeded());
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

    // status OK + wire seq + tableCount 0 — the minimal ack the non-durable
    // drain path consumes (same shape as BackgroundDrainerEndToEndTest).
    private static byte[] buildAck(long wireSeq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00);
        bb.putLong(wireSeq);
        bb.putShort((short) 0);
        return buf;
    }

    /** Captures every delivered event for post-close exact assertions. */
    private static final class RecordingListener implements SenderConnectionListener {
        private final List<SenderConnectionEvent> captured = new ArrayList<>();

        @Override
        public synchronized void onEvent(@NotNull SenderConnectionEvent event) {
            captured.add(event);
        }

        synchronized List<SenderConnectionEvent> ofKinds(SenderConnectionEvent.Kind... kinds) {
            List<SenderConnectionEvent> out = new ArrayList<>();
            for (int i = 0, n = captured.size(); i < n; i++) {
                SenderConnectionEvent e = captured.get(i);
                for (SenderConnectionEvent.Kind k : kinds) {
                    if (e.getKind() == k) {
                        out.add(e);
                        break;
                    }
                }
            }
            return out;
        }
    }

    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally no ack
        }
    }

    /**
     * Acks every frame with a per-connection wire sequence. The foreground
     * connection never sends data in these tests, so only drainer connections
     * show up here.
     */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        final java.util.Set<String> distinctPayloads =
                java.util.Collections.synchronizedSet(new java.util.HashSet<>());
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            distinctPayloads.add(java.util.Arrays.toString(data));
            long[] counter = wireSeqByConn.get(client);
            if (counter == null) {
                counter = new long[1];
                wireSeqByConn.put(client, counter);
            }
            try {
                client.sendBinary(buildAck(counter[0]++));
            } catch (IOException ignored) {
                // best-effort: connection may be racing its own close
            }
        }
    }

    /**
     * Deterministic mid-drain wire drop. The first connection that sends a
     * binary frame (the drainer — the foreground never sends in these tests)
     * gets exactly one frame acked, then the server closes its socket on the
     * next frame. Every later connection acks all traffic with a
     * per-connection wire sequence, so the reconnected drain runs to
     * completion. State is keyed per {@code ClientHandler} identity: a dead
     * connection's reader can deliver late buffered frames after a newer
     * connection started, and those must neither ack with a stale counter nor
     * disturb the live connection (same discipline as
     * BackgroundDrainerMidDrainCapabilityGapTest's GapScenarioHandler).
     */
    private static class DropFirstDataConnectionHandler
            implements TestWebSocketServer.WebSocketServerHandler {
        final java.util.Set<String> distinctPayloads =
                java.util.Collections.synchronizedSet(new java.util.HashSet<>());
        private final List<TestWebSocketServer.ClientHandler> arrivalOrder = new ArrayList<>();
        private final java.util.Map<TestWebSocketServer.ClientHandler, long[]> wireSeqByConn =
                new java.util.IdentityHashMap<>();

        synchronized int dataConnections() {
            return arrivalOrder.size();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            distinctPayloads.add(java.util.Arrays.toString(data));
            long[] counter = wireSeqByConn.get(client);
            if (counter == null) {
                counter = new long[1];
                wireSeqByConn.put(client, counter);
                arrivalOrder.add(client);
            }
            boolean firstConnection = arrivalOrder.get(0) == client;
            long seq = counter[0]++;
            try {
                if (firstConnection) {
                    if (seq == 0) {
                        client.sendBinary(buildAck(seq));
                    } else if (seq == 1) {
                        client.close(); // mid-drain wire drop
                    }
                    // seq > 1: late buffered frames from the condemned
                    // connection; ignore.
                } else {
                    client.sendBinary(buildAck(seq));
                }
            } catch (IOException ignored) {
                // best-effort: the connection died under us; the drainer
                // replays on its next connection
            }
        }
    }
}
