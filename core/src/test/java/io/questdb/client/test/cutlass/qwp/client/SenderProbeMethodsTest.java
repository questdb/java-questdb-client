package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.test.QuestDBFlushOnCloseTest;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The three probe methods the pool uses at release: {@code checkError()},
 * {@code hasRowInProgress()} and {@code pendingRowCount()}. Exercised on a
 * real WebSocket sender against the in-repo test server, which acks every
 * frame.
 */
public class SenderProbeMethodsTest {

    @Test
    public void testDeferredFlushShipsFlaggedFrameAndFlushCommits() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                Assert.assertEquals(0L, sender.rowsAppended());
                Assert.assertFalse(sender.hasDeferredFrames());
                Assert.assertEquals(-1L, sender.flushDeferred());          // nothing pending

                sender.table("t").longColumn("v", 1L).atNow();
                Assert.assertEquals(1L, sender.rowsAppended());
                Assert.assertTrue(sender.flushDeferred() >= 0L);
                Assert.assertTrue("a deferred frame is open", sender.hasDeferredFrames());
                Assert.assertEquals(0, sender.pendingRowCount());

                sender.table("t").longColumn("v", 2L).atNow();
                sender.flush();                                            // closes the group
                Assert.assertFalse(sender.hasDeferredFrames());
                Assert.assertEquals(2L, sender.rowsAppended());

                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals("no separate commit message: the committing frame carries the rows", 0, log.commitFrames());
                Assert.assertTrue("first frame carries FLAG_DEFER_COMMIT", log.deferFlag(0));
                Assert.assertFalse("committing frame is unflagged", log.deferFlag(1));
            }
        }
    }

    @Test
    public void testHealthySenderReportsBufferState() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.checkError();                       // healthy: no throw
                Assert.assertFalse(sender.hasRowInProgress());
                Assert.assertEquals(0, sender.pendingRowCount());

                sender.table("t").longColumn("v", 1L);     // row started, not committed
                Assert.assertTrue(sender.hasRowInProgress());
                Assert.assertEquals(0, sender.pendingRowCount());

                sender.cancelRow();
                Assert.assertFalse(sender.hasRowInProgress());

                sender.table("t").longColumn("v", 2L).atNow();
                Assert.assertFalse(sender.hasRowInProgress());
                Assert.assertEquals(1, sender.pendingRowCount());

                sender.flush();
                Assert.assertEquals(0, sender.pendingRowCount());
            }
        }
    }

    @Test
    public void testCheckErrorThrowsLatchedTerminal() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
            server.setRejectWithStatus(401, "Unauthorized");
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // async connect: build() returns, the I/O thread hits the 401 wall and latches a terminal
            try (Sender sender = Sender.fromConfig(config(server.getPort())
                    + "initial_connect_retry=async;reconnect_max_duration_millis=300;"
                    + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;")) {
                long deadline = System.currentTimeMillis() + 5_000;
                LineSenderException seen = null;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        sender.checkError();
                    } catch (LineSenderException e) {
                        seen = e;
                        break;
                    }
                    Thread.sleep(20);
                }
                Assert.assertNotNull("checkError() must throw the latched auth terminal", seen);
            }
        }
    }

    @Test
    public void testDefaultsOnNonWebSocketTransport() {
        try (Sender sender = Sender.fromConfig("http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;")) {
            sender.checkError();                            // default: no-op
            Assert.assertFalse(sender.hasRowInProgress());  // default: false
            Assert.assertEquals(0, sender.pendingRowCount()); // default: 0
            Assert.assertEquals(0L, sender.rowsAppended());
            Assert.assertFalse(sender.hasDeferredFrames());
            try {
                sender.flushDeferred();
                Assert.fail("expected UnsupportedOperationException");
            } catch (UnsupportedOperationException expected) {
            }
        }
    }

    private static String config(int port) {
        return "ws::addr=localhost:" + port + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=0;";
    }

    private static final class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq++));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
