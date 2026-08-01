package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * End-to-end proof of the tandem's load-bearing claim for STATUS_DICTIONARY_GAP
 * (0x0D): a real gap NACK recycles the wire and replays -- it does not
 * terminate the sender, does not advance the ack watermark past the rejected
 * frame, and a single gap never escalates to the poison terminal.
 */
public class DictionaryGapNackTest {

    @Test
    public void testSingleDictionaryGapNackRecyclesAndReplaysWithoutDataLoss() throws Exception {
        assertMemoryLeak(() -> {
            GapOnceHandler handler = new GapOnceHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Small reconnect backoff so the paced recycle does not slow the
                // test; the poison threshold stays at its default (4) because "no
                // escalation on a single gap" is part of what is under test.
                String cfg = "ws::addr=localhost:" + port
                        + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;";
                QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig(cfg);
                try {
                    sender.table("t").symbol("s", "alpha").longColumn("v", 1L).atNow();
                    long targetFsn = sender.flushAndGetSequence();

                    // The double NACKs this exact frame once with STATUS_DICTIONARY_GAP,
                    // then ACKs the byte-identical replay on the recycled connection.
                    Assert.assertTrue("the client must recover and drain after a single gap NACK",
                            sender.awaitAckedFsn(targetFsn, 10_000));

                    // Not terminal: no latched terminal error, watermark advanced.
                    Assert.assertNull("a single DICTIONARY_GAP must never latch a terminal",
                            sender.getLastTerminalError());
                    Assert.assertEquals(targetFsn, sender.getAckedFsn());

                    // The NACK was surfaced (informational dispatch counts it) and
                    // recycled the wire: a fresh WS upgrade happened.
                    Assert.assertTrue("the NACK must be counted as a server error",
                            sender.getTotalServerErrors() >= 1);
                    Assert.assertTrue("the NACK must force a fresh WS upgrade",
                            server.handshakeCount() >= 2);
                    Assert.assertTrue("the loop must count a successful reconnect",
                            sender.getTotalReconnectsSucceeded() >= 1);

                    // Replay: the frame reached the wire at least twice (once NACKed,
                    // once ACKed), while the server-side dictionary materialised the
                    // symbol exactly once and gap-free.
                    Assert.assertTrue("the frame must be retransmitted after the NACK",
                            handler.dataFramesSeen.get() >= 2);
                    Assert.assertEquals("the double must have sent exactly one gap NACK",
                            1, handler.gapNacksSent.get());
                    List<String> dict = handler.finalDict();
                    Assert.assertEquals("post-recycle dictionary must hold the symbol once",
                            1, dict.size());
                    Assert.assertEquals("alpha", dict.get(0));
                } finally {
                    sender.close();
                }
            }
        });
    }

    /**
     * ACKs everything except the FIRST data frame (tableCount > 0) it ever sees,
     * which it NACKs exactly once with STATUS_DICTIONARY_GAP -- modelling a server
     * whose per-connection dictionary lagged the delta the frame assumed. Catch-up
     * frames (tableCount == 0) and every later frame are ACKed normally, per-
     * connection ack sequences restarting at 0 like the real server's.
     */
    private static class GapOnceHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger dataFramesSeen = new AtomicInteger();
        final AtomicInteger gapNacksSent = new AtomicInteger();
        private TestWebSocketServer.ClientHandler currentClient;
        private final List<String> dict = new ArrayList<String>();
        private boolean gapDelivered;
        private long nextSeq;

        synchronized List<String> finalDict() {
            return new ArrayList<String>(dict);
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            if (QwpWireTestUtils.tableCount(data) > 0) {
                dataFramesSeen.incrementAndGet();
                if (!gapDelivered) {
                    gapDelivered = true;
                    gapNacksSent.incrementAndGet();
                    try {
                        client.sendBinary(QwpWireTestUtils.buildNack(
                                nextSeq++, WebSocketResponse.STATUS_DICTIONARY_GAP));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                }
            }
            try {
                QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq++));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
