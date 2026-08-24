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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Fault-injects a failure into recycle step 7 (the deferred reconnect) via
 * {@link QwpWebSocketSender#setLoopStartFaultForTesting(Runnable)} and pins
 * that the failure does NOT latch the sender terminal.
 */
public class SymbolDictRecycleStep7FaultTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * Pins that a step-7 (reconnect) failure does NOT latch the sender
     * terminal: the swap has committed, the sender is coherent and merely
     * disconnected, and the next send retries the deferred setup. Review
     * round 3, finding C5 (the test the PR body credited was deleted by
     * commit 6793928a).
     */
    @Test
    public void testStep7FailureDoesNotLatchAndRecovers() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.newFolder("step7-c5").getAbsolutePath();
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server, sfDir))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long f1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(f1, 5_000));
                    Assert.assertTrue(ws.isResetArmed());

                    RuntimeException fault = new RuntimeException("injected step-7 fault");
                    ws.setLoopStartFaultForTesting(() -> {
                        throw fault;
                    });
                    try {
                        sender.table("t");
                        Assert.fail("the triggering table() must surface the step-7 failure");
                    } catch (LineSenderException e) {
                        Assert.assertSame(fault, findRootCause(e));
                    }
                    // The swap committed; the sender must NOT be terminal.
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());
                    ws.setLoopStartFaultForTesting(null);
                    // Next send retries the deferred setup and data flows again.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long f2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recovery batch must be acked",
                            sender.awaitAckedFsn(f2, 5_000));
                }
            }
        });
    }

    private static TestWebSocketServer ackingServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler());
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static String cfg(TestWebSocketServer server, String sfDir) {
        return "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir
                + ";symbol_dict_reset_threshold=2"
                + ";reconnect_initial_backoff_millis=20"
                + ";reconnect_max_backoff_millis=80;";
    }

    /** Follows {@code getCause()} to the deepest non-null cause. */
    private static Throwable findRootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /** ACKs every frame it receives; does not otherwise inspect the wire. */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
