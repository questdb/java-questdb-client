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
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The producer-side dictionary cap ({@code MAX_SYMBOL_DICTIONARY_SIZE}) as the
 * application sees it: {@code symbol()} with a value that would create the
 * 2,000,001st distinct entry throws BEFORE the row is buffered, the row is
 * cancellable, and the sender keeps working with already-registered values --
 * the wire never carries the refused symbol.
 */
public class DeltaDictCeilingTest {

    @Test
    public void testSymbolPastCapThrowsAndSenderStaysUsable() throws Exception {
        assertMemoryLeak(() -> {
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port + ";")) {
                    // Drive the dictionary to exactly the cap through the test
                    // accessor -- the guard itself is unit-tested; this test is
                    // about the row-API contract around it.
                    GlobalSymbolDictionary dict =
                            ((QwpWebSocketSender) sender).getGlobalSymbolDictionaryForTest();
                    for (int i = 0; i < MAX_SYMBOL_DICTIONARY_SIZE; i++) {
                        dict.getOrAddSymbol("f" + i);
                    }

                    sender.table("t").symbol("s", "f0").longColumn("v", 1L).atNow();

                    try {
                        sender.table("t").symbol("s", "one-too-many");
                        Assert.fail("expected LineSenderException past the dictionary cap");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage().contains(String.valueOf(MAX_SYMBOL_DICTIONARY_SIZE)));
                    }
                    Assert.assertEquals("the refusal must not have grown the dictionary",
                            MAX_SYMBOL_DICTIONARY_SIZE, dict.size());
                    sender.cancelRow();

                    sender.table("t").symbol("s", "f1").longColumn("v", 2L).atNow();
                    sender.flush();
                    waitFor(() -> handler.dict.size() >= 2, 5_000);
                }

                // The delta on the wire carries ONLY the ids the rows used (f0, f1):
                // the refused symbol never reached the wire, and neither did the
                // prefilled tail above the used ids.
                List<String> wireDict = new ArrayList<>(handler.dict);
                Assert.assertEquals(2, wireDict.size());
                Assert.assertEquals("f0", wireDict.get(0));
                Assert.assertEquals("f1", wireDict.get(1));
            }
        });
    }

    /**
     * A threshold configured AT the cap, with automatic reset DISABLED, must
     * behave exactly like the undecorated cap: the refusal still fires, and
     * its message still names the reset valve even though this particular
     * sender has it switched off -- the valve is documented for senders that
     * want it, not conditioned on this sender having chosen it.
     * <p>
     * Out of scope here: whether {@code symbol_dict_reset=off} actually keeps
     * {@code armIfEligible()} from arming. That only runs from the tail of a
     * completed {@code flush()}, which this test never performs (the fill
     * goes through the raw dictionary test accessor, and the one
     * {@code Sender}-routed call throws inside {@code symbol()} before a row
     * completes) -- an {@code isResetArmed()} assertion here would pass
     * regardless of the knob, proving nothing. That arming-vs-flush property
     * is pinned in {@code SymbolDictRecycleArmingTest.testArmsAtThreshold}.
     */
    @Test
    public void testCapReachedWithResetDisabledStillThrowsAndNamesTheResetValve() throws Exception {
        assertMemoryLeak(() -> {
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + port
                        + ";symbol_dict_reset=off;symbol_dict_reset_threshold=" + MAX_SYMBOL_DICTIONARY_SIZE + ";")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    GlobalSymbolDictionary dict = ws.getGlobalSymbolDictionaryForTest();
                    for (int i = 0; i < MAX_SYMBOL_DICTIONARY_SIZE; i++) {
                        dict.getOrAddSymbol("f" + i);
                    }

                    try {
                        sender.table("t").symbol("s", "one-too-many");
                        Assert.fail("expected LineSenderException past the dictionary cap");
                    } catch (LineSenderException expected) {
                        String message = expected.getMessage();
                        Assert.assertTrue("message names the limit: " + message,
                                message.contains(String.valueOf(MAX_SYMBOL_DICTIONARY_SIZE)));
                        Assert.assertTrue("message points at the reset valve: " + message,
                                message.contains("symbol_dict_reset") && message.contains("resetSymbolDictionary()"));
                    }
                }
            }
        });
    }

    private static void waitFor(Condition condition, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (!condition.holds()) {
            if (System.currentTimeMillis() > deadline) {
                Assert.fail("condition not met within " + timeoutMillis + "ms");
            }
            Thread.sleep(10);
        }
    }

    private interface Condition {
        boolean holds() throws Exception;
    }

    /**
     * Accumulates every delta-dict entry it sees and ACKs every frame.
     */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        final List<String> dict = new CopyOnWriteArrayList<>();
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                QwpWireTestUtils.accumulateDeltaDictionary(data, dict);
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
