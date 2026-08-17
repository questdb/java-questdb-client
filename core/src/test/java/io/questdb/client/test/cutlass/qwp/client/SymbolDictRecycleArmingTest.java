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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Covers the arming half of the symbol-dictionary recycle feature:
 * {@code QwpWebSocketSender.armIfEligible()}, called at the tail of
 * {@code resetTableBuffersAfterFlush()}, and the manual advisory API
 * {@link Sender#resetSymbolDictionary()}.
 */
public class SymbolDictRecycleArmingTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testArmsAtThreshold() throws Exception {
        // threshold=3, send rows with symbols a,b -> flush -> not armed;
        // add c -> flush -> armed
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=3;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1).atNow();
                    sender.flush();
                    Assert.assertFalse(ws.isResetArmed());
                    sender.table("t").symbol("s", "c").longColumn("v", 1).atNow();
                    sender.flush();
                    Assert.assertTrue(ws.isResetArmed());
                }
            }
        });
    }

    /**
     * Decision 5: arming ignores {@code deltaDictEnabled}. Proving this by
     * crossing {@code symbol_dict_reset_threshold} while degraded (as
     * {@link #testArmsAtThreshold} does for the default delta-dict mode) would
     * additionally need a custom low threshold on a hand-built
     * {@code CursorSendEngine} carrying the fault-injecting {@code FilesFacade}
     * (fault injection requires bypassing {@code Sender.fromConfig}, which
     * offers no {@code FilesFacade} seam) -- reaching both together needs
     * {@code QwpWebSocketSender}'s widest 27-parameter {@code connect()}
     * overload. Substituting the manual {@link Sender#resetSymbolDictionary()}
     * advisory request for "cross the threshold" reaches the identical
     * {@code armIfEligible()} branch -- the other arm of the same {@code ||} --
     * through the same 9-parameter {@code connect()} overload
     * {@code MmapFaultDegradesTest} itself uses, with no loss of coverage:
     * {@code armIfEligible()} does not special-case either trigger on
     * {@code deltaDictEnabled}.
     */
    @Test
    public void testArmsInFullDictMode() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("arm-full-dict-sf").toString();
            String slot = Paths.get(sfDir, "default").toString();
            Assert.assertEquals(0, io.questdb.client.std.Files.mkdir(sfDir,
                    io.questdb.client.std.Files.DIR_MODE_DEFAULT));

            try (TestWebSocketServer server = ackingServer()) {
                int port = server.getPort();

                MmapFaultDictFacade ff = new MmapFaultDictFacade();
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
                QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 0, 0, 0L, null, false, engine);
                try {
                    ff.armed = true; // next dictionary mmap growth raises a recognised fault
                    sender.table("m").symbol("s", "boom").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("expected the injected mmap fault to fail this flush");
                    } catch (LineSenderException expected) {
                        // Same guard MmapFaultDegradesTest pins: the fault degrades the
                        // sender to full self-sufficient frames instead of propagating raw.
                    }
                    Assert.assertFalse("a recognised mmap access fault must degrade the sender "
                                    + "to full-dict mode",
                            sender.isDeltaDictEnabledForTest());

                    // The fault facade disarms itself after firing once, so this retry
                    // succeeds and clears pendingRowCount back to 0.
                    sender.flush();
                    Assert.assertFalse("neither threshold nor manual request has fired yet",
                            sender.isResetArmed());

                    sender.resetSymbolDictionary();
                    Assert.assertTrue("manual reset request must arm even in full-dict mode",
                            sender.isResetArmed());
                } finally {
                    sender.close();
                }
            }
        });
    }

    @Test
    public void testDoesNotArmWhenDisabled() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(
                        cfg(server) + "symbol_dict_reset=off;symbol_dict_reset_threshold=2;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertFalse("symbol_dict_reset=off must never arm", ws.isResetArmed());
                    sender.table("t").symbol("s", "c").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertFalse("symbol_dict_reset=off must never arm", ws.isResetArmed());
                }
            }
        });
    }

    @Test
    public void testManualResetRequestArms() throws Exception {
        assertMemoryLeak(() -> {
            // pendingRowCount == 0: resetSymbolDictionary() arms immediately.
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());
                }
            }

            // Mid-batch: a request while a row is buffered (pendingRowCount != 0)
            // only arms once the next flush runs armIfEligible() at its tail.
            try (TestWebSocketServer server = ackingServer()) {
                try (Sender sender = Sender.fromConfig(cfg(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.resetSymbolDictionary();
                    Assert.assertFalse("a mid-batch request must not arm before the next flush",
                            ws.isResetArmed());
                    sender.flush();
                    Assert.assertTrue(ws.isResetArmed());
                }
            }
        });
    }

    @Test
    public void testResetSymbolDictionaryOnNonWsSenderIsNoOp() throws Exception {
        assertMemoryLeak(() -> {
            // protocolVersion(2) skips the eager server-side settings detection
            // connect that build() otherwise performs, so no live server is needed
            // (see LineSenderBuilderTest.testCustomPemRootsDoNotRequirePassword).
            try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                    .address("localhost")
                    .protocolVersion(2)
                    .build()) {
                sender.resetSymbolDictionary();
            }
        });
    }

    @Test
    public void testSplitFlushPathArms() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = ackingServer()) {
                server.setAdvertisedMaxBatchSize(150); // forces the two-table batch to split
                // Padding inflates each table past half the cap, so the combined
                // two-table message exceeds it while each single-table split frame fits.
                String pad = TestUtils.repeat("x", 60);
                try (Sender sender = Sender.fromConfig(cfg(server) + "symbol_dict_reset_threshold=2;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t1").symbol("s", "a").stringColumn("p", pad).longColumn("v", 1L).atNow();
                    sender.table("t2").symbol("s", "b").stringColumn("p", pad).longColumn("v", 2L).atNow();
                    sender.flush();
                    Assert.assertTrue("the split-flush path shares resetTableBuffersAfterFlush's tail",
                            ws.isResetArmed());
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

    private static String cfg(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";";
    }

    /**
     * ACKs every frame it receives; does not otherwise inspect the wire.
     */
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

    /**
     * Raises a RECOGNISED mmap access fault out of the persisted dictionary's next
     * mmap growth, once, when {@link #armed}. Copied from
     * {@code MmapFaultDegradesTest.MmapFaultDictFacade}.
     */
    private static final class MmapFaultDictFacade extends DelegatingFilesFacade {
        boolean armed;

        @Override
        public boolean isMmapAllowed() {
            return true;
        }

        @Override
        public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
            if (armed) {
                armed = false;
                throw new InternalError(
                        "a fault occurred in a recent unsafe memory access operation in compiled Java code");
            }
            return INSTANCE.mmap(fd, len, offset, flags, memoryTag);
        }
    }
}
