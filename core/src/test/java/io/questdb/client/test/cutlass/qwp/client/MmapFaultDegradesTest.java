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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class MmapFaultDegradesTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testRecognisedMmapFaultIsDistinguishedFromAPlainError() {
        // commitMappedChunk uses Crc32c.updateUnsafe precisely so a SIGBUS on a sparse
        // page becomes a catchable InternalError. InternalError extends
        // VirtualMachineError extends Error, so an unqualified "if (t instanceof Error)
        // throw" hands back the one fault class the design made catchable, and the
        // sender never degrades to self-sufficient frames.
        Assert.assertTrue(MmapSegment.isMmapAccessFault(
                new InternalError("a fault occurred in an unsafe memory access operation")));
        Assert.assertFalse(MmapSegment.isMmapAccessFault(new OutOfMemoryError("heap")));
        Assert.assertFalse(MmapSegment.isMmapAccessFault(new InternalError("something else")));
    }

    /**
     * Drives a RECOGNISED mmap-access-fault {@code InternalError} through
     * {@code QwpWebSocketSender.persistNewSymbolsBeforePublish} -- one of the two guards
     * ({@code instanceof Error && !isMmapAccessFault(t)}) this class exists to protect, at
     * {@code QwpWebSocketSender.java:4028}. The sibling guard in {@code healPersistedDictionary}
     * ({@code :3944}) has the identical shape but is not independently exercised here; it
     * would need a recovery scenario (a torn persisted dictionary alongside surviving frames
     * that still reference the missing ids) to reach, which this test does not set up.
     * <p>
     * The fault is injected deterministically at the {@link io.questdb.client.std.FilesFacade#mmap}
     * seam {@code PersistedSymbolDict} already takes for testing -- the same technique
     * {@code MmapSegmentRecoveryFaultTest.testSegmentRingRefusesSlotOnUnconvertedMmapFault} uses
     * for {@code SegmentRing} -- rather than relying on a real SIGBUS, whose delivery frame is
     * JIT/JDK-version dependent (see {@code MmapSegmentRecoveryFaultTest.hasPreciseUnsafeAccessFaults}).
     * <p>
     * Before the guard existed (an unqualified {@code instanceof Error}), the injected
     * {@code InternalError} would be rethrown raw: {@code flush()} would surface it instead of
     * a {@link LineSenderException}, and {@link QwpWebSocketSender#isDeltaDictEnabledForTest()}
     * would stay {@code true} instead of degrading. Reverting either guard back to a bare
     * {@code instanceof Error} turns this test red.
     */
    @Test
    public void testMmapAccessFaultDegradesPersistInsteadOfPropagating() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("mmap-fault-sf").toString();
            String slot = Paths.get(sfDir, "default").toString();
            Assert.assertEquals(0, io.questdb.client.std.Files.mkdir(sfDir,
                    io.questdb.client.std.Files.DIR_MODE_DEFAULT));

            try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
                int port = silent.getPort();
                silent.start();
                Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));

                MmapFaultDictFacade ff = new MmapFaultDictFacade();
                // The engine owns the dictionary; the fault facade reaches only its mmap
                // growth, so segment files still write normally and the ONLY failure is
                // the persist.
                CursorSendEngine engine = new CursorSendEngine(
                        slot, 4L * 1024 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS, ff);
                QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, 0, 0, 0L, null, false, engine);
                try {
                    ff.armed = true; // the next dictionary mmap growth raises the fault
                    sender.table("m").symbol("s", "boom").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                        Assert.fail("a recognised mmap access fault during persist must still "
                                + "surface as a sender error, not silently succeed");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue("the fault must be reported as a sender error, not a "
                                        + "raw InternalError: " + expected.getMessage(),
                                expected.getMessage().contains("failed to persist symbol dictionary before publish"));
                    }
                    Assert.assertFalse("a recognised mmap access fault must degrade the sender to "
                                    + "full self-sufficient frames, not merely fail this one flush",
                            sender.isDeltaDictEnabledForTest());
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() re-flushes the still-buffered row; the facade has disarmed
                        // and delta-dict is now off, so this normally succeeds. Either way it
                        // is not what this test asserts.
                    }
                }
            }
        });
    }

    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks -- irrelevant here, the assertions run before any drain
        }
    }

    /**
     * Raises a RECOGNISED mmap access fault out of the persisted dictionary's next mmap
     * growth, once, when {@link #armed}. Mirrors {@code DeltaDictRecoveryTest.FullDiskDictFacade}
     * but injects the fault {@code QwpWebSocketSender}'s guards are specifically meant to
     * absorb, instead of the plain {@code IllegalStateException} an {@code ff.allocate} refusal
     * produces.
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
