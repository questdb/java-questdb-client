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
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test
    public void testSeededMirrorFreedWhenLoopClosedWithoutStart() throws Exception {
        Path sfDir = temporaryFolder.newFolder("qwp-mirror-leak").toPath();
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
                long persistedAddr = pd.loadedEntriesAddr();

                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0, 1_000_000L,
                        () -> {
                            throw new IOException("no reconnect in this test");
                        },
                        0, 1);
                // Close without start(): the ctor-seeded mirror is this
                // thread's to free, since the I/O loop never ran.
                Assert.assertTrue("precondition: the ctor seeded a non-empty mirror",
                        loop.sentDictCount() > 0);
                Assert.assertEquals("foreground loop must borrow the persisted buffer without copying",
                        persistedAddr, loop.sentDictBytesAddr());
                Assert.assertEquals("borrowing must leave the engine's pointer intact so a retried "
                                + "construction can re-seed from it",
                        persistedAddr, pd.loadedEntriesAddr());
                Assert.assertFalse("a borrowed mirror must not be loop-owned",
                        loop.sentDictBytesOwned());
                loop.close();
                Assert.assertEquals("close() must not free a buffer the loop only borrowed -- the "
                                + "engine still owns it",
                        persistedAddr, pd.loadedEntriesAddr());
                // close() must reset sentDictCount alongside freeing the buffer,
                // so the mirror stays all-or-nothing: a hypothetical post-close
                // start() (no closed guard) cannot read a stale count against a
                // freed buffer and drive a null-mirror catch-up.
                Assert.assertEquals("close() must reset sentDictCount to 0",
                        0, loop.sentDictCount());
            }
        });
    }

    @Test
    public void testRecycledLoopReSeedsMirrorFromPersistedDict() throws Exception {
        // C1 regression: the orphan drainer (BackgroundDrainer) builds a NEW
        // CursorWebSocketSendLoop per wire session against the SAME, persistent
        // engine when a durable-ack capability gap forces a mid-drain recycle. The
        // recovery mirror seed must survive that recycle. If the first loop consumes
        // the persisted dictionary's loaded entries (a one-shot ownership transfer),
        // the second loop seeds an EMPTY mirror (sentDictCount = 0), sends no
        // reconnect catch-up, and the first replayed delta frame (deltaStart > 0)
        // trips the torn-dict guard -- falsely quarantining a healthy slot with a
        // bogus "resend required" terminal. Borrowing the entries leaves the
        // dictionary intact for the engine's lifetime without making another native
        // copy, so every recycled loop can re-seed.
        Path sfDir = temporaryFolder.newFolder("qwp-mirror-reseed").toPath();
        populateRecoverableSlot(sfDir);
        Path slot = sfDir.resolve("default");
        assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
                PersistedSymbolDict pd = engine.getPersistedSymbolDict();
                Assert.assertNotNull(pd);
                int dictSize = pd.size();
                Assert.assertTrue("recovery must load a non-empty dictionary", dictSize > 0);
                long persistedAddr = pd.loadedEntriesAddr();

                // Session 1 seeds its mirror from the persisted dictionary.
                CursorWebSocketSendLoop loop1 = newRecoveryLoop(engine);
                try {
                    Assert.assertEquals("session-1 mirror must seed from the persisted dict",
                            dictSize, loop1.sentDictCount());
                    Assert.assertEquals("orphan session must borrow the persisted bytes",
                            persistedAddr, loop1.sentDictBytesAddr());
                    Assert.assertFalse("borrowed orphan mirror must not own the persisted bytes",
                            loop1.sentDictBytesOwned());
                } finally {
                    loop1.close();
                }
                Assert.assertEquals("closing a borrowed loop must leave the engine prefix alive",
                        persistedAddr, pd.loadedEntriesAddr());

                // Session 2 against the SAME engine (the drainer recycle): the
                // seed must NOT have been consumed -- the mirror must re-seed to
                // the full dictionary so the reconnect catch-up is complete.
                CursorWebSocketSendLoop loop2 = newRecoveryLoop(engine);
                try {
                    Assert.assertEquals("recycled session-2 mirror must re-seed from the "
                                    + "persisted dict (pre-fix it was 0)",
                            dictSize, loop2.sentDictCount());
                    Assert.assertEquals(persistedAddr, loop2.sentDictBytesAddr());
                } finally {
                    loop2.close();
                }
            }
        });
    }

    @Test
    public void testCtorFreesSeededMirrorWhenFrameSeedThrows() throws Exception {
        // C1 regression: the constructor seeds the recovery mirror in TWO steps. It
        // first BORROWS the persisted dictionary's intact prefix by reference, then
        // extends it with the recovered suffix the engine retains -- and that extension
        // calls ensureSentDictCapacity, which copy-on-writes the borrowed prefix into a
        // fresh loop-OWNED allocation. A throw just past that grow (a native realloc
        // OOM, or the MAX_SENT_DICT_BYTES ceiling) leaves the constructor with the
        // object unpublished, so neither ensureConnected's catch nor BackgroundDrainer's
        // finally can ever close() it -- and the owned mirror leaks. The constructor
        // must free it on the throw. Delete that free and assertMemoryLeak fails here.
        //
        // The fault therefore has to sit AFTER the grow. Injected before it, the mirror
        // is still borrowed (sentDictBytesOwned == false), the cleanup is a no-op, and
        // this test would pass with the cleanup deleted.
        Path sfDir = temporaryFolder.newFolder("qwp-mirror-ctor-throw").toPath();
        // A torn-dict SUBSET: three delta frames a@0,b@1,c@2 survive on disk, but the
        // .symbol-dict is rewritten to hold only [a,b] (a host-crash tail tear). On
        // recovery pd.size()==2 seeds the borrowed prefix, then the frame-seed grows the
        // mirror to take c@2 from the recovered suffix -- the step the fault interrupts.
        populateThreeFrameSlot(sfDir);
        Path slot = sfDir.resolve("default");
        replacePersistedDictionaryWithTwoSymbolPrefix(slot);

        assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
                PersistedSymbolDict pd = engine.getPersistedSymbolDict();
                Assert.assertNotNull("recovery must open the torn subset dict", pd);
                Assert.assertEquals("prefix seed must malloc a 2-entry mirror", 2, pd.size());
                Assert.assertTrue("the frame-seed path must run (frames out-reach the dict)",
                        engine.recoveredMaxSymbolDeltaStart() > 0L);

                CursorWebSocketSendLoop.forceMirrorSeedFailureForTest = true;
                try {
                    new CursorWebSocketSendLoop(
                            null, engine, 0, 1_000_000L,
                            () -> {
                                throw new IOException("no reconnect in this test");
                            },
                            0, 1);
                    Assert.fail("ctor must propagate the injected mirror-seed failure");
                } catch (LineSenderException expected) {
                    Assert.assertTrue("unexpected message: " + expected.getMessage(),
                            expected.getMessage().contains("simulated mirror seed allocation failure"));
                } finally {
                    CursorWebSocketSendLoop.forceMirrorSeedFailureForTest = false;
                }
                Assert.assertTrue("failed foreground construction must leave the persisted prefix owned",
                        pd.loadedEntriesAddr() != 0L);
                Assert.assertTrue("failed construction must retain the recovered suffix for retry",
                        engine.recoverySymbolNativeCapacity() > 0);
                // The outer assertMemoryLeak proves the prefix-seeded mirror the ctor
                // malloc'd was freed on the throw -- pre-fix it leaks here.
            }
        });
    }

    @Test
    public void testForegroundLoopRetainsRecoveredSuffixForRetry() throws Exception {
        Path sfDir = temporaryFolder.newFolder("qwp-mirror-suffix-release").toPath();
        populateThreeFrameSlot(sfDir);
        Path slot = sfDir.resolve("default");
        replacePersistedDictionaryWithTwoSymbolPrefix(slot);

        assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
                Assert.assertTrue("recovery must retain c@2 above the persisted [a,b] prefix",
                        engine.recoverySymbolNativeCapacity() > 0);
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null, engine, 0, 1_000_000L,
                        () -> {
                            throw new IOException("no reconnect in this test");
                        },
                        0, 1);
                try {
                    Assert.assertEquals("foreground mirror must include prefix and recovered suffix",
                            3, loop.sentDictCount());
                    Assert.assertTrue("the engine must RETAIN its recovery suffix: the foreground "
                                    + "loop borrows it, and a retried construction re-seeds from it",
                            engine.recoverySymbolNativeCapacity() > 0);
                } finally {
                    loop.close();
                }
            }
        });
    }

    /**
     * A foreground loop must be reconstructible against the same engine.
     * <p>
     * QwpWebSocketSender.ensureConnected's catch closes and nulls cursorSendLoop exactly so
     * a caller can retry, and `connected` is only set at the very end -- so a second
     * construction is reachable. The old foreground path consumed the engine's recovery
     * sources (pd.takeLoadedEntries + releaseRecoveredSymbolStorage), and neither cleared
     * the counts that gate re-seeding: the retry saw recoveredSize() > 0 with
     * loadedEntriesLen() == 0, left sentDictCount at 0, and then either tripped the
     * baseline mismatch or latched "resend required" on a healthy slot.
     * <p>
     * This is the foreground twin of testRecycledLoopReSeedsMirrorFromPersistedDict.
     */
    @Test
    public void testForegroundLoopReSeedsAfterAClosedFirstAttempt() throws Exception {
        Path sfDir = temporaryFolder.newFolder("qwp-mirror-foreground-retry").toPath();
        populateThreeFrameSlot(sfDir);
        Path slot = sfDir.resolve("default");
        replacePersistedDictionaryWithTwoSymbolPrefix(slot);

        assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
                int firstCount;
                CursorWebSocketSendLoop first = newForegroundLoop(engine);
                try {
                    firstCount = first.sentDictCount();
                    Assert.assertTrue("precondition: the first loop must seed a mirror",
                            firstCount > 0);
                } finally {
                    // Exactly what ensureConnected's catch does before a retry.
                    first.close();
                }

                CursorWebSocketSendLoop second = newForegroundLoop(engine);
                try {
                    Assert.assertEquals("a retried foreground construction must re-seed identically",
                            firstCount, second.sentDictCount());
                } finally {
                    second.close();
                }
            }
        });
    }

    @Test
    public void testOrphanLoopsRetainRecoveredSuffixForRecycle() throws Exception {
        Path sfDir = temporaryFolder.newFolder("qwp-mirror-suffix-recycle").toPath();
        populateThreeFrameSlot(sfDir);
        Path slot = sfDir.resolve("default");
        replacePersistedDictionaryWithTwoSymbolPrefix(slot);

        assertMemoryLeak(() -> {
            try (CursorSendEngine engine = new CursorSendEngine(slot.toString(), 4096)) {
                int suffixCapacity = engine.recoverySymbolNativeCapacity();
                Assert.assertTrue("recovery must retain c@2 above the persisted [a,b] prefix",
                        suffixCapacity > 0);
                for (int session = 0; session < 2; session++) {
                    CursorWebSocketSendLoop loop = newRecoveryLoop(engine);
                    try {
                        Assert.assertEquals("orphan mirror must include prefix and recovered suffix",
                                3, loop.sentDictCount());
                        Assert.assertEquals("orphan engine must retain suffix for the next session",
                                suffixCapacity, engine.recoverySymbolNativeCapacity());
                    } finally {
                        loop.close();
                    }
                }
            }
        });
    }

    // Constructs a recovery send loop but does NOT start it: the ctor seeds the
    // catch-up mirror synchronously, which is all these tests observe. The
    // reconnect factory is never invoked.
    private static CursorWebSocketSendLoop newRecoveryLoop(CursorSendEngine engine) {
        return new CursorWebSocketSendLoop(
                null, engine, 0, 1_000_000L,
                () -> {
                    throw new IOException("no reconnect in this test");
                },
                0, 1,
                false, 0L, 3, 0L, 0L,
                CursorWebSocketSendLoop.ReconnectPolicy.ORPHAN);
    }

    private static CursorWebSocketSendLoop newForegroundLoop(CursorSendEngine engine) {
        // The 7-arg ctor resolves to ReconnectPolicy.FOREGROUND -- newRecoveryLoop above
        // builds an ORPHAN loop, which borrows by construction and so would not exercise
        // the foreground path this test exists for.
        return new CursorWebSocketSendLoop(
                null, engine, 0, 1_000_000L,
                () -> {
                    throw new IOException("no reconnect in this test");
                },
                0, 1);
    }

    private static void populateRecoverableSlot(Path sfDir) throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";sf_max_segment_bytes=4096"
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

    // Three delta frames a@0, b@1, c@2, nothing acked, so all three survive and
    // replay from frame 0. Paired with a dictionary truncated to [a,b], this is a
    // torn-dict SUBSET whose recovery drives the constructor's frame-seed path.
    private static void populateThreeFrameSlot(Path sfDir) throws Exception {
        try (TestWebSocketServer silent = new TestWebSocketServer(new SilentHandler())) {
            int port = silent.getPort();
            silent.start();
            Assert.assertTrue(silent.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + port
                    + ";sf_dir=" + sfDir
                    + ";close_flush_timeout_millis=0;";
            try (Sender s = Sender.fromConfig(cfg)) {
                s.table("m").symbol("s", "a").longColumn("v", 0).atNow();
                s.flush();
                s.table("m").symbol("s", "b").longColumn("v", 1).atNow();
                s.flush();
                s.table("m").symbol("s", "c").longColumn("v", 2).atNow();
                s.flush();
            }
        }
    }

    private static void replacePersistedDictionaryWithTwoSymbolPrefix(Path slot) throws IOException {
        // Stamped with the surviving segment's own generation so the recovery
        // exercised right after this call still trusts this rewritten dictionary as
        // belonging to the same lineage -- see readSegmentGeneration.
        try (PersistedSymbolDict torn = PersistedSymbolDict.openClean(slot.toString(), readSegmentGeneration(slot))) {
            Assert.assertNotNull(torn);
            torn.appendSymbol("a");
            torn.appendSymbol("b");
            Assert.assertEquals(2, torn.size());
        }
    }

    /**
     * Reads the u64 generation {@code populateThreeFrameSlot}'s fresh-slot path
     * stamped into {@code sf-initial.sfa} (the last 8 bytes of the header -- see
     * MmapSegment's header layout).
     */
    private static long readSegmentGeneration(Path slot) throws IOException {
        byte[] bytes = Files.readAllBytes(slot.resolve("sf-initial.sfa"));
        int offset = MmapSegment.HEADER_SIZE - 8;
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v |= (long) (bytes[offset + i] & 0xFFL) << (8 * i);
        }
        return v;
    }

    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // never acks -- the sender leaves everything unacked in the slot
        }
    }
}
