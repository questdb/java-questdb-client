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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Regression test for M6 — drainer adopting an empty orphan slot would
 * leak a fresh sf-initial.sfa back to disk on close, and the next scanner
 * would re-adopt the same slot in a churn loop.
 *
 * <p>Setup: open a CursorSendEngine on a fresh slot, write nothing,
 * close. The engine creates an initial sf-initial.sfa during construction
 * but no frames are ever published (publishedFsn = -1).
 *
 * <p>Pre-fix behavior (CursorSendEngine.close): unlinkAllSegmentFiles is
 * gated on {@code publishedFsn() >= 0}, so the fresh empty initial file
 * survives close. Re-opening the slot would re-trigger recovery, which
 * unlinks the empty file and creates yet another one — burning CPU/IO
 * and cluttering logs.
 *
 * <p>Post-fix: the close gate also accepts {@code publishedFsn < 0}
 * (nothing ever published is a valid "drained" state), so the empty
 * initial gets unlinked on close and the slot dir is left clean.
 */
public class EmptyOrphanSlotChurnTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-empty-churn-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (sfDir == null) return;
        long find = Files.findFirst(sfDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(sfDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(sfDir);
    }

    @Test
    public void testFreshStartDiscardsSurvivingStaleDictionary() throws Exception {
        // Regression: a prior fully-drained lifecycle can leave a stale
        // .symbol-dict behind (a best-effort delete that failed, or a crash in the
        // close window) with NO segments. A fresh start must DISCARD it -- the
        // dictionary is load-bearing and the fresh-start producer is not seeded
        // from it, so trusting a survivor would diverge the producer ids from the
        // dictionary the send loop replays and misattribute symbols on reconnect.
        TestUtils.assertMemoryLeak(() -> {
            // Pre-seed a stale dictionary in the slot, with no segments behind it.
            PersistedSymbolDict stale = PersistedSymbolDict.openClean(sfDir);
            assertNotNull(stale);
            try {
                stale.appendSymbol("staleX");
                stale.appendSymbol("staleY");
                assertEquals(2, stale.size());
            } finally {
                stale.close();
            }

            // A fresh start (no recovered segments) must open a CLEAN, empty
            // dictionary -- not inherit the survivor.
            try (CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024)) {
                assertFalse("fresh start must not report a disk recovery",
                        engine.wasRecoveredFromDisk());
                PersistedSymbolDict pd = engine.getPersistedSymbolDict();
                assertNotNull(pd);
                assertEquals("fresh start must discard the surviving stale dictionary",
                        0, pd.size());
            }

            // The survivor's bytes are physically gone, not just hidden: this fresh-start
            // engine never publishes a frame, so its close() is the fully-drained path,
            // which unlinks the freshly-truncated dictionary alongside the ring and
            // watermark (see CursorSendEngine's finishClose: "the dictionary has no
            // frames behind it"). open() now honestly reports that as ABSENT (null)
            // rather than fabricating a replacement file to reopen, so the strongest
            // proof left that the stale bytes are gone is the file's own absence.
            assertFalse("fully-drained close must remove the discarded dictionary",
                    java.nio.file.Files.exists(Paths.get(sfDir, ".symbol-dict")));
        });
    }

    @Test
    public void testNeverPublishedCloseLeavesNoSfaFiles() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Phase 1: open and close without writing a single frame. This is
            // the exact code path a drainer takes when adopting an orphan
            // slot whose segments all turn out to be empty: openExisting
            // returns null, the engine constructor creates a fresh
            // sf-initial.sfa, the drainer observes publishedFsn=-1 (already
            // drained) and closes.
            try (CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024)) {
                assertEquals("nothing was published", -1L, engine.publishedFsn());
            }

            // Phase 2: assert the slot dir has no .sfa files. Pre-fix this
            // fails because sf-initial.sfa survives close.
            assertFalse(
                    "Empty orphan slots must not leave a fresh sf-initial.sfa "
                            + "behind on close — the next OrphanScanner pass would "
                            + "re-adopt the slot, unlink the file, recreate it, "
                            + "and loop indefinitely.",
                    hasAnySfaFile(sfDir));

            // Phase 3: re-opening must not re-create churn — same shape, no
            // file should appear after the second close either.
            try (CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024)) {
                assertEquals(-1L, engine.publishedFsn());
            }
            assertFalse("re-open + close must not churn either",
                    hasAnySfaFile(sfDir));
        });
    }

    private static boolean hasAnySfaFile(String dir) {
        long find = Files.findFirst(dir);
        if (find <= 0) return false;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && name.endsWith(".sfa")) return true;
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
        return false;
    }
}
