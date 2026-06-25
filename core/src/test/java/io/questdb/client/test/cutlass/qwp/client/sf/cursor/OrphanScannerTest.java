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

import io.questdb.client.cutlass.qwp.client.sf.cursor.OrphanScanner;
import io.questdb.client.std.Files;
import io.questdb.client.std.IntList;
import io.questdb.client.std.ObjList;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrphanScannerTest {

    private String sfDir;

    @Before
    public void setUp() {
        sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-orphans-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test
    public void testEmptyGroupRootHasNoOrphans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
            assertEquals(0, orphans.size());
        });
    }

    @Test
    public void testMissingGroupRootReturnsEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Spec: scanner is read-only; a non-existent dir is "no orphans",
            // not an error. Lets startup proceed cleanly when the group root
            // hasn't been created yet by any sender.
            ObjList<String> orphans = OrphanScanner.scan(
                    sfDir + "/never-created", "default");
            assertEquals(0, orphans.size());
        });
    }

    @Test
    public void testSlotWithSfaIsAnOrphan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = sfDir + "/orphan-a";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            touchFile(slot + "/sf-0001.sfa");

            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
            assertEquals(1, orphans.size());
            assertEquals(slot, orphans.get(0));
        });
    }

    @Test
    public void testEmptySlotDirIsNotAnOrphan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Per spec, empty slot dirs are cheap and stay forever — they
            // aren't candidates for drain because there's nothing to drain.
            String slot = sfDir + "/empty";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));

            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
            assertEquals(0, orphans.size());
        });
    }

    @Test
    public void testSlotWithFailedSentinelIsSkipped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // .failed = "human required, automation backed off". Scanner
            // must not treat such slots as orphans, even if they have data.
            String slot = sfDir + "/failed";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            touchFile(slot + "/sf-0001.sfa");
            OrphanScanner.markFailed(slot, "test-induced");
            assertTrue("sentinel exists",
                    Files.exists(slot + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
            assertEquals(0, orphans.size());
        });
    }

    @Test
    public void testExcludeSlotNameSkipsCallersOwnSlot() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The foreground sender's own slot must not appear as an orphan
            // (it isn't one — the sender is actively using it).
            String mineSlot = sfDir + "/mine";
            String otherSlot = sfDir + "/other";
            assertEquals(0, Files.mkdir(mineSlot, Files.DIR_MODE_DEFAULT));
            assertEquals(0, Files.mkdir(otherSlot, Files.DIR_MODE_DEFAULT));
            touchFile(mineSlot + "/sf-0001.sfa");
            touchFile(otherSlot + "/sf-0001.sfa");

            ObjList<String> orphans = OrphanScanner.scan(sfDir, "mine");
            assertEquals(1, orphans.size());
            assertEquals(otherSlot, orphans.get(0));
        });
    }

    @Test
    public void testMultipleOrphansReturned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (String name : new String[]{"a", "b", "c"}) {
                String slot = sfDir + "/" + name;
                assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
                touchFile(slot + "/sf-0001.sfa");
            }
            ObjList<String> orphans = OrphanScanner.scan(sfDir, "exclude-me");
            assertEquals(3, orphans.size());
        });
    }

    // ----------------------------------------------------------------------
    // Bounded (managed-slot) exclusion: scan(sfDir, exclude, base, count).
    // This is how a connection pool fences off its own live siblings while
    // still draining same-base leftovers, and the fix for the
    // "shrinking maxSize strands unacked SF data" bug.
    // ----------------------------------------------------------------------

    @Test
    public void testBoundedScanExcludesInRangeManagedSlots() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Pool at maxSize=2 co-manages default-0 and default-1; a startup
            // scan must not list a live sibling as an orphan. A foreign slot
            // is still reported.
            for (String name : new String[]{"default-0", "default-1"}) {
                String slot = sfDir + "/" + name;
                assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
                touchFile(slot + "/sf-0001.sfa");
            }
            String foreign = sfDir + "/legacy";
            assertEquals(0, Files.mkdir(foreign, Files.DIR_MODE_DEFAULT));
            touchFile(foreign + "/sf-0001.sfa");

            // caller is default-0; managed set is [0, 2)
            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default-0", "default", 2);
            assertEquals("only the foreign slot is a candidate", 1, orphans.size());
            assertEquals(foreign, orphans.get(0));
        });
    }

    @Test
    public void testBoundedScanDrainsOutOfRangeSameBaseSlotsAfterShrink() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The bug: a previous run used maxSize=4 (default-0..3 hold unacked
            // data); this run restarts at maxSize=2. default-0/1 are re-created
            // and self-recovered (excluded), but default-2/3 are OUT of the new
            // [0,2) index range forever -- they must be drainable, not stranded.
            for (String name : new String[]{"default-0", "default-1", "default-2", "default-3"}) {
                String slot = sfDir + "/" + name;
                assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
                touchFile(slot + "/sf-0001.sfa");
            }
            // caller is default-0; managed set is [0, 2)
            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default-0", "default", 2);
            assertEquals("default-2 and default-3 must be drainable orphans", 2, orphans.size());
            boolean has2 = false, has3 = false;
            for (int i = 0; i < orphans.size(); i++) {
                String p = orphans.get(i);
                if (p.equals(sfDir + "/default-2")) has2 = true;
                if (p.equals(sfDir + "/default-3")) has3 = true;
            }
            assertTrue("default-2 stranded", has2);
            assertTrue("default-3 stranded", has3);
        });
    }

    @Test
    public void testListStrandedOutOfRangeManagedSlots() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // A previous run used maxSize=2; default-0/1 are in range, default-2/3
            // are out of range and hold unacked data. A non-canonical same-base
            // name (default-04) and a foreign base (other-9) must NOT be listed:
            // the pool only ever minted canonical default-<i>, so only those are
            // its own stranded out-of-range slots.
            for (String name : new String[]{"default-0", "default-1", "default-2", "default-3",
                    "default-04", "other-9"}) {
                String slot = sfDir + "/" + name;
                assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
                touchFile(slot + "/sf-0001.sfa");
            }
            // An out-of-range slot already flagged .failed must be skipped.
            String failed = sfDir + "/default-5";
            assertEquals(0, Files.mkdir(failed, Files.DIR_MODE_DEFAULT));
            touchFile(failed + "/sf-0001.sfa");
            OrphanScanner.markFailed(failed, "test-induced");
            // An out-of-range slot with no segment file must be skipped.
            String empty = sfDir + "/default-6";
            assertEquals(0, Files.mkdir(empty, Files.DIR_MODE_DEFAULT));

            IntList stranded = OrphanScanner.listStrandedOutOfRangeManagedSlots(sfDir, "default", 2);
            assertEquals("only canonical same-base i>=2 with unacked data", 2, stranded.size());
            boolean has2 = false, has3 = false;
            for (int i = 0; i < stranded.size(); i++) {
                if (stranded.getQuick(i) == 2) has2 = true;
                if (stranded.getQuick(i) == 3) has3 = true;
            }
            assertTrue("default-2 must be listed", has2);
            assertTrue("default-3 must be listed", has3);

            // count <= 0 / null / empty base disables the bound.
            assertEquals(0, OrphanScanner.listStrandedOutOfRangeManagedSlots(sfDir, null, 2).size());
            assertEquals(0, OrphanScanner.listStrandedOutOfRangeManagedSlots(sfDir, "", 2).size());
        });
    }

    @Test
    public void testBoundedScanDrainsNonCanonicalSameBaseNames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The pool only ever mints canonical Integer.toString suffixes
            // ("0","1",...). A same-base dir with a leading-zero or non-numeric
            // suffix is not a managed slot, so it is drained like any foreign
            // leftover -- even if its numeric value would fall inside [0,count).
            for (String name : new String[]{"default-00", "default-01", "default-foo", "default-"}) {
                String slot = sfDir + "/" + name;
                assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
                touchFile(slot + "/sf-0001.sfa");
            }
            // also a genuinely-managed slot that must stay excluded
            String managed = sfDir + "/default-1";
            assertEquals(0, Files.mkdir(managed, Files.DIR_MODE_DEFAULT));
            touchFile(managed + "/sf-0001.sfa");

            ObjList<String> orphans = OrphanScanner.scan(sfDir, "default-0", "default", 4);
            assertEquals("all non-canonical same-base names are drainable", 4, orphans.size());
            for (int i = 0; i < orphans.size(); i++) {
                assertFalse("managed slot must not appear",
                        orphans.get(i).equals(managed));
            }
        });
    }

    @Test
    public void testBoundedScanDisabledWhenCountNonPositive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // count <= 0 or null/empty base disables the exclusion: every
            // sibling with data (except the explicit excludeSlotName) is a
            // candidate.
            String sibling = sfDir + "/default-1";
            assertEquals(0, Files.mkdir(sibling, Files.DIR_MODE_DEFAULT));
            touchFile(sibling + "/sf-0001.sfa");

            assertEquals(1, OrphanScanner.scan(sfDir, "default-0", "default", 0).size());
            assertEquals(1, OrphanScanner.scan(sfDir, "default-0", null, 4).size());
            assertEquals(1, OrphanScanner.scan(sfDir, "default-0", "", 4).size());
        });
    }

    @Test
    public void testIsManagedSlotPredicate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // In-range canonical indices are managed.
            assertTrue(OrphanScanner.isManagedSlot("default-0", "default-", 2));
            assertTrue(OrphanScanner.isManagedSlot("default-1", "default-", 2));
            // At-or-above the count is NOT managed (drainable).
            assertFalse(OrphanScanner.isManagedSlot("default-2", "default-", 2));
            assertFalse(OrphanScanner.isManagedSlot("default-10", "default-", 2));
            // Non-canonical / foreign suffixes are NOT managed.
            assertFalse(OrphanScanner.isManagedSlot("default-00", "default-", 4));
            assertFalse(OrphanScanner.isManagedSlot("default-01", "default-", 4));
            assertFalse(OrphanScanner.isManagedSlot("default-foo", "default-", 4));
            assertFalse(OrphanScanner.isManagedSlot("default-", "default-", 4));
            assertFalse(OrphanScanner.isManagedSlot("default--1", "default-", 4));
            assertFalse(OrphanScanner.isManagedSlot("other-0", "default-", 4));
            assertFalse(OrphanScanner.isManagedSlot("default", "default-", 4));
        });
    }

    @Test
    public void testIsCandidateOrphanDirect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = sfDir + "/probe";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            assertFalse("empty slot is not a candidate",
                    OrphanScanner.isCandidateOrphan(slot));
            touchFile(slot + "/sf-0001.sfa");
            assertTrue("slot with sfa is a candidate",
                    OrphanScanner.isCandidateOrphan(slot));
            OrphanScanner.markFailed(slot, "x");
            assertFalse("slot with .failed is not a candidate",
                    OrphanScanner.isCandidateOrphan(slot));
        });
    }

    private static void touchFile(String path) {
        int fd = Files.openRW(path);
        if (fd >= 0) Files.close(fd);
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
                        if (!Files.remove(child)) {
                            rmDirRec(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }
}
