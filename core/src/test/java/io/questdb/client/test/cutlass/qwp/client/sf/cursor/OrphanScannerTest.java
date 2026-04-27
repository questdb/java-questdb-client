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
import io.questdb.client.std.ObjList;
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
        assertEquals(0, Files.mkdir(sfDir, 0755));
    }

    @After
    public void tearDown() {
        if (sfDir != null) rmDirRec(sfDir);
    }

    @Test
    public void testEmptyGroupRootHasNoOrphans() {
        ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
        assertEquals(0, orphans.size());
    }

    @Test
    public void testMissingGroupRootReturnsEmpty() {
        // Spec: scanner is read-only; a non-existent dir is "no orphans",
        // not an error. Lets startup proceed cleanly when the group root
        // hasn't been created yet by any sender.
        ObjList<String> orphans = OrphanScanner.scan(
                sfDir + "/never-created", "default");
        assertEquals(0, orphans.size());
    }

    @Test
    public void testSlotWithSfaIsAnOrphan() {
        String slot = sfDir + "/orphan-a";
        assertEquals(0, Files.mkdir(slot, 0755));
        touchFile(slot + "/sf-0001.sfa");

        ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
        assertEquals(1, orphans.size());
        assertEquals(slot, orphans.get(0));
    }

    @Test
    public void testEmptySlotDirIsNotAnOrphan() {
        // Per spec, empty slot dirs are cheap and stay forever — they
        // aren't candidates for drain because there's nothing to drain.
        String slot = sfDir + "/empty";
        assertEquals(0, Files.mkdir(slot, 0755));

        ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
        assertEquals(0, orphans.size());
    }

    @Test
    public void testSlotWithFailedSentinelIsSkipped() {
        // .failed = "human required, automation backed off". Scanner
        // must not treat such slots as orphans, even if they have data.
        String slot = sfDir + "/failed";
        assertEquals(0, Files.mkdir(slot, 0755));
        touchFile(slot + "/sf-0001.sfa");
        OrphanScanner.markFailed(slot, "test-induced");
        assertTrue("sentinel exists",
                Files.exists(slot + "/" + OrphanScanner.FAILED_SENTINEL_NAME));

        ObjList<String> orphans = OrphanScanner.scan(sfDir, "default");
        assertEquals(0, orphans.size());
    }

    @Test
    public void testExcludeSlotNameSkipsCallersOwnSlot() {
        // The foreground sender's own slot must not appear as an orphan
        // (it isn't one — the sender is actively using it).
        String mineSlot = sfDir + "/mine";
        String otherSlot = sfDir + "/other";
        assertEquals(0, Files.mkdir(mineSlot, 0755));
        assertEquals(0, Files.mkdir(otherSlot, 0755));
        touchFile(mineSlot + "/sf-0001.sfa");
        touchFile(otherSlot + "/sf-0001.sfa");

        ObjList<String> orphans = OrphanScanner.scan(sfDir, "mine");
        assertEquals(1, orphans.size());
        assertEquals(otherSlot, orphans.get(0));
    }

    @Test
    public void testMultipleOrphansReturned() {
        for (String name : new String[]{"a", "b", "c"}) {
            String slot = sfDir + "/" + name;
            assertEquals(0, Files.mkdir(slot, 0755));
            touchFile(slot + "/sf-0001.sfa");
        }
        ObjList<String> orphans = OrphanScanner.scan(sfDir, "exclude-me");
        assertEquals(3, orphans.size());
    }

    @Test
    public void testIsCandidateOrphanDirect() {
        String slot = sfDir + "/probe";
        assertEquals(0, Files.mkdir(slot, 0755));
        assertFalse("empty slot is not a candidate",
                OrphanScanner.isCandidateOrphan(slot));
        touchFile(slot + "/sf-0001.sfa");
        assertTrue("slot with sfa is a candidate",
                OrphanScanner.isCandidateOrphan(slot));
        OrphanScanner.markFailed(slot, "x");
        assertFalse("slot with .failed is not a candidate",
                OrphanScanner.isCandidateOrphan(slot));
    }

    private static void touchFile(String path) {
        int fd = Files.openRW(path);
        if (fd >= 0) Files.close(fd);
    }

    private static void rmDirRec(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find != 0) {
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
