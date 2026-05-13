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

package io.questdb.client.test.std;

import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Red test for M7 — {@link Files#findFirst(String)} cannot today be used
 * to distinguish "directory does not exist / could not be opened" from
 * "directory is empty". Both return 0.
 *
 * <p>On POSIX, a real existing directory always contains at least
 * {@code .} and {@code ..}, so {@code findFirst == 0} in practice always
 * means an opendir failure. But callers in {@code SegmentRing.openExisting},
 * {@code OrphanScanner.scan}, {@code CursorSendEngine.unlinkAllSegmentFiles}
 * and {@code SegmentManager.scanMaxGeneration} all treat 0 as "nothing
 * to do, return silently" — so a transient EACCES / ENOENT during recovery
 * silently turns into "the slot was empty", and the engine's next step is
 * to write a fresh {@code sf-initial.sfa} that may overlap FSN 0 with on-
 * disk segments the JVM couldn't enumerate. Diagnostic loss + potential
 * data overlap.
 *
 * <p>This test pins the desired post-fix contract: {@code findFirst} on
 * a path that doesn't exist (or otherwise can't be opened) must return a
 * sentinel that callers can distinguish from a genuinely-empty existing
 * directory. The simplest workable convention is a negative return value
 * (e.g. {@code -1L}), preserving zero for the "directory exists, has zero
 * relevant entries" case (rare on POSIX, possible via Windows special
 * filesystems).
 *
 * <p>Whatever the fix shape (return {@code -1L}, throw, expose
 * {@code findLastErrno}), the user-visible invariant pinned here is:
 * <em>findFirst on a missing path must NOT return the same value it
 * returns for an empty existing directory</em>.
 */
public class FilesFindFirstErrorTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-files-findfirst-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        Files.remove(tmpDir);
    }

    /**
     * The sentinel for "opendir failed" should be a NEGATIVE value so
     * existing checks of the form {@code if (find == 0)} can be promoted
     * to {@code if (find <= 0)} without ambiguity, and {@code if (find < 0)}
     * surfaces the error so callers can warn / refuse rather than silently
     * treat an inaccessible slot as empty.
     *
     * <p>Pinning {@code -1L} specifically is one valid convention; the
     * test phrases the assertion as "negative" so the fix has freedom to
     * pick any negative sentinel.
     */
    @Test
    public void testFindFirstReturnsNegativeOnMissingPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String missing = tmpDir + "/never-existed-" + System.nanoTime();
            long h = Files.findFirst(missing);
            try {
                assertTrue(
                        "findFirst on a missing path returned " + h + ". "
                                + "After M7: should be negative so callers can "
                                + "distinguish 'opendir failed' (negative) from "
                                + "'empty directory' (zero).",
                        h < 0);
            } finally {
                if (h > 0L) Files.findClose(h);
            }
        });
    }
}
