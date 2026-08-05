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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.SfManifest;

import io.questdb.client.std.FilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Direct pin on {@link SfManifest#update}'s monotonic clamp. Committed
 * boundaries only ever move forward (head advances on trim, active on
 * rotation); the two writers are serialized on the ring monitor but may each
 * compute arguments from a snapshot the other has already moved past.
 * Regressing a durable boundary would let a later crash-recovery demand a
 * segment file the trim path already unlinked (permanent "missing head
 * segment" startup failure) or re-expose stale files below a committed head.
 * <p>
 * Before this test existed, deleting the clamp passed the entire sf suite
 * (verified by live mutation run) -- the same blindness class as the
 * neutralized rotation gate that commit 88d6b792 accidentally shipped. This
 * test lives in the production package deliberately: the manifest API is
 * package-private and the clamp deserves a direct unit pin, not an
 * integration-distance one.
 */
public class SfManifestClampTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-sf-manifest-clamp-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test
    public void testUpdateClampsRegressingBoundariesAndPersistsForward() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            FilesFacade ff = FilesFacade.INSTANCE;
            SfManifest manifest = SfManifest.create(ff, tmpDir, 10L, 20L);
            try {
                assertEquals(10L, manifest.headBase());
                assertEquals(20L, manifest.activeBase());

                // A fully regressing update must be clamped on both fields.
                manifest.update(5L, 15L);
                assertEquals("regressing headBase must be clamped", 10L, manifest.headBase());
                assertEquals("regressing activeBase must be clamped", 20L, manifest.activeBase());

                // Fields clamp independently: head may advance while a stale
                // active snapshot is clamped in the same call.
                manifest.update(12L, 18L);
                assertEquals(12L, manifest.headBase());
                assertEquals("stale activeBase snapshot must be clamped independently",
                        20L, manifest.activeBase());

                // Forward motion is untouched.
                manifest.update(12L, 25L);
                assertEquals(12L, manifest.headBase());
                assertEquals(25L, manifest.activeBase());
            } finally {
                manifest.close();
            }

            // The clamp must hold in the durable record, not just in memory:
            // reopen and verify the forward-only boundaries survived.
            SfManifest reopened = SfManifest.open(ff, tmpDir);
            assertNotNull("manifest must reopen", reopened);
            try {
                assertEquals(12L, reopened.headBase());
                assertEquals(25L, reopened.activeBase());
            } finally {
                reopened.close();
            }
        });
    }
}
