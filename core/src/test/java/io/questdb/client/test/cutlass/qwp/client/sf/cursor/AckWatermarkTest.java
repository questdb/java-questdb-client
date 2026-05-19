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

import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AckWatermarkTest {

    private String slotDir;

    @Before
    public void setUp() {
        slotDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ackwatermark-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (slotDir == null) return;
        Files.remove(slotDir + "/" + AckWatermark.FILE_NAME);
        Files.remove(slotDir);
    }

    @Test
    public void testCrossSessionPersistence() throws Exception {
        // Open, write, close (simulates a sender session). Then open
        // again (simulates a recovery in a fresh process) and observe
        // the previously-written value.
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(12_345L);
            }
            assertTrue("watermark file must persist across close",
                    Files.exists(slotDir + "/" + AckWatermark.FILE_NAME));
            try (AckWatermark w2 = AckWatermark.open(slotDir)) {
                assertNotNull(w2);
                assertEquals("recovered value must match written value",
                        12_345L, w2.read());
            }
        });
    }

    @Test
    public void testFreshFileReadsAsInvalid() throws Exception {
        // open() creates the file zero-filled, so magic is 0 and read()
        // must report INVALID until the first write stamps the magic.
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                assertEquals(AckWatermark.INVALID, w.read());
            }
        });
    }

    @Test
    public void testNegativeFsnRoundTrips() throws Exception {
        // Engine seeds use -1 in the no-prior-history case. A
        // persisted -1 should round-trip so recovery can pick the
        // right seed.
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(-1L);
                assertEquals(-1L, w.read());
            }
        });
    }

    @Test
    public void testRemoveOrphanDeletesFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(42L);
            }
            String path = slotDir + "/" + AckWatermark.FILE_NAME;
            assertTrue("write+close must leave file in place", Files.exists(path));
            AckWatermark.removeOrphan(slotDir);
            assertFalse("removeOrphan must delete the file", Files.exists(path));
            // Idempotent: second remove on missing file must not throw.
            AckWatermark.removeOrphan(slotDir);
        });
    }

    @Test
    public void testRepeatedWriteUpdatesValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(100L);
                assertEquals(100L, w.read());
                w.write(200L);
                assertEquals(200L, w.read());
                w.write(Long.MAX_VALUE);
                assertEquals(Long.MAX_VALUE, w.read());
            }
        });
    }

    @Test
    public void testStaleFileWithWrongSizeIsResetOnOpen() throws Exception {
        // A leftover file with an unexpected size (corruption, partial
        // write from an older format, manual tampering) must not poison
        // recovery. open() detects the wrong size and truncates to the
        // expected layout — read() then reports INVALID until the next
        // write.
        TestUtils.assertMemoryLeak(() -> {
            String path = slotDir + "/" + AckWatermark.FILE_NAME;
            int fd = Files.openCleanRW(path);
            assertTrue(fd >= 0);
            assertTrue(Files.truncate(fd, 4));
            Files.close(fd);
            assertEquals("precondition: file exists at wrong size",
                    4L, Files.length(path));

            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull("open must succeed despite wrong-sized stale file", w);
                assertEquals("stale wrong-sized file must read as INVALID",
                        AckWatermark.INVALID, w.read());
                w.write(777L);
                assertEquals(777L, w.read());
            }
            assertEquals("after open+write, file must be the expected size",
                    AckWatermark.FILE_SIZE, Files.length(path));
        });
    }

    @Test
    public void testWriteReadInSameSession() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(0L);
                assertEquals(0L, w.read());
            }
        });
    }
}
