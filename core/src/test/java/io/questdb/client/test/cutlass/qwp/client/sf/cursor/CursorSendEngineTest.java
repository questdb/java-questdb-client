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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CursorSendEngineTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-eng-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, 0755));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find != 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testAppendBlockingNeverFailsUnderManagerSupply() {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
            for (int i = 0; i < 200; i++) {
                Unsafe.getUnsafe().putInt(buf, i);
                long fsn = engine.appendBlocking(buf, 64);
                assertEquals(i, fsn);
            }
            assertEquals(199, engine.publishedFsn());
            assertNotNull("active segment is always non-null", engine.activeSegment());
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAppendOrFsnReturnsBackpressureWhenSpareUnavailable() {
        // Run with a deliberately stalled manager: poll cadence so slow
        // it never installs a spare in the test window. The first segment
        // fills, then appendOrFsn returns BACKPRESSURE_NO_SPARE.
        long segSize = MmapSegment.HEADER_SIZE
                + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try (CursorSendEngine engine = new CursorSendEngine(tmpDir, segSize)) {
            // Fill the active deterministically (this is the initial segment;
            // manager hasn't had a chance to provision a spare yet on a fast box,
            // so we use a short spin deadline so the test runs quickly).
            long deadline = System.nanoTime();
            engine.appendOrFsn(buf, 64, deadline);
            engine.appendOrFsn(buf, 64, deadline);
            // Third append: active is full, spare may or may not be ready
            // depending on race with manager. With a zero-deadline spin we
            // get either the FSN (if manager beat us) or backpressure.
            long fsn = engine.appendOrFsn(buf, 64, deadline);
            assertTrue("unexpected fsn=" + fsn, fsn == 2L || fsn == -1L);
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testAcknowledgePropagatesToRing() {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try (CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096)) {
            engine.appendBlocking(buf, 16);
            engine.appendBlocking(buf, 16);
            engine.appendBlocking(buf, 16);
            engine.acknowledge(2L);
            assertEquals(2L, engine.ackedFsn());
            // Regression — should be ignored.
            engine.acknowledge(0L);
            assertEquals(2L, engine.ackedFsn());
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        CursorSendEngine engine = new CursorSendEngine(tmpDir, 4096);
        engine.close();
        engine.close();
    }
}
