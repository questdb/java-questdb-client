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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class FilesTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-files-test-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, 0755));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) {
            return;
        }
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
    public void testWriteReadRoundtrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/test.bin";
            int fd = Files.openCleanRW(path, 0);
            assertTrue("expected fd > 0, got " + fd, fd > 0);
            try {
                long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(buf, 0xDEADBEEFCAFEBABEL);
                    assertEquals(8, Files.write(fd, buf, 8, 0));
                    assertEquals(0, Files.fsync(fd));
                    assertEquals(8, Files.length(fd));

                    long buf2 = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.getUnsafe().putLong(buf2, 0L);
                        assertEquals(8, Files.read(fd, buf2, 8, 0));
                        assertEquals(0xDEADBEEFCAFEBABEL, Unsafe.getUnsafe().getLong(buf2));
                    } finally {
                        Unsafe.free(buf2, 8, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                assertEquals(0, Files.close(fd));
            }
            assertEquals(8, Files.length(path));
        });
    }

    @Test
    public void testTruncate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/trunc.bin";
            int fd = Files.openCleanRW(path, 1024);
            try {
                assertEquals(1024, Files.length(fd));
                assertTrue(Files.truncate(fd, 0));
                assertEquals(0, Files.length(fd));
                assertTrue(Files.truncate(fd, 4096));
                assertEquals(4096, Files.length(fd));
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testAllocate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/alloc.bin";
            int fd = Files.openRW(path);
            try {
                assertTrue(Files.allocate(fd, 65536));
                assertTrue(Files.length(fd) >= 65536);
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testAppend() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/app.bin";
            int fd = Files.openAppend(path);
            try {
                long buf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(buf, 0xCAFEBABE);
                    assertEquals(4, Files.append(fd, buf, 4));
                    assertEquals(4, Files.append(fd, buf, 4));
                    assertEquals(8, Files.length(fd));
                } finally {
                    Unsafe.free(buf, 4, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }
        });
    }

    @Test
    public void testRename() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String a = tmpDir + "/a";
            String b = tmpDir + "/b";
            int fd = Files.openCleanRW(a, 0);
            Files.close(fd);
            assertTrue(Files.exists(a));
            assertEquals(0, Files.rename(a, b));
            assertFalse(Files.exists(a));
            assertTrue(Files.exists(b));
        });
    }

    @Test
    public void testFindFirstIteratesAllEntries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String[] names = {"alpha", "beta", "gamma"};
            for (String n : names) {
                int fd = Files.openCleanRW(tmpDir + "/" + n, 0);
                Files.close(fd);
            }
            long find = Files.findFirst(tmpDir);
            assertNotEquals(0, find);
            int countMatches = 0;
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null) {
                        for (String expected : names) {
                            if (expected.equals(name)) {
                                countMatches++;
                                break;
                            }
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
            assertEquals(3, countMatches);
        });
    }

    @Test
    public void testLockExclusive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/lock.bin";
            int fd1 = Files.openCleanRW(path, 0);
            int fd2 = Files.openRW(path);
            try {
                assertEquals(0, Files.lock(fd1));
                assertEquals(-1, Files.lock(fd2));
            } finally {
                Files.close(fd1);
                Files.close(fd2);
            }
        });
    }

    @Test
    public void testExistsAndRemove() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/x";
            assertFalse(Files.exists(path));
            int fd = Files.openCleanRW(path, 0);
            Files.close(fd);
            assertTrue(Files.exists(path));
            assertTrue(Files.remove(path));
            assertFalse(Files.exists(path));
        });
    }

    @Test
    public void testPageSizeIsSane() {
        assertTrue("PAGE_SIZE positive", Files.PAGE_SIZE > 0);
        long ps = Files.PAGE_SIZE;
        assertEquals("PAGE_SIZE power of 2", 0, ps & (ps - 1));
    }
}
