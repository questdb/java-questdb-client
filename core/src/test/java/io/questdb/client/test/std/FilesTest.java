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
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FilesTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-files-test-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) {
            return;
        }
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
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
            int fd = Files.openCleanRW(path);
            assertTrue("expected fd >= 0, got " + fd, fd >= 0);
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
            int fd = Files.openCleanRW(path);
            try {
                assertTrue(Files.truncate(fd, 1024));
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

    /** Pins the cross-platform contract on `Files.allocate`:
     * never-shrinks, short-circuits on size <= currentSize, extends on
     * size > currentSize. All four assertions must hold identically on
     * Linux, macOS, and Windows — see Files.allocate javadoc. */
    @Test
    public void testAllocateNeverShrinks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/alloc-shrink.bin";
            int fd = Files.openRW(path);
            try {
                // Grow to 64 KiB.
                assertTrue(Files.allocate(fd, 65536));
                assertEquals(65536, Files.length(fd));

                // Smaller request: must not shrink the file.
                assertTrue(Files.allocate(fd, 4096));
                assertEquals(65536, Files.length(fd));

                // Equal request: no-op success, size unchanged.
                assertTrue(Files.allocate(fd, 65536));
                assertEquals(65536, Files.length(fd));

                // Larger request: extends to the new target.
                assertTrue(Files.allocate(fd, 131072));
                assertEquals(131072, Files.length(fd));
            } finally {
                Files.close(fd);
            }
        });
    }

    /** A size=0 allocate on a fresh file is a no-op success — exercises
     * the same short-circuit as testAllocateNeverShrinks but with the
     * edge case of an empty file (no fallocate / F_PREALLOCATE syscall
     * should reach the kernel). */
    @Test
    public void testAllocateZeroOnFreshFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/alloc-zero.bin";
            int fd = Files.openRW(path);
            try {
                assertTrue(Files.allocate(fd, 0));
                assertEquals(0, Files.length(fd));
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
            int fd = Files.openCleanRW(a);
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
                int fd = Files.openCleanRW(tmpDir + "/" + n);
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
            int fd1 = Files.openCleanRW(path);
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
            int fd = Files.openCleanRW(path);
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

    @Test
    public void testMmapRoundtrip() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/mmap.bin";
            int fd = Files.openCleanRW(path);
            try {
                assertTrue(Files.allocate(fd, 8192));
                long addr = Files.mmap(fd, 8192, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
                assertNotEquals("mmap returned FAILED", Files.FAILED_MMAP_ADDRESS, addr);
                try {
                    // Write through the mapping.
                    Unsafe.getUnsafe().putLong(addr, 0xDEADBEEFCAFEBABEL);
                    Unsafe.getUnsafe().putLong(addr + 8, 0x0123456789ABCDEFL);
                    // Force pages to disk so a separate read sees them.
                    assertEquals(0, Files.msync(addr, 16, false));
                } finally {
                    Files.munmap(addr, 8192, MemoryTag.MMAP_DEFAULT);
                }
            } finally {
                Files.close(fd);
            }

            // Re-open and verify via pread that the bytes hit the file.
            int fd2 = Files.openRO(path);
            try {
                long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
                try {
                    assertEquals(16, Files.read(fd2, buf, 16, 0));
                    assertEquals(0xDEADBEEFCAFEBABEL, Unsafe.getUnsafe().getLong(buf));
                    assertEquals(0x0123456789ABCDEFL, Unsafe.getUnsafe().getLong(buf + 8));
                } finally {
                    Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                Files.close(fd2);
            }
        });
    }

    /**
     * Red test for bug M2 — {@code Files.close(int)} refuses fds 0/1/2 via
     * the predicate {@code if (fd > 2)} (lines 42-47), returning -1 without
     * invoking the underlying native {@code close(2)}. On a container where
     * stdin/stdout/stderr were pre-closed before the JVM started,
     * {@code openRW} can legitimately return 0/1/2 — and {@code Files.close}
     * then leaks the descriptor until JVM exit. The fix is to remove the
     * guard or change it to {@code if (fd >= 0)}.
     * <p>
     * Cannot test in-process because closing real fd 0/1/2 would break the
     * test runner's stdin/stdout/stderr. Instead spawn a child JVM whose
     * stdin is redirected to a temp file (so fd 0 is a closeable file). The
     * child calls {@code Files.close(0)} and reports the result via exit
     * code: 0 if close succeeded (post-fix expected), 1 if refused (current
     * bug).
     */
    @Test
    public void testFilesCloseAcceptsFdZero() throws Exception {
        Assume.assumeTrue("subprocess test needs java executable on PATH",
                new File(System.getProperty("java.home"), "bin/java").exists());

        File stdinFile = File.createTempFile("m2-stdin-", ".tmp");
        stdinFile.deleteOnExit();

        File javaBin = new File(System.getProperty("java.home"), "bin/java");
        // Surefire wraps the classpath in a manifest jar so java.class.path
        // is useless here. Compute the classpath from the actual class locations.
        File mainClasses = new File(
                Files.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        File testClasses = new File(
                FilesTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        String classpath = mainClasses.getAbsolutePath()
                + File.pathSeparator + testClasses.getAbsolutePath();

        ProcessBuilder pb = new ProcessBuilder(
                javaBin.getAbsolutePath(),
                "-cp", classpath,
                FilesCloseFdZeroChild.class.getName()
        );
        pb.redirectInput(stdinFile);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process p = pb.start();
        boolean finished = p.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            p.destroyForcibly();
            throw new AssertionError("child JVM did not exit within 30s");
        }
        int exit = p.exitValue();
        // Exit 0: Files.close(0) returned 0 (close attempted and succeeded).
        // Exit 1: Files.close(0) returned -1 (predicate refused — current bug).
        // Exit 2: child harness error.
        assertEquals(
                "Files.close(0) must attempt the close. Child returned " + exit
                        + " (1 = predicate refusal — bug M2; 0 = post-fix correct).",
                0, exit);
    }

    /**
     * Child JVM entry point for {@link #testFilesCloseAcceptsFdZero()}. Its
     * stdin is the redirected temp file from {@link ProcessBuilder}, so fd 0
     * is a regular file safe to close.
     */
    public static class FilesCloseFdZeroChild {
        public static void main(String[] args) {
            try {
                int rc = Files.close(0);
                System.exit(rc == 0 ? 0 : 1);
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(2);
            }
        }
    }
}
