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

package io.questdb.client.test.tools;

import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.ThreadLocal;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8Sequence;
import io.questdb.client.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertNotNull;

public final class TestUtils {
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private TestUtils() {
    }

    public static void assertContains(String message, CharSequence sequence, CharSequence term) {
        // Assume that "" is contained in any string.
        if (term.length() == 0) {
            return;
        }
        if (sequence.toString().contains(term.toString())) {
            return;
        }
        Assert.fail((message != null ? message + ": '" : "'") + sequence + "' does not contain: " + term);
    }

    public static void assertContains(CharSequence sequence, CharSequence term) {
        assertContains(null, sequence, term);
    }

    public static void assertEquals(CharSequence expected, Utf8Sequence actual) {
        StringSink sink = getTlSink();
        Utf8s.utf8ToUtf16(actual, sink);
        assertEquals(null, expected, sink);
    }

    public static void assertEquals(byte[] expected, Utf8Sequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + Arrays.toString(expected) + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length != actual.size()) {
            Assert.fail("Expected size: " + expected.length + ", but have " + actual.size());
        }

        for (int i = 0; i < expected.length; i++) {
            if (expected[i] != actual.byteAt(i)) {
                Assert.fail("Expected byte: " + expected[i] + ", but have " + actual.byteAt(i) + " at index " + i);
            }
        }
    }

    public static void assertEquals(CharSequence expected, CharSequence actual) {
        assertEquals(null, expected, actual);
    }

    public static void assertEquals(String message, CharSequence expected, CharSequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`but have NULL");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.length() != actual.length()) {
            Assert.assertEquals(message, expected, actual);
        }

        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                Assert.assertEquals(message, expected, actual);
            }
        }
    }

    public static void assertMemoryLeak(LeakProneCode runnable) throws Exception {
        try (LeakCheck ignore = new LeakCheck()) {
            try {
                runnable.run();
            } catch (Throwable e) {
                ignore.skipChecks();
                throw e;
            }
        }
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static long beHexToLong(String hex) {
        return Long.parseLong(reverseBeHex(hex), 16);
    }

    /**
     * Creates a unique temp directory under {@code java.io.tmpdir}, named
     * {@code prefix + <nanoTime>}, and returns its path. Pair with
     * {@link #removeTmpDir(String)} in {@code tearDown}.
     */
    public static String createTmpDir(String prefix) {
        String dir = Paths.get(System.getProperty("java.io.tmpdir"),
                prefix + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
        return dir;
    }

    @NotNull
    public static Rnd generateRandom(Logger log) {
        return generateRandom(log, System.nanoTime(), System.currentTimeMillis());
    }

    @NotNull
    public static Rnd generateRandom(Logger log, long s0, long s1) {
        if (log != null) {
            log.info("random seeds: {}L, {}L", s0, s1);
        }
        System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        Rnd rnd = new Rnd(s0, s1);
        // Random impl is biased on first few calls, always return same bool,
        // so we need to make a few calls to get it going randomly
        rnd.nextBoolean();
        rnd.nextBoolean();
        return rnd;
    }

    public static String getTestResourcePath(String resourceName) {
        URL resource = TestUtils.class.getResource(resourceName);
        assertNotNull("Someone accidentally deleted test resource " + resourceName + "?", resource);
        try {
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Could not determine resource path", e);
        }
    }

    public static String ipv4ToString(int ip) {
        return ((ip >> 24) & 0xff) + "." + ((ip >> 16) & 0xff) + "." + ((ip >> 8) & 0xff) + "." + (ip & 0xff);
    }

    /**
     * Flat (non-recursive) cleanup for a directory created by
     * {@link #createTmpDir(String)}: removes every entry in {@code tmpDir}
     * (the SF cursor tests only write flat {@code .sfa}/{@code .corrupt}
     * files) and then the directory itself. A {@code null} argument is a
     * no-op, so it is safe to call from {@code tearDown} before {@code setUp}
     * ran.
     */
    public static void removeTmpDir(String tmpDir) {
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

    /**
     * Java 8 stand-in for {@code String.repeat(int)} (added in Java 11).
     */
    public static String repeat(CharSequence s, int count) {
        StringBuilder sb = new StringBuilder(s.length() * Math.max(0, count));
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static String reverseBeHex(String hex) {
        char[] sb = new char[hex.length()];
        for (int i = 0; i < hex.length(); i += 2) {
            sb[hex.length() - i - 1] = hex.charAt(i + 1);
            sb[hex.length() - i - 2] = hex.charAt(i);
        }
        return new String(sb);
    }

    public static long toMemory(CharSequence sequence) {
        long ptr = Unsafe.malloc(sequence.length(), MemoryTag.NATIVE_DEFAULT);
        Utf8s.strCpyAscii(sequence, sequence.length(), ptr);
        return ptr;
    }

    public static void unchecked(CheckedRunnable runnable) {
        try {
            runnable.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T unchecked(CheckedSupplier<T> runnable) {
        try {
            return runnable.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public static int unchecked(CheckedIntFunction runnable) {
        try {
            return runnable.get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static StringSink getTlSink() {
        StringSink ss = tlSink.get();
        ss.clear();
        return ss;
    }

    public interface CheckedIntFunction {
        int get() throws Throwable;
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws Throwable;
    }

    public interface CheckedSupplier<T> {
        T get() throws Throwable;
    }

    @FunctionalInterface
    public interface LeakProneCode {
        void run() throws Exception;
    }

    public static class LeakCheck implements QuietCloseable {
        private final long mem;
        private final long[] memoryUsageByTag = new long[MemoryTag.SIZE];
        private boolean skipChecksOnClose;

        public LeakCheck() {
            mem = Unsafe.getMemUsed();
            for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
                memoryUsageByTag[i] = Unsafe.getMemUsedByTag(i);
            }
        }

        @Override
        public void close() {
            if (skipChecksOnClose) {
                return;
            }

            // Every tag must return to its baseline. The previous shape
            // (ported from upstream, which exempts NATIVE_SQL_COMPILER only)
            // absorbed any growth confined to a single tag into a tolerated
            // diff, so a lone-tag leak (e.g. NATIVE_DEFAULT) passed the check.
            // This client has no SQL-compiler tag, so no exemption applies:
            // assert strict per-tag equality, then total equality.
            long memAfter = Unsafe.getMemUsed();
            Assert.assertTrue(memAfter > -1);
            for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
                long actualMemByTag = Unsafe.getMemUsedByTag(i);
                if (memoryUsageByTag[i] != actualMemByTag) {
                    Assert.assertEquals(
                            "native memory leaked or over-freed under tag " + MemoryTag.nameOf(i),
                            memoryUsageByTag[i], actualMemByTag);
                }
            }
            Assert.assertEquals("total native memory", mem, memAfter);
        }

        public void skipChecks() {
            skipChecksOnClose = true;
        }
    }
}
