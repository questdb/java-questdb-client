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

package io.questdb.client.test.tools;

import io.questdb.client.std.BinarySequence;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Files;
import io.questdb.client.std.IntList;
import io.questdb.client.std.LongList;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Os;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.ThreadLocal;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Sinkable;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8Sequence;
import io.questdb.client.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;

public final class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);
    private static final ThreadLocal<StringSink> tlSink = new ThreadLocal<>(StringSink::new);

    private TestUtils() {
    }

    public static boolean areEqual(BinarySequence a, BinarySequence b) {
        if (a == b) return true;
        if (a == null || b == null) return false;

        if (a.length() != b.length()) return false;
        for (int i = 0; i < a.length(); i++) {
            if (a.byteAt(i) != b.byteAt(i)) return false;
        }
        return true;
    }

    public static void assertContains(String message, CharSequence sequence, CharSequence term) {
        // Assume that "" is contained in any string.
        if (term.length() == 0) {
            return;
        }
        if (Chars.contains(sequence, term)) {
            return;
        }
        Assert.fail((message != null ? message + ": '" : "'") + sequence + "' does not contain: " + term);
    }

    public static void assertContains(CharSequence sequence, CharSequence term) {
        assertContains(null, sequence, term);
    }

    public static void assertEquals(CharSequence expected, Sinkable actual) {
        StringSink sink = getTlSink();
        actual.toSink(sink);
        assertEquals(null, expected, sink);
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

    public static void assertEquals(Utf8Sequence expected, Utf8Sequence actual) {
        if (expected == null && actual == null) {
            return;
        }

        if (expected != null && actual == null) {
            Assert.fail("Expected: \n`" + expected + "`\nbut have NULL. ");
        }

        if (expected == null) {
            Assert.fail("Expected: NULL but have \n`" + actual + "`\n");
        }

        if (expected.size() != actual.size()) {
            Assert.assertEquals(expected, actual);
        }

        StringSink sink = getTlSink();
        Utf8s.utf8ToUtf16(expected, sink);
        String expectedStr = sink.toString();
        sink.clear();
        Utf8s.utf8ToUtf16(actual, sink);
        assertEquals(expectedStr, sink);
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

    public static void assertEquals(BinarySequence bs, BinarySequence actBs, long actualLen) {
        if (bs == null) {
            Assert.assertNull(actBs);
            Assert.assertEquals(-1, actualLen);
        } else {
            Assert.assertEquals(bs.length(), actBs.length());
            Assert.assertEquals(bs.length(), actualLen);
            for (long l = 0, z = bs.length(); l < z; l++) {
                byte b1 = bs.byteAt(l);
                byte b2 = actBs.byteAt(l);
                if (b1 != b2) {
                    Assert.fail("Failed comparison at [" + l + "], expected: " + b1 + ", actual: " + b2);
                }
                Assert.assertEquals(bs.byteAt(l), actBs.byteAt(l));
            }
        }
    }

    public static void assertEquals(LongList expected, LongList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    public static void assertEquals(IntList expected, IntList actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    public static <T> void assertEquals(ObjList<T> expected, ObjList<T> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0, n = expected.size(); i < n; i++) {
            if (expected.getQuick(i) != actual.getQuick(i)) {
                Assert.assertEquals("index " + i, expected.getQuick(i), actual.getQuick(i));
            }
        }
    }

    public static void assertEqualsIgnoreCase(CharSequence expected, CharSequence actual) {
        assertEqualsIgnoreCase(null, expected, actual);
    }

    public static void assertEqualsIgnoreCase(String message, CharSequence expected, CharSequence actual) {
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
            if (Character.toLowerCase(expected.charAt(i)) != Character.toLowerCase(actual.charAt(i))) {
                Assert.assertEquals(message, expected, actual);
            }
        }
    }

    public static void assertEventually(EventualCode assertion) throws Exception {
        assertEventually(assertion, 60);
    }

    public static void assertEventually(EventualCode assertion, Set<Class<?>> exceptionTypesToCatch) throws Exception {
        exceptionTypesToCatch.add(AssertionError.class);
        assertEventually(assertion, 30, exceptionTypesToCatch);
    }

    public static void assertEventually(EventualCode assertion, int timeoutSeconds, Set<Class<?>> exceptionTypesToCatch) throws Exception {
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertion.run();
                return;
            } catch (Exception error) {
                if (!exceptionTypesToCatch.contains(error.getClass())) {
                    throw error;
                }
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
        }
    }

    public static void assertEventually(EventualCode assertion, int timeoutSeconds) throws Exception {
        long maxSleepingTimeMillis = 1000;
        long nextSleepingTimeMillis = 10;
        long startTime = System.nanoTime();
        long deadline = startTime + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        for (; ; ) {
            try {
                assertion.run();
                return;
            } catch (AssertionError error) {
                if (System.nanoTime() >= deadline) {
                    throw error;
                }
            }
            Os.sleep(nextSleepingTimeMillis);
            nextSleepingTimeMillis = Math.min(maxSleepingTimeMillis, nextSleepingTimeMillis << 1);
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

    /**
     * Asserts that a {@code CharSequence} does NOT contain another {@code CharSequence}.
     *
     * @param sequence the {@code CharSequence} to check.
     * @param term     the {@code CharSequence} to search for (and assert its absence).
     * @see #assertNotContains(String, CharSequence, CharSequence)
     */
    public static void assertNotContains(CharSequence sequence, CharSequence term) {
        assertNotContains(null, sequence, term);
    }

    /**
     * Asserts that a {@code CharSequence} does NOT contain another {@code CharSequence}.
     * <p>
     * Fails if the {@code term} is empty (""), because the convention established by
     * {@link #assertContains(String, CharSequence, CharSequence)} considers an empty
     * term to be contained within any sequence.
     * </p>
     *
     * @param message  the identifying message for the {@link AssertionError} (<code>null</code> okay)
     * @param sequence the {@code CharSequence} to check.
     * @param term     the {@code CharSequence} to search for (and assert its absence).
     */
    public static void assertNotContains(String message, CharSequence sequence, CharSequence term) {
        if (term.length() == 0) {
            String formatted = "";
            if (message != null) {
                formatted = message + " ";
            }
            Assert.fail(formatted + "Cannot assert that sequence does not contain an empty term; an empty term is always considered contained by definition.");
        }

        if (!Chars.contains(sequence, term)) {
            return;
        }

        String formatted = "";
        if (message != null) {
            formatted = message + " ";
        }
        Assert.fail(formatted + "Expected sequence <" + sequence + "> to NOT contain term <" + term + "> but it did.");
    }

    public static void await(CyclicBarrier barrier) {
        try {
            barrier.await();
        } catch (Throwable ignore) {
        }
    }

    public static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (Throwable ignore) {
        }
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static long beHexToLong(String hex) {
        return Long.parseLong(reverseBeHex(hex), 16);
    }

    public static void copyMimeTypes(String targetDir) throws IOException {
        try (InputStream stream = Files.class.getResourceAsStream("/io/questdb/site/conf/mime.types")) {
            assertNotNull(stream);
            final File target = new File(targetDir, "conf/mime.types");
            Assert.assertTrue(target.getParentFile().mkdirs());
            try (FileOutputStream fos = new FileOutputStream(target)) {
                byte[] buffer = new byte[1024 * 1204];
                int len;
                while ((len = stream.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
            }
        }
    }

    public static boolean equals(CharSequence expected, CharSequence actual) {
        if (expected == null && actual == null) {
            return true;
        }

        if (expected == null || actual == null) {
            return false;
        }

        if (expected.length() != actual.length()) {
            return false;
        }

        for (int i = 0; i < expected.length(); i++) {
            if (expected.charAt(i) != actual.charAt(i)) {
                return false;
            }
        }
        return true;
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
        StringSink sink = getTlSink();
        Numbers.intToIPv4Sink(sink, ip);
        return sink.toString();
    }

    public static String readStringFromFile(File file) {
        try {
            try (FileInputStream fis = new FileInputStream(file)) {
                byte[] buffer = new byte[(int) fis.getChannel().size()];
                int totalRead = 0;
                int read;
                while (totalRead < buffer.length && (read = fis.read(buffer, totalRead, buffer.length - totalRead)) > 0) {
                    totalRead += read;
                }
                return new String(buffer, Files.UTF_8);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot read from " + file.getAbsolutePath(), e);
        }
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    public static String reverseBeHex(String hex) {
        var sb = new char[hex.length()];
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

    public static void unchecked(CheckedRunnable runnable, AtomicInteger failureCounter) {
        try {
            runnable.run();
        } catch (Throwable e) {
            failureCounter.incrementAndGet();
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

    public static void writeStringToFile(File file, String s) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(s.getBytes(Files.UTF_8));
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

    public interface EventualCode {
        void run() throws Exception;
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

            // Checks that the same tag used for allocation and freeing native memory
            long memAfter = Unsafe.getMemUsed();
            long memNativeSqlCompilerDiff = 0;
            Assert.assertTrue(memAfter > -1);
            if (mem != memAfter) {
                for (int i = MemoryTag.MMAP_DEFAULT; i < MemoryTag.SIZE; i++) {
                    long actualMemByTag = Unsafe.getMemUsedByTag(i);
                    if (memoryUsageByTag[i] != actualMemByTag) {
                        Assert.assertTrue(actualMemByTag >= memoryUsageByTag[i]);
                        memNativeSqlCompilerDiff = actualMemByTag - memoryUsageByTag[i];
                    }
                }
                Assert.assertEquals(mem + memNativeSqlCompilerDiff, memAfter);
            }
        }

        public void skipChecks() {
            skipChecksOnClose = true;
        }
    }
}
