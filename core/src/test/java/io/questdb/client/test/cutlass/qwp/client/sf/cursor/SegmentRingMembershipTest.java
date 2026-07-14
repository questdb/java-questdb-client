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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentRingMembershipTest {

    private static final String CURSOR_PACKAGE = "io.questdb.client.cutlass.qwp.client.sf.cursor.";
    private static final long SEGMENT_SIZE = MmapSegment.HEADER_SIZE
            + MmapSegment.FRAME_HEADER_SIZE + 16;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ring-membership-" + System.nanoTime()).toString();
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
    public void testLargeRecoveryCountsMembershipPrimitives() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int n = 2048;
            createChain(n);

            CountingOperations operations = new CountingOperations();
            try (SegmentRing ring = recover(newMembershipObserver(operations, false))) {
                assertRecoveredChain(ring, n);
            }
            long linearBound = 2L * n;
            assertTrue("production default took " + operations.count
                            + " membership operations; expected fewer than " + linearBound,
                    operations.count < linearBound);
            assertEquals("production default identity map must do one lookup per discovered segment",
                    n, operations.count);
        });
    }

    @Test
    public void testMembershipAllocationFailureRetainsLocalOwnership() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            createChain(8);
            String extraPath = tmpDir + "/sf-extra.sfa";
            MmapSegment.create(extraPath, 8, SEGMENT_SIZE).close();
            try {
                recover(newMembershipObserver(null, true));
                fail("expected injected membership allocation failure");
            } catch (OutOfMemoryError expected) {
                assertEquals("injected membership allocation failure", expected.getMessage());
            }

            try (SegmentRing ring = SegmentRing.openExisting(
                    FilesFacade.INSTANCE,
                    tmpDir,
                    SEGMENT_SIZE
            )) {
                assertRecoveredChain(ring, 8);
            }
            assertFalse("retry must close and remove the unselected empty extra", Files.exists(extraPath));
        });
    }

    private static void assertRecoveredChain(SegmentRing ring, int n) {
        assertNotNull(ring);
        assertEquals(n, ring.nextSeqHint());
        assertEquals(n - 1, ring.publishedFsn());
        assertEquals(n - 1, ring.getSealedSegments().size());
    }

    private void createChain(int n) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) i);
            }
            for (int i = 0; i < n; i++) {
                String name = String.format("sf-%05d.sfa", i);
                try (MmapSegment segment = MmapSegment.create(tmpDir + "/" + name, i, SEGMENT_SIZE)) {
                    assertTrue("setup append " + i, segment.tryAppend(buf, 16) >= 0);
                }
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private SegmentRing recover(Object membershipObserver) throws Exception {
        Class<?> observerClass = Class.forName(CURSOR_PACKAGE + "SegmentRing$MembershipObserver");
        Method recover = SegmentRing.class.getDeclaredMethod(
                "recover",
                FilesFacade.class,
                String.class,
                long.class,
                observerClass
        );
        recover.setAccessible(true);
        Object recovery;
        try {
            recovery = recover.invoke(null, FilesFacade.INSTANCE, tmpDir, SEGMENT_SIZE, membershipObserver);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new AssertionError(cause);
        }
        Method ring = recovery.getClass().getDeclaredMethod("ring");
        ring.setAccessible(true);
        return (SegmentRing) ring.invoke(recovery);
    }

    private static Object newMembershipObserver(
            final CountingOperations operations,
            final boolean failAllocation
    ) throws Exception {
        Class<?> observerClass = Class.forName(CURSOR_PACKAGE + "SegmentRing$MembershipObserver");
        return Proxy.newProxyInstance(
                SegmentRing.class.getClassLoader(),
                new Class<?>[]{observerClass},
                (proxy, method, args) -> {
                    if ("beforeMembershipAllocation".equals(method.getName())) {
                        if (failAllocation) {
                            throw new OutOfMemoryError("injected membership allocation failure");
                        }
                    } else if ("onMembershipOperation".equals(method.getName())) {
                        operations.onOperation();
                    }
                    return null;
                }
        );
    }

    private static final class CountingOperations {
        private long count;

        private void onOperation() {
            count++;
        }
    }
}
