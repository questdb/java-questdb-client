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

import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Regression for the close-time segment cleanup on a fully-drained slot.
 * <p>
 * {@code CursorSendEngine.finishClose} unlinks the acknowledged {@code .sfa}
 * files and then removes the ack watermark. When the unlink fails (transient
 * I/O error, permission problem), the residual segment files hold rows the
 * server already acknowledged. If the watermark does not cover them, a
 * successor engine on the same slot seeds recovery from {@code lowestBase - 1}
 * and replays every acknowledged row — duplicates on a non-DEDUP table.
 * <p>
 * The test injects the unlink failure by dropping write permission on the
 * slot directory (POSIX: unlink requires a writable parent directory), so it
 * is skipped on Windows and when permissions are not enforced (root).
 * The shared {@link SegmentManager} is deliberately never started: no worker
 * thread exists, so no manager tick can persist the watermark behind the
 * test's back, and the close-path quiescence barrier is trivially satisfied —
 * fully deterministic, no timing coordination needed.
 */
public class CursorSendEngineCloseUnlinkFailureTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-engine-close-unlink-fault-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir != null) {
            removeRecursive(tmpDir);
        }
    }

    @Test(timeout = 20_000L)
    public void testFailedCloseTimeUnlinkMustNotExposeAckedFramesToSuccessor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
            String slot = tmpDir + "/slot";
            Path slotPath = Paths.get(slot);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = new SegmentManager(segmentSize, TimeUnit.SECONDS.toNanos(60));
            CursorSendEngine pred = null;
            CursorSendEngine succ = null;
            boolean slotDirReadOnly = false;
            try {
                fill(payload, 32, (byte) 0x33);
                pred = new CursorSendEngine(slot, segmentSize, manager);
                Assert.assertEquals(0L, pred.appendBlocking(payload, 32));
                Assert.assertEquals(0L, pred.publishedFsn());
                // The server durably acknowledged FSN 0 in this session.
                Assert.assertTrue(pred.acknowledge(0L));
                Assert.assertEquals(0L, pred.ackedFsn());

                // Inject a close-time unlink failure: drop write permission on
                // the slot dir. Prove the injection works with a probe file --
                // root (and some filesystems) ignore directory permissions.
                String probePath = slot + "/probe";
                Assert.assertTrue(java.nio.file.Files.exists(
                        java.nio.file.Files.createFile(Paths.get(probePath))));
                try {
                    setPermissions(slotPath, "r-xr-xr-x");
                } catch (UnsupportedOperationException e) {
                    Assume.assumeNoException("POSIX permissions unavailable on this platform", e);
                }
                slotDirReadOnly = true;
                boolean probeRemoved = Files.remove(probePath);
                if (probeRemoved) {
                    setPermissions(slotPath, "rwxr-xr-x");
                    slotDirReadOnly = false;
                }
                Assume.assumeFalse("directory permissions not enforced (running as root?)",
                        probeRemoved);

                // Fully-drained close: tries to unlink the acknowledged
                // segment files and fails.
                pred.close();
                Assert.assertTrue("flock release needs no dir write; close must complete",
                        pred.isCloseCompleted());
                pred = null;
                Assert.assertTrue("injected unlink failure must leave the acknowledged segment",
                        Files.exists(slot + "/sf-initial.sfa"));

                // The transient failure clears before the successor arrives.
                setPermissions(slotPath, "rwxr-xr-x");
                slotDirReadOnly = false;
                Files.remove(probePath);

                succ = new CursorSendEngine(slot, segmentSize, manager);
                Assert.assertTrue(succ.wasRecoveredFromDisk());
                Assert.assertEquals(0L, succ.publishedFsn());
                // THE regression: FSN 0 was acknowledged by the server during
                // the predecessor's session. The successor must not see it as
                // replayable, or a non-DEDUP table receives duplicate rows.
                Assert.assertTrue("successor exposes already-acknowledged frames for replay "
                                + "[ackedFsn=" + succ.ackedFsn()
                                + ", publishedFsn=" + succ.publishedFsn() + "]",
                        succ.ackedFsn() >= succ.publishedFsn());

                // The successor's own fully-drained close retries the cleanup
                // now that the failure has cleared: segments and watermark gone.
                succ.close();
                Assert.assertTrue(succ.isCloseCompleted());
                succ = null;
                Assert.assertFalse("successor close did not retry the segment unlink",
                        Files.exists(slot + "/sf-initial.sfa"));
                Assert.assertFalse("watermark must be removed once no segment file remains",
                        Files.exists(slot + "/" + AckWatermark.FILE_NAME));
            } finally {
                if (slotDirReadOnly) {
                    try {
                        setPermissions(slotPath, "rwxr-xr-x");
                    } catch (Throwable ignored) {
                    }
                }
                if (pred != null) {
                    pred.close();
                }
                if (succ != null) {
                    succ.close();
                }
                manager.close();
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void fill(long address, int len, byte value) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, value);
        }
    }

    private static void removeRecursive(String dir) {
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            removeRecursive(child);
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

    private static void setPermissions(Path path, String posix) throws Exception {
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString(posix);
        java.nio.file.Files.setPosixFilePermissions(path, perms);
    }
}
