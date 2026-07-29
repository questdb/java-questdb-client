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

package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Proves {@code sf_dir} is never created world-writable by default, and that
 * {@code sf_dir_shared} widens {@code sf_dir} itself and its {@code .slot-locks}
 * sibling TOGETHER -- never one without the other. An earlier draft of this
 * fix widened only {@code .slot-locks}, which is the half-measure
 * {@link Files#DIR_MODE_SHARED}'s javadoc warns against: umask can only clear
 * bits, so a {@code sf_dir} left at 0755 while {@code .slot-locks} is 01777
 * means a second uid can create its lock file but still cannot create its own
 * slot directory under {@code sf_dir} -- multi-uid sharing relocated, not
 * fixed.
 */
public class SfDirPermissionsTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testDefaultModeIsNeverWorldWritable() throws Exception {
        // Files.mkdir is a verbatim POSIX binding, so its mode argument is always masked
        // by the process umask. DIR_MODE_SHARED (01777) yields 0755 under the usual
        // umask 022 but 0777 under umask 000 -- the default for a systemd unit with no
        // UMask= and for many container entrypoints. sf_dir holds unencrypted buffered
        // rows this client mmaps and replays onto the server.
        Assume.assumeFalse(System.getProperty("os.name").toLowerCase().contains("win"));
        String sfDir = temp.getRoot().getAbsolutePath() + "/sf-perm-check";
        Assert.assertEquals(0, Files.mkdir(sfDir, Files.DIR_MODE_DEFAULT));
        Set<PosixFilePermission> perms =
                java.nio.file.Files.getPosixFilePermissions(Paths.get(sfDir));
        Assert.assertFalse("sf_dir must never be world-writable",
                perms.contains(PosixFilePermission.OTHERS_WRITE));
    }

    @Test
    public void testSfDirAndLockDirAreGatedTogether() throws Exception {
        // umask can only clear bits, so widening only .slot-locks leaves a second uid
        // unable to create its slot directory -- build() then fails one level before
        // the problem the mode exists to solve.
        Assert.assertEquals(Files.DIR_MODE_DEFAULT, recordedModeFor(false));
        Assert.assertEquals(Files.DIR_MODE_DEFAULT, recordedLockDirModeFor(false));
        Assert.assertEquals(Files.DIR_MODE_SHARED, recordedModeFor(true));
        Assert.assertEquals(Files.DIR_MODE_SHARED, recordedLockDirModeFor(true));
    }

    /**
     * End-to-end version of {@link #testSfDirAndLockDirAreGatedTogether}: a real
     * {@code Sender.builder(cfg).build()} against a real {@link TestWebSocketServer}
     * (the same harness {@code SenderPoolSfTest} already uses), asserting the
     * ACTUAL on-disk POSIX permissions {@code Files.mkdir} left behind -- not a
     * recorded {@code int}. This exercises the real call sequence in
     * {@code Sender.build()} ({@code Files.mkdir(sfDir, dirMode)} then
     * {@code SlotLock.acquireLogical(slotPath, dirMode)}) and checks the result on a
     * real filesystem, which the component-level test above does not. It does NOT add
     * bit-for-bit regression coverage beyond that test: under the common umask 022,
     * {@code mkdir(2)}'s {@code mode & ~umask} only touches the permission bits, so
     * {@code DIR_MODE_SHARED} (01777) and {@code DIR_MODE_DEFAULT} (0755) come out
     * with identical 0755 permission bits -- the sticky bit itself does NOT collapse
     * (Linux honours it regardless of umask; see {@link io.questdb.client.std.Files#DIR_MODE_SHARED}),
     * but {@code PosixFilePermission} has no bit for {@code S_ISVTX} on any platform,
     * so this test cannot observe the one bit that actually still differs between the
     * two modes. The exact-mode regression guard remains
     * {@link #testSfDirAndLockDirAreGatedTogether}; what this test adds is proof the
     * real call sequence runs and a check against the real filesystem.
     * <p>
     * The "{@code sf_dir_shared=on} is observably wider" assertion is gated on
     * the ambient umask: {@code mkdir(2)} applies {@code mode & ~umask & 0777},
     * so {@code DIR_MODE_SHARED} (01777) and {@code DIR_MODE_DEFAULT} (0755)
     * collapse to the IDENTICAL 0755 under the common umask 022 -- verified
     * empirically on this suite's own CI/dev umask. The two directories always
     * matching EACH OTHER, and the default case never being world-writable, are
     * asserted unconditionally: those hold regardless of the ambient umask.
     */
    @Test
    public void testSfDirAndLockDirPermissionsMatchEndToEnd() throws Exception {
        Assume.assumeFalse(System.getProperty("os.name").toLowerCase().contains("win"));
        TestUtils.assertMemoryLeak(() -> {
            boolean umaskShowsWidening = ambientUmaskPreservesGroupOrOtherWrite();

            String defaultSfDir = buildRealSenderAndReturnSfDir(false);
            Set<PosixFilePermission> defaultSfPerms = statPerms(defaultSfDir);
            Set<PosixFilePermission> defaultLockPerms = statPerms(defaultSfDir + "/.slot-locks");

            String sharedSfDir = buildRealSenderAndReturnSfDir(true);
            Set<PosixFilePermission> sharedSfPerms = statPerms(sharedSfDir);
            Set<PosixFilePermission> sharedLockPerms = statPerms(sharedSfDir + "/.slot-locks");

            Assert.assertEquals("sf_dir and .slot-locks must have identical permissions (default)",
                    defaultSfPerms, defaultLockPerms);
            Assert.assertEquals("sf_dir and .slot-locks must have identical permissions (sf_dir_shared=on)",
                    sharedSfPerms, sharedLockPerms);

            Assert.assertFalse("default sf_dir must never be world-writable",
                    defaultSfPerms.contains(PosixFilePermission.OTHERS_WRITE));
            Assert.assertFalse("default .slot-locks must never be world-writable",
                    defaultLockPerms.contains(PosixFilePermission.OTHERS_WRITE));

            if (umaskShowsWidening) {
                Assert.assertNotEquals(
                        "sf_dir_shared=on must be observably wider than the default under this umask",
                        defaultSfPerms, sharedSfPerms);
            }
        });
    }

    /**
     * Builds a real {@code Sender} against a real local {@link TestWebSocketServer}
     * with {@code sf_dir_shared} set per {@code shared}, and returns the mode
     * {@code Sender.build()} actually passed to {@code mkdir} for {@code sf_dir}
     * itself -- observed through {@link Sender.LineSenderBuilder#setSfDirFilesFacadeForTest},
     * the seam added for exactly this purpose: {@code Files.mkdir} is a static,
     * non-facade binding, so without it a test asserting the
     * {@code sf_dir_shared -> dirMode} resolution has no way to watch the real call
     * and can only re-implement the ternary it means to guard -- which proves
     * nothing about {@code Sender.build()} itself (the bug this test now catches:
     * reverting {@code Sender.java}'s {@code dirMode} ternary to an unconditional
     * mode must turn this red).
     */
    private int recordedModeFor(boolean shared) throws Exception {
        String sfDir = temp.newFolder("sf-mode-" + System.nanoTime()).getAbsolutePath() + "/sf";
        RecordingMkdirFacade ff = new RecordingMkdirFacade();
        int[] recordedMode = new int[]{-1};
        TestUtils.assertMemoryLeak(() -> {
            Sender.LineSenderBuilder.setSfDirFilesFacadeForTest(ff);
            try (TestWebSocketServer server = new TestWebSocketServer(new NoOpServerHandler())) {
                server.start();
                Assert.assertTrue("test WS server never started", server.awaitStart(5, TimeUnit.SECONDS));
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";"
                        + (shared ? "sf_dir_shared=on;" : "");
                try (Sender sender = Sender.builder(cfg).build()) {
                    // No rows written -- Files.mkdir(sf_dir) already ran before connect().
                }
            } finally {
                Sender.LineSenderBuilder.setSfDirFilesFacadeForTest(null);
            }
            recordedMode[0] = ff.modeFor(sfDir);
        });
        return recordedMode[0];
    }

    private static Set<PosixFilePermission> statPerms(String path) throws Exception {
        return java.nio.file.Files.getPosixFilePermissions(Paths.get(path));
    }

    /**
     * Probes whether THIS process's umask lets a widened request bit survive
     * {@code mkdir}: creates a throwaway directory at {@link Files#DIR_MODE_SHARED}
     * and checks whether group- or others-write made it through. False under the
     * common umask 022 (both bits get cleared regardless of the requested mode);
     * true under a looser umask (002, 000).
     */
    private boolean ambientUmaskPreservesGroupOrOtherWrite() throws Exception {
        String probe = temp.newFolder("umask-probe-" + System.nanoTime()).getAbsolutePath() + "/probe";
        Assert.assertEquals(0, Files.mkdir(probe, Files.DIR_MODE_SHARED));
        Set<PosixFilePermission> perms = statPerms(probe);
        return perms.contains(PosixFilePermission.GROUP_WRITE) || perms.contains(PosixFilePermission.OTHERS_WRITE);
    }

    /**
     * Builds and immediately closes a real {@code Sender} against a real local
     * {@link TestWebSocketServer}, with {@code sf_dir_shared} set per {@code shared}.
     * No rows are written -- {@code Files.mkdir(sfDir, ...)} and
     * {@code SlotLock.acquireLogical} both run well before {@code connect()}'s
     * retry loop, so a bare WS upgrade is enough to exercise them.
     *
     * @return the {@code sf_dir} path passed to the connect string
     */
    private String buildRealSenderAndReturnSfDir(boolean shared) throws Exception {
        String sfDir = temp.newFolder("e2e-" + (shared ? "shared-" : "default-") + System.nanoTime())
                .getAbsolutePath() + "/sf";
        try (TestWebSocketServer server = new TestWebSocketServer(new NoOpServerHandler())) {
            server.start();
            Assert.assertTrue("test WS server never started", server.awaitStart(5, TimeUnit.SECONDS));
            String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";"
                    + (shared ? "sf_dir_shared=on;" : "");
            Sender sender = Sender.builder(cfg).build();
            sender.close();
        }
        return sfDir;
    }

    /**
     * Feeds the mode {@code sf_dir_shared} resolves to into
     * {@link SlotLock#acquireLogical(io.questdb.client.std.FilesFacade, String, int)}
     * and records the mode it actually passes to {@code mkdir} for the
     * {@code .slot-locks} parent -- proving the SAME resolved value that governs
     * {@code sf_dir} (see {@link #recordedModeFor(boolean)}, which observes that
     * side independently) is what reaches the lock directory too, not an
     * independently-hardcoded one (the exact way the fixed regression happened).
     */
    private int recordedLockDirModeFor(boolean shared) throws Exception {
        int mode = shared ? Files.DIR_MODE_SHARED : Files.DIR_MODE_DEFAULT;
        String parent = temp.newFolder("lock-check-" + System.nanoTime()).getAbsolutePath();
        String slot = parent + "/slot";
        String lockDir = parent + "/.slot-locks";
        RecordingMkdirFacade ff = new RecordingMkdirFacade();
        int[] recordedMode = new int[]{-1};
        TestUtils.assertMemoryLeak(() -> {
            try (SlotLock ignored = SlotLock.acquireLogical(ff, slot, mode)) {
                recordedMode[0] = ff.modeFor(lockDir);
            }
        });
        return recordedMode[0];
    }

    /** Never invoked in this test -- a real WS upgrade completes before any row would be flushed. */
    private static final class NoOpServerHandler implements TestWebSocketServer.WebSocketServerHandler {
    }

    /** Records the mode each directory was created with, then delegates. */
    private static final class RecordingMkdirFacade extends DelegatingFilesFacade {
        private final Map<String, Integer> modes = new HashMap<>();

        int modeFor(String path) {
            Integer mode = modes.get(path);
            return mode == null ? -1 : mode;
        }

        @Override
        public int mkdir(String path, int mode) {
            modes.put(path, mode);
            return super.mkdir(path, mode);
        }
    }
}
