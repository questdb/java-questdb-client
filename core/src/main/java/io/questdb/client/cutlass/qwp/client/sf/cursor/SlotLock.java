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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.std.Compat;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Advisory exclusive locks for a single SF slot.
 * <p>
 * {@link #acquire(String)} locks a {@code .lock} file inside the slot directory
 * for the entire lifetime of the engine that owns it. {@link #acquireLogical(String)}
 * locks a sibling file under the parent SF directory for short-lived pathname
 * transitions and orphan adoption; because it is outside the slot directory,
 * it remains stable if that directory is renamed. Both use
 * {@code flock}/{@code LockFileEx}. A lock is
 * automatically released when the fd is closed — including on hard process
 * exit, since the kernel cleans up file locks for terminated processes.
 * <p>
 * The holder's PID is written to a sibling {@code .lock.pid} file at
 * acquisition time. A failed acquisition reads it back so the error message
 * can name the offending process — turning a vague "slot in use" into
 * actionable diagnostics. The PID lives in a separate file because Windows'
 * {@code LockFileEx} is a mandatory range lock: while the {@code .lock}
 * file is held, a second handle cannot read its bytes, so we couldn't
 * recover the holder's PID from the lock file itself.
 * <p>
 * Two senders pointing at the same slot dir is the multi-writer footgun
 * the slot model exists to prevent: their FSN sequences would interleave
 * on disk and corrupt recovery. Detecting the collision at acquisition
 * time and refusing to start is the contract — recoverable, no data on
 * disk yet, vs. the alternative of silently scrambling the slot.
 */
public final class SlotLock implements QuietCloseable {

    private static final String LOCK_FILE_NAME = ".lock";
    private static final String LOCK_PID_FILE_NAME = ".lock.pid";
    private static final String LOGICAL_LOCK_DIR_NAME = ".slot-locks";
    private final FilesFacade ff;
    private final String slotDir;
    private int fd;

    private SlotLock(FilesFacade ff, String slotDir, int fd) {
        this.ff = ff;
        this.slotDir = slotDir;
        this.fd = fd;
    }

    /**
     * Creates {@code slotDir} if needed, opens {@code <slotDir>/.lock}, and
     * acquires an exclusive {@code flock} on it. On contention, reads the
     * existing PID payload and throws with a descriptive message.
     *
     * @throws IllegalStateException on dir-create failure, file-open failure,
     *                               or lock contention.
     */
    public static SlotLock acquire(String slotDir) {
        validateSlotDir(slotDir);
        // DIR_MODE_DEFAULT is right here: one process creates its own slot
        // directory and only that process writes inside it.
        ensureDirectory(FilesFacade.INSTANCE, slotDir, "slot dir", Files.DIR_MODE_DEFAULT);
        String lockPath = slotDir + "/" + LOCK_FILE_NAME;
        String pidPath = slotDir + "/" + LOCK_PID_FILE_NAME;
        return acquireAt(FilesFacade.INSTANCE, slotDir, lockPath, pidPath);
    }

    /**
     * Acquires the stable logical lock for {@code slotDir}. Unlike the
     * directory-local {@code .lock}, this lock is anchored in the parent SF
     * directory, so renaming the slot cannot move the lock inode away from the
     * logical slot name it guards.
     * <p>
     * Callers use this as a short-lived transition/adoption lock, always before
     * acquiring the directory-local lock. In particular it must cover an
     * unreplayable slot's close -&gt; rename -&gt; recreate transition, preventing a
     * queued orphan drainer from adopting the renamed inode and later touching
     * the fresh directory through the old pathname.
     */
    public static SlotLock acquireLogical(String slotDir) {
        return acquireLogical(FilesFacade.INSTANCE, slotDir);
    }

    /** Facade-aware variant used to exercise logical-lock I/O failures. */
    @TestOnly
    public static SlotLock acquireLogical(FilesFacade ff, String slotDir) {
        validateSlotDir(slotDir);
        String[] paths = resolveLogicalLock(slotDir);
        if (paths == null) {
            throw new IllegalArgumentException(
                    "slotDir must contain a parent and slot name: " + slotDir);
        }
        // SHARED_DIR_MODE, not DIR_MODE_DEFAULT: unlike a slot directory -- which
        // one process creates and only that process writes -- this directory is
        // shared by every sender under the same sf_dir, and each of them must
        // CREATE its own lock file inside it. At 0755 the first process to start
        // owns it and a sender running as a different uid cannot create its lock
        // file at all, so build() throws before it can open the slot. Passing
        // 0777 hands the sharing policy to the deployment's umask, which is what
        // already governs sf_dir itself: an unset umask leaves this identical to
        // the old 0755, while a shared-group deployment (umask 002) gets the
        // group-writable directory it needs.
        ensureDirectory(ff, paths[0], "logical slot lock dir", Files.DIR_MODE_SHARED);
        return acquireAt(ff, slotDir, paths[1], paths[2]);
    }

    /**
     * Best-effort removal of the parent-anchored logical lock files
     * ({@code <parent>/.slot-locks/<slotName>.lock} and its {@code .lock.pid}
     * sidecar) for {@code slotDir}, mirroring {@link AckWatermark#removeOrphan}
     * and {@link PersistedSymbolDict#removeOrphan} for the slot's in-directory
     * side-files. The fully-drained close that permanently retires a slot calls
     * this: the logical lock lives OUTSIDE the slot dir (so it survives a slot
     * rename), so nothing else reclaims it, and without this
     * {@code <sf_dir>/.slot-locks} accumulates one dead lock+pid pair per
     * distinct slot name for the lifetime of {@code sf_dir} -- unbounded under
     * rotating {@code senderId}s.
     * <p>
     * The unlink is best-effort and safe: the retiring engine still holds the
     * slot's directory-local {@link #acquire} lock (the real multi-writer guard)
     * across this cleanup, and fully-drained retirement performs no rename, so
     * the logical lock -- which only guards the close/rename/recreate transition
     * -- is not in use. An orphan drainer momentarily mid-{@link #acquireLogical}
     * fails its immediately-following candidacy / directory-lock check (the slot
     * is gone) and backs off. Unlike {@link #acquireLogical}, an unusable
     * {@code slotDir} is a silent no-op here rather than a throw.
     */
    public static void removeOrphanLogical(String slotDir) {
        removeOrphanLogical(FilesFacade.INSTANCE, slotDir);
    }

    /** Facade-aware variant of {@link #removeOrphanLogical(String)}. */
    public static void removeOrphanLogical(FilesFacade ff, String slotDir) {
        if (slotDir == null || slotDir.isEmpty()) {
            return;
        }
        String[] paths = resolveLogicalLock(slotDir);
        if (paths == null) {
            return;
        }
        ff.remove(paths[1]);
        ff.remove(paths[2]);
    }

    private static SlotLock acquireAt(FilesFacade ff, String slotDir, String lockPath, String pidPath) {
        int fd = ff.openRW(lockPath);
        if (fd < 0) {
            throw new IllegalStateException(
                    "could not open slot lock file: " + lockPath);
        }
        boolean ok = false;
        try {
            int rc = ff.lock(fd);
            if (rc != 0) {
                String holder = readHolder(pidPath);
                throw new IllegalStateException(
                        "sf slot already in use by another process [slot="
                                + slotDir + ", holder=" + holder + "]");
            }
            writePid(ff, pidPath);
            ok = true;
            return new SlotLock(ff, slotDir, fd);
        } finally {
            if (!ok) {
                ff.close(fd);
            }
        }
    }

    private static void ensureDirectory(FilesFacade ff, String path, String description, int mode) {
        if (!ff.exists(path)) {
            int rc = ff.mkdir(path, mode);
            // Multiple senders may create the shared parent lock directory
            // concurrently. Treat EEXIST as success, just as the builder does
            // for the SF root itself.
            if (rc != 0 && !ff.exists(path)) {
                throw new IllegalStateException(
                        "could not create " + description + ": " + path + " rc=" + rc);
            }
        }
    }

    /**
     * Resolves the parent-anchored logical lock layout for {@code slotDir}:
     * {@code [0]} the {@code .slot-locks} directory, {@code [1]} the
     * {@code <slotName>.lock} path, {@code [2]} the {@code .lock.pid} path.
     * Returns {@code null} when {@code slotDir} has no usable parent or name.
     * Shared by {@link #acquireLogical} and {@link #removeOrphanLogical} so the
     * two can never target different files.
     */
    private static String[] resolveLogicalLock(String slotDir) {
        Path slotPath = Paths.get(slotDir);
        Path parentPath = slotPath.getParent();
        Path slotNamePath = slotPath.getFileName();
        if (parentPath == null || slotNamePath == null || slotNamePath.toString().isEmpty()) {
            return null;
        }
        String logicalLockDir = parentPath + "/" + LOGICAL_LOCK_DIR_NAME;
        String slotName = slotNamePath.toString();
        return new String[]{
                logicalLockDir,
                logicalLockDir + "/" + slotName + LOCK_FILE_NAME,
                logicalLockDir + "/" + slotName + LOCK_PID_FILE_NAME
        };
    }

    private static void validateSlotDir(String slotDir) {
        if (slotDir == null || slotDir.isEmpty()) {
            throw new IllegalArgumentException("slotDir must not be empty");
        }
    }

    /** Slot dir this lock guards. */
    public String slotDir() {
        return slotDir;
    }

    @Override
    public void close() {
        // Closing the fd releases the lock. We do NOT remove the .lock
        // file or the .lock.pid sidecar — a stale PID is harmless (next
        // acquirer overwrites .lock.pid on success).
        if (fd >= 0) {
            ff.close(fd);
            fd = -1;
        }
    }

    private static String readHolder(String pidPath) {
        if (!Files.exists(pidPath)) return "unknown";
        int rfd = Files.openRO(pidPath);
        if (rfd < 0) return "unknown";
        try {
            long fileLen = Files.length(rfd);
            if (fileLen <= 0) return "unknown";
            int readLen = (int) Math.min(fileLen, 64L);
            long addr = Unsafe.malloc(readLen, MemoryTag.NATIVE_DEFAULT);
            try {
                long n = Files.read(rfd, addr, readLen, 0L);
                if (n <= 0) return "unknown";
                byte[] bytes = new byte[(int) n];
                for (int i = 0; i < n; i++) {
                    bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
                }
                return "pid=" + new String(bytes, StandardCharsets.UTF_8).trim();
            } finally {
                Unsafe.free(addr, readLen, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(rfd);
        }
    }

    private static void writePid(FilesFacade ff, String pidPath) {
        long pid;
        try {
            pid = Compat.currentPid();
        } catch (Throwable ignored) {
            // Diagnostic-only — never block lock acquisition on it.
            pid = -1L;
        }
        int wfd = ff.openRW(pidPath);
        if (wfd < 0) {
            // Diagnostic-only — never block lock acquisition on it.
            return;
        }
        try {
            ff.truncate(wfd, 0L);
            byte[] payload = (pid + "\n").getBytes(StandardCharsets.UTF_8);
            long addr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(addr + i, payload[i]);
                }
                ff.write(wfd, addr, payload.length, 0L);
            } finally {
                Unsafe.free(addr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            ff.close(wfd);
        }
    }
}
