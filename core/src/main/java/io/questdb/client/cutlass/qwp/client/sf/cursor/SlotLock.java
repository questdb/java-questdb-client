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
 * {@code flock}/{@code LockFileEx}. Normal teardown explicitly unlocks the
 * descriptor before closing it; hard process exit remains a backstop because
 * the kernel cleans up file locks for terminated processes.
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

    private static final int DEAD_FD_FOR_TESTING = 1_000_000_000;
    private static final String LOCK_FILE_NAME = ".lock";
    private static final String LOCK_PID_FILE_NAME = ".lock.pid";
    private static final String LOGICAL_LOCK_DIR_NAME = ".slot-locks";
    private static final Object RELEASE_RETRY_LOCK = new Object();
    private static SlotLock releaseRetryHead;
    private final String slotDir;
    private int fd;
    private boolean isReleaseRetryPending;
    private SlotLock releaseRetryNext;

    private SlotLock(String slotDir, int fd) {
        this.slotDir = slotDir;
        this.fd = fd;
    }

    /**
     * Creates {@code slotDir} if needed, opens {@code <slotDir>/.lock}, and
     * acquires an exclusive {@code flock} on it. On contention, reads the
     * existing PID payload and throws with a descriptive message.
     *
     * @throws SfOperationalException       on directory or lock-file setup failure
     * @throws SlotLockContentionException  on lock contention
     */
    public static SlotLock acquire(String slotDir) {
        return acquire(slotDir, false);
    }

    /**
     * Acquires a slot and optionally makes the slot entry durable in its
     * parent directory before any segment file is created.
     */
    public static SlotLock acquire(String slotDir, boolean syncParentDirectory) {
        validateSlotDir(slotDir);
        // Construction cleanup may have retained locks after explicit unlock
        // failures. Drive every pending owner before opening a new descriptor.
        // Path text cannot identify a physical file portably (symlinks and
        // Windows case aliases are counterexamples), while the pending list is
        // cold, error-only state and normally empty.
        retryPendingReleases();
        // DIR_MODE_DEFAULT is right here: one process creates its own slot
        // directory and only that process writes inside it.
        ensureDirectory(FilesFacade.INSTANCE, slotDir, "slot dir", Files.DIR_MODE_DEFAULT);
        if (syncParentDirectory && Files.fsyncParentDir(slotDir) != 0) {
            throw new SfOperationalException(
                    "could not sync parent directory for SF slot: " + slotDir);
        }
        String lockPath = slotDir + "/" + LOCK_FILE_NAME;
        String pidPath = slotDir + "/" + LOCK_PID_FILE_NAME;
        return acquireAt(FilesFacade.INSTANCE, slotDir, lockPath, pidPath);
    }

    /**
     * Acquires the stable logical lock for {@code slotDir}, creating its
     * {@code .slot-locks} parent at {@link Files#DIR_MODE_DEFAULT}. Unlike the
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
        // Same pre-step as acquire(): a logical lock this process retained after
        // an unconfirmed unlock would otherwise contend with its own successor.
        retryPendingReleases();
        String[] paths = resolveLogicalLock(slotDir);
        if (paths == null) {
            throw new IllegalArgumentException(
                    "slotDir must contain a parent and slot name: " + slotDir);
        }
        ensureDirectory(ff, paths[0], "logical slot lock dir", Files.DIR_MODE_DEFAULT);
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
        // Unlink ONLY while holding the lock. Removing a lock file another party holds
        // does not release that party's flock, but it does free the pathname, so the
        // next acquirer creates a SECOND inode and locks it successfully -- two owners
        // of a lock whose only job is mutual exclusion.
        //
        // The retiring engine's directory-local lock does not stand in for this. It says
        // nothing about the LOGICAL lock, which Sender.build() holds across its whole
        // construct -> connect -> quarantine transition, in a frame ABOVE this one: an
        // ordinary failed connect closes the engine from inside that scope, reaches here,
        // and used to unlink the very file build() was holding. Acquiring first turns
        // that into a no-op instead of silent double-ownership.
        SlotLock guard;
        try {
            guard = acquireAt(ff, slotDir, paths[1], paths[2]);
        } catch (Throwable t) {
            // Contended (someone holds it, possibly a caller above us) or unopenable.
            // Leaving the pair on disk is the safe outcome -- a live holder's lock must
            // outlive our cleanup. The next fully-drained retirement reclaims it.
            return;
        }
        try {
            ff.remove(paths[1]);
            ff.remove(paths[2]);
        } finally {
            guard.close();
        }
    }

    private static SlotLock acquireAt(FilesFacade ff, String slotDir, String lockPath, String pidPath) {
        int fd = ff.openRW(lockPath);
        if (fd < 0) {
            throw new SfOperationalException(
                    "could not open slot lock file: " + lockPath);
        }
        boolean ok = false;
        try {
            int rc = ff.lock(fd);
            if (rc != 0) {
                String holder = readHolder(pidPath);
                throw new SlotLockContentionException(
                        "sf slot already in use by another process [slot="
                                + slotDir + ", holder=" + holder + "]");
            }
            writePid(ff, pidPath);
            ok = true;
            return new SlotLock(slotDir, fd);
        } finally {
            if (!ok) {
                ff.close(fd);
            }
        }
    }

    /**
     * Side-effect-light contention probe: reports the current holder of the
     * slot flock without creating the slot dir or lock file and without
     * paying a full engine build. Opens the existing {@code .lock} file
     * (absent means nothing can hold a flock on it), try-locks it
     * non-blocking, and releases immediately on success.
     * <p>
     * Returns a non-null holder description (the {@code .lock.pid} payload,
     * or {@code "unknown"}) when the flock is currently held by a live
     * owner; {@code null} when the lock is free or the probe could not
     * determine state (missing lock file, open failure). Callers must treat
     * {@code null} as "proceed to a full acquire", which owns real error
     * classification -- the probe never throws.
     * <p>
     * Races are benign in both directions: a free probe can still lose the
     * subsequent acquire to a concurrent owner (the caller handles that
     * contention exactly as before), and a held probe that goes stale the
     * moment the owner exits is simply re-observed on the caller's next
     * cycle. The probe's momentary hold can make a concurrent acquirer see
     * spurious contention -- the same class of race two real contenders
     * already have.
     */
    public static String probeHolder(String slotDir) {
        if (slotDir == null || slotDir.isEmpty()) {
            return null;
        }
        // Same pre-step as acquire(): a lock retained by THIS process after
        // an unconfirmed unlock would otherwise read as a live holder for as
        // long as the retry list carries it.
        retryPendingReleases();
        String lockPath = slotDir + "/" + LOCK_FILE_NAME;
        if (!Files.exists(lockPath)) {
            return null;
        }
        int fd = Files.openRW(lockPath);
        if (fd < 0) {
            return null;
        }
        if (Files.lock(fd) != 0) {
            String holder = readHolder(slotDir + "/" + LOCK_PID_FILE_NAME);
            Files.close(fd);
            return holder;
        }
        // The flock was free and is momentarily ours. Route the release
        // through the standard close() so an unconfirmed unlock is retained
        // on the retry list exactly like a normal owner's -- a probe must
        // never leak a held flock.
        new SlotLock(slotDir, fd).close();
        return null;
    }

    private static void ensureDirectory(FilesFacade ff, String path, String description, int mode) {
        if (!ff.exists(path)) {
            int rc = ff.mkdir(path, mode);
            // Multiple senders may create the shared parent lock directory
            // concurrently. Treat EEXIST as success, just as the builder does
            // for the SF root itself.
            if (rc != 0 && !ff.exists(path)) {
                throw new SfOperationalException(
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

    /**
     * Replaces the live descriptor with a known-dead value until the returned
     * guard closes. Test-only: exercises release retry paths without exposing
     * mutable descriptor state.
     */
    @TestOnly
    public synchronized ReleaseFailureForTesting injectReleaseFailureForTesting() {
        if (fd < 0 || fd == DEAD_FD_FOR_TESTING) {
            throw new IllegalStateException("slot lock is not held by a live descriptor");
        }
        ReleaseFailureForTesting releaseFailure = new ReleaseFailureForTesting(fd);
        fd = DEAD_FD_FOR_TESTING;
        return releaseFailure;
    }

    @TestOnly
    public synchronized boolean isReleaseFailureInjectedForTesting() {
        return fd == DEAD_FD_FOR_TESTING;
    }

    /** Slot dir this lock guards. */
    public String slotDir() {
        return slotDir;
    }

    /**
     * Explicitly releases the flock and reports whether the release was
     * <b>confirmed</b>. After a successful unlock the native primitive closes
     * the descriptor once, best-effort, and this object forgets its numeric
     * value. It never retries that close: POSIX leaves descriptor state
     * unspecified after some close failures (notably {@code EINTR}), so a
     * retry could close an unrelated descriptor that reused the same number.
     * We do NOT remove the {@code .lock} file or {@code .lock.pid} sidecar; a
     * stale PID is harmless because the next acquirer overwrites it.
     * <p>
     * When the explicit unlock itself fails, the fd is retained so a later
     * attempt can safely retry the non-consuming unlock operation. Idempotent
     * once the unlock has succeeded.
     * <p>
     * Owners that gate a "slot dir is reusable" signal on the release
     * (e.g. {@code CursorSendEngine.finishClose} publishing
     * {@code closeCompleted}) must call this and check the result rather
     * than {@link #close()}, which is best-effort by contract.
     *
     * @return {@code true} if the lock was explicitly released (or was already
     *         released), {@code false} if the OS reported an unlock failure
     */
    public synchronized boolean release() {
        if (fd < 0) {
            return true;
        }
        if (release0(fd) == 0) {
            fd = -1;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        // QuietCloseable cannot report a failure, so retain this object on an
        // allocation-free retry list when unlock is unconfirmed. Serialize the
        // release attempt and publication: an acquire that starts after close
        // returns must not miss the retained owner. An acquire already racing
        // an in-progress close may still observe ordinary lock contention.
        synchronized (RELEASE_RETRY_LOCK) {
            if (!release()) {
                retainForReleaseRetryLocked();
            }
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

    private static native int release0(int fd);

    private void retainForReleaseRetryLocked() {
        if (!isReleaseRetryPending) {
            isReleaseRetryPending = true;
            releaseRetryNext = releaseRetryHead;
            releaseRetryHead = this;
        }
    }

    private synchronized void restoreFdForTesting(int savedFd) {
        if (fd != DEAD_FD_FOR_TESTING) {
            throw new IllegalStateException("slot lock release failure is not injected");
        }
        fd = savedFd;
    }

    private static void retryPendingReleases() {
        synchronized (RELEASE_RETRY_LOCK) {
            SlotLock previous = null;
            SlotLock lock = releaseRetryHead;
            while (lock != null) {
                SlotLock next = lock.releaseRetryNext;
                // release() reports operational unlock failure as false. Do
                // not catch Error or unexpected programming failures here:
                // hiding them as apparent lock contention would misdiagnose
                // the process and create a new retry contract for VM errors.
                if (lock.release()) {
                    if (previous == null) {
                        releaseRetryHead = next;
                    } else {
                        previous.releaseRetryNext = next;
                    }
                    lock.isReleaseRetryPending = false;
                    lock.releaseRetryNext = null;
                    lock = next;
                    continue;
                }
                previous = lock;
                lock = next;
            }
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

    @TestOnly
    public final class ReleaseFailureForTesting implements QuietCloseable {
        private final int savedFd;
        private boolean isRestored;

        private ReleaseFailureForTesting(int savedFd) {
            this.savedFd = savedFd;
        }

        @Override
        public synchronized void close() {
            if (!isRestored) {
                restoreFdForTesting(savedFd);
                isRestored = true;
            }
        }
    }
}
