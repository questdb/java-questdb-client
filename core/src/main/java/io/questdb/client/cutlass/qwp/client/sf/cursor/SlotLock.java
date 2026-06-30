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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;

/**
 * Advisory exclusive lock for a single SF slot directory.
 * <p>
 * One {@code .lock} file per slot, held via {@code flock}/{@code LockFileEx}
 * for the entire lifetime of the engine that owns the slot. The lock is
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
    private final String slotDir;
    private int fd;

    private SlotLock(String slotDir, int fd) {
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
        if (slotDir == null || slotDir.isEmpty()) {
            throw new IllegalArgumentException("slotDir must not be empty");
        }
        if (!Files.exists(slotDir)) {
            int rc = Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT);
            if (rc != 0) {
                throw new IllegalStateException(
                        "could not create slot dir: " + slotDir + " rc=" + rc);
            }
        }
        String lockPath = slotDir + "/" + LOCK_FILE_NAME;
        String pidPath = slotDir + "/" + LOCK_PID_FILE_NAME;
        int fd = Files.openRW(lockPath);
        if (fd < 0) {
            throw new IllegalStateException(
                    "could not open slot lock file: " + lockPath);
        }
        boolean ok = false;
        try {
            int rc = Files.lock(fd);
            if (rc != 0) {
                String holder = readHolder(pidPath);
                throw new IllegalStateException(
                        "sf slot already in use by another process [slot="
                                + slotDir + ", holder=" + holder + "]");
            }
            writePid(pidPath);
            ok = true;
            return new SlotLock(slotDir, fd);
        } finally {
            if (!ok) {
                Files.close(fd);
            }
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
            Files.close(fd);
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

    private static void writePid(String pidPath) {
        long pid;
        try {
            pid = Compat.currentPid();
        } catch (Throwable ignored) {
            // Diagnostic-only — never block lock acquisition on it.
            pid = -1L;
        }
        int wfd = Files.openRW(pidPath);
        if (wfd < 0) {
            // Diagnostic-only — never block lock acquisition on it.
            return;
        }
        try {
            Files.truncate(wfd, 0L);
            byte[] payload = (pid + "\n").getBytes(StandardCharsets.UTF_8);
            long addr = Unsafe.malloc(payload.length, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payload.length; i++) {
                    Unsafe.getUnsafe().putByte(addr + i, payload[i]);
                }
                Files.write(wfd, addr, payload.length, 0L);
            } finally {
                Unsafe.free(addr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
        } finally {
            Files.close(wfd);
        }
    }
}
