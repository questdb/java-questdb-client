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

package io.questdb.client.std;

/**
 * Indirection over the static {@link Files} JNI surface so callers can inject
 * fault behavior in tests (return short writes, ENOSPC, EIO from fsync, etc.)
 * without resorting to filesystem-level tricks.
 * <p>
 * Production code uses {@link #INSTANCE}, which delegates verbatim to {@link Files}.
 * Tests can subclass / wrap {@link #INSTANCE} and override individual methods.
 */
public interface FilesFacade {
    FilesFacade INSTANCE = new DefaultFilesFacade();

    /**
     * Extends the file to at least {@code size} bytes and reserves real
     * disk blocks for the newly-extended range. Returns {@code true} on
     * success, {@code false} on failure (most importantly {@code ENOSPC}
     * / {@code ERROR_DISK_FULL}). Never shrinks; requests where
     * {@code size <= currentSize} short-circuit as a no-op success. See
     * {@link Files#allocate(int, long)} for the full cross-platform
     * contract — sparse-fallback rules on Linux/macOS, the "pre-existing
     * sparse holes are not filled" caveat, per-platform primitives.
     *
     * <p>Test injection point: a wrapping facade can return {@code false}
     * to simulate disk-full at create time so the caller's recovery
     * path is exercised. Callers that observe {@code false} MUST close
     * the fd and unlink the partial file — the partially-extended file
     * would otherwise occupy up to {@code max(size, currentSize)} bytes
     * on disk. Default delegates to {@link Files#allocate(int, long)}.
     */
    boolean allocate(int fd, long size);

    /**
     * Allocate a native UTF-8 path pointer. Test injection point: a wrapping
     * facade can throw to simulate OOM without depending on actual memory
     * pressure. Production callers must release the returned pointer via
     * {@link #freeNativePath(long)}. Default delegates to
     * {@link Files#allocNativePath(String)}.
     */
    long allocNativePath(String path);

    int close(int fd);

    /**
     * The error code of this thread's most recent failed filesystem call:
     * {@code errno} on POSIX, the saved {@code GetLastError()} value on
     * Windows (see {@link Os#errno()}). Meaningful only when read immediately
     * after a facade call reported failure. Lives on the facade so a
     * fault-injecting test can pin a deterministic code next to an injected
     * failure -- a facade that fakes {@code length(path) < 0} must also fake
     * the errno that classifies it (e.g. via
     * {@link Files#isNotFoundError(int)}), or the classification would read
     * whatever the last REAL syscall left behind.
     */
    default int errno() {
        return Os.errno();
    }

    boolean exists(String path);

    void findClose(long findPtr);

    long findFirst(String dir);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    /**
     * Release a pointer returned by {@link #allocNativePath(String)}.
     * Default delegates to {@link Files#freeNativePath(long)}.
     */
    void freeNativePath(long pathPtr);

    int fsync(int fd);

    default int fsyncDir(String dir) {
        return Files.fsyncDir(dir);
    }

    /**
     * Whether callers should use this facade's mmap path. The production facade
     * returns {@code true}; wrapping fault facades retain positioned I/O unless
     * they explicitly opt in, preserving their ability to inject short reads and
     * writes.
     */
    default boolean isMmapAllowed() {
        return this == INSTANCE;
    }

    /**
     * Returns the current byte length of the file referenced by open descriptor
     * {@code fd}, or a negative value when the descriptor cannot be statted.
     */
    long length(int fd);

    /**
     * Stat length of the file at {@code path}, in bytes.
     * {@link DefaultFilesFacade} delegates to {@link Files#length(String)}.
     * Code that already owns an open descriptor should prefer
     * {@link #length(int)} so path replacement cannot make the stat refer to a
     * different file.
     */
    long length(String path);

    int lock(int fd);

    int mkdir(String path, int mode);

    /**
     * Best-effort page pin over {@code [addr, addr+len)} of an mmap'd region.
     * Returns 0 when the range is locked, non-zero when the platform refuses
     * (RLIMIT_MEMLOCK, missing privilege, or a native library without the
     * symbol). Callers must treat refusal as a soft downgrade, never an error.
     */
    default int mlock(long addr, long len) {
        return Files.mlock(addr, len);
    }

    /**
     * Maps a file region. Kept on the facade so mmap failures can be injected
     * without relying on platform-specific filesystem behavior.
     */
    default long mmap(int fd, long len, long offset, int flags, int memoryTag) {
        return Files.mmap(fd, len, offset, flags, memoryTag);
    }

    default int msync(long addr, long len, boolean async) {
        return Files.msync(addr, len, async);
    }

    /**
     * Releases a pin taken by {@link #mlock(long, long)}. Best-effort;
     * refusals are ignorable ({@code munmap} implicitly unlocks).
     */
    default int munlock(long addr, long len) {
        return Files.munlock(addr, len);
    }

    /** Releases a region returned by {@link #mmap(int, long, long, int, int)}. */
    default void munmap(long address, long len, int memoryTag) {
        Files.munmap(address, len, memoryTag);
    }

    int openCleanRW(String path);

    /**
     * Variant of {@link #openCleanRW(String)} taking a pre-encoded
     * native UTF-8 path pointer; lets callers in hot paths cache the encoded
     * path (e.g. via a reused {@code DirectUtf8Sink}) and skip the per-call
     * {@code byte[]} + native-malloc allocation.
     */
    int openCleanRW(long pathPtr);

    int openRW(String path);

    /** Variant of {@link #openRW(String)} taking a pre-encoded native UTF-8 path pointer. */
    int openRW(long pathPtr);

    default int openRWExclusive(String path) {
        return Files.openRWExclusive(path);
    }

    default int openRWExclusive(long pathPtr) {
        return Files.openRWExclusive(pathPtr);
    }

    /**
     * Variant of {@code length(String)} taking a pre-encoded native UTF-8 path
     * pointer; same allocation-elision rationale as {@link #openRW(long)}.
     */
    long length(long pathPtr);

    /**
     * Reads up to {@code len} bytes from the absolute file {@code offset} into
     * native memory at {@code addr}, without changing the descriptor position.
     * A positive result may be shorter than requested and callers that require
     * the whole range must retry. For a non-zero request, {@code 0} means EOF
     * or no progress and a negative result indicates an operating-system error.
     */
    long read(int fd, long addr, long len, long offset);

    boolean remove(String path);

    /**
     * Variant of {@link #remove(String)} taking a native path pointer; lets
     * callers cache the encoded path and avoid the byte[] allocation that
     * the String-based overload incurs on every call.
     */
    boolean remove(long pathPtr);

    int rename(String oldPath, String newPath);

    boolean truncate(int fd, long size);

    long write(int fd, long addr, long len, long offset);
}
