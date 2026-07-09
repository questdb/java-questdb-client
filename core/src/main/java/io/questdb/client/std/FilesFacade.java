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

    long length(int fd);

    /**
     * Stat length of the file at {@code path}, in bytes. Default delegates to
     * {@link Files#length(String)}.
     *
     * <p>Test injection point: {@code MmapSegment.openExisting} maps the file to
     * this length and scans straight out of the mapping, so a wrapping facade
     * that returns a value <em>larger</em> than the real file makes the mapping
     * extend past end-of-file. A read of a page beyond real EOF raises SIGBUS on
     * <em>every</em> filesystem (which HotSpot translates to a catchable
     * {@code InternalError} at an {@code Unsafe} intrinsic site) — the same fault
     * a genuinely unbacked/sparse page raises on ZFS, but reproduced
     * deterministically on ext4/xfs too. That is what lets recovery's mmap-fault
     * guard be regression-tested on any CI runner rather than only on ZFS.
     */
    long length(String path);

    int lock(int fd);

    int mkdir(String path, int mode);

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

    /**
     * Variant of {@code length(String)} taking a pre-encoded native UTF-8 path
     * pointer; same allocation-elision rationale as {@link #openRW(long)}.
     */
    long length(long pathPtr);

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
