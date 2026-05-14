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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Thin Java wrappers over POSIX / Win32 file-I/O syscalls. Used by client-side
 * components that cannot depend on {@code java.nio.FileChannel} for either
 * deterministic-allocation reasons (no off-heap buffer churn) or for behavior
 * that the JDK does not expose (e.g. {@code flock}, {@code F_PREALLOCATE}).
 * <p>
 * Path arguments are encoded as UTF-8 and passed to JNI as a
 * native-malloc'd null-terminated string; the encoding allocation is hidden
 * inside each wrapper. Callers performing a path operation in a hot loop
 * should encode the path once via {@link #allocNativePath(String)} and use
 * the {@code long}-pointer overload (where one exists) to skip the per-call
 * {@code byte[]} allocation.
 * <p>
 * File descriptors returned by the {@code open*} methods are raw integers and
 * must be released by {@link #close(int)}. {@code -1} is a sentinel for "no
 * fd" and is safe to pass to {@link #close(int)} (no-op).
 * <p>
 * Return-value conventions:
 * <ul>
 *   <li>{@code int} fd-returning methods: {@code >= 0} = success, {@code -1}
 *       = failure (errno set by the OS).</li>
 *   <li>{@code int} status-returning methods (close, fsync, mkdir, rename,
 *       lock): {@code 0} = success, non-zero = failure.</li>
 *   <li>{@code long} byte-count methods (read, write, append): non-negative
 *       byte count actually transferred (may be less than requested for
 *       short transfers under ENOSPC etc.); {@code -1} on hard failure.</li>
 *   <li>{@code long} length methods: file size in bytes, or {@code -1} on
 *       fstat / stat failure.</li>
 *   <li>{@code boolean} truncate/allocate/exists/remove: success.</li>
 * </ul>
 * This class is final and not instantiable; all members are static.
 */
public final class Files {
    /** UTF-8 charset; convenience reference for callers encoding paths or names. */
    public static final Charset UTF_8;

    /**
     * System page size in bytes, captured once at class init. Useful for
     * sizing aligned writes to avoid kernel-side rmw on partial pages.
     */
    public static final long PAGE_SIZE;

    /**
     * Default POSIX mode bits (rwxr-xr-x) used by {@link #mkdir(String, int)}
     * for directories created by the client. Owner has full access; group and
     * others may traverse and read but not modify. 0755 octal
     */
    public static final int DIR_MODE_DEFAULT = 493;

    /** {@code dirent.d_type} sentinel: type unknown (filesystem doesn't fill it). */
    public static final int DT_UNKNOWN = 0;
    /** {@code dirent.d_type}: directory entry. */
    public static final int DT_DIR = 4;
    /** {@code dirent.d_type}: regular file entry. */
    public static final int DT_FILE = 8;
    /** {@code dirent.d_type}: symbolic link entry. */
    public static final int DT_LNK = 10;

    /** {@link #mmap} flag: map for read-only access. */
    public static final int MAP_RO = 1;
    /** {@link #mmap} flag: map for read-write access. */
    public static final int MAP_RW = 2;

    /**
     * Sentinel returned by {@link #mmap} on failure. The value mirrors
     * POSIX {@code MAP_FAILED} ({@code (void*)-1}); on Win32 we map
     * {@code MapViewOfFileEx} failure to the same sentinel so callers
     * have a single value to test against.
     */
    public static final long FAILED_MMAP_ADDRESS = -1L;

    private Files() {
    }

    /**
     * Close a file descriptor obtained from {@link #openRW(String)} et al.
     * Accepts any non-negative fd, including 0/1/2 — those can legitimately
     * appear when the JVM was started with stdin/stdout/stderr pre-closed.
     * Returns 0 on success, non-zero on failure (errno set by the OS).
     * Returns -1 without invoking the syscall when {@code fd < 0} (sentinel
     * for "not opened").
     */
    public static int close(int fd) {
        if (fd >= 0) {
            return close0(fd);
        }
        return -1;
    }

    /**
     * Opens {@code path} for read-only access. Does not create the file.
     * Returns a non-negative fd on success or -1 on failure.
     */
    public static int openRO(String path) {
        long ptr = pathPtr(path);
        try {
            return openRO0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Opens {@code path} for read-write access, creating it (mode 0644) if
     * absent. Existing content is preserved. Returns a non-negative fd on
     * success or -1 on failure.
     */
    public static int openRW(String path) {
        long ptr = pathPtr(path);
        try {
            return openRW0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Variant of {@link #openRW(String)} that takes a pre-encoded native UTF-8
     * path pointer (from {@link #allocNativePath(String)} or a reused
     * {@code DirectUtf8Sink}). Lets callers in hot paths avoid the {@code
     * byte[]} + native-malloc allocations that {@link #pathPtr(String)} incurs
     * on every call. The pointer must reference a null-terminated UTF-8 string.
     */
    public static int openRW(long pathPtr) {
        return openRW0(pathPtr);
    }

    /**
     * Opens {@code path} for append-only writes, creating it (mode 0644) if
     * absent. Every {@link #append(int, long, long)} writes at end-of-file
     * regardless of the current logical position. Returns a non-negative fd
     * on success or -1 on failure.
     */
    public static int openAppend(String path) {
        long ptr = pathPtr(path);
        try {
            return openAppend0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Opens {@code path} for read-write access, truncating any existing
     * content (mode 0644). The new file is empty; callers that need a
     * specific size should follow this with {@link #allocate(int, long)}
     * (reserves real disk blocks, fails synchronously on ENOSPC) or
     * {@link #truncate(int, long)} (sets logical size only, leaves blocks
     * sparse). Returns a non-negative fd on success or -1 on failure.
     */
    public static int openCleanRW(String path) {
        long ptr = pathPtr(path);
        try {
            return openCleanRW0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Variant of {@link #openCleanRW(String)} taking a pre-encoded
     * native UTF-8 path pointer; lets callers cache the encoded path and
     * skip the per-call {@code byte[]} + native-malloc that
     * {@link #pathPtr(String)} incurs.
     */
    public static int openCleanRW(long pathPtr) {
        return openCleanRW0(pathPtr);
    }

    /**
     * Returns the on-disk size of {@code path} via {@code stat}, or -1 if
     * the path does not exist or is otherwise unreadable.
     */
    public static long length(String path) {
        long ptr = pathPtr(path);
        try {
            return length0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Variant of {@link #length(String)} taking a pre-encoded native UTF-8
     * path pointer; same allocation-elision rationale as {@link #openRW(long)}.
     */
    public static long length(long pathPtr) {
        return length0(pathPtr);
    }

    /**
     * Creates a directory at {@code path} with the given mode (POSIX-style
     * permission bits, e.g. {@link #DIR_MODE_DEFAULT}). Returns 0 on success,
     * non-zero on failure (e.g. parent missing, already exists, permission
     * denied). Non-recursive — caller must ensure the parent exists.
     */
    public static int mkdir(String path, int mode) {
        long ptr = pathPtr(path);
        try {
            return mkdir0(ptr, mode);
        } finally {
            freePathPtr(ptr);
        }
    }

    /** Returns {@code true} if {@code path} exists (as anything: file, dir, link). */
    public static boolean exists(String path) {
        long ptr = pathPtr(path);
        try {
            return exists0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Removes the file or empty directory at {@code path}. Returns
     * {@code true} on success.
     */
    public static boolean remove(String path) {
        long ptr = pathPtr(path);
        try {
            return remove0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Variant of {@link #remove(String)} that takes a pre-allocated native UTF-8
     * path pointer (from {@link #allocNativePath(String)}). Lets callers avoid
     * the byte[] allocation that {@link #pathPtr(String)} incurs on every call.
     */
    public static boolean remove(long pathPtr) {
        return remove0(pathPtr);
    }

    /**
     * Allocate a native UTF-8 representation of {@code path} suitable for
     * {@link #remove(long)} and other native call sites. The returned pointer
     * MUST be released via {@link #freeNativePath(long)}; failing to free it
     * leaks {@code path.length() + 9} bytes of native memory tagged
     * {@code MemoryTag.NATIVE_PATH}.
     */
    public static long allocNativePath(String path) {
        return pathPtr(path);
    }

    /** Releases a pointer returned by {@link #allocNativePath(String)}. */
    public static void freeNativePath(long pathPtr) {
        freePathPtr(pathPtr);
    }

    /**
     * Renames {@code oldPath} to {@code newPath} via the {@code rename}
     * syscall. On POSIX this is atomic when both paths live on the same
     * filesystem; on Win32 this uses {@code MoveFileExW}. Returns 0 on
     * success, non-zero on failure (errno set).
     */
    public static int rename(String oldPath, String newPath) {
        long o = pathPtr(oldPath);
        try {
            long n = pathPtr(newPath);
            try {
                return rename0(o, n);
            } finally {
                freePathPtr(n);
            }
        } finally {
            freePathPtr(o);
        }
    }

    /**
     * Begins iterating directory entries of {@code path}. Returns an opaque
     * native handle to be paired with {@link #findName(long)},
     * {@link #findType(long)}, {@link #findNext(long)}, and finally released
     * by {@link #findClose(long)}.
     * <p>
     * Return-value contract:
     * <ul>
     *   <li>{@code > 0}: handle to iterator with at least one entry buffered
     *       (POSIX/Win32 directories always have at least {@code .} and
     *       {@code ..}).</li>
     *   <li>{@code -1}: opendir / FindFirstFile failed — directory does not
     *       exist, no read permission, transient error, etc. The caller
     *       should NOT pass this value to {@link #findClose}, {@link #findName},
     *       {@link #findNext}, or {@link #findType}. Distinguishing this from
     *       a "real empty" success matters for recovery code paths that would
     *       otherwise silently treat an inaccessible directory as containing
     *       no entries to restore.</li>
     *   <li>{@code 0}: directory exists and was successfully enumerated but
     *       returned zero entries. POSIX/Win32 cannot in practice produce this
     *       (the special entries are always present); kept as a defensive
     *       case for unusual filesystems.</li>
     * </ul>
     * Typical usage:
     * <pre>{@code
     * long find = Files.findFirst(dir);
     * if (find < 0) {
     *     LOG.warn("could not enumerate {}", dir);
     *     return;
     * }
     * if (find == 0) return; // directory empty (rare)
     * try {
     *     int rc = 1;
     *     while (rc > 0) {
     *         String name = Files.utf8ToString(Files.findName(find));
     *         int type = Files.findType(find);
     *         // ... process entry ...
     *         rc = Files.findNext(find);
     *     }
     * } finally {
     *     Files.findClose(find);
     * }
     * }</pre>
     */
    public static long findFirst(String path) {
        long ptr = pathPtr(path);
        try {
            long h = findFirst0(ptr);
            // Native returns 0 on opendir/readdir failure. POSIX/Win32 dirs
            // that exist always contain ./.., so 0 in practice always means
            // "could not enumerate". Surface as -1 so callers can warn rather
            // than silently treat an inaccessible directory as empty.
            return h == 0 ? -1L : h;
        } finally {
            freePathPtr(ptr);
        }
    }

    /**
     * Decodes a native null-terminated UTF-8 string at {@code nameZ} into a
     * heap {@link String}. Returns {@code null} when {@code nameZ == 0}.
     * Allocates a {@code byte[]} of length {@code strlen(nameZ)} plus the
     * resulting String — not suitable for hot paths.
     */
    public static String utf8ToString(long nameZ) {
        if (nameZ == 0) {
            return null;
        }
        int len = 0;
        while (Unsafe.getUnsafe().getByte(nameZ + len) != 0) {
            len++;
        }
        byte[] bytes = new byte[len];
        Unsafe.getUnsafe().copyMemory(null, nameZ, bytes, Unsafe.BYTE_OFFSET, len);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads up to {@code len} bytes into native memory at {@code addr},
     * starting at file offset {@code offset}. Returns the actual number of
     * bytes read (may be less than {@code len} for short reads at EOF or on
     * a signal-interrupted syscall — though POSIX retries are done in C),
     * or -1 on hard failure.
     */
    public static native long read(int fd, long addr, long len, long offset);

    /**
     * Writes {@code len} bytes from native memory at {@code addr} to the file
     * at the given {@code offset} via {@code pwrite}. Returns the number of
     * bytes actually written; a short write (return value &lt; {@code len})
     * typically indicates ENOSPC mid-write and the caller should treat the
     * file as torn until truncated back. Returns -1 on hard failure.
     */
    public static native long write(int fd, long addr, long len, long offset);

    /**
     * Appends {@code len} bytes at end-of-file (whatever the current logical
     * position is). Used with fds opened via {@link #openAppend(String)}.
     */
    public static native long append(int fd, long addr, long len);

    /**
     * Forces all dirty pages of the open file to durable storage via
     * {@code fsync(2)}. Returns 0 on success, non-zero on failure (e.g.
     * EIO on a failing disk). Slow on most filesystems — use sparingly.
     */
    public static native int fsync(int fd);

    /**
     * Truncates the file to exactly {@code size} bytes via {@code ftruncate}.
     * Returns {@code true} on success. Does NOT reserve disk space — the
     * file's logical size is changed but blocks may be sparse.
     */
    public static native boolean truncate(int fd, long size);

    /**
     * Extends the file to at least {@code size} bytes and reserves real
     * disk blocks for the newly-extended range. Lets the caller catch
     * {@code ENOSPC} as a clean {@code false} return at this call site
     * rather than as a runtime SIGBUS (POSIX) or in-page exception
     * (Windows) on a later store into an mmap'd region.
     *
     * <p>Cross-platform contract — identical observable behaviour on
     * Linux, macOS, and Windows for any caller that does not
     * deliberately produce sparse files:
     * <ul>
     *   <li><b>Never shrinks.</b> Let {@code currentSize} be the file's
     *       current logical size and {@code target = max(size, currentSize)}.
     *       Requests where {@code size <= currentSize} short-circuit
     *       as a no-op success — the file is left exactly as it was.</li>
     *   <li><b>Reserves blocks for {@code [currentSize, target)}.</b>
     *       Pre-existing sparse holes inside {@code [0, currentSize)}
     *       are not retroactively filled. (Linux and macOS anchor the
     *       reservation at {@code currentSize}; Windows'
     *       {@code FILE_ALLOCATION_INFO} is file-scope and will
     *       re-reserve the existing range too, but a caller relying on
     *       hole-filling is writing non-portable code.)</li>
     *   <li><b>Errors surface as {@code false}.</b> Notably
     *       {@code ENOSPC} / {@code EFBIG} / {@code EIO} on POSIX and
     *       {@code ERROR_DISK_FULL} on Windows. The caller is
     *       responsible for closing the fd and unlinking the partial
     *       file — the partially-extended file would otherwise occupy
     *       up to {@code target} bytes on disk.</li>
     *   <li><b>Sparse fallback (Linux / macOS only).</b> When the
     *       reservation primitive itself reports the filesystem
     *       doesn't support the operation — {@code EINVAL} /
     *       {@code EOPNOTSUPP} on Linux, {@code ENOTSUP} /
     *       {@code EOPNOTSUPP} on macOS — the call still extends the
     *       logical size via {@code ftruncate} but leaves blocks
     *       sparse. The SIGBUS risk re-emerges for that filesystem
     *       only; operators should size requests conservatively
     *       against free space. Windows has no equivalent fallback.</li>
     * </ul>
     *
     * <p>Per-platform primitives (provided for the curious; not part
     * of the observable contract above):
     * <ul>
     *   <li><b>Linux</b>:
     *       {@code posix_fallocate(fd, currentSize, target - currentSize)}.</li>
     *   <li><b>macOS</b>: {@code fcntl(F_PREALLOCATE)} with
     *       {@code F_ALLOCATECONTIG | F_ALLOCATEALL} first, retrying
     *       with just {@code F_ALLOCATEALL} on failure (relaxed
     *       contiguity). Followed by {@code ftruncate(fd, target)} to
     *       advance EOF — F_PREALLOCATE does not.</li>
     *   <li><b>Windows</b>:
     *       {@code SetFileInformationByHandle(FileAllocationInfo)} with
     *       allocation target {@code target}, followed by
     *       {@code SetFileInformationByHandle(FileEndOfFileInfo)} with
     *       end-of-file {@code target}.</li>
     * </ul>
     */
    public static native boolean allocate(int fd, long size);

    /**
     * Returns the current file size in bytes via {@code fstat}, or -1 on
     * failure. Callers MUST treat -1 as a hard error and not as "empty
     * file"; the latter would silently mask filesystem failures.
     */
    public static native long length(int fd);

    /**
     * Acquires a non-blocking exclusive {@code flock} on {@code fd}. Returns
     * 0 on success, non-zero if another process already holds the lock or
     * the call failed. The lock is released automatically when the fd is
     * closed (or the process exits).
     */
    public static native int lock(int fd);

    /**
     * Maps {@code len} bytes of {@code fd} starting at {@code offset} into
     * the process address space. {@code flags} is one of {@link #MAP_RO} or
     * {@link #MAP_RW}; the mapping is always {@code MAP_SHARED} so writes
     * are visible to other mappers and to the underlying file. Returns the
     * native address of the mapping, or {@link #FAILED_MMAP_ADDRESS} on
     * failure (errno set). On success the {@code memoryTag} bucket is
     * incremented by {@code len} for accounting.
     * <p>
     * The file must already exist and be at least {@code offset + len} bytes
     * long; mmap does not extend files. Use {@link #allocate(int, long)} or
     * {@link #truncate(int, long)} first.
     */
    public static long mmap(int fd, long len, long offset, int flags, int memoryTag) {
        long addr = mmap0(fd, len, offset, flags, 0);
        if (addr != FAILED_MMAP_ADDRESS) {
            Unsafe.recordMemAlloc(len, memoryTag);
        }
        return addr;
    }

    /**
     * Releases a mapping established by {@link #mmap}. {@code address} and
     * {@code len} must match the values returned/used by the corresponding
     * {@link #mmap} call (partial unmap of a single mapping is technically
     * legal on POSIX but not supported by this wrapper). On success the
     * {@code memoryTag} bucket is decremented by {@code len}.
     */
    public static void munmap(long address, long len, int memoryTag) {
        if (munmap0(address, len) == 0) {
            Unsafe.recordMemAlloc(-len, memoryTag);
        }
    }

    /**
     * Flushes dirty pages in {@code [addr, addr+len)} of an mmap'd region
     * to durable storage. {@code async = true} issues {@code MS_ASYNC}
     * (kicks the writeback off, returns immediately); {@code async = false}
     * issues {@code MS_SYNC} (blocks until pages are persisted). Returns
     * 0 on success, non-zero on failure.
     */
    public static native int msync(long addr, long len, boolean async);

    /**
     * Returns a native pointer to the current entry's null-terminated name
     * (UTF-8). Pointer is valid only until the next {@link #findNext(long)}
     * or {@link #findClose(long)} on the same find handle.
     */
    public static native long findName(long findPtr);

    /**
     * Advances to the next directory entry. Returns {@code 1} on success,
     * {@code 0} at end-of-directory (no error), {@code -1} on read error.
     */
    public static native int findNext(long findPtr);

    /**
     * Returns the {@code DT_*} type constant for the current entry.
     * On filesystems that don't fill {@code d_type}, returns {@link #DT_UNKNOWN}.
     */
    public static native int findType(long findPtr);

    /** Releases the native iterator handle returned by {@link #findFirst(String)}. */
    public static native void findClose(long findPtr);

    static native int close0(int fd);

    static native int openRO0(long lpszName);

    static native int openRW0(long lpszName);

    static native int openAppend0(long lpszName);

    static native int openCleanRW0(long lpszName);

    static native long length0(long lpszName);

    static native int mkdir0(long lpszPath, int mode);

    static native boolean exists0(long lpszPath);

    static native boolean remove0(long lpszPath);

    static native int rename0(long lpszOld, long lpszNew);

    static native long findFirst0(long lpszName);

    static native long mmap0(int fd, long len, long offset, int flags, long baseAddress);

    static native int munmap0(long address, long len);

    private static native long getPageSize0();

    static long pathPtr(String path) {
        byte[] bytes = path.getBytes(StandardCharsets.UTF_8);
        long total = 8L + bytes.length + 1L;
        long base = Unsafe.malloc(total, MemoryTag.NATIVE_PATH);
        Unsafe.getUnsafe().putLong(base, total);
        long body = base + 8L;
        if (bytes.length > 0) {
            Unsafe.getUnsafe().copyMemory(bytes, Unsafe.BYTE_OFFSET, null, body, bytes.length);
        }
        Unsafe.getUnsafe().putByte(body + bytes.length, (byte) 0);
        return body;
    }

    static void freePathPtr(long bodyPtr) {
        if (bodyPtr == 0) {
            return;
        }
        long base = bodyPtr - 8L;
        long total = Unsafe.getUnsafe().getLong(base);
        Unsafe.free(base, total, MemoryTag.NATIVE_PATH);
    }

    static {
        Os.init();
        UTF_8 = StandardCharsets.UTF_8;
        PAGE_SIZE = getPageSize0();
    }
}
