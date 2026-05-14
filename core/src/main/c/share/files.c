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

#define _GNU_SOURCE

#include <unistd.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <stdint.h>

#include "files.h"

/* Mirror of io.questdb.client.std.Files.MAP_RO / MAP_RW. Hard-coded rather
 * than #include'd from a javah-generated header because this file does not
 * pull in any generated symbols (the rest of the file works the same way). */
#define QDB_MAP_RO 1
#define QDB_MAP_RW 2

#define RESTARTABLE(_expr_, _rc_) \
    do { _rc_ = (_expr_); } while ((_rc_) == -1 && errno == EINTR)

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_close0
        (JNIEnv *e, jclass cl, jint fd) {
    return close((int) fd);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openRO0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    int fd;
    RESTARTABLE(open((const char *) (uintptr_t) lpszName, O_RDONLY), fd);
    return (jint) fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openRW0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    int fd;
    RESTARTABLE(open((const char *) (uintptr_t) lpszName, O_CREAT | O_RDWR, 0644), fd);
    return (jint) fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openAppend0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    int fd;
    RESTARTABLE(open((const char *) (uintptr_t) lpszName, O_CREAT | O_WRONLY | O_APPEND, 0644), fd);
    return (jint) fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openCleanRW0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    int fd;
    RESTARTABLE(open((const char *) (uintptr_t) lpszName, O_CREAT | O_TRUNC | O_RDWR, 0644), fd);
    return (jint) fd;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_read
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jlong len, jlong offset) {
    // Reject negative len explicitly: jlong is signed but pread takes a
    // size_t. Without this guard the cast wraps a small negative value
    // into an enormous unsigned read length and the kernel may either
    // SEGV on the address space or scribble far past the caller's buffer.
    // The Win32 path already does this; matching here.
    if (len < 0) {
        errno = EINVAL;
        return -1;
    }
    ssize_t res;
    RESTARTABLE(pread((int) fd, (void *) (uintptr_t) addr, (size_t) len, (off_t) offset), res);
    return (jlong) res;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_write
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jlong len, jlong offset) {
    if (len < 0) {
        errno = EINVAL;
        return -1;
    }
    ssize_t res;
    RESTARTABLE(pwrite((int) fd, (const void *) (uintptr_t) addr, (size_t) len, (off_t) offset), res);
    return (jlong) res;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_append
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jlong len) {
    if (len < 0) {
        errno = EINVAL;
        return -1;
    }
    ssize_t res;
    RESTARTABLE(write((int) fd, (const void *) (uintptr_t) addr, (size_t) len), res);
    return (jlong) res;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_fsync
        (JNIEnv *e, jclass cl, jint fd) {
    int res;
    RESTARTABLE(fsync((int) fd), res);
    return res;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_truncate
        (JNIEnv *e, jclass cl, jint fd, jlong size) {
    int res;
    RESTARTABLE(ftruncate((int) fd, (off_t) size), res);
    return res == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong size) {
    /* Cross-platform contract — full version lives on
     * Files.allocate's javadoc; key invariants restated here so the
     * implementation reads on its own:
     *   - Never shrinks: target = max(size, currentSize); if size <=
     *     currentSize, return success without touching the file.
     *   - Reserves real disk blocks for [currentSize, target). The
     *     pre-existing range [0, currentSize) is left untouched so the
     *     three platforms agree on what the call does — anchoring the
     *     reservation at currentSize matches macOS's F_PEOFPOSMODE
     *     semantics (which can only allocate beyond EOF without writes
     *     that would corrupt mmap'd content).
     *   - Real errors (ENOSPC, EFBIG, EIO, ...) surface as JNI_FALSE.
     *     Filesystem-doesn't-support errnos degrade to a sparse
     *     ftruncate fallback per sf-client.md §6. */
    struct stat st;
    if (fstat((int) fd, &st) != 0) {
        return JNI_FALSE;
    }
    off_t target = (off_t) size;
    if (st.st_size > target) {
        target = st.st_size;
    }
    if (target == st.st_size) {
        /* Nothing to extend, nothing to reserve. Returning here is what
         * makes the never-shrinks property hold across the
         * ftruncate-fallback path below. */
        return JNI_TRUE;
    }
    off_t newBytes = target - st.st_size;

#if defined(__linux__)
    /* posix_fallocate at offset=currentSize reserves only the
     * newly-extended range [currentSize, target), matching macOS's
     * F_PEOFPOSMODE behaviour and keeping the cross-platform contract
     * consistent on whether pre-existing sparse holes get filled (they
     * do not). On success the file's logical size is already target —
     * we return early to skip the unnecessary ftruncate. */
    int res = posix_fallocate((int) fd, st.st_size, newBytes);
    if (res == 0) {
        return JNI_TRUE;
    }
    if (res != EINVAL && res != EOPNOTSUPP) {
        errno = res;
        return JNI_FALSE;
    }
    /* Filesystem doesn't support fallocate; fall through to ftruncate.
     * That is the sparse-fallback path — extends to target but blocks
     * remain sparse, so a later store past an unallocated page may
     * still raise SIGBUS. Per the contract, ftruncate here only ever
     * grows (target > st.st_size) so "never shrinks" still holds. */
#elif defined(__APPLE__)
    fstore_t fst;
    fst.fst_flags = F_ALLOCATECONTIG | F_ALLOCATEALL;
    fst.fst_posmode = F_PEOFPOSMODE;
    fst.fst_offset = 0;
    /* fst_length is the number of bytes to allocate BEYOND EOF — not the
     * target total. Passing the full target would over-allocate by
     * currentSize on a non-empty file. */
    fst.fst_length = newBytes;
    fst.fst_bytesalloc = 0;
    if (fcntl((int) fd, F_PREALLOCATE, &fst) == -1) {
        /* Contiguous allocation failed (e.g. fragmented filesystem); retry
         * non-contiguous all-or-nothing. Only fall through to ftruncate when
         * the filesystem doesn't support F_PREALLOCATE at all; real failures
         * (notably ENOSPC) must surface so the caller doesn't end up with a
         * sparse file that SIGBUSes on later mmap store (sf-client.md §6). */
        fst.fst_flags = F_ALLOCATEALL;
        if (fcntl((int) fd, F_PREALLOCATE, &fst) == -1
            && errno != ENOTSUP && errno != EOPNOTSUPP) {
            return JNI_FALSE;
        }
    }
    /* F_PREALLOCATE never advances EOF, so ftruncate below is part of
     * the normal path on macOS — it's NOT just a sparse-fallback. */
#endif
    int res2;
    RESTARTABLE(ftruncate((int) fd, target), res2);
    return res2 == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_length
        (JNIEnv *e, jclass cl, jint fd) {
    struct stat st;
    if (fstat((int) fd, &st) != 0) {
        return -1;
    }
    return (jlong) st.st_size;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_length0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    struct stat st;
    if (stat((const char *) (uintptr_t) lpszName, &st) != 0) {
        return -1;
    }
    return (jlong) st.st_size;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_lock
        (JNIEnv *e, jclass cl, jint fd) {
    return flock((int) fd, LOCK_EX | LOCK_NB);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_mkdir0
        (JNIEnv *e, jclass cl, jlong lpszPath, jint mode) {
    return mkdir((const char *) (uintptr_t) lpszPath, (mode_t) mode);
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_exists0
        (JNIEnv *e, jclass cl, jlong lpszPath) {
    return access((const char *) (uintptr_t) lpszPath, F_OK) == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_remove0
        (JNIEnv *e, jclass cl, jlong lpszPath) {
    return remove((const char *) (uintptr_t) lpszPath) == 0 ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_rename0
        (JNIEnv *e, jclass cl, jlong lpszOld, jlong lpszNew) {
    return rename((const char *) (uintptr_t) lpszOld, (const char *) (uintptr_t) lpszNew);
}

typedef struct {
    DIR *dir;
    struct dirent *entry;
} qdb_find_t;

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_findFirst0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    DIR *dir = opendir((const char *) (uintptr_t) lpszName);
    if (!dir) {
        return 0;
    }
    qdb_find_t *find = (qdb_find_t *) malloc(sizeof(qdb_find_t));
    if (!find) {
        closedir(dir);
        return 0;
    }
    find->dir = dir;
    errno = 0;
    find->entry = readdir(dir);
    if (!find->entry) {
        int saved = errno;
        closedir(dir);
        free(find);
        errno = saved;
        return 0;
    }
    return (jlong) (uintptr_t) find;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (!find) {
        return -1;
    }
    errno = 0;
    find->entry = readdir(find->dir);
    if (find->entry) {
        return 1;
    }
    return errno == 0 ? 0 : -1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (!find || !find->entry) {
        return 0;
    }
    return (jlong) (uintptr_t) find->entry->d_name;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (!find || !find->entry) {
        return 0;
    }
    return (jint) find->entry->d_type;
}

JNIEXPORT void JNICALL Java_io_questdb_client_std_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (find) {
        closedir(find->dir);
        free(find);
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_getPageSize0
        (JNIEnv *e, jclass cl) {
    long sz = sysconf(_SC_PAGESIZE);
    return (jlong) (sz > 0 ? sz : 4096);
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_mmap0
        (JNIEnv *e, jclass cl, jint fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
    int prot = 0;
    if (flags == QDB_MAP_RO) {
        prot = PROT_READ;
    } else if (flags == QDB_MAP_RW) {
        prot = PROT_READ | PROT_WRITE;
    }
    void *addr = mmap((void *) (uintptr_t) baseAddress, (size_t) len, prot, MAP_SHARED, (int) fd, (off_t) offset);
    /* MAP_FAILED is (void *) -1; cast to jlong gives -1 sentinel matching FAILED_MMAP_ADDRESS. */
    return (jlong) (intptr_t) addr;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_munmap0
        (JNIEnv *e, jclass cl, jlong address, jlong len) {
    return munmap((void *) (uintptr_t) address, (size_t) len);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_msync
        (JNIEnv *e, jclass cl, jlong addr, jlong len, jboolean async) {
    return msync((void *) (uintptr_t) addr, (size_t) len, async ? MS_ASYNC : MS_SYNC);
}
