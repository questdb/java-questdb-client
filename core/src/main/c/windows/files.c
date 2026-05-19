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

#include <shlwapi.h>
#include <minwindef.h>
#include <fileapi.h>
#include <winbase.h>
#include <direct.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include "../share/files.h"
#include "errno.h"
#include "files.h"

#include <stdio.h>
#include <ntdef.h>

/* Convert UTF-8 path to wide-char on the heap. Caller must free with free(). */
static wchar_t *utf8_to_wide(const char *utf8) {
    int n = MultiByteToWideChar(CP_UTF8, 0, utf8, -1, NULL, 0);
    if (n <= 0) {
        return NULL;
    }
    wchar_t *wide = (wchar_t *) malloc(sizeof(wchar_t) * (size_t) n);
    if (!wide) {
        return NULL;
    }
    if (MultiByteToWideChar(CP_UTF8, 0, utf8, -1, wide, n) <= 0) {
        free(wide);
        return NULL;
    }
    return wide;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_close0
        (JNIEnv *e, jclass cl, jint fd) {
    jint r = CloseHandle(FD_TO_HANDLE(fd));
    if (!r) {
        SaveLastError();
        return -1;
    }
    return 0;
}

static jint open_file(const char *utf8Path,
                      DWORD desiredAccess,
                      DWORD shareMode,
                      DWORD creationDisposition,
                      DWORD flagsAndAttributes) {
    wchar_t *wide = utf8_to_wide(utf8Path);
    if (!wide) {
        SaveLastError();
        return -1;
    }
    HANDLE h = CreateFileW(wide, desiredAccess, shareMode, NULL,
                           creationDisposition, flagsAndAttributes, NULL);
    free(wide);
    if (h == INVALID_HANDLE_VALUE) {
        SaveLastError();
        return -1;
    }
    return HANDLE_TO_FD(h);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openRO0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open_file((const char *) (uintptr_t) lpszName,
                     GENERIC_READ,
                     FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                     OPEN_EXISTING,
                     FILE_ATTRIBUTE_NORMAL);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openRW0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open_file((const char *) (uintptr_t) lpszName,
                     GENERIC_READ | GENERIC_WRITE,
                     FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                     OPEN_ALWAYS,
                     FILE_ATTRIBUTE_NORMAL);
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openAppend0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    jint fd = open_file((const char *) (uintptr_t) lpszName,
                        FILE_APPEND_DATA,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                        OPEN_ALWAYS,
                        FILE_ATTRIBUTE_NORMAL);
    if (fd < 0) {
        return fd;
    }
    LARGE_INTEGER zero;
    zero.QuadPart = 0;
    if (!SetFilePointerEx(FD_TO_HANDLE(fd), zero, NULL, FILE_END)) {
        SaveLastError();
        CloseHandle(FD_TO_HANDLE(fd));
        return -1;
    }
    return fd;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_openCleanRW0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    return open_file((const char *) (uintptr_t) lpszName,
                     GENERIC_READ | GENERIC_WRITE,
                     FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                     CREATE_ALWAYS,
                     FILE_ATTRIBUTE_NORMAL);
}

/* ReadFile/WriteFile take a DWORD (uint32) byte count, but the JNI signature
 * exposes a jlong. A direct (DWORD) cast silently truncates the high 32 bits,
 * which means a 4 GiB request becomes a 0-byte transfer — the worst kind of
 * silent failure. Clamp to MAXDWORD so any oversized request is served as a
 * short transfer (matching POSIX semantics on the share/files.c side); the
 * Java caller already loops on the return value. Reject negative len up front
 * so it doesn't get reinterpreted as a huge unsigned DWORD. */
static inline DWORD clamp_len(jlong len) {
    if (len > (jlong) MAXDWORD) {
        return MAXDWORD;
    }
    return (DWORD) len;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_read
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jlong len, jlong offset) {
    if (len < 0) {
        SetLastError(ERROR_INVALID_PARAMETER);
        SaveLastError();
        return -1;
    }
    if (len == 0) return 0;
    OVERLAPPED ov;
    memset(&ov, 0, sizeof(ov));
    ov.Offset = (DWORD) (offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD) (offset >> 32);
    DWORD got = 0;
    if (!ReadFile(FD_TO_HANDLE(fd), (LPVOID) (uintptr_t) addr, clamp_len(len), &got, &ov)) {
        DWORD err = GetLastError();
        if (err == ERROR_HANDLE_EOF) {
            return 0;
        }
        SaveLastError();
        return -1;
    }
    return (jlong) got;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_write
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jlong len, jlong offset) {
    if (len < 0) {
        SetLastError(ERROR_INVALID_PARAMETER);
        SaveLastError();
        return -1;
    }
    if (len == 0) return 0;
    OVERLAPPED ov;
    memset(&ov, 0, sizeof(ov));
    ov.Offset = (DWORD) (offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD) (offset >> 32);
    DWORD wrote = 0;
    if (!WriteFile(FD_TO_HANDLE(fd), (LPCVOID) (uintptr_t) addr, clamp_len(len), &wrote, &ov)) {
        SaveLastError();
        return -1;
    }
    return (jlong) wrote;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_append
        (JNIEnv *e, jclass cl, jint fd, jlong addr, jlong len) {
    if (len < 0) {
        SetLastError(ERROR_INVALID_PARAMETER);
        SaveLastError();
        return -1;
    }
    if (len == 0) return 0;
    DWORD wrote = 0;
    if (!WriteFile(FD_TO_HANDLE(fd), (LPCVOID) (uintptr_t) addr, clamp_len(len), &wrote, NULL)) {
        SaveLastError();
        return -1;
    }
    return (jlong) wrote;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_fsync
        (JNIEnv *e, jclass cl, jint fd) {
    if (!FlushFileBuffers(FD_TO_HANDLE(fd))) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_truncate
        (JNIEnv *e, jclass cl, jint fd, jlong size) {
    FILE_END_OF_FILE_INFO eof;
    eof.EndOfFile.QuadPart = size;
    if (!SetFileInformationByHandle(FD_TO_HANDLE(fd), FileEndOfFileInfo, &eof, sizeof(eof))) {
        SaveLastError();
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_allocate
        (JNIEnv *e, jclass cl, jint fd, jlong size) {
    /* Cross-platform contract — full version lives on
     * Files.allocate's javadoc; key invariants restated here so the
     * implementation reads on its own:
     *   - Never shrinks: target = max(size, currentSize); if size <=
     *     currentSize, return success without touching the file.
     *   - Reserves real disk clusters for [currentSize, target). On NTFS
     *     FILE_ALLOCATION_INFO is file-scope (no per-range API), so it
     *     implicitly re-reserves [0, currentSize) as well — visible only
     *     to a caller who deliberately created sparse holes inside that
     *     range, and that caller should treat hole-filling as
     *     non-portable behaviour.
     *   - ERROR_DISK_FULL surfaces as JNI_FALSE. There is no
     *     sparse-fallback equivalent — Windows always reserves or
     *     fails; spec-compliant fallback only applies on Linux/macOS. */
    HANDLE handle = FD_TO_HANDLE(fd);

    LARGE_INTEGER current;
    if (!GetFileSizeEx(handle, &current)) {
        SaveLastError();
        return JNI_FALSE;
    }
    jlong target = size > current.QuadPart ? size : (jlong) current.QuadPart;
    if (target == current.QuadPart) {
        /* Nothing to extend, nothing to reserve. The early-return is
         * what makes "never shrinks" hold and keeps behaviour aligned
         * with the Linux/macOS short-circuit. */
        return JNI_TRUE;
    }

    FILE_ALLOCATION_INFO alloc;
    alloc.AllocationSize.QuadPart = target;
    if (!SetFileInformationByHandle(handle, FileAllocationInfo, &alloc, sizeof(alloc))) {
        SaveLastError();
        return JNI_FALSE;
    }

    /* FILE_ALLOCATION_INFO reserves clusters but does not advance EOF.
     * We've already ruled out target == current above, so the file
     * always needs its logical size pushed out to target. */
    FILE_END_OF_FILE_INFO eof;
    eof.EndOfFile.QuadPart = target;
    if (!SetFileInformationByHandle(handle, FileEndOfFileInfo, &eof, sizeof(eof))) {
        SaveLastError();
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_length
        (JNIEnv *e, jclass cl, jint fd) {
    LARGE_INTEGER sz;
    if (!GetFileSizeEx(FD_TO_HANDLE(fd), &sz)) {
        SaveLastError();
        return -1;
    }
    return (jlong) sz.QuadPart;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_length0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    wchar_t *wide = utf8_to_wide((const char *) (uintptr_t) lpszName);
    if (!wide) {
        SaveLastError();
        return -1;
    }
    HANDLE h = CreateFileW(wide,
                           GENERIC_READ,
                           FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                           NULL,
                           OPEN_EXISTING,
                           FILE_ATTRIBUTE_NORMAL,
                           NULL);
    free(wide);
    if (h == INVALID_HANDLE_VALUE) {
        SaveLastError();
        return -1;
    }
    LARGE_INTEGER sz;
    BOOL ok = GetFileSizeEx(h, &sz);
    CloseHandle(h);
    if (!ok) {
        SaveLastError();
        return -1;
    }
    return (jlong) sz.QuadPart;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_lock
        (JNIEnv *e, jclass cl, jint fd) {
    OVERLAPPED ov;
    memset(&ov, 0, sizeof(ov));
    if (!LockFileEx(FD_TO_HANDLE(fd),
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    MAXDWORD,
                    MAXDWORD,
                    &ov)) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_mkdir0
        (JNIEnv *e, jclass cl, jlong lpszPath, jint mode) {
    (void) mode;
    wchar_t *wide = utf8_to_wide((const char *) (uintptr_t) lpszPath);
    if (!wide) {
        SaveLastError();
        return -1;
    }
    BOOL ok = CreateDirectoryW(wide, NULL);
    free(wide);
    if (!ok) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_exists0
        (JNIEnv *e, jclass cl, jlong lpszPath) {
    wchar_t *wide = utf8_to_wide((const char *) (uintptr_t) lpszPath);
    if (!wide) {
        return JNI_FALSE;
    }
    BOOL ok = PathFileExistsW(wide);
    free(wide);
    return ok ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL Java_io_questdb_client_std_Files_remove0
        (JNIEnv *e, jclass cl, jlong lpszPath) {
    wchar_t *wide = utf8_to_wide((const char *) (uintptr_t) lpszPath);
    if (!wide) {
        SaveLastError();
        return JNI_FALSE;
    }
    DWORD attrs = GetFileAttributesW(wide);
    BOOL ok;
    if (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY)) {
        ok = RemoveDirectoryW(wide);
    } else {
        ok = DeleteFileW(wide);
    }
    free(wide);
    if (!ok) {
        SaveLastError();
        return JNI_FALSE;
    }
    return JNI_TRUE;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_rename0
        (JNIEnv *e, jclass cl, jlong lpszOld, jlong lpszNew) {
    wchar_t *oldW = utf8_to_wide((const char *) (uintptr_t) lpszOld);
    if (!oldW) {
        SaveLastError();
        return -1;
    }
    wchar_t *newW = utf8_to_wide((const char *) (uintptr_t) lpszNew);
    if (!newW) {
        SaveLastError();
        free(oldW);
        return -1;
    }
    BOOL ok = MoveFileExW(oldW, newW, MOVEFILE_REPLACE_EXISTING);
    free(oldW);
    free(newW);
    if (!ok) {
        SaveLastError();
        return -1;
    }
    return 0;
}

typedef struct {
    HANDLE handle;
    WIN32_FIND_DATAW data;
    char utf8name[1024];
    int hasEntry;
} qdb_find_t;

static void win_findname_to_utf8(qdb_find_t *find) {
    int n = WideCharToMultiByte(CP_UTF8, 0, find->data.cFileName, -1,
                                 find->utf8name, (int) sizeof(find->utf8name),
                                 NULL, NULL);
    if (n <= 0) {
        find->utf8name[0] = '\0';
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_findFirst0
        (JNIEnv *e, jclass cl, jlong lpszName) {
    /* Windows FindFirstFile expects a search pattern, e.g. C:\\path\\* */
    const char *path = (const char *) (uintptr_t) lpszName;
    size_t pathLen = strlen(path);
    /* allocate path + "\\*" + NUL */
    char *pattern = (char *) malloc(pathLen + 3);
    if (!pattern) {
        return 0;
    }
    memcpy(pattern, path, pathLen);
    if (pathLen > 0 && pattern[pathLen - 1] != '\\' && pattern[pathLen - 1] != '/') {
        pattern[pathLen++] = '\\';
    }
    pattern[pathLen++] = '*';
    pattern[pathLen] = '\0';

    wchar_t *wide = utf8_to_wide(pattern);
    free(pattern);
    if (!wide) {
        SaveLastError();
        return 0;
    }

    qdb_find_t *find = (qdb_find_t *) malloc(sizeof(qdb_find_t));
    if (!find) {
        free(wide);
        return 0;
    }
    find->handle = FindFirstFileW(wide, &find->data);
    free(wide);
    if (find->handle == INVALID_HANDLE_VALUE) {
        SaveLastError();
        free(find);
        return 0;
    }
    find->hasEntry = 1;
    win_findname_to_utf8(find);
    return (jlong) (uintptr_t) find;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_findNext
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (!find) {
        return -1;
    }
    if (!FindNextFileW(find->handle, &find->data)) {
        DWORD err = GetLastError();
        find->hasEntry = 0;
        if (err == ERROR_NO_MORE_FILES) {
            return 0;
        }
        SaveLastError();
        return -1;
    }
    find->hasEntry = 1;
    win_findname_to_utf8(find);
    return 1;
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_findName
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (!find || !find->hasEntry) {
        return 0;
    }
    return (jlong) (uintptr_t) find->utf8name;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_findType
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (!find || !find->hasEntry) {
        return 0; /* DT_UNKNOWN */
    }
    DWORD attrs = find->data.dwFileAttributes;
    if (attrs & FILE_ATTRIBUTE_REPARSE_POINT) {
        return 10; /* DT_LNK */
    }
    if (attrs & FILE_ATTRIBUTE_DIRECTORY) {
        return 4; /* DT_DIR */
    }
    return 8; /* DT_FILE */
}

JNIEXPORT void JNICALL Java_io_questdb_client_std_Files_findClose
        (JNIEnv *e, jclass cl, jlong findPtr) {
    qdb_find_t *find = (qdb_find_t *) (uintptr_t) findPtr;
    if (find) {
        if (find->handle != INVALID_HANDLE_VALUE) {
            FindClose(find->handle);
        }
        free(find);
    }
}

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_getPageSize0
        (JNIEnv *e, jclass cl) {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return (jlong) si.dwAllocationGranularity;
}

/* Mirror of io.questdb.client.std.Files.MAP_RO / MAP_RW. */
#define QDB_MAP_RO 1
#define QDB_MAP_RW 2

JNIEXPORT jlong JNICALL Java_io_questdb_client_std_Files_mmap0
        (JNIEnv *e, jclass cl, jint fd, jlong len, jlong offset, jint flags, jlong baseAddress) {
    if (len == 0) {
        /* Win32 MapViewOfFileEx interprets dwNumberOfBytesToMap == 0 as
         * "map to end of mapping". Reject explicitly so the wrapper has
         * POSIX-compatible semantics (POSIX mmap with len==0 returns
         * EINVAL). */
        SetLastError(ERROR_INVALID_PARAMETER);
        SaveLastError();
        return -1;
    }

    jlong maxsize = offset + len;
    DWORD flProtect;
    DWORD dwDesiredAccess;
    if (flags == QDB_MAP_RW) {
        flProtect = PAGE_READWRITE;
        dwDesiredAccess = FILE_MAP_WRITE;
    } else {
        flProtect = PAGE_READONLY;
        dwDesiredAccess = FILE_MAP_READ;
    }

    HANDLE hMapping = CreateFileMapping(
            FD_TO_HANDLE(fd),
            NULL,
            flProtect | SEC_RESERVE,
            (DWORD) (maxsize >> 32),
            (DWORD) maxsize,
            NULL);
    if (hMapping == NULL) {
        SaveLastError();
        return -1;
    }

    LPCVOID address = MapViewOfFileEx(
            hMapping,
            dwDesiredAccess,
            (DWORD) (offset >> 32),
            (DWORD) offset,
            (SIZE_T) len,
            (LPVOID) (uintptr_t) baseAddress);

    SaveLastError();

    /* The mapping handle can be closed immediately — the view holds its own
     * reference and the file mapping persists until the last view is unmapped. */
    if (CloseHandle(hMapping) == 0) {
        SaveLastError();
        if (address != NULL) {
            UnmapViewOfFile(address);
        }
        return -1;
    }

    if (address == NULL) {
        return -1;
    }
    return (jlong) (uintptr_t) address;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_munmap0
        (JNIEnv *e, jclass cl, jlong address, jlong len) {
    if (UnmapViewOfFile((LPCVOID) (uintptr_t) address) == 0) {
        SaveLastError();
        return -1;
    }
    return 0;
}

JNIEXPORT jint JNICALL Java_io_questdb_client_std_Files_msync
        (JNIEnv *e, jclass cl, jlong addr, jlong len, jboolean async) {
    /* FlushViewOfFile schedules a write, blocking until the file system
     * driver has accepted the write into its cache. For "fully durable"
     * (POSIX MS_SYNC equivalent) we need a follow-up FlushFileBuffers,
     * but that needs the file handle which we no longer hold here.
     * MS_ASYNC maps cleanly: don't wait for further confirmation. */
    if (FlushViewOfFile((LPCVOID) (uintptr_t) addr, (SIZE_T) len) == 0) {
        SaveLastError();
        return -1;
    }
    /* We deliberately do NOT call FlushFileBuffers in the async case;
     * sync callers wanting the strongest durability should fsync the fd
     * separately via Files.fsync. */
    (void) async;
    return 0;
}
