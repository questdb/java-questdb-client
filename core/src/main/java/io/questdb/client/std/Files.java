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

public final class Files {
    public static final Charset UTF_8;
    public static final long PAGE_SIZE;

    public static final int DT_UNKNOWN = 0;
    public static final int DT_DIR = 4;
    public static final int DT_FILE = 8;
    public static final int DT_LNK = 10;

    private Files() {
    }

    public static int close(int fd) {
        if (fd > 2) {
            return close0(fd);
        }
        return -1;
    }

    public static int openRO(String path) {
        long ptr = pathPtr(path);
        try {
            return openRO0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static int openRW(String path) {
        long ptr = pathPtr(path);
        try {
            return openRW0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static int openAppend(String path) {
        long ptr = pathPtr(path);
        try {
            return openAppend0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static int openCleanRW(String path, long size) {
        long ptr = pathPtr(path);
        try {
            return openCleanRW0(ptr, size);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static long length(String path) {
        long ptr = pathPtr(path);
        try {
            return length0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static int mkdir(String path, int mode) {
        long ptr = pathPtr(path);
        try {
            return mkdir0(ptr, mode);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static boolean exists(String path) {
        long ptr = pathPtr(path);
        try {
            return exists0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static boolean remove(String path) {
        long ptr = pathPtr(path);
        try {
            return remove0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

    public static int rename(String oldPath, String newPath) {
        long o = pathPtr(oldPath);
        long n = pathPtr(newPath);
        try {
            return rename0(o, n);
        } finally {
            freePathPtr(o);
            freePathPtr(n);
        }
    }

    public static long findFirst(String path) {
        long ptr = pathPtr(path);
        try {
            return findFirst0(ptr);
        } finally {
            freePathPtr(ptr);
        }
    }

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

    public static native long read(int fd, long addr, long len, long offset);

    public static native long write(int fd, long addr, long len, long offset);

    public static native long append(int fd, long addr, long len);

    public static native int fsync(int fd);

    public static native boolean truncate(int fd, long size);

    public static native boolean allocate(int fd, long size);

    public static native long length(int fd);

    public static native int lock(int fd);

    public static native long findName(long findPtr);

    public static native int findNext(long findPtr);

    public static native int findType(long findPtr);

    public static native void findClose(long findPtr);

    static native int close0(int fd);

    static native int openRO0(long lpszName);

    static native int openRW0(long lpszName);

    static native int openAppend0(long lpszName);

    static native int openCleanRW0(long lpszName, long size);

    static native long length0(long lpszName);

    static native int mkdir0(long lpszPath, int mode);

    static native boolean exists0(long lpszPath);

    static native boolean remove0(long lpszPath);

    static native int rename0(long lpszOld, long lpszNew);

    static native long findFirst0(long lpszName);

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
