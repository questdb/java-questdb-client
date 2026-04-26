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

    int close(int fd);

    boolean exists(String path);

    void findClose(long findPtr);

    long findFirst(String dir);

    long findName(long findPtr);

    int findNext(long findPtr);

    int findType(long findPtr);

    int fsync(int fd);

    long length(int fd);

    int lock(int fd);

    int mkdir(String path, int mode);

    int openCleanRW(String path, long size);

    int openRW(String path);

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
