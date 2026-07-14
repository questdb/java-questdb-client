/*******************************************************************************
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

package io.questdb.client.test.tools;

import io.questdb.client.std.FilesFacade;

/**
 * Pass-through {@link FilesFacade} that forwards every call to
 * {@link FilesFacade#INSTANCE}. Subclass it and override the ONE method whose
 * failure you want to inject -- a short write, a truncate that cannot trim, an
 * implausible file length -- so a test can drive a real I/O failure path without a
 * real broken disk.
 * <p>
 * {@code FilesFacade} has no default methods, so the delegation boilerplate is
 * unavoidable; it lives here once rather than being stamped into each test that
 * needs a fault seam.
 */
public class DelegatingFilesFacade implements FilesFacade {
    @Override
    public long allocNativePath(String path) {
        return INSTANCE.allocNativePath(path);
    }

    @Override
    public boolean allocate(int fd, long size) {
        return INSTANCE.allocate(fd, size);
    }

    @Override
    public int close(int fd) {
        return INSTANCE.close(fd);
    }

    @Override
    public boolean exists(String path) {
        return INSTANCE.exists(path);
    }

    @Override
    public void findClose(long findPtr) {
        INSTANCE.findClose(findPtr);
    }

    @Override
    public long findFirst(String dir) {
        return INSTANCE.findFirst(dir);
    }

    @Override
    public long findName(long findPtr) {
        return INSTANCE.findName(findPtr);
    }

    @Override
    public int findNext(long findPtr) {
        return INSTANCE.findNext(findPtr);
    }

    @Override
    public int findType(long findPtr) {
        return INSTANCE.findType(findPtr);
    }

    @Override
    public void freeNativePath(long pathPtr) {
        INSTANCE.freeNativePath(pathPtr);
    }

    @Override
    public int fsync(int fd) {
        return INSTANCE.fsync(fd);
    }

    @Override
    public long length(int fd) {
        return INSTANCE.length(fd);
    }

    @Override
    public long length(String path) {
        return INSTANCE.length(path);
    }

    @Override
    public long length(long pathPtr) {
        return INSTANCE.length(pathPtr);
    }

    @Override
    public int lock(int fd) {
        return INSTANCE.lock(fd);
    }

    @Override
    public int mkdir(String path, int mode) {
        return INSTANCE.mkdir(path, mode);
    }

    @Override
    public int openCleanRW(String path) {
        return INSTANCE.openCleanRW(path);
    }

    @Override
    public int openCleanRW(long pathPtr) {
        return INSTANCE.openCleanRW(pathPtr);
    }

    @Override
    public int openRW(String path) {
        return INSTANCE.openRW(path);
    }

    @Override
    public int openRW(long pathPtr) {
        return INSTANCE.openRW(pathPtr);
    }

    @Override
    public long read(int fd, long addr, long len, long offset) {
        return INSTANCE.read(fd, addr, len, offset);
    }

    @Override
    public boolean remove(String path) {
        return INSTANCE.remove(path);
    }

    @Override
    public boolean remove(long pathPtr) {
        return INSTANCE.remove(pathPtr);
    }

    @Override
    public int rename(String oldPath, String newPath) {
        return INSTANCE.rename(oldPath, newPath);
    }

    @Override
    public boolean truncate(int fd, long size) {
        return INSTANCE.truncate(fd, size);
    }

    @Override
    public long write(int fd, long addr, long len, long offset) {
        return INSTANCE.write(fd, addr, len, offset);
    }
}
