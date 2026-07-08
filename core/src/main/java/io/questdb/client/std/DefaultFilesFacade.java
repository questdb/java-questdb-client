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
 * Default {@link FilesFacade} that forwards every call straight to the static
 * {@link Files} JNI surface. No-op overhead in steady state; lets tests wrap
 * or replace any single call.
 */
final class DefaultFilesFacade implements FilesFacade {

    @Override
    public boolean allocate(int fd, long size) {
        return Files.allocate(fd, size);
    }

    @Override
    public long allocNativePath(String path) {
        return Files.allocNativePath(path);
    }

    @Override
    public int close(int fd) {
        return Files.close(fd);
    }

    @Override
    public boolean exists(String path) {
        return Files.exists(path);
    }

    @Override
    public void findClose(long findPtr) {
        Files.findClose(findPtr);
    }

    @Override
    public long findFirst(String dir) {
        return Files.findFirst(dir);
    }

    @Override
    public long findName(long findPtr) {
        return Files.findName(findPtr);
    }

    @Override
    public int findNext(long findPtr) {
        return Files.findNext(findPtr);
    }

    @Override
    public int findType(long findPtr) {
        return Files.findType(findPtr);
    }

    @Override
    public void freeNativePath(long pathPtr) {
        Files.freeNativePath(pathPtr);
    }

    @Override
    public int fsync(int fd) {
        return Files.fsync(fd);
    }

    @Override
    public long length(int fd) {
        return Files.length(fd);
    }

    @Override
    public long length(String path) {
        return Files.length(path);
    }

    @Override
    public int lock(int fd) {
        return Files.lock(fd);
    }

    @Override
    public int mkdir(String path, int mode) {
        return Files.mkdir(path, mode);
    }

    @Override
    public int openCleanRW(String path) {
        return Files.openCleanRW(path);
    }

    @Override
    public int openCleanRW(long pathPtr) {
        return Files.openCleanRW(pathPtr);
    }

    @Override
    public int openRW(String path) {
        return Files.openRW(path);
    }

    @Override
    public int openRW(long pathPtr) {
        return Files.openRW(pathPtr);
    }

    @Override
    public long length(long pathPtr) {
        return Files.length(pathPtr);
    }

    @Override
    public long read(int fd, long addr, long len, long offset) {
        return Files.read(fd, addr, len, offset);
    }

    @Override
    public boolean remove(String path) {
        return Files.remove(path);
    }

    @Override
    public boolean remove(long pathPtr) {
        return Files.remove(pathPtr);
    }

    @Override
    public int rename(String oldPath, String newPath) {
        return Files.rename(oldPath, newPath);
    }

    @Override
    public boolean truncate(int fd, long size) {
        return Files.truncate(fd, size);
    }

    @Override
    public long write(int fd, long addr, long len, long offset) {
        return Files.write(fd, addr, len, offset);
    }
}
