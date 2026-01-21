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

package io.questdb.network;

import io.questdb.KqueueAccessor;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public final class Kqueue implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Kqueue.class);

    private final int bufferSize;
    private final int capacity;
    private final long changeList;
    private final long eventList;
    private final int kq;
    private final KqueueFacade kqf;

    private long writeAddress;

    public Kqueue(KqueueFacade kqf, int capacity) {
        try {
            this.kqf = kqf;
            this.capacity = capacity;
            this.bufferSize = KqueueAccessor.SIZEOF_KEVENT * capacity;
            this.changeList = this.writeAddress = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            this.eventList = Unsafe.calloc(bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            this.kq = kqf.kqueue();
            if (kq < 0) {
                throw NetworkError.instance(kqf.getNetworkFacade().errno(), "could not create kqueue");
            }
        } catch (Throwable t) {
            close();
            throw t;
        }
    }

    @Override
    public void close() {
        kqf.getNetworkFacade().close(kq, LOG);
        Unsafe.free(this.changeList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        Unsafe.free(this.eventList, bufferSize, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
    }

    public int poll(int timeout) {
        return kqf.kevent(kq, 0, 0, eventList, capacity, timeout);
    }

    public void readFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_READ);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FLAGS_OFFSET, (short) (KqueueAccessor.EV_ADD | KqueueAccessor.EV_ONESHOT));
    }

    public int register(int n) {
        return kqf.kevent(kq, changeList, n, 0, 0, 0);
    }

    public void setWriteOffset(int offset) {
        this.writeAddress = changeList + offset;
    }

    public void writeFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_WRITE);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FLAGS_OFFSET, (short) (KqueueAccessor.EV_ADD | KqueueAccessor.EV_ONESHOT));
    }

    private void commonFd(int fd, long data) {
        Unsafe.getUnsafe().putInt(writeAddress + KqueueAccessor.FD_OFFSET, fd);
        Unsafe.getUnsafe().putLong(writeAddress + KqueueAccessor.DATA_OFFSET, data);
    }
}