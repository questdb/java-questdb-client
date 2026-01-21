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

import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public final class Net {

    @SuppressWarnings("unused")
    public static final int EOTHERDISCONNECT = -2;
    @SuppressWarnings("unused")
    public static final int EPEERDISCONNECT = -1;
    @SuppressWarnings("unused")
    public static final int ERETRY = 0;
    public static final int EWOULDBLOCK;
    public static final long MMSGHDR_BUFFER_ADDRESS_OFFSET;
    public static final long MMSGHDR_BUFFER_LENGTH_OFFSET;
    public static final long MMSGHDR_SIZE;
    private static final AtomicInteger ADDR_INFO_COUNTER = new AtomicInteger();
    private static final Logger LOG = LoggerFactory.getLogger(Net.class);
    private static final AtomicInteger SOCK_ADDR_COUNTER = new AtomicInteger();
    // TCP KeepAlive not meant to be configurable. It's a last resort measure to disable/change keepalive if the default
    // value causes problems in some environments. If it does not cause problems then this option should be removed after a few releases.
    // It's not exposed as PropertyKey, because it would become a supported and hard to remove API.
    private static final int TCP_KEEPALIVE_SECONDS = Integer.getInteger("questdb.unsupported.tcp.keepalive.seconds", 30);

    private Net() {
    }

    public static void appendIP4(CharSink<?> sink, long ip) {
        sink.put((ip >> 24) & 0xff).putAscii('.')
                .put((ip >> 16) & 0xff).putAscii('.')
                .put((ip >> 8) & 0xff).putAscii('.')
                .put(ip & 0xff);
    }

    public static int close(int fd) {
        return Files.close(fd);
    }

    public static void configureKeepAlive(int fd) {
        if (TCP_KEEPALIVE_SECONDS < 0 || fd < 0) {
            return;
        }
        if (setKeepAlive0(fd, TCP_KEEPALIVE_SECONDS) < 0) {
            int errno = Os.errno();
            LOG.error("could not set tcp keepalive [fd={}, errno={}]", fd, errno);
        }
    }

    public static void freeAddrInfo(long pAddrInfo) {
        if (pAddrInfo != 0) {
            ADDR_INFO_COUNTER.decrementAndGet();
        }
        freeAddrInfo0(pAddrInfo);
    }

    public static void freeSockAddr(long sockaddr) {
        if (sockaddr != 0) {
            SOCK_ADDR_COUNTER.decrementAndGet();
        }
        freeSockAddr0(sockaddr);
    }

    public static long getAddrInfo(CharSequence host, int port) {
        try (DirectUtf8Sink sink = new DirectUtf8Sink(host.length())) {
            sink.put(host);
            return getAddrInfo(sink, port);
        }
    }

    public static long getAddrInfo(DirectUtf8Sequence host, int port) {
        return getAddrInfo(host.ptr(), port);
    }

    public static long getAddrInfo(long lpszHost, int port) {
        long addrInfo = getAddrInfo0(lpszHost, port);
        if (addrInfo != -1) {
            ADDR_INFO_COUNTER.incrementAndGet();
        }
        return addrInfo;
    }


    public static int send(long fd, long ptr, int len) {
        return send(fd, ptr, len);
    }

    public static long sockaddr(int ipv4address, int port) {
        SOCK_ADDR_COUNTER.incrementAndGet();
        return sockaddr0(ipv4address, port);
    }

    public static native int configureNonBlocking(int fd);

    public native static int connect(int fd, long sockaddr);

    public native static int connectAddrInfo(int fd, long lpAddrInfo);

    private static native void freeAddrInfo0(long pAddrInfo);

    private static native void freeSockAddr0(long sockaddr);

    private static native long getAddrInfo0(long lpszHost, int port);

    private static native int getEwouldblock();

    private static native long getMsgHeaderBufferAddressOffset();

    private static native long getMsgHeaderBufferLengthOffset();

    private static native long getMsgHeaderSize();

    public native static int getSndBuf(int fd);

    public native static boolean join(int fd, int bindIPv4Address, int groupIPv4Address);

    public static native int peek(int fd, long ptr, int len);

    public static native int recv(int fd, long ptr, int len);

    public static native int send(int fd, long ptr, int len);

    public static native int setKeepAlive0(int fd, int seconds);

    public native static int setSndBuf(int fd, int size);

    public native static int setTcpNoDelay(int fd, boolean noDelay);

    public native static long sockaddr0(int ipv4address, int port);

    public native static int socketTcp(boolean blocking);

    static {
        Os.init();
        EWOULDBLOCK = getEwouldblock();
        if (Os.isLinux()) {
            MMSGHDR_SIZE = getMsgHeaderSize();
            MMSGHDR_BUFFER_ADDRESS_OFFSET = getMsgHeaderBufferAddressOffset();
            MMSGHDR_BUFFER_LENGTH_OFFSET = getMsgHeaderBufferLengthOffset();
        } else {
            MMSGHDR_SIZE = -1L;
            MMSGHDR_BUFFER_ADDRESS_OFFSET = -1L;
            MMSGHDR_BUFFER_LENGTH_OFFSET = -1L;
        }
    }
}