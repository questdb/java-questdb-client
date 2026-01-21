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
import io.questdb.std.str.LPSZ;
import org.slf4j.Logger;

public class NetworkFacadeImpl implements NetworkFacade {
    public static final NetworkFacade INSTANCE = new NetworkFacadeImpl();

    public int close(int fd) {
        return Net.close(fd);
    }

    public void close(int fd, Logger logger) {
        if (close(fd) != 0) {
            logger.error("could not close [fd={}, errno={}]", fd, errno());
        }
    }

    @Override
    public void configureKeepAlive(int fd) {
        Net.configureKeepAlive(fd);
    }

    @Override
    public int configureNonBlocking(int fd) {
        return Net.configureNonBlocking(fd);
    }

    @Override
    public int connect(int fd, long pSockaddr) {
        return Net.connect(fd, pSockaddr);
    }

    @Override
    public int connectAddrInfo(int fd, long pAddrInfo) {
        return Net.connectAddrInfo(fd, pAddrInfo);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public void freeAddrInfo(long pAddrInfo) {
        Net.freeAddrInfo(pAddrInfo);
    }

    @Override
    public void freeSockAddr(long pSockaddr) {
        Net.freeSockAddr(pSockaddr);
    }

    @Override
    public long getAddrInfo(CharSequence host, int port) {
        return Net.getAddrInfo(host, port);
    }

    @Override
    public int getSndBuf(int fd) {
        return Net.getSndBuf(fd);
    }

    @Override
    public int recvRaw(int fd, long buffer, int bufferLen) {
        return Net.recv(fd, buffer, bufferLen);
    }

    @Override
    public int sendRaw(int fd, long buffer, int bufferLen) {
        return Net.send(fd, buffer, bufferLen);
    }

    @Override
    public boolean setSndBuf(int fd, int size) {
        return Net.setSndBuf(fd, size) == 0;
    }

    @Override
    public int setTcpNoDelay(int fd, boolean noDelay) {
        return Net.setTcpNoDelay(fd, noDelay);
    }

    @Override
    public long sockaddr(int address, int port) {
        return Net.sockaddr(address, port);
    }

    @Override
    public int socketTcp(boolean blocking) {
        return Net.socketTcp(blocking);
    }

    /**
     * Return true if a disconnect happened, false otherwise.
     **/
    @Override
    public boolean testConnection(int fd, long buffer, int bufferSize) {
        if (fd == -1) {
            return true;
        }

        final int nRead = Net.peek(fd, buffer, bufferSize);
        return nRead < 0;
    }
}