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

import org.slf4j.Logger;

public interface NetworkFacade {

    void close(int fd, Logger logger);

    void configureKeepAlive(int fd);

    int configureNonBlocking(int fd);

    int connect(int fd, long pSockaddr);

    int connectAddrInfo(int fd, long pAddrInfo);

    int errno();

    void freeAddrInfo(long pAddrInfo);

    void freeSockAddr(long pSockaddr);

    long getAddrInfo(CharSequence host, int port);

    int getSndBuf(int fd);

    int recvRaw(int fd, long buffer, int bufferLen);

    int sendRaw(int fd, long buffer, int bufferLen);

    boolean setSndBuf(int fd, int size);

    int setTcpNoDelay(int fd, boolean noDelay);

    long sockaddr(int address, int port);

    int socketTcp(boolean blocking);

    /**
     * Returns true if a disconnect happened, false otherwise.
     *
     * @param fd         file descriptor
     * @param buffer     test buffer
     * @param bufferSize test buffer size
     * @return true if a disconnect happened, false otherwise
     */
    boolean testConnection(int fd, long buffer, int bufferSize);
}