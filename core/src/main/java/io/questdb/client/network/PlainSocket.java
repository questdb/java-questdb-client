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

package io.questdb.client.network;

import org.slf4j.Logger;

public class PlainSocket implements Socket {
    private final Logger log;
    private final NetworkFacade nf;
    private int fd = -1;

    public PlainSocket(NetworkFacade nf, Logger log) {
        this.nf = nf;
        this.log = log;
    }

    @Override
    public void close() {
        if (fd != -1) {
            nf.close(fd, log);
            fd = -1;
        }
    }

    @Override
    public int getFd() {
        return fd;
    }

    @Override
    public boolean isClosed() {
        return fd == -1;
    }

    @Override
    public void of(int fd) {
        assert this.fd == -1;
        this.fd = fd;
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        return nf.recvRaw(fd, bufferPtr, bufferLen);
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        return nf.sendRaw(fd, bufferPtr, bufferLen);
    }

    @Override
    public void startTlsSession(CharSequence peerName) throws TlsSessionInitFailedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsTls() {
        return false;
    }

    @Override
    public int tlsIO(int readinessFlags) {
        return 0;
    }

    @Override
    public boolean wantsTlsWrite() {
        return false;
    }
}