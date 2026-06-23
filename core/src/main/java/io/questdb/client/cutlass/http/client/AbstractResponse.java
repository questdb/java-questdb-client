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

package io.questdb.client.cutlass.http.client;

public abstract class AbstractResponse implements Response, Fragment {
    private final long bufHi;
    private final long bufLo;
    private final int defaultTimeout;
    private long bytesReceived;
    private long contentLength;
    private long dataHi;
    private long dataLo;
    private boolean receive = true;

    public AbstractResponse(long bufLo, long bufHi, int defaultTimeout) {
        this.bufLo = bufLo;
        this.bufHi = bufHi;
        this.defaultTimeout = defaultTimeout;
    }

    public void begin(long lo, long hi, long contentLength) {
        this.dataLo = lo;
        this.dataHi = hi;
        this.contentLength = contentLength;
        this.bytesReceived = 0;
        this.receive = (lo == hi);
    }

    @Override
    public long hi() {
        return dataHi;
    }

    @Override
    public long lo() {
        return dataLo;
    }

    public Fragment recv(int timeout) {
        if (bytesReceived >= contentLength) {
            return null;
        }
        if (receive) {
            dataLo = bufLo;
            dataHi = bufLo;
            // A positive timeout bounds the whole call, not each socket read. recvOrDie can return 0
            // without consuming the full timeout - e.g. an incomplete TLS record that decrypts to no
            // application bytes - so without one shared deadline a server dribbling such reads would
            // re-arm the full timeout on every iteration and keep this recv() running without bound,
            // defeating a caller's wall-clock bound (e.g. OidcDeviceAuth.parseBody). A non-positive
            // timeout keeps the legacy "no bound" behaviour.
            final boolean bounded = timeout > 0;
            final long startNanos = bounded ? System.nanoTime() : 0L;
            int len = 0;
            while (len == 0) {
                int callTimeout = timeout;
                if (bounded) {
                    callTimeout = timeout - (int) ((System.nanoTime() - startNanos) / 1_000_000L);
                    if (callTimeout <= 0) {
                        throw new HttpClientException("timed out reading the response body");
                    }
                }
                len = recvOrDie(dataHi, bufHi, callTimeout);
            }
            dataHi += len;
        }
        bytesReceived += dataHi - dataLo;
        receive = true;
        return this;
    }

    @Override
    public Fragment recv() {
        return recv(defaultTimeout);
    }

    protected abstract int recvOrDie(long bufLo, long bufHi, int timeout);
}