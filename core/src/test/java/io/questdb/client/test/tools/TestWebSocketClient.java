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

package io.questdb.client.test.tools;

import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.network.SocketFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Base for {@link WebSocketClient} test doubles without a real socket. The
 * idle wait honours {@link #wake()} like the real transport, but also returns
 * after at most {@link #MAX_PARK_NANOS} so frames a scripted
 * {@code tryReceiveFrame} produces "spontaneously" are still picked up. The
 * bounded park is test-only; production blocks in the OS wait.
 */
public abstract class TestWebSocketClient extends WebSocketClient {
    public static final long MAX_PARK_NANOS = 1_000_000L;
    private final AtomicBoolean permit = new AtomicBoolean();
    private volatile Thread waiter;

    protected TestWebSocketClient(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        super(configuration, socketFactory);
    }

    @Override
    public boolean awaitReadableOrWake(int timeoutMillis) {
        if (permit.getAndSet(false)) {
            return true;
        }
        long parkNanos = MAX_PARK_NANOS;
        if (timeoutMillis >= 0) {
            parkNanos = Math.min(parkNanos, timeoutMillis * 1_000_000L);
        }
        waiter = Thread.currentThread();
        try {
            LockSupport.parkNanos(this, parkNanos);
        } finally {
            waiter = null;
        }
        return permit.getAndSet(false);
    }

    @Override
    public void wake() {
        permit.set(true);
        Thread t = waiter;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }
}
