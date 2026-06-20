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

package io.questdb.client.impl;

/**
 * Daemon thread that periodically asks both pools to reap idle / over-age
 * slots. Owned by {@link QuestDBImpl}; one instance per {@code QuestDB}
 * handle.
 */
final class PoolHousekeeper {

    private final long intervalMillis;
    private final QueryClientPool queryPool;
    private final SenderPool senderPool;
    private final Object signalLock = new Object();
    private final Thread thread;
    private volatile boolean stop;

    PoolHousekeeper(SenderPool senderPool, QueryClientPool queryPool, long intervalMillis) {
        this.senderPool = senderPool;
        this.queryPool = queryPool;
        this.intervalMillis = intervalMillis;
        this.thread = new Thread(this::runLoop, "questdb-pool-housekeeper");
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    void stop() {
        stop = true;
        synchronized (signalLock) {
            signalLock.notifyAll();
        }
        try {
            thread.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runLoop() {
        while (!stop) {
            synchronized (signalLock) {
                if (stop) {
                    return;
                }
                try {
                    signalLock.wait(intervalMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (stop) {
                return;
            }
            try {
                senderPool.reapIdle();
            } catch (Throwable ignored) {
                // Defensive, intentionally unreachable in normal operation:
                // SenderPool.reapIdle() already swallows per-delegate close()
                // failures internally. The outer catch is a belt-and-braces
                // guard. Reaping must not propagate -- it's best-effort
                // housekeeping. Catch Throwable (not just RuntimeException) so
                // an Error from a delegate teardown can never kill this daemon
                // thread and stop all future reaping for the life of the handle.
            }
            try {
                queryPool.reapIdle();
            } catch (Throwable ignored) {
                // Same rationale as the senderPool guard above: best-effort,
                // must never propagate, and Throwable (not RuntimeException) so
                // an Error from query-client teardown cannot kill the daemon.
            }
        }
    }
}
