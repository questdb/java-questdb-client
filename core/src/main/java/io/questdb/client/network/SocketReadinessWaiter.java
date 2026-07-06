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

package io.questdb.client.network;

/**
 * Blocks until a non-blocking socket is ready for a given I/O operation, or
 * throws a timeout-flagged exception once the caller's deadline passes.
 * <p>
 * Used to drive the TLS handshake off the client's event loop: instead of
 * busy-spinning on a non-blocking socket that returns "would block", the
 * handshake hands control to this waiter, which parks on epoll/kqueue/select
 * with the remaining connect budget. This bounds the handshake by the same
 * deadline as the TCP connect and keeps a stalled peer from pinning a CPU.
 */
@FunctionalInterface
public interface SocketReadinessWaiter {
    /**
     * Blocks until the socket is ready for {@code ioOperation}, or throws a
     * timeout-flagged exception when the connect deadline is exceeded.
     *
     * @param ioOperation {@link IOOperation#READ} or {@link IOOperation#WRITE}
     */
    void awaitReady(int ioOperation);
}
