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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.http.client.WebSocketClient;

/**
 * Re-establish a fresh WebSocket connection. Used by {@link WebSocketSendQueue}
 * in store-and-forward mode when the current connection drops; the queue calls
 * {@link #reconnect()} from the I/O thread, gets back a connected, upgraded
 * client, and resumes sending (after replaying SF state).
 * <p>
 * Implementations must:
 * <ul>
 *   <li>Close the previous client (if any).</li>
 *   <li>Construct a fresh client with the same configuration (host/port/TLS/auth).</li>
 *   <li>Run the handshake / WebSocket upgrade.</li>
 *   <li>Reset client-side per-connection state (e.g. schema id cache).</li>
 *   <li>Return the connected client.</li>
 * </ul>
 * Throwing from {@code reconnect()} is recoverable — the caller will sleep and
 * retry. Connection-fatal errors (auth failure, protocol mismatch) should still
 * be thrown; classification of fatal vs recoverable is the caller's job.
 */
public interface Reconnector {
    WebSocketClient reconnect() throws Exception;
}
