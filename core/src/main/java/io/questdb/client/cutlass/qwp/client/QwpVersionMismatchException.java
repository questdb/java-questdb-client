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

import io.questdb.client.cutlass.http.client.HttpClientException;

/**
 * Server completed the WebSocket upgrade (HTTP 101) but advertised an
 * {@code X-QWP-Version} outside the client's supported range. Treated as
 * transient at every layer per sf-client.md section 13.3: the per-endpoint
 * round walks to the next host (rolling upgrade can leave one node ahead of
 * or behind its peers). The background reconnect loop retries a full round
 * of mismatches indefinitely (Invariant B: no wall-clock give-up); the
 * blocking (sync) initial connect consumes its retry budget and surfaces a
 * {@code LineSenderException} from {@code fromConfig} on exhaustion. Never
 * classified as {@code SECURITY_ERROR}.
 */
public final class QwpVersionMismatchException extends HttpClientException {
    public QwpVersionMismatchException(int serverVersion, int clientMaxVersion) {
        super("WebSocket upgrade failed: server advertised unsupported QWP version ");
        put(serverVersion).put(" [client max=").put(clientMaxVersion).put(']');
    }
}
