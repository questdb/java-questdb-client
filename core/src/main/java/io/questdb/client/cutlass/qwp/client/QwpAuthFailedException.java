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
 * WebSocket upgrade rejected with {@code 401} or {@code 403}. Terminal across all
 * configured endpoints: a rejected credential is uniformly rejected across the
 * cluster, so failing fast surfaces the configuration error immediately. Path
 * mismatches ({@code 404}) are NOT routed through this exception because a single
 * misconfigured node mid-deploy can return 404 while peers are healthy.
 */
public final class QwpAuthFailedException extends HttpClientException {
    private final String host;
    private final int port;
    private final int statusCode;

    public QwpAuthFailedException(int statusCode, String host, int port) {
        super("WebSocket upgrade rejected with HTTP ");
        put(statusCode).put(" for ").put(host).put(':').put(port);
        this.statusCode = statusCode;
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
