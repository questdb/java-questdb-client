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

import io.questdb.client.cutlass.line.LineSenderException;

/**
 * Signals that the client could not OBTAIN an Authorization credential for a
 * (re)connect handshake: the configured {@code httpTokenProvider} threw instead of
 * returning a token -- a failed silent refresh, or no sign-in yet.
 * <p>
 * Distinct from {@link QwpAuthFailedException}, which means the server rejected a
 * credential the client did present (a terminal auth failure). A credential the client
 * cannot ACQUIRE is instead handled by connection phase, exactly like a transport outage:
 * the RUNNING store-and-forward drainer retries it indefinitely with capped backoff under
 * Invariant B -- the IdP becomes reachable again, or the user completes an interactive
 * sign-in -- holding the un-acked rows in SF meanwhile, and NEVER bounds it by
 * {@code reconnectMaxDurationMillis} nor latches a terminal (either would drop a producer
 * store-and-forward promised to keep alive). Only the foreground/SYNC initial connect
 * fails fast, because a connectivity error is the caller's to see during initialization,
 * not after the drainer is running.
 * <p>
 * It exists so the send loop can tell "the provider failed" apart from "the network
 * failed", and it carries the provider's own exception so a handler can surface that
 * instead of this wrapper.
 * <p>
 * <b>Where a caller can meet it.</b> Not from the ordinary sender API: no path out of
 * {@code build()}, {@code flush()} or any row call delivers this type. The foreground
 * connects - SYNC in {@code CursorWebSocketSendLoop.connectWithRetry}, and the OFF-mode
 * connect in {@code QwpWebSocketSender} - both catch it and rethrow
 * {@link #providerFailure()}, so a token-provider failure reaches the caller as the
 * provider's own exception; the running background drainer catches it and retries under
 * the invariant above. It is public because both of those packages handle it, and
 * because {@code QwpWebSocketSender.newReconnectFactory()} is public: a caller that
 * drives {@code ReconnectFactory.reconnect()} itself runs the endpoint walk directly and
 * so can receive this type unwrapped. Such a caller should treat it as the provider
 * having failed rather than the cluster, and unwrap it with {@link #providerFailure()}
 * the way the two foreground paths do.
 */
public class QwpCredentialUnavailableException extends LineSenderException {
    private final RuntimeException providerFailure;

    public QwpCredentialUnavailableException(RuntimeException providerFailure) {
        super(providerFailure.getMessage() == null
                ? "token provider failed to supply a credential"
                : providerFailure.getMessage(), providerFailure);
        this.providerFailure = providerFailure;
    }

    /**
     * The exception the token provider threw, for a caller that must surface the
     * provider's own error rather than this wrapper. Never null: the wrapper is only
     * ever constructed around a provider failure.
     */
    public RuntimeException providerFailure() {
        return providerFailure;
    }
}
