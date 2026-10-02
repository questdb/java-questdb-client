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

import io.questdb.client.cutlass.auth.TokenUnavailableException;
import io.questdb.client.cutlass.line.LineSenderException;

/**
 * Signals that the client could not OBTAIN an Authorization credential for a
 * (re)connect handshake: the configured {@code httpTokenProvider} threw instead of
 * returning a token -- a failed silent refresh, or no sign-in yet. This is the
 * {@code credential-unavailable} failure class of the dynamic-credential specification
 * (design/qwp-token-provider-spec.md, section 8.1).
 * <p>
 * Distinct from {@link QwpAuthFailedException}, which means the server rejected a
 * credential the client did present. A credential the client cannot ACQUIRE is instead
 * handled by connection phase, exactly like a transport outage:
 * the RUNNING store-and-forward drainer retries it indefinitely with capped backoff under
 * Invariant B -- the IdP becomes reachable again, or the user completes an interactive
 * sign-in -- holding the un-acked rows in SF meanwhile, and NEVER bounds it by
 * {@code reconnectMaxDurationMillis} nor latches a terminal (either would drop a producer
 * store-and-forward promised to keep alive). During initialization the classification
 * decides: {@link #isRetryable()} is true only when the provider threw a
 * {@link TokenUnavailableException} marked retryable, which a SYNC initial connect keeps
 * retrying within its budget; any other provider failure is permanent and fails startup
 * fast (decision D8), as does any failure on an OFF initial connect.
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
 * the invariant above. The query client ({@code QwpQueryClient}) does throw it from
 * {@code connect()}, with a message that starts with {@code credential-unavailable}. It is
 * public because both of those packages handle it, and
 * because {@code QwpWebSocketSender.newReconnectFactory()} is public: a caller that
 * drives {@code ReconnectFactory.reconnect()} itself runs the endpoint walk directly and
 * so can receive this type unwrapped. Such a caller should treat it as the provider
 * having failed rather than the cluster, and unwrap it with {@link #providerFailure()}
 * the way the two foreground paths do.
 */
public class QwpCredentialUnavailableException extends LineSenderException {
    private final RuntimeException providerFailure;

    public QwpCredentialUnavailableException(RuntimeException providerFailure) {
        this(providerFailure.getMessage() == null
                ? "token provider failed to supply a credential"
                : providerFailure.getMessage(), providerFailure);
    }

    /**
     * @param message         the message, which must not contain a token
     * @param providerFailure the exception the token provider threw
     */
    public QwpCredentialUnavailableException(String message, RuntimeException providerFailure) {
        super(message, providerFailure);
        this.providerFailure = providerFailure;
    }

    /**
     * True only when the provider threw a {@link TokenUnavailableException} marked retryable. Any other
     * exception from a provider counts as a permanent credential failure (decision D8), so that startup fails
     * fast, as it does for an OIDC device-flow provider that is not signed in yet.
     */
    @Override
    public boolean isRetryable() {
        return providerFailure instanceof TokenUnavailableException && ((TokenUnavailableException) providerFailure).isRetryable();
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
