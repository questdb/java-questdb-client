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
 * This is an internal marker that carries the provider's own exception: it exists so
 * the send loop can tell "the provider failed" apart from "the network failed". A
 * foreground connect unwraps it via {@link #providerFailure()} and rethrows the
 * provider's exception, so {@code build()} surfaces the provider's error directly.
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
     * provider's own error instead of this marker.
     */
    public RuntimeException providerFailure() {
        return providerFailure;
    }
}
