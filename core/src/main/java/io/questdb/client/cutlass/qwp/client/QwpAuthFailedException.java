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

import io.questdb.client.cutlass.auth.CredentialRedaction;
import io.questdb.client.cutlass.http.BearerChallenge;
import io.questdb.client.cutlass.http.client.HttpClientException;

/**
 * WebSocket upgrade rejected with {@code 401} or {@code 403}: the {@code auth-rejected} failure class of the
 * dynamic-credential specification (design/qwp-token-provider-spec.md, section 8.1). The message starts with
 * {@code auth-rejected} so every report of it names the class.
 * <p>
 * A credential is uniformly accepted or rejected across a cluster, so the connect walk stops at the first such
 * rejection instead of trying the remaining endpoints. What happens next depends on the phase (section 8.3):
 * startup fails, an established store-and-forward sender keeps retrying, an orphan drainer rides a rotating
 * credential out before quarantining, and a query fails. Before any of that, a {@code 401} against a dynamic
 * credential earns one same-endpoint retry with a refreshed token (section 8.2) - see
 * {@link #isTokenRefreshable()}. Path mismatches ({@code 404}) are NOT routed through this exception because a
 * single misconfigured node mid-deploy can return 404 while peers are healthy.
 */
public final class QwpAuthFailedException extends HttpClientException {
    private static final int MAX_BEARER_ERROR_LENGTH = 64;
    private final String bearerError;
    private final String host;
    private final int port;
    private final int statusCode;

    public QwpAuthFailedException(int statusCode, String host, int port) {
        this(statusCode, host, port, null);
    }

    /**
     * @param wwwAuthenticate the {@code WWW-Authenticate} header value(s) of the rejection, or null
     */
    public QwpAuthFailedException(int statusCode, String host, int port, String wwwAuthenticate) {
        super("auth-rejected: WebSocket upgrade rejected with HTTP ");
        put(statusCode);
        String error = BearerChallenge.bearerError(wwwAuthenticate);
        if (error != null) {
            error = CredentialRedaction.sanitizeErrorText(error);
            if (error.length() > MAX_BEARER_ERROR_LENGTH) {
                error = error.substring(0, MAX_BEARER_ERROR_LENGTH);
            }
            put(" [error=").put(error).put(']');
        }
        put(" for ").put(host).put(':').put(port);
        this.statusCode = statusCode;
        this.host = host;
        this.port = port;
        this.bearerError = error;
    }

    /**
     * The {@code error} parameter of the rejection's {@code WWW-Authenticate: Bearer} challenge (for example
     * {@code invalid_token} or {@code insufficient_scope}), sanitized, or null when the server sent none.
     */
    public String getBearerError() {
        return bearerError;
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

    /**
     * Whether a new token can help: the status is {@code 401}, and the server either sent no {@code Bearer}
     * challenge with an {@code error} or sent {@code error="invalid_token"} (RFC 6750, section 3.1). A
     * {@code 403} never qualifies - it is an authorization decision a new token does not change (decision D2).
     */
    public boolean isTokenRefreshable() {
        return statusCode == 401 && (bearerError == null || BearerChallenge.INVALID_TOKEN.equals(bearerError));
    }
}
