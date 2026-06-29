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

package io.questdb.client;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.Chars;

/**
 * Supplies an HTTP authentication token to a {@link Sender} on demand, so a provider returning a
 * freshly refreshed token - e.g. {@code OidcDeviceAuth::getToken} - keeps a long-lived sender
 * authenticated as the token rotates, without rebuilding it. Over HTTP the sender calls
 * {@link #getToken()} as it builds each request; over WebSocket it calls it once per connection
 * handshake, on the initial connect and again on every reconnect.
 * <p>
 * {@link #getToken()} runs on the sender's flush and reconnect paths: it must return promptly and must
 * not block on interactive input. A quick silent token refresh is fine, but it must not start an
 * interactive sign-in; a provider that coordinates a shared token store across processes (for example
 * {@code OidcDeviceAuth} with a {@code FileTokenStore}) may add a brief, bounded wait to acquire that
 * store's cross-process lock before such a refresh, which still counts as a quick silent refresh. An
 * exception from {@link #getToken()} fails the in-flight flush (HTTP) or the connection attempt (WebSocket).
 *
 * @see Sender.LineSenderBuilder#httpTokenProvider(HttpTokenProvider)
 */
@FunctionalInterface
public interface HttpTokenProvider {
    /**
     * Validates a token returned by {@link #getToken()} before the sender writes it into an
     * {@code Authorization: Bearer} header. Rejects a null, empty or blank token, and any token
     * carrying a control or non-ASCII character (outside {@code 0x20}-{@code 0x7e}): a real bearer
     * token is printable ASCII, so a stray CR/LF (which would inject into the HTTP request line) or a
     * non-ASCII byte (silently truncated to one byte by the ASCII header writer, yielding a corrupt
     * credential the server only answers with 401) is refused rather than sent. The token itself is
     * never placed in the exception message - it is the secret this guards.
     *
     * @param token the token returned by a provider
     * @throws LineSenderException if the token is null, empty, blank, or carries a control or
     *                             non-ASCII character
     */
    static void validateToken(CharSequence token) {
        if (Chars.isBlank(token)) {
            throw new LineSenderException("token provider returned a null or empty token");
        }
        for (int i = 0, n = token.length(); i < n; i++) {
            char c = token.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                throw new LineSenderException("token provider returned a token containing a control or non-ASCII character; refusing to send it as a credential");
            }
        }
    }

    /**
     * Returns the current HTTP authentication token, without the {@code "Bearer "} prefix (the sender
     * adds it). Must not return null or empty, and must contain only printable ASCII (no control or
     * non-ASCII characters) - the sender splices the value verbatim into an {@code Authorization:
     * Bearer} header and rejects a token that violates this (see {@link #validateToken(CharSequence)}).
     *
     * @return the current HTTP authentication token
     */
    CharSequence getToken();
}
