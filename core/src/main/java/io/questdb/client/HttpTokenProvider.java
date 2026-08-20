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
 * Supplies an HTTP authentication token to a {@link Sender} or pooled {@link QuestDB} connection on
 * demand, so a provider returning a freshly refreshed token - e.g. {@code OidcDeviceAuth::getToken}
 * - keeps long-lived ingest and query connections authenticated as the token rotates, without
 * rebuilding them. An HTTP sender calls {@link #getToken()} as it builds each request; WebSocket
 * ingest and query clients call it once per connection handshake, on the initial connect and again
 * on every reconnect.
 * <p>
 * {@link #getToken()} runs on HTTP flush and pooled connection/reconnection paths. Different pooled
 * connections may call it concurrently, so implementations must be thread-safe. It must return
 * promptly and must not block on interactive input. A quick silent token refresh is fine, but it must
 * not start an interactive sign-in; a provider that coordinates a shared token store across processes
 * (for example {@code OidcDeviceAuth} with a {@code FileTokenStore}) may add a brief, bounded wait to
 * acquire that store's cross-process lock before such a refresh, which still counts as a quick silent
 * refresh. Note that "quick" bounds the interactive wait, not the network: the silent refresh is a
 * synchronous HTTP round-trip to the token endpoint, and its connection phase (DNS, TCP connect, TLS)
 * is bounded by the OS, not by the client timeout - so a black-holed token endpoint can stall a refresh
 * for the OS connect timeout (commonly ~2 minutes on Linux). A producer sizing flush backpressure
 * against this call should expect that worst case. An exception from {@link #getToken()} fails the
 * in-flight flush (HTTP) or the connection attempt (WebSocket).
 *
 * @see QuestDB#connect(CharSequence, HttpTokenProvider)
 * @see QuestDBBuilder#httpTokenProvider(HttpTokenProvider)
 * @see Sender.LineSenderBuilder#httpTokenProvider(HttpTokenProvider)
 */
@FunctionalInterface
public interface HttpTokenProvider {
    /**
     * Validates a token returned by {@link #getToken()} before the client writes it into an
     * {@code Authorization: Bearer} header.
     * <p>
     * Callers must pass a value that cannot change between this check and the write that follows it.
     * {@code getToken()} may return a reused buffer, so validating the provider's sequence and then
     * re-reading it to build the header reads it twice: a mutation in between passes the check and
     * splices the mutated bytes - a CR/LF among them - into the header. Snapshot with
     * {@link Object#toString()} first, then validate and send the snapshot. Every call site in this
     * library does.
     * <p>
     * Rejects a null, empty or blank token, and any token
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
     * Returns the current HTTP authentication token, without the {@code "Bearer "} prefix (the client
     * adds it). Must not return null or empty, and must contain only printable ASCII (no control or
     * non-ASCII characters) - the client splices the value verbatim into an {@code Authorization:
     * Bearer} header and rejects a token that violates this (see {@link #validateToken(CharSequence)}).
     * <p>
     * Returning a reused, mutable {@link CharSequence} - the idiomatic zero-allocation style - is
     * supported and expected: the client re-validates every pulled token rather than trusting instance
     * identity, so a buffer whose contents changed since the last call is checked again. What an
     * implementation must not do is mutate a sequence it has already returned <i>while the client is
     * still reading it</i>. The client snapshots each returned value before validating it, so a
     * concurrent mutation cannot slip past the check into the header; an implementation that mutates
     * mid-call is nonetheless racing with a reader and may see its own token dropped for the one the
     * snapshot captured. Mutate between calls, not during one.
     *
     * @return the current HTTP authentication token
     */
    CharSequence getToken();
}
