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

package io.questdb.client.cutlass.auth;

/**
 * Obtains a new bearer token from an identity platform - Microsoft Entra ID, an OAuth client-credentials
 * endpoint, a secrets manager - together with its expiry. A {@link RefreshingTokenProvider} wraps a source and
 * caches its tokens, refreshing them in the background before they expire. This is the token-source contract of
 * the dynamic-credential specification (design/qwp-token-provider-spec.md, section 4).
 * <p>
 * Threading. The provider calls {@link #fetchToken()} only from its own background refresher thread, never from
 * a connection thread, and never concurrently with itself. The call may block, but it should bound its own
 * network operations; 30 seconds per attempt is recommended. A provider being closed interrupts its refresher,
 * so a source blocked in an interruptible wait should let the interrupt end it.
 * <p>
 * Failures. Throw {@link TokenUnavailableException} to classify a failure:
 * <ul>
 *   <li>retryable - network failures, timeouts, HTTP 429, HTTP 5xx, and IMDS 404 or 410. Pass the platform's
 *   {@code Retry-After}, when it gave one, as {@code retryAfterMillis};</li>
 *   <li>permanent - configuration that is missing or wrong: no credential configured, identity not found,
 *   invalid client.</li>
 * </ul>
 * When unsure, report the failure as retryable. Any other exception is treated as retryable. The provider keeps
 * retrying either way - an operator can repair a "permanent" failure, such as a missing role assignment,
 * without restarting the application - but the classification decides how a caller waiting for a token reacts.
 * <p>
 * Secrets. A failure message must not contain a token or a raw response body: report only the shape of a
 * problem, such as a missing or invalid field. Never attach an object that serializes a raw HTTP request, such
 * as a client-credentials body carrying a {@code client_secret}, to an exception.
 */
@FunctionalInterface
public interface TokenSource {

    /**
     * Obtains a new token from the identity platform.
     *
     * @return the token and its expiry; never null
     * @throws TokenUnavailableException to report a classified failure; any other exception counts as retryable
     */
    ExpiringToken fetchToken();
}
