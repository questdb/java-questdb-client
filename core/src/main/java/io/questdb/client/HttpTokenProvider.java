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

/**
 * Supplies an HTTP authentication token to a {@link Sender} on demand, so a provider returning a
 * freshly refreshed token - e.g. {@code OidcDeviceAuth::getTokenSilently} - keeps a long-lived sender
 * authenticated as the token rotates, without rebuilding it. Over HTTP the sender calls
 * {@link #getToken()} as it builds each request; over WebSocket it calls it once per connection
 * handshake, on the initial connect and again on every reconnect.
 * <p>
 * {@link #getToken()} runs on the sender's flush and reconnect paths: it must return promptly and must
 * not block on interactive input. A quick silent token refresh is fine, but it must not start an
 * interactive sign-in. An exception from {@link #getToken()} fails the in-flight flush (HTTP) or the
 * connection attempt (WebSocket).
 *
 * @see Sender.LineSenderBuilder#httpTokenProvider(HttpTokenProvider)
 */
@FunctionalInterface
public interface HttpTokenProvider {
    /**
     * Returns the current HTTP authentication token, without the {@code "Bearer "} prefix (the sender
     * adds it). Must not return null or empty.
     *
     * @return the current HTTP authentication token
     */
    CharSequence getToken();
}
