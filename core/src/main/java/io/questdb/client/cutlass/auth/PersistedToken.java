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
 * An immutable snapshot of the token state an {@link OidcDeviceAuth} holds, passed to and from a
 * {@link TokenStore} so the device flow does not have to be re-run after a process restart. Mirrors
 * the in-memory fields: the access token, the id token, the refresh token, the absolute wall-clock
 * expiry of the access/id token, and the (clamped) lifetime that expiry was derived from.
 * <p>
 * Any of the three token strings may be {@code null}. {@link #getExpiresAtMillis()} is an absolute
 * {@code System.currentTimeMillis()} value, so it remains meaningful across a restart (unlike a
 * monotonic clock reading).
 */
public final class PersistedToken {
    private final String accessToken;
    private final long expiresAtMillis;
    private final String idToken;
    private final String refreshToken;
    private final long tokenTtlMillis;

    public PersistedToken(String accessToken, String idToken, String refreshToken, long expiresAtMillis, long tokenTtlMillis) {
        this.accessToken = accessToken;
        this.idToken = idToken;
        this.refreshToken = refreshToken;
        this.expiresAtMillis = expiresAtMillis;
        this.tokenTtlMillis = tokenTtlMillis;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public long getExpiresAtMillis() {
        return expiresAtMillis;
    }

    public String getIdToken() {
        return idToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public long getTokenTtlMillis() {
        return tokenTtlMillis;
    }
}
