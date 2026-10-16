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

import java.time.Instant;

/**
 * A bearer token together with the instant it expires, as returned by a {@link TokenSource}. This is the
 * {@code TokenResult} of the dynamic-credential specification (design/qwp-token-provider-spec.md, section 4).
 * <p>
 * The token is opaque and is stored without the {@code "Bearer "} prefix. It must be non-blank printable ASCII
 * ({@code 0x20}-{@code 0x7e}); the constructors reject anything else, never echoing the token in the message.
 * <p>
 * {@code expiresAtEpochMillis} is an absolute wall-clock instant. A source that receives a relative lifetime
 * (such as {@code expires_in}) should add it to its own local time of receipt, which makes the cache robust to
 * clock skew between the client and the identity platform; a source that cannot determine the expiry must
 * supply a conservative value. {@code refreshAtEpochMillis}, when present, is the earliest instant the platform
 * suggests refreshing; {@link #NO_REFRESH_AT} means none.
 * <p>
 * {@link #toString()} never renders the token: it shows the length, an 8-hex-digit SHA-256 fingerprint and the
 * timestamps.
 */
public final class ExpiringToken {
    /**
     * Value of {@code refreshAtEpochMillis} meaning the platform gave no refresh hint.
     */
    public static final long NO_REFRESH_AT = 0;
    private final long expiresAtEpochMillis;
    private final long refreshAtEpochMillis;
    private final String token;

    /**
     * @param token                the token, without the {@code "Bearer "} prefix
     * @param expiresAtEpochMillis the absolute expiry, in milliseconds since the Unix epoch
     * @throws IllegalArgumentException if the token is null, blank or not printable ASCII
     */
    public ExpiringToken(String token, long expiresAtEpochMillis) {
        this(token, expiresAtEpochMillis, NO_REFRESH_AT);
    }

    /**
     * @param token                the token, without the {@code "Bearer "} prefix
     * @param expiresAtEpochMillis the absolute expiry, in milliseconds since the Unix epoch
     * @param refreshAtEpochMillis the earliest instant the platform suggests refreshing, or
     *                             {@link #NO_REFRESH_AT} (any value {@code <= 0}) for none
     * @throws IllegalArgumentException if the token is null, blank or not printable ASCII
     */
    public ExpiringToken(String token, long expiresAtEpochMillis, long refreshAtEpochMillis) {
        String problem = describeInvalidToken(token);
        if (problem != null) {
            throw new IllegalArgumentException(problem);
        }
        this.token = token;
        this.expiresAtEpochMillis = expiresAtEpochMillis;
        this.refreshAtEpochMillis = refreshAtEpochMillis > 0 ? refreshAtEpochMillis : NO_REFRESH_AT;
    }

    /**
     * Applies the token rules of the specification's section 3: non-null, not blank, and every character within
     * printable ASCII. Returns a token-free description of the first violation, or {@code null} when the token
     * is acceptable.
     */
    static String describeInvalidToken(CharSequence token) {
        if (token == null || token.length() == 0) {
            return "token is null or empty";
        }
        boolean blank = true;
        for (int i = 0, n = token.length(); i < n; i++) {
            char c = token.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                return "token contains a control or non-ASCII character";
            }
            if (c != ' ') {
                blank = false;
            }
        }
        return blank ? "token is blank" : null;
    }

    private static String instant(long epochMillis) {
        try {
            return Instant.ofEpochMilli(epochMillis).toString();
        } catch (RuntimeException e) {
            return Long.toString(epochMillis);
        }
    }

    /**
     * @return the absolute expiry, in milliseconds since the Unix epoch
     */
    public long getExpiresAtEpochMillis() {
        return expiresAtEpochMillis;
    }

    /**
     * @return the platform's earliest suggested refresh instant, or {@link #NO_REFRESH_AT} when none was given
     */
    public long getRefreshAtEpochMillis() {
        return refreshAtEpochMillis;
    }

    /**
     * @return the token, without the {@code "Bearer "} prefix
     */
    public String getToken() {
        return token;
    }

    /**
     * @return true when the platform gave a refresh hint
     */
    public boolean hasRefreshAt() {
        return refreshAtEpochMillis != NO_REFRESH_AT;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ExpiringToken{token=")
                .append(CredentialRedaction.describeToken(token))
                .append(", expiresAt=").append(instant(expiresAtEpochMillis));
        if (hasRefreshAt()) {
            sb.append(", refreshAt=").append(instant(refreshAtEpochMillis));
        }
        return sb.append('}').toString();
    }
}
