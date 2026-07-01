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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The non-secret identity a persisted token belongs to: the client id, the (canonicalised) token and
 * device-authorization endpoints, the scope, the optional audience, and whether the server expects
 * groups encoded in the token. A {@link TokenStore} keys its entries by this so a token minted for one
 * server / identity provider / scope / audience is never served to a process configured for another.
 * <p>
 * {@link #hash()} is a stable, lowercase-hex SHA-256 over a canonical, NUL-separated rendering of the
 * fields - intended as a file name (or any opaque key) that is identical across client implementations
 * (the Python client mirrors this), so several processes - and languages - sharing one identity address
 * the same persisted entry. The fields themselves are exposed (they are not secret) so a store can also
 * record them and re-check them on load as a defence against a hash collision or a copied file.
 */
public final class TokenStoreKey {
    // the canonical-string prefix doubles as a domain tag and a schema version, so a future format change
    // produces a different hash (and hence a different file) rather than silently colliding with v1 entries
    private static final String CANONICAL_PREFIX = "questdb-oidc-token-v1";
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private final String audience;
    private final String clientId;
    private final String deviceAuthorizationEndpoint;
    private final boolean groupsInToken;
    private final String hash;
    private final String scope;
    private final String tokenEndpoint;

    /**
     * @param clientId                     the OIDC client id
     * @param tokenEndpoint                the canonical token endpoint ({@code scheme://host:port/path},
     *                                     scheme and host lower-cased, port explicit)
     * @param deviceAuthorizationEndpoint  the canonical device-authorization endpoint, same form
     * @param scope                        the requested scope
     * @param audience                     the audience, or {@code null} if none
     * @param groupsInToken                whether the id token (rather than the access token) is served
     */
    public TokenStoreKey(
            String clientId,
            String tokenEndpoint,
            String deviceAuthorizationEndpoint,
            String scope,
            String audience,
            boolean groupsInToken
    ) {
        // the identity fields are required; reject a null up front with a clear error rather than letting it
        // surface later as a raw NullPointerException deep inside a TokenStore's serialize/fingerprint path
        if (clientId == null || tokenEndpoint == null || deviceAuthorizationEndpoint == null || scope == null) {
            throw new OidcAuthException(
                    "clientId, tokenEndpoint, deviceAuthorizationEndpoint and scope are required for a token store key");
        }
        this.clientId = clientId;
        this.tokenEndpoint = tokenEndpoint;
        this.deviceAuthorizationEndpoint = deviceAuthorizationEndpoint;
        this.scope = scope;
        // normalise an empty audience to null so getAudience(), hash() (which already folds null and "" together
        // via nullToEmpty), and a TokenStore's save/load round-trip all agree that an absent audience is null -
        // matching how OidcDeviceAuth builds the key
        this.audience = audience != null && !audience.isEmpty() ? audience : null;
        this.groupsInToken = groupsInToken;
        this.hash = computeHash(clientId, tokenEndpoint, deviceAuthorizationEndpoint, scope, this.audience, groupsInToken);
    }

    public String getAudience() {
        return audience;
    }

    public String getClientId() {
        return clientId;
    }

    public String getDeviceAuthorizationEndpoint() {
        return deviceAuthorizationEndpoint;
    }

    public String getScope() {
        return scope;
    }

    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    /**
     * @return a stable lowercase-hex SHA-256 of the canonical identity string; suitable as an opaque file
     * name. Identical inputs (across processes and language implementations) yield an identical hash.
     */
    public String hash() {
        return hash;
    }

    public boolean isGroupsInToken() {
        return groupsInToken;
    }

    private static String computeHash(
            String clientId,
            String tokenEndpoint,
            String deviceAuthorizationEndpoint,
            String scope,
            String audience,
            boolean groupsInToken
    ) {
        // NUL-separate the fields so no field value can be confused with a separator; an OAuth client id,
        // url, scope or audience never contains a NUL. The prefix tags the domain and schema version.
        StringBuilder canonical = new StringBuilder();
        canonical.append(CANONICAL_PREFIX).append('\0')
                .append(nullToEmpty(clientId)).append('\0')
                .append(nullToEmpty(tokenEndpoint)).append('\0')
                .append(nullToEmpty(deviceAuthorizationEndpoint)).append('\0')
                .append(nullToEmpty(scope)).append('\0')
                .append(nullToEmpty(audience)).append('\0')
                .append(groupsInToken ? '1' : '0');
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return toHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated on every JVM, so this is unreachable; rethrow defensively rather than
            // declare a checked exception across the whole construction path
            throw new OidcAuthException(e).put("SHA-256 is not available to key the OIDC token store");
        }
    }

    private static String nullToEmpty(String s) {
        return s != null ? s : "";
    }

    private static String toHex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xff;
            out[i * 2] = HEX[v >>> 4];
            out[i * 2 + 1] = HEX[v & 0x0f];
        }
        return new String(out);
    }
}
