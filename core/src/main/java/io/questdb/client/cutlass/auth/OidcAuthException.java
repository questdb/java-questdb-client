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

import io.questdb.client.std.str.StringSink;

/**
 * Thrown when the OIDC device authorization flow cannot obtain a token. The message is built
 * with the fluent {@link #put(CharSequence)} family, backed by a {@link StringSink}.
 * <p>
 * When the failure originates from an OAuth error response (RFC 6749 / RFC 8628),
 * {@link #getOauthError()} returns the machine-readable error code (for example
 * {@code access_denied} or {@code expired_token}); otherwise it returns {@code null}.
 */
public class OidcAuthException extends RuntimeException {
    private final StringSink message = new StringSink();
    private String oauthError;

    public OidcAuthException() {
    }

    public OidcAuthException(CharSequence message) {
        this.message.put(message);
    }

    public OidcAuthException(Throwable cause) {
        super(cause);
    }

    /**
     * Builds an exception out of an OAuth error response.
     *
     * @param error       the OAuth {@code error} code, never null
     * @param description the optional {@code error_description}, may be null or empty
     * @return a new exception carrying the error code
     */
    public static OidcAuthException oauthError(CharSequence error, CharSequence description) {
        OidcAuthException e = new OidcAuthException();
        e.oauthError = error != null ? error.toString() : null;
        e.put("the identity provider returned an error [error=").putSanitized(error);
        if (description != null && description.length() > 0) {
            e.put(", description=").putSanitized(description);
        }
        e.put(']');
        return e;
    }

    @Override
    public String getMessage() {
        return message.toString();
    }

    public String getOauthError() {
        return oauthError;
    }

    public OidcAuthException put(char ch) {
        message.put(ch);
        return this;
    }

    public OidcAuthException put(CharSequence cs) {
        message.put(cs);
        return this;
    }

    public OidcAuthException put(long value) {
        message.put(value);
        return this;
    }

    // appends untrusted text with control characters stripped, so an attacker-influenced IdP error
    // string cannot inject ANSI escapes or forge log lines when the exception message is rendered
    private void putSanitized(CharSequence cs) {
        if (cs != null) {
            for (int i = 0, n = cs.length(); i < n; i++) {
                char c = cs.charAt(i);
                if (!Character.isISOControl(c)) {
                    message.put(c);
                }
            }
        }
    }
}
