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
 * Thrown when the OIDC device authorization flow cannot obtain a token. The message is built via
 * the fluent {@link #put(CharSequence)} family, backed by a {@link StringSink}.
 * <p>
 * For an OAuth error response (RFC 6749 / RFC 8628), {@link #getOauthError()} returns the
 * machine-readable error code (e.g. {@code access_denied}, {@code expired_token}); else {@code null}.
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
     * Builds an exception from an OAuth error response.
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

    // Reports characters that must never reach a terminal or log line. The argument is a code point, not
    // a UTF-16 unit: putSanitized scans with codePointAt, which joins a surrogate pair into one code point,
    // so a supplementary-plane format/control char is judged whole rather than as two harmless-looking
    // halves (the gap that once let an invisible U+E00xx "tag" char through). A lone unpaired surrogate
    // surfaces as a SURROGATE code point and is stripped too, having no displayable meaning.
    // Beyond the C0/C1 controls and DEL from isISOControl, this strips the Unicode format category (Cf:
    // zero-width joiners, BOM, bidi embedding/override/isolate controls, U+E00xx tag chars) plus an
    // explicit bidi/BOM set, so an attacker-influenced value (verification_uri, user_code, error string)
    // cannot reorder, hide, or spoof displayed text - even on a JDK that categorizes these differently.
    // Hex literals (not char escapes) keep this source ASCII, so it carries none of the chars it guards.
    static boolean isUnsafeForDisplay(int c) {
        return Character.isISOControl(c)
                || Character.getType(c) == Character.FORMAT
                || Character.getType(c) == Character.SURROGATE // unpaired surrogate (lone half), no displayable meaning
                || (c >= 0x202A && c <= 0x202E) // LRE, RLE, PDF, LRO, RLO
                || (c >= 0x2066 && c <= 0x2069) // LRI, RLI, FSI, PDI
                || c == 0x200E || c == 0x200F   // LRM, RLM
                || c == 0xFEFF;                 // BOM / zero-width no-break space
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

    // appends untrusted text with display-unsafe chars stripped, so an attacker-influenced IdP error
    // string cannot inject ANSI escapes, forge log lines, or smuggle bidi/zero-width formatting when
    // the exception message is rendered
    private void putSanitized(CharSequence cs) {
        if (cs != null) {
            for (int i = 0, n = cs.length(); i < n; ) {
                final int cp = Character.codePointAt(cs, i);
                final int count = Character.charCount(cp);
                if (!isUnsafeForDisplay(cp)) {
                    message.put(cs, i, i + count);
                }
                i += count;
            }
        }
    }
}
