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

import io.questdb.client.std.str.DisplaySafe;
import io.questdb.client.std.str.StringSink;

/**
 * Thrown when the OIDC device authorization flow cannot obtain a token. The message is built via
 * the fluent {@link #put(CharSequence)} family, backed by a {@link StringSink}.
 * <p>
 * For an OAuth error response (RFC 6749 / RFC 8628), {@link #getOauthError()} returns the
 * machine-readable error code (e.g. {@code access_denied}, {@code expired_token}), stripped of any
 * display-unsafe characters so a caller may log it as-is; else {@code null}.
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
        // Sanitize the IdP error code once and reuse the result for both the message and the field.
        // getOauthError() is public, so it must not hand a logging caller the raw provider string: JsonLexer
        // now decodes JSON escapes, so an "error" such as access_deniedESC[2J would otherwise deliver a
        // real ESC (ANSI/CR-LF/bidi injection) straight out of getOauthError().
        e.oauthError = sanitize(error);
        e.put("the identity provider returned an error [error=");
        if (e.oauthError != null) {
            e.put(e.oauthError);
        }
        if (description != null && description.length() > 0) {
            e.put(", description=").putSanitized(description);
        }
        e.put(']');
        return e;
    }

    // Whether a character must never reach a terminal or log line, delegated to the shared DisplaySafe
    // classifier so the auth layer and Utf16Sink.putAsPrintable judge display safety identically. The
    // argument is a code point, not a UTF-16 unit: appendSanitized scans with codePointAt, which joins a
    // surrogate pair into one code point, so a supplementary-plane format/control char is judged whole
    // rather than as two harmless-looking halves (the gap that once let an invisible U+E00xx "tag" char
    // through). A lone unpaired surrogate surfaces as a SURROGATE code point and is stripped too.
    static boolean isUnsafeForDisplay(int c) {
        return DisplaySafe.isUnsafeForDisplay(c);
    }

    // strips display-unsafe chars from cs into sink; the shared primitive behind putSanitized and sanitize
    private static void appendSanitized(StringSink sink, CharSequence cs) {
        if (cs != null) {
            for (int i = 0, n = cs.length(); i < n; ) {
                final int cp = Character.codePointAt(cs, i);
                final int count = Character.charCount(cp);
                if (!isUnsafeForDisplay(cp)) {
                    sink.put(cs, i, i + count);
                }
                i += count;
            }
        }
    }

    // returns cs with display-unsafe chars stripped, or null when cs is null. Backs the oauthError field,
    // which getOauthError() exposes to callers that may render it raw.
    private static String sanitize(CharSequence cs) {
        if (cs == null) {
            return null;
        }
        StringSink sink = new StringSink();
        appendSanitized(sink, cs);
        return sink.toString();
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
        appendSanitized(message, cs);
    }
}
