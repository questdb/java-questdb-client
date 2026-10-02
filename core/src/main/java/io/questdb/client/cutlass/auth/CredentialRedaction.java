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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Redaction rules for bearer credentials and for error text that crosses the client boundary, as required by
 * the dynamic-credential specification (design/qwp-token-provider-spec.md, section 9).
 * <ul>
 *   <li>A token is never rendered. {@link #describeToken(CharSequence)} gives its length and at most the first
 *   8 hex digits of its SHA-256 - enough to correlate a rotation across log lines, useless to replay.</li>
 *   <li>Error text that came from a library or an identity endpoint goes through
 *   {@link #sanitizeErrorText(CharSequence)} before it reaches a client error or a log line: control,
 *   bidirectional and other non-displayable characters are removed, and the result is capped at
 *   {@value #MAX_ERROR_TEXT_LENGTH} characters.</li>
 * </ul>
 */
public final class CredentialRedaction {
    /**
     * Upper bound, in characters, on error text from a library or endpoint once sanitized.
     */
    public static final int MAX_ERROR_TEXT_LENGTH = 256;
    private static final int FINGERPRINT_HEX_DIGITS = 8;
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final String TRUNCATION_MARKER = "...";

    private CredentialRedaction() {
    }

    /**
     * Renders a token as {@code <redacted, N chars, sha256:xxxxxxxx>}: its length plus the first 8 hex digits of
     * its SHA-256. Never the token itself.
     *
     * @param token the token, may be null
     * @return a description that is safe to log
     */
    public static String describeToken(CharSequence token) {
        if (token == null) {
            return "<none>";
        }
        return "<redacted, " + token.length() + " chars, sha256:" + fingerprint(token) + '>';
    }

    /**
     * The first 8 hex digits of the SHA-256 of {@code token}, encoded as UTF-8.
     *
     * @param token the token, never null
     * @return 8 lowercase hex digits
     */
    public static String fingerprint(CharSequence token) {
        final byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(token.toString().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            // every Java platform is required to provide SHA-256
            throw new IllegalStateException("SHA-256 is not available", e);
        }
        final char[] out = new char[FINGERPRINT_HEX_DIGITS];
        for (int i = 0; i < FINGERPRINT_HEX_DIGITS / 2; i++) {
            out[2 * i] = HEX[(digest[i] >> 4) & 0xf];
            out[2 * i + 1] = HEX[digest[i] & 0xf];
        }
        return new String(out);
    }

    /**
     * Prepares untrusted error text - from a token source, an identity library or an endpoint - for a client
     * error or a log line: removes every code point {@link DisplaySafe} rejects (controls including CR/LF,
     * bidirectional overrides, zero-width and other format characters, lone surrogates) and caps the result at
     * {@value #MAX_ERROR_TEXT_LENGTH} characters, ending in {@code ...} when it had to cut.
     * <p>
     * This does not, and cannot, remove a secret the text already carries. Keeping tokens and response bodies
     * out of error text is the job of whoever produces it.
     *
     * @param text untrusted text, may be null
     * @return the sanitized text, or {@code null} when {@code text} is null
     */
    public static String sanitizeErrorText(CharSequence text) {
        if (text == null) {
            return null;
        }
        final StringBuilder sb = new StringBuilder(Math.min(text.length(), MAX_ERROR_TEXT_LENGTH));
        final int limit = MAX_ERROR_TEXT_LENGTH - TRUNCATION_MARKER.length();
        for (int i = 0, n = text.length(); i < n; ) {
            final int cp = Character.codePointAt(text, i);
            final int count = Character.charCount(cp);
            if (DisplaySafe.isDisplaySafe(cp)) {
                if (sb.length() + count > limit) {
                    // Only cut when something displayable is left to drop: text that fits exactly is kept whole.
                    if (hasDisplayableFrom(text, i, MAX_ERROR_TEXT_LENGTH - sb.length())) {
                        sb.append(TRUNCATION_MARKER);
                        return sb.toString();
                    }
                }
                sb.appendCodePoint(cp);
            }
            i += count;
        }
        return sb.toString();
    }

    // True when the displayable remainder of text, starting at from, does not fit in budget characters.
    private static boolean hasDisplayableFrom(CharSequence text, int from, int budget) {
        int needed = 0;
        for (int i = from, n = text.length(); i < n; ) {
            final int cp = Character.codePointAt(text, i);
            final int count = Character.charCount(cp);
            if (DisplaySafe.isDisplaySafe(cp)) {
                needed += count;
                if (needed > budget) {
                    return true;
                }
            }
            i += count;
        }
        return false;
    }
}
