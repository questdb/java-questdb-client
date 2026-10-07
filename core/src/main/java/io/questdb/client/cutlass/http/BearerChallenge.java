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

package io.questdb.client.cutlass.http;

/**
 * Reads the {@code error} parameter of a {@code Bearer} challenge in a {@code WWW-Authenticate} header
 * (RFC 6750, section 3; RFC 7235, section 4.1). Clients use it to decide whether a {@code 401} is worth a token
 * refresh: {@code invalid_token} is, {@code insufficient_scope} is not.
 * <p>
 * The parser is tolerant. It walks a comma-separated list of challenges - {@code scheme [token68 | auth-param,
 * ...]} - accepts quoted and unquoted parameter values, and ignores what it cannot read. Only the first
 * {@code error} parameter of a {@code Bearer} challenge counts.
 */
public final class BearerChallenge {
    /**
     * The {@code error} value that means the presented token itself is bad, so a new one can help.
     */
    public static final String INVALID_TOKEN = "invalid_token";

    private BearerChallenge() {
    }

    /**
     * @param wwwAuthenticate the value of one or more {@code WWW-Authenticate} headers (joined with commas), or
     *                        null
     * @return the {@code error} parameter of the first {@code Bearer} challenge that carries one, or null when
     * there is no such challenge
     */
    public static String bearerError(CharSequence wwwAuthenticate) {
        if (wwwAuthenticate == null) {
            return null;
        }
        final int n = wwwAuthenticate.length();
        boolean inBearer = false;
        int i = 0;
        while (i < n) {
            char c = wwwAuthenticate.charAt(i);
            if (c == ',' || isWhitespace(c)) {
                i++;
                continue;
            }
            final int start = i;
            while (i < n && isTokenChar(wwwAuthenticate.charAt(i))) {
                i++;
            }
            if (i == start) {
                i++; // not a token character: skip it
                continue;
            }
            final String token = wwwAuthenticate.subSequence(start, i).toString();
            int j = skipWhitespace(wwwAuthenticate, i);
            if (j < n && wwwAuthenticate.charAt(j) == '=') {
                // auth-param: name = ( token / quoted-string )
                j = skipWhitespace(wwwAuthenticate, j + 1);
                final String value;
                if (j < n && wwwAuthenticate.charAt(j) == '"') {
                    final StringBuilder sb = new StringBuilder();
                    j++;
                    while (j < n) {
                        char q = wwwAuthenticate.charAt(j++);
                        if (q == '\\' && j < n) {
                            sb.append(wwwAuthenticate.charAt(j++));
                        } else if (q == '"') {
                            break;
                        } else {
                            sb.append(q);
                        }
                    }
                    value = sb.toString();
                } else {
                    final int valueStart = j;
                    while (j < n && wwwAuthenticate.charAt(j) != ',' && !isWhitespace(wwwAuthenticate.charAt(j))) {
                        j++;
                    }
                    value = wwwAuthenticate.subSequence(valueStart, j).toString();
                }
                if (inBearer && "error".equalsIgnoreCase(token)) {
                    return value;
                }
                i = j;
            } else {
                // a new challenge's auth-scheme (or a token68, which carries no parameters)
                inBearer = "Bearer".equalsIgnoreCase(token);
                i = j;
            }
        }
        return null;
    }

    /**
     * Whether a {@code 401} carrying {@code wwwAuthenticate} warrants refreshing the token: true when the header
     * has no {@code Bearer} challenge with an {@code error} parameter (the status code alone decides), or when
     * that error is {@code invalid_token}.
     */
    public static boolean allowsTokenRefresh(CharSequence wwwAuthenticate) {
        final String error = bearerError(wwwAuthenticate);
        return error == null || INVALID_TOKEN.equals(error);
    }

    private static boolean isTokenChar(char c) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
            return true;
        }
        switch (c) {
            case '!':
            case '#':
            case '$':
            case '%':
            case '&':
            case '\'':
            case '*':
            case '+':
            case '-':
            case '.':
            case '^':
            case '_':
            case '`':
            case '|':
            case '~':
                return true;
            default:
                return false;
        }
    }

    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t';
    }

    private static int skipWhitespace(CharSequence s, int i) {
        while (i < s.length() && isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }
}
