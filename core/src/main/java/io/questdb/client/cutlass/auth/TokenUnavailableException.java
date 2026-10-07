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

import io.questdb.client.cutlass.line.LineSenderException;

/**
 * No token could be obtained. This is the {@code TokenError} of the dynamic-credential specification
 * (design/qwp-token-provider-spec.md, section 4) and also its {@code token-unavailable} error.
 * <ul>
 *   <li>A {@link TokenSource} throws it to classify a failed fetch.</li>
 *   <li>A {@link RefreshingTokenProvider} throws it from {@code getToken()} when it holds no usable token and
 *   cannot get one in time. It then carries the classification of the provider's most recent failed fetch, or
 *   is retryable when no fetch has failed (the wait timed out, or the caller was interrupted).</li>
 * </ul>
 * {@link #isRetryable()} tells a caller whether trying again can succeed. A permanent failure is configuration
 * that is missing or wrong; a retryable one is a network failure, a timeout, throttling or a server error.
 * {@link #getRetryAfterMillis()} is the platform's suggested wait before the next attempt, or
 * {@link #NO_RETRY_AFTER}.
 * <p>
 * Connection code treats an exception from an application-supplied token provider by type: only a
 * {@code TokenUnavailableException} marked retryable is retried while a {@code Sender} with
 * {@code initial_connect_retry=on} starts up; anything else fails startup fast (decision D8).
 * <p>
 * The message must never contain a token or a raw response body.
 */
public class TokenUnavailableException extends LineSenderException {
    /**
     * Value of {@link #getRetryAfterMillis()} meaning the platform suggested no wait.
     */
    public static final long NO_RETRY_AFTER = -1;
    private final long retryAfterMillis;
    private final boolean retryable;

    /**
     * @param message   a description of the failure, never containing a token or a raw response body
     * @param retryable whether retrying can succeed
     */
    public TokenUnavailableException(CharSequence message, boolean retryable) {
        this(message, retryable, NO_RETRY_AFTER);
    }

    /**
     * @param message          a description of the failure, never containing a token or a raw response body
     * @param retryable        whether retrying can succeed
     * @param retryAfterMillis the platform's suggested wait before retrying, or {@link #NO_RETRY_AFTER}
     */
    public TokenUnavailableException(CharSequence message, boolean retryable, long retryAfterMillis) {
        super(message, retryable);
        this.retryable = retryable;
        this.retryAfterMillis = retryAfterMillis < 0 ? NO_RETRY_AFTER : retryAfterMillis;
    }

    /**
     * Carries a cause. Attach only a cause that is safe to log: never one whose message holds a token or a raw
     * response body, and never an object that serializes a raw HTTP request.
     *
     * @param message          a description of the failure, never containing a token or a raw response body
     * @param retryable        whether retrying can succeed
     * @param retryAfterMillis the platform's suggested wait before retrying, or {@link #NO_RETRY_AFTER}
     * @param cause            the underlying failure, may be null
     */
    public TokenUnavailableException(String message, boolean retryable, long retryAfterMillis, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
        this.retryAfterMillis = retryAfterMillis < 0 ? NO_RETRY_AFTER : retryAfterMillis;
    }

    /**
     * A permanent failure: configuration that is missing or wrong.
     */
    public static TokenUnavailableException permanent(CharSequence message) {
        return new TokenUnavailableException(message, false);
    }

    /**
     * A retryable failure without a suggested wait.
     */
    public static TokenUnavailableException retryable(CharSequence message) {
        return new TokenUnavailableException(message, true);
    }

    /**
     * A retryable failure with the platform's suggested wait, such as an HTTP {@code Retry-After}.
     */
    public static TokenUnavailableException retryable(CharSequence message, long retryAfterMillis) {
        return new TokenUnavailableException(message, true, retryAfterMillis);
    }

    /**
     * @return the platform's suggested wait before the next attempt, in milliseconds, or
     * {@link #NO_RETRY_AFTER} when it gave none
     */
    public long getRetryAfterMillis() {
        return retryAfterMillis;
    }

    /**
     * @return true when retrying can succeed (a network failure, timeout, throttling or server error), false for
     * configuration that is missing or wrong
     */
    @Override
    public boolean isRetryable() {
        return retryable;
    }
}
