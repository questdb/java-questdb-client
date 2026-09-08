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
 * The user-facing part of an RFC 8628 device authorization response: the code to type and the URL
 * to type it at. A {@link DeviceCodePrompt} receives this object and shows it to the user.
 * <p>
 * The {@code device_code} secret is deliberately not exposed here; it stays inside
 * {@link OidcDeviceAuth} and is never shown to the user.
 */
public class DeviceAuthorizationChallenge {
    private final int expiresInSeconds;
    private final int intervalSeconds;
    private final String userCode;
    private final String verificationUri;
    private final String verificationUriComplete;

    public DeviceAuthorizationChallenge(
            String userCode,
            String verificationUri,
            String verificationUriComplete,
            int expiresInSeconds,
            int intervalSeconds
    ) {
        this.userCode = userCode;
        this.verificationUri = verificationUri;
        this.verificationUriComplete = verificationUriComplete;
        this.expiresInSeconds = expiresInSeconds;
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * @return seconds the {@link #getUserCode() user code} stays valid.
     */
    public int getExpiresInSeconds() {
        return expiresInSeconds;
    }

    /**
     * @return minimum seconds the client must wait between polls.
     */
    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    /**
     * @return the code the user enters at the {@link #getVerificationUri() verification URL}.
     */
    public String getUserCode() {
        return userCode;
    }

    /**
     * @return the URL the user opens to authorize the device.
     */
    public String getVerificationUri() {
        return verificationUri;
    }

    /**
     * @return a URL with the user code already embedded, so the user need not type it, or
     * {@code null} when the identity provider does not supply one.
     */
    public String getVerificationUriComplete() {
        return verificationUriComplete;
    }
}
