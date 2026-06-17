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
 * Shows an RFC 8628 device authorization challenge to the user, who then opens the
 * verification URL in any browser (on the same machine or on a phone) and enters the
 * code. {@link OidcDeviceAuth} calls this once per interactive sign-in, just before it
 * starts polling the token endpoint.
 * <p>
 * The {@link #SYSTEM_OUT default implementation} prints the instructions to
 * {@code System.out}. Supply your own implementation to render the challenge somewhere
 * else, for example as a clickable link or a QR code in a notebook.
 */
@FunctionalInterface
public interface DeviceCodePrompt {

    /**
     * Prints the sign-in instructions to {@code System.out} using plain ASCII text.
     */
    DeviceCodePrompt SYSTEM_OUT = challenge -> {
        String newLine = System.lineSeparator();
        StringSink sb = new StringSink();
        sb.put(newLine);
        sb.put("=== QuestDB OIDC sign-in ===").put(newLine);
        sb.put("To sign in, open this URL in a browser:").put(newLine);
        sb.put("    ").put(challenge.getVerificationUri()).put(newLine);
        sb.put("and enter the code: ").put(challenge.getUserCode()).put(newLine);
        if (challenge.getVerificationUriComplete() != null) {
            sb.put("(or open this URL, the code is already filled in:").put(newLine);
            sb.put("    ").put(challenge.getVerificationUriComplete()).put(')').put(newLine);
        }
        sb.put("Waiting for authorization, up to ").put(challenge.getExpiresInSeconds()).put(" seconds...");
        System.out.println(sb);
    };

    /**
     * Shows the challenge to the user. This method must return quickly; the actual waiting
     * for the user happens afterwards while {@link OidcDeviceAuth} polls the token endpoint.
     *
     * @param challenge the user code, verification URL and timing parameters to show
     */
    void promptUser(DeviceAuthorizationChallenge challenge);
}
