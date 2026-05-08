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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;

final class QwpUpgradeFailures {
    private QwpUpgradeFailures() {
    }

    /**
     * Inspects {@code client}'s rejected-upgrade state and returns a typed
     * exception if the failure is classifiable as a role reject ({@code 421} +
     * {@code X-QuestDB-Role}) or a credential failure ({@code 401}/{@code 403}).
     * Falls through to {@code ex} for any other status, including {@code 404}
     * (per-endpoint path mismatch) and unknown codes.
     */
    static HttpClientException classify(WebSocketClient client, String host, int port, HttpClientException ex) {
        String role = client.getUpgradeRejectRole();
        if (role != null) {
            QwpIngressRoleRejectedException re = new QwpIngressRoleRejectedException(
                    role, host, port, client.getUpgradeRejectZone());
            re.initCause(ex);
            return re;
        }
        int status = client.getUpgradeStatusCode();
        if (status == 401 || status == 403) {
            QwpAuthFailedException ae = new QwpAuthFailedException(status, host, port);
            ae.initCause(ex);
            return ae;
        }
        return ex;
    }
}
