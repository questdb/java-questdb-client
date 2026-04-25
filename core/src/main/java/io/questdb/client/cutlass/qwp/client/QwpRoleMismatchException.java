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

/**
 * Thrown by {@link QwpQueryClient#connect()} when every configured endpoint
 * reports a role that does not satisfy the client's {@code target=} filter.
 * Callers can distinguish this from a plain {@link HttpClientException} (all
 * endpoints unreachable) to tailor retry behaviour: "no primary available" is
 * worth waiting and retrying after a failover window, "all endpoints down" is
 * a harder failure.
 */
public class QwpRoleMismatchException extends HttpClientException {

    private final QwpServerInfo lastObserved;
    private final String targetRole;

    public QwpRoleMismatchException(String targetRole, QwpServerInfo lastObserved, String message) {
        super(message);
        this.targetRole = targetRole;
        this.lastObserved = lastObserved;
    }

    /**
     * The {@link QwpServerInfo} from the last endpoint the client tried before
     * giving up. Useful for diagnostics; may be null if no endpoint even
     * responded with a {@code SERVER_INFO} frame.
     */
    public QwpServerInfo getLastObserved() {
        return lastObserved;
    }

    public String getTargetRole() {
        return targetRole;
    }
}
