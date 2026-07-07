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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default listener installed when the user does not call
 * {@code LineSenderBuilder.connectionListener(...)}. Logs every connection
 * transition so silence is never the default -- connect-string-only users
 * still see failover and outage signals in their logs.
 *
 * <p>Terminal kind {@code AUTH_FAILED} and {@code ALL_ENDPOINTS_UNREACHABLE}
 * fire at WARN level; everything else fires at INFO.
 */
public final class DefaultSenderConnectionListener implements SenderConnectionListener {

    public static final DefaultSenderConnectionListener INSTANCE = new DefaultSenderConnectionListener();
    private static final Logger LOG = LoggerFactory.getLogger("io.questdb.client.SenderConnection");

    private DefaultSenderConnectionListener() {
    }

    @Override
    public void onEvent(@NotNull SenderConnectionEvent e) {
        switch (e.getKind()) {
            case AUTH_FAILED:
            case ALL_ENDPOINTS_UNREACHABLE:
                LOG.warn("connection event {}", e);
                break;
            default:
                LOG.info("connection event {}", e);
        }
    }
}
