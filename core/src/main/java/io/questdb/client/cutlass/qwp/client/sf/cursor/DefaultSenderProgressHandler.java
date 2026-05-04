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

import io.questdb.client.SenderProgressHandler;

/**
 * No-op default handler. Installed when the user does not register a
 * {@link SenderProgressHandler}. Unlike the error path, the happy-path
 * watermark-advance event is high-volume and not interesting to log — silence
 * is the right default.
 */
public final class DefaultSenderProgressHandler implements SenderProgressHandler {

    public static final DefaultSenderProgressHandler INSTANCE = new DefaultSenderProgressHandler();

    private DefaultSenderProgressHandler() {
    }

    @Override
    public void onAcked(long ackedFsn) {
        // intentionally empty
    }
}
