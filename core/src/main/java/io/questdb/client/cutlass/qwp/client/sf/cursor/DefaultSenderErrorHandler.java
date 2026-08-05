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

import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default handler installed when the user does not call
 * {@code LineSenderBuilder.errorHandler(...)}. Logs every server rejection,
 * plus every client-side {@link SenderError.Category#DATA_LOSS} verdict, so
 * silence is never the default — connect-string-only users still see errors
 * in their logs.
 *
 * <p>{@link SenderError.Policy#TERMINAL} and {@link SenderError.Policy#ABANDONED} fire at ERROR level; the retriable policies fire at WARN (the batch is replayed, not lost).
 */
public final class DefaultSenderErrorHandler implements SenderErrorHandler {

    public static final DefaultSenderErrorHandler INSTANCE = new DefaultSenderErrorHandler();
    private static final Logger LOG = LoggerFactory.getLogger("io.questdb.client.SenderError");

    private DefaultSenderErrorHandler() {
    }

    @Override
    public void onError(SenderError e) {
        if (e.getCategory() == SenderError.Category.DATA_LOSS) {
            // No server verdict exists for this category, so the headline must not claim
            // one; the server-shaped fields (status byte, fsn span, seq, table) are always
            // sentinels here and would be noise.
            LOG.error("buffered data abandoned [category={}, policy={}, quarantined={}, msg={}]",
                    e.getCategory(), e.getAppliedPolicy(), e.getQuarantinedPath(), e.getServerMessage());
            return;
        }
        // Single template; SLF4J fans out the levels so the call site stays
        // identical and the message format is reviewable in one place.
        String fmt = "server rejected batch [category={}, policy={}, status=0x{}, "
                + "fsn=[{},{}], table={}, seq={}, msg={}]";
        Object[] args = new Object[]{
                e.getCategory(),
                e.getAppliedPolicy(),
                Integer.toHexString(e.getServerStatusByte() & 0xFF),
                e.getFromFsn(),
                e.getToFsn(),
                e.getTableName() == null ? "(multi)" : e.getTableName(),
                e.getMessageSequence(),
                e.getServerMessage()
        };
        if (e.getAppliedPolicy() == SenderError.Policy.TERMINAL
                || e.getAppliedPolicy() == SenderError.Policy.ABANDONED) {
            LOG.error(fmt, args);
        } else {
            LOG.warn(fmt, args);
        }
    }
}
