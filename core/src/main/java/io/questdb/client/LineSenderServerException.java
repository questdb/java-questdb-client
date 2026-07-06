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

package io.questdb.client;

import io.questdb.client.cutlass.line.LineSenderException;
import org.jetbrains.annotations.NotNull;

/**
 * Thrown from a producer-thread API call (typically {@link Sender#flush()}) when the
 * asynchronous SF send loop has latched a server-side rejection with policy
 * {@link SenderError.Policy#TERMINAL}.
 *
 * <p>The wrapped {@link SenderError} carries the rejection details — category, status byte,
 * server message, FSN span, and (best-effort) table name. Use {@link #getServerError()} to
 * unpack.
 *
 * <p>Catching this exception leaves the sender in a halted state. To recover, close and
 * rebuild the sender.
 *
 * @see SenderError
 * @see SenderErrorHandler
 */
public class LineSenderServerException extends LineSenderException {

    private final transient SenderError serverError;

    public LineSenderServerException(@NotNull SenderError serverError) {
        super(buildMessage(serverError));
        this.serverError = serverError;
    }

    /**
     * @return the underlying {@link SenderError} payload describing the rejection.
     */
    public @NotNull SenderError getServerError() {
        return serverError;
    }

    private static String buildMessage(SenderError e) {
        StringBuilder sb = new StringBuilder(160);
        sb.append("server rejected batch: ").append(e.getCategory());
        int status = e.getServerStatusByte();
        if (status != SenderError.NO_STATUS_BYTE) {
            sb.append(" (status=0x").append(Integer.toHexString(status & 0xFF)).append(')');
        }
        sb.append(" fsn=[").append(e.getFromFsn()).append(',').append(e.getToFsn()).append(']');
        if (e.getTableName() != null) {
            sb.append(" table=").append(e.getTableName());
        }
        long seq = e.getMessageSequence();
        if (seq != SenderError.NO_MESSAGE_SEQUENCE) {
            sb.append(" seq=").append(seq);
        }
        String msg = e.getServerMessage();
        if (msg != null && !msg.isEmpty()) {
            sb.append(" - ").append(msg);
        }
        return sb.toString();
    }
}
