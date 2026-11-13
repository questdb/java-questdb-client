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

import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import io.questdb.client.cutlass.qwp.client.QwpQueryOptions;

/**
 * Thrown from {@link Completion#await()} / {@link Completion#await(long, java.util.concurrent.TimeUnit)}
 * when the server reported an error for the corresponding {@link Query},
 * when {@link Completion#cancel()} won the race, or when an unrecoverable
 * transport failure occurred during submission.
 * <p>
 * The original wire-level status byte is exposed via {@link #getStatus()} so
 * callers can distinguish cancellation from schema errors etc. without
 * string-matching the message. When the query asked for the error position
 * with {@link QwpQueryOptions#errorPosition(boolean)}, {@link #getPosition()}
 * points at the offending character of the SQL text.
 */
public class QueryException extends RuntimeException {

    private final int position;
    private final byte status;

    public QueryException(byte status, String message) {
        this(status, message, QwpEgressMsgKind.NO_ERROR_POSITION);
    }

    public QueryException(byte status, String message, int position) {
        super(message);
        this.status = status;
        this.position = position;
    }

    public QueryException(byte status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.position = QwpEgressMsgKind.NO_ERROR_POSITION;
    }

    /**
     * Returns the {@code char} index into the SQL text the server-reported
     * error points at, or {@link QwpEgressMsgKind#NO_ERROR_POSITION} when the
     * query did not ask for it with {@link QwpQueryOptions#errorPosition(boolean)},
     * the server did not report it, or the error does not come from the SQL
     * text.
     */
    public int getPosition() {
        return position;
    }

    /**
     * Returns the server-reported wire status byte (see QWP protocol
     * definitions), or {@code 0} if this exception was raised by the client
     * (for example, transport failure before any server response).
     */
    public byte getStatus() {
        return status;
    }
}
