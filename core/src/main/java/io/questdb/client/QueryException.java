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

/**
 * Thrown from {@link Completion#await()} / {@link Completion#await(long, java.util.concurrent.TimeUnit)}
 * when the server reported an error for the corresponding {@link Query},
 * when {@link Completion#cancel()} won the race, or when an unrecoverable
 * transport failure occurred during submission.
 * <p>
 * The original wire-level status byte is exposed via {@link #getStatus()} so
 * callers can distinguish cancellation from schema errors etc. without
 * string-matching the message.
 */
public class QueryException extends RuntimeException {

    private final byte status;

    public QueryException(byte status, String message) {
        super(message);
        this.status = status;
    }

    public QueryException(byte status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
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
