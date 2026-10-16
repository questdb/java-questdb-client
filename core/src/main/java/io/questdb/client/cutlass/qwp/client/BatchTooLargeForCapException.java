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

import io.questdb.client.cutlass.line.LineSenderException;

/**
 * A batch that cannot fit the server's advertised cap however it is split. Distinct from
 * a plain {@link LineSenderException} because the batch is RETAINED for a later flush
 * against a larger-cap node, so {@code close()} has to recognise it and discard the batch
 * rather than abandon every row an earlier flush already published. Matching on the
 * message text would be the alternative, and would silently swallow unrelated failures.
 * {@link io.questdb.client.Sender#reset()} discards the retained batch and leaves the
 * sender usable -- the non-destructive recovery when producing smaller batches is not
 * an option.
 */
public class BatchTooLargeForCapException extends LineSenderException {

    // Length of getMessage() when endDetail() ran: the detail ends there and the
    // standalone remedy follows. -1 until then, when the whole message is the detail.
    private int detailLength = -1;

    public BatchTooLargeForCapException(CharSequence message) {
        super(message);
    }

    /**
     * Marks the end of the detail: the text appended so far says what was rejected
     * and the sizes involved, and the text appended after this call is the remedy for
     * a standalone sender, which {@link #getDetail()} leaves out.
     *
     * @return this exception, to append the remedy
     */
    public BatchTooLargeForCapException endDetail() {
        detailLength = getMessage().length();
        return this;
    }

    /**
     * The message without the standalone remedy ("call reset() and keep this sender"),
     * for a caller that handles the rejection itself, such as the connection pool,
     * which retires the slot. Equals {@link #getMessage()} when the message carries no
     * such remedy.
     *
     * @return what was rejected and the sizes involved
     */
    public String getDetail() {
        String message = getMessage();
        return detailLength < 0 || detailLength > message.length() ? message : message.substring(0, detailLength);
    }
}
