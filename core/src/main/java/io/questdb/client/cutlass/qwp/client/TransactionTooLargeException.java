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
 * A strict transaction ({@code transaction=strict}, or a sender with no auto-flush
 * trigger such as {@code auto_flush=off}, on the WebSocket transport) outgrew a
 * limit that would stop it from committing as one transaction: the server's
 * per-table {@code qwp.max.uncommitted.rows}, past which the server commits the
 * rows on its own, or the client's {@code sf_max_total_bytes}, which must hold the
 * whole uncommitted transaction until the server commits it. To send transactions
 * of that size, raise the limit the message names: {@code qwp.max.uncommitted.rows}
 * on the server, or {@code sf_max_total_bytes} on the client, whose default is
 * larger with {@code sf_dir} set.
 * <p>
 * {@code at()} and {@code atNow()} throw it for the row that would take the
 * transaction past either limit, and that row is not part of the transaction.
 * {@code flush()} throws it when the client buffer cannot hold the flush, and
 * sends nothing. Either way nothing past the limit reaches the server, and the
 * transaction keeps the rows accepted before the error.
 */
public class TransactionTooLargeException extends LineSenderException {

    public TransactionTooLargeException(CharSequence message) {
        super(message);
    }
}
