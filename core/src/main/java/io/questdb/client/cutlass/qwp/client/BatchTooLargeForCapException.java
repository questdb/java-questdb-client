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
 */
public class BatchTooLargeForCapException extends LineSenderException {

    public BatchTooLargeForCapException(CharSequence message) {
        super(message);
    }
}
