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

/**
 * Callback that populates the typed bind parameters of a single
 * {@link QwpQueryClient#execute(String, QwpBindSetter, QwpColumnBatchHandler)}
 * invocation. The callback runs on the user thread while the client prepares
 * the QUERY_REQUEST frame; the encoded bind bytes are then handed to the I/O
 * thread for transmission.
 * <p>
 * Implementations must call the typed setters on the supplied
 * {@link QwpBindValues} in strictly ascending index order starting at 0, with
 * no gaps. The client validates this and throws on violation.
 */
@FunctionalInterface
public interface QwpBindSetter {
    /**
     * Populates the supplied bind-value sink. Callers must invoke the typed
     * setters in ascending index order (0, 1, 2, ...).
     */
    void apply(QwpBindValues binds);
}
