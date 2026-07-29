/*******************************************************************************
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

import io.questdb.client.cutlass.line.LineSenderException;

/**
 * A recovered store-and-forward slot whose symbol dictionary cannot be rebuilt at all --
 * neither from the persisted side-file's intact prefix nor from the surviving frames' own
 * delta sections. Its frames reference ids that nothing still holds, so replaying them
 * would make the server reject the frame with STATUS_DICTIONARY_GAP -- the ids cannot be
 * reconstructed, so the frame cannot be replayed at all.
 * <p>
 * This is the ONE verdict {@code seedGlobalDictionaryFromPersisted} reaches after it has
 * tried every source of truth it has, so it is the only signal that may quarantine a slot.
 * A distinct type, rather than a message match, because the difference between "this slot
 * is unreplayable" and any other {@link LineSenderException} out of the connect path is the
 * difference between setting a slot aside and silently discarding one that was fine.
 * <p>
 * It stays a {@code LineSenderException} so that a caller which does NOT handle it -- the
 * background drainer, a test constructing the sender directly -- keeps the old fail-clean
 * behaviour rather than seeing a new checked type. Only {@code Sender.build()} treats it as
 * recoverable, by setting the slot aside and starting the producer on a fresh one.
 */
public class UnreplayableSlotException extends LineSenderException {

    public UnreplayableSlotException(CharSequence message) {
        super(message);
    }
}
