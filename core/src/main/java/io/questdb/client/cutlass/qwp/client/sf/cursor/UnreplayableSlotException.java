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
 * A recovered store-and-forward slot that cannot be replayed at all. Three causes, all
 * rooted in the symbol dictionary:
 * <ul>
 *   <li>A loaded dictionary entry is truncated, or the loaded region has trailing bytes,
 *       discovered while decoding bytes {@code PersistedSymbolDict.open} already
 *       CRC-validated ({@code PersistedSymbolDict.addLoadedSymbolsTo}).</li>
 *   <li>The dictionary cannot be rebuilt at all, neither from the persisted side-file's
 *       intact prefix nor from the surviving frames' own delta sections
 *       ({@code seedGlobalDictionaryFromPersisted}, {@code CursorSendEngine}'s
 *       post-recovery symbol baseline check).</li>
 *   <li>A fresh slot's dictionary file survives from a prior generation and cannot be
 *       truncated ({@code PersistedSymbolDict.openFresh}, reached from {@code openClean}
 *       when a fresh slot must start with an empty dictionary) -- degrading to {@code null}
 *       instead would leave that generation's id space on disk while this session writes
 *       against a fresh id space from 0, corrupting the NEXT recovery's catch-up rather
 *       than this one.</li>
 * </ul>
 * In the first two cases the frames reference ids that nothing still holds, so replaying
 * them would make the server reject the frame with STATUS_DICTIONARY_GAP.
 * <p>
 * A distinct type, rather than a message match, because the difference between "this slot
 * is unreplayable" and any other {@link LineSenderException} out of the connect path is the
 * difference between setting a slot aside and silently discarding one that was fine.
 * <p>
 * It stays a {@code LineSenderException} so that a caller which does NOT handle it -- a test
 * constructing the sender directly, say -- keeps the old fail-clean behaviour rather than
 * seeing a new checked type. {@code Sender.build()} and {@code BackgroundDrainer} both treat
 * it as recoverable: the former sets the slot aside and starts the producer on a fresh one,
 * the latter drops the {@code .failed} sentinel so the next orphan scan skips the slot.
 */
public class UnreplayableSlotException extends LineSenderException {

    public UnreplayableSlotException(CharSequence message) {
        super(message);
    }
}
