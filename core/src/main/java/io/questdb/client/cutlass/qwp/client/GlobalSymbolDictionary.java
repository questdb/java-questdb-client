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
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.CharSequenceIntHashMap;
import io.questdb.client.std.ObjList;

/**
 * Global symbol dictionary that maps symbol strings to sequential integer IDs.
 * <p>
 * This dictionary is shared across all tables and columns within a client instance.
 * IDs are assigned sequentially starting from 0, ensuring contiguous ID space.
 * <p>
 * Thread safety: This class is NOT thread-safe. External synchronization is required
 * if accessed from multiple threads.
 */
public class GlobalSymbolDictionary {

    private final ObjList<String> idToSymbol;
    private final CharSequenceIntHashMap symbolToId;

    public GlobalSymbolDictionary() {
        this(64); // Default initial capacity
    }

    public GlobalSymbolDictionary(int initialCapacity) {
        this.symbolToId = new CharSequenceIntHashMap(initialCapacity);
        this.idToSymbol = new ObjList<>(initialCapacity);
    }

    /**
     * Appends {@code symbol} at the next sequential id, matching a recovered /
     * persisted dictionary's dense id order, WITHOUT de-duplicating.
     * <p>
     * Recovery ({@code QwpWebSocketSender.seedGlobalDictionaryFromPersisted})
     * replays the persisted entries in id order to rebuild this dictionary. It must
     * NOT collapse two source strings that decode to the same characters, because
     * the persisted {@code .symbol-dict}, the on-wire delta and the I/O-thread
     * catch-up mirror all key on the entry POSITION (id), not on the string. The
     * only strings that collide this way are malformed lone UTF-16 surrogates,
     * which the UTF-8 encoder maps to {@code '?'}: {@link #getOrAddSymbol} would
     * de-dup them and leave this dictionary SHORTER than the persisted entry count,
     * desyncing the producer's delta baseline from the catch-up mirror (which uses
     * {@code pd.size()}) and silently misattributing later symbols. Appending
     * unconditionally keeps {@link #size()} equal to that count. The reverse lookup
     * keeps the highest id for a colliding string, which is harmless: both ids
     * encode to the same bytes, so resolving either is equivalent.
     * <p>
     * Deliberately NOT capped at {@link QwpConstants#MAX_SYMBOL_DICTIONARY_SIZE}:
     * recovery replays entries {@link #getOrAddSymbol} already admitted under the
     * cap, so an over-cap replay is unreachable from data this client wrote.
     *
     * @param symbol the recovered symbol string (must not be null)
     * @return the id assigned (the previous {@link #size()})
     */
    public int addRecoveredSymbol(String symbol) {
        if (symbol == null) {
            throw new IllegalArgumentException("symbol cannot be null");
        }
        int newId = idToSymbol.size();
        symbolToId.put(symbol, newId);
        idToSymbol.add(symbol);
        return newId;
    }

    /**
     * Clears all symbols from the dictionary.
     * <p>
     * After clearing, the next symbol added will get ID 0.
     */
    public void clear() {
        symbolToId.clear();
        idToSymbol.clear();
    }

    /**
     * Checks if the dictionary contains the given symbol.
     *
     * @param symbol the symbol to check
     * @return true if the symbol exists in the dictionary
     */
    public boolean contains(String symbol) {
        return symbol != null && symbolToId.get(symbol) != CharSequenceIntHashMap.NO_ENTRY_VALUE;
    }

    /**
     * Gets the ID for an existing symbol, or -1 if not found.
     *
     * @param symbol the symbol string
     * @return the symbol ID, or -1 if not in dictionary
     */
    public int getId(String symbol) {
        if (symbol == null) {
            return -1;
        }
        int id = symbolToId.get(symbol);
        return id == CharSequenceIntHashMap.NO_ENTRY_VALUE ? -1 : id;
    }

    /**
     * Gets or adds a symbol to the dictionary.
     * <p>
     * If the symbol already exists, returns its existing ID.
     * If the symbol is new, assigns the next sequential ID and returns it.
     *
     * @param symbol the symbol string (must not be null)
     * @return the global ID for this symbol (>= 0)
     * @throws IllegalArgumentException if symbol is null
     * @throws LineSenderException if the symbol is new and the dictionary already holds {@link QwpConstants#MAX_SYMBOL_DICTIONARY_SIZE} entries
     */
    public int getOrAddSymbol(CharSequence symbol) {
        if (symbol == null) {
            throw new IllegalArgumentException("symbol cannot be null");
        }

        int existingId = symbolToId.get(symbol);
        if (existingId != CharSequenceIntHashMap.NO_ENTRY_VALUE) {
            return existingId;
        }

        // The server rejects any delta or catch-up whose deltaStartId + deltaCount
        // exceeds the protocol cap, and that rejection is terminal for the sender --
        // in store-and-forward mode it would strand an already-buffered backlog with
        // no drainer able to deliver it. Refusing the symbol HERE, before the row is
        // buffered, keeps everything buffered deliverable: the check is > on the
        // server, so a dictionary of exactly the cap still catches up cleanly.
        if (idToSymbol.size() >= QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE) {
            throw new LineSenderException(
                    "global symbol dictionary is full: the QWP protocol caps a sender's distinct symbol values at "
                            + QwpConstants.MAX_SYMBOL_DICTIONARY_SIZE
                            + ". Rows using already-registered symbol values continue to work. To start a fresh "
                            + "dictionary, close this sender and build a new one (with store-and-forward the "
                            + "buffered backlog drains first). For unbounded-cardinality data use varchar "
                            + "columns instead of symbol");
        }

        // Assign new ID — toString() only for new symbols that must be stored
        String symbolStr = symbol.toString();
        int newId = idToSymbol.size();
        symbolToId.put(symbolStr, newId);
        idToSymbol.add(symbolStr);
        return newId;
    }

    /**
     * Gets the symbol string for a given ID.
     *
     * @param id the symbol ID
     * @return the symbol string
     * @throws IndexOutOfBoundsException if id is out of range
     */
    public String getSymbol(int id) {
        if (id < 0 || id >= idToSymbol.size()) {
            throw new IndexOutOfBoundsException("Invalid symbol ID: " + id + ", dictionary size: " + idToSymbol.size());
        }
        return idToSymbol.getQuick(id);
    }

    /**
     * Checks if the dictionary is empty.
     *
     * @return true if no symbols have been added
     */
    public boolean isEmpty() {
        return idToSymbol.size() == 0;
    }

    /**
     * Returns the number of symbols in the dictionary.
     *
     * @return dictionary size
     */
    public int size() {
        return idToSymbol.size();
    }

    /**
     * Drops every entry at id {@code >= newSize}, returning those ids to the
     * unassigned space so the next {@link #getOrAddSymbol} reuses them. A no-op
     * when {@code newSize} is at or above the current {@link #size()}.
     * <p>
     * <b>Reusing an id is only safe while nothing else has recorded it.</b> The
     * persisted {@code .symbol-dict}, the on-wire delta and the send loop's
     * catch-up mirror all key on the entry POSITION, so an id that any of them
     * already binds to one string must never be handed to another -- that is the
     * silent symbol misattribution the dense id space exists to prevent. The
     * caller owns that proof; see {@code QwpWebSocketSender.reset()}, which
     * reclaims only above both the sent watermark and the persisted size, and
     * only in delta mode.
     *
     * @param newSize the number of entries to keep, from id 0
     * @throws IllegalArgumentException if {@code newSize} is negative
     */
    public void truncateTo(int newSize) {
        if (newSize < 0) {
            throw new IllegalArgumentException("newSize cannot be negative: " + newSize);
        }
        int size = idToSymbol.size();
        if (newSize >= size) {
            return;
        }
        // Release the discarded strings: setPos only moves the cursor, so the
        // backing array would otherwise keep every reclaimed entry alive.
        for (int id = newSize; id < size; id++) {
            idToSymbol.setQuick(id, null);
        }
        idToSymbol.setPos(newSize);
        // CharSequenceIntHashMap has no remove(), so rebuild the reverse index
        // from the survivors. O(newSize), on a path a caller only reaches when it
        // is genuinely reclaiming ids -- the steady state never gets here. The
        // rebuild also preserves addRecoveredSymbol's "highest id wins" rule for
        // two source strings that decode to the same characters.
        symbolToId.clear();
        for (int id = 0; id < newSize; id++) {
            symbolToId.put(idToSymbol.getQuick(id), id);
        }
    }
}
