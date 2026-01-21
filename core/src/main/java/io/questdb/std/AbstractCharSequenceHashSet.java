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

package io.questdb.std;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * Abstract base class for hash sets storing CharSequence keys.
 */
public abstract class AbstractCharSequenceHashSet implements Mutable {
    /**
     * Minimum initial capacity.
     */
    protected static final int MIN_INITIAL_CAPACITY = 16;
    /**
     * Marker for empty slots.
     */
    protected static final CharSequence noEntryKey = null;
    /**
     * The load factor for the hash set.
     */
    protected final double loadFactor;
    /**
     * The capacity of the hash set.
     */
    protected int capacity;
    /**
     * The number of free slots.
     */
    protected int free;
    /**
     * The keys array.
     */
    protected CharSequence[] keys;
    /**
     * The mask for index calculation.
     */
    protected int mask;

    /**
     * Constructs a new hash set with the given initial capacity and load factor.
     *
     * @param initialCapacity the initial capacity
     * @param loadFactor      the load factor
     */
    public AbstractCharSequenceHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        keys = new CharSequence[len];
        mask = len - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKey);
        free = capacity;
    }

    /**
     * Checks if the set contains the given key.
     *
     * @param key the key to check
     * @return true if the set contains the key
     */
    public boolean contains(@NotNull CharSequence key) {
        return keyIndex(key) < 0;
    }

    /**
     * Returns the index of a free slot where this key can be placed.
     * Returns the negative index of the key if it's already present.
     *
     * @param key the key whose slot to look for
     * @return the index of a free slot where this key can be placed,
     * or the negative index of the key if it's already present.
     */
    public int keyIndex(@NotNull CharSequence key) {
        int index = Hash.spread(Chars.hashCode(key)) & mask;
        if (keys[index] == noEntryKey) {
            return index;
        }
        if (Chars.equals(key, keys[index])) {
            return -index - 1;
        }
        return probe(key, index);
    }

    /**
     * Returns the number of elements in the set.
     *
     * @return the size
     */
    public int size() {
        return capacity - free;
    }

    private int probe(CharSequence key, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryKey) {
                return index;
            }
            if (Chars.equals(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

}