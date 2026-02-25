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

package io.questdb.client.std;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;


public class CharSequenceIntHashMap extends AbstractCharSequenceHashSet {
    public static final int NO_ENTRY_VALUE = -1;
    private final ObjList<CharSequence> list;
    private final int noEntryValue;
    private int[] values;

    public CharSequenceIntHashMap() {
        this(8);
    }

    public CharSequenceIntHashMap(int initialCapacity) {
        this(initialCapacity, 0.4, NO_ENTRY_VALUE);
    }

    public CharSequenceIntHashMap(int initialCapacity, double loadFactor, int noEntryValue) {
        super(initialCapacity, loadFactor);
        this.noEntryValue = noEntryValue;
        this.list = new ObjList<>(capacity);
        values = new int[keys.length];
        clear();
    }

    @Override
    public final void clear() {
        super.clear();
        list.clear();
        Arrays.fill(values, noEntryValue);
    }

    public int get(@NotNull CharSequence key) {
        return valueAt(keyIndex(key));
    }

    public void inc(@NotNull CharSequence key) {
        int index = keyIndex(key);
        if (index < 0) {
            values[-index - 1]++;
        } else {
            String keyString = Chars.toString(key);
            putAt0(index, keyString, 1);
            list.add(keyString);
        }
    }

    public ObjList<CharSequence> keys() {
        return list;
    }

    public boolean put(@NotNull CharSequence key, int value) {
        return putAt(keyIndex(key), key, value);
    }

    public void putAll(@NotNull CharSequenceIntHashMap other) {
        CharSequence[] otherKeys = other.keys;
        int[] otherValues = other.values;
        for (int i = 0, n = otherKeys.length; i < n; i++) {
            if (otherKeys[i] != noEntryKey) {
                put(otherKeys[i], otherValues[i]);
            }
        }
    }

    public boolean putAt(int index, @NotNull CharSequence key, int value) {
        if (index < 0) {
            values[-index - 1] = value;
            return false;
        }
        final String keyString = Chars.toString(key);
        putAt0(index, keyString, value);
        list.add(keyString);
        return true;
    }

    public void putIfAbsent(@NotNull CharSequence key, int value) {
        int index = keyIndex(key);
        if (index > -1) {
            String keyString = Chars.toString(key);
            putAt0(index, keyString, value);
            list.add(keyString);
        }
    }

    public void removeAt(int index) {
        if (index < 0) {
            int from = -index - 1;
            CharSequence key = keys[from];
            erase(from);
            free++;

            // after we have freed up a slot
            // consider non-empty keys directly below
            // they may have been a direct hit but because
            // directly hit slot wasn't empty these keys would
            // have moved.
            //
            // After slot is freed these keys require re-hash
            from = (from + 1) & mask;
            for (
                    CharSequence k = keys[from];
                    k != noEntryKey;
                    from = (from + 1) & mask, k = keys[from]
            ) {
                int idealHit = Hash.spread(Chars.hashCode(k)) & mask;
                if (idealHit != from) {
                    int to;
                    if (keys[idealHit] != noEntryKey) {
                        to = probe0(k, idealHit);
                    } else {
                        to = idealHit;
                    }

                    if (to > -1) {
                        move(from, to);
                    }
                }
            }

            list.remove(key);
        }
    }

    public int valueAt(int index) {
        int index1 = -index - 1;
        return index < 0 ? values[index1] : noEntryValue;
    }

    public int valueQuick(int index) {
        return get(list.getQuick(index));
    }

    private void erase(int index) {
        keys[index] = noEntryKey;
        values[index] = noEntryValue;
    }

    private void move(int from, int to) {
        keys[to] = keys[from];
        values[to] = values[from];
        erase(from);
    }

    private int probe0(CharSequence key, int index) {
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

    private void putAt0(int index, CharSequence key, int value) {
        keys[index] = key;
        values[index] = value;
        if (--free == 0) {
            rehash();
        }
    }

    private void rehash() {
        int[] oldValues = values;
        CharSequence[] oldKeys = keys;
        int size = capacity - free;
        capacity = capacity * 2;
        free = capacity - size;
        mask = Numbers.ceilPow2((int) (capacity / loadFactor)) - 1;
        this.keys = new CharSequence[mask + 1];
        this.values = new int[mask + 1];
        for (int i = oldKeys.length - 1; i > -1; i--) {
            CharSequence key = oldKeys[i];
            if (key != null) {
                final int index = keyIndex(key);
                keys[index] = key;
                values[index] = oldValues[i];
            }
        }
    }
}
