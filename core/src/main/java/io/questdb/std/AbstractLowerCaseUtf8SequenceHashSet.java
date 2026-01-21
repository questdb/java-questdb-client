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

import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;

import java.util.Arrays;

public abstract class AbstractLowerCaseUtf8SequenceHashSet implements Mutable {
    protected static final int MIN_INITIAL_CAPACITY = 16;
    protected static final Utf8String noEntryKey = null;
    protected final double loadFactor;
    protected int capacity;
    protected int free;
    protected int[] hashCodes;
    protected Utf8Sequence[] keys;
    protected int mask;

    public AbstractLowerCaseUtf8SequenceHashSet(int initialCapacity, double loadFactor) {
        if (loadFactor <= 0d || loadFactor >= 1d) {
            throw new IllegalArgumentException("0 < loadFactor < 1");
        }

        free = this.capacity = initialCapacity < MIN_INITIAL_CAPACITY ? MIN_INITIAL_CAPACITY : Numbers.ceilPow2(initialCapacity);
        this.loadFactor = loadFactor;
        int len = Numbers.ceilPow2((int) (this.capacity / loadFactor));
        keys = new Utf8Sequence[len];
        hashCodes = new int[len];
        mask = len - 1;
    }

    @Override
    public void clear() {
        Arrays.fill(keys, noEntryKey);
        free = capacity;
    }

    public int keyIndex(Utf8Sequence key) {
        int hashCode = Utf8s.lowerCaseAsciiHashCode(key);
        int index = Hash.spread(hashCode) & mask;
        if (keys[index] == noEntryKey) {
            return index;
        }
        if (hashCode == hashCodes[index] && Utf8s.equalsIgnoreCaseAscii(key, keys[index])) {
            return -index - 1;
        }
        return probe(key, hashCode, index);
    }

    public int size() {
        return capacity - free;
    }

    private int probe(Utf8Sequence key, int hashCode, int index) {
        do {
            index = (index + 1) & mask;
            if (keys[index] == noEntryKey) {
                return index;
            }
            if (hashCode == hashCodes[index] && Utf8s.equalsIgnoreCaseAscii(key, keys[index])) {
                return -index - 1;
            }
        } while (true);
    }

}