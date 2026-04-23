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

package io.questdb.client.test.std;

import io.questdb.client.std.CharSequenceLongHashMap;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class CharSequenceLongHashMapTest {

    @Test
    public void testClear() {
        CharSequenceLongHashMap map = new CharSequenceLongHashMap();
        map.put("a", 1);
        map.put("b", 2);
        Assert.assertEquals(2, map.size());

        map.clear();
        Assert.assertEquals(0, map.size());
        Assert.assertEquals(CharSequenceLongHashMap.NO_ENTRY_VALUE, map.get("a"));
        Assert.assertEquals(CharSequenceLongHashMap.NO_ENTRY_VALUE, map.get("b"));
    }

    @Test
    public void testContains() {
        CharSequenceLongHashMap map = new CharSequenceLongHashMap();
        Rnd rnd = new Rnd();
        final int N = 1000;

        for (int i = 0; i < N; i++) {
            String s = rnd.nextString(10).substring(1, 9);
            map.put(s, i);
        }

        ObjList<CharSequence> keys = map.keys();
        for (int i = 0; i < keys.size(); i++) {
            Assert.assertTrue(map.contains(keys.get(i)));
        }
    }

    @Test
    public void testCustomNoEntryValue() {
        long customNoEntry = -42L;
        CharSequenceLongHashMap map = new CharSequenceLongHashMap(8, 0.4, customNoEntry);
        Assert.assertEquals(customNoEntry, map.get("missing"));
        map.put("x", 100);
        Assert.assertEquals(100, map.get("x"));
        Assert.assertEquals(customNoEntry, map.get("y"));
    }

    @Test
    public void testGetMissingKeyReturnsNoEntryValue() {
        CharSequenceLongHashMap map = new CharSequenceLongHashMap();
        Assert.assertEquals(CharSequenceLongHashMap.NO_ENTRY_VALUE, map.get("missing"));
        map.put("present", 42);
        Assert.assertEquals(CharSequenceLongHashMap.NO_ENTRY_VALUE, map.get("absent"));
    }

    @Test
    public void testPutAndGet() {
        Rnd rnd = new Rnd();
        CharSequenceLongHashMap map = new CharSequenceLongHashMap();
        final int N = 1000;
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextString(15);
            boolean isNew = map.put(cs, rnd.nextLong());
            Assert.assertTrue(isNew);
        }
        Assert.assertEquals(N, map.size());

        rnd.reset();

        // verify all values are retrievable
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextString(15);
            Assert.assertEquals(rnd.nextLong(), map.get(cs));
        }
    }

    @Test
    public void testPutUpdatesExistingKey() {
        CharSequenceLongHashMap map = new CharSequenceLongHashMap();
        Assert.assertTrue(map.put("key", 1));
        Assert.assertFalse(map.put("key", 2));
        Assert.assertEquals(2, map.get("key"));
        Assert.assertEquals(1, map.size());
    }

    @Test
    public void testRehash() {
        // use a small initial capacity to force multiple rehashes
        CharSequenceLongHashMap map = new CharSequenceLongHashMap(8);
        Rnd rnd = new Rnd();
        final int N = 500;
        for (int i = 0; i < N; i++) {
            map.put(rnd.nextString(15), rnd.nextLong());
        }
        Assert.assertEquals(N, map.size());

        // verify all values survive rehash
        rnd.reset();
        for (int i = 0; i < N; i++) {
            CharSequence cs = rnd.nextString(15);
            long expected = rnd.nextLong();
            Assert.assertEquals(expected, map.get(cs));
        }
    }

    @Test
    public void testValueAtWithKeyIndex() {
        CharSequenceLongHashMap map = new CharSequenceLongHashMap();
        map.put("alpha", 10);
        map.put("beta", 20);

        int indexAlpha = map.keyIndex("alpha");
        Assert.assertTrue(indexAlpha < 0);
        Assert.assertEquals(10, map.valueAt(indexAlpha));

        int indexBeta = map.keyIndex("beta");
        Assert.assertTrue(indexBeta < 0);
        Assert.assertEquals(20, map.valueAt(indexBeta));

        // missing key returns a positive index
        int indexMissing = map.keyIndex("gamma");
        Assert.assertTrue(indexMissing >= 0);
        Assert.assertEquals(CharSequenceLongHashMap.NO_ENTRY_VALUE, map.valueAt(indexMissing));
    }
}
