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

package io.questdb.client.test.std;

import io.questdb.client.std.LongList;
import org.junit.Assert;
import org.junit.Test;

public class LongListTest {
    @Test
    public void testEquals() {
        final LongList list1 = new LongList();
        list1.add(1L);
        list1.add(2L);
        list1.add(3L);

        // different order
        final LongList list2 = new LongList();
        list2.add(1L);
        list2.add(3L);
        list2.add(2L);
        Assert.assertNotEquals(list1, list2);

        // longer
        final LongList list3 = new LongList();
        list3.add(1L);
        list3.add(2L);
        list3.add(3L);
        list3.add(4L);
        Assert.assertNotEquals(list1, list3);

        // shorter
        final LongList list4 = new LongList();
        list4.add(1L);
        list4.add(2L);
        Assert.assertNotEquals(list1, list4);

        // empty
        final LongList list5 = new LongList();
        Assert.assertNotEquals(list1, list5);

        // null
        Assert.assertNotEquals(null, list1);

        // different noEntryValue
        final LongList list6 = new LongList(4, Long.MIN_VALUE);
        list6.add(1L);
        list6.add(2L);
        list6.add(3L);
        Assert.assertNotEquals(list1, list6);

        // equals
        final LongList list7 = new LongList();
        list7.add(1L);
        list7.add(2L);
        list7.add(3L);
        Assert.assertEquals(list1, list7);

        final LongList list8 = new LongList(4, -1L);
        list8.add(1L);
        list8.add(2L);
        list8.add(3L);
        Assert.assertEquals(list1, list8);
    }
}
