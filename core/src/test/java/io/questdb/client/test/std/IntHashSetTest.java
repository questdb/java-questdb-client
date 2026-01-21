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

import io.questdb.client.std.IntHashSet;
import io.questdb.client.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

public class IntHashSetTest {

    @Test
    public void testBasicOperations() {
        Rnd rnd = new Rnd();
        IntHashSet set = new IntHashSet();
        final int N = 1000;

        for (int i = 0; i < N; i++) {
            set.add(rnd.nextPositiveInt());
        }

        Assert.assertEquals(N, set.size());

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.keyIndex(rnd.nextPositiveInt()) < 0);
        }

        rnd.reset();

        for (int i = 0; i < N; i++) {
            Assert.assertTrue(set.contains(rnd.nextPositiveInt()));
        }
    }
}
