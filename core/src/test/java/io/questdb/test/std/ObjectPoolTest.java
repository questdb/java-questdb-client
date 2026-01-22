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

package io.questdb.test.std;

import io.questdb.std.Mutable;
import io.questdb.std.ObjectPool;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ObjectPoolTest extends AbstractTest {
    private static final int INITIAL_SIZE = 4;
    private ObjectPool<TestObject> pool;

    @Before
    public void setUp() {
        pool = new ObjectPool<>(TestObject::new, INITIAL_SIZE);
    }

    @Test
    public void testBorrowAfterClear() {
        pool.next();
        pool.clear();

        TestObject obj2 = pool.next();
        Assert.assertNotNull("Should be able to borrow after clear", obj2);
    }

    @Test
    public void testNextReturnsNewObject() {
        TestObject obj = pool.next();
        Assert.assertNotNull("Pool should return non-null object", obj);
    }

    private static class TestObject implements Mutable {
        @Override
        public void clear() {
        }
    }
}
