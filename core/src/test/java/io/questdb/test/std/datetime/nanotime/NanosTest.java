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

package io.questdb.test.std.datetime.nanotime;

import io.questdb.std.datetime.nanotime.Nanos;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NanosTest {
    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testWallHour() {
        Assert.assertEquals(19, Nanos.getWallHours(1592078287051004114L));
        Assert.assertEquals(15, Nanos.getWallHours(1592063943181693345L));
        Assert.assertEquals(8, Nanos.getWallHours(-1592063943181693876L));
    }
}
