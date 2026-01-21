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

import io.questdb.client.std.Os;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class OsTest {
    @Test
    public void testCurrentTimeMicros() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeMicros();
        long delta = actual / 1000 - reference;
        assertTrue(delta < 200);
    }

    @Test
    public void testCurrentTimeNanos() {
        long reference = System.currentTimeMillis();
        long actual = Os.currentTimeNanos();
        assertTrue(actual > 0);
        long delta = actual / 1_000_000 - reference;
        assertTrue(delta < 200);
    }
}
