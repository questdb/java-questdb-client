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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendCounters;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CursorSendCountersTest {

    @Test
    public void testAddAllFoldsEveryFieldWithNonZeroValuesOnBothSides() {
        CursorSendCounters a = new CursorSendCounters();
        a.acks.set(1);
        a.backpressureStalls.set(2);
        a.framesReplayed.set(3);
        a.framesSent.set(4);
        a.reconnectAttempts.set(5);
        a.reconnects.set(6);
        a.serverErrors.set(7);

        CursorSendCounters b = new CursorSendCounters();
        b.acks.set(10);
        b.backpressureStalls.set(20);
        b.framesReplayed.set(30);
        b.framesSent.set(40);
        b.reconnectAttempts.set(50);
        b.reconnects.set(60);
        b.serverErrors.set(70);

        a.addAll(b);

        assertEquals("acks must sum both sides", 11, a.acks.get());
        assertEquals("backpressureStalls must sum both sides", 22, a.backpressureStalls.get());
        assertEquals("framesReplayed must sum both sides", 33, a.framesReplayed.get());
        assertEquals("framesSent must sum both sides", 44, a.framesSent.get());
        assertEquals("reconnectAttempts must sum both sides", 55, a.reconnectAttempts.get());
        assertEquals("reconnects must sum both sides", 66, a.reconnects.get());
        assertEquals("serverErrors must sum both sides", 77, a.serverErrors.get());

        assertEquals("addAll must not mutate the argument's acks", 10, b.acks.get());
        assertEquals("addAll must not mutate the argument's backpressureStalls", 20, b.backpressureStalls.get());
        assertEquals("addAll must not mutate the argument's framesReplayed", 30, b.framesReplayed.get());
        assertEquals("addAll must not mutate the argument's framesSent", 40, b.framesSent.get());
        assertEquals("addAll must not mutate the argument's reconnectAttempts", 50, b.reconnectAttempts.get());
        assertEquals("addAll must not mutate the argument's reconnects", 60, b.reconnects.get());
        assertEquals("addAll must not mutate the argument's serverErrors", 70, b.serverErrors.get());
    }
}
