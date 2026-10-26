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
        a.commitFramesSynthesised.set(8);
        a.framesDeferred.set(9);
        a.framesReplayed.set(3);
        a.framesSent.set(4);
        a.groupsClosedByAge.set(11);
        a.groupsClosedByFrame.set(12);
        a.groupsClosedByRows.set(13);
        a.groupsClosedOnDemand.set(14);
        a.reconnectAttempts.set(5);
        a.reconnects.set(6);
        a.serverErrors.set(7);
        a.terminalReplays.set(15);

        CursorSendCounters b = new CursorSendCounters();
        b.acks.set(10);
        b.backpressureStalls.set(20);
        b.commitFramesSynthesised.set(80);
        b.framesDeferred.set(90);
        b.framesReplayed.set(30);
        b.framesSent.set(40);
        b.groupsClosedByAge.set(110);
        b.groupsClosedByFrame.set(120);
        b.groupsClosedByRows.set(130);
        b.groupsClosedOnDemand.set(140);
        b.reconnectAttempts.set(50);
        b.reconnects.set(60);
        b.serverErrors.set(70);
        b.terminalReplays.set(150);

        a.addAll(b);

        assertEquals("acks must sum both sides", 11, a.acks.get());
        assertEquals("backpressureStalls must sum both sides", 22, a.backpressureStalls.get());
        assertEquals("commitFramesSynthesised must sum both sides", 88, a.commitFramesSynthesised.get());
        assertEquals("framesDeferred must sum both sides", 99, a.framesDeferred.get());
        assertEquals("framesReplayed must sum both sides", 33, a.framesReplayed.get());
        assertEquals("framesSent must sum both sides", 44, a.framesSent.get());
        assertEquals("groupsClosedByAge must sum both sides", 121, a.groupsClosedByAge.get());
        assertEquals("groupsClosedByFrame must sum both sides", 132, a.groupsClosedByFrame.get());
        assertEquals("groupsClosedByRows must sum both sides", 143, a.groupsClosedByRows.get());
        assertEquals("groupsClosedOnDemand must sum both sides", 154, a.groupsClosedOnDemand.get());
        assertEquals("reconnectAttempts must sum both sides", 55, a.reconnectAttempts.get());
        assertEquals("reconnects must sum both sides", 66, a.reconnects.get());
        assertEquals("serverErrors must sum both sides", 77, a.serverErrors.get());
        assertEquals("terminalReplays must sum both sides", 165, a.terminalReplays.get());

        assertEquals("addAll must not mutate the argument's acks", 10, b.acks.get());
        assertEquals("addAll must not mutate the argument's backpressureStalls", 20, b.backpressureStalls.get());
        assertEquals("addAll must not mutate the argument's commitFramesSynthesised", 80, b.commitFramesSynthesised.get());
        assertEquals("addAll must not mutate the argument's framesDeferred", 90, b.framesDeferred.get());
        assertEquals("addAll must not mutate the argument's framesReplayed", 30, b.framesReplayed.get());
        assertEquals("addAll must not mutate the argument's framesSent", 40, b.framesSent.get());
        assertEquals("addAll must not mutate the argument's groupsClosedByAge", 110, b.groupsClosedByAge.get());
        assertEquals("addAll must not mutate the argument's groupsClosedByFrame", 120, b.groupsClosedByFrame.get());
        assertEquals("addAll must not mutate the argument's groupsClosedByRows", 130, b.groupsClosedByRows.get());
        assertEquals("addAll must not mutate the argument's groupsClosedOnDemand", 140, b.groupsClosedOnDemand.get());
        assertEquals("addAll must not mutate the argument's reconnectAttempts", 50, b.reconnectAttempts.get());
        assertEquals("addAll must not mutate the argument's reconnects", 60, b.reconnects.get());
        assertEquals("addAll must not mutate the argument's serverErrors", 70, b.serverErrors.get());
        assertEquals("addAll must not mutate the argument's terminalReplays", 150, b.terminalReplays.get());
    }
}
