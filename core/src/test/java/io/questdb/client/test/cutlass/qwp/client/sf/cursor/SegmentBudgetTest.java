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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import org.junit.Assert;
import org.junit.Test;

/**
 * How many senders a {@link SegmentBudget} is declared for: a sender sizes the
 * strict transactions it accepts against its share of the budget.
 */
public class SegmentBudgetTest {

    @Test
    public void testCapacityIsValidatedBeforeSharers() {
        assertRejected(0, 0, "capacityBytes must be positive: 0");
        assertRejected(-1, 2, "capacityBytes must be positive: -1");
    }

    @Test
    public void testMaxSharersDefaultsToOne() {
        // A budget built for one sender, as every sender built on its own gets.
        Assert.assertEquals(1, new SegmentBudget(1).getMaxSharers());
        Assert.assertEquals(1, new SegmentBudget(Long.MAX_VALUE).getMaxSharers());
    }

    @Test
    public void testMaxSharersIsKept() {
        Assert.assertEquals(1, new SegmentBudget(1024, 1).getMaxSharers());
        Assert.assertEquals(2, new SegmentBudget(1024, 2).getMaxSharers());
        Assert.assertEquals(Integer.MAX_VALUE, new SegmentBudget(1024, Integer.MAX_VALUE).getMaxSharers());
    }

    @Test
    public void testMaxSharersMustBePositive() {
        assertRejected(1024, 0, "maxSharers must be positive: 0");
        assertRejected(1024, -1, "maxSharers must be positive: -1");
        assertRejected(1024, Integer.MIN_VALUE, "maxSharers must be positive: " + Integer.MIN_VALUE);
    }

    private static void assertRejected(long capacityBytes, int maxSharers, String expectedMessage) {
        try {
            new SegmentBudget(capacityBytes, maxSharers);
            Assert.fail("must reject capacityBytes=" + capacityBytes + ", maxSharers=" + maxSharers);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }
}
