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

import io.questdb.client.cutlass.qwp.client.sf.cursor.SyntheticWireSeqs;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SyntheticWireSeqsTest {

    @Test
    public void testAddRejectsANonIncreasingSeq() {
        SyntheticWireSeqs s = new SyntheticWireSeqs();
        s.add(5);
        try {
            s.add(5);
            org.junit.Assert.fail();
        } catch (IllegalStateException expected) {
        }
        try {
            s.add(4);
            org.junit.Assert.fail();
        } catch (IllegalStateException expected) {
        }
    }

    @Test
    public void testClearForgetsEverything() {
        SyntheticWireSeqs s = new SyntheticWireSeqs();
        s.add(1);
        s.trimThrough(1);
        s.add(4);
        s.clear();
        assertEquals(0, s.total());
        assertEquals(0, s.countAtOrBelow(10));
        assertEquals(0, s.retained());
        s.add(0);
        assertEquals(1, s.total());
    }

    @Test
    public void testCountsSynthesisedSeqsAtOrBelow() {
        SyntheticWireSeqs s = new SyntheticWireSeqs();
        s.add(3);
        s.add(7);
        assertEquals(0, s.countAtOrBelow(2));
        assertEquals(1, s.countAtOrBelow(3));
        assertEquals(1, s.countAtOrBelow(6));
        assertEquals(2, s.countAtOrBelow(7));
        assertEquals(2, s.countAtOrBelow(100));
        assertEquals(2, s.total());
    }

    @Test
    public void testTrimKeepsCountsAtAndAboveTheTrimPoint() {
        SyntheticWireSeqs s = new SyntheticWireSeqs();
        s.add(3);
        s.add(7);
        s.add(9);
        s.trimThrough(7);
        assertEquals(1, s.retained());
        assertEquals(2, s.countAtOrBelow(7));
        assertEquals(2, s.countAtOrBelow(8));
        assertEquals(3, s.countAtOrBelow(9));
        assertEquals(3, s.total());
    }
}
