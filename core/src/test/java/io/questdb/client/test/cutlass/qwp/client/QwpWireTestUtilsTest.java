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

package io.questdb.client.test.cutlass.qwp.client;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Pins {@link QwpWireTestUtils#accumulateDeltaDictionary} against the real server's
 * DELTA_DICT_GAP behaviour: a delta whose start runs past the dictionary must be
 * rejected, not null-padded. A test oracle that models the pre-rejection server can
 * pass a sequence a live server would NACK -- see {@code accumulateDeltaDictionary}'s
 * javadoc for the full rationale.
 */
public class QwpWireTestUtilsTest {

    @Test
    public void testContiguousAppendAccepted() {
        List<String> dictionary = new ArrayList<String>();
        QwpWireTestUtils.accumulateDeltaDictionary(
                QwpWireTestUtils.buildDeltaFrame(0, "a", "b"), dictionary);
        QwpWireTestUtils.accumulateDeltaDictionary(
                QwpWireTestUtils.buildDeltaFrame(2, "c"), dictionary);
        Assert.assertEquals(3, dictionary.size());
        Assert.assertEquals("c", dictionary.get(2));
    }

    @Test
    public void testGapCanBeObservedWhenExplicitlyAllowed() {
        // Catch-up tiling assertions exist to PROVE there is no hole, so they must be
        // able to observe one rather than be stopped by it.
        List<String> dictionary = new ArrayList<String>();
        QwpWireTestUtils.accumulateDeltaDictionary(
                QwpWireTestUtils.buildDeltaFrame(0, "a"), dictionary, true);
        QwpWireTestUtils.accumulateDeltaDictionary(
                QwpWireTestUtils.buildDeltaFrame(3, "d"), dictionary, true);
        Assert.assertEquals(4, dictionary.size());
        Assert.assertNull(dictionary.get(1));
        Assert.assertNull(dictionary.get(2));
        Assert.assertEquals("d", dictionary.get(3));
    }

    @Test
    public void testGapIsRejectedNotPadded() {
        // The suite's only model of the server must match the server. Null-padding is
        // what the real decoder now rejects, and a permissive model cannot fail on the
        // regression class this branch is about.
        List<String> dictionary = new ArrayList<String>();
        QwpWireTestUtils.accumulateDeltaDictionary(
                QwpWireTestUtils.buildDeltaFrame(0, "a", "b"), dictionary);
        try {
            QwpWireTestUtils.accumulateDeltaDictionary(
                    QwpWireTestUtils.buildDeltaFrame(3, "d"), dictionary);
            Assert.fail("expected the fake decoder to reject a gapped delta");
        } catch (QwpWireTestUtils.DictionaryGapException expected) {
            Assert.assertEquals("a rejected frame must not grow the dictionary",
                    2, dictionary.size());
        }
    }
}
