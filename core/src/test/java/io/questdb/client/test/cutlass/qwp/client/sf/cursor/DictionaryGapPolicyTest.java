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

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import org.junit.Assert;
import org.junit.Test;

public class DictionaryGapPolicyTest {

    @Test
    public void testDictionaryGapIsRetriable() {
        // The gap verdict is a function of the server's per-connection dictionary size,
        // so the same frame succeeds once the catch-up has re-registered from id 0.
        // Latching it TERMINAL forbids the one recovery that works and strands the slot.
        Assert.assertEquals((byte) 0x0D, WebSocketResponse.STATUS_DICTIONARY_GAP);
        Assert.assertEquals(SenderError.Category.DICTIONARY_GAP,
                CursorWebSocketSendLoop.classify(WebSocketResponse.STATUS_DICTIONARY_GAP));
        Assert.assertEquals(SenderError.Policy.RETRIABLE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.DICTIONARY_GAP));
    }

    @Test
    public void testParseErrorStaysTerminal() {
        // A genuinely malformed frame must not become retriable as a side effect.
        Assert.assertEquals(SenderError.Policy.TERMINAL,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.PARSE_ERROR));
    }

    @Test
    public void testUnknownStatusFromANewerServerStaysRetriable() {
        // The fail-open rule is what makes adding a server status byte safe without a
        // version bump: an older client sees UNKNOWN and retries rather than latching.
        Assert.assertEquals(SenderError.Category.UNKNOWN,
                CursorWebSocketSendLoop.classify((byte) 0x7E));
        Assert.assertEquals(SenderError.Policy.RETRIABLE,
                CursorWebSocketSendLoop.defaultPolicyFor(SenderError.Category.UNKNOWN));
    }
}
