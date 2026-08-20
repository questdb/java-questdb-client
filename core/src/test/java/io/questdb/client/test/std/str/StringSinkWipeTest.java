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

package io.questdb.client.test.std.str;

import io.questdb.client.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Covers {@link StringSink#wipe()}, the hygiene primitive the OIDC client uses to stop a token remaining
 * legible in a reusable sink after the instance that read it is closed.
 */
public class StringSinkWipeTest {

    @Test
    public void testClearLeavesTheTailLegibleAndWipeDoesNot() {
        // subSequence() reads the backing array directly rather than the write position, so it can see what
        // clear() left behind - which is exactly the retention wipe() exists to close, demonstrated here
        // rather than asserted.
        StringSink sink = new StringSink();
        sink.put("REFRESH-TOKEN-abcdef0123456789");
        final int held = sink.length();
        sink.clear();
        sink.put("ok"); // a short write after a long secret: the position rewinds, the characters do not

        Assert.assertEquals("ok", sink.toString());
        Assert.assertTrue("clear() only rewinds, so the tail is still readable: " + sink.subSequence(0, held),
                sink.subSequence(0, held).toString().contains("TOKEN-abcdef"));

        sink.wipe();

        Assert.assertEquals(0, sink.length());
        Assert.assertEquals("", sink.toString());
        Assert.assertFalse("wipe() must overwrite the whole buffer, not just rewind: "
                        + sink.subSequence(0, held),
                sink.subSequence(0, held).toString().contains("TOKEN"));
    }

    @Test
    public void testWipeLeavesTheSinkUsable() {
        // it is a hygiene step, not a teardown: the OIDC client wipes on clearCache() and keeps going
        StringSink sink = new StringSink();
        sink.put("secret");
        sink.wipe();
        sink.put("reused");
        Assert.assertEquals("reused", sink.toString());
        Assert.assertEquals(6, sink.length());
    }

    @Test
    public void testWipeOfAnEmptySinkIsANoOp() {
        StringSink sink = new StringSink();
        sink.wipe();
        Assert.assertEquals(0, sink.length());
        Assert.assertEquals("", sink.toString());
    }
}
