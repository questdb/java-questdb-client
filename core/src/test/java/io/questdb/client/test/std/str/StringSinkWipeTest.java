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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

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
    public void testGrowthZeroesTheBufferItAbandons() throws Exception {
        // wipe() can only reach the buffer the sink currently holds. Growth replaces that buffer, so every
        // generation left behind used to keep its contents legible on the heap - the collector is under no
        // obligation to overwrite them, and a heap dump taken meanwhile shows the lot.
        final Field bufferField = StringSink.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);

        StringSink sink = new StringSink(16);
        // exactly fills the initial buffer, so no growth has happened yet
        sink.put("SECRET-0123456789".substring(0, 16));
        final char[] abandoned = (char[]) bufferField.get(sink);
        Assert.assertEquals(16, abandoned.length);
        Assert.assertEquals("precondition: the secret really is in this array",
                "SECRET-012345678", new String(abandoned));

        // one more character forces the grow-and-copy
        sink.put('9');
        Assert.assertNotSame("precondition: the sink must have moved to a new array",
                abandoned, bufferField.get(sink));

        Assert.assertEquals("the array growth abandoned still holds the secret it carried: "
                        + new String(abandoned).trim(),
                "", new String(abandoned).replace((char) 0, ' ').trim());
        // and the live sink is intact
        Assert.assertEquals("SECRET-0123456789", sink.toString());
    }

    @Test
    public void testNoGenerationKeepsTheTokenAfterAFormBodyIsBuiltAndWiped() throws Exception {
        // The shape that matters: OidcDeviceAuth's formSink is a default 16-char sink that builds the
        // refresh POST body. It is already holding the whole refresh token by the time the later parameters
        // make it grow again, so each hand-off carried a full copy - and wipe() at close() reached only the
        // last one.
        final String token = "REFRESH-TOKEN-abcdef0123456789";
        final Field bufferField = StringSink.class.getDeclaredField("buffer");
        bufferField.setAccessible(true);

        StringSink formSink = new StringSink();
        final List<char[]> generations = new ArrayList<>();
        generations.add((char[]) bufferField.get(formSink));

        // Sampled after EVERY write, not at the end: once the sink has moved on, the array it abandoned is
        // unreachable from it, which is precisely why wipe() cannot clean them and why this has to catch
        // each one as it goes.
        final String[] body = {
                "grant_type=refresh_token",
                "&refresh_token=", token,
                "&client_id=questdb",
                "&scope=openid+profile+email",
        };
        for (String part : body) {
            formSink.put(part);
            final char[] live = (char[]) bufferField.get(formSink);
            if (generations.get(generations.size() - 1) != live) {
                generations.add(live);
            }
        }
        Assert.assertTrue("precondition: the sink must have grown at least twice while holding the token, "
                        + "or this test is not exercising the hand-off it is about",
                generations.size() >= 3);

        formSink.wipe();

        for (int i = 0; i < generations.size(); i++) {
            final String contents = new String(generations.get(i));
            Assert.assertFalse(
                    "generation " + i + " of " + generations.size() + " still holds the refresh token after "
                            + "wipe(); it was abandoned by growth, so wipe() never reached it: "
                            + contents.replace((char) 0, '.'),
                    contents.contains(token));
        }
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
