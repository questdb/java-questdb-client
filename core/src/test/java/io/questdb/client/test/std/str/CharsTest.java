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

package io.questdb.client.test.std.str;

import io.questdb.client.std.Chars;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CharsTest {
    @Test
    public void testEqualsLowerCaseAscii() {
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo bar baz", "foo bar baz"));
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo bar baz", "FoO bAr BaZ"));
        Assert.assertTrue(Chars.equalsLowerCaseAscii("foo_bar_baz", "foo_BAR_baz"));
        Assert.assertFalse(Chars.equalsLowerCaseAscii("foo_bar_baz", "foo_foo_baz"));
    }

    @Test
    public void testIPv4ToString() {
        Assert.assertEquals("255.255.255.255", TestUtils.ipv4ToString(0xffffffff));
        Assert.assertEquals("0.0.0.25", TestUtils.ipv4ToString(25));
    }

    @Test
    public void testIsBlank() {
        Assert.assertTrue(Chars.isBlank(null));
        Assert.assertTrue(Chars.isBlank(""));
        Assert.assertTrue(Chars.isBlank(" "));
        Assert.assertTrue(Chars.isBlank("      "));
        Assert.assertTrue(Chars.isBlank("\r\f\n\t"));

        Assert.assertFalse(Chars.isBlank("a"));
        Assert.assertFalse(Chars.isBlank("0"));
        Assert.assertFalse(Chars.isBlank("\\"));
        Assert.assertFalse(Chars.isBlank("\\r"));
        Assert.assertFalse(Chars.isBlank("ac/dc"));
    }
}
