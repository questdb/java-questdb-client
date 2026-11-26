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

package io.questdb.client.test.std;

import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.NumericException;
import org.junit.Assert;
import org.junit.Test;

public class DecimalExponentLimitsTest {

    @Test
    public void testExponentAtIntLimitsIsOutOfRange() {
        for (String value : new String[]{"1e2147483647", "-1e2147483647", "1e-2147483648", "1.5e-2147483647"}) {
            assertOutOfRange(value, () -> new Decimal64().ofString(value));
            assertOutOfRange(value, () -> new Decimal128().ofString(value));
            assertOutOfRange(value, () -> new Decimal256().ofString(value));
        }
    }

    private static void assertOutOfRange(String value, Runnable parse) {
        try {
            parse.run();
            Assert.fail("expected NumericException for " + value);
        } catch (NumericException expected) {
            // Out-of-range decimal text must be rejected with NumericException.
        }
    }
}
