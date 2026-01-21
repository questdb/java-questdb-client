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

package io.questdb.client.test.cairo;

import io.questdb.client.cairo.NanosTimestampDriver;
import io.questdb.client.cairo.TimestampDriver;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

public class NanoTimestampDriverTest {
    TimestampDriver driver = NanosTimestampDriver.INSTANCE;

    @Test
    public void testFromChronosUnit() {
        Assert.assertEquals(1_000, driver.from(1000, ChronoUnit.NANOS));
        Assert.assertEquals(1_000, driver.from(1, ChronoUnit.MICROS));
        Assert.assertEquals(1000_000, driver.from(1, ChronoUnit.MILLIS));
        Assert.assertEquals(1_000_000_000, driver.from(1, ChronoUnit.SECONDS));

        Assert.assertEquals(60 * 1000 * 1000 * 1000L, driver.from(1, ChronoUnit.MINUTES));
        Assert.assertEquals(Long.MAX_VALUE, driver.from(Long.MAX_VALUE, ChronoUnit.NANOS));

        Assert.assertEquals(driver.from(1, ChronoUnit.HOURS), driver.from(60, ChronoUnit.MINUTES));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.NANOS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MICROS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MILLIS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.SECONDS));
        Assert.assertEquals(0, driver.from(0, ChronoUnit.MINUTES));

        // nanos values remain unchanged
        Assert.assertEquals(123456789L, driver.from(123456789L, ChronoUnit.NANOS));
        Assert.assertEquals(123456789001L, driver.from(123456789001L, ChronoUnit.NANOS));
        try {
            driver.from(Long.MAX_VALUE, ChronoUnit.SECONDS);
            Assert.fail();
        } catch (ArithmeticException e) {
            Assert.assertTrue(e.getMessage().contains("long overflow"));
        }
    }
}
