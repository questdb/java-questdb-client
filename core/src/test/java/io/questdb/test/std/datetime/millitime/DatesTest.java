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

package io.questdb.test.std.datetime.millitime;

import io.questdb.std.NumericException;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DatesTest {

    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testAddDaysPrevEpoch() {
        long millis = DateFormatUtils.parseUTCDate("1888-05-12T23:45:51.045Z");
        DateFormatUtils.appendDateTime(sink, Dates.addDays(millis, 24));
        TestUtils.assertEquals("1888-06-05T23:45:51.045Z", sink);
    }

    @Test
    public void testDayOfWeek() {
        long millis = DateFormatUtils.parseUTCDate("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(7, Dates.getDayOfWeek(millis));
        Assert.assertEquals(1, Dates.getDayOfWeekSundayFirst(millis));
        millis = DateFormatUtils.parseUTCDate("2017-04-09T17:16:30.192Z");
        Assert.assertEquals(7, Dates.getDayOfWeek(millis));
        Assert.assertEquals(1, Dates.getDayOfWeekSundayFirst(millis));
    }

    @Test
    public void testDayOfYear() {
        long millis = DateFormatUtils.parseUTCDate("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(69, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(70, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(78, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(366, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(365, Dates.getDayOfYear(millis));

        millis = DateFormatUtils.parseUTCDate("-2021-12-31T12:00:00.000Z");
        Assert.assertEquals(365, Dates.getDayOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("-2020-12-31T12:00:00.000Z");
        Assert.assertEquals(366, Dates.getDayOfYear(millis));
    }

    @Test
    public void testNExtOrSameDow3() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.nextOrSameDayOfWeek(millis, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000Z", sink);
    }

    @Test
    public void testNextOrSameDow1() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.nextOrSameDayOfWeek(millis, 3));
        TestUtils.assertEquals("2017-04-12T00:00:00.000Z", sink);
    }

    @Test
    public void testNextOrSameDow2() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.nextOrSameDayOfWeek(millis, 6));
        TestUtils.assertEquals("2017-04-08T00:00:00.000Z", sink);
    }

    @Test
    public void testOverflowDate() {
        Assert.assertEquals("6477-07-27T03:15:50.400Z", Dates.toString(142245170150400L));
    }

    @Test
    public void testParseBadISODate() {
        expectExceptionDateTime("201");
        expectExceptionDateTime("2014");
        expectExceptionDateTime("2014-");
        expectExceptionDateTime("2014-0");
        expectExceptionDateTime("2014-03");
        expectExceptionDateTime("2014-03-");
        expectExceptionDateTime("2014-03-1");
        expectExceptionDateTime("2014-03-10");
        expectExceptionDateTime("2014-03-10T0");
        expectExceptionDateTime("2014-03-10T01");
        expectExceptionDateTime("2014-03-10T01-");
        expectExceptionDateTime("2014-03-10T01:1");
        expectExceptionDateTime("2014-03-10T01:19");
        expectExceptionDateTime("2014-03-10T01:19:");
        expectExceptionDateTime("2014-03-10T01:19:28.");
        expectExceptionDateTime("2014-03-10T01:19:28.2");
        expectExceptionDateTime("2014-03-10T01:19:28.255K");
    }

    @Test
    public void testParseDateTime() {
        String date = "2008-02-29T10:54:01.010Z";
        DateFormatUtils.appendDateTime(sink, DateFormatUtils.parseUTCDate(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test
    public void testParseDateTimePrevEpoch() {
        String date = "1812-02-29T10:54:01.010Z";
        DateFormatUtils.appendDateTime(sink, DateFormatUtils.parseUTCDate(date));
        TestUtils.assertEquals(date, sink);
    }

    @Test(expected = NumericException.class)
    public void testParseWrongDay() {
        DateFormatUtils.parseUTCDate("2013-09-31T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHour() {
        DateFormatUtils.parseUTCDate("2013-09-30T25:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMillis() {
        DateFormatUtils.parseUTCDate("2013-09-30T22:04:34.1024Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMinute() {
        DateFormatUtils.parseUTCDate("2013-09-30T22:61:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongMonth() {
        DateFormatUtils.parseUTCDate("2013-00-12T00:00:00.000Z");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongSecond() {
        DateFormatUtils.parseUTCDate("2013-09-30T22:04:60.000Z");
    }

    @Test
    public void testPreviousOrSameDow1() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.previousOrSameDayOfWeek(millis, 3));
        TestUtils.assertEquals("2017-04-05T00:00:00.000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow2() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.previousOrSameDayOfWeek(millis, 6));
        TestUtils.assertEquals("2017-04-01T00:00:00.000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow3() {
        // thursday
        long millis = DateFormatUtils.parseUTCDate("2017-04-06T00:00:00.000Z");
        DateFormatUtils.appendDateTime(sink, Dates.previousOrSameDayOfWeek(millis, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000Z", sink);
    }

    @Test
    public void testWeekOfMonth() {
        long millis = DateFormatUtils.parseUTCDate("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(2, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("-2019-03-10T07:16:30.192Z");
        Assert.assertEquals(2, Dates.getWeekOfMonth(millis));
        millis = DateFormatUtils.parseUTCDate("-2020-12-31T12:00:00.000Z");
        Assert.assertEquals(5, Dates.getWeekOfMonth(millis));
    }

    @Test
    public void testWeekOfYear() {
        long millis = DateFormatUtils.parseUTCDate("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(10, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(11, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(12, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("-2020-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
        millis = DateFormatUtils.parseUTCDate("-2021-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Dates.getWeekOfYear(millis));
    }

    private void expectExceptionDateTime(String s) {
        try {
            DateFormatUtils.parseUTCDate(s);
            Assert.fail("Expected exception");
        } catch (NumericException ignore) {
        }
    }
}
