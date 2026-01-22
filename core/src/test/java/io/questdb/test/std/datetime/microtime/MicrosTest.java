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

package io.questdb.test.std.datetime.microtime;

import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.std.datetime.microtime.MicrosFormatUtils.parseHTTP;

public class MicrosTest {
    private final StringSink sink = new StringSink();

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    public void testFloorMM() {
        testFloorMM("1961-01-12T23:45:51.123123Z", "1961-01-01T00:00:00.000000Z");
        testFloorMM("1969-01-12T23:45:51.123123Z", "1969-01-01T00:00:00.000000Z");
        testFloorMM("1969-06-15T01:01:00.345345Z", "1969-06-01T00:00:00.000000Z");
        testFloorMM("1970-01-12T23:45:51.123123Z", "1970-01-01T00:00:00.000000Z");
        testFloorMM("1970-02-12T23:45:51.045045Z", "1970-02-01T00:00:00.000000Z");
        testFloorMM("1970-11-12T23:45:51.123123Z", "1970-11-01T00:00:00.000000Z");
        testFloorMM("2008-05-12T23:45:51.045045Z", "2008-05-01T00:00:00.000000Z");
        testFloorMM("2022-02-22T20:18:30.283283Z", "2022-02-01T00:00:00.000000Z");
    }

    @Test
    public void testFloorNS() {
        testFloorNS("1969-12-31T23:59:59.999999Z", "1969-12-31T23:59:59.999999Z");
        testFloorNS("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorNS("1969-01-01T12:13:14.567567Z", "1969-01-01T12:13:14.567567Z");
        testFloorNS("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorNS("2008-05-12T23:45:51.045045Z", "2008-05-12T23:45:51.045045Z");
        testFloorNS("2025-09-03T23:59:59.999999Z", "2025-09-03T23:59:59.999999Z");
    }

    @Test
    public void testFloorWW() {
        testFloorWW("1969-01-01T00:00:00.000000Z", "1968-12-30T00:00:00.000000Z");
        testFloorWW("1970-01-01T00:00:00.000000Z", "1969-12-29T00:00:00.000000Z");
        testFloorWW("2025-01-02T23:59:59.999999Z", "2024-12-30T00:00:00.000000Z");
        testFloorWW("2025-09-01T00:00:00.000000Z", "2025-09-01T00:00:00.000000Z");
        testFloorWW("2025-09-02T13:59:59.999999Z", "2025-09-01T00:00:00.000000Z");
    }

    @Test
    public void testFloorYYYY() {
        testFloorYYYY("1969-01-01T00:00:00.000000Z", "1969-01-01T00:00:00.000000Z");
        testFloorYYYY("1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00.000000Z");
        testFloorYYYY("2008-05-12T23:45:51.045045Z", "2008-01-01T00:00:00.000000Z");
        testFloorYYYY("2025-12-31T23:59:59.999999Z", "2025-01-01T00:00:00.000000Z");
    }

    @Test
    public void testGetDayOfTheWeekOfEndOfYear() {
        Assert.assertEquals(0, Micros.getDayOfTheWeekOfEndOfYear(2017));
        Assert.assertEquals(1, Micros.getDayOfTheWeekOfEndOfYear(1984));
        Assert.assertEquals(2, Micros.getDayOfTheWeekOfEndOfYear(2019));
        Assert.assertEquals(3, Micros.getDayOfTheWeekOfEndOfYear(2014));
        Assert.assertEquals(4, Micros.getDayOfTheWeekOfEndOfYear(2020));
        Assert.assertEquals(5, Micros.getDayOfTheWeekOfEndOfYear(2021));
        Assert.assertEquals(6, Micros.getDayOfTheWeekOfEndOfYear(1994));
    }

    @Test
    public void testGetIsoYearDayOffset() {
        Assert.assertEquals(-3, CommonUtils.getIsoYearDayOffset(2015));
        Assert.assertEquals(3, CommonUtils.getIsoYearDayOffset(2016));
        Assert.assertEquals(1, CommonUtils.getIsoYearDayOffset(2017));
        Assert.assertEquals(0, CommonUtils.getIsoYearDayOffset(2018));
        Assert.assertEquals(-1, CommonUtils.getIsoYearDayOffset(2019));
        Assert.assertEquals(-2, CommonUtils.getIsoYearDayOffset(2020));
        Assert.assertEquals(3, CommonUtils.getIsoYearDayOffset(2021));
        Assert.assertEquals(2, CommonUtils.getIsoYearDayOffset(2022));
    }

    @Test
    public void testGetWeek() {
        long micros = MicrosFormatUtils.parseTimestamp("2017-12-31T13:32:12.531Z");
        Assert.assertEquals(52, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2018-01-01T03:32:12.531Z");
        Assert.assertEquals(1, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-20T13:32:12.531Z");
        Assert.assertEquals(51, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-23T13:32:12.531Z");
        Assert.assertEquals(52, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-01-01T13:32:12.531Z");
        Assert.assertEquals(53, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-01-04T13:32:12.531Z");
        Assert.assertEquals(1, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2022-01-09T13:32:12.531Z");
        Assert.assertEquals(1, Micros.getWeek(micros));
        micros = MicrosFormatUtils.parseTimestamp("2022-01-10T13:32:12.531Z");
        Assert.assertEquals(2, Micros.getWeek(micros));
    }

    @Test
    public void testGetWeeks() {
        Assert.assertEquals(52, CommonUtils.getWeeks(2017));
        Assert.assertEquals(52, CommonUtils.getWeeks(2021));
        Assert.assertEquals(53, CommonUtils.getWeeks(2020));
    }

    @Test
    public void testNExtOrSameDow3() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.nextOrSameDayOfWeek(micros, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000000Z", sink);
    }

    @Test
    public void testNextOrSameDow1() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.nextOrSameDayOfWeek(micros, 3));
        TestUtils.assertEquals("2017-04-12T00:00:00.000000Z", sink);
    }

    @Test
    public void testNextOrSameDow2() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.nextOrSameDayOfWeek(micros, 6));
        TestUtils.assertEquals("2017-04-08T00:00:00.000000Z", sink);
    }

    @Test
    public void testParseBadISODate() {
        expectExceptionDateTime("201");
        expectExceptionDateTime("2014-");
        expectExceptionDateTime("2014-0");
        expectExceptionDateTime("2014-03-");
        expectExceptionDateTime("2014-03-1");
        expectExceptionDateTime("2014-03-10T0");
        expectExceptionDateTime("2014-03-10T01-");
        expectExceptionDateTime("2014-03-10T01:1");
        expectExceptionDateTime("2014-03-10T01:19");
        expectExceptionDateTime("2014-03-10T01:19:");
        expectExceptionDateTime("2014-03-10T01:19:28.");
        expectExceptionDateTime("2014-03-10T01:19:28.2");
        expectExceptionDateTime("2014-03-10T01:19:28.255K");
    }

    @Test
    public void testParseHttp() throws NumericException {
        Assert.assertEquals(1744545248000000L, parseHTTP("Sun, 13 Apr 2025 11:54:08 GMT"));
        Assert.assertEquals(1744545248000000L, parseHTTP("Sun, 13-Apr-2025 11:54:08 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 07 Mar 2025 19:23:19 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 07-Mar-2025 19:23:19 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 7 Mar 2025 19:23:19 GMT"));
        Assert.assertEquals(1741375399000000L, parseHTTP("Fri, 7-Mar-2025 19:23:19 GMT"));
        // HTTP 1.0 format with 2-digit year
        Assert.assertEquals(1760975876000000L, parseHTTP("Mon, 20-Oct-25 15:57:56 GMT"));
        Assert.assertEquals(1744545248000000L, parseHTTP("Sun, 13-Apr-25 11:54:08 GMT"));
        // ANSI C asctime format
        Assert.assertEquals(1760975876000000L, parseHTTP("Mon Oct 20 15:57:56 2025"));
        Assert.assertEquals(1744545248000000L, parseHTTP("Sun Apr 13 11:54:08 2025"));
        Assert.assertEquals(784111777000000L, parseHTTP("Sun Nov  6 08:49:37 1994"));
    }

    @Test
    public void testParseTimestampNotNullLocale() {
        try {
            // we deliberately mangle timezone so that function begins to rely on locale to resolve text
            MicrosFormatUtils.parseTimestamp("2020-01-10T15:00:01.000143Zz");
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    @Test
    public void testParseWrongDay() {
        Assert.assertThrows(NumericException.class, () -> {
            MicrosFormatUtils.parseTimestamp("2013-09-31T00:00:00.000Z");
        });
    }

    @Test
    public void testParseWrongHour() {
        Assert.assertThrows(NumericException.class, () -> {
            MicrosFormatUtils.parseTimestamp("2013-09-30T25:00:00.000Z");
        });
    }

    @Test
    public void testParseWrongMicros() {
        Assert.assertThrows(NumericException.class, () -> {
            MicrosFormatUtils.parseTimestamp("2013-09-30T22:04:34.1024091Z");
        });
    }

    @Test
    public void testParseWrongMinute() {
        Assert.assertThrows(NumericException.class, () -> {
            MicrosFormatUtils.parseTimestamp("2013-09-30T22:61:00.000Z");
        });
    }

    @Test
    public void testParseWrongMonth() {
        Assert.assertThrows(NumericException.class, () -> {
            MicrosFormatUtils.parseTimestamp("2013-00-12T00:00:00.000Z");
        });
    }

    @Test
    public void testParseWrongSecond() {
        Assert.assertThrows(NumericException.class, () -> {
            MicrosFormatUtils.parseTimestamp("2013-09-30T22:04:60.000Z");
        });
    }

    @Test
    public void testPreviousOrSameDow1() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.previousOrSameDayOfWeek(micros, 3));
        TestUtils.assertEquals("2017-04-05T00:00:00.000000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow2() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.previousOrSameDayOfWeek(micros, 6));
        TestUtils.assertEquals("2017-04-01T00:00:00.000000Z", sink);
    }

    @Test
    public void testPreviousOrSameDow3() {
        // thursday
        long micros = MicrosFormatUtils.parseTimestamp("2017-04-06T00:00:00.000000Z");
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.previousOrSameDayOfWeek(micros, 4));
        TestUtils.assertEquals("2017-04-06T00:00:00.000000Z", sink);
    }

    @Test
    public void testWeekOfYear() {
        long micros = MicrosFormatUtils.parseTimestamp("2020-01-01T17:16:30.192Z");
        Assert.assertEquals(1, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2019-03-10T07:16:30.192Z");
        Assert.assertEquals(10, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-03-10T07:16:30.192Z");
        Assert.assertEquals(11, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("1893-03-19T17:16:30.192Z");
        Assert.assertEquals(12, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2020-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Micros.getWeekOfYear(micros));
        micros = MicrosFormatUtils.parseTimestamp("2021-12-31T12:00:00.000Z");
        Assert.assertEquals(53, Micros.getWeekOfYear(micros));
    }

    private void expectExceptionDateTime(String s) {
        try {
            MicrosFormatUtils.parseTimestamp(s);
            Assert.fail("Expected exception");
        } catch (NumericException ignore) {
        }
    }

    private void testFloorMM(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorMM(micros));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorNS(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorNS(micros, 1));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorWW(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorWW(micros));
        TestUtils.assertEquals(expected, sink);
        sink.clear();
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorWW(micros, 1));
        TestUtils.assertEquals(expected, sink);
    }

    private void testFloorYYYY(String timestamp, String expected) {
        sink.clear();
        final long micros = MicrosFormatUtils.parseTimestamp(timestamp);
        MicrosFormatUtils.appendDateTimeUSec(sink, Micros.floorYYYY(micros));
        TestUtils.assertEquals(expected, sink);
    }
}
