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

package io.questdb.client.test.std;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Rnd;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8Sequence;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.client.std.Numbers.ceilPow2;
import static org.junit.Assert.assertEquals;

public class NumbersTest {
    private final StringSink sink = new StringSink();
    private Rnd rnd;

    @Test
    public void appendHexPadded() {
        sink.clear();
        Numbers.appendHexPadded(sink, 0xff - 1, 2);
        TestUtils.assertEquals("00fe", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xff0, 4);
        TestUtils.assertEquals("00000ff0", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 1, 4);
        TestUtils.assertEquals("00000001", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xff - 1, 3);
        TestUtils.assertEquals("0000fe", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xff - 1, 1);
        TestUtils.assertEquals("fe", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0xffff, 0);
        TestUtils.assertEquals("ffff", sink);

        sink.clear();
        Numbers.appendHexPadded(sink, 0, 8);
        TestUtils.assertEquals("0000000000000000", sink);
    }

    @Test(expected = NumericException.class)
    public void parseExplicitDouble2() {
        Numbers.parseDouble("1234dx");
    }

    @Test
    public void parseExplicitLong() {
        assertEquals(10000L, Numbers.parseLong("10000L"));
    }

    @Test(expected = NumericException.class)
    public void parseExplicitLong2() {
        Numbers.parseLong("10000LL");
    }

    @Before
    public void setUp() {
        rnd = new Rnd();
        sink.clear();
    }

    @Test
    public void testAppendZeroLong256() {
        sink.clear();
        Numbers.appendLong256(0, 0, 0, 0, sink);
        TestUtils.assertEquals("0x00", sink);
    }

    @Test
    public void testCeilPow2() {
        assertEquals(16, ceilPow2(15));
        assertEquals(16, ceilPow2(16));
        assertEquals(32, ceilPow2(17));
    }

    @Test(expected = NumericException.class)
    public void testEmptyDouble() {
        Numbers.parseDouble("D");
    }

    @Test(expected = NumericException.class)
    public void testEmptyFloat() {
        Numbers.parseFloat("f");
    }

    @Test(expected = NumericException.class)
    public void testEmptyLong() {
        Numbers.parseLong("L");
    }

    @Test
    public void testFormatByte() {
        for (int i = 0; i < 1000; i++) {
            byte n = (byte) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            assertEquals(Byte.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatChar() {
        for (int i = 0; i < 1000; i++) {
            char n = (char) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatDouble2() {
        sink.clear();
        Numbers.append(sink, 0.8998893432);
        TestUtils.assertEquals("0.8998893432", sink);
    }

    @Test
    public void testFormatDoubleExp() {
        sink.clear();
        Numbers.append(sink, 112333.989922222);
        TestUtils.assertEquals("112333.989922222", sink);
    }

    @Test
    public void testFormatDoubleExp10() {
        sink.clear();
        Numbers.append(sink, 1.23E3);
        TestUtils.assertEquals("1230.0", sink);
    }

    @Test
    public void testFormatDoubleExp100() {
        sink.clear();
        Numbers.append(sink, 1.23E105);
        TestUtils.assertEquals("1.23E105", sink);
    }

    @Test
    public void testFormatDoubleExpNeg() {
        sink.clear();
        Numbers.append(sink, -8892.88001);
        TestUtils.assertEquals("-8892.88001", sink);
    }

    @Test
    public void testFormatDoubleFast() {
        sink.clear();
        Numbers.append(sink, -5.9522650387500933e18);
        TestUtils.assertEquals("-5.9522650387500933E18", sink);
    }

    @Test
    public void testFormatDoubleFastInteractive() {
        sink.clear();
        Numbers.append(sink, 0.872989018674569);
        TestUtils.assertEquals("0.872989018674569", sink);
    }

    @Test
    public void testFormatDoubleHugeZero() {
        sink.clear();
        Numbers.append(sink, -0.000000000000001);
        TestUtils.assertEquals("-1.0E-15", sink);
    }

    @Test
    public void testFormatDoubleInt() {
        sink.clear();
        Numbers.append(sink, 44556d);
        TestUtils.assertEquals("44556.0", sink);
    }

    @Test
    public void testFormatDoubleLargeExp() {
        sink.clear();
        Numbers.append(sink, 1123338789079878978979879d);
        TestUtils.assertEquals("1.123338789079879E24", sink);
    }

    @Test
    public void testFormatDoubleNegZero() {
        sink.clear();
        Numbers.append(sink, -0d);
        TestUtils.assertEquals("-0.0", sink);
    }

    @Test
    public void testFormatDoubleNoExponent() {
        sink.clear();
        Numbers.append(sink, 0.2213323334);
        TestUtils.assertEquals("0.2213323334", sink);
    }

    @Test
    public void testFormatDoubleNoExponentNeg() {
        sink.clear();
        Numbers.append(sink, -0.2213323334);
        TestUtils.assertEquals("-0.2213323334", sink);
    }

    @Test
    public void testFormatDoubleRound() {
        sink.clear();
        Numbers.append(sink, 4455630333333333333333334444d);
        TestUtils.assertEquals("4.4556303333333335E27", sink);
    }

    @Test
    public void testFormatDoubleSlowInteractive() {
        sink.clear();
        Numbers.append(sink, 1.1317400099603851e308);
        TestUtils.assertEquals("1.1317400099603851E308", sink);
    }

    @Test
    public void testFormatDoubleZero() {
        sink.clear();
        Numbers.append(sink, 0d);
        TestUtils.assertEquals("0.0", sink);
    }

    @Test
    public void testFormatDoubleZeroExp() {
        sink.clear();
        Numbers.append(sink, -2.225073858507201E-308);
        TestUtils.assertEquals("-2.225073858507201E-308", sink);
    }

    @Test
    public void testFormatInt() {
        for (int i = 0; i < 1000; i++) {
            int n = rnd.nextInt();
            sink.clear();
            Numbers.append(sink, n);
            assertEquals(Integer.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatLong() {
        for (int i = 0; i < 1000; i++) {
            long n = rnd.nextLong();
            sink.clear();
            Numbers.append(sink, n);
            assertEquals(Long.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatShort() {
        for (int i = 0; i < 1000; i++) {
            short n = (short) rnd.nextInt();

            sink.clear();
            Numbers.append(sink, n);
            assertEquals(Short.toString(n), sink.toString());
        }
    }

    @Test
    public void testFormatSpecialDouble() {
        double d = -1.040218505859375E10d;
        Numbers.append(sink, d);
        assertEquals(Double.toString(d), sink.toString());

        sink.clear();
        d = -1.040218505859375E-10d;
        Numbers.append(sink, d);
        assertEquals(Double.toString(d), sink.toString());
    }

    @Test
    public void testHexInt() {
        assertEquals('w', (char) Numbers.parseHexInt("77"));
        assertEquals(0xf0, Numbers.parseHexInt("F0"));
        assertEquals(0xac, Numbers.parseHexInt("ac"));
    }

    @Test
    public void testIntEdge() {
        Numbers.append(sink, Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, Numbers.parseInt(sink));

        sink.clear();

        Numbers.append(sink, Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, Numbers.parseIntQuiet(sink));
    }

    @Test
    public void testLongToHex() {
        long value = -8372462554923253491L;
        StringSink sink = new StringSink();
        Numbers.appendHex(sink, value, false);
        TestUtils.assertEquals(Long.toHexString(value), sink);
    }

    @Test
    public void testLongToHex2() {
        long value = 0x5374f5fbcef4819L;
        StringSink sink = new StringSink();
        Numbers.appendHex(sink, value, false);
        TestUtils.assertEquals("0" + Long.toHexString(value), sink);
    }

    @Test
    public void testLongToHex3() {
        long value = 0xbfbca5da8f0645L;
        StringSink sink = new StringSink();
        Numbers.appendHex(sink, value, false);
        TestUtils.assertEquals(Long.toHexString(value), sink);
    }

    @Test
    public void testLongToString() {
        Numbers.append(sink, 6103390276L);
        TestUtils.assertEquals("6103390276", sink);
    }

    @Test(expected = NumericException.class)
    public void testParse000Greedy0() throws NumericException {
        Numbers.parseInt000Greedy("", 0, 0);
    }

    @Test
    public void testParse000Greedy1() throws NumericException {
        String input = "2";
        long val = Numbers.parseInt000Greedy(input, 0, input.length());
        assertEquals(input.length(), Numbers.decodeHighInt(val));
        assertEquals(200, Numbers.decodeLowInt(val));
    }

    @Test
    public void testParse000Greedy2() throws NumericException {
        String input = "06";
        long val = Numbers.parseInt000Greedy(input, 0, input.length());
        assertEquals(input.length(), Numbers.decodeHighInt(val));
        assertEquals(60, Numbers.decodeLowInt(val));
    }

    @Test
    public void testParse000Greedy3() throws NumericException {
        String input = "219";
        long val = Numbers.parseInt000Greedy(input, 0, input.length());
        assertEquals(input.length(), Numbers.decodeHighInt(val));
        assertEquals(219, Numbers.decodeLowInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParse000Greedy4() throws NumericException {
        Numbers.parseInt000Greedy("1234", 0, 4);
    }

    @Test
    public void testParseDouble() {

        String s9 = "0.33458980809808359835083490580348503845E203";
        assertEquals(Double.parseDouble(s9), Numbers.parseDouble(s9), 0.000000001);

        String s0 = "0.33458980809808359835083490580348503845";
        assertEquals(Double.parseDouble(s0), Numbers.parseDouble(s0), 0.000000001);


        String s1 = "0.45677888912387699";
        assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "1.459983E35";
        assertEquals(Double.parseDouble(s2) / 1e35d, Numbers.parseDouble(s2) / 1e35d, 0.00001);


        String s3 = "0.000000023E-30";
        assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "0.000000023E-204";
        assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);

        String s5 = "0.0000E-204";
        assertEquals(Double.parseDouble(s5), Numbers.parseDouble(s5), 0.000000001);

        String s6 = "200E2";
        assertEquals(Double.parseDouble(s6), Numbers.parseDouble(s6), 0.000000001);

        String s7 = "NaN";
        assertEquals(Double.parseDouble(s7), Numbers.parseDouble(s7), 0.000000001);

        String s8 = "-Infinity";
        assertEquals(Double.parseDouble(s8), Numbers.parseDouble(s8), 0.000000001);

        String s10 = "2E+2";
        assertEquals(Double.parseDouble(s10), Numbers.parseDouble(s10), 0.000000001);

        String s11 = "2E+02";
        assertEquals(Double.parseDouble(s11), Numbers.parseDouble(s11), 0.000000001);

    }

    @Test
    public void testParseDoubleCloseToZero() {
        String s1 = "0.123456789";
        assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "0.12345678901234567890123456789E12";
        assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);
    }

    @Test
    public void testParseDoubleIntegerLargerThanLongMaxValue() {
        String s1 = "9223372036854775808";
        assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "9223372036854775808123";
        assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);

        String s3 = "92233720368547758081239223372036854775808123";
        assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "9223372036854775808123922337203685477580812392233720368547758081239223372036854775808123";
        assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);
    }

    @Test
    public void testParseDoubleLargerThanLongMaxValue() throws NumericException {
        String s1 = "9223372036854775808.0123456789";
        assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "9223372036854775808.0123456789";
        assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);

        String s3 = "9223372036854775808123.0123456789";
        assertEquals(Double.parseDouble(s3), Numbers.parseDouble(s3), 0.000000001);

        String s4 = "92233720368547758081239223372036854775808123.01239223372036854775808123";
        assertEquals(Double.parseDouble(s4), Numbers.parseDouble(s4), 0.000000001);
    }

    @Test
    public void testParseDoubleNegativeZero() throws NumericException {
        double actual = Numbers.parseDouble("-0.0");

        //check it's zero at all
        assertEquals(0, actual, 0.0);

        //check it's *negative* zero
        double res = 1 / actual;
        assertEquals(Double.NEGATIVE_INFINITY, res, 0.0);
    }

    @Test
    public void testParseDoubleWithManyLeadingZeros() {
        String s1 = "000000.000000000033458980809808359835083490580348503845";
        assertEquals(Double.parseDouble(s1), Numbers.parseDouble(s1), 0.000000001);

        String s2 = "000000.00000000003345898080E25";
        assertEquals(Double.parseDouble(s2), Numbers.parseDouble(s2), 0.000000001);
    }

    @Test
    public void testParseExplicitDouble() {
        assertEquals(1234.123d, Numbers.parseDouble("1234.123d"), 0.000001);
    }

    @Test
    public void testParseExplicitFloat() {
        assertEquals(12345.02f, Numbers.parseFloat("12345.02f"), 0.0001f);
    }

    @Test(expected = NumericException.class)
    public void testParseExplicitFloat2() {
        Numbers.parseFloat("12345.02fx");
    }

    @Test
    public void testParseFloat() {
        String s1 = "0.45677899234";
        assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "1.459983E35";
        assertEquals(Float.parseFloat(s2) / 1e35d, Numbers.parseFloat(s2) / 1e35d, 0.00001);

        String s3 = "0.000000023E-30";
        assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        // overflow
        try {
            Numbers.parseFloat("1.0000E-204");
            Assert.fail();
        } catch (NumericException ignored) {
        }

        try {
            Numbers.parseFloat("1E39");
            Assert.fail();
        } catch (NumericException ignored) {
        }

        try {
            Numbers.parseFloat("1.0E39");
            Assert.fail();
        } catch (NumericException ignored) {
        }

        String s6 = "200E2";
        assertEquals(Float.parseFloat(s6), Numbers.parseFloat(s6), 0.000000001);

        String s7 = "NaN";
        assertEquals(Float.parseFloat(s7), Numbers.parseFloat(s7), 0.000000001);

        String s8 = "-Infinity";
        assertEquals(Float.parseFloat(s8), Numbers.parseFloat(s8), 0.000000001);

        // min exponent float
        String s9 = "1.4e-45";
        assertEquals(1.4e-45f, Numbers.parseFloat(s9), 0.001);

        // false overflow
        String s10 = "0003000.0e-46";
        assertEquals(1.4e-45f, Numbers.parseFloat(s10), 0.001);

        // false overflow
        String s11 = "0.00001e40";
        assertEquals(1e35f, Numbers.parseFloat(s11), 0.001);
    }

    @Test
    public void testParseFloatCloseToZero() {
        String s1 = "0.123456789";
        assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "0.12345678901234567890123456789E12";
        assertEquals(Float.parseFloat(s2), Numbers.parseFloat(s2), 0.000000001);
    }

    @Test
    public void testParseFloatIntegerLargerThanLongMaxValue() {
        String s1 = "9223372036854775808";
        assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "9223372036854775808123";
        assertEquals(Float.parseFloat(s2), Numbers.parseFloat(s2), 0.000000001);

        String s3 = "9223372036854775808123922337203685477";
        assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        String s4 = "92233720368547758081239223372036854771";
        assertEquals(Float.parseFloat(s4), Numbers.parseFloat(s4), 0.000000001);
    }

    @Test
    public void testParseFloatLargerThanLongMaxValue() throws NumericException {
        String s1 = "9223372036854775808.0123456789";
        assertEquals(Float.parseFloat(s1), Numbers.parseFloat(s1), 0.000000001);

        String s2 = "9223372036854775808.0123456789";
        assertEquals(Float.parseFloat(s2), Numbers.parseFloat(s2), 0.000000001);

        String s3 = "9223372036854775808123.0123456789";
        assertEquals(Float.parseFloat(s3), Numbers.parseFloat(s3), 0.000000001);

        String s4 = "922337203685477580812392233720368547758081.01239223372036854775808123"; // overflow
        try {
            Numbers.parseFloat(s4);
            Assert.fail();
        } catch (NumericException ignored) {
        }
    }

    @Test
    public void testParseFloatNegativeZero() throws NumericException {
        float actual = Numbers.parseFloat("-0.0");

        //check it's zero at all
        assertEquals(0, actual, 0.0);

        //check it's *negative* zero
        float res = 1 / actual;
        assertEquals(Float.NEGATIVE_INFINITY, res, 0.0);
    }

    @Test
    public void testParseIPv4() {
        assertEquals(84413540, Numbers.parseIPv4("5.8.12.100"));
        assertEquals(204327201, Numbers.parseIPv4("12.45.201.33"));
    }

    @Test
    public void testParseIPv42() {
        assertEquals(0, Numbers.parseIPv4((CharSequence) null));
        assertEquals(0, Numbers.parseIPv4("null"));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Empty() {
        Numbers.parseIPv4("");
    }

    @Test
    public void testParseIPv4LeadingAndTrailingDots() {
        assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".....1.2.3.4......")));
    }

    @Test
    public void testParseIPv4LeadingAndTrailingDots2() {
        assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".1.2.3.4.")));
    }

    @Test
    public void testParseIPv4LeadingDots() {
        assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".....1.2.3.4")));
    }

    @Test
    public void testParseIPv4LeadingDots2() {
        assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4(".1.2.3.4")));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4LeadingDots3() {
        Numbers.parseIPv4("...a.1.2.3.4");
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4MiddleDots() {
        Numbers.parseIPv4("1..2..3..4");
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Overflow1() {
        String i1 = "256.256.256.256";
        Numbers.parseIPv4(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Overflow2() {
        String i1 = "255.255.255.256";
        Numbers.parseIPv4(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4Overflow3() {
        Numbers.parseIPv4("12.1.3500.2");
    }

    @Test
    public void testParseIPv4Quiet() {
        assertEquals(0, Numbers.parseIPv4Quiet(null));
        assertEquals(0, Numbers.parseIPv4Quiet("NaN"));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4SignOnly() {
        Numbers.parseIPv4("-");
    }

    @Test
    public void testParseIPv4TrailingDots() {
        assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4("1.2.3.4......")));
    }

    @Test
    public void testParseIPv4TrailingDots2() {
        assertEquals("1.2.3.4", TestUtils.ipv4ToString(Numbers.parseIPv4("1.2.3.4.")));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4TrailingDots3() {
        Numbers.parseIPv4("1.2.3.4...a.");
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4WrongChars() {
        Numbers.parseIPv4("1.2.3.ab");
    }

    @Test
    public void testParseIPv4_0() {
        assertEquals(84413540, Numbers.parseIPv4_0("5.8.12.100", 0, 10));
        assertEquals(204327201, Numbers.parseIPv4_0("12.45.201.33", 0, 12));
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Empty() {
        Numbers.parseIPv4_0("", 0, 0);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Null() {
        Numbers.parseIPv4_0(null, 0, 0);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Overflow1() {
        String i1 = "256.256.256.256";
        Numbers.parseIPv4_0(i1, 0, 15);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Overflow2() {
        String i1 = "255.255.255.256";
        Numbers.parseIPv4_0(i1, 0, 15);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0Overflow3() {
        Numbers.parseIPv4_0("12.1.3500.2", 0, 11);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0SignOnly() {
        Numbers.parseIPv4_0("-", 0, 1);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0WrongChars() {
        Numbers.parseIPv4_0("1.2.3.ab", 0, 8);
    }

    @Test(expected = NumericException.class)
    public void testParseIPv4_0WrongCount() {
        Numbers.parseIPv4_0("5.6", 0, 3);
    }

    @Test
    public void testParseInt() {
        assertEquals(567963, Numbers.parseInt("567963"));
        assertEquals(-23346346, Numbers.parseInt("-23346346"));
    }

    @Test(expected = NumericException.class)
    public void testParseIntEmpty() {
        Numbers.parseInt("");
    }

    @Test(expected = NumericException.class)
    public void testParseIntNull() {
        Numbers.parseInt((CharSequence) null);
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow1() {
        String i1 = "12345566787";
        Numbers.parseInt(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow2() {
        Numbers.parseInt("2147483648");
    }

    @Test(expected = NumericException.class)
    public void testParseIntOverflow3() {
        Numbers.parseInt("5000000000");
    }

    @Test(expected = NumericException.class)
    public void testParseIntSignOnly() {
        Numbers.parseInt("-");
    }

    @Test
    public void testParseIntToDelim() {
        String in = "1234x5";
        long val = Numbers.parseIntSafely(in, 0, in.length());
        assertEquals(1234, Numbers.decodeLowInt(val));
        assertEquals(4, Numbers.decodeHighInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParseIntToDelimEmpty() {
        String in = "x";
        Numbers.parseIntSafely(in, 0, in.length());
    }

    @Test
    public void testParseIntToDelimNoChar() {
        String in = "12345";
        long val = Numbers.parseIntSafely(in, 0, in.length());
        assertEquals(12345, Numbers.decodeLowInt(val));
        assertEquals(5, Numbers.decodeHighInt(val));
    }

    @Test(expected = NumericException.class)
    public void testParseIntWrongChars() {
        Numbers.parseInt("123ab");
    }

    @Test(expected = NumericException.class)
    public void testParseLongEmpty() {
        Numbers.parseLong("");
    }

    @Test(expected = NumericException.class)
    public void testParseLongNullCharSequence() {
        Numbers.parseLong((CharSequence) null);
    }

    @Test(expected = NumericException.class)
    public void testParseLongNullUtf8Sequence() {
        Numbers.parseLong((Utf8Sequence) null);
    }

    @Test(expected = NumericException.class)
    public void testParseLongOverflow1() {
        String i1 = "1234556678723234234234234234234";
        Numbers.parseLong(i1);
    }

    @Test(expected = NumericException.class)
    public void testParseLongOverflow2() {
        Numbers.parseLong("9223372036854775808");
    }

    @Test(expected = NumericException.class)
    public void testParseLongSignOnly() {
        Numbers.parseLong("-");
    }

    @Test
    public void testParseLongUnderscore() throws NumericException {
        assertEquals(123_000, Numbers.parseLong("123_000"));
        assertEquals(123_343_123, Numbers.parseLong("123_343_123"));
        assertParseLongException("_889");
        assertParseLongException("__8289");
        assertParseLongException("8289_");
        assertParseLongException("8289__");
        assertParseLongException("82__89");
    }

    @Test(expected = NumericException.class)
    public void testParseLongWrongChars() {
        Numbers.parseLong("123ab");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongHexInt() {
        Numbers.parseHexInt("0N");
    }

    @Test(expected = NumericException.class)
    public void testParseWrongNan() {
        Numbers.parseDouble("NaN1");
    }

    private static void assertParseLongException(String input) {
        try {
            Numbers.parseLong(input);
            Assert.fail();
        } catch (NumericException ignore) {
        }
    }
}
