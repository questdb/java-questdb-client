/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.client.test.std;

import io.questdb.client.std.Decimal256;
import io.questdb.client.std.DecimalParser;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import org.junit.Assert;
import org.junit.Test;

public class DecimalParserServerCompatibilityTest {
    @Test
    public void testFullScaleAndTargetScalePaddingUseServerPrecisionRules() throws NumericException {
        assertParsed("0.5", 1, 1, ".5", 1, 1);
        assertParsed("0.500", 3, 3, "0.5", 3, 3);
        assertParsed("0.100", 3, 3, ".1", 3, 3);

        String fraction = "0." + repeat('9', 76);
        assertParsed(fraction, 76, 76, fraction, 76, 76);
    }

    @Test
    public void testLeadingZerosAndZeroExponentUseServerPrecisionRules() throws NumericException {
        assertParsed("0", 1, 0, "0000.e80", 1, 0);
        assertParsed("1", 1, 0, "0.01e2", -1, -1);
        assertParsed("123", 3, 0, "00.123e3", -1, -1);
        assertParsed("0", 1, 0, "0e5", -1, -1);
        assertParsed("0.000", 3, 3, "0e-3", 3, 3);
    }

    @Test
    public void testPrecisionIsAtLeastOneAndZeroFitsFullScale() throws NumericException {
        assertParsed("0", 1, 0, "0.000", -1, -1);
        assertParsed("0", 1, 0, "0.", -1, -1);
        assertParsed("0." + repeat('0', 76), 76, 76, "0", 76, 76);
    }

    @Test
    public void testSpecialValuesMatchByPrefixLikeServer() throws NumericException {
        assertNullParsed("NaN", 38, 4);
        assertNullParsed("-Infinity", 76, 8);
        // The server's DecimalParser treats NaN and Infinity as prefixes.
        assertNullParsed("NaNjunk", 38, 4);
        assertNullParsed("NaN ", 38, 4);
        assertNullParsed("Infinitygarbage", 76, 8);
        assertRejected("Na", 38, 4);
        assertRejected("Infinit", 76, 8);
    }

    @Test
    public void testSuffixOrderAndExtremeExponentsAreRejectedNormally() throws NumericException {
        assertParsed("1.23", 3, 2, "1.2300", 38, 2);
        assertRejected("1.2300M", 38, 2);
        assertRejected("1.2300fm", 38, 2);
        assertRejected("1.2300mf", 38, 2);
        assertRejected("1e2147483647", 76, 0);
        assertRejected("1e-2147483648", 76, 0);
    }

    private static void assertParsed(
            String expected,
            int expectedPrecision,
            int expectedScale,
            String input,
            int precision,
            int scale
    ) throws NumericException {
        Decimal256 viaParser = new Decimal256();
        long parserMeta = DecimalParser.parse(
                viaParser, input, 0, input.length(), precision, scale, false, false);
        Assert.assertEquals(expected, viaParser.toString());
        Assert.assertEquals(expectedPrecision, Numbers.decodeLowInt(parserMeta));
        Assert.assertEquals(expectedScale, Numbers.decodeHighInt(parserMeta));

        Decimal256 viaPublicDecimal = new Decimal256();
        long decimalMeta = viaPublicDecimal.ofString(
                input, 0, input.length(), precision, scale, false, false);
        Assert.assertEquals(expected, viaPublicDecimal.toString());
        Assert.assertEquals(expectedPrecision, Numbers.decodeLowInt(decimalMeta));
        Assert.assertEquals(expectedScale, Numbers.decodeHighInt(decimalMeta));
    }

    private static void assertNullParsed(String input, int precision, int scale) throws NumericException {
        Decimal256 viaParser = new Decimal256();
        Assert.assertEquals(0, DecimalParser.parse(
                viaParser, input, 0, input.length(), precision, scale, false, false));
        Assert.assertTrue(viaParser.isNull());

        Decimal256 viaPublicDecimal = new Decimal256();
        Assert.assertEquals(0, viaPublicDecimal.ofString(
                input, 0, input.length(), precision, scale, false, false));
        Assert.assertTrue(viaPublicDecimal.isNull());
    }

    private static void assertRejected(String input, int precision, int scale) {
        try {
            DecimalParser.parse(new Decimal256(), input, 0, input.length(), precision, scale, false, false);
            Assert.fail("expected DecimalParser to reject: " + input);
        } catch (NumericException expected) {
            // expected
        }
        try {
            new Decimal256().ofString(input, 0, input.length(), precision, scale, false, false);
            Assert.fail("expected Decimal256 to reject: " + input);
        } catch (NumericException expected) {
            // expected
        }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        for (int i = 0; i < count; i++) chars[i] = value;
        return new String(chars);
    }
}
