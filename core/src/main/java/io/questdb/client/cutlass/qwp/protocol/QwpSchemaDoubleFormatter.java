/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.str.StringSink;

/** Schema-only floating formatter matching the server's Ryu presentation. */
final class QwpSchemaDoubleFormatter {
    private static final int EXP_SHIFT = 52;
    private static final long EXP_BIT_MASK = 0x7ff0000000000000L;
    private static final long SIGN_BIT_MASK = 0x8000000000000000L;
    private static final long SIGNIFICAND_BIT_MASK = 0x000fffffffffffffL;

    private QwpSchemaDoubleFormatter() {
    }

    static void append(StringSink sink, double value, int[] exponentScratch) {
        long bits = Double.doubleToRawLongBits(value);
        long mantissa = bits & SIGNIFICAND_BIT_MASK;
        int exponent = (int) ((bits & EXP_BIT_MASK) >>> EXP_SHIFT);
        if (exponent == 2047) {
            if (mantissa != 0) {
                sink.putAscii("NaN");
            } else if ((bits & SIGN_BIT_MASK) != 0) {
                sink.putAscii("-Infinity");
            } else {
                sink.putAscii("Infinity");
            }
            return;
        }
        if ((bits & SIGN_BIT_MASK) != 0) {
            sink.putAscii('-');
        }
        if (exponent == 0 && mantissa == 0) {
            sink.putAscii("0.0");
            return;
        }
        long output = QwpSchemaRyuDouble.d2d(mantissa, exponent, exponentScratch);
        int length = QwpSchemaRyuDouble.decimalLength17(output);
        int decimalExponent = exponentScratch[0] + length;
        if (decimalExponent > 0 && decimalExponent < 8) {
            appendFixedPositive(sink, output, length, decimalExponent);
        } else if (decimalExponent <= 0 && decimalExponent > -3) {
            appendFixedLeadingZero(sink, output, length, decimalExponent);
        } else {
            appendScientific(sink, output, length, decimalExponent);
        }
    }

    private static void appendFixedLeadingZero(
            StringSink sink, long output, int length, int decimalExponent
    ) {
        sink.putAscii("0.");
        for (int i = 0; i < -decimalExponent; i++) {
            sink.putAscii('0');
        }
        appendDigits(sink, output, length, 0, length);
    }

    private static void appendFixedPositive(
            StringSink sink, long output, int length, int decimalExponent
    ) {
        if (length <= decimalExponent) {
            appendDigits(sink, output, length, 0, length);
            for (int i = length; i < decimalExponent; i++) {
                sink.putAscii('0');
            }
            sink.putAscii(".0");
        } else {
            appendDigits(sink, output, length, 0, decimalExponent);
            sink.putAscii('.');
            appendDigits(sink, output, length, decimalExponent, length);
        }
    }

    private static void appendScientific(
            StringSink sink, long output, int length, int decimalExponent
    ) {
        appendDigit(sink, output, length - 1);
        sink.putAscii('.');
        if (length > 1) {
            appendDigits(sink, output, length, 1, length);
        } else {
            sink.putAscii('0');
        }
        sink.putAscii('E');
        Numbers.append(sink, decimalExponent - 1);
    }

    private static void appendDigits(StringSink sink, long output, int length, int from, int to) {
        for (int i = from; i < to; i++) {
            appendDigit(sink, output, length - 1 - i);
        }
    }

    private static void appendDigit(StringSink sink, long output, int power) {
        sink.putAscii((char) ('0' + (int) (output / Numbers.pow10[power] % 10)));
    }
}
