/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.datetime.CommonUtils;
import io.questdb.client.std.str.Utf8StringSink;

/** Schema-only UTC millisecond formatter matching the server timestamp-to-text conversion. */
final class QwpSchemaTimestampFormatter {
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final long AVG_YEAR_MICROS = (long) (365.2425 * DAY_MICROS);
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final long MILLI_MICROS = 1_000L;
    private static final long MINUTE_MICROS = 60_000_000L;
    private static final long SECOND_MICROS = 1_000_000L;
    private static final long YEAR_MICROS_LEAP = 366 * DAY_MICROS;
    private static final long YEAR_MICROS_NONLEAP = 365 * DAY_MICROS;
    private static final long[] LEAP_MONTH_MICROS = new long[12];
    private static final long[] MONTH_MICROS = new long[12];

    static {
        long normal = 0;
        long leap = 0;
        for (int month = 0; month < 11; month++) {
            normal += CommonUtils.DAYS_PER_MONTH[month] * DAY_MICROS;
            leap += CommonUtils.getDaysPerMonth(month + 1, true) * DAY_MICROS;
            MONTH_MICROS[month + 1] = normal;
            LEAP_MONTH_MICROS[month + 1] = leap;
        }
    }

    private QwpSchemaTimestampFormatter() {
    }

    static void appendDateTime(Utf8StringSink sink, long micros) {
        if (micros == Long.MIN_VALUE) {
            return;
        }
        int year = getYear(micros);
        boolean leap = CommonUtils.isLeapYear(year);
        int month = getMonthOfYear(micros, year, leap);
        appendYear000(sink, year);
        sink.putAscii('-');
        append0(sink, month);
        sink.putAscii('-');
        append0(sink, getDayOfMonth(micros, year, month, leap));
        sink.putAscii('T');
        append0(sink, getHourOfDay(micros));
        sink.putAscii(':');
        append0(sink, getMinuteOfHour(micros));
        sink.putAscii(':');
        append0(sink, getSecondOfMinute(micros));
        sink.putAscii('.');
        append00(sink, getMillisOfSecond(micros));
        sink.putAscii('Z');
    }

    private static void append0(Utf8StringSink sink, int value) {
        if (Math.abs(value) < 10) {
            sink.putAscii('0');
        }
        Numbers.append(sink, value);
    }

    private static void append00(Utf8StringSink sink, int value) {
        int absolute = Math.abs(value);
        if (absolute < 10) {
            sink.putAscii('0').putAscii('0');
        } else if (absolute < 100) {
            sink.putAscii('0');
        }
        Numbers.append(sink, value);
    }

    private static void appendYear000(Utf8StringSink sink, int value) {
        int absolute = Math.abs(value);
        if (absolute < 10) {
            sink.putAscii('0').putAscii('0').putAscii('0');
        } else if (absolute < 100) {
            sink.putAscii('0').putAscii('0');
        } else if (absolute < 1000) {
            sink.putAscii('0');
        }
        Numbers.append(sink, value != 0 ? value : 1);
    }

    private static int getDayOfMonth(long micros, int year, int month, boolean leap) {
        long start = yearMicros(year, leap) + monthOfYearMicros(month, leap);
        return (int) ((micros - start) / DAY_MICROS) + 1;
    }

    private static int getHourOfDay(long micros) {
        if (micros > -1) {
            return (int) ((micros / HOUR_MICROS) % 24);
        }
        return 23 + (int) (((micros + 1) / HOUR_MICROS) % 24);
    }

    private static int getMillisOfSecond(long micros) {
        if (micros > -1) {
            return (int) ((micros / MILLI_MICROS) % 1000);
        }
        return 999 + (int) (((micros + 1) / MILLI_MICROS) % 1000);
    }

    private static int getMinuteOfHour(long micros) {
        if (micros > -1) {
            return (int) ((micros / MINUTE_MICROS) % 60);
        }
        return 59 + (int) (((micros + 1) / MINUTE_MICROS) % 60);
    }

    // Keep the server's threshold-based month lookup: it is stable at the wrapped long extrema.
    private static int getMonthOfYear(long micros, int year, boolean leap) {
        int i = (int) (((micros - yearMicros(year, leap)) / 1000) >> 10);
        if (leap) {
            if (i < 182 * 84375) {
                if (i < 91 * 84375) {
                    return i < 31 * 84375 ? 1 : i < 60 * 84375 ? 2 : 3;
                }
                return i < 121 * 84375 ? 4 : i < 152 * 84375 ? 5 : 6;
            }
            if (i < 274 * 84375) {
                return i < 213 * 84375 ? 7 : i < 244 * 84375 ? 8 : 9;
            }
            return i < 305 * 84375 ? 10 : i < 335 * 84375 ? 11 : 12;
        }
        if (i < 181 * 84375) {
            if (i < 90 * 84375) {
                return i < 31 * 84375 ? 1 : i < 59 * 84375 ? 2 : 3;
            }
            return i < 120 * 84375 ? 4 : i < 151 * 84375 ? 5 : 6;
        }
        if (i < 273 * 84375) {
            return i < 212 * 84375 ? 7 : i < 243 * 84375 ? 8 : 9;
        }
        return i < 304 * 84375 ? 10 : i < 334 * 84375 ? 11 : 12;
    }

    private static int getSecondOfMinute(long micros) {
        if (micros > -1) {
            return (int) ((micros / SECOND_MICROS) % 60);
        }
        return 59 + (int) (((micros + 1) / SECOND_MICROS) % 60);
    }

    // Source-compatible estimate and adjustment from the server Micros implementation.
    private static int getYear(long micros) {
        int yearsSinceEpoch = (int) (micros / AVG_YEAR_MICROS);
        int estimate = 1970 + yearsSinceEpoch;
        if (micros < 0 && estimate >= 1970) {
            estimate = 1969;
        }
        boolean leap = CommonUtils.isLeapYear(estimate);
        long start = yearMicros(estimate, leap);
        long difference = micros - start;
        if (difference < 0) {
            estimate--;
        } else if (difference >= (leap ? YEAR_MICROS_LEAP : YEAR_MICROS_NONLEAP)) {
            estimate++;
        }
        return estimate;
    }

    private static long monthOfYearMicros(int month, boolean leap) {
        return (leap ? LEAP_MONTH_MICROS : MONTH_MICROS)[month - 1];
    }

    private static long yearMicros(int year, boolean leap) {
        int leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (leap) {
                leapYears--;
            }
        }
        long days = year * 365L + (leapYears - 719527L);
        long micros = days * DAY_MICROS;
        return days < 0 && micros > 0 ? Long.MIN_VALUE : micros;
    }
}
