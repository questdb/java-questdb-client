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

package io.questdb.client.std.datetime.microtime;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.Os;
import io.questdb.client.std.datetime.CommonUtils;
import io.questdb.client.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;

import static io.questdb.client.std.datetime.CommonUtils.DAYS_PER_MONTH;

public final class Micros {
    public static final long DAY_MICROS = 86_400_000_000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long HOUR_MICROS = 3600000000L;
    public static final Micros INSTANCE = new Micros();
    public static final long MICRO_NANOS = 1000L;
    public static final long MILLI_MICROS = 1000L;
    public static final long MINUTE_MICROS = 60000000L;
    public static final long SECOND_MICROS = 1000000L;
    public static final long STARTUP_TIMESTAMP;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long[] MAX_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long[] MIN_MONTH_OF_YEAR_MICROS = new long[12];

    private Micros() {
    }

    public static long floor(CharSequence value) throws NumericException {
        return INSTANCE.parseFloorLiteral(value);
    }

    public static long monthOfYearMicros(int month, boolean leap) {
        return leap ? MAX_MONTH_OF_YEAR_MICROS[month - 1] : MIN_MONTH_OF_YEAR_MICROS[month - 1];
    }

    /**
     * Calculated epoch offset in microseconds of the beginning of the year. For example of year 2008 this is
     * equivalent to parsing "2008-01-01T00:00:00.000Z", except this method is faster.
     *
     * @param year the year
     * @param leap true if given year is leap year
     * @return epoch offset in micros.
     */
    public static long yearMicros(int year, boolean leap) {
        int leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (leap) {
                leapYears--;
            }
        }

        long days = year * 365L + (leapYears - DAYS_0000_TO_1970);
        long micros = days * DAY_MICROS;
        if (days < 0 & micros > 0) {
            return Long.MIN_VALUE;
        }
        return micros;
    }

    public long parseFloor(Utf8Sequence str, int lo, int hi) throws NumericException {
        long ts;
        if (hi - lo < 4) {
            throw NumericException.instance();
        }
        int p = lo;
        int year = Numbers.parseInt(str, p, p += 4);
        boolean l = CommonUtils.isLeapYear(year);
        if (CommonUtils.checkLen3(p, hi)) {
            CommonUtils.checkChar(str, p++, hi, '-');
            int month = Numbers.parseInt(str, p, p += 2);
            CommonUtils.checkRange(month, 1, 12);
            if (CommonUtils.checkLen3(p, hi)) {
                CommonUtils.checkChar(str, p++, hi, '-');
                int day = Numbers.parseInt(str, p, p += 2);
                CommonUtils.checkRange(day, 1, CommonUtils.getDaysPerMonth(month, l));
                if (CommonUtils.checkLen3(p, hi)) {
                    CommonUtils.checkSpecialChar(str, p++, hi);
                    int hour = Numbers.parseInt(str, p, p += 2);
                    CommonUtils.checkRange(hour, 0, 23);
                    if (CommonUtils.checkLen3(p, hi)) {
                        CommonUtils.checkChar(str, p++, hi, ':');
                        int min = Numbers.parseInt(str, p, p += 2);
                        CommonUtils.checkRange(min, 0, 59);
                        if (CommonUtils.checkLen3(p, hi)) {
                            CommonUtils.checkChar(str, p++, hi, ':');
                            int sec = Numbers.parseInt(str, p, p += 2);
                            CommonUtils.checkRange(sec, 0, 59);
                            if (p < hi && str.byteAt(p) == '.') {

                                p++;
                                // varlen milli and micros
                                int micrLim = p + 6;
                                int mlim = Math.min(hi, micrLim);
                                int micr = 0;
                                for (; p < mlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    micr *= 10;
                                    micr += c - '0';
                                }
                                micr *= CommonUtils.tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(hi, p + 3); p < nlim; p++) {
                                    char c = (char) str.byteAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                }

                                // micros
                                ts = yearMicros(year, l)
                                        + monthOfYearMicros(month, l)
                                        + (day - 1) * DAY_MICROS
                                        + hour * HOUR_MICROS
                                        + min * MINUTE_MICROS
                                        + sec * SECOND_MICROS
                                        + micr
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = yearMicros(year, l)
                                        + monthOfYearMicros(month, l)
                                        + (day - 1) * DAY_MICROS
                                        + hour * HOUR_MICROS
                                        + min * MINUTE_MICROS
                                        + sec * SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = yearMicros(year, l)
                                    + monthOfYearMicros(month, l)
                                    + (day - 1) * DAY_MICROS
                                    + hour * HOUR_MICROS
                                    + min * MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = yearMicros(year, l)
                                + monthOfYearMicros(month, l)
                                + (day - 1) * DAY_MICROS
                                + hour * HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = yearMicros(year, l)
                            + monthOfYearMicros(month, l)
                            + (day - 1) * DAY_MICROS;
                }
            } else {
                // year + month
                ts = (yearMicros(year, l) + monthOfYearMicros(month, l));
            }
        } else {
            // year
            ts = (yearMicros(year, l) + monthOfYearMicros(1, l));
        }
        return ts;
    }

    public long parseFloor(CharSequence str, int lo, int hi) throws NumericException {
        long ts;
        if (hi - lo < 4) {
            throw NumericException.instance();
        }
        int p = lo;
        int year = Numbers.parseInt(str, p, p += 4);
        boolean l = CommonUtils.isLeapYear(year);
        if (CommonUtils.checkLen3(p, hi)) {
            CommonUtils.checkChar(str, p++, hi, '-');
            int month = Numbers.parseInt(str, p, p += 2);
            CommonUtils.checkRange(month, 1, 12);
            if (CommonUtils.checkLen3(p, hi)) {
                CommonUtils.checkChar(str, p++, hi, '-');
                int day = Numbers.parseInt(str, p, p += 2);
                CommonUtils.checkRange(day, 1, CommonUtils.getDaysPerMonth(month, l));
                if (CommonUtils.checkLen3(p, hi)) {
                    CommonUtils.checkSpecialChar(str, p++, hi);
                    int hour = Numbers.parseInt(str, p, p += 2);
                    CommonUtils.checkRange(hour, 0, 23);
                    if (CommonUtils.checkLen3(p, hi)) {
                        CommonUtils.checkChar(str, p++, hi, ':');
                        int min = Numbers.parseInt(str, p, p += 2);
                        CommonUtils.checkRange(min, 0, 59);
                        if (CommonUtils.checkLen3(p, hi)) {
                            CommonUtils.checkChar(str, p++, hi, ':');
                            int sec = Numbers.parseInt(str, p, p += 2);
                            CommonUtils.checkRange(sec, 0, 59);
                            if (p < hi && str.charAt(p) == '.') {
                                p++;
                                // varlen milli and micros
                                int micrLim = p + 6;
                                int mlim = Math.min(hi, micrLim);
                                int micr = 0;
                                for (; p < mlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    micr *= 10;
                                    micr += c - '0';
                                }
                                micr *= CommonUtils.tenPow(micrLim - p);

                                // truncate remaining nanos if any
                                for (int nlim = Math.min(hi, p + 3); p < nlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                }

                                // micros
                                ts = yearMicros(year, l)
                                        + monthOfYearMicros(month, l)
                                        + (day - 1) * DAY_MICROS
                                        + hour * HOUR_MICROS
                                        + min * MINUTE_MICROS
                                        + sec * SECOND_MICROS
                                        + micr
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = yearMicros(year, l)
                                        + monthOfYearMicros(month, l)
                                        + (day - 1) * DAY_MICROS
                                        + hour * HOUR_MICROS
                                        + min * MINUTE_MICROS
                                        + sec * SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = yearMicros(year, l)
                                    + monthOfYearMicros(month, l)
                                    + (day - 1) * DAY_MICROS
                                    + hour * HOUR_MICROS
                                    + min * MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = yearMicros(year, l)
                                + monthOfYearMicros(month, l)
                                + (day - 1) * DAY_MICROS
                                + hour * HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = yearMicros(year, l)
                            + monthOfYearMicros(month, l)
                            + (day - 1) * DAY_MICROS;
                }
            } else {
                // year + month
                ts = (yearMicros(year, l) + monthOfYearMicros(month, l));
            }
        } else {
            // year
            ts = (yearMicros(year, l) + monthOfYearMicros(1, l));
        }
        return ts;
    }

    public long parseFloorLiteral(@Nullable CharSequence timestampLiteral) throws NumericException {
        return timestampLiteral != null ? parseFloor(timestampLiteral, 0, timestampLiteral.length()) : Numbers.LONG_NULL;
    }

    private static long checkTimezoneTail(CharSequence seq, int p, int lim) throws NumericException {
        if (lim == p) {
            return 0;
        }

        if (lim - p < 2) {
            CommonUtils.checkChar(seq, p, lim, 'Z');
            return 0;
        }

        if (lim - p > 2) {
            int tzSign = CommonUtils.parseSign(seq.charAt(p++));
            int hour = Numbers.parseInt(seq, p, p += 2);
            CommonUtils.checkRange(hour, 0, 23);

            if (lim - p == 3) {
                // Optional : separator between hours and mins in timezone
                CommonUtils.checkChar(seq, p++, lim, ':');
            }

            if (CommonUtils.checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                CommonUtils.checkRange(min, 0, 59);
                return tzSign * (hour * Micros.HOUR_MICROS + min * Micros.MINUTE_MICROS);
            } else {
                return tzSign * (hour * Micros.HOUR_MICROS);
            }
        }
        throw NumericException.instance();
    }

    private static long checkTimezoneTail(Utf8Sequence seq, int p, int lim) throws NumericException {
        if (lim == p) {
            return 0;
        }

        if (lim - p < 2) {
            CommonUtils.checkChar(seq, p, lim, 'Z');
            return 0;
        }

        if (lim - p > 2) {
            int tzSign = CommonUtils.parseSign((char) seq.byteAt(p++));
            int hour = Numbers.parseInt(seq, p, p += 2);
            CommonUtils.checkRange(hour, 0, 23);

            if (lim - p == 3) {
                // Optional : separator between hours and mins in timezone
                CommonUtils.checkChar(seq, p++, lim, ':');
            }

            if (CommonUtils.checkLenStrict(p, lim)) {
                int min = Numbers.parseInt(seq, p, p + 2);
                CommonUtils.checkRange(min, 0, 59);
                return tzSign * (hour * Micros.HOUR_MICROS + min * Micros.MINUTE_MICROS);
            } else {
                return tzSign * (hour * Micros.HOUR_MICROS);
            }
        }
        throw NumericException.instance();
    }

    static {
        STARTUP_TIMESTAMP = Os.currentTimeMicros();
        long minSum = 0;
        long maxSum = 0;
        for (int i = 0; i < 11; i++) {
            minSum += DAYS_PER_MONTH[i] * DAY_MICROS;
            MIN_MONTH_OF_YEAR_MICROS[i + 1] = minSum;
            maxSum += CommonUtils.getDaysPerMonth(i + 1, true) * DAY_MICROS;
            MAX_MONTH_OF_YEAR_MICROS[i + 1] = maxSum;
        }
    }
}
