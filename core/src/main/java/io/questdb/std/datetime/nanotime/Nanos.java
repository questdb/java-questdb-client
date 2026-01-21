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

package io.questdb.std.datetime.nanotime;

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.CommonUtils;

import static io.questdb.std.datetime.microtime.Micros.*;

public final class Nanos {
    public static final long DAY_NANOS = 86_400_000_000_000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long AVG_YEAR_NANOS = (long) (365.2425 * DAY_NANOS);
    public static final long HOUR_NANOS = 3_600_000_000_000L;
    public static final long MICRO_NANOS = 1000L;
    public static final long MILLI_NANOS = 1_000_000L;
    public static final long MINUTE_NANOS = 60_000_000_000L;
    public static final long SECOND_NANOS = 1_000_000_000L;
    public static final long WEEK_NANOS = 7 * DAY_NANOS;
    public static final long YEAR_NANOS_NONLEAP = 365 * DAY_NANOS;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long YEAR_NANOS_LEAP = 366 * DAY_NANOS;

    private Nanos() {
    }

    public static long addDays(long nanos, int days) {
        return nanos + days * DAY_NANOS;
    }

    public static long endOfYear(int year) {
        return toNanos(year, 12, 31, 23, 59) + 59 * SECOND_NANOS + 999_999_999L;
    }

    public static int getDayOfMonth(long nanos, int year, int month, boolean leap) {
        long yearNanos = yearNanos(year, leap);
        yearNanos += monthOfYearNanos(month, leap);
        return (int) ((nanos - yearNanos) / DAY_NANOS) + 1;
    }

    public static int getDayOfWeek(long nanos) {
        // 1970-01-01 is Thursday.
        long d;
        if (nanos > -1) {
            d = nanos / DAY_NANOS;
        } else {
            d = (nanos - (DAY_NANOS - 1)) / DAY_NANOS;
            if (d < -3) {
                return 7 + (int) ((d + 4) % 7);
            }
        }
        return 1 + (int) ((d + 3) % 7);
    }

    public static int getDayOfWeekSundayFirst(long nanos) {
        // 1970-01-01 is Thursday.
        long d;
        if (nanos > -1) {
            d = nanos / DAY_NANOS;
        } else {
            d = (nanos - (DAY_NANOS - 1)) / DAY_NANOS;
            if (d < -4) {
                return 7 + (int) ((d + 5) % 7);
            }
        }
        return 1 + (int) ((d + 4) % 7);
    }

    public static int getDayOfYear(long nanos) {
        int year = getYear(nanos);
        boolean leap = isLeapYear(year);
        long yearStart = yearNanos(year, leap);
        return (int) ((nanos - yearStart) / DAY_NANOS) + 1;
    }

    public static int getDoy(long nanos) {
        final int year = getYear(nanos);
        final boolean leap = isLeapYear(year);
        final long yearStart = yearNanos(year, leap);
        return (int) ((nanos - yearStart) / DAY_NANOS) + 1;
    }

    // Each ISO 8601 week-numbering year begins with the Monday of the week containing the 4th of January,
    // so in early January or late December the ISO year may be different from the Gregorian year.
    // See the getWeek() method for more information.
    public static int getIsoYear(long nanos) {
        int w = (10 + getDoy(nanos) - getDayOfWeek(nanos)) / 7;
        int y = getYear(nanos);
        if (w < 1) {
            return y - 1;
        }

        if (w > CommonUtils.getWeeks(y)) {
            return y + 1;
        }

        return y;
    }

    public static int getMonthOfYear(long nanos) {
        final int y = Nanos.getYear(nanos);
        final boolean leap = Nanos.isLeapYear(y);
        return getMonthOfYear(nanos, y, leap);
    }

    /**
     * Calculates month of year from absolute nanos.
     *
     * @param nanos nanos since 1970
     * @param year  year of month
     * @param leap  true if year was leap
     * @return month of year
     */
    // TODO: reuse complex code
    public static int getMonthOfYear(long nanos, int year, boolean leap) {
        int i = (int) (((nanos - yearNanos(year, leap)) / MILLI_NANOS) >> 10);
        return leap
                ? ((i < 182 * 84375)
                ? ((i < 91 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 60 * 84375) ? 2 : 3)
                : ((i < 121 * 84375) ? 4 : (i < 152 * 84375) ? 5 : 6))
                : ((i < 274 * 84375)
                ? ((i < 213 * 84375) ? 7 : (i < 244 * 84375) ? 8 : 9)
                : ((i < 305 * 84375) ? 10 : (i < 335 * 84375) ? 11 : 12)))
                : ((i < 181 * 84375)
                ? ((i < 90 * 84375)
                ? ((i < 31 * 84375) ? 1 : (i < 59 * 84375) ? 2 : 3)
                : ((i < 120 * 84375) ? 4 : (i < 151 * 84375) ? 5 : 6))
                : ((i < 273 * 84375)
                ? ((i < 212 * 84375) ? 7 : (i < 243 * 84375) ? 8 : 9)
                : ((i < 304 * 84375) ? 10 : (i < 334 * 84375) ? 11 : 12)));
    }

    public static int getNanosOfSecond(long nanos) {
        if (nanos > -1) {
            return (int) (nanos % SECOND_NANOS);
        } else {
            return (int) (SECOND_NANOS - 1 + ((nanos + 1) % SECOND_NANOS));
        }
    }

    public static int getWallHours(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / HOUR_NANOS) % CommonUtils.DAY_HOURS);
        } else {
            return CommonUtils.DAY_HOURS - 1 + (int) (((nanos + 1) / HOUR_NANOS) % CommonUtils.DAY_HOURS);
        }
    }

    public static int getWallMicros(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos % MILLI_NANOS) / MICRO_NANOS);
        } else {
            return (int) (((((nanos % Nanos.SECOND_NANOS) + Nanos.SECOND_NANOS) % Nanos.SECOND_NANOS) / Nanos.MICRO_NANOS) % Nanos.MICRO_NANOS);
        }
    }

    public static int getWallMillis(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MILLI_NANOS) % SECOND_MILLIS);
        } else {
            return SECOND_MILLIS - 1 + (int) (((nanos + 1) / MILLI_NANOS) % SECOND_MILLIS);
        }
    }

    public static int getWallMinutes(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / MINUTE_NANOS) % CommonUtils.HOUR_MINUTES);
        } else {
            return CommonUtils.HOUR_MINUTES - 1 + (int) (((nanos + 1) / MINUTE_NANOS) % CommonUtils.HOUR_MINUTES);
        }
    }

    public static int getWallNanos(long nanoe) {
        if (nanoe > -1) {
            return (int) (nanoe % MICRO_NANOS);
        } else {
            return (int) (MICRO_NANOS - 1 + ((nanoe + 1) % MICRO_NANOS));
        }
    }

    public static int getWallSeconds(long nanos) {
        if (nanos > -1) {
            return (int) ((nanos / SECOND_NANOS) % CommonUtils.MINUTE_SECONDS);
        } else {
            return (int) (CommonUtils.MINUTE_SECONDS - 1 + (int) (((nanos + 1) / SECOND_NANOS) % CommonUtils.MINUTE_SECONDS));
        }
    }

    // https://en.wikipedia.org/wiki/ISO_week_date
    public static int getWeek(long nanos) {
        int w = (10 + getDoy(nanos) - getDayOfWeek(nanos)) / 7;
        int y = getYear(nanos);
        if (w < 1) {
            return CommonUtils.getWeeks(y - 1);
        }

        if (w > CommonUtils.getWeeks(y)) {
            return 1;
        }

        return w;
    }

    public static int getWeekOfYear(long nanos) {
        return getDayOfYear(nanos) / 7 + 1;
    }

    /**
     * Calculates year number from nanos.
     *
     * @param nanos time nanos.
     * @return year
     */
    public static int getYear(long nanos) {
        // Initial year estimate relative to 1970
        // Use a reasonable approximation of days per year to avoid overflow
        // 365.25 days per year approximation
        int yearsSinceEpoch = (int) (nanos / AVG_YEAR_NANOS);
        int yearEstimate = 1970 + yearsSinceEpoch;

        // Handle negative years appropriately
        if (nanos < 0 && yearEstimate >= 1970) {
            yearEstimate = 1969;
        }

        // Calculate year start
        boolean leap = isLeapYear(yearEstimate);
        long yearStart = yearNanos(yearEstimate, leap);

        // Check if we need to adjust
        long diff = nanos - yearStart;

        if (diff < 0) {
            // We're in the previous year
            yearEstimate--;
        } else {
            // Check if we're in the next year
            long yearLength = leap ? YEAR_NANOS_LEAP : YEAR_NANOS_NONLEAP;
            if (diff >= yearLength) {
                yearEstimate++;
            }
        }

        return yearEstimate;
    }

    /**
     * Calculates if year is leap year using following algorithm:
     * <p>
     * <a href="http://en.wikipedia.org/wiki/Leap_year">...</a>
     *
     * @param year the year
     * @return true if year is leap
     */
    public static boolean isLeapYear(int year) {
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    public static long monthOfYearNanos(int month, boolean leap) {
        return monthOfYearMicros(month, leap) * MICRO_NANOS;
    }

    public static long nextOrSameDayOfWeek(long nanos, int dow) {
        int thisDow = getDayOfWeek(nanos);
        if (thisDow == dow) {
            return nanos;
        }

        if (thisDow < dow) {
            return nanos + (dow - thisDow) * DAY_NANOS;
        } else {
            return nanos + (7 - (thisDow - dow)) * DAY_NANOS;
        }
    }

    public static long parseNanosAsMicrosGreedy(CharSequence sequence, final int p, int lim) throws NumericException {
        if (lim == p) {
            throw NumericException.instance();
        }

        boolean negative = sequence.charAt(p) == '-';
        int i = p;
        if (negative) {
            i++;
        }

        if (i >= lim || Numbers.notDigit(sequence.charAt(i))) {
            throw NumericException.instance();
        }

        int val = 0;
        for (; i < lim; i++) {
            char c = sequence.charAt(i);

            if (Numbers.notDigit(c)) {
                break;
            }

            // val * 10 + (c - '0')
            int r = (val << 3) + (val << 1) - (c - '0');
            if (r > val) {
                throw NumericException.instance();
            }
            val = r;
        }

        final int len = i - p;

        if (len > 9 || val == Integer.MIN_VALUE && !negative) {
            throw NumericException.instance();
        }

        while (i - p < 9) {
            val *= 10;
            i++;
        }

        return Numbers.encodeLowHighInts(negative ? val : -val, len);
    }

    public static long previousOrSameDayOfWeek(long nanos, int dow) {
        int thisDow = getDayOfWeek(nanos);
        if (thisDow == dow) {
            return nanos;
        }

        if (thisDow < dow) {
            return nanos - (7 + (thisDow - dow)) * DAY_NANOS;
        } else {
            return nanos - (thisDow - dow) * DAY_NANOS;
        }
    }

    public static long toNanos(int y, int m, int d, int h, int mi) {
        return toNanos(y, isLeapYear(y), m, d, h, mi);
    }

    public static long toNanos(int y, boolean leap, int m, int d, int h, int mi) {
        return yearNanos(y, leap) + monthOfYearNanos(m, leap) + (d - 1) * DAY_NANOS + h * HOUR_NANOS + mi * MINUTE_NANOS;
    }

    public static long yearNanos(int year, boolean leap) {
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
        long nanos = days * DAY_NANOS;
        if (days < 0 & nanos > 0) {
            return Long.MIN_VALUE;
        }
        return nanos;
    }

    private static long getRemainderNanos(long nanos, long interval) {
        final long rem = nanos % interval;
        return rem < 0 ? interval + rem : rem;
    }

    private static long getTimeNanos(long nanos) {
        return getRemainderNanos(nanos, DAY_NANOS);
    }

}