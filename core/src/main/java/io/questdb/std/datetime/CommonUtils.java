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

package io.questdb.std.datetime;

import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Micros;

public class CommonUtils {
    public static final int[] DAYS_PER_MONTH = {
            31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
    };
    public static final int DAY_HOURS = 24;
    public static final String DAY_PATTERN = "yyyy-MM-dd";
    public static final String GREEDY_MILLIS1_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.Sz";
    public static final String GREEDY_MILLIS2_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSz";
    public static final int HOUR_24 = 2;
    public static final int HOUR_AM = 0;
    public static final int HOUR_MINUTES = 60;
    public static final String HOUR_PATTERN = "yyyy-MM-ddTHH";
    public static final int HOUR_PM = 1;
    // "2261-12-31 23:59:59.999999999" for nano timestamp
    public static final long MINUTE_SECONDS = 60;
    public static final String MONTH_PATTERN = "yyyy-MM";
    public static final String NSEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUNNNz";
    public static final String PG_TIMESTAMP_MILLI_TIME_Z_PATTERN = "y-MM-dd HH:mm:ss.SSSz";
    public static final String SEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ssz";
    public static final String USEC_UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSUUUz";
    public static final String UTC_PATTERN = "yyyy-MM-ddTHH:mm:ss.SSSz";
    public static final String WEEK_PATTERN = "YYYY-Www";
    public static final String YEAR_PATTERN = "yyyy";

    public static void checkChar(CharSequence s, int p, int lim, char c) throws NumericException {
        if (p >= lim || s.charAt(p) != c) {
            throw NumericException.instance();
        }
    }

    public static boolean checkLen3(int p, int lim) throws NumericException {
        if (lim - p > 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static boolean checkLenStrict(int p, int lim) throws NumericException {
        if (lim - p == 2) {
            return true;
        }
        if (lim <= p) {
            return false;
        }

        throw NumericException.instance();
    }

    public static void checkRange(int x, int min, int max) throws NumericException {
        if (x < min || x > max) {
            throw NumericException.instance();
        }
    }

    public static void checkSpecialChar(CharSequence s, int p, int lim) throws NumericException {
        if (p >= lim || (s.charAt(p) != 'T' && s.charAt(p) != ' ')) {
            throw NumericException.instance();
        }
    }

    /**
     * Days in a given month. This method expects you to know if month is in leap year.
     *
     * @param m    month from 1 to 12
     * @param leap true if this is for leap year
     * @return number of days in month.
     */
    public static int getDaysPerMonth(int m, boolean leap) {
        return leap & m == 2 ? 29 : DAYS_PER_MONTH[m - 1];
    }

    /**
     * Since ISO weeks don't always start on the first day of the year, there is an offset of days from the 1st day of the year.
     *
     * @param year of timestamp
     * @return difference in the days from the start of the year (January 1st) and the first ISO week
     */
    public static int getIsoYearDayOffset(int year) {
        int dayOfTheWeekOfEndOfPreviousYear = Micros.getDayOfTheWeekOfEndOfYear(year - 1);
        return ((dayOfTheWeekOfEndOfPreviousYear <= 3) ? 0 : 7) - dayOfTheWeekOfEndOfPreviousYear;
    }

    public static int getWeeks(int y) {
        if (Micros.getDayOfTheWeekOfEndOfYear(y) == 4 || Micros.getDayOfTheWeekOfEndOfYear(y - 1) == 3) {
            return 53;
        }
        return 52;
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

    public static int parseSign(char c) throws NumericException {
        int tzSign;
        switch (c) {
            case '+':
                tzSign = -1;
                break;
            case '-':
                tzSign = 1;
                break;
            default:
                throw NumericException.instance();
        }
        return tzSign;
    }

    public static int tenPow(int i) throws NumericException {
        switch (i) {
            case 0:
                return 1;
            case 1:
                return 10;
            case 2:
                return 100;
            case 3:
                return 1000;
            case 4:
                return 10000;
            case 5:
                return 100000;
            case 6:
                return 1000000;
            case 7:
                return 10000000;
            case 8:
                return 100000000;
            default:
                throw NumericException.instance();
        }
    }

}