/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.std.NumericException;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.datetime.CommonUtils;

import java.time.ZoneOffset;

/**
 * Source-faithful parser for the fixed formats accepted by QWP STRING-to-timestamp
 * and STRING-to-DATE conversion. The grammar and computation mirror the server's
 * {@code MicrosFormatUtils}, {@code DateFormatUtils} and generic format parsers;
 * this is deliberately not a general date-format compiler.
 * <p>
 * A time zone is {@code Z}, {@code UTC}, {@code GMT} or a numeric offset, which
 * {@code UTC} or {@code GMT} may precede. The server also resolves zone names such
 * as {@code EST} or {@code Europe/Berlin}, with its JVM's zone data. A client JVM
 * can hold other data and resolve the same name to another offset, so this parser
 * rejects every other name instead.
 */
final class QwpSchemaTimestampParser {
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long GREEDY_MILLIS_FAILED = -1L;
    private static final int[] MONTH_START_DAYS_COMMON = monthStartDays(false);
    private static final int[] MONTH_START_DAYS_LEAP = monthStartDays(true);
    private static final int REFERENCE_CENTURY = referenceCentury();
    // The server parses TIMESTAMP text with MicrosFormatUtils and DATE text with
    // DateFormatUtils, both over UTF-8 bytes. The two differ in year overflow.
    private static final Target DATE = new Target(1L, false);
    private static final Target TIMESTAMP = new Target(1_000L, true);

    private QwpSchemaTimestampParser() {
    }

    /**
     * Parses TIMESTAMP text to epoch micros. The server parses the UTF-8 bytes,
     * where no non-ASCII byte matches anything, so any non-ASCII character fails.
     */
    static long parse(CharSequence value) throws NumericException {
        // Only the PG layout has a space after the day, and a space there fails
        // every ISO layout, so that one character selects the parser.
        int n = value.length();
        int dash = firstDash(value, n);
        if (dash >= 1 && dash + 6 < n && value.charAt(dash + 6) == ' ') {
            return parsePg(value, dash);
        }
        return parseIso(value);
    }

    /** Parses DATE text to epoch millis. */
    static long parseDate(CharSequence value) throws NumericException {
        int n = value.length();
        int dash = firstDash(value, n);
        if (dash < 1) {
            // Both date layouts need an inner dash and a raw number cannot hold one.
            return Numbers.parseLong(value);
        }
        // PG ends at the day or continues with a space. UTC has a 'T' at a fixed
        // position, which parseUtcDate() checks.
        int separator = dash + 6;
        if (separator >= n || value.charAt(separator) == ' ') {
            return parsePgDate(value, dash);
        }
        return parseUtcDate(value);
    }

    private static int firstDash(CharSequence value, int n) {
        return indexOf(value, '-', n > 0 && value.charAt(0) == '-' ? 1 : 0, n);
    }

    private static long parsePgDate(CharSequence value, int dash) throws NumericException {
        int n = value.length();
        int year = greedyInt(value, 0, dash);
        if (dash == 2) {
            year += REFERENCE_CENTURY;
        }
        int pos = dash + 1;
        int month = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '-');
        int day = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return localTime(DATE, year, month, day, 0, 0, 0, 0, 0);
        }
        require(value, pos++, n, ' ');
        int tail = pos;
        try {
            int hour = fixedInt(value, pos, pos += 2, n);
            require(value, pos++, n, ':');
            int minute = fixedInt(value, pos, pos += 2, n);
            require(value, pos++, n, ':');
            int second = fixedInt(value, pos, pos += 2, n);
            int millis = 0;
            if (pos < n && value.charAt(pos) == '.') {
                long parsed = greedyMillis(value, pos + 1, n);
                if (parsed == GREEDY_MILLIS_FAILED) {
                    throw NumericException.instance();
                }
                millis = (int) parsed;
                pos += 1 + (int) (parsed >>> 32);
            }
            return zoned(DATE, localTime(DATE, year, month, day, hour, minute, second, millis, 0), value, pos, n);
        } catch (NumericException ignored) {
            return zoned(DATE, localTime(DATE, year, month, day, 0, 0, 0, 0, 0), value, tail, n);
        }
    }

    private static long parseUtcDate(CharSequence value) throws NumericException {
        int n = value.length();
        // A signed year has four digits after the sign, as in parseIso(). The text
        // holds a dash, so it is not empty.
        int pos = value.charAt(0) == '-' ? 5 : 4;
        int year = pos == 5 ? -fixedInt(value, 1, pos, n) : fixedInt(value, 0, pos, n);
        require(value, pos++, n, '-');
        int month = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '-');
        int day = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, 'T');
        int hour = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        int minute = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        int second = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '.');
        int millis = fixedInt(value, pos, pos += 3, n);
        return zoned(DATE, localTime(DATE, year, month, day, hour, minute, second, millis, 0), value, pos, n);
    }

    private static long parseIso(CharSequence value) throws NumericException {
        int n = value.length();
        int pos = n > 0 && value.charAt(0) == '-' ? 5 : 4;
        int year = pos == 5 ? -fixedInt(value, 1, pos, n) : fixedInt(value, 0, pos, n);
        if (pos == n) {
            return localTime(TIMESTAMP, year, 1, 1, 0, 0, 0, 0, 0);
        }
        require(value, pos++, n, '-');
        if (pos + 3 <= n && value.charAt(pos) == 'W') {
            int week = Numbers.parseInt(value, pos + 1, pos + 3);
            if (pos + 3 != n) {
                throw NumericException.instance();
            }
            return weekMicros(year, week);
        }
        int month = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return localTime(TIMESTAMP, year, month, 1, 0, 0, 0, 0, 0);
        }
        require(value, pos++, n, '-');
        int day = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return localTime(TIMESTAMP, year, month, day, 0, 0, 0, 0, 0);
        }
        require(value, pos++, n, 'T');
        int hour = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return localTime(TIMESTAMP, year, month, day, hour, 0, 0, 0, 0);
        }
        require(value, pos++, n, ':');
        int minute = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        int second = fixedInt(value, pos, pos += 2, n);
        if (pos < n && value.charAt(pos) == '.') {
            return parseIsoFraction(value, pos + 1, n, year, month, day, hour, minute, second);
        }
        return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, 0, 0), value, pos, n);
    }

    /**
     * Tries the server's fraction layouts in its order: greedy S to SSS, then
     * SSSUUU, then S followed by greedy S to SSS, then fixed SSS. A layout whose
     * fraction, fields or zone fails hands over to the next one.
     */
    private static long parseIsoFraction(
            CharSequence value,
            int pos,
            int n,
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second
    ) throws NumericException {
        long parsed = greedyMillis(value, pos, n);
        if (parsed != GREEDY_MILLIS_FAILED) {
            try {
                long local = localTime(TIMESTAMP, year, month, day, hour, minute, second, (int) parsed, 0);
                return zoned(TIMESTAMP, local, value, pos + (int) (parsed >>> 32), n);
            } catch (NumericException ignored) {
            }
        }
        if (pos + 6 <= n) {
            try {
                int millis = fixedInt(value, pos, pos + 3, n);
                int micros = fixedInt(value, pos + 3, pos + 6, n);
                return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, millis, micros), value, pos + 6, n);
            } catch (NumericException ignored) {
            }
        }
        if (pos + 2 <= n) {
            try {
                fixedInt(value, pos, pos + 1, n);
                parsed = greedyMillis(value, pos + 1, n);
                if (parsed != GREEDY_MILLIS_FAILED) {
                    long local = localTime(TIMESTAMP, year, month, day, hour, minute, second, (int) parsed, 0);
                    return zoned(TIMESTAMP, local, value, pos + 1 + (int) (parsed >>> 32), n);
                }
            } catch (NumericException ignored) {
            }
        }
        // Final UTC_PATTERN fallback is fixed SSS. It intentionally follows
        // the greedy S and SS forms, and therefore accepts parseInt forms
        // such as +12 and 1_2 as well as a numeric-zone suffix after 3 chars.
        if (pos + 3 <= n) {
            try {
                int millis = fixedInt(value, pos, pos + 3, n);
                return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, millis, 0), value, pos + 3, n);
            } catch (NumericException ignored) {
            }
        }
        throw NumericException.instance();
    }

    private static long parsePg(CharSequence value, int dash) throws NumericException {
        int n = value.length();
        int year = greedyInt(value, 0, dash);
        if (dash == 2) {
            year += REFERENCE_CENTURY;
        }
        int pos = dash + 1;
        int month = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '-');
        int day = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ' ');
        int hour = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        int minute = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        int second = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '.');
        int millis = fixedInt(value, pos, pos += 3, n);
        return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, millis, 0), value, pos, n);
    }

    /**
     * Returns {@code (width << 32) | millis} for a greedy 1-3 digit fraction with
     * an optional minus sign, or {@link #GREEDY_MILLIS_FAILED}.
     */
    private static long greedyMillis(CharSequence value, int lo, int hi) {
        int p = lo;
        boolean negative = p < hi && value.charAt(p) == '-';
        if (negative) {
            p++;
        }
        int digitStart = p;
        int parsed = 0;
        while (p < hi && value.charAt(p) >= '0' && value.charAt(p) <= '9') {
            parsed = parsed * 10 + value.charAt(p++) - '0';
        }
        int width = p - lo;
        if (p == digitStart || width > 3) {
            return GREEDY_MILLIS_FAILED;
        }
        while (width < 3) {
            parsed *= 10;
            width++;
        }
        return ((long) (p - lo) << 32) | ((negative ? -parsed : parsed) & 0xffffffffL);
    }

    /** Local time in {@code t}'s unit, from validated fields. */
    private static long localTime(
            Target t,
            int year,
            int month,
            int day,
            int hour,
            int minute,
            int second,
            int millis,
            int micros
    ) throws NumericException {
        boolean leap = CommonUtils.isLeapYear(year);
        if (month < 1 || month > 12 || day < 1 || day > CommonUtils.getDaysPerMonth(month, leap)
                || hour < 0 || hour > 24 || minute < 0 || minute > 59
                || second < 0 || second > 59) {
            throw NumericException.instance();
        }
        return yearStart(t, year, leap) + monthStart(t, month, leap) + (day - 1L) * t.day
                + (hour % 24L) * t.hour + minute * t.minute + second * t.second
                + millis * t.milli + micros;
    }

    private static long weekMicros(int year, int week) throws NumericException {
        if (week == -1) {
            // The server's "no week" sentinel is -1, so an explicit W-1 parses
            // as the bare year.
            return localTime(TIMESTAMP, year, 1, 1, 0, 0, 0, 0, 0);
        }
        if (week < 1 || week > weeks(year)) {
            throw NumericException.instance();
        }
        boolean leap = CommonUtils.isLeapYear(year);
        long first = yearStart(TIMESTAMP, year, leap) + (week - 1L) * 7 * TIMESTAMP.day
                + isoYearDayOffset(year) * TIMESTAMP.day;
        int actualYear = year(first);
        int month = monthOfYear(first, actualYear, CommonUtils.isLeapYear(actualYear));
        int weekYear = year + (week == 1 && isoYearDayOffset(year) < 0 ? -1 : 0);
        int day = dayOfMonth(first, weekYear, month, CommonUtils.isLeapYear(weekYear));
        // GenericMicrosFormat retains the originally parsed year's leap flag
        // after an ISO week crosses a calendar-year boundary.
        return yearStart(TIMESTAMP, weekYear, leap) + monthStart(TIMESTAMP, month, leap) + (day - 1L) * TIMESTAMP.day;
    }

    /** Converts {@code local}, in {@code t}'s unit, to UTC using the zone text in {@code [lo, hi)}. */
    private static long zoned(Target t, long local, CharSequence value, int lo, int hi) throws NumericException {
        return local - ZoneMatcher.offset(t, value, lo, hi);
    }

    private static int fixedInt(CharSequence s, int lo, int hi, int limit) throws NumericException {
        if (hi > limit) {
            throw NumericException.instance();
        }
        return Numbers.parseInt(s, lo, hi);
    }

    private static int greedyInt(CharSequence s, int lo, int hi) throws NumericException {
        if (lo >= hi) {
            throw NumericException.instance();
        }
        int sign = 1;
        if (s.charAt(lo) == '-') {
            sign = -1;
            lo++;
        }
        if (lo >= hi) {
            throw NumericException.instance();
        }
        long value = 0;
        for (int i = lo; i < hi; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                throw NumericException.instance();
            }
            value = value * 10 + c - '0';
            if (value > (sign < 0 ? 2147483648L : Integer.MAX_VALUE)) {
                throw NumericException.instance();
            }
        }
        return (int) (sign * value);
    }

    private static int indexOf(CharSequence s, char c, int lo, int hi) {
        for (int i = lo; i < hi; i++) {
            if (s.charAt(i) == c) {
                return i;
            }
        }
        return -1;
    }

    private static void require(CharSequence s, int pos, int hi, char c) throws NumericException {
        if (pos >= hi || s.charAt(pos) != c) {
            throw NumericException.instance();
        }
    }

    private static int referenceCentury() {
        int year = java.time.ZonedDateTime.now(ZoneOffset.UTC).getYear();
        int offset = year % 100;
        return offset + 20 > 100 ? year - offset + 100 : year - offset;
    }

    private static int[] monthStartDays(boolean leap) {
        int[] result = new int[12];
        for (int i = 1; i < 12; i++) {
            result[i] = result[i - 1] + CommonUtils.getDaysPerMonth(i, leap);
        }
        return result;
    }
    private static long monthStart(Target t, int month, boolean leap) {
        return (leap ? MONTH_START_DAYS_LEAP : MONTH_START_DAYS_COMMON)[month - 1] * t.day;
    }
    private static long yearStart(Target t, int year, boolean leap) {
        // Preserve the server's unchecked multiplication. Only its micros
        // variant saturates on negative overflow, and asymmetrically.
        int leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (leap) {
                leapYears--;
            }
        }
        long days = year * 365L + leapYears - DAYS_0000_TO_1970;
        long start = days * t.day;
        return t.isYearSaturating && days < 0 && start > 0 ? Long.MIN_VALUE : start;
    }
    private static int endYearDow(int year) {
        return (year + Math.abs(year / 4) - Math.abs(year / 100) + Math.abs(year / 400)) % 7;
    }

    private static int weeks(int year) {
        return endYearDow(year) == 4 || endYearDow(year - 1) == 3 ? 53 : 52;
    }

    private static int isoYearDayOffset(int year) {
        int d = endYearDow(year - 1);
        return (d <= 3 ? 0 : 7) - d;
    }

    private static int monthOfYear(long micros, int year, boolean leap) {
        long elapsed = micros - yearStart(TIMESTAMP, year, leap);
        int month = 12;
        while (month > 1 && elapsed < monthStart(TIMESTAMP, month, leap)) {
            month--;
        }
        return month;
    }
    private static int year(long micros) {
        int estimate = 1970 + (int) (micros / 31_556_952_000_000L);
        if (micros < 0 && estimate >= 1970) {
            estimate = 1969;
        }
        boolean leap = CommonUtils.isLeapYear(estimate);
        long diff = micros - yearStart(TIMESTAMP, estimate, leap);
        if (diff < 0) {
            return estimate - 1;
        }
        return diff >= (leap ? 366L : 365L) * TIMESTAMP.day ? estimate + 1 : estimate;
    }
    private static int dayOfMonth(long micros, int year, int month, boolean leap) {
        return (int) ((micros - yearStart(TIMESTAMP, year, leap) - monthStart(TIMESTAMP, month, leap)) / TIMESTAMP.day) + 1;
    }
    private static final class ZoneMatcher {

        /**
         * Zone offset in {@code t}'s unit. The server matches zone text against
         * UTF-8 bytes, where a non-ASCII character never matches.
         */
        private static long offset(Target t, CharSequence s, int lo, int hi) throws NumericException {
            if (lo >= hi) {
                throw NumericException.instance();
            }
            int minutes = offsetMinutes(s, lo, hi);
            if (minutes == Integer.MIN_VALUE) {
                // A zone name. See the class comment.
                throw NumericException.instance();
            }
            return minutes * t.minute;
        }

        // Z, UTC, GMT, or a numeric offset that UTC or GMT may precede.
        // Integer.MIN_VALUE for anything else.
        private static int offsetMinutes(CharSequence s, int lo, int hi) {
            if (hi - lo == 1 && (s.charAt(lo) == 'Z' || s.charAt(lo) == 'z')) {
                return 0;
            }
            int p = lo;
            boolean namedPrefix = startsWithIgnoreCase(s, p, hi, "UTC") || startsWithIgnoreCase(s, p, hi, "GMT");
            if (namedPrefix) {
                p += 3;
                if (p == hi) {
                    return 0;
                }
            }
            // The zone text is not empty and a bare UTC or GMT has returned, so p < hi.
            boolean negative = false;
            if (s.charAt(p) == '+' || s.charAt(p) == '-') {
                negative = s.charAt(p++) == '-';
            } else if (namedPrefix) {
                return Integer.MIN_VALUE;
            }
            if (p + 2 > hi || !digit(s.charAt(p)) || !digit(s.charAt(p + 1))) {
                return Integer.MIN_VALUE;
            }
            int hour = (s.charAt(p) - '0') * 10 + s.charAt(p + 1) - '0';
            p += 2;
            int minute = 0;
            if (p < hi) {
                if (s.charAt(p) == ':') {
                    p++;
                }
                if (p + 2 > hi || !digit(s.charAt(p)) || !digit(s.charAt(p + 1))) {
                    return Integer.MIN_VALUE;
                }
                minute = (s.charAt(p) - '0') * 10 + s.charAt(p + 1) - '0';
                p += 2;
            }
            return p == hi && hour <= 23 && minute <= 59
                    ? (negative ? -1 : 1) * (hour * 60 + minute)
                    : Integer.MIN_VALUE;
        }

        private static boolean digit(char c) {
            return c >= '0' && c <= '9';
        }

        // prefix is upper-case ASCII; only an ASCII letter matches it in either case.
        private static boolean startsWithIgnoreCase(CharSequence value, int lo, int hi, String prefix) {
            if (hi - lo < prefix.length()) {
                return false;
            }
            for (int i = 0; i < prefix.length(); i++) {
                char c = value.charAt(lo + i);
                if (c >= 'a' && c <= 'z') {
                    c -= 'a' - 'A';
                }
                if (c != prefix.charAt(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    /** The server's TIMESTAMP (micros) or DATE (millis) text parser family. */
    private static final class Target {
        private final long day;
        private final long hour;
        private final boolean isYearSaturating;
        private final long milli;
        private final long minute;
        private final long second;

        private Target(long unitsPerMilli, boolean isYearSaturating) {
            this.milli = unitsPerMilli;
            this.second = 1_000L * unitsPerMilli;
            this.minute = 60_000L * unitsPerMilli;
            this.hour = 3_600_000L * unitsPerMilli;
            this.day = 86_400_000L * unitsPerMilli;
            this.isYearSaturating = isYearSaturating;
        }
    }
}
