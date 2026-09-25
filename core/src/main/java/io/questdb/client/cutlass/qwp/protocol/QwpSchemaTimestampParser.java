/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.std.NumericException;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.datetime.CommonUtils;

import java.text.DateFormatSymbols;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneOffsetTransitionRule;
import java.time.zone.ZoneRules;
import java.time.zone.ZoneRulesProvider;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Source-faithful parser for the fixed formats accepted by QWP STRING-to-timestamp
 * and STRING-to-DATE conversion. The grammar and computation mirror the server's
 * {@code MicrosFormatUtils}, {@code DateFormatUtils} and generic format parsers;
 * this is deliberately not a general date-format compiler.
 */
final class QwpSchemaTimestampParser {
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long GREEDY_MILLIS_FAILED = -1L;
    private static final int[] MONTH_START_DAYS_COMMON = monthStartDays(false);
    private static final int[] MONTH_START_DAYS_LEAP = monthStartDays(true);
    private static final int REFERENCE_CENTURY = referenceCentury();
    // The server parses TIMESTAMP text with MicrosFormatUtils over UTF-8 bytes,
    // and DATE text with DateFormatUtils over UTF-16. Its two parsers also differ
    // in year overflow and in zone rules for negative years.
    private static final Target DATE = new Target(1L, false, false, true);
    private static final Target TIMESTAMP = new Target(1_000L, true, true, false);

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

    /** Parses DATE text to epoch millis; the server parses DATE text as UTF-16. */
    static long parseDate(CharSequence value) throws NumericException {
        int n = value.length();
        int dash = firstDash(value, n);
        if (dash < 1) {
            // Both date layouts need an inner dash and a raw number cannot hold one.
            return Numbers.parseLong(value);
        }
        // PG ends at the day or continues with a space; only UTC has a 'T' there.
        int separator = dash + 6;
        if (separator >= n || value.charAt(separator) == ' ') {
            return parsePgDate(value, dash);
        }
        if (value.charAt(separator) == 'T') {
            return parseUtcDate(value);
        }
        throw NumericException.instance();
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
            return zoned(DATE, localTime(DATE, year, month, day, hour, minute, second, millis, 0), year, value, pos, n);
        } catch (NumericException ignored) {
            return zoned(DATE, localTime(DATE, year, month, day, 0, 0, 0, 0, 0), year, value, tail, n);
        }
    }

    private static long parseUtcDate(CharSequence value) throws NumericException {
        int n = value.length();
        int pos = 0;
        int year = fixedInt(value, pos, pos += 4, n);
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
        return zoned(DATE, localTime(DATE, year, month, day, hour, minute, second, millis, 0), year, value, pos, n);
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
        return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, 0, 0), year, value, pos, n);
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
                return zoned(TIMESTAMP, local, year, value, pos + (int) (parsed >>> 32), n);
            } catch (NumericException ignored) {
            }
        }
        if (pos + 6 <= n) {
            try {
                int millis = fixedInt(value, pos, pos + 3, n);
                int micros = fixedInt(value, pos + 3, pos + 6, n);
                return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, millis, micros), year, value, pos + 6, n);
            } catch (NumericException ignored) {
            }
        }
        if (pos + 2 <= n) {
            try {
                fixedInt(value, pos, pos + 1, n);
                parsed = greedyMillis(value, pos + 1, n);
                if (parsed != GREEDY_MILLIS_FAILED) {
                    long local = localTime(TIMESTAMP, year, month, day, hour, minute, second, (int) parsed, 0);
                    return zoned(TIMESTAMP, local, year, value, pos + 1 + (int) (parsed >>> 32), n);
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
                return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, millis, 0), year, value, pos + 3, n);
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
        return zoned(TIMESTAMP, localTime(TIMESTAMP, year, month, day, hour, minute, second, millis, 0), year, value, pos, n);
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
    private static long zoned(Target t, long local, int year, CharSequence value, int lo, int hi) throws NumericException {
        return local - ZoneMatcher.offset(t, value, lo, hi, local, year);
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
    private static int dayOfWeek(Target t, long time) {
        long d;
        if (time > -1) {
            d = time / t.day;
        } else {
            d = (time - (t.day - 1)) / t.day;
            if (d < -3) {
                return 7 + (int) ((d + 4) % 7);
            }
        }
        return 1 + (int) ((d + 3) % 7);
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
    private static long nextOrSame(Target t, long time, int dow) {
        int current = dayOfWeek(t, time);
        return current <= dow ? time + (dow - current) * t.day : time + (7 - current + dow) * t.day;
    }
    private static long previousOrSame(Target t, long time, int dow) {
        int current = dayOfWeek(t, time);
        return current >= dow ? time - (current - dow) * t.day : time - (7 + current - dow) * t.day;
    }
    private static final class ZoneMatcher {

        /**
         * Zone offset in {@code t}'s unit. The server matches TIMESTAMP zone text
         * against UTF-8 bytes, where a non-ASCII name never matches, and DATE zone
         * text case-insensitively as UTF-16.
         */
        private static long offset(Target t, CharSequence s, int lo, int hi, long epoch, int year) throws NumericException {
            if (lo >= hi) {
                throw NumericException.instance();
            }
            int minutes = numericOffset(s, lo, hi);
            if (minutes != Integer.MIN_VALUE) {
                return minutes * t.minute;
            }
            return NamedZones.find(s, lo, hi, t.isZoneAsciiOnly).offset(t, epoch, year);
        }

        private static int numericOffset(CharSequence s, int lo, int hi) {
            if (hi - lo == 1 && (s.charAt(lo) == 'Z' || s.charAt(lo) == 'z')) {
                return 0;
            }
            int p = lo;
            boolean namedPrefix = startsWithIgnoreCase(s, p, hi, "UTC") || startsWithIgnoreCase(s, p, hi, "GMT");
            if (namedPrefix) {
                p += 3;
            }
            boolean negative = false;
            if (p < hi && (s.charAt(p) == '+' || s.charAt(p) == '-')) {
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

        private static boolean startsWithIgnoreCase(CharSequence value, int lo, int hi, String prefix) {
            if (hi - lo < prefix.length()) {
                return false;
            }
            for (int i = 0; i < prefix.length(); i++) {
                if (Character.toUpperCase(value.charAt(lo + i)) != prefix.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Zone names by case-insensitive text. A holder class, so that only a
         * named-zone lookup loads every zone's rules; 'Z' and numeric offsets
         * never initialize it.
         */
        private static final class NamedZones {
            private static final int MASK;
            private static final String[] NAMES;
            private static final Zone[] ZONES;

            static {
                Map<String, Zone> rules = new HashMap<>();
                for (String id : ZoneRulesProvider.getAvailableZoneIds()) {
                    rules.put(id, new Zone(ZoneRulesProvider.getRules(id, true)));
                }
                for (Map.Entry<String, String> alias : ZoneId.SHORT_IDS.entrySet()) {
                    if (!rules.containsKey(alias.getKey())) {
                        Zone target = rules.get(alias.getValue());
                        if (target == null) {
                            target = new Zone(ZoneId.of(alias.getValue()).getRules());
                        }
                        rules.put(alias.getKey(), target);
                    }
                }
                String[][] rows = new DateFormatSymbols(Locale.ENGLISH).getZoneStrings();
                int names = 1;
                for (String[] row : rows) {
                    names += row.length;
                }
                // At most half full, so a miss ends at an empty slot quickly.
                int capacity = 16;
                while (capacity < 2 * names) {
                    capacity <<= 1;
                }
                MASK = capacity - 1;
                NAMES = new String[capacity];
                ZONES = new Zone[capacity];
                // The first definition of a name wins; UTC goes first.
                define(rules.get("UTC"), "UTC");
                for (String[] row : rows) {
                    if (row.length == 0 || !rules.containsKey(row[0])) {
                        continue;
                    }
                    Zone zone = rules.get(row[0]);
                    for (String name : row) {
                        define(zone, name);
                    }
                }
            }

            private static void define(Zone zone, String name) {
                if (zone == null || name == null || name.isEmpty()) {
                    return;
                }
                String text = name.toUpperCase(Locale.ROOT);
                int hash = 0;
                for (int i = 0, n = text.length(); i < n; i++) {
                    hash = 31 * hash + text.charAt(i);
                }
                int slot = spread(hash) & MASK;
                while (NAMES[slot] != null) {
                    if (NAMES[slot].equals(text)) {
                        return;
                    }
                    slot = (slot + 1) & MASK;
                }
                NAMES[slot] = text;
                ZONES[slot] = zone;
            }

            private static Zone find(CharSequence s, int lo, int hi, boolean isAsciiOnly) throws NumericException {
                int hash = 0;
                for (int i = lo; i < hi; i++) {
                    char c = s.charAt(i);
                    if (isAsciiOnly && c > 0x7f) {
                        throw NumericException.instance();
                    }
                    hash = 31 * hash + Character.toUpperCase(c);
                }
                int length = hi - lo;
                for (int slot = spread(hash) & MASK; NAMES[slot] != null; slot = (slot + 1) & MASK) {
                    String name = NAMES[slot];
                    if (name.length() == length && regionMatches(s, lo, name)) {
                        return ZONES[slot];
                    }
                }
                throw NumericException.instance();
            }

            private static boolean regionMatches(CharSequence value, int lo, String name) {
                for (int i = 0, n = name.length(); i < n; i++) {
                    if (Character.toUpperCase(value.charAt(lo + i)) != name.charAt(i)) {
                        return false;
                    }
                }
                return true;
            }

            private static int spread(int hash) {
                return hash ^ (hash >>> 16);
            }
        }

        private static final class Zone {
            // Seconds, or Long.MIN_VALUE when absent.
            private final long cutoffSeconds;
            private final long fixedOffsetSeconds;
            private final long lastWallSeconds;
            private final List<ZoneOffsetTransitionRule> recurring;
            private final ZoneRules rules;

            private Zone(ZoneRules rules) {
                this.rules = rules;
                List<ZoneOffsetTransition> history = rules.getTransitions();
                cutoffSeconds = history.isEmpty() ? Long.MIN_VALUE : history.get(history.size() - 1).toEpochSecond();
                recurring = rules.getTransitionRules();
                fixedOffsetSeconds = rules.isFixedOffset() ? rules.getOffset(Instant.EPOCH).getTotalSeconds() : Long.MIN_VALUE;
                lastWallSeconds = history.isEmpty() ? rules.getOffset(Instant.EPOCH).getTotalSeconds()
                        : history.get(history.size() - 1).getOffsetAfter().getTotalSeconds();
            }

            private static long transitionEpoch(Target t, ZoneOffsetTransitionRule rule, int year) {
                boolean leap = CommonUtils.isLeapYear(year);
                int month = rule.getMonth().getValue();
                int dom = rule.getDayOfMonthIndicator();
                int dow = rule.getDayOfWeek() == null ? -1 : rule.getDayOfWeek().getValue();
                long time = rule.getLocalTime().getHour() * t.hour + rule.getLocalTime().getMinute() * t.minute
                        + rule.getLocalTime().getSecond() * t.second;
                long timestamp;
                if (dom < 0) {
                    timestamp = yearStart(t, year, leap) + monthStart(t, month, leap)
                            + (CommonUtils.getDaysPerMonth(month, leap) + dom) * t.day + time;
                    if (dow > -1) {
                        timestamp = previousOrSame(t, timestamp, dow);
                    }
                } else {
                    timestamp = yearStart(t, year, leap) + monthStart(t, month, leap) + (dom - 1L) * t.day + time;
                    if (dow > -1) {
                        timestamp = nextOrSame(t, timestamp, dow);
                    }
                }
                if (rule.isMidnightEndOfDay()) {
                    timestamp += t.day;
                }
                int before = rule.getOffsetBefore().getTotalSeconds();
                if (rule.getTimeDefinition() == ZoneOffsetTransitionRule.TimeDefinition.UTC) {
                    timestamp += before * t.second;
                } else if (rule.getTimeDefinition() == ZoneOffsetTransitionRule.TimeDefinition.STANDARD) {
                    timestamp += (before - rule.getStandardOffset().getTotalSeconds()) * t.second;
                }
                return timestamp - before * t.second;
            }

            private long offset(Target t, long epoch, int year) throws NumericException {
                if (fixedOffsetSeconds != Long.MIN_VALUE) {
                    return fixedOffsetSeconds * t.second;
                }
                long cutoff = cutoffSeconds == Long.MIN_VALUE ? Long.MIN_VALUE : cutoffSeconds * t.second;
                if (!recurring.isEmpty() && epoch > cutoff) {
                    if (t.isNegativeYearRuleRejected && year < 0) {
                        // The server's DATE recurring-rule cache rejects negative
                        // year keys. This is only reachable after full-width DATE
                        // arithmetic wraps past the cutoff.
                        throw NumericException.instance();
                    }
                    // The server computes future rules with the parsed year,
                    // even when calendar arithmetic has wrapped the epoch.
                    long after = 0;
                    for (int i = 0, n = recurring.size(); i < n; i++) {
                        ZoneOffsetTransitionRule rule = recurring.get(i);
                        if (epoch < transitionEpoch(t, rule, year)) {
                            return rule.getOffsetBefore().getTotalSeconds() * t.second;
                        }
                        after = rule.getOffsetAfter().getTotalSeconds() * t.second;
                    }
                    return after;
                }
                if (epoch > cutoff) {
                    return lastWallSeconds * t.second;
                }
                Instant instant = Instant.ofEpochSecond(Math.floorDiv(epoch, t.second), Math.floorMod(epoch, t.second) * t.nanos);
                return rules.getOffset(instant).getTotalSeconds() * t.second;
            }
        }
    }

    /** The server's TIMESTAMP (micros) or DATE (millis) text parser family. */
    private static final class Target {
        private final long day;
        private final long hour;
        private final boolean isNegativeYearRuleRejected;
        private final boolean isYearSaturating;
        private final boolean isZoneAsciiOnly;
        private final long milli;
        private final long minute;
        // Nanos per unit.
        private final long nanos;
        private final long second;

        private Target(
                long unitsPerMilli,
                boolean isYearSaturating,
                boolean isZoneAsciiOnly,
                boolean isNegativeYearRuleRejected
        ) {
            this.milli = unitsPerMilli;
            this.second = 1_000L * unitsPerMilli;
            this.minute = 60_000L * unitsPerMilli;
            this.hour = 3_600_000L * unitsPerMilli;
            this.day = 86_400_000L * unitsPerMilli;
            this.nanos = 1_000_000L / unitsPerMilli;
            this.isYearSaturating = isYearSaturating;
            this.isZoneAsciiOnly = isZoneAsciiOnly;
            this.isNegativeYearRuleRejected = isNegativeYearRuleRejected;
        }
    }
}
