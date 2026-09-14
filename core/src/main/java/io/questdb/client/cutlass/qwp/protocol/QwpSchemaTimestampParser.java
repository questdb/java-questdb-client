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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Source-faithful parser for the eleven formats accepted by QWP STRING-to-timestamp conversion.
 * The grammar and computation mirror the fixed formats in the server's
 * {@code MicrosFormatUtils} and {@code GenericMicrosFormat}; this is deliberately
 * not a general date-format compiler.
 */
final class QwpSchemaTimestampParser {
    private static final long DAY = 86_400_000_000L;
    private static final int DAYS_0000_TO_1970 = 719527;
    private static final long HOUR = 3_600_000_000L;
    private static final long MINUTE = 60_000_000L;
    private static final long SECOND = 1_000_000L;
    private static final long WEEK = 7 * DAY;
    private static final long[] MONTH_COMMON = monthStarts(false);
    private static final long[] MONTH_LEAP = monthStarts(true);
    private static final int REFERENCE_CENTURY = referenceCentury();

    private QwpSchemaTimestampParser() {
    }

    static long parse(CharSequence value) throws NumericException {
        NumericException failure = NumericException.instance();
        try {
            return parsePg(value);
        } catch (NumericException ignored) {
        }
        try {
            return parseIso(value);
        } catch (NumericException ignored) {
        }
        throw failure;
    }

    private static long parseIso(CharSequence value) throws NumericException {
        int n = value.length();
        Parsed p = new Parsed();
        int pos = parseFixedYear(value, 0, n, p);
        if (pos == n) {
            return compute(p, null);
        }
        require(value, pos++, n, '-');
        if (pos + 3 <= n && value.charAt(pos) == 'W') {
            p.week = Numbers.parseInt(value, pos + 1, pos + 3);
            if (pos + 3 != n) {
                throw NumericException.instance();
            }
            return compute(p, null);
        }
        p.month = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return compute(p, null);
        }
        require(value, pos++, n, '-');
        p.day = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return compute(p, null);
        }
        require(value, pos++, n, 'T');
        p.hour = fixedInt(value, pos, pos += 2, n);
        if (pos == n) {
            return compute(p, null);
        }
        require(value, pos++, n, ':');
        p.minute = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        p.second = fixedInt(value, pos, pos += 2, n);
        if (pos < n && value.charAt(pos) == '.') {
            pos++;
            try {
                Parsed copy = new Parsed(p);
                long parsed = greedyMillis(value, pos, n);
                copy.millis = (int) parsed;
                return compute(copy, ZoneMatcher.parse(value, pos + (int) (parsed >>> 32), n));
            } catch (NumericException ignored) {
            }
            if (pos + 6 <= n) {
                try {
                    Parsed copy = new Parsed(p);
                    copy.millis = fixedInt(value, pos, pos + 3, n);
                    copy.micros = fixedInt(value, pos + 3, pos + 6, n);
                    return compute(copy, ZoneMatcher.parse(value, pos + 6, n));
                } catch (NumericException ignored) {
                }
            }
            if (pos + 2 <= n) {
                try {
                    fixedInt(value, pos, pos + 1, n);
                    Parsed copy = new Parsed(p);
                    long parsed = greedyMillis(value, pos + 1, n);
                    copy.millis = (int) parsed;
                    return compute(copy, ZoneMatcher.parse(value, pos + 1 + (int) (parsed >>> 32), n));
                } catch (NumericException ignored) {
                }
            }
            // Final UTC_PATTERN fallback is fixed SSS. It intentionally follows
            // the greedy S and SS forms, and therefore accepts parseInt forms
            // such as +12 and 1_2 as well as a numeric-zone suffix after 3 chars.
            if (pos + 3 <= n) {
                try {
                    Parsed copy = new Parsed(p);
                    copy.millis = fixedInt(value, pos, pos + 3, n);
                    return compute(copy, ZoneMatcher.parse(value, pos + 3, n));
                } catch (NumericException ignored) {
                }
            }
            throw NumericException.instance();
        }
        return compute(p, ZoneMatcher.parse(value, pos, n));
    }

    private static long parsePg(CharSequence value) throws NumericException {
        int n = value.length();
        int dash = indexOf(value, '-', value.length() > 0 && value.charAt(0) == '-' ? 1 : 0, n);
        if (dash < 1) {
            throw NumericException.instance();
        }
        Parsed p = new Parsed();
        int year = greedyInt(value, 0, dash);
        p.year = dash == 2 ? REFERENCE_CENTURY + year : year;
        int pos = dash + 1;
        p.month = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '-');
        p.day = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ' ');
        p.hour = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        p.minute = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, ':');
        p.second = fixedInt(value, pos, pos += 2, n);
        require(value, pos++, n, '.');
        p.millis = fixedInt(value, pos, pos += 3, n);
        return compute(p, ZoneMatcher.parse(value, pos, n));
    }

    private static long greedyMillis(CharSequence value, int lo, int hi) throws NumericException {
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
            throw NumericException.instance();
        }
        while (width < 3) {
            parsed *= 10;
            width++;
        }
        return ((long) (p - lo) << 32) | ((negative ? -parsed : parsed) & 0xffffffffL);
    }

    private static long compute(Parsed p, ZoneMatcher.Zone zone) throws NumericException {
        boolean leap = CommonUtils.isLeapYear(p.year);
        if (p.month < 1 || p.month > 12 || p.day < 1 || p.day > CommonUtils.getDaysPerMonth(p.month, leap)
                || p.hour < 0 || p.hour > 24 || p.minute < 0 || p.minute > 59
                || p.second < 0 || p.second > 59 || (p.week != -1 && (p.week < 1 || p.week > weeks(p.year)))) {
            throw NumericException.instance();
        }
        if (p.week != -1) {
            long first = yearMicros(p.year, CommonUtils.isLeapYear(p.year))
                    + (p.week - 1L) * WEEK + isoYearDayOffset(p.year) * DAY;
            int actualYear = year(first);
            p.month = monthOfYear(first, actualYear, CommonUtils.isLeapYear(actualYear));
            p.year += p.week == 1 && isoYearDayOffset(p.year) < 0 ? -1 : 0;
            p.day = dayOfMonth(first, p.year, p.month, CommonUtils.isLeapYear(p.year));
            // GenericMicrosFormat retains the originally parsed year's leap flag
            // after an ISO week crosses a calendar-year boundary.
        }
        long out = yearMicros(p.year, leap) + monthMicros(p.month, leap) + (p.day - 1L) * DAY
                + (p.hour % 24L) * HOUR + p.minute * MINUTE + p.second * SECOND
                + p.millis * 1000L + p.micros;
        return zone == null ? out : out - zone.offset(out, p.year);
    }

    private static int parseFixedYear(CharSequence s, int pos, int hi, Parsed p) throws NumericException {
        int width = pos < hi && s.charAt(pos) == '-' ? 5 : 4;
        p.year = width == 5
                ? -fixedInt(s, pos + 1, pos + width, hi)
                : fixedInt(s, pos, pos + width, hi);
        return pos + width;
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

    private static long[] monthStarts(boolean leap) {
        long[] result = new long[12];
        for (int i = 1; i < 12; i++) {
            result[i] = result[i - 1] + CommonUtils.getDaysPerMonth(i, leap) * DAY;
        }
        return result;
    }

    private static long monthMicros(int month, boolean leap) {
        return (leap ? MONTH_LEAP : MONTH_COMMON)[month - 1];
    }

    private static long yearMicros(int year, boolean leap) {
        // Preserve server Micros.yearMicros arithmetic, including unchecked
        // multiplication and its asymmetric negative-overflow saturation.
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
        long micros = days * DAY;
        return days < 0 && micros > 0 ? Long.MIN_VALUE : micros;
    }

    private static int dayOfWeek(long micros) {
        long d;
        if (micros > -1) {
            d = micros / DAY;
        } else {
            d = (micros - (DAY - 1)) / DAY;
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
        long elapsed = micros - yearMicros(year, leap);
        long[] starts = leap ? MONTH_LEAP : MONTH_COMMON;
        int month = 12;
        while (month > 1 && elapsed < starts[month - 1]) {
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
        long diff = micros - yearMicros(estimate, leap);
        if (diff < 0) {
            return estimate - 1;
        }
        return diff >= (leap ? 366L : 365L) * DAY ? estimate + 1 : estimate;
    }

    private static int dayOfMonth(long micros, int year, int month, boolean leap) {
        return (int) ((micros - yearMicros(year, leap) - monthMicros(month, leap)) / DAY) + 1;
    }

    private static long nextOrSame(long micros, int dow) {
        int current = dayOfWeek(micros);
        return current <= dow ? micros + (dow - current) * DAY : micros + (7 - current + dow) * DAY;
    }

    private static long previousOrSame(long micros, int dow) {
        int current = dayOfWeek(micros);
        return current >= dow ? micros - (current - dow) * DAY : micros - (7 + current - dow) * DAY;
    }

    private static final class Parsed {
        private int day = 1;
        private int hour;
        private int micros;
        private int millis;
        private int minute;
        private int month = 1;
        private int second;
        private int week = -1;
        private int year;

        private Parsed() {
        }

        private Parsed(Parsed that) {
            day = that.day;
            hour = that.hour;
            micros = that.micros;
            millis = that.millis;
            minute = that.minute;
            month = that.month;
            second = that.second;
            week = that.week;
            year = that.year;
        }
    }

    private static final class ZoneMatcher {
        private static final List<Token> TOKENS = tokens();

        private static Zone parse(CharSequence value, int lo, int hi) throws NumericException {
            if (lo >= hi) {
                throw NumericException.instance();
            }
            int minutes = numericOffset(value, lo, hi);
            if (minutes != Integer.MIN_VALUE) {
                return new Zone(minutes * MINUTE);
            }
            for (Token token : TOKENS) {
                if (token.text.length() == hi - lo && regionMatches(value, lo, token.text)) {
                    return token.zone;
                }
            }
            throw NumericException.instance();
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

        private static List<Token> tokens() {
            List<Token> result = new ArrayList<>();
            Set<String> seen = new java.util.HashSet<>();
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
            add(result, seen, rules.get("UTC"), "UTC");
            String[][] names = new DateFormatSymbols(Locale.ENGLISH).getZoneStrings();
            for (String[] row : names) {
                if (row.length == 0 || !rules.containsKey(row[0])) {
                    continue;
                }
                for (String name : row) {
                    add(result, seen, rules.get(row[0]), name);
                }
            }
            result.sort(Comparator.comparingInt((Token t) -> t.text.length()).reversed());
            return result;
        }

        private static void add(List<Token> tokens, Set<String> seen, Zone zone, String name) {
            if (zone != null && name != null && !name.isEmpty() && seen.add(name)) {
                tokens.add(new Token(name.toUpperCase(), zone));
            }
        }

        private static boolean digit(char c) {
            return c >= '0' && c <= '9';
        }

        private static boolean regionMatches(CharSequence value, int lo, String token) {
            for (int i = 0; i < token.length(); i++) {
                if (Character.toUpperCase(value.charAt(lo + i)) != token.charAt(i)) {
                    return false;
                }
            }
            return true;
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

        private static final class Token {
            private final String text;
            private final Zone zone;

            private Token(String text, Zone zone) {
                this.text = text;
                this.zone = zone;
            }
        }

        private static final class Zone {
            private final long cutoff;
            private final long fixedOffset;
            private final long lastWall;
            private final List<ZoneOffsetTransitionRule> recurring;
            private final ZoneRules rules;

            private Zone(long fixedOffset) {
                this.cutoff = Long.MIN_VALUE;
                this.fixedOffset = fixedOffset;
                this.lastWall = fixedOffset;
                this.recurring = java.util.Collections.emptyList();
                this.rules = null;
            }

            private Zone(ZoneRules rules) {
                this.rules = rules;
                List<ZoneOffsetTransition> history = rules.getTransitions();
                cutoff = history.isEmpty() ? Long.MIN_VALUE : history.get(history.size() - 1).toEpochSecond() * SECOND;
                recurring = rules.getTransitionRules();
                fixedOffset = rules.isFixedOffset() ? rules.getOffset(Instant.EPOCH).getTotalSeconds() * SECOND : Long.MIN_VALUE;
                lastWall = history.isEmpty() ? rules.getOffset(Instant.EPOCH).getTotalSeconds() * SECOND
                        : history.get(history.size() - 1).getOffsetAfter().getTotalSeconds() * SECOND;
            }

            private long offset(long epoch, int year) {
                if (fixedOffset != Long.MIN_VALUE) {
                    return fixedOffset;
                }
                if (!recurring.isEmpty() && epoch > cutoff) {
                    // The server computes future rules with the parsed year,
                    // even when calendar arithmetic has wrapped the epoch.
                    long after = 0;
                    for (ZoneOffsetTransitionRule rule : recurring) {
                        long transition = transitionEpoch(rule, year);
                        long before = rule.getOffsetBefore().getTotalSeconds() * SECOND;
                        if (epoch < transition) {
                            return before;
                        }
                        after = rule.getOffsetAfter().getTotalSeconds() * SECOND;
                    }
                    return after;
                }
                if (epoch > cutoff) {
                    return lastWall;
                }
                Instant instant = Instant.ofEpochSecond(Math.floorDiv(epoch, SECOND), Math.floorMod(epoch, SECOND) * 1000);
                return rules.getOffset(instant).getTotalSeconds() * SECOND;
            }

            private static long transitionEpoch(ZoneOffsetTransitionRule rule, int year) {
                boolean leap = CommonUtils.isLeapYear(year);
                int month = rule.getMonth().getValue();
                int dom = rule.getDayOfMonthIndicator();
                int dow = rule.getDayOfWeek() == null ? -1 : rule.getDayOfWeek().getValue();
                long timestamp;
                if (dom < 0) {
                    timestamp = yearMicros(year, leap) + monthMicros(month, leap)
                            + (CommonUtils.getDaysPerMonth(month, leap) + dom) * DAY
                            + rule.getLocalTime().getHour() * HOUR + rule.getLocalTime().getMinute() * MINUTE
                            + rule.getLocalTime().getSecond() * SECOND;
                    if (dow > -1) {
                        timestamp = previousOrSame(timestamp, dow);
                    }
                } else {
                    timestamp = yearMicros(year, leap) + monthMicros(month, leap) + (dom - 1L) * DAY
                            + rule.getLocalTime().getHour() * HOUR + rule.getLocalTime().getMinute() * MINUTE
                            + rule.getLocalTime().getSecond() * SECOND;
                    if (dow > -1) {
                        timestamp = nextOrSame(timestamp, dow);
                    }
                }
                if (rule.isMidnightEndOfDay()) {
                    timestamp += DAY;
                }
                int before = rule.getOffsetBefore().getTotalSeconds();
                if (rule.getTimeDefinition() == ZoneOffsetTransitionRule.TimeDefinition.UTC) {
                    timestamp += before * SECOND;
                } else if (rule.getTimeDefinition() == ZoneOffsetTransitionRule.TimeDefinition.STANDARD) {
                    timestamp += (before - rule.getStandardOffset().getTotalSeconds()) * SECOND;
                }
                return timestamp - before * SECOND;
            }
        }
    }
}
