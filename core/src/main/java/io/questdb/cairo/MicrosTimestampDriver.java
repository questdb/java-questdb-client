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

package io.questdb.cairo;

import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.DateLocale;
import io.questdb.std.datetime.TimeZoneRules;
import io.questdb.std.datetime.microtime.Micros;

import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.std.datetime.TimeZoneRuleFactory.RESOLUTION_MICROS;
import static io.questdb.std.datetime.microtime.MicrosFormatUtils.*;

public class MicrosTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new MicrosTimestampDriver();

    private MicrosTimestampDriver() {
    }

    @Override
    public void append(CharSink<?> sink, long timestamp) {
        MicrosFormatUtils.appendDateTimeUSec(sink, timestamp);
    }

    public boolean append(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            MicrosFormatUtils.appendDateTimeUSec(sink, value);
            return true;
        }
        return false;
    }

    @Override
    public long from(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / Micros.MICRO_NANOS;
            case MICROS:
                return value;
            case MILLIS:
                return Math.multiplyExact(value, Micros.MILLI_MICROS);
            case SECONDS:
                return Math.multiplyExact(value, Micros.SECOND_MICROS);
            case MINUTES:
                return Math.multiplyExact(value, Micros.MINUTE_MICROS);
            case HOURS:
                return Math.multiplyExact(value, Micros.HOUR_MICROS);
            default:
                Duration duration = unit.getDuration();
                long micros = Math.multiplyExact(duration.getSeconds(), Micros.SECOND_MICROS);
                micros = Math.addExact(micros, duration.getNano() / Micros.MICRO_NANOS);
                return Math.multiplyExact(micros, value);
        }
    }

    @Override
    public long from(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), Micros.SECOND_MICROS), instant.getNano() / Micros.MICRO_NANOS);
    }

    @Override
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
                                ts = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS
                                        + micr
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Micros.yearMicros(year, l)
                                        + Micros.monthOfYearMicros(month, l)
                                        + (day - 1) * Micros.DAY_MICROS
                                        + hour * Micros.HOUR_MICROS
                                        + min * Micros.MINUTE_MICROS
                                        + sec * Micros.SECOND_MICROS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = Micros.yearMicros(year, l)
                                    + Micros.monthOfYearMicros(month, l)
                                    + (day - 1) * Micros.DAY_MICROS
                                    + hour * Micros.HOUR_MICROS
                                    + min * Micros.MINUTE_MICROS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Micros.yearMicros(year, l)
                                + Micros.monthOfYearMicros(month, l)
                                + (day - 1) * Micros.DAY_MICROS
                                + hour * Micros.HOUR_MICROS;

                    }
                } else {
                    // year + month + day
                    ts = Micros.yearMicros(year, l)
                            + Micros.monthOfYearMicros(month, l)
                            + (day - 1) * Micros.DAY_MICROS;
                }
            } else {
                // year + month
                ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(month, l));
            }
        } else {
            // year
            ts = (Micros.yearMicros(year, l) + Micros.monthOfYearMicros(1, l));
        }
        return ts;
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
}