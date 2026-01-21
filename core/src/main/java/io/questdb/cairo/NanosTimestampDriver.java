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

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.nanotime.Nanos;

import io.questdb.std.datetime.nanotime.NanosFormatUtils;
import io.questdb.std.str.CharSink;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class NanosTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new NanosTimestampDriver();

    private NanosTimestampDriver() {
    }

    @Override
    public void append(CharSink<?> sink, long timestamp) {
        NanosFormatUtils.appendDateTimeNSec(sink, timestamp);
    }

    public boolean append(long fixedAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(fixedAddr);
        if (value != Numbers.LONG_NULL) {
            NanosFormatUtils.appendDateTimeNSec(sink, value);
            return true;
        }
        return false;
    }

    public static long floor(CharSequence value) throws NumericException {
        return INSTANCE.parseFloorLiteral(value);
    }

    @Override
    public long from(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value;
            case MICROS:
                return Math.multiplyExact(value, Nanos.MICRO_NANOS);
            case MILLIS:
                return Math.multiplyExact(value, Nanos.MILLI_NANOS);
            case SECONDS:
                return Math.multiplyExact(value, Nanos.SECOND_NANOS);
            default:
                Duration duration = unit.getDuration();
                long totalSeconds = Math.multiplyExact(duration.getSeconds(), value);
                long totalNanos = Math.multiplyExact(duration.getNano(), value);
                return Math.addExact(
                        Math.multiplyExact(totalSeconds, Nanos.SECOND_NANOS),
                        totalNanos
                );
        }
    }

    @Override
    public long from(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), Nanos.SECOND_NANOS), instant.getNano());
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
                                // varlen milli, micros and nanos
                                int nanoLim = p + 9;
                                int nlim = Math.min(hi, nanoLim);
                                int nano = 0;
                                for (; p < nlim; p++) {
                                    char c = str.charAt(p);
                                    if (Numbers.notDigit(c)) {
                                        // Timezone
                                        break;
                                    }
                                    nano *= 10;
                                    nano += c - '0';
                                }
                                nano *= CommonUtils.tenPow(nanoLim - p);

                                // nanos
                                ts = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS
                                        + nano
                                        + checkTimezoneTail(str, p, hi);
                            } else {
                                // seconds
                                ts = Nanos.yearNanos(year, l)
                                        + Nanos.monthOfYearNanos(month, l)
                                        + (day - 1) * Nanos.DAY_NANOS
                                        + hour * Nanos.HOUR_NANOS
                                        + min * Nanos.MINUTE_NANOS
                                        + sec * Nanos.SECOND_NANOS
                                        + checkTimezoneTail(str, p, hi);
                            }
                        } else {
                            // minute
                            ts = Nanos.yearNanos(year, l)
                                    + Nanos.monthOfYearNanos(month, l)
                                    + (day - 1) * Nanos.DAY_NANOS
                                    + hour * Nanos.HOUR_NANOS
                                    + min * Nanos.MINUTE_NANOS;

                        }
                    } else {
                        // year + month + day + hour
                        ts = Nanos.yearNanos(year, l)
                                + Nanos.monthOfYearNanos(month, l)
                                + (day - 1) * Nanos.DAY_NANOS
                                + hour * Nanos.HOUR_NANOS;

                    }
                } else {
                    // year + month + day
                    ts = Nanos.yearNanos(year, l)
                            + Nanos.monthOfYearNanos(month, l)
                            + (day - 1) * Nanos.DAY_NANOS;
                }
            } else {
                // year + month
                ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(month, l));
            }
        } else {
            // year
            ts = (Nanos.yearNanos(year, l) + Nanos.monthOfYearNanos(1, l));
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
                return tzSign * (hour * Nanos.HOUR_NANOS + min * Nanos.MINUTE_NANOS);
            } else {
                return tzSign * (hour * Nanos.HOUR_NANOS);
            }
        }
        throw NumericException.instance();
    }
}