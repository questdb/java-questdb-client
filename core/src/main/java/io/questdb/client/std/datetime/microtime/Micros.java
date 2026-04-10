/*+*****************************************************************************
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

import io.questdb.client.std.datetime.CommonUtils;

import static io.questdb.client.std.datetime.CommonUtils.DAYS_PER_MONTH;

public final class Micros {
    public static final long DAY_MICROS = 86_400_000_000L; // 24 * 60 * 60 * 1000 * 1000L
    public static final long HOUR_MICROS = 3600000000L;
    public static final long MICRO_NANOS = 1000L;
    public static final long MILLI_MICROS = 1000L;
    public static final long MINUTE_MICROS = 60000000L;
    public static final long SECOND_MICROS = 1000000L;
    private static final long[] MAX_MONTH_OF_YEAR_MICROS = new long[12];
    private static final long[] MIN_MONTH_OF_YEAR_MICROS = new long[12];

    private Micros() {
    }

    static {
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
