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

package io.questdb.client.cairo;

import io.questdb.client.std.datetime.microtime.Micros;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class MicrosTimestampDriver implements TimestampDriver {
    public static final TimestampDriver INSTANCE = new MicrosTimestampDriver();

    private MicrosTimestampDriver() {
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
}