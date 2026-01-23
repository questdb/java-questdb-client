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

import io.questdb.client.std.datetime.nanotime.Nanos;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class NanosTimestampDriver implements TimestampDriver {
    public static final NanosTimestampDriver INSTANCE = new NanosTimestampDriver();

    private NanosTimestampDriver() {
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
}