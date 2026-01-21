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

import io.questdb.std.str.CharSink;

import org.jetbrains.annotations.Nullable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public interface TimestampDriver {

    void append(CharSink<?> sink, long timestamp);

    /**
     * Converts a value from the specified {@link ChronoUnit} to the timestamp value.
     *
     * @param value the time value to convert
     * @param unit  the {@link ChronoUnit} of the input value
     * @return the timestamp value
     */
    long from(long value, ChronoUnit unit);

    /**
     * Converts a {@link Instant} to the timestamp value.
     *
     * @param instant the {@link Instant} to convert
     * @return the timestamp value
     */
    long from(Instant instant);

    long parseFloor(CharSequence str, int lo, int hi) throws NumericException;

    default long parseFloorLiteral(@Nullable CharSequence timestampLiteral) throws NumericException {
        return timestampLiteral != null ? parseFloor(timestampLiteral, 0, timestampLiteral.length()) : Numbers.LONG_NULL;
    }
}