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

package io.questdb.client.std.fastdouble;

import io.questdb.client.std.NumericException;

/**
 * Provides static method for parsing a {@code float} from a
 * {@link CharSequence}, {@code char} array or {@code byte} array.
 */
public final class FastFloatParser {

    /**
     * Convenience method for calling {@link #parseFloat(CharSequence, int, int, boolean)}.
     *
     * @param str            the string to be parsed
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(CharSequence str, boolean rejectOverflow) throws NumericException {
        return parseFloat(str, 0, str.length(), rejectOverflow);
    }

    /**
     * Parses a {@code FloatingPointLiteral} from a {@link CharSequence} and converts it
     * into a {@code float} value.
     * <p>
     * See {@link io.questdb.std.fastdouble} for the syntax of {@code FloatingPointLiteral}.
     *
     * @param str            the string to be parsed
     * @param offset         the start offset of the {@code FloatingPointLiteral} in {@code str}
     * @param length         the length of {@code FloatingPointLiteral} in {@code str}
     * @param rejectOverflow reject parsed values that overflow float type
     * @return the parsed value
     * @throws NumericException if the string can not be parsed
     */
    public static float parseFloat(CharSequence str, int offset, int length, boolean rejectOverflow) throws NumericException {
        return FastFloat.parseFloatingPointLiteral(str, offset, length, rejectOverflow);
    }

}