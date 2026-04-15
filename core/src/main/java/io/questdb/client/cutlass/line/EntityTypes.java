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

package io.questdb.client.cutlass.line;

import io.questdb.client.cairo.ColumnType;

public class EntityTypes {

    public static final byte ARRAY = 14;
    /**
     * Representation of the {@link ColumnType#DECIMAL} type in ILP.
     * <p>
     * - text format: float-formatted number suffixed with `d`
     * <p>
     * - binary format:
     * <pre>
     *    +--------+--------+------------+
     *    | scale  |  len   |   values   |
     *    +--------+--------+------------+
     *    | 1 byte | 1 byte | $len bytes |
     *    +--------+--------+------------+
     * </pre>
     * <p>
     * Values is the unscaled value of the decimal in big-endian two's complement format.
     * <p>
     * Casting:
     * <p>
     * - From: STRING, FLOAT, INTEGER
     */
    public static final byte DECIMAL = 23;
    public static final byte DOUBLE = 16;
}
