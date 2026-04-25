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

package io.questdb.client.std;

/**
 * Mutable 128-bit UUID value split into two 64-bit words. Intended as a
 * reusable sink: hand the same instance to a series of producers (e.g. QWP
 * batch accessors) and read {@link #getLo} / {@link #getHi} after each
 * {@link #setAll} call.
 */
public class Uuid {
    public static final int BYTES = 16;
    private long hi;
    private long lo;

    /**
     * Loads the two 64-bit words from {@code address} (little-endian, low word first).
     */
    public void fromAddress(long address) {
        setAll(
                Unsafe.getUnsafe().getLong(address),
                Unsafe.getUnsafe().getLong(address + 8L)
        );
    }

    public long getHi() {
        return hi;
    }

    public long getLo() {
        return lo;
    }

    /**
     * Sets both 64-bit words. {@code lo} is the least significant; {@code hi}
     * the most significant.
     */
    public void setAll(long lo, long hi) {
        this.lo = lo;
        this.hi = hi;
    }
}
