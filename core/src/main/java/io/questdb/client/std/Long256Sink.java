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
 * Write-side of a 256-bit value split into four 64-bit words. Implementations
 * accept a full value in a single call, so producers can ship all four words
 * with one virtual dispatch instead of one per word.
 */
public interface Long256Sink {

    /**
     * Copies four little-endian 64-bit words starting at {@code address} into
     * this sink. Default implementation issues four native 64-bit loads and
     * delegates to {@link #setAll}.
     */
    default void fromAddress(long address) {
        setAll(
                Unsafe.getUnsafe().getLong(address),
                Unsafe.getUnsafe().getLong(address + 8L),
                Unsafe.getUnsafe().getLong(address + 16L),
                Unsafe.getUnsafe().getLong(address + 24L)
        );
    }

    /**
     * Sets all four 64-bit words of this value. {@code l0} is the least
     * significant word; {@code l3} the most significant.
     */
    void setAll(long l0, long l1, long l2, long l3);
}
