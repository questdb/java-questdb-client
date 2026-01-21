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

package io.questdb.std;

public final class Vect {
    // Up is decreasing scan direction
    public static final int BIN_SEARCH_SCAN_UP = -1;

    public static void memcpy(long dst, long src, long len) {
        // the split length was determined experimentally
        // using 'MemCopyBenchmark' bench
        if (len < 4096) {
            Unsafe.getUnsafe().copyMemory(src, dst, len);
        } else {
            memcpy0(src, dst, len);
        }
    }

    public static native void memmove(long dst, long src, long len);

    // note: memset only uses single byte of the given int
    public static native void memset(long dst, long len, int value);

    private static native void memcpy0(long src, long dst, long len);

    static {
        Os.init();
    }
}