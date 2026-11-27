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

public final class Vect {
    private static final int SMALL_COPY_MAX = 64;

    public static void memcpy(long dst, long src, long len) {
        if (len <= SMALL_COPY_MAX) {
            // A column of one row is a few bytes: word moves beat the copy
            // stub's call and dispatch.
            long i = 0;
            for (; i + 8 <= len; i += 8) {
                Unsafe.getUnsafe().putLong(dst + i, Unsafe.getUnsafe().getLong(src + i));
            }
            for (; i < len; i++) {
                Unsafe.getUnsafe().putByte(dst + i, Unsafe.getUnsafe().getByte(src + i));
            }
            return;
        }
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
