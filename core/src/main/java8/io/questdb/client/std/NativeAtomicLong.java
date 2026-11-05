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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.std;

final class NativeAtomicLong {
    static final boolean IS_CAS_REQUIRED;

    private NativeAtomicLong() {
    }

    static long getAndAdd(sun.misc.Unsafe unsafe, long address, long delta) {
        // Java 8 AArch64 C1 truncates constant native offsets in getAndAddLong.
        long previous;
        do {
            previous = unsafe.getLongVolatile(null, address);
        } while (!unsafe.compareAndSwapLong(null, address, previous, previous + delta));
        return previous;
    }

    static {
        String arch = System.getProperty("os.arch");
        String vmName = System.getProperty("java.vm.name", "");
        IS_CAS_REQUIRED = ("aarch64".equals(arch) || "arm64".equals(arch))
                && (vmName.startsWith("OpenJDK 64-Bit Server VM")
                || vmName.startsWith("Java HotSpot(TM) 64-Bit Server VM"));
    }
}
