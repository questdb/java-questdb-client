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

import io.questdb.client.cairo.CairoException;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.LongAdder;

public final class Unsafe {
    // The various _ADDR fields are `long` in Java, but they are `* mut usize` in Rust, or `size_t*` in C.
    // These are off-heap allocated atomic counters for memory usage tracking.

    public static final long BYTE_OFFSET;
    private static final LongAdder[] COUNTERS = new LongAdder[MemoryTag.SIZE];
    private static final long FREE_COUNT_ADDR;
    private static final long MALLOC_COUNT_ADDR;
    private static final long[] NATIVE_MEM_COUNTER_ADDRS = new long[MemoryTag.SIZE];
    private static final long NON_RSS_MEM_USED_ADDR;
    private static final long OVERRIDE;
    private static final long REALLOC_COUNT_ADDR;
    private static final long RSS_MEM_USED_ADDR;
    private static final sun.misc.Unsafe UNSAFE;

    private Unsafe() {
    }

    public static int byteArrayGetInt(byte[] array, int index) {
        assert index > -1 && index < array.length - 3;
        return Unsafe.getUnsafe().getInt(array, BYTE_OFFSET + index);
    }

    public static long byteArrayGetLong(byte[] array, int index) {
        assert index > -1 && index < array.length - 7;
        return Unsafe.getUnsafe().getLong(array, BYTE_OFFSET + index);
    }

    public static short byteArrayGetShort(byte[] array, int index) {
        assert index > -1 && index < array.length - 1;
        return Unsafe.getUnsafe().getShort(array, BYTE_OFFSET + index);
    }

    public static long calloc(long size, int memoryTag) {
        long ptr = malloc(size, memoryTag);
        Vect.memset(ptr, size, 0);
        return ptr;
    }

    public static long free(long ptr, long size, int memoryTag) {
        if (ptr != 0) {
            Unsafe.getUnsafe().freeMemory(ptr);
            incrFreeCount();
            recordMemAlloc(-size, memoryTag);
        }
        return 0;
    }

    public static long getFieldOffset(Class<?> clazz, String name) {
        try {
            return UNSAFE.objectFieldOffset(clazz.getDeclaredField(name));
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static long getFreeCount() {
        return UNSAFE.getLongVolatile(null, FREE_COUNT_ADDR);
    }

    public static long getMallocCount() {
        return UNSAFE.getLongVolatile(null, MALLOC_COUNT_ADDR);
    }

    /**
     * Get the total memory used by the process, this includes both resident memory
     * and that assigned to memory mapped files.
     */
    public static long getMemUsed() {
        return UNSAFE.getLongVolatile(null, NON_RSS_MEM_USED_ADDR) +
                UNSAFE.getLongVolatile(null, RSS_MEM_USED_ADDR);
    }

    public static long getMemUsedByTag(int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        return COUNTERS[memoryTag].sum() + UNSAFE.getLongVolatile(null, NATIVE_MEM_COUNTER_ADDRS[memoryTag]);
    }

    public static long getReallocCount() {
        return UNSAFE.getLongVolatile(null, REALLOC_COUNT_ADDR);
    }

    public static long getRssMemUsed() {
        return UNSAFE.getLongVolatile(null, RSS_MEM_USED_ADDR);
    }

    public static sun.misc.Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static void incrFreeCount() {
        UNSAFE.getAndAddLong(null, FREE_COUNT_ADDR, 1);
    }

    public static void incrMallocCount() {
        UNSAFE.getAndAddLong(null, MALLOC_COUNT_ADDR, 1);
    }

    public static void incrReallocCount() {
        UNSAFE.getAndAddLong(null, REALLOC_COUNT_ADDR, 1);
    }

    /**
     * Equivalent to {@link AccessibleObject#setAccessible(boolean) AccessibleObject.setAccessible(true)}, except that
     * it does not produce an illegal access error or warning.
     *
     * @param accessibleObject the instance to make accessible
     */
    public static void makeAccessible(AccessibleObject accessibleObject) {
        UNSAFE.putBooleanVolatile(accessibleObject, OVERRIDE, true);
    }

    public static long malloc(long size, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            long ptr = Unsafe.getUnsafe().allocateMemory(size);
            recordMemAlloc(size, memoryTag);
            incrMallocCount();
            return ptr;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical()
                    .put("sun.misc.Unsafe.allocateMemory() OutOfMemoryError [RSS_MEM_USED=")
                    .put(getRssMemUsed())
                    .put(", size=")
                    .put(size)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    public static long realloc(long address, long oldSize, long newSize, int memoryTag) {
        try {
            assert memoryTag >= MemoryTag.NATIVE_PATH;
            long ptr = Unsafe.getUnsafe().reallocateMemory(address, newSize);
            recordMemAlloc(-oldSize + newSize, memoryTag);
            incrReallocCount();
            return ptr;
        } catch (OutOfMemoryError oom) {
            CairoException e = CairoException.nonCritical()
                    .put("sun.misc.Unsafe.reallocateMemory() OutOfMemoryError [RSS_MEM_USED=")
                    .put(getRssMemUsed())
                    .put(", oldSize=")
                    .put(oldSize)
                    .put(", newSize=")
                    .put(newSize)
                    .put(", memoryTag=").put(memoryTag)
                    .put("], original message: ")
                    .put(oom.getMessage());
            System.err.println(e.getFlyweightMessage());
            throw e;
        }
    }

    public static void recordMemAlloc(long size, int memoryTag) {
        assert memoryTag >= 0 && memoryTag < MemoryTag.SIZE;
        COUNTERS[memoryTag].add(size);
        if (memoryTag >= MemoryTag.NATIVE_DEFAULT) {
            final long mem = UNSAFE.getAndAddLong(null, RSS_MEM_USED_ADDR, size) + size;
            assert mem >= 0 : "unexpected RSS mem: " + mem + ", size: " + size + ", memoryTag:" + memoryTag;
        } else {
            final long mem = UNSAFE.getAndAddLong(null, NON_RSS_MEM_USED_ADDR, size) + size;
            assert mem >= 0 : "unexpected non-RSS mem: " + mem + ", size: " + size + ", memoryTag:" + memoryTag;
        }
    }

    private static long AccessibleObject_override_fieldOffset() {
        if (isJava8Or11()) {
            return getFieldOffset(AccessibleObject.class, "override");
        }
        // From Java 12 onwards, AccessibleObject#override is protected and cannot be accessed reflectively.
        boolean is32BitJVM = is32BitJVM();
        if (is32BitJVM) {
            return 8L;
        }
        if (getOrdinaryObjectPointersCompressionStatus(is32BitJVM)) {
            return 12L;
        }
        return 16L;
    }

    private static boolean getOrdinaryObjectPointersCompressionStatus(boolean is32BitJVM) {
        class Probe {
            @SuppressWarnings("unused")
            private int intField; // Accessed through reflection

            boolean probe() {
                long offset = getFieldOffset(Probe.class, "intField");
                if (offset == 8L) {
                    assert is32BitJVM;
                    return false;
                }
                if (offset == 12L) {
                    return true;
                }
                if (offset == 16L) {
                    return false;
                }
                throw new AssertionError(offset);
            }
        }
        return new Probe().probe();
    }

    private static boolean is32BitJVM() {
        String sunArchDataModel = System.getProperty("sun.arch.data.model");
        return sunArchDataModel.equals("32");
    }

    private static boolean isJava8Or11() {
        String javaVersion = System.getProperty("java.version");
        return javaVersion.startsWith("11") || javaVersion.startsWith("1.8");
    }

    static {
        try {
            Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) theUnsafe.get(null);

            BYTE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);

            OVERRIDE = AccessibleObject_override_fieldOffset();
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }

        // A single allocation for all the off-heap native memory counters.
        // Might help with locality, given they're often incremented together.
        // All initial values set to 0.
        final long nativeMemCountersArraySize = (5 + COUNTERS.length) * 8;
        final long nativeMemCountersArray = UNSAFE.allocateMemory(nativeMemCountersArraySize);
        long ptr = nativeMemCountersArray;
        Vect.memset(nativeMemCountersArray, nativeMemCountersArraySize, 0);

        RSS_MEM_USED_ADDR = ptr;
        ptr += 8;
        MALLOC_COUNT_ADDR = ptr;
        ptr += 8;
        REALLOC_COUNT_ADDR = ptr;
        ptr += 8;
        FREE_COUNT_ADDR = ptr;
        ptr += 8;
        NON_RSS_MEM_USED_ADDR = ptr;
        ptr += 8;
        for (int i = 0; i < COUNTERS.length; i++) {
            COUNTERS[i] = new LongAdder();
            NATIVE_MEM_COUNTER_ADDRS[i] = ptr;
            ptr += 8;
        }
    }
}
