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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

/**
 * Thin bridge over the JDK's internal {@code jdk.internal.math.FDBigInteger},
 * used only by the {@link Numbers} double-to-string slow path. This is the
 * Java 9+ variant; the parallel copy under {@code src/main/java8} targets
 * {@code sun.misc.FDBigInteger} directly. Keeping the type behind this wrapper
 * lets {@code Numbers} carry a single, JDK-agnostic copy of the algorithm.
 * <p>
 * The bignum class is never named in source. Up to JDK 25 it was a
 * {@code public} class in a non-exported package, so a compile-time
 * {@code --add-exports} plus a reflective runtime export was enough to reach
 * it. JDK 26 (JDK-8366017) made the class itself package-private, and no
 * module export can make a non-public type accessible. Instead, every method
 * is resolved once into a {@code static final} {@link MethodHandle}:
 * <ol>
 * <li>{@link Class#forName(String)} loads the class; loading performs no
 * accessibility check.</li>
 * <li>{@link Class#getDeclaredMethod} finds the member; lookup performs no
 * accessibility check either.</li>
 * <li>{@link Unsafe#makeAccessible} flips {@code AccessibleObject.override}
 * directly, bypassing {@code setAccessible}'s module check.</li>
 * <li>{@link MethodHandles.Lookup#unreflect} sees the override flag and
 * resolves through the JDK's trusted {@code IMPL_LOOKUP}, which skips access
 * checks entirely and yields a plain direct method handle.</li>
 * </ol>
 * The handles are {@code static final} and invoked via {@code invokeExact}
 * with erased ({@code Object}) signatures, so the JIT treats them as
 * constants and inlines the calls: the slow path costs the same as a direct
 * call did before. The technique works on JDK 9 through 27.
 * <p>
 * JDK 27 needs no change here, but it exposed a latent bug in
 * {@link Unsafe#makeAccessible}: compact object headers (JEP 450, enabled by
 * default in JDK 27) shrink the object header to 8 bytes, so the
 * {@code override} field that step 3 writes moved from offset 12/16 to 8.
 * {@code Unsafe} now derives that offset by measuring the first-field boundary
 * instead of hard-coding it, which tracks compact, compressed, uncompressed
 * and 32-bit layouts alike. The remaining long-term liability is
 * {@code sun.misc.Unsafe} itself being removed from a future JDK; the durable
 * answer then is a self-contained bignum that needs no JDK-internal access at
 * all. The {@code mrjar-smoke} 27-ea CI job guards the current behaviour.
 */
final class FdBig {
    private static final MethodHandle ADD_AND_CMP;           // (Object, Object, Object) int
    private static final MethodHandle CMP;                   // (Object, Object) int
    private static final MethodHandle GET_NORMALIZATION_BIAS; // (Object) int
    private static final MethodHandle LEFT_SHIFT;            // (Object, int) Object
    private static final MethodHandle MULT_BY_10;            // (Object) Object
    private static final MethodHandle QUO_REM_ITERATION;     // (Object, Object) int
    private static final MethodHandle VALUE_OF_MUL_POW52;    // (long, int, int) Object
    private static final MethodHandle VALUE_OF_POW52;        // (int, int) Object

    private final Object value;

    private FdBig(Object value) {
        this.value = value;
    }

    static FdBig valueOfMulPow52(long value, int p5, int p2) {
        try {
            return new FdBig((Object) VALUE_OF_MUL_POW52.invokeExact(value, p5, p2));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static FdBig valueOfPow52(int p5, int p2) {
        try {
            return new FdBig((Object) VALUE_OF_POW52.invokeExact(p5, p2));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    int addAndCmp(FdBig x, FdBig y) {
        try {
            return (int) ADD_AND_CMP.invokeExact(value, x.value, y.value);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    int cmp(FdBig other) {
        try {
            return (int) CMP.invokeExact(value, other.value);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    int getNormalizationBias() {
        try {
            return (int) GET_NORMALIZATION_BIAS.invokeExact(value);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    FdBig leftShift(int shift) {
        try {
            return new FdBig((Object) LEFT_SHIFT.invokeExact(value, shift));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    FdBig multBy10() {
        try {
            return new FdBig((Object) MULT_BY_10.invokeExact(value));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    int quoRemIteration(FdBig s) {
        try {
            return (int) QUO_REM_ITERATION.invokeExact(value, s.value);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Resolves {@code owner.name(params)} into a direct method handle whose
     * type is {@code erased} (the bignum type replaced by {@code Object}),
     * without the caller needing access to {@code owner}.
     */
    private static MethodHandle handle(
            MethodHandles.Lookup lookup,
            Class<?> owner,
            String name,
            MethodType erased,
            Class<?>... params
    ) throws ReflectiveOperationException {
        Method m = owner.getDeclaredMethod(name, params);
        // Sets AccessibleObject.override without setAccessible()'s module
        // check. Lookup.unreflect() then treats the member as pre-authorised
        // and resolves it via IMPL_LOOKUP, i.e. with no access check at all.
        Unsafe.makeAccessible(m);
        return lookup.unreflect(m).asType(erased);
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        return new IllegalStateException(t);
    }

    static {
        try {
            final Class<?> big = Class.forName("jdk.internal.math.FDBigInteger");
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            final Class<?> obj = Object.class;
            final Class<?> i32 = int.class;
            VALUE_OF_MUL_POW52 = handle(lookup, big, "valueOfMulPow52", MethodType.methodType(obj, long.class, i32, i32), long.class, i32, i32);
            VALUE_OF_POW52 = handle(lookup, big, "valueOfPow52", MethodType.methodType(obj, i32, i32), i32, i32);
            ADD_AND_CMP = handle(lookup, big, "addAndCmp", MethodType.methodType(i32, obj, obj, obj), big, big);
            CMP = handle(lookup, big, "cmp", MethodType.methodType(i32, obj, obj), big);
            GET_NORMALIZATION_BIAS = handle(lookup, big, "getNormalizationBias", MethodType.methodType(i32, obj));
            LEFT_SHIFT = handle(lookup, big, "leftShift", MethodType.methodType(obj, obj, i32), i32);
            MULT_BY_10 = handle(lookup, big, "multBy10", MethodType.methodType(obj, obj));
            QUO_REM_ITERATION = handle(lookup, big, "quoRemIteration", MethodType.methodType(i32, obj, obj), big);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
