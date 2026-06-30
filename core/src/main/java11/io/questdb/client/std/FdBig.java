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

import jdk.internal.math.FDBigInteger;

/**
 * Thin bridge over the JDK's internal {@code FDBigInteger}, used only by the
 * {@link Numbers} double-to-string slow path. The bignum class lives in
 * {@code jdk.internal.math} on Java 9+; the parallel copy of this file under
 * {@code src/main/java8} targets {@code sun.misc.FDBigInteger}. Keeping the
 * type behind this wrapper lets {@code Numbers} carry a single, JDK-agnostic
 * copy of the algorithm.
 */
final class FdBig {
    static {
        // Mirror the compile-time --add-exports: grant this module runtime
        // access to jdk.internal.math before any FDBigInteger reference is
        // resolved. No-op on Java 8 (sun.misc is open).
        Compat.exportFdBigInteger();
    }

    private final FDBigInteger value;

    private FdBig(FDBigInteger value) {
        this.value = value;
    }

    static FdBig valueOfMulPow52(long value, int p5, int p2) {
        return new FdBig(FDBigInteger.valueOfMulPow52(value, p5, p2));
    }

    static FdBig valueOfPow52(int p5, int p2) {
        return new FdBig(FDBigInteger.valueOfPow52(p5, p2));
    }

    int addAndCmp(FdBig x, FdBig y) {
        return value.addAndCmp(x.value, y.value);
    }

    int cmp(FdBig other) {
        return value.cmp(other.value);
    }

    int getNormalizationBias() {
        return value.getNormalizationBias();
    }

    FdBig leftShift(int shift) {
        return new FdBig(value.leftShift(shift));
    }

    FdBig multBy10() {
        return new FdBig(value.multBy10());
    }

    int quoRemIteration(FdBig s) {
        return value.quoRemIteration(s.value);
    }
}
