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

package io.questdb.client.test.std;

import io.questdb.client.std.Numbers;
import io.questdb.client.std.str.StringSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for {@link Numbers#append(io.questdb.client.std.str.CharSink, double)},
 * the double-to-text routine behind every double column the client sends.
 * <p>
 * Its bignum slow path goes through the {@code FdBig} bridge over the JDK's
 * internal {@code FDBigInteger}. On Java 9+ that bridge binds the bignum
 * methods via {@code static final} method handles resolved once at class-init
 * (JDK 26 made the class package-private, so it can no longer be named in
 * source). This benchmark exists to show the handle-based bridge costs the
 * same as the direct calls it replaced: run it once against a client jar
 * built from the previous {@code main} and once against the current tree,
 * same JDK, and compare {@code shape=extreme} (100% slow path).
 * <p>
 * Input shapes, each a fixed pool of 1024 values cycled per invocation. The
 * slow-path share was measured by running the pool against a pre-fix jar on
 * JDK 26, where the bignum path throws and the fast path does not:
 * <ul>
 * <li>{@code simple} -- short literals like {@code 1.0}, {@code 123456.789};
 * 0/1024 slow path.</li>
 * <li>{@code unit} -- {@code Random.nextDouble()}: full 53-bit mantissas in
 * [0,1), the typical measurement value; 2/1024 slow path.</li>
 * <li>{@code scaled} -- {@code nextDouble() * 1e6}: full mantissas around
 * 1e5..1e6, e.g. prices, counters; 0/1024 slow path.</li>
 * <li>{@code wide} -- {@code longBitsToDouble(random)}: uniform over the whole
 * finite double range, mostly huge/tiny exponents; 972/1024 slow path.</li>
 * <li>{@code extreme} -- 1e300, 4.9e-324, {@code Double.MAX_VALUE} and the
 * like; 1024/1024 slow path.</li>
 * </ul>
 * {@code jdkToString} is the {@code StringBuilder.append(double)} reference
 * for the same inputs.
 * <p>
 * Run from the packaged tests jar (the JMH annotation processor writes the
 * benchmark list into it):
 * <pre>
 * java -cp questdb-client-tests.jar:questdb-client.jar:jmh-core.jar:jopt-simple.jar:commons-math3.jar \
 *      org.openjdk.jmh.Main DoubleFormatBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class DoubleFormatBenchmark {
    private static final int POOL = 1024;
    private static final int MASK = POOL - 1;

    @Param({"simple", "unit", "scaled", "wide", "extreme"})
    public String shape;

    private int index;
    private final StringBuilder builder = new StringBuilder(32);
    private final StringSink sink = new StringSink(32);
    private double[] values;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(DoubleFormatBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public int jdkToString() {
        builder.setLength(0);
        builder.append(next());
        return builder.length();
    }

    @Benchmark
    public int numbersAppend() {
        sink.clear();
        Numbers.append(sink, next());
        return sink.length();
    }

    @Setup
    public void setup() {
        Random rnd = new Random(0x51ED1D5L);
        values = new double[POOL];
        if ("simple".equals(shape)) {
            double[] seed = {1.0, 0.1, 42.5, 123456.789, 3.25, 1000.0, 0.5, 99.99};
            for (int i = 0; i < POOL; i++) {
                values[i] = seed[i % seed.length];
            }
        } else if ("unit".equals(shape)) {
            for (int i = 0; i < POOL; i++) {
                values[i] = rnd.nextDouble();
            }
        } else if ("scaled".equals(shape)) {
            for (int i = 0; i < POOL; i++) {
                values[i] = rnd.nextDouble() * 1e6;
            }
        } else if ("wide".equals(shape)) {
            for (int i = 0; i < POOL; i++) {
                double d;
                do {
                    d = Double.longBitsToDouble(rnd.nextLong());
                } while (Double.isNaN(d) || Double.isInfinite(d));
                values[i] = d;
            }
        } else if ("extreme".equals(shape)) {
            double[] seed = {1e300, 4.9e-324, Double.MAX_VALUE, Double.MIN_NORMAL, 1e-200, 3.141592653589793e100, 2.2250738585072014E-308, 1e200};
            for (int i = 0; i < POOL; i++) {
                values[i] = seed[i % seed.length];
            }
        } else {
            throw new IllegalArgumentException("unknown shape: " + shape);
        }
        index = 0;
    }

    private double next() {
        return values[index++ & MASK];
    }
}
