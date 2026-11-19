/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Per-row cost of writing an exact-type row through {@link QwpSchemaBinding}
 * versus the legacy {@link QwpTableBuffer#getOrCreateColumn} path. Both write
 * the same 10 columns (5 LONG, 5 DOUBLE) in the same order, so the difference
 * is the binding's per-value column resolution. Run with {@link #main}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class QwpSchemaBindingBenchmark {
    private static final int COLUMN_COUNT = 10;
    private static final int ROWS_PER_BATCH = 1_000;
    private final String[] names = new String[COLUMN_COUNT];
    private QwpSchemaBinding binding;
    private QwpTableBuffer legacyBuffer;
    @Param({"2", "14"})
    private int nameLength;
    private QwpTableBuffer schemaBuffer;
    private long value;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpSchemaBindingBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void legacyRow() {
        long v = value++;
        for (int i = 0; i < COLUMN_COUNT; i += 2) {
            legacyBuffer.getOrCreateColumn(names[i], QwpConstants.TYPE_LONG, true).addLong(v);
            legacyBuffer.getOrCreateColumn(names[i + 1], QwpConstants.TYPE_DOUBLE, true).addDouble(v);
        }
        legacyBuffer.nextRow();
        if (legacyBuffer.getRowCount() == ROWS_PER_BATCH) {
            legacyBuffer.reset();
        }
    }

    @Benchmark
    public void schemaRow() {
        long v = value++;
        for (int i = 0; i < COLUMN_COUNT; i += 2) {
            binding.longColumn(names[i], v);
            binding.doubleColumn(names[i + 1], v);
        }
        schemaBuffer.nextRow();
        if (schemaBuffer.getRowCount() == ROWS_PER_BATCH) {
            schemaBuffer.reset();
        }
    }

    @Setup(Level.Trial)
    public void setUp() {
        byte[][] columns = new byte[COLUMN_COUNT][];
        for (int i = 0; i < COLUMN_COUNT; i++) {
            StringBuilder name = new StringBuilder().append((char) ('a' + i));
            while (name.length() < nameLength) {
                name.append('x');
            }
            names[i] = name.toString();
            columns[i] = QwpSchemaTestFixtures.column(names[i], i % 2 == 0 ? ColumnType.LONG : ColumnType.DOUBLE);
        }
        legacyBuffer = new QwpTableBuffer("t");
        schemaBuffer = new QwpTableBuffer("t");
        binding = QwpSchemaTestFixtures.binding(schemaBuffer, columns);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        legacyBuffer.close();
        schemaBuffer.close();
    }
}
