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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.Sender;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * JMH latency benchmark for QWP ingress -- the user-facing counterpart to
 * {@code QwpEgressLatencyBenchmark} in the QuestDB OSS repo. Measures the
 * end-to-end wall time of a single row {@code .at(...) + flush()} against a
 * locally running QuestDB, excluding connection setup (the {@link Sender} is
 * opened once per trial and reused across every benchmarked invocation).
 * <p>
 * Default mode (SF on) measures user-handover latency: {@code flush()} blocks
 * only until the row is durable on the local SF segment (CRC + two pwrites);
 * the wire send and server ACK are processed asynchronously by the I/O thread
 * and are NOT included in the measurement window. This is the number to quote
 * when the user app's contract is "the row is recoverable if I crash now",
 * not "the server has confirmed the row".
 * <p>
 * With {@code -Dsf=false}, store-and-forward is disabled. {@code flush()} then
 * blocks for the full row encode &rarr; WS send &rarr; server ACK round-trip.
 * This is the symmetric counterpart of the egress benchmark's {@code SELECT 1}
 * round-trip -- useful when comparing the ingress and egress wire paths head
 * to head, but it is NOT what a real SF-enabled user app experiences.
 * <p>
 * Runs two modes on each invocation:
 * <ul>
 *   <li>{@code SampleTime} -- reports p50/p90/p99/p99.9 percentiles per
 *       iteration. This is the main signal; ingest UX is gated by the tail,
 *       not the mean.</li>
 *   <li>{@code AverageTime} -- arithmetic mean. Useful when comparing two
 *       builds: a smaller mean with an unchanged tail is usually the honest
 *       win (no outlier distortion).</li>
 * </ul>
 * <p>
 * Prerequisites:
 * <ul>
 *   <li>A QuestDB server listening on 9000 (HTTP/WS) and 8812 (PG wire).</li>
 * </ul>
 * <p>
 * Tune via system properties:
 * <ul>
 *   <li>{@code -Dskip.populate=true} to re-use an existing
 *       {@code latency_bench_ingress} table instead of dropping and recreating
 *       it in {@code @Setup}.</li>
 *   <li>{@code -Dsf=true} to enable store-and-forward. {@code -Dsf.dir=<path>}
 *       overrides the SF directory (default: a fresh tmp dir per trial).</li>
 *   <li>{@code -Dfsync.on.flush=true} to also fsync the SF segment on every
 *       flush ({@code sf_durability=flush}; only meaningful with
 *       {@code -Dsf=true}). Note: cursor engine does not yet implement
 *       {@code sf_durability=flush}, so this currently fails fast at build().</li>
 * </ul>
 * <p>
 * Run via Maven exec:
 * <pre>
 *   mvn -pl core test-compile
 *   mvn -pl core exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.questdb.client.test.cutlass.qwp.client.QwpIngressLatencyBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode({Mode.SampleTime, Mode.AverageTime})
// -Xlog:gc* prints every GC pause + reason to the fork's stdout. With JMH's
// default forking, those lines are streamed live so a sub-millisecond pause
// landing inside a measurement window is easy to correlate with the p99.99
// outlier that prompted us to look. The unified-logging flag is JDK 9+.
@Fork(jvmArgsAppend = {"-Xlog:gc*=info"})
public class QwpIngressLatencyBenchmark {

    static {
        // The WS / SF code paths emit a handful of DEBUG lines per flush.
        // At 7-8k flushes/sec that's enough I/O to inflate measured latency
        // by ~70 us (verified: same harness, root=DEBUG vs root=WARN, p50 went
        // 200 us -> 38 us). Force WARN before any other class loads so the
        // first log line we'd otherwise emit is also gone. If SLF4J is bound
        // to something other than logback, leave the level alone -- the
        // benchmark still runs, just with whatever the binding's default is.
        org.slf4j.ILoggerFactory factory = org.slf4j.LoggerFactory.getILoggerFactory();
        if (factory instanceof ch.qos.logback.classic.LoggerContext) {
            ((ch.qos.logback.classic.LoggerContext) factory)
                    .getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
                    .setLevel(ch.qos.logback.classic.Level.WARN);
        }
    }

    private static final boolean FSYNC_ON_FLUSH = Boolean.parseBoolean(System.getProperty("fsync.on.flush", "false"));
    private static final String HOST = "localhost";
    private static final int HTTP_PORT = 9000;
    private static final int PG_PORT = 8812;
    private static final boolean SF_ENABLED = Boolean.parseBoolean(System.getProperty("sf", "true"));
    private static final String SF_DIR_OVERRIDE = System.getProperty("sf.dir");
    private static final boolean SKIP_POPULATE = Boolean.parseBoolean(System.getProperty("skip.populate", "false"));
    private static final String TABLE = "latency_bench_ingress";

    private long rowCounter;
    private Sender sender;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QwpIngressLatencyBenchmark.class.getSimpleName())
                // Five warmup iterations at two seconds each so the JIT gets
                // past C2 tiering and the WAL writer / WS encoder are hot
                // before we record samples.
                .warmupIterations(5)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(10)
                .measurementTime(TimeValue.seconds(2))
                .threads(1)
                .forks(2)
                // GCProfiler reports allocation rate + young/old churn per
                // iteration as extra result rows ("·gc.alloc.rate", etc.).
                // Profilers can't be wired via annotation, so they go here.
                .addProfiler(GCProfiler.class)
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void ingestSingleRow() {
        // Monotonic id and ts so rows are unique and the WAL writer is
        // exercised in append-mostly mode (no out-of-order rewrites).
        long n = ++rowCounter;
        sender.table(TABLE)
                .longColumn("id", n)
                .at(n, ChronoUnit.MICROS);
        sender.flush();
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        if (!SKIP_POPULATE) {
            recreateTable();
        } else {
            System.out.println("skip.populate=true, re-using existing " + TABLE);
        }

        String cfg = "ws::addr=" + HOST + ":" + HTTP_PORT + ";";
        if (SF_ENABLED) {
            String sfDir = SF_DIR_OVERRIDE != null
                    ? SF_DIR_OVERRIDE
                    : Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-sf-ingress-bench-" + System.nanoTime()).toString();
            cfg += "sf_dir=" + sfDir + ";";
            if (FSYNC_ON_FLUSH) {
                cfg += "sf_durability=flush;";
            }
            System.out.println("SF enabled, dir=" + sfDir + ", sf_durability=" +
                    (FSYNC_ON_FLUSH ? "flush" : "memory"));
        }
        sender = Sender.fromConfig(cfg);

        // Prime: first flush registers the table schema with the server and
        // warms WS encoder / async pipeline state. Keeps those one-time
        // costs out of the measurement window.
        rowCounter = 0;
        sender.table(TABLE)
                .longColumn("id", 0L)
                .at(0L, ChronoUnit.MICROS);
        sender.flush();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (sender != null) {
            sender.close();
        }
    }

    private static Connection createPgConnection() throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "admin");
        p.setProperty("password", "quest");
        p.setProperty("sslmode", "disable");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/qdb", HOST, PG_PORT), p);
    }

    private static void recreateTable() throws Exception {
        try (Connection c = createPgConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE);
            st.execute("CREATE TABLE " + TABLE + " (id LONG, ts TIMESTAMP) "
                    + "TIMESTAMP(ts) PARTITION BY DAY WAL");
        }
    }
}
