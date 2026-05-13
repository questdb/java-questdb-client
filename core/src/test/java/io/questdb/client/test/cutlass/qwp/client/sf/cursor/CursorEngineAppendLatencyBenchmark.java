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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;

import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Standalone latency benchmark for the cursor engine's user-thread append
 * path. Measures the wall time of one
 * {@link CursorSendEngine#appendBlocking(long, int)} call from the producer's
 * point of view: write into mmap, advance cursors, return. No network, no
 * I/O thread interaction beyond the segment manager provisioning spares
 * in the background.
 * <p>
 * This is the floor: the latency a fully-wired cursor-engine
 * {@code QwpWebSocketSender} would inherit on its hot path. Comparing this
 * number against the legacy bench's p50 (~38 µs in the SF mode of
 * {@code QwpIngressLatencyBenchmark}) tells us how much of the latency
 * currently spent in {@code processingLock.wait/notify} can actually
 * disappear once the cross-thread handoff goes away.
 * <p>
 * Run via Maven exec:
 * <pre>
 *   mvn -pl core test-compile
 *   mvn -pl core exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.questdb.client.test.cutlass.qwp.client.sf.cursor.CursorEngineAppendLatencyBenchmark \
 *     -Dexec.args="--payload-bytes=64 --measure=1000000"
 * </pre>
 */
public final class CursorEngineAppendLatencyBenchmark {

    private static final long DEFAULT_MAX_BYTES_PER_SEGMENT = 64L * 1024 * 1024;
    private static final int DEFAULT_MEASURE = 1_000_000;
    private static final int DEFAULT_PAYLOAD_BYTES = 64;
    private static final int DEFAULT_WARMUP = 50_000;

    public static void main(String[] args) {
        int payloadBytes = DEFAULT_PAYLOAD_BYTES;
        int warmup = DEFAULT_WARMUP;
        int measure = DEFAULT_MEASURE;
        long maxBytesPerSegment = DEFAULT_MAX_BYTES_PER_SEGMENT;
        String dirOverride = null;

        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            } else if (arg.startsWith("--payload-bytes=")) {
                payloadBytes = Integer.parseInt(arg.substring("--payload-bytes=".length()));
            } else if (arg.startsWith("--warmup=")) {
                warmup = Integer.parseInt(arg.substring("--warmup=".length()));
            } else if (arg.startsWith("--measure=")) {
                measure = Integer.parseInt(arg.substring("--measure=".length()));
            } else if (arg.startsWith("--max-bytes-per-segment=")) {
                maxBytesPerSegment = parseSize(arg.substring("--max-bytes-per-segment=".length()));
            } else if (arg.startsWith("--dir=")) {
                dirOverride = arg.substring("--dir=".length());
            } else {
                System.err.println("Unknown option: " + arg);
                printUsage();
                System.exit(1);
            }
        }

        if (payloadBytes <= 0 || measure <= 0 || warmup < 0) {
            System.err.println("payload/measure/warmup out of range");
            System.exit(1);
        }

        String dir = dirOverride != null
                ? dirOverride
                : Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-bench-" + System.nanoTime()).toString();

        System.out.println("CursorSendEngine.appendBlocking latency benchmark");
        System.out.println("==================================================");
        System.out.println("Payload bytes:          " + format(payloadBytes));
        System.out.println("Warmup iterations:      " + format(warmup));
        System.out.println("Measure iterations:     " + format(measure));
        System.out.println("Max bytes per segment:  " + format(maxBytesPerSegment));
        System.out.println("SF directory:           " + dir);
        System.out.println();

        long buf = Unsafe.malloc(payloadBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payloadBytes; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i * 31 + 17));
            }
            try (CursorSendEngine engine = new CursorSendEngine(dir, maxBytesPerSegment)) {
                for (int i = 0; i < warmup; i++) {
                    engine.appendBlocking(buf, payloadBytes);
                }

                long[] samples = new long[measure];
                long startNs = System.nanoTime();
                for (int i = 0; i < measure; i++) {
                    long t0 = System.nanoTime();
                    engine.appendBlocking(buf, payloadBytes);
                    samples[i] = System.nanoTime() - t0;
                }
                long elapsedNs = System.nanoTime() - startNs;

                report(samples, elapsedNs, payloadBytes);
            }
        } finally {
            Unsafe.free(buf, payloadBytes, MemoryTag.NATIVE_DEFAULT);
            rmTree(dir);
        }
    }

    private static String format(long n) {
        return String.format("%,d", n);
    }

    private static String formatDouble(double d) {
        if (d >= 1000) return String.format("%,.0f", d);
        if (d >= 10) return String.format("%,.1f", d);
        return String.format("%,.2f", d);
    }

    private static long parseSize(String s) {
        s = s.trim().toUpperCase();
        long mult = 1;
        if (s.endsWith("K") || s.endsWith("KB")) {
            mult = 1024L;
            s = s.substring(0, s.length() - (s.endsWith("KB") ? 2 : 1));
        } else if (s.endsWith("M") || s.endsWith("MB")) {
            mult = 1024L * 1024;
            s = s.substring(0, s.length() - (s.endsWith("MB") ? 2 : 1));
        } else if (s.endsWith("G") || s.endsWith("GB")) {
            mult = 1024L * 1024 * 1024;
            s = s.substring(0, s.length() - (s.endsWith("GB") ? 2 : 1));
        }
        return Long.parseLong(s.trim()) * mult;
    }

    private static void printUsage() {
        System.out.println("Usage: CursorEngineAppendLatencyBenchmark [options]");
        System.out.println("  --payload-bytes=<n>          Frame payload size (default: 64)");
        System.out.println("  --warmup=<n>                 Warmup append count (default: 50,000)");
        System.out.println("  --measure=<n>                Measured append count (default: 1,000,000)");
        System.out.println("  --max-bytes-per-segment=<sz> Segment rotation threshold (default: 64M)");
        System.out.println("  --dir=<path>                 Use this dir instead of an autogenerated tmp dir");
    }

    private static void report(long[] samples, long elapsedNs, int payloadBytes) {
        Arrays.sort(samples);
        int n = samples.length;
        long min = samples[0];
        long p50 = samples[(int) (n * 0.50)];
        long p90 = samples[(int) (n * 0.90)];
        long p99 = samples[(int) (n * 0.99)];
        long p999 = samples[Math.min(n - 1, (int) (n * 0.999))];
        long max = samples[n - 1];

        long sum = 0;
        for (long s : samples) sum += s;
        double meanNs = (double) sum / n;

        double seconds = elapsedNs / 1e9;
        double appendsPerSec = n / seconds;
        double mbPerSec = appendsPerSec * (payloadBytes + 8) / (1024.0 * 1024.0);

        System.out.println("Latency (ns):");
        System.out.println("  min:    " + format(min));
        System.out.println("  p50:    " + format(p50));
        System.out.println("  p90:    " + format(p90));
        System.out.println("  p99:    " + format(p99));
        System.out.println("  p99.9:  " + format(p999));
        System.out.println("  max:    " + format(max));
        System.out.println("  mean:   " + format((long) meanNs));
        System.out.println();
        System.out.println("Throughput:");
        System.out.println("  appends/sec:           " + formatDouble(appendsPerSec));
        System.out.println("  MB/sec (payload+env):  " + formatDouble(mbPerSec));
    }

    private static void rmTree(String dir) {
        if (dir == null || !Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }
}
