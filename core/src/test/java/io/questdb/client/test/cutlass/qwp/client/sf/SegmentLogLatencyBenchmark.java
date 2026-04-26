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

package io.questdb.client.test.cutlass.qwp.client.sf;

import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;

import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Latency benchmark for {@link SegmentLog#append}, the per-frame entry point
 * the QWiP store-and-forward layer uses to persist outgoing batches before
 * they leave the wire.
 * <p>
 * Measures the wall-clock latency of a single {@code append} call from the
 * caller's perspective: CRC32C over the payload, frame-envelope construction,
 * two pwrite syscalls (header + payload), bookkeeping, and an optional
 * {@code fsync} when {@code --fsync=each}. Reports min / p50 / p90 / p99 /
 * p99.9 / max in nanoseconds, plus throughput in frames/sec and MB/sec.
 * <p>
 * Run via Maven exec:
 * <pre>
 *   mvn -pl core test-compile
 *   mvn -pl core exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.questdb.client.test.cutlass.qwp.client.sf.SegmentLogLatencyBenchmark \
 *     -Dexec.args="--payload-bytes=512 --measure=100000 --fsync=off"
 * </pre>
 * Or directly via your IDE — it's a plain {@code main} method, no JMH.
 * <p>
 * Defaults are tuned for a quick local sanity check (~1 second runtime). For
 * publication-quality numbers run with {@code --warmup=200000 --measure=1000000}
 * on an idle machine; the SF code path is short enough that JIT effects fade
 * within a few thousand iterations.
 */
public final class SegmentLogLatencyBenchmark {

    private static final long DEFAULT_MAX_BYTES_PER_SEGMENT = 64L * 1024 * 1024; // 64 MiB
    private static final long DEFAULT_MAX_TOTAL_BYTES = Long.MAX_VALUE;
    private static final int DEFAULT_MEASURE = 100_000;
    private static final int DEFAULT_PAYLOAD_BYTES = 512;
    private static final int DEFAULT_WARMUP = 10_000;

    public static void main(String[] args) throws Exception {
        int payloadBytes = DEFAULT_PAYLOAD_BYTES;
        int warmup = DEFAULT_WARMUP;
        int measure = DEFAULT_MEASURE;
        long maxBytesPerSegment = DEFAULT_MAX_BYTES_PER_SEGMENT;
        long maxTotalBytes = DEFAULT_MAX_TOTAL_BYTES;
        FsyncMode fsyncMode = FsyncMode.OFF;
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
            } else if (arg.startsWith("--max-total-bytes=")) {
                maxTotalBytes = parseSize(arg.substring("--max-total-bytes=".length()));
            } else if (arg.startsWith("--fsync=")) {
                fsyncMode = FsyncMode.parse(arg.substring("--fsync=".length()));
            } else if (arg.startsWith("--dir=")) {
                dirOverride = arg.substring("--dir=".length());
            } else {
                System.err.println("Unknown option: " + arg);
                printUsage();
                System.exit(1);
            }
        }

        if (payloadBytes <= 0) {
            System.err.println("--payload-bytes must be > 0");
            System.exit(1);
        }
        if (measure <= 0) {
            System.err.println("--measure must be > 0");
            System.exit(1);
        }
        if (warmup < 0) {
            System.err.println("--warmup must be >= 0");
            System.exit(1);
        }
        long oneFrameTotal = 8L /* FRAME_HEADER_SIZE */ + payloadBytes;
        if (24L /* HEADER_SIZE */ + oneFrameTotal > maxBytesPerSegment) {
            System.err.println("--max-bytes-per-segment too small for a single frame "
                    + "(need >= " + (24 + oneFrameTotal) + " bytes for the configured payload)");
            System.exit(1);
        }

        String dir = dirOverride != null
                ? dirOverride
                : Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-sf-bench-" + System.nanoTime()).toString();
        boolean ownDir = dirOverride == null;
        if (ownDir) {
            int rc = Files.mkdir(dir, 0755);
            if (rc != 0) {
                System.err.println("Failed to create benchmark dir: " + dir + " (rc=" + rc + ")");
                System.exit(1);
            }
        }

        System.out.println("SegmentLog.append latency benchmark");
        System.out.println("====================================");
        System.out.println("Payload bytes:          " + format(payloadBytes));
        System.out.println("Warmup iterations:      " + format(warmup));
        System.out.println("Measure iterations:     " + format(measure));
        System.out.println("Max bytes per segment:  " + format(maxBytesPerSegment));
        System.out.println("Max total bytes:        "
                + (maxTotalBytes == Long.MAX_VALUE ? "unlimited" : format(maxTotalBytes)));
        System.out.println("Fsync mode:             " + fsyncMode);
        System.out.println("SF directory:           " + dir);
        System.out.println();

        long buf = Unsafe.malloc(payloadBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            // Deterministic-but-non-zero payload so the CRC isn't trivially short-circuited
            // by an all-zero stream and so any branch on payload content is exercised.
            for (int i = 0; i < payloadBytes; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i * 31 + 17));
            }

            try (SegmentLog log = SegmentLog.open(dir, maxBytesPerSegment, maxTotalBytes,
                    fsyncMode == FsyncMode.EACH)) {

                // Warmup — discard timing, let the JIT settle and the first segment fill.
                for (int i = 0; i < warmup; i++) {
                    log.append(buf, payloadBytes);
                }

                long[] samples = new long[measure];
                long startNs = System.nanoTime();
                for (int i = 0; i < measure; i++) {
                    long t0 = System.nanoTime();
                    log.append(buf, payloadBytes);
                    samples[i] = System.nanoTime() - t0;
                }
                long elapsedNs = System.nanoTime() - startNs;

                // Optional final fsync when the per-call mode was OFF, so disk
                // committed bytes are stable before we report.
                if (fsyncMode == FsyncMode.FINAL_ONLY) {
                    log.fsync();
                }

                report(samples, elapsedNs, payloadBytes, log);
            }
        } finally {
            Unsafe.free(buf, payloadBytes, MemoryTag.NATIVE_DEFAULT);
            if (ownDir) {
                rmTree(dir);
            }
        }
    }

    private static String format(long n) {
        return String.format("%,d", n);
    }

    private static String formatDouble(double d) {
        if (d >= 1000) {
            return String.format("%,.0f", d);
        }
        if (d >= 10) {
            return String.format("%,.1f", d);
        }
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
        System.out.println("Usage: SegmentLogLatencyBenchmark [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --payload-bytes=<n>           Frame payload size in bytes (default: 512)");
        System.out.println("  --warmup=<n>                  Warmup append count (default: 10,000)");
        System.out.println("  --measure=<n>                 Measured append count (default: 100,000)");
        System.out.println("  --max-bytes-per-segment=<sz>  Segment rotation threshold (default: 64M)");
        System.out.println("                                Suffixes: K, M, G");
        System.out.println("  --max-total-bytes=<sz>        Total disk cap (default: unlimited)");
        System.out.println("  --fsync=off|each|final        Per-append fsync mode (default: off)");
        System.out.println("                                  off:   no fsync, fastest");
        System.out.println("                                  each:  fsync after every append (durability max)");
        System.out.println("                                  final: fsync once after the run (closer to flush())");
        System.out.println("  --dir=<path>                  Use this dir instead of an autogenerated tmp dir");
        System.out.println("  -h, --help                    Show this help");
    }

    private static void report(long[] samples, long elapsedNs, int payloadBytes, SegmentLog log) {
        Arrays.sort(samples);
        int n = samples.length;
        long min = samples[0];
        long p50 = samples[(int) (n * 0.50)];
        long p90 = samples[(int) (n * 0.90)];
        long p99 = samples[(int) (n * 0.99)];
        long p999 = samples[Math.min(n - 1, (int) (n * 0.999))];
        long max = samples[n - 1];

        long sum = 0;
        for (long s : samples) {
            sum += s;
        }
        double meanNs = (double) sum / n;

        double seconds = elapsedNs / 1e9;
        double framesPerSec = n / seconds;
        // payload + 8-byte SF envelope; the segment header is amortised across
        // every frame in a segment and small enough to ignore here.
        double mbPerSec = framesPerSec * (payloadBytes + 8) / (1024.0 * 1024.0);

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
        System.out.println("  frames/sec:           " + formatDouble(framesPerSec));
        System.out.println("  MB/sec (payload+env): " + formatDouble(mbPerSec));
        System.out.println();
        System.out.println("Final SegmentLog state:");
        System.out.println("  segments:    " + log.segmentCount());
        System.out.println("  bytesOnDisk: " + format(log.bytesOnDisk()));
        System.out.println("  nextSeq:     " + format(log.nextSeq()));
    }

    private static void rmTree(String dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find != 0) {
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

    private enum FsyncMode {
        OFF, EACH, FINAL_ONLY;

        static FsyncMode parse(String s) {
            switch (s.toLowerCase()) {
                case "off":
                    return OFF;
                case "each":
                    return EACH;
                case "final":
                    return FINAL_ONLY;
                default:
                    throw new IllegalArgumentException("--fsync must be off|each|final, got: " + s);
            }
        }

        @Override
        public String toString() {
            switch (this) {
                case OFF:
                    return "off";
                case EACH:
                    return "each";
                case FINAL_ONLY:
                    return "final";
                default:
                    return name();
            }
        }
    }
}
