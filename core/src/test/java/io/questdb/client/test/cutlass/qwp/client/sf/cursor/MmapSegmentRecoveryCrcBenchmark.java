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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;

import java.nio.file.Path;

/**
 * Standalone large-corpus benchmark for the mmap-safe recovery CRC path.
 * The generated segment contains one large frame, so the measured
 * {@link MmapSegment#openExisting(String)} time is dominated by recovery CRC
 * rather than frame-header traversal or syscalls.
 * <p>
 * Run via Maven exec:
 * <pre>
 *   mvn -pl core test-compile
 *   mvn -pl core exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=io.questdb.client.test.cutlass.qwp.client.sf.cursor.MmapSegmentRecoveryCrcBenchmark \
 *     -Dexec.args="--corpus-bytes=512M --warmup=2 --iterations=5"
 * </pre>
 */
public final class MmapSegmentRecoveryCrcBenchmark {

    private static final int DEFAULT_CORPUS_BYTES = 256 * 1024 * 1024;
    private static final int DEFAULT_ITERATIONS = 5;
    private static final int DEFAULT_WARMUP = 2;

    public static void main(String[] args) throws Exception {
        int corpusBytes = DEFAULT_CORPUS_BYTES;
        int frameBytes = -1;
        int iterations = DEFAULT_ITERATIONS;
        int warmup = DEFAULT_WARMUP;
        for (String arg : args) {
            if (arg.startsWith("--corpus-bytes=")) {
                long parsed = parseSize(arg.substring("--corpus-bytes=".length()));
                if (parsed <= 0 || parsed > Integer.MAX_VALUE - MmapSegment.FRAME_HEADER_SIZE) {
                    throw new IllegalArgumentException("corpus size out of range: " + parsed);
                }
                corpusBytes = (int) parsed;
            } else if (arg.startsWith("--frame-bytes=")) {
                frameBytes = Integer.parseInt(arg.substring("--frame-bytes=".length()));
            } else if (arg.startsWith("--iterations=")) {
                iterations = Integer.parseInt(arg.substring("--iterations=".length()));
            } else if (arg.startsWith("--warmup=")) {
                warmup = Integer.parseInt(arg.substring("--warmup=".length()));
            } else {
                throw new IllegalArgumentException("unknown option: " + arg);
            }
        }
        if (iterations <= 0 || warmup < 0 || frameBytes == 0 || frameBytes < -1) {
            throw new IllegalArgumentException("iterations/frame-bytes/warmup out of range");
        }

        int payloadBytes = frameBytes > 0 ? frameBytes : corpusBytes;
        long frameCount = frameBytes > 0
                ? Math.max(1L, corpusBytes / (MmapSegment.FRAME_HEADER_SIZE + (long) payloadBytes))
                : 1L;
        long scannedBytes = frameCount * (MmapSegment.FRAME_HEADER_SIZE + (long) payloadBytes);
        Path dir = java.nio.file.Files.createTempDirectory("qdb-mmap-recovery-crc-");
        Path segmentPath = dir.resolve("0.sfa");
        long payload = Unsafe.malloc(payloadBytes, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(payload, payloadBytes, (byte) 0xA5);
            long segmentBytes = MmapSegment.HEADER_SIZE + scannedBytes;
            try (MmapSegment segment = MmapSegment.create(segmentPath.toString(), 0, segmentBytes)) {
                for (long i = 0; i < frameCount; i++) {
                    if (segment.tryAppend(payload, payloadBytes) < 0) {
                        throw new AssertionError("failed to create benchmark frame " + i);
                    }
                }
            }

            for (int i = 0; i < warmup; i++) {
                verifyRecovery(segmentPath, segmentBytes, frameCount);
            }

            long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                verifyRecovery(segmentPath, segmentBytes, frameCount);
            }
            long elapsed = System.nanoTime() - start;
            double gib = (double) scannedBytes * iterations / (1024.0 * 1024.0 * 1024.0);
            double seconds = elapsed / 1_000_000_000.0;
            System.out.printf("corpus=%,d bytes, frames=%,d, payload=%,d bytes, iterations=%d, "
                            + "elapsed=%.3f s, throughput=%.3f GiB/s%n",
                    scannedBytes, frameCount, payloadBytes, iterations, seconds, gib / seconds);
        } finally {
            Unsafe.free(payload, payloadBytes, MemoryTag.NATIVE_DEFAULT);
            java.nio.file.Files.deleteIfExists(segmentPath);
            java.nio.file.Files.deleteIfExists(dir);
        }
    }

    private static long parseSize(String value) {
        String s = value.trim().toUpperCase();
        long multiplier = 1;
        if (s.endsWith("K") || s.endsWith("KB")) {
            multiplier = 1024L;
            s = s.substring(0, s.length() - (s.endsWith("KB") ? 2 : 1));
        } else if (s.endsWith("M") || s.endsWith("MB")) {
            multiplier = 1024L * 1024L;
            s = s.substring(0, s.length() - (s.endsWith("MB") ? 2 : 1));
        } else if (s.endsWith("G") || s.endsWith("GB")) {
            multiplier = 1024L * 1024L * 1024L;
            s = s.substring(0, s.length() - (s.endsWith("GB") ? 2 : 1));
        }
        return Long.parseLong(s.trim()) * multiplier;
    }

    private static void verifyRecovery(Path segmentPath, long expectedOffset, long expectedFrameCount) {
        try (MmapSegment segment = MmapSegment.openExisting(segmentPath.toString())) {
            if (segment.publishedOffset() != expectedOffset || segment.frameCount() != expectedFrameCount) {
                throw new AssertionError("recovery did not preserve the benchmark frames");
            }
        }
    }
}
