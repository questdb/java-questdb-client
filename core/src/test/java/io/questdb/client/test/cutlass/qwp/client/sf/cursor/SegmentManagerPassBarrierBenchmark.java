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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Standalone comparison of the service-pass ownership barriers used by
 * SegmentManager before and after its per-entry state change. This deliberately
 * has no pass/fail threshold: it reports target-JVM costs while the production
 * state machine and lifecycle tests establish correctness.
 *
 * <pre>
 * mvn -pl core test-compile
 * mvn -pl core exec:java -Dexec.classpathScope=test \
 *   -Dexec.mainClass=io.questdb.client.test.cutlass.qwp.client.sf.cursor.SegmentManagerPassBarrierBenchmark \
 *   -Dexec.args="--passes=10000000"
 * </pre>
 */
public final class SegmentManagerPassBarrierBenchmark {

    private static final Object MONITOR = new Object();
    private static final AtomicIntegerFieldUpdater<Entry> STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(Entry.class, "state");
    private static volatile long checksum;
    private static volatile int serviceProbe;

    public static void main(String[] args) {
        int passes = 10_000_000;
        for (String arg : args) {
            if (arg.startsWith("--passes=")) {
                passes = Integer.parseInt(arg.substring("--passes=".length()));
            } else {
                throw new IllegalArgumentException("unknown argument: " + arg);
            }
        }
        if (passes < 1) {
            throw new IllegalArgumentException("passes must be positive");
        }

        int warmup = Math.max(100_000, passes / 10);
        for (int rings : new int[]{1, 32, 256}) {
            measureMonitor(rings, warmup);
            measureAtomic(rings, warmup);
            long monitorNanos = measureMonitor(rings, passes);
            long atomicNanos = measureAtomic(rings, passes);
            System.out.printf(
                    "rings=%d passes=%d monitor(two enters)=%.2f ns/pass atomic(two CAS)=%.2f ns/pass ratio=%.2f%n",
                    rings,
                    passes,
                    (double) monitorNanos / passes,
                    (double) atomicNanos / passes,
                    (double) monitorNanos / atomicNanos);
        }
        System.out.println("checksum=" + checksum);
    }

    private static Entry[] entries(int count) {
        Entry[] entries = new Entry[count];
        for (int i = 0; i < count; i++) {
            entries[i] = new Entry();
        }
        return entries;
    }

    private static long measureAtomic(int rings, int passes) {
        Entry[] entries = entries(rings);
        long start = System.nanoTime();
        for (int i = 0; i < passes; i++) {
            Entry entry = entries[i % rings];
            if (!STATE_UPDATER.compareAndSet(entry, 0, 1)) {
                throw new AssertionError("claim failed");
            }
            service(entry);
            if (!STATE_UPDATER.compareAndSet(entry, 1, 0)) {
                throw new AssertionError("completion failed");
            }
        }
        checksum = checksum * 31 + entries[passes % rings].state;
        return System.nanoTime() - start;
    }

    private static long measureMonitor(int rings, int passes) {
        Entry[] entries = entries(rings);
        long start = System.nanoTime();
        for (int i = 0; i < passes; i++) {
            Entry entry = entries[i % rings];
            synchronized (MONITOR) {
                entry.state = 1;
            }
            service(entry);
            synchronized (MONITOR) {
                entry.state = 0;
            }
        }
        checksum = checksum * 31 + entries[passes % rings].state;
        return System.nanoTime() - start;
    }

    private static void service(Entry entry) {
        // Models the non-trivial serviceRing0 call between claim and complete
        // and prevents the JVM from coarsening both monitor regions into one.
        serviceProbe = entry.state;
    }

    private static final class Entry {
        volatile int state;
    }
}
