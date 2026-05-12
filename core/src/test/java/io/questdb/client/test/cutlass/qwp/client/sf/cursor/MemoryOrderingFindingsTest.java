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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderProgressDispatcher;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Red tests for cross-thread memory-ordering findings from PR-17 review.
 * Each test pins down an invariant that the JMM does NOT guarantee unless
 * a load-bearing field is declared {@code volatile}. They fail today and
 * turn green when the corresponding fields are made volatile.
 *
 * <p>x86's strong memory model usually masks plain-long staleness in
 * practice — a stress test would be flaky. The reflection check is
 * deterministic: the field either has the volatile modifier or it
 * doesn't. That's enough to lock in the invariant and keep it locked
 * once fixed.
 */
public class MemoryOrderingFindingsTest {

    /**
     * M3: {@code CursorSendEngine.closed} is checked-then-set with no fence,
     * and the engine has no documented single-threaded close contract. A
     * second concurrent {@code close()} on a fresh engine can pass the gate
     * before the first writes {@code closed=true}, leading to double
     * deregister / double ring.close() / double slotLock.close() under load.
     * Declare it volatile and use a CAS, or document and enforce single-thread.
     */
    @Test
    public void testCursorSendEngineClosedIsVolatile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Field f;
            try {
                f = CursorSendEngine.class.getDeclaredField("closed");
            } catch (NoSuchFieldException nsf) {
                fail("CursorSendEngine.closed field is missing; close() guard removed?");
                return;
            }
            assertTrue(
                    "CursorSendEngine.closed must be volatile — close() is publicly "
                            + "callable from any thread (sender.close(), JVM shutdown hooks, "
                            + "test cleanup), and a non-volatile check-then-set lets two "
                            + "racing closers both pass the if-closed gate and double-close "
                            + "the manager / ring / slotLock.",
                    Modifier.isVolatile(f.getModifiers()));
        });
    }

    /**
     * M1: {@code MmapSegment.frameCount} is read cross-thread by the I/O
     * thread (via {@code SegmentRing.findSegmentContaining} and
     * {@code SegmentRing.appendOrFsn}-time computations) but written by the
     * producer in {@code tryAppend} without taking the ring monitor. The
     * synchronized accessors give one-sided fencing only — the writer
     * publishes {@code frameCount} with no happens-before to the reader.
     * Declare it volatile.
     */
    @Test
    public void testMmapSegmentFrameCountIsVolatile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Field f = MmapSegment.class.getDeclaredField("frameCount");
            assertTrue(
                    "MmapSegment.frameCount must be volatile — it is written by "
                            + "the producer thread and read by the I/O thread without a "
                            + "common monitor (the writer is not synchronized on the ring). "
                            + "Without volatile the JMM permits the I/O thread to observe a "
                            + "stale frameCount, which makes findSegmentContaining return null "
                            + "for an FSN that was actually published.",
                    Modifier.isVolatile(f.getModifiers()));
        });
    }

    /**
     * H9: {@code SenderConnectionDispatcher.dispatcherThread} is the
     * first-null guard of a classic double-checked lazy-init: {@code offer()}
     * reads it without the {@code lock} monitor, while
     * {@code startDispatcherIfNeeded()} and {@code close()} write it under
     * the monitor. Without {@code volatile} the off-lock read has no
     * happens-before with the under-lock writes -- a JMM race. The
     * synchronized re-check inside {@code startDispatcherIfNeeded()} keeps
     * the bug benign today, but the field must carry the volatile modifier
     * so the contract is spec-correct.
     */
    @Test
    public void testSenderConnectionDispatcherThreadIsVolatile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Field f = SenderConnectionDispatcher.class.getDeclaredField("dispatcherThread");
            assertTrue(
                    "SenderConnectionDispatcher.dispatcherThread must be volatile -- "
                            + "offer() reads it off-lock as the double-checked first-null "
                            + "guard, while startDispatcherIfNeeded() and close() write it "
                            + "under `lock`. Without volatile the off-lock read can observe "
                            + "stale null indefinitely on weak memory models, forcing every "
                            + "offer() through a redundant synchronized re-check.",
                    Modifier.isVolatile(f.getModifiers()));
        });
    }

    /**
     * H9: {@code SenderErrorDispatcher.dispatcherThread} is the first-null
     * guard of a classic double-checked lazy-init: {@code offer()} reads it
     * without the {@code lock} monitor, while
     * {@code startDispatcherIfNeeded()} and {@code close()} write it under
     * the monitor. Same JMM race, same fix as
     * {@link #testSenderConnectionDispatcherThreadIsVolatile()}.
     */
    @Test
    public void testSenderErrorDispatcherThreadIsVolatile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Field f = SenderErrorDispatcher.class.getDeclaredField("dispatcherThread");
            assertTrue(
                    "SenderErrorDispatcher.dispatcherThread must be volatile -- "
                            + "offer() reads it off-lock as the double-checked first-null "
                            + "guard, while startDispatcherIfNeeded() and close() write it "
                            + "under `lock`. Without volatile the off-lock read can observe "
                            + "stale null indefinitely on weak memory models, forcing every "
                            + "offer() through a redundant synchronized re-check.",
                    Modifier.isVolatile(f.getModifiers()));
        });
    }

    /**
     * H9: {@code SenderProgressDispatcher.dispatcherThread} mirrors the
     * pattern in the connection and error dispatchers: {@code offer()} reads
     * the field twice off-lock (once before the lazy-start gate, once after,
     * to obtain the handle passed to {@code LockSupport.unpark}) while
     * {@code startDispatcherIfNeeded()} and {@code close()} write it under
     * {@code lock}. Same JMM race, same fix as
     * {@link #testSenderConnectionDispatcherThreadIsVolatile()}; an extra
     * symptom here is that a stale-null on the post-gate read silently
     * skips the unpark and adds one idle-tick of latency.
     */
    @Test
    public void testSenderProgressDispatcherThreadIsVolatile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Field f = SenderProgressDispatcher.class.getDeclaredField("dispatcherThread");
            assertTrue(
                    "SenderProgressDispatcher.dispatcherThread must be volatile -- "
                            + "offer() reads it off-lock twice (as the double-checked "
                            + "first-null guard and to obtain the unpark target), while "
                            + "startDispatcherIfNeeded() and close() write it under `lock`. "
                            + "Without volatile a stale-null on the post-gate read silently "
                            + "skips LockSupport.unpark(), delaying delivery by one idle "
                            + "tick.",
                    Modifier.isVolatile(f.getModifiers()));
        });
    }
}
