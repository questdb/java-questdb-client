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

package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

// Error-safety of the SenderPool constructor prewarm catch that the
// teardown-hardening fix widened from catch (RuntimeException) to
// catch (Throwable). createUnlocked() runs a heavy native build path (mmap,
// flock, WebSocket connect) that can throw an Error under -ea; if the prewarm
// catch does not fire, the cleanup loop never runs and every already-built
// delegate leaks its flock + mmap'd ring + I/O thread -- resurrecting
// "sf slot already in use" on the next attempt.
//
// Sender is an interface, so the build path is faked with a Proxy whose close()
// flips a flag. The fake is injected via the package-private senderFactory
// constructor (reached by reflection -- the main module is declared `open`).
public class SenderPoolErrorSafetyTest {

    // Non-SF http config: fromConfig is never reached (the factory replaces the
    // build), but the constructor's eager config probe must still parse it.
    private static final String CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    // RED: catch (RuntimeException) -> the Error from the 2nd build skips the
    //      cleanup loop -> the 1st delegate is never closed.
    // GREEN: catch (Throwable) -> the cleanup loop closes the 1st delegate.
    @Test(timeout = 30_000)
    public void preWarmClosesBuiltDelegatesWhenBuildThrowsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicBoolean firstClosed = new AtomicBoolean(false);
            AtomicInteger calls = new AtomicInteger();
            IntFunction<Sender> factory = slotIndex -> {
                if (calls.incrementAndGet() >= 2) {
                    throw new AssertionError("injected native build failure");
                }
                return fakeSender(firstClosed);
            };

            try {
                newPool(CFG, 2, 2, 250, factory);
                Assert.fail("expected prewarm to propagate the injected Error");
            } catch (Throwable expected) {
                // expected -- construction aborts
            }

            Assert.assertTrue(
                    "prewarm leaked an already-built delegate: its close() was never called on an Error",
                    firstClosed.get());
        });
    }

    // Companion to the catch (RuntimeException) -> track-normal-completion fix in
    // PooledSender.close(). flush() can exit with an Error (AssertionError under
    // -ea, OutOfMemoryError, ...) as well as a RuntimeException; the wrapper is
    // unsafe to recycle either way because Sender does not clear its buffer on a
    // failed flush and WebSocket transport latches the failure.
    //
    // RED  (catch (RuntimeException)): the AssertionError from flush() is not
    //      caught, broken stays false, the finally runs giveBack() -> the broken
    //      wrapper is recycled -> the next borrow() hands back the SAME instance.
    // GREEN (track normal completion): flush() throwing leaves flushed=false ->
    //      discardBroken() -> the next borrow() builds a fresh wrapper.
    @Test(timeout = 30_000)
    public void flushErrorDiscardsBrokenSenderInsteadOfRecycling() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            IntFunction<Sender> factory = slotIndex -> flushThrowingSender();

            try (SenderPool pool = newPool(CFG, 1, 1, 1_000, factory)) {
                Sender first = pool.borrow();
                // Capture the underlying slot before close(): borrow() always hands
                // out a FRESH PooledSender wrapper, so assertNotSame(first, second)
                // on the wrappers is vacuously true and proves nothing -- it stays
                // true whether or not the broken slot was discarded. The pool
                // recycles slots, not wrappers, so a broken slot leaking back to
                // the next borrower shows up as the SAME slot. Assert on the slot.
                Object firstSlot = slotOf(first);
                try {
                    first.close();
                    Assert.fail("close() must propagate the Error thrown by flush()");
                } catch (AssertionError expected) {
                    // expected: the original throwable propagates naturally
                }

                Sender second = pool.borrow();
                try {
                    Assert.assertNotSame(
                            "a sender whose flush() exited with an Error must be discarded, not recycled",
                            firstSlot, slotOf(second));
                } finally {
                    // second's flush() also throws on close(); swallow on teardown.
                    try {
                        second.close();
                    } catch (AssertionError ignored) {
                        // expected
                    }
                }
            }
        });
    }

    private static Object slotOf(Sender pooledWrapper) throws Exception {
        Field f = PooledSender.class.getDeclaredField("slot");
        f.setAccessible(true);
        return f.get(pooledWrapper);
    }

    // Like fakeSender(), but flush() throws an Error to drive the
    // PooledSender.close() abnormal-exit branch.
    private static Sender flushThrowingSender() {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "flush":
                            throw new AssertionError("injected flush failure");
                        case "close":
                            return null;
                        case "toString":
                            return "FlushThrowingSender";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) return false;
                            if (rt == byte.class) return (byte) 0;
                            if (rt == short.class) return (short) 0;
                            if (rt == int.class) return 0;
                            if (rt == long.class) return 0L;
                            if (rt == float.class) return 0f;
                            if (rt == double.class) return 0d;
                            if (rt == char.class) return (char) 0;
                            if (rt == void.class) return null;
                            // fluent ArraySender methods return Sender
                            if (rt.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
    }

    // Companion to the SF slot-index reservation in borrow(): when
    // createUnlocked() throws on a borrow-triggered grow, the reserved slot
    // index MUST be returned via freeSlotIndex(). Otherwise slotInUse[idx] is
    // stuck true, pool capacity is permanently lowered, and the next borrow()
    // either trips the "no free SF slot index" invariant in allocateSlotIndex()
    // or eventually only times out -- the exact failure mode this PR fixes.
    //
    // The other SenderPool error-injection test fails in the constructor
    // pre-warm loop with a non-SF config (slotIndex == -1), so neither the
    // borrow-path freeSlotIndex nor the SF (slotIndex >= 0) case is otherwise
    // covered.
    //
    // RED  (freeSlotIndex(slotIndex) removed from the borrow catch): the 2nd
    //      borrow() throws IllegalStateException out of allocateSlotIndex().
    // GREEN (slot index returned): the 2nd borrow() reuses the slot and
    //      succeeds, proving capacity survived the failed grow.
    @Test(timeout = 30_000)
    public void borrowReleasesSfSlotIndexWhenCreationFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Unique, non-existent sf_dir: minSize=0 means no pre-warm, so the dir
            // is never created and the constructor's startup SF recovery is a no-op.
            // The factory replaces createUnlocked(), so localhost:1 is never dialed.
            String sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-sf-borrowfail-" + System.nanoTime()).toString();
            String sfCfg = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";

            AtomicInteger calls = new AtomicInteger();
            IntFunction<Sender> factory = slotIndex -> {
                // First borrow-triggered build fails (the slot index reserved for
                // it must be released); later builds succeed.
                if (calls.getAndIncrement() == 0) {
                    throw new AssertionError("injected native build failure on first grow");
                }
                return fakeSender(new AtomicBoolean());
            };

            try (SenderPool pool = newPool(sfCfg, 0, 1, 2_000, factory)) {
                try {
                    pool.borrow();
                    Assert.fail("borrow() must propagate the Error from the failed build");
                } catch (AssertionError expected) {
                    // expected: the original throwable propagates out of borrow()
                }

                // The single SF slot index must have been returned to the free set.
                // If it leaked, this borrow() trips the capacity invariant (or, in
                // the timeout-only variant, exhausts the acquire budget).
                Sender second = pool.borrow();
                try {
                    Assert.assertNotNull(
                            "after a failed grow the SF slot index must be reusable", second);
                } finally {
                    second.close();
                }
            }
        });
    }

    private static Sender fakeSender(AtomicBoolean closedFlag) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            closedFlag.set(true);
                            return null;
                        case "toString":
                            return "FakeSender";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) return false;
                            if (rt == byte.class) return (byte) 0;
                            if (rt == short.class) return (short) 0;
                            if (rt == int.class) return 0;
                            if (rt == long.class) return 0L;
                            if (rt == float.class) return 0f;
                            if (rt == double.class) return 0d;
                            if (rt == char.class) return (char) 0;
                            if (rt == void.class) return null;
                            // fluent ArraySender methods return Sender
                            if (rt.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
    }

    private static SenderPool newPool(
            String cfg, int min, int max, long acquireMs, IntFunction<Sender> senderFactory
    ) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, senderFactory);
    }
}
