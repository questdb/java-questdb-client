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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;

/**
 * Ingest-side mirror of {@code QueryLeaseGenerationTest}: a stale pooled-Sender
 * handle (held after close, with the slot since re-borrowed) must not disturb a
 * later borrow of the same slot. {@code PooledSender} is now a fresh per-borrow
 * wrapper carrying the lease generation; the reused {@code SenderSlot} validates
 * it under the pool lock so a stale close/write is dropped.
 * <p>
 * Reaches package-private internals by reflection (same white-box style as the
 * other tests here); {@code SenderSlot} is constructed with a {@code null}
 * delegate, which the paths under test never dereference.
 */
public class SenderLeaseGenerationTest {

    private static final String DEAD_HTTP_CONFIG =
            "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    /**
     * The pool-wide blast radius: a stale (duplicate / post-reborrow) close must
     * never enqueue a slot a live borrower owns, or two borrowers would write
     * into one delegate's buffer at once. {@code giveBack} validates the lease
     * generation under the pool lock, so this is impossible.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testStaleGiveBackDoesNotEnqueueSlotTwice() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Class<?> slotClass = Class.forName("io.questdb.client.impl.SenderSlot");
            Constructor<?> slotCtor = slotClass.getDeclaredConstructor(Sender.class, SenderPool.class, int.class);
            slotCtor.setAccessible(true);
            Method bump = slotClass.getDeclaredMethod("bumpGeneration");
            bump.setAccessible(true);
            Method generation = slotClass.getDeclaredMethod("generation");
            generation.setAccessible(true);
            Constructor<PooledSender> leaseCtor =
                    PooledSender.class.getDeclaredConstructor(slotClass, long.class);
            leaseCtor.setAccessible(true);
            Field availableF = SenderPool.class.getDeclaredField("available");
            availableF.setAccessible(true);

            try (SenderPool pool = new SenderPool(
                    DEAD_HTTP_CONFIG, /*minSize*/ 0, /*maxSize*/ 2,
                    /*acquireTimeoutMillis*/ 1_000L,
                    /*idleTimeoutMillis*/ Long.MAX_VALUE,
                    /*maxLifetimeMillis*/ Long.MAX_VALUE)) {
                ArrayDeque<Object> available = (ArrayDeque<Object>) availableF.get(pool);
                Object slot = slotCtor.newInstance(null, pool, -1);

                // borrow #1 stamps generation 1; lease A captures 1.
                bump.invoke(slot);
                Assert.assertEquals(1L, generation.invoke(slot));
                PooledSender leaseA = leaseCtor.newInstance(slot, 1L);

                // close A -> giveBack(A): matches, enqueues once.
                pool.giveBack(leaseA);
                Assert.assertEquals("valid close must enqueue the slot once", 1, available.size());

                // duplicate close A (e.g. explicit close + try-with-resources)
                // -> giveBack(A): generation already bumped to 2, so it is dropped.
                pool.giveBack(leaseA);
                Assert.assertEquals("duplicate close of the same lease must be dropped",
                        1, available.size());

                // borrow #2 hands the slot to a new borrower B: pull it out, stamp 3.
                available.pollFirst();
                bump.invoke(slot);
                Assert.assertEquals(3L, generation.invoke(slot));
                PooledSender leaseB = leaseCtor.newInstance(slot, 3L);

                // A stray close from the long-dead lease A -> dropped, so B's slot is
                // NOT re-enqueued while B still owns it.
                pool.giveBack(leaseA);
                Assert.assertEquals("a post-reborrow stale close must NOT enqueue the slot "
                        + "while another borrower owns it", 0, available.size());

                // B's own close -> giveBack(B): matches, enqueues legitimately.
                pool.giveBack(leaseB);
                Assert.assertEquals("the current borrower's close must still work",
                        1, available.size());
            }
        });
    }

    /**
     * A stale lease's data write must be rejected (not silently land in a slot a
     * later borrower now owns). The generation guard in
     * {@code SenderSlot.live()} throws before the delegate is touched.
     */
    @Test
    public void testStaleWriteIsRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Class<?> slotClass = Class.forName("io.questdb.client.impl.SenderSlot");
            Constructor<?> slotCtor = slotClass.getDeclaredConstructor(Sender.class, SenderPool.class, int.class);
            slotCtor.setAccessible(true);
            Method bump = slotClass.getDeclaredMethod("bumpGeneration");
            bump.setAccessible(true);
            Constructor<PooledSender> leaseCtor =
                    PooledSender.class.getDeclaredConstructor(slotClass, long.class);
            leaseCtor.setAccessible(true);

            Object slot = slotCtor.newInstance(null, null, -1);
            bump.invoke(slot); // generation -> 1, lease A captures 1
            PooledSender leaseA = leaseCtor.newInstance(slot, 1L);
            bump.invoke(slot); // released
            bump.invoke(slot); // re-borrowed -> generation 3

            try {
                leaseA.table("x");
                Assert.fail("a stale lease's write must throw, not reach the re-borrowed slot");
            } catch (IllegalStateException expected) {
                Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
            }
        });
    }
}
