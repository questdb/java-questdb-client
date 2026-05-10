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

import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerPool;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Coverage of {@link BackgroundDrainerPool#setListener} propagation.
 * <p>
 * The pool's listener is a fallback default applied at submit time only
 * when the drainer doesn't already carry one. Per-drainer listeners
 * (set by the caller before submit) win over the pool default. A null
 * pool listener leaves drainers untouched.
 * <p>
 * Tests use the {@link BackgroundDrainer#BackgroundDrainer() test-only
 * no-arg constructor} to obtain a no-op drainer — enough to assert
 * listener state without standing up real I/O. The pool's executor will
 * fail when it tries to run the no-op drainer, but the failure is
 * swallowed by the executor and is unrelated to what the assertions check.
 */
public class BackgroundDrainerPoolListenerTest {

    @Test
    public void testPerDrainerListenerWinsOverPoolDefault() {
        BackgroundDrainerListener poolListener = new NoopListener();
        BackgroundDrainerListener perDrainerListener = new NoopListener();
        try (BackgroundDrainerPool pool = new BackgroundDrainerPool(1)) {
            pool.setListener(poolListener);
            BackgroundDrainer drainer = new BackgroundDrainer();
            drainer.setListener(perDrainerListener);
            pool.submit(drainer);
            assertSame("per-drainer listener must not be overridden by pool default",
                    perDrainerListener, drainer.getListener());
        }
    }

    @Test
    public void testPoolListenerAppliedWhenDrainerHasNone() {
        BackgroundDrainerListener poolListener = new NoopListener();
        try (BackgroundDrainerPool pool = new BackgroundDrainerPool(1)) {
            pool.setListener(poolListener);
            BackgroundDrainer drainer = new BackgroundDrainer();
            pool.submit(drainer);
            assertSame("pool default listener must be applied to a drainer with none set",
                    poolListener, drainer.getListener());
        }
    }

    @Test
    public void testPoolListenerNullDoesNotApply() {
        try (BackgroundDrainerPool pool = new BackgroundDrainerPool(1)) {
            // Default state: no listener configured.
            BackgroundDrainer drainer = new BackgroundDrainer();
            pool.submit(drainer);
            assertNull("null pool listener must not touch the drainer",
                    drainer.getListener());
        }
    }

    @Test
    public void testPoolListenerRotation() {
        BackgroundDrainerListener first = new NoopListener();
        BackgroundDrainerListener second = new NoopListener();
        try (BackgroundDrainerPool pool = new BackgroundDrainerPool(2)) {
            pool.setListener(first);
            BackgroundDrainer d1 = new BackgroundDrainer();
            pool.submit(d1);
            assertSame(first, d1.getListener());
            // Rotate the default mid-flight; later submits must pick up the new value.
            pool.setListener(second);
            BackgroundDrainer d2 = new BackgroundDrainer();
            pool.submit(d2);
            assertSame(second, d2.getListener());
            // Earlier drainer's listener is unchanged.
            assertSame(first, d1.getListener());
        }
    }

    private static final class NoopListener implements BackgroundDrainerListener {
        @Override
        public void onDurableAckPersistentFailure(String slotPath, int totalAttempts, long elapsedMillis) {
            // no-op
        }

        @Override
        public void onDurableAckUnavailable(String slotPath, int attemptNumber) {
            // no-op
        }
    }
}
