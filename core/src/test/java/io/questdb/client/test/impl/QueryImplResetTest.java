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

import io.questdb.client.Query;
import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class QueryImplResetTest {

    /**
     * Regression test for the state-carryover bug between consecutive
     * submits on the per-thread {@code QuestDB#query()} handle.
     * <p>
     * The Javadoc on both {@code Query} and {@code QuestDB#query()} promises
     * that the returned instance is "reset to empty" / "in a reset state".
     * Before the fix, {@code QuestDBImpl.query()} returned the bare
     * thread-local without nulling {@code userHandler} / {@code userBinds},
     * so the second call below would silently reuse {@code h1}:
     * <pre>
     *   db.query().sql("SELECT 1").handler(h1).submit().await();
     *   db.query().sql("SELECT 2").submit();    // no .handler() -- reuses h1
     * </pre>
     * The {@code if (userHandler == null)} check in {@code submit()} could
     * not catch the misuse because the field was still set from the prior
     * submit.
     * <p>
     * The fix is {@code QueryImpl.resetIfDone()}, invoked from
     * {@code QuestDBImpl.query()} before the per-thread handle is returned.
     * This test reaches into {@code QueryImpl} via reflection (the class is
     * package-private and lives in a different package from this test) and
     * asserts the reset clears all three configured fields when the prior
     * run is in a terminal state.
     */
    @Test
    public void testResetIfDoneClearsBuilderStateInTerminalState() throws Exception {
        Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");
        Class<?> poolClass = Class.forName("io.questdb.client.impl.QueryClientPool");

        Constructor<?> ctor = queryImplClass.getDeclaredConstructor(poolClass);
        ctor.setAccessible(true);
        // QueryImpl never dereferences the pool outside of submit(); a null
        // pool is fine for this state-only test.
        Query q = (Query) ctor.newInstance(new Object[]{null});

        // Mirror the post-submit().await() state: builder fields set,
        // done flag true (the constructor default).
        QwpColumnBatchHandler h = new NoopHandler();
        QwpBindSetter b = values -> {
            // no-op
        };
        q.sql("SELECT 1").binds(b).handler(h);

        Method reset = queryImplClass.getDeclaredMethod("resetIfDone");
        reset.setAccessible(true);
        reset.invoke(q);

        Field handlerF = queryImplClass.getDeclaredField("userHandler");
        Field bindsF = queryImplClass.getDeclaredField("userBinds");
        Field sqlBufF = queryImplClass.getDeclaredField("sqlBuffer");
        handlerF.setAccessible(true);
        bindsF.setAccessible(true);
        sqlBufF.setAccessible(true);

        Assert.assertNull("userHandler must be cleared so a follow-up submit() without .handler() fails fast",
                handlerF.get(q));
        Assert.assertNull("userBinds must be cleared so a follow-up submit() without .binds() does not reuse the prior setter",
                bindsF.get(q));
        CharSequence sqlBuffer = (CharSequence) sqlBufF.get(q);
        Assert.assertEquals("sqlBuffer must be empty so a follow-up submit() without .sql() throws 'sql is required'",
                0, sqlBuffer.length());
    }

    /**
     * Symmetric guard: when a submit is in flight ({@code done == false}),
     * {@code resetIfDone()} must NOT touch the configured fields. The
     * dispatched worker thread is reading {@code sqlBuffer} in
     * {@code runOn()} and {@code userHandler} via the wrapping handler;
     * clearing them mid-flight would race.
     */
    @Test
    public void testResetIfDoneIsNoOpWhileSubmitInFlight() throws Exception {
        Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");
        Class<?> poolClass = Class.forName("io.questdb.client.impl.QueryClientPool");

        Constructor<?> ctor = queryImplClass.getDeclaredConstructor(poolClass);
        ctor.setAccessible(true);
        Query q = (Query) ctor.newInstance(new Object[]{null});

        QwpColumnBatchHandler h = new NoopHandler();
        QwpBindSetter b = values -> {
            // no-op
        };
        q.sql("SELECT 1").binds(b).handler(h);

        // Flip the in-flight flag by setting done=false directly.
        Field doneF = queryImplClass.getDeclaredField("done");
        doneF.setAccessible(true);
        doneF.setBoolean(q, false);

        Method reset = queryImplClass.getDeclaredMethod("resetIfDone");
        reset.setAccessible(true);
        reset.invoke(q);

        Field handlerF = queryImplClass.getDeclaredField("userHandler");
        Field bindsF = queryImplClass.getDeclaredField("userBinds");
        Field sqlBufF = queryImplClass.getDeclaredField("sqlBuffer");
        handlerF.setAccessible(true);
        bindsF.setAccessible(true);
        sqlBufF.setAccessible(true);

        Assert.assertSame("userHandler must survive resetIfDone() while a submit is in flight",
                h, handlerF.get(q));
        Assert.assertSame("userBinds must survive resetIfDone() while a submit is in flight",
                b, bindsF.get(q));
        CharSequence sqlBuffer = (CharSequence) sqlBufF.get(q);
        Assert.assertEquals("sqlBuffer must survive resetIfDone() while a submit is in flight",
                "SELECT 1", sqlBuffer.toString());
    }

    private static final class NoopHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onEnd(long requestId, long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }

        @Override
        public void onError(long requestId, byte status, String message) {
        }

        @Override
        public void onExecDone(long requestId, short opType, long rowsAffected) {
        }

        @Override
        public void onFailoverReset(long requestId, QwpServerInfo newNode) {
        }
    }
}
