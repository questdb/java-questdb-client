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

import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpServerInfo;
import io.questdb.client.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class QueryImplResetTest {

    /**
     * The Javadoc on both {@code Query} and {@code QuestDB#borrowQuery()}
     * promises the leased handle is handed out "reset to empty". The reset is
     * {@code QueryImpl.resetForBorrow()}, invoked from {@code QueryWorker.lease()}
     * when {@code borrowQuery()} hands the pre-allocated handle out. It must
     * clear the builder state (SQL, binds, handler) so a follow-up
     * {@code submit()} cannot silently reuse a prior borrow's handler/binds,
     * and it must leave the handle idle (done).
     * <p>
     * The reset is unconditional: the leased worker was just acquired from the
     * pool, so it is always idle (done) at borrow time. This test reaches into
     * {@code QueryImpl} by reflection (the class is package-private and lives
     * in a different package from this test). Builder state is seeded directly
     * via reflection rather than through the {@code Query} API because the
     * lease-generation guard on the setters would dereference the (null) worker.
     */
    @Test
    public void testResetForBorrowClearsBuilderState() throws Exception {
        Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");
        Class<?> workerClass = Class.forName("io.questdb.client.impl.QueryWorker");

        Constructor<?> ctor = queryImplClass.getDeclaredConstructor(workerClass);
        ctor.setAccessible(true);
        // resetForBorrow() never dereferences the worker; a null worker is fine
        // for this state-only test.
        Object q = ctor.newInstance(new Object[]{null});

        Field handlerF = queryImplClass.getDeclaredField("userHandler");
        Field bindsF = queryImplClass.getDeclaredField("userBinds");
        Field sqlBufF = queryImplClass.getDeclaredField("sqlBuffer");
        Field doneF = queryImplClass.getDeclaredField("done");
        handlerF.setAccessible(true);
        bindsF.setAccessible(true);
        sqlBufF.setAccessible(true);
        doneF.setAccessible(true);

        // Seed builder state as a prior borrow would have left it.
        handlerF.set(q, new NoopHandler());
        bindsF.set(q, (QwpBindSetter) values -> {
            // no-op
        });
        ((StringSink) sqlBufF.get(q)).put("SELECT 1");
        doneF.setBoolean(q, false);

        Method reset = queryImplClass.getDeclaredMethod("resetForBorrow");
        reset.setAccessible(true);
        reset.invoke(q);

        Assert.assertNull("userHandler must be cleared so a follow-up submit() without .handler() fails fast",
                handlerF.get(q));
        Assert.assertNull("userBinds must be cleared so a follow-up submit() without .binds() does not reuse the prior setter",
                bindsF.get(q));
        CharSequence sqlBuffer = (CharSequence) sqlBufF.get(q);
        Assert.assertEquals("sqlBuffer must be empty so a follow-up submit() without .sql() throws 'sql is required'",
                0, sqlBuffer.length());
        Assert.assertTrue("done must be true so the handle starts idle, not in flight",
                doneF.getBoolean(q));
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
