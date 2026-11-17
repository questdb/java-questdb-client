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

package io.questdb.client.test;

import io.questdb.client.QueryException;
import io.questdb.client.cutlass.qwp.client.QwpEgressMsgKind;
import org.junit.Assert;
import org.junit.Test;

public class QueryExceptionTest {

    @Test
    public void testPosition() {
        QueryException e = new QueryException((byte) 5, "table does not exist", 14);
        Assert.assertEquals(5, e.getStatus());
        Assert.assertEquals("table does not exist", e.getMessage());
        Assert.assertEquals(14, e.getPosition());
        Assert.assertNull(e.getCause());
    }

    @Test
    public void testPositionAbsentByDefault() {
        QueryException e = new QueryException((byte) 5, "x");
        Assert.assertEquals(5, e.getStatus());
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, e.getPosition());

        IllegalStateException cause = new IllegalStateException();
        e = new QueryException((byte) 0, "x", cause);
        Assert.assertSame(cause, e.getCause());
        Assert.assertEquals(QwpEgressMsgKind.NO_ERROR_POSITION, e.getPosition());
    }
}
