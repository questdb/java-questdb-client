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

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderErrorHandler;
import org.junit.Assert;
import org.junit.Test;

public class DefaultSenderErrorHandlerTest {

    @Test
    public void testDoesNotThrowOnNullableFields() {
        // Tableless and message-less errors must not NPE — both fields are
        // documented nullable on SenderError.
        SenderError e = new SenderError(
                SenderError.Category.PROTOCOL_VIOLATION,
                SenderError.Policy.TERMINAL,
                SenderError.NO_STATUS_BYTE,
                null, // null serverMessage
                SenderError.NO_MESSAGE_SEQUENCE,
                10L, 20L,
                null, // null tableName
                0L
        );
        DefaultSenderErrorHandler.INSTANCE.onError(e); // must not throw
    }

    @Test
    public void testHandlesAllCategoriesWithoutThrowing() {
        // Defense against missing case branches: every category, both
        // policies, must format cleanly.
        for (SenderError.Category c : SenderError.Category.values()) {
            for (SenderError.Policy p : SenderError.Policy.values()) {
                SenderError e = new SenderError(
                        c, p, 0x42, "msg", 7L, 100L, 100L, "tbl", 0L);
                DefaultSenderErrorHandler.INSTANCE.onError(e);
            }
        }
    }

    @Test
    public void testInstanceIsSingleton() {
        Assert.assertSame(DefaultSenderErrorHandler.INSTANCE,
                DefaultSenderErrorHandler.INSTANCE);
        Assert.assertNotNull(DefaultSenderErrorHandler.INSTANCE);
    }
}
