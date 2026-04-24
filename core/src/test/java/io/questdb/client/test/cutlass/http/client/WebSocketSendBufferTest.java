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

package io.questdb.client.test.cutlass.http.client;

import io.questdb.client.cutlass.http.client.WebSocketSendBuffer;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;

public class WebSocketSendBufferTest {

    @Test
    public void testPutUtf8InvalidSurrogatePair() throws Exception {
        assertMemoryLeak(() -> {
            try (WebSocketSendBuffer buf = new WebSocketSendBuffer(256)) {
                // High surrogate \uD800 followed by non-low-surrogate 'X'.
                // Should produce '?' for the lone high surrogate, then 'X'.
                buf.putUtf8("\uD800X");
                assertEquals(2, buf.getWritePos());
                assertEquals((byte) '?', Unsafe.getUnsafe().getByte(buf.getBufferPtr()));
                assertEquals((byte) 'X', Unsafe.getUnsafe().getByte(buf.getBufferPtr() + 1));
            }
        });
    }

    @Test
    public void testPutUtf8MixedAsciiAndNonAsciiAfterGrow() throws Exception {
        assertMemoryLeak(() -> {
            try (WebSocketSendBuffer buf = new WebSocketSendBuffer(8)) {
                String value = "abcdefghijklmnop世界世界世界世界世界世界世界世界世界世界";

                buf.putUtf8(value);

                int utf8Len = Utf8s.utf8Bytes(value);
                assertEquals(utf8Len, buf.getWritePos());
                assertEquals(value, Utf8s.stringFromUtf8Bytes(buf.getBufferPtr(), buf.getBufferPtr() + utf8Len));
            }
        });
    }
}
