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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;

public class WebSocketSendBufferTest {

    @Test
    public void testAppendBinaryFrameWritesBackToBackMaskedFrames() throws Exception {
        assertMemoryLeak(() -> {
            int size = 10 + 4 + 296;
            long src = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try (WebSocketSendBuffer buf = new WebSocketSendBuffer(64)) {
                for (int i = 0; i < size; i++) {
                    Unsafe.getUnsafe().putByte(src + i, (byte) (i * 7 + 3));
                }
                long single = src;
                long first = src + 10;
                long second = src + 14;
                buf.appendBinaryFrame(single, 10, 0, 0);
                buf.appendBinaryFrame(first, 4, second, 296);

                long p = buf.getBufferPtr();
                // frame 1: FIN + binary, mask bit + 7-bit length, 4-byte mask, payload
                assertEquals(0x82, Unsafe.getUnsafe().getByte(p) & 0xFF);
                assertEquals(0x80 | 10, Unsafe.getUnsafe().getByte(p + 1) & 0xFF);
                assertUnmaskedPayload(p + 2, single, 10);
                long q = p + 2 + 4 + 10;
                // frame 2: FIN + binary, mask bit + 126, 16-bit big-endian length, mask, payload
                assertEquals(0x82, Unsafe.getUnsafe().getByte(q) & 0xFF);
                assertEquals(0x80 | 126, Unsafe.getUnsafe().getByte(q + 1) & 0xFF);
                int length = ((Unsafe.getUnsafe().getByte(q + 2) & 0xFF) << 8) | (Unsafe.getUnsafe().getByte(q + 3) & 0xFF);
                assertEquals(300, length);
                assertUnmaskedPayload(q + 4, first, 300);
                assertEquals("both frames, exact-size headers", (2 + 4 + 10) + (4 + 4 + 300), buf.getWritePos());
            } finally {
                Unsafe.free(src, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

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

    // The 4-byte mask key sits at maskAddr and the masked payload right after it;
    // unmasking it must give back the len source bytes at expected.
    private static void assertUnmaskedPayload(long maskAddr, long expected, int len) {
        for (int i = 0; i < len; i++) {
            int mask = Unsafe.getUnsafe().getByte(maskAddr + (i & 3)) & 0xFF;
            int sent = Unsafe.getUnsafe().getByte(maskAddr + 4 + i) & 0xFF;
            assertEquals("payload byte " + i, Unsafe.getUnsafe().getByte(expected + i) & 0xFF, sent ^ mask);
        }
    }
}
