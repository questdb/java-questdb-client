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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class WebSocketResponseTest {

    @Test
    public void testDurableAckFactory() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.durableAck(42L);
            Assert.assertTrue(response.isDurableAck());
            Assert.assertFalse(response.isSuccess());
            Assert.assertEquals(42L, response.getSequence());
            Assert.assertEquals(WebSocketResponse.STATUS_DURABLE_ACK, response.getStatus());
            Assert.assertEquals("DURABLE_ACK", response.getStatusName());
            Assert.assertNull(response.getErrorMessage());
        });
    }

    @Test
    public void testDurableAckIsStructurallyValidExact9Bytes() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.durableAck(7L);
            int size = response.serializedSize();
            Assert.assertEquals(WebSocketResponse.MIN_RESPONSE_SIZE, size);

            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                response.writeTo(ptr);
                Assert.assertTrue(WebSocketResponse.isStructurallyValid(ptr, size));
                // Extra bytes make it invalid
                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size + 1));
                // Too few bytes also invalid
                Assert.assertFalse(WebSocketResponse.isStructurallyValid(ptr, size - 1));
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDurableAckRoundTripThroughNativeMemory() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.durableAck(12345L);
            int size = original.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertTrue(parsed.isDurableAck());
                Assert.assertFalse(parsed.isSuccess());
                Assert.assertEquals(12345L, parsed.getSequence());
                Assert.assertNull(parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDurableAckDoesNotCarryErrorMessage() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.durableAck(99L);
            int size = response.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                response.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertNull(parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSuccessIsNotDurableAck() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.success(10L);
            Assert.assertTrue(response.isSuccess());
            Assert.assertFalse(response.isDurableAck());
            Assert.assertEquals("OK", response.getStatusName());
        });
    }

    @Test
    public void testErrorIsNotDurableAck() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse response = WebSocketResponse.error(
                    5L, WebSocketResponse.STATUS_PARSE_ERROR, "bad input");
            Assert.assertFalse(response.isDurableAck());
            Assert.assertFalse(response.isSuccess());
            Assert.assertEquals("PARSE_ERROR", response.getStatusName());
        });
    }

    @Test
    public void testSuccessRoundTripUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.success(77L);
            int size = original.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertTrue(parsed.isSuccess());
                Assert.assertFalse(parsed.isDurableAck());
                Assert.assertEquals(77L, parsed.getSequence());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testErrorRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            WebSocketResponse original = WebSocketResponse.error(
                    3L, WebSocketResponse.STATUS_INTERNAL_ERROR, "oops");
            int size = original.serializedSize();
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                original.writeTo(ptr);

                WebSocketResponse parsed = new WebSocketResponse();
                Assert.assertTrue(parsed.readFrom(ptr, size));
                Assert.assertFalse(parsed.isSuccess());
                Assert.assertFalse(parsed.isDurableAck());
                Assert.assertEquals(3L, parsed.getSequence());
                Assert.assertEquals("oops", parsed.getErrorMessage());
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }
}
